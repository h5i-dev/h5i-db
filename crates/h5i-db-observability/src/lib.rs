//! Lightweight observability contracts with no Arrow or DataFusion dependency.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Hash normalized SQL so telemetry never stores query text or literals.
pub fn query_fingerprint(sql: &str) -> String {
    let normalized = sql.split_whitespace().collect::<Vec<_>>().join(" ");
    blake3::hash(normalized.as_bytes()).to_hex().to_string()
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScanMetrics {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_id: Option<Uuid>,
    pub table: String,
    pub version: u64,
    pub segments_total: usize,
    pub segments_pruned: usize,
    pub segments_scanned: usize,
    pub bytes_scheduled: u64,
    #[serde(default)]
    pub predicate_cache_lookups: usize,
    #[serde(default)]
    pub predicate_cache_hits: usize,
    #[serde(default)]
    pub predicate_cache_misses: usize,
    #[serde(default)]
    pub predicate_cache_builds: usize,
    #[serde(default)]
    pub predicate_cache_rejected: usize,
    #[serde(default)]
    pub predicate_cache_row_groups_reused: usize,
    #[serde(default)]
    pub predicate_cache_evictions: usize,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum QueryStatus {
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct OperatorPerformanceMetrics {
    pub name: String,
    pub output_rows: u64,
    pub elapsed_compute_ns: u64,
    pub bytes_scanned: u64,
    pub spill_count: u64,
    pub spilled_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct QueryPerformanceReport {
    pub query_id: Uuid,
    pub query_fingerprint: String,
    pub started_at_ns: i64,
    pub status: QueryStatus,
    pub planning_ns: u64,
    pub execution_ns: u64,
    pub output_batches: u64,
    pub output_rows: u64,
    pub bytes_scanned: u64,
    pub scan_output_rows: u64,
    pub row_groups_pruned: u64,
    pub page_index_rows_pruned: u64,
    pub pushdown_rows_pruned: u64,
    pub spill_count: u64,
    pub spilled_bytes: u64,
    pub sort_operators: usize,
    #[serde(default)]
    pub predicate_cache_lookups: usize,
    #[serde(default)]
    pub predicate_cache_hits: usize,
    #[serde(default)]
    pub predicate_cache_misses: usize,
    #[serde(default)]
    pub predicate_cache_builds: usize,
    #[serde(default)]
    pub predicate_cache_rejected: usize,
    #[serde(default)]
    pub predicate_cache_row_groups_reused: usize,
    #[serde(default)]
    pub predicate_cache_evictions: usize,
    pub scans: Vec<ScanMetrics>,
    pub operators: Vec<OperatorPerformanceMetrics>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkloadTelemetryEnvelope {
    pub format: u32,
    pub session_id: Uuid,
    pub reports: Vec<QueryPerformanceReport>,
}

#[derive(Debug, Clone)]
pub struct WorkloadTelemetryBuffer {
    capacity: usize,
    inner: Arc<Mutex<VecDeque<QueryPerformanceReport>>>,
}

impl WorkloadTelemetryBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            inner: Arc::new(Mutex::new(VecDeque::with_capacity(capacity))),
        }
    }

    pub fn record(&self, report: QueryPerformanceReport) {
        if self.capacity == 0 {
            return;
        }
        let mut guard = self.inner.lock().unwrap();
        while guard.len() >= self.capacity {
            guard.pop_front();
        }
        guard.push_back(report);
    }

    pub fn snapshot(&self) -> Vec<QueryPerformanceReport> {
        self.inner.lock().unwrap().iter().cloned().collect()
    }

    pub fn clear(&self) {
        self.inner.lock().unwrap().clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report(id: Uuid, sql: &str) -> QueryPerformanceReport {
        QueryPerformanceReport {
            query_id: id,
            query_fingerprint: query_fingerprint(sql),
            started_at_ns: 1,
            status: QueryStatus::Succeeded,
            planning_ns: 2,
            execution_ns: 3,
            output_batches: 1,
            output_rows: 4,
            bytes_scanned: 5,
            scan_output_rows: 4,
            row_groups_pruned: 0,
            page_index_rows_pruned: 0,
            pushdown_rows_pruned: 0,
            spill_count: 0,
            spilled_bytes: 0,
            sort_operators: 0,
            predicate_cache_lookups: 0,
            predicate_cache_hits: 0,
            predicate_cache_misses: 0,
            predicate_cache_builds: 0,
            predicate_cache_rejected: 0,
            predicate_cache_row_groups_reused: 0,
            predicate_cache_evictions: 0,
            scans: Vec::new(),
            operators: Vec::new(),
        }
    }

    #[test]
    fn fingerprint_normalizes_whitespace_without_exposing_sql() {
        let a = query_fingerprint("SELECT *  FROM trades\nWHERE symbol = 'SECRET'");
        let b = query_fingerprint("SELECT * FROM trades WHERE symbol = 'SECRET'");
        assert_eq!(a, b);
        assert_eq!(a.len(), 64);
        assert!(!a.contains("SECRET"));
    }

    #[test]
    fn telemetry_is_bounded_and_round_trips() {
        let buffer = WorkloadTelemetryBuffer::new(1);
        let first = report(Uuid::new_v4(), "SELECT 1");
        let second = report(Uuid::new_v4(), "SELECT 2");
        buffer.record(first);
        buffer.record(second.clone());
        assert_eq!(buffer.snapshot(), vec![second.clone()]);

        let envelope = WorkloadTelemetryEnvelope {
            format: 1,
            session_id: Uuid::new_v4(),
            reports: buffer.snapshot(),
        };
        let json = serde_json::to_string(&envelope).unwrap();
        assert!(!json.contains("SELECT"));
        assert_eq!(
            serde_json::from_str::<WorkloadTelemetryEnvelope>(&json).unwrap(),
            envelope
        );

        buffer.clear();
        assert!(buffer.snapshot().is_empty());
    }

    #[test]
    fn zero_capacity_disables_collection() {
        let buffer = WorkloadTelemetryBuffer::new(0);
        buffer.record(report(Uuid::new_v4(), "SELECT 1"));
        assert!(buffer.snapshot().is_empty());
    }
}
