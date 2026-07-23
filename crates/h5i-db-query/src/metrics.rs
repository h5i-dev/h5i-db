//! Query-local performance reporting and bounded workload telemetry.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Instant;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::dataframe::DataFrame;
use datafusion::error::Result as DfResult;
use datafusion::physical_plan::{
    execute_stream, ExecutionPlan, RecordBatchStream, SendableRecordBatchStream,
};
use futures::{Stream, StreamExt};
use h5i_db_observability::{query_fingerprint, WorkloadTelemetryBuffer};
pub use h5i_db_observability::{
    OperatorPerformanceMetrics, QueryPerformanceReport, QueryStatus, ScanMetrics,
    WorkloadTelemetryEnvelope,
};
use uuid::Uuid;

tokio::task_local! {
    static ACTIVE_QUERY_ID: Uuid;
}

pub(crate) async fn query_scope<F>(query_id: Uuid, future: F) -> F::Output
where
    F: Future,
{
    ACTIVE_QUERY_ID.scope(query_id, future).await
}

fn active_query_id() -> Option<Uuid> {
    ACTIVE_QUERY_ID.try_with(|id| *id).ok()
}

/// Shared sink used by snapshot-bound providers. Query IDs make concurrent
/// planning attributable without requiring a global drain.
#[derive(Debug, Clone, Default)]
pub struct ScanMetricsCollector {
    inner: Arc<Mutex<Vec<ScanMetrics>>>,
}

impl ScanMetricsCollector {
    pub fn record(&self, mut metrics: ScanMetrics) {
        metrics.query_id = metrics.query_id.or_else(active_query_id);
        self.inner.lock().unwrap().push(metrics);
    }

    /// Legacy session-wide drain retained for compatibility.
    pub fn take(&self) -> Vec<ScanMetrics> {
        std::mem::take(&mut *self.inner.lock().unwrap())
    }

    pub(crate) fn take_for(&self, query_id: Uuid) -> Vec<ScanMetrics> {
        let mut guard = self.inner.lock().unwrap();
        let mut selected = Vec::new();
        let mut retained = Vec::with_capacity(guard.len());
        for metric in guard.drain(..) {
            if metric.query_id == Some(query_id) {
                selected.push(metric);
            } else {
                retained.push(metric);
            }
        }
        *guard = retained;
        selected
    }
}

struct QueryIdentity {
    query_id: Uuid,
    query_fingerprint: String,
    started_at_ns: i64,
    logical_planning_ns: u64,
}

impl QueryIdentity {
    fn new(sql: &str, logical_planning_ns: u64) -> Self {
        Self {
            query_id: Uuid::new_v4(),
            query_fingerprint: query_fingerprint(sql),
            started_at_ns: chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default(),
            logical_planning_ns,
        }
    }
}

/// A DataFrame whose planning and execution are attributed to one query.
pub struct ReportedDataFrame {
    dataframe: DataFrame,
    identity: QueryIdentity,
    collector: ScanMetricsCollector,
    telemetry: WorkloadTelemetryBuffer,
}

impl ReportedDataFrame {
    pub(crate) fn new(
        dataframe: DataFrame,
        sql: &str,
        logical_planning_ns: u64,
        collector: ScanMetricsCollector,
        telemetry: WorkloadTelemetryBuffer,
    ) -> Self {
        Self {
            dataframe,
            identity: QueryIdentity::new(sql, logical_planning_ns),
            collector,
            telemetry,
        }
    }

    pub fn query_id(&self) -> Uuid {
        self.identity.query_id
    }

    pub fn schema(&self) -> SchemaRef {
        Arc::new(self.dataframe.schema().as_arrow().clone())
    }

    pub fn limit(mut self, skip: usize, fetch: Option<usize>) -> DfResult<Self> {
        self.dataframe = self.dataframe.limit(skip, fetch)?;
        Ok(self)
    }

    pub async fn execute_stream(self) -> DfResult<ReportedQueryStream> {
        let query_id = self.identity.query_id;
        let physical_started = Instant::now();
        let plan = query_scope(query_id, self.dataframe.create_physical_plan()).await?;
        let physical_planning_ns = nanos(physical_started.elapsed());
        let task_ctx = Arc::new(self.dataframe.task_ctx());
        let inner = execute_stream(Arc::clone(&plan), task_ctx)?;
        Ok(ReportedQueryStream {
            schema: inner.schema(),
            inner,
            plan,
            query_id,
            query_fingerprint: self.identity.query_fingerprint,
            started_at_ns: self.identity.started_at_ns,
            planning_ns: self
                .identity
                .logical_planning_ns
                .saturating_add(physical_planning_ns),
            execution_started: Instant::now(),
            output_batches: 0,
            output_rows: 0,
            collector: self.collector,
            telemetry: self.telemetry,
            final_report: None,
        })
    }

    pub async fn collect(self) -> DfResult<(Vec<RecordBatch>, QueryPerformanceReport)> {
        let mut stream = self.execute_stream().await?;
        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            batches.push(batch?);
        }
        let report = stream
            .report()
            .expect("reported stream finalizes when it reaches EOF")
            .clone();
        Ok((batches, report))
    }
}

/// A normal DataFusion record-batch stream plus a finalized performance report.
pub struct ReportedQueryStream {
    schema: SchemaRef,
    inner: SendableRecordBatchStream,
    plan: Arc<dyn ExecutionPlan>,
    query_id: Uuid,
    query_fingerprint: String,
    started_at_ns: i64,
    planning_ns: u64,
    execution_started: Instant,
    output_batches: u64,
    output_rows: u64,
    collector: ScanMetricsCollector,
    telemetry: WorkloadTelemetryBuffer,
    final_report: Option<QueryPerformanceReport>,
}

impl ReportedQueryStream {
    pub fn query_id(&self) -> Uuid {
        self.query_id
    }

    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    /// Available after EOF or an execution error has been observed.
    pub fn report(&self) -> Option<&QueryPerformanceReport> {
        self.final_report.as_ref()
    }

    fn finish(&mut self, status: QueryStatus) {
        if self.final_report.is_some() {
            return;
        }
        let scans = self.collector.take_for(self.query_id);
        let mut operators = Vec::new();
        collect_operator_metrics(&self.plan, &mut operators);
        let bytes_scanned = operators.iter().map(|m| m.bytes_scanned).sum();
        let scan_output_rows = operators
            .iter()
            .filter(|m| m.name == "DataSourceExec")
            .map(|m| m.output_rows)
            .sum();
        let spill_count = operators.iter().map(|m| m.spill_count).sum();
        let spilled_bytes = operators.iter().map(|m| m.spilled_bytes).sum();
        let sort_operators = operators.iter().filter(|m| m.name.contains("Sort")).count();
        let predicate_cache_lookups = scans.iter().map(|m| m.predicate_cache_lookups).sum();
        let predicate_cache_hits = scans.iter().map(|m| m.predicate_cache_hits).sum();
        let predicate_cache_misses = scans.iter().map(|m| m.predicate_cache_misses).sum();
        let predicate_cache_builds = scans.iter().map(|m| m.predicate_cache_builds).sum();
        let predicate_cache_rejected = scans.iter().map(|m| m.predicate_cache_rejected).sum();
        let predicate_cache_row_groups_reused = scans
            .iter()
            .map(|m| m.predicate_cache_row_groups_reused)
            .sum();
        let predicate_cache_evictions = scans.iter().map(|m| m.predicate_cache_evictions).sum();
        let report = QueryPerformanceReport {
            query_id: self.query_id,
            query_fingerprint: self.query_fingerprint.clone(),
            started_at_ns: self.started_at_ns,
            status,
            planning_ns: self.planning_ns,
            execution_ns: nanos(self.execution_started.elapsed()),
            output_batches: self.output_batches,
            output_rows: self.output_rows,
            bytes_scanned,
            scan_output_rows,
            row_groups_pruned: sum_plan_metric_prefix(&self.plan, "row_groups_pruned_"),
            page_index_rows_pruned: sum_plan_metric(&self.plan, "page_index_rows_pruned"),
            pushdown_rows_pruned: sum_plan_metric(&self.plan, "pushdown_rows_pruned"),
            spill_count,
            spilled_bytes,
            sort_operators,
            predicate_cache_lookups,
            predicate_cache_hits,
            predicate_cache_misses,
            predicate_cache_builds,
            predicate_cache_rejected,
            predicate_cache_row_groups_reused,
            predicate_cache_evictions,
            scans,
            operators,
        };
        self.telemetry.record(report.clone());
        self.final_report = Some(report);
    }
}

impl Stream for ReportedQueryStream {
    type Item = DfResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                self.output_batches += 1;
                self.output_rows += batch.num_rows() as u64;
                Poll::Ready(Some(Ok(batch)))
            }
            Poll::Ready(Some(Err(error))) => {
                self.finish(QueryStatus::Failed);
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(None) => {
                self.finish(QueryStatus::Succeeded);
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for ReportedQueryStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Drop for ReportedQueryStream {
    fn drop(&mut self) {
        self.finish(QueryStatus::Cancelled);
    }
}

fn collect_operator_metrics(
    plan: &Arc<dyn ExecutionPlan>,
    output: &mut Vec<OperatorPerformanceMetrics>,
) {
    let metrics = plan.metrics();
    let operator = OperatorPerformanceMetrics {
        name: plan.name().to_string(),
        output_rows: metrics
            .as_ref()
            .and_then(|m| m.output_rows())
            .unwrap_or_default() as u64,
        elapsed_compute_ns: metric_from_set(metrics.as_ref(), "elapsed_compute"),
        bytes_scanned: metric_from_set(metrics.as_ref(), "bytes_scanned"),
        spill_count: metric_from_set(metrics.as_ref(), "spill_count"),
        spilled_bytes: metric_from_set(metrics.as_ref(), "spilled_bytes"),
    };
    if metrics.is_some() || operator.name.contains("Sort") {
        output.push(operator);
    }
    for child in plan.children() {
        collect_operator_metrics(child, output);
    }
}

fn metric_from_set(
    metrics: Option<&datafusion::physical_plan::metrics::MetricsSet>,
    name: &str,
) -> u64 {
    metrics
        .into_iter()
        .flat_map(|set| set.iter())
        .filter(|metric| metric.value().name() == name)
        .map(|metric| metric.value().as_usize() as u64)
        .sum()
}

fn sum_plan_metric(plan: &Arc<dyn ExecutionPlan>, name: &str) -> u64 {
    metric_from_set(plan.metrics().as_ref(), name).saturating_add(
        plan.children()
            .into_iter()
            .map(|child| sum_plan_metric(child, name))
            .sum(),
    )
}

fn sum_plan_metric_prefix(plan: &Arc<dyn ExecutionPlan>, prefix: &str) -> u64 {
    let own = plan
        .metrics()
        .into_iter()
        .flat_map(|set| set.iter().cloned().collect::<Vec<_>>())
        .filter(|metric| metric.value().name().starts_with(prefix))
        .map(|metric| metric.value().as_usize() as u64)
        .sum::<u64>();
    own.saturating_add(
        plan.children()
            .into_iter()
            .map(|child| sum_plan_metric_prefix(child, prefix))
            .sum(),
    )
}

fn nanos(duration: std::time::Duration) -> u64 {
    duration.as_nanos().min(u64::MAX as u128) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scan(query_id: Option<Uuid>, table: &str) -> ScanMetrics {
        ScanMetrics {
            query_id,
            table: table.into(),
            ..Default::default()
        }
    }

    #[test]
    fn take_for_drains_only_the_requested_query_and_retains_the_rest() {
        let collector = ScanMetricsCollector::default();
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        collector.record(scan(Some(a), "t1"));
        collector.record(scan(Some(b), "t2"));
        collector.record(scan(None, "t3")); // plain sql() path, unattributed

        let mine = collector.take_for(a);
        assert_eq!(mine.len(), 1);
        assert_eq!(mine[0].table, "t1");

        // b's and the unattributed record stay for their own consumers.
        let rest = collector.take();
        let tables: Vec<_> = rest.iter().map(|m| m.table.as_str()).collect();
        assert_eq!(tables, vec!["t2", "t3"]);
        assert!(collector.take().is_empty());
    }

    #[test]
    fn records_inside_query_scope_are_attributed_to_that_query() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let collector = ScanMetricsCollector::default();
        let id = Uuid::new_v4();
        runtime.block_on(query_scope(id, async {
            collector.record(scan(None, "scoped"));
        }));
        collector.record(scan(None, "unscoped"));

        let scoped = collector.take_for(id);
        assert_eq!(scoped.len(), 1);
        assert_eq!(scoped[0].query_id, Some(id));
        assert_eq!(scoped[0].table, "scoped");
        let rest = collector.take();
        assert_eq!(rest.len(), 1);
        assert_eq!(rest[0].query_id, None);
    }
}
