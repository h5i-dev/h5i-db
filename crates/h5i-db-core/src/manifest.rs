//! Version manifests: the immutable, per-version description of a table's
//! exact contents. Directly addressed by sequence number, so any version read
//! is O(1) and `as_of` is an O(log V) binary search — no version chain.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{Error, Result};

/// The operation that produced a version (audit trail).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OpKind {
    Create,
    Write,
    Append,
    ReplaceRange,
    DeleteRange,
    Restore,
    Compact,
}

impl std::fmt::Display for OpKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            OpKind::Create => "create",
            OpKind::Write => "write",
            OpKind::Append => "append",
            OpKind::ReplaceRange => "replace_range",
            OpKind::DeleteRange => "delete_range",
            OpKind::Restore => "restore",
            OpKind::Compact => "compact",
        };
        f.write_str(s)
    }
}

/// Per-column statistics recorded in the manifest for pruning.
///
/// Min/max are stored as JSON scalars: integers and floats natively,
/// timestamps as their raw i64 representation (unit defined by the schema),
/// strings as (possibly truncated) UTF-8. `min`/`max` are `None` when the
/// column is all-null in the segment or its type is not stats-eligible.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct ColumnStats {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max: Option<serde_json::Value>,
    pub null_count: u64,
}

/// Immutable metadata for one Parquet segment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentMeta {
    pub id: Uuid,
    /// Object path relative to the database root.
    pub path: String,
    pub rows: u64,
    /// Encoded (on-disk) size in bytes.
    pub bytes: u64,
    /// blake3 of the encoded file bytes; also the dedup content address.
    pub checksum: String,
    /// Min/max of the time column (raw i64 in the schema's unit), if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_range: Option<(i64, i64)>,
    /// True when the segment's rows are sorted by the table sort key.
    pub sorted: bool,
    /// Schema revision the segment was written under.
    pub schema_revision: u32,
    /// Sequence of the version that first introduced this segment.
    pub created_by_sequence: u64,
    /// Per-column min/max/null-count statistics.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub columns: BTreeMap<String, ColumnStats>,
}

impl SegmentMeta {
    /// Whether this segment can contain rows in `[start, end)` (time units of
    /// the table's time column). Segments without a time range never prune.
    pub fn overlaps_time(&self, start: Option<i64>, end: Option<i64>) -> bool {
        match self.time_range {
            None => true,
            Some((seg_min, seg_max)) => {
                let after_start = end.is_none_or(|e| seg_min < e);
                let before_end = start.is_none_or(|s| seg_max >= s);
                after_start && before_end
            }
        }
    }
}

/// One immutable manifest per committed version.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionManifest {
    pub format: u32,
    pub table_id: Uuid,
    pub sequence: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent: Option<u64>,
    /// Checksum of the parent manifest's file bytes — a hash chain covering
    /// the whole history from HEAD backwards.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_checksum: Option<String>,
    /// Strictly monotonic across the chain (see `monotonic_commit_ts`).
    pub committed_at_ns: i64,
    pub op: OpKind,
    /// How the mutation was executed: "direct" (immediate commit) or
    /// "planned" (reviewed plan/apply flow). Audit trail — a bypassed preview
    /// is itself recorded (DESIGN_CLAUDE.md §5).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_mode: Option<String>,
    /// Checksum of the applied `MutationPlan` (planned commits only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    #[serde(default, skip_serializing_if = "serde_json::Map::is_empty")]
    pub user_meta: serde_json::Map<String, serde_json::Value>,
    pub schema_revision: u32,
    pub rows: u64,
    pub bytes: u64,
    /// Global time bounds over all segments (raw i64), if a time column exists
    /// and the table is non-empty.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_range: Option<(i64, i64)>,
    pub segments: Vec<SegmentMeta>,
}

impl VersionManifest {
    /// Recompute the row/byte/time rollups from the segment list.
    pub fn recompute_rollups(&mut self) {
        self.rows = self.segments.iter().map(|s| s.rows).sum();
        self.bytes = self.segments.iter().map(|s| s.bytes).sum();
        self.time_range = self
            .segments
            .iter()
            .filter_map(|s| s.time_range)
            .reduce(|(amin, amax), (bmin, bmax)| (amin.min(bmin), amax.max(bmax)));
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        // Compact (non-pretty) JSON: every commit rewrites the full segment
        // list, so encoding density directly bounds the O(segments)-per-commit
        // manifest cost (2.10). Pretty-print with `jq` when inspecting.
        Ok(serde_json::to_vec(self)?)
    }

    pub fn from_bytes(bytes: &[u8], object: &str) -> Result<Self> {
        let manifest: VersionManifest = serde_json::from_slice(bytes)
            .map_err(|e| Error::corruption(object, format!("manifest parse error: {e}")))?;
        if manifest.format > crate::layout::FORMAT_VERSION {
            return Err(Error::FormatTooNew {
                found: manifest.format,
                supported: crate::layout::FORMAT_VERSION,
            });
        }
        Ok(manifest)
    }

    /// Content-hash → segment lookup used for dedup on write.
    pub fn segments_by_checksum(&self) -> BTreeMap<&str, &SegmentMeta> {
        self.segments
            .iter()
            .map(|s| (s.checksum.as_str(), s))
            .collect()
    }
}

/// The mutable head pointer: current sequence + checksum of that manifest's
/// bytes, so the reader can detect a torn or tampered manifest immediately.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Head {
    pub format: u32,
    pub table_id: Uuid,
    pub sequence: u64,
    pub manifest_checksum: String,
}

impl Head {
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }

    pub fn from_bytes(bytes: &[u8], object: &str) -> Result<Self> {
        let head: Head = serde_json::from_slice(bytes)
            .map_err(|e| Error::corruption(object, format!("HEAD parse error: {e}")))?;
        if head.format > crate::layout::FORMAT_VERSION {
            return Err(Error::FormatTooNew {
                found: head.format,
                supported: crate::layout::FORMAT_VERSION,
            });
        }
        Ok(head)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seg(min: i64, max: i64) -> SegmentMeta {
        SegmentMeta {
            id: Uuid::new_v4(),
            path: "p".into(),
            rows: 10,
            bytes: 100,
            checksum: "c".into(),
            time_range: Some((min, max)),
            sorted: true,
            schema_revision: 1,
            created_by_sequence: 0,
            columns: BTreeMap::new(),
        }
    }

    #[test]
    fn time_overlap_semantics() {
        let s = seg(100, 200);
        // [start, end) intersection with inclusive segment bounds
        assert!(s.overlaps_time(Some(150), Some(160)));
        assert!(s.overlaps_time(Some(200), None)); // max == start → contains start
        assert!(!s.overlaps_time(Some(201), None));
        assert!(s.overlaps_time(None, Some(101))); // end exclusive, min=100 < 101
        assert!(!s.overlaps_time(None, Some(100))); // end exclusive at min
        assert!(s.overlaps_time(None, None));
    }

    #[test]
    fn rollups() {
        let mut m = VersionManifest {
            format: 1,
            table_id: Uuid::new_v4(),
            sequence: 3,
            parent: Some(2),
            parent_checksum: Some("x".into()),
            committed_at_ns: 1,
            op: OpKind::Append,
            execution_mode: None,
            plan_hash: None,
            note: None,
            user_meta: serde_json::Map::new(),
            schema_revision: 1,
            rows: 0,
            bytes: 0,
            time_range: None,
            segments: vec![seg(100, 200), seg(50, 120)],
        };
        m.recompute_rollups();
        assert_eq!(m.rows, 20);
        assert_eq!(m.bytes, 200);
        assert_eq!(m.time_range, Some((50, 200)));
    }
}
