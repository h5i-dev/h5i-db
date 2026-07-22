//! Incremental reads between versions (ROADMAP §4.3).
//!
//! On a pure-append chain, "rows added between v1 and v2" is exactly the
//! data of segments whose `created_by_sequence` lies in `(v1, v2]` — a
//! metadata set-difference, no diffing of row data. That makes incremental
//! consumers (OHLCV maintenance, feature pipelines, tailing) nearly free.
//!
//! The fast path is **gated on the chain's ops**: `write`, `replace_range`,
//! `delete_range`, `restore`, and `compact` rewrite or re-reference
//! segments, so for those the segment set-difference is NOT "added rows".
//! When any non-append version sits in the range, `diff` fails with
//! `Unsupported` and a hint to fall back to a full scan (or narrow the
//! range) — never a silently wrong answer.

use arrow::array::RecordBatch;

use crate::database::{Database, ScanOptions, ScanReport};
use crate::error::{Error, Result};
use crate::manifest::{OpKind, SegmentMeta};

/// Description of what changed between two versions of a table.
#[derive(Debug, Clone)]
pub struct VersionDiff {
    pub table: String,
    pub from_sequence: u64,
    pub to_sequence: u64,
    /// Segments introduced in `(from, to]` — their rows are exactly the
    /// appended rows.
    pub added_segments: Vec<SegmentMeta>,
    pub added_rows: u64,
    pub added_bytes: u64,
}

impl Database {
    /// Metadata-only diff of `(from, to]` on a pure-append chain.
    pub async fn diff(&self, name: &str, from: u64, to: u64) -> Result<VersionDiff> {
        if from > to {
            return Err(Error::invalid(format!(
                "diff range is reversed: from {from} > to {to}"
            )));
        }
        let resolved = self
            .resolve(name, crate::database::ReadAt::Version(to))
            .await?;
        let table_id = resolved.entry.table_id;

        // Gate: every version in (from, to] must be an append (the manifest
        // records `op` per version precisely for this).
        for seq in (from + 1)..=to {
            let m = self.manifest_at(table_id, seq).await?;
            if !matches!(m.op, OpKind::Append | OpKind::EvolveSchema) {
                return Err(Error::Unsupported {
                    detail: format!(
                        "incremental diff requires a pure-append chain, but version {seq} \
                         is `{}`; fall back to a full scan of version {to}, or diff a \
                         range that excludes it",
                        m.op
                    ),
                });
            }
        }

        let added_segments: Vec<SegmentMeta> = resolved
            .manifest
            .segments
            .iter()
            .filter(|s| s.created_by_sequence > from && s.created_by_sequence <= to)
            .cloned()
            .collect();
        Ok(VersionDiff {
            table: name.to_string(),
            from_sequence: from,
            to_sequence: to,
            added_rows: added_segments.iter().map(|s| s.rows).sum(),
            added_bytes: added_segments.iter().map(|s| s.bytes).sum(),
            added_segments,
        })
    }

    /// Scan only the rows appended in `(from, to]` (pure-append chains).
    /// Projection / time bounds / limit from `options` apply as in `scan`.
    pub async fn diff_scan(
        &self,
        name: &str,
        from: u64,
        to: u64,
        options: ScanOptions,
    ) -> Result<(Vec<RecordBatch>, ScanReport)> {
        let diff = self.diff(name, from, to).await?;
        let resolved = self
            .resolve(name, crate::database::ReadAt::Version(to))
            .await?;
        // Restrict the resolved view to the added segments, then reuse the
        // normal scan machinery (pruning, projection, limit, verification).
        let mut narrowed = resolved.clone();
        narrowed.manifest.segments = diff.added_segments;
        self.scan_resolved(&narrowed, options).await
    }
}
