//! Previewable mutations: plan / apply separation.
//!
//! `plan_*` runs the full write path — including uploading the new segments —
//! but stops short of the HEAD swap. The result is a `MutationPlan`: an
//! inspectable, persisted description of exactly what `apply` will publish
//! (affected rows, reused vs rewritten segments, byte estimates, before/after
//! row samples). `apply` is then a metadata-only CAS commit that fails with
//! `VersionConflict` if the table head moved after planning; `discard` (or
//! plan expiry + vacuum) cleans up.
//!
//! Plans are stored inside the database (`tables/<uuid>/plans/<id>.json`) so
//! vacuum can see them: segments referenced by a live, unexpired plan are
//! protected from collection.

use arrow::array::RecordBatch;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::database::{CommitResult, Database, ReadAt, ScanOptions, WriteOptions};
use crate::error::{Error, Result};
use crate::manifest::{OpKind, SegmentMeta};
use crate::segment::{sort_batches, SegmentWriter};
use crate::util;

/// Default plan time-to-live. An unapplied plan older than this is expired:
/// `apply` refuses it and vacuum stops protecting its segments.
pub const PLAN_TTL_SECONDS: u64 = 7 * 24 * 3600;

/// How many rows to keep in each of the before/after samples.
const SAMPLE_ROWS: usize = 8;

fn plans_prefix(table_id: Uuid) -> object_store::path::Path {
    object_store::path::Path::from(format!("tables/{table_id}/plans"))
}

fn plan_path(table_id: Uuid, plan_id: Uuid) -> object_store::path::Path {
    object_store::path::Path::from(format!("tables/{table_id}/plans/{plan_id}.json"))
}

/// Human/agent-facing summary of what applying the plan will do.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanSummary {
    pub rows_before: u64,
    pub rows_after: u64,
    /// Rows removed or replaced (exact — planning reads the boundary data).
    pub rows_affected: u64,
    /// Segments carried over unchanged from the base version.
    pub segments_reused: usize,
    /// Newly written segments this plan would publish.
    pub segments_added: usize,
    /// Encoded bytes of the newly written segments.
    pub added_bytes: u64,
    /// Time range touched by the mutation (raw units), when applicable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub affected_time_range: Option<(i64, i64)>,
}

/// A prepared, previewable, not-yet-published mutation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MutationPlan {
    pub plan_id: Uuid,
    pub created_at_ns: i64,
    pub expires_at_ns: i64,
    pub table: String,
    pub table_id: Uuid,
    /// The head sequence this plan was computed against. `apply` requires the
    /// head to still be exactly this version.
    pub base_version: u64,
    pub base_manifest_checksum: String,
    pub op: OpKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    #[serde(default, skip_serializing_if = "serde_json::Map::is_empty")]
    pub user_meta: serde_json::Map<String, serde_json::Value>,
    pub schema_revision: u32,
    /// The complete segment list the applied manifest will carry.
    pub segments: Vec<SegmentMeta>,
    pub summary: PlanSummary,
    /// Up to a few affected rows as they exist in the base version
    /// (Arrow IPC, base64). Empty for pure appends.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub before_sample_ipc_b64: Option<String>,
    /// Up to a few rows as they will exist after apply.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after_sample_ipc_b64: Option<String>,
    #[serde(default)]
    pub checksum: String,
}

impl MutationPlan {
    fn compute_checksum(&self) -> Result<String> {
        let mut clone = self.clone();
        clone.checksum = String::new();
        Ok(util::checksum_hex(&serde_json::to_vec(&clone)?))
    }

    pub fn seal(mut self) -> Result<Self> {
        self.checksum = self.compute_checksum()?;
        Ok(self)
    }

    pub fn verify(&self, object: &str) -> Result<()> {
        if self.checksum != self.compute_checksum()? {
            return Err(Error::corruption(object, "plan checksum mismatch"));
        }
        Ok(())
    }

    pub fn is_expired(&self) -> bool {
        util::monotonic_commit_ts(None) > self.expires_at_ns
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec_pretty(self)?)
    }

    pub fn from_bytes(bytes: &[u8], object: &str) -> Result<Self> {
        let plan: MutationPlan = serde_json::from_slice(bytes)
            .map_err(|e| Error::corruption(object, format!("plan parse: {e}")))?;
        plan.verify(object)?;
        Ok(plan)
    }

    /// Decode a sample column back into record batches (for display).
    pub fn decode_sample(b64: &str) -> Result<Vec<RecordBatch>> {
        use base64::Engine;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(b64)
            .map_err(|e| Error::corruption("plan sample", format!("bad base64: {e}")))?;
        let reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
            .map_err(Error::Arrow)?;
        let mut out = Vec::new();
        for b in reader {
            out.push(b.map_err(Error::Arrow)?);
        }
        Ok(out)
    }
}

fn encode_sample(batches: &[RecordBatch], limit: usize) -> Result<Option<String>> {
    use base64::Engine;
    let mut taken: Vec<RecordBatch> = Vec::new();
    let mut rows = 0;
    for b in batches {
        if rows >= limit || b.num_rows() == 0 {
            break;
        }
        let take = (limit - rows).min(b.num_rows());
        taken.push(b.slice(0, take));
        rows += take;
    }
    if taken.is_empty() {
        return Ok(None);
    }
    let mut buf = Vec::new();
    {
        let mut w = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &taken[0].schema())
            .map_err(Error::Arrow)?;
        for b in &taken {
            w.write(b).map_err(Error::Arrow)?;
        }
        w.finish().map_err(Error::Arrow)?;
    }
    Ok(Some(base64::engine::general_purpose::STANDARD.encode(&buf)))
}

impl Database {
    /// Plan a `replace_range` (or with empty `new_batches`, a `delete_range`)
    /// without publishing it. Segments are written; HEAD is untouched.
    pub async fn plan_replace_range(
        &self,
        name: &str,
        start: i64,
        end: i64,
        new_batches: Vec<RecordBatch>,
        opts: WriteOptions,
    ) -> Result<MutationPlan> {
        if start >= end {
            return Err(Error::invalid(format!(
                "empty range: start {start} must be < end {end}"
            )));
        }
        let op = if new_batches.is_empty() {
            OpKind::DeleteRange
        } else {
            OpKind::ReplaceRange
        };
        let resolved = self.resolve(name, ReadAt::Latest).await?;
        let spec = &resolved.spec;
        let tc = spec.time_column.clone().ok_or_else(|| Error::Unsupported {
            detail: format!("{op} requires a table with a time column"),
        })?;
        let schema = resolved.schema.clone();
        crate::database::validate_batches_schema(&schema, &new_batches)?;
        for b in &new_batches {
            if b.num_rows() == 0 {
                continue;
            }
            for v in crate::segment::time_values_i64(b, &tc)? {
                if v < start || v >= end {
                    return Err(Error::invalid(format!(
                        "replacement row at time {v} falls outside [{start}, {end})"
                    )));
                }
            }
        }

        // Before-sample: first rows currently inside the range.
        let (before_batches, _) = self
            .scan_resolved(
                &resolved,
                ScanOptions {
                    time_start: Some(start),
                    time_end: Some(end),
                    limit: Some(SAMPLE_ROWS),
                    ..Default::default()
                },
            )
            .await?;

        // Exact affected count: rows inside the range at base.
        let (all_in_range, _) = self
            .scan_resolved(
                &resolved,
                ScanOptions {
                    projection: Some(vec![tc.clone()]),
                    time_start: Some(start),
                    time_end: Some(end),
                    ..Default::default()
                },
            )
            .await?;
        let rows_affected: u64 = all_in_range.iter().map(|b| b.num_rows() as u64).sum();

        // Build the new segment set exactly like replace_range does.
        let next_seq = resolved.head_sequence + 1;
        let mut kept: Vec<SegmentMeta> = Vec::new();
        let mut boundary: Vec<SegmentMeta> = Vec::new();
        for seg in &resolved.manifest.segments {
            if seg.overlaps_time(Some(start), Some(end)) {
                boundary.push(seg.clone());
            } else {
                kept.push(seg.clone());
            }
        }
        let mut writer = SegmentWriter::new(self.backend(), spec, schema.clone(), next_seq);
        for seg in &boundary {
            let batches = crate::segment::read_segment(self.backend(), seg, None, None).await?;
            for b in
                crate::segment::filter_batches_by_time(batches.clone(), &tc, None, Some(start))?
            {
                writer.push(b).await?;
            }
            for b in crate::segment::filter_batches_by_time(batches, &tc, Some(end), None)? {
                writer.push(b).await?;
            }
        }
        let after_sample_src = if new_batches.is_empty() {
            vec![]
        } else {
            vec![sort_batches(&schema, &new_batches, &spec.sort_key)?]
        };
        for b in &after_sample_src {
            writer.push(b.clone()).await?;
        }
        let (mut rewritten, _, lease) = writer.finish().await?;
        let deduped =
            crate::database::dedup_segments(self.backend(), &mut rewritten, &resolved.manifest)
                .await;

        let mut segments = kept.clone();
        segments.extend(rewritten.clone());
        let rows_after: u64 = segments.iter().map(|s| s.rows).sum();
        let added_bytes: u64 = rewritten
            .iter()
            .filter(|s| s.created_by_sequence == next_seq)
            .map(|s| s.bytes)
            .sum();

        let now = util::monotonic_commit_ts(None);
        let plan = MutationPlan {
            plan_id: Uuid::new_v4(),
            created_at_ns: now,
            expires_at_ns: now + (PLAN_TTL_SECONDS as i64) * 1_000_000_000,
            table: name.to_string(),
            table_id: resolved.entry.table_id,
            base_version: resolved.head_sequence,
            base_manifest_checksum: {
                // resolve() verified it against HEAD already.
                let head = self
                    .backend()
                    .heads
                    .read(resolved.entry.table_id)
                    .await?
                    .ok_or_else(|| Error::internal("HEAD vanished during planning"))?;
                head.head.manifest_checksum
            },
            op,
            note: opts.note,
            user_meta: opts.user_meta,
            schema_revision: resolved.manifest.schema_revision,
            segments,
            summary: PlanSummary {
                rows_before: resolved.manifest.rows,
                rows_after,
                rows_affected,
                segments_reused: kept.len() + deduped,
                segments_added: rewritten.len() - deduped,
                added_bytes,
                affected_time_range: Some((start, end)),
            },
            before_sample_ipc_b64: encode_sample(&before_batches, SAMPLE_ROWS)?,
            after_sample_ipc_b64: encode_sample(&after_sample_src, SAMPLE_ROWS)?,
            checksum: String::new(),
        }
        .seal()?;

        self.store_plan(&plan).await?;
        // The stored plan now protects the staged segments; the staging
        // lease is redundant.
        if let Some(lp) = lease {
            let _ = self.backend().delete(&lp).await;
        }
        Ok(plan)
    }

    /// Plan a full-table `write` without publishing it.
    pub async fn plan_write(
        &self,
        name: &str,
        batches: Vec<RecordBatch>,
        opts: WriteOptions,
    ) -> Result<MutationPlan> {
        let resolved = self.resolve(name, ReadAt::Latest).await?;
        let spec = &resolved.spec;
        let schema = resolved.schema.clone();
        crate::database::validate_batches_schema(&schema, &batches)?;

        let (before_batches, _) = self
            .scan_resolved(
                &resolved,
                ScanOptions {
                    limit: Some(SAMPLE_ROWS),
                    ..Default::default()
                },
            )
            .await?;

        let next_seq = resolved.head_sequence + 1;
        let mut writer = SegmentWriter::new(self.backend(), spec, schema.clone(), next_seq);
        // First rows of the post-write table, captured while streaming.
        let mut after_head: Option<RecordBatch> = None;
        if spec.sort_key.is_empty() {
            for b in &batches {
                if after_head.is_none() && b.num_rows() > 0 {
                    after_head = Some(b.slice(0, b.num_rows().min(SAMPLE_ROWS)));
                }
                writer.push(b.clone()).await?;
            }
        } else {
            // Chunked sort + k-way merge, mirroring Database::write (2.4).
            let sorted = crate::segment::sort_each_batch(&batches, &spec.sort_key)?;
            drop(batches);
            let mut merger = crate::segment::SortedBatchMerger::new(
                sorted,
                &spec.sort_key,
                crate::segment::MERGE_CHUNK_ROWS,
            )?;
            while let Some(chunk) = merger.next_chunk()? {
                if after_head.is_none() && chunk.num_rows() > 0 {
                    after_head = Some(chunk.slice(0, chunk.num_rows().min(SAMPLE_ROWS)));
                }
                writer.push(chunk).await?;
            }
        }
        let after_sample_src: Vec<RecordBatch> = after_head.into_iter().collect();
        let (mut segments, _, lease) = writer.finish().await?;
        let deduped =
            crate::database::dedup_segments(self.backend(), &mut segments, &resolved.manifest)
                .await;
        let rows_after: u64 = segments.iter().map(|s| s.rows).sum();
        let added_bytes: u64 = segments
            .iter()
            .filter(|s| s.created_by_sequence == next_seq)
            .map(|s| s.bytes)
            .sum();

        let now = util::monotonic_commit_ts(None);
        let head = self
            .backend()
            .heads
            .read(resolved.entry.table_id)
            .await?
            .ok_or_else(|| Error::internal("HEAD vanished during planning"))?;
        let plan = MutationPlan {
            plan_id: Uuid::new_v4(),
            created_at_ns: now,
            expires_at_ns: now + (PLAN_TTL_SECONDS as i64) * 1_000_000_000,
            table: name.to_string(),
            table_id: resolved.entry.table_id,
            base_version: resolved.head_sequence,
            base_manifest_checksum: head.head.manifest_checksum,
            op: OpKind::Write,
            note: opts.note,
            user_meta: opts.user_meta,
            schema_revision: resolved.manifest.schema_revision,
            segments: segments.clone(),
            summary: PlanSummary {
                rows_before: resolved.manifest.rows,
                rows_after,
                rows_affected: resolved.manifest.rows.max(rows_after),
                segments_reused: deduped,
                segments_added: segments.len() - deduped,
                added_bytes,
                affected_time_range: resolved.manifest.time_range,
            },
            before_sample_ipc_b64: encode_sample(&before_batches, SAMPLE_ROWS)?,
            after_sample_ipc_b64: encode_sample(&after_sample_src, SAMPLE_ROWS)?,
            checksum: String::new(),
        }
        .seal()?;
        self.store_plan(&plan).await?;
        // The stored plan now protects the staged segments; the staging
        // lease is redundant.
        if let Some(lp) = lease {
            let _ = self.backend().delete(&lp).await;
        }
        Ok(plan)
    }

    async fn store_plan(&self, plan: &MutationPlan) -> Result<()> {
        let path = plan_path(plan.table_id, plan.plan_id);
        self.backend().put(&path, plan.to_bytes()?.into()).await?;
        self.backend().sync_objects(&[path]).await
    }

    /// Load a plan by table and id.
    pub async fn load_plan(&self, table: &str, plan_id: Uuid) -> Result<MutationPlan> {
        let resolved_entry = catalog_entry(self, table).await?;
        let path = plan_path(resolved_entry.table_id, plan_id);
        let bytes = self.backend().get_opt(&path).await?.ok_or_else(|| {
            Error::invalid(format!("plan {plan_id} not found for table {table:?}"))
        })?;
        MutationPlan::from_bytes(&bytes, path.as_ref())
    }

    /// List stored plans for a table.
    pub async fn list_plans(&self, table: &str) -> Result<Vec<MutationPlan>> {
        let entry = catalog_entry(self, table).await?;
        let metas = self.backend().list(&plans_prefix(entry.table_id)).await?;
        let mut out = Vec::new();
        for m in metas {
            let bytes = self.backend().get(&m.location).await?;
            out.push(MutationPlan::from_bytes(&bytes, m.location.as_ref())?);
        }
        out.sort_by_key(|p| p.created_at_ns);
        Ok(out)
    }

    /// Publish a previously planned mutation. Fails with `VersionConflict`
    /// if the table head is no longer the plan's base version, and refuses
    /// expired or tampered plans.
    pub async fn apply_plan(&self, plan: &MutationPlan) -> Result<CommitResult> {
        if self.is_read_only() {
            return Err(Error::ReadOnly {
                op: "apply_plan".into(),
            });
        }
        plan.verify("plan")?;
        if plan.is_expired() {
            return Err(Error::invalid(format!(
                "plan {} expired at {}; re-plan the mutation",
                plan.plan_id, plan.expires_at_ns
            )));
        }
        // Validate every referenced segment still exists (vacuum with a
        // too-short grace, or a discarded sibling plan, could have removed
        // one — fail closed before touching HEAD).
        for seg in &plan.segments {
            let p = object_store::path::Path::from(seg.path.as_str());
            use object_store::ObjectStoreExt;
            match self.backend().store.head(&p).await {
                Ok(meta) if meta.size == seg.bytes => {}
                Ok(meta) => {
                    return Err(Error::corruption(
                        &seg.path,
                        format!(
                            "size changed since planning ({} != {})",
                            meta.size, seg.bytes
                        ),
                    ))
                }
                Err(object_store::Error::NotFound { .. }) => {
                    return Err(Error::invalid(format!(
                        "plan segment {} no longer exists (vacuumed?); re-plan",
                        seg.path
                    )))
                }
                Err(e) => return Err(Error::ObjectStore(e)),
            }
        }

        let result = self
            .commit_planned(
                &plan.table,
                plan.table_id,
                plan.base_version,
                &plan.base_manifest_checksum,
                plan,
            )
            .await?;

        // Best-effort cleanup of the consumed plan.
        let _ = self
            .backend()
            .delete(&plan_path(plan.table_id, plan.plan_id))
            .await;
        Ok(result)
    }

    /// Drop a plan; its unpublished segments become vacuumable orphans.
    pub async fn discard_plan(&self, table: &str, plan_id: Uuid) -> Result<()> {
        let entry = catalog_entry(self, table).await?;
        self.backend()
            .delete(&plan_path(entry.table_id, plan_id))
            .await
    }

    /// Segment paths protected by live plans (used by vacuum).
    ///
    /// Fails *closed* (3.4): a storage error or an unreadable plan aborts the
    /// caller (vacuum) instead of silently unprotecting staged segments.
    pub(crate) async fn plan_protected_paths(
        &self,
        table_id: Uuid,
    ) -> Result<std::collections::BTreeSet<String>> {
        let mut protected = std::collections::BTreeSet::new();
        for m in self.backend().list(&plans_prefix(table_id)).await? {
            let bytes = self.backend().get(&m.location).await?;
            let plan = MutationPlan::from_bytes(&bytes, m.location.as_ref())?;
            if !plan.is_expired() {
                for seg in &plan.segments {
                    protected.insert(seg.path.clone());
                }
            }
        }
        Ok(protected)
    }
}

async fn catalog_entry(db: &Database, table: &str) -> Result<crate::catalog::CatalogEntry> {
    crate::catalog::load_entry(db.backend(), table)
        .await?
        .ok_or_else(|| Error::TableNotFound { name: table.into() })
}
