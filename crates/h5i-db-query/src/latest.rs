//! Version-aware "latest row per group" over immutable segments (Part IV A3).
//!
//! Answers `LATEST ON <by>` / "the most recent row per symbol" by precomputing,
//! per immutable segment, that segment's last row per group value and caching it
//! as a checksummed sidecar. A query merges the per-segment contributions in
//! manifest order, so an append-only version reuses every prior segment's cached
//! contribution and scans only the newly added segments — O(segments × groups)
//! instead of O(rows), exactly like the finance aggregate-state store.
//!
//! The cache is a pure accelerator: a miss, corrupt entry, or version mismatch
//! rebuilds from the segment and never changes the answer.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, RecordBatch, StringArray, UInt32Array};
use arrow::datatypes::{DataType, SchemaRef};
use base64::Engine;
use bytes::Bytes;
use datafusion::catalog::{TableFunctionArgs, TableFunctionImpl, TableProvider};
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::scalar::ScalarValue;
use h5i_db_core::{Database, ReadAt, SegmentMeta};
use object_store::path::Path as ObjectPath;
use serde::{Deserialize, Serialize};

use crate::asof::time_column_i64;
use crate::udtf::block_on;

const FORMAT: u32 = 1;
const SEMANTICS_VERSION: u32 = 1;
const PREFIX: &str = "cache/latest/v1";
const MAX_CACHE_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum LatestByMode {
    Disabled,
    ReadOnly,
    #[default]
    ReadWrite,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LatestByMetrics {
    pub segments_total: usize,
    pub states_reused: usize,
    pub states_built: usize,
    pub segments_scanned: usize,
    pub rows_scanned: u64,
    pub corrupt_entries: usize,
    pub evictions: usize,
}

/// One segment's cached "last row per group", as an Arrow-IPC mini-batch.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct LatestEntry {
    format: u32,
    segment_checksum: String,
    schema_revision: u32,
    by_column: String,
    semantics_version: u32,
    source_row_count: u64,
    /// base64 Arrow-IPC stream of the per-group last rows (full table schema).
    ipc_b64: String,
    checksum: String,
}

impl LatestEntry {
    fn sealed(
        segment: &SegmentMeta,
        by_column: &str,
        mini: &RecordBatch,
    ) -> h5i_db_core::Result<Self> {
        let mut entry = Self {
            format: FORMAT,
            segment_checksum: segment.checksum.clone(),
            schema_revision: segment.schema_revision,
            by_column: by_column.to_string(),
            semantics_version: SEMANTICS_VERSION,
            source_row_count: segment.rows,
            ipc_b64: encode_batch_ipc(mini)?,
            checksum: String::new(),
        };
        entry.checksum = h5i_db_core::util::checksum_hex(&serde_json::to_vec(&entry)?);
        Ok(entry)
    }

    /// Verify keys + checksum and decode the mini-batch, or `None` if anything
    /// is off (→ rebuild). Never returns a wrong batch.
    fn verified(&self, segment: &SegmentMeta, by_column: &str, schema: &SchemaRef) -> Option<RecordBatch> {
        if self.format != FORMAT
            || self.segment_checksum != segment.checksum
            || self.schema_revision != segment.schema_revision
            || self.by_column != by_column
            || self.semantics_version != SEMANTICS_VERSION
            || self.source_row_count != segment.rows
        {
            return None;
        }
        let mut unsigned = self.clone();
        let stored = std::mem::take(&mut unsigned.checksum);
        let actual = h5i_db_core::util::checksum_hex(&serde_json::to_vec(&unsigned).ok()?);
        if stored != actual {
            return None;
        }
        decode_batch_ipc(&self.ipc_b64, schema)
    }
}

fn encode_batch_ipc(batch: &RecordBatch) -> h5i_db_core::Result<String> {
    let mut buf = Vec::new();
    {
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &batch.schema())
            .map_err(|e| h5i_db_core::Error::invalid(format!("ipc writer: {e}")))?;
        writer
            .write(batch)
            .map_err(|e| h5i_db_core::Error::invalid(format!("ipc write: {e}")))?;
        writer
            .finish()
            .map_err(|e| h5i_db_core::Error::invalid(format!("ipc finish: {e}")))?;
    }
    Ok(base64::engine::general_purpose::STANDARD.encode(&buf))
}

fn decode_batch_ipc(b64: &str, schema: &SchemaRef) -> Option<RecordBatch> {
    let bytes = base64::engine::general_purpose::STANDARD.decode(b64).ok()?;
    let mut reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None).ok()?;
    let batch = reader.next()?.ok()?;
    // The cached schema must match the current table schema (guarded already by
    // schema_revision, but defend in depth).
    if batch.schema().fields() != schema.fields() {
        return None;
    }
    Some(batch)
}

/// Reduce a batch to one row per (non-null) group value: the row with the
/// greatest timestamp, ties broken toward the later row. Output rows are ordered
/// by group value for determinism. `group` is compared as UTF-8, so it must be a
/// string-family column.
fn last_row_per_group(
    batch: &RecordBatch,
    time_idx: usize,
    group_idx: usize,
    out_schema: &SchemaRef,
) -> DfResult<RecordBatch> {
    if batch.num_rows() == 0 {
        return Ok(RecordBatch::new_empty(out_schema.clone()));
    }
    let times = time_column_i64(batch, time_idx)?;
    let group = arrow::compute::cast(batch.column(group_idx), &DataType::Utf8)?;
    let group = group
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DataFusionError::Execution("latest_on: group column is not string-like".into()))?;

    let mut best: HashMap<&str, (i64, usize)> = HashMap::new();
    for row in 0..batch.num_rows() {
        if group.is_null(row) {
            continue;
        }
        let g = group.value(row);
        let t = times[row];
        match best.get(g) {
            // Strictly-earlier row loses; equal timestamps prefer the later row.
            Some(&(bt, _)) if t < bt => {}
            _ => {
                best.insert(g, (t, row));
            }
        }
    }
    let mut winners: Vec<(&str, usize)> = best.iter().map(|(g, (_, r))| (*g, *r)).collect();
    winners.sort_by(|a, b| a.0.cmp(b.0));
    let indices = UInt32Array::from(winners.iter().map(|(_, r)| *r as u32).collect::<Vec<_>>());

    let columns = batch
        .columns()
        .iter()
        .map(|c| arrow::compute::take(c, &indices, None))
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new(out_schema.clone(), columns).map_err(DataFusionError::from)
}

/// Store + query engine for A3.
#[derive(Clone)]
pub struct LatestByStore {
    db: Arc<Database>,
    mode: LatestByMode,
}

impl LatestByStore {
    pub fn new(db: Arc<Database>, mode: LatestByMode) -> Self {
        Self { db, mode }
    }

    /// Latest row per `by_column` value at `at`. Returns the result batch plus
    /// cache-reuse metrics.
    pub async fn latest_by(
        &self,
        table: &str,
        at: ReadAt,
        by_column: &str,
    ) -> h5i_db_core::Result<(RecordBatch, LatestByMetrics)> {
        let resolved = self.db.resolve(table, at).await?;
        let schema = resolved.schema.clone();
        let time_col = resolved.spec.time_column.clone().ok_or_else(|| {
            h5i_db_core::Error::invalid("latest_on requires a table with a declared time column")
        })?;
        let time_idx = schema
            .index_of(&time_col)
            .map_err(|e| h5i_db_core::Error::invalid(format!("latest_on: {e}")))?;
        let group_idx = schema.index_of(by_column).map_err(|_| {
            h5i_db_core::Error::invalid(format!("latest_on: column {by_column:?} does not exist"))
        })?;
        match schema.field(group_idx).data_type() {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Dictionary(_, _) => {}
            other => {
                return Err(h5i_db_core::Error::invalid(format!(
                    "latest_on: group column {by_column:?} must be string-like, got {other}"
                )))
            }
        }

        let mut metrics = LatestByMetrics {
            segments_total: resolved.manifest.segments.len(),
            ..Default::default()
        };
        let projection: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
        let mut minis: Vec<RecordBatch> = Vec::with_capacity(resolved.manifest.segments.len());

        for segment in &resolved.manifest.segments {
            let path = latest_path(segment, by_column);
            let cached = if self.mode != LatestByMode::Disabled {
                self.load(&path, segment, by_column, &schema, &mut metrics).await
            } else {
                None
            };
            let mini = match cached {
                Some(mini) => {
                    metrics.states_reused += 1;
                    mini
                }
                None => {
                    let mini = self
                        .build(segment, &projection, &schema, time_idx, group_idx)
                        .await?;
                    metrics.states_built += 1;
                    metrics.segments_scanned += 1;
                    metrics.rows_scanned += segment.rows;
                    if self.mode == LatestByMode::ReadWrite {
                        self.try_store(&path, segment, by_column, &mini, &mut metrics)
                            .await;
                    }
                    mini
                }
            };
            minis.push(mini);
        }

        let result = if minis.is_empty() {
            RecordBatch::new_empty(schema.clone())
        } else {
            let combined = arrow::compute::concat_batches(&schema, &minis)
                .map_err(|e| h5i_db_core::Error::invalid(format!("latest_on concat: {e}")))?;
            last_row_per_group(&combined, time_idx, group_idx, &schema)
                .map_err(|e| h5i_db_core::Error::invalid(format!("latest_on reduce: {e}")))?
        };
        Ok((result, metrics))
    }

    async fn load(
        &self,
        path: &ObjectPath,
        segment: &SegmentMeta,
        by_column: &str,
        schema: &SchemaRef,
        metrics: &mut LatestByMetrics,
    ) -> Option<RecordBatch> {
        let bytes = self.db.backend().get_opt(path).await.ok()??;
        if let Ok(entry) = serde_json::from_slice::<LatestEntry>(&bytes) {
            if let Some(batch) = entry.verified(segment, by_column, schema) {
                return Some(batch);
            }
        }
        metrics.corrupt_entries += 1;
        None
    }

    async fn build(
        &self,
        segment: &SegmentMeta,
        projection: &[String],
        schema: &SchemaRef,
        time_idx: usize,
        group_idx: usize,
    ) -> h5i_db_core::Result<RecordBatch> {
        let batches =
            h5i_db_core::segment::read_segment(self.db.backend(), segment, Some(projection), None)
                .await?;
        let combined = arrow::compute::concat_batches(schema, &batches)
            .map_err(|e| h5i_db_core::Error::invalid(format!("latest_on segment concat: {e}")))?;
        last_row_per_group(&combined, time_idx, group_idx, schema)
            .map_err(|e| h5i_db_core::Error::invalid(format!("latest_on build: {e}")))
    }

    async fn try_store(
        &self,
        path: &ObjectPath,
        segment: &SegmentMeta,
        by_column: &str,
        mini: &RecordBatch,
        metrics: &mut LatestByMetrics,
    ) {
        // Best-effort: a read-only backend (or a race) simply leaves the cache
        // un-warmed; correctness never depends on it.
        if let Ok(entry) = LatestEntry::sealed(segment, by_column, mini) {
            if let Ok(bytes) = serde_json::to_vec(&entry) {
                if let Ok(true) = self
                    .db
                    .backend()
                    .put_if_absent(path, Bytes::from(bytes))
                    .await
                {
                    metrics.evictions += crate::sidecar::enforce_budget(
                        self.db.backend(),
                        PREFIX,
                        MAX_CACHE_BYTES,
                    )
                    .await
                    .unwrap_or(0);
                }
            }
        }
    }
}

fn latest_path(segment: &SegmentMeta, by_column: &str) -> ObjectPath {
    let key = format!(
        "{}:{}:{}:{}",
        segment.checksum, segment.schema_revision, by_column, SEMANTICS_VERSION
    );
    let digest = blake3::hash(key.as_bytes()).to_hex().to_string();
    ObjectPath::from(format!("{PREFIX}/{}/{digest}.json", &digest[..2]))
}

/// `latest_on('table', 'by_column')` table function: one row per group, the
/// most recent by the table's time column.
#[derive(Debug)]
pub struct LatestByFunc {
    db: Arc<Database>,
}

impl LatestByFunc {
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }
}

fn string_arg(args: &[datafusion::logical_expr::Expr], i: usize, what: &str) -> DfResult<String> {
    use datafusion::logical_expr::Expr;
    match args.get(i) {
        Some(Expr::Literal(ScalarValue::Utf8(Some(v)), _)) => Ok(v.clone()),
        Some(Expr::Literal(ScalarValue::LargeUtf8(Some(v)), _)) => Ok(v.clone()),
        _ => Err(DataFusionError::Plan(format!(
            "latest_on: argument {i} must be {what} (a string literal)"
        ))),
    }
}

impl TableFunctionImpl for LatestByFunc {
    fn call_with_args(&self, args: TableFunctionArgs) -> DfResult<Arc<dyn TableProvider>> {
        let args = args.exprs();
        if args.len() != 2 {
            return Err(DataFusionError::Plan(
                "latest_on('table', 'by_column') takes exactly two string arguments".into(),
            ));
        }
        let table = string_arg(args, 0, "the table name")?;
        let by_column = string_arg(args, 1, "the group column")?;
        let store = LatestByStore::new(self.db.clone(), LatestByMode::ReadWrite);
        let (batch, _metrics) = block_on(store.latest_by(&table, ReadAt::Latest, &by_column))
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let schema = batch.schema();
        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }
}
