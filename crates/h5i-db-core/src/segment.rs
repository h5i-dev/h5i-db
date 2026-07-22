//! Parquet segment writer/reader plus per-column statistics collection.
//!
//! Segments are immutable: written once under a fresh UUID path, referenced
//! by manifests, and only ever deleted by vacuum when unreachable.

use std::collections::{BTreeMap, BTreeSet};

use arrow::array::{Array, ArrayRef, RecordBatch};
use arrow::compute::SortColumn;
use arrow::datatypes::{DataType, SchemaRef};
use bytes::Bytes;
use futures::TryStreamExt;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::arrow::{ArrowWriter, ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use uuid::Uuid;

use crate::error::{Error, Result};
use crate::manifest::{ColumnStats, SegmentMeta};
use crate::spec::{Codec, TableSpec};
use crate::Backend;

/// Truncate string statistics to keep manifests small; a truncated max is
/// still a valid upper bound only if we extend it, so we mark truncated
/// values conservatively (drop the stat rather than store a wrong bound).
const MAX_STRING_STAT_LEN: usize = 64;
const MAX_DISTINCT_VALUES: usize = 128;

// ---------------------------------------------------------------------------
// statistics accumulation
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum ScalarStat {
    Int(i64),
    UInt(u64),
    Float(f64),
    Str(String),
    Bool(bool),
}

impl ScalarStat {
    fn to_json(&self) -> serde_json::Value {
        match self {
            ScalarStat::Int(v) => serde_json::json!(v),
            ScalarStat::UInt(v) => serde_json::json!(v),
            ScalarStat::Float(v) => serde_json::json!(v),
            ScalarStat::Str(v) => serde_json::json!(v),
            ScalarStat::Bool(v) => serde_json::json!(v),
        }
    }

    fn less_than(&self, other: &ScalarStat) -> bool {
        match (self, other) {
            (ScalarStat::Int(a), ScalarStat::Int(b)) => a < b,
            (ScalarStat::UInt(a), ScalarStat::UInt(b)) => a < b,
            (ScalarStat::Float(a), ScalarStat::Float(b)) => a < b,
            (ScalarStat::Str(a), ScalarStat::Str(b)) => a < b,
            (ScalarStat::Bool(a), ScalarStat::Bool(b)) => !a & b,
            _ => false,
        }
    }
}

#[derive(Debug, Default)]
struct ColumnAcc {
    min: Option<ScalarStat>,
    max: Option<ScalarStat>,
    null_count: u64,
    /// Set when the column type is not stats-eligible or a string stat blew
    /// the length budget — min/max are then omitted from the manifest.
    stats_dropped: bool,
    distinct: BTreeSet<String>,
    distinct_eligible: bool,
    distinct_dropped: bool,
}

impl ColumnAcc {
    fn observe(&mut self, min: Option<ScalarStat>, max: Option<ScalarStat>, nulls: u64) {
        self.null_count += nulls;
        if self.stats_dropped {
            return;
        }
        if let Some(m) = min {
            if self.min.as_ref().is_none_or(|cur| m.less_than(cur)) {
                self.min = Some(m);
            }
        }
        if let Some(m) = max {
            if self.max.as_ref().is_none_or(|cur| cur.less_than(&m)) {
                self.max = Some(m);
            }
        }
    }

    fn drop_stats(&mut self, nulls: u64) {
        self.null_count += nulls;
        self.stats_dropped = true;
        self.min = None;
        self.max = None;
    }

    fn observe_distinct(&mut self, array: &ArrayRef) {
        if self.distinct_dropped {
            return;
        }
        match array.data_type() {
            DataType::Utf8 => {
                let a = array
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .unwrap();
                self.distinct_eligible = true;
                for value in a.iter().flatten() {
                    self.insert_distinct(value);
                    if self.distinct_dropped {
                        return;
                    }
                }
            }
            DataType::LargeUtf8 => {
                let a = array
                    .as_any()
                    .downcast_ref::<arrow::array::LargeStringArray>()
                    .unwrap();
                self.distinct_eligible = true;
                for value in a.iter().flatten() {
                    self.insert_distinct(value);
                    if self.distinct_dropped {
                        return;
                    }
                }
            }
            DataType::Dictionary(_, value) if **value == DataType::Utf8 => {
                let Ok(materialized) = arrow::compute::cast(array, &DataType::Utf8) else {
                    self.distinct_dropped = true;
                    return;
                };
                let a = materialized
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .unwrap();
                self.distinct_eligible = true;
                for value in a.iter().flatten() {
                    self.insert_distinct(value);
                    if self.distinct_dropped {
                        return;
                    }
                }
            }
            _ => {}
        }
    }

    fn insert_distinct(&mut self, value: &str) {
        self.distinct.insert(value.to_string());
        if self.distinct.len() > MAX_DISTINCT_VALUES {
            self.distinct.clear();
            self.distinct_dropped = true;
        }
    }

    fn finish(self) -> ColumnStats {
        ColumnStats {
            min: self.min.map(|s| s.to_json()),
            max: self.max.map(|s| s.to_json()),
            null_count: self.null_count,
            distinct_values: (self.distinct_eligible && !self.distinct_dropped).then(|| {
                self.distinct
                    .into_iter()
                    .map(serde_json::Value::String)
                    .collect()
            }),
        }
    }
}

macro_rules! minmax_primitive {
    ($array:expr, $ty:ty, $variant:ident, $cast:ty) => {{
        let arr = $array.as_any().downcast_ref::<$ty>().unwrap();
        let min = arrow::compute::min(arr).map(|v| ScalarStat::$variant(v as $cast));
        let max = arrow::compute::max(arr).map(|v| ScalarStat::$variant(v as $cast));
        (min, max)
    }};
}

/// Extract (min, max) for one array, or `None` if the type is not
/// stats-eligible.
fn array_minmax(array: &ArrayRef) -> Option<(Option<ScalarStat>, Option<ScalarStat>)> {
    use arrow::array::*;
    use arrow::datatypes::TimeUnit;
    let (min, max) = match array.data_type() {
        DataType::Int8 => minmax_primitive!(array, Int8Array, Int, i64),
        DataType::Int16 => minmax_primitive!(array, Int16Array, Int, i64),
        DataType::Int32 => minmax_primitive!(array, Int32Array, Int, i64),
        DataType::Int64 => minmax_primitive!(array, Int64Array, Int, i64),
        DataType::UInt8 => minmax_primitive!(array, UInt8Array, UInt, u64),
        DataType::UInt16 => minmax_primitive!(array, UInt16Array, UInt, u64),
        DataType::UInt32 => minmax_primitive!(array, UInt32Array, UInt, u64),
        DataType::UInt64 => minmax_primitive!(array, UInt64Array, UInt, u64),
        DataType::Float32 => minmax_primitive!(array, Float32Array, Float, f64),
        DataType::Float64 => minmax_primitive!(array, Float64Array, Float, f64),
        DataType::Date32 => minmax_primitive!(array, Date32Array, Int, i64),
        DataType::Date64 => minmax_primitive!(array, Date64Array, Int, i64),
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            (
                arrow::compute::min_boolean(arr).map(ScalarStat::Bool),
                arrow::compute::max_boolean(arr).map(ScalarStat::Bool),
            )
        }
        DataType::Timestamp(unit, _) => {
            // Stored as raw i64 in the schema's unit.
            match unit {
                TimeUnit::Second => {
                    minmax_primitive!(array, TimestampSecondArray, Int, i64)
                }
                TimeUnit::Millisecond => {
                    minmax_primitive!(array, TimestampMillisecondArray, Int, i64)
                }
                TimeUnit::Microsecond => {
                    minmax_primitive!(array, TimestampMicrosecondArray, Int, i64)
                }
                TimeUnit::Nanosecond => {
                    minmax_primitive!(array, TimestampNanosecondArray, Int, i64)
                }
            }
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            let min = arrow::compute::min_string(arr).map(|s| ScalarStat::Str(s.to_string()));
            let max = arrow::compute::max_string(arr).map(|s| ScalarStat::Str(s.to_string()));
            (min, max)
        }
        DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            let min = arrow::compute::min_string(arr).map(|s| ScalarStat::Str(s.to_string()));
            let max = arrow::compute::max_string(arr).map(|s| ScalarStat::Str(s.to_string()));
            (min, max)
        }
        DataType::Dictionary(_, value) if **value == DataType::Utf8 => {
            // Compare over the materialized values; cheap for low cardinality.
            let arr = arrow::compute::cast(array, &DataType::Utf8).ok()?;
            let arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
            let min = arrow::compute::min_string(arr).map(|s| ScalarStat::Str(s.to_string()));
            let max = arrow::compute::max_string(arr).map(|s| ScalarStat::Str(s.to_string()));
            (min, max)
        }
        _ => return None,
    };
    // Guard string stats against unbounded manifest growth.
    let guard = |s: Option<ScalarStat>| match s {
        Some(ScalarStat::Str(v)) if v.len() > MAX_STRING_STAT_LEN => None,
        other => other,
    };
    Some((guard(min), guard(max)))
}

// ---------------------------------------------------------------------------
// time-column helpers
// ---------------------------------------------------------------------------

/// Borrow the raw i64 values of an i64-backed time column (Int64, Date64, or
/// any Timestamp unit) without casting or copying.
fn time_slice_i64(col: &ArrayRef) -> Option<&[i64]> {
    use arrow::array::*;
    use arrow::datatypes::TimeUnit;
    let any = col.as_any();
    let values: &[i64] = match col.data_type() {
        DataType::Int64 => &any.downcast_ref::<Int64Array>()?.values()[..],
        DataType::Date64 => &any.downcast_ref::<Date64Array>()?.values()[..],
        DataType::Timestamp(TimeUnit::Second, _) => {
            &any.downcast_ref::<TimestampSecondArray>()?.values()[..]
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            &any.downcast_ref::<TimestampMillisecondArray>()?.values()[..]
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            &any.downcast_ref::<TimestampMicrosecondArray>()?.values()[..]
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            &any.downcast_ref::<TimestampNanosecondArray>()?.values()[..]
        }
        _ => return None,
    };
    Some(values)
}

/// Read the time column of a batch as raw i64 values (unit per schema).
pub fn time_values_i64(batch: &RecordBatch, time_col: &str) -> Result<Vec<i64>> {
    let idx = batch.schema().index_of(time_col).map_err(Error::Arrow)?;
    let col = batch.column(idx);
    if col.null_count() > 0 {
        return Err(Error::invalid(format!(
            "time column {time_col:?} contains nulls"
        )));
    }
    // Fast path: i64-backed types are reinterpreted with a single copy.
    if let Some(values) = time_slice_i64(col) {
        return Ok(values.to_vec());
    }
    let casted = arrow::compute::cast(col, &DataType::Int64).map_err(Error::Arrow)?;
    let arr = casted
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .ok_or_else(|| Error::internal("time column cast to i64 failed"))?;
    Ok(arr.values().to_vec())
}

/// Min/max of a batch's time column without materializing a copy.
pub fn time_min_max(batch: &RecordBatch, time_col: &str) -> Result<Option<(i64, i64)>> {
    if batch.num_rows() == 0 {
        return Ok(None);
    }
    let idx = batch.schema().index_of(time_col).map_err(Error::Arrow)?;
    let col = batch.column(idx);
    if col.null_count() > 0 {
        return Err(Error::invalid(format!(
            "time column {time_col:?} contains nulls"
        )));
    }
    if let Some(values) = time_slice_i64(col) {
        let mut mn = i64::MAX;
        let mut mx = i64::MIN;
        for &v in values {
            mn = mn.min(v);
            mx = mx.max(v);
        }
        return Ok(Some((mn, mx)));
    }
    let casted = arrow::compute::cast(col, &DataType::Int64).map_err(Error::Arrow)?;
    let arr = casted
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .ok_or_else(|| Error::internal("time column cast to i64 failed"))?;
    match (arrow::compute::min(arr), arrow::compute::max(arr)) {
        (Some(mn), Some(mx)) => Ok(Some((mn, mx))),
        _ => Ok(None),
    }
}

/// Check that `batch` is sorted ascending by the sort key columns.
///
/// Pairwise O(n·k) comparator walk (short-circuits on the first violation) —
/// deliberately not a lexsort, which would cost O(n log n) and allocate the
/// full index vector on the append hot path.
pub fn batch_is_sorted(batch: &RecordBatch, sort_key: &[String]) -> Result<bool> {
    if sort_key.is_empty() || batch.num_rows() < 2 {
        return Ok(true);
    }
    let comparators: Vec<_> = sort_key
        .iter()
        .map(|name| {
            let idx = batch.schema().index_of(name).map_err(Error::Arrow)?;
            let col = batch.column(idx);
            arrow::array::make_comparator(
                col.as_ref(),
                col.as_ref(),
                arrow::compute::SortOptions::default(),
            )
            .map_err(Error::Arrow)
        })
        .collect::<Result<_>>()?;
    for i in 1..batch.num_rows() {
        for cmp in &comparators {
            match cmp(i - 1, i) {
                std::cmp::Ordering::Less => break,
                std::cmp::Ordering::Equal => continue,
                std::cmp::Ordering::Greater => return Ok(false),
            }
        }
    }
    Ok(true)
}

/// Sort a set of batches by the sort key, returning a single concatenated,
/// sorted batch. Used by `write` (which accepts unsorted input) and compact.
pub fn sort_batches(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    sort_key: &[String],
) -> Result<RecordBatch> {
    let combined = arrow::compute::concat_batches(schema, batches).map_err(Error::Arrow)?;
    if sort_key.is_empty() || combined.num_rows() < 2 {
        return Ok(combined);
    }
    let columns: Vec<SortColumn> = sort_key
        .iter()
        .map(|name| -> Result<SortColumn> {
            let idx = combined.schema().index_of(name).map_err(Error::Arrow)?;
            Ok(SortColumn {
                values: combined.column(idx).clone(),
                options: None,
            })
        })
        .collect::<Result<_>>()?;
    let indices = arrow::compute::lexsort_to_indices(&columns, None).map_err(Error::Arrow)?;
    let cols: Vec<ArrayRef> = combined
        .columns()
        .iter()
        .map(|c| arrow::compute::take(c, &indices, None).map_err(Error::Arrow))
        .collect::<Result<_>>()?;
    RecordBatch::try_new(schema.clone(), cols).map_err(Error::Arrow)
}

/// Sort each batch individually by `sort_key` (no concatenation).
pub fn sort_each_batch(batches: &[RecordBatch], sort_key: &[String]) -> Result<Vec<RecordBatch>> {
    batches
        .iter()
        .filter(|b| b.num_rows() > 0)
        .map(|b| {
            if sort_key.is_empty() || b.num_rows() < 2 || batch_is_sorted(b, sort_key)? {
                return Ok(b.clone());
            }
            let columns: Vec<SortColumn> = sort_key
                .iter()
                .map(|name| -> Result<SortColumn> {
                    let idx = b.schema().index_of(name).map_err(Error::Arrow)?;
                    Ok(SortColumn {
                        values: b.column(idx).clone(),
                        options: None,
                    })
                })
                .collect::<Result<_>>()?;
            let indices =
                arrow::compute::lexsort_to_indices(&columns, None).map_err(Error::Arrow)?;
            let cols: Vec<ArrayRef> = b
                .columns()
                .iter()
                .map(|c| arrow::compute::take(c, &indices, None).map_err(Error::Arrow))
                .collect::<Result<_>>()?;
            RecordBatch::try_new(b.schema(), cols).map_err(Error::Arrow)
        })
        .collect()
}

/// K-way merge over individually sorted batches, yielding globally sorted
/// chunks of at most `chunk_rows` rows.
///
/// Unlike concat-then-lexsort this never materializes the full input twice:
/// peak extra memory is the row-encoded sort keys plus one output chunk, and
/// downstream `SegmentWriter::push` sees bounded batches so
/// `target_segment_bytes` actually takes effect.
pub struct SortedBatchMerger {
    batches: Vec<RecordBatch>,
    rows: Vec<arrow::row::Rows>,
    positions: Vec<usize>,
    chunk_rows: usize,
}

impl SortedBatchMerger {
    /// `batches` must each be sorted by `sort_key` (see [`sort_each_batch`]).
    pub fn new(batches: Vec<RecordBatch>, sort_key: &[String], chunk_rows: usize) -> Result<Self> {
        use arrow::row::{RowConverter, SortField};
        let batches: Vec<RecordBatch> = batches.into_iter().filter(|b| b.num_rows() > 0).collect();
        let fields: Vec<SortField> = match batches.first() {
            None => vec![],
            Some(first) => sort_key
                .iter()
                .map(|name| -> Result<SortField> {
                    let idx = first.schema().index_of(name).map_err(Error::Arrow)?;
                    Ok(SortField::new(
                        first.schema().field(idx).data_type().clone(),
                    ))
                })
                .collect::<Result<_>>()?,
        };
        let converter = RowConverter::new(fields).map_err(Error::Arrow)?;
        let rows = batches
            .iter()
            .map(|b| {
                let cols: Vec<ArrayRef> = sort_key
                    .iter()
                    .map(|name| -> Result<ArrayRef> {
                        let idx = b.schema().index_of(name).map_err(Error::Arrow)?;
                        Ok(b.column(idx).clone())
                    })
                    .collect::<Result<_>>()?;
                converter.convert_columns(&cols).map_err(Error::Arrow)
            })
            .collect::<Result<Vec<_>>>()?;
        let positions = vec![0; batches.len()];
        Ok(Self {
            batches,
            rows,
            positions,
            chunk_rows: chunk_rows.max(1),
        })
    }

    /// Next globally sorted chunk, or `None` when the input is exhausted.
    pub fn next_chunk(&mut self) -> Result<Option<RecordBatch>> {
        if self.batches.is_empty() {
            return Ok(None);
        }
        let schema = self.batches[0].schema();
        let mut picks: Vec<(usize, usize)> = Vec::with_capacity(self.chunk_rows);
        while picks.len() < self.chunk_rows {
            // Linear scan over the K batch heads; K is the number of input
            // batches, which is small relative to row count.
            let mut best: Option<usize> = None;
            for (b, &pos) in self.positions.iter().enumerate() {
                if pos >= self.batches[b].num_rows() {
                    continue;
                }
                best = match best {
                    None => Some(b),
                    Some(cur) => {
                        let cur_row = self.rows[cur].row(self.positions[cur]);
                        let cand_row = self.rows[b].row(pos);
                        if cand_row < cur_row {
                            Some(b)
                        } else {
                            Some(cur)
                        }
                    }
                };
            }
            match best {
                None => break,
                Some(b) => {
                    picks.push((b, self.positions[b]));
                    self.positions[b] += 1;
                }
            }
        }
        if picks.is_empty() {
            return Ok(None);
        }
        let cols: Vec<ArrayRef> = (0..schema.fields().len())
            .map(|c| {
                let sources: Vec<&dyn Array> =
                    self.batches.iter().map(|b| b.column(c).as_ref()).collect();
                arrow::compute::interleave(&sources, &picks).map_err(Error::Arrow)
            })
            .collect::<Result<_>>()?;
        Ok(Some(
            RecordBatch::try_new(schema, cols).map_err(Error::Arrow)?,
        ))
    }
}

/// Rows per merged chunk fed to the segment writer (small enough to bound
/// interleave cost, large enough to amortize per-batch overhead).
pub const MERGE_CHUNK_ROWS: usize = 64 * 1024;

// ---------------------------------------------------------------------------
// segment writing
// ---------------------------------------------------------------------------

fn writer_properties(
    spec: &TableSpec,
    schema: &SchemaRef,
    sample_rows_bytes: Option<(usize, usize)>,
) -> WriterProperties {
    let compression = match spec.storage.codec {
        Codec::Zstd => Compression::ZSTD(ZstdLevel::try_new(3).expect("valid zstd level")),
        Codec::Lz4 => Compression::LZ4_RAW,
        Codec::Snappy => Compression::SNAPPY,
        Codec::Uncompressed => Compression::UNCOMPRESSED,
    };
    // Convert the byte-based row-group target into a row count using the
    // observed average row width (fallback 64 B/row).
    let row_width = sample_rows_bytes
        .map(|(rows, bytes)| (bytes.max(1) / rows.max(1)).max(1))
        .unwrap_or(64);
    let rows_per_group =
        (spec.storage.target_row_group_bytes as usize / row_width).clamp(8 * 1024, 4 * 1024 * 1024);
    let _ = schema;
    WriterProperties::builder()
        .set_compression(compression)
        .set_max_row_group_row_count(Some(rows_per_group))
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_column_index_truncate_length(Some(64))
        .build()
}

/// Approximate in-memory Arrow size of a batch (drives segment splitting).
fn batch_mem_bytes(batch: &RecordBatch) -> usize {
    batch.get_array_memory_size()
}

/// A fully written, uploaded segment plus its metadata.
pub struct WrittenSegment {
    pub meta: SegmentMeta,
}

/// How long a staging lease protects uploaded-but-uncommitted segments from
/// vacuum. Long enough for any realistic ingest + review latency on the
/// direct path; the plan/apply path is additionally protected by the plan
/// file itself until plan expiry.
pub const STAGING_LEASE_TTL_SECONDS: u64 = 24 * 3600;

/// On-disk staging lease: written *before* each segment upload, so every
/// staged object is covered from the moment it exists. Vacuum protects the
/// listed paths while the lease is unexpired and collects the lease file
/// afterwards.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct StagingLeaseFile {
    pub writer_id: Uuid,
    pub created_at_ns: i64,
    pub expires_at_ns: i64,
    pub segment_paths: Vec<String>,
}

impl StagingLeaseFile {
    pub(crate) fn is_expired(&self) -> bool {
        crate::util::monotonic_commit_ts(None) > self.expires_at_ns
    }
}

/// Writes record batches into one or more Parquet segment objects, splitting
/// at `target_segment_bytes` of in-memory Arrow data, collecting statistics
/// and the content checksum along the way.
pub struct SegmentWriter<'a> {
    backend: &'a Backend,
    spec: &'a TableSpec,
    schema: SchemaRef,
    created_by_sequence: u64,
    /// Segments written so far.
    pub written: Vec<WrittenSegment>,
    // current in-progress buffer
    buffered: Vec<RecordBatch>,
    buffered_bytes: usize,
    input_sorted: bool,
    last_sort_row: Option<Vec<ScalarStat>>,
    // staging lease (created lazily on first upload)
    writer_id: Uuid,
    lease: Option<StagingLeaseFile>,
}

impl<'a> SegmentWriter<'a> {
    pub fn new(
        backend: &'a Backend,
        spec: &'a TableSpec,
        schema: SchemaRef,
        created_by_sequence: u64,
    ) -> Self {
        Self {
            backend,
            spec,
            schema,
            created_by_sequence,
            written: Vec::new(),
            buffered: Vec::new(),
            buffered_bytes: 0,
            input_sorted: true,
            last_sort_row: None,
            writer_id: Uuid::new_v4(),
            lease: None,
        }
    }

    /// Path of this writer's staging lease, if any segment was uploaded.
    /// Callers delete it (best-effort) once the segments are referenced by a
    /// committed manifest or a stored plan.
    pub fn lease_path(&self) -> Option<object_store::path::Path> {
        self.lease
            .as_ref()
            .map(|_| crate::layout::staging_lease_path(self.spec.table_id, self.writer_id))
    }

    /// Record `path` in the staging lease and persist it — called before the
    /// segment object itself is uploaded so vacuum can never see an
    /// unprotected staged segment.
    async fn record_staged(&mut self, path: &str) -> Result<()> {
        let now = crate::util::monotonic_commit_ts(None);
        let lease = self.lease.get_or_insert_with(|| StagingLeaseFile {
            writer_id: self.writer_id,
            created_at_ns: now,
            expires_at_ns: now + (STAGING_LEASE_TTL_SECONDS as i64) * 1_000_000_000,
            segment_paths: Vec::new(),
        });
        lease.segment_paths.push(path.to_string());
        let lease_path = crate::layout::staging_lease_path(self.spec.table_id, self.writer_id);
        self.backend
            .put(&lease_path, serde_json::to_vec(&self.lease)?.into())
            .await
    }

    /// Feed one batch; flushes a segment whenever the buffer crosses the
    /// target size.
    pub async fn push(&mut self, batch: RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        // Track cross-batch sortedness on the sort key (cheap scalar compare
        // of boundary rows + per-batch check).
        if !self.spec.sort_key.is_empty() {
            if !batch_is_sorted(&batch, &self.spec.sort_key)? {
                self.input_sorted = false;
            }
            if self.input_sorted {
                let first = sort_row_scalars(&batch, 0, &self.spec.sort_key)?;
                if let Some(last) = &self.last_sort_row {
                    if row_less_than(&first, last) {
                        self.input_sorted = false;
                    }
                }
                self.last_sort_row = Some(sort_row_scalars(
                    &batch,
                    batch.num_rows() - 1,
                    &self.spec.sort_key,
                )?);
            }
        }
        self.buffered_bytes += batch_mem_bytes(&batch);
        self.buffered.push(batch);
        if self.buffered_bytes >= self.spec.storage.target_segment_bytes as usize {
            self.flush().await?;
        }
        Ok(())
    }

    /// Flush the buffered rows into one segment object.
    pub async fn flush(&mut self) -> Result<()> {
        if self.buffered.is_empty() {
            return Ok(());
        }
        let batches = std::mem::take(&mut self.buffered);
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        let bytes = std::mem::take(&mut self.buffered_bytes);
        let props = writer_properties(self.spec, &self.schema, Some((rows, bytes)));

        // Encode to an in-memory Parquet buffer.
        let mut buf: Vec<u8> = Vec::with_capacity(bytes / 4);
        {
            let mut writer = ArrowWriter::try_new(&mut buf, self.schema.clone(), Some(props))
                .map_err(Error::Parquet)?;
            for b in &batches {
                writer.write(b).map_err(Error::Parquet)?;
            }
            writer.close().map_err(Error::Parquet)?;
        }

        // Statistics.
        let mut accs: BTreeMap<String, ColumnAcc> = BTreeMap::new();
        for b in &batches {
            for (i, field) in self.schema.fields().iter().enumerate() {
                let arr = b.column(i);
                let acc = accs.entry(field.name().clone()).or_default();
                match array_minmax(arr) {
                    Some((min, max)) => acc.observe(min, max, arr.null_count() as u64),
                    None => acc.drop_stats(arr.null_count() as u64),
                }
                acc.observe_distinct(arr);
            }
        }

        let time_range = match &self.spec.time_column {
            Some(tc) => {
                let mut mn = i64::MAX;
                let mut mx = i64::MIN;
                for b in &batches {
                    if let Some((bmn, bmx)) = time_min_max(b, tc)? {
                        mn = mn.min(bmn);
                        mx = mx.max(bmx);
                    }
                }
                if mn <= mx {
                    Some((mn, mx))
                } else {
                    None
                }
            }
            None => None,
        };

        let checksum = crate::util::checksum_hex(&buf);
        let id = Uuid::new_v4();
        let path = crate::layout::segment_path(self.spec.table_id, id);
        let encoded_len = buf.len() as u64;
        self.record_staged(path.as_ref()).await?;
        self.backend.put(&path, Bytes::from(buf)).await?;

        self.written.push(WrittenSegment {
            meta: SegmentMeta {
                id,
                path: path.as_ref().to_string(),
                rows: rows as u64,
                bytes: encoded_len,
                checksum,
                time_range,
                sorted: self.input_sorted && !self.spec.sort_key.is_empty(),
                schema_revision: self.spec.schema_revision,
                created_by_sequence: self.created_by_sequence,
                columns: accs.into_iter().map(|(k, v)| (k, v.finish())).collect(),
            },
        });
        Ok(())
    }

    /// Finish: flush the tail and return all segment metadata, whether the
    /// input was sorted, and the staging lease path (if any segment was
    /// uploaded) for the caller to delete once the segments are referenced
    /// by a committed manifest or stored plan.
    pub async fn finish(
        mut self,
    ) -> Result<(Vec<SegmentMeta>, bool, Option<object_store::path::Path>)> {
        self.flush().await?;
        let sorted = self.input_sorted;
        let lease = self.lease_path();
        Ok((
            self.written.into_iter().map(|w| w.meta).collect(),
            sorted,
            lease,
        ))
    }
}

fn sort_row_scalars(
    batch: &RecordBatch,
    row: usize,
    sort_key: &[String],
) -> Result<Vec<ScalarStat>> {
    sort_key
        .iter()
        .map(|name| -> Result<ScalarStat> {
            let idx = batch.schema().index_of(name).map_err(Error::Arrow)?;
            let col = batch.column(idx).slice(row, 1);
            match array_minmax(&col) {
                Some((Some(min), _)) => Ok(min),
                _ => Err(Error::invalid(format!(
                    "sort key column {name:?} has a null or non-comparable value at a batch boundary"
                ))),
            }
        })
        .collect()
}

fn row_less_than(a: &[ScalarStat], b: &[ScalarStat]) -> bool {
    for (x, y) in a.iter().zip(b) {
        if x.less_than(y) {
            return true;
        }
        if y.less_than(x) {
            return false;
        }
    }
    false
}

// ---------------------------------------------------------------------------
// segment reading
// ---------------------------------------------------------------------------

/// Read one segment with its full-file blake3 checksum verified against the
/// manifest before decoding — the strong integrity path for readers that opt
/// in (`ScanOptions::verify_checksums`). Necessarily fetches the whole
/// object, so row-group pruning does not apply; the exact row-level time
/// filter and projection still do.
///
/// (parquet-rs 58 cannot *write* page-level CRCs — the writer always emits
/// `crc: None` — so whole-file verification is the only self-contained
/// integrity check available for our own segments. The `crc` read feature is
/// enabled for externally written files that do carry page CRCs.)
pub async fn read_segment_verified(
    backend: &Backend,
    segment: &SegmentMeta,
    projection: Option<&[String]>,
    time_filter: Option<(&str, Option<i64>, Option<i64>)>,
) -> Result<Vec<RecordBatch>> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let path = object_store::path::Path::from(segment.path.as_str());
    let bytes = backend.get(&path).await?;
    let actual = crate::util::checksum_hex(&bytes);
    if actual != segment.checksum {
        return Err(Error::corruption(
            &segment.path,
            format!(
                "segment content checksum mismatch (manifest {}, object {actual})",
                segment.checksum
            ),
        ));
    }
    let mut builder = ParquetRecordBatchReaderBuilder::try_new(bytes).map_err(Error::Parquet)?;
    if let Some(cols) = projection {
        let arrow_schema = builder.schema().clone();
        let indices: Vec<usize> = cols
            .iter()
            .map(|c| arrow_schema.index_of(c).map_err(Error::Arrow))
            .collect::<Result<_>>()?;
        let mask = ProjectionMask::roots(builder.parquet_schema(), indices);
        builder = builder.with_projection(mask);
    }
    let reader = builder.build().map_err(Error::Parquet)?;
    let mut batches: Vec<RecordBatch> = reader
        .collect::<std::result::Result<_, _>>()
        .map_err(Error::Arrow)?;
    if let Some((time_col, start, end)) = time_filter {
        if start.is_some() || end.is_some() {
            batches = filter_batches_by_time(batches, time_col, start, end)?;
        }
    }
    Ok(batches)
}

/// Stream one segment's batches with optional projection and time-range
/// filtering (`[start, end)` in raw time units). Row groups whose metadata
/// excludes the range are skipped before any data I/O.
pub async fn read_segment(
    backend: &Backend,
    segment: &SegmentMeta,
    projection: Option<&[String]>,
    time_filter: Option<(&str, Option<i64>, Option<i64>)>,
) -> Result<Vec<RecordBatch>> {
    let path = object_store::path::Path::from(segment.path.as_str());
    let reader = ParquetObjectReader::new(backend.store.clone(), path);
    let metadata = ArrowReaderMetadata::load_async(
        &mut reader.clone(),
        ArrowReaderOptions::new()
            .with_page_index_policy(parquet::file::metadata::PageIndexPolicy::Optional),
    )
    .await
    .map_err(Error::Parquet)?;
    let mut builder = ParquetRecordBatchStreamBuilder::new_with_metadata(reader, metadata.clone());

    // Projection by column name.
    if let Some(cols) = projection {
        let parquet_schema = metadata.metadata().file_metadata().schema_descr();
        let arrow_schema = metadata.schema();
        let indices: Vec<usize> = cols
            .iter()
            .map(|c| arrow_schema.index_of(c).map_err(Error::Arrow))
            .collect::<Result<_>>()?;
        builder = builder.with_projection(ProjectionMask::roots(parquet_schema, indices));
    }

    // Row-group pruning on the time column via parquet stats.
    if let Some((time_col, start, end)) = time_filter {
        let arrow_schema = metadata.schema();
        if let Ok(col_idx) = arrow_schema.index_of(time_col) {
            let keep: Vec<usize> = metadata
                .metadata()
                .row_groups()
                .iter()
                .enumerate()
                .filter_map(|(rg_idx, rg)| {
                    let col = rg.column(col_idx);
                    let stats = col.statistics()?;
                    let (min, max) = parquet_i64_stats(stats)?;
                    let after_start = end.is_none_or(|e| min < e);
                    let before_end = start.is_none_or(|s| max >= s);
                    (after_start && before_end).then_some(rg_idx)
                })
                .collect();
            // Only apply when stats existed for every row group; otherwise
            // scan everything rather than risk dropping data.
            if metadata
                .metadata()
                .row_groups()
                .iter()
                .all(|rg| rg.column(col_idx).statistics().is_some())
            {
                builder = builder.with_row_groups(keep);
            }
        }
    }

    let stream = builder.build().map_err(Error::Parquet)?;
    let mut batches: Vec<RecordBatch> = stream.try_collect().await.map_err(Error::Parquet)?;

    // Exact row-level time filter.
    if let Some((time_col, start, end)) = time_filter {
        if start.is_some() || end.is_some() {
            batches = filter_batches_by_time(batches, time_col, start, end)?;
        }
    }
    Ok(batches)
}

fn parquet_i64_stats(stats: &parquet::file::statistics::Statistics) -> Option<(i64, i64)> {
    use parquet::file::statistics::Statistics::*;
    match stats {
        Int64(s) => Some((*s.min_opt()?, *s.max_opt()?)),
        Int32(s) => Some((*s.min_opt()? as i64, *s.max_opt()? as i64)),
        _ => None,
    }
}

/// Keep only rows with `start <= t < end`. Batches without the time column
/// (projection pushdown upstream) cannot be filtered here — callers must
/// include the time column when they pass a filter.
pub fn filter_batches_by_time(
    batches: Vec<RecordBatch>,
    time_col: &str,
    start: Option<i64>,
    end: Option<i64>,
) -> Result<Vec<RecordBatch>> {
    let mut out = Vec::with_capacity(batches.len());
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let idx = match batch.schema().index_of(time_col) {
            Ok(i) => i,
            Err(_) => {
                return Err(Error::internal(
                    "time filter requested but time column not present in projection",
                ))
            }
        };
        let col = batch.column(idx);
        let casted = arrow::compute::cast(col, &DataType::Int64).map_err(Error::Arrow)?;
        let arr = casted
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .ok_or_else(|| Error::internal("time column cast to i64 failed"))?;
        // A null here means a corrupt segment (writes reject null time
        // values) — fail with a diagnosable error instead of panicking.
        if arr.null_count() > 0 {
            return Err(Error::corruption(
                time_col,
                "null time value in stored segment (writes reject nulls; segment is corrupt)",
            ));
        }
        let mask: arrow::array::BooleanArray = arr
            .values()
            .iter()
            .map(|&v| Some(start.is_none_or(|s| v >= s) && end.is_none_or(|e| v < e)))
            .collect();
        let filtered = arrow::compute::filter_record_batch(&batch, &mask).map_err(Error::Arrow)?;
        if filtered.num_rows() > 0 {
            out.push(filtered);
        }
    }
    Ok(out)
}
