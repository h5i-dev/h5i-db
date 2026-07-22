//! Parquet segment writer/reader plus per-column statistics collection.
//!
//! Segments are immutable: written once under a fresh UUID path, referenced
//! by manifests, and only ever deleted by vacuum when unreachable.

use std::collections::BTreeMap;

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
}

impl ColumnAcc {
    fn observe(&mut self, min: Option<ScalarStat>, max: Option<ScalarStat>, nulls: u64) {
        self.null_count += nulls;
        if self.stats_dropped {
            return;
        }
        if let Some(m) = min {
            if self.min.as_ref().map_or(true, |cur| m.less_than(cur)) {
                self.min = Some(m);
            }
        }
        if let Some(m) = max {
            if self.max.as_ref().map_or(true, |cur| cur.less_than(&m)) {
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

    fn finish(self) -> ColumnStats {
        ColumnStats {
            min: self.min.map(|s| s.to_json()),
            max: self.max.map(|s| s.to_json()),
            null_count: self.null_count,
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

/// Read the time column of a batch as raw i64 values (unit per schema).
pub fn time_values_i64(batch: &RecordBatch, time_col: &str) -> Result<Vec<i64>> {
    let idx = batch.schema().index_of(time_col).map_err(Error::Arrow)?;
    let col = batch.column(idx);
    let casted = arrow::compute::cast(col, &DataType::Int64).map_err(Error::Arrow)?;
    let arr = casted
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .ok_or_else(|| Error::internal("time column cast to i64 failed"))?;
    if arr.null_count() > 0 {
        return Err(Error::invalid(format!(
            "time column {time_col:?} contains nulls"
        )));
    }
    Ok(arr.values().to_vec())
}

/// Check that `batch` is sorted ascending by the sort key columns.
pub fn batch_is_sorted(batch: &RecordBatch, sort_key: &[String]) -> Result<bool> {
    if sort_key.is_empty() || batch.num_rows() < 2 {
        return Ok(true);
    }
    let columns: Vec<SortColumn> = sort_key
        .iter()
        .map(|name| -> Result<SortColumn> {
            let idx = batch.schema().index_of(name).map_err(Error::Arrow)?;
            Ok(SortColumn {
                values: batch.column(idx).clone(),
                options: None,
            })
        })
        .collect::<Result<_>>()?;
    let ranks = arrow::compute::lexsort_to_indices(&columns, None).map_err(Error::Arrow)?;
    Ok(ranks.values().windows(2).all(|w| w[0] < w[1]) || {
        // lexsort_to_indices is stable, so sorted input yields identity.
        ranks
            .values()
            .iter()
            .enumerate()
            .all(|(i, &v)| v as usize == i)
    })
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
        }
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
            }
        }

        let time_range = match &self.spec.time_column {
            Some(tc) => {
                let mut mn = i64::MAX;
                let mut mx = i64::MIN;
                for b in &batches {
                    let vals = time_values_i64(b, tc)?;
                    for v in vals {
                        mn = mn.min(v);
                        mx = mx.max(v);
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

    /// Finish: flush the tail and return all segment metadata.
    pub async fn finish(mut self) -> Result<(Vec<SegmentMeta>, bool)> {
        self.flush().await?;
        let sorted = self.input_sorted;
        Ok((self.written.into_iter().map(|w| w.meta).collect(), sorted))
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
                    let after_start = end.map_or(true, |e| min < e);
                    let before_end = start.map_or(true, |s| max >= s);
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
            .unwrap();
        let mask: arrow::array::BooleanArray = arr
            .iter()
            .map(|v| {
                let v = v.expect("time column is non-null");
                Some(start.map_or(true, |s| v >= s) && end.map_or(true, |e| v < e))
            })
            .collect();
        let filtered = arrow::compute::filter_record_batch(&batch, &mask).map_err(Error::Arrow)?;
        if filtered.num_rows() > 0 {
            out.push(filtered);
        }
    }
    Ok(out)
}
