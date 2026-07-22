//! `PruningStatistics` over h5i-db manifest segment statistics.
//!
//! This is the integration point that lets DataFusion's `PruningPredicate`
//! prove "no rows in this segment can match" from manifest min/max/null
//! statistics — before any object I/O.

use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, UInt64Array};
use arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::pruning::PruningStatistics;
use datafusion::common::Column;
use datafusion::scalar::ScalarValue;
use h5i_db_core::SegmentMeta;

/// Build a typed Arrow scalar from a manifest JSON stat for `data_type`.
fn json_stat_to_scalar(v: &serde_json::Value, data_type: &DataType) -> Option<ScalarValue> {
    use arrow::datatypes::TimeUnit;
    match data_type {
        DataType::Int8 => v.as_i64().map(|x| ScalarValue::Int8(Some(x as i8))),
        DataType::Int16 => v.as_i64().map(|x| ScalarValue::Int16(Some(x as i16))),
        DataType::Int32 => v.as_i64().map(|x| ScalarValue::Int32(Some(x as i32))),
        DataType::Int64 => v.as_i64().map(|x| ScalarValue::Int64(Some(x))),
        DataType::UInt8 => v.as_u64().map(|x| ScalarValue::UInt8(Some(x as u8))),
        DataType::UInt16 => v.as_u64().map(|x| ScalarValue::UInt16(Some(x as u16))),
        DataType::UInt32 => v.as_u64().map(|x| ScalarValue::UInt32(Some(x as u32))),
        DataType::UInt64 => v.as_u64().map(|x| ScalarValue::UInt64(Some(x))),
        DataType::Float32 => v.as_f64().map(|x| ScalarValue::Float32(Some(x as f32))),
        DataType::Float64 => v.as_f64().map(|x| ScalarValue::Float64(Some(x))),
        DataType::Boolean => v.as_bool().map(|x| ScalarValue::Boolean(Some(x))),
        DataType::Utf8 => v.as_str().map(|s| ScalarValue::Utf8(Some(s.to_string()))),
        DataType::LargeUtf8 => v
            .as_str()
            .map(|s| ScalarValue::LargeUtf8(Some(s.to_string()))),
        DataType::Date32 => v.as_i64().map(|x| ScalarValue::Date32(Some(x as i32))),
        DataType::Date64 => v.as_i64().map(|x| ScalarValue::Date64(Some(x))),
        DataType::Timestamp(unit, tz) => {
            let x = v.as_i64()?;
            Some(match unit {
                TimeUnit::Second => ScalarValue::TimestampSecond(Some(x), tz.clone()),
                TimeUnit::Millisecond => ScalarValue::TimestampMillisecond(Some(x), tz.clone()),
                TimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(Some(x), tz.clone()),
                TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(Some(x), tz.clone()),
            })
        }
        // Dictionary-encoded strings: stats were computed over values.
        DataType::Dictionary(_, value) if **value == DataType::Utf8 => {
            v.as_str().map(|s| ScalarValue::Utf8(Some(s.to_string())))
        }
        _ => None,
    }
}

fn null_scalar(data_type: &DataType) -> Option<ScalarValue> {
    ScalarValue::try_from(data_type).ok()
}

/// Manifest-backed pruning statistics: one "container" per segment.
pub struct ManifestPruningStats<'a> {
    segments: &'a [SegmentMeta],
    schema: SchemaRef,
}

impl<'a> ManifestPruningStats<'a> {
    pub fn new(segments: &'a [SegmentMeta], schema: SchemaRef) -> Self {
        Self { segments, schema }
    }

    /// Collect min or max values for `column` across all segments into one
    /// typed array (None entries where a segment lacks the stat).
    fn stat_array(&self, column: &Column, max: bool) -> Option<ArrayRef> {
        let field = self.schema.field_with_name(&column.name).ok()?;
        let dt = match field.data_type() {
            // Stats for dictionary columns are stored as plain strings.
            DataType::Dictionary(_, value) if **value == DataType::Utf8 => DataType::Utf8,
            other => other.clone(),
        };
        let scalars: Vec<ScalarValue> = self
            .segments
            .iter()
            .map(|seg| {
                seg.columns
                    .get(&column.name)
                    .and_then(|st| {
                        if max {
                            st.max.as_ref()
                        } else {
                            st.min.as_ref()
                        }
                    })
                    .and_then(|v| json_stat_to_scalar(v, &dt))
                    .or_else(|| null_scalar(&dt))
                    .unwrap_or(ScalarValue::Null)
            })
            .collect();
        ScalarValue::iter_to_array(scalars).ok()
    }
}

impl PruningStatistics for ManifestPruningStats<'_> {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        self.stat_array(column, false)
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        self.stat_array(column, true)
    }

    fn num_containers(&self) -> usize {
        self.segments.len()
    }

    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        let vals: Vec<Option<u64>> = self
            .segments
            .iter()
            .map(|seg| seg.columns.get(&column.name).map(|st| st.null_count))
            .collect();
        Some(Arc::new(UInt64Array::from(vals)))
    }

    fn row_counts(&self) -> Option<ArrayRef> {
        Some(Arc::new(UInt64Array::from(
            self.segments
                .iter()
                .map(|s| Some(s.rows))
                .collect::<Vec<_>>(),
        )))
    }

    fn contained(
        &self,
        column: &Column,
        values: &std::collections::HashSet<ScalarValue>,
    ) -> Option<BooleanArray> {
        let field = self.schema.field_with_name(&column.name).ok()?;
        let dt = match field.data_type() {
            DataType::Dictionary(_, value) if **value == DataType::Utf8 => DataType::Utf8,
            other => other.clone(),
        };
        let result: Vec<Option<bool>> = self
            .segments
            .iter()
            .map(|segment| {
                let stats = segment.columns.get(&column.name)?;
                let distinct = stats.distinct_values.as_ref()?;
                if distinct.is_empty() {
                    return None;
                }
                let stored: Vec<ScalarValue> = distinct
                    .iter()
                    .filter_map(|v| json_stat_to_scalar(v, &dt))
                    .collect();
                if stored.len() != distinct.len() {
                    return None;
                }
                if stored.iter().all(|v| values.contains(v)) {
                    Some(true)
                } else if stored.iter().all(|v| !values.contains(v)) {
                    Some(false)
                } else {
                    None
                }
            })
            .collect();
        result
            .iter()
            .any(Option::is_some)
            .then(|| BooleanArray::from(result))
    }
}
