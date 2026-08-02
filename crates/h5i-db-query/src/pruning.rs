//! `PruningStatistics` over h5i-db manifest segment statistics.
//!
//! This is the integration point that lets DataFusion's `PruningPredicate`
//! prove "no rows in this segment can match" from manifest min/max/null
//! statistics — before any object I/O.

use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, UInt64Array};
use arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::Column;
use datafusion::common::pruning::PruningStatistics;
use datafusion::scalar::ScalarValue;
use h5i_db_core::SegmentMeta;

/// Build a typed Arrow scalar from a manifest JSON stat for `data_type`.
///
/// `None` means "no usable bound", which every caller treats as unknown and
/// therefore keeps the segment. That is the fail-open path, and it is what a
/// stat of an unexpected shape lands in -- a manifest written by a newer
/// version, or a type this does not record stats for.
///
/// Shared with [`crate::provider`] rather than duplicated. The two copies
/// were byte-identical, which is exactly the arrangement where adding a type
/// to one and not the other produces a pruner and a scanner that disagree.
pub(crate) fn json_stat_to_scalar(
    v: &serde_json::Value,
    data_type: &DataType,
) -> Option<ScalarValue> {
    use arrow::datatypes::TimeUnit;
    // Narrowing conversions go through `try_from`, never `as`. A truncating
    // `as` turns an out-of-range stat into a plausible in-range bound (i64
    // 300 becomes i8 44), and a *wrong bound prunes*: the segment holding the
    // matching rows is proved impossible and skipped, so the query silently
    // returns fewer rows. Refusing the stat instead lands in the fail-open path
    // above, which keeps the segment. A stat outside the column's own range is
    // a manifest bug either way, but one of the two readings loses data.
    match data_type {
        DataType::Int8 => v
            .as_i64()
            .and_then(|x| i8::try_from(x).ok())
            .map(|x| ScalarValue::Int8(Some(x))),
        DataType::Int16 => v
            .as_i64()
            .and_then(|x| i16::try_from(x).ok())
            .map(|x| ScalarValue::Int16(Some(x))),
        DataType::Int32 => v
            .as_i64()
            .and_then(|x| i32::try_from(x).ok())
            .map(|x| ScalarValue::Int32(Some(x))),
        DataType::Int64 => v.as_i64().map(|x| ScalarValue::Int64(Some(x))),
        DataType::UInt8 => v
            .as_u64()
            .and_then(|x| u8::try_from(x).ok())
            .map(|x| ScalarValue::UInt8(Some(x))),
        DataType::UInt16 => v
            .as_u64()
            .and_then(|x| u16::try_from(x).ok())
            .map(|x| ScalarValue::UInt16(Some(x))),
        DataType::UInt32 => v
            .as_u64()
            .and_then(|x| u32::try_from(x).ok())
            .map(|x| ScalarValue::UInt32(Some(x))),
        DataType::UInt64 => v.as_u64().map(|x| ScalarValue::UInt64(Some(x))),
        // f64 -> f32 is not a truncation but a rounding, and it can round a
        // min *up* or a max *down*, which are the two directions that prune a
        // live segment. Keep only values that survive the round trip; an
        // f32-typed column's stats were written from f32 values, so they do.
        DataType::Float32 => v
            .as_f64()
            .filter(|x| f64::from(*x as f32) == *x)
            .map(|x| ScalarValue::Float32(Some(x as f32))),
        DataType::Float64 => v.as_f64().map(|x| ScalarValue::Float64(Some(x))),
        DataType::Boolean => v.as_bool().map(|x| ScalarValue::Boolean(Some(x))),
        DataType::Utf8 => v.as_str().map(|s| ScalarValue::Utf8(Some(s.to_string()))),
        DataType::LargeUtf8 => v
            .as_str()
            .map(|s| ScalarValue::LargeUtf8(Some(s.to_string()))),
        DataType::Date32 => v
            .as_i64()
            .and_then(|x| i32::try_from(x).ok())
            .map(|x| ScalarValue::Date32(Some(x))),
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
        // Written as a decimal string, because an `i128` does not fit a JSON
        // number. The precision and scale come from `data_type` -- the
        // schema is the single authority on them, and a column's cannot
        // change under evolution -- so the stat carries only the unscaled
        // integer and there is no scale to mismatch.
        DataType::Decimal128(precision, scale) => {
            let raw: i128 = v.as_str()?.parse().ok()?;
            Some(ScalarValue::Decimal128(Some(raw), *precision, *scale))
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::TimeUnit;
    use serde_json::json;

    #[test]
    fn json_stat_maps_signed_and_unsigned_integers() {
        assert_eq!(
            json_stat_to_scalar(&json!(42), &DataType::Int64),
            Some(ScalarValue::Int64(Some(42)))
        );
        assert_eq!(
            json_stat_to_scalar(&json!(-1), &DataType::Int32),
            Some(ScalarValue::Int32(Some(-1)))
        );
        assert_eq!(
            json_stat_to_scalar(&json!(7), &DataType::UInt64),
            Some(ScalarValue::UInt64(Some(7)))
        );
    }

    /// A stat that does not fit the column's type is refused rather than
    /// truncated: `300 as i8` is 44, and a bound of 44 on a column whose values
    /// reach 300 proves the wrong segments impossible.
    #[test]
    fn out_of_range_integer_stats_are_refused_not_truncated() {
        assert_eq!(json_stat_to_scalar(&json!(300), &DataType::Int8), None);
        assert_eq!(json_stat_to_scalar(&json!(70_000), &DataType::Int16), None);
        assert_eq!(
            json_stat_to_scalar(&json!(i64::MAX), &DataType::Int32),
            None
        );
        assert_eq!(json_stat_to_scalar(&json!(300), &DataType::UInt8), None);
        // A value f32 cannot hold exactly is refused for the same reason:
        // rounding a min up or a max down prunes live rows.
        assert_eq!(json_stat_to_scalar(&json!(1e300), &DataType::Float32), None);
        // In-range values are untouched.
        assert_eq!(
            json_stat_to_scalar(&json!(44), &DataType::Int8),
            Some(ScalarValue::Int8(Some(44)))
        );
        assert_eq!(
            json_stat_to_scalar(&json!(1.5), &DataType::Float32),
            Some(ScalarValue::Float32(Some(1.5)))
        );
    }

    #[test]
    fn json_stat_maps_floats_bools_and_strings() {
        assert_eq!(
            json_stat_to_scalar(&json!(1.5), &DataType::Float64),
            Some(ScalarValue::Float64(Some(1.5)))
        );
        assert_eq!(
            json_stat_to_scalar(&json!(true), &DataType::Boolean),
            Some(ScalarValue::Boolean(Some(true)))
        );
        assert_eq!(
            json_stat_to_scalar(&json!("AAPL"), &DataType::Utf8),
            Some(ScalarValue::Utf8(Some("AAPL".into())))
        );
    }

    #[test]
    fn json_stat_maps_timestamps_preserving_unit_and_tz() {
        assert_eq!(
            json_stat_to_scalar(
                &json!(1_000),
                &DataType::Timestamp(TimeUnit::Nanosecond, None)
            ),
            Some(ScalarValue::TimestampNanosecond(Some(1_000), None))
        );
        let tz: Arc<str> = Arc::from("UTC");
        assert_eq!(
            json_stat_to_scalar(
                &json!(5),
                &DataType::Timestamp(TimeUnit::Second, Some(tz.clone()))
            ),
            Some(ScalarValue::TimestampSecond(Some(5), Some(tz)))
        );
    }

    #[test]
    fn json_stat_maps_dictionary_of_utf8_to_its_values() {
        let dict = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        assert_eq!(
            json_stat_to_scalar(&json!("MSFT"), &dict),
            Some(ScalarValue::Utf8(Some("MSFT".into())))
        );
    }

    #[test]
    fn json_stat_returns_none_on_type_mismatch_or_unsupported() {
        // A string where an integer is expected: no coercion.
        assert_eq!(json_stat_to_scalar(&json!("x"), &DataType::Int64), None);
        // JSON null is never a usable stat.
        assert_eq!(json_stat_to_scalar(&json!(null), &DataType::Int64), None);
        // Unsupported target type.
        assert_eq!(json_stat_to_scalar(&json!(1), &DataType::Binary), None);
    }

    #[test]
    fn empty_segment_set_has_no_containers() {
        let schema: SchemaRef = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("v", DataType::Int64, true),
        ]));
        let stats = ManifestPruningStats::new(&[], schema);
        assert_eq!(stats.num_containers(), 0);
    }
}
