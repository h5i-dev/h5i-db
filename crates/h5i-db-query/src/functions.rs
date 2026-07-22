//! Time-series scalar functions: `time_bucket`.
//!
//! Semantics follow DuckDB/TimescaleDB: fixed-width buckets are aligned to
//! origin 2000-01-03 00:00:00 UTC (a Monday, so weekly buckets start on
//! Mondays); month/year widths use calendar bucketing from 2000-01-01.
//! An optional third argument overrides the origin.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, PrimitiveArray};
use arrow::datatypes::{
    DataType, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType,
};
use chrono::{Datelike, NaiveDate};
use datafusion::common::ScalarValue;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

/// 2000-01-03T00:00:00Z in seconds (Monday; DuckDB/Timescale default origin).
const DEFAULT_ORIGIN_SECS: i64 = 946_857_600;
/// 2000-01-01 as (year, month) origin for calendar (month-width) buckets.
const MONTH_ORIGIN_YEAR: i32 = 2000;

#[derive(Debug, Clone, Copy, PartialEq)]
enum Width {
    /// Fixed width in nanoseconds.
    Nanos(i64),
    /// Calendar months.
    Months(i32),
}

fn parse_interval_scalar(v: &ScalarValue) -> DfResult<Width> {
    match v {
        ScalarValue::IntervalMonthDayNano(Some(i)) => {
            if i.months != 0 {
                if i.days != 0 || i.nanoseconds != 0 {
                    return Err(DataFusionError::Plan(
                        "time_bucket: mixed month + sub-month intervals are not supported".into(),
                    ));
                }
                Ok(Width::Months(i.months))
            } else {
                let ns = i.days as i64 * 86_400_000_000_000 + i.nanoseconds;
                if ns <= 0 {
                    return Err(DataFusionError::Plan(
                        "time_bucket: interval must be positive".into(),
                    ));
                }
                Ok(Width::Nanos(ns))
            }
        }
        ScalarValue::IntervalDayTime(Some(i)) => {
            let ns = i.days as i64 * 86_400_000_000_000 + i.milliseconds as i64 * 1_000_000;
            if ns <= 0 {
                return Err(DataFusionError::Plan(
                    "time_bucket: interval must be positive".into(),
                ));
            }
            Ok(Width::Nanos(ns))
        }
        ScalarValue::IntervalYearMonth(Some(m)) => Ok(Width::Months(*m)),
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => parse_interval_str(s),
        other => Err(DataFusionError::Plan(format!(
            "time_bucket: unsupported interval argument {other:?} (use INTERVAL '…' or a \
             string like '5m', '1 hour')"
        ))),
    }
}

/// Parse `"5m"`, `"1 hour"`, `"30 minutes"`, `"1mo"`, … into a width.
fn parse_interval_str(s: &str) -> DfResult<Width> {
    let s = s.trim().to_ascii_lowercase();
    let split = s
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .ok_or_else(|| {
            DataFusionError::Plan(format!("time_bucket: missing unit in interval {s:?}"))
        })?;
    let (num, unit) = s.split_at(split);
    let n: f64 = num
        .trim()
        .parse()
        .map_err(|_| DataFusionError::Plan(format!("time_bucket: bad interval number in {s:?}")))?;
    let unit = unit.trim().trim_end_matches('s');
    let ns_per: i64 = match unit {
        "ns" | "nanosecond" => 1,
        "us" | "microsecond" => 1_000,
        "ms" | "millisecond" => 1_000_000,
        "s" | "sec" | "second" => 1_000_000_000,
        "m" | "min" | "minute" => 60_000_000_000,
        "h" | "hr" | "hour" => 3_600_000_000_000,
        "d" | "day" => 86_400_000_000_000,
        "w" | "week" => 7 * 86_400_000_000_000,
        "mo" | "mon" | "month" => {
            return Ok(Width::Months(n as i32));
        }
        "y" | "yr" | "year" => {
            return Ok(Width::Months(n as i32 * 12));
        }
        other => {
            return Err(DataFusionError::Plan(format!(
                "time_bucket: unknown interval unit {other:?}"
            )))
        }
    };
    let ns = (n * ns_per as f64) as i64;
    if ns <= 0 {
        return Err(DataFusionError::Plan(
            "time_bucket: interval must be positive".into(),
        ));
    }
    Ok(Width::Nanos(ns))
}

fn unit_factor(unit: &TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => 1_000_000_000,
        TimeUnit::Millisecond => 1_000_000,
        TimeUnit::Microsecond => 1_000,
        TimeUnit::Nanosecond => 1,
    }
}

fn floor_div(a: i64, b: i64) -> i64 {
    let (q, r) = (a / b, a % b);
    if r != 0 && (r < 0) != (b < 0) {
        q - 1
    } else {
        q
    }
}

/// Bucket one raw value (in `unit`) with a fixed nanosecond width.
fn bucket_fixed(value: i64, unit_ns: i64, width_ns: i64, origin_ns: i64) -> Option<i64> {
    let t_ns = value.checked_mul(unit_ns)?;
    let offset = t_ns.checked_sub(origin_ns)?;
    let bucket_ns = origin_ns.checked_add(floor_div(offset, width_ns).checked_mul(width_ns)?)?;
    Some(bucket_ns / unit_ns)
}

/// Bucket one raw value (in `unit`) with a calendar month width.
fn bucket_months(value: i64, unit_ns: i64, months: i32, origin_ns: i64) -> Option<i64> {
    let t_ns = value.checked_mul(unit_ns)?;
    let dt = chrono::DateTime::from_timestamp_nanos(t_ns);
    // Origin defines the month phase; default is 2000-01.
    let origin = chrono::DateTime::from_timestamp_nanos(origin_ns);
    let origin_months = origin.year() * 12 + origin.month0() as i32;
    let t_months = dt.year() * 12 + dt.month0() as i32;
    let rel = t_months - origin_months;
    let bucket_start = origin_months + floor_div(rel as i64, months as i64) as i32 * months;
    let (y, m0) = (bucket_start.div_euclid(12), bucket_start.rem_euclid(12));
    let date = NaiveDate::from_ymd_opt(y, (m0 + 1) as u32, 1)?;
    let ns = date.and_hms_opt(0, 0, 0)?.and_utc().timestamp_nanos_opt()?;
    Some(ns / unit_ns)
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TimeBucketUdf {
    signature: Signature,
}

impl Default for TimeBucketUdf {
    fn default() -> Self {
        Self {
            // (interval, ts) and (interval, ts, origin); validated in invoke.
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for TimeBucketUdf {
    fn name(&self) -> &str {
        "time_bucket"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DfResult<DataType> {
        if arg_types.len() < 2 || arg_types.len() > 3 {
            return Err(DataFusionError::Plan(
                "time_bucket(interval, timestamp [, origin]) takes 2 or 3 arguments".into(),
            ));
        }
        match &arg_types[1] {
            DataType::Timestamp(_, _) => Ok(arg_types[1].clone()),
            other => Err(DataFusionError::Plan(format!(
                "time_bucket: second argument must be a timestamp, got {other}"
            ))),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        if args.len() < 2 || args.len() > 3 {
            return Err(DataFusionError::Plan(
                "time_bucket(interval, timestamp [, origin]) takes 2 or 3 arguments".into(),
            ));
        }
        let width = match &args[0] {
            ColumnarValue::Scalar(s) => parse_interval_scalar(s)?,
            ColumnarValue::Array(_) => {
                return Err(DataFusionError::Plan(
                    "time_bucket: interval must be a literal, not a column".into(),
                ))
            }
        };
        let origin_ns = match args.get(2) {
            None => match width {
                Width::Nanos(_) => DEFAULT_ORIGIN_SECS * 1_000_000_000,
                Width::Months(_) => NaiveDate::from_ymd_opt(MONTH_ORIGIN_YEAR, 1, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap()
                    .and_utc()
                    .timestamp_nanos_opt()
                    .unwrap(),
            },
            Some(ColumnarValue::Scalar(s)) => scalar_ts_to_ns(s)?,
            Some(ColumnarValue::Array(_)) => {
                return Err(DataFusionError::Plan(
                    "time_bucket: origin must be a literal".into(),
                ))
            }
        };

        let array = match &args[1] {
            ColumnarValue::Array(a) => a.clone(),
            ColumnarValue::Scalar(s) => s.to_array()?,
        };
        let DataType::Timestamp(unit, _) = array.data_type().clone() else {
            return Err(DataFusionError::Plan(
                "time_bucket: second argument must be a timestamp".into(),
            ));
        };
        let uf = unit_factor(&unit);
        let bucket = |v: i64| -> Option<i64> {
            match width {
                Width::Nanos(w) => bucket_fixed(v, uf, w, origin_ns),
                Width::Months(m) => bucket_months(v, uf, m, origin_ns),
            }
        };

        macro_rules! bucket_array {
            ($ty:ty) => {{
                let typed = array
                    .as_any()
                    .downcast_ref::<PrimitiveArray<$ty>>()
                    .expect("checked timestamp type");
                let out: PrimitiveArray<$ty> = typed
                    .iter()
                    .map(|v| v.and_then(bucket))
                    .collect::<PrimitiveArray<$ty>>()
                    .with_data_type(array.data_type().clone());
                Arc::new(out) as ArrayRef
            }};
        }
        let out: ArrayRef = match unit {
            TimeUnit::Second => bucket_array!(TimestampSecondType),
            TimeUnit::Millisecond => bucket_array!(TimestampMillisecondType),
            TimeUnit::Microsecond => bucket_array!(TimestampMicrosecondType),
            TimeUnit::Nanosecond => bucket_array!(TimestampNanosecondType),
        };
        Ok(ColumnarValue::Array(out))
    }
}

fn scalar_ts_to_ns(s: &ScalarValue) -> DfResult<i64> {
    let (v, factor) = match s {
        ScalarValue::TimestampSecond(Some(v), _) => (*v, 1_000_000_000),
        ScalarValue::TimestampMillisecond(Some(v), _) => (*v, 1_000_000),
        ScalarValue::TimestampMicrosecond(Some(v), _) => (*v, 1_000),
        ScalarValue::TimestampNanosecond(Some(v), _) => (*v, 1),
        ScalarValue::Utf8(Some(s)) => {
            let dt = chrono::DateTime::parse_from_rfc3339(s).map_err(|e| {
                DataFusionError::Plan(format!("time_bucket: bad origin timestamp {s:?}: {e}"))
            })?;
            (
                dt.timestamp_nanos_opt().ok_or_else(|| {
                    DataFusionError::Plan("time_bucket: origin out of range".into())
                })?,
                1,
            )
        }
        other => {
            return Err(DataFusionError::Plan(format!(
                "time_bucket: unsupported origin {other:?}"
            )))
        }
    };
    v.checked_mul(factor).ok_or_else(|| {
        DataFusionError::Plan("time_bucket: origin out of representable range".into())
    })
}

pub fn time_bucket_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(TimeBucketUdf::default())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_buckets_align_to_monday_origin() {
        // 1h buckets: any ts is floored to the hour (origin is midnight).
        let one_h = 3_600_000_000_000;
        let t = DEFAULT_ORIGIN_SECS * 1_000_000_000 + one_h + 1234;
        assert_eq!(
            bucket_fixed(t, 1, one_h, DEFAULT_ORIGIN_SECS * 1_000_000_000),
            Some(DEFAULT_ORIGIN_SECS * 1_000_000_000 + one_h)
        );
        // Weekly buckets start on Monday 2000-01-03.
        let one_w = 7 * 86_400_000_000_000;
        let wed = DEFAULT_ORIGIN_SECS * 1_000_000_000 + 2 * 86_400_000_000_000;
        assert_eq!(
            bucket_fixed(wed, 1, one_w, DEFAULT_ORIGIN_SECS * 1_000_000_000),
            Some(DEFAULT_ORIGIN_SECS * 1_000_000_000)
        );
        // Pre-origin timestamps floor correctly (negative offsets).
        let before = DEFAULT_ORIGIN_SECS * 1_000_000_000 - 1;
        assert_eq!(
            bucket_fixed(before, 1, one_w, DEFAULT_ORIGIN_SECS * 1_000_000_000),
            Some(DEFAULT_ORIGIN_SECS * 1_000_000_000 - one_w)
        );
    }

    #[test]
    fn month_buckets_are_calendar_aligned() {
        let origin = NaiveDate::from_ymd_opt(2000, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_nanos_opt()
            .unwrap();
        // 2026-07-22 → month bucket 2026-07-01.
        let t = NaiveDate::from_ymd_opt(2026, 7, 22)
            .unwrap()
            .and_hms_opt(13, 45, 0)
            .unwrap()
            .and_utc()
            .timestamp_nanos_opt()
            .unwrap();
        let expect = NaiveDate::from_ymd_opt(2026, 7, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_nanos_opt()
            .unwrap();
        assert_eq!(bucket_months(t, 1, 1, origin), Some(expect));
        // Quarterly bucket → 2026-07 (Q3).
        assert_eq!(bucket_months(t, 1, 3, origin), Some(expect));
    }

    #[test]
    fn interval_strings() {
        assert_eq!(
            parse_interval_str("5m").unwrap(),
            Width::Nanos(300_000_000_000)
        );
        assert_eq!(
            parse_interval_str("1 hour").unwrap(),
            Width::Nanos(3_600_000_000_000)
        );
        assert_eq!(
            parse_interval_str("2 weeks").unwrap(),
            Width::Nanos(2 * 7 * 86_400_000_000_000)
        );
        assert_eq!(parse_interval_str("1mo").unwrap(), Width::Months(1));
        assert_eq!(parse_interval_str("2y").unwrap(), Width::Months(24));
        assert!(parse_interval_str("h").is_err());
    }
}
