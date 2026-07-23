//! Time-series scalar functions: `time_bucket`.
//!
//! Semantics follow DuckDB/TimescaleDB: fixed-width buckets are aligned to
//! origin 2000-01-03 00:00:00 UTC (a Monday, so weekly buckets start on
//! Mondays); month/year widths use calendar bucketing from 2000-01-01.
//! An optional third argument supplies either an origin or an IANA timezone;
//! the four-argument form accepts both `(width, timestamp, origin, timezone)`.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, PrimitiveArray};
use arrow::datatypes::{
    DataType, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType,
};
use chrono::{DateTime, Datelike, Duration as ChronoDuration, NaiveDate, TimeZone, Utc};
use chrono_tz::Tz;
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

/// Validate a month-width: must be a positive whole number of months that
/// fits in i32. Zero would divide by zero at execution; negative widths are
/// meaningless for bucketing.
fn validate_months(months: i64) -> DfResult<i32> {
    if months <= 0 {
        return Err(DataFusionError::Plan(
            "time_bucket: month/year interval must be positive".into(),
        ));
    }
    i32::try_from(months).map_err(|_| {
        DataFusionError::Plan(format!(
            "time_bucket: month interval {months} out of range (max {})",
            i32::MAX
        ))
    })
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
                Ok(Width::Months(validate_months(i.months as i64)?))
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
        ScalarValue::IntervalYearMonth(Some(m)) => Ok(Width::Months(validate_months(*m as i64)?)),
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
    if !n.is_finite() || n <= 0.0 {
        return Err(DataFusionError::Plan(
            "time_bucket: interval must be positive".into(),
        ));
    }
    let unit = unit.trim();
    // Whole month count for calendar units; rejects "1.5mo" and overflow.
    let whole_months = |factor: i64| -> DfResult<Width> {
        if n.fract() != 0.0 {
            return Err(DataFusionError::Plan(format!(
                "time_bucket: fractional month/year intervals are not supported ({s:?})"
            )));
        }
        if n > i32::MAX as f64 {
            return Err(DataFusionError::Plan(format!(
                "time_bucket: interval {s:?} out of range"
            )));
        }
        let months = (n as i64).checked_mul(factor).ok_or_else(|| {
            DataFusionError::Plan(format!("time_bucket: interval {s:?} out of range"))
        })?;
        Ok(Width::Months(validate_months(months)?))
    };
    /// Nanoseconds per unit, or the month factor for calendar units.
    fn lookup_unit(unit: &str) -> Option<Result<i64, i64>> {
        Some(match unit {
            "ns" | "nanosecond" => Ok(1),
            "us" | "microsecond" => Ok(1_000),
            "ms" | "millisecond" => Ok(1_000_000),
            "s" | "sec" | "second" => Ok(1_000_000_000),
            "m" | "min" | "minute" => Ok(60_000_000_000),
            "h" | "hr" | "hour" => Ok(3_600_000_000_000),
            "d" | "day" => Ok(86_400_000_000_000),
            "w" | "week" => Ok(7 * 86_400_000_000_000),
            "mo" | "mon" | "month" => Err(1),
            "y" | "yr" | "year" => Err(12),
            _ => return None,
        })
    }
    // Exact unit first; only when unknown, retry with one trailing 's'
    // stripped to accept plurals ("seconds" → "second"). Stripping first
    // would corrupt units that themselves end in 's' ('s' → "", 'ms' →
    // minutes, 'us'/'ns' → unknown).
    let ns_per: i64 = match lookup_unit(unit)
        .or_else(|| {
            unit.strip_suffix('s')
                .filter(|u| !u.is_empty())
                .and_then(lookup_unit)
        })
        .ok_or_else(|| {
            DataFusionError::Plan(format!("time_bucket: unknown interval unit {unit:?}"))
        })? {
        Ok(ns) => ns,
        Err(month_factor) => return whole_months(month_factor),
    };
    let ns_f = n * ns_per as f64;
    if !ns_f.is_finite() || ns_f > i64::MAX as f64 {
        return Err(DataFusionError::Plan(format!(
            "time_bucket: interval {s:?} out of range"
        )));
    }
    let ns = ns_f as i64;
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
    // Parse paths validate months >= 1; guard anyway so no width can divide
    // by zero (panic = abort workspace-wide).
    if months <= 0 {
        return None;
    }
    let t_ns = value.checked_mul(unit_ns)?;
    let dt = chrono::DateTime::from_timestamp_nanos(t_ns);
    // Origin defines the month phase; default is 2000-01. All month math in
    // i64: chrono-representable dates keep these small, but a huge (validated,
    // positive) width must produce a null bucket, not an i32 overflow.
    let origin = chrono::DateTime::from_timestamp_nanos(origin_ns);
    let origin_months = origin.year() as i64 * 12 + origin.month0() as i64;
    let t_months = dt.year() as i64 * 12 + dt.month0() as i64;
    let rel = t_months - origin_months;
    let bucket_start = origin_months + floor_div(rel, months as i64) * months as i64;
    let (y, m0) = (bucket_start.div_euclid(12), bucket_start.rem_euclid(12));
    let date = NaiveDate::from_ymd_opt(i32::try_from(y).ok()?, (m0 + 1) as u32, 1)?;
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
        if arg_types.len() < 2 || arg_types.len() > 4 {
            return Err(DataFusionError::Plan(
                "time_bucket(interval, timestamp [, origin|timezone [, timezone]]) takes 2 to 4 arguments".into(),
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
        if args.len() < 2 || args.len() > 4 {
            return Err(DataFusionError::Plan(
                "time_bucket(interval, timestamp [, origin|timezone [, timezone]]) takes 2 to 4 arguments".into(),
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
        let third_is_timezone = args.len() == 3
            && matches!(
                &args[2],
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(value)))
                    if value.parse::<Tz>().is_ok() || value.contains('/')
            );
        let timezone = if args.len() == 4 || third_is_timezone {
            let index = if args.len() == 4 { 3 } else { 2 };
            Some(parse_timezone(&args[index])?)
        } else {
            None
        };
        let origin_arg = if args.len() == 4 || (args.len() == 3 && !third_is_timezone) {
            args.get(2)
        } else {
            None
        };
        let origin_ns = match origin_arg {
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
            match timezone {
                Some(tz) => bucket_in_timezone(v, uf, width, origin_ns, tz),
                None => match width {
                    Width::Nanos(w) => bucket_fixed(v, uf, w, origin_ns),
                    Width::Months(m) => bucket_months(v, uf, m, origin_ns),
                },
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

fn parse_timezone(value: &ColumnarValue) -> DfResult<Tz> {
    let ColumnarValue::Scalar(ScalarValue::Utf8(Some(value))) = value else {
        return Err(DataFusionError::Plan(
            "time_bucket: timezone must be an IANA timezone string literal".into(),
        ));
    };
    value
        .parse::<Tz>()
        .map_err(|_| DataFusionError::Plan(format!("time_bucket: unknown IANA timezone {value:?}")))
}

fn bucket_in_timezone(
    value: i64,
    unit_ns: i64,
    width: Width,
    origin_ns: i64,
    timezone: Tz,
) -> Option<i64> {
    let instant_ns = value.checked_mul(unit_ns)?;
    let instant = datetime_from_ns(instant_ns)?;
    let local = instant.with_timezone(&timezone).naive_local();
    let local_ns = local.and_utc().timestamp_nanos_opt()?;
    let bucket_local_ns = match width {
        Width::Nanos(width) => bucket_fixed(local_ns, 1, width, origin_ns),
        Width::Months(months) => bucket_months(local_ns, 1, months, origin_ns),
    }?;
    let bucket_local = datetime_from_ns(bucket_local_ns)?.naive_utc();
    // DST can make a local boundary ambiguous or nonexistent. Pick the first
    // occurrence when ambiguous and advance to the first valid instant for a
    // gap, matching common resampling behavior.
    let mut candidate = bucket_local;
    for _ in 0..=180 {
        if let Some(bucket) = timezone.from_local_datetime(&candidate).earliest() {
            return bucket.timestamp_nanos_opt()?.checked_div(unit_ns);
        }
        candidate = candidate.checked_add_signed(ChronoDuration::minutes(1))?;
    }
    None
}

fn datetime_from_ns(value: i64) -> Option<DateTime<Utc>> {
    DateTime::from_timestamp(
        value.div_euclid(1_000_000_000),
        value.rem_euclid(1_000_000_000) as u32,
    )
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

    /// Units whose spelling ends in 's' ('s', 'ms', 'us', 'ns') must parse as
    /// themselves — plural stripping may only apply when the exact unit is
    /// unknown ('seconds' → 'second'), never turn 'ms' into minutes or eat
    /// 's' entirely.
    #[test]
    fn interval_units_ending_in_s() {
        assert_eq!(
            parse_interval_str("5s").unwrap(),
            Width::Nanos(5_000_000_000)
        );
        assert_eq!(
            parse_interval_str("30s").unwrap(),
            Width::Nanos(30_000_000_000)
        );
        assert_eq!(
            parse_interval_str("250ms").unwrap(),
            Width::Nanos(250_000_000)
        );
        assert_eq!(parse_interval_str("10us").unwrap(), Width::Nanos(10_000));
        assert_eq!(parse_interval_str("100ns").unwrap(), Width::Nanos(100));
        // Plural spellings still work.
        assert_eq!(
            parse_interval_str("5 secs").unwrap(),
            Width::Nanos(5_000_000_000)
        );
        assert_eq!(
            parse_interval_str("30 seconds").unwrap(),
            Width::Nanos(30_000_000_000)
        );
        assert_eq!(
            parse_interval_str("2 mins").unwrap(),
            Width::Nanos(120_000_000_000)
        );
        assert_eq!(
            parse_interval_str("3 hrs").unwrap(),
            Width::Nanos(3 * 3_600_000_000_000)
        );
        assert_eq!(
            parse_interval_str("2 days").unwrap(),
            Width::Nanos(2 * 86_400_000_000_000)
        );
        assert_eq!(
            parse_interval_str("250 milliseconds").unwrap(),
            Width::Nanos(250_000_000)
        );
        assert_eq!(parse_interval_str("6 months").unwrap(), Width::Months(6));
        assert_eq!(parse_interval_str("2 years").unwrap(), Width::Months(24));
        // 'm' stays minutes; unknown units still error by their given name.
        assert_eq!(
            parse_interval_str("1m").unwrap(),
            Width::Nanos(60_000_000_000)
        );
        assert!(parse_interval_str("5 parsecs").is_err());
    }

    #[test]
    fn zero_negative_and_huge_intervals_error_not_panic() {
        // Zero-width month buckets used to divide by zero at execution.
        assert!(parse_interval_str("0mo").is_err());
        assert!(parse_interval_str("0y").is_err());
        assert!(parse_interval_str("0s").is_err());
        // Fractional months are silently-truncating nonsense; reject.
        assert!(parse_interval_str("1.5mo").is_err());
        // Huge widths used to wrap through `as i32`.
        assert!(parse_interval_str("999999999999y").is_err());
        assert!(parse_interval_str("99999999999999999h").is_err());

        // INTERVAL literal paths.
        use arrow::datatypes::IntervalMonthDayNano;
        let zero_mo = ScalarValue::IntervalMonthDayNano(Some(IntervalMonthDayNano::new(0, 0, 0)));
        assert!(parse_interval_scalar(&zero_mo).is_err());
        let neg_mo = ScalarValue::IntervalMonthDayNano(Some(IntervalMonthDayNano::new(-1, 0, 0)));
        assert!(parse_interval_scalar(&neg_mo).is_err());
        assert!(parse_interval_scalar(&ScalarValue::IntervalYearMonth(Some(0))).is_err());
        assert!(parse_interval_scalar(&ScalarValue::IntervalYearMonth(Some(-3))).is_err());
        assert_eq!(
            parse_interval_scalar(&ScalarValue::IntervalYearMonth(Some(2))).unwrap(),
            Width::Months(2)
        );
    }

    #[test]
    fn huge_month_width_yields_null_not_overflow() {
        // A validated-but-extreme width must degrade to a null bucket: one
        // second before the origin floors to a date far outside chrono range.
        assert_eq!(bucket_months(-1_000_000_000, 1, i32::MAX, 0), None);
        // Zero months (unreachable via parse) is a null bucket, never a panic.
        assert_eq!(bucket_months(0, 1, 0, 0), None);
    }
}
