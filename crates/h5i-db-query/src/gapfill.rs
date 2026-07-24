//! Gap filling for regular time-series grids.
//!
//! SQL entry point:
//! `gapfill('table', 'time_column', step [, 'null'|'locf'|'interpolate'|'value', <const>])`.
//!
//! Fill modes mirror QuestDB's `SAMPLE BY … FILL(...)`: `null` (`FILL(NULL)`),
//! `locf`/`prev` (`FILL(PREV)`), `interpolate`/`linear` (`FILL(LINEAR)`), and
//! `value` (`FILL(x)`) — a numeric constant applied to numeric columns (a
//! missing bar's volume becomes 0, etc.), null elsewhere. First/last per bucket
//! are DataFusion's `first_value`/`last_value` aggregates over `time_bucket`.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::catalog::{TableFunctionArgs, TableFunctionImpl, TableProvider};
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::Expr;
use datafusion::scalar::ScalarValue;
use h5i_db_core::{Database, ReadAt, ScanOptions};

use crate::udtf::block_on;

#[derive(Debug, Clone)]
enum FillMode {
    Null,
    Locf,
    Interpolate,
    /// Constant fill: numeric columns get this value (cast to their type),
    /// non-numeric columns get null.
    Value(ScalarValue),
}

#[derive(Debug)]
pub struct GapFillFunc {
    db: Arc<Database>,
}

impl GapFillFunc {
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }
}

fn string_arg(args: &[Expr], i: usize, what: &str) -> DfResult<String> {
    match args.get(i) {
        Some(Expr::Literal(ScalarValue::Utf8(Some(v)), _)) => Ok(v.clone()),
        _ => Err(DataFusionError::Plan(format!(
            "gapfill: argument {i} must be {what} (a string literal)"
        ))),
    }
}

fn null_scalar(dt: &DataType) -> ScalarValue {
    ScalarValue::try_from(dt).unwrap_or(ScalarValue::Null)
}

fn is_numeric_scalar(sv: &ScalarValue) -> bool {
    sv.data_type().is_numeric()
}

fn time_scalar(dt: &DataType, value: i64) -> DfResult<ScalarValue> {
    Ok(match dt {
        DataType::Int64 => ScalarValue::Int64(Some(value)),
        DataType::Timestamp(TimeUnit::Second, tz) => {
            ScalarValue::TimestampSecond(Some(value), tz.clone())
        }
        DataType::Timestamp(TimeUnit::Millisecond, tz) => {
            ScalarValue::TimestampMillisecond(Some(value), tz.clone())
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            ScalarValue::TimestampMicrosecond(Some(value), tz.clone())
        }
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
            ScalarValue::TimestampNanosecond(Some(value), tz.clone())
        }
        other => {
            return Err(DataFusionError::Plan(format!(
                "gapfill: time column must be Int64 or Timestamp, got {other}"
            )))
        }
    })
}

fn interpolate(a: &ScalarValue, b: &ScalarValue, ratio: f64) -> Option<ScalarValue> {
    macro_rules! interp {
        ($variant:ident, $ty:ty) => {
            match (a, b) {
                (ScalarValue::$variant(Some(x)), ScalarValue::$variant(Some(y))) => {
                    Some(ScalarValue::$variant(Some(
                        (*x as f64 + (*y as f64 - *x as f64) * ratio).round() as $ty,
                    )))
                }
                _ => None,
            }
        };
    }
    match (a, b) {
        (ScalarValue::Float32(Some(x)), ScalarValue::Float32(Some(y))) => {
            Some(ScalarValue::Float32(Some(*x + (*y - *x) * ratio as f32)))
        }
        (ScalarValue::Float64(Some(x)), ScalarValue::Float64(Some(y))) => {
            Some(ScalarValue::Float64(Some(*x + (*y - *x) * ratio)))
        }
        (ScalarValue::Int8(_), ScalarValue::Int8(_)) => interp!(Int8, i8),
        (ScalarValue::Int16(_), ScalarValue::Int16(_)) => interp!(Int16, i16),
        (ScalarValue::Int32(_), ScalarValue::Int32(_)) => interp!(Int32, i32),
        (ScalarValue::Int64(_), ScalarValue::Int64(_)) => interp!(Int64, i64),
        (ScalarValue::UInt8(_), ScalarValue::UInt8(_)) => interp!(UInt8, u8),
        (ScalarValue::UInt16(_), ScalarValue::UInt16(_)) => interp!(UInt16, u16),
        (ScalarValue::UInt32(_), ScalarValue::UInt32(_)) => interp!(UInt32, u32),
        (ScalarValue::UInt64(_), ScalarValue::UInt64(_)) => interp!(UInt64, u64),
        _ => None,
    }
}

fn build_gapfilled(
    schema: SchemaRef,
    batches: &[RecordBatch],
    time_col: &str,
    step: i64,
    mode: FillMode,
) -> DfResult<RecordBatch> {
    let combined = arrow::compute::concat_batches(&schema, batches)?;
    if combined.num_rows() == 0 {
        return Ok(RecordBatch::new_empty(schema));
    }
    let time_idx = schema.index_of(time_col)?;
    let times = h5i_db_core::segment::time_values_i64(&combined, time_col)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let mut rows_by_time = BTreeMap::new();
    for (row, time) in times.iter().copied().enumerate() {
        rows_by_time.insert(time, row);
    }
    let start = *rows_by_time.first_key_value().unwrap().0;
    let end = *rows_by_time.last_key_value().unwrap().0;
    let count = ((end - start) / step) as usize + 1;
    if count > 1_000_000 {
        return Err(DataFusionError::ResourcesExhausted(format!(
            "gapfill would generate {count} rows (limit 1000000)"
        )));
    }

    let mut columns: Vec<Vec<ScalarValue>> = schema
        .fields()
        .iter()
        .map(|_| Vec::with_capacity(count))
        .collect();
    for n in 0..count {
        let time = start + (n as i64) * step;
        if let Some(&row) = rows_by_time.get(&time) {
            for (col, out) in columns.iter_mut().enumerate() {
                out.push(ScalarValue::try_from_array(combined.column(col), row)?);
            }
            continue;
        }
        let prev = rows_by_time.range(..time).next_back().map(|(_, r)| *r);
        let next = rows_by_time.range(time..).next().map(|(_, r)| *r);
        for (col, out) in columns.iter_mut().enumerate() {
            let field = schema.field(col);
            if col == time_idx {
                out.push(time_scalar(field.data_type(), time)?);
                continue;
            }
            let value = match &mode {
                FillMode::Null => null_scalar(field.data_type()),
                FillMode::Value(c) => {
                    if field.data_type().is_numeric() {
                        c.cast_to(field.data_type())
                            .unwrap_or_else(|_| null_scalar(field.data_type()))
                    } else {
                        null_scalar(field.data_type())
                    }
                }
                FillMode::Locf => prev
                    .map(|r| ScalarValue::try_from_array(combined.column(col), r))
                    .transpose()?
                    .unwrap_or_else(|| null_scalar(field.data_type())),
                FillMode::Interpolate => match (prev, next) {
                    (Some(p), Some(q)) => {
                        let a = ScalarValue::try_from_array(combined.column(col), p)?;
                        let b = ScalarValue::try_from_array(combined.column(col), q)?;
                        let ratio = (time - times[p]) as f64 / (times[q] - times[p]) as f64;
                        interpolate(&a, &b, ratio).unwrap_or(a)
                    }
                    (Some(p), None) => ScalarValue::try_from_array(combined.column(col), p)?,
                    _ => null_scalar(field.data_type()),
                },
            };
            out.push(value);
        }
    }
    let arrays = columns
        .into_iter()
        .map(ScalarValue::iter_to_array)
        .collect::<DfResult<Vec<_>>>()?;
    RecordBatch::try_new(schema, arrays).map_err(DataFusionError::from)
}

impl TableFunctionImpl for GapFillFunc {
    fn call_with_args(&self, args: TableFunctionArgs) -> DfResult<Arc<dyn TableProvider>> {
        let args = args.exprs();
        if !(3..=5).contains(&args.len()) {
            return Err(DataFusionError::Plan(
                "gapfill('table', 'time_column', step \
                 [, 'null'|'locf'|'prev'|'interpolate'|'linear'|'value', <const>])"
                    .into(),
            ));
        }
        let table = string_arg(args, 0, "the table name")?;
        let time_col = string_arg(args, 1, "the time column")?;
        let step = match args.get(2) {
            Some(Expr::Literal(ScalarValue::Int64(Some(v)), _)) if *v > 0 => *v,
            _ => {
                return Err(DataFusionError::Plan(
                    "gapfill: step must be a positive Int64 literal".into(),
                ))
            }
        };
        let mode = match args.get(3) {
            None => FillMode::Null,
            Some(_) => match string_arg(args, 3, "a fill mode")?.as_str() {
                "null" => FillMode::Null,
                "locf" | "prev" => FillMode::Locf,
                "interpolate" | "linear" => FillMode::Interpolate,
                "value" => {
                    let c = match args.get(4) {
                        Some(Expr::Literal(sv, _)) if is_numeric_scalar(sv) => sv.clone(),
                        _ => {
                            return Err(DataFusionError::Plan(
                                "gapfill: 'value' fill needs a numeric constant, \
                                 e.g. gapfill('t', 'ts', step, 'value', 0)"
                                    .into(),
                            ))
                        }
                    };
                    FillMode::Value(c)
                }
                other => {
                    return Err(DataFusionError::Plan(format!(
                        "gapfill: unknown fill mode {other:?}"
                    )))
                }
            },
        };
        if !matches!(mode, FillMode::Value(_)) && args.len() == 5 {
            return Err(DataFusionError::Plan(
                "gapfill: a 5th argument (constant) is only valid with the 'value' fill mode"
                    .into(),
            ));
        }
        let (resolved, batches) = block_on(async {
            let resolved = self.db.resolve(&table, ReadAt::Latest).await?;
            let (batches, _) = self
                .db
                .scan_resolved(&resolved, ScanOptions::default())
                .await?;
            Ok::<_, h5i_db_core::Error>((resolved, batches))
        })
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
        if resolved.spec.time_column.as_deref() != Some(time_col.as_str()) {
            return Err(DataFusionError::Plan(format!(
                "gapfill: {time_col:?} is not the table's declared time column"
            )));
        }
        // Generated grid rows can contain nulls even when the stored schema is
        // non-nullable. The provider must advertise that honestly or MemTable
        // rejects a valid null-mode resample.
        let output_schema = Arc::new(Schema::new_with_metadata(
            resolved
                .schema
                .fields()
                .iter()
                .map(|field| {
                    if field.name() == &time_col {
                        field.as_ref().clone()
                    } else {
                        Field::new(field.name(), field.data_type().clone(), true)
                            .with_metadata(field.metadata().clone())
                    }
                })
                .collect::<Vec<_>>(),
            resolved.schema.metadata().clone(),
        ));
        let batch = build_gapfilled(output_schema.clone(), &batches, &time_col, step, mode)?;
        Ok(Arc::new(MemTable::try_new(
            output_schema,
            vec![vec![batch]],
        )?))
    }
}
