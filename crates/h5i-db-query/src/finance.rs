//! Finance-oriented aggregates and window functions.
//!
//! Policy (DESIGN_CLAUDE.md, kdb+ lesson): the core ships fast *ordered
//! primitives* — weighted aggregates (`vwap`/`wavg`), exponentially weighted
//! moving averages (`ewma`) — that compose with the engine's own
//! `lag`/`stddev`/`covar`/`corr`/window frames into returns, realized
//! volatility, and rolling analytics. Model-heavy formulas (Black–Scholes,
//! curve bootstrapping, …) stay out of the engine.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::ScalarValue;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::function::{
    AccumulatorArgs, PartitionEvaluatorArgs, WindowUDFFieldArgs,
};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, PartitionEvaluator, Signature, Volatility,
    WindowUDF, WindowUDFImpl,
};

// ---------------------------------------------------------------------------
// vwap / wavg: weighted average aggregate
// ---------------------------------------------------------------------------

/// `vwap(price, weight)` / `wavg(weight, value)` — weighted mean as a
/// streaming, mergeable aggregate: state is (Σ value·weight, Σ weight).
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct WeightedAvgUdaf {
    signature: Signature,
    name: &'static str,
    /// vwap takes (value, weight); kdb-style wavg takes (weight, value).
    weight_first: bool,
}

impl WeightedAvgUdaf {
    pub fn vwap() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            name: "vwap",
            weight_first: false,
        }
    }

    pub fn wavg() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            name: "wavg",
            weight_first: true,
        }
    }
}

impl AggregateUDFImpl for WeightedAvgUdaf {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DfResult<DataType> {
        if arg_types.len() != 2 {
            return Err(DataFusionError::Plan(format!(
                "{}(a, b) takes exactly two numeric arguments",
                self.name
            )));
        }
        for t in arg_types {
            if !t.is_numeric() {
                return Err(DataFusionError::Plan(format!(
                    "{}: arguments must be numeric, got {t}",
                    self.name
                )));
            }
        }
        Ok(DataType::Float64)
    }

    fn accumulator(&self, _args: AccumulatorArgs) -> DfResult<Box<dyn Accumulator>> {
        Ok(Box::new(WeightedAvgAccumulator {
            sum_vw: 0.0,
            sum_w: 0.0,
            weight_first: self.weight_first,
        }))
    }

    fn state_fields(
        &self,
        args: datafusion::logical_expr::function::StateFieldsArgs,
    ) -> DfResult<Vec<FieldRef>> {
        Ok(vec![
            Arc::new(Field::new(
                format!("{}_sum_vw", args.name),
                DataType::Float64,
                true,
            )),
            Arc::new(Field::new(
                format!("{}_sum_w", args.name),
                DataType::Float64,
                true,
            )),
        ])
    }
}

#[derive(Debug)]
struct WeightedAvgAccumulator {
    sum_vw: f64,
    sum_w: f64,
    weight_first: bool,
}

fn to_f64_array(array: &ArrayRef) -> DfResult<Float64Array> {
    Ok(arrow::compute::cast(array, &DataType::Float64)?
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("cast to f64")
        .clone())
}

impl Accumulator for WeightedAvgAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DfResult<()> {
        let (v_idx, w_idx) = if self.weight_first { (1, 0) } else { (0, 1) };
        let v = to_f64_array(&values[v_idx])?;
        let w = to_f64_array(&values[w_idx])?;
        for i in 0..v.len() {
            if v.is_valid(i) && w.is_valid(i) {
                self.sum_vw += v.value(i) * w.value(i);
                self.sum_w += w.value(i);
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> DfResult<ScalarValue> {
        if self.sum_w == 0.0 {
            Ok(ScalarValue::Float64(None))
        } else {
            Ok(ScalarValue::Float64(Some(self.sum_vw / self.sum_w)))
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }

    fn state(&mut self) -> DfResult<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::Float64(Some(self.sum_vw)),
            ScalarValue::Float64(Some(self.sum_w)),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DfResult<()> {
        let vw = to_f64_array(&states[0])?;
        let w = to_f64_array(&states[1])?;
        for i in 0..vw.len() {
            if vw.is_valid(i) {
                self.sum_vw += vw.value(i);
            }
            if w.is_valid(i) {
                self.sum_w += w.value(i);
            }
        }
        Ok(())
    }
}

pub fn vwap_udaf() -> AggregateUDF {
    AggregateUDF::new_from_impl(WeightedAvgUdaf::vwap())
}

pub fn wavg_udaf() -> AggregateUDF {
    AggregateUDF::new_from_impl(WeightedAvgUdaf::wavg())
}

// ---------------------------------------------------------------------------
// ewma: exponentially weighted moving average (window function)
// ---------------------------------------------------------------------------

/// `ewma(value, alpha) OVER (PARTITION BY … ORDER BY ts)`:
/// `y_0 = x_0; y_i = alpha·x_i + (1-alpha)·y_{i-1}` — a single ordered pass
/// per partition. Nulls carry the previous smoothed value forward.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct EwmaUdwf {
    signature: Signature,
}

impl Default for EwmaUdwf {
    fn default() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl WindowUDFImpl for EwmaUdwf {
    fn name(&self) -> &str {
        "ewma"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn partition_evaluator(
        &self,
        _args: PartitionEvaluatorArgs,
    ) -> DfResult<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(EwmaEvaluator))
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> DfResult<FieldRef> {
        Ok(Arc::new(Field::new(
            field_args.name(),
            DataType::Float64,
            true,
        )))
    }
}

#[derive(Debug)]
struct EwmaEvaluator;

impl PartitionEvaluator for EwmaEvaluator {
    fn evaluate_all(&mut self, values: &[ArrayRef], num_rows: usize) -> DfResult<ArrayRef> {
        if values.len() != 2 {
            return Err(DataFusionError::Plan(
                "ewma(value, alpha) takes exactly two arguments".into(),
            ));
        }
        let x = to_f64_array(&values[0])?;
        let alpha_arr = to_f64_array(&values[1])?;
        if alpha_arr.is_empty() {
            return Ok(Arc::new(Float64Array::from(Vec::<f64>::new())));
        }
        let alpha = alpha_arr.value(0);
        if !(0.0..=1.0).contains(&alpha) {
            return Err(DataFusionError::Execution(format!(
                "ewma: alpha must be in [0, 1], got {alpha}"
            )));
        }
        let mut out = Vec::with_capacity(num_rows);
        let mut prev: Option<f64> = None;
        for i in 0..num_rows {
            if x.is_valid(i) {
                let xi = x.value(i);
                let yi = match prev {
                    None => xi,
                    Some(p) => alpha * xi + (1.0 - alpha) * p,
                };
                prev = Some(yi);
                out.push(Some(yi));
            } else {
                out.push(prev);
            }
        }
        Ok(Arc::new(Float64Array::from(out)))
    }

    fn uses_window_frame(&self) -> bool {
        false
    }

    fn include_rank(&self) -> bool {
        false
    }
}

pub fn ewma_udwf() -> WindowUDF {
    WindowUDF::new_from_impl(EwmaUdwf::default())
}
