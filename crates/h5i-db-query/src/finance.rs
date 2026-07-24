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

/// Neumaier-compensated summation step: adds `value` into the running
/// `sum` while accumulating the low-order bits lost to rounding in `c`. The
/// corrected total is `sum + c`. This is the Kahan–Babuška–Neumaier variant,
/// which stays accurate even when `value` is larger in magnitude than the
/// running `sum` (the case plain Kahan mishandles). Valid for negative
/// addends too, so sliding-window retraction is just `neumaier_add(-x)`.
///
/// Used by both the streaming `vwap`/`wavg` accumulator here and the
/// version-aware finance aggregate-state store, so warm (cached) and cold
/// (recomputed) rollups agree to the last representable bit.
#[inline]
pub(crate) fn neumaier_add(sum: &mut f64, c: &mut f64, value: f64) {
    let t = *sum + value;
    if sum.abs() >= value.abs() {
        *c += (*sum - t) + value;
    } else {
        *c += (value - t) + *sum;
    }
    *sum = t;
}

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
            c_vw: 0.0,
            c_w: 0.0,
            count: 0,
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
    /// Neumaier compensation terms for `sum_vw` / `sum_w`; the accurate totals
    /// are `sum_vw + c_vw` and `sum_w + c_w`. Kept in-memory only — emitted
    /// state folds them into the two f64 totals, so the on-wire/merge state
    /// format is unchanged.
    c_vw: f64,
    c_w: f64,
    /// Live (update − retract) contribution count: makes "window is empty"
    /// exact under retraction, where float cancellation can leave `sum_w`
    /// slightly off zero.
    count: u64,
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
                neumaier_add(&mut self.sum_vw, &mut self.c_vw, v.value(i) * w.value(i));
                neumaier_add(&mut self.sum_w, &mut self.c_w, w.value(i));
                self.count += 1;
            }
        }
        Ok(())
    }

    /// Sliding-window support: remove rows leaving the frame so rolling
    /// `vwap`/`wavg` are O(n) instead of re-accumulating O(n·w). Retraction is
    /// a compensated addition of the negated contribution.
    fn retract_batch(&mut self, values: &[ArrayRef]) -> DfResult<()> {
        let (v_idx, w_idx) = if self.weight_first { (1, 0) } else { (0, 1) };
        let v = to_f64_array(&values[v_idx])?;
        let w = to_f64_array(&values[w_idx])?;
        for i in 0..v.len() {
            if v.is_valid(i) && w.is_valid(i) {
                neumaier_add(&mut self.sum_vw, &mut self.c_vw, -(v.value(i) * w.value(i)));
                neumaier_add(&mut self.sum_w, &mut self.c_w, -w.value(i));
                self.count = self.count.saturating_sub(1);
            }
        }
        if self.count == 0 {
            // Empty frame: snap accumulated float error back to exact zero.
            self.sum_vw = 0.0;
            self.sum_w = 0.0;
            self.c_vw = 0.0;
            self.c_w = 0.0;
        }
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }

    fn evaluate(&mut self) -> DfResult<ScalarValue> {
        let sum_w = self.sum_w + self.c_w;
        if self.count == 0 || sum_w == 0.0 {
            Ok(ScalarValue::Float64(None))
        } else {
            Ok(ScalarValue::Float64(Some((self.sum_vw + self.c_vw) / sum_w)))
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }

    fn state(&mut self) -> DfResult<Vec<ScalarValue>> {
        // Fold the compensation into each total so the partial-aggregate state
        // stays two f64 (format unchanged) while carrying the corrected value.
        Ok(vec![
            ScalarValue::Float64(Some(self.sum_vw + self.c_vw)),
            ScalarValue::Float64(Some(self.sum_w + self.c_w)),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DfResult<()> {
        let vw = to_f64_array(&states[0])?;
        let w = to_f64_array(&states[1])?;
        for i in 0..vw.len() {
            if vw.is_valid(i) {
                neumaier_add(&mut self.sum_vw, &mut self.c_vw, vw.value(i));
                self.count += 1;
            }
            if w.is_valid(i) {
                neumaier_add(&mut self.sum_w, &mut self.c_w, w.value(i));
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Float64Array;

    fn vwap_acc() -> WeightedAvgAccumulator {
        WeightedAvgAccumulator {
            sum_vw: 0.0,
            sum_w: 0.0,
            c_vw: 0.0,
            c_w: 0.0,
            count: 0,
            weight_first: false,
        }
    }

    /// Control + proof: a big value followed by many ones stalls a naive f64
    /// sum at `big` (each `+= 1.0` rounds away), while Neumaier recovers every
    /// one. This is the drift `vwap`/`wavg` and the aggregate-state store would
    /// otherwise accumulate over millions of ticks.
    #[test]
    fn neumaier_add_recovers_bits_lost_by_naive_sum() {
        let big = 1e16_f64;
        let n_ones = 1_000_000usize;
        let mut naive = big;
        let (mut sum, mut c) = (big, 0.0);
        for _ in 0..n_ones {
            naive += 1.0;
            neumaier_add(&mut sum, &mut c, 1.0);
        }
        assert_eq!(naive, big, "control: naive sum must have lost the ones");
        assert_eq!(
            sum + c,
            big + n_ones as f64,
            "neumaier recovers every lost one"
        );
    }

    /// `vwap` over a full-mantissa dataset matches the high-precision reference;
    /// a naive accumulator would be off by ~1.0 here.
    #[test]
    fn vwap_long_sum_matches_high_precision_reference() {
        let big = 1e16_f64;
        let n_ones = 1_000_000usize;
        let mut prices = Vec::with_capacity(n_ones + 1);
        prices.push(big);
        prices.extend(std::iter::repeat_n(1.0_f64, n_ones));
        let weights = vec![1.0_f64; n_ones + 1];
        let pv: ArrayRef = Arc::new(Float64Array::from(prices));
        let wv: ArrayRef = Arc::new(Float64Array::from(weights));

        let mut acc = vwap_acc();
        acc.update_batch(&[pv, wv]).unwrap();
        let got = match acc.evaluate().unwrap() {
            ScalarValue::Float64(Some(x)) => x,
            other => panic!("expected f64, got {other:?}"),
        };

        // Weights are all 1, so vwap == mean(price) = (big + n_ones)/(n_ones+1).
        let reference = (big + n_ones as f64) / (n_ones as f64 + 1.0);
        let naive_would_be = big / (n_ones as f64 + 1.0);
        assert!(
            (got - reference).abs() <= 1e-3,
            "vwap {got} vs reference {reference}"
        );
        assert!(
            (got - naive_would_be).abs() > 0.5,
            "result must differ from the naive-sum answer"
        );
    }

    /// Sliding-window retraction (compensated) equals a fresh accumulation over
    /// the surviving rows.
    #[test]
    fn retract_matches_fresh_accumulation() {
        let prices: ArrayRef = Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0, 40.0]));
        let weights: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0]));
        let mut acc = vwap_acc();
        acc.update_batch(&[prices, weights]).unwrap();
        // Retract the first two rows.
        let r_p: ArrayRef = Arc::new(Float64Array::from(vec![10.0, 20.0]));
        let r_w: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 2.0]));
        acc.retract_batch(&[r_p, r_w]).unwrap();
        let got = match acc.evaluate().unwrap() {
            ScalarValue::Float64(Some(x)) => x,
            other => panic!("expected f64, got {other:?}"),
        };
        // Surviving rows (30,3),(40,4): (30*3 + 40*4)/(3+4) = 250/7.
        assert!((got - 250.0 / 7.0).abs() <= 1e-12, "got {got}");
    }
}
