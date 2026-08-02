//! Rolling time-series operators (ROADMAP Part VII-B1).
//!
//! # Scope discipline: implement the gap, not the vocabulary
//!
//! A source study of qlib's operator set (ROADMAP Part VII) found that ~20
//! operators cover its entire published factor zoo (Alpha158 + Alpha360) and
//! that DataFusion 54 already provides most of them. Reimplementing those
//! would be a defect, not a feature (the same rule Part IV applied to
//! `approx_distinct`/TopK: verify reachable, do not rebuild). Verified
//! reachable and therefore **not** implemented here:
//!
//! | qlib operator | reachable in h5i-db SQL as |
//! |---|---|
//! | `Mean` / `Sum` / `Count` | `avg` / `sum` / `count` over a window frame |
//! | `Std` / `Var` | `stddev` (`stddev_samp`) / `var_samp` |
//! | `Max` / `Min` | `max` / `min` |
//! | `Med` | `median` |
//! | `Quantile(x, N, q)` | `percentile_cont(q)` (exact) or `approx_percentile_cont` |
//! | `Corr` / `Cov` | `corr` / `covar_samp` — **expanding frames and `GROUP BY` only**; see `ts_corr`/`ts_cov` below for sliding frames |
//! | `Delta(x, N)` | `x - lag(x, N)` |
//! | `Ref(x, N)` | `lag(x, N)` (and `lead` for negative N) |
//! | `EMA` | `ewma` (this crate, [`crate::finance`]) |
//! | `WMA` | `wavg` (this crate) |
//! | `Slope` / `Rsquare` | `regr_slope` / `regr_r2` against a monotonic x |
//! | `Resi` | `x - (regr_slope(x,i)*i + regr_intercept(x,i))` |
//!
//! The regression family deserves a note, because it looks like it needs a
//! bespoke operator and does not. qlib regresses on the within-window
//! positions `1..N`, but OLS slope, R², and the *fitted value at a point* are
//! all invariant to translating x by a constant. Regressing against any
//! monotonic, evenly-spaced x therefore reproduces qlib's semantics exactly,
//! and `row_number() OVER (PARTITION BY … ORDER BY ts)` supplies one:
//!
//! ```sql
//! -- qlib BETA20 = Slope($close, 20) / $close
//! SELECT regr_slope(close, i) OVER w / close AS beta20,
//!        regr_r2(close, i)    OVER w         AS rsqr20,
//!        close - (regr_slope(close, i) OVER w * i
//!                 + regr_intercept(close, i) OVER w) AS resi20
//! FROM (SELECT *, row_number() OVER (PARTITION BY symbol ORDER BY ts) AS i
//!       FROM bars)
//! WINDOW w AS (PARTITION BY symbol ORDER BY ts ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
//! ```
//!
//! One row of that table came with a caveat found by testing rather than by
//! reading: on DataFusion 54.1 `corr`/`covar_samp` cannot be used over a
//! *sliding* frame at all, because neither accumulator opts into retraction.
//! Rolling correlation is required by Alpha158, so this module supplies
//! `ts_corr`/`ts_cov` (see [`PairRollingUdwf`]).
//!
//! # What is implemented here
//!
//! Six single-argument operators with no DataFusion equivalent, plus the two
//! sliding-capable pair statistics just described:
//!
//! - `idxmax(x)` / `idxmin(x)` — the **1-based position within the frame** of
//!   the first maximum / minimum. (qlib's `IdxMax`/`IdxMin`; Aroon's building
//!   block.) `max`/`min` give the value, never the position.
//! - `mad(x)` — mean absolute deviation about the frame mean.
//! - `skew(x)` / `kurt(x)` — sample skewness and **excess** kurtosis, both
//!   bias-corrected to match pandas/Excel exactly.
//! - `ts_rank(x)` — the percentile rank of the *current* row's value among the
//!   values in its own trailing frame. This is **not** `percent_rank`, which
//!   ranks a row within the whole partition; `ts_rank` is a rolling statistic
//!   whose value changes as the frame slides.
//!
//! All six are window functions requiring an `OVER` clause with a frame, which
//! is what makes them rolling. They return `Float64` (including the two
//! position operators, so that qlib's `IdxMax(x,d)/d` normalisation does not
//! silently integer-divide).
//!
//! # Cost
//!
//! Each is O(frame width) per row: they are genuinely two-pass statistics over
//! the frame (`mad`/`skew`/`kurt` need the mean first; `ts_rank` needs the
//! comparison set; the position operators must locate an extremum). No
//! incremental/retractable formulation is used, so a 20-wide frame does ~20
//! units of work per row. Per Part II invariant 7, optimise only on a measured
//! loss (ROADMAP D6 tracks the rolling-window benchmark); correctness first.
//! The input cast is cached per input array so evaluation is not O(n²).

use std::ops::Range;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::ScalarValue;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::function::{PartitionEvaluatorArgs, WindowUDFFieldArgs};
use datafusion::logical_expr::{
    PartitionEvaluator, Signature, Volatility, WindowUDF, WindowUDFImpl,
};

use crate::finance::to_f64_array;

/// Which rolling statistic a [`RollingUdwf`] computes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RollingKind {
    /// 1-based position of the first maximum within the frame.
    IdxMax,
    /// 1-based position of the first minimum within the frame.
    IdxMin,
    /// Mean absolute deviation about the frame mean.
    Mad,
    /// Bias-corrected sample skewness (pandas/Excel `SKEW`).
    Skew,
    /// Bias-corrected sample **excess** kurtosis (pandas/Excel `KURT`).
    Kurt,
    /// Percentile rank of the current row's value within its own frame.
    TsRank,
}

impl RollingKind {
    const fn name(self) -> &'static str {
        match self {
            RollingKind::IdxMax => "idxmax",
            RollingKind::IdxMin => "idxmin",
            RollingKind::Mad => "mad",
            RollingKind::Skew => "skew",
            RollingKind::Kurt => "kurt",
            RollingKind::TsRank => "ts_rank",
        }
    }

    /// Minimum number of valid values in the frame for a defined result.
    /// Below it the operator yields NULL rather than an error, matching
    /// pandas' `NaN` for short windows (qlib rejects `N<3`/`N<4` at
    /// construction time; a window frame's width is not known to the operator,
    /// so the check necessarily happens per frame).
    const fn min_valid(self) -> usize {
        match self {
            RollingKind::Skew => 3,
            RollingKind::Kurt => 4,
            _ => 1,
        }
    }
}

/// A rolling window operator over a single numeric argument.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct RollingUdwf {
    signature: Signature,
    kind: RollingKind,
}

impl RollingUdwf {
    pub fn new(kind: RollingKind) -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            kind,
        }
    }
}

impl WindowUDFImpl for RollingUdwf {
    fn name(&self) -> &str {
        self.kind.name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn partition_evaluator(
        &self,
        _args: PartitionEvaluatorArgs,
    ) -> DfResult<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(RollingEvaluator {
            kind: self.kind,
            cached: None,
        }))
    }

    /// Arity and type validation happens here, at planning time. It cannot
    /// live in `PartitionEvaluator::evaluate`: when `uses_window_frame` is set,
    /// DataFusion passes the ordering columns alongside the function's own
    /// arguments, so the slice length there is not the user-visible arity.
    fn field(&self, field_args: WindowUDFFieldArgs) -> DfResult<FieldRef> {
        let inputs = field_args.input_fields();
        if inputs.len() != 1 {
            return Err(DataFusionError::Plan(format!(
                "{}(value) takes exactly one argument, got {}",
                self.kind.name(),
                inputs.len()
            )));
        }
        let dt = inputs[0].data_type();
        if !dt.is_numeric() && !matches!(dt, DataType::Null) {
            return Err(DataFusionError::Plan(format!(
                "{}: argument must be numeric, got {dt}",
                self.kind.name()
            )));
        }
        Ok(Arc::new(Field::new(
            field_args.name(),
            DataType::Float64,
            true,
        )))
    }
}

#[derive(Debug)]
struct RollingEvaluator {
    kind: RollingKind,
    /// Cast-once cache: `evaluate` is called per row with the same input
    /// array, so casting on every call would make a partition scan O(n²).
    ///
    /// The cache **holds the input `ArrayRef`** rather than remembering its
    /// address. Identity by raw pointer alone is unsound here: once the
    /// original array is dropped the allocator is free to hand the same address
    /// to a different array, and the cache would then answer with the previous
    /// column's values, silently. Keeping the `Arc` alive makes the address
    /// unrecyclable for as long as it is used as the key, which is what turns
    /// `Arc::ptr_eq` into a real identity test.
    cached: Option<(ArrayRef, Float64Array)>,
}

impl RollingEvaluator {
    fn values(&mut self, input: &ArrayRef) -> DfResult<&Float64Array> {
        let stale = match &self.cached {
            Some((cached_input, _)) => !Arc::ptr_eq(cached_input, input),
            None => true,
        };
        if stale {
            self.cached = Some((input.clone(), to_f64_array(input)?));
        }
        Ok(&self.cached.as_ref().expect("just populated").1)
    }
}

impl PartitionEvaluator for RollingEvaluator {
    fn uses_window_frame(&self) -> bool {
        true
    }

    fn evaluate(&mut self, values: &[ArrayRef], range: &Range<usize>) -> DfResult<ScalarValue> {
        // `values[0]` is the function's argument; DataFusion may append the
        // window's ordering columns after it, so only emptiness is checked
        // here (real arity validation is in `RollingUdwf::field`).
        if values.is_empty() {
            return Err(DataFusionError::Execution(format!(
                "{}: no input column supplied",
                self.kind.name()
            )));
        }
        let kind = self.kind;
        let x = self.values(&values[0])?;
        Ok(ScalarValue::Float64(evaluate_frame(kind, x, range)))
    }
}

/// Compute one rolling statistic over `x[range]`. Split out from the
/// evaluator so the numerics are unit-testable without a DataFusion session.
fn evaluate_frame(kind: RollingKind, x: &Float64Array, range: &Range<usize>) -> Option<f64> {
    if range.start >= range.end {
        return None;
    }

    // Positional operators need frame offsets, so they walk the raw frame
    // rather than a compacted value list. Nulls are skipped when locating the
    // extremum, but positions are counted over every row in the frame (a null
    // row still occupies a position), which is what makes `idxmax(x)/width`
    // comparable across frames.
    if matches!(kind, RollingKind::IdxMax | RollingKind::IdxMin) {
        let mut best: Option<(f64, usize)> = None;
        for i in range.clone() {
            if !x.is_valid(i) {
                continue;
            }
            let v = x.value(i);
            if v.is_nan() {
                continue;
            }
            let better = match best {
                None => true,
                // Strict comparison keeps the *first* extremum on ties, as
                // pandas' argmax/argmin do.
                Some((b, _)) => match kind {
                    RollingKind::IdxMax => v > b,
                    _ => v < b,
                },
            };
            if better {
                best = Some((v, i - range.start + 1));
            }
        }
        return best.map(|(_, pos)| pos as f64);
    }

    // `ts_rank` ranks the current row's value, which is the frame's last row
    // (frames here end at CURRENT ROW). A null/NaN current value has no rank.
    if kind == RollingKind::TsRank {
        let last = range.end - 1;
        if !x.is_valid(last) {
            return None;
        }
        let current = x.value(last);
        if current.is_nan() {
            return None;
        }
        let mut n = 0usize;
        let mut less = 0usize;
        let mut equal = 0usize;
        for i in range.clone() {
            if !x.is_valid(i) {
                continue;
            }
            let v = x.value(i);
            if v.is_nan() {
                continue;
            }
            n += 1;
            if v < current {
                less += 1;
            } else if v == current {
                equal += 1;
            }
        }
        if n == 0 {
            return None;
        }
        // pandas `rank(pct=True)` with the default `method="average"`: tied
        // values share the mean of the ranks they span, then divide by n.
        let avg_rank = less as f64 + (equal as f64 + 1.0) / 2.0;
        return Some(avg_rank / n as f64);
    }

    // Moment-based statistics: collect valid values, then two passes.
    let mut vals: Vec<f64> = Vec::with_capacity(range.len());
    for i in range.clone() {
        if x.is_valid(i) {
            let v = x.value(i);
            if !v.is_nan() {
                vals.push(v);
            }
        }
    }
    let n = vals.len();
    if n < kind.min_valid() {
        return None;
    }
    let nf = n as f64;
    let mean = vals.iter().sum::<f64>() / nf;

    match kind {
        RollingKind::Mad => Some(vals.iter().map(|v| (v - mean).abs()).sum::<f64>() / nf),
        RollingKind::Skew | RollingKind::Kurt => {
            // Sample standard deviation (ddof = 1), as pandas uses.
            let ss = vals.iter().map(|v| (v - mean) * (v - mean)).sum::<f64>();
            let var = ss / (nf - 1.0);
            if var <= 0.0 {
                // A constant frame has no defined shape; pandas yields NaN.
                return None;
            }
            let sd = var.sqrt();
            if kind == RollingKind::Skew {
                // pandas/Excel SKEW: n/((n-1)(n-2)) · Σ((x-x̄)/s)³
                let m3: f64 = vals.iter().map(|v| ((v - mean) / sd).powi(3)).sum();
                Some(nf / ((nf - 1.0) * (nf - 2.0)) * m3)
            } else {
                // pandas/Excel KURT (excess):
                //   n(n+1)/((n-1)(n-2)(n-3)) · Σ((x-x̄)/s)⁴
                //     − 3(n-1)²/((n-2)(n-3))
                let m4: f64 = vals.iter().map(|v| ((v - mean) / sd).powi(4)).sum();
                let lead = nf * (nf + 1.0) / ((nf - 1.0) * (nf - 2.0) * (nf - 3.0));
                let tail = 3.0 * (nf - 1.0) * (nf - 1.0) / ((nf - 2.0) * (nf - 3.0));
                Some(lead * m4 - tail)
            }
        }
        // Handled above.
        RollingKind::IdxMax | RollingKind::IdxMin | RollingKind::TsRank => unreachable!(),
    }
}

// ---------------------------------------------------------------------------
// Two-argument rolling statistics: ts_corr / ts_cov
// ---------------------------------------------------------------------------

/// Which pairwise rolling statistic a [`PairRollingUdwf`] computes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PairRollingKind {
    /// Pearson correlation over the frame.
    Corr,
    /// Sample covariance (ddof = 1) over the frame.
    Cov,
}

impl PairRollingKind {
    const fn name(self) -> &'static str {
        match self {
            PairRollingKind::Corr => "ts_corr",
            PairRollingKind::Cov => "ts_cov",
        }
    }
}

/// `ts_corr(x, y)` / `ts_cov(x, y)` — rolling Pearson correlation and sample
/// covariance over a window frame.
///
/// # Why these exist when `corr` and `covar_samp` already do
///
/// DataFusion's `corr`/`covar_samp` are *aggregates*. Using an aggregate over
/// a **sliding** frame (`ROWS BETWEEN n PRECEDING AND CURRENT ROW`) requires it
/// to support retraction, and in DataFusion 54.1 — the version this workspace
/// links — neither overrides `supports_retract_batch`, so such a query fails
/// with "Aggregate can not be used as a sliding accumulator". They remain
/// perfectly usable for `GROUP BY` and for expanding frames
/// (`UNBOUNDED PRECEDING`), and `tests/quant_rolling.rs` pins both facts.
///
/// Rolling correlation is not optional for this project: it is qlib's `Corr`,
/// and Alpha158 uses it directly (`CORR{d}`, `CORD{d}`), so Part VII-B3's
/// conformance corpus cannot run without it.
///
/// Upstream has since added retraction to both accumulators, so on a future
/// DataFusion upgrade the built-ins will slide too. These operators stay
/// worthwhile regardless: they are frame-based two-pass computations, so they
/// carry no retraction-drift risk at all, which for a correlation over
/// millions of bars is a real numerical difference rather than a stylistic one.
///
/// # Semantics
///
/// A row contributes only when **both** arguments are non-null and non-NaN
/// (pandas' pairwise rule). Fewer than two contributing rows yields NULL.
/// `ts_corr` yields NULL when either side is constant over the frame, because
/// correlation is genuinely undefined there — note this differs from qlib,
/// which masks on a near-zero standard-deviation tolerance (`atol 2e-5`);
/// exact-zero variance is the defensible line, and a near-constant window
/// produces a real, if noisy, correlation.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PairRollingUdwf {
    signature: Signature,
    kind: PairRollingKind,
}

impl PairRollingUdwf {
    pub fn new(kind: PairRollingKind) -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            kind,
        }
    }
}

impl WindowUDFImpl for PairRollingUdwf {
    fn name(&self) -> &str {
        self.kind.name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn partition_evaluator(
        &self,
        _args: PartitionEvaluatorArgs,
    ) -> DfResult<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(PairRollingEvaluator {
            kind: self.kind,
            cached: None,
        }))
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> DfResult<FieldRef> {
        let inputs = field_args.input_fields();
        if inputs.len() != 2 {
            return Err(DataFusionError::Plan(format!(
                "{}(x, y) takes exactly two arguments, got {}",
                self.kind.name(),
                inputs.len()
            )));
        }
        for f in inputs {
            let dt = f.data_type();
            if !dt.is_numeric() && !matches!(dt, DataType::Null) {
                return Err(DataFusionError::Plan(format!(
                    "{}: arguments must be numeric, got {dt}",
                    self.kind.name()
                )));
            }
        }
        Ok(Arc::new(Field::new(
            field_args.name(),
            DataType::Float64,
            true,
        )))
    }
}

#[derive(Debug)]
struct PairRollingEvaluator {
    kind: PairRollingKind,
    /// Cast-once cache for both inputs, holding them alive as their own keys;
    /// see [`RollingEvaluator::values`] for why the arrays and not their
    /// addresses.
    cached: Option<(ArrayRef, ArrayRef, Float64Array, Float64Array)>,
}

impl PairRollingEvaluator {
    fn values(&mut self, x: &ArrayRef, y: &ArrayRef) -> DfResult<(&Float64Array, &Float64Array)> {
        let stale = match &self.cached {
            Some((cx, cy, _, _)) => !Arc::ptr_eq(cx, x) || !Arc::ptr_eq(cy, y),
            None => true,
        };
        if stale {
            self.cached = Some((x.clone(), y.clone(), to_f64_array(x)?, to_f64_array(y)?));
        }
        let (_, _, xa, ya) = self.cached.as_ref().expect("just populated");
        Ok((xa, ya))
    }
}

impl PartitionEvaluator for PairRollingEvaluator {
    fn uses_window_frame(&self) -> bool {
        true
    }

    fn evaluate(&mut self, values: &[ArrayRef], range: &Range<usize>) -> DfResult<ScalarValue> {
        if values.len() < 2 {
            return Err(DataFusionError::Execution(format!(
                "{}: expected two input columns",
                self.kind.name()
            )));
        }
        let kind = self.kind;
        let (x, y) = self.values(&values[0], &values[1])?;
        Ok(ScalarValue::Float64(evaluate_pair_frame(kind, x, y, range)))
    }
}

/// Compute one pairwise rolling statistic over `x[range]`, `y[range]`.
fn evaluate_pair_frame(
    kind: PairRollingKind,
    x: &Float64Array,
    y: &Float64Array,
    range: &Range<usize>,
) -> Option<f64> {
    if range.start >= range.end {
        return None;
    }
    let mut xs: Vec<f64> = Vec::with_capacity(range.len());
    let mut ys: Vec<f64> = Vec::with_capacity(range.len());
    for i in range.clone() {
        if !x.is_valid(i) || !y.is_valid(i) {
            continue;
        }
        let (xv, yv) = (x.value(i), y.value(i));
        if xv.is_nan() || yv.is_nan() {
            continue;
        }
        xs.push(xv);
        ys.push(yv);
    }
    let n = xs.len();
    // Sample statistics need two degrees of freedom.
    if n < 2 {
        return None;
    }
    let nf = n as f64;
    let mx = xs.iter().sum::<f64>() / nf;
    let my = ys.iter().sum::<f64>() / nf;
    let mut sxx = 0.0;
    let mut syy = 0.0;
    let mut sxy = 0.0;
    for i in 0..n {
        let dx = xs[i] - mx;
        let dy = ys[i] - my;
        sxx += dx * dx;
        syy += dy * dy;
        sxy += dx * dy;
    }
    match kind {
        PairRollingKind::Cov => Some(sxy / (nf - 1.0)),
        PairRollingKind::Corr => {
            if sxx <= 0.0 || syy <= 0.0 {
                // A constant series has no correlation with anything.
                None
            } else {
                // Equivalent to cov/(sd_x·sd_y); the (n-1) factors cancel, so
                // this form saves two divisions and a square root. It is the
                // *wider* intermediate, not the narrower one: `sxx · syy` can
                // reach infinity where `sqrt(sxx) · sqrt(syy)` would not, and
                // the result would then be 0 rather than the true correlation.
                // That needs |sxx·syy| > 1e308, i.e. sums of squares around
                // 1e154, which no price or return series reaches.
                Some(sxy / (sxx * syy).sqrt())
            }
        }
    }
}

/// Every rolling operator this module adds, for registration.
pub fn rolling_udwfs() -> Vec<WindowUDF> {
    let single = [
        RollingKind::IdxMax,
        RollingKind::IdxMin,
        RollingKind::Mad,
        RollingKind::Skew,
        RollingKind::Kurt,
        RollingKind::TsRank,
    ]
    .into_iter()
    .map(|kind| WindowUDF::new_from_impl(RollingUdwf::new(kind)));

    let pair = [PairRollingKind::Corr, PairRollingKind::Cov]
        .into_iter()
        .map(|kind| WindowUDF::new_from_impl(PairRollingUdwf::new(kind)));

    single.chain(pair).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arr(values: Vec<Option<f64>>) -> Float64Array {
        Float64Array::from(values)
    }

    fn eval(kind: RollingKind, values: &[f64]) -> Option<f64> {
        let a = arr(values.iter().copied().map(Some).collect());
        evaluate_frame(kind, &a, &(0..values.len()))
    }

    #[test]
    fn idxmax_and_idxmin_are_one_based_positions_within_the_frame() {
        let v = [3.0, 7.0, 5.0, 1.0];
        assert_eq!(eval(RollingKind::IdxMax, &v), Some(2.0));
        assert_eq!(eval(RollingKind::IdxMin, &v), Some(4.0));
    }

    /// pandas' argmax/argmin report the *first* extremum; ties must not drift
    /// to the last one, or `IMXD`-style differences change sign.
    #[test]
    fn positional_operators_keep_the_first_extremum_on_ties() {
        let v = [5.0, 2.0, 5.0, 2.0];
        assert_eq!(eval(RollingKind::IdxMax, &v), Some(1.0));
        assert_eq!(eval(RollingKind::IdxMin, &v), Some(2.0));
    }

    /// Positions count every row in the frame, including null rows, so
    /// `idxmax(x)/width` stays comparable across frames.
    #[test]
    fn positions_count_null_rows_but_skip_them_when_ranking() {
        let a = arr(vec![Some(1.0), None, Some(9.0), Some(4.0)]);
        assert_eq!(evaluate_frame(RollingKind::IdxMax, &a, &(0..4)), Some(3.0));
    }

    #[test]
    fn positional_operators_are_relative_to_the_frame_not_the_partition() {
        let a = arr(vec![Some(9.0), Some(1.0), Some(2.0), Some(8.0)]);
        // Frame [1..4) is (1.0, 2.0, 8.0): the max sits at frame position 3.
        assert_eq!(evaluate_frame(RollingKind::IdxMax, &a, &(1..4)), Some(3.0));
    }

    #[test]
    fn mad_is_mean_absolute_deviation_about_the_mean() {
        // mean = 4; deviations 3,1,1,3 → 8/4 = 2.
        let got = eval(RollingKind::Mad, &[1.0, 3.0, 5.0, 7.0]).unwrap();
        assert!((got - 2.0).abs() < 1e-12, "got {got}");
    }

    /// Reference values for the pandas/Excel bias-corrected conventions, so a
    /// refactor cannot quietly switch to the uncorrected population moment.
    ///
    /// The expected skew is cross-checked by a second, independent formula —
    /// `√(n(n-1))/(n-2) · m3/m2^1.5` — which agrees to the last bit
    /// (1.6970562748477143 vs 1.697056274847714). Both routes are documented
    /// because the first draft of this test carried a hand-arithmetic error
    /// that the implementation correctly contradicted.
    #[test]
    fn skew_matches_the_bias_corrected_reference() {
        // x = [1,2,3,4,10]: mean 4, SS 50, s = sqrt(12.5) = 3.53553390593.
        // Σ((x-x̄)/s)³ = (-27-8-1+0+216)/12.5^1.5 = 180/44.194173824 = 4.0729349
        // skew = 5/((4)(3)) · 4.0729349 = 1.69705627
        // Cross-check: √(5·4)/3 · (36/10^1.5) = 1.4907120 · 1.1384199 = 1.69705627
        let got = eval(RollingKind::Skew, &[1.0, 2.0, 3.0, 4.0, 10.0]).unwrap();
        assert!((got - 1.697_056_274_847_714).abs() < 1e-12, "got {got}");
    }

    #[test]
    fn kurt_matches_the_bias_corrected_excess_reference() {
        // Same sample. Σ((x-x̄)/s)⁴ = (81+16+1+0+1296)/156.25 = 8.9216
        // lead = 5·6/((4)(3)(2)) = 1.25 ; tail = 3·16/((3)(2)) = 8
        // kurt = 1.25·8.9216 − 8 = 3.152
        let got = eval(RollingKind::Kurt, &[1.0, 2.0, 3.0, 4.0, 10.0]).unwrap();
        assert!((got - 3.152).abs() < 1e-12, "got {got}");
    }

    /// A normal-ish symmetric sample must have ~zero skew; this catches a
    /// sign or centring error that the asymmetric fixture above would not.
    #[test]
    fn skew_of_a_symmetric_sample_is_zero() {
        let got = eval(RollingKind::Skew, &[1.0, 2.0, 3.0, 4.0, 5.0]).unwrap();
        assert!(got.abs() < 1e-12, "got {got}");
    }

    #[test]
    fn skew_needs_three_points_and_kurt_needs_four() {
        assert_eq!(eval(RollingKind::Skew, &[1.0, 2.0]), None);
        assert!(eval(RollingKind::Skew, &[1.0, 2.0, 4.0]).is_some());
        assert_eq!(eval(RollingKind::Kurt, &[1.0, 2.0, 4.0]), None);
        assert!(eval(RollingKind::Kurt, &[1.0, 2.0, 4.0, 8.0]).is_some());
    }

    /// A constant frame has zero variance: shape statistics are undefined
    /// (NULL), never a division-by-zero infinity or NaN leaking into results.
    #[test]
    fn shape_statistics_are_null_on_a_constant_frame() {
        assert_eq!(eval(RollingKind::Skew, &[2.0, 2.0, 2.0, 2.0]), None);
        assert_eq!(eval(RollingKind::Kurt, &[2.0, 2.0, 2.0, 2.0]), None);
        // ...but mad is well defined and zero.
        assert_eq!(eval(RollingKind::Mad, &[2.0, 2.0, 2.0]), Some(0.0));
    }

    #[test]
    fn ts_rank_ranks_the_current_value_within_its_own_frame() {
        // Current value 4 is the largest of 4 → pct rank 1.0.
        assert_eq!(eval(RollingKind::TsRank, &[1.0, 2.0, 3.0, 4.0]), Some(1.0));
        // Current value 1 is the smallest of 4 → 1/4.
        assert_eq!(eval(RollingKind::TsRank, &[4.0, 3.0, 2.0, 1.0]), Some(0.25));
    }

    /// pandas `rank(pct=True)` averages tied ranks; the current value tying
    /// with others must land mid-band, not at either edge.
    #[test]
    fn ts_rank_averages_ties_like_pandas() {
        // [1,2,2]: current 2 ties with one other. less=1, equal=2
        // avg rank = 1 + (2+1)/2 = 2.5 → 2.5/3.
        let got = eval(RollingKind::TsRank, &[1.0, 2.0, 2.0]).unwrap();
        assert!((got - 2.5 / 3.0).abs() < 1e-12, "got {got}");
    }

    #[test]
    fn ts_rank_is_null_when_the_current_value_is_missing() {
        let a = arr(vec![Some(1.0), Some(2.0), None]);
        assert_eq!(evaluate_frame(RollingKind::TsRank, &a, &(0..3)), None);
    }

    #[test]
    fn ts_rank_slides_with_the_frame() {
        // Same value (5.0) is top of one frame and bottom of the next.
        let a = arr(vec![Some(1.0), Some(5.0), Some(9.0)]);
        assert_eq!(evaluate_frame(RollingKind::TsRank, &a, &(0..2)), Some(1.0));
        assert_eq!(evaluate_frame(RollingKind::TsRank, &a, &(1..3)), Some(1.0));
        // Frame (5.0) alone: a lone value is its own 100th percentile.
        assert_eq!(evaluate_frame(RollingKind::TsRank, &a, &(1..2)), Some(1.0));
    }

    #[test]
    fn every_operator_is_null_on_an_empty_frame() {
        let a = arr(vec![Some(1.0)]);
        for kind in [
            RollingKind::IdxMax,
            RollingKind::IdxMin,
            RollingKind::Mad,
            RollingKind::Skew,
            RollingKind::Kurt,
            RollingKind::TsRank,
        ] {
            assert_eq!(
                evaluate_frame(kind, &a, &(0..0)),
                None,
                "{} must be NULL on an empty frame",
                kind.name()
            );
        }
    }

    #[test]
    fn all_null_frame_yields_null_not_a_spurious_zero() {
        let a = arr(vec![None, None, None]);
        for kind in [
            RollingKind::IdxMax,
            RollingKind::IdxMin,
            RollingKind::Mad,
            RollingKind::TsRank,
        ] {
            assert_eq!(
                evaluate_frame(kind, &a, &(0..3)),
                None,
                "{} must be NULL when every row is NULL",
                kind.name()
            );
        }
    }

    #[test]
    fn registration_exposes_every_operator_under_stable_names() {
        let names: Vec<String> = rolling_udwfs()
            .iter()
            .map(|f| f.name().to_string())
            .collect();
        assert_eq!(
            names,
            vec![
                "idxmax", "idxmin", "mad", "skew", "kurt", "ts_rank", "ts_corr", "ts_cov"
            ]
        );
    }

    // -----------------------------------------------------------------------
    // ts_corr / ts_cov
    // -----------------------------------------------------------------------

    fn eval_pair(kind: PairRollingKind, xs: &[f64], ys: &[f64]) -> Option<f64> {
        let xa = arr(xs.iter().copied().map(Some).collect());
        let ya = arr(ys.iter().copied().map(Some).collect());
        evaluate_pair_frame(kind, &xa, &ya, &(0..xs.len()))
    }

    #[test]
    fn ts_corr_is_one_for_a_perfect_positive_relationship() {
        let got = eval_pair(PairRollingKind::Corr, &[1.0, 2.0, 3.0], &[2.0, 4.0, 6.0]).unwrap();
        assert!((got - 1.0).abs() < 1e-12, "got {got}");
    }

    #[test]
    fn ts_corr_is_minus_one_for_a_perfect_inverse_relationship() {
        let got = eval_pair(PairRollingKind::Corr, &[1.0, 2.0, 3.0], &[6.0, 4.0, 2.0]).unwrap();
        assert!((got + 1.0).abs() < 1e-12, "got {got}");
    }

    /// Hand-checked reference so a refactor cannot silently switch to a
    /// population (ddof = 0) convention or transpose the arguments.
    #[test]
    fn ts_corr_and_ts_cov_match_a_hand_computed_reference() {
        // x = [1,2,3,4], y = [2,4,5,9]: mx = 2.5, my = 5.
        // dx = [-1.5,-0.5,0.5,1.5], dy = [-3,-1,0,4]
        // sxy = 4.5+0.5+0+6 = 11 ; sxx = 5 ; syy = 26
        // cov = 11/3 = 3.6666667 ; corr = 11/sqrt(130) = 0.96470
        let cov = eval_pair(
            PairRollingKind::Cov,
            &[1.0, 2.0, 3.0, 4.0],
            &[2.0, 4.0, 5.0, 9.0],
        );
        assert!((cov.unwrap() - 11.0 / 3.0).abs() < 1e-12, "cov {cov:?}");
        let corr = eval_pair(
            PairRollingKind::Corr,
            &[1.0, 2.0, 3.0, 4.0],
            &[2.0, 4.0, 5.0, 9.0],
        );
        let want = 11.0 / 130.0_f64.sqrt();
        assert!((corr.unwrap() - want).abs() < 1e-12, "corr {corr:?}");
    }

    /// `ts_cov(x, x)` must equal the sample variance of x, which is the
    /// cheapest available cross-check against an independent formula.
    #[test]
    fn ts_cov_with_itself_is_the_sample_variance() {
        let v = [1.0, 2.0, 3.0, 4.0, 10.0];
        let got = eval_pair(PairRollingKind::Cov, &v, &v).unwrap();
        assert!((got - 12.5).abs() < 1e-12, "got {got}");
    }

    #[test]
    fn ts_corr_is_null_when_either_side_is_constant() {
        assert_eq!(
            eval_pair(PairRollingKind::Corr, &[1.0, 1.0, 1.0], &[1.0, 2.0, 3.0]),
            None
        );
        assert_eq!(
            eval_pair(PairRollingKind::Corr, &[1.0, 2.0, 3.0], &[7.0, 7.0, 7.0]),
            None
        );
        // Covariance is still defined (and zero) against a constant.
        let cov = eval_pair(PairRollingKind::Cov, &[1.0, 2.0, 3.0], &[7.0, 7.0, 7.0]);
        assert_eq!(cov, Some(0.0));
    }

    /// Pairwise nulls: a row is skipped when *either* side is missing, so the
    /// surviving pairs stay aligned. If alignment broke, this fixture's
    /// correlation would not be exactly 1.
    #[test]
    fn pair_operators_skip_rows_where_either_side_is_missing() {
        let xa = arr(vec![Some(1.0), None, Some(2.0), Some(3.0), Some(99.0)]);
        let ya = arr(vec![Some(2.0), Some(50.0), Some(4.0), Some(6.0), None]);
        // Surviving pairs: (1,2), (2,4), (3,6) — perfectly proportional.
        let got = evaluate_pair_frame(PairRollingKind::Corr, &xa, &ya, &(0..5)).unwrap();
        assert!((got - 1.0).abs() < 1e-12, "got {got}");
    }

    #[test]
    fn pair_operators_need_two_surviving_pairs() {
        let xa = arr(vec![Some(1.0), None]);
        let ya = arr(vec![Some(2.0), Some(4.0)]);
        assert_eq!(
            evaluate_pair_frame(PairRollingKind::Corr, &xa, &ya, &(0..2)),
            None
        );
        assert_eq!(
            evaluate_pair_frame(PairRollingKind::Cov, &xa, &ya, &(0..2)),
            None
        );
    }

    #[test]
    fn pair_operators_are_null_on_an_empty_frame() {
        let xa = arr(vec![Some(1.0), Some(2.0)]);
        let ya = arr(vec![Some(1.0), Some(2.0)]);
        for kind in [PairRollingKind::Corr, PairRollingKind::Cov] {
            assert_eq!(evaluate_pair_frame(kind, &xa, &ya, &(0..0)), None);
        }
    }
}
