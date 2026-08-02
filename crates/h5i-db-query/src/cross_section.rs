//! Cross-sectional operators (ROADMAP Part VII-B2).
//!
//! Cross-sectional means "across entities at one instant": rank every symbol's
//! factor value against its peers *within the same timestamp*, which in SQL is
//! `OVER (PARTITION BY ts)`. Both reference projects surveyed in Part VII need
//! this and neither can express it in their query layer: qlib has **zero**
//! cross-sectional operators in its expression engine (cross-section happens in
//! a separate pandas processor stage over `groupby("datetime")`), and zipline
//! implements it as a triple-nested Python loop. Doing it in SQL collapses a
//! two-stage factor pipeline into one statement.
//!
//! # What plain SQL already does (not reimplemented here)
//!
//! Two of the four operators Part VII-B2 names need no new code, because a
//! window aggregate partitioned by the time bucket *is* the cross-sectional
//! statistic:
//!
//! ```sql
//! -- cs_demean: subtract the cross-sectional mean
//! factor - avg(factor) OVER (PARTITION BY ts)
//!
//! -- cs_zscore: standardise within the cross-section.
//! -- `stddev` is stddev_samp (ddof = 1), which matches pandas' `.std()`
//! -- default, so this agrees with qlib's CSZScoreNorm.
//! (factor - avg(factor) OVER (PARTITION BY ts))
//!   / stddev(factor) OVER (PARTITION BY ts)
//! ```
//!
//! `tests/quant_cross_section.rs` pins both forms against reference values, so
//! the capability is covered by tests even though no operator implements it.
//! Adding `cs_demean`/`cs_zscore` aliases would duplicate engine code for
//! nothing, which this project treats as a defect rather than a feature.
//!
//! # What is implemented here, and why SQL cannot do it
//!
//! - **`cs_rank(x)`** — percentile rank within the cross-section using pandas'
//!   `rank(pct=True)` convention, where tied values share the *mean* of the
//!   ranks they span. SQL's ranking functions all compute something subtly
//!   different: `percent_rank()` is `(min_rank - 1)/(n - 1)` (so the smallest
//!   value is always 0.0), and `cume_dist()` is `count(<= v)/n` (so ties take
//!   the *top* of their band). Neither equals pandas, and factor pipelines are
//!   compared against pandas output, so the difference is not cosmetic. This is
//!   also qlib's `CSRankNorm` input.
//! - **`cs_winsorize(x, lower_pct, upper_pct)`** — clip the tails of the
//!   cross-section at percentile cutoffs, following zipline's implementation
//!   exactly (count-based cutoffs over non-null values, tail values replaced by
//!   the surviving boundary value). Not expressible as one SQL statement:
//!   `percentile_cont` is an ordered-set aggregate with `WITHIN GROUP`, which is
//!   not usable as a window function, so the cutoff would need a self-join.
//!
//! Both are window functions over the whole partition (no frame): they are
//! evaluated once per partition in O(n log n), not once per row.
//!
//! # NULL discipline
//!
//! A NULL (or NaN) input yields NULL output and is excluded from every
//! statistic, so a missing factor value never shifts its peers' ranks. This
//! matches zipline's masking rule: entities outside the computation are absent
//! from the statistic but present as NULL in the output.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::function::{PartitionEvaluatorArgs, WindowUDFFieldArgs};
use datafusion::logical_expr::{
    PartitionEvaluator, Signature, Volatility, WindowUDF, WindowUDFImpl,
};

use crate::finance::to_f64_array;

/// Collect `(value, row_index)` for every row with a usable numeric value,
/// sorted ascending by value. NULL and NaN rows are excluded entirely.
///
/// Sorting once is what keeps these operators O(n log n); the obvious
/// "compare each value against every other" formulation is O(n²), which on a
/// few thousand symbols per timestamp is the difference between a usable and
/// an unusable query.
fn sorted_valid(x: &Float64Array, num_rows: usize) -> Vec<(f64, usize)> {
    let mut pairs: Vec<(f64, usize)> = (0..num_rows)
        .filter(|&i| x.is_valid(i))
        .map(|i| (x.value(i), i))
        .filter(|(v, _)| !v.is_nan())
        .collect();
    // Values are non-NaN here, so `total_cmp` is a total order and never panics.
    pairs.sort_by(|a, b| a.0.total_cmp(&b.0));
    pairs
}

// ---------------------------------------------------------------------------
// cs_rank
// ---------------------------------------------------------------------------

/// `cs_rank(x) OVER (PARTITION BY <bucket>)` — pandas-compatible percentile
/// rank within the cross-section. See the module docs for why the SQL built-ins
/// do not substitute.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct CsRankUdwf {
    signature: Signature,
}

impl Default for CsRankUdwf {
    fn default() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl WindowUDFImpl for CsRankUdwf {
    fn name(&self) -> &str {
        "cs_rank"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn partition_evaluator(
        &self,
        _args: PartitionEvaluatorArgs,
    ) -> DfResult<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(CsRankEvaluator))
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> DfResult<FieldRef> {
        let inputs = field_args.input_fields();
        if inputs.len() != 1 {
            return Err(DataFusionError::Plan(format!(
                "cs_rank(value) takes exactly one argument, got {}",
                inputs.len()
            )));
        }
        let dt = inputs[0].data_type();
        if !dt.is_numeric() && !matches!(dt, DataType::Null) {
            return Err(DataFusionError::Plan(format!(
                "cs_rank: argument must be numeric, got {dt}"
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
struct CsRankEvaluator;

impl PartitionEvaluator for CsRankEvaluator {
    /// Cross-sectional statistics are partition-wide, never frame-relative.
    fn uses_window_frame(&self) -> bool {
        false
    }

    fn evaluate_all(&mut self, values: &[ArrayRef], num_rows: usize) -> DfResult<ArrayRef> {
        if values.is_empty() {
            return Err(DataFusionError::Execution(
                "cs_rank: no input column supplied".into(),
            ));
        }
        let x = to_f64_array(&values[0])?;
        Ok(Arc::new(cs_rank_values(&x, num_rows)))
    }
}

/// Percentile rank of every row within the partition, pandas `rank(pct=True)`
/// with the default `method="average"`.
fn cs_rank_values(x: &Float64Array, num_rows: usize) -> Float64Array {
    let pairs = sorted_valid(x, num_rows);
    let n = pairs.len();
    let mut out: Vec<Option<f64>> = vec![None; num_rows];
    if n == 0 {
        return Float64Array::from(out);
    }
    let nf = n as f64;
    let mut i = 0usize;
    while i < n {
        // Extend over the run of equal values: every tied row receives the
        // mean of the 1-based ranks the run spans.
        let mut j = i + 1;
        while j < n && pairs[j].0 == pairs[i].0 {
            j += 1;
        }
        // Ranks i+1 .. j (1-based inclusive) average to (i+1 + j)/2.
        let avg_rank = ((i + 1) as f64 + j as f64) / 2.0;
        let pct = avg_rank / nf;
        for k in i..j {
            out[pairs[k].1] = Some(pct);
        }
        i = j;
    }
    Float64Array::from(out)
}

// ---------------------------------------------------------------------------
// cs_winsorize
// ---------------------------------------------------------------------------

/// `cs_winsorize(x, lower_pct, upper_pct) OVER (PARTITION BY <bucket>)` —
/// clip cross-sectional tails at percentile cutoffs.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct CsWinsorizeUdwf {
    signature: Signature,
}

impl Default for CsWinsorizeUdwf {
    fn default() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl WindowUDFImpl for CsWinsorizeUdwf {
    fn name(&self) -> &str {
        "cs_winsorize"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn partition_evaluator(
        &self,
        _args: PartitionEvaluatorArgs,
    ) -> DfResult<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(CsWinsorizeEvaluator))
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> DfResult<FieldRef> {
        let inputs = field_args.input_fields();
        if inputs.len() != 3 {
            return Err(DataFusionError::Plan(format!(
                "cs_winsorize(value, lower_pct, upper_pct) takes exactly three \
                 arguments, got {}",
                inputs.len()
            )));
        }
        for f in inputs {
            let dt = f.data_type();
            if !dt.is_numeric() && !matches!(dt, DataType::Null) {
                return Err(DataFusionError::Plan(format!(
                    "cs_winsorize: arguments must be numeric, got {dt}"
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
struct CsWinsorizeEvaluator;

impl PartitionEvaluator for CsWinsorizeEvaluator {
    fn uses_window_frame(&self) -> bool {
        false
    }

    fn evaluate_all(&mut self, values: &[ArrayRef], num_rows: usize) -> DfResult<ArrayRef> {
        if values.len() < 3 {
            return Err(DataFusionError::Execution(
                "cs_winsorize: expected three input columns".into(),
            ));
        }
        let x = to_f64_array(&values[0])?;
        let lower = scalar_pct(&values[1], "lower_pct")?;
        let upper = scalar_pct(&values[2], "upper_pct")?;
        if lower > upper {
            return Err(DataFusionError::Execution(format!(
                "cs_winsorize: lower_pct ({lower}) must not exceed upper_pct ({upper})"
            )));
        }
        Ok(Arc::new(cs_winsorize_values(&x, num_rows, lower, upper)))
    }
}

/// Read a percentile argument, which must be a constant in `[0, 1]`.
fn scalar_pct(array: &ArrayRef, what: &str) -> DfResult<f64> {
    let a = to_f64_array(array)?;
    if a.is_empty() || !a.is_valid(0) {
        return Err(DataFusionError::Execution(format!(
            "cs_winsorize: {what} must be a non-NULL constant"
        )));
    }
    let v = a.value(0);
    if !(0.0..=1.0).contains(&v) {
        return Err(DataFusionError::Execution(format!(
            "cs_winsorize: {what} must be in [0, 1], got {v}"
        )));
    }
    Ok(v)
}

/// Winsorize the partition, following zipline's `winsorize` row function.
///
/// The cutoffs are **counts derived from percentiles**, not interpolated
/// quantiles: `lower_cutoff = floor(lower_pct · n)` values are pulled up to the
/// value at that sorted position, and everything from
/// `upper_cutoff = ceil(upper_pct · n)` onward is pulled down to the value just
/// below it. Counting rather than interpolating is what makes the result a
/// genuine member of the original cross-section, which matters when the output
/// feeds a rank or a trade size.
///
/// One deliberate divergence: at `upper_pct == 0` zipline's `max(cutoff-1, 0)`
/// makes the whole cross-section collapse to its minimum, because "winsorize
/// everything above the 0th percentile" is read as "clamp to the smallest
/// value". Here that degenerate cutoff clamps nothing and the values pass
/// through. Returning the input unchanged is the safer reading of an argument
/// that asks for no winsorization.
fn cs_winsorize_values(
    x: &Float64Array,
    num_rows: usize,
    lower_pct: f64,
    upper_pct: f64,
) -> Float64Array {
    let pairs = sorted_valid(x, num_rows);
    let n = pairs.len();
    let mut out: Vec<Option<f64>> = vec![None; num_rows];
    if n == 0 {
        return Float64Array::from(out);
    }
    // Start from the identity, then clamp the tails.
    for &(v, idx) in &pairs {
        out[idx] = Some(v);
    }

    if lower_pct > 0.0 {
        let lower_cutoff = (lower_pct * n as f64) as usize;
        if lower_cutoff < n {
            let boundary = pairs[lower_cutoff].0;
            for &(_, idx) in &pairs[..lower_cutoff] {
                out[idx] = Some(boundary);
            }
        }
    }
    if upper_pct < 1.0 {
        let upper_cutoff = (upper_pct * n as f64).ceil() as usize;
        // A cutoff at or past the end removes nothing; guard the `- 1` too.
        if upper_cutoff < n && upper_cutoff >= 1 {
            let boundary = pairs[upper_cutoff - 1].0;
            for &(_, idx) in &pairs[upper_cutoff..] {
                out[idx] = Some(boundary);
            }
        }
    }
    Float64Array::from(out)
}

/// Every cross-sectional operator this module adds, for registration.
pub fn cross_section_udwfs() -> Vec<WindowUDF> {
    vec![
        WindowUDF::new_from_impl(CsRankUdwf::default()),
        WindowUDF::new_from_impl(CsWinsorizeUdwf::default()),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arr(values: Vec<Option<f64>>) -> Float64Array {
        Float64Array::from(values)
    }

    fn ranks(values: Vec<Option<f64>>) -> Vec<Option<f64>> {
        let n = values.len();
        let a = arr(values);
        let out = cs_rank_values(&a, n);
        (0..out.len())
            .map(|i| {
                if out.is_valid(i) {
                    Some(out.value(i))
                } else {
                    None
                }
            })
            .collect()
    }

    fn winsorized(values: Vec<Option<f64>>, lo: f64, hi: f64) -> Vec<Option<f64>> {
        let n = values.len();
        let a = arr(values);
        let out = cs_winsorize_values(&a, n, lo, hi);
        (0..out.len())
            .map(|i| {
                if out.is_valid(i) {
                    Some(out.value(i))
                } else {
                    None
                }
            })
            .collect()
    }

    // -----------------------------------------------------------------------
    // cs_rank
    // -----------------------------------------------------------------------

    #[test]
    fn cs_rank_is_uniform_on_distinct_values() {
        // 4 distinct values → 1/4, 2/4, 3/4, 4/4 regardless of input order.
        let got = ranks(vec![Some(30.0), Some(10.0), Some(40.0), Some(20.0)]);
        assert_eq!(
            got,
            vec![Some(0.75), Some(0.25), Some(1.0), Some(0.5)],
            "ranks must follow value order, not row order"
        );
    }

    /// pandas averages tied ranks. This is the exact behaviour SQL's
    /// `percent_rank` and `cume_dist` get wrong, so it is the load-bearing
    /// test for this operator existing at all.
    #[test]
    fn cs_rank_averages_ties_like_pandas() {
        // [10, 20, 20, 40]: the two 20s span ranks 2 and 3 → mean 2.5 → 0.625.
        let got = ranks(vec![Some(10.0), Some(20.0), Some(20.0), Some(40.0)]);
        assert_eq!(got[0], Some(0.25));
        assert_eq!(got[1], Some(0.625));
        assert_eq!(got[2], Some(0.625));
        assert_eq!(got[3], Some(1.0));
    }

    /// An all-tied cross-section must collapse to the midpoint, not to 0 (what
    /// `percent_rank` gives) or 1 (what `cume_dist` gives).
    #[test]
    fn cs_rank_of_an_all_tied_cross_section_is_the_midpoint() {
        let got = ranks(vec![Some(5.0), Some(5.0), Some(5.0)]);
        // Ranks 1,2,3 average to 2 → 2/3.
        for g in got {
            let g = g.unwrap();
            assert!((g - 2.0 / 3.0).abs() < 1e-12, "got {g}");
        }
    }

    /// Missing values must not shift their peers: the surviving three values
    /// rank as if the NULL row were absent.
    #[test]
    fn cs_rank_excludes_nulls_from_the_statistic_and_returns_null_for_them() {
        let got = ranks(vec![Some(10.0), None, Some(20.0), Some(30.0)]);
        assert_eq!(got[1], None, "a NULL factor value has no rank");
        assert_eq!(
            (got[0], got[2], got[3]),
            (Some(1.0 / 3.0), Some(2.0 / 3.0), Some(3.0 / 3.0)),
            "peers rank over the 3 valid values, not 4"
        );
    }

    #[test]
    fn cs_rank_treats_nan_as_missing() {
        let got = ranks(vec![Some(f64::NAN), Some(1.0), Some(2.0)]);
        assert_eq!(got[0], None);
        assert_eq!(got[1], Some(0.5));
        assert_eq!(got[2], Some(1.0));
    }

    #[test]
    fn cs_rank_of_an_empty_or_all_null_cross_section_is_all_null() {
        assert_eq!(ranks(vec![]), Vec::<Option<f64>>::new());
        assert_eq!(ranks(vec![None, None]), vec![None, None]);
    }

    #[test]
    fn cs_rank_handles_negative_values_and_is_monotone() {
        let got = ranks(vec![Some(-5.0), Some(0.0), Some(-10.0), Some(3.0)]);
        // Order: -10 < -5 < 0 < 3.
        assert_eq!(
            got,
            vec![Some(0.5), Some(0.75), Some(0.25), Some(1.0)],
            "ranking must respect sign"
        );
    }

    // -----------------------------------------------------------------------
    // cs_winsorize
    // -----------------------------------------------------------------------

    #[test]
    fn cs_winsorize_clips_both_tails_to_the_surviving_boundary_values() {
        // n = 10, sorted 1..10. lower 20% → cutoff index 2 → boundary 3.
        // upper 80% → cutoff ceil(8) = 8 → boundary = sorted[7] = 8.
        let v: Vec<Option<f64>> = (1..=10).map(|i| Some(i as f64)).collect();
        let got = winsorized(v, 0.2, 0.8);
        let want = [3.0, 3.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 8.0, 8.0];
        for (i, w) in want.iter().enumerate() {
            assert_eq!(got[i], Some(*w), "row {i}");
        }
    }

    /// The clipped values must be real members of the cross-section, which is
    /// what distinguishes a count-based winsorize from an interpolated
    /// quantile clamp.
    #[test]
    fn cs_winsorize_output_values_all_occur_in_the_input() {
        let v: Vec<Option<f64>> = vec![Some(1.0), Some(2.5), Some(3.75), Some(9.0), Some(100.0)];
        let got = winsorized(v.clone(), 0.2, 0.8);
        let input: Vec<f64> = v.into_iter().map(|x| x.unwrap()).collect();
        for g in got.into_iter().flatten() {
            assert!(
                input.iter().any(|i| (i - g).abs() < 1e-12),
                "winsorized value {g} is not a member of the input"
            );
        }
    }

    #[test]
    fn cs_winsorize_is_the_identity_with_full_range_percentiles() {
        let v: Vec<Option<f64>> = vec![Some(1.0), Some(5.0), Some(3.0)];
        assert_eq!(winsorized(v.clone(), 0.0, 1.0), v);
    }

    #[test]
    fn cs_winsorize_keeps_nulls_null_and_excludes_them_from_cutoffs() {
        // 4 valid values (1,2,3,4) plus a NULL. lower 25% → cutoff 1 →
        // boundary 2; upper 1.0 → no upper clipping.
        let got = winsorized(
            vec![Some(1.0), None, Some(2.0), Some(3.0), Some(4.0)],
            0.25,
            1.0,
        );
        assert_eq!(got[1], None, "NULL stays NULL");
        assert_eq!(got[0], Some(2.0), "the single lowest value is pulled up");
        assert_eq!(got[2], Some(2.0));
        assert_eq!(got[4], Some(4.0), "top is untouched at upper_pct = 1");
    }

    /// A one-sided winsorize must leave the other tail exactly alone.
    #[test]
    fn cs_winsorize_can_clip_one_tail_only() {
        let v: Vec<Option<f64>> = (1..=4).map(|i| Some(i as f64)).collect();
        let lower_only = winsorized(v.clone(), 0.5, 1.0);
        assert_eq!(
            lower_only,
            vec![Some(3.0), Some(3.0), Some(3.0), Some(4.0)],
            "lower half pulled up to sorted[2] = 3"
        );
        let upper_only = winsorized(v, 0.0, 0.5);
        assert_eq!(
            upper_only,
            vec![Some(1.0), Some(2.0), Some(2.0), Some(2.0)],
            "upper half pulled down to sorted[1] = 2"
        );
    }

    #[test]
    fn cs_winsorize_of_an_all_null_cross_section_is_all_null() {
        assert_eq!(winsorized(vec![None, None], 0.1, 0.9), vec![None, None]);
    }

    /// Degenerate percentiles must not panic or index out of bounds: this is
    /// the kind of edge a fuzzer finds and a release build turns into an abort.
    #[test]
    fn cs_winsorize_survives_degenerate_percentiles() {
        let v: Vec<Option<f64>> = vec![Some(1.0), Some(2.0), Some(3.0)];
        // Everything collapses onto one value when both cutoffs meet.
        let all_low = winsorized(v.clone(), 0.0, 0.001);
        assert_eq!(all_low, vec![Some(1.0), Some(1.0), Some(1.0)]);
        let all_high = winsorized(v.clone(), 0.999, 1.0);
        assert_eq!(all_high, vec![Some(3.0), Some(3.0), Some(3.0)]);
        // A single-element cross-section is unchanged whatever the cutoffs.
        assert_eq!(winsorized(vec![Some(7.0)], 0.4, 0.6), vec![Some(7.0)]);
    }

    #[test]
    fn registration_exposes_both_operators_under_stable_names() {
        let names: Vec<String> = cross_section_udwfs()
            .iter()
            .map(|f| f.name().to_string())
            .collect();
        assert_eq!(names, vec!["cs_rank", "cs_winsorize"]);
    }
}
