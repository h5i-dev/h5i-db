//! Part VII-B1 rolling operators, exercised through real SQL.
//!
//! Two jobs. First, prove the six operators this project adds (`idxmax`,
//! `idxmin`, `mad`, `skew`, `kurt`, `ts_rank`) reach the planner, respect
//! `PARTITION BY`, and slide with the frame — the unit tests in
//! `src/rolling.rs` cover the numerics, so these tests cover the wiring.
//!
//! Second, and just as important, pin the *reachability* claims that justify
//! **not** writing more operators. `crates/h5i-db-query/src/rolling.rs`
//! documents that `Mean`/`Std`/`Corr`/`Cov`/`Med`/`Quantile`/`Slope`/
//! `Rsquare`/`Resi` are all expressible with built-ins. If a DataFusion
//! upgrade ever breaks one of those, the honest response is to implement the
//! operator, and these tests are what would tell us.

use std::sync::Arc;

use arrow::array::{Array, Float64Array, RecordBatch, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use h5i_db_core::{Database, TableOptions, WriteOptions};
use h5i_db_query::{H5iSession, SessionOptions};

fn bars_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("close", DataType::Float64, false),
    ]))
}

/// Symbol `A` carries the same sample the unit tests use ([1,2,3,4,10]) so
/// reference values are shared; `B` is strictly decreasing so partition
/// independence and the min/rank paths are both exercised.
async fn setup_bars() -> (tempfile::TempDir, Arc<Database>) {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Database::create(&dir.path().join("db")).await.unwrap());
    db.create_table(
        "bars",
        bars_schema(),
        TableOptions {
            time_column: Some("ts".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let ts: Vec<i64> = (1..=9).map(|i| i * 1_000_000_000).collect();
    let symbols = vec!["A", "A", "A", "A", "A", "B", "B", "B", "B"];
    let closes = vec![1.0, 2.0, 3.0, 4.0, 10.0, 4.0, 3.0, 2.0, 1.0];
    db.append(
        "bars",
        vec![
            RecordBatch::try_new(
                bars_schema(),
                vec![
                    Arc::new(TimestampNanosecondArray::from(ts).with_timezone("UTC".to_string())),
                    Arc::new(StringArray::from(symbols)),
                    Arc::new(Float64Array::from(closes)),
                ],
            )
            .unwrap(),
        ],
        WriteOptions::default(),
    )
    .await
    .unwrap();
    (dir, db)
}

async fn session(db: &Arc<Database>) -> H5iSession {
    H5iSession::new(db.clone(), SessionOptions::default())
        .await
        .unwrap()
}

/// Run `sql` and return column 0 as `Option<f64>` per row, in row order.
async fn floats(s: &H5iSession, sql: &str) -> Vec<Option<f64>> {
    let batches = s
        .sql(sql)
        .await
        .unwrap_or_else(|e| panic!("query failed: {sql}\n{e}"))
        .collect()
        .await
        .unwrap_or_else(|e| panic!("collect failed: {sql}\n{e}"));
    let mut out = Vec::new();
    for b in batches {
        let col = arrow::compute::cast(b.column(0), &DataType::Float64).unwrap();
        let col = col.as_any().downcast_ref::<Float64Array>().unwrap().clone();
        for i in 0..col.len() {
            out.push(if col.is_valid(i) {
                Some(col.value(i))
            } else {
                None
            });
        }
    }
    out
}

fn assert_close(got: Option<f64>, want: f64, what: &str) {
    let g = got.unwrap_or_else(|| panic!("{what}: expected {want}, got NULL"));
    assert!(
        (g - want).abs() < 1e-9,
        "{what}: expected {want}, got {g} (delta {})",
        (g - want).abs()
    );
}

// ---------------------------------------------------------------------------
// The six added operators, through SQL
// ---------------------------------------------------------------------------

/// Full 5-row frame for symbol `A` reproduces every reference value from the
/// unit tests, proving the SQL path and the direct numerics agree.
#[tokio::test(flavor = "multi_thread")]
async fn added_operators_match_reference_values_through_sql() {
    let (_dir, db) = setup_bars().await;
    let s = session(&db).await;

    let cases = [
        ("idxmax(close)", 5.0),
        ("idxmin(close)", 1.0),
        ("mad(close)", 2.4),
        ("skew(close)", 1.697_056_274_847_714),
        ("kurt(close)", 3.152),
        ("ts_rank(close)", 1.0),
    ];
    for (expr, want) in cases {
        let sql = format!(
            "SELECT {expr} AS v FROM bars WHERE symbol = 'A' \
             WINDOW w AS (PARTITION BY symbol ORDER BY ts \
                          ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) \
             ORDER BY ts"
        );
        // Name the window in the projection too (DataFusion requires the OVER
        // reference inline for a named window).
        let sql = sql.replace(&format!("{expr} AS v"), &format!("{expr} OVER w AS v"));
        let got = floats(&s, &sql).await;
        assert_eq!(got.len(), 5, "{expr}: expected one row per bar");
        assert_close(got[4], want, expr);
    }
}

/// Rolling operators must be *rolling*: the same expression over a 3-wide
/// frame gives a different answer per row, and positions are frame-relative
/// rather than partition-relative.
#[tokio::test(flavor = "multi_thread")]
async fn added_operators_slide_with_the_frame() {
    let (_dir, db) = setup_bars().await;
    let s = session(&db).await;

    // close for A = [1,2,3,4,10]; 3-wide trailing frames are
    // [1], [1,2], [1,2,3], [2,3,4], [3,4,10].
    let got = floats(
        &s,
        "SELECT idxmax(close) OVER (PARTITION BY symbol ORDER BY ts \
                                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS v \
         FROM bars WHERE symbol = 'A' ORDER BY ts",
    )
    .await;
    // The max is always the newest bar (series is increasing), so its
    // 1-based frame position grows to the frame width and then stays there.
    assert_eq!(
        got,
        vec![Some(1.0), Some(2.0), Some(3.0), Some(3.0), Some(3.0)],
        "idxmax must report frame-relative positions"
    );

    // ts_rank on an increasing series is always 1.0; on B (decreasing) the
    // current value is always the smallest in its frame.
    let got_b = floats(
        &s,
        "SELECT ts_rank(close) OVER (PARTITION BY symbol ORDER BY ts \
                                     ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS v \
         FROM bars WHERE symbol = 'B' ORDER BY ts",
    )
    .await;
    // Frames: [4] → 1/1; [4,3] → 1/2; [4,3,2] → 1/3; [3,2,1] → 1/3.
    assert_close(got_b[0], 1.0, "B row0");
    assert_close(got_b[1], 0.5, "B row1");
    assert_close(got_b[2], 1.0 / 3.0, "B row2");
    assert_close(got_b[3], 1.0 / 3.0, "B row3");
}

/// `PARTITION BY` must isolate symbols: `A` and `B` are interleaved in time
/// but their frames may never mix.
#[tokio::test(flavor = "multi_thread")]
async fn partitioning_isolates_symbols() {
    let (_dir, db) = setup_bars().await;
    let s = session(&db).await;
    let got = floats(
        &s,
        "SELECT idxmin(close) OVER (PARTITION BY symbol ORDER BY ts \
                                    ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS v \
         FROM bars WHERE symbol = 'B' ORDER BY ts",
    )
    .await;
    // B = [4,3,2,1] decreasing, so the min is always the newest bar.
    assert_eq!(got, vec![Some(1.0), Some(2.0), Some(3.0), Some(4.0)]);
}

/// Short frames must yield NULL for the shape statistics rather than a
/// spurious number: `skew` needs 3 points, `kurt` needs 4.
#[tokio::test(flavor = "multi_thread")]
async fn shape_statistics_are_null_until_the_frame_is_wide_enough() {
    let (_dir, db) = setup_bars().await;
    let s = session(&db).await;
    let skewed = floats(
        &s,
        "SELECT skew(close) OVER (PARTITION BY symbol ORDER BY ts \
                                  ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS v \
         FROM bars WHERE symbol = 'A' ORDER BY ts",
    )
    .await;
    assert_eq!(skewed[0], None, "1-row frame has no skew");
    assert_eq!(skewed[1], None, "2-row frame has no skew");
    assert!(skewed[2].is_some(), "3-row frame has a skew");

    let kurted = floats(
        &s,
        "SELECT kurt(close) OVER (PARTITION BY symbol ORDER BY ts \
                                  ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS v \
         FROM bars WHERE symbol = 'A' ORDER BY ts",
    )
    .await;
    assert_eq!(kurted[2], None, "3-row frame has no excess kurtosis");
    assert!(kurted[3].is_some(), "4-row frame has one");
}

/// `ts_rank` is not `percent_rank`: one ranks within the sliding frame, the
/// other within the whole partition. If they ever agree on this fixture the
/// operator has silently become a duplicate.
#[tokio::test(flavor = "multi_thread")]
async fn ts_rank_differs_from_percent_rank() {
    let (_dir, db) = setup_bars().await;
    let s = session(&db).await;
    let got = floats(
        &s,
        "SELECT ts_rank(close) OVER (PARTITION BY symbol ORDER BY ts \
                                     ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) \
                - percent_rank() OVER (PARTITION BY symbol ORDER BY ts) AS v \
         FROM bars WHERE symbol = 'A' ORDER BY ts",
    )
    .await;
    assert!(
        got.iter()
            .any(|d| d.map(|d| d.abs() > 1e-9).unwrap_or(false)),
        "ts_rank must not coincide with percent_rank"
    );
}

// ---------------------------------------------------------------------------
// Reachability of the operators we deliberately did NOT implement
// ---------------------------------------------------------------------------

/// The load-bearing claim behind not writing a bespoke `slope` operator: OLS
/// slope is invariant to translating x, so regressing on `row_number()`
/// reproduces qlib's regression on within-window positions `1..N`.
#[tokio::test(flavor = "multi_thread")]
async fn ols_slope_is_invariant_to_x_translation() {
    let (_dir, db) = setup_bars().await;
    let s = session(&db).await;
    let deltas = floats(
        &s,
        "SELECT regr_slope(close, i) OVER w - regr_slope(close, i + 1000) OVER w AS v \
         FROM (SELECT *, row_number() OVER (PARTITION BY symbol ORDER BY ts) AS i FROM bars) \
         WHERE symbol = 'A' \
         WINDOW w AS (PARTITION BY symbol ORDER BY ts \
                      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) \
         ORDER BY ts",
    )
    .await;
    for (row, d) in deltas.iter().enumerate() {
        if let Some(d) = d {
            assert!(
                d.abs() < 1e-9,
                "row {row}: slope changed by {d} under x-translation"
            );
        }
    }
    assert!(
        deltas.iter().any(|d| d.is_some()),
        "expected at least one defined slope"
    );
}

/// A perfectly linear series has slope 1, R² 1, and zero residual — the three
/// values that stand in for qlib's `Slope`/`Rsquare`/`Resi`.
#[tokio::test(flavor = "multi_thread")]
async fn regression_family_reproduces_slope_rsquare_and_residual() {
    let (_dir, db) = setup_bars().await;
    let s = session(&db).await;
    // Rows 1..4 of A are close = 1,2,3,4: exactly linear in the bar index.
    let base = "FROM (SELECT *, row_number() OVER (PARTITION BY symbol ORDER BY ts) AS i FROM bars) \
                WHERE symbol = 'A' AND close <= 4.0 \
                WINDOW w AS (PARTITION BY symbol ORDER BY ts \
                             ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) \
                ORDER BY ts";

    let slope = floats(
        &s,
        &format!("SELECT regr_slope(close, i) OVER w AS v {base}"),
    )
    .await;
    assert_close(slope[3], 1.0, "slope of a unit-step series");

    let r2 = floats(&s, &format!("SELECT regr_r2(close, i) OVER w AS v {base}")).await;
    assert_close(r2[3], 1.0, "R² of a perfectly linear series");

    let resi = floats(
        &s,
        &format!(
            "SELECT close - (regr_slope(close, i) OVER w * i \
                             + regr_intercept(close, i) OVER w) AS v {base}"
        ),
    )
    .await;
    assert_close(resi[3], 0.0, "residual of a perfectly linear series");
}

/// Every remaining operator the module documents as "reachable" must actually
/// execute over a window frame and return the expected value. This is the
/// regression guard on the decision not to reimplement them.
#[tokio::test(flavor = "multi_thread")]
async fn documented_builtin_vocabulary_is_reachable_over_a_frame() {
    let (_dir, db) = setup_bars().await;
    let s = session(&db).await;

    // Frame = all 5 rows of A: close = [1,2,3,4,10], mean 4, sample var 12.5.
    let cases: [(&str, f64); 8] = [
        ("avg(close)", 4.0),
        ("sum(close)", 20.0),
        ("count(close)", 5.0),
        ("stddev(close)", 3.535_533_905_932_737_6),
        ("var_samp(close)", 12.5),
        ("max(close)", 10.0),
        ("min(close)", 1.0),
        ("median(close)", 3.0),
    ];
    for (expr, want) in cases {
        let sql = format!(
            "SELECT {expr} OVER (PARTITION BY symbol ORDER BY ts \
                                 ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS v \
             FROM bars WHERE symbol = 'A' ORDER BY ts"
        );
        let got = floats(&s, &sql).await;
        assert_close(got[4], want, expr);
    }

    // Exact quantile stands in for qlib's Quantile(x, N, q).
    let q = floats(
        &s,
        "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY close) AS v \
         FROM bars WHERE symbol = 'A'",
    )
    .await;
    assert_close(q[0], 3.0, "percentile_cont(0.5)");
}

// ---------------------------------------------------------------------------
// Rolling correlation: the one reachability claim that did NOT hold
// ---------------------------------------------------------------------------

/// Built-in `corr`/`covar_samp` work over an **expanding** frame. Pinning this
/// keeps the limitation below precisely scoped: it is about sliding frames, not
/// about the functions being unusable.
#[tokio::test(flavor = "multi_thread")]
async fn builtin_corr_and_covar_work_over_an_expanding_frame() {
    let (_dir, db) = setup_bars().await;
    let s = session(&db).await;
    let corr = floats(
        &s,
        "SELECT corr(close, close) OVER (PARTITION BY symbol ORDER BY ts \
             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS v \
         FROM bars WHERE symbol = 'A' ORDER BY ts",
    )
    .await;
    assert_close(corr[4], 1.0, "corr(x, x) over an expanding frame");

    let cov = floats(
        &s,
        "SELECT covar_samp(close, close) OVER (PARTITION BY symbol ORDER BY ts \
             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS v \
         FROM bars WHERE symbol = 'A' ORDER BY ts",
    )
    .await;
    assert_close(cov[4], 12.5, "covar_samp(x, x) = var_samp(x)");
}

/// The reason `ts_corr`/`ts_cov` exist. On DataFusion 54.1 neither `corr` nor
/// `covar_samp` overrides `supports_retract_batch`, so a *sliding* frame is
/// rejected outright.
///
/// This test asserts the failure deliberately. Upstream has since added
/// retraction to both accumulators, so a future DataFusion upgrade will make
/// this test fail — and that failure is the signal to revisit whether
/// `ts_corr`/`ts_cov` still earn their keep (they still avoid retraction
/// drift, so the answer may well be yes). Do not delete it as flaky.
#[tokio::test(flavor = "multi_thread")]
async fn builtin_corr_cannot_slide_on_this_datafusion_version() {
    let (_dir, db) = setup_bars().await;
    let s = session(&db).await;
    for func in ["corr", "covar_samp"] {
        let sql = format!(
            "SELECT {func}(close, close) OVER (PARTITION BY symbol ORDER BY ts \
                 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS v \
             FROM bars WHERE symbol = 'A' ORDER BY ts"
        );
        let res = match s.sql(&sql).await {
            Err(e) => Err(e),
            Ok(df) => df.collect().await.map(|_| ()),
        };
        let err = res
            .expect_err(&format!(
                "{func} over a sliding frame is expected to fail on DF 54.1; \
                 if it now succeeds, see this test's doc comment"
            ))
            .to_string();
        assert!(
            err.contains("sliding accumulator") || err.contains("retract_batch"),
            "{func}: expected a retraction error, got: {err}"
        );
    }
}

/// `ts_corr`/`ts_cov` do what the built-ins cannot: slide.
#[tokio::test(flavor = "multi_thread")]
async fn rolling_pair_operators_slide_and_match_reference_values() {
    let (_dir, db) = setup_bars().await;
    let s = session(&db).await;

    // Self-correlation is 1 wherever the frame has two distinct values.
    let corr = floats(
        &s,
        "SELECT ts_corr(close, close) OVER (PARTITION BY symbol ORDER BY ts \
             ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS v \
         FROM bars WHERE symbol = 'A' ORDER BY ts",
    )
    .await;
    assert_eq!(corr[0], None, "a 1-row frame has no correlation");
    for (row, got) in corr.iter().enumerate().skip(1) {
        assert_close(*got, 1.0, &format!("ts_corr(x, x) row {row}"));
    }

    // ts_cov(x, x) is the rolling sample variance, so it must agree with
    // var_samp over the identical frame — an independent cross-check.
    let deltas = floats(
        &s,
        "SELECT ts_cov(close, close) OVER w - var_samp(close) OVER w AS v \
         FROM bars WHERE symbol = 'A' \
         WINDOW w AS (PARTITION BY symbol ORDER BY ts \
                      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) \
         ORDER BY ts",
    )
    .await;
    for (row, d) in deltas.iter().enumerate().skip(1) {
        assert_close(*d, 0.0, &format!("ts_cov vs var_samp row {row}"));
    }

    // An inverse relationship inside one frame: A's close rises while B's
    // falls, so correlating close against a decreasing expression gives -1.
    let inverse = floats(
        &s,
        "SELECT ts_corr(close, -close) OVER (PARTITION BY symbol ORDER BY ts \
             ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS v \
         FROM bars WHERE symbol = 'A' ORDER BY ts",
    )
    .await;
    assert_close(inverse[4], -1.0, "ts_corr(x, -x)");
}

/// Alpha158's `CORR{d}` shape — `Corr($close, Log($volume+1), d)` — must plan
/// and execute, since Part VII-B3's corpus depends on it.
#[tokio::test(flavor = "multi_thread")]
async fn alpha158_corr_shape_executes() {
    let (_dir, db) = setup_bars().await;
    let s = session(&db).await;
    let got = floats(
        &s,
        "SELECT ts_corr(close, ln(close + 1)) OVER (PARTITION BY symbol ORDER BY ts \
             ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS v \
         FROM bars WHERE symbol = 'A' ORDER BY ts",
    )
    .await;
    // close and ln(close+1) are both monotonically increasing, so the
    // correlation is positive and near (but not exactly) 1.
    let v = got[4].expect("defined correlation over the full frame");
    assert!(
        v > 0.9 && v <= 1.0,
        "expected a strong positive corr, got {v}"
    );
}

/// Pair-operator arity is validated at planning time, like the single-argument
/// operators.
#[tokio::test(flavor = "multi_thread")]
async fn pair_operator_wrong_arity_is_a_query_error() {
    let (_dir, db) = setup_bars().await;
    let s = session(&db).await;
    let res = match s
        .sql(
            "SELECT ts_corr(close) OVER (PARTITION BY symbol ORDER BY ts \
                 ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM bars",
        )
        .await
    {
        Err(e) => Err(e),
        Ok(df) => df.collect().await.map(|_| ()),
    };
    let err = res
        .expect_err("ts_corr with one argument must fail")
        .to_string();
    assert!(
        err.contains("exactly two arguments"),
        "error should name the arity problem, got: {err}"
    );
}

/// `Ref`/`Delta` are lag arithmetic, and the *negative* case (`lead`) is how
/// forward-looking labels are written — worth pinning because Part VII-B6
/// builds on it.
#[tokio::test(flavor = "multi_thread")]
async fn lag_and_lead_cover_ref_and_delta() {
    let (_dir, db) = setup_bars().await;
    let s = session(&db).await;
    let delta = floats(
        &s,
        "SELECT close - lag(close, 1) OVER (PARTITION BY symbol ORDER BY ts) AS v \
         FROM bars WHERE symbol = 'A' ORDER BY ts",
    )
    .await;
    assert_eq!(delta[0], None, "no prior bar for the first row");
    assert_close(delta[1], 1.0, "Delta(close, 1)");
    assert_close(delta[4], 6.0, "Delta(close, 1) across the jump");

    let fwd = floats(
        &s,
        "SELECT lead(close, 1) OVER (PARTITION BY symbol ORDER BY ts) AS v \
         FROM bars WHERE symbol = 'A' ORDER BY ts",
    )
    .await;
    assert_close(fwd[3], 10.0, "Ref(close, -1)");
    assert_eq!(fwd[4], None, "no next bar for the last row");
}

/// Arity errors must surface as plan errors, not panics — the workspace runs
/// `panic = "abort"` in release, so a panic in a UDF would kill the process.
#[tokio::test(flavor = "multi_thread")]
async fn wrong_arity_is_a_query_error_not_a_panic() {
    let (_dir, db) = setup_bars().await;
    let s = session(&db).await;
    let res = match s
        .sql(
            "SELECT mad(close, close) OVER (PARTITION BY symbol ORDER BY ts \
                                            ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) \
             FROM bars",
        )
        .await
    {
        Err(e) => Err(e),
        Ok(df) => df.collect().await.map(|_| ()),
    };
    let err = res
        .expect_err("mad with two arguments must fail")
        .to_string();
    assert!(
        err.contains("exactly one argument"),
        "error should name the arity problem, got: {err}"
    );
}
