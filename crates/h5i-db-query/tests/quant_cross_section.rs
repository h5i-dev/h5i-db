//! Part VII-B2 cross-sectional operators, exercised through real SQL.
//!
//! Three jobs:
//!
//! 1. Prove `cs_rank` / `cs_winsorize` reach the planner and partition by the
//!    time bucket correctly (the numerics live in `src/cross_section.rs` unit
//!    tests).
//! 2. Prove `cs_demean` / `cs_zscore` need no operator, by pinning the plain-SQL
//!    forms against reference values. Part VII-B2 names four operators; two of
//!    them are a window aggregate partitioned by time, and this file is the
//!    evidence that the capability is delivered without duplicating engine code.
//! 3. Show explicitly that `cs_rank` is **not** reproducible with
//!    `percent_rank` or `cume_dist`. That comparison is the entire
//!    justification for the operator existing, so it is asserted rather than
//!    asserted-in-a-comment.

use std::sync::Arc;

use arrow::array::{Array, Float64Array, RecordBatch, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use h5i_db_core::{Database, TableOptions, WriteOptions};
use h5i_db_query::{H5iSession, SessionOptions};

fn factors_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("factor", DataType::Float64, true),
    ]))
}

/// Two cross-sections. `t1` carries a deliberate tie (20, 20) because ties are
/// exactly where the SQL ranking built-ins diverge from pandas; `t2` is
/// tie-free and has different magnitudes, so partition isolation is visible.
async fn setup_factors() -> (tempfile::TempDir, Arc<Database>) {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Database::create(&dir.path().join("db")).await.unwrap());
    db.create_table(
        "factors",
        factors_schema(),
        TableOptions {
            time_column: Some("ts".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let t1 = 1_000_000_000i64;
    let t2 = 2_000_000_000i64;
    let ts = vec![t1, t1, t1, t1, t2, t2, t2, t2];
    let symbols = vec!["A", "B", "C", "D", "A", "B", "C", "D"];
    let factors = vec![
        Some(10.0),
        Some(20.0),
        Some(20.0),
        Some(40.0),
        Some(1.0),
        Some(2.0),
        Some(3.0),
        Some(4.0),
    ];
    db.append(
        "factors",
        vec![
            RecordBatch::try_new(
                factors_schema(),
                vec![
                    Arc::new(TimestampNanosecondArray::from(ts).with_timezone("UTC".to_string())),
                    Arc::new(StringArray::from(symbols)),
                    Arc::new(Float64Array::from(factors)),
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

/// Run `sql` and return `(symbol, value)` pairs for the first cross-section,
/// keyed so assertions read by symbol rather than by row position.
async fn by_symbol(s: &H5iSession, sql: &str) -> Vec<(String, Option<f64>)> {
    let batches = s
        .sql(sql)
        .await
        .unwrap_or_else(|e| panic!("query failed: {sql}\n{e}"))
        .collect()
        .await
        .unwrap_or_else(|e| panic!("collect failed: {sql}\n{e}"));
    let mut out = Vec::new();
    for b in batches {
        let sym = b
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("column 0 must be the symbol")
            .clone();
        let val = arrow::compute::cast(b.column(1), &DataType::Float64).unwrap();
        let val = val.as_any().downcast_ref::<Float64Array>().unwrap().clone();
        for i in 0..sym.len() {
            out.push((
                sym.value(i).to_string(),
                if val.is_valid(i) {
                    Some(val.value(i))
                } else {
                    None
                },
            ));
        }
    }
    out
}

fn get(rows: &[(String, Option<f64>)], symbol: &str) -> Option<f64> {
    rows.iter()
        .find(|(s, _)| s == symbol)
        .unwrap_or_else(|| panic!("symbol {symbol} missing from result"))
        .1
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
// cs_rank
// ---------------------------------------------------------------------------

/// The tied cross-section (10, 20, 20, 40) ranks to (0.25, 0.625, 0.625, 1.0)
/// under pandas' averaging convention.
#[tokio::test(flavor = "multi_thread")]
async fn cs_rank_matches_pandas_convention_through_sql() {
    let (_dir, db) = setup_factors().await;
    let s = session(&db).await;
    let rows = by_symbol(
        &s,
        "SELECT symbol, cs_rank(factor) OVER (PARTITION BY ts) AS v \
         FROM factors WHERE ts = '1970-01-01T00:00:01Z' ORDER BY symbol",
    )
    .await;
    assert_close(get(&rows, "A"), 0.25, "A");
    assert_close(get(&rows, "B"), 0.625, "B (tied)");
    assert_close(get(&rows, "C"), 0.625, "C (tied)");
    assert_close(get(&rows, "D"), 1.0, "D");
}

/// The justification test. `cs_rank` must differ from both SQL ranking
/// built-ins on the tie, or it would be redundant. If a future DataFusion
/// gains a pandas-compatible ranking function, this test is where that shows
/// up as an opportunity to delete code.
#[tokio::test(flavor = "multi_thread")]
async fn cs_rank_is_not_reproducible_with_percent_rank_or_cume_dist() {
    let (_dir, db) = setup_factors().await;
    let s = session(&db).await;
    // Each ranking function is queried separately so every comparison below
    // names exactly which pair it is contrasting.
    let rows = by_symbol(
        &s,
        "SELECT symbol, cs_rank(factor) OVER (PARTITION BY ts) AS v \
         FROM factors WHERE ts = '1970-01-01T00:00:01Z' ORDER BY symbol",
    )
    .await;
    assert_close(get(&rows, "A"), 0.25, "cs_rank(A)");

    let pr = by_symbol(
        &s,
        "SELECT symbol, percent_rank() OVER (PARTITION BY ts ORDER BY factor) AS v \
         FROM factors WHERE ts = '1970-01-01T00:00:01Z' ORDER BY symbol",
    )
    .await;
    // percent_rank puts the smallest value at exactly 0.0; cs_rank never does.
    assert_close(get(&pr, "A"), 0.0, "percent_rank(A)");
    assert!(
        get(&pr, "A") != get(&rows, "A"),
        "percent_rank must differ from cs_rank at the bottom of the cross-section"
    );

    let cd = by_symbol(
        &s,
        "SELECT symbol, cume_dist() OVER (PARTITION BY ts ORDER BY factor) AS v \
         FROM factors WHERE ts = '1970-01-01T00:00:01Z' ORDER BY symbol",
    )
    .await;
    // cume_dist gives ties the TOP of their band (0.75), pandas the mean (0.625).
    assert_close(get(&cd, "B"), 0.75, "cume_dist(B)");
    assert!(
        get(&cd, "B") != get(&rows, "B"),
        "cume_dist must differ from cs_rank on ties"
    );
}

/// Cross-sections must be independent: `t2`'s much smaller values may not
/// affect `t1`'s ranks, and both must span the full 0..1 range.
#[tokio::test(flavor = "multi_thread")]
async fn cs_rank_partitions_are_independent() {
    let (_dir, db) = setup_factors().await;
    let s = session(&db).await;
    let t2 = by_symbol(
        &s,
        "SELECT symbol, cs_rank(factor) OVER (PARTITION BY ts) AS v \
         FROM factors WHERE ts = '1970-01-01T00:00:02Z' ORDER BY symbol",
    )
    .await;
    // t2 = (1,2,3,4), all distinct → quartiles.
    assert_close(get(&t2, "A"), 0.25, "t2 A");
    assert_close(get(&t2, "B"), 0.5, "t2 B");
    assert_close(get(&t2, "C"), 0.75, "t2 C");
    assert_close(get(&t2, "D"), 1.0, "t2 D");
}

/// The realistic factor form: partition by a bucket rather than a raw
/// timestamp, so bars that share a day rank together.
#[tokio::test(flavor = "multi_thread")]
async fn cs_rank_composes_with_time_bucket() {
    let (_dir, db) = setup_factors().await;
    let s = session(&db).await;
    let rows = by_symbol(
        &s,
        "SELECT symbol, cs_rank(factor) OVER (PARTITION BY time_bucket('1d', ts)) AS v \
         FROM factors ORDER BY symbol, ts",
    )
    .await;
    // All 8 rows fall in one day, so the cross-section is the whole table:
    // sorted = 1,2,3,4,10,20,20,40 (n = 8). A appears twice (10 and 1).
    assert_eq!(rows.len(), 8, "expected every row to be ranked");
    // The smallest value overall is t2's A = 1.0 → rank 1/8.
    let a_values: Vec<Option<f64>> = rows
        .iter()
        .filter(|(s, _)| s == "A")
        .map(|(_, v)| *v)
        .collect();
    assert!(
        a_values.contains(&Some(0.125)),
        "the global minimum must rank at 1/8, got {a_values:?}"
    );
}

// ---------------------------------------------------------------------------
// cs_demean / cs_zscore: delivered as plain SQL
// ---------------------------------------------------------------------------

/// `cs_demean` needs no operator: subtracting a partitioned mean is a window
/// aggregate. Pinned here so the claim in `src/cross_section.rs` is tested.
#[tokio::test(flavor = "multi_thread")]
async fn cs_demean_is_plain_sql() {
    let (_dir, db) = setup_factors().await;
    let s = session(&db).await;
    let rows = by_symbol(
        &s,
        "SELECT symbol, factor - avg(factor) OVER (PARTITION BY ts) AS v \
         FROM factors WHERE ts = '1970-01-01T00:00:01Z' ORDER BY symbol",
    )
    .await;
    // mean(10, 20, 20, 40) = 22.5
    assert_close(get(&rows, "A"), -12.5, "demean A");
    assert_close(get(&rows, "B"), -2.5, "demean B");
    assert_close(get(&rows, "C"), -2.5, "demean C");
    assert_close(get(&rows, "D"), 17.5, "demean D");
    // A demeaned cross-section sums to zero, which is the defining property.
    let total: f64 = rows.iter().filter_map(|(_, v)| *v).sum();
    assert!(
        total.abs() < 1e-9,
        "demeaned values must sum to 0, got {total}"
    );
}

/// `cs_zscore` likewise. `stddev` is `stddev_samp` (ddof = 1), matching
/// pandas' `.std()` default and therefore qlib's `CSZScoreNorm`.
#[tokio::test(flavor = "multi_thread")]
async fn cs_zscore_is_plain_sql_and_uses_the_sample_deviation() {
    let (_dir, db) = setup_factors().await;
    let s = session(&db).await;
    let rows = by_symbol(
        &s,
        "SELECT symbol, (factor - avg(factor) OVER (PARTITION BY ts)) \
                        / stddev(factor) OVER (PARTITION BY ts) AS v \
         FROM factors WHERE ts = '1970-01-01T00:00:01Z' ORDER BY symbol",
    )
    .await;
    // Deviations -12.5, -2.5, -2.5, 17.5; SS = 475; sample var = 475/3.
    let sd = (475.0_f64 / 3.0).sqrt();
    assert_close(get(&rows, "A"), -12.5 / sd, "zscore A");
    assert_close(get(&rows, "D"), 17.5 / sd, "zscore D");

    // Cross-check the ddof choice: with a population deviation (ddof = 0) the
    // divisor would be sqrt(475/4) and A's z-score would be materially larger.
    let population_would_be = -12.5 / (475.0_f64 / 4.0).sqrt();
    let got = get(&rows, "A").unwrap();
    assert!(
        (got - population_would_be).abs() > 1e-3,
        "stddev must be the sample (ddof = 1) form, not the population form"
    );
}

// ---------------------------------------------------------------------------
// cs_winsorize
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn cs_winsorize_clips_the_lower_tail() {
    let (_dir, db) = setup_factors().await;
    let s = session(&db).await;
    let rows = by_symbol(
        &s,
        "SELECT symbol, cs_winsorize(factor, 0.25, 1.0) OVER (PARTITION BY ts) AS v \
         FROM factors WHERE ts = '1970-01-01T00:00:01Z' ORDER BY symbol",
    )
    .await;
    // n = 4, sorted (10, 20, 20, 40). lower cutoff = floor(0.25·4) = 1, so the
    // single lowest value is pulled up to sorted[1] = 20; the top is untouched.
    assert_close(get(&rows, "A"), 20.0, "A pulled up");
    assert_close(get(&rows, "B"), 20.0, "B unchanged");
    assert_close(get(&rows, "D"), 40.0, "D unchanged at upper_pct = 1");
}

#[tokio::test(flavor = "multi_thread")]
async fn cs_winsorize_clips_the_upper_tail() {
    let (_dir, db) = setup_factors().await;
    let s = session(&db).await;
    let rows = by_symbol(
        &s,
        "SELECT symbol, cs_winsorize(factor, 0.0, 0.75) OVER (PARTITION BY ts) AS v \
         FROM factors WHERE ts = '1970-01-01T00:00:01Z' ORDER BY symbol",
    )
    .await;
    // upper cutoff = ceil(0.75·4) = 3 → everything from sorted position 3 on
    // is pulled down to sorted[2] = 20. Only D (40) qualifies.
    assert_close(get(&rows, "A"), 10.0, "A unchanged at lower_pct = 0");
    assert_close(get(&rows, "D"), 20.0, "D pulled down");
}

/// Winsorizing must never invent a value: the outlier is replaced by a real
/// member of the same cross-section.
#[tokio::test(flavor = "multi_thread")]
async fn cs_winsorize_replaces_outliers_with_real_peer_values() {
    let (_dir, db) = setup_factors().await;
    let s = session(&db).await;
    let rows = by_symbol(
        &s,
        "SELECT symbol, cs_winsorize(factor, 0.0, 0.75) OVER (PARTITION BY ts) AS v \
         FROM factors WHERE ts = '1970-01-01T00:00:02Z' ORDER BY symbol",
    )
    .await;
    // t2 = (1,2,3,4); upper cutoff = 3 → D pulled down to 3.0, an actual member.
    assert_close(get(&rows, "D"), 3.0, "D pulled to a real peer value");
}

/// An out-of-range percentile must be a query error, not a panic: the release
/// profile aborts on panic, so a bad literal would take the process down.
#[tokio::test(flavor = "multi_thread")]
async fn cs_winsorize_rejects_out_of_range_percentiles() {
    let (_dir, db) = setup_factors().await;
    let s = session(&db).await;
    for (lo, hi, expect) in [
        ("1.5", "1.0", "must be in [0, 1]"),
        ("0.0", "-0.2", "must be in [0, 1]"),
        ("0.8", "0.2", "must not exceed"),
    ] {
        let sql =
            format!("SELECT cs_winsorize(factor, {lo}, {hi}) OVER (PARTITION BY ts) FROM factors");
        let res = match s.sql(&sql).await {
            Err(e) => Err(e),
            Ok(df) => df.collect().await.map(|_| ()),
        };
        let err = res.unwrap_err().to_string();
        assert!(
            err.contains(expect),
            "cs_winsorize({lo}, {hi}): expected an error containing {expect:?}, got: {err}"
        );
    }
}

/// Arity is validated at planning time for both operators.
#[tokio::test(flavor = "multi_thread")]
async fn cross_sectional_arity_errors_are_actionable() {
    let (_dir, db) = setup_factors().await;
    let s = session(&db).await;
    for (sql, expect) in [
        (
            "SELECT cs_rank(factor, factor) OVER (PARTITION BY ts) FROM factors",
            "exactly one argument",
        ),
        (
            "SELECT cs_winsorize(factor, 0.1) OVER (PARTITION BY ts) FROM factors",
            "exactly three",
        ),
    ] {
        let res = match s.sql(sql).await {
            Err(e) => Err(e),
            Ok(df) => df.collect().await.map(|_| ()),
        };
        let err = res.unwrap_err().to_string();
        assert!(
            err.contains(expect),
            "expected an error containing {expect:?}, got: {err}"
        );
    }
}
