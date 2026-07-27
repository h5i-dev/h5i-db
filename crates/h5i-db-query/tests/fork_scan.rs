//! Cross-fork scan tests (ROADMAP Part X, X-A2).
//!
//! Two properties, and the second is the reason the feature exists.
//!
//! **Correctness**: `forks('t')` returns exactly what scanning each fork
//! separately and stacking the results returns, with a `__fork` label. That is
//! asserted differentially rather than against hand-written expectations, so
//! the test cannot drift into agreeing with a wrong implementation.
//!
//! **Cost**: a segment shared by N forks is opened once, not N times. Without
//! that, a thousand-branch aggregation reads the base a thousand times and the
//! feature is a convenience wrapper rather than a capability.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Array, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use h5i_db_core::{Database, ReadAt, ScanOptions, TableOptions, WriteOptions};
use h5i_db_query::{H5iSession, SessionOptions};

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
    ]))
}

fn batch(ts: &[i64], symbol: &[&str], price: &[f64]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ts.to_vec())),
            Arc::new(StringArray::from(symbol.to_vec())),
            Arc::new(Float64Array::from(price.to_vec())),
        ],
    )
    .unwrap()
}

fn options() -> TableOptions {
    TableOptions {
        time_column: Some("ts".into()),
        ..Default::default()
    }
}

/// A base `prices` table with three rows, and no forks yet.
async fn setup() -> (tempfile::TempDir, Arc<Database>) {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Database::create(&dir.path().join("db")).await.unwrap());
    db.create_table("prices", schema(), options())
        .await
        .unwrap();
    db.write(
        "prices",
        vec![batch(&[1, 2, 3], &["A", "B", "A"], &[10.0, 20.0, 30.0])],
        WriteOptions::default(),
    )
    .await
    .unwrap();
    (dir, db)
}

async fn session(db: Arc<Database>) -> H5iSession {
    H5iSession::new(db, SessionOptions::default())
        .await
        .unwrap()
}

/// `(fork, price)` pairs from a query result, sorted — the comparable form of
/// a cross-fork scan.
fn labelled_prices(batches: &[RecordBatch]) -> Vec<(String, i64)> {
    let mut out = Vec::new();
    for b in batches {
        let fork = b
            .column_by_name("__fork")
            .expect("__fork column missing")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("__fork should be Utf8");
        let price = b
            .column_by_name("price")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        for i in 0..b.num_rows() {
            out.push((fork.value(i).to_string(), price.value(i) as i64));
        }
    }
    out.sort();
    out
}

/// The same shape, computed the slow way: scan each fork on its own.
async fn per_fork_prices(db: &Database, forks: &[&str]) -> Vec<(String, i64)> {
    let mut out = Vec::new();
    for name in forks {
        let handle = db.open_fork(name).await.unwrap();
        let (batches, _) = handle
            .scan("prices", ReadAt::Latest, ScanOptions::default())
            .await
            .unwrap();
        for b in &batches {
            let price = b
                .column_by_name("price")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            for i in 0..b.num_rows() {
                out.push((name.to_string(), price.value(i) as i64));
            }
        }
    }
    out.sort();
    out
}

/// Build `n` forks; the first `writers` of them append one distinct row each,
/// so some forks share every segment with the base and some do not.
async fn forks_with_writers(db: &Database, n: usize, writers: usize) -> Vec<String> {
    let mut names = Vec::new();
    for i in 0..n {
        let name = format!("f{i:02}");
        db.create_fork(&name, None, None, serde_json::Map::new())
            .await
            .unwrap();
        if i < writers {
            let handle = db.open_fork(&name).await.unwrap();
            handle
                .append(
                    "prices",
                    vec![batch(&[100 + i as i64], &["Z"], &[1000.0 + i as f64])],
                    WriteOptions::default(),
                )
                .await
                .unwrap();
        }
        names.push(name);
    }
    names
}

// ---------------------------------------------------------------------------
// correctness
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_cross_fork_scan_equals_stacked_per_fork_scans() {
    let (_dir, db) = setup().await;
    forks_with_writers(&db, 4, 2).await;

    let s = session(db.clone()).await;
    let got = labelled_prices(
        &s.sql("SELECT * FROM forks('prices')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap(),
    );
    let expected = per_fork_prices(&db, &["f00", "f01", "f02", "f03"]).await;

    assert!(!expected.is_empty(), "fixture produced no rows");
    assert_eq!(got, expected);
    // Two forks wrote a row, two did not: 4 forks x 3 base rows + 2 extra.
    assert_eq!(got.len(), 14);
}

#[tokio::test]
async fn a_named_subset_returns_only_those_forks() {
    let (_dir, db) = setup().await;
    forks_with_writers(&db, 4, 2).await;

    let s = session(db.clone()).await;
    let got = labelled_prices(
        &s.sql("SELECT * FROM forks('prices', 'f00,f03')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap(),
    );
    assert_eq!(got, per_fork_prices(&db, &["f00", "f03"]).await);
}

/// A fork that never wrote shares every segment with the base; a fork that did
/// has one of its own. Both must appear, and the shared rows must not be
/// attributed to the wrong branch.
#[tokio::test]
async fn shadowed_and_untouched_forks_are_both_represented() {
    let (_dir, db) = setup().await;
    forks_with_writers(&db, 2, 1).await;

    let s = session(db.clone()).await;
    let rows = labelled_prices(
        &s.sql("SELECT * FROM forks('prices')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap(),
    );
    let by_fork: BTreeMap<String, Vec<i64>> =
        rows.into_iter().fold(BTreeMap::new(), |mut m, (f, p)| {
            m.entry(f).or_default().push(p);
            m
        });
    assert_eq!(by_fork["f00"], vec![10, 20, 30, 1000], "the writer's rows");
    assert_eq!(
        by_fork["f01"],
        vec![10, 20, 30],
        "the untouched fork's rows"
    );
}

/// A table that exists in only some forks contributes rows from those and
/// silence from the rest — not an error, because "this branch never created
/// that table" is a normal state mid-exploration.
#[tokio::test]
async fn forks_without_the_table_contribute_no_rows() {
    let (_dir, db) = setup().await;
    for name in ["has", "hasnt"] {
        db.create_fork(name, None, None, serde_json::Map::new())
            .await
            .unwrap();
    }
    let handle = db.open_fork("has").await.unwrap();
    handle
        .create_table("scratch", schema(), options())
        .await
        .unwrap();
    handle
        .write(
            "scratch",
            vec![batch(&[7], &["S"], &[70.0])],
            WriteOptions::default(),
        )
        .await
        .unwrap();

    let s = session(db.clone()).await;
    let rows = labelled_prices(
        &s.sql("SELECT * FROM forks('scratch')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap(),
    );
    assert_eq!(rows, vec![("has".to_string(), 70)]);
}

#[tokio::test]
async fn a_grouped_aggregate_over_forks_reports_one_row_per_fork() {
    let (_dir, db) = setup().await;
    forks_with_writers(&db, 3, 1).await;

    let s = session(db.clone()).await;
    let batches = s
        .sql("SELECT __fork, count(*) AS n FROM forks('prices') GROUP BY __fork ORDER BY __fork")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut got = Vec::new();
    for b in &batches {
        let fork = b
            .column_by_name("__fork")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let n = b
            .column_by_name("n")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..b.num_rows() {
            got.push((fork.value(i).to_string(), n.value(i)));
        }
    }
    assert_eq!(
        got,
        vec![
            ("f00".to_string(), 4),
            ("f01".to_string(), 3),
            ("f02".to_string(), 3),
        ]
    );
}

/// An empty table across forks is zero rows, not a planning failure: a branch
/// that deleted everything is a legitimate outcome to aggregate over.
#[tokio::test]
async fn an_empty_table_yields_no_rows_rather_than_failing() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Database::create(&dir.path().join("db")).await.unwrap());
    db.create_table("prices", schema(), options())
        .await
        .unwrap();
    db.create_fork("f", None, None, serde_json::Map::new())
        .await
        .unwrap();

    let s = session(db.clone()).await;
    let batches = s
        .sql("SELECT * FROM forks('prices')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 0);
}

// ---------------------------------------------------------------------------
// cost: shared segments are opened once
// ---------------------------------------------------------------------------

/// The load-bearing claim. Ten forks over a table none of them modified share
/// every segment, so the scan must open each segment exactly once — the same
/// number it would open for a single fork.
#[tokio::test]
async fn a_segment_shared_by_every_fork_is_scanned_once() {
    let (_dir, db) = setup().await;
    // Three segments in the base, so the count is not trivially one.
    for i in 0..2i64 {
        db.append(
            "prices",
            vec![batch(&[10 + i], &["C"], &[1.0])],
            WriteOptions::default(),
        )
        .await
        .unwrap();
    }
    let base_segments = db
        .resolve("prices", ReadAt::Latest)
        .await
        .unwrap()
        .manifest
        .segments
        .len();
    assert_eq!(base_segments, 3, "fixture should have three segments");

    forks_with_writers(&db, 10, 0).await;

    let s = session(db.clone()).await;
    s.take_scan_metrics();
    let rows: usize = s
        .sql("SELECT * FROM forks('prices')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum();
    let scanned: usize = s
        .take_scan_metrics()
        .iter()
        .map(|m| m.segments_scanned)
        .sum();

    // Ten forks' worth of rows...
    assert_eq!(rows, 50, "10 forks x 5 rows");
    // ...from three segment reads, not thirty.
    assert_eq!(
        scanned, base_segments,
        "a segment shared by 10 forks was scanned {scanned} times instead of {base_segments}"
    );
}

/// The mixed case: forks that wrote have a private segment each, forks that
/// did not share the base's. Total reads = distinct segments, regardless of
/// how the sharing is distributed.
#[tokio::test]
async fn each_distinct_segment_is_scanned_exactly_once() {
    let (_dir, db) = setup().await;
    let names = forks_with_writers(&db, 6, 3).await;

    // The true number of distinct segment paths across all forks.
    let mut distinct = std::collections::BTreeSet::new();
    for name in &names {
        let handle = db.open_fork(name).await.unwrap();
        for seg in handle
            .resolve("prices", ReadAt::Latest)
            .await
            .unwrap()
            .manifest
            .segments
        {
            distinct.insert(seg.path);
        }
    }

    let s = session(db.clone()).await;
    s.take_scan_metrics();
    s.sql("SELECT * FROM forks('prices')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let scanned: usize = s
        .take_scan_metrics()
        .iter()
        .map(|m| m.segments_scanned)
        .sum();

    assert_eq!(
        scanned,
        distinct.len(),
        "expected one scan per distinct segment ({}), got {scanned}",
        distinct.len()
    );
    // Sanity: the fixture really does mix shared and private segments.
    assert!(
        distinct.len() > 1 && distinct.len() < names.len() * 2,
        "fixture is degenerate: {} distinct segments over {} forks",
        distinct.len(),
        names.len()
    );
}

/// A filter must still prune segments inside the union, or the cross-fork path
/// would quietly give up the pruning every other scan gets.
#[tokio::test]
async fn predicates_still_prune_segments_under_the_union() {
    let (_dir, db) = setup().await;
    for i in 0..3i64 {
        db.append(
            "prices",
            vec![batch(&[100 + i * 10], &["C"], &[1.0])],
            WriteOptions::default(),
        )
        .await
        .unwrap();
    }
    forks_with_writers(&db, 4, 0).await;

    let s = session(db.clone()).await;
    s.take_scan_metrics();
    s.sql("SELECT * FROM forks('prices') WHERE ts > 1000")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let metrics = s.take_scan_metrics();
    let scanned: usize = metrics.iter().map(|m| m.segments_scanned).sum();
    let pruned: usize = metrics.iter().map(|m| m.segments_pruned).sum();

    assert!(pruned > 0, "an unsatisfiable range pruned nothing");
    assert_eq!(scanned, 0, "no segment can satisfy ts > 1000");
}

// ---------------------------------------------------------------------------
// refusals
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_named_fork_that_does_not_exist_is_an_error() {
    let (_dir, db) = setup().await;
    forks_with_writers(&db, 1, 0).await;

    let s = session(db.clone()).await;
    let err = s
        .sql("SELECT * FROM forks('prices', 'f00,nope')")
        .await
        .unwrap_err();
    assert!(
        format!("{err}").contains("nope"),
        "the error should name the missing fork: {err}"
    );
}

#[tokio::test]
async fn forks_that_disagree_on_the_schema_are_refused_with_a_pointer_to_diff() {
    let (_dir, db) = setup().await;
    forks_with_writers(&db, 2, 1).await;
    // Evolve the schema inside one fork only.
    let handle = db.open_fork("f00").await.unwrap();
    handle
        .evolve_schema(
            "prices",
            Arc::new(Schema::new(vec![
                Field::new("ts", DataType::Int64, false),
                Field::new("symbol", DataType::Utf8, false),
                Field::new("price", DataType::Float64, false),
                Field::new("tier", DataType::Int64, true),
            ])),
            WriteOptions::default(),
        )
        .await
        .unwrap();

    let s = session(db.clone()).await;
    let err = s.sql("SELECT * FROM forks('prices')").await.unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("disagree on the schema"), "{msg}");
    assert!(msg.contains("fork diff"), "should point at the tool: {msg}");
}

#[tokio::test]
async fn a_database_with_no_forks_says_so() {
    let (_dir, db) = setup().await;
    let s = session(db.clone()).await;
    let err = s.sql("SELECT * FROM forks('prices')").await.unwrap_err();
    assert!(
        format!("{err}").contains("no forks"),
        "expected a clear empty-database message, got: {err}"
    );
}

#[tokio::test]
async fn a_bad_argument_list_is_rejected_at_planning_time() {
    let (_dir, db) = setup().await;
    forks_with_writers(&db, 1, 0).await;
    let s = session(db.clone()).await;

    assert!(s.sql("SELECT * FROM forks()").await.is_err());
    assert!(
        s.sql("SELECT * FROM forks('prices', 'a', 'b')")
            .await
            .is_err()
    );
    assert!(s.sql("SELECT * FROM forks(42)").await.is_err());
    assert!(
        s.sql("SELECT * FROM forks('prices', 'f00,,x')")
            .await
            .is_err()
    );
}
