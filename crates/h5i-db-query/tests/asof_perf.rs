//! ASOF join performance-path tests: pushdown to child scans, memory-pool
//! accounting for the buffered right side, declared output ordering, and
//! current-thread-runtime support for the planning-time UDTFs.

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::assert_batches_eq;
use h5i_db_core::{Database, TableOptions, WriteOptions};
use h5i_db_query::{H5iSession, SessionOptions};

fn trades_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
        Field::new("size", DataType::Int64, false),
    ]))
}

fn quotes_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("bid", DataType::Float64, false),
        Field::new("ask", DataType::Float64, false),
    ]))
}

fn trades_batch(ts: &[i64], symbols: &[&str], prices: &[f64]) -> RecordBatch {
    let sizes: Vec<i64> = ts.iter().map(|_| 10).collect();
    RecordBatch::try_new(
        trades_schema(),
        vec![
            Arc::new(TimestampNanosecondArray::from(ts.to_vec()).with_timezone("UTC".to_string())),
            Arc::new(StringArray::from(symbols.to_vec())),
            Arc::new(Float64Array::from(prices.to_vec())),
            Arc::new(Int64Array::from(sizes)),
        ],
    )
    .unwrap()
}

fn quotes_batch(ts: &[i64], symbols: &[&str], bids: &[f64]) -> RecordBatch {
    let asks: Vec<f64> = bids.iter().map(|b| b + 0.5).collect();
    RecordBatch::try_new(
        quotes_schema(),
        vec![
            Arc::new(TimestampNanosecondArray::from(ts.to_vec()).with_timezone("UTC".to_string())),
            Arc::new(StringArray::from(symbols.to_vec())),
            Arc::new(Float64Array::from(bids.to_vec())),
            Arc::new(Float64Array::from(asks)),
        ],
    )
    .unwrap()
}

fn time_options() -> TableOptions {
    TableOptions {
        time_column: Some("ts".into()),
        sort_key: vec![],
        storage: Default::default(),
        max_segments_per_manifest: None,
    }
}

async fn setup() -> (tempfile::TempDir, Arc<Database>) {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Database::create(&dir.path().join("db")).await.unwrap());
    (dir, db)
}

/// Small joined dataset: three trades matched backward against three quotes.
async fn small_asof_db() -> (tempfile::TempDir, Arc<Database>) {
    let (dir, db) = setup().await;
    db.create_table("trades", trades_schema(), time_options())
        .await
        .unwrap();
    db.create_table("quotes", quotes_schema(), time_options())
        .await
        .unwrap();
    db.write(
        "trades",
        vec![trades_batch(
            &[1_000, 2_000, 3_000],
            &["A", "A", "B"],
            &[10.0, 20.0, 30.0],
        )],
        WriteOptions::default(),
    )
    .await
    .unwrap();
    db.write(
        "quotes",
        vec![quotes_batch(
            &[500, 1_500, 2_500],
            &["A", "A", "B"],
            &[1.0, 2.0, 3.0],
        )],
        WriteOptions::default(),
    )
    .await
    .unwrap();
    (dir, db)
}

/// 1.4: the planning-time UDTFs (`h5i`, `asof_join`) must work on a
/// current-thread Tokio runtime — `block_in_place` would panic here.
#[tokio::test]
async fn udtfs_work_on_current_thread_runtime() {
    let (_dir, db) = small_asof_db().await;
    let session = H5iSession::new(db, SessionOptions::default())
        .await
        .unwrap();

    let batches = session
        .sql("SELECT count(*) AS n FROM h5i('trades')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_batches_eq!(["+---+", "| n |", "+---+", "| 3 |", "+---+",], &batches);

    let batches = session
        .sql(
            "SELECT count(*) AS n \
             FROM asof_join('trades', 'quotes', 'ts', 'ts', 'symbol')",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_batches_eq!(["+---+", "| n |", "+---+", "| 3 |", "+---+",], &batches);
}

/// 2.2: the buffered right side is charged to the session memory pool, so a
/// `memory_limit` turns a would-be OOM into a clean ResourcesExhausted error.
#[tokio::test(flavor = "multi_thread")]
async fn right_buffer_respects_memory_limit() {
    let (_dir, db) = setup().await;
    db.create_table("trades", trades_schema(), time_options())
        .await
        .unwrap();
    db.create_table("quotes", quotes_schema(), time_options())
        .await
        .unwrap();
    db.write(
        "trades",
        vec![trades_batch(&[1_000], &["A"], &[10.0])],
        WriteOptions::default(),
    )
    .await
    .unwrap();
    // ~30k right rows (> 1 MB buffered) against a 128 KiB limit.
    for chunk in 0..3 {
        let ts: Vec<i64> = (0..10_000)
            .map(|i| chunk * 10_000_000 + i * 1_000)
            .collect();
        let symbols: Vec<&str> = ts.iter().map(|_| "A").collect();
        let bids: Vec<f64> = ts.iter().map(|_| 1.0).collect();
        db.append(
            "quotes",
            vec![quotes_batch(&ts, &symbols, &bids)],
            WriteOptions::default(),
        )
        .await
        .unwrap();
    }

    let limited = H5iSession::new(
        db.clone(),
        SessionOptions {
            memory_limit: Some(128 * 1024),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let err = limited
        .sql("SELECT count(*) FROM asof_join('trades', 'quotes', 'ts', 'ts', 'symbol')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("AsOfJoinExec") || err.contains("Resources exhausted"),
        "expected a memory-pool failure, got: {err}"
    );

    // Without a limit the same query runs.
    let unlimited = H5iSession::new(db, SessionOptions::default())
        .await
        .unwrap();
    let batches = unlimited
        .sql("SELECT count(*) AS n FROM asof_join('trades', 'quotes', 'ts', 'ts', 'symbol')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_batches_eq!(["+---+", "| n |", "+---+", "| 1 |", "+---+",], &batches);
}

/// 2.1: WHERE bounds on the left time column prune segments on *both* child
/// scans (the right side via tolerance-widened bounds).
#[tokio::test(flavor = "multi_thread")]
async fn time_filters_prune_both_child_scans() {
    let (_dir, db) = setup().await;
    db.create_table("trades", trades_schema(), time_options())
        .await
        .unwrap();
    db.create_table("quotes", quotes_schema(), time_options())
        .await
        .unwrap();
    // Ten one-segment appends per table, each covering [i*10_000, i*10_000+9_000].
    for i in 0..10i64 {
        let ts: Vec<i64> = (0..10).map(|j| i * 10_000 + j * 1_000).collect();
        let symbols: Vec<&str> = ts.iter().map(|_| "A").collect();
        let vals: Vec<f64> = ts.iter().map(|_| 1.0).collect();
        db.append(
            "trades",
            vec![trades_batch(&ts, &symbols, &vals)],
            WriteOptions::default(),
        )
        .await
        .unwrap();
        db.append(
            "quotes",
            vec![quotes_batch(&ts, &symbols, &vals)],
            WriteOptions::default(),
        )
        .await
        .unwrap();
    }

    let session = H5iSession::new(db, SessionOptions::default())
        .await
        .unwrap();
    let batches = session
        .sql(
            "SELECT count(*) AS n \
             FROM asof_join('trades', 'quotes', 'ts', 'ts', 'symbol', 'backward', 5000) \
             WHERE ts >= arrow_cast(40000, 'Timestamp(Nanosecond, Some(\"UTC\"))') \
               AND ts <= arrow_cast(49000, 'Timestamp(Nanosecond, Some(\"UTC\"))')",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_batches_eq!(
        ["+----+", "| n  |", "+----+", "| 10 |", "+----+",],
        &batches
    );

    let metrics = session.take_scan_metrics();
    let trades = metrics.iter().find(|m| m.table == "trades").unwrap();
    let quotes = metrics.iter().find(|m| m.table == "quotes").unwrap();
    assert_eq!(trades.segments_total, 10);
    assert_eq!(quotes.segments_total, 10);
    // Left keeps only [40k, 49k]; right keeps [35k, 49k] (bounds widened by
    // the 5000 tolerance).
    assert_eq!(trades.segments_scanned, 1, "left scan not pruned");
    assert_eq!(quotes.segments_scanned, 2, "right scan not pruned");
}

/// 2.1: projections forward to the child scans (unused columns never read)
/// and collision-renamed columns keep their names.
#[tokio::test(flavor = "multi_thread")]
async fn projection_pushdown_narrows_scans_and_keeps_names() {
    let (_dir, db) = small_asof_db().await;
    let session = H5iSession::new(db, SessionOptions::default())
        .await
        .unwrap();
    let df = session
        .sql(
            "SELECT arrow_cast(ts_right, 'Int64') AS tsr, price, bid \
             FROM asof_join('trades', 'quotes', 'ts', 'ts', 'symbol') \
             ORDER BY price",
        )
        .await
        .unwrap();

    // Unprojected columns (trades.size, quotes.ask) must not appear anywhere
    // in the physical plan — they were pruned from the child scans.
    let plan = df.clone().create_physical_plan().await.unwrap();
    let display = datafusion::physical_plan::displayable(plan.as_ref())
        .indent(true)
        .to_string();
    assert!(
        !display.contains("size"),
        "left scan not narrowed:\n{display}"
    );
    assert!(
        !display.contains("ask"),
        "right scan not narrowed:\n{display}"
    );

    let batches = df.collect().await.unwrap();
    assert_batches_eq!(
        [
            "+------+-------+-----+",
            "| tsr  | price | bid |",
            "+------+-------+-----+",
            "| 500  | 10.0  | 1.0 |",
            "| 1500 | 20.0  | 2.0 |",
            "| 2500 | 30.0  | 3.0 |",
            "+------+-------+-----+",
        ],
        &batches
    );
}

/// 2.6: the join declares its output ordering (left time ascending), so an
/// ORDER BY on the join key needs no re-sort.
#[tokio::test(flavor = "multi_thread")]
async fn declared_ordering_elides_order_by_sort() {
    let (_dir, db) = small_asof_db().await;
    let session = H5iSession::new(db, SessionOptions::default())
        .await
        .unwrap();
    let df = session
        .sql(
            "SELECT * FROM asof_join('trades', 'quotes', 'ts', 'ts', 'symbol') \
             ORDER BY ts",
        )
        .await
        .unwrap();
    let plan = df.clone().create_physical_plan().await.unwrap();
    let display = datafusion::physical_plan::displayable(plan.as_ref())
        .indent(true)
        .to_string();
    assert!(
        !display.contains("SortExec"),
        "ORDER BY on the join key should not re-sort:\n{display}"
    );

    let batches = df.collect().await.unwrap();
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 3);
}

/// 2.1: a bare LIMIT forwards to the left scan of a LEFT asof join.
#[tokio::test(flavor = "multi_thread")]
async fn limit_bounds_left_scan() {
    let (_dir, db) = small_asof_db().await;
    let session = H5iSession::new(db, SessionOptions::default())
        .await
        .unwrap();
    let batches = session
        .sql("SELECT * FROM asof_join('trades', 'quotes', 'ts', 'ts', 'symbol') LIMIT 2")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 2);
}
