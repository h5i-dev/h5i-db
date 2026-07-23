//! Query-layer integration tests: SQL over versioned tables, manifest
//! pruning, time travel, time_bucket, and ASOF join correctness.

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::assert_batches_eq;
use h5i_db_core::{Database, ReadAt, StorageOptions, TableOptions, WriteOptions};
use h5i_db_query::{asof_join, AsOfDirection, AsOfOptions, H5iSession, SessionOptions};

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

fn time_options(small_segments: bool) -> TableOptions {
    TableOptions {
        time_column: Some("ts".into()),
        sort_key: vec![],
        storage: if small_segments {
            StorageOptions {
                target_segment_bytes: 8 * 1024,
                target_row_group_bytes: 4 * 1024,
                ..Default::default()
            }
        } else {
            StorageOptions::default()
        },
        max_segments_per_manifest: None,
    }
}

async fn setup() -> (tempfile::TempDir, Arc<Database>) {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Database::create(&dir.path().join("db")).await.unwrap());
    (dir, db)
}

#[tokio::test(flavor = "multi_thread")]
async fn sql_roundtrip_and_aggregation() {
    let (_dir, db) = setup().await;
    db.create_table("trades", trades_schema(), time_options(false))
        .await
        .unwrap();
    db.write(
        "trades",
        vec![trades_batch(
            &[1_000, 2_000, 3_000, 4_000],
            &["A", "B", "A", "B"],
            &[10.0, 20.0, 12.0, 22.0],
        )],
        WriteOptions::default(),
    )
    .await
    .unwrap();

    let session = H5iSession::new(db, SessionOptions::default())
        .await
        .unwrap();
    let df = session
        .sql(
            "SELECT symbol, avg(price) AS avg_price, sum(size) AS total \
              FROM trades GROUP BY symbol ORDER BY symbol",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert_batches_eq!(
        [
            "+--------+-----------+-------+",
            "| symbol | avg_price | total |",
            "+--------+-----------+-------+",
            "| A      | 11.0      | 20    |",
            "| B      | 21.0      | 20    |",
            "+--------+-----------+-------+",
        ],
        &batches
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn time_range_queries_prune_segments() {
    let (_dir, db) = setup().await;
    db.create_table("trades", trades_schema(), time_options(true))
        .await
        .unwrap();
    // Three disjoint time blocks → ≥3 segments.
    for base in [0i64, 1_000_000, 2_000_000] {
        let ts: Vec<i64> = (base..base + 400).collect();
        let syms: Vec<&str> = ts.iter().map(|_| "A").collect();
        let prices: Vec<f64> = ts.iter().map(|t| *t as f64).collect();
        db.append(
            "trades",
            vec![trades_batch(&ts, &syms, &prices)],
            WriteOptions::default(),
        )
        .await
        .unwrap();
    }
    let session = H5iSession::new(db, SessionOptions::default())
        .await
        .unwrap();

    let df = session
        .sql(
            "SELECT count(*) FROM trades \
             WHERE ts >= to_timestamp_nanos(1000000) AND ts < to_timestamp_nanos(1000100)",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 100);

    // The pruning must have skipped the other time blocks.
    let metrics = session.take_scan_metrics();
    let m = metrics
        .iter()
        .find(|m| m.table == "trades")
        .expect("scan metrics recorded");
    assert!(
        m.segments_pruned >= 2,
        "expected >=2 segments pruned, got {m:?}"
    );
    assert!(m.segments_scanned < m.segments_total);
}

#[tokio::test(flavor = "multi_thread")]
async fn time_travel_via_table_function() {
    let (_dir, db) = setup().await;
    db.create_table("t", trades_schema(), time_options(false))
        .await
        .unwrap();
    db.write(
        "t",
        vec![trades_batch(&[1], &["A"], &[1.0])],
        WriteOptions::default(),
    )
    .await
    .unwrap();
    db.append(
        "t",
        vec![trades_batch(&[2, 3], &["B", "C"], &[2.0, 3.0])],
        WriteOptions::default(),
    )
    .await
    .unwrap();
    db.create_snapshot("pin", &["t".into()], None)
        .await
        .unwrap();
    db.append(
        "t",
        vec![trades_batch(&[4], &["D"], &[4.0])],
        WriteOptions::default(),
    )
    .await
    .unwrap();

    let session = H5iSession::new(db, SessionOptions::default())
        .await
        .unwrap();
    let count = |sql: &str| {
        let session = &session;
        let sql = sql.to_string();
        async move {
            let batches = session.sql(&sql).await.unwrap().collect().await.unwrap();
            batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
        }
    };

    assert_eq!(count("SELECT count(*) FROM h5i('t')").await, 4);
    assert_eq!(count("SELECT count(*) FROM h5i('t', 1)").await, 1);
    assert_eq!(count("SELECT count(*) FROM h5i('t', 2)").await, 3);
    assert_eq!(count("SELECT count(*) FROM h5i('t', 'pin')").await, 3);
    // Registered table name = session-bound latest.
    assert_eq!(count("SELECT count(*) FROM t").await, 4);
}

#[tokio::test(flavor = "multi_thread")]
async fn time_bucket_ohlc_style_rollup() {
    let (_dir, db) = setup().await;
    db.create_table("trades", trades_schema(), time_options(false))
        .await
        .unwrap();
    // Two 1-minute buckets of nanosecond data.
    let minute = 60 * 1_000_000_000i64;
    let ts = vec![0, 10, minute - 1, minute, minute + 5];
    db.write(
        "trades",
        vec![trades_batch(
            &ts,
            &["A", "A", "A", "A", "A"],
            &[1.0, 2.0, 3.0, 10.0, 20.0],
        )],
        WriteOptions::default(),
    )
    .await
    .unwrap();

    let session = H5iSession::new(db, SessionOptions::default())
        .await
        .unwrap();
    let df = session
        .sql(
            "SELECT time_bucket('1m', ts) AS bucket, \
                    first_value(price ORDER BY ts) AS open, \
                    max(price) AS high, min(price) AS low, \
                    last_value(price ORDER BY ts) AS close, \
                    count(*) AS n \
             FROM trades GROUP BY bucket ORDER BY bucket",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert_batches_eq!(
        [
            "+----------------------+------+------+------+-------+---+",
            "| bucket               | open | high | low  | close | n |",
            "+----------------------+------+------+------+-------+---+",
            "| 1970-01-01T00:00:00Z | 1.0  | 3.0  | 1.0  | 3.0   | 3 |",
            "| 1970-01-01T00:01:00Z | 10.0 | 20.0 | 10.0 | 20.0  | 2 |",
            "+----------------------+------+------+------+-------+---+",
        ],
        &batches
    );
}

// ---------------------------------------------------------------------------
// ASOF join
// ---------------------------------------------------------------------------

async fn asof_setup() -> (tempfile::TempDir, Arc<Database>) {
    let (dir, db) = setup().await;
    db.create_table("trades", trades_schema(), time_options(false))
        .await
        .unwrap();
    db.create_table("quotes", quotes_schema(), time_options(false))
        .await
        .unwrap();
    // Trades at t=5, 15, 25 for A; t=10 for B.
    db.write(
        "trades",
        vec![trades_batch(
            &[5, 10, 15, 25],
            &["A", "B", "A", "A"],
            &[100.0, 200.0, 101.0, 102.0],
        )],
        WriteOptions::default(),
    )
    .await
    .unwrap();
    // Quotes for A at t=0, 12, 20; for B at t=11 (after B's trade).
    db.write(
        "quotes",
        vec![quotes_batch(
            &[0, 11, 12, 20],
            &["A", "B", "A", "A"],
            &[99.0, 199.0, 100.5, 101.5],
        )],
        WriteOptions::default(),
    )
    .await
    .unwrap();
    (dir, db)
}

#[tokio::test(flavor = "multi_thread")]
async fn asof_join_dataframe_backward_by_symbol() {
    let (_dir, db) = asof_setup().await;
    let session = H5iSession::new(db, SessionOptions::default())
        .await
        .unwrap();
    let trades = session.read_table("trades", ReadAt::Latest).await.unwrap();
    let quotes = session.read_table("quotes", ReadAt::Latest).await.unwrap();

    let joined = asof_join(
        trades,
        quotes,
        AsOfOptions {
            left_on: "ts".into(),
            right_on: "ts".into(),
            by: vec![("symbol".into(), "symbol".into())],
            direction: AsOfDirection::Backward,
            tolerance: None,
            inner: false,
        },
    )
    .unwrap();
    let batches = joined
        .sort_by(vec![datafusion::prelude::col("ts")])
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_batches_eq!(
        [
            "+--------------------------------+--------+-------+------+--------------------------------+-------+-------+",
            "| ts                             | symbol | price | size | ts_right                       | bid   | ask   |",
            "+--------------------------------+--------+-------+------+--------------------------------+-------+-------+",
            "| 1970-01-01T00:00:00.000000005Z | A      | 100.0 | 10   | 1970-01-01T00:00:00Z           | 99.0  | 99.5  |",
            "| 1970-01-01T00:00:00.000000010Z | B      | 200.0 | 10   |                                |       |       |",
            "| 1970-01-01T00:00:00.000000015Z | A      | 101.0 | 10   | 1970-01-01T00:00:00.000000012Z | 100.5 | 101.0 |",
            "| 1970-01-01T00:00:00.000000025Z | A      | 102.0 | 10   | 1970-01-01T00:00:00.000000020Z | 101.5 | 102.0 |",
            "+--------------------------------+--------+-------+------+--------------------------------+-------+-------+",
        ],
        &batches
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn asof_join_sql_function_with_tolerance_and_forward() {
    let (_dir, db) = asof_setup().await;
    let session = H5iSession::new(db, SessionOptions::default())
        .await
        .unwrap();

    // Backward with tolerance 4ns: trade A@15 matches quote A@12 (diff 3);
    // trade A@5 has quote A@0 at diff 5 → no match; A@25 vs A@20 diff 5 → no.
    let batches = session
        .sql(
            "SELECT symbol, price, bid FROM \
             asof_join('trades', 'quotes', 'ts', 'ts', 'symbol', 'backward', 4) \
             WHERE bid IS NOT NULL ORDER BY ts",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_batches_eq!(
        [
            "+--------+-------+-------+",
            "| symbol | price | bid   |",
            "+--------+-------+-------+",
            "| A      | 101.0 | 100.5 |",
            "+--------+-------+-------+",
        ],
        &batches
    );

    // Forward: trade B@10 matches quote B@11.
    let batches = session
        .sql(
            "SELECT symbol, price, bid FROM \
             asof_join('trades', 'quotes', 'ts', 'ts', 'symbol', 'forward') \
             WHERE symbol = 'B' ORDER BY ts",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_batches_eq!(
        [
            "+--------+-------+-------+",
            "| symbol | price | bid   |",
            "+--------+-------+-------+",
            "| B      | 200.0 | 199.0 |",
            "+--------+-------+-------+",
        ],
        &batches
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn asof_join_matches_ties_and_picks_latest_of_equal_times() {
    let (_dir, db) = setup().await;
    db.create_table("l", trades_schema(), time_options(false))
        .await
        .unwrap();
    db.create_table("r", quotes_schema(), time_options(false))
        .await
        .unwrap();
    db.write(
        "l",
        vec![trades_batch(&[10], &["A"], &[1.0])],
        WriteOptions::default(),
    )
    .await
    .unwrap();
    // Two quotes at exactly t=10: the later row in input order wins ("most
    // recent"), matching pandas/DuckDB semantics.
    db.write(
        "r",
        vec![quotes_batch(&[10, 10], &["A", "A"], &[1.0, 2.0])],
        WriteOptions::default(),
    )
    .await
    .unwrap();

    let session = H5iSession::new(db, SessionOptions::default())
        .await
        .unwrap();
    let batches = session
        .sql("SELECT bid FROM asof_join('l', 'r', 'ts', 'ts', 'symbol')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let bid = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(0);
    assert_eq!(
        bid, 2.0,
        "tie must resolve to the last equal-time right row"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn memory_limit_is_enforced() {
    let (_dir, db) = setup().await;
    db.create_table("t", trades_schema(), time_options(false))
        .await
        .unwrap();
    let ts: Vec<i64> = (0..200_000).collect();
    let syms: Vec<&str> = ts
        .iter()
        .map(|t| if t % 2 == 0 { "A" } else { "B" })
        .collect();
    let prices: Vec<f64> = ts.iter().map(|t| *t as f64).collect();
    db.write(
        "t",
        vec![trades_batch(&ts, &syms, &prices)],
        WriteOptions::default(),
    )
    .await
    .unwrap();

    // A tiny memory budget: the query must either succeed by spilling or
    // fail with a resources error — never OOM the process.
    let session = H5iSession::new(
        db,
        SessionOptions {
            memory_limit: Some(2 * 1024 * 1024),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let result = session
        .sql("SELECT symbol, ts, sum(price) OVER (PARTITION BY symbol ORDER BY ts) FROM t ORDER BY ts DESC LIMIT 5")
        .await
        .unwrap()
        .collect()
        .await;
    match result {
        Ok(batches) => assert!(!batches.is_empty()),
        Err(e) => {
            let msg = e.to_string();
            assert!(
                msg.contains("Resources exhausted") || msg.contains("memory"),
                "unexpected error under memory limit: {msg}"
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn finance_functions_vwap_wavg_ewma() {
    let (_dir, db) = setup().await;
    db.create_table("trades", trades_schema(), time_options(false))
        .await
        .unwrap();
    // Two symbols; sizes are all 10 (from trades_batch), so vwap == avg here;
    // use explicit different sizes via price*size weighting sanity below.
    db.write(
        "trades",
        vec![trades_batch(
            &[1, 2, 3, 4],
            &["A", "A", "B", "B"],
            &[10.0, 20.0, 30.0, 50.0],
        )],
        WriteOptions::default(),
    )
    .await
    .unwrap();
    let session = H5iSession::new(db, SessionOptions::default())
        .await
        .unwrap();

    // vwap(price, size) with equal sizes == plain average; wavg(size, price)
    // is the kdb-style argument order for the same computation.
    let batches = session
        .sql(
            "SELECT symbol, vwap(price, size) AS v, wavg(size, price) AS w \
             FROM trades GROUP BY symbol ORDER BY symbol",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_batches_eq!(
        [
            "+--------+------+------+",
            "| symbol | v    | w    |",
            "+--------+------+------+",
            "| A      | 15.0 | 15.0 |",
            "| B      | 40.0 | 40.0 |",
            "+--------+------+------+",
        ],
        &batches
    );

    // ewma over an ordered partition: y = [10, 15, ...] with alpha=0.5.
    let batches = session
        .sql(
            "SELECT ewma(price, 0.5) OVER (PARTITION BY symbol ORDER BY ts) AS e \
             FROM trades WHERE symbol = 'A' ORDER BY ts",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_batches_eq!(
        ["+------+", "| e    |", "+------+", "| 10.0 |", "| 15.0 |", "+------+",],
        &batches
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn quant_pipeline_end_to_end() {
    // The design's flagship demo: trades ASOF quotes → 1-minute OHLCV+VWAP →
    // log returns → rolling volatility, all in one SQL statement over
    // versioned storage.
    let (_dir, db) = asof_setup().await;
    let session = H5iSession::new(db, SessionOptions::default())
        .await
        .unwrap();
    let batches = session
        .sql(
            "WITH enriched AS ( \
                 SELECT * FROM asof_join('trades', 'quotes', 'ts', 'ts', 'symbol') \
             ), bars AS ( \
                 SELECT symbol, time_bucket('1m', ts) AS bucket, \
                        first_value(price ORDER BY ts) AS open, \
                        max(price) AS high, min(price) AS low, \
                        last_value(price ORDER BY ts) AS close, \
                        vwap(price, size) AS vwap_px, \
                        avg(bid) AS avg_bid \
                 FROM enriched GROUP BY symbol, bucket \
             ) \
             SELECT symbol, open, high, low, close, vwap_px, \
                    ln(close / open) AS bar_log_return, avg_bid \
             FROM bars ORDER BY symbol",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    // A: trades 100, 101, 102 in one ns-scale bucket; B: single trade 200.
    let rendered = arrow::util::pretty::pretty_format_batches(&batches)
        .unwrap()
        .to_string();
    assert!(rendered.contains("| A"), "{rendered}");
    assert!(rendered.contains("| B"), "{rendered}");
    assert!(rendered.contains("100.0"), "{rendered}");
    assert!(rendered.contains("102.0"), "{rendered}");
}

#[tokio::test(flavor = "multi_thread")]
async fn asof_inner_join_and_no_by_keys() {
    let (_dir, db) = asof_setup().await;
    let session = H5iSession::new(db, SessionOptions::default())
        .await
        .unwrap();

    // INNER drops the unmatched B trade (its only quote is later).
    let trades = session.read_table("trades", ReadAt::Latest).await.unwrap();
    let quotes = session.read_table("quotes", ReadAt::Latest).await.unwrap();
    let joined = asof_join(
        trades,
        quotes,
        AsOfOptions {
            by: vec![("symbol".into(), "symbol".into())],
            inner: true,
            ..Default::default()
        },
    )
    .unwrap();
    let n: usize = joined
        .collect()
        .await
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(n, 3, "B@10 has no backward quote and must be dropped");

    // No by-keys: global as-of against the latest quote regardless of symbol.
    let trades = session.read_table("trades", ReadAt::Latest).await.unwrap();
    let quotes = session.read_table("quotes", ReadAt::Latest).await.unwrap();
    let joined = asof_join(trades, quotes, AsOfOptions::default()).unwrap();
    let batches = joined.collect().await.unwrap();
    let n: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(n, 4, "left join keeps all trades");
}

#[tokio::test(flavor = "multi_thread")]
async fn time_bucket_calendar_months_in_sql() {
    let (_dir, db) = setup().await;
    db.create_table("t", trades_schema(), time_options(false))
        .await
        .unwrap();
    // Jan 15 and Feb 2, 2026, as ns timestamps.
    let jan = 1_768_435_200_000_000_000i64; // 2026-01-15T00:00:00Z
    let feb = 1_769_990_400_000_000_000i64; // 2026-02-02T00:00:00Z
    db.write(
        "t",
        vec![trades_batch(&[jan, feb], &["A", "A"], &[1.0, 2.0])],
        WriteOptions::default(),
    )
    .await
    .unwrap();
    let session = H5iSession::new(db, SessionOptions::default())
        .await
        .unwrap();
    let batches = session
        .sql("SELECT time_bucket('1mo', ts) b, count(*) n FROM t GROUP BY b ORDER BY b")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let rendered = arrow::util::pretty::pretty_format_batches(&batches)
        .unwrap()
        .to_string();
    assert!(rendered.contains("2026-01-01"), "{rendered}");
    assert!(rendered.contains("2026-02-01"), "{rendered}");
}

#[tokio::test(flavor = "multi_thread")]
async fn udtf_error_paths_are_actionable() {
    let (_dir, db) = setup().await;
    db.create_table("t", trades_schema(), time_options(false))
        .await
        .unwrap();
    let session = H5iSession::new(db, SessionOptions::default())
        .await
        .unwrap();

    // Unknown table through the UDTF.
    let err = session
        .sql("SELECT * FROM h5i('missing')")
        .await
        .err()
        .map(|e| e.to_string())
        .unwrap_or_default();
    assert!(err.contains("missing"), "{err}");

    // Nonexistent version.
    let err = session
        .sql("SELECT * FROM h5i('t', 99)")
        .await
        .err()
        .map(|e| e.to_string())
        .unwrap_or_default();
    assert!(err.contains("99"), "{err}");

    // Bad asof_join arity.
    let err = session
        .sql("SELECT * FROM asof_join('t')")
        .await
        .err()
        .map(|e| e.to_string())
        .unwrap_or_default();
    assert!(err.contains("asof_join"), "{err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn snapshot_bound_sessions_are_stable_under_writes() {
    let (_dir, db) = setup().await;
    db.create_table("t", trades_schema(), time_options(false))
        .await
        .unwrap();
    db.write(
        "t",
        vec![trades_batch(&[1], &["A"], &[1.0])],
        WriteOptions::default(),
    )
    .await
    .unwrap();
    let session = H5iSession::new(db.clone(), SessionOptions::default())
        .await
        .unwrap();
    // A concurrent commit after session creation…
    db.append(
        "t",
        vec![trades_batch(&[2], &["A"], &[2.0])],
        WriteOptions::default(),
    )
    .await
    .unwrap();
    // …is invisible to the session's registered table (statement stability)…
    let batches = session
        .sql("SELECT count(*) FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let n = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(n, 1, "session tables are snapshot-bound at creation");
    // …while the h5i() UDTF resolves fresh.
    let batches = session
        .sql("SELECT count(*) FROM h5i('t')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let n = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(n, 2, "h5i('t') resolves the current head");
}

/// Two uncorrelated scalar subqueries over *different versions* of one table
/// must not be unified. DataFusion plans every `h5i(...)` call under the same
/// bare relation name and dedups structurally-equal subqueries, so without a
/// distinguishing read-point stamp in the provider schema, both filters
/// receive one shared max(ts) and the other version's rows silently vanish.
#[tokio::test(flavor = "multi_thread")]
async fn scalar_subqueries_over_different_versions_stay_distinct() {
    let (_dir, db) = setup().await;
    db.create_table("t", trades_schema(), time_options(false))
        .await
        .unwrap();
    // Version 1: A/B at ts 1-2. Version 2 adds C/D at ts 3-4, so the two
    // versions have different max(ts) and disjoint "latest" row sets.
    db.append(
        "t",
        vec![trades_batch(&[1, 2], &["A", "B"], &[1.0, 2.0])],
        WriteOptions::default(),
    )
    .await
    .unwrap();
    db.append(
        "t",
        vec![trades_batch(&[3, 4], &["C", "D"], &[3.0, 4.0])],
        WriteOptions::default(),
    )
    .await
    .unwrap();

    let session = H5iSession::new(db, SessionOptions::default())
        .await
        .unwrap();
    // cur = the latest row of version 2 (D@4), prev = latest of version 1
    // (B@2). A merged subquery filters version 1 by version 2's max (or vice
    // versa), emptying one side.
    let batches = session
        .sql(
            "WITH cur AS (SELECT symbol FROM h5i('t', 2) \
                          WHERE ts = (SELECT max(ts) FROM h5i('t', 2))), \
                  prev AS (SELECT symbol FROM h5i('t', 1) \
                           WHERE ts = (SELECT max(ts) FROM h5i('t', 1))) \
             SELECT cur.symbol AS c, prev.symbol AS p \
             FROM cur FULL OUTER JOIN prev ON cur.symbol = prev.symbol \
             ORDER BY c",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_batches_eq!(
        [
            "+---+---+",
            "| c | p |",
            "+---+---+",
            "| D |   |",
            "|   | B |",
            "+---+---+",
        ],
        &batches
    );

    // Same hazard through the identical-argument case: equal calls SHOULD
    // still dedup (same data), so this stays correct and cheap.
    let batches = session
        .sql(
            "SELECT count(*) AS n FROM h5i('t', 2) \
             WHERE ts = (SELECT max(ts) FROM h5i('t', 2)) \
                OR ts = (SELECT max(ts) FROM h5i('t', 2))",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_batches_eq!(["+---+", "| n |", "+---+", "| 1 |", "+---+",], &batches);
}
