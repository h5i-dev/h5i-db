//! Tests for roadmap fixes: `time_bucket` interval validation (1.2),
//! manifest-backed planner statistics (2.3), retractable rolling
//! `vwap`/`wavg` (2.7), and session refresh / shared runtime (2.9).

use std::sync::Arc;

use arrow::array::{
    Array, Float64Array, Int64Array, RecordBatch, StringArray, TimestampNanosecondArray,
    UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::common::stats::Precision;
use datafusion::physical_plan::displayable;
use h5i_db_core::{Database, StorageOptions, TableOptions, WriteOptions};
use h5i_db_query::{
    AggregateStateMode, AggregateStateStore, FinanceAggregateSpec, H5iSession, PredicateCacheMode,
    QueryStatus, SessionOptions, WorkloadTelemetryEnvelope,
};
use object_store::{path::Path as ObjectPath, ObjectStoreExt};

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

fn trades_batch(ts: &[i64], symbols: &[&str], prices: &[f64], sizes: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        trades_schema(),
        vec![
            Arc::new(TimestampNanosecondArray::from(ts.to_vec()).with_timezone("UTC".to_string())),
            Arc::new(StringArray::from(symbols.to_vec())),
            Arc::new(Float64Array::from(prices.to_vec())),
            Arc::new(Int64Array::from(sizes.to_vec())),
        ],
    )
    .unwrap()
}

fn time_options() -> TableOptions {
    TableOptions {
        time_column: Some("ts".into()),
        ..Default::default()
    }
}

async fn setup_trades() -> (tempfile::TempDir, Arc<Database>) {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Database::create(&dir.path().join("db")).await.unwrap());
    db.create_table("trades", trades_schema(), time_options())
        .await
        .unwrap();
    db.append(
        "trades",
        vec![trades_batch(
            &[1_000, 2_000, 3_000, 4_000],
            &["A", "B", "A", "B"],
            &[10.0, 20.0, 12.0, 22.0],
            &[1, 2, 3, 4],
        )],
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

// ---------------------------------------------------------------------------
// 1.2 time_bucket interval validation
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn time_bucket_rejects_degenerate_intervals() {
    let (_dir, db) = setup_trades().await;
    let s = session(&db).await;
    // Each of these used to panic (divide-by-zero / i32 wrap) at execution —
    // fatal under the workspace panic=abort profile. They must now surface as
    // plain query errors.
    for sql in [
        "SELECT time_bucket('0mo', ts) FROM trades",
        "SELECT time_bucket('0s', ts) FROM trades",
        "SELECT time_bucket(INTERVAL '0' MONTH, ts) FROM trades",
        "SELECT time_bucket(INTERVAL '-2' MONTH, ts) FROM trades",
        "SELECT time_bucket('1.5mo', ts) FROM trades",
        "SELECT time_bucket('999999999999y', ts) FROM trades",
    ] {
        let res = match s.sql(sql).await {
            Ok(df) => df.collect().await.map(|_| ()),
            Err(e) => Err(e),
        };
        let err = res.expect_err(sql).to_string();
        assert!(
            err.contains("time_bucket"),
            "unexpected error for {sql}: {err}"
        );
    }
    // Sane widths still work.
    s.sql("SELECT time_bucket('1mo', ts) FROM trades")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    s.sql("SELECT time_bucket(INTERVAL '1' MONTH, ts) FROM trades")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// 2.3 manifest statistics
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn provider_statistics_are_exact_from_manifest() {
    let (_dir, db) = setup_trades().await;
    let s = session(&db).await;
    let provider = s.context().table_provider("trades").await.unwrap();
    let stats = provider.statistics().expect("manifest statistics");
    assert_eq!(stats.num_rows, Precision::Exact(4));
    assert!(matches!(stats.total_byte_size, Precision::Inexact(b) if b > 0));
    let schema = trades_schema();
    let price_idx = schema.index_of("price").unwrap();
    let price = &stats.column_statistics[price_idx];
    assert_eq!(price.null_count, Precision::Exact(0));
    assert_eq!(
        price.min_value,
        Precision::Exact(datafusion::scalar::ScalarValue::Float64(Some(10.0)))
    );
    assert_eq!(
        price.max_value,
        Precision::Exact(datafusion::scalar::ScalarValue::Float64(Some(22.0)))
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn count_star_is_answered_from_metadata() {
    let (_dir, db) = setup_trades().await;
    let s = session(&db).await;
    let df = s.sql("SELECT count(*) FROM trades").await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    let display = displayable(plan.as_ref()).indent(true).to_string();
    // Exact scan statistics let the aggregate fold to a literal: no scan node
    // may survive in the plan.
    assert!(
        !display.contains("DataSourceExec"),
        "count(*) still scans:\n{display}"
    );
    let batches = datafusion::physical_plan::collect(plan, s.context().task_ctx())
        .await
        .unwrap();
    let n = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(n, 4);
}

// ---------------------------------------------------------------------------
// 2.7 rolling vwap via retract_batch
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn rolling_vwap_window_matches_reference() {
    let (_dir, db) = setup_trades().await;
    let s = session(&db).await;
    let batches = s
        .sql(
            "SELECT vwap(price, size) OVER (\
               ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW\
             ) AS v FROM trades ORDER BY ts",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let all: Vec<f64> = batches
        .iter()
        .flat_map(|b| {
            let a = b.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
            (0..a.len()).map(|i| a.value(i)).collect::<Vec<_>>()
        })
        .collect();
    // Reference: 2-row sliding weighted mean over (price, size) pairs
    // (10,1) (20,2) (12,3) (22,4), computed from scratch per frame.
    let expect = [
        10.0,
        (10.0 + 40.0) / 3.0,
        (40.0 + 36.0) / 5.0,
        (36.0 + 88.0) / 7.0,
    ];
    assert_eq!(all.len(), expect.len());
    for (got, want) in all.iter().zip(expect) {
        assert!((got - want).abs() < 1e-9, "got {got}, want {want}");
    }
}

#[test]
fn vwap_accumulator_retract_matches_fresh_state() {
    use datafusion::logical_expr::function::AccumulatorArgs;
    // Drive the accumulator exactly like a sliding frame: bulk update, then
    // retract the rows that left, and compare with a from-scratch frame.
    let udaf = h5i_db_query::finance::vwap_udaf();
    let schema = Schema::new(vec![
        Field::new("p", DataType::Float64, true),
        Field::new("w", DataType::Float64, true),
    ]);
    let args = AccumulatorArgs {
        return_field: Arc::new(Field::new("vwap", DataType::Float64, true)),
        schema: &schema,
        ignore_nulls: false,
        order_bys: &[],
        is_reversed: false,
        name: "vwap",
        is_distinct: false,
        exprs: &[],
        expr_fields: &[],
    };
    let mut acc = udaf.accumulator(args).unwrap();
    let p: Arc<dyn Array> = Arc::new(Float64Array::from(vec![10.0, 20.0, 12.0]));
    let w: Arc<dyn Array> = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0]));
    acc.update_batch(&[p, w]).unwrap();
    assert!(acc.supports_retract_batch());
    // Retract the first row → frame is rows 2..3.
    let rp: Arc<dyn Array> = Arc::new(Float64Array::from(vec![10.0]));
    let rw: Arc<dyn Array> = Arc::new(Float64Array::from(vec![1.0]));
    acc.retract_batch(&[rp, rw]).unwrap();
    let got = acc.evaluate().unwrap();
    let want = (20.0 * 2.0 + 12.0 * 3.0) / 5.0;
    assert_eq!(got, datafusion::scalar::ScalarValue::Float64(Some(want)));
    // Retract the rest → empty frame must evaluate to NULL, exactly.
    let rp: Arc<dyn Array> = Arc::new(Float64Array::from(vec![20.0, 12.0]));
    let rw: Arc<dyn Array> = Arc::new(Float64Array::from(vec![2.0, 3.0]));
    acc.retract_batch(&[rp, rw]).unwrap();
    assert_eq!(
        acc.evaluate().unwrap(),
        datafusion::scalar::ScalarValue::Float64(None)
    );
}

// ---------------------------------------------------------------------------
// 2.9 session refresh + shared runtime
// ---------------------------------------------------------------------------

async fn count_trades(s: &H5iSession) -> i64 {
    let batches = s
        .sql("SELECT count(*) AS n FROM trades")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

#[tokio::test(flavor = "multi_thread")]
async fn refresh_repoints_tables_at_latest_without_new_session() {
    let (_dir, db) = setup_trades().await;
    let s = session(&db).await;
    assert_eq!(count_trades(&s).await, 4);

    db.append(
        "trades",
        vec![trades_batch(&[5_000], &["C"], &[30.0], &[5])],
        WriteOptions::default(),
    )
    .await
    .unwrap();
    // Registered names are snapshot-bound: still the old version.
    assert_eq!(count_trades(&s).await, 4);

    s.refresh().await.unwrap();
    assert_eq!(count_trades(&s).await, 5);

    // New tables appear after refresh.
    db.create_table("quotes", trades_schema(), time_options())
        .await
        .unwrap();
    s.refresh().await.unwrap();
    s.sql("SELECT * FROM quotes").await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn sessions_can_share_a_runtime_env() {
    let (_dir, db) = setup_trades().await;
    let s1 = session(&db).await;
    assert_eq!(count_trades(&s1).await, 4);

    let s2 = H5iSession::new_with_runtime(db.clone(), SessionOptions::default(), s1.runtime_env())
        .await
        .unwrap();
    assert!(Arc::ptr_eq(&s1.runtime_env(), &s2.runtime_env()));
    assert_eq!(count_trades(&s2).await, 4);
}

#[tokio::test(flavor = "multi_thread")]
async fn gapfill_supports_locf_and_linear_interpolation() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Database::create(&dir.path().join("db")).await.unwrap());
    db.create_table("trades", trades_schema(), time_options())
        .await
        .unwrap();
    db.append(
        "trades",
        vec![trades_batch(&[0, 20], &["A", "A"], &[0.0, 20.0], &[1, 3])],
        WriteOptions::default(),
    )
    .await
    .unwrap();
    let s = session(&db).await;
    let interpolated = s
        .sql("SELECT price, size FROM gapfill('trades', 'ts', 10, 'interpolate') ORDER BY ts")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let price = interpolated[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let size = interpolated[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(price.values(), &[0.0, 10.0, 20.0]);
    assert_eq!(size.values(), &[1, 2, 3]);

    let locf = s
        .sql("SELECT price FROM gapfill('trades', 'ts', 10, 'locf') ORDER BY ts")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let price = locf[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(price.values(), &[0.0, 0.0, 20.0]);

    let resampled = s
        .sql("SELECT count(*) AS n FROM resample('trades', 'ts', 10, 'null')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        resampled[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        3
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn rolling_sugar_expands_to_bounded_window_frames() {
    let (_dir, db) = setup_trades().await;
    let s = session(&db).await;
    let batches = s
        .sql("SELECT rolling_avg(price, ts, 2) AS value FROM trades ORDER BY ts")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let values = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(values.values(), &[10.0, 15.0, 16.0, 17.0]);

    let err = s
        .sql("SELECT rolling_sum(size, ts, 0) FROM trades")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("between 1 and 1000000"));
}

#[tokio::test(flavor = "multi_thread")]
async fn sql_asof_keyword_and_cross_version_join_work() {
    let (_dir, db) = setup_trades().await;
    db.create_table("quotes", trades_schema(), time_options())
        .await
        .unwrap();
    db.append(
        "quotes",
        vec![trades_batch(
            &[500, 1500],
            &["A", "A"],
            &[9.0, 11.0],
            &[1, 1],
        )],
        WriteOptions::default(),
    )
    .await
    .unwrap();
    db.append(
        "trades",
        vec![trades_batch(&[5000], &["C"], &[30.0], &[5])],
        WriteOptions::default(),
    )
    .await
    .unwrap();
    let s = session(&db).await;
    let asof = s
        .sql(
            "SELECT * FROM trades ASOF JOIN quotes \
             MATCH_CONDITION (trades.ts >= quotes.ts) ON trades.symbol = quotes.symbol",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(asof.iter().map(RecordBatch::num_rows).sum::<usize>(), 5);

    let versions = s
        .sql(
            "SELECT count(*) AS n FROM h5i('trades', 1) a \
             JOIN h5i('trades', 2) b ON a.ts = b.ts",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        versions[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        4
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn exact_symbol_sets_prune_unrelated_segments() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Database::create(&dir.path().join("db")).await.unwrap());
    db.create_table("trades", trades_schema(), time_options())
        .await
        .unwrap();
    db.append(
        "trades",
        vec![trades_batch(&[0, 1], &["A", "A"], &[1.0, 2.0], &[1, 1])],
        WriteOptions::default(),
    )
    .await
    .unwrap();
    db.append(
        "trades",
        vec![trades_batch(&[2, 3], &["B", "B"], &[3.0, 4.0], &[1, 1])],
        WriteOptions::default(),
    )
    .await
    .unwrap();
    let s = session(&db).await;
    s.sql("SELECT * FROM trades WHERE symbol = 'A'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let metrics = s.take_scan_metrics();
    assert_eq!(metrics[0].segments_total, 2);
    assert_eq!(metrics[0].segments_scanned, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn tail_table_function_is_an_unbounded_append_stream() {
    let (_dir, db) = setup_trades().await;
    let s = session(&db).await;
    let query = tokio::spawn(async move {
        s.sql("SELECT ts FROM tail('trades', 1, 10) LIMIT 1")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
    });
    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    db.append(
        "trades",
        vec![trades_batch(&[5_000], &["A"], &[13.0], &[1])],
        WriteOptions::default(),
    )
    .await
    .unwrap();
    let batches = tokio::time::timeout(std::time::Duration::from_secs(2), query)
        .await
        .expect("tail query timed out")
        .unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn time_bucket_timezone_tracks_dst_local_midnight() {
    let (_dir, db) = setup_trades().await;
    let ns = |value: &str| {
        chrono::DateTime::parse_from_rfc3339(value)
            .unwrap()
            .timestamp_nanos_opt()
            .unwrap()
    };
    db.append(
        "trades",
        vec![trades_batch(
            &[ns("2024-03-10T07:30:00Z"), ns("2024-03-11T07:30:00Z")],
            &["A", "A"],
            &[1.0, 2.0],
            &[1, 1],
        )],
        WriteOptions::default(),
    )
    .await
    .unwrap();
    let s = session(&db).await;
    let batches = s
        .sql(
            "SELECT time_bucket('1d', ts, 'America/New_York') AS bucket \
             FROM trades WHERE price < 3 ORDER BY ts",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let values = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();
    assert_eq!(values.value(0), ns("2024-03-10T05:00:00Z"));
    assert_eq!(values.value(1), ns("2024-03-11T04:00:00Z"));

    let err = s
        .sql("SELECT time_bucket('1d', ts, 'Mars/Olympus') FROM trades")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err();
    assert!(err.to_string().contains("unknown IANA timezone"));
}

// ---------------------------------------------------------------------------
// P0 query-local performance adapter (pure telemetry tests live in the
// lightweight h5i-db-observability crate).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn reported_queries_isolate_scans_and_persist_private_telemetry() {
    let (_dir, db) = setup_trades().await;
    let session = H5iSession::new(
        db.clone(),
        SessionOptions {
            telemetry_capacity: 2,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let (a, b) = tokio::join!(
        session.sql_reported("SELECT * FROM trades WHERE symbol = 'A'"),
        session.sql_reported("SELECT * FROM trades WHERE symbol = 'B'")
    );
    let (a, b) = tokio::join!(a.unwrap().collect(), b.unwrap().collect());
    let (_, a) = a.unwrap();
    let (_, b) = b.unwrap();

    assert_eq!(a.status, QueryStatus::Succeeded);
    assert_eq!(b.status, QueryStatus::Succeeded);
    assert_ne!(a.query_id, b.query_id);
    assert_eq!(a.output_rows, 2);
    assert_eq!(b.output_rows, 2);
    assert!(a.bytes_scanned > 0);
    assert!(b.bytes_scanned > 0);
    assert!(!a.scans.is_empty());
    assert!(!b.scans.is_empty());
    assert!(a.scans.iter().all(|scan| scan.query_id == Some(a.query_id)));
    assert!(b.scans.iter().all(|scan| scan.query_id == Some(b.query_id)));

    let telemetry = session.workload_telemetry();
    assert_eq!(telemetry.len(), 2);
    let serialized = serde_json::to_string(&telemetry).unwrap();
    assert!(!serialized.contains("SELECT"));

    let path = session.flush_workload_telemetry().await.unwrap().unwrap();
    let bytes = db
        .backend()
        .store
        .get(&ObjectPath::from(path.as_str()))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let envelope: WorkloadTelemetryEnvelope = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(envelope.format, 1);
    assert_eq!(envelope.reports, telemetry);
}

#[tokio::test]
async fn failed_and_cancelled_executions_still_record_reports() {
    let (_dir, db) = setup_trades().await;
    let session = H5iSession::new(
        db,
        SessionOptions {
            telemetry_capacity: 4,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // Runtime failure: planning succeeds, execution errors. The stream must
    // finalize a Failed report instead of losing the execution.
    let failing = session
        .sql_reported("SELECT 1 / (count(*) - count(*)) FROM trades")
        .await
        .unwrap();
    let failed_id = failing.query_id();
    let mut stream = failing.execute_stream().await.unwrap();
    let mut saw_error = false;
    while let Some(batch) = futures::StreamExt::next(&mut stream).await {
        if batch.is_err() {
            saw_error = true;
            break;
        }
    }
    assert!(saw_error, "division by zero should fail execution");
    let report = stream.report().expect("failed stream still reports");
    assert_eq!(report.status, QueryStatus::Failed);
    assert_eq!(report.query_id, failed_id);

    // Cancellation: dropping the stream mid-query finalizes as Cancelled.
    let cancelled = session.sql_reported("SELECT * FROM trades").await.unwrap();
    let cancelled_id = cancelled.query_id();
    let stream = cancelled.execute_stream().await.unwrap();
    drop(stream);

    let telemetry = session.workload_telemetry();
    let status_of = |id| {
        telemetry
            .iter()
            .find(|r| r.query_id == id)
            .map(|r| r.status)
    };
    assert_eq!(status_of(failed_id), Some(QueryStatus::Failed));
    assert_eq!(status_of(cancelled_id), Some(QueryStatus::Cancelled));

    // Neither abandoned execution may leave scan records behind for the next
    // query to drain.
    let session_wide = session.take_scan_metrics();
    assert!(
        session_wide.is_empty(),
        "finalized reports must drain their scan records: {session_wide:?}"
    );
}

// ---------------------------------------------------------------------------
// P2 immutable predicate cache.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn predicate_cache_reuses_row_group_selection_and_recovers_from_corruption() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Database::create(&dir.path().join("db")).await.unwrap());
    db.create_table(
        "trades",
        trades_schema(),
        TableOptions {
            storage: StorageOptions {
                target_segment_bytes: 2 * 1024 * 1024,
                target_row_group_bytes: 16 * 1024,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // The writer intentionally floors row groups at 8K rows. Four groups let
    // the first two encode a correlation that min/max statistics cannot see.
    let rows = 32_768usize;
    let timestamps = (0..rows).map(|i| i as i64).collect::<Vec<_>>();
    let mut symbols = Vec::with_capacity(rows);
    let mut prices = Vec::with_capacity(rows);
    let mut sizes = Vec::with_capacity(rows);
    for i in 0..rows {
        let second_half = i >= rows / 2;
        let symbol_a = i % 2 == 0;
        symbols.push(if symbol_a { "A" } else { "B" });
        prices.push(i as f64);
        sizes.push(i64::from(symbol_a == second_half));
    }
    db.append(
        "trades",
        vec![trades_batch(&timestamps, &symbols, &prices, &sizes)],
        WriteOptions::default(),
    )
    .await
    .unwrap();

    let session = H5iSession::new(
        db.clone(),
        SessionOptions {
            predicate_cache: PredicateCacheMode::ReadWrite,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let sql = "SELECT count(*) FROM trades WHERE symbol = 'A' AND size = 1";

    let (cold_batches, cold) = session
        .sql_reported(sql)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let count = cold_batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, (rows / 4) as i64);
    assert_eq!(cold.predicate_cache_builds, 1, "{cold:#?}");
    assert_eq!(cold.predicate_cache_hits, 0);

    let (_, warm) = session
        .sql_reported(sql)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(warm.predicate_cache_hits, 1);
    assert_eq!(warm.predicate_cache_builds, 0);
    assert!(warm.predicate_cache_row_groups_reused > 1);
    assert!(
        warm.bytes_scanned < cold.bytes_scanned,
        "warm={} cold={}",
        warm.bytes_scanned,
        cold.bytes_scanned
    );

    let cache_objects = db
        .backend()
        .list(&ObjectPath::from("cache/predicates/v1"))
        .await
        .unwrap();
    assert_eq!(cache_objects.len(), 1);
    db.backend()
        .put(
            &cache_objects[0].location,
            bytes::Bytes::from_static(b"corrupt"),
        )
        .await
        .unwrap();

    let (recovered_batches, recovered) = session
        .sql_reported(sql)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let recovered_count = recovered_batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(recovered_count, count);
    assert_eq!(recovered.predicate_cache_hits, 0);
    assert_eq!(recovered.predicate_cache_misses, 1);
    assert_eq!(recovered.predicate_cache_builds, 1);

    // A new version reuses the old segment sidecar and misses only for the
    // newly added immutable segment.
    let added = 8192usize;
    let added_ts = (0..added).map(|i| (rows + i) as i64).collect::<Vec<_>>();
    db.append(
        "trades",
        vec![trades_batch(
            &added_ts,
            &vec!["A"; added],
            &vec![1.0; added],
            &vec![1; added],
        )],
        WriteOptions::default(),
    )
    .await
    .unwrap();
    session.refresh().await.unwrap();
    let (appended_batches, appended) = session
        .sql_reported(sql)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let appended_count = appended_batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(appended_count, count + added as i64);
    assert_eq!(appended.predicate_cache_hits, 1);
    assert_eq!(appended.predicate_cache_builds, 1);

    // Compaction rewrites both segments under a new checksum: it is a clean
    // miss, never a stale hit, and the result remains identical.
    db.compact("trades", WriteOptions::default()).await.unwrap();
    session.refresh().await.unwrap();
    let (compacted_batches, compacted) = session
        .sql_reported(sql)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let compacted_count = compacted_batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(compacted_count, appended_count);
    assert_eq!(compacted.predicate_cache_hits, 0);
    assert_eq!(compacted.predicate_cache_builds, 1);
}

// ---------------------------------------------------------------------------
// P3 version-aware aggregate state store.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn aggregate_states_reuse_unchanged_segments_and_match_full_recomputation() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Database::create(&dir.path().join("db")).await.unwrap());
    db.create_table("trades", trades_schema(), TableOptions::default())
        .await
        .unwrap();
    for (offset, prices, sizes) in [
        (0_i64, vec![10.0, 20.0, 12.0, 18.0], vec![1, 2, 3, 4]),
        (4_i64, vec![11.0, 21.0, 13.0, 19.0], vec![5, 6, 7, 8]),
    ] {
        db.append(
            "trades",
            vec![trades_batch(
                &[offset + 1, offset + 2, offset + 3, offset + 4],
                &["A", "B", "A", "B"],
                &prices,
                &sizes,
            )],
            WriteOptions::default(),
        )
        .await
        .unwrap();
    }
    let spec = FinanceAggregateSpec::ohlcv("ts", "price", "size").grouped_by("symbol");
    let store = AggregateStateStore::new(db.clone(), AggregateStateMode::ReadWrite);

    let cold = store
        .finance_rollup("trades", h5i_db_core::ReadAt::Latest, &spec)
        .await
        .unwrap();
    assert_eq!(cold.metrics.states_built, 2);
    assert_eq!(cold.metrics.states_reused, 0);
    assert_eq!(cold.metrics.segments_scanned, 2);
    let a = cold
        .groups
        .iter()
        .find(|group| group.group.as_deref() == Some("A"))
        .unwrap();
    assert_eq!(a.rows, 4);
    assert_eq!((a.open, a.high, a.low, a.close), (10.0, 13.0, 10.0, 13.0));
    assert_eq!(a.volume, 16.0);
    assert!((a.vwap.unwrap() - 12.0).abs() < 1e-12);

    let warm = store
        .finance_rollup("trades", h5i_db_core::ReadAt::Latest, &spec)
        .await
        .unwrap();
    assert_eq!(warm.groups, cold.groups);
    assert_eq!(warm.metrics.states_reused, 2);
    assert_eq!(warm.metrics.states_built, 0);
    assert_eq!(warm.metrics.segments_scanned, 0);

    db.append(
        "trades",
        vec![trades_batch(
            &[9, 10, 11, 12],
            &["A", "B", "A", "B"],
            &[14.0, 22.0, 15.0, 23.0],
            &[9, 10, 11, 12],
        )],
        WriteOptions::default(),
    )
    .await
    .unwrap();
    let appended = store
        .finance_rollup("trades", h5i_db_core::ReadAt::Latest, &spec)
        .await
        .unwrap();
    assert_eq!(appended.metrics.states_reused, 2);
    assert_eq!(appended.metrics.states_built, 1);
    assert_eq!(appended.metrics.segments_scanned, 1);

    let historical = store
        .finance_rollup("trades", h5i_db_core::ReadAt::Version(2), &spec)
        .await
        .unwrap();
    assert_eq!(historical.groups, cold.groups);
    assert_eq!(historical.metrics.states_reused, 2);
    assert_eq!(historical.metrics.segments_scanned, 0);

    let forced = AggregateStateStore::new(db.clone(), AggregateStateMode::Disabled)
        .finance_rollup("trades", h5i_db_core::ReadAt::Latest, &spec)
        .await
        .unwrap();
    assert_eq!(appended.groups, forced.groups);
    assert_eq!(forced.metrics.segments_scanned, 3);

    let objects = db
        .backend()
        .list(&ObjectPath::from("cache/aggregates/v1"))
        .await
        .unwrap();
    assert_eq!(objects.len(), 3);
    db.backend()
        .put(&objects[0].location, bytes::Bytes::from_static(b"corrupt"))
        .await
        .unwrap();
    let recovered = store
        .finance_rollup("trades", h5i_db_core::ReadAt::Latest, &spec)
        .await
        .unwrap();
    assert_eq!(recovered.groups, forced.groups);
    assert_eq!(recovered.metrics.corrupt_entries, 1);
    assert_eq!(recovered.metrics.states_reused, 2);
    assert_eq!(recovered.metrics.states_built, 1);

    db.compact("trades", WriteOptions::default()).await.unwrap();
    let compacted = store
        .finance_rollup("trades", h5i_db_core::ReadAt::Latest, &spec)
        .await
        .unwrap();
    let compacted_forced = AggregateStateStore::new(db.clone(), AggregateStateMode::Disabled)
        .finance_rollup("trades", h5i_db_core::ReadAt::Latest, &spec)
        .await
        .unwrap();
    assert_eq!(compacted.groups, compacted_forced.groups);
    assert_eq!(compacted.metrics.states_reused, 0);
    assert_eq!(compacted.metrics.states_built, 1);
}

/// End-to-end proof of A2: a bloom filter on a high-cardinality entity column
/// prunes row groups that min/max statistics structurally cannot. The early row
/// groups alternate only "AAA"/"ZZZ" (min/max span = [AAA, ZZZ]), while the
/// queried "MMM" lives solely in the final row group — so statistics keep every
/// group, but the bloom excludes the MMM-free ones. A no-bloom control proves
/// min/max alone prunes nothing here.
#[tokio::test]
async fn bloom_filter_prunes_row_groups_min_max_cannot() {
    // 3 floored 8K row groups in a single segment.
    const ROWS: usize = 24_576;

    async fn row_groups_pruned(bloom: bool) -> (i64, u64) {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(Database::create(&dir.path().join("db")).await.unwrap());
        db.create_table(
            "trades",
            trades_schema(),
            TableOptions {
                storage: StorageOptions {
                    target_segment_bytes: 16 * 1024 * 1024,
                    target_row_group_bytes: 4 * 1024, // floored up to 8K rows/group
                    bloom_filter_columns: if bloom {
                        vec!["symbol".to_string()]
                    } else {
                        vec![]
                    },
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await
        .unwrap();

        let timestamps: Vec<i64> = (0..ROWS as i64).collect();
        let symbols: Vec<&str> = (0..ROWS)
            .map(|i| {
                if i >= ROWS - 8192 {
                    "MMM" // only the last row group holds the queried symbol
                } else if i % 2 == 0 {
                    "AAA"
                } else {
                    "ZZZ"
                }
            })
            .collect();
        let prices = vec![1.0_f64; ROWS];
        let sizes = vec![1_i64; ROWS];
        db.append(
            "trades",
            vec![trades_batch(&timestamps, &symbols, &prices, &sizes)],
            WriteOptions::default(),
        )
        .await
        .unwrap();

        let session = H5iSession::new(db.clone(), SessionOptions::default())
            .await
            .unwrap();
        let (batches, report) = session
            .sql_reported("SELECT count(*) FROM trades WHERE symbol = 'MMM'")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let count = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        (count, report.row_groups_pruned)
    }

    let (bloom_count, bloom_pruned) = row_groups_pruned(true).await;
    let (control_count, control_pruned) = row_groups_pruned(false).await;

    // Correctness is identical regardless of pruning.
    assert_eq!(bloom_count, 8192);
    assert_eq!(control_count, 8192);
    // Min/max cannot exclude any group for this layout.
    assert_eq!(
        control_pruned, 0,
        "control: statistics must prune no row groups"
    );
    // The bloom excludes both MMM-free groups.
    assert!(
        bloom_pruned >= 2,
        "bloom should prune the two MMM-free row groups, pruned {bloom_pruned}"
    );
}

/// C3: approximate distinct-count (HyperLogLog) and parallel top-K are provided
/// by DataFusion built-ins (`approx_distinct`, and `ORDER BY … LIMIT` planned as
/// a TopK) — registered through the session's default features. This verifies
/// they are reachable via h5i SQL and return correct results, so we consume them
/// rather than reimplement (matching the "don't rebuild DataFusion" principle).
#[tokio::test]
async fn approx_distinct_and_topk_are_available_and_correct() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Database::create(&dir.path().join("db")).await.unwrap());
    db.create_table("trades", trades_schema(), time_options())
        .await
        .unwrap();

    // 300 rows across 3 symbols with distinct total volume ordering A > B > C.
    let n = 300usize;
    let timestamps: Vec<i64> = (0..n as i64).collect();
    let symbols: Vec<&str> = (0..n)
        .map(|i| match i % 3 {
            0 => "AAA",
            1 => "BBB",
            _ => "CCC",
        })
        .collect();
    let prices = vec![1.0_f64; n];
    // Weight volumes so AAA > BBB > CCC deterministically.
    let sizes: Vec<i64> = (0..n)
        .map(|i| match i % 3 {
            0 => 100,
            1 => 10,
            _ => 1,
        })
        .collect();
    db.append(
        "trades",
        vec![trades_batch(&timestamps, &symbols, &prices, &sizes)],
        WriteOptions::default(),
    )
    .await
    .unwrap();

    let session = H5iSession::new(db.clone(), SessionOptions::default())
        .await
        .unwrap();

    // approx_distinct (HyperLogLog) — exact for a tiny cardinality of 3.
    let (b, _) = session
        .sql_reported("SELECT approx_distinct(symbol) AS d FROM trades")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let distinct = b[0]
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap()
        .value(0);
    assert_eq!(distinct, 3, "approx_distinct should count 3 symbols");

    // Parallel top-K: top-2 symbols by total volume.
    let (b, _) = session
        .sql_reported(
            "SELECT symbol FROM trades GROUP BY symbol \
             ORDER BY sum(size) DESC LIMIT 2",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let col = b[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let top: Vec<&str> = (0..col.len()).map(|i| col.value(i)).collect();
    assert_eq!(top, vec!["AAA", "BBB"], "top-2 by volume");
}
