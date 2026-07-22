//! Review-UI API tests: drive the axum router directly against a temp
//! database — overview triage payload, plan approve/reject flow, read-only
//! gating, version diff, and the SQL scratchpad.

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use h5i_db_core::{Database, StorageOptions, TableOptions, WriteOptions};
use h5i_db_ui::{build_router, UiState};
use http_body_util::BodyExt;
use tower::util::ServiceExt;

fn schema() -> SchemaRef {
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

fn batch(ts: &[i64], price: f64) -> RecordBatch {
    let syms: Vec<&str> = ts.iter().map(|_| "A").collect();
    let prices: Vec<f64> = ts.iter().map(|_| price).collect();
    let sizes: Vec<i64> = ts.iter().map(|_| 1).collect();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(TimestampNanosecondArray::from(ts.to_vec()).with_timezone("UTC".to_string())),
            Arc::new(StringArray::from(syms)),
            Arc::new(Float64Array::from(prices)),
            Arc::new(Int64Array::from(sizes)),
        ],
    )
    .unwrap()
}

const TEST_TOKEN: &str = "test-token";

/// Router with a fixed token so the helpers can authenticate.
fn router_for(db: &Arc<Database>, allow_mutations: bool) -> axum::Router {
    let mut state = UiState::new(db.clone(), "test.db".into(), allow_mutations);
    state.token = TEST_TOKEN.into();
    build_router(state)
}

async fn setup(allow_mutations: bool) -> (tempfile::TempDir, axum::Router, Arc<Database>) {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Database::create(&dir.path().join("db")).await.unwrap());
    db.create_table(
        "trades",
        schema(),
        TableOptions {
            time_column: Some("ts".into()),
            sort_key: vec![],
            storage: StorageOptions {
                target_segment_bytes: 8 * 1024,
                target_row_group_bytes: 4 * 1024,
                ..Default::default()
            },
            max_segments_per_manifest: None,
        },
    )
    .await
    .unwrap();
    let ts: Vec<i64> = (0..500).collect();
    db.write("trades", vec![batch(&ts, 100.0)], WriteOptions::default())
        .await
        .unwrap();
    let ts2: Vec<i64> = (500..800).collect();
    db.append("trades", vec![batch(&ts2, 101.0)], WriteOptions::default())
        .await
        .unwrap();
    let router = router_for(&db, allow_mutations);
    (dir, router, db)
}

async fn get_json(router: &axum::Router, path: &str) -> (StatusCode, serde_json::Value) {
    let res = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(path)
                .header("host", "127.0.0.1:8000")
                .header("authorization", format!("Bearer {TEST_TOKEN}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}

async fn post_json(
    router: &axum::Router,
    path: &str,
    body: Option<serde_json::Value>,
) -> (StatusCode, serde_json::Value) {
    let req = Request::builder()
        .method("POST")
        .uri(path)
        .header("content-type", "application/json")
        .header("host", "127.0.0.1:8000")
        .header("authorization", format!("Bearer {TEST_TOKEN}"))
        .header("x-h5i-csrf", "1")
        .body(Body::from(body.map(|b| b.to_string()).unwrap_or_default()))
        .unwrap();
    let res = router.clone().oneshot(req).await.unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}

#[tokio::test]
async fn overview_and_table_endpoints() {
    let (_dir, router, _db) = setup(false).await;
    let (status, ov) = get_json(&router, "/api/overview").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(ov["allow_mutations"], false);
    assert_eq!(ov["tables"][0]["name"], "trades");
    assert_eq!(ov["tables"][0]["rows"], 800);
    assert_eq!(ov["plans"].as_array().unwrap().len(), 0);

    let (status, detail) = get_json(&router, "/api/table/trades").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(detail["head"], 2);
    assert_eq!(detail["versions"].as_array().unwrap().len(), 3);
    assert_eq!(detail["versions"][2]["execution_mode"], "direct");

    let (status, sample) = get_json(&router, "/api/table/trades/sample?rows=7").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(sample["rows"].as_array().unwrap().len(), 7);

    // Unknown table → envelope with a hint, HTTP 400.
    let (status, err) = get_json(&router, "/api/table/nope").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(err["code"], "table_not_found");
    assert!(err["hint"].as_str().unwrap().contains("tables"));
}

#[tokio::test]
async fn version_diff_reports_segment_reuse() {
    let (_dir, router, db) = setup(false).await;
    db.delete_range("trades", 0, 100, WriteOptions::default())
        .await
        .unwrap();
    let (status, diff) = get_json(&router, "/api/table/trades/diff?from=2&to=3").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(diff["rows_delta"], -100);
    assert_eq!(diff["schema_changed"], false);
    assert!(diff["segments"]["kept"].as_u64().unwrap() >= 1);
    assert!(!diff["segments"]["removed"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn plan_flow_via_ui_respects_read_only_and_applies() {
    let (_dir, ro_router, db) = setup(false).await;
    let plan = db
        .plan_replace_range("trades", 0, 100, vec![], WriteOptions::default())
        .await
        .unwrap();

    // Plans surface in the overview with staleness info.
    let (_, ov) = get_json(&ro_router, "/api/overview").await;
    assert_eq!(ov["plans"].as_array().unwrap().len(), 1);
    assert_eq!(ov["plans"][0]["stale"], false);

    // Detail decodes samples.
    let (status, detail) =
        get_json(&ro_router, &format!("/api/plan/trades/{}", plan.plan_id)).await;
    assert_eq!(status, StatusCode::OK);
    assert!(!detail["before_sample"]["rows"]
        .as_array()
        .unwrap()
        .is_empty());

    // Read-only UI refuses to apply.
    let (status, err) = post_json(
        &ro_router,
        &format!("/api/plan/trades/{}/apply", plan.plan_id),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(err["code"], "read_only");

    // A mutations-enabled UI applies it.
    let rw_router = router_for(&db, true);
    let (status, applied) = post_json(
        &rw_router,
        &format!("/api/plan/trades/{}/apply", plan.plan_id),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{applied}");
    assert_eq!(applied["sequence"], 3);
    assert_eq!(applied["op"], "delete_range");

    // Applying again (plan consumed) fails cleanly.
    let (status, err) = post_json(
        &rw_router,
        &format!("/api/plan/trades/{}/apply", plan.plan_id),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(err["code"], "invalid_input");
}

#[tokio::test]
async fn stale_plan_conflicts_with_409() {
    let (_dir, _router, db) = setup(false).await;
    let plan = db
        .plan_replace_range("trades", 0, 100, vec![], WriteOptions::default())
        .await
        .unwrap();
    // Head moves.
    db.append("trades", vec![batch(&[5000], 1.0)], WriteOptions::default())
        .await
        .unwrap();
    let rw_router = router_for(&db, true);
    // The overview marks it stale…
    let (_, ov) = get_json(&rw_router, "/api/overview").await;
    assert_eq!(ov["plans"][0]["stale"], true);
    // …and apply is a 409 conflict, not a silent re-plan.
    let (status, err) = post_json(
        &rw_router,
        &format!("/api/plan/trades/{}/apply", plan.plan_id),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(err["code"], "version_conflict");
    assert_eq!(err["retryable"], true);
}

#[tokio::test]
async fn sql_scratchpad_returns_rows_and_metrics() {
    let (_dir, router, _db) = setup(false).await;
    let (status, out) = post_json(
        &router,
        "/api/query",
        Some(serde_json::json!({
            "sql": "SELECT symbol, count(*) AS n FROM trades GROUP BY symbol"
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(out["rows"][0][1], 800);
    assert_eq!(out["truncated"], false);
    assert!(out["wall_ms"].as_f64().unwrap() > 0.0);
    assert!(!out["scan_metrics"].as_array().unwrap().is_empty());

    // SQL errors map to the envelope.
    let (status, err) = post_json(
        &router,
        "/api/query",
        Some(serde_json::json!({"sql": "SELECT nope FROM nowhere"})),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(err["code"], "invalid_input");
}

#[tokio::test]
async fn index_serves_embedded_html() {
    let (_dir, router, _db) = setup(false).await;
    // The index page needs no token — only a loopback Host.
    let res = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/")
                .header("host", "localhost:7777")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let html = String::from_utf8_lossy(&bytes);
    assert!(html.contains("h5i-db review"));
    assert!(html.contains("Apply plan"));
}

async fn raw_status(router: &axum::Router, req: Request<Body>) -> (StatusCode, serde_json::Value) {
    let res = router.clone().oneshot(req).await.unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let body = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, body)
}

#[tokio::test]
async fn guard_rejects_foreign_hosts_missing_tokens_and_csrf() {
    let (_dir, router, _db) = setup(false).await;

    // No token → 401 with the standard envelope.
    let (status, err) = raw_status(
        &router,
        Request::builder()
            .uri("/api/overview")
            .header("host", "127.0.0.1:8000")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(err["code"], "unauthorized");
    assert_eq!(err["retryable"], false);

    // Wrong token → 401.
    let (status, _) = raw_status(
        &router,
        Request::builder()
            .uri("/api/overview")
            .header("host", "127.0.0.1:8000")
            .header("authorization", "Bearer wrong")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // Foreign Host (DNS rebinding) → 403 even with the right token.
    let (status, err) = raw_status(
        &router,
        Request::builder()
            .uri("/api/overview")
            .header("host", "evil.example:80")
            .header("authorization", format!("Bearer {TEST_TOKEN}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(err["code"], "forbidden_host");

    // Missing Host header → 403.
    let (status, _) = raw_status(
        &router,
        Request::builder()
            .uri("/api/overview")
            .header("authorization", format!("Bearer {TEST_TOKEN}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // POST without the CSRF header → 403, even authenticated.
    let (status, err) = raw_status(
        &router,
        Request::builder()
            .method("POST")
            .uri("/api/query")
            .header("host", "127.0.0.1:8000")
            .header("authorization", format!("Bearer {TEST_TOKEN}"))
            .header("content-type", "application/json")
            .body(Body::from(r#"{"sql":"SELECT 1"}"#))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(err["code"], "csrf_required");

    // Foreign Host on the index page → 403 too.
    let (status, _) = raw_status(
        &router,
        Request::builder()
            .uri("/")
            .header("host", "evil.example")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // Bracketed IPv6 loopback with a port is accepted.
    let (status, _) = raw_status(
        &router,
        Request::builder()
            .uri("/api/overview")
            .header("host", "[::1]:9000")
            .header("authorization", format!("Bearer {TEST_TOKEN}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn scratchpad_query_times_out_with_envelope() {
    let (_dir, _router, db) = setup(false).await;
    let mut state = UiState::new(db.clone(), "test.db".into(), false);
    state.token = TEST_TOKEN.into();
    state.query_timeout = std::time::Duration::from_millis(100);
    let router = build_router(state);
    let (status, err) = post_json(
        &router,
        "/api/query",
        Some(serde_json::json!({
            "sql": "SELECT count(*) FROM trades a, trades b, trades c, trades d"
        })),
    )
    .await;
    assert_eq!(status, StatusCode::REQUEST_TIMEOUT);
    assert_eq!(err["code"], "query_timeout");
    assert_eq!(err["retryable"], false);
    assert!(err["hint"].as_str().unwrap().contains("CLI"));
}

#[tokio::test]
async fn scratchpad_disambiguates_duplicate_columns_and_flags_truncation() {
    let (_dir, router, _db) = setup(false).await;
    // Self-join: both sides expose a column whose output name is `price`.
    let (status, out) = post_json(
        &router,
        "/api/query",
        Some(serde_json::json!({
            "sql": "SELECT a.price, b.price FROM trades a JOIN trades b ON a.ts = b.ts LIMIT 3"
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["schema"][0]["name"], "price");
    assert_eq!(out["schema"][1]["name"], "price_1");
    let r0 = out["rows"][0].as_array().unwrap();
    assert!(r0[0].is_number());
    assert_eq!(r0[0], r0[1], "second column must not collapse to null");
    assert_eq!(out["truncated"], false);

    // 640 000-row cross join hits the 1 000-row cap and says so.
    let (status, out) = post_json(
        &router,
        "/api/query",
        Some(serde_json::json!({
            "sql": "SELECT a.symbol FROM trades a CROSS JOIN trades b"
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["rows"].as_array().unwrap().len(), 1000);
    assert_eq!(out["truncated"], true);
}
