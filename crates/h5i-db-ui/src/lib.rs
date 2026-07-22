//! Local review UI for h5i-db.
//!
//! A thin review surface, not a database GUI (DESIGN_CLAUDE.md §8-UI): its
//! job is letting a human inspect and approve what agents did or plan to do —
//! pending mutation plans first (they block someone), then version history,
//! snapshots, tables, and an SQL scratchpad.
//!
//! Server design follows `h5i serve`: axum on loopback only, single embedded
//! HTML asset, flat `/api/*` JSON endpoints, read-only unless the process was
//! started with `--allow-mutations` (and even then every apply re-validates
//! the plan's base version — a moved HEAD is a `version_conflict`, never a
//! silent re-plan).

use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path as AxPath, Query, Request, State};
use axum::http::{header, Method, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::json;
use tower::limit::ConcurrencyLimitLayer;
use tower_http::trace::TraceLayer;

use h5i_db_core::{Database, Error, MutationPlan, ReadAt};
use h5i_db_query::{H5iSession, SessionOptions};

const INDEX_HTML: &str = include_str!("../assets/index.html");
/// Rows returned to the browser per query/sample (the UI is a review
/// surface; exports belong to the CLI).
const UI_ROW_LIMIT: usize = 1_000;
/// Wall-clock budget for a scratchpad query; long exports belong to the CLI.
const UI_QUERY_TIMEOUT: Duration = Duration::from_secs(30);
/// Memory cap per scratchpad session; DataFusion spills past this.
const UI_QUERY_MEMORY_LIMIT: usize = 512 << 20;
/// Scratchpad queries running at once; extra requests queue at the route.
const UI_QUERY_CONCURRENCY: usize = 2;
/// Header required on every mutating request. Cross-origin pages cannot set
/// custom headers without a CORS preflight, and we never answer preflights.
const CSRF_HEADER: &str = "x-h5i-csrf";

#[derive(Clone)]
pub struct UiState {
    pub db: Arc<Database>,
    pub allow_mutations: bool,
    pub db_label: String,
    /// Per-process bearer token; the startup URL carries it once.
    pub token: String,
    pub query_timeout: Duration,
    pub query_memory_limit: usize,
    /// Deterministic fault-injection hook for timeout tests; zero in normal
    /// construction and not exposed by the server CLI.
    #[doc(hidden)]
    pub query_start_delay: Duration,
}

impl UiState {
    /// State with a fresh random token and default query limits.
    pub fn new(db: Arc<Database>, db_label: String, allow_mutations: bool) -> Self {
        Self {
            db,
            allow_mutations,
            db_label,
            token: uuid::Uuid::new_v4().simple().to_string(),
            query_timeout: UI_QUERY_TIMEOUT,
            query_memory_limit: UI_QUERY_MEMORY_LIMIT,
            query_start_delay: Duration::ZERO,
        }
    }
}

/// Serve the UI on 127.0.0.1:`port`. Never binds a non-loopback address.
pub async fn serve(
    db: Arc<Database>,
    db_label: String,
    port: u16,
    allow_mutations: bool,
) -> Result<(), Error> {
    let state = UiState::new(db, db_label, allow_mutations);
    let token = state.token.clone();
    let app = build_router(state);
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));
    let listener = tokio::net::TcpListener::bind(addr).await.map_err(|e| {
        if e.kind() == std::io::ErrorKind::AddrInUse {
            Error::invalid(format!(
                "port {port} is already in use — pick another with `h5i-db ui --port <N>`"
            ))
        } else {
            Error::io(addr.to_string(), e)
        }
    })?;
    eprintln!("h5i-db review UI");
    eprintln!("  open   http://127.0.0.1:{port}/?token={token}");
    eprintln!("         (the URL carries this session's access token — the API refuses requests without it)");
    eprintln!(
        "  mode   {}",
        if allow_mutations {
            "mutations allowed (plan apply/discard enabled)"
        } else {
            "read-only (start with --allow-mutations to approve plans)"
        }
    );
    eprintln!("  stop   Ctrl+C");
    axum::serve(listener, app)
        .await
        .map_err(|e| Error::internal(format!("server error: {e}")))
}

/// Router extracted for tests.
pub fn build_router(state: UiState) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/api/overview", get(overview))
        .route("/api/table/{name}", get(table_detail))
        .route("/api/table/{name}/sample", get(table_sample))
        .route("/api/table/{name}/diff", get(version_diff))
        .route("/api/plan/{table}/{id}", get(plan_detail))
        .route("/api/plan/{table}/{id}/apply", post(plan_apply))
        .route("/api/plan/{table}/{id}/discard", post(plan_discard))
        .route(
            "/api/query",
            post(run_query).layer(ConcurrencyLimitLayer::new(UI_QUERY_CONCURRENCY)),
        )
        .layer(middleware::from_fn_with_state(state.clone(), guard))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

// ---------------------------------------------------------------------------
// request guard: Host allowlist + bearer token + CSRF header
// ---------------------------------------------------------------------------

/// Reject anything a hostile web page could make a browser send here: DNS
/// rebinding arrives with a foreign `Host`; cross-site requests cannot carry
/// the bearer token (never issued cross-origin) or a custom header.
async fn guard(State(st): State<UiState>, req: Request, next: Next) -> Response {
    let host_ok = req
        .headers()
        .get(header::HOST)
        .and_then(|h| h.to_str().ok())
        .is_some_and(host_is_loopback);
    if !host_ok {
        return guard_reject(
            StatusCode::FORBIDDEN,
            "forbidden_host",
            "request Host is not a loopback name",
            "the review UI only answers to localhost / 127.0.0.1 / [::1]",
        );
    }
    if req.uri().path().starts_with("/api/") {
        let authed = req
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|h| h.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .is_some_and(|t| token_eq(t, &st.token));
        if !authed {
            return guard_reject(
                StatusCode::UNAUTHORIZED,
                "unauthorized",
                "missing or wrong access token",
                "open the UI through the exact URL printed at startup — it carries the token",
            );
        }
        if req.method() == Method::POST && !req.headers().contains_key(CSRF_HEADER) {
            return guard_reject(
                StatusCode::FORBIDDEN,
                "csrf_required",
                "mutating request without the CSRF header",
                "send `x-h5i-csrf: 1` on POST requests",
            );
        }
    }
    next.run(req).await
}

fn guard_reject(status: StatusCode, code: &str, message: &str, hint: &str) -> Response {
    let body = json!({"code": code, "message": message, "retryable": false, "hint": hint});
    (status, Json(body)).into_response()
}

/// `Host` allowlist (port ignored): loopback names only.
fn host_is_loopback(host: &str) -> bool {
    let h = host.trim();
    let bare = if let Some(rest) = h.strip_prefix('[') {
        match rest.split_once(']') {
            Some((ip, _)) => ip,
            None => return false,
        }
    } else {
        match h.rsplit_once(':') {
            Some((name, port)) if !port.is_empty() && port.bytes().all(|b| b.is_ascii_digit()) => {
                name
            }
            _ => h,
        }
    };
    matches!(
        bare.to_ascii_lowercase().as_str(),
        "localhost" | "127.0.0.1" | "::1"
    )
}

/// Constant-time-ish comparison; enough to blunt timing probes on loopback.
fn token_eq(a: &str, b: &str) -> bool {
    a.len() == b.len()
        && a.bytes()
            .zip(b.bytes())
            .fold(0u8, |acc, (x, y)| acc | (x ^ y))
            == 0
}

// ---------------------------------------------------------------------------
// error envelope (same contract as the CLI)
// ---------------------------------------------------------------------------

enum ApiError {
    Db(Error),
    /// Envelope not backed by a core error (e.g. scratchpad timeout).
    Custom {
        status: StatusCode,
        code: &'static str,
        message: String,
        hint: &'static str,
    },
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        match self {
            ApiError::Db(e) => {
                let status = match e.exit_category() {
                    h5i_db_core::ExitCategory::UserError => StatusCode::BAD_REQUEST,
                    h5i_db_core::ExitCategory::Conflict => StatusCode::CONFLICT,
                    h5i_db_core::ExitCategory::Limit => StatusCode::TOO_MANY_REQUESTS,
                    _ => StatusCode::INTERNAL_SERVER_ERROR,
                };
                let body = json!({
                    "code": e.code(),
                    "message": e.to_string(),
                    "retryable": e.retryable(),
                    "hint": e.hint(),
                });
                (status, Json(body)).into_response()
            }
            ApiError::Custom {
                status,
                code,
                message,
                hint,
            } => {
                let body =
                    json!({"code": code, "message": message, "retryable": false, "hint": hint});
                (status, Json(body)).into_response()
            }
        }
    }
}

impl From<Error> for ApiError {
    fn from(e: Error) -> Self {
        Self::Db(e)
    }
}

type ApiResult = Result<Json<serde_json::Value>, ApiError>;

// ---------------------------------------------------------------------------
// handlers
// ---------------------------------------------------------------------------

/// The triage projection: everything the front page needs in one call —
/// pending plans (needs-a-human), tables, snapshots, policy, mode.
async fn overview(State(st): State<UiState>) -> ApiResult {
    let mut tables = Vec::new();
    let mut plans = Vec::new();
    for entry in st.db.list_tables().await? {
        let resolved = st.db.resolve(&entry.name, ReadAt::Latest).await?;
        tables.push(json!({
            "name": entry.name,
            "version": resolved.manifest.sequence,
            "rows": resolved.manifest.rows,
            "bytes": resolved.manifest.bytes,
            "segments": resolved.manifest.segments.len(),
            "time_range": resolved.manifest.time_range,
            "time_column": resolved.spec.time_column,
            "committed_at_ns": resolved.manifest.committed_at_ns,
            "op": resolved.manifest.op.to_string(),
            "execution_mode": resolved.manifest.execution_mode,
        }));
        for plan in st.db.list_plans(&entry.name).await? {
            plans.push(plan_summary_json(
                &plan,
                st.db
                    .resolve(&entry.name, ReadAt::Latest)
                    .await?
                    .head_sequence,
            ));
        }
    }
    let snapshots: Vec<_> = st
        .db
        .list_snapshots()
        .await?
        .iter()
        .map(|s| {
            json!({
                "name": s.name,
                "created_at_ns": s.created_at_ns,
                "note": s.note,
                "tables": s.entries.values().map(|e| json!({
                    "table": e.table_name, "version": e.sequence,
                })).collect::<Vec<_>>(),
            })
        })
        .collect();
    Ok(Json(json!({
        "db": st.db_label,
        "allow_mutations": st.allow_mutations,
        "read_only_db": st.db.is_read_only(),
        "policy": st.db.policy().await?,
        "plans": plans,
        "tables": tables,
        "snapshots": snapshots,
    })))
}

fn plan_summary_json(plan: &MutationPlan, head: u64) -> serde_json::Value {
    json!({
        "plan_id": plan.plan_id,
        "table": plan.table,
        "op": plan.op.to_string(),
        "base_version": plan.base_version,
        "head_version": head,
        // A plan whose base is no longer head can only conflict: surface it.
        "stale": head != plan.base_version,
        "expired": plan.is_expired(),
        "created_at_ns": plan.created_at_ns,
        "expires_at_ns": plan.expires_at_ns,
        "note": plan.note,
        "summary": plan.summary,
    })
}

async fn table_detail(State(st): State<UiState>, AxPath(name): AxPath<String>) -> ApiResult {
    let resolved = st.db.resolve(&name, ReadAt::Latest).await?;
    let versions = st.db.list_versions(&name).await?;
    let fields: Vec<_> = resolved
        .schema
        .fields()
        .iter()
        .map(|f| {
            json!({
                "name": f.name(),
                "type": f.data_type().to_string(),
                "nullable": f.is_nullable(),
            })
        })
        .collect();
    // list_versions only exposes summaries; re-read manifests for the audit
    // columns the review UI cares about.
    let mut version_rows = Vec::with_capacity(versions.len());
    for v in &versions {
        let m = st
            .db
            .resolve(&name, ReadAt::Version(v.sequence))
            .await?
            .manifest;
        version_rows.push(json!({
            "version": v.sequence,
            "op": v.op,
            "committed_at_ns": v.committed_at_ns,
            "rows": v.rows,
            "bytes": v.bytes,
            "segments": v.segments,
            "note": v.note,
            "execution_mode": m.execution_mode,
            "plan_hash": m.plan_hash,
        }));
    }
    Ok(Json(json!({
        "name": name,
        "schema": fields,
        "time_column": resolved.spec.time_column,
        "sort_key": resolved.spec.sort_key,
        "head": resolved.head_sequence,
        "versions": version_rows,
    })))
}

#[derive(Deserialize)]
struct SampleParams {
    version: Option<u64>,
    #[serde(default = "default_sample_rows")]
    rows: usize,
}

fn default_sample_rows() -> usize {
    50
}

async fn table_sample(
    State(st): State<UiState>,
    AxPath(name): AxPath<String>,
    Query(p): Query<SampleParams>,
) -> ApiResult {
    let at = p.version.map(ReadAt::Version).unwrap_or(ReadAt::Latest);
    let (batches, _) = st
        .db
        .scan(
            &name,
            at,
            h5i_db_core::ScanOptions {
                limit: Some(p.rows.min(UI_ROW_LIMIT)),
                ..Default::default()
            },
        )
        .await?;
    Ok(Json(batches_to_json(&batches)?))
}

#[derive(Deserialize)]
struct DiffParams {
    from: u64,
    to: u64,
}

/// Version diff: rollup deltas plus segment-level added/removed/kept.
async fn version_diff(
    State(st): State<UiState>,
    AxPath(name): AxPath<String>,
    Query(p): Query<DiffParams>,
) -> ApiResult {
    let a = st
        .db
        .resolve(&name, ReadAt::Version(p.from))
        .await?
        .manifest;
    let b = st.db.resolve(&name, ReadAt::Version(p.to)).await?.manifest;
    let a_ids: std::collections::BTreeMap<_, _> = a.segments.iter().map(|s| (s.id, s)).collect();
    let b_ids: std::collections::BTreeMap<_, _> = b.segments.iter().map(|s| (s.id, s)).collect();
    let added: Vec<_> = b
        .segments
        .iter()
        .filter(|s| !a_ids.contains_key(&s.id))
        .map(|s| json!({"id": s.id, "rows": s.rows, "bytes": s.bytes, "time_range": s.time_range}))
        .collect();
    let removed: Vec<_> = a
        .segments
        .iter()
        .filter(|s| !b_ids.contains_key(&s.id))
        .map(|s| json!({"id": s.id, "rows": s.rows, "bytes": s.bytes, "time_range": s.time_range}))
        .collect();
    let kept = b
        .segments
        .iter()
        .filter(|s| a_ids.contains_key(&s.id))
        .count();
    Ok(Json(json!({
        "table": name,
        "from": {"version": a.sequence, "rows": a.rows, "bytes": a.bytes, "op": a.op.to_string(), "schema_revision": a.schema_revision},
        "to": {"version": b.sequence, "rows": b.rows, "bytes": b.bytes, "op": b.op.to_string(), "schema_revision": b.schema_revision, "execution_mode": b.execution_mode, "note": b.note},
        "rows_delta": b.rows as i64 - a.rows as i64,
        "bytes_delta": b.bytes as i64 - a.bytes as i64,
        "schema_changed": a.schema_revision != b.schema_revision,
        "segments": {"added": added, "removed": removed, "kept": kept},
    })))
}

async fn plan_detail(
    State(st): State<UiState>,
    AxPath((table, id)): AxPath<(String, uuid::Uuid)>,
) -> ApiResult {
    let plan = st.db.load_plan(&table, id).await?;
    let head = st.db.resolve(&table, ReadAt::Latest).await?.head_sequence;
    let mut out = plan_summary_json(&plan, head);
    let decode = |b64: &Option<String>| -> Result<serde_json::Value, Error> {
        match b64 {
            None => Ok(serde_json::Value::Null),
            Some(b) => {
                let batches = MutationPlan::decode_sample(b)?;
                batches_to_json(&batches)
            }
        }
    };
    out["before_sample"] = decode(&plan.before_sample_ipc_b64)?;
    out["after_sample"] = decode(&plan.after_sample_ipc_b64)?;
    Ok(Json(out))
}

fn require_mutations(st: &UiState) -> Result<(), ApiError> {
    if st.allow_mutations {
        Ok(())
    } else {
        Err(ApiError::Db(Error::ReadOnly {
            op: "plan apply/discard from the UI (restart with --allow-mutations)".into(),
        }))
    }
}

async fn plan_apply(
    State(st): State<UiState>,
    AxPath((table, id)): AxPath<(String, uuid::Uuid)>,
) -> ApiResult {
    require_mutations(&st)?;
    let plan = st.db.load_plan(&table, id).await?;
    tracing::info!(table = %table, plan_id = %id, op = %plan.op, "ui: plan apply requested");
    let result = match st.db.apply_plan(&plan).await {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(table = %table, plan_id = %id, code = e.code(), "ui: plan apply failed");
            return Err(e.into());
        }
    };
    let value = serde_json::to_value(&result).map_err(Error::from)?;
    tracing::info!(
        table = %table,
        plan_id = %id,
        version = ?value.get("sequence").and_then(|v| v.as_u64()),
        "ui: plan applied"
    );
    Ok(Json(value))
}

async fn plan_discard(
    State(st): State<UiState>,
    AxPath((table, id)): AxPath<(String, uuid::Uuid)>,
) -> ApiResult {
    require_mutations(&st)?;
    st.db.discard_plan(&table, id).await?;
    tracing::info!(table = %table, plan_id = %id, "ui: plan discarded");
    Ok(Json(json!({"discarded": id})))
}

#[derive(Deserialize)]
struct QueryBody {
    sql: String,
}

/// SQL scratchpad: read-only session per request, memory-capped, row-capped,
/// wall-clock-bounded, with scan metrics so pruning is visible in the UI.
async fn run_query(State(st): State<UiState>, Json(body): Json<QueryBody>) -> ApiResult {
    let started = std::time::Instant::now();
    let session = H5iSession::new(
        st.db.clone(),
        SessionOptions {
            memory_limit: Some(st.query_memory_limit),
            ..Default::default()
        },
    )
    .await
    .map_err(|e| ApiError::Db(Error::internal(e)))?;
    let execute = async {
        if !st.query_start_delay.is_zero() {
            tokio::time::sleep(st.query_start_delay).await;
        }
        // Fetch one row past the cap so truncation is detectable.
        let df = session
            .sql_reported(&body.sql)
            .await
            .and_then(|df| df.limit(0, Some(UI_ROW_LIMIT + 1)))
            .map_err(|e| ApiError::Db(Error::invalid(e.to_string())))?;
        let schema = df.schema();
        let (batches, report) = df
            .collect()
            .await
            .map_err(|e| ApiError::Db(Error::invalid(e.to_string())))?;
        Ok::<_, ApiError>((schema, batches, report))
    };
    let (schema, mut batches, report) = match tokio::time::timeout(st.query_timeout, execute).await
    {
        Err(_) => {
            return Err(ApiError::Custom {
                status: StatusCode::REQUEST_TIMEOUT,
                code: "query_timeout",
                message: format!(
                    "query exceeded the {:?} scratchpad budget",
                    st.query_timeout
                ),
                hint: "narrow the query, or run it via the CLI which has no interactive timeout",
            })
        }
        Ok(r) => r?,
    };
    let truncated = truncate_batches(&mut batches, UI_ROW_LIMIT);
    let mut out = batches_to_json_with_schema(&batches, Some(schema))?;
    out["truncated"] = json!(truncated);
    out["scan_metrics"] = serde_json::to_value(&report.scans).map_err(Error::from)?;
    out["performance"] = serde_json::to_value(report).map_err(Error::from)?;
    out["wall_ms"] = json!(started.elapsed().as_secs_f64() * 1000.0);
    Ok(Json(out))
}

/// Cap total rows across `batches`; returns whether anything was dropped.
fn truncate_batches(batches: &mut Vec<arrow::array::RecordBatch>, cap: usize) -> bool {
    let mut seen = 0usize;
    for i in 0..batches.len() {
        let n = batches[i].num_rows();
        if seen + n > cap {
            batches[i] = batches[i].slice(0, cap - seen);
            batches.truncate(i + 1);
            return true;
        }
        seen += n;
    }
    false
}

/// Render batches as `{schema: [...], rows: [[...]]}` for the browser.
fn batches_to_json(batches: &[arrow::array::RecordBatch]) -> Result<serde_json::Value, Error> {
    batches_to_json_with_schema(batches, None)
}

/// Like [`batches_to_json`], but keeps the schema when there are no batches.
fn batches_to_json_with_schema(
    batches: &[arrow::array::RecordBatch],
    empty_schema: Option<arrow::datatypes::SchemaRef>,
) -> Result<serde_json::Value, Error> {
    let schema = match batches.first() {
        Some(b) => b.schema(),
        None => match empty_schema {
            Some(s) => s,
            None => return Ok(json!({"schema": [], "rows": []})),
        },
    };
    // arrow-json emits row *objects*, so duplicate output names (e.g.
    // `SELECT a.x, b.x`) would collapse; disambiguate with `_N` suffixes.
    let mut used = std::collections::HashSet::new();
    let mut names = Vec::with_capacity(schema.fields().len());
    for f in schema.fields() {
        let mut name = f.name().clone();
        let mut n = 1usize;
        while !used.insert(name.clone()) {
            name = format!("{}_{n}", f.name());
            n += 1;
        }
        names.push(name);
    }
    let owned: Vec<arrow::array::RecordBatch>;
    let (schema, batches) = if names
        .iter()
        .zip(schema.fields())
        .any(|(n, f)| n != f.name())
    {
        let fields: Vec<_> = schema
            .fields()
            .iter()
            .zip(&names)
            .map(|(f, n)| f.as_ref().clone().with_name(n))
            .collect();
        let renamed = Arc::new(arrow::datatypes::Schema::new_with_metadata(
            fields,
            schema.metadata().clone(),
        ));
        owned = batches
            .iter()
            .map(|b| {
                arrow::array::RecordBatch::try_new(renamed.clone(), b.columns().to_vec())
                    .map_err(Error::Arrow)
            })
            .collect::<Result<_, _>>()?;
        (renamed, owned.as_slice())
    } else {
        (schema, batches)
    };
    let fields: Vec<_> = schema
        .fields()
        .iter()
        .map(|f| json!({"name": f.name(), "type": f.data_type().to_string()}))
        .collect();
    if batches.iter().all(|b| b.num_rows() == 0) {
        return Ok(json!({"schema": fields, "rows": []}));
    }
    // arrow-json produces row objects; convert to arrays to preserve column
    // order and keep payloads small.
    let mut buf = Vec::new();
    {
        let mut writer = arrow::json::writer::WriterBuilder::new()
            .with_explicit_nulls(true)
            .build::<_, arrow::json::writer::JsonArray>(&mut buf);
        for b in batches {
            writer.write(b).map_err(Error::Arrow)?;
        }
        writer.finish().map_err(Error::Arrow)?;
    }
    let objects: Vec<serde_json::Map<String, serde_json::Value>> = serde_json::from_slice(&buf)?;
    let rows: Vec<Vec<serde_json::Value>> = objects
        .into_iter()
        .map(|mut obj| {
            schema
                .fields()
                .iter()
                .map(|f| obj.remove(f.name()).unwrap_or(serde_json::Value::Null))
                .collect()
        })
        .collect();
    Ok(json!({"schema": fields, "rows": rows}))
}
