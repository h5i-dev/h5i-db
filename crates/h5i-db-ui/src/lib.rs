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

use axum::extract::{Path as AxPath, Query, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::json;

use h5i_db_core::{Database, Error, MutationPlan, ReadAt};
use h5i_db_query::{H5iSession, SessionOptions};

const INDEX_HTML: &str = include_str!("../assets/index.html");
/// Rows returned to the browser per query/sample (the UI is a review
/// surface; exports belong to the CLI).
const UI_ROW_LIMIT: usize = 1_000;

#[derive(Clone)]
pub struct UiState {
    pub db: Arc<Database>,
    pub allow_mutations: bool,
    pub db_label: String,
}

/// Serve the UI on 127.0.0.1:`port`. Never binds a non-loopback address.
pub async fn serve(
    db: Arc<Database>,
    db_label: String,
    port: u16,
    allow_mutations: bool,
) -> Result<(), Error> {
    let state = UiState {
        db,
        allow_mutations,
        db_label,
    };
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
    eprintln!("  open   http://127.0.0.1:{port}");
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
        .route("/api/query", post(run_query))
        .with_state(state)
}

async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

// ---------------------------------------------------------------------------
// error envelope (same contract as the CLI)
// ---------------------------------------------------------------------------

struct ApiError(Error);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let e = &self.0;
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
}

impl From<Error> for ApiError {
    fn from(e: Error) -> Self {
        Self(e)
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
        Err(ApiError(Error::ReadOnly {
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
    let result = st.db.apply_plan(&plan).await?;
    Ok(Json(serde_json::to_value(&result).map_err(Error::from)?))
}

async fn plan_discard(
    State(st): State<UiState>,
    AxPath((table, id)): AxPath<(String, uuid::Uuid)>,
) -> ApiResult {
    require_mutations(&st)?;
    st.db.discard_plan(&table, id).await?;
    Ok(Json(json!({"discarded": id})))
}

#[derive(Deserialize)]
struct QueryBody {
    sql: String,
}

/// SQL scratchpad: read-only session per request, row-capped, with scan
/// metrics so pruning is visible in the UI.
async fn run_query(State(st): State<UiState>, Json(body): Json<QueryBody>) -> ApiResult {
    let started = std::time::Instant::now();
    let session = H5iSession::new(st.db.clone(), SessionOptions::default())
        .await
        .map_err(|e| ApiError(Error::internal(e)))?;
    let df = session
        .sql(&body.sql)
        .await
        .and_then(|df| df.limit(0, Some(UI_ROW_LIMIT)))
        .map_err(|e| ApiError(Error::invalid(e.to_string())))?;
    let batches = df
        .collect()
        .await
        .map_err(|e| ApiError(Error::invalid(e.to_string())))?;
    let mut out = batches_to_json(&batches)?;
    out["scan_metrics"] = serde_json::to_value(session.take_scan_metrics()).map_err(Error::from)?;
    out["wall_ms"] = json!(started.elapsed().as_secs_f64() * 1000.0);
    Ok(Json(out))
}

/// Render batches as `{schema: [...], rows: [[...]]}` for the browser.
fn batches_to_json(batches: &[arrow::array::RecordBatch]) -> Result<serde_json::Value, Error> {
    if batches.is_empty() {
        return Ok(json!({"schema": [], "rows": []}));
    }
    let schema = batches[0].schema();
    let fields: Vec<_> = schema
        .fields()
        .iter()
        .map(|f| json!({"name": f.name(), "type": f.data_type().to_string()}))
        .collect();
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
