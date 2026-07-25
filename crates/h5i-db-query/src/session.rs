//! `H5iSession`: a preconfigured DataFusion `SessionContext` over an h5i-db
//! database — tables registered snapshot-bound at session creation, resource
//! limits wired to the runtime, time-travel UDTF and time-series UDFs
//! installed, and scan metrics collected per query.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use datafusion::dataframe::DataFrame;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::disk_manager::DiskManagerBuilder;
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use h5i_db_core::{Database, ReadAt, ResolvedTable};
use h5i_db_observability::WorkloadTelemetryBuffer;
use object_store::{ObjectStoreExt, path::Path as ObjectPath};
use uuid::Uuid;

use crate::PredicateCacheMode;
use crate::asof::{AsOfJoinFunc, AsOfQueryPlanner};
use crate::finance::{ewma_udwf, vwap_udaf, wavg_udaf};
use crate::functions::time_bucket_udf;
use crate::gapfill::GapFillFunc;
use crate::latest::LatestByFunc;
use crate::metrics::{
    QueryPerformanceReport, ReportedDataFrame, ScanMetrics, ScanMetricsCollector,
    WorkloadTelemetryEnvelope,
};
use crate::provider::H5iTableProvider;
use crate::tail::TailFunc;
use crate::udtf::TimeTravelFunc;

/// Resource and execution options for a session.
#[derive(Debug, Clone, Default)]
pub struct SessionOptions {
    /// Cap on query memory in bytes; enables spilling when exceeded.
    pub memory_limit: Option<usize>,
    /// Directory for spill files (temp dir by default when a limit is set).
    pub spill_dir: Option<PathBuf>,
    /// Parallelism (defaults to the number of cores).
    pub target_partitions: Option<usize>,
    pub batch_size: Option<usize>,
    /// Privacy-preserving query reports retained in memory. Zero disables
    /// workload telemetry; collection is deliberately opt-in.
    pub telemetry_capacity: usize,
    /// Disposable predicate sidecar policy. Disabled by default so opening a
    /// query session never introduces hidden writes.
    pub predicate_cache: PredicateCacheMode,
}

pub struct H5iSession {
    ctx: SessionContext,
    db: Arc<Database>,
    url: ObjectStoreUrl,
    metrics: ScanMetricsCollector,
    session_id: Uuid,
    telemetry: WorkloadTelemetryBuffer,
    predicate_cache_mode: PredicateCacheMode,
    /// Catalog table names currently registered (kept for [`Self::refresh`]).
    registered: Mutex<HashSet<String>>,
}

impl H5iSession {
    /// Build a session over a database. Table heads are resolved once, here:
    /// every query in this session sees the same immutable versions.
    pub async fn new(db: Arc<Database>, options: SessionOptions) -> DfResult<Self> {
        let runtime = Self::build_runtime(&options)?;
        Self::new_with_runtime(db, options, runtime).await
    }

    /// Build a session that pins **every** table at `at` (e.g.
    /// `ReadAt::AsOf(ts)`) instead of its latest version. This is the basis of
    /// the point-in-time / look-ahead-bias-free surface (ROADMAP V-A1): a query
    /// run in such a session sees only data available as of `at`.
    pub async fn new_at(db: Arc<Database>, options: SessionOptions, at: ReadAt) -> DfResult<Self> {
        Self::new_pinned(db, options, ResearchPin::at(at)).await
    }

    /// Build a session pinned on both research-mode axes (ROADMAP Part VI).
    ///
    /// The arrival pin alone cannot see the most common look-ahead bugs — a
    /// window overrunning into future rows, a join reading later timestamps —
    /// because those rows were always in the table. The event-time cutoff
    /// closes that: it is injected into every scan, so no query in the session
    /// can read a row stamped after it.
    pub async fn new_pinned(
        db: Arc<Database>,
        options: SessionOptions,
        pin: ResearchPin,
    ) -> DfResult<Self> {
        let runtime = Self::build_runtime(&options)?;
        Self::new_with_runtime_pinned(db, options, runtime, pin).await
    }

    fn build_runtime(options: &SessionOptions) -> DfResult<Arc<RuntimeEnv>> {
        let mut runtime = RuntimeEnvBuilder::new();
        if let Some(limit) = options.memory_limit {
            runtime = runtime.with_memory_pool(Arc::new(FairSpillPool::new(limit)));
            let disk = match &options.spill_dir {
                Some(dir) => DiskManagerBuilder::default().with_mode(
                    datafusion::execution::disk_manager::DiskManagerMode::Directories(vec![
                        dir.clone(),
                    ]),
                ),
                None => DiskManagerBuilder::default(),
            };
            runtime = runtime.with_disk_manager_builder(disk);
        }
        Ok(Arc::new(runtime.build()?))
    }

    /// Like [`Self::new`], but reusing a caller-supplied [`RuntimeEnv`].
    ///
    /// The runtime owns the Parquet footer-metadata cache (~40% of warm scan
    /// latency) and the memory pool, so passing the previous session's
    /// [`Self::runtime_env`] here makes "new session over fresh data" cheap
    /// instead of cache-cold. `options.memory_limit` / `options.spill_dir`
    /// are ignored — they describe a runtime, and this one already exists.
    pub async fn new_with_runtime(
        db: Arc<Database>,
        options: SessionOptions,
        runtime: Arc<RuntimeEnv>,
    ) -> DfResult<Self> {
        Self::new_with_runtime_at(db, options, runtime, ReadAt::Latest).await
    }

    /// Like [`Self::new_with_runtime`], but pins every table at `at` instead of
    /// its latest version (ROADMAP V-A1). See [`Self::new_at`].
    pub async fn new_with_runtime_at(
        db: Arc<Database>,
        options: SessionOptions,
        runtime: Arc<RuntimeEnv>,
        at: ReadAt,
    ) -> DfResult<Self> {
        Self::new_with_runtime_pinned(db, options, runtime, ResearchPin::at(at)).await
    }

    /// Like [`Self::new_pinned`], but reusing a caller-supplied [`RuntimeEnv`].
    pub async fn new_with_runtime_pinned(
        db: Arc<Database>,
        options: SessionOptions,
        runtime: Arc<RuntimeEnv>,
        pin: ResearchPin,
    ) -> DfResult<Self> {
        let ResearchPin {
            at,
            event_time_cutoff_ns,
        } = pin;
        let telemetry = WorkloadTelemetryBuffer::new(options.telemetry_capacity);
        let predicate_cache_mode = options.predicate_cache;
        let mut config = SessionConfig::new().with_information_schema(true);
        if let Some(tp) = options.target_partitions {
            config = config.with_target_partitions(tp.max(1));
        }
        if let Some(bs) = options.batch_size {
            config = config.with_batch_size(bs.max(64));
        }
        // DataFusion 54's physical uncorrelated-scalar-subquery path dedups
        // subqueries by logical-plan equality — but every invocation of a
        // table function plans under one bare name (`h5i()`, `asof_join()`,
        // …) and `TableScan` equality never consults the provider instance,
        // so two subqueries over *different* versions of one table compare
        // equal and collapse into a single shared result, silently returning
        // one side's value for both (e.g. `WHERE ts = (SELECT max(ts) FROM
        // h5i('t', 1))` filtered by version 2's max). Route scalar subqueries
        // through the ScalarSubqueryToJoin rewrite instead, which keeps each
        // subquery's own plan (and provider) intact.
        config
            .options_mut()
            .optimizer
            .enable_physical_uncorrelated_scalar_subquery = false;

        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(runtime)
            .with_default_features()
            .with_query_planner(Arc::new(AsOfQueryPlanner))
            .build();
        let ctx = SessionContext::new_with_state(state);

        // Register the database's object store under a stable per-DB URL so
        // segment paths in FileScanConfig resolve to our backend.
        let url_string = format!(
            "h5i://{}/",
            h5i_db_core::util::checksum_hex(db.backend().base_url.as_str().as_bytes())
                .chars()
                .take(16)
                .collect::<String>()
        );
        let url = ObjectStoreUrl::parse(&url_string)?;
        ctx.register_object_store(url.as_ref(), db.backend().store.clone());

        let metrics = ScanMetricsCollector::default();

        // Snapshot-bound registration of every table at its current head,
        // resolved concurrently (serial resolution dominated multi-table
        // session startup).
        let mut registered = HashSet::new();
        for resolved in resolve_all_at(&db, at.clone()).await? {
            let name = resolved.entry.name.clone();
            let time_column = resolved.spec.time_column.clone();
            let schema = resolved.schema.clone();
            let provider = Arc::new(
                H5iTableProvider::new(resolved, url.clone(), metrics.clone())
                    .with_predicate_cache(db.backend().clone(), predicate_cache_mode),
            );
            match event_time_cutoff_ns {
                // Register the table behind a view carrying the cutoff, so the
                // predicate is part of every plan that names it rather than
                // something a query could forget. It optimizes like any other
                // filter, which means it prunes segments too.
                Some(cutoff_ns) => {
                    let view = embargoed_view(
                        &name,
                        provider,
                        &schema,
                        time_column.as_deref(),
                        cutoff_ns,
                    )?;
                    ctx.register_table(&name, view)?;
                }
                None => {
                    ctx.register_table(&name, provider)?;
                }
            }
            registered.insert(name);
        }

        // Time travel + time-series functions. These resolve their tables
        // themselves rather than through the catalog, so they are handed the
        // session's pin explicitly; without it they would read past it (see
        // `pin.rs`).
        let pin = crate::pin::PinContext::new(at.clone(), event_time_cutoff_ns);
        ctx.register_udtf(
            "h5i",
            Arc::new(TimeTravelFunc::pinned(
                db.clone(),
                url.clone(),
                metrics.clone(),
                pin.clone(),
            )),
        );
        ctx.register_udtf(
            "asof_join",
            Arc::new(AsOfJoinFunc::pinned(
                db.clone(),
                url.clone(),
                metrics.clone(),
                pin.clone(),
            )),
        );
        ctx.register_udtf(
            "gapfill",
            Arc::new(GapFillFunc::pinned(db.clone(), pin.clone())),
        );
        ctx.register_udtf(
            "resample",
            Arc::new(GapFillFunc::pinned(db.clone(), pin.clone())),
        );
        ctx.register_udtf("tail", Arc::new(TailFunc::pinned(db.clone(), pin.clone())));
        ctx.register_udtf("latest_on", Arc::new(LatestByFunc::pinned(db.clone(), pin)));
        ctx.register_udf(time_bucket_udf());
        ctx.register_udaf(vwap_udaf());
        ctx.register_udaf(wavg_udaf());
        ctx.register_udwf(ewma_udwf());
        // Part VII-B1 rolling operators (only those DataFusion lacks; see
        // `crate::rolling` for the reachable-as mapping of the rest).
        for udwf in crate::rolling::rolling_udwfs() {
            ctx.register_udwf(udwf);
        }

        Ok(Self {
            ctx,
            db,
            url,
            metrics,
            session_id: Uuid::new_v4(),
            telemetry,
            predicate_cache_mode,
            registered: Mutex::new(registered),
        })
    }

    /// Re-point every catalog table at its latest version without rebuilding
    /// the session: caches, UDFs, and the runtime survive. New tables appear,
    /// dropped tables disappear. `h5i('t')` never needs this — it re-resolves
    /// at planning time — this is for the plain `SELECT … FROM t` names that
    /// are otherwise snapshot-bound to session creation.
    pub async fn refresh(&self) -> DfResult<()> {
        let resolved = resolve_all_at(&self.db, ReadAt::Latest).await?;
        let fresh: HashSet<String> = resolved.iter().map(|r| r.entry.name.clone()).collect();
        let mut registered = self.registered.lock().unwrap();
        for stale in registered.difference(&fresh) {
            self.ctx.deregister_table(stale)?;
        }
        for r in resolved {
            let name = r.entry.name.clone();
            if registered.contains(&name) {
                self.ctx.deregister_table(&name)?;
            }
            self.ctx.register_table(
                &name,
                Arc::new(
                    H5iTableProvider::new(r, self.url.clone(), self.metrics.clone())
                        .with_predicate_cache(self.db.backend().clone(), self.predicate_cache_mode),
                ),
            )?;
        }
        *registered = fresh;
        Ok(())
    }

    /// The session's runtime environment — pass to [`Self::new_with_runtime`]
    /// to share the footer-metadata cache and memory pool across sessions.
    pub fn runtime_env(&self) -> Arc<RuntimeEnv> {
        self.ctx.runtime_env()
    }

    pub fn context(&self) -> &SessionContext {
        &self.ctx
    }

    pub fn database(&self) -> &Arc<Database> {
        &self.db
    }

    pub fn object_store_url(&self) -> &ObjectStoreUrl {
        &self.url
    }

    pub fn metrics_collector(&self) -> &ScanMetricsCollector {
        &self.metrics
    }

    /// Run SQL and return a lazy DataFrame.
    pub async fn sql(&self, query: &str) -> DfResult<DataFrame> {
        let rewritten = rewrite_rolling_sugar(&rewrite_asof_join(query)?)?;
        self.ctx.sql(&rewritten).await
    }

    /// Plan SQL for query-local physical metrics and workload telemetry.
    /// Unlike [`Self::take_scan_metrics`], concurrent executions cannot drain
    /// or mix one another's scan records.
    pub async fn sql_reported(&self, query: &str) -> DfResult<ReportedDataFrame> {
        let started = std::time::Instant::now();
        let dataframe = self.sql(query).await?;
        let logical_planning_ns = started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        Ok(ReportedDataFrame::new(
            dataframe,
            query,
            logical_planning_ns,
            self.metrics.clone(),
            self.telemetry.clone(),
        ))
    }

    /// DataFrame over a table at a given read point (DataFrame-API entry).
    pub async fn read_table(&self, name: &str, at: ReadAt) -> DfResult<DataFrame> {
        let resolved = self
            .db
            .resolve(name, at)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let provider = Arc::new(
            H5iTableProvider::new(resolved, self.url.clone(), self.metrics.clone())
                .with_predicate_cache(self.db.backend().clone(), self.predicate_cache_mode),
        );
        self.ctx.read_table(provider)
    }

    /// Drain per-scan pruning metrics recorded since the last call.
    pub fn take_scan_metrics(&self) -> Vec<ScanMetrics> {
        self.metrics.take()
    }

    /// Snapshot the bounded telemetry ring without clearing it.
    pub fn workload_telemetry(&self) -> Vec<QueryPerformanceReport> {
        self.telemetry.snapshot()
    }

    pub fn clear_workload_telemetry(&self) {
        self.telemetry.clear();
    }

    /// Persist this session's bounded telemetry snapshot as one replaceable
    /// sidecar. Query text and literal values are never written.
    pub async fn flush_workload_telemetry(&self) -> DfResult<Option<String>> {
        let reports = self.telemetry.snapshot();
        if reports.is_empty() {
            return Ok(None);
        }
        let envelope = WorkloadTelemetryEnvelope {
            format: 1,
            session_id: self.session_id,
            reports,
        };
        let bytes =
            serde_json::to_vec(&envelope).map_err(|e| DataFusionError::External(Box::new(e)))?;
        let path = ObjectPath::from(format!("telemetry/workload/v1/{}.json", self.session_id));
        self.db
            .backend()
            .store
            .put(&path, bytes.into())
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Some(path.to_string()))
    }
}

/// Expand `rolling_avg(value, order_by, rows)` (and sum/min/max variants)
/// into a standard SQL window frame. This keeps the convenience spelling in
/// the h5i layer while leaving execution and optimization to DataFusion.
fn rewrite_rolling_sugar(query: &str) -> DfResult<String> {
    const FUNCTIONS: [(&str, &str); 4] = [
        ("rolling_avg", "AVG"),
        ("rolling_sum", "SUM"),
        ("rolling_min", "MIN"),
        ("rolling_max", "MAX"),
    ];
    let mut rewritten = query.to_string();
    loop {
        let found = FUNCTIONS
            .iter()
            .filter_map(|(name, aggregate)| {
                find_function_call(&rewritten, name).map(|start| (start, *name, *aggregate))
            })
            .min_by_key(|(start, _, _)| *start);
        let Some((start, name, aggregate)) = found else {
            return Ok(rewritten);
        };
        let open = start + name.len();
        let close = matching_paren(&rewritten, open)
            .ok_or_else(|| DataFusionError::Plan(format!("{name}: missing closing parenthesis")))?;
        let args = split_function_args(&rewritten[open + 1..close]);
        if args.len() != 3 {
            return Err(DataFusionError::Plan(format!(
                "{name}(value, order_by, rows) takes exactly 3 arguments"
            )));
        }
        let rows = args[2].trim().parse::<u64>().map_err(|_| {
            DataFusionError::Plan(format!("{name}: rows must be a positive integer literal"))
        })?;
        if rows == 0 || rows > 1_000_000 {
            return Err(DataFusionError::Plan(format!(
                "{name}: rows must be between 1 and 1000000"
            )));
        }
        let replacement = format!(
            "{aggregate}({}) OVER (ORDER BY {} ROWS BETWEEN {} PRECEDING AND CURRENT ROW)",
            args[0].trim(),
            args[1].trim(),
            rows - 1
        );
        rewritten.replace_range(start..=close, &replacement);
    }
}

fn find_function_call(haystack: &str, name: &str) -> Option<usize> {
    let bytes = haystack.as_bytes();
    let mut quote = None;
    let mut i = 0;
    while i + name.len() < bytes.len() {
        match bytes[i] {
            b'\'' | b'"' => {
                let current = bytes[i];
                if quote == Some(current) {
                    if i + 1 < bytes.len() && bytes[i + 1] == current {
                        i += 2;
                        continue;
                    }
                    quote = None;
                } else if quote.is_none() {
                    quote = Some(current);
                }
            }
            _ if quote.is_none()
                && bytes[i..i + name.len()].eq_ignore_ascii_case(name.as_bytes())
                && bytes.get(i + name.len()) == Some(&b'(')
                && (i == 0 || !is_identifier_byte(bytes[i - 1])) =>
            {
                return Some(i);
            }
            _ => {}
        }
        i += 1;
    }
    None
}

fn is_identifier_byte(value: u8) -> bool {
    value.is_ascii_alphanumeric() || value == b'_'
}

fn matching_paren(value: &str, open: usize) -> Option<usize> {
    let bytes = value.as_bytes();
    let mut depth = 0_u32;
    let mut quote = None;
    let mut i = open;
    while i < bytes.len() {
        let byte = bytes[i];
        if let Some(active) = quote {
            if byte == active {
                if bytes.get(i + 1) == Some(&active) {
                    i += 2;
                    continue;
                }
                quote = None;
            }
        } else if byte == b'\'' || byte == b'"' {
            quote = Some(byte);
        } else if byte == b'(' {
            depth += 1;
        } else if byte == b')' {
            depth = depth.checked_sub(1)?;
            if depth == 0 {
                return Some(i);
            }
        }
        i += 1;
    }
    None
}

fn split_function_args(value: &str) -> Vec<&str> {
    let bytes = value.as_bytes();
    let mut result = Vec::new();
    let mut start = 0;
    let mut depth = 0_u32;
    let mut quote = None;
    let mut i = 0;
    while i < bytes.len() {
        let byte = bytes[i];
        if let Some(active) = quote {
            if byte == active {
                if bytes.get(i + 1) == Some(&active) {
                    i += 2;
                    continue;
                }
                quote = None;
            }
        } else if byte == b'\'' || byte == b'"' {
            quote = Some(byte);
        } else if byte == b'(' {
            depth += 1;
        } else if byte == b')' {
            depth = depth.saturating_sub(1);
        } else if byte == b',' && depth == 0 {
            result.push(&value[start..i]);
            start = i + 1;
        }
        i += 1;
    }
    result.push(&value[start..]);
    result
}

/// Translate sqlparser's ASOF keyword form into the native `asof_join` table
/// provider. DataFusion 54 parses the syntax but does not plan it itself.
///
/// Supported form (additional WHERE/GROUP/ORDER/LIMIT clauses are preserved):
/// `FROM l ASOF JOIN r MATCH_CONDITION (l.ts >= r.ts) ON l.key = r.key`.
fn rewrite_asof_join(query: &str) -> DfResult<String> {
    let upper = query.to_ascii_uppercase();
    let Some(asof_at) = upper.find(" ASOF JOIN ") else {
        return Ok(query.to_string());
    };
    let from_at = upper[..asof_at].rfind(" FROM ").ok_or_else(|| {
        datafusion::error::DataFusionError::Plan("ASOF JOIN requires FROM".into())
    })?;
    let match_marker = " MATCH_CONDITION (";
    let match_at = upper[asof_at..]
        .find(match_marker)
        .map(|i| asof_at + i)
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Plan(
                "ASOF JOIN requires MATCH_CONDITION (left.time >= right.time)".into(),
            )
        })?;
    let condition_start = match_at + match_marker.len();
    let condition_end = query[condition_start..]
        .find(')')
        .map(|i| condition_start + i)
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Plan("unterminated ASOF MATCH_CONDITION".into())
        })?;
    let after_match = condition_end + 1;
    let on_rel = upper[after_match..].find(" ON ").ok_or_else(|| {
        datafusion::error::DataFusionError::Plan("ASOF JOIN requires an ON constraint".into())
    })?;
    let on_start = after_match + on_rel + 4;
    let clause_markers = [" WHERE ", " GROUP BY ", " HAVING ", " ORDER BY ", " LIMIT "];
    let on_end = clause_markers
        .iter()
        .filter_map(|m| upper[on_start..].find(m).map(|i| on_start + i))
        .min()
        .unwrap_or(query.len());

    let left = query[from_at + 6..asof_at].trim();
    let right_start = asof_at + " ASOF JOIN ".len();
    let right = query[right_start..match_at].trim();
    let valid_name = |s: &str| {
        !s.is_empty()
            && s.chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
    };
    if !valid_name(left) || !valid_name(right) {
        return Err(datafusion::error::DataFusionError::Plan(
            "ASOF JOIN currently requires bare table names (no aliases)".into(),
        ));
    }

    fn qualified(expr: &str) -> Option<(&str, &str)> {
        let (table, col) = expr.trim().split_once('.')?;
        Some((table.trim(), col.trim()))
    }
    let condition = query[condition_start..condition_end].trim();
    let parts: Vec<&str> = condition.split_whitespace().collect();
    if parts.len() != 3 {
        return Err(datafusion::error::DataFusionError::Plan(
            "ASOF MATCH_CONDITION must be `left.time >= right.time` (or <= for forward)".into(),
        ));
    }
    let (lq, lc) = qualified(parts[0]).ok_or_else(|| {
        datafusion::error::DataFusionError::Plan("ASOF left time must be qualified".into())
    })?;
    let (rq, rc) = qualified(parts[2]).ok_or_else(|| {
        datafusion::error::DataFusionError::Plan("ASOF right time must be qualified".into())
    })?;
    let direction = if lq == left && rq == right {
        match parts[1] {
            ">" | ">=" => "backward",
            "<" | "<=" => "forward",
            _ => {
                return Err(datafusion::error::DataFusionError::Plan(
                    "ASOF MATCH_CONDITION must use >=, >, <=, or <".into(),
                ));
            }
        }
    } else {
        return Err(datafusion::error::DataFusionError::Plan(
            "ASOF MATCH_CONDITION must compare the joined left and right tables".into(),
        ));
    };

    let mut by = Vec::new();
    for equality in query[on_start..on_end].split(" AND ") {
        let Some((a, b)) = equality.split_once('=') else {
            return Err(datafusion::error::DataFusionError::Plan(
                "ASOF ON currently supports equality keys joined by AND".into(),
            ));
        };
        let (aq, ac) = qualified(a).ok_or_else(|| {
            datafusion::error::DataFusionError::Plan("ASOF ON keys must be qualified".into())
        })?;
        let (bq, bc) = qualified(b).ok_or_else(|| {
            datafusion::error::DataFusionError::Plan("ASOF ON keys must be qualified".into())
        })?;
        if aq == left && bq == right {
            by.push(if ac == bc {
                ac.to_string()
            } else {
                format!("{ac}={bc}")
            });
        } else if aq == right && bq == left {
            by.push(if ac == bc {
                ac.to_string()
            } else {
                format!("{bc}={ac}")
            });
        } else {
            return Err(datafusion::error::DataFusionError::Plan(
                "ASOF ON must compare left and right table keys".into(),
            ));
        }
    }
    let escape = |s: &str| s.replace('\'', "''");
    let relation = format!(
        "asof_join('{}', '{}', '{}', '{}', '{}', '{}')",
        escape(left),
        escape(right),
        escape(lc),
        escape(rc),
        escape(&by.join(",")),
        direction
    );
    Ok(format!(
        "{} FROM {}{}",
        query[..from_at].trim_end(),
        relation,
        &query[on_end..]
    ))
}

/// Both axes of a research-mode pin (ROADMAP Part VI).
///
/// The arrival axis says which *commits* are visible; the event-time axis says
/// which *rows* are, regardless of when they were committed. Only the second
/// one works on a database whose whole history arrived in a single bulk
/// ingest, which is why the flagship surface needs both.
#[derive(Debug, Clone)]
pub struct ResearchPin {
    /// Arrival axis: the read point every table is pinned at.
    pub at: ReadAt,
    /// Event-time axis: rows whose time column exceeds this instant
    /// (nanoseconds since the epoch) are unreadable in this session.
    pub event_time_cutoff_ns: Option<i64>,
}

impl ResearchPin {
    /// Arrival axis only — the historical [`H5iSession::new_at`] behaviour.
    pub fn at(at: ReadAt) -> Self {
        Self {
            at,
            event_time_cutoff_ns: None,
        }
    }

    /// Add an event-time cutoff to an existing pin.
    pub fn with_event_time_cutoff_ns(mut self, cutoff_ns: i64) -> Self {
        self.event_time_cutoff_ns = Some(cutoff_ns);
        self
    }

    /// True when neither axis constrains anything, so the caller can take the
    /// ordinary unpinned path instead of paying for a pinned session.
    pub fn is_unpinned(&self) -> bool {
        matches!(self.at, ReadAt::Latest) && self.event_time_cutoff_ns.is_none()
    }
}

impl Default for ResearchPin {
    fn default() -> Self {
        Self::at(ReadAt::Latest)
    }
}

/// Express `cutoff_ns` in the time column's own type.
///
/// Fails closed rather than guessing. An integer time column carries no unit,
/// so a nanosecond instant cannot be converted into it, and quietly comparing
/// against the wrong scale would produce a jail with a hole in it — which is
/// worse than no jail, because it looks like one.
fn cutoff_scalar(
    table: &str,
    column: &str,
    dt: &arrow::datatypes::DataType,
    cutoff_ns: i64,
) -> DfResult<datafusion::scalar::ScalarValue> {
    use arrow::datatypes::{DataType, TimeUnit};
    use datafusion::scalar::ScalarValue;
    Ok(match dt {
        DataType::Timestamp(TimeUnit::Second, tz) => {
            ScalarValue::TimestampSecond(Some(cutoff_ns.div_euclid(1_000_000_000)), tz.clone())
        }
        DataType::Timestamp(TimeUnit::Millisecond, tz) => {
            ScalarValue::TimestampMillisecond(Some(cutoff_ns.div_euclid(1_000_000)), tz.clone())
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            ScalarValue::TimestampMicrosecond(Some(cutoff_ns.div_euclid(1_000)), tz.clone())
        }
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
            ScalarValue::TimestampNanosecond(Some(cutoff_ns), tz.clone())
        }
        other => {
            return Err(DataFusionError::External(Box::new(
                h5i_db_core::Error::invalid(format!(
                    "table {table:?} has time column {column:?} of type {other}, which carries no \
                     time unit, so an event-time cutoff cannot be expressed against it; use a \
                     timestamp-typed time column, or drop --embargo and pin the arrival axis only"
                )),
            )));
        }
    })
}

/// Wrap `provider` in a view that applies the event-time cutoff.
fn embargoed_view(
    name: &str,
    provider: Arc<H5iTableProvider>,
    schema: &arrow::datatypes::SchemaRef,
    time_column: Option<&str>,
    cutoff_ns: i64,
) -> DfResult<Arc<dyn datafusion::datasource::TableProvider>> {
    use datafusion::datasource::provider_as_source;
    use datafusion::logical_expr::{LogicalPlanBuilder, col, lit};

    // A table with no time column cannot be embargoed, and letting it through
    // unfiltered would be a hole in the jail — so refuse the session instead.
    let Some(time_column) = time_column else {
        return Err(DataFusionError::External(Box::new(
            h5i_db_core::Error::invalid(format!(
                "table {name:?} has no time column, so an event-time cutoff cannot be enforced on \
                 it; give it a time column, or drop --embargo and pin the arrival axis only"
            )),
        )));
    };
    let field = schema
        .field_with_name(time_column)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
    let bound = cutoff_scalar(name, time_column, field.data_type(), cutoff_ns)?;

    let plan = LogicalPlanBuilder::scan(name, provider_as_source(provider), None)?
        .filter(col(time_column).lt_eq(lit(bound)))?
        .build()?;
    Ok(Arc::new(datafusion::datasource::ViewTable::new(plan, None)))
}

/// Resolve every catalog table at read point `at`, concurrently.
async fn resolve_all_at(db: &Arc<Database>, at: ReadAt) -> DfResult<Vec<ResolvedTable>> {
    let tables = db
        .list_tables()
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    futures::future::try_join_all(
        tables
            .iter()
            .map(|entry| db.resolve(&entry.name, at.clone())),
    )
    .await
    .map_err(|e| DataFusionError::External(Box::new(e)))
}
