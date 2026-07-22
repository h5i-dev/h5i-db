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

use crate::asof::{AsOfJoinFunc, AsOfQueryPlanner};
use crate::finance::{ewma_udwf, vwap_udaf, wavg_udaf};
use crate::functions::time_bucket_udf;
use crate::gapfill::GapFillFunc;
use crate::provider::{H5iTableProvider, ScanMetrics, ScanMetricsCollector};
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
}

pub struct H5iSession {
    ctx: SessionContext,
    db: Arc<Database>,
    url: ObjectStoreUrl,
    metrics: ScanMetricsCollector,
    /// Catalog table names currently registered (kept for [`Self::refresh`]).
    registered: Mutex<HashSet<String>>,
}

impl H5iSession {
    /// Build a session over a database. Table heads are resolved once, here:
    /// every query in this session sees the same immutable versions.
    pub async fn new(db: Arc<Database>, options: SessionOptions) -> DfResult<Self> {
        let mut runtime = RuntimeEnvBuilder::new();
        if let Some(limit) = options.memory_limit {
            runtime = runtime.with_memory_pool(Arc::new(FairSpillPool::new(limit)));
            let disk = match &options.spill_dir {
                Some(dir) => DiskManagerBuilder::default().with_mode(
                    datafusion::execution::disk_manager::DiskManagerMode::Directories(vec![
                        dir.clone()
                    ]),
                ),
                None => DiskManagerBuilder::default(),
            };
            runtime = runtime.with_disk_manager_builder(disk);
        }
        let runtime = Arc::new(runtime.build()?);
        Self::new_with_runtime(db, options, runtime).await
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
        let mut config = SessionConfig::new().with_information_schema(true);
        if let Some(tp) = options.target_partitions {
            config = config.with_target_partitions(tp.max(1));
        }
        if let Some(bs) = options.batch_size {
            config = config.with_batch_size(bs.max(64));
        }

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
        for resolved in resolve_all_latest(&db).await? {
            let name = resolved.entry.name.clone();
            ctx.register_table(
                &name,
                Arc::new(H5iTableProvider::new(
                    resolved,
                    url.clone(),
                    metrics.clone(),
                )),
            )?;
            registered.insert(name);
        }

        // Time travel + time-series functions.
        ctx.register_udtf(
            "h5i",
            Arc::new(TimeTravelFunc::new(
                db.clone(),
                url.clone(),
                metrics.clone(),
            )),
        );
        ctx.register_udtf(
            "asof_join",
            Arc::new(AsOfJoinFunc::new(db.clone(), url.clone(), metrics.clone())),
        );
        ctx.register_udtf("gapfill", Arc::new(GapFillFunc::new(db.clone())));
        ctx.register_udf(time_bucket_udf());
        ctx.register_udaf(vwap_udaf());
        ctx.register_udaf(wavg_udaf());
        ctx.register_udwf(ewma_udwf());

        Ok(Self {
            ctx,
            db,
            url,
            metrics,
            registered: Mutex::new(registered),
        })
    }

    /// Re-point every catalog table at its latest version without rebuilding
    /// the session: caches, UDFs, and the runtime survive. New tables appear,
    /// dropped tables disappear. `h5i('t')` never needs this — it re-resolves
    /// at planning time — this is for the plain `SELECT … FROM t` names that
    /// are otherwise snapshot-bound to session creation.
    pub async fn refresh(&self) -> DfResult<()> {
        let resolved = resolve_all_latest(&self.db).await?;
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
                Arc::new(H5iTableProvider::new(
                    r,
                    self.url.clone(),
                    self.metrics.clone(),
                )),
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
        let rewritten = rewrite_asof_join(query)?;
        self.ctx.sql(&rewritten).await
    }

    /// DataFrame over a table at a given read point (DataFrame-API entry).
    pub async fn read_table(&self, name: &str, at: ReadAt) -> DfResult<DataFrame> {
        let resolved = self
            .db
            .resolve(name, at)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let provider = Arc::new(H5iTableProvider::new(
            resolved,
            self.url.clone(),
            self.metrics.clone(),
        ));
        self.ctx.read_table(provider)
    }

    /// Drain per-scan pruning metrics recorded since the last call.
    pub fn take_scan_metrics(&self) -> Vec<ScanMetrics> {
        self.metrics.take()
    }
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
                ))
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

/// Resolve every catalog table at its latest version, concurrently.
async fn resolve_all_latest(db: &Arc<Database>) -> DfResult<Vec<ResolvedTable>> {
    let tables = db
        .list_tables()
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    futures::future::try_join_all(
        tables
            .iter()
            .map(|entry| db.resolve(&entry.name, ReadAt::Latest)),
    )
    .await
    .map_err(|e| DataFusionError::External(Box::new(e)))
}
