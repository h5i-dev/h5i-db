//! `H5iSession`: a preconfigured DataFusion `SessionContext` over an h5i-db
//! database — tables registered snapshot-bound at session creation, resource
//! limits wired to the runtime, time-travel UDTF and time-series UDFs
//! installed, and scan metrics collected per query.

use std::path::PathBuf;
use std::sync::Arc;

use datafusion::dataframe::DataFrame;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::disk_manager::DiskManagerBuilder;
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use h5i_db_core::{Database, ReadAt};

use crate::asof::{AsOfJoinFunc, AsOfQueryPlanner};
use crate::finance::{ewma_udwf, vwap_udaf, wavg_udaf};
use crate::functions::time_bucket_udf;
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

        // Snapshot-bound registration of every table at its current head.
        let tables = db
            .list_tables()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        for entry in tables {
            let resolved = db
                .resolve(&entry.name, ReadAt::Latest)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            ctx.register_table(
                &entry.name,
                Arc::new(H5iTableProvider::new(
                    resolved,
                    url.clone(),
                    metrics.clone(),
                )),
            )?;
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
        ctx.register_udf(time_bucket_udf());
        ctx.register_udaf(vwap_udaf());
        ctx.register_udaf(wavg_udaf());
        ctx.register_udwf(ewma_udwf());

        Ok(Self {
            ctx,
            db,
            url,
            metrics,
        })
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
        self.ctx.sql(query).await
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
