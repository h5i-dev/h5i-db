//! Snapshot-bound `TableProvider`: one instance per resolved table version.
//!
//! Scan pipeline: pushed-down filters → `PruningPredicate` over manifest
//! statistics (segment pruning, zero I/O) → `ParquetSource` over the
//! surviving segment objects (row-group/page pruning + row-level predicate
//! pushdown handled by DataFusion's Parquet machinery).

use std::sync::{Arc, Mutex};

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::DFSchema;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::memory::DataSourceExec;
use datafusion::datasource::physical_plan::{FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::{expressions, LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_pruning::PruningPredicate;
use h5i_db_core::{ResolvedTable, SegmentMeta};
use serde::Serialize;

use crate::pruning::ManifestPruningStats;

/// Pruning observability for one scan (DESIGN_CLAUDE.md §6.7).
#[derive(Debug, Clone, Default, Serialize)]
pub struct ScanMetrics {
    pub table: String,
    pub version: u64,
    pub segments_total: usize,
    pub segments_pruned: usize,
    pub segments_scanned: usize,
    pub bytes_scheduled: u64,
}

/// Shared collector: sessions hand one to every provider they create and
/// read it back after query execution.
#[derive(Debug, Default, Clone)]
pub struct ScanMetricsCollector {
    inner: Arc<Mutex<Vec<ScanMetrics>>>,
}

impl ScanMetricsCollector {
    pub fn record(&self, m: ScanMetrics) {
        self.inner.lock().unwrap().push(m);
    }

    /// Drain all recorded scans (typically called once per query).
    pub fn take(&self) -> Vec<ScanMetrics> {
        std::mem::take(&mut self.inner.lock().unwrap())
    }
}

/// A `TableProvider` bound to one immutable table version.
pub struct H5iTableProvider {
    resolved: ResolvedTable,
    object_store_url: ObjectStoreUrl,
    metrics: ScanMetricsCollector,
}

impl H5iTableProvider {
    pub fn new(
        resolved: ResolvedTable,
        object_store_url: ObjectStoreUrl,
        metrics: ScanMetricsCollector,
    ) -> Self {
        Self {
            resolved,
            object_store_url,
            metrics,
        }
    }

    pub fn resolved(&self) -> &ResolvedTable {
        &self.resolved
    }

    /// Prune segments with the pushed-down predicate against manifest stats.
    fn prune_segments(&self, state: &dyn Session, filters: &[Expr]) -> DfResult<Vec<&SegmentMeta>> {
        let segments = &self.resolved.manifest.segments;
        let Some(predicate) = conjunction(filters.to_vec()) else {
            return Ok(segments.iter().collect());
        };
        let df_schema = DFSchema::try_from(self.schema())?;
        let physical = state.create_physical_expr(predicate, &df_schema)?;
        let pruning = match PruningPredicate::try_new(physical, self.schema()) {
            Ok(p) => p,
            // A predicate the pruning machinery can't analyze must never
            // drop segments — scan everything.
            Err(e) => {
                tracing::debug!("pruning predicate unavailable: {e}");
                return Ok(segments.iter().collect());
            }
        };
        let stats = ManifestPruningStats::new(segments, self.schema());
        match pruning.prune(&stats) {
            Ok(keep) => Ok(segments
                .iter()
                .zip(keep)
                .filter_map(|(s, k)| k.then_some(s))
                .collect()),
            Err(e) => {
                tracing::debug!("pruning evaluation failed, scanning all: {e}");
                Ok(segments.iter().collect())
            }
        }
    }

    /// Declared output ordering: each file is sorted by the sort key and (for
    /// append-only histories) files don't interleave, but DataFusion's
    /// per-partition ordering claim only needs within-file order, which
    /// `sorted` segments guarantee.
    fn output_ordering(&self) -> Option<LexOrdering> {
        let spec = &self.resolved.spec;
        let tc = spec.time_column.as_ref()?;
        let all_sorted = !self.resolved.manifest.segments.is_empty()
            && self.resolved.manifest.segments.iter().all(|s| s.sorted);
        if !all_sorted {
            return None;
        }
        let idx = self.schema().index_of(tc).ok()?;
        LexOrdering::new(vec![PhysicalSortExpr::new_default(Arc::new(
            expressions::Column::new(tc, idx),
        ))])
    }
}

impl std::fmt::Debug for H5iTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("H5iTableProvider")
            .field("table", &self.resolved.entry.name)
            .field("version", &self.resolved.manifest.sequence)
            .finish()
    }
}

#[async_trait]
impl TableProvider for H5iTableProvider {
    fn schema(&self) -> SchemaRef {
        self.resolved.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        // Inexact everywhere: pruning skips whole segments/row-groups and the
        // Parquet reader applies row filters, but DataFusion re-validates —
        // always correct, never trusted blindly.
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let survivors = self.prune_segments(state, filters)?;

        self.metrics.record(ScanMetrics {
            table: self.resolved.entry.name.clone(),
            version: self.resolved.manifest.sequence,
            segments_total: self.resolved.manifest.segments.len(),
            segments_pruned: self.resolved.manifest.segments.len() - survivors.len(),
            segments_scanned: survivors.len(),
            bytes_scheduled: survivors.iter().map(|s| s.bytes).sum(),
        });

        let mut source = ParquetSource::new(self.schema());
        // Footer-metadata cache: segments are immutable and content-addressed,
        // so caching parsed Parquet footers across queries is unconditionally
        // sound and saves one footer read+parse per segment per query
        // (measured: ~40% of warm full-scan latency at 50 segments).
        let metadata_cache = state.runtime_env().cache_manager.get_file_metadata_cache();
        if let Ok(store) = state.runtime_env().object_store(&self.object_store_url) {
            source = source.with_parquet_file_reader_factory(Arc::new(
                datafusion_datasource_parquet::CachedParquetFileReaderFactory::new(
                    store,
                    metadata_cache,
                ),
            ));
        }
        // The predicate enables Parquet row-group + page pruning. Row-level
        // filter pushdown (`with_pushdown_filters`) is deliberately NOT
        // enabled: measured on tick data it costs ~2x on selective time-range
        // scans versus decode-then-filter (segments are already pruned by the
        // manifest, so decoded batches are mostly relevant anyway).
        if let Some(predicate) = conjunction(filters.to_vec()) {
            let df_schema = DFSchema::try_from(self.schema())?;
            if let Ok(physical) = state.create_physical_expr(predicate, &df_schema) {
                source = source.with_predicate(physical);
            }
        }

        let mut builder =
            FileScanConfigBuilder::new(self.object_store_url.clone(), Arc::new(source))
                .with_projection_indices(projection.cloned())
                .map_err(|e| DataFusionError::External(Box::new(e)))?
                .with_limit(limit);
        if let Some(ordering) = self.output_ordering() {
            builder = builder.with_output_ordering(vec![ordering]);
        }
        for seg in survivors {
            builder = builder.with_file(PartitionedFile::new(seg.path.clone(), seg.bytes));
        }
        Ok(DataSourceExec::from_data_source(builder.build()))
    }
}
