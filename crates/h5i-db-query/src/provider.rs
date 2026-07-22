//! Snapshot-bound `TableProvider`: one instance per resolved table version.
//!
//! Scan pipeline: pushed-down filters → `PruningPredicate` over manifest
//! statistics (segment pruning, zero I/O) → `ParquetSource` over the
//! surviving segment objects (row-group/page pruning + row-level predicate
//! pushdown handled by DataFusion's Parquet machinery).

use std::sync::Arc;

pub use crate::metrics::{ScanMetrics, ScanMetricsCollector};
use crate::predicate_cache::{
    eligible_predicate, PredicateCache, PredicateCacheMode, PredicateCacheStats,
};
use crate::pruning::ManifestPruningStats;
use arrow::datatypes::{DataType, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::stats::Precision;
use datafusion::common::{ColumnStatistics, DFSchema, ScalarValue, Statistics};
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

/// Typed Arrow scalar from a manifest JSON stat (mirrors the conversion in
/// `pruning.rs`, restricted to the types manifest stats are recorded for).
fn json_stat_to_scalar(v: &serde_json::Value, data_type: &DataType) -> Option<ScalarValue> {
    use arrow::datatypes::TimeUnit;
    match data_type {
        DataType::Int8 => v.as_i64().map(|x| ScalarValue::Int8(Some(x as i8))),
        DataType::Int16 => v.as_i64().map(|x| ScalarValue::Int16(Some(x as i16))),
        DataType::Int32 => v.as_i64().map(|x| ScalarValue::Int32(Some(x as i32))),
        DataType::Int64 => v.as_i64().map(|x| ScalarValue::Int64(Some(x))),
        DataType::UInt8 => v.as_u64().map(|x| ScalarValue::UInt8(Some(x as u8))),
        DataType::UInt16 => v.as_u64().map(|x| ScalarValue::UInt16(Some(x as u16))),
        DataType::UInt32 => v.as_u64().map(|x| ScalarValue::UInt32(Some(x as u32))),
        DataType::UInt64 => v.as_u64().map(|x| ScalarValue::UInt64(Some(x))),
        DataType::Float32 => v.as_f64().map(|x| ScalarValue::Float32(Some(x as f32))),
        DataType::Float64 => v.as_f64().map(|x| ScalarValue::Float64(Some(x))),
        DataType::Boolean => v.as_bool().map(|x| ScalarValue::Boolean(Some(x))),
        DataType::Utf8 => v.as_str().map(|s| ScalarValue::Utf8(Some(s.to_string()))),
        DataType::LargeUtf8 => v
            .as_str()
            .map(|s| ScalarValue::LargeUtf8(Some(s.to_string()))),
        DataType::Date32 => v.as_i64().map(|x| ScalarValue::Date32(Some(x as i32))),
        DataType::Date64 => v.as_i64().map(|x| ScalarValue::Date64(Some(x))),
        DataType::Timestamp(unit, tz) => {
            let x = v.as_i64()?;
            Some(match unit {
                TimeUnit::Second => ScalarValue::TimestampSecond(Some(x), tz.clone()),
                TimeUnit::Millisecond => ScalarValue::TimestampMillisecond(Some(x), tz.clone()),
                TimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(Some(x), tz.clone()),
                TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(Some(x), tz.clone()),
            })
        }
        // Dictionary-encoded strings: stats were computed over values.
        DataType::Dictionary(_, value) if **value == DataType::Utf8 => {
            v.as_str().map(|s| ScalarValue::Utf8(Some(s.to_string())))
        }
        _ => None,
    }
}

/// Fold manifest segment stats into planner `Statistics` (DESIGN §7 Tier 1):
/// exact row counts always; per-column min/max/null-count exact when every
/// segment recorded the stat, absent otherwise (never guessed).
fn manifest_statistics(schema: &SchemaRef, segments: &[&SegmentMeta]) -> Statistics {
    let num_rows = segments.iter().map(|s| s.rows as usize).sum::<usize>();
    // Encoded Parquet bytes understate in-memory Arrow size — keep Inexact.
    let total_bytes = segments.iter().map(|s| s.bytes as usize).sum::<usize>();

    let column_statistics = schema
        .fields()
        .iter()
        .map(|field| {
            let stat_type = match field.data_type() {
                DataType::Dictionary(_, value) if **value == DataType::Utf8 => DataType::Utf8,
                other => other.clone(),
            };
            let per_seg: Vec<Option<&h5i_db_core::ColumnStats>> = segments
                .iter()
                .map(|seg| seg.columns.get(field.name()))
                .collect();
            let mut stats = ColumnStatistics::new_unknown();
            if per_seg.iter().all(|s| s.is_some()) && !per_seg.is_empty() {
                let cols: Vec<_> = per_seg.into_iter().flatten().collect();
                stats.null_count =
                    Precision::Exact(cols.iter().map(|c| c.null_count as usize).sum());
                let mins: Option<Vec<ScalarValue>> = cols
                    .iter()
                    .map(|c| {
                        c.min
                            .as_ref()
                            .and_then(|v| json_stat_to_scalar(v, &stat_type))
                    })
                    .collect();
                if let Some(mins) = mins {
                    if let Some(min) = mins.into_iter().reduce(|a, b| {
                        if b.partial_cmp(&a) == Some(std::cmp::Ordering::Less) {
                            b
                        } else {
                            a
                        }
                    }) {
                        stats.min_value = Precision::Exact(min);
                    }
                }
                let maxs: Option<Vec<ScalarValue>> = cols
                    .iter()
                    .map(|c| {
                        c.max
                            .as_ref()
                            .and_then(|v| json_stat_to_scalar(v, &stat_type))
                    })
                    .collect();
                if let Some(maxs) = maxs {
                    if let Some(max) = maxs.into_iter().reduce(|a, b| {
                        if b.partial_cmp(&a) == Some(std::cmp::Ordering::Greater) {
                            b
                        } else {
                            a
                        }
                    }) {
                        stats.max_value = Precision::Exact(max);
                    }
                }
            }
            stats
        })
        .collect();

    Statistics {
        num_rows: Precision::Exact(num_rows),
        total_byte_size: Precision::Inexact(total_bytes),
        column_statistics,
    }
}

/// A `TableProvider` bound to one immutable table version.
pub struct H5iTableProvider {
    resolved: ResolvedTable,
    object_store_url: ObjectStoreUrl,
    metrics: ScanMetricsCollector,
    predicate_cache: Option<PredicateCache>,
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
            predicate_cache: None,
        }
    }

    pub fn with_predicate_cache(
        mut self,
        backend: h5i_db_core::Backend,
        mode: PredicateCacheMode,
    ) -> Self {
        if mode != PredicateCacheMode::Disabled {
            self.predicate_cache = Some(PredicateCache::new(backend, mode));
        }
        self
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

    /// Exact table-level statistics straight from the manifest — lets the
    /// planner answer metadata-only aggregates and pick join sides with zero
    /// object I/O.
    fn statistics(&self) -> Option<Statistics> {
        let segments: Vec<&SegmentMeta> = self.resolved.manifest.segments.iter().collect();
        Some(manifest_statistics(&self.schema(), &segments))
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
        let predicate_cache = self
            .predicate_cache
            .as_ref()
            .and_then(|_| eligible_predicate(&self.schema(), filters));
        let mut cache_stats = PredicateCacheStats::default();
        if self.predicate_cache.is_some() && predicate_cache.is_none() && !filters.is_empty() {
            cache_stats.rejected = 1;
        }

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

        // Post-pruning statistics for the physical plan: exact over the
        // surviving segments when the scan emits them verbatim; degraded to
        // inexact when a predicate (row-group/page pruning) or limit can
        // shrink the output.
        let mut scan_stats = manifest_statistics(&self.schema(), &survivors);
        if !filters.is_empty() || limit.is_some() {
            scan_stats = scan_stats.to_inexact();
        }

        let mut builder =
            FileScanConfigBuilder::new(self.object_store_url.clone(), Arc::new(source))
                .with_projection_indices(projection.cloned())
                .map_err(|e| DataFusionError::External(Box::new(e)))?
                .with_statistics(scan_stats)
                .with_limit(limit);
        if let Some(ordering) = self.output_ordering() {
            builder = builder.with_output_ordering(vec![ordering]);
        }
        for seg in &survivors {
            let mut file = PartitionedFile::new(seg.path.clone(), seg.bytes);
            if let (Some(cache), Some(predicate)) = (&self.predicate_cache, &predicate_cache) {
                let application = cache.apply(state, seg, predicate).await;
                cache_stats.lookups += application.stats.lookups;
                cache_stats.hits += application.stats.hits;
                cache_stats.misses += application.stats.misses;
                cache_stats.builds += application.stats.builds;
                cache_stats.rejected += application.stats.rejected;
                cache_stats.row_groups_reused += application.stats.row_groups_reused;
                cache_stats.evictions += application.stats.evictions;
                if let Some(access_plan) = application.access_plan {
                    file = file.with_extension(access_plan);
                }
            }
            builder = builder.with_file(file);
        }
        self.metrics.record(ScanMetrics {
            query_id: None,
            table: self.resolved.entry.name.clone(),
            version: self.resolved.manifest.sequence,
            segments_total: self.resolved.manifest.segments.len(),
            segments_pruned: self.resolved.manifest.segments.len() - survivors.len(),
            segments_scanned: survivors.len(),
            bytes_scheduled: survivors.iter().map(|s| s.bytes).sum(),
            predicate_cache_lookups: cache_stats.lookups,
            predicate_cache_hits: cache_stats.hits,
            predicate_cache_misses: cache_stats.misses,
            predicate_cache_builds: cache_stats.builds,
            predicate_cache_rejected: cache_stats.rejected,
            predicate_cache_row_groups_reused: cache_stats.row_groups_reused,
            predicate_cache_evictions: cache_stats.evictions,
        });
        Ok(DataSourceExec::from_data_source(builder.build()))
    }
}
