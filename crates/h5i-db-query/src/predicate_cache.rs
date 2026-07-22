//! Disposable predicate sidecars keyed by immutable segment content.

use std::collections::BTreeSet;

use arrow::array::BooleanArray;
use datafusion::catalog::Session;
use datafusion::common::{Column, DFSchema};
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use datafusion_datasource_parquet::{ParquetAccessPlan, RowGroupAccess};
use futures::TryStreamExt;
use h5i_db_core::{Backend, SegmentMeta};
use object_store::path::Path as ObjectPath;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use serde::{Deserialize, Serialize};

const FORMAT: u32 = 1;
const SEMANTICS_VERSION: u32 = 1;
const PREFIX: &str = "cache/predicates/v1";
const MAX_CACHE_BYTES: u64 = 256 * 1024 * 1024;

/// Sidecar policy. Building is explicit because it writes disposable objects
/// even when the table itself was opened read-only.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum PredicateCacheMode {
    #[default]
    Disabled,
    ReadOnly,
    ReadWrite,
}

#[derive(Debug, Clone)]
pub(crate) struct EligiblePredicate {
    pub hash: String,
    pub columns: Vec<String>,
    pub expression: Expr,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct PredicateCacheStats {
    pub lookups: usize,
    pub hits: usize,
    pub misses: usize,
    pub builds: usize,
    pub rejected: usize,
    pub row_groups_reused: usize,
    pub evictions: usize,
}

pub(crate) struct CacheApplication {
    pub access_plan: Option<ParquetAccessPlan>,
    pub stats: PredicateCacheStats,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PredicateCacheEntry {
    format: u32,
    segment_checksum: String,
    schema_revision: u32,
    predicate_hash: String,
    expression_semantics_version: u32,
    row_group_count: usize,
    selected_row_groups: Vec<usize>,
    source_row_count: u64,
    qualified_row_count: u64,
    checksum: String,
}

impl PredicateCacheEntry {
    fn seal(&mut self) -> DfResult<()> {
        self.checksum.clear();
        self.checksum =
            h5i_db_core::util::checksum_hex(&serde_json::to_vec(self).map_err(external)?);
        Ok(())
    }

    fn verify(&self, segment: &SegmentMeta, predicate_hash: &str) -> DfResult<()> {
        if self.format != FORMAT
            || self.segment_checksum != segment.checksum
            || self.schema_revision != segment.schema_revision
            || self.predicate_hash != predicate_hash
            || self.expression_semantics_version != SEMANTICS_VERSION
            || self.source_row_count != segment.rows
            || self
                .selected_row_groups
                .iter()
                .any(|index| *index >= self.row_group_count)
        {
            return Err(DataFusionError::Execution(
                "predicate cache key or shape mismatch".into(),
            ));
        }
        let mut unsigned = self.clone();
        let stored = std::mem::take(&mut unsigned.checksum);
        let actual =
            h5i_db_core::util::checksum_hex(&serde_json::to_vec(&unsigned).map_err(external)?);
        if stored != actual {
            return Err(DataFusionError::Execution(
                "predicate cache checksum mismatch".into(),
            ));
        }
        Ok(())
    }

    fn access_plan(&self) -> ParquetAccessPlan {
        let mut plan = ParquetAccessPlan::new_none(self.row_group_count);
        for index in &self.selected_row_groups {
            plan.set(*index, RowGroupAccess::Scan);
        }
        plan
    }
}

#[derive(Clone)]
pub(crate) struct PredicateCache {
    backend: Backend,
    mode: PredicateCacheMode,
}

impl PredicateCache {
    pub fn new(backend: Backend, mode: PredicateCacheMode) -> Self {
        Self { backend, mode }
    }

    pub async fn apply(
        &self,
        state: &dyn Session,
        segment: &SegmentMeta,
        predicate: &EligiblePredicate,
    ) -> CacheApplication {
        let mut stats = PredicateCacheStats {
            lookups: 1,
            ..Default::default()
        };
        let path = cache_path(segment, &predicate.hash);
        match self.load(&path, segment, &predicate.hash).await {
            Ok(Some(entry)) => {
                stats.hits = 1;
                stats.row_groups_reused = entry.row_group_count;
                return CacheApplication {
                    access_plan: Some(entry.access_plan()),
                    stats,
                };
            }
            Ok(None) => stats.misses = 1,
            Err(error) => {
                tracing::debug!(%error, path = %path, "ignoring corrupt predicate cache entry");
                stats.misses = 1;
                if self.mode == PredicateCacheMode::ReadWrite {
                    let _ = self.backend.delete(&path).await;
                }
            }
        }

        if self.mode != PredicateCacheMode::ReadWrite {
            return CacheApplication {
                access_plan: None,
                stats,
            };
        }
        match self.build(state, segment, predicate).await {
            Ok(mut entry) => {
                stats.builds = 1;
                if let Err(error) = entry.seal() {
                    tracing::debug!(%error, "could not seal predicate cache entry");
                } else {
                    match serde_json::to_vec(&entry) {
                        Ok(bytes) => {
                            match self
                                .backend
                                .put_if_absent(&path, bytes::Bytes::from(bytes))
                                .await
                            {
                                Ok(true) => match self.enforce_budget().await {
                                    Ok(evictions) => stats.evictions = evictions,
                                    Err(error) => {
                                        tracing::debug!(%error, "predicate cache eviction failed")
                                    }
                                },
                                Ok(false) => {}
                                Err(error) => {
                                    tracing::debug!(%error, path = %path, "could not publish predicate cache entry");
                                }
                            }
                        }
                        Err(error) => {
                            tracing::debug!(%error, "could not encode predicate cache entry")
                        }
                    }
                }
                CacheApplication {
                    // The miss remains an ordinary scan. Only a subsequent
                    // query trusts the checksum-verified published sidecar.
                    access_plan: None,
                    stats,
                }
            }
            Err(error) => {
                tracing::debug!(%error, segment = %segment.path, "predicate cache build rejected");
                stats.rejected = 1;
                CacheApplication {
                    access_plan: None,
                    stats,
                }
            }
        }
    }

    async fn load(
        &self,
        path: &ObjectPath,
        segment: &SegmentMeta,
        predicate_hash: &str,
    ) -> DfResult<Option<PredicateCacheEntry>> {
        let Some(bytes) = self.backend.get_opt(path).await.map_err(external)? else {
            return Ok(None);
        };
        let entry: PredicateCacheEntry = serde_json::from_slice(&bytes).map_err(external)?;
        entry.verify(segment, predicate_hash)?;
        Ok(Some(entry))
    }

    async fn build(
        &self,
        state: &dyn Session,
        segment: &SegmentMeta,
        predicate: &EligiblePredicate,
    ) -> DfResult<PredicateCacheEntry> {
        let path = ObjectPath::from(segment.path.as_str());
        let reader = ParquetObjectReader::new(self.backend.store.clone(), path);
        let metadata = ArrowReaderMetadata::load_async(
            &mut reader.clone(),
            ArrowReaderOptions::new()
                .with_page_index_policy(parquet::file::metadata::PageIndexPolicy::Optional),
        )
        .await
        .map_err(external)?;
        let indices = predicate
            .columns
            .iter()
            .map(|column| metadata.schema().index_of(column).map_err(external))
            .collect::<DfResult<Vec<_>>>()?;
        let projected_schema =
            std::sync::Arc::new(metadata.schema().project(&indices).map_err(external)?);
        let physical = state.create_physical_expr(
            predicate.expression.clone(),
            &DFSchema::try_from(projected_schema.clone())?,
        )?;
        let parquet_schema = metadata.metadata().file_metadata().schema_descr();
        let projection = ProjectionMask::roots(parquet_schema, indices);
        let row_group_count = metadata.metadata().num_row_groups();
        let mut selected_row_groups = Vec::new();
        let mut qualified_row_count = 0u64;
        let mut source_row_count = 0u64;

        for row_group in 0..row_group_count {
            let builder = ParquetRecordBatchStreamBuilder::new_with_metadata(
                reader.clone(),
                metadata.clone(),
            )
            .with_projection(projection.clone())
            .with_row_groups(vec![row_group]);
            let mut stream = builder.build().map_err(external)?;
            let mut matches = 0u64;
            while let Some(batch) = stream.try_next().await.map_err(external)? {
                source_row_count += batch.num_rows() as u64;
                let value = physical.evaluate(&batch)?;
                let array = value.into_array(batch.num_rows())?;
                let booleans = array
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| {
                        DataFusionError::Execution(
                            "eligible predicate did not evaluate to boolean".into(),
                        )
                    })?;
                matches += booleans.true_count() as u64;
            }
            if matches > 0 {
                selected_row_groups.push(row_group);
                qualified_row_count += matches;
            }
        }
        if source_row_count != segment.rows {
            return Err(DataFusionError::Execution(format!(
                "predicate cache source row mismatch: expected {}, read {source_row_count}",
                segment.rows
            )));
        }
        Ok(PredicateCacheEntry {
            format: FORMAT,
            segment_checksum: segment.checksum.clone(),
            schema_revision: segment.schema_revision,
            predicate_hash: predicate.hash.clone(),
            expression_semantics_version: SEMANTICS_VERSION,
            row_group_count,
            selected_row_groups,
            source_row_count,
            qualified_row_count,
            checksum: String::new(),
        })
    }

    async fn enforce_budget(&self) -> h5i_db_core::Result<usize> {
        let prefix = ObjectPath::from(PREFIX);
        let mut objects = self.backend.list(&prefix).await?;
        let mut total = objects.iter().map(|object| object.size).sum::<u64>();
        if total <= MAX_CACHE_BYTES {
            return Ok(0);
        }
        objects.sort_by_key(|object| object.last_modified);
        let mut evictions = 0;
        for object in objects {
            if total <= MAX_CACHE_BYTES {
                break;
            }
            self.backend.delete(&object.location).await?;
            total = total.saturating_sub(object.size);
            evictions += 1;
        }
        Ok(evictions)
    }
}

pub(crate) fn eligible_predicate(
    schema: &arrow::datatypes::SchemaRef,
    filters: &[Expr],
) -> Option<EligiblePredicate> {
    let expression = datafusion::logical_expr::utils::conjunction(filters.to_vec())?;
    let mut columns = BTreeSet::new();
    let mut terms = Vec::new();
    let mut has_equality = false;
    collect_terms(
        schema,
        &expression,
        &mut columns,
        &mut terms,
        &mut has_equality,
    )?;
    if !has_equality || terms.is_empty() || terms.len() > 8 {
        return None;
    }
    terms.sort();
    // ProjectionMask emits columns in file-schema order, regardless of the
    // order requested. Keep the physical expression schema in that same
    // order so column indices and Arrow types remain aligned.
    let columns = schema
        .fields()
        .iter()
        .filter(|field| columns.contains(field.name()))
        .map(|field| field.name().clone())
        .collect();
    Some(EligiblePredicate {
        hash: blake3::hash(terms.join("&").as_bytes())
            .to_hex()
            .to_string(),
        columns,
        // Provider filters can retain a table qualifier. The projected
        // predicate-only batch is intentionally unqualified.
        expression: unqualify(&expression)?,
    })
}

fn unqualify(expression: &Expr) -> Option<Expr> {
    Some(match expression {
        Expr::BinaryExpr(binary) => Expr::BinaryExpr(BinaryExpr::new(
            Box::new(unqualify(&binary.left)?),
            binary.op,
            Box::new(unqualify(&binary.right)?),
        )),
        Expr::Column(column) => Expr::Column(Column::new_unqualified(column.name.clone())),
        Expr::Literal(value, metadata) => Expr::Literal(value.clone(), metadata.clone()),
        _ => return None,
    })
}

fn collect_terms(
    schema: &arrow::datatypes::SchemaRef,
    expression: &Expr,
    columns: &mut BTreeSet<String>,
    terms: &mut Vec<String>,
    has_equality: &mut bool,
) -> Option<()> {
    let Expr::BinaryExpr(binary) = expression else {
        return None;
    };
    if binary.op == Operator::And {
        collect_terms(schema, &binary.left, columns, terms, has_equality)?;
        collect_terms(schema, &binary.right, columns, terms, has_equality)?;
        return Some(());
    }
    if !matches!(
        binary.op,
        Operator::Eq | Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq
    ) {
        return None;
    }
    let (column, scalar, operator) = match (&*binary.left, &*binary.right) {
        (Expr::Column(column), Expr::Literal(value, _)) => (column.name.as_str(), value, binary.op),
        (Expr::Literal(value, _), Expr::Column(column)) => {
            (column.name.as_str(), value, reverse(binary.op)?)
        }
        _ => return None,
    };
    if scalar.is_null() {
        return None;
    }
    let field = schema.field_with_name(column).ok()?;
    if !matches!(
        field.data_type(),
        arrow::datatypes::DataType::Utf8
            | arrow::datatypes::DataType::LargeUtf8
            | arrow::datatypes::DataType::Int8
            | arrow::datatypes::DataType::Int16
            | arrow::datatypes::DataType::Int32
            | arrow::datatypes::DataType::Int64
            | arrow::datatypes::DataType::UInt8
            | arrow::datatypes::DataType::UInt16
            | arrow::datatypes::DataType::UInt32
            | arrow::datatypes::DataType::UInt64
            | arrow::datatypes::DataType::Date32
            | arrow::datatypes::DataType::Date64
            | arrow::datatypes::DataType::Timestamp(_, _)
    ) {
        return None;
    }
    *has_equality |= operator == Operator::Eq;
    columns.insert(column.to_string());
    terms.push(format!(
        "{column}:{:?}:{operator:?}:{scalar:?}",
        field.data_type()
    ));
    Some(())
}

fn reverse(operator: Operator) -> Option<Operator> {
    Some(match operator {
        Operator::Eq => Operator::Eq,
        Operator::Lt => Operator::Gt,
        Operator::LtEq => Operator::GtEq,
        Operator::Gt => Operator::Lt,
        Operator::GtEq => Operator::LtEq,
        _ => return None,
    })
}

fn cache_path(segment: &SegmentMeta, predicate_hash: &str) -> ObjectPath {
    let key = format!(
        "{}:{}:{}:{}",
        segment.checksum, segment.schema_revision, predicate_hash, SEMANTICS_VERSION
    );
    let digest = blake3::hash(key.as_bytes()).to_hex().to_string();
    ObjectPath::from(format!("{PREFIX}/{}/{digest}.json", &digest[..2]))
}

fn external(error: impl std::error::Error + Send + Sync + 'static) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}
