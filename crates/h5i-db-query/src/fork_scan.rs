//! Cross-fork scan: `forks('table' [, 'fork-a,fork-b'])` (ROADMAP Part X, X-A2).
//!
//! ```sql
//! SELECT * FROM forks('trades');                        -- every fork
//! SELECT * FROM forks('trades', 'agent-01,agent-02');   -- two of them
//! SELECT __fork, avg(price) FROM forks('trades') GROUP BY __fork;
//! ```
//!
//! Comparing outcomes across speculative branches is the operation agentic
//! workloads end with, and the one no branchable database was found to support
//! (BranchBench, arXiv:2604.17180 §3.3). It is cheap here for a structural
//! reason: a fork shares its base's segments *by path*, so N forks over a
//! mostly-unmodified table are N manifests over almost exactly one set of
//! files.
//!
//! # One read per segment, not one per fork
//!
//! The naive plan — scan each fork, union the results — reads a shared segment
//! once per fork, so a thousand-branch aggregation reads the base a thousand
//! times. Instead this groups segments by *which forks reference them*:
//!
//! ```text
//!   segment s1 ── forks {a, b, c}  ─┐
//!   segment s2 ── forks {a, b, c}  ─┴─► scan once ─► cross join ('a'),('b'),('c')
//!   segment s3 ── forks {b}        ───► scan once ─► cross join ('b')
//! ```
//!
//! Each group is scanned once and cross-joined against the literal names of
//! the forks that share it, so every distinct segment is opened exactly once
//! however many forks reference it, and the row it produces is labelled once
//! per referencing fork. The branches are then unioned.
//!
//! # What it refuses
//!
//! Forks whose schemas for the table disagree are refused rather than coerced.
//! Cross-branch comparison assumes a common relation, and the workloads that
//! need it (Monte Carlo simulation, MCTS) perform no schema changes — the ones
//! that do (software engineering, data cleaning) want to see the divergence,
//! not have it silently papered over. `fork diff` is the tool for that.

use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::catalog::{TableFunctionArgs, TableFunctionImpl};
use datafusion::datasource::{TableProvider, ViewTable, provider_as_source};
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder, col, lit};
use h5i_db_core::{Database, Error, ReadAt, ResolvedTable, SegmentMeta};

use crate::pin::PinContext;
use crate::provider::{H5iTableProvider, ScanMetricsCollector};
use crate::udtf::block_on;

/// The column naming the fork each row came from.
pub const FORK_COLUMN: &str = "__fork";

#[derive(Debug)]
pub struct ForkScanFunc {
    db: Arc<Database>,
    url: ObjectStoreUrl,
    metrics: ScanMetricsCollector,
    pin: PinContext,
}

impl ForkScanFunc {
    pub fn new(db: Arc<Database>, url: ObjectStoreUrl, metrics: ScanMetricsCollector) -> Self {
        Self::pinned(db, url, metrics, PinContext::default())
    }

    pub(crate) fn pinned(
        db: Arc<Database>,
        url: ObjectStoreUrl,
        metrics: ScanMetricsCollector,
        pin: PinContext,
    ) -> Self {
        Self {
            db,
            url,
            metrics,
            pin,
        }
    }
}

fn literal_str(expr: &datafusion::logical_expr::Expr) -> Option<&str> {
    use datafusion::scalar::ScalarValue;
    match expr {
        datafusion::logical_expr::Expr::Literal(ScalarValue::Utf8(Some(s)), _) => Some(s),
        datafusion::logical_expr::Expr::Literal(ScalarValue::LargeUtf8(Some(s)), _) => Some(s),
        _ => None,
    }
}

/// Split a comma-separated fork list, rejecting empty entries rather than
/// silently ignoring them: `'a,,b'` is a typo, and treating it as `'a,b'`
/// would hide a fork name the caller meant to type.
fn parse_fork_list(raw: &str) -> DfResult<Vec<String>> {
    let mut out = Vec::new();
    for part in raw.split(',') {
        let name = part.trim();
        if name.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "forks: fork list {raw:?} has an empty entry; give a comma-separated list of \
                 fork names"
            )));
        }
        if !out.contains(&name.to_string()) {
            out.push(name.to_string());
        }
    }
    Ok(out)
}

impl TableFunctionImpl for ForkScanFunc {
    fn call_with_args(&self, args: TableFunctionArgs) -> DfResult<Arc<dyn TableProvider>> {
        let args = args.exprs();
        if args.is_empty() || args.len() > 2 {
            return Err(DataFusionError::Plan(
                "forks(table_name [, 'fork-a,fork-b']) takes 1 or 2 arguments".into(),
            ));
        }
        let table = literal_str(&args[0]).ok_or_else(|| {
            DataFusionError::Plan("forks: first argument must be a table name string".into())
        })?;
        let requested = match args.get(1) {
            None => None,
            Some(arg) => Some(parse_fork_list(literal_str(arg).ok_or_else(|| {
                DataFusionError::Plan(
                    "forks: second argument must be a comma-separated string of fork names".into(),
                )
            })?)?),
        };
        self.pin.check_usable("forks")?;
        let at = self.pin.read_at();

        let resolved = block_on(resolve_across_forks(&self.db, table, requested, at))
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let plan = build_union_plan(table, resolved, &self.url, &self.metrics)?;
        Ok(Arc::new(ViewTable::new(plan, None)))
    }
}

/// Resolve `table` inside every requested fork, skipping forks that do not
/// have it.
///
/// A fork that never touched the table resolves through its pin, so it shares
/// every segment with the base — which is exactly the case the grouping below
/// collapses to a single scan.
async fn resolve_across_forks(
    db: &Database,
    table: &str,
    requested: Option<Vec<String>>,
    at: ReadAt,
) -> Result<Vec<(String, ResolvedTable)>, Error> {
    let base = db.base();
    let names = match requested {
        Some(names) => {
            // An explicitly named fork that does not exist is an error: the
            // caller believes it is querying it, and quietly returning the
            // other forks' rows would answer a different question.
            for name in &names {
                base.fork_info(name).await?;
            }
            names
        }
        None => base.fork_names().await?,
    };
    if names.is_empty() {
        return Err(Error::invalid(
            "forks(): this database has no forks; create one with `h5i-db fork create`",
        ));
    }

    let mut out = Vec::with_capacity(names.len());
    for name in names {
        let handle = base.open_fork(&name).await?;
        match handle.resolve(table, at.clone()).await {
            Ok(resolved) => out.push((name, resolved)),
            // Not every fork need have every table: one created after the
            // fork, or created inside a sibling, is simply absent here and
            // contributes no rows.
            Err(Error::TableNotFound { .. }) => continue,
            Err(e) => return Err(e),
        }
    }
    if out.is_empty() {
        return Err(Error::table_not_found(table));
    }
    Ok(out)
}

/// Build the union-of-groups plan described in the module docs.
fn build_union_plan(
    table: &str,
    resolved: Vec<(String, ResolvedTable)>,
    url: &ObjectStoreUrl,
    metrics: &ScanMetricsCollector,
) -> DfResult<LogicalPlan> {
    let (first_name, template) = resolved
        .first()
        .ok_or_else(|| DataFusionError::Plan("forks: no fork provides this table".into()))?;
    let schema = template.schema.clone();

    if schema.field_with_name(FORK_COLUMN).is_ok() {
        return Err(DataFusionError::Plan(format!(
            "forks: table {table:?} already has a column named {FORK_COLUMN:?}, which is the \
             name this function adds; rename the column to query across forks"
        )));
    }
    for (name, r) in &resolved {
        if r.schema != schema {
            return Err(DataFusionError::Plan(format!(
                "forks: fork {name:?} and fork {first_name:?} disagree on the schema of table \
                 {table:?} (schema revision {} vs {}); a cross-fork scan needs one common \
                 relation — compare them with `h5i-db fork diff` instead",
                r.manifest.schema_revision, template.manifest.schema_revision
            )));
        }
    }

    // Which forks reference each distinct segment. Insertion order of the
    // fork list follows `resolved`, which is name-ordered, so the grouping key
    // is stable and two runs plan identically.
    let mut by_path: BTreeMap<&str, (&SegmentMeta, Vec<String>)> = BTreeMap::new();
    for (name, r) in &resolved {
        for seg in &r.manifest.segments {
            by_path
                .entry(seg.path.as_str())
                .or_insert_with(|| (seg, Vec::new()))
                .1
                .push(name.clone());
        }
    }

    let mut groups: BTreeMap<Vec<String>, Vec<SegmentMeta>> = BTreeMap::new();
    for (_, (seg, forks)) in by_path {
        groups.entry(forks).or_default().push(seg.clone());
    }
    // Every fork's copy of the table is empty. There is nothing to scan, but
    // the query still has a schema and must return zero rows rather than fail,
    // so emit one empty single-fork group each.
    if groups.is_empty() {
        for (name, _) in &resolved {
            groups.insert(vec![name.clone()], Vec::new());
        }
    }

    let mut builder: Option<LogicalPlanBuilder> = None;
    for (forks, segments) in groups {
        let mut resolved_group = template.clone();
        resolved_group.manifest.segments = segments;
        resolved_group.manifest.recompute_rollups();
        let provider = Arc::new(H5iTableProvider::new(
            resolved_group,
            url.clone(),
            metrics.clone(),
        ));
        let scan = LogicalPlanBuilder::scan(table, provider_as_source(provider), None)?;

        let branch = match forks.split_first() {
            // Segments only this fork can see: the label is a constant, so a
            // projection adds it without a join. This is also the shape that
            // makes an empty group work at all — a scan over zero files has no
            // partitioning for a join to require.
            Some((only, [])) => scan
                .project(
                    schema
                        .fields()
                        .iter()
                        .map(|f| col(f.name()))
                        .chain(std::iter::once(lit(only.as_str()).alias(FORK_COLUMN)))
                        .collect::<Vec<_>>(),
                )?
                .build()?,
            // Shared segments: scanned once, then labelled once per fork that
            // references them. The cross join is what turns "read the base a
            // thousand times" into "read it once".
            _ => {
                let labels = LogicalPlanBuilder::values(
                    forks
                        .iter()
                        .map(|f| vec![lit(f.as_str())])
                        .collect::<Vec<_>>(),
                )?
                .project(vec![col("column1").alias(FORK_COLUMN)])?
                .build()?;
                scan.cross_join(labels)?.build()?
            }
        };

        builder = Some(match builder {
            None => LogicalPlanBuilder::new(branch),
            Some(b) => b.union(branch)?,
        });
    }

    builder
        .expect("at least one group is guaranteed above")
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fork_lists_split_trim_and_dedup() {
        assert_eq!(parse_fork_list("a,b").unwrap(), vec!["a", "b"]);
        assert_eq!(parse_fork_list(" a , b ").unwrap(), vec!["a", "b"]);
        // A repeated fork must not double every row it contributes.
        assert_eq!(parse_fork_list("a,b,a").unwrap(), vec!["a", "b"]);
        assert_eq!(parse_fork_list("solo").unwrap(), vec!["solo"]);
    }

    #[test]
    fn an_empty_entry_is_a_typo_not_an_omission() {
        assert!(parse_fork_list("a,,b").is_err());
        assert!(parse_fork_list("").is_err());
        assert!(parse_fork_list("a,").is_err());
    }
}
