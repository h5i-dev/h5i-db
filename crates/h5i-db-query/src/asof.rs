//! ASOF join: for each left row, find the most recent right row at or before
//! it (backward; or at/after for forward), matching on optional equality keys.
//!
//! Design (from the DuckDB study, DESIGN_CLAUDE.md §6.4): equality keys are
//! encoded into memcmp-able byte rows (`arrow::row`), the right side is
//! buffered into per-key time-sorted runs, and each left row probes its run
//! with binary search. The left side streams; memory is bounded by the right
//! side — the same profile as `pandas.merge_asof`, with a streaming left.
//!
//! Exposed three ways:
//! - `asof_join(&session, left_df, right_df, options)` (DataFrame API)
//! - SQL table function `asof_join('left', 'right', 'l_ts', 'r_ts', 'by,...')`
//! - The physical operator itself (`AsOfJoinExec`) for plan composition.

use std::cmp::Ordering as CmpOrdering;
use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::sync::Arc;

use arrow::array::{new_null_array, Array, ArrayRef, RecordBatch};
use arrow::buffer::ScalarBuffer;
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::row::{RowConverter, SortField};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableFunctionArgs, TableFunctionImpl, TableProvider};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Column, DFSchema, DFSchemaRef};
use datafusion::dataframe::DataFrame;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::context::{QueryPlanner, SessionState};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{
    lit, BinaryExpr, Expr, Extension, LogicalPlan, Operator, TableProviderFilterPushDown,
    TableType, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};
use datafusion::physical_expr::{
    expressions, EquivalenceProperties, LexOrdering, PhysicalSortExpr,
};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties,
};
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use datafusion::scalar::ScalarValue;
use futures::{StreamExt, TryStreamExt};

use crate::provider::{H5iTableProvider, ScanMetricsCollector};
use crate::udtf::block_on;

// ---------------------------------------------------------------------------
// options and schema
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AsOfDirection {
    /// Most recent right row with `r.time <= l.time` (default).
    Backward,
    /// Earliest right row with `r.time >= l.time`.
    Forward,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AsOfOptions {
    pub left_on: String,
    pub right_on: String,
    /// Equality key pairs (left column, right column).
    pub by: Vec<(String, String)>,
    pub direction: AsOfDirection,
    /// Max |l.time - r.time| in the time column's raw units.
    pub tolerance: Option<i64>,
    /// When false (LEFT join, default) unmatched left rows emit nulls;
    /// when true (INNER) they are dropped.
    pub inner: bool,
}

impl Default for AsOfOptions {
    fn default() -> Self {
        Self {
            left_on: "ts".into(),
            right_on: "ts".into(),
            by: vec![],
            direction: AsOfDirection::Backward,
            tolerance: None,
            inner: false,
        }
    }
}

/// Output schema: all left fields, then right fields minus the `by` columns
/// (they equal the left ones), with `_right` suffixed onto name collisions.
/// Returns the schema and the kept right column indices.
fn asof_output_schema(
    left: &Schema,
    right: &Schema,
    options: &AsOfOptions,
) -> DfResult<(SchemaRef, Vec<usize>)> {
    // Validate referenced columns exist and time columns are comparable.
    let lt = left.field_with_name(&options.left_on).map_err(|_| {
        DataFusionError::Plan(format!(
            "asof: left time column {:?} not found",
            options.left_on
        ))
    })?;
    let rt = right.field_with_name(&options.right_on).map_err(|_| {
        DataFusionError::Plan(format!(
            "asof: right time column {:?} not found",
            options.right_on
        ))
    })?;
    if lt.data_type() != rt.data_type() {
        return Err(DataFusionError::Plan(format!(
            "asof: time column types differ ({} vs {})",
            lt.data_type(),
            rt.data_type()
        )));
    }
    for (l, r) in &options.by {
        let lf = left
            .field_with_name(l)
            .map_err(|_| DataFusionError::Plan(format!("asof: left by-column {l:?} not found")))?;
        let rf = right
            .field_with_name(r)
            .map_err(|_| DataFusionError::Plan(format!("asof: right by-column {r:?} not found")))?;
        if lf.data_type() != rf.data_type() {
            return Err(DataFusionError::Plan(format!(
                "asof: by-column types differ for ({l}, {r}): {} vs {}",
                lf.data_type(),
                rf.data_type()
            )));
        }
    }

    let right_by: std::collections::HashSet<&str> =
        options.by.iter().map(|(_, r)| r.as_str()).collect();
    let left_names: std::collections::HashSet<&str> =
        left.fields().iter().map(|f| f.name().as_str()).collect();

    let mut fields: Vec<Field> = left.fields().iter().map(|f| f.as_ref().clone()).collect();
    let mut kept = Vec::new();
    for (i, f) in right.fields().iter().enumerate() {
        if right_by.contains(f.name().as_str()) {
            continue;
        }
        kept.push(i);
        let name = if left_names.contains(f.name().as_str()) {
            format!("{}_right", f.name())
        } else {
            f.name().clone()
        };
        // Right side is nullable in the output (LEFT-join semantics).
        fields.push(Field::new(name, f.data_type().clone(), true));
    }
    Ok((Arc::new(Schema::new(fields)), kept))
}

// ---------------------------------------------------------------------------
// logical node
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AsOfJoinNode {
    pub left: LogicalPlan,
    pub right: LogicalPlan,
    pub options: AsOfOptions,
    schema: DFSchemaRef,
}

// Required by UserDefinedLogicalNodeCore bounds; there is no meaningful
// ordering between join nodes.
impl PartialOrd for AsOfJoinNode {
    fn partial_cmp(&self, _other: &Self) -> Option<CmpOrdering> {
        None
    }
}

impl AsOfJoinNode {
    pub fn try_new(left: LogicalPlan, right: LogicalPlan, options: AsOfOptions) -> DfResult<Self> {
        let (schema, _) = asof_output_schema(
            left.schema().as_arrow(),
            right.schema().as_arrow(),
            &options,
        )?;
        let df_schema = DFSchema::try_from(schema)?;
        Ok(Self {
            left,
            right,
            options,
            schema: Arc::new(df_schema),
        })
    }
}

impl UserDefinedLogicalNodeCore for AsOfJoinNode {
    fn name(&self) -> &str {
        "AsOfJoin"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.left, &self.right]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "AsOfJoin: on=({} {} {}), by={:?}, tolerance={:?}, {}",
            self.options.left_on,
            match self.options.direction {
                AsOfDirection::Backward => ">=",
                AsOfDirection::Forward => "<=",
            },
            self.options.right_on,
            self.options.by,
            self.options.tolerance,
            if self.options.inner { "inner" } else { "left" },
        )
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> DfResult<Self> {
        if inputs.len() != 2 {
            return Err(DataFusionError::Internal(
                "AsOfJoin requires exactly two inputs".into(),
            ));
        }
        let right = inputs.pop().unwrap();
        let left = inputs.pop().unwrap();
        Self::try_new(left, right, self.options.clone())
    }
}

// ---------------------------------------------------------------------------
// planner
// ---------------------------------------------------------------------------

/// Session query planner = default planner + the ASOF extension planner.
#[derive(Debug, Default)]
pub struct AsOfQueryPlanner;

#[async_trait]
impl QueryPlanner for AsOfQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let planner =
            DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(AsOfExtensionPlanner)]);
        planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

struct AsOfExtensionPlanner;

#[async_trait]
impl ExtensionPlanner for AsOfExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> DfResult<Option<Arc<dyn ExecutionPlan>>> {
        let Some(asof) = node.as_any().downcast_ref::<AsOfJoinNode>() else {
            return Ok(None);
        };
        let left = physical_inputs[0].clone();
        let right = physical_inputs[1].clone();
        Ok(Some(AsOfJoinExec::try_new_with_sort(
            left,
            right,
            asof.options.clone(),
        )?))
    }
}

// ---------------------------------------------------------------------------
// physical operator
// ---------------------------------------------------------------------------

fn sort_by_time(input: Arc<dyn ExecutionPlan>, time_col: &str) -> DfResult<Arc<dyn ExecutionPlan>> {
    let idx = input.schema().index_of(time_col).map_err(|e| {
        DataFusionError::Plan(format!("asof: time column {time_col:?} missing: {e}"))
    })?;
    let ordering = LexOrdering::new(vec![PhysicalSortExpr::new(
        Arc::new(expressions::Column::new(time_col, idx)),
        SortOptions::default(),
    )])
    .expect("non-empty ordering");
    // TODO(perf): hash-repartition both sides on the `by` keys and run a
    // partitioned join instead of collapsing to a single partition.
    let single: Arc<dyn ExecutionPlan> = if input.output_partitioning().partition_count() > 1 {
        // When every partition is already time-sorted (our providers declare
        // this on sorted segments), merge instead of concatenating so the
        // sort below becomes a no-op and gets removed by EnforceSorting.
        let sorted = input
            .properties()
            .equivalence_properties()
            .ordering_satisfy(ordering.clone())
            .unwrap_or(false);
        if sorted {
            Arc::new(SortPreservingMergeExec::new(ordering.clone(), input))
        } else {
            Arc::new(CoalescePartitionsExec::new(input))
        }
    } else {
        input
    };
    // EnforceSorting removes this if the input is already sorted (our
    // providers declare time ordering on sorted segments).
    Ok(Arc::new(SortExec::new(ordering, single)))
}

#[derive(Debug)]
pub struct AsOfJoinExec {
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    options: AsOfOptions,
    schema: SchemaRef,
    right_kept: Vec<usize>,
    properties: Arc<PlanProperties>,
}

impl AsOfJoinExec {
    /// Wrap both inputs in time sorts (removed by the optimizer when already
    /// sorted) and build the exec.
    pub fn try_new_with_sort(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        options: AsOfOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let left = sort_by_time(left, &options.left_on)?;
        let right = sort_by_time(right, &options.right_on)?;
        Ok(Arc::new(Self::try_new(left, right, options)?))
    }

    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        options: AsOfOptions,
    ) -> DfResult<Self> {
        let (schema, right_kept) = asof_output_schema(&left.schema(), &right.schema(), &options)?;
        // Output preserves the left side's time order — declare it so
        // downstream sorts on the join key (e.g. ORDER BY left_on) are
        // elided. The left column keeps its name and position in the output;
        // `index_of` finds it (a colliding right column is `_right`-renamed).
        let mut eq = EquivalenceProperties::new(schema.clone());
        if let Ok(idx) = schema.index_of(&options.left_on) {
            eq.add_ordering(vec![PhysicalSortExpr::new(
                Arc::new(expressions::Column::new(&options.left_on, idx)),
                SortOptions::default(),
            )]);
        }
        let properties = Arc::new(PlanProperties::new(
            eq,
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            left,
            right,
            options,
            schema,
            right_kept,
            properties,
        })
    }
}

impl DisplayAs for AsOfJoinExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "AsOfJoinExec: on=({}, {}), by={:?}, direction={:?}, tolerance={:?}, {}",
            self.options.left_on,
            self.options.right_on,
            self.options.by,
            self.options.direction,
            self.options.tolerance,
            if self.options.inner { "inner" } else { "left" },
        )
    }
}

impl ExecutionPlan for AsOfJoinExec {
    fn name(&self) -> &str {
        "AsOfJoinExec"
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.left, &self.right]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::try_new(
            children[0].clone(),
            children[1].clone(),
            self.options.clone(),
        )?))
    }

    fn required_input_ordering(
        &self,
    ) -> Vec<Option<datafusion::physical_expr::OrderingRequirements>> {
        // Sorting is inserted explicitly in try_new_with_sort.
        vec![None, None]
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(
                "AsOfJoinExec produces a single partition".into(),
            ));
        }
        let left_stream = self.left.execute(0, context.clone())?;
        let right_stream = self.right.execute(0, context.clone())?;
        // The buffered right side is charged to the query memory pool so
        // `memory_limit` is honored: the query fails with ResourcesExhausted
        // instead of OOMing the process.
        let reservation =
            MemoryConsumer::new(format!("AsOfJoinExec[{partition}]")).register(context.memory_pool());
        let options = self.options.clone();
        let schema = self.schema.clone();
        let right_kept = self.right_kept.clone();
        let left_schema = self.left.schema();
        let right_schema = self.right.schema();

        let out = futures::stream::once(async move {
            // Phase 1: buffer the right side into per-key sorted runs,
            // growing the reservation batch by batch.
            let reservation = reservation;
            let mut right_stream = right_stream;
            let mut right_batches: Vec<RecordBatch> = Vec::new();
            while let Some(batch) = right_stream.next().await {
                let batch = batch?;
                reservation.try_grow(batch.get_array_memory_size())?;
                right_batches.push(batch);
            }
            let runs = RightRuns::build(&right_batches, &right_schema, &options)?;
            reservation.try_grow(runs.mem_bytes)?;

            // Phase 2: stream the left side, probing per row.
            let left_time_idx = left_schema.index_of(&options.left_on)?;
            let left_by_idx: Vec<usize> = options
                .by
                .iter()
                .map(|(l, _)| left_schema.index_of(l))
                .collect::<Result<_, _>>()?;
            let joiner = Joiner {
                runs,
                right_batches,
                options: options.clone(),
                schema,
                right_kept,
                right_schema,
                left_time_idx,
                left_by_idx,
                _reservation: reservation,
            };
            let joined = left_stream
                .map(move |batch| -> DfResult<RecordBatch> { joiner.join_batch(&batch?) });
            Ok::<_, DataFusionError>(joined)
        })
        .try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            out,
        )))
    }
}

/// Per-key run of right-side rows in time order.
struct Run {
    times: Vec<i64>,
    /// (batch index, row index) parallel to `times`.
    locs: Vec<(usize, usize)>,
}

/// Right-side index: one global run when there are no by-keys (no row
/// encoding at all), otherwise per-key runs keyed on the memcmp-able row
/// encoding of the by columns.
enum RunIndex {
    Single(Run),
    Keyed(HashMap<Box<[u8]>, Run>),
}

struct RightRuns {
    index: RunIndex,
    converter: Option<RowConverter>,
    /// Estimated bytes held by the index itself (runs + keys), on top of the
    /// buffered batches — reported to the memory reservation.
    mem_bytes: usize,
}

impl RightRuns {
    fn build(
        batches: &[RecordBatch],
        right_schema: &SchemaRef,
        options: &AsOfOptions,
    ) -> DfResult<Self> {
        let time_idx = right_schema.index_of(&options.right_on)?;
        let by_idx: Vec<usize> = options
            .by
            .iter()
            .map(|(_, r)| right_schema.index_of(r).map_err(DataFusionError::from))
            .collect::<DfResult<_>>()?;
        // Per (times, locs) entry: one i64 plus one (usize, usize).
        const RUN_ROW_BYTES: usize = 8 + 16;

        if by_idx.is_empty() {
            let mut run = Run {
                times: Vec::new(),
                locs: Vec::new(),
            };
            for (bi, batch) in batches.iter().enumerate() {
                let time = time_column_i64(batch, time_idx)?;
                for ri in 0..batch.num_rows() {
                    run.times.push(time[ri]);
                    run.locs.push((bi, ri));
                }
            }
            Self::ensure_sorted(&mut run);
            let mem_bytes = run.times.len() * RUN_ROW_BYTES;
            return Ok(Self {
                index: RunIndex::Single(run),
                converter: None,
                mem_bytes,
            });
        }

        let converter = RowConverter::new(
            by_idx
                .iter()
                .map(|&i| SortField::new(right_schema.field(i).data_type().clone()))
                .collect(),
        )?;
        let mut map: HashMap<Box<[u8]>, Run> = HashMap::new();
        let mut key_bytes = 0usize;
        let mut total_rows = 0usize;
        for (bi, batch) in batches.iter().enumerate() {
            let time = time_column_i64(batch, time_idx)?;
            let cols: Vec<ArrayRef> = by_idx.iter().map(|&i| batch.column(i).clone()).collect();
            let rows = converter.convert_columns(&cols)?;
            for ri in 0..batch.num_rows() {
                let key = rows.row(ri).data();
                // Owned key allocation only on first sight of a key.
                if !map.contains_key(key) {
                    key_bytes += key.len();
                    map.insert(
                        key.to_vec().into_boxed_slice(),
                        Run {
                            times: Vec::new(),
                            locs: Vec::new(),
                        },
                    );
                }
                let run = map.get_mut(key).expect("key just inserted");
                run.times.push(time[ri]);
                run.locs.push((bi, ri));
                total_rows += 1;
            }
        }
        for run in map.values_mut() {
            Self::ensure_sorted(run);
        }
        let mem_bytes = total_rows * RUN_ROW_BYTES + key_bytes;
        Ok(Self {
            index: RunIndex::Keyed(map),
            converter: Some(converter),
            mem_bytes,
        })
    }

    /// Inputs arrive time-sorted globally, so each per-key run is sorted.
    /// Defensive check (cheap): verify and stable-sort if violated.
    fn ensure_sorted(run: &mut Run) {
        if run.times.windows(2).any(|w| w[0] > w[1]) {
            let mut idx: Vec<usize> = (0..run.times.len()).collect();
            idx.sort_by_key(|&i| run.times[i]);
            run.times = idx.iter().map(|&i| run.times[i]).collect();
            run.locs = idx.iter().map(|&i| run.locs[i]).collect();
        }
    }

    /// Find the matching right location for a left time value. `key` is the
    /// encoded by-key of the left row (`None` when the join has no by-keys).
    fn probe(&self, key: Option<&[u8]>, t: i64, options: &AsOfOptions) -> Option<(usize, usize)> {
        let run = match (&self.index, key) {
            (RunIndex::Single(run), _) => run,
            (RunIndex::Keyed(map), Some(key)) => map.get(key)?,
            (RunIndex::Keyed(_), None) => return None,
        };
        match options.direction {
            AsOfDirection::Backward => {
                // Last index with times[i] <= t.
                let pos = run.times.partition_point(|&x| x <= t);
                if pos == 0 {
                    return None;
                }
                let i = pos - 1;
                if let Some(tol) = options.tolerance {
                    if t - run.times[i] > tol {
                        return None;
                    }
                }
                Some(run.locs[i])
            }
            AsOfDirection::Forward => {
                // First index with times[i] >= t.
                let pos = run.times.partition_point(|&x| x < t);
                if pos >= run.times.len() {
                    return None;
                }
                if let Some(tol) = options.tolerance {
                    if run.times[pos] - t > tol {
                        return None;
                    }
                }
                Some(run.locs[pos])
            }
        }
    }
}

/// View a time column as raw i64 (any timestamp unit / integer type).
/// Zero-copy for timestamp and Int64 columns (a `ScalarBuffer` clone only
/// bumps a refcount); other integer types fall back to a cast.
pub(crate) fn time_column_i64(batch: &RecordBatch, idx: usize) -> DfResult<ScalarBuffer<i64>> {
    use arrow::array::{
        Int64Array, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray,
    };
    let col = batch.column(idx);
    if col.null_count() > 0 {
        return Err(DataFusionError::Execution(
            "asof: time column contains nulls".into(),
        ));
    }
    macro_rules! values {
        ($ty:ty) => {
            col.as_any()
                .downcast_ref::<$ty>()
                .expect("checked data type")
                .values()
                .clone()
        };
    }
    Ok(match col.data_type() {
        DataType::Int64 => values!(Int64Array),
        DataType::Timestamp(TimeUnit::Second, _) => values!(TimestampSecondArray),
        DataType::Timestamp(TimeUnit::Millisecond, _) => values!(TimestampMillisecondArray),
        DataType::Timestamp(TimeUnit::Microsecond, _) => values!(TimestampMicrosecondArray),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => values!(TimestampNanosecondArray),
        _ => {
            let casted = arrow::compute::cast(col, &DataType::Int64)?;
            let arr = casted
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("time column cast failed".into()))?;
            arr.values().clone()
        }
    })
}

struct Joiner {
    runs: RightRuns,
    right_batches: Vec<RecordBatch>,
    options: AsOfOptions,
    schema: SchemaRef,
    right_kept: Vec<usize>,
    right_schema: SchemaRef,
    left_time_idx: usize,
    left_by_idx: Vec<usize>,
    /// Keeps the right-side buffer charged to the memory pool for the
    /// operator's lifetime; freed on drop when the stream completes.
    _reservation: MemoryReservation,
}

impl Joiner {
    fn join_batch(&self, left: &RecordBatch) -> DfResult<RecordBatch> {
        let times = time_column_i64(left, self.left_time_idx)?;

        // Encode left by-keys with the right side's converter (types match).
        let left_rows = match &self.runs.converter {
            Some(conv) => {
                let cols: Vec<ArrayRef> = self
                    .left_by_idx
                    .iter()
                    .map(|&i| left.column(i).clone())
                    .collect();
                Some(conv.convert_columns(&cols)?)
            }
            None => None,
        };

        // Match every left row (no per-row allocations: keys are borrowed
        // row-encoding bytes).
        let mut matches: Vec<Option<(usize, usize)>> = Vec::with_capacity(left.num_rows());
        for ri in 0..left.num_rows() {
            let key = left_rows.as_ref().map(|rows| rows.row(ri).data());
            matches.push(self.runs.probe(key, times[ri], &self.options));
        }

        // INNER: filter unmatched left rows.
        let (left_out, matches): (RecordBatch, Vec<Option<(usize, usize)>>) = if self.options.inner
        {
            let mask: arrow::array::BooleanArray =
                matches.iter().map(|m| Some(m.is_some())).collect();
            let filtered = arrow::compute::filter_record_batch(left, &mask)?;
            (filtered, matches.into_iter().flatten().map(Some).collect())
        } else {
            (left.clone(), matches)
        };

        // Gather right columns via interleave, with a one-row null batch as
        // the sink for unmatched rows.
        let mut arrays: Vec<ArrayRef> = left_out.columns().to_vec();
        for &col in &self.right_kept {
            let mut sources: Vec<ArrayRef> = self
                .right_batches
                .iter()
                .map(|b| b.column(col).clone())
                .collect();
            let null_arr = new_null_array(self.right_schema.field(col).data_type(), 1);
            sources.push(null_arr);
            let null_batch_idx = sources.len() - 1;
            let refs: Vec<&dyn arrow::array::Array> = sources.iter().map(|a| a.as_ref()).collect();
            let indices: Vec<(usize, usize)> = matches
                .iter()
                .map(|m| m.unwrap_or((null_batch_idx, 0)))
                .collect();
            arrays.push(arrow::compute::interleave(&refs, &indices)?);
        }
        RecordBatch::try_new(self.schema.clone(), arrays).map_err(DataFusionError::from)
    }
}

// ---------------------------------------------------------------------------
// DataFrame + SQL surfaces
// ---------------------------------------------------------------------------

/// DataFrame-level ASOF join.
pub fn asof_join(left: DataFrame, right: DataFrame, options: AsOfOptions) -> DfResult<DataFrame> {
    let (session_state, left_plan) = left.into_parts();
    let (_, right_plan) = right.into_parts();
    let node = AsOfJoinNode::try_new(left_plan, right_plan, options)?;
    let plan = LogicalPlan::Extension(Extension {
        node: Arc::new(node),
    });
    Ok(DataFrame::new(session_state, plan))
}

/// SQL table function:
/// `asof_join('left', 'right', 'left_on', 'right_on' [, 'by1,by2'
///            [, 'backward'|'forward' [, tolerance]]])`.
#[derive(Debug)]
pub struct AsOfJoinFunc {
    db: Arc<h5i_db_core::Database>,
    url: datafusion::execution::object_store::ObjectStoreUrl,
    metrics: ScanMetricsCollector,
}

impl AsOfJoinFunc {
    pub fn new(
        db: Arc<h5i_db_core::Database>,
        url: datafusion::execution::object_store::ObjectStoreUrl,
        metrics: ScanMetricsCollector,
    ) -> Self {
        Self { db, url, metrics }
    }
}

fn expect_str(args: &[Expr], i: usize, what: &str) -> DfResult<String> {
    match args.get(i) {
        Some(Expr::Literal(datafusion::scalar::ScalarValue::Utf8(Some(s)), _)) => Ok(s.clone()),
        _ => Err(DataFusionError::Plan(format!(
            "asof_join: argument {i} must be {what} (a string literal)"
        ))),
    }
}

impl TableFunctionImpl for AsOfJoinFunc {
    fn call_with_args(&self, args: TableFunctionArgs) -> DfResult<Arc<dyn TableProvider>> {
        let args = args.exprs();
        if args.len() < 4 || args.len() > 7 {
            return Err(DataFusionError::Plan(
                "asof_join('left_table', 'right_table', 'left_on', 'right_on' \
                 [, 'by_cols' [, 'backward'|'forward' [, tolerance]]])"
                    .into(),
            ));
        }
        let left_table = expect_str(args, 0, "the left table name")?;
        let right_table = expect_str(args, 1, "the right table name")?;
        let left_on = expect_str(args, 2, "the left time column")?;
        let right_on = expect_str(args, 3, "the right time column")?;
        let by: Vec<(String, String)> = match args.get(4) {
            None => vec![],
            Some(_) => expect_str(args, 4, "comma-separated by columns")?
                .split(',')
                .filter(|s| !s.trim().is_empty())
                .map(|s| {
                    // "col" or "lcol=rcol"
                    match s.split_once('=') {
                        Some((l, r)) => (l.trim().to_string(), r.trim().to_string()),
                        None => (s.trim().to_string(), s.trim().to_string()),
                    }
                })
                .collect(),
        };
        let direction = match args.get(5) {
            None => AsOfDirection::Backward,
            Some(_) => match expect_str(args, 5, "'backward' or 'forward'")?.as_str() {
                "backward" => AsOfDirection::Backward,
                "forward" => AsOfDirection::Forward,
                other => {
                    return Err(DataFusionError::Plan(format!(
                        "asof_join: direction must be 'backward' or 'forward', got {other:?}"
                    )))
                }
            },
        };
        let tolerance = match args.get(6) {
            None => None,
            Some(Expr::Literal(datafusion::scalar::ScalarValue::Int64(Some(v)), _)) => Some(*v),
            Some(_) => {
                return Err(DataFusionError::Plan(
                    "asof_join: tolerance must be an integer (raw time units)".into(),
                ))
            }
        };

        let options = AsOfOptions {
            left_on,
            right_on,
            by,
            direction,
            tolerance,
            inner: false,
        };

        let left = block_on(self.db.resolve(&left_table, h5i_db_core::ReadAt::Latest))
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let right = block_on(self.db.resolve(&right_table, h5i_db_core::ReadAt::Latest))
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let (schema, right_kept) = asof_output_schema(&left.schema, &right.schema, &options)?;
        Ok(Arc::new(AsOfTableProvider {
            left: Arc::new(H5iTableProvider::new(
                left,
                self.url.clone(),
                self.metrics.clone(),
            )),
            right: Arc::new(H5iTableProvider::new(
                right,
                self.url.clone(),
                self.metrics.clone(),
            )),
            options,
            schema,
            right_kept,
        }))
    }
}

/// Provider produced by the `asof_join` table function: scanning it plans
/// both sides and wraps them in the ASOF operator, forwarding left-side
/// filters, widened right-side time bounds, projections, and (for LEFT
/// joins) limits to the child scans so segment pruning applies.
#[derive(Debug)]
struct AsOfTableProvider {
    left: Arc<H5iTableProvider>,
    right: Arc<H5iTableProvider>,
    options: AsOfOptions,
    schema: SchemaRef,
    /// Right-table column indices kept in the output (parallel to the output
    /// fields after the left ones).
    right_kept: Vec<usize>,
}

/// Raw i64 time value inside a comparison literal, if it is a type we can
/// widen (timestamps of any unit, or Int64).
fn scalar_time_i64(s: &ScalarValue) -> Option<i64> {
    match s {
        ScalarValue::TimestampSecond(v, _)
        | ScalarValue::TimestampMillisecond(v, _)
        | ScalarValue::TimestampMicrosecond(v, _)
        | ScalarValue::TimestampNanosecond(v, _)
        | ScalarValue::Int64(v) => *v,
        _ => None,
    }
}

/// The same scalar variant carrying an adjusted time value.
fn scalar_with_time(proto: &ScalarValue, v: i64) -> ScalarValue {
    match proto {
        ScalarValue::TimestampSecond(_, tz) => ScalarValue::TimestampSecond(Some(v), tz.clone()),
        ScalarValue::TimestampMillisecond(_, tz) => {
            ScalarValue::TimestampMillisecond(Some(v), tz.clone())
        }
        ScalarValue::TimestampMicrosecond(_, tz) => {
            ScalarValue::TimestampMicrosecond(Some(v), tz.clone())
        }
        ScalarValue::TimestampNanosecond(_, tz) => {
            ScalarValue::TimestampNanosecond(Some(v), tz.clone())
        }
        _ => ScalarValue::Int64(Some(v)),
    }
}

/// Strip table qualifiers so an output-schema filter resolves against the
/// bare left table schema.
fn unqualify(expr: &Expr) -> Expr {
    expr.clone()
        .transform(|e| {
            Ok(match e {
                Expr::Column(c) => {
                    Transformed::yes(Expr::Column(Column::new_unqualified(c.name)))
                }
                other => Transformed::no(other),
            })
        })
        .map(|t| t.data)
        .unwrap_or_else(|_| expr.clone())
}

impl AsOfTableProvider {
    /// A filter can run below the join iff every column it references is a
    /// left-side output column (left columns keep their names in the join
    /// output). Right-side rows can never be filtered below the join —
    /// that would change which row is the asof match.
    fn is_left_filter(&self, expr: &Expr) -> bool {
        let left = self.left.schema();
        let lw = left.fields().len();
        expr.column_refs().iter().all(|c| {
            left.index_of(&c.name).is_ok()
                && self.schema.fields().iter().skip(lw).all(|f| f.name() != &c.name)
        })
    }

    /// `left_on` compared to a literal, normalized to `col <op> lit`.
    fn time_cmp(&self, f: &Expr) -> Option<(Operator, ScalarValue)> {
        let Expr::BinaryExpr(BinaryExpr { left, op, right }) = f else {
            return None;
        };
        let (op, lit) = match (left.as_ref(), right.as_ref()) {
            (Expr::Column(c), Expr::Literal(v, _)) if c.name == self.options.left_on => {
                (*op, v.clone())
            }
            (Expr::Literal(v, _), Expr::Column(c)) if c.name == self.options.left_on => {
                (op.swap()?, v.clone())
            }
            _ => return None,
        };
        scalar_time_i64(&lit)?;
        Some((op, lit))
    }

    /// Derive right-side time-bound filters from the forwarded left filters,
    /// widened so no potentially-matching right row is dropped. This is sound
    /// because the same left filters are re-applied above the join (Inexact):
    /// any left row whose match could depend on a pruned right row is itself
    /// outside the bounds and gets filtered out.
    fn right_time_bounds(&self, left_filters: &[Expr]) -> Vec<Expr> {
        let mut lo: Option<i64> = None;
        let mut hi: Option<i64> = None;
        let mut proto: Option<ScalarValue> = None;
        for f in left_filters {
            let Some((op, lit)) = self.time_cmp(f) else {
                continue;
            };
            let Some(v) = scalar_time_i64(&lit) else {
                continue;
            };
            // Strict bounds treated as inclusive: conservative and sound.
            match op {
                Operator::Gt | Operator::GtEq => lo = Some(lo.map_or(v, |x| x.max(v))),
                Operator::Lt | Operator::LtEq => hi = Some(hi.map_or(v, |x| x.min(v))),
                Operator::Eq => {
                    lo = Some(lo.map_or(v, |x| x.max(v)));
                    hi = Some(hi.map_or(v, |x| x.min(v)));
                }
                _ => continue,
            }
            proto = Some(lit);
        }
        let Some(proto) = proto else {
            return vec![];
        };
        let right_col = Expr::Column(Column::new_unqualified(&self.options.right_on));
        // A negative tolerance never restricts matches, so it must not be
        // used to derive bounds.
        let tol = self.options.tolerance.filter(|t| *t >= 0);
        let mut filters = Vec::new();
        match self.options.direction {
            AsOfDirection::Backward => {
                // Matches satisfy r.ts <= l.ts, and with a tolerance also
                // l.ts - r.ts <= tol.
                if let Some(hi) = hi {
                    filters.push(right_col.clone().lt_eq(lit(scalar_with_time(&proto, hi))));
                }
                if let (Some(lo), Some(tol)) = (lo, tol) {
                    filters.push(
                        right_col
                            .clone()
                            .gt_eq(lit(scalar_with_time(&proto, lo.saturating_sub(tol)))),
                    );
                }
            }
            AsOfDirection::Forward => {
                // Matches satisfy r.ts >= l.ts, and with a tolerance also
                // r.ts - l.ts <= tol.
                if let Some(lo) = lo {
                    filters.push(right_col.clone().gt_eq(lit(scalar_with_time(&proto, lo))));
                }
                if let (Some(hi), Some(tol)) = (hi, tol) {
                    filters.push(
                        right_col
                            .clone()
                            .lt_eq(lit(scalar_with_time(&proto, hi.saturating_add(tol)))),
                    );
                }
            }
        }
        filters
    }
}

#[async_trait]
impl TableProvider for AsOfTableProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        // Left-only filters are forwarded to the left scan for segment
        // pruning; Inexact, so DataFusion re-applies them above the join.
        Ok(filters
            .iter()
            .map(|f| {
                if self.is_left_filter(f) {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let left_schema = self.left.schema();
        let right_schema = self.right.schema();
        let lw = left_schema.fields().len();

        // Left filters run below the join for pruning; the right scan gets
        // widened time bounds derived from them.
        let left_filters: Vec<Expr> = filters
            .iter()
            .filter(|f| self.is_left_filter(f))
            .map(unqualify)
            .collect();
        let right_filters = self.right_time_bounds(&left_filters);
        // Output rows are 1:1 with left rows for LEFT asof joins, so a bare
        // limit bounds the left scan. DataFusion only passes a limit when no
        // filter sits above the scan, but guard anyway: a limit under a
        // filter would truncate before filtering.
        let left_limit = if filters.is_empty() && !self.options.inner {
            limit
        } else {
            None
        };

        // Column pushdown: children read only what the join and the
        // requested projection need (join keys always included).
        let (left_proj, right_proj) = match projection {
            None => (None, None),
            Some(indices) => {
                let mut left_needed: BTreeSet<usize> =
                    indices.iter().filter(|&&i| i < lw).copied().collect();
                left_needed.insert(left_schema.index_of(&self.options.left_on)?);
                for (l, _) in &self.options.by {
                    left_needed.insert(left_schema.index_of(l)?);
                }
                let mut right_needed: BTreeSet<usize> = indices
                    .iter()
                    .filter(|&&i| i >= lw)
                    .map(|&i| self.right_kept[i - lw])
                    .collect();
                right_needed.insert(right_schema.index_of(&self.options.right_on)?);
                for (_, r) in &self.options.by {
                    right_needed.insert(right_schema.index_of(r)?);
                }
                (
                    Some(left_needed.into_iter().collect::<Vec<_>>()),
                    Some(right_needed.into_iter().collect::<Vec<_>>()),
                )
            }
        };

        let left = self
            .left
            .scan(state, left_proj.as_ref(), &left_filters, left_limit)
            .await?;
        let right = self
            .right
            .scan(state, right_proj.as_ref(), &right_filters, None)
            .await?;
        // Join options refer to columns by name, so they remap onto the
        // projected child schemas unchanged.
        let joined = AsOfJoinExec::try_new_with_sort(left, right, self.options.clone())?;

        let Some(indices) = projection else {
            return Ok(joined);
        };
        let (left_proj, right_proj) = (left_proj.unwrap(), right_proj.unwrap());
        // Positions of the kept right columns inside the projected join
        // output: projected right columns minus the by-columns, in order.
        let by_right: std::collections::HashSet<&str> = self
            .options
            .by
            .iter()
            .map(|(_, r)| r.as_str())
            .collect();
        let right_out: Vec<usize> = right_proj
            .iter()
            .filter(|&&r| !by_right.contains(right_schema.field(r).name().as_str()))
            .copied()
            .collect();
        let joined_schema = joined.schema();
        // Restore the requested columns under their full-output names — the
        // collision renaming inside the join depends on which left columns
        // survived projection, so alias explicitly.
        let exprs: Vec<(Arc<dyn datafusion::physical_expr::PhysicalExpr>, String)> = indices
            .iter()
            .map(|&i| {
                let pos = if i < lw {
                    left_proj
                        .iter()
                        .position(|&l| l == i)
                        .expect("projected left column present")
                } else {
                    let rj = self.right_kept[i - lw];
                    left_proj.len()
                        + right_out
                            .iter()
                            .position(|&r| r == rj)
                            .expect("projected right column present")
                };
                (
                    Arc::new(expressions::Column::new(joined_schema.field(pos).name(), pos))
                        as Arc<dyn datafusion::physical_expr::PhysicalExpr>,
                    self.schema.field(i).name().clone(),
                )
            })
            .collect();
        Ok(Arc::new(
            datafusion::physical_plan::projection::ProjectionExec::try_new(exprs, joined)?,
        ))
    }
}
