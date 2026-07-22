//! Unbounded DataFusion table provider for append-only table tails.

use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::time::Duration;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableFunctionImpl, TableProvider};
use datafusion::common::ScalarValue;
use datafusion::datasource::TableType;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::LexOrdering;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use futures::StreamExt;
use h5i_db_core::{Database, ReadAt};

use crate::udtf::block_on;

/// Table function `tail('table' [, after_version [, poll_ms]])`.
///
/// With no version it starts after the current head. The result is unbounded;
/// callers should apply `LIMIT` or cancel the query when they are done.
pub struct TailFunc {
    db: Arc<Database>,
}

impl TailFunc {
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }
}

impl Debug for TailFunc {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TailFunc").finish_non_exhaustive()
    }
}

impl TableFunctionImpl for TailFunc {
    fn call(&self, args: &[Expr]) -> DfResult<Arc<dyn TableProvider>> {
        if args.is_empty() || args.len() > 3 {
            return Err(DataFusionError::Plan(
                "tail(table [, after_version [, poll_ms]]) takes 1 to 3 arguments".into(),
            ));
        }
        let Expr::Literal(ScalarValue::Utf8(Some(table)), _) = &args[0] else {
            return Err(DataFusionError::Plan(
                "tail: first argument must be a table name string".into(),
            ));
        };
        let requested_after = args.get(1).map(literal_u64).transpose()?;
        let poll_ms = args
            .get(2)
            .map(literal_u64)
            .transpose()?
            .unwrap_or(250)
            .max(10);
        let resolved = block_on(self.db.resolve(table, ReadAt::Latest))
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let after = requested_after.unwrap_or(resolved.manifest.sequence);
        Ok(Arc::new(TailProvider {
            db: self.db.clone(),
            table: table.clone(),
            schema: resolved.schema,
            after,
            poll_interval: Duration::from_millis(poll_ms),
        }))
    }
}

fn literal_u64(expr: &Expr) -> DfResult<u64> {
    let Expr::Literal(value, _) = expr else {
        return Err(DataFusionError::Plan(
            "tail: version and poll interval must be integer literals".into(),
        ));
    };
    let value = match value {
        ScalarValue::UInt64(Some(v)) => Some(*v),
        ScalarValue::UInt32(Some(v)) => Some(u64::from(*v)),
        ScalarValue::Int64(Some(v)) => u64::try_from(*v).ok(),
        ScalarValue::Int32(Some(v)) => u64::try_from(*v).ok(),
        _ => None,
    };
    value.ok_or_else(|| DataFusionError::Plan("tail: expected a non-negative integer".into()))
}

/// Snapshot-schema provider whose single partition follows appended versions.
pub struct TailProvider {
    db: Arc<Database>,
    table: String,
    schema: SchemaRef,
    after: u64,
    poll_interval: Duration,
}

impl Debug for TailProvider {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TailProvider")
            .field("table", &self.table)
            .field("after", &self.after)
            .finish()
    }
}

#[async_trait]
impl TableProvider for TailProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let partition: Arc<dyn PartitionStream> = Arc::new(TailPartition {
            db: self.db.clone(),
            table: self.table.clone(),
            schema: self.schema.clone(),
            after: self.after,
            poll_interval: self.poll_interval,
        });
        Ok(Arc::new(StreamingTableExec::try_new(
            self.schema.clone(),
            vec![partition],
            projection,
            std::iter::empty::<LexOrdering>(),
            true,
            limit,
        )?))
    }
}

struct TailPartition {
    db: Arc<Database>,
    table: String,
    schema: SchemaRef,
    after: u64,
    poll_interval: Duration,
}

impl Debug for TailPartition {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TailPartition")
            .field("table", &self.table)
            .field("after", &self.after)
            .finish()
    }
}

impl PartitionStream for TailPartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<datafusion::execution::TaskContext>) -> SendableRecordBatchStream {
        let schema = self.schema.clone();
        let stream = self
            .db
            .clone()
            .tail_stream(self.table.clone(), self.after, self.poll_interval)
            .map(|result| result.map_err(|e| DataFusionError::External(Box::new(e))));
        Box::pin(RecordBatchStreamAdapter::new(schema, stream))
    }
}
