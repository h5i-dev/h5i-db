//! Time-travel table function: `h5i('table' [, version | 'as-of ts' | 'snapshot'])`.
//!
//! Examples:
//! ```sql
//! SELECT * FROM h5i('trades');                              -- latest
//! SELECT * FROM h5i('trades', 42);                          -- exact version
//! SELECT * FROM h5i('trades', '2026-07-01T00:00:00Z');      -- as-of commit time
//! SELECT * FROM h5i('trades', 'eod-2026-07-18');            -- named snapshot
//! ```
//!
//! No SQL grammar changes needed — this is a standard DataFusion table
//! function, which is exactly why the design chose it (DESIGN_CLAUDE.md §6.3).

use std::sync::Arc;

use datafusion::catalog::{TableFunctionArgs, TableFunctionImpl};
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::Expr;
use datafusion::scalar::ScalarValue;
use h5i_db_core::{Database, ReadAt};

use crate::provider::{H5iTableProvider, ScanMetricsCollector};

/// Run an async resolution from DataFusion's synchronous planning context.
pub(crate) fn block_on<F: std::future::Future>(fut: F) -> F::Output {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => tokio::task::block_in_place(|| handle.block_on(fut)),
        Err(_) => tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build fallback runtime")
            .block_on(fut),
    }
}

#[derive(Debug)]
pub struct TimeTravelFunc {
    db: Arc<Database>,
    url: ObjectStoreUrl,
    metrics: ScanMetricsCollector,
}

impl TimeTravelFunc {
    pub fn new(db: Arc<Database>, url: ObjectStoreUrl, metrics: ScanMetricsCollector) -> Self {
        Self { db, url, metrics }
    }
}

fn literal_str(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(s)), _) => Some(s),
        Expr::Literal(ScalarValue::LargeUtf8(Some(s)), _) => Some(s),
        _ => None,
    }
}

fn literal_int(expr: &Expr) -> Option<u64> {
    match expr {
        Expr::Literal(ScalarValue::Int64(Some(v)), _) if *v >= 0 => Some(*v as u64),
        Expr::Literal(ScalarValue::UInt64(Some(v)), _) => Some(*v),
        Expr::Literal(ScalarValue::Int32(Some(v)), _) if *v >= 0 => Some(*v as u64),
        _ => None,
    }
}

impl TableFunctionImpl for TimeTravelFunc {
    fn call_with_args(&self, args: TableFunctionArgs) -> DfResult<Arc<dyn TableProvider>> {
        let args = args.exprs();
        if args.is_empty() || args.len() > 2 {
            return Err(DataFusionError::Plan(
                "h5i(table_name [, version | 'as-of-timestamp' | 'snapshot-name']) \
                 takes 1 or 2 arguments"
                    .into(),
            ));
        }
        let table = literal_str(&args[0]).ok_or_else(|| {
            DataFusionError::Plan("h5i: first argument must be a table name string".into())
        })?;
        let at = match args.get(1) {
            None => ReadAt::Latest,
            Some(arg) => {
                if let Some(v) = literal_int(arg) {
                    ReadAt::Version(v)
                } else if let Some(s) = literal_str(arg) {
                    match chrono::DateTime::parse_from_rfc3339(s) {
                        Ok(ts) => ReadAt::AsOf(ts.timestamp_nanos_opt().ok_or_else(|| {
                            DataFusionError::Plan(format!("h5i: timestamp {s:?} out of range"))
                        })?),
                        // Not a timestamp → treat as snapshot name.
                        Err(_) => ReadAt::Snapshot(s.to_string()),
                    }
                } else {
                    return Err(DataFusionError::Plan(
                        "h5i: second argument must be an integer version, an RFC3339 \
                         timestamp, or a snapshot name"
                            .into(),
                    ));
                }
            }
        };

        let resolved = block_on(self.db.resolve(table, at))
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Arc::new(H5iTableProvider::new(
            resolved,
            self.url.clone(),
            self.metrics.clone(),
        )))
    }
}
