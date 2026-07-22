//! # h5i-db-query
//!
//! DataFusion query layer for h5i-db: snapshot-bound `TableProvider` with
//! manifest-statistics pruning, time-travel table function, `time_bucket`,
//! and the ASOF join operator. All DataFusion types stay inside this crate —
//! `h5i-db-core` is engine-free.

pub mod asof;
pub mod finance;
pub mod functions;
pub mod gapfill;
pub mod provider;
pub mod pruning;
pub mod session;
pub mod tail;
pub mod udtf;

pub use asof::{asof_join, AsOfDirection, AsOfJoinExec, AsOfJoinNode, AsOfOptions};
pub use provider::{H5iTableProvider, ScanMetrics, ScanMetricsCollector};
pub use session::{H5iSession, SessionOptions};
pub use tail::TailProvider;

// Re-export the engine so downstream crates (CLI, bench) use one version.
pub use datafusion;
