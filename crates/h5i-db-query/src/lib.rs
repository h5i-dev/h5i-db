//! # h5i-db-query
//!
//! DataFusion query layer for h5i-db: snapshot-bound `TableProvider` with
//! manifest-statistics pruning, time-travel table function, `time_bucket`,
//! and the ASOF join operator. All DataFusion types stay inside this crate —
//! `h5i-db-core` is engine-free.

mod aggregate_state;
pub mod arrival;
pub mod asof;
pub mod cross_section;
pub mod finance;
pub mod fork_scan;
pub mod functions;
pub mod gapfill;
pub mod latest;
pub mod metrics;
mod pin;
mod predicate_cache;
pub mod provider;
pub mod pruning;
pub mod rolling;
pub mod session;
mod sidecar;
pub mod tail;
pub mod udtf;

pub use aggregate_state::{
    AggregateStateMetrics, AggregateStateMode, AggregateStateStore, FinanceAggregate,
    FinanceAggregateResult, FinanceAggregateSpec,
};
pub use arrival::{
    ArrivalDeltaReport, ColumnDelta, DEFAULT_TOLERANCE, TableVersionDelta, arrival_delta,
};
pub use asof::{AsOfDirection, AsOfJoinExec, AsOfJoinNode, AsOfOptions, asof_join};
pub use fork_scan::{FORK_COLUMN, ForkScanFunc};
pub use metrics::{
    OperatorPerformanceMetrics, QueryPerformanceReport, QueryStatus, ReportedDataFrame,
    ReportedQueryStream, ScanMetrics, ScanMetricsCollector, WorkloadTelemetryEnvelope,
};
pub use predicate_cache::PredicateCacheMode;
pub use provider::H5iTableProvider;
pub use session::{H5iSession, ResearchPin, SessionOptions};
pub use tail::TailProvider;

// Re-export the engine so downstream crates (CLI, bench) use one version.
pub use datafusion;
