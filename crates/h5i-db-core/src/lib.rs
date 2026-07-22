//! # h5i-db-core
//!
//! The versioned Arrow/Parquet storage kernel of h5i-db: immutable segments,
//! per-version manifests directly addressed by sequence number, an atomic
//! compare-and-swap commit protocol, snapshots, compaction, vacuum, and
//! verify — with no query-engine dependency.
//!
//! See DESIGN_CLAUDE.md at the repository root for the full design.

pub mod backend;
pub mod catalog;
pub mod database;
pub mod error;
pub mod layout;
pub mod manifest;
pub mod plan;
pub mod segment;
pub mod snapshot;
pub mod spec;
pub mod util;

pub use backend::{Backend, HeadState, HeadStore, HeadTag};
pub use database::{
    CommitResult, Database, ReadAt, ResolvedTable, ScanOptions, ScanReport, VacuumReport,
    VerifyReport, VersionSummary, WriteOptions,
};
pub use error::{Error, ExitCategory, Result};
pub use manifest::{ColumnStats, Head, OpKind, SegmentMeta, VersionManifest};
pub use plan::{MutationPlan, PlanSummary};
pub use snapshot::{Snapshot, SnapshotEntry};
pub use spec::{Codec, StorageOptions, TableOptions, TableSpec};
