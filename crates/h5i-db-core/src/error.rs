//! Error model shared by every h5i-db layer.
//!
//! The error envelope is part of the public contract (CLI serializes it as
//! `{code, message, retryable, hint}`), so every variant carries a stable
//! machine-readable `code`, a `retryable` classification, and an exit-code
//! category.

use std::fmt;

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Stable exit-code categories used by the CLI.
///
/// 0 = success, 2 = user error, 3 = version conflict, 4 = resource limit /
/// cancelled, 5 = corruption or internal error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitCategory {
    Success = 0,
    UserError = 2,
    Conflict = 3,
    Limit = 4,
    Internal = 5,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("database not found at {path}")]
    DatabaseNotFound { path: String },

    #[error("database already exists at {path}")]
    DatabaseExists { path: String },

    #[error("table {name:?} not found")]
    TableNotFound { name: String },

    #[error("table {name:?} already exists")]
    TableExists { name: String },

    #[error("version {requested} of table {table:?} not found ({hint})")]
    VersionNotFound {
        table: String,
        requested: String,
        hint: String,
    },

    #[error("snapshot {name:?} not found")]
    SnapshotNotFound { name: String },

    #[error(
        "version conflict on table {table:?}: expected head {expected}, found {actual}; \
         another writer committed first"
    )]
    VersionConflict {
        table: String,
        expected: u64,
        actual: u64,
    },

    #[error("schema mismatch: {detail}")]
    SchemaMismatch { detail: String },

    #[error("sort-order violation: {detail}")]
    SortOrderViolation { detail: String },

    #[error("invalid input: {detail}")]
    InvalidInput { detail: String },

    #[error("unsupported operation: {detail}")]
    Unsupported { detail: String },

    #[error("database is open read-only; {op} is a write operation")]
    ReadOnly { op: String },

    #[error(
        "mutation policy forbids direct {op}; create a reviewed plan and apply it          (CLI: --plan, then `plan apply`)"
    )]
    PolicyViolation { op: String },

    #[error("corruption detected in object {object}: {detail}")]
    Corruption { object: String, detail: String },

    #[error("format version {found} is newer than this reader supports (max {supported})")]
    FormatTooNew { found: u32, supported: u32 },

    #[error("resource limit exceeded: {detail}")]
    LimitExceeded { detail: String },

    #[error("operation timed out after {seconds}s")]
    Timeout { seconds: u64 },

    #[error("could not acquire writer lock on table {table:?} within {waited_ms}ms")]
    LockTimeout { table: String, waited_ms: u64 },

    #[error("storage error: {0}")]
    ObjectStore(#[from] object_store::Error),

    #[error("io error on {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },

    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("metadata (de)serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("internal error: {detail}")]
    Internal { detail: String },
}

impl Error {
    /// Stable machine-readable code for the CLI error envelope.
    pub fn code(&self) -> &'static str {
        match self {
            Error::DatabaseNotFound { .. } => "database_not_found",
            Error::DatabaseExists { .. } => "database_exists",
            Error::TableNotFound { .. } => "table_not_found",
            Error::TableExists { .. } => "table_exists",
            Error::VersionNotFound { .. } => "version_not_found",
            Error::SnapshotNotFound { .. } => "snapshot_not_found",
            Error::VersionConflict { .. } => "version_conflict",
            Error::SchemaMismatch { .. } => "schema_mismatch",
            Error::SortOrderViolation { .. } => "sort_order_violation",
            Error::InvalidInput { .. } => "invalid_input",
            Error::Unsupported { .. } => "unsupported",
            Error::ReadOnly { .. } => "read_only",
            Error::PolicyViolation { .. } => "policy_violation",
            Error::Corruption { .. } => "corruption",
            Error::FormatTooNew { .. } => "format_too_new",
            Error::LimitExceeded { .. } => "limit_exceeded",
            Error::Timeout { .. } => "timeout",
            Error::LockTimeout { .. } => "lock_timeout",
            Error::ObjectStore(_) => "storage",
            Error::Io { .. } => "io",
            Error::Arrow(_) => "arrow",
            Error::Parquet(_) => "parquet",
            Error::Serde(_) => "metadata",
            Error::Internal { .. } => "internal",
        }
    }

    /// Whether re-running the same operation can plausibly succeed.
    ///
    /// A supervising agent uses this to decide between retrying and
    /// replanning: conflicts and lock/timeout races are retryable, schema and
    /// input errors are not.
    pub fn retryable(&self) -> bool {
        matches!(
            self,
            Error::VersionConflict { .. }
                | Error::LockTimeout { .. }
                | Error::Timeout { .. }
                | Error::ObjectStore(_)
                | Error::Io { .. }
        )
    }

    /// A one-line actionable hint, when one exists.
    pub fn hint(&self) -> Option<String> {
        match self {
            Error::VersionConflict { table, .. } => Some(format!(
                "re-read the head of {table:?} and retry; for pure appends use append_with_retry"
            )),
            Error::VersionNotFound { table, hint, .. } => {
                Some(format!("{hint}; run `h5i-db versions <db> {table}`"))
            }
            Error::TableNotFound { .. } => Some("run `h5i-db tables <db>` to list tables".into()),
            Error::SnapshotNotFound { .. } => {
                Some("run `h5i-db snapshot list <db>` to list snapshots".into())
            }
            Error::SortOrderViolation { .. } => Some(
                "append requires input sorted by the time column with min >= current table max; \
                 use `write` or sort the input"
                    .into(),
            ),
            Error::FormatTooNew { .. } => Some("upgrade h5i-db to read this database".into()),
            Error::ReadOnly { .. } => Some("re-open without --read-only".into()),
            Error::PolicyViolation { op } => Some(format!(
                "run the {op} with --plan to preview it, review, then `h5i-db plan apply`; \
                 or relax the policy with `h5i-db policy set`"
            )),
            _ => None,
        }
    }

    pub fn exit_category(&self) -> ExitCategory {
        match self {
            Error::VersionConflict { .. } | Error::LockTimeout { .. } => ExitCategory::Conflict,
            Error::LimitExceeded { .. } | Error::Timeout { .. } => ExitCategory::Limit,
            Error::Corruption { .. } | Error::Internal { .. } => ExitCategory::Internal,
            Error::ObjectStore(_)
            | Error::Io { .. }
            | Error::Arrow(_)
            | Error::Parquet(_)
            | Error::Serde(_) => ExitCategory::Internal,
            _ => ExitCategory::UserError,
        }
    }

    pub fn internal(detail: impl fmt::Display) -> Self {
        Error::Internal {
            detail: detail.to_string(),
        }
    }

    pub fn invalid(detail: impl fmt::Display) -> Self {
        Error::InvalidInput {
            detail: detail.to_string(),
        }
    }

    pub fn corruption(object: impl fmt::Display, detail: impl fmt::Display) -> Self {
        Error::Corruption {
            object: object.to_string(),
            detail: detail.to_string(),
        }
    }

    pub fn io(path: impl fmt::Display, source: std::io::Error) -> Self {
        Error::Io {
            path: path.to_string(),
            source,
        }
    }
}
