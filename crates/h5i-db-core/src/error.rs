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

/// A recovery step an agent can run verbatim.
///
/// `hint` explains a failure to a human in prose; a `NextAction` is the same
/// advice in a form a caller can execute without parsing English. `cmd` uses
/// the `<db>` placeholder for the database path, which the error does not
/// know — the CLI substitutes the path it was invoked with before writing the
/// envelope, so what reaches an agent is directly runnable.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct NextAction {
    /// Runnable command, e.g. `h5i-db versions <db> trades`.
    pub cmd: String,
    /// Why this is the right next step.
    pub why: String,
}

impl NextAction {
    fn new(cmd: impl Into<String>, why: impl Into<String>) -> Self {
        Self {
            cmd: cmd.into(),
            why: why.into(),
        }
    }
}

/// Case-insensitive Levenshtein distance, used only on the error path.
fn edit_distance(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.to_lowercase().chars().collect();
    let b: Vec<char> = b.to_lowercase().chars().collect();
    if a.is_empty() {
        return b.len();
    }
    if b.is_empty() {
        return a.len();
    }
    // Two rolling rows; identifiers are short, so this stays trivial.
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    let mut cur = vec![0usize; b.len() + 1];
    for (i, ca) in a.iter().enumerate() {
        cur[0] = i + 1;
        for (j, cb) in b.iter().enumerate() {
            let cost = usize::from(ca != cb);
            cur[j + 1] = (prev[j] + cost).min(prev[j + 1] + 1).min(cur[j] + 1);
        }
        std::mem::swap(&mut prev, &mut cur);
    }
    prev[b.len()]
}

/// The closest candidate to `name`, when one is close enough to be a plausible
/// typo rather than a different word. Returns `None` for an empty candidate
/// list or when nothing is near — a confidently wrong suggestion is worse than
/// none, because an agent will act on it.
pub fn nearest_name<'a, I>(name: &str, candidates: I) -> Option<String>
where
    I: IntoIterator<Item = &'a str>,
{
    // Allow roughly one edit per three characters, capped: "trade" → "trades"
    // qualifies, "xyz" → "trades" does not.
    let budget = (name.chars().count() / 3).clamp(1, 3);
    candidates
        .into_iter()
        .map(|c| (edit_distance(name, c), c))
        .filter(|(d, _)| *d <= budget)
        .min_by_key(|(d, c)| (*d, c.len()))
        .map(|(_, c)| c.to_string())
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("database not found at {path}")]
    DatabaseNotFound { path: String },

    #[error("database already exists at {path}")]
    DatabaseExists { path: String },

    #[error("table {name:?} not found")]
    TableNotFound {
        name: String,
        /// Closest existing table name, when the lookup looked like a typo.
        did_you_mean: Option<String>,
    },

    #[error("table {name:?} already exists")]
    TableExists { name: String },

    #[error("version {requested} of table {table:?} not found ({hint})")]
    VersionNotFound {
        table: String,
        requested: String,
        hint: String,
    },

    #[error("snapshot {name:?} not found")]
    SnapshotNotFound {
        name: String,
        /// Closest existing snapshot name, when the lookup looked like a typo.
        did_you_mean: Option<String>,
    },

    #[error(
        "version conflict on table {table:?}: expected head {expected}, found {actual}; \
         another writer committed first"
    )]
    VersionConflict {
        table: String,
        expected: u64,
        actual: u64,
    },

    #[error("fork {name:?} not found")]
    ForkNotFound {
        name: String,
        /// Closest existing fork name, when the lookup looked like a typo.
        did_you_mean: Option<String>,
    },

    #[error("fork {name:?} already exists")]
    ForkExists { name: String },

    #[error(
        "cannot promote table {table:?} from fork {fork:?}: base moved from version \
         {base} to {actual} since the fork was created{compaction_note}"
    )]
    PromoteConflict {
        fork: String,
        table: String,
        /// Version the fork was created from — the compare-and-swap expectation.
        base: u64,
        actual: u64,
        /// Filled in when every intervening base commit was a compaction, i.e.
        /// the base's *contents* did not actually change.
        compaction_note: String,
    },

    #[error("schema mismatch: {detail}")]
    SchemaMismatch { detail: String },

    #[error("sort-order violation on table {table:?}: {detail}")]
    SortOrderViolation { table: String, detail: String },

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

    #[error("data policy violation ({constraint}): {detail}")]
    DataPolicyViolation { constraint: String, detail: String },

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
            Error::ForkNotFound { .. } => "fork_not_found",
            Error::ForkExists { .. } => "fork_exists",
            Error::PromoteConflict { .. } => "promote_conflict",
            Error::SchemaMismatch { .. } => "schema_mismatch",
            Error::SortOrderViolation { .. } => "sort_order_violation",
            Error::InvalidInput { .. } => "invalid_input",
            Error::Unsupported { .. } => "unsupported",
            Error::ReadOnly { .. } => "read_only",
            Error::PolicyViolation { .. } => "policy_violation",
            Error::DataPolicyViolation { .. } => "data_policy_violation",
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
        // PromoteConflict is deliberately NOT retryable: the losing fork's work
        // was computed against a base that no longer exists, so re-running the
        // same promote can only fail again. The recovery is to re-fork and
        // re-run the analysis, which is a new operation, not a retry.
    }

    /// A one-line actionable hint, when one exists.
    pub fn hint(&self) -> Option<String> {
        match self {
            // Keep this hint surface-neutral: it reaches Rust, CLI, and
            // Python users alike, so it must not name an API that exists on
            // only one of them.
            Error::VersionConflict { table, .. } => Some(format!(
                "re-read the head of {table:?} and retry against it; pure appends rebase \
                 safely (the CLI and Python bindings already auto-retry those)"
            )),
            Error::VersionNotFound { table, hint, .. } => {
                Some(format!("{hint}; run `h5i-db versions <db> {table}`"))
            }
            Error::TableNotFound { .. } => Some("run `h5i-db tables <db>` to list tables".into()),
            Error::SnapshotNotFound { .. } => {
                Some("run `h5i-db snapshot list <db>` to list snapshots".into())
            }
            Error::ForkNotFound { .. } => Some("run `h5i-db fork list <db>` to list forks".into()),
            Error::ForkExists { .. } => {
                Some("pick another fork name, or drop the existing one with `h5i-db fork drop`".into())
            }
            Error::PromoteConflict { fork, table, .. } => Some(format!(
                "main advanced under this fork; re-fork from the current head and re-run, \
                 or inspect what changed with `h5i-db fork diff <db> {fork} --table {table}`"
            )),
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
            Error::DataPolicyViolation { .. } => Some(
                "the write breaks a data-safety constraint on this table; fix the offending \
                 rows, or inspect/relax the policy with `h5i-db data-policy get`"
                    .into(),
            ),
            _ => None,
        }
    }

    /// The closest existing identifier, when this failure looks like a typo.
    ///
    /// Kept separate from [`Self::next_actions`] because it is a *guess*: an
    /// agent should re-run against it, not assume it.
    pub fn did_you_mean(&self) -> Option<&str> {
        match self {
            Error::TableNotFound { did_you_mean, .. }
            | Error::SnapshotNotFound { did_you_mean, .. }
            | Error::ForkNotFound { did_you_mean, .. } => did_you_mean.as_deref(),
            _ => None,
        }
    }

    /// Recovery steps a caller can execute verbatim.
    ///
    /// The ordering is meaningful: the first entry is the one most likely to
    /// resolve the failure. `<db>` is a placeholder the CLI fills in. An empty
    /// list means there is no mechanical recovery — read the message.
    pub fn next_actions(&self) -> Vec<NextAction> {
        match self {
            Error::TableNotFound { name, did_you_mean } => {
                let mut actions = Vec::new();
                if let Some(guess) = did_you_mean {
                    actions.push(NextAction::new(
                        format!("h5i-db schema <db> {guess}"),
                        format!("{name:?} does not exist; {guess:?} is the closest name"),
                    ));
                }
                actions.push(NextAction::new(
                    "h5i-db tables <db>",
                    "list the tables that do exist",
                ));
                actions
            }
            Error::SnapshotNotFound { name, did_you_mean } => {
                let mut actions = Vec::new();
                if let Some(guess) = did_you_mean {
                    actions.push(NextAction::new(
                        format!("h5i-db snapshot list <db> | grep {guess}"),
                        format!("{name:?} does not exist; {guess:?} is the closest name"),
                    ));
                }
                actions.push(NextAction::new(
                    "h5i-db snapshot list <db>",
                    "list the snapshots that do exist",
                ));
                actions
            }
            // The canonical agent mistake: appending data that overlaps rows
            // already in the table. Both escapes are real commands.
            Error::SortOrderViolation { table, .. } => vec![
                NextAction::new(
                    format!(
                        "h5i-db replace-range <db> {table} --input <file> \
                         --start <ts> --end <ts> --plan"
                    ),
                    "overwrite just the affected time range, previewed before it lands",
                ),
                NextAction::new(
                    format!("h5i-db ingest <db> {table} <file> --mode write"),
                    "replace the table's whole contents when the input is a full restatement",
                ),
                NextAction::new(
                    format!("h5i-db tables <db> | grep {table}"),
                    "check the table's current max timestamp before re-appending",
                ),
            ],
            Error::VersionConflict { table, .. } => vec![NextAction::new(
                format!("h5i-db versions <db> {table}"),
                "another writer committed first; re-read the head before retrying",
            )],
            Error::ForkNotFound { name, did_you_mean } => {
                let mut actions = Vec::new();
                if let Some(guess) = did_you_mean {
                    actions.push(NextAction::new(
                        format!("h5i-db fork show <db> {guess}"),
                        format!("{name:?} does not exist; {guess:?} is the closest name"),
                    ));
                }
                actions.push(NextAction::new(
                    "h5i-db fork list <db>",
                    "list the forks that do exist",
                ));
                actions
            }
            // Ordered by what an agent should actually do: look, then discard.
            // Discarding is the common case — most speculative branches lose.
            Error::PromoteConflict { fork, table, .. } => vec![
                NextAction::new(
                    format!("h5i-db fork diff <db> {fork} --table {table}"),
                    "see what this fork changed before deciding whether to redo it",
                ),
                NextAction::new(
                    format!("h5i-db versions <db> {table}"),
                    "see what landed on main since the fork was created",
                ),
                NextAction::new(
                    format!("h5i-db fork drop <db> {fork}"),
                    "discard the fork; its base is stale and its result cannot be promoted",
                ),
            ],
            Error::VersionNotFound { table, .. } => vec![NextAction::new(
                format!("h5i-db versions <db> {table}"),
                "list the versions that are still retained",
            )],
            Error::PolicyViolation { op } => vec![
                NextAction::new(
                    format!("h5i-db {op} <db> <table> … --plan"),
                    "policy requires a reviewed plan; prepare one instead of committing directly",
                ),
                NextAction::new(
                    "h5i-db policy show <db>",
                    "see which operations this database allows without a plan",
                ),
            ],
            Error::DataPolicyViolation { .. } => vec![NextAction::new(
                "h5i-db data-policy get <db> <table>",
                "read the constraint the rejected rows broke",
            )],
            Error::SchemaMismatch { .. } => vec![NextAction::new(
                "h5i-db schema <db> <table>",
                "compare the input's columns and types against the table's",
            )],
            Error::ReadOnly { .. } => vec![NextAction::new(
                "h5i-db policy show <db>",
                "this handle is read-only; re-open for writing to mutate",
            )],
            Error::LockTimeout { table, .. } => vec![NextAction::new(
                format!("h5i-db versions <db> {table}"),
                "another writer holds the lock; check whether it committed, then retry",
            )],
            Error::Corruption { .. } => vec![NextAction::new(
                "h5i-db verify <db> <table> --deep",
                "locate every damaged object before touching the database further",
            )],
            _ => Vec::new(),
        }
    }

    pub fn exit_category(&self) -> ExitCategory {
        match self {
            Error::VersionConflict { .. }
            | Error::LockTimeout { .. }
            | Error::PromoteConflict { .. } => ExitCategory::Conflict,
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

    /// A missing table, with no name suggestion (the caller has no catalog to
    /// compare against). Prefer [`Self::table_not_found_among`] where the list
    /// of existing tables is already at hand.
    pub fn table_not_found(name: impl Into<String>) -> Self {
        Error::TableNotFound {
            name: name.into(),
            did_you_mean: None,
        }
    }

    /// A missing table, suggesting the closest existing name when the lookup
    /// looks like a typo.
    pub fn table_not_found_among<'a>(
        name: impl Into<String>,
        existing: impl IntoIterator<Item = &'a str>,
    ) -> Self {
        let name = name.into();
        let did_you_mean = nearest_name(&name, existing);
        Error::TableNotFound { name, did_you_mean }
    }

    /// A missing snapshot, suggesting the closest existing name.
    pub fn snapshot_not_found_among<'a>(
        name: impl Into<String>,
        existing: impl IntoIterator<Item = &'a str>,
    ) -> Self {
        let name = name.into();
        let did_you_mean = nearest_name(&name, existing);
        Error::SnapshotNotFound { name, did_you_mean }
    }

    /// A missing fork, suggesting the closest existing name.
    pub fn fork_not_found_among<'a>(
        name: impl Into<String>,
        existing: impl IntoIterator<Item = &'a str>,
    ) -> Self {
        let name = name.into();
        let did_you_mean = nearest_name(&name, existing);
        Error::ForkNotFound { name, did_you_mean }
    }

    pub fn io(path: impl fmt::Display, source: std::io::Error) -> Self {
        Error::Io {
            path: path.to_string(),
            source,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One representative value of every `Error` variant. Kept exhaustive so
    /// that adding a variant without extending the classification tables here
    /// makes the coverage assertions below fail loudly.
    fn all_variants() -> Vec<Error> {
        vec![
            Error::DatabaseNotFound { path: "/db".into() },
            Error::DatabaseExists { path: "/db".into() },
            Error::table_not_found("t"),
            Error::TableExists { name: "t".into() },
            Error::VersionNotFound {
                table: "t".into(),
                requested: "9".into(),
                hint: "max is 3".into(),
            },
            Error::SnapshotNotFound {
                name: "s".into(),
                did_you_mean: Some("snap".into()),
            },
            Error::VersionConflict {
                table: "t".into(),
                expected: 1,
                actual: 2,
            },
            Error::ForkNotFound {
                name: "f".into(),
                did_you_mean: Some("fork".into()),
            },
            Error::ForkExists { name: "f".into() },
            Error::PromoteConflict {
                fork: "agent-01".into(),
                table: "t".into(),
                base: 3,
                actual: 5,
                compaction_note: String::new(),
            },
            Error::SchemaMismatch { detail: "d".into() },
            Error::SortOrderViolation {
                table: "t".into(),
                detail: "d".into(),
            },
            Error::InvalidInput { detail: "d".into() },
            Error::Unsupported { detail: "d".into() },
            Error::ReadOnly {
                op: "append".into(),
            },
            Error::PolicyViolation { op: "write".into() },
            Error::DataPolicyViolation {
                constraint: "c".into(),
                detail: "d".into(),
            },
            Error::Corruption {
                object: "o".into(),
                detail: "d".into(),
            },
            Error::FormatTooNew {
                found: 2,
                supported: 1,
            },
            Error::LimitExceeded { detail: "d".into() },
            Error::Timeout { seconds: 5 },
            Error::LockTimeout {
                table: "t".into(),
                waited_ms: 100,
            },
            Error::ObjectStore(object_store::Error::NotFound {
                path: "p".into(),
                source: "x".into(),
            }),
            Error::io("/p", std::io::Error::other("boom")),
            Error::Arrow(arrow::error::ArrowError::ComputeError("c".into())),
            Error::Parquet(parquet::errors::ParquetError::General("g".into())),
            Error::Serde(serde_json::from_str::<i32>("nope").unwrap_err()),
            Error::Internal { detail: "d".into() },
        ]
    }

    #[test]
    fn every_variant_has_a_stable_nonempty_code() {
        for e in all_variants() {
            let code = e.code();
            assert!(!code.is_empty(), "empty code for {e:?}");
            assert_eq!(code, code.trim(), "code has whitespace for {e:?}");
        }
    }

    #[test]
    fn codes_are_unique_per_variant() {
        let variants = all_variants();
        let mut codes: Vec<&str> = variants.iter().map(|e| e.code()).collect();
        codes.sort_unstable();
        let unique = codes.len();
        codes.dedup();
        assert_eq!(
            codes.len(),
            unique,
            "two Error variants share the same code string"
        );
    }

    #[test]
    fn retryable_classification_matches_contract() {
        // Only transient / racy conditions are retryable.
        assert!(
            Error::VersionConflict {
                table: "t".into(),
                expected: 1,
                actual: 2,
            }
            .retryable()
        );
        assert!(
            Error::LockTimeout {
                table: "t".into(),
                waited_ms: 1,
            }
            .retryable()
        );
        assert!(Error::Timeout { seconds: 1 }.retryable());
        assert!(Error::io("/p", std::io::Error::other("x")).retryable());

        // Deterministic user/logic errors are not.
        assert!(!Error::invalid("x").retryable());
        assert!(!Error::SchemaMismatch { detail: "x".into() }.retryable());
        assert!(!Error::corruption("o", "d").retryable());
        assert!(!Error::table_not_found("t").retryable());
    }

    #[test]
    fn exit_category_maps_conflict_limit_and_internal() {
        assert_eq!(
            Error::VersionConflict {
                table: "t".into(),
                expected: 1,
                actual: 2,
            }
            .exit_category(),
            ExitCategory::Conflict
        );
        assert_eq!(
            Error::LockTimeout {
                table: "t".into(),
                waited_ms: 1,
            }
            .exit_category(),
            ExitCategory::Conflict
        );
        assert_eq!(
            Error::LimitExceeded { detail: "d".into() }.exit_category(),
            ExitCategory::Limit
        );
        assert_eq!(
            Error::Timeout { seconds: 1 }.exit_category(),
            ExitCategory::Limit
        );
        assert_eq!(
            Error::corruption("o", "d").exit_category(),
            ExitCategory::Internal
        );
        assert_eq!(Error::internal("d").exit_category(), ExitCategory::Internal);
        // Plain user errors default to UserError.
        assert_eq!(Error::invalid("d").exit_category(), ExitCategory::UserError);
        assert_eq!(
            Error::table_not_found("t").exit_category(),
            ExitCategory::UserError
        );
    }

    #[test]
    fn hints_present_only_for_actionable_variants() {
        // These variants must carry an actionable hint.
        assert!(
            Error::VersionConflict {
                table: "t".into(),
                expected: 1,
                actual: 2,
            }
            .hint()
            .is_some()
        );
        assert!(Error::table_not_found("t").hint().is_some());
        assert!(
            Error::snapshot_not_found_among("s", ["snap"])
                .hint()
                .is_some()
        );
        assert!(
            Error::SortOrderViolation {
                table: "t".into(),
                detail: "d".into()
            }
            .hint()
            .is_some()
        );
        assert!(
            Error::FormatTooNew {
                found: 2,
                supported: 1,
            }
            .hint()
            .is_some()
        );
        assert!(
            Error::ReadOnly {
                op: "append".into()
            }
            .hint()
            .is_some()
        );
        let ph = Error::PolicyViolation { op: "write".into() }
            .hint()
            .unwrap();
        assert!(ph.contains("write"), "policy hint should name the op: {ph}");

        // A generic internal error offers no hint.
        assert!(Error::internal("boom").hint().is_none());
        assert!(
            Error::SchemaMismatch { detail: "d".into() }
                .hint()
                .is_none()
        );
    }

    #[test]
    fn next_actions_are_runnable_and_ordered_best_first() {
        // The canonical agent mistake must offer a real escape, not prose.
        let e = Error::SortOrderViolation {
            table: "trades".into(),
            detail: "overlaps existing rows".into(),
        };
        let actions = e.next_actions();
        assert!(
            !actions.is_empty(),
            "out-of-order append needs a next action"
        );
        assert!(
            actions[0]
                .cmd
                .starts_with("h5i-db replace-range <db> trades"),
            "the previewable range overwrite should be suggested first: {:?}",
            actions[0]
        );
        for a in &actions {
            assert!(
                a.cmd.starts_with("h5i-db "),
                "not a runnable command: {a:?}"
            );
            assert!(!a.why.is_empty(), "action without a reason: {a:?}");
        }

        // Every action names the placeholder the CLI substitutes, never a
        // literal path, so the same string is correct for any database.
        for e in all_variants() {
            for a in e.next_actions() {
                assert!(
                    !a.cmd.contains("/db"),
                    "action must not bake in a path: {a:?}"
                );
            }
        }
    }

    #[test]
    fn retryable_conflicts_tell_the_caller_where_to_re_read() {
        let actions = Error::VersionConflict {
            table: "trades".into(),
            expected: 1,
            actual: 2,
        }
        .next_actions();
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].cmd, "h5i-db versions <db> trades");
    }

    #[test]
    fn did_you_mean_suggests_near_names_and_stays_silent_otherwise() {
        let tables = ["trades", "quotes"];
        // A plausible typo gets a suggestion...
        let e = Error::table_not_found_among("trade", tables);
        assert_eq!(e.did_you_mean(), Some("trades"));
        assert!(
            e.next_actions()[0].cmd.contains("trades"),
            "the suggestion should lead the actions: {:?}",
            e.next_actions()
        );
        // ...including a pure case difference.
        assert_eq!(
            Error::table_not_found_among("TRADES", tables).did_you_mean(),
            Some("trades")
        );
        // An unrelated word does not. A confident wrong guess is worse than
        // none, because an agent will act on it.
        assert_eq!(
            Error::table_not_found_among("positions", tables).did_you_mean(),
            None
        );
        // Neither does an empty catalog.
        assert_eq!(
            Error::table_not_found_among("trades", []).did_you_mean(),
            None
        );
        // A table that is merely missing carries no guess.
        assert_eq!(Error::table_not_found("trades").did_you_mean(), None);
    }

    #[test]
    fn nearest_name_prefers_the_closest_then_the_shortest() {
        // Equal distance: the shorter candidate wins, deterministically.
        assert_eq!(
            nearest_name("tradex", ["trades", "tradesxy"]),
            Some("trades".to_string())
        );
        // Distance budget scales with length: one edit is always allowed.
        assert_eq!(nearest_name("ab", ["abc"]), Some("abc".to_string()));
        // But a long name cannot match something entirely different.
        assert_eq!(nearest_name("abcdefghij", ["zzzzzzzzzz"]), None);
    }

    #[test]
    fn constructor_helpers_populate_fields() {
        match Error::corruption("seg-1", "bad crc") {
            Error::Corruption { object, detail } => {
                assert_eq!(object, "seg-1");
                assert_eq!(detail, "bad crc");
            }
            other => panic!("expected Corruption, got {other:?}"),
        }
        // Display renders the fields.
        let msg = Error::VersionConflict {
            table: "trades".into(),
            expected: 4,
            actual: 5,
        }
        .to_string();
        assert!(msg.contains("trades") && msg.contains('4') && msg.contains('5'));
    }
}
