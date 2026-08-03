//! Error type for the notebook crate.
//!
//! Shape mirrors `h5i_db_core::Error` on purpose: `code()` / `retryable()` /
//! `hint()` / `exit_category()`, so `h5i-db nb …` renders the same
//! `{code, message, retryable, hint}` envelope on stderr and the same exit
//! codes as every other subcommand. An agent supervising the CLI should not
//! have to learn a second error contract because the subcommand happens to
//! talk to a kernel.

use std::fmt;

pub type Result<T> = std::result::Result<T, Error>;

/// Exit-code buckets, matching the CLI contract in `DESIGN.md` §8:
/// 0 ok / 2 user error / 3 conflict / 4 limit / 5 internal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitCategory {
    User,
    Conflict,
    Limit,
    Internal,
}

impl ExitCategory {
    pub fn code(self) -> i32 {
        match self {
            ExitCategory::User => 2,
            ExitCategory::Conflict => 3,
            ExitCategory::Limit => 4,
            ExitCategory::Internal => 5,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    // ---- document -------------------------------------------------------
    #[error("{path}: not valid notebook JSON: {detail}")]
    NotebookParse { path: String, detail: String },

    #[error(
        "{path}: nbformat {major}.{minor} is not supported (this build reads \
         nbformat 4.0 through 4.5)"
    )]
    UnsupportedNbformat {
        path: String,
        major: i64,
        minor: i64,
    },

    #[error("notebook has no cell at index {index} (it has {len} cells)")]
    CellIndexOutOfRange { index: usize, len: usize },

    #[error("no cell with id {id:?} in this notebook")]
    CellIdNotFound { id: String },

    #[error("cell {index} is a {actual} cell, not a code cell")]
    NotACodeCell { index: usize, actual: &'static str },

    #[error("cell {index} has no output at index {output}(it has {len})")]
    OutputIndexOutOfRange {
        index: usize,
        output: usize,
        len: usize,
    },

    #[error("invalid cell range {spec:?}: {detail}")]
    InvalidCellRange { spec: String, detail: String },

    // ---- kernel ---------------------------------------------------------
    #[error("no Jupyter kernel named {name:?} is installed{listed}")]
    KernelNotFound { name: String, listed: String },

    #[error("kernel {name:?} failed to start: {detail}")]
    KernelStartFailed { name: String, detail: String },

    #[error("kernel {name:?} did not respond within {timeout_secs}s of starting")]
    KernelStartTimeout { name: String, timeout_secs: u64 },

    #[error("the kernel is not running")]
    KernelNotRunning,

    #[error("the kernel died{detail}")]
    KernelDied { detail: String },

    #[error("kernel protocol error: {detail}")]
    Protocol { detail: String },

    #[error("cell execution exceeded the {timeout_secs}s timeout")]
    ExecuteTimeout { timeout_secs: u64 },

    #[error("cell execution was interrupted")]
    Interrupted,

    // ---- session --------------------------------------------------------
    #[error("no live session for {path}; run `h5i-db nb kernel start {path}` first")]
    NoSession { path: String },

    #[error("the session for {path} is already running a cell")]
    SessionBusy { path: String },

    // ---- generic --------------------------------------------------------
    #[error("{detail}")]
    InvalidInput { detail: String },

    #[error("{operation} exceeded the {limit} limit")]
    LimitExceeded { operation: String, limit: String },

    #[error("io error on {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },

    #[error("internal error: {detail}")]
    Internal { detail: String },
}

impl Error {
    /// Stable machine-readable code. Never reword these; agents match on them.
    pub fn code(&self) -> &'static str {
        match self {
            Error::NotebookParse { .. } => "notebook_parse",
            Error::UnsupportedNbformat { .. } => "unsupported_nbformat",
            Error::CellIndexOutOfRange { .. } => "cell_index_out_of_range",
            Error::CellIdNotFound { .. } => "cell_id_not_found",
            Error::NotACodeCell { .. } => "not_a_code_cell",
            Error::OutputIndexOutOfRange { .. } => "output_index_out_of_range",
            Error::InvalidCellRange { .. } => "invalid_cell_range",
            Error::KernelNotFound { .. } => "kernel_not_found",
            Error::KernelStartFailed { .. } => "kernel_start_failed",
            Error::KernelStartTimeout { .. } => "kernel_start_timeout",
            Error::KernelNotRunning => "kernel_not_running",
            Error::KernelDied { .. } => "kernel_died",
            Error::Protocol { .. } => "kernel_protocol",
            Error::ExecuteTimeout { .. } => "execute_timeout",
            Error::Interrupted => "interrupted",
            Error::NoSession { .. } => "no_session",
            Error::SessionBusy { .. } => "session_busy",
            Error::InvalidInput { .. } => "invalid_input",
            Error::LimitExceeded { .. } => "limit_exceeded",
            Error::Io { .. } => "io",
            Error::Internal { .. } => "internal",
        }
    }

    /// Whether re-running the same call can plausibly succeed.
    ///
    /// The rule is the same as core's: transient resource and liveness
    /// problems are retryable, anything that encodes a wrong request is not.
    /// A dead kernel is retryable because the session layer restarts it; a
    /// missing kernelspec is not, because no amount of retrying installs one.
    pub fn retryable(&self) -> bool {
        match self {
            Error::KernelDied { .. }
            | Error::KernelNotRunning
            | Error::KernelStartTimeout { .. }
            | Error::SessionBusy { .. }
            | Error::ExecuteTimeout { .. } => true,

            Error::Io { source, .. } => matches!(
                source.kind(),
                std::io::ErrorKind::Interrupted
                    | std::io::ErrorKind::WouldBlock
                    | std::io::ErrorKind::TimedOut
            ),

            Error::NotebookParse { .. }
            | Error::UnsupportedNbformat { .. }
            | Error::CellIndexOutOfRange { .. }
            | Error::CellIdNotFound { .. }
            | Error::NotACodeCell { .. }
            | Error::OutputIndexOutOfRange { .. }
            | Error::InvalidCellRange { .. }
            | Error::KernelNotFound { .. }
            | Error::KernelStartFailed { .. }
            | Error::Protocol { .. }
            | Error::Interrupted
            | Error::NoSession { .. }
            | Error::InvalidInput { .. }
            | Error::LimitExceeded { .. }
            | Error::Internal { .. } => false,
        }
    }

    /// Actionable next step, when there is exactly one obvious one.
    ///
    /// Only populated where the fix is unambiguous: a hint that guesses is
    /// worse than no hint, because the consumer replans from the text.
    pub fn hint(&self) -> Option<String> {
        match self {
            Error::KernelNotFound { .. } => Some(
                "run `h5i-db nb kernel list` to see installed kernels, or \
                 `python -m ipykernel install --user` to install one"
                    .into(),
            ),
            Error::KernelDied { .. } => {
                Some("run `h5i-db nb kernel restart <notebook>`; kernel state is lost".into())
            }
            Error::KernelNotRunning | Error::NoSession { .. } => {
                Some("run `h5i-db nb kernel start <notebook>`".into())
            }
            Error::ExecuteTimeout { .. } => Some(
                "raise --timeout, or run the cell with --detach and poll \
                 `h5i-db nb cells`"
                    .into(),
            ),
            Error::UnsupportedNbformat { .. } => {
                Some("open and re-save the file in Jupyter to upgrade it to nbformat 4".into())
            }
            Error::SessionBusy { .. } => {
                Some("wait for the running cell, or `h5i-db nb kernel interrupt <notebook>`".into())
            }
            _ => None,
        }
    }

    pub fn exit_category(&self) -> ExitCategory {
        match self {
            Error::SessionBusy { .. } => ExitCategory::Conflict,
            Error::LimitExceeded { .. } | Error::ExecuteTimeout { .. } => ExitCategory::Limit,
            Error::Internal { .. } | Error::Protocol { .. } => ExitCategory::Internal,
            Error::Io { .. } => ExitCategory::Internal,
            _ => ExitCategory::User,
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

    pub fn protocol(detail: impl fmt::Display) -> Self {
        Error::Protocol {
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Every variant, so the code-uniqueness test below actually covers the
    /// enum rather than whatever subset someone remembered to list.
    fn all_variants() -> Vec<Error> {
        vec![
            Error::NotebookParse {
                path: "a".into(),
                detail: "d".into(),
            },
            Error::UnsupportedNbformat {
                path: "a".into(),
                major: 3,
                minor: 0,
            },
            Error::CellIndexOutOfRange { index: 1, len: 0 },
            Error::CellIdNotFound { id: "x".into() },
            Error::NotACodeCell {
                index: 0,
                actual: "markdown",
            },
            Error::OutputIndexOutOfRange {
                index: 0,
                output: 2,
                len: 1,
            },
            Error::InvalidCellRange {
                spec: "3-".into(),
                detail: "d".into(),
            },
            Error::KernelNotFound {
                name: "python3".into(),
                listed: String::new(),
            },
            Error::KernelStartFailed {
                name: "python3".into(),
                detail: "d".into(),
            },
            Error::KernelStartTimeout {
                name: "python3".into(),
                timeout_secs: 30,
            },
            Error::KernelNotRunning,
            Error::KernelDied {
                detail: String::new(),
            },
            Error::Protocol { detail: "d".into() },
            Error::ExecuteTimeout { timeout_secs: 5 },
            Error::Interrupted,
            Error::NoSession { path: "a".into() },
            Error::SessionBusy { path: "a".into() },
            Error::InvalidInput { detail: "d".into() },
            Error::LimitExceeded {
                operation: "op".into(),
                limit: "1".into(),
            },
            Error::Io {
                path: "a".into(),
                source: std::io::Error::other("x"),
            },
            Error::Internal { detail: "d".into() },
        ]
    }

    #[test]
    fn codes_are_unique_per_variant() {
        let variants = all_variants();
        let mut codes: Vec<&str> = variants.iter().map(|e| e.code()).collect();
        let total = codes.len();
        codes.sort_unstable();
        codes.dedup();
        assert_eq!(codes.len(), total, "two variants share an error code");
    }

    #[test]
    fn messages_are_non_empty_and_lowercase_led() {
        for e in all_variants() {
            let msg = e.to_string();
            assert!(!msg.is_empty(), "{} has an empty message", e.code());
            let first = msg.chars().next().unwrap();
            assert!(
                !first.is_uppercase(),
                "{} message should not start capitalised: {msg}",
                e.code()
            );
        }
    }

    #[test]
    fn a_missing_kernelspec_is_not_retryable_but_a_dead_kernel_is() {
        // The distinction a supervising agent acts on: one of these means
        // "install something", the other means "call me again".
        assert!(
            !Error::KernelNotFound {
                name: "python3".into(),
                listed: String::new()
            }
            .retryable()
        );
        assert!(
            Error::KernelDied {
                detail: String::new()
            }
            .retryable()
        );
    }

    #[test]
    fn exit_categories_match_the_cli_contract() {
        assert_eq!(
            Error::SessionBusy { path: "a".into() }
                .exit_category()
                .code(),
            3
        );
        assert_eq!(
            Error::ExecuteTimeout { timeout_secs: 1 }
                .exit_category()
                .code(),
            4
        );
        assert_eq!(Error::internal("x").exit_category().code(), 5);
        assert_eq!(Error::invalid("x").exit_category().code(), 2);
    }
}
