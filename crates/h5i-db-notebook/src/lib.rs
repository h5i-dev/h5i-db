//! In-terminal, Jupyter-compatible notebook for h5i-db.
//!
//! Design and rationale: `ROADMAP_NOTEBOOK.md` at the repo root.
//!
//! The crate is a notebook *client*, not a kernel and not a server. It owns
//! the document (nbformat v4), the kernel lifecycle, the rendering of outputs
//! for two very different audiences (a human at a TUI, and an agent reading a
//! token-budgeted digest), and the CLI those are driven through.
//!
//! # Unix only
//!
//! The crate compiles to nothing anywhere else, and the `nb` subcommand says
//! so rather than going missing. This is not an oversight waiting on a
//! `#[cfg]`: the session supervisor is built on Unix domain sockets, the
//! single-writer guarantee on `flock`, kernel reaping on POSIX signals, and
//! the socket's privacy on Unix file modes. Windows has an answer for each
//! (named pipes, `LockFileEx`, `TerminateProcess`, ACLs), but they are
//! different enough that pretending otherwise would mean a supervisor that
//! compiles and then loses notebooks.

#![cfg(unix)]

pub mod cli;
pub mod document;
pub mod error;
pub mod export;
pub mod kernel;
pub mod magic;
pub mod render;
pub mod session;
pub mod split;
pub mod supervisor;
pub mod tui;

pub use document::Notebook;
pub use error::{Error, ExitCategory, Result};
pub use session::Session;
