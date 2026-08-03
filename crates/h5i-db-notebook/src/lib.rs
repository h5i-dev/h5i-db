//! In-terminal, Jupyter-compatible notebook for h5i-db.
//!
//! Design and rationale: `ROADMAP_NOTEBOOK.md` at the repo root.
//!
//! The crate is a notebook *client*, not a kernel and not a server. It owns
//! the document (nbformat v4), the kernel lifecycle, the rendering of outputs
//! for two very different audiences (a human at a TUI, and an agent reading a
//! token-budgeted digest), and the CLI those are driven through.

pub mod cli;
pub mod document;
pub mod error;
pub mod kernel;
pub mod magic;
pub mod render;
pub mod session;
pub mod supervisor;
pub mod tui;

pub use document::Notebook;
pub use error::{Error, ExitCategory, Result};
pub use session::Session;
