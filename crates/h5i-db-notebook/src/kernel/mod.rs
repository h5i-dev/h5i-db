//! Kernel abstraction: one trait, several backends.
//!
//! [`JupyterKernel`] drives any installed Jupyter kernelspec over ZeroMQ.
//! A native h5i-db SQL backend lands behind the same trait (roadmap N3), which
//! is why execution is expressed in terms of a mime-bundle stream rather than
//! anything Python-shaped.

pub mod jupyter;
pub mod spec;
pub mod sql;

pub use jupyter::JupyterKernel;
pub use spec::{KernelSpecInfo, discover, find};
pub use sql::SqlKernel;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

use crate::document::{MimeBundle, Output, StreamName, StreamOutput};
use crate::error::Result;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KernelStatus {
    /// Launched, handshake not finished.
    Starting,
    Idle,
    Busy,
    Restarting,
    /// The process exited, or never came up. Terminal until a restart.
    Dead,
}

impl KernelStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            KernelStatus::Starting => "starting",
            KernelStatus::Idle => "idle",
            KernelStatus::Busy => "busy",
            KernelStatus::Restarting => "restarting",
            KernelStatus::Dead => "dead",
        }
    }

    pub fn is_alive(self) -> bool {
        !matches!(self, KernelStatus::Dead)
    }
}

#[derive(Debug, Clone)]
pub struct ExecOptions {
    /// Wall-clock cap on the whole cell. `None` waits forever, which is only
    /// ever right for an interactive human.
    pub timeout: Option<Duration>,
    /// Suppress `execute_result` and history recording (`silent` in the spec).
    pub silent: bool,
    pub store_history: bool,
    /// Whether the kernel may ask for input.
    ///
    /// Off by default, which makes a cell calling `input()` raise
    /// `StdinNotImplementedError` at once instead of waiting on a human who is
    /// not there. Turning it on without an attached input provider gets an
    /// automatic empty reply: the cell finishes rather than wedging the
    /// session, but it reads `""`, not real input.
    pub allow_stdin: bool,
    pub stop_on_error: bool,
}

impl Default for ExecOptions {
    fn default() -> Self {
        ExecOptions {
            timeout: Some(Duration::from_secs(300)),
            silent: false,
            store_history: true,
            // Off by default: an agent driving this over a CLI has no terminal
            // to type into, and a silent hang is the worst possible failure.
            // The session layer answers `input_request` with an error instead.
            allow_stdin: false,
            stop_on_error: true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecStatus {
    Ok,
    Error,
    /// The kernel skipped the request, usually because an earlier cell in the
    /// same batch raised and `stop_on_error` was set.
    Aborted,
}

impl ExecStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            ExecStatus::Ok => "ok",
            ExecStatus::Error => "error",
            ExecStatus::Aborted => "aborted",
        }
    }
}

#[derive(Debug, Clone)]
pub struct ExecOutcome {
    pub status: ExecStatus,
    pub execution_count: Option<i64>,
    pub outputs: Vec<Output>,
    pub elapsed: Duration,
}

impl ExecOutcome {
    /// The error output, when the cell raised.
    pub fn error(&self) -> Option<&crate::document::ErrorOutput> {
        self.outputs.iter().find_map(|o| match o {
            Output::Error(e) => Some(e),
            _ => None,
        })
    }
}

/// Events streamed while a cell runs.
///
/// Delivered as they arrive rather than at cell end, so a TUI paints
/// incrementally and a cell that prints for ten minutes is observable while it
/// runs instead of only afterwards.
#[derive(Debug, Clone)]
pub enum OutputEvent {
    Output(Output),
    Update {
        display_id: String,
        data: MimeBundle,
    },
    Clear {
        /// Defer the clear until the next output arrives, so a progress
        /// display does not flicker to empty between frames.
        wait: bool,
    },
    ExecutionCount(i64),
    Status(KernelStatus),
    /// The kernel is blocked on `input()`.
    InputRequest {
        prompt: String,
        password: bool,
    },
}

/// Receives [`OutputEvent`]s during execution.
pub trait OutputSink: Send {
    fn emit(&mut self, event: OutputEvent);
}

/// A sink that discards everything, for callers that only want the outcome.
pub struct NullSink;

impl OutputSink for NullSink {
    fn emit(&mut self, _event: OutputEvent) {}
}

impl<F: FnMut(OutputEvent) + Send> OutputSink for F {
    fn emit(&mut self, event: OutputEvent) {
        self(event)
    }
}

/// Folds an [`OutputEvent`] stream into the `outputs` array of a code cell.
///
/// Two behaviours here are conventions every Jupyter client implements, and
/// notebooks look wrong without them: consecutive `stream` outputs on the same
/// stream are merged into one (otherwise every `print` becomes its own output),
/// and `update_display_data` rewrites the output a previous `display_data`
/// created rather than appending.
#[derive(Debug, Default)]
pub struct OutputCollector {
    outputs: Vec<Output>,
    /// Indices of outputs published under each `display_id`.
    display_ids: HashMap<String, Vec<usize>>,
    /// A `clear_output(wait=True)` that has not been applied yet.
    clear_pending: bool,
}

impl OutputCollector {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn apply(&mut self, event: &OutputEvent) {
        match event {
            OutputEvent::Output(output) => self.push(output.clone()),
            OutputEvent::Update { display_id, data } => self.update(display_id, data),
            OutputEvent::Clear { wait } => self.clear(*wait),
            _ => {}
        }
    }

    pub fn push(&mut self, output: Output) {
        if self.clear_pending {
            self.outputs.clear();
            self.display_ids.clear();
            self.clear_pending = false;
        }
        if let Output::Stream(incoming) = &output
            && let Some(Output::Stream(last)) = self.outputs.last_mut()
            && last.name == incoming.name
        {
            last.text.push_str(&incoming.text);
            return;
        }
        self.outputs.push(output);
    }

    /// Record which `display_id` the output just pushed belongs to.
    pub fn tag_display_id(&mut self, display_id: &str) {
        if self.outputs.is_empty() {
            return;
        }
        self.display_ids
            .entry(display_id.to_string())
            .or_default()
            .push(self.outputs.len() - 1);
    }

    fn update(&mut self, display_id: &str, data: &MimeBundle) {
        let Some(indices) = self.display_ids.get(display_id) else {
            return;
        };
        for &i in indices {
            match self.outputs.get_mut(i) {
                Some(Output::DisplayData(o)) => o.data = data.clone(),
                Some(Output::ExecuteResult(o)) => o.data = data.clone(),
                _ => {}
            }
        }
    }

    fn clear(&mut self, wait: bool) {
        if wait {
            self.clear_pending = true;
        } else {
            self.outputs.clear();
            self.display_ids.clear();
            self.clear_pending = false;
        }
    }

    pub fn outputs(&self) -> &[Output] {
        &self.outputs
    }

    pub fn into_outputs(self) -> Vec<Output> {
        self.outputs
    }

    pub fn push_stream(&mut self, name: StreamName, text: impl Into<String>) {
        self.push(Output::Stream(StreamOutput {
            name,
            text: text.into(),
            extra: Default::default(),
        }));
    }
}

/// Completion candidates from a `complete_request`.
#[derive(Debug, Clone, Default)]
pub struct Completions {
    pub matches: Vec<String>,
    /// Byte offsets in the submitted code that the matches replace.
    pub cursor_start: usize,
    pub cursor_end: usize,
    /// `_jupyter_types_experimental` when the kernel provides it, giving a
    /// type per match ("function", "instance", …).
    pub types: Vec<Option<String>>,
}

/// Result of `is_complete_request`, used by the TUI to decide whether Enter
/// submits the cell or inserts a newline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Completeness {
    Complete,
    Incomplete { indent: String },
    Invalid,
    Unknown,
}

/// A cheap, cloneable way to interrupt a cell that is already running.
///
/// [`Kernel::interrupt`] takes `&mut self`, so it cannot be called while
/// `execute` holds the borrow. That is exactly when a user presses Ctrl-C, so
/// the handle exists to break the deadlock: it only sets a flag and wakes the
/// running `execute`, which then performs whatever interruption its kernel
/// actually needs (a signal, a control message, dropping a query future).
#[derive(Clone, Default)]
pub struct InterruptHandle {
    requested: Arc<AtomicBool>,
    notify: Arc<Notify>,
}

impl InterruptHandle {
    pub fn new() -> Self {
        Self::default()
    }

    /// Request an interrupt. Safe to call from anywhere, including a UI event
    /// loop that holds no lock on the kernel.
    pub fn interrupt(&self) {
        self.requested.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    /// Wait until an interrupt is requested.
    ///
    /// Checks the flag first so a request that arrived before the wait began
    /// is not lost.
    pub async fn requested(&self) {
        if self.requested.load(Ordering::Acquire) {
            return;
        }
        self.notify.notified().await;
    }

    pub fn is_requested(&self) -> bool {
        self.requested.load(Ordering::Acquire)
    }

    /// Clear the flag, so the next cell starts uninterrupted.
    pub fn reset(&self) {
        self.requested.store(false, Ordering::Release);
    }
}

impl std::fmt::Debug for InterruptHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InterruptHandle")
            .field("requested", &self.is_requested())
            .finish()
    }
}

#[async_trait]
pub trait Kernel: Send {
    fn spec(&self) -> &KernelSpecInfo;
    fn status(&self) -> KernelStatus;

    /// A handle that can interrupt a running cell without borrowing the kernel.
    fn interrupt_handle(&self) -> InterruptHandle;

    /// Run `code` to completion, streaming events to `sink`.
    async fn execute(
        &mut self,
        code: &str,
        options: &ExecOptions,
        sink: &mut dyn OutputSink,
    ) -> Result<ExecOutcome>;

    async fn complete(&mut self, code: &str, cursor: usize) -> Result<Completions>;

    /// Introspection (`?` in IPython). Returns the mime bundle, or `None` when
    /// the kernel found nothing at the cursor.
    async fn inspect(
        &mut self,
        code: &str,
        cursor: usize,
        detail: u8,
    ) -> Result<Option<MimeBundle>>;

    async fn is_complete(&mut self, code: &str) -> Result<Completeness>;

    /// Interrupt the running cell. State is preserved.
    async fn interrupt(&mut self) -> Result<()>;

    /// Restart the process. **All kernel state is lost.**
    async fn restart(&mut self) -> Result<()>;

    async fn shutdown(&mut self) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::document::{DisplayDataOutput, ExecuteResultOutput};
    use serde_json::json;

    fn bundle(text: &str) -> MimeBundle {
        let mut b = MimeBundle::new();
        b.insert("text/plain", json!(text));
        b
    }

    fn display(text: &str) -> Output {
        Output::DisplayData(DisplayDataOutput {
            data: bundle(text),
            metadata: Default::default(),
            extra: Default::default(),
        })
    }

    #[test]
    fn consecutive_stream_output_is_merged() {
        let mut c = OutputCollector::new();
        c.push_stream(StreamName::Stdout, "a\n");
        c.push_stream(StreamName::Stdout, "b\n");
        c.push_stream(StreamName::Stderr, "warn\n");
        c.push_stream(StreamName::Stdout, "c\n");
        assert_eq!(c.outputs().len(), 3, "streams were not coalesced");
        match &c.outputs()[0] {
            Output::Stream(s) => assert_eq!(s.text, "a\nb\n"),
            other => panic!("{other:?}"),
        }
        match &c.outputs()[2] {
            Output::Stream(s) => assert_eq!(s.text, "c\n"),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn a_different_stream_breaks_the_run() {
        // stdout/stderr interleaving must not be flattened into one blob:
        // the ordering is what tells you which print raced which warning.
        let mut c = OutputCollector::new();
        c.push_stream(StreamName::Stderr, "e1");
        c.push_stream(StreamName::Stdout, "o1");
        c.push_stream(StreamName::Stderr, "e2");
        assert_eq!(c.outputs().len(), 3);
    }

    #[test]
    fn update_display_data_rewrites_in_place() {
        let mut c = OutputCollector::new();
        c.push(display("frame 1"));
        c.tag_display_id("d1");
        c.push_stream(StreamName::Stdout, "between");
        c.apply(&OutputEvent::Update {
            display_id: "d1".into(),
            data: bundle("frame 2"),
        });
        assert_eq!(
            c.outputs().len(),
            2,
            "the update should rewrite the existing display, not append a third output"
        );
        assert_eq!(
            c.outputs()[0].data().unwrap().text_plain(),
            Some("frame 2"),
            "the display was appended instead of updated"
        );
    }

    #[test]
    fn one_display_id_can_cover_several_outputs() {
        let mut c = OutputCollector::new();
        c.push(display("a"));
        c.tag_display_id("shared");
        c.push(display("b"));
        c.tag_display_id("shared");
        c.apply(&OutputEvent::Update {
            display_id: "shared".into(),
            data: bundle("new"),
        });
        for o in c.outputs() {
            assert_eq!(o.data().unwrap().text_plain(), Some("new"));
        }
    }

    #[test]
    fn updating_an_unknown_display_id_is_a_no_op() {
        let mut c = OutputCollector::new();
        c.push(display("a"));
        c.apply(&OutputEvent::Update {
            display_id: "never-seen".into(),
            data: bundle("x"),
        });
        assert_eq!(c.outputs().len(), 1);
        assert_eq!(c.outputs()[0].data().unwrap().text_plain(), Some("a"));
    }

    #[test]
    fn clear_output_without_wait_empties_immediately() {
        let mut c = OutputCollector::new();
        c.push_stream(StreamName::Stdout, "old");
        c.apply(&OutputEvent::Clear { wait: false });
        assert!(c.outputs().is_empty());
    }

    #[test]
    fn clear_output_with_wait_defers_until_the_next_output() {
        // What makes a progress bar redraw instead of flickering to blank.
        let mut c = OutputCollector::new();
        c.push_stream(StreamName::Stdout, "old");
        c.apply(&OutputEvent::Clear { wait: true });
        assert_eq!(c.outputs().len(), 1, "cleared too early");
        c.push_stream(StreamName::Stdout, "new");
        assert_eq!(c.outputs().len(), 1);
        match &c.outputs()[0] {
            Output::Stream(s) => assert_eq!(s.text, "new"),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn a_deferred_clear_drops_stale_display_ids() {
        let mut c = OutputCollector::new();
        c.push(display("old"));
        c.tag_display_id("d1");
        c.apply(&OutputEvent::Clear { wait: true });
        c.push_stream(StreamName::Stdout, "new");
        c.apply(&OutputEvent::Update {
            display_id: "d1".into(),
            data: bundle("zombie"),
        });
        assert_eq!(c.outputs().len(), 1);
        match &c.outputs()[0] {
            Output::Stream(s) => assert_eq!(s.text, "new"),
            other => panic!("cleared display came back: {other:?}"),
        }
    }

    #[test]
    fn execute_result_is_updatable_too() {
        let mut c = OutputCollector::new();
        c.push(Output::ExecuteResult(ExecuteResultOutput {
            data: bundle("v1"),
            metadata: Default::default(),
            execution_count: Some(1),
            extra: Default::default(),
        }));
        c.tag_display_id("d");
        c.apply(&OutputEvent::Update {
            display_id: "d".into(),
            data: bundle("v2"),
        });
        assert_eq!(c.outputs()[0].data().unwrap().text_plain(), Some("v2"));
    }
}
