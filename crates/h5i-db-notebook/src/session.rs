//! A notebook bound to a kernel.
//!
//! This is the layer that turns "run some code" into "run cell 4 and record
//! what happened in the file". The TUI drives it in-process; the CLI drives it
//! through the supervisor. Both get the same semantics because both go through
//! here.
//!
//! The invariant worth stating: **a cell's outputs are saved even when the cell
//! fails**. A traceback is the most valuable thing in a failed run, and a tool
//! that discards it on error forces the user to re-run to find out what broke.

use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::document::{Cell, Notebook, Output};
use crate::error::{Error, Result};
use crate::kernel::jupyter::StartOptions;
use crate::kernel::sql::{SqlKernel, SqlOptions};
use crate::kernel::{
    ExecOptions, ExecStatus, InterruptHandle, JupyterKernel, Kernel, KernelStatus, OutputCollector,
    OutputEvent, OutputSink, spec,
};
use crate::magic::{CellRoute, SqlMagic, into_variable_code, route};
use crate::supervisor::protocol::{EditRequest, NewCellKind};

/// What happened to one cell in a run.
#[derive(Debug, Clone)]
pub struct CellResult {
    pub index: usize,
    pub cell_id: Option<String>,
    pub status: ExecStatus,
    pub execution_count: Option<i64>,
    pub outputs: Vec<Output>,
    pub elapsed: Duration,
}

impl CellResult {
    pub fn is_error(&self) -> bool {
        self.status == ExecStatus::Error
    }
}

pub struct Session {
    path: PathBuf,
    notebook: Notebook,
    kernel: Option<JupyterKernel>,
    /// The native SQL backend, opened lazily on the first `%%sql` cell so a
    /// pure-Python notebook never pays to open a database.
    sql: Option<SqlKernel>,
    sql_options: SqlOptions,
    /// Database `%%sql` uses when a cell does not name one. Read from the
    /// notebook's own metadata, so it travels with the file.
    database: Option<PathBuf>,
    exec_options: ExecOptions,
    start_options: StartOptions,
    /// Shared with every kernel this session starts, so a UI can interrupt a
    /// running cell without holding a borrow on the session.
    interrupt: InterruptHandle,
    /// Temp files backing `%%sql --into` handoffs, deleted with the session.
    handoffs: Vec<tempfile::TempPath>,
    /// Set when the in-memory notebook differs from the file.
    dirty: bool,
}

impl Session {
    /// Open a notebook without starting a kernel.
    ///
    /// Kernel startup costs a Python interpreter, so it is deferred until a
    /// cell actually runs: `nb cells` and `nb output` never pay for it.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let mut notebook = Notebook::read(&path)?;
        notebook.normalize();
        let database = notebook_database(&notebook, &path);
        Ok(Session {
            // The kernel's working directory is the notebook's directory, so
            // relative paths in cells mean what the author expects.
            start_options: StartOptions {
                cwd: path
                    .parent()
                    .filter(|p| !p.as_os_str().is_empty())
                    .map(Path::to_path_buf),
                ..Default::default()
            },
            path,
            notebook,
            kernel: None,
            sql: None,
            sql_options: SqlOptions::default(),
            database,
            exec_options: ExecOptions::default(),
            interrupt: InterruptHandle::new(),
            handoffs: Vec::new(),
            dirty: false,
        })
    }

    /// Create a notebook on disk and open it.
    pub fn create(path: impl AsRef<Path>, kernel_name: &str) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if path.exists() {
            return Err(Error::invalid(format!(
                "{} already exists; pass a different path or edit it in place",
                path.display()
            )));
        }
        let spec = spec::find(kernel_name)?;
        let notebook = Notebook::new(&spec.name, &spec.display_name, &spec.language);
        notebook.write(&path)?;
        Session::open(path)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn notebook(&self) -> &Notebook {
        &self.notebook
    }

    pub fn notebook_mut(&mut self) -> &mut Notebook {
        self.dirty = true;
        &mut self.notebook
    }

    pub fn exec_options_mut(&mut self) -> &mut ExecOptions {
        &mut self.exec_options
    }

    pub fn start_options_mut(&mut self) -> &mut StartOptions {
        &mut self.start_options
    }

    /// A handle that interrupts whichever kernel is running a cell.
    pub fn interrupt_handle(&self) -> InterruptHandle {
        self.interrupt.clone()
    }

    pub fn kernel_status(&self) -> KernelStatus {
        match &self.kernel {
            Some(kernel) => kernel.status(),
            None => KernelStatus::Dead,
        }
    }

    pub fn kernel_name(&self) -> String {
        self.notebook.kernel_name().unwrap_or("python3").to_string()
    }

    /// Start the kernel if it is not already running.
    pub async fn ensure_kernel(&mut self) -> Result<()> {
        if self.kernel.as_ref().is_some_and(|k| k.status().is_alive()) {
            return Ok(());
        }
        let spec = spec::find(&self.kernel_name())?;
        let mut kernel = JupyterKernel::start(spec, self.start_options.clone()).await?;
        kernel.set_interrupt_handle(self.interrupt.clone());
        self.kernel = Some(kernel);
        Ok(())
    }

    /// The live kernel, for the request/reply calls that do not run cells
    /// (completion, inspection). Errors when no kernel has been started.
    pub fn kernel_mut(&mut self) -> Result<&mut JupyterKernel> {
        self.kernel.as_mut().ok_or(Error::KernelNotRunning)
    }

    /// Append a code cell and run it.
    ///
    /// The append happens before execution so that a cell which never returns
    /// is still visible in the file, with its source, for whoever has to work
    /// out what the session is stuck on.
    pub async fn exec(&mut self, code: &str, sink: &mut dyn OutputSink) -> Result<CellResult> {
        let index = self.append(code)?;
        self.run_cell(index, sink).await
    }

    /// Append a code cell and save, without running it.
    ///
    /// Split out of [`exec`](Self::exec) so a detached run can report the
    /// cell's index to its caller before the cell has finished, which is the
    /// only thing that makes a detached run pollable.
    pub fn append(&mut self, code: &str) -> Result<usize> {
        let index = self.notebook.push(Cell::new_code(code));
        self.dirty = true;
        self.save()?;
        Ok(index)
    }

    /// Run the cell at `index`, recording its outputs into the notebook.
    pub async fn run_cell(
        &mut self,
        index: usize,
        sink: &mut dyn OutputSink,
    ) -> Result<CellResult> {
        let cell = self.notebook.get(index)?;
        if cell.as_code().is_none() {
            return Err(Error::NotACodeCell {
                index,
                actual: match cell.cell_type() {
                    crate::document::CellType::Markdown => "markdown",
                    crate::document::CellType::Raw => "raw",
                    _ => "non-code",
                },
            });
        }
        let source = cell.source().to_string();
        let cell_id = cell.id().map(str::to_string);
        let (destination, body) = route(&source)?;
        let code = body.to_string();
        let options = self.exec_options.clone();

        // Mirror every event into a collector as well as the caller's sink, so
        // that a failure part-way through still leaves us the partial output to
        // write back.
        let mut collector = OutputCollector::new();
        let mut execution_count = None;
        let mut outcome = match &destination {
            CellRoute::Kernel => {
                self.ensure_kernel().await?;
                let kernel = self.kernel_mut()?;
                let mut tee = Tee {
                    collector: &mut collector,
                    execution_count: &mut execution_count,
                    inner: sink,
                };
                kernel.execute(&code, &options, &mut tee).await
            }
            CellRoute::Sql(magic) => {
                self.ensure_sql(magic).await?;
                let kernel = self.sql.as_mut().ok_or(Error::KernelNotRunning)?;
                let mut tee = Tee {
                    collector: &mut collector,
                    execution_count: &mut execution_count,
                    inner: sink,
                };
                kernel.execute(&code, &options, &mut tee).await
            }
        };

        // `--into` runs a second, generated cell against the Python kernel.
        // It is executed only after a successful query, and its own output is
        // appended to the SQL cell's, so the notebook shows one cell that both
        // queried and bound the variable.
        if let (CellRoute::Sql(magic), Ok(result)) = (&destination, &outcome)
            && let Some(name) = magic.into.clone()
            && result.status == ExecStatus::Ok
            && let Err(error) = self
                .hand_off_to_python(&code, &name, &mut collector, sink)
                .await
        {
            collector.push(Output::Error(crate::document::ErrorOutput {
                ename: "HandoffError".to_string(),
                evalue: error.to_string(),
                traceback: vec![format!("HandoffError: {error}")],
                extra: Default::default(),
            }));
            outcome = Ok(crate::kernel::ExecOutcome {
                status: ExecStatus::Error,
                execution_count,
                outputs: Vec::new(),
                elapsed: Duration::ZERO,
            });
        }

        // Outputs always come from the tee collector rather than the kernel's
        // own list: a `%%sql --into` cell runs two kernels, and only the
        // collector saw both halves. A timeout or a dead kernel still produced
        // output worth keeping, and the collector has that too.
        let outputs = collector.into_outputs();
        let (status, elapsed) = match &outcome {
            Ok(o) => (o.status, o.elapsed),
            Err(_) => (ExecStatus::Error, Duration::ZERO),
        };

        // The notebook, not the kernel, numbers executions. Two kernels share
        // one file, so leaving each to its own counter produces a notebook
        // where a SQL cell reads [5] beside a Python cell that reads [2].
        let count = Some(self.notebook.max_execution_count() + 1);
        let _ = execution_count;

        if let Some(code_cell) = self.notebook.get_mut(index)?.as_code_mut() {
            code_cell.outputs = outputs.clone();
            code_cell.execution_count = count;
        }
        self.dirty = true;
        self.save()?;

        // Report the execution failure only after the notebook is consistent.
        outcome?;

        Ok(CellResult {
            index,
            cell_id,
            status,
            execution_count: count,
            outputs,
            elapsed,
        })
    }

    /// Open (or reopen) the SQL backend for a cell's requirements.
    ///
    /// A cell naming a different database or fork than the one currently open
    /// gets a fresh backend: silently querying the wrong database because an
    /// earlier cell opened it first would be the worst possible failure here.
    async fn ensure_sql(&mut self, magic: &SqlMagic) -> Result<()> {
        let wanted = match &magic.database {
            Some(path) => self.resolve_database(path),
            None => self.database.clone().ok_or_else(|| {
                Error::invalid(
                    "this %%sql cell has no database: pass `%%sql --db <path>`, or set one \
                     for the whole notebook with `h5i-nb new … --db <path>`",
                )
            })?,
        };

        // A cell that asks to write cannot be served by a read-only handle, so
        // the open mode is part of what identifies the backend, exactly like
        // the database and the fork.
        let read_only = !magic.write;
        let matches = self.sql.as_ref().is_some_and(|k| {
            k.database_path() == wanted
                && k.fork() == magic.fork.as_deref()
                && k.is_read_only() == read_only
        });
        if !matches {
            let mut options = self.sql_options.clone();
            options.read_only = read_only;
            if let Some(max_rows) = magic.max_rows {
                options.max_rows = max_rows;
            }
            let mut kernel = SqlKernel::open(&wanted, magic.fork.clone(), options).await?;
            kernel.set_interrupt_handle(self.interrupt.clone());
            self.sql = Some(kernel);
        } else if let Some(max_rows) = magic.max_rows
            && let Some(kernel) = self.sql.as_mut()
        {
            kernel.set_max_rows(max_rows);
        }

        // Keep SQL numbering continuous with whatever the notebook records, so
        // two kernels sharing one file do not both count from [1].
        let next = self.notebook.max_execution_count();
        if let Some(kernel) = self.sql.as_mut() {
            kernel.set_execution_count(next);
        }
        Ok(())
    }

    /// Relative database paths resolve against the notebook, not the process.
    fn resolve_database(&self, path: &str) -> PathBuf {
        let candidate = PathBuf::from(path);
        if candidate.is_absolute() {
            return candidate;
        }
        match self.path.parent().filter(|p| !p.as_os_str().is_empty()) {
            Some(dir) => dir.join(candidate),
            None => candidate,
        }
    }

    /// Re-run the query, write it as Arrow IPC, and bind it in the Python
    /// kernel.
    ///
    /// The query runs twice rather than the result being kept from the first
    /// execution, because the rendered result is capped at `max_rows` while a
    /// handoff should bind what the query actually returns. Paying for a second
    /// scan is the honest trade against silently handing over a truncated
    /// frame.
    async fn hand_off_to_python(
        &mut self,
        sql: &str,
        name: &str,
        collector: &mut OutputCollector,
        sink: &mut dyn OutputSink,
    ) -> Result<()> {
        let kernel = self.sql.as_ref().ok_or(Error::KernelNotRunning)?;
        let file = tempfile::Builder::new()
            .prefix("h5i-nb-handoff-")
            .suffix(".arrow")
            .tempfile()
            .map_err(|e| Error::io("temp file", e))?;
        let path = file.into_temp_path();
        kernel.write_ipc(sql, &path).await?;
        let code = into_variable_code(name, &path.to_string_lossy());
        // Held so the file outlives the kernel's read but still goes away with
        // the session.
        self.handoffs.push(path);

        self.ensure_kernel().await?;
        let options = self.exec_options.clone();
        let kernel = self.kernel_mut()?;
        let mut execution_count = None;
        let mut tee = Tee {
            collector,
            execution_count: &mut execution_count,
            inner: sink,
        };
        let outcome = kernel.execute(&code, &options, &mut tee).await?;
        if outcome.status != ExecStatus::Ok {
            return Err(Error::invalid(format!(
                "binding the result to `{name}` failed in the Python kernel; \
                 pyarrow must be importable there"
            )));
        }
        Ok(())
    }

    /// Run several cells in order, stopping at the first error.
    ///
    /// Stopping is the right default: later cells almost always depend on
    /// earlier ones, and running them against half-initialised state produces
    /// a cascade of misleading tracebacks.
    pub async fn run_cells(
        &mut self,
        indices: &[usize],
        stop_on_error: bool,
        sink: &mut dyn OutputSink,
    ) -> Result<Vec<CellResult>> {
        let mut results = Vec::new();
        for &index in indices {
            // Skip non-code cells silently: a range like `0-9` over a notebook
            // with prose in it should not be an error.
            if self.notebook.get(index)?.as_code().is_none() {
                continue;
            }
            let result = self.run_cell(index, sink).await?;
            let failed = result.is_error();
            results.push(result);
            if failed && stop_on_error {
                break;
            }
        }
        Ok(results)
    }

    /// Every code cell, in order.
    pub fn code_cell_indices(&self) -> Vec<usize> {
        self.notebook
            .cells
            .iter()
            .enumerate()
            .filter(|(_, c)| c.as_code().is_some())
            .map(|(i, _)| i)
            .collect()
    }

    /// Restart the kernel, optionally clearing every recorded output.
    ///
    /// Clearing is what makes "restart and run all" a real reproducibility
    /// check: outputs left over from the previous session would otherwise be
    /// indistinguishable from ones the fresh run produced.
    pub async fn restart(&mut self, clear_outputs: bool) -> Result<()> {
        match self.kernel.as_mut() {
            Some(kernel) if kernel.status().is_alive() => kernel.restart().await?,
            _ => {
                self.kernel = None;
                self.ensure_kernel().await?;
            }
        }
        if clear_outputs {
            self.notebook.clear_all_outputs();
            self.dirty = true;
            self.save()?;
        }
        Ok(())
    }

    /// Record a failure into the cell's own outputs.
    ///
    /// For a detached run there is no caller left to return an error to, so a
    /// failure that happens before any output exists (a kernel that will not
    /// start, a database that will not open, a bad `%%sql` option) would leave
    /// the cell looking exactly like one that has not run yet. Writing it
    /// where the outputs go puts it where whoever polls the notebook is
    /// already looking.
    pub fn record_cell_error(&mut self, index: usize, error: &Error) {
        let output = Output::Error(crate::document::ErrorOutput {
            ename: error.code().to_string(),
            evalue: error.to_string(),
            traceback: match error.hint() {
                Some(hint) => vec![format!("{error}"), format!("hint: {hint}")],
                None => vec![format!("{error}")],
            },
            extra: Default::default(),
        });
        if let Ok(cell) = self.notebook.get_mut(index)
            && let Some(code) = cell.as_code_mut()
        {
            code.outputs.push(output);
        }
        self.dirty = true;
        let _ = self.save();
    }

    /// Apply a cell edit and save, returning the description to report.
    ///
    /// Edits have to reach the session rather than the file whenever one is
    /// running: the session rewrites the whole notebook after every cell, so
    /// an edit written straight to disk behind its back would be silently
    /// reverted by the next run.
    pub fn apply_edit(&mut self, action: &EditRequest) -> Result<String> {
        let message = apply_edit(&mut self.notebook, action)?;
        self.dirty = true;
        self.save()?;
        Ok(message)
    }

    pub async fn interrupt(&mut self) -> Result<()> {
        self.kernel_mut()?.interrupt().await
    }

    pub async fn shutdown(&mut self) -> Result<()> {
        if let Some(kernel) = self.kernel.as_mut() {
            kernel.shutdown().await?;
        }
        self.kernel = None;
        Ok(())
    }

    pub fn save(&mut self) -> Result<()> {
        if !self.dirty {
            return Ok(());
        }
        self.notebook.write(&self.path)?;
        self.dirty = false;
        Ok(())
    }

    pub fn is_dirty(&self) -> bool {
        self.dirty
    }
}

/// Apply one edit to a notebook, returning the description to report.
///
/// Lives here rather than in the CLI because both the supervisor and the
/// direct-to-file path apply the same edits and must word them identically.
pub fn apply_edit(notebook: &mut Notebook, action: &EditRequest) -> Result<String> {
    let message = match action {
        EditRequest::Set { cell, source } => {
            let index = notebook.resolve(cell)?;
            notebook.get_mut(index)?.set_source(source.clone());
            // Outputs describe code that no longer exists.
            notebook.get_mut(index)?.clear_outputs();
            format!("updated cell {index}")
        }
        EditRequest::Insert { at, kind, source } => {
            let cell = match kind {
                NewCellKind::Code => Cell::new_code(source.clone()),
                NewCellKind::Markdown => Cell::new_markdown(source.clone()),
                NewCellKind::Raw => Cell::new_raw(source.clone()),
            };
            let index = match at {
                Some(at) => notebook.insert(*at, cell),
                None => notebook.push(cell),
            };
            format!("inserted cell {index}")
        }
        EditRequest::Delete { cell } => {
            let index = notebook.resolve(cell)?;
            notebook.remove(index)?;
            format!("deleted cell {index}")
        }
        EditRequest::Move { cell, to } => {
            let index = notebook.resolve(cell)?;
            notebook.move_cell(index, *to)?;
            format!("moved cell {index} to {to}")
        }
        EditRequest::ClearOutputs => {
            notebook.clear_all_outputs();
            "cleared all outputs".to_string()
        }
    };
    Ok(message)
}

/// Forwards events to the caller's sink while folding them into a collector.
struct Tee<'a> {
    collector: &'a mut OutputCollector,
    execution_count: &'a mut Option<i64>,
    inner: &'a mut dyn OutputSink,
}

impl OutputSink for Tee<'_> {
    fn emit(&mut self, event: OutputEvent) {
        self.collector.apply(&event);
        if let OutputEvent::ExecutionCount(count) = &event {
            *self.execution_count = Some(*count);
        }
        self.inner.emit(event);
    }
}

/// Parse a cell range specification: `3`, `3-7`, `3-`, `-7`, `1,4,9`, `all`.
///
/// Ranges are inclusive because they name cells the way a human reading the
/// notebook does, not the way a slice does.
pub fn parse_cell_range(spec: &str, total: usize) -> Result<Vec<usize>> {
    let spec = spec.trim();
    if spec.is_empty() || spec.eq_ignore_ascii_case("all") {
        return Ok((0..total).collect());
    }
    let invalid = |detail: &str| Error::InvalidCellRange {
        spec: spec.to_string(),
        detail: detail.to_string(),
    };

    let mut out: Vec<usize> = Vec::new();
    for part in spec.split(',') {
        let part = part.trim();
        if part.is_empty() {
            return Err(invalid("empty element between commas"));
        }
        let (start, end) = match part.split_once('-') {
            None => {
                let n: usize = part
                    .parse()
                    .map_err(|_| invalid(&format!("{part:?} is not a cell number")))?;
                (n, n)
            }
            Some((a, b)) => {
                let start = if a.trim().is_empty() {
                    0
                } else {
                    a.trim()
                        .parse()
                        .map_err(|_| invalid(&format!("{a:?} is not a cell number")))?
                };
                let end = if b.trim().is_empty() {
                    total.saturating_sub(1)
                } else {
                    b.trim()
                        .parse()
                        .map_err(|_| invalid(&format!("{b:?} is not a cell number")))?
                };
                (start, end)
            }
        };
        if total == 0 {
            return Err(invalid("the notebook has no cells"));
        }
        if start > end {
            return Err(invalid(&format!(
                "{start} is after {end}; ranges run forwards"
            )));
        }
        if end >= total {
            return Err(invalid(&format!(
                "cell {end} does not exist; the notebook has {total} cells (0-{})",
                total - 1
            )));
        }
        for i in start..=end {
            if !out.contains(&i) {
                out.push(i);
            }
        }
    }
    Ok(out)
}

/// The database a notebook's `%%sql` cells default to.
///
/// Stored under `metadata.h5i.database` so it travels with the file, and
/// resolved relative to the notebook so a checked-in notebook works from any
/// working directory.
pub fn notebook_database(notebook: &Notebook, notebook_path: &Path) -> Option<PathBuf> {
    let raw = notebook
        .metadata
        .get("h5i")?
        .get("database")?
        .as_str()?
        .to_string();
    let candidate = PathBuf::from(&raw);
    if candidate.is_absolute() {
        return Some(candidate);
    }
    Some(
        match notebook_path.parent().filter(|p| !p.as_os_str().is_empty()) {
            Some(dir) => dir.join(candidate),
            None => candidate,
        },
    )
}

/// Record the notebook-wide default database.
pub fn set_notebook_database(notebook: &mut Notebook, database: &str) {
    let entry = notebook
        .metadata
        .entry("h5i")
        .or_insert_with(|| serde_json::json!({}));
    if let Some(map) = entry.as_object_mut() {
        map.insert(
            "database".to_string(),
            serde_json::Value::String(database.to_string()),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_cell_and_inclusive_ranges() {
        assert_eq!(parse_cell_range("3", 10).unwrap(), [3]);
        assert_eq!(parse_cell_range("3-5", 10).unwrap(), [3, 4, 5]);
        assert_eq!(parse_cell_range("0-0", 10).unwrap(), [0]);
    }

    #[test]
    fn open_ended_ranges_reach_the_edges() {
        assert_eq!(parse_cell_range("-2", 5).unwrap(), [0, 1, 2]);
        assert_eq!(parse_cell_range("3-", 5).unwrap(), [3, 4]);
    }

    #[test]
    fn all_and_empty_mean_every_cell() {
        assert_eq!(parse_cell_range("all", 3).unwrap(), [0, 1, 2]);
        assert_eq!(parse_cell_range("", 3).unwrap(), [0, 1, 2]);
        assert_eq!(parse_cell_range("all", 0).unwrap(), Vec::<usize>::new());
    }

    #[test]
    fn lists_are_deduplicated_but_keep_their_order() {
        assert_eq!(parse_cell_range("4,1,4,2-3", 10).unwrap(), [4, 1, 2, 3]);
    }

    #[test]
    fn out_of_range_says_what_does_exist() {
        let err = parse_cell_range("9", 3).unwrap_err();
        assert_eq!(err.code(), "invalid_cell_range");
        let message = err.to_string();
        assert!(message.contains("3 cells"), "unhelpful message: {message}");
        assert!(message.contains("0-2"), "unhelpful message: {message}");
    }

    #[test]
    fn malformed_specs_are_rejected() {
        for spec in ["x", "1,,2", "5-2", "1-x"] {
            let err = parse_cell_range(spec, 10).unwrap_err();
            assert_eq!(err.code(), "invalid_cell_range", "accepted {spec:?}");
        }
    }

    #[test]
    fn creating_over_an_existing_file_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nb.ipynb");
        std::fs::write(&path, "{}").unwrap();
        let err = match Session::create(&path, "python3") {
            Err(e) => e,
            Ok(_) => panic!("creating over an existing file should be refused"),
        };
        assert_eq!(err.code(), "invalid_input");
        // The original file must survive a refused create.
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "{}");
    }

    #[test]
    fn opening_normalizes_duplicate_cell_ids() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nb.ipynb");
        std::fs::write(
            &path,
            r#"{"cells": [
                 {"cell_type": "code", "execution_count": null, "id": "dup", "metadata": {}, "outputs": [], "source": "a"},
                 {"cell_type": "code", "execution_count": null, "id": "dup", "metadata": {}, "outputs": [], "source": "b"}
               ], "metadata": {}, "nbformat": 4, "nbformat_minor": 5}"#,
        )
        .unwrap();
        let session = Session::open(&path).unwrap();
        let a = session.notebook().cells[0].id().unwrap();
        let b = session.notebook().cells[1].id().unwrap();
        assert_ne!(a, b, "duplicate ids survived open, breaking id addressing");
    }

    #[test]
    fn code_cell_indices_skip_prose() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nb.ipynb");
        let mut nb = Notebook::new("python3", "Python 3", "python");
        nb.push(Cell::new_markdown("# title"));
        nb.push(Cell::new_code("a"));
        nb.push(Cell::new_raw("raw"));
        nb.push(Cell::new_code("b"));
        nb.write(&path).unwrap();
        let session = Session::open(&path).unwrap();
        assert_eq!(session.code_cell_indices(), [1, 3]);
    }
}
