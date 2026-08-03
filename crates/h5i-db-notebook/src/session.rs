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
use crate::kernel::{
    ExecOptions, ExecStatus, JupyterKernel, Kernel, KernelStatus, OutputCollector, OutputEvent,
    OutputSink, spec,
};

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
    exec_options: ExecOptions,
    start_options: StartOptions,
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
            exec_options: ExecOptions::default(),
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
        let kernel = JupyterKernel::start(spec, self.start_options.clone()).await?;
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
        let index = self.notebook.push(Cell::new_code(code));
        self.dirty = true;
        self.save()?;
        self.run_cell(index, sink).await
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
        let code = cell.source().to_string();
        let cell_id = cell.id().map(str::to_string);

        self.ensure_kernel().await?;
        let options = self.exec_options.clone();

        // Mirror every event into a collector as well as the caller's sink, so
        // that a failure part-way through still leaves us the partial output to
        // write back.
        let mut collector = OutputCollector::new();
        let mut execution_count = None;
        let mut outcome = {
            let kernel = self.kernel_mut()?;
            let mut tee = Tee {
                collector: &mut collector,
                execution_count: &mut execution_count,
                inner: sink,
            };
            kernel.execute(&code, &options, &mut tee).await
        };

        // A timeout or a dead kernel still produced output worth keeping.
        let (status, outputs, count, elapsed) = match &mut outcome {
            Ok(o) => (
                o.status,
                std::mem::take(&mut o.outputs),
                o.execution_count,
                o.elapsed,
            ),
            Err(_) => (
                ExecStatus::Error,
                collector.into_outputs(),
                execution_count,
                Duration::ZERO,
            ),
        };

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
