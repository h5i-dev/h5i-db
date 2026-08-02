//! Output profiles: how much of a result is allowed to reach the caller.
//!
//! An agent's context window is a resource the database can exhaust. Asking
//! the agent to remember `--max-rows` does not work — one forgotten flag and
//! a 3-million-row result destroys the session. So the budget lives in the
//! environment instead of in the agent's memory: `H5I_DB_PROFILE=agent` caps
//! every query, and the rows that did not fit are still recoverable from a
//! Parquet spill file whose path is reported back.
//!
//! Two rules keep this predictable:
//!
//! 1. **Never switch on the TTY.** Output content is identical whether stdout
//!    is a terminal, a pipe, or a file; only an explicit profile changes it.
//!    A tool whose data changes when you pipe it is a footgun (git changes
//!    only *colour* on a pipe, for exactly this reason).
//! 2. **Explicit flags beat the profile.** A caller who passes `--max-bytes`
//!    asked for a hard limit and still gets the `limit_exceeded` error; the
//!    profile's own budget is a soft one that truncates and reports instead.

use std::io::Write;
use std::path::{Path, PathBuf};

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use h5i_db_core::{Error, Result};

/// Rows rendered to stdout before the rest goes to the spill file. Matches the
/// `--max-rows 1000` the skill has always recommended, so an agent that was
/// already following the guidance sees no change.
pub const AGENT_MAX_ROWS: usize = 1000;
/// Byte ceiling for rendered output. Bounds a result whose rows are individually
/// huge (long strings), which a row cap alone cannot.
pub const AGENT_MAX_BYTES: u64 = 1024 * 1024;
/// Upper bound on what the spill file will hold. Beyond this the spill stops and
/// says so — a silent truncation would read as "here is everything".
pub const AGENT_SPILL_ROW_CAP: usize = 5_000_000;

/// Which output profile this invocation runs under.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Profile {
    /// Historical behaviour: unlimited unless the caller passes a flag.
    Default,
    /// Budgeted output with a recoverable spill (`H5I_DB_PROFILE=agent`).
    Agent,
}

impl Profile {
    /// Read the profile from the environment. Unknown values fall back to
    /// `Default`: a typo must not silently uncap an agent, and it must not
    /// fail a command either.
    pub fn from_env() -> Self {
        match std::env::var("H5I_DB_PROFILE") {
            Ok(v) if v.eq_ignore_ascii_case("agent") => Profile::Agent,
            _ => Profile::Default,
        }
    }

    pub fn is_agent(self) -> bool {
        self == Profile::Agent
    }
}

/// Where spilled results are written. `H5I_DB_RESULT_DIR` overrides the system
/// temp directory for sandboxes where it is not writable.
fn result_dir() -> PathBuf {
    match std::env::var_os("H5I_DB_RESULT_DIR") {
        Some(dir) => PathBuf::from(dir),
        None => std::env::temp_dir().join("h5i-db-results"),
    }
}

/// The directory this process writes its spills into, created so that only
/// this user can read them.
///
/// A spill holds query results, which is the same data the database holds and
/// no less sensitive. Writing it straight into a fixed, world-readable
/// `/tmp/h5i-db-results` published it to every other user on the host, and let
/// any of them pre-create that path and read whatever landed there. So each
/// run gets its own unguessable subdirectory: `create_dir` fails outright if
/// the name already exists, which is what makes squatting on it impossible
/// rather than merely unlikely.
fn private_result_dir() -> Result<PathBuf> {
    let base = result_dir();
    std::fs::create_dir_all(&base).map_err(|e| Error::io(base.display().to_string(), e))?;
    restrict_to_owner(&base);
    let dir = base.join(format!("run-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&dir).map_err(|e| Error::io(dir.display().to_string(), e))?;
    restrict_to_owner(&dir);
    Ok(dir)
}

/// Best effort `0700`. A failure here is not worth failing the query over:
/// the unguessable directory name is the load-bearing part, and on platforms
/// without Unix modes there is nothing to set.
fn restrict_to_owner(path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700));
    }
    #[cfg(not(unix))]
    let _ = path;
}

/// Create the spill file readable and writable by its owner only.
fn create_private_file(path: &Path) -> Result<std::fs::File> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options
        .open(path)
        .map_err(|e| Error::io(path.display().to_string(), e))
}

/// A Parquet file holding the rows that did not fit in the rendered output.
///
/// Created lazily: a result that fits the budget writes no file at all, so the
/// common small query leaves nothing behind.
pub struct ResultSpill {
    schema: SchemaRef,
    writer: Option<parquet::arrow::ArrowWriter<std::fs::File>>,
    path: Option<PathBuf>,
    /// Batches held back before the spill was needed, so the file still starts
    /// at row 0 once it is created.
    pending: Vec<RecordBatch>,
    /// Rows accepted so far (buffered or written). Stops at `row_cap`.
    spilled_rows: usize,
    complete: bool,
    row_cap: usize,
    /// Once the result exceeds this many rows, stdout cannot hold all of it and
    /// the file has to exist. Decided here rather than by the caller, because
    /// the whole result can arrive in a single batch that crosses the budget.
    spill_above_rows: usize,
}

impl ResultSpill {
    pub fn new(schema: SchemaRef, spill_above_rows: usize) -> Self {
        Self {
            schema,
            writer: None,
            path: None,
            pending: Vec::new(),
            spilled_rows: 0,
            complete: true,
            row_cap: AGENT_SPILL_ROW_CAP,
            spill_above_rows,
        }
    }

    #[cfg(test)]
    fn with_row_cap(mut self, cap: usize) -> Self {
        self.row_cap = cap;
        self
    }

    /// Take one batch, opening the file once the result outgrows what stdout
    /// will render.
    pub fn push(&mut self, batch: &RecordBatch) -> Result<()> {
        if !self.complete {
            return Ok(());
        }
        if self.spilled_rows + batch.num_rows() > self.row_cap {
            // Write what still fits, then stop and record the truncation.
            let room = self.row_cap.saturating_sub(self.spilled_rows);
            if room > 0 {
                let head = batch.slice(0, room);
                self.accept(&head)?;
                self.force()?;
            }
            self.complete = false;
            return Ok(());
        }
        self.accept(batch)
    }

    /// Ensure a file exists even though the row budget was not exceeded —
    /// used when stdout was cut short by the *byte* cap instead.
    pub fn force(&mut self) -> Result<()> {
        if self.writer.is_none() && self.spilled_rows > 0 {
            self.open()?;
        }
        Ok(())
    }

    fn accept(&mut self, batch: &RecordBatch) -> Result<()> {
        self.spilled_rows += batch.num_rows();
        if self.writer.is_none() {
            self.pending.push(batch.clone());
            if self.spilled_rows > self.spill_above_rows {
                self.open()?;
            }
            return Ok(());
        }
        let writer = self.writer.as_mut().expect("writer present");
        writer
            .write(batch)
            .map_err(|e| Error::invalid(format!("spilling result: {e}")))
    }

    fn open(&mut self) -> Result<()> {
        let dir = private_result_dir()?;
        let path = dir.join(format!("result-{}.parquet", uuid::Uuid::new_v4()));
        let file = create_private_file(&path)?;
        let mut writer = parquet::arrow::ArrowWriter::try_new(file, self.schema.clone(), None)
            .map_err(Error::Parquet)?;
        for batch in std::mem::take(&mut self.pending) {
            writer.write(&batch).map_err(Error::Parquet)?;
        }
        self.writer = Some(writer);
        self.path = Some(path);
        Ok(())
    }

    /// Close the file, if one was ever opened, and report where it landed.
    pub fn finish(mut self) -> Result<SpillOutcome> {
        let path = self.path.take();
        if let Some(writer) = self.writer.take() {
            writer.close().map_err(Error::Parquet)?;
        }
        Ok(SpillOutcome {
            path,
            complete: self.complete,
            rows: self.spilled_rows,
        })
    }
}

/// What became of the spill.
pub struct SpillOutcome {
    /// `None` when the result fit the budget and no file was needed.
    pub path: Option<PathBuf>,
    /// False when the spill hit [`AGENT_SPILL_ROW_CAP`] and stopped early.
    pub complete: bool,
    /// Rows the spill holds.
    pub rows: usize,
}

/// The stderr summary that tells an agent what it did *not* see.
///
/// Emitted on every agent-profile query, truncated or not: knowing the true
/// row count without paying for a second `COUNT(*)` is worth the two lines.
#[derive(serde::Serialize)]
pub struct ResultSummary {
    pub schema_version: u32,
    pub profile: &'static str,
    /// True when stdout does not contain the whole result.
    pub truncated: bool,
    /// Rows the query produced, not the rows that were rendered.
    pub total_rows: u64,
    pub returned_rows: u64,
    pub max_rows: usize,
    pub max_bytes: u64,
    /// Parquet file holding the full result; absent when nothing was withheld.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub full_result_path: Option<String>,
    /// Rows the spill file holds. Equal to `total_rows` unless the spill was
    /// capped, in which case the gap is exactly what was dropped.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub full_result_rows: Option<u64>,
    /// False when the spill itself was capped — the file is a prefix, not the
    /// whole result. Never let a cap pass silently.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub full_result_truncated: bool,
    /// How to read the withheld rows back. Deliberately points at an ordinary
    /// Parquet reader rather than a SQL function: the query session registers
    /// only this database's tables, and giving SQL the ability to open
    /// arbitrary files would widen the read surface for no gain here.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub read_full_result_with: Option<String>,
}

impl ResultSummary {
    pub const SCHEMA_VERSION: u32 = 1;

    pub fn build(
        total_rows: u64,
        returned_rows: u64,
        truncated: bool,
        max_rows: usize,
        max_bytes: u64,
        spill: &SpillOutcome,
    ) -> Self {
        let path = spill.path.as_deref().map(display_path);
        let read_with = path.as_ref().map(|p| {
            format!("python -c \"import pyarrow.parquet as pq; print(pq.read_table('{p}'))\"")
        });
        Self {
            schema_version: Self::SCHEMA_VERSION,
            profile: "agent",
            truncated,
            total_rows,
            returned_rows,
            max_rows,
            max_bytes,
            full_result_rows: spill.path.is_some().then_some(spill.rows as u64),
            full_result_path: path,
            full_result_truncated: !spill.complete,
            read_full_result_with: read_with,
        }
    }

    /// Write the summary to stderr, keeping stdout a clean data stream (the
    /// same split `--stats` and the error envelope already use).
    pub fn emit(&self) {
        if let Ok(line) = serde_json::to_string(self) {
            let mut stderr = std::io::stderr().lock();
            let _ = writeln!(stderr, "{line}");
        }
    }
}

fn display_path(p: &Path) -> String {
    p.display().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]))
    }

    fn batch(n: i64) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![Arc::new(Int64Array::from((0..n).collect::<Vec<_>>()))],
        )
        .unwrap()
    }

    #[test]
    fn a_result_that_fits_leaves_no_file_behind() {
        // Budget of 100 rows, result of 10: nothing to withhold.
        let mut spill = ResultSpill::new(schema(), 100);
        spill.push(&batch(10)).unwrap();
        let out = spill.finish().unwrap();
        assert!(out.path.is_none(), "no spill file for a result that fit");
        assert!(out.complete);
        assert_eq!(out.rows, 10);
    }

    #[test]
    fn spill_starts_at_row_zero_even_though_it_opens_late() {
        let dir = tempfile::tempdir().unwrap();
        // SAFETY: single-threaded test; the override is read on the next open.
        unsafe { std::env::set_var("H5I_DB_RESULT_DIR", dir.path()) };
        // Budget of 6 rows: the first batch is buffered, the second crosses
        // the budget and opens the file — both must end up in it.
        let mut spill = ResultSpill::new(schema(), 6);
        spill.push(&batch(5)).unwrap();
        spill.push(&batch(7)).unwrap();
        let out = spill.finish().unwrap();
        let path = out.path.expect("spill file");
        let file = std::fs::File::open(&path).unwrap();
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let rows: usize = reader.map(|b| b.unwrap().num_rows()).sum();
        assert_eq!(rows, 12, "the buffered prefix must be in the spill too");
        unsafe { std::env::remove_var("H5I_DB_RESULT_DIR") };
    }

    #[test]
    fn hitting_the_spill_cap_is_reported_not_hidden() {
        let dir = tempfile::tempdir().unwrap();
        unsafe { std::env::set_var("H5I_DB_RESULT_DIR", dir.path()) };
        let mut spill = ResultSpill::new(schema(), 0).with_row_cap(10);
        spill.push(&batch(8)).unwrap();
        spill.push(&batch(8)).unwrap(); // crosses the cap
        spill.push(&batch(8)).unwrap(); // ignored entirely
        let out = spill.finish().unwrap();
        assert!(!out.complete, "a capped spill must not claim completeness");
        assert_eq!(out.rows, 10);
        unsafe { std::env::remove_var("H5I_DB_RESULT_DIR") };
    }

    #[test]
    fn unknown_profile_values_fall_back_to_default() {
        unsafe { std::env::set_var("H5I_DB_PROFILE", "agnet") };
        assert_eq!(Profile::from_env(), Profile::Default);
        unsafe { std::env::set_var("H5I_DB_PROFILE", "AGENT") };
        assert_eq!(Profile::from_env(), Profile::Agent);
        unsafe { std::env::remove_var("H5I_DB_PROFILE") };
        assert_eq!(Profile::from_env(), Profile::Default);
    }
}
