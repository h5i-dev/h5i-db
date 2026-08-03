//! A native h5i-db kernel: SQL straight through DataFusion, no subprocess.
//!
//! This is what makes the crate an *h5i-db* notebook rather than a generic
//! one. Schema discovery and the first twenty exploratory queries of any
//! session are pure SQL, and routing them through Python costs a interpreter
//! start, a wheel import, and an Arrow round trip for nothing. Here they cost
//! a function call.
//!
//! It sits behind the same [`Kernel`] trait as [`JupyterKernel`](super::JupyterKernel),
//! so the session, the CLI, and the TUI drive both the same way.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use futures::StreamExt;
use h5i_db_core::{Database, ReadAt};
use h5i_db_query::{H5iSession, SessionOptions};
use serde_json::json;

use crate::document::{ErrorOutput, ExecuteResultOutput, MimeBundle, Output, StreamName};
use crate::error::{Error, Result};
use crate::kernel::spec::{InterruptMode, KernelSpecInfo};
use crate::kernel::{
    Completeness, Completions, ExecOptions, ExecOutcome, ExecStatus, InterruptHandle, Kernel,
    KernelStatus, OutputEvent, OutputSink,
};

/// Mime type carrying the aligned text table, so a renderer can tell a query
/// result apart from an arbitrary `text/plain` repr.
pub const TABLE_MIME: &str = "application/vnd.h5i.table+text";

#[derive(Debug, Clone)]
pub struct SqlOptions {
    /// Rows materialised before the result is truncated. A notebook cell that
    /// silently pulls ten million rows into a file is a worse outcome than a
    /// visibly truncated one.
    pub max_rows: usize,
    /// Rows actually rendered into the cell's output.
    ///
    /// Separate from `max_rows` on purpose: materialising ten thousand rows is
    /// a memory decision, printing them is a readability one. A result set is
    /// summarised by its shape and its first rows; the rest is noise in a
    /// terminal and cost in an agent's context.
    pub display_rows: usize,
    /// Query memory budget, in bytes.
    pub memory_limit: Option<usize>,
    /// Read-only: `open_read_only` refuses a database that would need
    /// recovery, so an exploration session can never mutate what it reads.
    pub read_only: bool,
}

impl Default for SqlOptions {
    fn default() -> Self {
        SqlOptions {
            max_rows: 10_000,
            display_rows: 20,
            memory_limit: None,
            read_only: true,
        }
    }
}

pub struct SqlKernel {
    spec: KernelSpecInfo,
    database_path: PathBuf,
    fork: Option<String>,
    options: SqlOptions,
    session: H5iSession,
    status: KernelStatus,
    execution_count: i64,
    interrupt: InterruptHandle,
}

impl SqlKernel {
    /// Open a database and build a query session for it.
    pub async fn open(
        database: impl AsRef<Path>,
        fork: Option<String>,
        options: SqlOptions,
    ) -> Result<Self> {
        let database_path = database.as_ref().to_path_buf();
        let session = Self::build_session(&database_path, fork.as_deref(), &options).await?;
        Ok(SqlKernel {
            spec: sql_spec(&database_path),
            database_path,
            fork,
            options,
            session,
            status: KernelStatus::Idle,
            execution_count: 0,
            interrupt: InterruptHandle::new(),
        })
    }

    async fn build_session(
        path: &Path,
        fork: Option<&str>,
        options: &SqlOptions,
    ) -> Result<H5iSession> {
        let db = Database::open(path)
            .await
            .map_err(|e| Error::invalid(format!("cannot open database {}: {e}", path.display())))?;
        let db = match fork {
            Some(name) => db.open_fork(name).await.map_err(|e| {
                Error::invalid(format!(
                    "cannot open fork {name:?} in {}: {e}",
                    path.display()
                ))
            })?,
            None => db,
        };
        let session_options = SessionOptions {
            memory_limit: options.memory_limit,
            spill_dir: None,
            target_partitions: None,
            batch_size: None,
            telemetry_capacity: 0,
            predicate_cache: h5i_db_query::PredicateCacheMode::Disabled,
        };
        H5iSession::new(Arc::new(db), session_options)
            .await
            .map_err(|e| Error::invalid(format!("cannot start a query session: {e}")))
    }

    /// Continue execution numbering from what the notebook already recorded.
    ///
    /// Two kernels share one notebook, so without this a SQL cell would start
    /// back at `[1]` beside Python cells numbered `[7]`.
    pub fn set_execution_count(&mut self, count: i64) {
        self.execution_count = count;
    }

    pub fn database_path(&self) -> &Path {
        &self.database_path
    }

    pub fn fork(&self) -> Option<&str> {
        self.fork.as_deref()
    }

    /// Adopt an externally owned interrupt handle. See
    /// [`JupyterKernel::set_interrupt_handle`](super::JupyterKernel::set_interrupt_handle).
    pub fn set_interrupt_handle(&mut self, handle: InterruptHandle) {
        self.interrupt = handle;
    }

    pub fn set_max_rows(&mut self, max_rows: usize) {
        self.options.max_rows = max_rows;
    }

    /// Run `sql` and write the *whole* result to an Arrow IPC file.
    ///
    /// Deliberately not row-capped: the cap exists so a rendered table does
    /// not swamp a terminal, and applying it to a handoff would silently bind
    /// a truncated frame in the Python kernel.
    pub async fn write_ipc(&self, sql: &str, path: &Path) -> Result<usize> {
        let frame = self
            .session
            .sql(sql)
            .await
            .map_err(|e| Error::invalid(sql_message(&e)))?;
        let schema: SchemaRef = Arc::new(frame.schema().as_arrow().clone());
        let mut stream = frame
            .execute_stream()
            .await
            .map_err(|e| Error::invalid(sql_message(&e)))?;

        let file = std::fs::File::create(path).map_err(|e| Error::io(path.display(), e))?;
        let mut writer = arrow::ipc::writer::FileWriter::try_new(file, &schema)
            .map_err(|e| Error::internal(format!("cannot write Arrow IPC: {e}")))?;
        let mut rows = 0usize;
        while let Some(batch) = stream.next().await {
            let batch = batch.map_err(|e| Error::invalid(sql_message(&e)))?;
            rows += batch.num_rows();
            writer
                .write(&batch)
                .map_err(|e| Error::internal(format!("cannot write Arrow IPC: {e}")))?;
        }
        writer
            .finish()
            .map_err(|e| Error::internal(format!("cannot finish Arrow IPC: {e}")))?;
        Ok(rows)
    }

    /// Table names in the database, for completion and for `%%sql --tables`.
    pub async fn table_names(&self) -> Vec<String> {
        match self.session.database().list_tables().await {
            Ok(entries) => entries.into_iter().map(|e| e.name).collect(),
            Err(_) => Vec::new(),
        }
    }

    /// Run one statement and collect it, honouring the row cap.
    async fn collect(&self, sql: &str) -> Result<(SchemaRef, Vec<RecordBatch>, bool)> {
        let frame = self
            .session
            .sql(sql)
            .await
            .map_err(|e| Error::invalid(sql_message(&e)))?;
        let schema: SchemaRef = Arc::new(frame.schema().as_arrow().clone());
        let mut stream = frame
            .execute_stream()
            .await
            .map_err(|e| Error::invalid(sql_message(&e)))?;

        let mut batches = Vec::new();
        let mut rows = 0usize;
        let mut truncated = false;
        while let Some(batch) = stream.next().await {
            let batch = batch.map_err(|e| Error::invalid(sql_message(&e)))?;
            if rows + batch.num_rows() > self.options.max_rows {
                let keep = self.options.max_rows.saturating_sub(rows);
                if keep > 0 {
                    batches.push(batch.slice(0, keep));
                }
                truncated = true;
                break;
            }
            rows += batch.num_rows();
            batches.push(batch);
        }
        Ok((schema, batches, truncated))
    }
}

/// A synthetic kernelspec, so the SQL backend reports itself like any other.
fn sql_spec(database: &Path) -> KernelSpecInfo {
    KernelSpecInfo {
        name: "h5i-sql".to_string(),
        display_name: "h5i-db SQL".to_string(),
        language: "sql".to_string(),
        path: database.to_path_buf(),
        argv: Vec::new(),
        env: Default::default(),
        interrupt_mode: InterruptMode::Message,
        metadata: Default::default(),
    }
}

/// DataFusion errors are prefixed and wrapped several layers deep; the first
/// line is the part that tells a reader what is wrong with their query.
fn sql_message(error: &impl std::fmt::Display) -> String {
    let text = error.to_string();
    let first = text.lines().next().unwrap_or(&text).trim();
    first
        .strip_prefix("Error during planning: ")
        .or_else(|| first.strip_prefix("SQL error: "))
        .unwrap_or(first)
        .to_string()
}

#[async_trait]
impl Kernel for SqlKernel {
    fn spec(&self) -> &KernelSpecInfo {
        &self.spec
    }

    fn status(&self) -> KernelStatus {
        self.status
    }

    fn interrupt_handle(&self) -> InterruptHandle {
        self.interrupt.clone()
    }

    async fn execute(
        &mut self,
        code: &str,
        options: &ExecOptions,
        sink: &mut dyn OutputSink,
    ) -> Result<ExecOutcome> {
        let started = Instant::now();
        let sql = code.trim();
        if sql.is_empty() {
            return Ok(ExecOutcome {
                status: ExecStatus::Ok,
                execution_count: None,
                outputs: Vec::new(),
                elapsed: started.elapsed(),
            });
        }

        self.interrupt.reset();
        self.execution_count += 1;
        let count = self.execution_count;
        self.status = KernelStatus::Busy;
        sink.emit(OutputEvent::Status(KernelStatus::Busy));
        sink.emit(OutputEvent::ExecutionCount(count));

        // Three things race: the query, the caller's timeout, and an interrupt
        // from somewhere else entirely. Losing the race simply drops the query
        // future, which is how DataFusion cancellation works.
        let interrupt = self.interrupt.clone();
        let timeout = options.timeout;
        let result = {
            let query = self.collect(sql);
            tokio::pin!(query);
            let deadline = timeout.map(|t| tokio::time::Instant::now() + t);
            tokio::select! {
                outcome = &mut query => Some(outcome),
                _ = interrupt.requested() => None,
                _ = async {
                    match deadline {
                        Some(deadline) => tokio::time::sleep_until(deadline).await,
                        // Never completes, so the branch is simply inert.
                        None => std::future::pending::<()>().await,
                    }
                } => Some(Err(Error::ExecuteTimeout {
                    timeout_secs: timeout.map(|t| t.as_secs()).unwrap_or(0).max(1),
                })),
            }
        };

        self.status = KernelStatus::Idle;
        sink.emit(OutputEvent::Status(KernelStatus::Idle));

        let Some(result) = result else {
            self.interrupt.reset();
            return Err(Error::Interrupted);
        };

        match result {
            Ok((schema, batches, truncated)) => {
                let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
                let mut outputs = Vec::new();

                if truncated {
                    let warning = format!(
                        "note: showing the first {} rows; raise --max-rows or add a LIMIT\n",
                        self.options.max_rows
                    );
                    let output = Output::stream(StreamName::Stderr, warning);
                    sink.emit(OutputEvent::Output(output.clone()));
                    outputs.push(output);
                }

                let bundle = render_result(
                    &schema,
                    &batches,
                    rows,
                    truncated,
                    self.options.display_rows,
                )?;
                let output = Output::ExecuteResult(ExecuteResultOutput {
                    data: bundle,
                    metadata: Default::default(),
                    execution_count: Some(count),
                    extra: Default::default(),
                });
                sink.emit(OutputEvent::Output(output.clone()));
                outputs.push(output);

                Ok(ExecOutcome {
                    status: ExecStatus::Ok,
                    execution_count: Some(count),
                    outputs,
                    elapsed: started.elapsed(),
                })
            }
            Err(Error::ExecuteTimeout { timeout_secs }) => {
                Err(Error::ExecuteTimeout { timeout_secs })
            }
            Err(error) => {
                // A bad query is a cell that raised, not a tool failure: it
                // becomes an error output exactly like a Python traceback so
                // the notebook records it in the same shape.
                let output = Output::Error(ErrorOutput {
                    ename: "SqlError".to_string(),
                    evalue: error.to_string(),
                    traceback: vec![format!("SqlError: {error}")],
                    extra: Default::default(),
                });
                sink.emit(OutputEvent::Output(output.clone()));
                Ok(ExecOutcome {
                    status: ExecStatus::Error,
                    execution_count: Some(count),
                    outputs: vec![output],
                    elapsed: started.elapsed(),
                })
            }
        }
    }

    async fn complete(&mut self, code: &str, cursor: usize) -> Result<Completions> {
        // Complete the identifier under the cursor against table names and the
        // SQL keywords, which is what a reader is reaching for nine times in
        // ten. Column completion would need the table in scope, which means
        // parsing the statement; that is a later refinement.
        let cursor = cursor.min(code.len());
        let prefix_start = code[..cursor]
            .rfind(|c: char| !(c.is_alphanumeric() || c == '_' || c == '.'))
            .map(|i| i + 1)
            .unwrap_or(0);
        let prefix = &code[prefix_start..cursor];

        let mut matches: Vec<String> = self
            .table_names()
            .await
            .into_iter()
            .filter(|name| name.starts_with(prefix))
            .collect();
        matches.extend(
            SQL_KEYWORDS
                .iter()
                .filter(|k| k.starts_with(&prefix.to_uppercase()))
                .map(|k| k.to_string()),
        );
        matches.sort();
        matches.dedup();
        let types = vec![None; matches.len()];

        Ok(Completions {
            matches,
            cursor_start: prefix_start,
            cursor_end: cursor,
            types,
        })
    }

    async fn inspect(
        &mut self,
        code: &str,
        cursor: usize,
        _detail: u8,
    ) -> Result<Option<MimeBundle>> {
        let cursor = cursor.min(code.len());
        let start = code[..cursor]
            .rfind(|c: char| !(c.is_alphanumeric() || c == '_'))
            .map(|i| i + 1)
            .unwrap_or(0);
        let end = code[cursor..]
            .find(|c: char| !(c.is_alphanumeric() || c == '_'))
            .map(|i| i + cursor)
            .unwrap_or(code.len());
        let name = &code[start..end];
        if name.is_empty() {
            return Ok(None);
        }
        if !self.table_names().await.iter().any(|t| t == name) {
            return Ok(None);
        }
        let frame = self
            .session
            .read_table(name, ReadAt::Latest)
            .await
            .map_err(|e| Error::invalid(sql_message(&e)))?;
        let schema = frame.schema();
        let mut text = format!("table {name}\n");
        for field in schema.fields() {
            text.push_str(&format!(
                "  {:<28} {}{}\n",
                field.name(),
                field.data_type(),
                if field.is_nullable() { "" } else { " not null" }
            ));
        }
        let mut bundle = MimeBundle::new();
        bundle.insert("text/plain", json!(text));
        Ok(Some(bundle))
    }

    async fn is_complete(&mut self, code: &str) -> Result<Completeness> {
        let trimmed = code.trim_end();
        if trimmed.is_empty() {
            return Ok(Completeness::Incomplete {
                indent: String::new(),
            });
        }
        // A trailing semicolon is an explicit terminator; otherwise a
        // balanced-parenthesis statement is treated as ready to run, which
        // matches how every SQL console behaves.
        if trimmed.ends_with(';') {
            return Ok(Completeness::Complete);
        }
        let mut depth = 0i32;
        let mut in_string = false;
        for c in trimmed.chars() {
            match c {
                '\'' => in_string = !in_string,
                '(' if !in_string => depth += 1,
                ')' if !in_string => depth -= 1,
                _ => {}
            }
        }
        Ok(if depth > 0 || in_string {
            Completeness::Incomplete {
                indent: "  ".to_string(),
            }
        } else {
            Completeness::Complete
        })
    }

    async fn interrupt(&mut self) -> Result<()> {
        self.interrupt.interrupt();
        Ok(())
    }

    async fn restart(&mut self) -> Result<()> {
        // There is no process to restart; rebuilding the session is the
        // equivalent, and it re-resolves every table at current head.
        self.session =
            Self::build_session(&self.database_path, self.fork.as_deref(), &self.options).await?;
        self.execution_count = 0;
        self.status = KernelStatus::Idle;
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<()> {
        self.status = KernelStatus::Dead;
        Ok(())
    }
}

/// Render a result set as the mime bundle a notebook stores.
///
/// Both representations are written: `text/plain` so a terminal and an agent
/// can read it, `text/html` so the same file renders as a table when a human
/// opens it in JupyterLab.
fn render_result(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    rows: usize,
    truncated: bool,
    display_rows: usize,
) -> Result<MimeBundle> {
    let shown = head(batches, display_rows);
    let text = arrow::util::pretty::pretty_format_batches(&shown)
        .map_err(|e| Error::internal(format!("cannot format the result: {e}")))?
        .to_string();
    let shape = if rows > display_rows {
        format!(
            "[showing {display_rows} of {rows}{} rows x {} columns]",
            if truncated { "+" } else { "" },
            schema.fields().len()
        )
    } else {
        format!(
            "[{rows}{} rows x {} columns]",
            if truncated { "+" } else { "" },
            schema.fields().len()
        )
    };
    let plain = if text.is_empty() {
        shape.clone()
    } else {
        format!("{text}\n{shape}")
    };

    let mut bundle = MimeBundle::new();
    bundle.insert("text/plain", json!(plain));
    bundle.insert(TABLE_MIME, json!(plain));
    bundle.insert("text/html", json!(render_html(schema, &shown, &shape)));
    Ok(bundle)
}

/// The first `limit` rows across a batch list, without copying the rest.
fn head(batches: &[RecordBatch], limit: usize) -> Vec<RecordBatch> {
    let mut out = Vec::new();
    let mut remaining = limit;
    for batch in batches {
        if remaining == 0 {
            break;
        }
        if batch.num_rows() <= remaining {
            remaining -= batch.num_rows();
            out.push(batch.clone());
        } else {
            out.push(batch.slice(0, remaining));
            remaining = 0;
        }
    }
    out
}

fn render_html(schema: &SchemaRef, batches: &[RecordBatch], shape: &str) -> String {
    use arrow::util::display::{ArrayFormatter, FormatOptions};

    let mut html = String::from("<table border=\"1\" class=\"dataframe h5i-sql\">\n<thead><tr>");
    for field in schema.fields() {
        html.push_str(&format!("<th>{}</th>", escape_html(field.name())));
    }
    html.push_str("</tr></thead>\n<tbody>\n");

    let format_options = FormatOptions::default().with_null("");
    for batch in batches {
        let formatters: Vec<_> = match batch
            .columns()
            .iter()
            .map(|c| ArrayFormatter::try_new(c.as_ref(), &format_options))
            .collect::<std::result::Result<Vec<_>, _>>()
        {
            Ok(formatters) => formatters,
            // Formatting is presentational; a column we cannot render must not
            // lose the caller their query result.
            Err(_) => continue,
        };
        for row in 0..batch.num_rows() {
            html.push_str("<tr>");
            for formatter in &formatters {
                html.push_str(&format!(
                    "<td>{}</td>",
                    escape_html(&formatter.value(row).to_string())
                ));
            }
            html.push_str("</tr>\n");
        }
    }
    html.push_str("</tbody></table>\n");
    html.push_str(&format!("<p>{}</p>", escape_html(shape)));
    html
}

fn escape_html(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            _ => out.push(c),
        }
    }
    out
}

const SQL_KEYWORDS: &[&str] = &[
    "SELECT",
    "FROM",
    "WHERE",
    "GROUP BY",
    "ORDER BY",
    "LIMIT",
    "JOIN",
    "LEFT JOIN",
    "INNER JOIN",
    "HAVING",
    "DISTINCT",
    "AS",
    "AND",
    "OR",
    "NOT",
    "NULL",
    "COUNT",
    "SUM",
    "AVG",
    "MIN",
    "MAX",
    "OVER",
    "PARTITION BY",
    "WITH",
    "UNION",
    "CASE",
    "WHEN",
    "THEN",
    "ELSE",
    "END",
    "BETWEEN",
    "DESC",
    "ASC",
    "CAST",
    "INTERVAL",
];

/// Timeout applied to a SQL cell when the caller sets none.
pub const DEFAULT_SQL_TIMEOUT: Duration = Duration::from_secs(300);

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    fn sample() -> (SchemaRef, Vec<RecordBatch>) {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("volume", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["AAPL", "MSFT<script>"])),
                Arc::new(Int64Array::from(vec![Some(10), None])),
            ],
        )
        .unwrap();
        (schema, vec![batch])
    }

    #[test]
    fn results_carry_both_a_text_and_an_html_representation() {
        let (schema, batches) = sample();
        let bundle = render_result(&schema, &batches, 2, false, 20).unwrap();
        let plain = bundle.text_plain().unwrap();
        assert!(plain.contains("symbol"), "{plain}");
        assert!(plain.contains("AAPL"), "{plain}");
        assert!(plain.contains("[2 rows x 2 columns]"), "{plain}");
        let html = bundle.text_of("text/html").unwrap();
        assert!(html.contains("<table"), "{html}");
        assert!(html.contains("<th>symbol</th>"), "{html}");
    }

    #[test]
    fn html_escapes_cell_values() {
        // A symbol containing markup must not be able to inject into the
        // notebook's rendered HTML.
        let (schema, batches) = sample();
        let bundle = render_result(&schema, &batches, 2, false, 20).unwrap();
        let html = bundle.text_of("text/html").unwrap();
        assert!(html.contains("MSFT&lt;script&gt;"), "{html}");
        assert!(!html.contains("<script>"), "{html}");
    }

    #[test]
    fn truncation_is_visible_in_the_shape_line() {
        let (schema, batches) = sample();
        let bundle = render_result(&schema, &batches, 2, true, 20).unwrap();
        assert!(
            bundle.text_plain().unwrap().contains("[2+ rows"),
            "truncation must be obvious: {}",
            bundle.text_plain().unwrap()
        );
    }

    #[test]
    fn an_empty_result_still_reports_its_shape() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let bundle = render_result(&schema, &[], 0, false, 20).unwrap();
        assert!(
            bundle
                .text_plain()
                .unwrap()
                .contains("[0 rows x 1 columns]")
        );
    }

    #[test]
    fn datafusion_error_prefixes_are_stripped() {
        struct E(&'static str);
        impl std::fmt::Display for E {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }
        assert_eq!(
            sql_message(&E("Error during planning: table 'x' not found\nbacktrace…")),
            "table 'x' not found"
        );
        assert_eq!(sql_message(&E("plain message")), "plain message");
    }

    #[test]
    fn html_escaping_covers_every_dangerous_character() {
        assert_eq!(
            escape_html("<a href=\"x\">&'</a>"),
            "&lt;a href=&quot;x&quot;&gt;&amp;&#39;&lt;/a&gt;"
        );
    }
}
