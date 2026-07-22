//! Output rendering: table (human), json, jsonl, csv, arrow IPC stream.
//! Machine formats write to stdout; the error envelope goes to stderr.
//!
//! Query results stream through [`BatchWriter`] batch-by-batch (nothing is
//! collected first except for the human `table` format, which needs every row
//! to align columns). Empty results always carry their schema: `arrow` emits
//! a schema-only IPC stream, `csv` a header line, `json` an empty array.

use std::io::{IsTerminal, StdoutLock, Write};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::RecordBatch;
use arrow::csv::WriterBuilder as CsvWriterBuilder;
use arrow::datatypes::SchemaRef;
use arrow::json::writer::{JsonArray, LineDelimited, WriterBuilder as JsonWriterBuilder};
use h5i_db_core::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Format {
    /// Human-readable aligned table.
    Table,
    /// One JSON array of row objects (with a schema envelope via --envelope).
    Json,
    /// One JSON object per row per line.
    Jsonl,
    Csv,
    /// Arrow IPC stream on stdout (lossless; pipe into other tools).
    Arrow,
}

/// Counts bytes flowing to the wrapped writer, observable from outside the
/// arrow writers that take ownership of it. Also records EPIPE, because some
/// arrow writers (csv) flatten io errors to strings, losing the error kind.
struct CountingWriter<W> {
    inner: W,
    count: Arc<AtomicU64>,
    broken_pipe: Arc<AtomicBool>,
}

impl<W: Write> CountingWriter<W> {
    fn note_pipe(&self, e: std::io::Error) -> std::io::Error {
        if e.kind() == std::io::ErrorKind::BrokenPipe {
            self.broken_pipe.store(true, Ordering::Relaxed);
        }
        e
    }
}

impl<W: Write> Write for CountingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf).map_err(|e| self.note_pipe(e))?;
        self.count.fetch_add(n as u64, Ordering::Relaxed);
        Ok(n)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush().map_err(|e| self.note_pipe(e))
    }
}

type Out = CountingWriter<StdoutLock<'static>>;

enum Sink {
    /// Buffered: pretty output needs every row to align columns.
    Table(Vec<RecordBatch>),
    Json(arrow::json::Writer<Out, JsonArray>),
    Jsonl(arrow::json::Writer<Out, LineDelimited>),
    Csv(arrow::csv::Writer<Out>),
    Arrow(arrow::ipc::writer::StreamWriter<Out>),
}

/// Streaming batch writer with an optional output byte cap (`--max-bytes`).
///
/// The cap is enforced at batch boundaries: `write` returns `Ok(false)` once
/// the cap is reached (the batch that crossed it is already written), after
/// which the caller stops feeding batches and calls `finish` — the output
/// stays well-formed, just truncated.
pub struct BatchWriter {
    sink: Sink,
    schema: SchemaRef,
    bytes: Arc<AtomicU64>,
    broken_pipe: Arc<AtomicBool>,
    max_bytes: Option<u64>,
    wrote_any: bool,
    /// In-memory size proxy for the buffered `table` format.
    buffered_mem: u64,
}

impl BatchWriter {
    pub fn new(format: Format, schema: SchemaRef, max_bytes: Option<u64>) -> Result<Self> {
        let bytes = Arc::new(AtomicU64::new(0));
        let broken_pipe = Arc::new(AtomicBool::new(false));
        let out = || CountingWriter {
            inner: std::io::stdout().lock(),
            count: bytes.clone(),
            broken_pipe: broken_pipe.clone(),
        };
        let sink = match format {
            Format::Table => Sink::Table(Vec::new()),
            Format::Json => Sink::Json(
                JsonWriterBuilder::new()
                    .with_explicit_nulls(true)
                    .build::<_, JsonArray>(out()),
            ),
            Format::Jsonl => Sink::Jsonl(
                JsonWriterBuilder::new()
                    .with_explicit_nulls(true)
                    .build::<_, LineDelimited>(out()),
            ),
            Format::Csv => Sink::Csv(CsvWriterBuilder::new().with_header(true).build(out())),
            Format::Arrow => Sink::Arrow(
                arrow::ipc::writer::StreamWriter::try_new(out(), &schema)
                    .map_err(Error::Arrow)?,
            ),
        };
        Ok(Self {
            sink,
            schema,
            bytes,
            broken_pipe,
            max_bytes,
            wrote_any: false,
            buffered_mem: 0,
        })
    }

    /// Write one batch. Returns `Ok(false)` when the byte cap has been
    /// reached and the caller should stop.
    pub fn write(&mut self, batch: &RecordBatch) -> Result<bool> {
        if batch.num_rows() == 0 && self.wrote_any {
            return Ok(true);
        }
        let pipe = &self.broken_pipe;
        match &mut self.sink {
            Sink::Table(buf) => {
                self.buffered_mem += batch.get_array_memory_size() as u64;
                buf.push(batch.clone());
            }
            Sink::Json(w) => w.write(batch).map_err(|e| map_write_err(pipe, e))?,
            Sink::Jsonl(w) => w.write(batch).map_err(|e| map_write_err(pipe, e))?,
            Sink::Csv(w) => w.write(batch).map_err(|e| map_write_err(pipe, e))?,
            Sink::Arrow(w) => w.write(batch).map_err(|e| map_write_err(pipe, e))?,
        }
        self.wrote_any = true;
        Ok(match self.max_bytes {
            Some(cap) => self.produced_bytes() < cap,
            None => true,
        })
    }

    /// Bytes produced so far (in-memory proxy for the buffered table format).
    fn produced_bytes(&self) -> u64 {
        match self.sink {
            Sink::Table(_) => self.buffered_mem,
            _ => self.bytes.load(Ordering::Relaxed),
        }
    }

    pub fn finish(mut self) -> Result<()> {
        let pipe = self.broken_pipe.clone();
        // A schema-only result must still identify itself: header for csv,
        // `[]` handled by the json writer, schema message by the IPC writer.
        if !self.wrote_any {
            if let Sink::Csv(w) = &mut self.sink {
                w.write(&RecordBatch::new_empty(self.schema.clone()))
                    .map_err(|e| map_write_err(&pipe, e))?;
            }
        }
        match self.sink {
            Sink::Table(batches) => {
                let display = arrow::util::pretty::pretty_format_batches(&batches)
                    .map_err(Error::Arrow)?;
                let mut stdout = std::io::stdout().lock();
                writeln!(stdout, "{display}").map_err(|e| Error::io("stdout", e))?;
            }
            Sink::Json(mut w) => {
                w.finish().map_err(|e| map_write_err(&pipe, e))?;
                let mut out = w.into_inner();
                writeln!(out).map_err(|e| Error::io("stdout", e))?;
            }
            Sink::Jsonl(mut w) => {
                w.finish().map_err(|e| map_write_err(&pipe, e))?;
            }
            Sink::Csv(w) => drop(w),
            Sink::Arrow(mut w) => {
                w.finish().map_err(|e| map_write_err(&pipe, e))?;
            }
        }
        Ok(())
    }
}

/// Restore the io error kind that arrow's writers may have flattened away, so
/// EPIPE stays recognizable to [`is_broken_pipe`].
fn map_write_err(broken_pipe: &AtomicBool, e: arrow::error::ArrowError) -> Error {
    if broken_pipe.load(Ordering::Relaxed) {
        Error::io(
            "stdout",
            std::io::Error::new(std::io::ErrorKind::BrokenPipe, e.to_string()),
        )
    } else {
        Error::Arrow(e)
    }
}

/// Render fully-collected batches (sample, plan previews).
pub fn write_batches(batches: &[RecordBatch], schema: &SchemaRef, format: Format) -> Result<()> {
    let mut w = BatchWriter::new(format, schema.clone(), None)?;
    for b in batches {
        w.write(b)?;
    }
    w.finish()
}

/// Serialize any Serialize value per the chosen format (metadata commands).
/// `table` renders as pretty JSON too — metadata is naturally nested.
pub fn write_value<T: serde::Serialize>(value: &T, format: Format) -> Result<()> {
    let mut stdout = std::io::stdout().lock();
    match format {
        Format::Jsonl => {
            writeln!(stdout, "{}", serde_json::to_string(value)?)
                .map_err(|e| Error::io("stdout", e))?;
        }
        _ => {
            writeln!(stdout, "{}", serde_json::to_string_pretty(value)?)
                .map_err(|e| Error::io("stdout", e))?;
        }
    }
    Ok(())
}

/// The machine-readable error envelope, written to stderr.
pub fn write_error(err: &Error) {
    let envelope = serde_json::json!({
        "code": err.code(),
        "message": err.to_string(),
        "retryable": err.retryable(),
        "hint": err.hint(),
    });
    let mut stderr = std::io::stderr().lock();
    let _ = writeln!(stderr, "{envelope}");
}

/// True when the error is stdout closing under us (e.g. `… | head`): the
/// conventional response is a quiet, successful exit, not an error envelope.
pub fn is_broken_pipe(err: &Error) -> bool {
    match err {
        Error::Io { source, .. } => source.kind() == std::io::ErrorKind::BrokenPipe,
        Error::Arrow(arrow::error::ArrowError::IoError(_, source)) => {
            source.kind() == std::io::ErrorKind::BrokenPipe
        }
        _ => false,
    }
}

/// Coarse progress reporting on stderr for long operations. Silent unless
/// stderr is a TTY, so piped/scripted runs stay clean.
pub struct Progress {
    enabled: bool,
    label: &'static str,
    last: Instant,
    printed: bool,
}

impl Progress {
    pub fn start(label: &'static str) -> Self {
        Self {
            enabled: std::io::stderr().is_terminal(),
            label,
            last: Instant::now(),
            printed: false,
        }
    }

    /// Update the progress line, rate-limited to every 100 ms.
    pub fn update(&mut self, rows: u64) {
        if !self.enabled {
            return;
        }
        if self.printed && self.last.elapsed() < Duration::from_millis(100) {
            return;
        }
        eprint!("\r{}: {} rows…", self.label, rows);
        let _ = std::io::stderr().flush();
        self.last = Instant::now();
        self.printed = true;
    }

    /// Clear the progress line.
    pub fn finish(&mut self) {
        if self.enabled && self.printed {
            eprint!("\r\x1b[2K");
            let _ = std::io::stderr().flush();
            self.printed = false;
        }
    }
}

impl Drop for Progress {
    fn drop(&mut self) {
        self.finish();
    }
}
