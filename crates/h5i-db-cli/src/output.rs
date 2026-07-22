//! Output rendering: table (human), json, jsonl, csv, arrow IPC stream.
//! Machine formats write to stdout; the error envelope goes to stderr.

use std::io::Write;

use arrow::array::RecordBatch;
use arrow::csv::WriterBuilder as CsvWriterBuilder;
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

pub fn write_batches(batches: &[RecordBatch], format: Format) -> Result<()> {
    let mut stdout = std::io::stdout().lock();
    match format {
        Format::Table => {
            let display =
                arrow::util::pretty::pretty_format_batches(batches).map_err(Error::Arrow)?;
            writeln!(stdout, "{display}").map_err(|e| Error::io("stdout", e))?;
        }
        Format::Json => {
            let mut w = JsonWriterBuilder::new()
                .with_explicit_nulls(true)
                .build::<_, JsonArray>(&mut stdout);
            for b in batches {
                w.write(b).map_err(Error::Arrow)?;
            }
            w.finish().map_err(Error::Arrow)?;
            writeln!(stdout).map_err(|e| Error::io("stdout", e))?;
        }
        Format::Jsonl => {
            let mut w = JsonWriterBuilder::new()
                .with_explicit_nulls(true)
                .build::<_, LineDelimited>(&mut stdout);
            for b in batches {
                w.write(b).map_err(Error::Arrow)?;
            }
            w.finish().map_err(Error::Arrow)?;
        }
        Format::Csv => {
            let mut w = CsvWriterBuilder::new().with_header(true).build(&mut stdout);
            for b in batches {
                w.write(b).map_err(Error::Arrow)?;
            }
        }
        Format::Arrow => {
            if batches.is_empty() {
                return Ok(());
            }
            let mut w =
                arrow::ipc::writer::StreamWriter::try_new(&mut stdout, &batches[0].schema())
                    .map_err(Error::Arrow)?;
            for b in batches {
                w.write(b).map_err(Error::Arrow)?;
            }
            w.finish().map_err(Error::Arrow)?;
        }
    }
    Ok(())
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
    eprintln!("{envelope}");
}
