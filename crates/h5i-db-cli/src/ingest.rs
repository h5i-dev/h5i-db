//! Reading input data for ingest: Parquet / CSV / Arrow IPC, from a file or
//! stdin (`-`).
//!
//! Files are decoded through streaming per-batch readers so ingest never
//! holds a whole file as raw bytes. Stdin is buffered once (Parquet needs
//! random access and format sniffing needs a peek) and then decoded
//! batch-by-batch from that buffer.

use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use h5i_db_core::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum InputFormat {
    /// Guess from the file extension (parquet/csv/arrow); sniffed from the
    /// leading bytes for stdin.
    Auto,
    Parquet,
    Csv,
    /// Arrow IPC stream.
    Arrow,
}

/// A streaming batch source with its schema known up front.
pub struct InputReader {
    pub schema: SchemaRef,
    iter: Box<dyn Iterator<Item = Result<RecordBatch>>>,
}

impl Iterator for InputReader {
    type Item = Result<RecordBatch>;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

/// Open `path` (or stdin for `-`) as a stream of record batches.
pub fn open_input(
    path: &str,
    format: InputFormat,
    schema_hint: Option<SchemaRef>,
) -> Result<InputReader> {
    if path == "-" {
        let mut buf = Vec::new();
        std::io::stdin()
            .lock()
            .read_to_end(&mut buf)
            .map_err(|e| Error::io("stdin", e))?;
        let format = match format {
            InputFormat::Auto => sniff_format(&buf)?,
            other => other,
        };
        return open_buffered(buf, format, schema_hint);
    }

    let format = match format {
        InputFormat::Auto => {
            let ext = Path::new(path)
                .extension()
                .and_then(|e| e.to_str())
                .unwrap_or("")
                .to_ascii_lowercase();
            match ext.as_str() {
                "csv" => InputFormat::Csv,
                "arrow" | "arrows" | "ipc" => InputFormat::Arrow,
                _ => InputFormat::Parquet,
            }
        }
        other => other,
    };

    match format {
        InputFormat::Parquet => {
            let file = File::open(path).map_err(|e| Error::io(path, e))?;
            let builder =
                parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
                    .map_err(Error::Parquet)?;
            let schema = builder.schema().clone();
            let reader = builder.build().map_err(Error::Parquet)?;
            Ok(InputReader {
                schema,
                iter: Box::new(reader.map(|b| b.map_err(Error::Arrow))),
            })
        }
        InputFormat::Csv => {
            let schema = match schema_hint {
                Some(s) => s,
                None => {
                    // Inference pass streams the file; reopen for the reader.
                    let file = File::open(path).map_err(|e| Error::io(path, e))?;
                    let (inferred, _) = arrow::csv::reader::Format::default()
                        .with_header(true)
                        .infer_schema(BufReader::new(file), Some(10_000))
                        .map_err(Error::Arrow)?;
                    Arc::new(inferred)
                }
            };
            let file = File::open(path).map_err(|e| Error::io(path, e))?;
            let reader = arrow::csv::ReaderBuilder::new(schema.clone())
                .with_header(true)
                .build(BufReader::new(file))
                .map_err(Error::Arrow)?;
            Ok(InputReader {
                schema,
                iter: Box::new(reader.map(|b| b.map_err(Error::Arrow))),
            })
        }
        InputFormat::Arrow => {
            let file = File::open(path).map_err(|e| Error::io(path, e))?;
            let reader = arrow::ipc::reader::StreamReader::try_new(BufReader::new(file), None)
                .map_err(Error::Arrow)?;
            Ok(InputReader {
                schema: reader.schema(),
                iter: Box::new(reader.map(|b| b.map_err(Error::Arrow))),
            })
        }
        InputFormat::Auto => unreachable!("resolved above"),
    }
}

/// Decode already-buffered bytes (stdin) batch-by-batch.
fn open_buffered(
    bytes: Vec<u8>,
    format: InputFormat,
    schema_hint: Option<SchemaRef>,
) -> Result<InputReader> {
    match format {
        InputFormat::Parquet => {
            let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                bytes::Bytes::from(bytes),
            )
            .map_err(Error::Parquet)?;
            let schema = builder.schema().clone();
            let reader = builder.build().map_err(Error::Parquet)?;
            Ok(InputReader {
                schema,
                iter: Box::new(reader.map(|b| b.map_err(Error::Arrow))),
            })
        }
        InputFormat::Csv => {
            let cursor = std::io::Cursor::new(bytes);
            let schema = match schema_hint {
                Some(s) => s,
                None => {
                    let (inferred, _) = arrow::csv::reader::Format::default()
                        .with_header(true)
                        .infer_schema(cursor.clone(), Some(10_000))
                        .map_err(Error::Arrow)?;
                    Arc::new(inferred)
                }
            };
            let reader = arrow::csv::ReaderBuilder::new(schema.clone())
                .with_header(true)
                .build(cursor)
                .map_err(Error::Arrow)?;
            Ok(InputReader {
                schema,
                iter: Box::new(reader.map(|b| b.map_err(Error::Arrow))),
            })
        }
        InputFormat::Arrow => {
            let reader =
                arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
                    .map_err(Error::Arrow)?;
            Ok(InputReader {
                schema: reader.schema(),
                iter: Box::new(reader.map(|b| b.map_err(Error::Arrow))),
            })
        }
        InputFormat::Auto => unreachable!("caller resolves Auto"),
    }
}

/// Detect the format of stdin bytes from their leading magic.
fn sniff_format(bytes: &[u8]) -> Result<InputFormat> {
    if bytes.starts_with(b"PAR1") {
        return Ok(InputFormat::Parquet);
    }
    if bytes.starts_with(&[0xFF, 0xFF, 0xFF, 0xFF]) {
        return Ok(InputFormat::Arrow);
    }
    if bytes.starts_with(b"ARROW1") {
        return Err(Error::invalid(
            "stdin is an Arrow IPC *file*; pipe an Arrow IPC *stream* instead \
             (or pass a file path with --input-format arrow)",
        ));
    }
    // CSV: leading bytes are text with a delimiter or line break.
    let head = &bytes[..bytes.len().min(8192)];
    let text_prefix = match std::str::from_utf8(head) {
        Ok(s) => Some(s),
        // A multi-byte char cut at the window edge is still text.
        Err(e) if e.error_len().is_none() && e.valid_up_to() > 0 => {
            std::str::from_utf8(&head[..e.valid_up_to()]).ok()
        }
        Err(_) => None,
    };
    if let Some(s) = text_prefix {
        if s.contains(',') || s.contains('\n') {
            return Ok(InputFormat::Csv);
        }
    }
    Err(Error::invalid(
        "cannot auto-detect the stdin input format; pass --input-format parquet|csv|arrow",
    ))
}

/// Coerce an ingested batch to the table schema where the difference is purely
/// representational (e.g. CSV inference produced Timestamp without timezone,
/// or non-nullable data typed as nullable). Real mismatches still error in
/// the core validation.
pub fn align_batch(batch: RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    if batch.schema() == *schema {
        return Ok(batch);
    }
    if batch.num_columns() != schema.fields().len() {
        return Ok(batch); // let core produce the precise error
    }
    let columns = batch
        .columns()
        .iter()
        .zip(schema.fields())
        .map(|(col, field)| {
            if col.data_type() == field.data_type() {
                Ok(col.clone())
            } else {
                arrow::compute::cast(col, field.data_type()).map_err(Error::Arrow)
            }
        })
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(schema.clone(), columns).map_err(Error::Arrow)
}
