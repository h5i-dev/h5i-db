//! Reading input data for ingest: Parquet / CSV / Arrow IPC, from a file or
//! stdin (`-`).

use std::io::Read;
use std::path::Path;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use h5i_db_core::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum InputFormat {
    /// Guess from the file extension (parquet/csv/arrow); parquet for stdin.
    Auto,
    Parquet,
    Csv,
    /// Arrow IPC stream.
    Arrow,
}

pub fn read_input(
    path: &str,
    format: InputFormat,
    schema_hint: Option<SchemaRef>,
) -> Result<Vec<RecordBatch>> {
    let bytes: Vec<u8> = if path == "-" {
        let mut buf = Vec::new();
        std::io::stdin()
            .lock()
            .read_to_end(&mut buf)
            .map_err(|e| Error::io("stdin", e))?;
        buf
    } else {
        std::fs::read(path).map_err(|e| Error::io(path, e))?
    };

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
            let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                bytes::Bytes::from(bytes),
            )
            .map_err(Error::Parquet)?
            .build()
            .map_err(Error::Parquet)?;
            reader
                .into_iter()
                .map(|b| b.map_err(Error::Arrow))
                .collect()
        }
        InputFormat::Csv => {
            let cursor = std::io::Cursor::new(bytes);
            let schema = match schema_hint {
                Some(s) => s,
                None => {
                    // Infer from the data.
                    let (inferred, _) = arrow::csv::reader::Format::default()
                        .with_header(true)
                        .infer_schema(cursor.clone(), Some(10_000))
                        .map_err(Error::Arrow)?;
                    Arc::new(inferred)
                }
            };
            let reader = arrow::csv::ReaderBuilder::new(schema)
                .with_header(true)
                .build(cursor)
                .map_err(Error::Arrow)?;
            reader
                .into_iter()
                .map(|b| b.map_err(Error::Arrow))
                .collect()
        }
        InputFormat::Arrow => {
            let reader =
                arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
                    .map_err(Error::Arrow)?;
            reader
                .into_iter()
                .map(|b| b.map_err(Error::Arrow))
                .collect()
        }
        InputFormat::Auto => unreachable!("resolved above"),
    }
}

/// Coerce ingested batches to the table schema where the difference is purely
/// representational (e.g. CSV inference produced Timestamp without timezone,
/// or non-nullable data typed as nullable). Real mismatches still error in
/// the core validation.
pub fn align_to_schema(batches: Vec<RecordBatch>, schema: &SchemaRef) -> Result<Vec<RecordBatch>> {
    batches
        .into_iter()
        .map(|batch| {
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
        })
        .collect()
}
