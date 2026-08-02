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
                Some(table) => {
                    // Header pass streams the file; reopen for the reader.
                    let file = File::open(path).map_err(|e| Error::io(path, e))?;
                    csv_schema_from_header(BufReader::new(file), &table)?
                }
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
            // The header pass borrows the payload rather than cloning the
            // cursor: `Cursor<Vec<u8>>::clone` copies the whole `Vec`, which
            // is a second copy of stdin the module doc says we do not make.
            let schema = match schema_hint {
                Some(table) => csv_schema_from_header(std::io::Cursor::new(&bytes), &table)?,
                None => {
                    let (inferred, _) = arrow::csv::reader::Format::default()
                        .with_header(true)
                        .infer_schema(std::io::Cursor::new(&bytes), Some(10_000))
                        .map_err(Error::Arrow)?;
                    Arc::new(inferred)
                }
            };
            let reader = arrow::csv::ReaderBuilder::new(schema.clone())
                .with_header(true)
                .build(std::io::Cursor::new(bytes))
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
    if let Some(s) = text_prefix
        && (s.contains(',') || s.contains('\n'))
    {
        return Ok(InputFormat::Csv);
    }
    Err(Error::invalid(
        "cannot auto-detect the stdin input format; pass --input-format parquet|csv|arrow",
    ))
}

/// The reader schema for a CSV that is being ingested into a known table.
///
/// The arrow CSV reader applies a supplied schema **positionally** and skips
/// the header row, so handing it the table schema reads `price,size` into a
/// file written `size,price` without a word. Nothing downstream can catch it
/// either: the batch already carries the table's schema, so core's name check
/// compares that schema with itself and passes.
///
/// So the header decides the *order* and the table decides the *types*: each
/// header name is looked up in the table and the table's own field is used,
/// which keeps CSV parsing byte-identical to before for a file whose columns
/// are already in table order. A header naming a column the table does not
/// have is refused here rather than guessed at.
fn csv_schema_from_header(reader: impl Read, table: &SchemaRef) -> Result<SchemaRef> {
    let (header, _) = arrow::csv::reader::Format::default()
        .with_header(true)
        .infer_schema(reader, Some(1))
        .map_err(Error::Arrow)?;
    if header
        .fields()
        .iter()
        .map(|f| f.name())
        .eq(table.fields().iter().map(|f| f.name()))
    {
        return Ok(table.clone());
    }
    let mut fields = Vec::with_capacity(header.fields().len());
    for field in header.fields() {
        let index = match match_column(table, field.name()) {
            ColumnMatch::At(i) => i,
            ColumnMatch::Missing => {
                return Err(Error::invalid(format!(
                    "CSV column {:?} is not a column of the table; the table has [{}]",
                    field.name(),
                    column_names(table)
                )));
            }
            ColumnMatch::Ambiguous(candidates) => {
                return Err(Error::invalid(format!(
                    "CSV column {:?} matches more than one table column when case \
                     is ignored ([{candidates}]); rename the header to match one exactly",
                    field.name(),
                )));
            }
        };
        // The table's field, so the reader labels the column the way the
        // table spells it and the batch needs no later renaming.
        fields.push(table.field(index).clone());
    }
    Ok(Arc::new(arrow::datatypes::Schema::new(fields)))
}

/// Where a name landed when resolved against a schema.
enum ColumnMatch {
    At(usize),
    Missing,
    /// Several fields differ from `name` only by case, so folding would have
    /// to pick one arbitrarily.
    Ambiguous(String),
}

/// Resolve a column by name: exactly first, then ignoring ASCII case.
///
/// SQL folds unquoted identifiers to lowercase, so a table created as
/// `ts,symbol,price` reads back that way and a user has no reason to think a
/// file headed `TS,SYMBOL,PRICE` describes different columns. Before the
/// by-name rewrite the positional zip loaded such a file correctly, so an
/// exact-match-only lookup silently broke working files.
///
/// An exact match always wins, so a schema that genuinely distinguishes `ts`
/// from `TS` keeps its old behaviour; only a fold with more than one
/// candidate is refused, and it says so.
fn match_column(schema: &SchemaRef, name: &str) -> ColumnMatch {
    if let Some((index, _)) = schema.column_with_name(name) {
        return ColumnMatch::At(index);
    }
    let folded: Vec<usize> = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| f.name().eq_ignore_ascii_case(name))
        .map(|(i, _)| i)
        .collect();
    match folded.as_slice() {
        [] => ColumnMatch::Missing,
        [only] => ColumnMatch::At(*only),
        many => ColumnMatch::Ambiguous(
            many.iter()
                .map(|i| schema.field(*i).name().as_str())
                .collect::<Vec<_>>()
                .join(", "),
        ),
    }
}

fn column_names(schema: &SchemaRef) -> String {
    schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

/// Coerce an ingested batch to the table schema where the difference is purely
/// representational (e.g. CSV inference produced Timestamp without timezone,
/// or non-nullable data typed as nullable). Real mismatches still error in
/// the core validation.
///
/// Columns are matched by **name**. Matching them by position, as this once
/// did, is silent data corruption rather than a mismatch: a file whose
/// same-typed columns are in a different order is cast column-for-column and
/// re-labelled with the table's schema, so the values land in the wrong
/// columns and every later check sees a batch that already claims to be the
/// table's own shape.
pub fn align_batch(batch: RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    if batch.schema() == *schema {
        return Ok(batch);
    }
    if batch.num_columns() != schema.fields().len() {
        return Ok(batch); // let core produce the precise error
    }
    let incoming = batch.schema();
    let mut columns = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let index = match match_column(&incoming, field.name()) {
            ColumnMatch::At(i) => i,
            ColumnMatch::Missing => {
                return Err(Error::invalid(format!(
                    "input has no column named {:?}; the table's columns are [{}] \
                     and the input's are [{}]",
                    field.name(),
                    column_names(schema),
                    column_names(&incoming)
                )));
            }
            ColumnMatch::Ambiguous(candidates) => {
                return Err(Error::invalid(format!(
                    "table column {:?} matches more than one input column when case \
                     is ignored ([{candidates}]); rename the input columns to match exactly",
                    field.name(),
                )));
            }
        };
        let col = batch.column(index);
        if col.data_type() == field.data_type() {
            columns.push(col.clone());
        } else {
            columns.push(arrow::compute::cast(col, field.data_type()).map_err(Error::Arrow)?);
        }
    }
    RecordBatch::try_new(schema.clone(), columns).map_err(Error::Arrow)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};

    fn table() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("price", DataType::Float64, false),
            Field::new("size", DataType::Float64, false),
        ]))
    }

    #[test]
    fn a_reordered_input_is_reordered_not_relabelled() {
        // Same names, same types, different order. Zipping by position stored
        // this swapped and nothing downstream could tell, because the batch
        // came out wearing the table's schema.
        let incoming = Arc::new(Schema::new(vec![
            Field::new("size", DataType::Float64, false),
            Field::new("price", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            incoming,
            vec![
                Arc::new(Float64Array::from(vec![7.0])),
                Arc::new(Float64Array::from(vec![101.5])),
            ],
        )
        .unwrap();

        let aligned = align_batch(batch, &table()).unwrap();
        assert_eq!(aligned.schema(), table());
        let price = aligned
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let size = aligned
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(price.value(0), 101.5, "price is the price column");
        assert_eq!(size.value(0), 7.0, "size is the size column");
    }

    #[test]
    fn an_uppercase_header_still_loads() {
        // `TS,SYMBOL,PRICE` into `ts,symbol,price` loaded before the by-name
        // rewrite (positional zip, right order) and must keep loading: SQL
        // folds unquoted identifiers, so the user's model is case-insensitive.
        let incoming = Arc::new(Schema::new(vec![
            Field::new("SIZE", DataType::Float64, false),
            Field::new("PRICE", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            incoming,
            vec![
                Arc::new(Float64Array::from(vec![7.0])),
                Arc::new(Float64Array::from(vec![101.5])),
            ],
        )
        .unwrap();

        let aligned = align_batch(batch, &table()).unwrap();
        assert_eq!(aligned.schema(), table());
        let price = aligned
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let size = aligned
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        // Folded by name, so the reorder still applies.
        assert_eq!(price.value(0), 101.5);
        assert_eq!(size.value(0), 7.0);
    }

    #[test]
    fn an_exact_match_wins_over_a_folded_one() {
        let mixed: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("PRICE", DataType::Float64, false),
            Field::new("price", DataType::Float64, false),
        ]));
        match match_column(&mixed, "price") {
            ColumnMatch::At(i) => assert_eq!(i, 1, "the exactly-spelled field"),
            _ => panic!("exact match should not be ambiguous"),
        }
    }

    #[test]
    fn a_fold_with_two_candidates_is_refused_and_says_why() {
        let mixed: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("Price", DataType::Float64, false),
            Field::new("PRICE", DataType::Float64, false),
        ]));
        match match_column(&mixed, "price") {
            ColumnMatch::Ambiguous(candidates) => {
                assert!(candidates.contains("Price") && candidates.contains("PRICE"));
            }
            _ => panic!("two case-only variants are ambiguous"),
        }
    }

    #[test]
    fn a_csv_header_in_another_case_maps_onto_the_table_fields() {
        let csv = "SIZE,PRICE\n7.0,101.5\n";
        let schema =
            csv_schema_from_header(std::io::Cursor::new(csv.as_bytes()), &table()).unwrap();
        // Table spellings and table types, in the file's order.
        assert_eq!(
            schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>(),
            vec!["size", "price"]
        );
    }

    #[test]
    fn a_column_the_table_does_not_have_is_named_in_the_error() {
        let incoming = Arc::new(Schema::new(vec![
            Field::new("price", DataType::Float64, false),
            Field::new("quantity", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            incoming,
            vec![
                Arc::new(Float64Array::from(vec![1.0])),
                Arc::new(Float64Array::from(vec![2.0])),
            ],
        )
        .unwrap();
        let error = align_batch(batch, &table()).unwrap_err().to_string();
        assert!(error.contains("size"), "{error}");
        assert!(error.contains("quantity"), "{error}");
    }

    #[test]
    fn a_matching_input_still_casts_representational_differences() {
        // The reason this function exists: CSV inference types an integer
        // column Int64 where the table wants Float64.
        let incoming = Arc::new(Schema::new(vec![
            Field::new("price", DataType::Int64, false),
            Field::new("size", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            incoming,
            vec![
                Arc::new(Int64Array::from(vec![101])),
                Arc::new(Float64Array::from(vec![7.0])),
            ],
        )
        .unwrap();
        let aligned = align_batch(batch, &table()).unwrap();
        assert_eq!(aligned.schema(), table());
        assert_eq!(
            aligned
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            101.0
        );
    }

    #[test]
    fn a_csv_header_decides_the_column_order_and_the_table_the_types() {
        // A header already in table order changes nothing at all.
        let in_order = "price,size\n1.5,2\n";
        assert_eq!(
            csv_schema_from_header(in_order.as_bytes(), &table()).unwrap(),
            table()
        );

        // A reordered header is honoured, with the table's types, so the
        // reader parses the file it was actually given.
        let swapped = "size,price\n2,1.5\n";
        let schema = csv_schema_from_header(swapped.as_bytes(), &table()).unwrap();
        assert_eq!(schema.field(0).name(), "size");
        assert_eq!(schema.field(1).name(), "price");
        assert_eq!(schema.field(0).data_type(), &DataType::Float64);

        // A header naming a column the table does not have is refused rather
        // than quietly read into whichever column shares its position.
        let unknown = "price,quantity\n1.5,2\n";
        let error = csv_schema_from_header(unknown.as_bytes(), &table())
            .unwrap_err()
            .to_string();
        assert!(error.contains("quantity"), "{error}");
    }
}
