//! Fuzz the CSV ingest parse path. Mirrors `h5i-db-cli/src/ingest.rs`
//! (schema inference over the raw bytes, then a full batch read with the
//! inferred schema) — arbitrary CSV bytes must error cleanly, never panic.
#![no_main]

use std::io::Cursor;
use std::sync::Arc;

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let cursor = Cursor::new(data);
    let format = arrow::csv::reader::Format::default().with_header(true);
    let Ok((schema, _)) = format.infer_schema(cursor.clone(), Some(1_000)) else {
        return;
    };
    let Ok(reader) = arrow::csv::ReaderBuilder::new(Arc::new(schema))
        .with_header(true)
        .build(cursor)
    else {
        return;
    };
    for batch in reader {
        if batch.is_err() {
            break;
        }
    }
});
