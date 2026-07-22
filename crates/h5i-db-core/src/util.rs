//! Checksums, schema (de)serialization, and small shared helpers.

use arrow::datatypes::{Schema, SchemaRef};
use base64::Engine;

use crate::error::{Error, Result};

/// blake3 hex digest of raw bytes; used for both integrity checksums and
/// content-address deduplication.
pub fn checksum_hex(bytes: &[u8]) -> String {
    blake3::hash(bytes).to_hex().to_string()
}

/// Serialize an Arrow schema to base64-encoded IPC bytes for embedding in
/// JSON metadata. IPC is the canonical Arrow schema encoding: it round-trips
/// every type (including nested, dictionary, decimal, and timezone metadata)
/// that a JSON re-implementation would risk getting wrong. Encoded as an
/// empty IPC stream carrying only the schema message.
pub fn schema_to_b64(schema: &Schema) -> String {
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut buf, schema)
            .expect("in-memory IPC stream write cannot fail");
        writer.finish().expect("in-memory IPC finish cannot fail");
    }
    base64::engine::general_purpose::STANDARD.encode(&buf)
}

/// Inverse of [`schema_to_b64`].
pub fn schema_from_b64(b64: &str) -> Result<SchemaRef> {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(b64)
        .map_err(|e| Error::corruption("schema", format!("invalid base64: {e}")))?;
    let reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
        .map_err(|e| Error::corruption("schema", format!("invalid IPC stream: {e}")))?;
    Ok(reader.schema())
}

/// Monotonic commit timestamp: `max(wall_clock, parent + 1ns)`.
///
/// Keeps the committed chain strictly increasing even when the client clock
/// jumps backwards, which is what makes `as_of(ts)` binary search valid.
pub fn monotonic_commit_ts(parent_committed_at_ns: Option<i64>) -> i64 {
    let wall = chrono::Utc::now()
        .timestamp_nanos_opt()
        .unwrap_or(i64::MAX - 1);
    match parent_committed_at_ns {
        Some(p) => wall.max(p.saturating_add(1)),
        None => wall,
    }
}

/// Human-readable byte counts for logs and CLI output.
pub fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 6] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, TimeUnit};

    #[test]
    fn schema_round_trips_with_metadata() {
        let schema = Schema::new(vec![
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
            Field::new("symbol", DataType::Utf8, false),
            Field::new("price", DataType::Decimal128(18, 6), true),
            Field::new("size", DataType::UInt64, true),
        ]);
        let b64 = schema_to_b64(&schema);
        let back = schema_from_b64(&b64).unwrap();
        assert_eq!(back.as_ref(), &schema);
    }

    #[test]
    fn commit_ts_is_monotonic() {
        let t1 = monotonic_commit_ts(None);
        let t2 = monotonic_commit_ts(Some(t1));
        assert!(t2 > t1);
        // parent in the "future" (clock skew): still monotonic
        let future = t2 + 1_000_000_000;
        let t3 = monotonic_commit_ts(Some(future));
        assert_eq!(t3, future + 1);
    }
}
