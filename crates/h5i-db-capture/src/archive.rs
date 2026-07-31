//! The on-disk envelope: one JSON object per line, arrival stamp first.
//!
//! The shape is Hyperliquid's, on purpose. `h5i-db-venues` already reads it
//! (`hyperliquid::read_archive_lz4`), and a format with one reader is worth
//! more than a format tuned to one venue.
//!
//! ```json
//! {"time":"2026-07-31T14:03:11.482913770","ver_num":1,"raw":{ … }}
//! ```
//!
//! `time` is when *this process* saw the frame, not when the venue says it
//! happened; `raw` is the payload verbatim.

use serde_json::Value;

/// Envelope version. Bump only if `time`/`raw` change meaning, since a
/// reader keys its interpretation off this and old files never get rewritten.
pub const ARCHIVE_VERSION: u64 = 1;

/// Nanoseconds since the Unix epoch, right now.
///
/// Wall clock rather than a monotonic instant: a backtest needs an absolute
/// epoch to join against venue data, and no monotonic clock provides one. The
/// cost is that an NTP step can make two lines non-monotonic, which is why
/// readers must sort rather than assume file order.
pub fn now_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.as_nanos() as u64)
        .unwrap_or_default()
}

/// Format an arrival stamp the way the archives do: naive UTC, nine digits.
///
/// Byte-identical to `hyperliquid::format_archive_time`, so a recording and a
/// vendor download compare equal rather than merely parse the same.
pub fn format_archive_time(received_at: u64) -> String {
    let seconds = (received_at / 1_000_000_000) as i64;
    let nanos = (received_at % 1_000_000_000) as u32;
    let stamp = chrono::DateTime::from_timestamp(seconds, nanos).unwrap_or_default();
    format!("{}{:09}", stamp.format("%Y-%m-%dT%H:%M:%S."), nanos)
}

/// Wrap one websocket text frame for the archive.
///
/// Infallible by design. A frame that is not JSON is stored as a JSON
/// *string* rather than rejected: the recorder's job is to lose nothing, and
/// a venue that starts emitting an unparseable heartbeat must not be able to
/// end the capture. Readers see a string where they expected an object and
/// skip it, which is a diagnosable outcome; a dropped line is not.
pub fn archive_line(received_at: u64, payload: &str) -> String {
    let raw = serde_json::from_str::<Value>(payload)
        .unwrap_or_else(|_| Value::String(payload.to_string()));
    envelope(received_at, raw)
}

/// A recorder-generated line: connection lifecycle, not market data.
///
/// Shaped as a Hyperliquid-style `{channel, data}` body so existing readers
/// classify it as a channel they do not model (skipped, counted) rather than
/// as a corrupt line. The channel name is deliberately not a venue channel.
pub fn marker_line(received_at: u64, event: &str, data: Value) -> String {
    let mut body = serde_json::Map::new();
    body.insert("event".to_string(), Value::String(event.to_string()));
    if let Value::Object(extra) = data {
        body.extend(extra);
    }
    envelope(
        received_at,
        serde_json::json!({ "channel": MARKER_CHANNEL, "data": Value::Object(body) }),
    )
}

/// The channel name reserved for recorder markers.
pub const MARKER_CHANNEL: &str = "h5iCapture";

fn envelope(received_at: u64, raw: Value) -> String {
    // Key order matters: with serde_json's `preserve_order` the emitted bytes
    // follow insertion order, and matching Hyperliquid's order is what makes
    // the byte-equality test below meaningful.
    serde_json::json!({
        "time": format_archive_time(received_at),
        "ver_num": ARCHIVE_VERSION,
        "raw": raw,
    })
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lines_are_byte_identical_to_the_hyperliquid_archive_writer() {
        // The whole reason this crate copies the envelope instead of owning
        // one: if these ever diverge, files we record stop being readable by
        // the archive reader and nobody finds out until replay.
        let received_at = 1_735_689_602_238_437_296u64;
        let payload = r#"{"channel":"trades","data":[{"coin":"BTC","px":"1.5"}]}"#;
        let theirs = h5i_db_venues::hyperliquid::archive_line(
            // `UnixNanos` is signed; a wall clock arrival stamp never is, so
            // the cast is only a type change, not a range one.
            h5i_db_backtest::types::UnixNanos::new(received_at as i64),
            payload,
        )
        .unwrap();
        assert_eq!(archive_line(received_at, payload), theirs);
    }

    #[test]
    fn a_non_json_frame_is_stored_rather_than_dropped() {
        let line = archive_line(1_000_000_000, "PONG");
        let parsed: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(parsed["raw"], Value::String("PONG".to_string()));
        assert_eq!(parsed["ver_num"], 1);
    }

    #[test]
    fn markers_carry_their_fields_on_a_channel_no_venue_uses() {
        let line = marker_line(0, "reconnect", serde_json::json!({ "attempt": 3 }));
        let parsed: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(parsed["raw"]["channel"], MARKER_CHANNEL);
        assert_eq!(parsed["raw"]["data"]["event"], "reconnect");
        assert_eq!(parsed["raw"]["data"]["attempt"], 3);
    }

    #[test]
    fn stamps_keep_nanosecond_digits_that_a_millisecond_format_would_round_away() {
        assert_eq!(
            format_archive_time(1_735_689_602_238_437_296),
            "2025-01-01T00:00:02.238437296"
        );
    }
}
