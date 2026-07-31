//! Record a venue websocket to disk, losslessly.
//!
//! # Why this is not in `h5i-db-venues`
//!
//! That crate is deliberately parse-only: every function there takes bytes
//! the caller already downloaded, which is what makes the whole mapping a
//! pure function testable offline against recorded payloads. A socket, a
//! credential and a reconnect policy would end that. So the network client
//! lives here instead, and the two crates meet at a file format rather than
//! at a function call.
//!
//! # Why record at all
//!
//! Kalshi publishes no historical order-book deltas. Third-party archives
//! exist, but the best free one lags by weeks and its snapshot rows carry no
//! venue timestamp at all, so queue position cannot be reconstructed from
//! them. Anyone who needs queue-accurate, current Kalshi data has to capture
//! it as it happens. There is no way to obtain it later.
//!
//! # The two rules of the write path
//!
//! **Stamp arrival, in nanoseconds.** The instant the process saw a frame is
//! the only clock a backtest can honestly order on: venue timestamps are the
//! venue's opinion about a moment you could not act on, and they arrive out
//! of order. An arrival stamp cannot be reconstructed after the fact, so a
//! capture that omits it is permanently broken.
//!
//! **Never normalise.** The payload goes to disk exactly as it came off the
//! socket. Parsing on the write path means a parser bug costs you the data
//! rather than an afternoon: you cannot re-run the recorder over yesterday.
//!
//! # The format
//!
//! One lz4 frame of newline-delimited JSON per hour, matching what
//! `h5i_db_venues::hyperliquid::read_archive_lz4` already reads, so a
//! recording and a vendor download go through the same reader.
//!
//! ```text
//! <out>/<venue>/<YYYY-MM-DD>/<HH>.ndjson.lz4
//! <out>/<venue>/<YYYY-MM-DD>/<HH>.001.ndjson.lz4   (restart within the hour)
//! ```
//!
//! See [`archive`] for the envelope and [`writer`] for the file handling,
//! including why a restart opens a new part instead of appending.

pub mod archive;
pub mod venue;
pub mod writer;

pub use archive::{ARCHIVE_VERSION, archive_line, format_archive_time, marker_line, now_nanos};
pub use venue::{Keepalive, Venue};
pub use writer::CaptureWriter;
