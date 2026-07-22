//! Tailing / streaming reads (ROADMAP §4.5).
//!
//! Everything in h5i-db is snapshot-bound: a resolved version never changes
//! under a reader. Tailing therefore composes two primitives instead of
//! introducing mutable state:
//!
//! - [`Database::wait_for_version`] — poll the head until it advances past a
//!   known sequence (HEAD reads are one small file / conditional GET, so
//!   polling is cheap);
//! - [`Database::diff_scan`] — fetch exactly the appended rows.
//!
//! The CLI's `tail` command and the Python `tail()` iterator are thin loops
//! over these. Non-append versions (rewrite/compact) surface as
//! `Unsupported` from `diff_scan`; tailers handle that by re-anchoring on
//! the current head (documented in the consumers).

use std::time::{Duration, Instant};

use crate::database::Database;
use crate::error::{Error, Result};

/// Outcome of one wait.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TailEvent {
    /// The head advanced to this sequence (> the awaited sequence).
    Advanced(u64),
    /// The timeout elapsed with no new version.
    TimedOut,
}

impl Database {
    /// Block until `name`'s head sequence exceeds `after`, or until
    /// `timeout` elapses. Polls every `poll_interval` (clamped to ≥ 10 ms).
    pub async fn wait_for_version(
        &self,
        name: &str,
        after: u64,
        poll_interval: Duration,
        timeout: Duration,
    ) -> Result<TailEvent> {
        let interval = poll_interval.max(Duration::from_millis(10));
        let start = Instant::now();
        loop {
            let entry = crate::catalog::load_entry(self.backend(), name)
                .await?
                .ok_or_else(|| Error::TableNotFound { name: name.into() })?;
            if let Some(state) = self.backend().heads.read(entry.table_id).await? {
                if state.head.sequence > after {
                    return Ok(TailEvent::Advanced(state.head.sequence));
                }
            }
            if start.elapsed() >= timeout {
                return Ok(TailEvent::TimedOut);
            }
            tokio::time::sleep(interval.min(timeout.saturating_sub(start.elapsed()))).await;
        }
    }
}
