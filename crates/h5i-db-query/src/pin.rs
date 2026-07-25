//! The research pin, as the table functions see it.
//!
//! `--as-of` and `--decision-time` bound a session by swapping what the
//! *catalog* hands out, so `FROM trades` is bounded. The table functions
//! (`h5i()`, `asof_join()`, `gapfill()`, `tail()`, `latest_on()`) do not go
//! through the catalog: each resolves its tables straight from the
//! [`Database`]. Without the guard below they read past the pin, and a jail
//! with a documented door in it is worse than no jail, because it reads like
//! one.
//!
//! Two rules, chosen so there is no partially-enforced middle:
//!
//! * **Arrival axis is applied.** Every table function resolves at the pinned
//!   read point instead of the head, so `--as-of` holds everywhere and the
//!   operators stay usable. Asking a table function for a *different* read
//!   point inside a pinned session is refused rather than silently honoured.
//! * **Event-time axis refuses.** These functions consume their tables
//!   internally rather than returning something the cutoff predicate can be
//!   pushed into, so under `--decision-time` they fail with an error naming
//!   the alternative. Enforcing it for some and not others would leave exactly
//!   the kind of quiet hole this module exists to close.

use datafusion::error::{DataFusionError, Result as DfResult};
use h5i_db_core::ReadAt;

/// The pin a session was opened with, shared with its table functions.
#[derive(Debug, Clone)]
pub(crate) struct PinContext {
    at: ReadAt,
    event_time_cutoff_ns: Option<i64>,
}

impl Default for PinContext {
    fn default() -> Self {
        Self {
            at: ReadAt::Latest,
            event_time_cutoff_ns: None,
        }
    }
}

impl PinContext {
    pub(crate) fn new(at: ReadAt, event_time_cutoff_ns: Option<i64>) -> Self {
        Self {
            at,
            event_time_cutoff_ns,
        }
    }

    /// True when the session constrains nothing.
    fn unpinned(&self) -> bool {
        matches!(self.at, ReadAt::Latest) && self.event_time_cutoff_ns.is_none()
    }

    /// The read point a table function must use. Identical to `ReadAt::Latest`
    /// for an unpinned session, so the ordinary path is unchanged.
    pub(crate) fn read_at(&self) -> ReadAt {
        self.at.clone()
    }

    /// Refuse a table function that cannot honour an event-time cutoff.
    ///
    /// Call this *before* doing any work, so the failure is a plan error
    /// rather than a partially-computed result.
    pub(crate) fn check_usable(&self, func: &str) -> DfResult<()> {
        if self.event_time_cutoff_ns.is_none() {
            return Ok(());
        }
        Err(DataFusionError::Plan(format!(
            "{func}() reads its tables directly and cannot apply this session's event-time \
             cutoff, so it is refused here rather than quietly returning rows from after the \
             decision instant. Reference the table by name instead (plain table reads are \
             bounded), or drop --decision-time to use {func}()"
        )))
    }

    /// Refuse an explicitly requested read point inside a pinned session.
    ///
    /// Comparing an arbitrary request against the pin is not decidable in
    /// general (a snapshot name says nothing about ordering), so the safe rule
    /// is that a pinned session picks the read point and nothing inside it may
    /// choose another.
    pub(crate) fn check_no_explicit_read_point(&self, func: &str) -> DfResult<()> {
        if self.unpinned() {
            return Ok(());
        }
        Err(DataFusionError::Plan(format!(
            "this session is pinned to a read point, so {func}() may not select a different \
             one; drop the second argument to read the pinned version, or run the query in an \
             unpinned session"
        )))
    }
}
