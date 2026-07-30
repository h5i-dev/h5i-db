//! Time windows and coverage accounting.
//!
//! Windows are **half-open**, `[start, end)`, and this module is the only
//! place that decides what a window means. That is a direct lesson from the
//! production prediction-market stack studied for this design: its window
//! semantics were duplicated across book loaders, trade loaders, progress
//! reporting and cache keys, one of them walked calendar days inclusively,
//! and the disagreement silently skipped usable markets. The authors'
//! conclusion was that the fix is a single owner, normalized before any
//! source I/O happens.
//!
//! [`Coverage`] is the other half. A request and what actually loaded are
//! different facts and are never collapsed into one: a backtest over a
//! window that only half loaded should say so, not quietly report the
//! performance of half a strategy.

use std::fmt;

use crate::error::{BacktestError, Result};
use crate::types::UnixNanos;

/// A half-open time window, `[start, end)`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct TimeWindow {
    start: UnixNanos,
    end: UnixNanos,
}

impl TimeWindow {
    /// A window from `start` (inclusive) to `end` (exclusive).
    pub fn new(start: UnixNanos, end: UnixNanos) -> Result<Self> {
        if end <= start {
            return Err(BacktestError::EmptyWindow {
                start: start.get(),
                end: end.get(),
            });
        }
        Ok(Self { start, end })
    }

    /// From an **inclusive** end, as vendors and humans usually state it.
    ///
    /// The conversion happens here, once, so that "through the 28th" turns
    /// into `[.., 29th)` in one place rather than being re-derived (and
    /// re-fumbled) at each call site.
    pub fn from_inclusive_end(start: UnixNanos, last: UnixNanos) -> Result<Self> {
        Self::new(start, last.checked_add_nanos(1)?)
    }

    #[inline]
    pub fn start(self) -> UnixNanos {
        self.start
    }

    /// The exclusive end.
    #[inline]
    pub fn end(self) -> UnixNanos {
        self.end
    }

    /// The last instant inside the window.
    #[inline]
    pub fn last(self) -> UnixNanos {
        UnixNanos::new(self.end.get() - 1)
    }

    #[inline]
    pub fn duration_nanos(self) -> i64 {
        self.end.get() - self.start.get()
    }

    #[inline]
    pub fn contains(self, ts: UnixNanos) -> bool {
        ts >= self.start && ts < self.end
    }

    pub fn overlaps(self, other: Self) -> bool {
        self.start < other.end && other.start < self.end
    }

    /// The overlap of two windows, or `None` when they do not touch.
    pub fn intersect(self, other: Self) -> Option<Self> {
        let start = self.start.max(other.start);
        let end = self.end.min(other.end);
        (end > start).then_some(Self { start, end })
    }

    /// The smallest window containing both.
    pub fn union(self, other: Self) -> Self {
        Self {
            start: self.start.min(other.start),
            end: self.end.max(other.end),
        }
    }

    /// Split into consecutive chunks of at most `chunk_nanos`.
    ///
    /// Chunks tile the window exactly: no gap, no overlap, and the last one
    /// is short rather than overhanging.
    pub fn chunks(self, chunk_nanos: i64) -> Result<Vec<Self>> {
        if chunk_nanos <= 0 {
            return Err(BacktestError::invalid("chunk size must be positive"));
        }
        let mut out = Vec::new();
        let mut cursor = self.start;
        while cursor < self.end {
            let next = UnixNanos::new(cursor.get().saturating_add(chunk_nanos).min(self.end.get()));
            out.push(Self {
                start: cursor,
                end: next,
            });
            cursor = next;
        }
        Ok(out)
    }
}

impl fmt::Display for TimeWindow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}, {})", self.start, self.end)
    }
}

/// What was asked for versus what actually arrived.
///
/// `loaded` is the window the data really spans, which may be narrower than
/// `requested` (a vendor gap, a market that started late) and is never
/// silently substituted for it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Coverage {
    pub requested: TimeWindow,
    pub loaded: Option<TimeWindow>,
    /// Nanoseconds inside `requested` known to be missing.
    pub missing_nanos: i64,
}

impl Coverage {
    /// Nothing loaded for this request.
    pub fn empty(requested: TimeWindow) -> Self {
        Self {
            requested,
            loaded: None,
            missing_nanos: requested.duration_nanos(),
        }
    }

    /// Everything asked for arrived.
    pub fn complete(requested: TimeWindow) -> Self {
        Self {
            requested,
            loaded: Some(requested),
            missing_nanos: 0,
        }
    }

    /// A partial load, with explicit holes.
    ///
    /// `gaps` are windows inside `requested` known to be absent; they are
    /// clipped to the request and to the loaded span before counting, so a
    /// caller reporting a gap that reaches outside the request cannot push
    /// the ratio negative.
    pub fn partial(requested: TimeWindow, loaded: TimeWindow, gaps: &[TimeWindow]) -> Self {
        let clipped = loaded.intersect(requested);
        let mut missing = match clipped {
            Some(window) => requested.duration_nanos() - window.duration_nanos(),
            None => requested.duration_nanos(),
        };
        if let Some(window) = clipped {
            for gap in gaps {
                if let Some(overlap) = gap.intersect(window) {
                    missing += overlap.duration_nanos();
                }
            }
        }
        Self {
            requested,
            loaded: clipped,
            missing_nanos: missing.clamp(0, requested.duration_nanos()),
        }
    }

    /// Fraction of the requested window actually covered, in `[0, 1]`.
    pub fn ratio(self) -> f64 {
        let total = self.requested.duration_nanos();
        if total <= 0 {
            return 0.0;
        }
        let covered = (total - self.missing_nanos).max(0);
        covered as f64 / total as f64
    }

    #[inline]
    pub fn is_complete(self) -> bool {
        self.missing_nanos == 0 && self.loaded == Some(self.requested)
    }

    /// Refuse to proceed when coverage is below `minimum`.
    ///
    /// Thin data is a reason to stop, not to scale the answer down: a
    /// Sharpe from 40% of a window is not 40% of a Sharpe.
    pub fn require(self, minimum: f64) -> Result<Self> {
        if self.ratio() + f64::EPSILON < minimum {
            return Err(BacktestError::invalid(format!(
                "coverage {:.1}% of {} is below the required {:.1}%; the \
                 window loaded {} and is missing {} ns",
                self.ratio() * 100.0,
                self.requested,
                minimum * 100.0,
                self.loaded
                    .map(|w| w.to_string())
                    .unwrap_or_else(|| "nothing".to_string()),
                self.missing_nanos,
            )));
        }
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ns(value: i64) -> UnixNanos {
        UnixNanos::new(value)
    }

    fn window(start: i64, end: i64) -> TimeWindow {
        TimeWindow::new(ns(start), ns(end)).unwrap()
    }

    #[test]
    fn windows_are_half_open() {
        let w = window(100, 200);
        assert!(w.contains(ns(100)));
        assert!(!w.contains(ns(200)), "the end is excluded");
        assert!(w.contains(ns(199)));
        assert_eq!(w.last(), ns(199));
        assert_eq!(w.duration_nanos(), 100);
    }

    #[test]
    fn empty_and_inverted_windows_are_refused() {
        assert!(TimeWindow::new(ns(100), ns(100)).is_err());
        assert!(TimeWindow::new(ns(200), ns(100)).is_err());
    }

    #[test]
    fn inclusive_ends_convert_once_here() {
        // "through 199" is the same window as "[100, 200)".
        let stated = TimeWindow::from_inclusive_end(ns(100), ns(199)).unwrap();
        assert_eq!(stated, window(100, 200));
        assert!(stated.contains(ns(199)));
    }

    #[test]
    fn adjacent_windows_do_not_overlap() {
        // The bug this half-open rule prevents: [a, b) and [b, c) touching
        // must not both claim instant b.
        let first = window(0, 100);
        let second = window(100, 200);
        assert!(!first.overlaps(second));
        assert_eq!(first.intersect(second), None);
        assert_eq!(first.union(second), window(0, 200));
    }

    #[test]
    fn intersect_and_union_behave() {
        let a = window(0, 100);
        let b = window(50, 150);
        assert_eq!(a.intersect(b), Some(window(50, 100)));
        assert_eq!(a.union(b), window(0, 150));
        assert!(a.overlaps(b));
    }

    #[test]
    fn chunks_tile_the_window_exactly() {
        let w = window(0, 250);
        let chunks = w.chunks(100).unwrap();
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0], window(0, 100));
        assert_eq!(chunks[1], window(100, 200));
        assert_eq!(chunks[2], window(200, 250), "the last chunk is short");
        // No gaps, no overlaps, and they sum to the original.
        let total: i64 = chunks.iter().map(|c| c.duration_nanos()).sum();
        assert_eq!(total, w.duration_nanos());
        for pair in chunks.windows(2) {
            assert_eq!(pair[0].end(), pair[1].start());
        }
    }

    #[test]
    fn chunk_size_must_be_positive() {
        assert!(window(0, 10).chunks(0).is_err());
    }

    #[test]
    fn coverage_reports_a_short_load_rather_than_hiding_it() {
        let requested = window(0, 1000);
        let loaded = window(0, 600);
        let coverage = Coverage::partial(requested, loaded, &[]);
        assert_eq!(coverage.missing_nanos, 400);
        assert!((coverage.ratio() - 0.6).abs() < 1e-12);
        assert!(!coverage.is_complete());
    }

    #[test]
    fn coverage_counts_interior_gaps() {
        let requested = window(0, 1000);
        let coverage = Coverage::partial(requested, requested, &[window(100, 200)]);
        assert_eq!(coverage.missing_nanos, 100);
        assert!((coverage.ratio() - 0.9).abs() < 1e-12);
        assert!(!coverage.is_complete());
    }

    #[test]
    fn a_gap_outside_the_request_cannot_push_coverage_negative() {
        let requested = window(0, 100);
        let coverage = Coverage::partial(requested, requested, &[window(-10_000, 10_000)]);
        assert_eq!(coverage.missing_nanos, 100);
        assert_eq!(coverage.ratio(), 0.0);
    }

    #[test]
    fn complete_and_empty_are_the_two_extremes() {
        let requested = window(0, 100);
        assert!(Coverage::complete(requested).is_complete());
        assert_eq!(Coverage::complete(requested).ratio(), 1.0);
        assert_eq!(Coverage::empty(requested).ratio(), 0.0);
        assert_eq!(Coverage::empty(requested).loaded, None);
    }

    #[test]
    fn require_refuses_thin_data() {
        let requested = window(0, 1000);
        let thin = Coverage::partial(requested, window(0, 400), &[]);
        let err = thin.require(0.9).unwrap_err().to_string();
        assert!(err.contains("40.0%"), "{err}");
        assert!(thin.require(0.4).is_ok(), "exactly meeting the bar passes");
        assert!(Coverage::complete(requested).require(1.0).is_ok());
    }
}
