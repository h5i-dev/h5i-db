//! The replay clock and its timers.
//!
//! There is no wall clock anywhere in a backtest. Time is whatever the data
//! says it is, and it only ever moves forward: [`Clock::advance_to`] refuses
//! to go backwards, because a clock that can rewind turns a merge bug into a
//! strategy that appears to predict the past.
//!
//! Timers are the second priority queue in the kernel (the first is the data
//! merge). Their order is `(fire time, name, sequence)` -- the name is a
//! declared string and the sequence is monotonic, so two timers due at the
//! same instant fire in the same order every run. Nautilus reaches the same
//! conclusion from the other direction: it tie-breaks on a monotonic
//! sequence *instead of* the UUID it would otherwise have used, explicitly
//! for reproducibility.

use std::cmp::Reverse;
use std::collections::BinaryHeap;

use crate::error::{BacktestError, Result};
use crate::types::UnixNanos;

/// A timer that has come due.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TimeEvent {
    pub name: String,
    pub scheduled_for: UnixNanos,
    /// Monotonic within a run; the last tie-break and a stable event id.
    pub sequence: u64,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct TimerKey {
    at: i64,
    name: String,
    sequence: u64,
}

struct Timer {
    key: TimerKey,
    /// Set for repeating timers.
    interval: Option<i64>,
}

impl PartialEq for Timer {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}
impl Eq for Timer {}
impl PartialOrd for Timer {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Timer {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key.cmp(&other.key)
    }
}

/// The run's clock.
pub struct Clock {
    now: UnixNanos,
    timers: BinaryHeap<Reverse<Timer>>,
    sequence: u64,
}

impl Clock {
    pub fn new(start: UnixNanos) -> Self {
        Self {
            now: start,
            timers: BinaryHeap::new(),
            sequence: 0,
        }
    }

    #[inline]
    pub fn now(&self) -> UnixNanos {
        self.now
    }

    /// Schedule a one-shot timer.
    ///
    /// Scheduling in the past is refused rather than fired immediately: it
    /// means the caller's notion of "now" disagrees with the clock's, and
    /// firing it would hide that.
    pub fn set_timer(&mut self, name: impl Into<String>, at: UnixNanos) -> Result<()> {
        self.push(name.into(), at, None)
    }

    /// Schedule a repeating timer, first firing at `start`.
    pub fn set_repeating(
        &mut self,
        name: impl Into<String>,
        start: UnixNanos,
        interval_nanos: i64,
    ) -> Result<()> {
        if interval_nanos <= 0 {
            return Err(BacktestError::invalid(
                "a repeating timer needs a positive interval",
            ));
        }
        self.push(name.into(), start, Some(interval_nanos))
    }

    fn push(&mut self, name: String, at: UnixNanos, interval: Option<i64>) -> Result<()> {
        if at < self.now {
            return Err(BacktestError::invalid(format!(
                "timer {name:?} scheduled for {at}, which is before now ({})",
                self.now
            )));
        }
        self.sequence += 1;
        self.timers.push(Reverse(Timer {
            key: TimerKey {
                at: at.get(),
                name,
                sequence: self.sequence,
            },
            interval,
        }));
        Ok(())
    }

    /// Cancel every timer with this name. Returns how many were removed.
    pub fn cancel(&mut self, name: &str) -> usize {
        let before = self.timers.len();
        let kept: Vec<_> = self
            .timers
            .drain()
            .filter(|Reverse(timer)| timer.key.name != name)
            .collect();
        self.timers = kept.into_iter().collect();
        before - self.timers.len()
    }

    pub fn pending(&self) -> usize {
        self.timers.len()
    }

    /// The next time a timer is due.
    pub fn next_timer(&self) -> Option<UnixNanos> {
        self.timers.peek().map(|Reverse(t)| UnixNanos::new(t.key.at))
    }

    /// Move the clock to `ts` and return every timer that came due, in
    /// order.
    ///
    /// Timers due at exactly `ts` fire: the data at that instant has not
    /// been delivered yet when the kernel calls this, so a timer scheduled
    /// for the same nanosecond precedes it, which is the ordering a live
    /// system would produce.
    pub fn advance_to(&mut self, ts: UnixNanos) -> Result<Vec<TimeEvent>> {
        if ts < self.now {
            return Err(BacktestError::invalid(format!(
                "clock cannot move backwards: {ts} precedes {}",
                self.now
            )));
        }
        let mut fired = Vec::new();
        while let Some(Reverse(timer)) = self.timers.peek() {
            if timer.key.at > ts.get() {
                break;
            }
            let Reverse(timer) = self.timers.pop().expect("peeked");
            // The clock reads as the timer's own time while it fires, not
            // as the target: a handler asking "what time is it" during a
            // 09:30 timer must not be told 09:45.
            self.now = UnixNanos::new(timer.key.at);
            fired.push(TimeEvent {
                name: timer.key.name.clone(),
                scheduled_for: UnixNanos::new(timer.key.at),
                sequence: timer.key.sequence,
            });
            if let Some(interval) = timer.interval {
                let next = timer.key.at.saturating_add(interval);
                self.sequence += 1;
                self.timers.push(Reverse(Timer {
                    key: TimerKey {
                        at: next,
                        name: timer.key.name,
                        sequence: self.sequence,
                    },
                    interval: Some(interval),
                }));
            }
        }
        self.now = ts;
        Ok(fired)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(value: i64) -> UnixNanos {
        UnixNanos::new(value)
    }

    #[test]
    fn the_clock_starts_where_it_is_told_and_moves_forward() {
        let mut clock = Clock::new(ts(1_000));
        assert_eq!(clock.now(), ts(1_000));
        clock.advance_to(ts(2_000)).unwrap();
        assert_eq!(clock.now(), ts(2_000));
        assert!(clock.advance_to(ts(1_500)).is_err(), "no rewinding");
    }

    #[test]
    fn timers_fire_when_due_and_only_once() {
        let mut clock = Clock::new(ts(0));
        clock.set_timer("rebalance", ts(100)).unwrap();
        assert!(clock.advance_to(ts(50)).unwrap().is_empty());
        let fired = clock.advance_to(ts(150)).unwrap();
        assert_eq!(fired.len(), 1);
        assert_eq!(fired[0].name, "rebalance");
        assert_eq!(fired[0].scheduled_for, ts(100));
        assert!(clock.advance_to(ts(1_000)).unwrap().is_empty());
    }

    #[test]
    fn a_timer_due_exactly_now_fires() {
        let mut clock = Clock::new(ts(0));
        clock.set_timer("t", ts(100)).unwrap();
        assert_eq!(clock.advance_to(ts(100)).unwrap().len(), 1);
    }

    #[test]
    fn simultaneous_timers_fire_in_a_declared_order() {
        let mut clock = Clock::new(ts(0));
        // Scheduled deliberately out of alphabetical order.
        clock.set_timer("zulu", ts(10)).unwrap();
        clock.set_timer("alpha", ts(10)).unwrap();
        clock.set_timer("mike", ts(10)).unwrap();
        let names: Vec<String> = clock
            .advance_to(ts(10))
            .unwrap()
            .into_iter()
            .map(|e| e.name)
            .collect();
        assert_eq!(names, vec!["alpha", "mike", "zulu"]);
    }

    #[test]
    fn same_name_same_instant_breaks_by_sequence() {
        let mut clock = Clock::new(ts(0));
        clock.set_timer("t", ts(5)).unwrap();
        clock.set_timer("t", ts(5)).unwrap();
        let fired = clock.advance_to(ts(5)).unwrap();
        assert_eq!(fired.len(), 2);
        assert!(fired[0].sequence < fired[1].sequence);
    }

    #[test]
    fn timer_order_is_reproducible() {
        let run = || {
            let mut clock = Clock::new(ts(0));
            for (name, at) in [("c", 5), ("a", 5), ("b", 3), ("a", 3), ("d", 5)] {
                clock.set_timer(name, ts(at)).unwrap();
            }
            clock
                .advance_to(ts(10))
                .unwrap()
                .into_iter()
                .map(|e| (e.name, e.scheduled_for.get()))
                .collect::<Vec<_>>()
        };
        let first = run();
        assert_eq!(first[0], ("a".to_string(), 3));
        assert_eq!(first[1], ("b".to_string(), 3));
        for _ in 0..5 {
            assert_eq!(run(), first);
        }
    }

    #[test]
    fn the_clock_reads_as_the_timers_own_time_while_it_fires() {
        // A handler asking the time during a 09:30 timer must not see 09:45.
        let mut clock = Clock::new(ts(0));
        clock.set_timer("t", ts(30)).unwrap();
        let fired = clock.advance_to(ts(45)).unwrap();
        assert_eq!(fired[0].scheduled_for, ts(30));
        assert_eq!(clock.now(), ts(45), "and lands on the target afterwards");
    }

    #[test]
    fn repeating_timers_keep_their_cadence() {
        let mut clock = Clock::new(ts(0));
        clock.set_repeating("funding", ts(10), 10).unwrap();
        let fired = clock.advance_to(ts(35)).unwrap();
        let times: Vec<i64> = fired.iter().map(|e| e.scheduled_for.get()).collect();
        assert_eq!(times, vec![10, 20, 30]);
        assert_eq!(clock.next_timer(), Some(ts(40)));
    }

    #[test]
    fn a_repeating_timer_needs_a_positive_interval() {
        let mut clock = Clock::new(ts(0));
        assert!(clock.set_repeating("x", ts(1), 0).is_err());
        assert!(clock.set_repeating("x", ts(1), -5).is_err());
    }

    #[test]
    fn scheduling_in_the_past_is_refused() {
        let mut clock = Clock::new(ts(100));
        assert!(clock.set_timer("late", ts(50)).is_err());
        assert!(clock.set_timer("now", ts(100)).is_ok());
    }

    #[test]
    fn cancel_removes_matching_timers_only() {
        let mut clock = Clock::new(ts(0));
        clock.set_timer("keep", ts(10)).unwrap();
        clock.set_timer("drop", ts(11)).unwrap();
        clock.set_timer("drop", ts(12)).unwrap();
        assert_eq!(clock.cancel("drop"), 2);
        assert_eq!(clock.pending(), 1);
        let fired = clock.advance_to(ts(100)).unwrap();
        assert_eq!(fired.len(), 1);
        assert_eq!(fired[0].name, "keep");
    }
}
