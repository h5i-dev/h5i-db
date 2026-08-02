//! Merging sorted data streams into one deterministic replay order.
//!
//! The merge is ordinary; the *tie-break* is the point. When two records
//! carry the same `ts_init` -- which happens constantly, because venues
//! stamp at millisecond or second granularity -- something has to decide
//! which the strategy sees first, and that decision changes fills. If it is
//! decided by hash order, thread scheduling, or a UUID, the backtest is not
//! reproducible and every downstream guarantee is void.
//!
//! So the order here is a **total order on explicit fields**:
//! `(ts_init, stream priority, stream index, arrival index)`. Every
//! component is declared by the caller or fixed at construction, none of it
//! is incidental, and the last two guarantee no two records ever compare
//! equal.
//!
//! Streams must be sorted by `ts_init`, and that is checked rather than
//! assumed: an unsorted stream silently produces a replay that jumps
//! backwards in time, which looks like a strategy bug for a long while
//! before anyone suspects the data. The check is *incremental*, applied as
//! each record is pulled, so a stream far too large to hold in memory is
//! validated just as strictly as a vector.
//!
//! Sources are lazy for the same reason. A day of full-depth book data is
//! hundreds of millions of events; materialising it as `Vec<Record>` is not
//! slow, it is impossible. The merge holds one record per stream.

use std::cmp::Reverse;
use std::collections::BinaryHeap;

use crate::error::{BacktestError, Result};
use crate::event::Record;
use crate::types::UnixNanos;

/// Where a stream sits when timestamps tie. Lower goes first.
///
/// The default order exists because it is the one that keeps a venue
/// honest: the exchange must see a trade before the strategy does, or a
/// strategy could react to a print the matching engine has not processed.
pub mod priority {
    /// Explicit feed gaps: they invalidate state, so nothing should be
    /// processed at that instant before the invalidation is known.
    pub const GAP: u32 = 0;
    /// Full book replacements.
    pub const SNAPSHOT: u32 = 10;
    /// Incremental book updates.
    pub const DELTA: u32 = 20;
    /// Corporate actions, which change what a position *is* and so must
    /// land before anything is priced against it.
    pub const CORPORATE: u32 = 5;
    /// Venue-published mark and oracle prices.
    ///
    /// Early, for the same reason corporate actions are: the venue's mark
    /// is the authority on what a position is *worth* at this instant, and
    /// every check that prices against it -- margin, liquidation, equity --
    /// runs per record. Landing it after the book would let a book that
    /// gapped liquidate a position the venue was still valuing calmly, and
    /// the liquidation would then read as a strategy result.
    pub const REFERENCE: u32 = 7;
    /// Prints.
    pub const TRADE: u32 = 30;
    /// Funding, after prints so it settles against a current book.
    pub const FUNDING: u32 = 35;
    /// Aggregates, which summarise everything above them.
    pub const BAR: u32 = 40;
}

/// The total order key. Every field is explicit; none is incidental.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct Key {
    ts: i64,
    priority: u32,
    stream: usize,
    arrival: u64,
}

/// A lazy source of records, already sorted by `ts_init`.
///
/// Boxed rather than generic so a replay can merge sources of different
/// kinds -- a vector in a test, a decoded Arrow batch in production --
/// without the whole engine becoming generic over them.
pub type RecordSource = Box<dyn Iterator<Item = Result<Record>>>;

struct StreamState {
    name: String,
    priority: u32,
    source: RecordSource,
    head: Option<Record>,
    arrival: u64,
    /// The last timestamp seen, for incremental order validation.
    last_ts: Option<i64>,
}

impl std::fmt::Debug for StreamState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamState")
            .field("name", &self.name)
            .field("priority", &self.priority)
            .field("arrival", &self.arrival)
            .finish_non_exhaustive()
    }
}

impl StreamState {
    /// Pull the next record, checking it does not go backwards.
    ///
    /// A lazy source cannot be scanned up front, so sortedness is checked
    /// as records arrive. That is strictly better than the old whole-vector
    /// pass: the error now names the exact record that broke the order,
    /// and a stream too large to hold is still validated.
    fn pull(&mut self) -> Result<Option<Record>> {
        let Some(next) = self.source.next() else {
            return Ok(None);
        };
        let record = next?;
        let ts = record.ts().get();
        if let Some(previous) = self.last_ts
            && ts < previous
        {
            return Err(BacktestError::OutOfOrder {
                stream: self.name.clone(),
                ts,
                previous,
            });
        }
        self.last_ts = Some(ts);
        Ok(Some(record))
    }
}

/// A k-way merge over sorted record streams.
#[derive(Debug)]
pub struct Replay {
    streams: Vec<StreamState>,
    heap: BinaryHeap<Reverse<Key>>,
    /// The last instant handed out.
    ///
    /// **Written, never read.** Kept because it is the merge's own record of
    /// where it is, which the per-stream ordering check cannot give: each
    /// `StreamState` refuses a record before its own predecessor, and nothing
    /// yet asserts the merged output is monotone *across* streams. That
    /// assertion belongs here when it is added.
    last_emitted: Option<UnixNanos>,
    emitted: u64,
}

impl Replay {
    pub fn builder() -> ReplayBuilder {
        ReplayBuilder::default()
    }

    /// The next record in replay order.
    pub fn next_record(&mut self) -> Result<Option<Record>> {
        let Some(Reverse(key)) = self.heap.pop() else {
            return Ok(None);
        };
        let stream = &mut self.streams[key.stream];
        let record = stream
            .head
            .take()
            .expect("heap key without a head record is a merge bug");
        if let Some(next) = stream.pull()? {
            stream.arrival += 1;
            self.heap.push(Reverse(Key {
                ts: next.ts().get(),
                priority: stream.priority,
                stream: key.stream,
                arrival: stream.arrival,
            }));
            stream.head = Some(next);
        }
        self.last_emitted = Some(record.ts());
        self.emitted += 1;
        Ok(Some(record))
    }

    /// Drain the whole replay.
    ///
    /// Materialises every record, so it is for tests and small runs. The
    /// engine pulls one at a time, which is the point of the lazy sources.
    pub fn collect_all(&mut self) -> Result<Vec<Record>> {
        let mut out = Vec::new();
        while let Some(record) = self.next_record()? {
            out.push(record);
        }
        Ok(out)
    }

    /// The timestamp of the next record, without consuming it.
    pub fn peek_ts(&self) -> Option<UnixNanos> {
        self.heap.peek().map(|Reverse(key)| UnixNanos::new(key.ts))
    }

    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    /// How many records have been emitted so far.
    pub fn emitted(&self) -> u64 {
        self.emitted
    }

    /// Names of the merged streams, in index order.
    pub fn stream_names(&self) -> Vec<&str> {
        self.streams.iter().map(|s| s.name.as_str()).collect()
    }
}

#[derive(Default)]
pub struct ReplayBuilder {
    streams: Vec<(String, u32, RecordSource)>,
}

impl ReplayBuilder {
    /// Add a stream from a vector, which must already be sorted by `ts_init`.
    pub fn stream(self, name: impl Into<String>, priority: u32, records: Vec<Record>) -> Self {
        self.source(name, priority, Box::new(records.into_iter().map(Ok)))
    }

    /// Add a lazy stream.
    ///
    /// The source is pulled one record at a time, so a day of full-depth
    /// book data never exists as a `Vec<Record>`. Only one record per
    /// stream is held at any moment, plus whatever the source itself
    /// buffers.
    pub fn source(mut self, name: impl Into<String>, priority: u32, source: RecordSource) -> Self {
        self.streams.push((name.into(), priority, source));
        self
    }

    pub fn build(self) -> Result<Replay> {
        let mut streams = Vec::with_capacity(self.streams.len());
        let mut heap = BinaryHeap::new();

        for (index, (name, priority, source)) in self.streams.into_iter().enumerate() {
            let mut state = StreamState {
                name,
                priority,
                source,
                head: None,
                arrival: 0,
                last_ts: None,
            };
            let head = state.pull()?;
            if let Some(record) = &head {
                heap.push(Reverse(Key {
                    ts: record.ts().get(),
                    priority,
                    stream: index,
                    arrival: 0,
                }));
            }
            state.head = head;
            streams.push(state);
        }

        Ok(Replay {
            streams,
            heap,
            last_emitted: None,
            emitted: 0,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::book::BookDelta;
    use crate::event::MarketEvent;
    use crate::instrument::{InstrumentId, OutcomeId};
    use crate::types::{Price, Qty, Side, Stamps, UnixNanos};

    fn record(ts: i64, tag: &str) -> Record {
        Record::new(
            Stamps::immediate(UnixNanos::new(ts)),
            InstrumentId::new(tag).unwrap(),
            OutcomeId::FIRST,
            MarketEvent::Gap,
        )
    }

    fn trade(ts: i64, tag: &str, price: f64) -> Record {
        Record::new(
            Stamps::immediate(UnixNanos::new(ts)),
            InstrumentId::new(tag).unwrap(),
            OutcomeId::FIRST,
            MarketEvent::Trade {
                price: Price::from_f64(price).unwrap(),
                size: Qty::from_f64(1.0).unwrap(),
                aggressor: Some(Side::Buy),
            },
        )
    }

    #[test]
    fn merges_streams_in_timestamp_order() {
        let mut replay = Replay::builder()
            .stream("a", priority::TRADE, vec![record(10, "a"), record(30, "a")])
            .stream("b", priority::TRADE, vec![record(20, "b"), record(40, "b")])
            .build()
            .unwrap();
        let order: Vec<i64> = replay
            .collect_all()
            .unwrap()
            .iter()
            .map(|r| r.ts().get())
            .collect();
        assert_eq!(order, vec![10, 20, 30, 40]);
    }

    #[test]
    fn ties_break_by_priority_so_the_venue_sees_data_first() {
        // A delta and a trade at the same instant: the book update must be
        // applied before the print that may consume it.
        let delta = Record::new(
            Stamps::immediate(UnixNanos::new(100)),
            InstrumentId::new("m").unwrap(),
            OutcomeId::FIRST,
            MarketEvent::BookDelta(BookDelta::set(
                Side::Buy,
                Price::from_f64(0.5).unwrap(),
                Qty::from_f64(1.0).unwrap(),
            )),
        );
        let mut replay = Replay::builder()
            .stream("trades", priority::TRADE, vec![trade(100, "m", 0.5)])
            .stream("deltas", priority::DELTA, vec![delta])
            .build()
            .unwrap();
        let kinds: Vec<&str> = replay
            .collect_all()
            .unwrap()
            .iter()
            .map(|r| r.event.kind())
            .collect();
        assert_eq!(kinds, vec!["book_delta", "trade"]);
    }

    #[test]
    fn gaps_sort_before_everything_at_the_same_instant() {
        let mut replay = Replay::builder()
            .stream("trades", priority::TRADE, vec![trade(5, "m", 0.4)])
            .stream("gaps", priority::GAP, vec![record(5, "m")])
            .stream("bars", priority::BAR, vec![record(5, "m")])
            .build()
            .unwrap();
        let first = replay.next_record().unwrap().unwrap();
        assert_eq!(first.event.kind(), "gap");
    }

    #[test]
    fn equal_priority_ties_break_by_stream_then_arrival() {
        // Two streams at identical priority and timestamp: the declared
        // stream order decides, and it decides the same way every run.
        let mut first = Replay::builder()
            .stream(
                "x",
                priority::TRADE,
                vec![trade(1, "x", 0.1), trade(1, "x", 0.2)],
            )
            .stream("y", priority::TRADE, vec![trade(1, "y", 0.3)])
            .build()
            .unwrap();
        let tags: Vec<String> = first
            .collect_all()
            .unwrap()
            .iter()
            .map(|r| r.instrument.to_string())
            .collect();
        assert_eq!(tags, vec!["x", "x", "y"]);
    }

    #[test]
    fn the_merge_is_reproducible_across_runs() {
        // Timestamps deliberately collide across streams so the tie-break,
        // not the timestamp, decides most of the order.
        let build = || {
            Replay::builder()
                .stream(
                    "a",
                    priority::TRADE,
                    (0..50).map(|i| trade(i / 3, "a", 0.5)).collect(),
                )
                .stream(
                    "b",
                    priority::TRADE,
                    (0..50).map(|i| trade(i / 5, "b", 0.6)).collect(),
                )
                .stream(
                    "c",
                    priority::DELTA,
                    (0..50).map(|i| record(i / 7, "c")).collect(),
                )
                .build()
                .unwrap()
        };
        let first: Vec<_> = build().collect_all().unwrap();
        for _ in 0..5 {
            assert_eq!(
                build().collect_all().unwrap(),
                first,
                "merge order must not vary"
            );
        }
    }

    #[test]
    fn an_unsorted_stream_is_refused_when_it_is_reached() {
        // With lazy sources the check happens as records are pulled, not in
        // one pass up front: a stream too large to hold is still validated,
        // and the error names the record that broke the order rather than
        // the stream as a whole.
        let mut replay = Replay::builder()
            .stream(
                "bad",
                priority::TRADE,
                vec![record(10, "m"), record(5, "m")],
            )
            .build()
            .expect("the first record is fine");
        // The offending record is pulled while emitting the one before it,
        // so the error arrives on the call that would have needed it.
        let err = replay.next_record().unwrap_err();
        match err {
            BacktestError::OutOfOrder {
                stream,
                ts,
                previous,
            } => {
                assert_eq!(stream, "bad");
                assert_eq!((ts, previous), (5, 10));
            }
            other => panic!("expected out of order, got {other:?}"),
        }
    }

    #[test]
    fn replay_orders_by_ts_init_not_ts_event() {
        // A record that happened first but was learned last must be
        // delivered last. This is the whole point of the dual stamp.
        let late = Record::new(
            Stamps::new(UnixNanos::new(1), UnixNanos::new(100)).unwrap(),
            InstrumentId::new("late").unwrap(),
            OutcomeId::FIRST,
            MarketEvent::Gap,
        );
        let prompt = Record::new(
            Stamps::new(UnixNanos::new(50), UnixNanos::new(50)).unwrap(),
            InstrumentId::new("prompt").unwrap(),
            OutcomeId::FIRST,
            MarketEvent::Gap,
        );
        // Separate streams, each trivially sorted, so only the merge's
        // choice of key decides. By ts_event the late record comes first;
        // by ts_init it comes last, and ts_init is what replay must use.
        let mut replay = Replay::builder()
            .stream("late", priority::TRADE, vec![late])
            .stream("prompt", priority::TRADE, vec![prompt])
            .build()
            .unwrap();
        let tags: Vec<String> = replay
            .collect_all()
            .unwrap()
            .iter()
            .map(|r| r.instrument.to_string())
            .collect();
        assert_eq!(
            tags,
            vec!["prompt", "late"],
            "a record learned late must arrive late"
        );
    }

    #[test]
    fn peek_does_not_consume() {
        let mut replay = Replay::builder()
            .stream("a", priority::TRADE, vec![record(7, "a")])
            .build()
            .unwrap();
        assert_eq!(replay.peek_ts(), Some(UnixNanos::new(7)));
        assert_eq!(replay.peek_ts(), Some(UnixNanos::new(7)));
        assert!(replay.next_record().unwrap().is_some());
        assert_eq!(replay.peek_ts(), None);
        assert!(replay.is_empty());
        assert_eq!(replay.emitted(), 1);
    }

    #[test]
    fn empty_streams_are_harmless() {
        let mut replay = Replay::builder()
            .stream("empty", priority::TRADE, vec![])
            .stream("full", priority::TRADE, vec![record(1, "m")])
            .build()
            .unwrap();
        assert_eq!(replay.collect_all().unwrap().len(), 1);
        assert_eq!(replay.stream_names(), vec!["empty", "full"]);
    }
}
