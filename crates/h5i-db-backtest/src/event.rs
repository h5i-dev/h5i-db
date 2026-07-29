//! What flows through a replay.

use crate::book::BookDelta;
use crate::instrument::{InstrumentId, OutcomeId};
use crate::types::{Price, Qty, Side, Stamps};

/// A market data event for one instrument outcome.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum MarketEvent {
    /// A full replacement of the book.
    BookSnapshot {
        bids: Vec<(Price, Qty)>,
        asks: Vec<(Price, Qty)>,
    },
    /// One incremental change.
    BookDelta(BookDelta),
    /// A declared hole in the feed. Not an absence of records -- an explicit
    /// statement that records are missing, which is what lets the book
    /// refuse to reconstruct across it.
    Gap,
    /// A print.
    Trade {
        price: Price,
        size: Qty,
        /// Which side lifted. `None` where the vendor does not say, which
        /// is common and must not be guessed.
        aggressor: Option<Side>,
    },
    /// An aggregated bar.
    Bar {
        open: Price,
        high: Price,
        low: Price,
        close: Price,
        volume: Qty,
    },
}

impl MarketEvent {
    pub fn kind(&self) -> &'static str {
        match self {
            MarketEvent::BookSnapshot { .. } => "book_snapshot",
            MarketEvent::BookDelta(_) => "book_delta",
            MarketEvent::Gap => "gap",
            MarketEvent::Trade { .. } => "trade",
            MarketEvent::Bar { .. } => "bar",
        }
    }
}

/// One replayable record: what happened, to what, and when it was knowable.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Record {
    pub stamps: Stamps,
    pub instrument: InstrumentId,
    pub outcome: OutcomeId,
    pub event: MarketEvent,
}

impl Record {
    pub fn new(
        stamps: Stamps,
        instrument: InstrumentId,
        outcome: OutcomeId,
        event: MarketEvent,
    ) -> Self {
        Self {
            stamps,
            instrument,
            outcome,
            event,
        }
    }

    /// The instant replay orders by: when the system could have known this.
    #[inline]
    pub fn ts(&self) -> crate::types::UnixNanos {
        self.stamps.ts_init
    }
}
