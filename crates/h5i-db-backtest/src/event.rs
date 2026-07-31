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
    /// A funding payment becoming due on a perpetual.
    ///
    /// Carried as a market event rather than a periodic venue module so it
    /// inherits the replay's deterministic ordering: funding at the same
    /// instant as a book update resolves the same way on every run, which a
    /// module firing off its own clock would not guarantee.
    Funding {
        /// Positive means longs pay shorts, the near-universal convention.
        rate: Price,
    },
    /// A corporate action taking effect.
    ///
    /// Applied forward, to positions and resting orders, at the instant it
    /// becomes effective. Replayed prices are never rewritten: nobody ever
    /// traded a split-adjusted price.
    Corporate(crate::corporate::CorporateAction),
    /// An aggregated bar.
    Bar {
        open: Price,
        high: Price,
        low: Price,
        close: Price,
        volume: Qty,
    },
    /// Reference prices the venue publishes, which are not the book.
    ///
    /// A derivatives venue does not margin you against the mid. Hyperliquid
    /// keeps three distinct prices: an **oracle** built from spot exchanges,
    /// a **mark** derived from it and the book, and the book's own mid. The
    /// mark is what margin, unrealised PnL and liquidation read; the oracle
    /// is what funding is charged on.
    ///
    /// Substituting the mid for either is not a small approximation. A thin
    /// book or a one-print wick moves the mid far enough to liquidate a
    /// position the venue would not have touched, and that liquidation then
    /// looks like a strategy result.
    Reference {
        /// The venue's mark, for margin and valuation.
        mark: Option<Price>,
        /// The venue's oracle, for funding.
        oracle: Option<Price>,
    },
}

impl MarketEvent {
    pub fn kind(&self) -> &'static str {
        match self {
            MarketEvent::BookSnapshot { .. } => "book_snapshot",
            MarketEvent::BookDelta(_) => "book_delta",
            MarketEvent::Gap => "gap",
            MarketEvent::Trade { .. } => "trade",
            MarketEvent::Funding { .. } => "funding",
            MarketEvent::Corporate(_) => "corporate",
            MarketEvent::Bar { .. } => "bar",
            MarketEvent::Reference { .. } => "reference",
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
