//! L2 order book reconstruction, and what to do when the data has a hole.
//!
//! The rule that matters here is the gap rule. Incremental updates are only
//! meaningful applied to a book that is already correct; apply them across a
//! missing hour and the result is a book that looks perfectly plausible and
//! is wrong, which then produces fills at prices that never existed. So a
//! declared gap marks the book **stale**, every incremental update against a
//! stale book is an error, and only a fresh snapshot restores it.
//!
//! This is deliberately louder than the reference implementation, which
//! warned and reset local state. An error is better than a warning because a
//! backtest is unattended: nobody reads the warning until after the numbers
//! have been believed.

use std::collections::BTreeMap;

use crate::error::{BacktestError, Result};
use crate::types::{Price, Qty, Raw, Side, UnixNanos};

/// What an incremental update does to a price level.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum BookAction {
    /// Set the size at this level (add or replace).
    Set,
    /// Remove this level.
    Delete,
    /// Empty the whole side.
    Clear,
}

/// One incremental change to one side of the book.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct BookDelta {
    pub action: BookAction,
    pub side: Side,
    pub price: Price,
    pub size: Qty,
}

impl BookDelta {
    pub fn set(side: Side, price: Price, size: Qty) -> Self {
        Self {
            action: BookAction::Set,
            side,
            price,
            size,
        }
    }

    pub fn delete(side: Side, price: Price) -> Self {
        Self {
            action: BookAction::Delete,
            side,
            price,
            size: Qty::ZERO,
        }
    }

    pub fn clear(side: Side) -> Self {
        Self {
            action: BookAction::Clear,
            side,
            price: Price::ZERO,
            size: Qty::ZERO,
        }
    }
}

/// Whether the book can be trusted right now.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum BookStatus {
    /// Reconstructed from a snapshot and every update since.
    Fresh,
    /// A gap was declared; incremental updates are refused until a snapshot
    /// rebuilds the book.
    Stale { since: UnixNanos },
}

/// A two-sided L2 book for one instrument outcome.
///
/// Levels are kept in a `BTreeMap` so iteration order is price order rather
/// than insertion order: the top of book must not depend on the sequence
/// updates happened to arrive in.
#[derive(Clone, Debug, Default)]
pub struct OrderBook {
    bids: BTreeMap<Raw, Qty>,
    asks: BTreeMap<Raw, Qty>,
    status: Option<BookStatus>,
    last_update: Option<UnixNanos>,
}

impl OrderBook {
    pub fn new() -> Self {
        Self::default()
    }

    /// Status, or `None` when no snapshot has ever been applied.
    pub fn status(&self) -> Option<BookStatus> {
        self.status
    }

    pub fn is_fresh(&self) -> bool {
        matches!(self.status, Some(BookStatus::Fresh))
    }

    pub fn last_update(&self) -> Option<UnixNanos> {
        self.last_update
    }

    /// Replace the whole book. This is what clears a stale state.
    ///
    /// Infallible today: levels are taken as recorded and a non-positive size
    /// is dropped rather than refused. The `Result` stays because this is
    /// where snapshot validation belongs if it is ever added, and widening a
    /// return type later is a breaking change for every caller. Callers must
    /// not read the `Result` as "validated": [`crate::models::TickSlippage`]
    /// once turned it into `.ok()?`, which would have spelled a refused
    /// snapshot as "no slippage applied", the flattering direction.
    ///
    /// One thing it deliberately does not refuse: a **crossed** snapshot,
    /// where the best bid is at or above the best ask. Venues publish crossed
    /// and locked books, and aborting a run on a transient would discard real
    /// data. It is worth knowing what such a book means while it stands --
    /// both sides are marketable at once, so a round trip through it prints a
    /// profit nobody could have taken.
    pub fn apply_snapshot(
        &mut self,
        bids: &[(Price, Qty)],
        asks: &[(Price, Qty)],
        ts: UnixNanos,
    ) -> Result<()> {
        self.bids.clear();
        self.asks.clear();
        for (price, size) in bids {
            if size.is_positive() {
                self.bids.insert(price.raw(), *size);
            }
        }
        for (price, size) in asks {
            if size.is_positive() {
                self.asks.insert(price.raw(), *size);
            }
        }
        self.status = Some(BookStatus::Fresh);
        self.last_update = Some(ts);
        Ok(())
    }

    /// Declare a hole in the data feed.
    ///
    /// Everything derived from incremental updates is untrustworthy from
    /// here until the next snapshot, so the levels are dropped rather than
    /// kept around to be accidentally used.
    pub fn mark_gap(&mut self, ts: UnixNanos) {
        self.bids.clear();
        self.asks.clear();
        self.status = Some(BookStatus::Stale { since: ts });
        self.last_update = Some(ts);
    }

    /// Apply one incremental update.
    ///
    /// Errors when the book is stale or was never snapshotted: an
    /// incremental update has no meaning without a base to apply it to.
    pub fn apply_delta(&mut self, instrument: &str, delta: BookDelta, ts: UnixNanos) -> Result<()> {
        match self.status {
            Some(BookStatus::Fresh) => {}
            Some(BookStatus::Stale { since }) => {
                return Err(BacktestError::BookGap {
                    instrument: instrument.to_string(),
                    ts: ts.get(),
                    gap_ts: since.get(),
                });
            }
            None => {
                return Err(BacktestError::BookGap {
                    instrument: instrument.to_string(),
                    ts: ts.get(),
                    gap_ts: ts.get(),
                });
            }
        }
        let side = match delta.side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        match delta.action {
            BookAction::Set => {
                if delta.size.is_positive() {
                    side.insert(delta.price.raw(), delta.size);
                } else {
                    // A zero size is how most venues spell "delete".
                    side.remove(&delta.price.raw());
                }
            }
            BookAction::Delete => {
                side.remove(&delta.price.raw());
            }
            BookAction::Clear => side.clear(),
        }
        self.last_update = Some(ts);
        Ok(())
    }

    /// Requote every level in post-split terms.
    ///
    /// A split changes none of a book's value and all of its numbers, and the
    /// venue requotes at the instant the action lands. Leaving the book alone
    /// is what lets a resting order that *was* repriced meet a book that was
    /// not: a resting sell of 100 at 50 becomes 200 at 25 and then trades
    /// against a pre-split bid of 49.99, which is a fill from the other side
    /// of the split.
    ///
    /// Two pre-split levels can round onto one post-split price, so sizes are
    /// added rather than replaced: keeping only the last would delete depth
    /// the venue still displays. A level whose size rounds away entirely is
    /// dropped, which is what a zero size means to `apply_snapshot` too.
    ///
    /// The status is untouched. A split is not a feed gap: these are still
    /// the levels the last snapshot established, said in new units.
    pub fn apply_split(&mut self, ratio: Price, ts: UnixNanos) -> Result<()> {
        self.bids = Self::split_side(&self.bids, ratio)?;
        self.asks = Self::split_side(&self.asks, ratio)?;
        self.last_update = Some(ts);
        Ok(())
    }

    fn split_side(levels: &BTreeMap<Raw, Qty>, ratio: Price) -> Result<BTreeMap<Raw, Qty>> {
        let mut out: BTreeMap<Raw, Qty> = BTreeMap::new();
        for (price, size) in levels {
            let (size, price) = crate::corporate::CorporateAction::split_position(
                ratio,
                *size,
                Price::from_raw(*price),
            )?;
            if !size.is_positive() {
                continue;
            }
            let level = out.entry(price.raw()).or_insert(Qty::ZERO);
            *level = level.checked_add(size)?;
        }
        Ok(out)
    }

    /// Best bid: highest price anyone will buy at.
    pub fn best_bid(&self) -> Option<(Price, Qty)> {
        self.bids
            .iter()
            .next_back()
            .map(|(price, size)| (Price::from_raw(*price), *size))
    }

    /// Best ask: lowest price anyone will sell at.
    pub fn best_ask(&self) -> Option<(Price, Qty)> {
        self.asks
            .iter()
            .next()
            .map(|(price, size)| (Price::from_raw(*price), *size))
    }

    pub fn spread(&self) -> Option<Price> {
        match (self.best_bid(), self.best_ask()) {
            (Some((bid, _)), Some((ask, _))) => Some(Price::from_raw(ask.raw() - bid.raw())),
            _ => None,
        }
    }

    /// Mid price, or `None` when either side is empty.
    ///
    /// Rounded half away from zero, like every other conversion in the crate.
    /// Integer division truncates toward zero, which on an odd-raw spread
    /// made the mid disagree with `Price::from_f64` of the same two numbers
    /// by one raw unit, and biased it downward on every book instead of
    /// splitting the difference.
    pub fn mid(&self) -> Option<Price> {
        match (self.best_bid(), self.best_ask()) {
            (Some((bid, _)), Some((ask, _))) => {
                let sum = bid.raw() + ask.raw();
                let away = if sum >= 0 { sum + 1 } else { sum - 1 };
                Some(Price::from_raw(away / 2))
            }
            _ => None,
        }
    }

    /// Levels on one side, best first.
    pub fn levels(&self, side: Side) -> Vec<(Price, Qty)> {
        match side {
            Side::Buy => self
                .bids
                .iter()
                .rev()
                .map(|(p, q)| (Price::from_raw(*p), *q))
                .collect(),
            Side::Sell => self
                .asks
                .iter()
                .map(|(p, q)| (Price::from_raw(*p), *q))
                .collect(),
        }
    }

    pub fn depth(&self, side: Side) -> usize {
        match side {
            Side::Buy => self.bids.len(),
            Side::Sell => self.asks.len(),
        }
    }

    /// Displayed quantity at one exact price.
    pub fn quantity_at(&self, side: Side, price: Price) -> Option<Qty> {
        match side {
            Side::Buy => self.bids.get(&price.raw()).copied(),
            Side::Sell => self.asks.get(&price.raw()).copied(),
        }
    }

    /// Walk the book consuming up to `qty`, as a taker would.
    ///
    /// Returns the fills level by level rather than an average price,
    /// because the caller needs to know it ran out of liquidity, and an
    /// average silently hides a partial fill.
    pub fn walk(&self, taker_side: Side, qty: Qty, limit: Option<Price>) -> BookWalk {
        let mut remaining = qty.raw();
        let mut fills = Vec::new();

        // Walk the maps directly. `levels()` materialises the entire side,
        // which is especially wasteful for the common case where an order
        // fills at the touch of a deep book.
        let mut visit = |price_raw: &Raw, size: &Qty| {
            if remaining <= 0 {
                return false;
            }
            let price = Price::from_raw(*price_raw);
            if let Some(cap) = limit {
                let acceptable = match taker_side {
                    Side::Buy => price <= cap,
                    Side::Sell => price >= cap,
                };
                if !acceptable {
                    return false;
                }
            }
            let take = remaining.min(size.raw());
            if take > 0 {
                fills.push((price, Qty::from_raw(take)));
                remaining -= take;
            }
            remaining > 0
        };

        // A buyer lifts asks from low to high; a seller hits bids from high
        // to low.
        match taker_side {
            Side::Buy => {
                for (price, size) in &self.asks {
                    if !visit(price, size) {
                        break;
                    }
                }
            }
            Side::Sell => {
                for (price, size) in self.bids.iter().rev() {
                    if !visit(price, size) {
                        break;
                    }
                }
            }
        }

        BookWalk {
            fills,
            unfilled: Qty::from_raw(remaining),
        }
    }
}

/// The result of walking the book for a taker order.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct BookWalk {
    /// `(price, quantity)` per level consumed, best first.
    pub fills: Vec<(Price, Qty)>,
    /// What could not be filled from the visible book.
    pub unfilled: Qty,
}

impl BookWalk {
    pub fn filled(&self) -> Qty {
        Qty::from_raw(self.fills.iter().map(|(_, q)| q.raw()).sum())
    }

    pub fn is_empty(&self) -> bool {
        self.fills.is_empty()
    }

    /// Size-weighted average fill price, or `None` if nothing filled.
    ///
    /// Rounded half away from zero rather than truncated, so a walk over
    /// several levels reports the price the fills actually average to. The
    /// overflow tag used to name `OrderBook::weighted_mid`, a function that
    /// has never existed, which sent anyone chasing the error to the wrong
    /// file.
    ///
    /// A narrowing failure reports `None`, which reads the same as "nothing
    /// filled". The signature has nowhere to put an error, and the only way
    /// to reach it is a walk whose weighted notional exceeds `Raw`, which
    /// the fills themselves would already have refused.
    pub fn average_price(&self) -> Option<Price> {
        let total = self.filled().raw() as i128;
        if total == 0 {
            return None;
        }
        let weighted: i128 = self
            .fills
            .iter()
            .map(|(p, q)| p.raw() as i128 * q.raw() as i128)
            .sum();
        let rounded = if weighted >= 0 {
            (weighted + total / 2) / total
        } else {
            (weighted - total / 2) / total
        };
        crate::types::narrow(rounded, "BookWalk::average_price")
            .ok()
            .map(Price::from_raw)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn price(value: f64) -> Price {
        Price::from_f64(value).unwrap()
    }

    fn qty(value: f64) -> Qty {
        Qty::from_f64(value).unwrap()
    }

    fn ts(value: i64) -> UnixNanos {
        UnixNanos::new(value)
    }

    fn snapshotted() -> OrderBook {
        let mut book = OrderBook::new();
        book.apply_snapshot(
            &[(price(0.40), qty(100.0)), (price(0.39), qty(200.0))],
            &[(price(0.42), qty(150.0)), (price(0.43), qty(300.0))],
            ts(1_000),
        )
        .unwrap();
        book
    }

    #[test]
    fn top_of_book_is_price_ordered_not_insertion_ordered() {
        let mut book = OrderBook::new();
        // Insert deliberately out of order.
        book.apply_snapshot(
            &[
                (price(0.39), qty(1.0)),
                (price(0.41), qty(1.0)),
                (price(0.40), qty(1.0)),
            ],
            &[(price(0.45), qty(1.0)), (price(0.42), qty(1.0))],
            ts(1),
        )
        .unwrap();
        assert_eq!(book.best_bid().unwrap().0, price(0.41));
        assert_eq!(book.best_ask().unwrap().0, price(0.42));
        assert_eq!(book.spread().unwrap(), Price::from_raw(price(0.01).raw()));
        assert_eq!(book.mid().unwrap(), price(0.415));
    }

    #[test]
    fn deltas_update_delete_and_clear() {
        let mut book = snapshotted();
        book.apply_delta(
            "m",
            BookDelta::set(Side::Buy, price(0.41), qty(50.0)),
            ts(2),
        )
        .unwrap();
        assert_eq!(book.best_bid().unwrap(), (price(0.41), qty(50.0)));

        book.apply_delta("m", BookDelta::delete(Side::Buy, price(0.41)), ts(3))
            .unwrap();
        assert_eq!(book.best_bid().unwrap().0, price(0.40));

        // A zero-size set is how most venues spell a delete.
        book.apply_delta(
            "m",
            BookDelta::set(Side::Buy, price(0.40), Qty::ZERO),
            ts(4),
        )
        .unwrap();
        assert_eq!(book.best_bid().unwrap().0, price(0.39));

        book.apply_delta("m", BookDelta::clear(Side::Sell), ts(5))
            .unwrap();
        assert_eq!(book.best_ask(), None);
        assert_eq!(book.depth(Side::Sell), 0);
    }

    #[test]
    fn an_incremental_update_without_a_snapshot_is_refused() {
        let mut book = OrderBook::new();
        let err = book
            .apply_delta("m", BookDelta::set(Side::Buy, price(0.4), qty(1.0)), ts(1))
            .unwrap_err();
        assert!(matches!(err, BacktestError::BookGap { .. }));
    }

    #[test]
    fn a_gap_makes_the_book_stale_and_empty() {
        let mut book = snapshotted();
        assert!(book.is_fresh());
        book.mark_gap(ts(5_000));
        assert!(!book.is_fresh());
        assert_eq!(book.best_bid(), None, "stale levels must not linger");
        assert_eq!(book.best_ask(), None);
    }

    #[test]
    fn updates_across_a_gap_are_refused_until_a_snapshot() {
        let mut book = snapshotted();
        book.mark_gap(ts(5_000));

        let err = book
            .apply_delta(
                "mkt",
                BookDelta::set(Side::Buy, price(0.4), qty(1.0)),
                ts(6_000),
            )
            .unwrap_err();
        match err {
            BacktestError::BookGap {
                instrument,
                ts,
                gap_ts,
            } => {
                assert_eq!(instrument, "mkt");
                assert_eq!(ts, 6_000);
                assert_eq!(gap_ts, 5_000);
            }
            other => panic!("expected a book gap, got {other:?}"),
        }

        // A snapshot is the only thing that restores it.
        book.apply_snapshot(&[(price(0.5), qty(10.0))], &[], ts(7_000))
            .unwrap();
        assert!(book.is_fresh());
        assert!(
            book.apply_delta(
                "mkt",
                BookDelta::set(Side::Buy, price(0.49), qty(1.0)),
                ts(7_001)
            )
            .is_ok()
        );
    }

    #[test]
    fn walking_the_book_consumes_levels_best_first() {
        let book = snapshotted();
        let walk = book.walk(Side::Buy, qty(200.0), None);
        assert_eq!(
            walk.fills,
            vec![(price(0.42), qty(150.0)), (price(0.43), qty(50.0))]
        );
        assert_eq!(walk.filled(), qty(200.0));
        assert_eq!(walk.unfilled, Qty::ZERO);
        // (150*0.42 + 50*0.43) / 200 = 0.4225
        assert_eq!(walk.average_price().unwrap(), price(0.4225));
    }

    #[test]
    fn walking_past_the_visible_book_reports_what_is_unfilled() {
        let book = snapshotted();
        let walk = book.walk(Side::Buy, qty(1_000.0), None);
        assert_eq!(walk.filled(), qty(450.0));
        assert_eq!(walk.unfilled, qty(550.0));
    }

    #[test]
    fn a_limit_stops_the_walk_rather_than_filling_through_it() {
        let book = snapshotted();
        let walk = book.walk(Side::Buy, qty(1_000.0), Some(price(0.42)));
        assert_eq!(walk.fills, vec![(price(0.42), qty(150.0))]);
        assert_eq!(walk.unfilled, qty(850.0));

        let sell = book.walk(Side::Sell, qty(1_000.0), Some(price(0.40)));
        assert_eq!(sell.fills, vec![(price(0.40), qty(100.0))]);
    }

    #[test]
    fn selling_hits_bids_and_buying_lifts_offers() {
        let book = snapshotted();
        assert_eq!(
            book.walk(Side::Sell, qty(1.0), None).fills[0].0,
            price(0.40)
        );
        assert_eq!(book.walk(Side::Buy, qty(1.0), None).fills[0].0, price(0.42));
    }

    #[test]
    fn an_empty_walk_has_no_average_price() {
        let book = OrderBook::new();
        let walk = book.walk(Side::Buy, qty(10.0), None);
        assert!(walk.is_empty());
        assert_eq!(walk.average_price(), None);
        assert_eq!(walk.unfilled, qty(10.0));
    }
}
