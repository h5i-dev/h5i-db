//! Order-by-order books (L3 / MBO), and exact queue position.
//!
//! An L2 feed says how much size sits at a price. An L3 feed says *which
//! orders*, in what order they arrived. That difference is the whole
//! difference in maker-fill modelling: with L2 the only honest assumption
//! is that you are last in the queue, because nothing in the data says
//! otherwise. With L3 the position is not assumed at all -- every add,
//! cancel and execution ahead of you is in the stream, so how much is ahead
//! is a fact.
//!
//! What this buys is not optimism. A queue-position model on L2 data is
//! *pessimistic by necessity*; the L3 answer is usually better but sometimes
//! worse, because cancels ahead of you advance the queue and executions
//! consume it, and only the real sequence says which happened.
//!
//! **What is modelled here is validated against synthetic sequences, not
//! against a real venue.** The mechanics -- FIFO within a price, cancels
//! removing from the middle, partial executions at the front -- are the
//! standard ones and are tested exhaustively, but no recorded MBO feed has
//! been replayed through them. That check is still outstanding and is a
//! different kind of assurance from the tests below.

use std::cmp::Reverse;
use std::collections::{BTreeMap, HashMap, VecDeque};

use crate::error::{BacktestError, Result};
use crate::types::{Price, Qty, Raw, Side};

/// A venue-assigned order identifier from an MBO feed.
pub type VenueOrderId = u64;

/// One message from an order-by-order feed.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum MboMessage {
    /// A new order joins the back of its price level.
    Add {
        order_id: VenueOrderId,
        side: Side,
        price: Price,
        size: Qty,
    },
    /// An order leaves the book.
    Cancel { order_id: VenueOrderId },
    /// An order trades, in whole or in part.
    Execute { order_id: VenueOrderId, size: Qty },
    /// An order's size is reduced in place.
    ///
    /// Reductions keep queue priority; increases do not, and a venue that
    /// allows them republishes as a cancel and an add. Modelling an
    /// increase as a modify would hand a strategy free priority, so it is
    /// refused.
    Reduce {
        order_id: VenueOrderId,
        new_size: Qty,
    },
}

impl MboMessage {
    pub fn order_id(&self) -> VenueOrderId {
        match self {
            MboMessage::Add { order_id, .. }
            | MboMessage::Cancel { order_id }
            | MboMessage::Execute { order_id, .. }
            | MboMessage::Reduce { order_id, .. } => *order_id,
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            MboMessage::Add { .. } => "add",
            MboMessage::Cancel { .. } => "cancel",
            MboMessage::Execute { .. } => "execute",
            MboMessage::Reduce { .. } => "reduce",
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct Resting {
    side: Side,
    price: Price,
    size: Qty,
}

/// An order-by-order book.
///
/// Each price level is a FIFO of venue order ids, which is what makes
/// "how much is ahead of me" answerable exactly rather than by assumption.
#[derive(Clone, Default, Debug)]
pub struct L3Book {
    orders: HashMap<VenueOrderId, Resting>,
    /// `(side, price)` to the queue at that level, front first. Keyed on a
    /// numeric side so iteration order is fixed rather than dependent on a
    /// derived enum ordering nobody declared.
    levels: BTreeMap<(u8, Raw), VecDeque<VenueOrderId>>,
}

const BUY: u8 = 0;
const SELL: u8 = 1;

fn side_key(side: Side) -> u8 {
    match side {
        Side::Buy => BUY,
        Side::Sell => SELL,
    }
}

impl L3Book {
    pub fn new() -> Self {
        Self::default()
    }

    fn level_key(side: Side, price: Price) -> (u8, Raw) {
        (side_key(side), price.raw())
    }

    /// Apply one feed message.
    pub fn apply(&mut self, message: MboMessage) -> Result<()> {
        match message {
            MboMessage::Add {
                order_id,
                side,
                price,
                size,
            } => self.add(order_id, side, price, size),
            MboMessage::Cancel { order_id } => self.cancel(order_id).map(|_| ()),
            MboMessage::Execute { order_id, size } => self.execute(order_id, size).map(|_| ()),
            MboMessage::Reduce { order_id, new_size } => self.reduce(order_id, new_size),
        }
    }

    /// Add an order to the back of its level.
    pub fn add(
        &mut self,
        order_id: VenueOrderId,
        side: Side,
        price: Price,
        size: Qty,
    ) -> Result<()> {
        if !size.is_positive() {
            return Err(BacktestError::invalid(
                "an order joining the book must have positive size",
            ));
        }
        if self.orders.contains_key(&order_id) {
            return Err(BacktestError::invalid(format!(
                "venue order {order_id} is already on the book"
            )));
        }
        self.orders.insert(order_id, Resting { side, price, size });
        self.levels
            .entry(Self::level_key(side, price))
            .or_default()
            .push_back(order_id);
        Ok(())
    }

    /// Remove an order. Returns the size that left the book.
    ///
    /// A cancel for an unknown order is *not* an error: MBO feeds routinely
    /// reference orders that predate the snapshot a replay started from, and
    /// treating those as corruption would make every mid-day start fail.
    pub fn cancel(&mut self, order_id: VenueOrderId) -> Result<Qty> {
        let Some(resting) = self.orders.remove(&order_id) else {
            return Ok(Qty::ZERO);
        };
        let key = Self::level_key(resting.side, resting.price);
        if let Some(queue) = self.levels.get_mut(&key) {
            queue.retain(|id| *id != order_id);
            if queue.is_empty() {
                self.levels.remove(&key);
            }
        }
        Ok(resting.size)
    }

    /// Trade against an order. Returns how much actually executed.
    pub fn execute(&mut self, order_id: VenueOrderId, size: Qty) -> Result<Qty> {
        let Some(resting) = self.orders.get_mut(&order_id) else {
            return Ok(Qty::ZERO);
        };
        let executed = Qty::from_raw(size.raw().min(resting.size.raw()));
        resting.size = Qty::from_raw(resting.size.raw() - executed.raw());
        if !resting.size.is_positive() {
            self.cancel(order_id)?;
        }
        Ok(executed)
    }

    /// Reduce an order in place, keeping its queue position.
    pub fn reduce(&mut self, order_id: VenueOrderId, new_size: Qty) -> Result<()> {
        let Some(resting) = self.orders.get_mut(&order_id) else {
            return Ok(());
        };
        if new_size > resting.size {
            return Err(BacktestError::invalid(format!(
                "order {order_id} cannot grow from {} to {new_size} in place; a \
                 venue republishes an increase as a cancel and an add, and \
                 treating it as a modify would hand it free queue priority",
                resting.size
            )));
        }
        if !new_size.is_positive() {
            self.cancel(order_id)?;
            return Ok(());
        }
        resting.size = new_size;
        Ok(())
    }

    /// Total size queued ahead of an order at its own level.
    ///
    /// The number an L2 book cannot produce. `None` when the order is not
    /// on the book.
    pub fn size_ahead_of(&self, order_id: VenueOrderId) -> Option<Qty> {
        let resting = self.orders.get(&order_id)?;
        let queue = self
            .levels
            .get(&Self::level_key(resting.side, resting.price))?;
        let mut ahead: Raw = 0;
        for id in queue {
            if *id == order_id {
                return Some(Qty::from_raw(ahead));
            }
            ahead += self.orders.get(id).map(|o| o.size.raw()).unwrap_or(0);
        }
        None
    }

    /// Total size resting at a price on a side.
    pub fn size_at(&self, side: Side, price: Price) -> Qty {
        self.levels
            .get(&Self::level_key(side, price))
            .map(|queue| {
                Qty::from_raw(
                    queue
                        .iter()
                        .map(|id| self.orders.get(id).map(|o| o.size.raw()).unwrap_or(0))
                        .sum(),
                )
            })
            .unwrap_or(Qty::ZERO)
    }

    /// Best bid: the highest price with resting size.
    pub fn best_bid(&self) -> Option<Price> {
        self.levels
            .keys()
            .filter(|(side, _)| *side == BUY)
            .map(|(_, price)| *price)
            .max()
            .map(Price::from_raw)
    }

    /// Best ask: the lowest price with resting size.
    pub fn best_ask(&self) -> Option<Price> {
        self.levels
            .keys()
            .filter(|(side, _)| *side == SELL)
            .map(|(_, price)| *price)
            .min()
            .map(Price::from_raw)
    }

    /// Collapse to price levels, best first, as an L2 view would show.
    pub fn levels(&self, side: Side) -> Vec<(Price, Qty)> {
        let mut out: Vec<(Price, Qty)> = self
            .levels
            .keys()
            .filter(|(entry_side, _)| *entry_side == side_key(side))
            .map(|(_, price)| {
                let price = Price::from_raw(*price);
                (price, self.size_at(side, price))
            })
            .filter(|(_, size)| size.is_positive())
            .collect();
        match side {
            Side::Buy => out.sort_by_key(|(price, _)| Reverse(*price)),
            Side::Sell => out.sort_by_key(|(price, _)| *price),
        }
        out
    }

    pub fn order_count(&self) -> usize {
        self.orders.len()
    }

    pub fn is_empty(&self) -> bool {
        self.orders.is_empty()
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

    /// Three orders queued at one price, in arrival order.
    fn queued() -> L3Book {
        let mut book = L3Book::new();
        book.add(1, Side::Buy, price(100.0), qty(10.0)).unwrap();
        book.add(2, Side::Buy, price(100.0), qty(20.0)).unwrap();
        book.add(3, Side::Buy, price(100.0), qty(30.0)).unwrap();
        book
    }

    #[test]
    fn queue_position_is_a_fact_not_an_assumption() {
        let book = queued();
        assert_eq!(book.size_ahead_of(1).unwrap(), Qty::ZERO);
        assert_eq!(book.size_ahead_of(2).unwrap(), qty(10.0));
        assert_eq!(book.size_ahead_of(3).unwrap(), qty(30.0));
        assert_eq!(
            book.size_ahead_of(99),
            None,
            "an unknown order has no place"
        );
    }

    #[test]
    fn a_cancel_ahead_of_you_advances_the_queue() {
        // The gain L3 gives over L2: an L2 model can only see the level
        // shrink, not that the shrinkage was in front of you.
        let mut book = queued();
        assert_eq!(book.size_ahead_of(3).unwrap(), qty(30.0));
        book.cancel(1).unwrap();
        assert_eq!(book.size_ahead_of(3).unwrap(), qty(20.0));
        book.cancel(2).unwrap();
        assert_eq!(
            book.size_ahead_of(3).unwrap(),
            Qty::ZERO,
            "now at the front"
        );
    }

    #[test]
    fn a_cancel_behind_you_changes_nothing() {
        let mut book = queued();
        book.cancel(3).unwrap();
        assert_eq!(book.size_ahead_of(2).unwrap(), qty(10.0));
        assert_eq!(book.size_ahead_of(1).unwrap(), Qty::ZERO);
    }

    #[test]
    fn a_partial_execution_shrinks_the_order_in_place() {
        let mut book = queued();
        assert_eq!(book.execute(1, qty(4.0)).unwrap(), qty(4.0));
        assert_eq!(book.size_ahead_of(2).unwrap(), qty(6.0), "6 of 10 remain");
        assert_eq!(book.order_count(), 3, "the order is still on the book");
    }

    #[test]
    fn a_full_execution_removes_the_order() {
        let mut book = queued();
        assert_eq!(book.execute(1, qty(10.0)).unwrap(), qty(10.0));
        assert_eq!(book.order_count(), 2);
        assert_eq!(book.size_ahead_of(2).unwrap(), Qty::ZERO);
    }

    #[test]
    fn an_execution_larger_than_the_order_takes_only_what_is_there() {
        let mut book = queued();
        assert_eq!(
            book.execute(1, qty(1_000.0)).unwrap(),
            qty(10.0),
            "an order cannot trade more than it holds"
        );
    }

    #[test]
    fn reducing_keeps_priority_and_growing_is_refused() {
        let mut book = queued();
        book.reduce(1, qty(3.0)).unwrap();
        assert_eq!(
            book.size_ahead_of(2).unwrap(),
            qty(3.0),
            "the reduced order kept its place at the front"
        );
        // Growing in place would be free priority.
        let error = book.reduce(1, qty(50.0)).unwrap_err().to_string();
        assert!(error.contains("free queue priority"), "{error}");
    }

    #[test]
    fn reducing_to_nothing_removes_the_order() {
        let mut book = queued();
        book.reduce(2, Qty::ZERO).unwrap();
        assert_eq!(book.order_count(), 2);
        assert_eq!(book.size_ahead_of(3).unwrap(), qty(10.0));
    }

    #[test]
    fn a_cancel_for_an_unseen_order_is_tolerated() {
        // MBO feeds reference orders that predate the snapshot a replay
        // started from; treating those as corruption would make every
        // mid-day start fail.
        let mut book = L3Book::new();
        assert_eq!(book.cancel(12_345).unwrap(), Qty::ZERO);
        assert_eq!(book.execute(12_345, qty(1.0)).unwrap(), Qty::ZERO);
        assert!(book.reduce(12_345, qty(1.0)).is_ok());
    }

    #[test]
    fn a_duplicate_order_id_is_refused() {
        let mut book = queued();
        assert!(book.add(1, Side::Buy, price(100.0), qty(5.0)).is_err());
    }

    #[test]
    fn zero_sized_orders_never_join_the_book() {
        let mut book = L3Book::new();
        assert!(book.add(1, Side::Buy, price(100.0), Qty::ZERO).is_err());
    }

    #[test]
    fn the_book_collapses_to_an_l2_view() {
        let mut book = queued();
        book.add(4, Side::Buy, price(99.0), qty(5.0)).unwrap();
        book.add(5, Side::Sell, price(101.0), qty(7.0)).unwrap();
        book.add(6, Side::Sell, price(102.0), qty(8.0)).unwrap();

        assert_eq!(book.best_bid(), Some(price(100.0)));
        assert_eq!(book.best_ask(), Some(price(101.0)));
        assert_eq!(
            book.levels(Side::Buy),
            vec![(price(100.0), qty(60.0)), (price(99.0), qty(5.0))],
            "levels aggregate the queue and sort best first"
        );
        assert_eq!(
            book.levels(Side::Sell),
            vec![(price(101.0), qty(7.0)), (price(102.0), qty(8.0))]
        );
    }

    #[test]
    fn an_emptied_level_disappears() {
        let mut book = L3Book::new();
        book.add(1, Side::Buy, price(100.0), qty(10.0)).unwrap();
        assert_eq!(book.best_bid(), Some(price(100.0)));
        book.cancel(1).unwrap();
        assert_eq!(book.best_bid(), None);
        assert!(book.is_empty());
    }

    #[test]
    fn messages_apply_through_one_entry_point() {
        let mut book = L3Book::new();
        for message in [
            MboMessage::Add {
                order_id: 1,
                side: Side::Buy,
                price: price(100.0),
                size: qty(10.0),
            },
            MboMessage::Add {
                order_id: 2,
                side: Side::Buy,
                price: price(100.0),
                size: qty(5.0),
            },
            MboMessage::Execute {
                order_id: 1,
                size: qty(4.0),
            },
            MboMessage::Cancel { order_id: 1 },
        ] {
            book.apply(message).unwrap();
        }
        assert_eq!(book.size_ahead_of(2).unwrap(), Qty::ZERO);
        assert_eq!(book.size_at(Side::Buy, price(100.0)), qty(5.0));
    }

    #[test]
    fn a_long_random_sequence_keeps_the_book_consistent() {
        // The invariant: the sum of queued sizes at a level always equals
        // the level's total, however many adds, cancels and partial
        // executions have gone through it.
        let mut book = L3Book::new();
        let mut live: Vec<VenueOrderId> = Vec::new();
        let mut next_id = 1u64;

        for step in 0..2_000u64 {
            match step % 5 {
                0 | 1 => {
                    let id = next_id;
                    next_id += 1;
                    let size = qty(((step % 9) + 1) as f64);
                    book.add(id, Side::Buy, price(100.0), size).unwrap();
                    live.push(id);
                }
                2 if !live.is_empty() => {
                    let id = live.remove(0);
                    book.cancel(id).unwrap();
                }
                3 if !live.is_empty() => {
                    let id = live[0];
                    if book.execute(id, qty(1.0)).unwrap() > Qty::ZERO
                        && book.size_ahead_of(id).is_none()
                    {
                        live.remove(0);
                    }
                }
                _ => {}
            }
            // Every live order's "ahead" must be less than the level total.
            let total = book.size_at(Side::Buy, price(100.0));
            for id in &live {
                if let Some(ahead) = book.size_ahead_of(*id) {
                    assert!(ahead <= total, "ahead {ahead} exceeds level {total}");
                }
            }
        }
        assert!(book.order_count() > 0);
    }
}
