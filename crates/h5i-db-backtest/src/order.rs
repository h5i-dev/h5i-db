//! Orders and fills.

use crate::error::{BacktestError, Result};
use crate::instrument::{InstrumentId, OutcomeId};
use crate::types::{Money, Price, Qty, Side, UnixNanos};

/// A monotonic identifier, assigned by the engine in submission order.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
pub struct OrderId(pub u64);

impl std::fmt::Display for OrderId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "O{}", self.0)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum OrderKind {
    /// Take whatever the book offers.
    Market,
    /// Trade at `limit` or better.
    Limit { limit: Price },
}

/// How long an order lives.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum TimeInForce {
    /// Rest on the book until cancelled.
    #[default]
    GoodTilCancel,
    /// Fill what is available now, cancel the rest.
    ImmediateOrCancel,
    /// Fill entirely now or not at all.
    FillOrKill,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum OrderStatus {
    /// Submitted, not yet seen by the venue (in the latency queue).
    InFlight,
    /// Live at the venue.
    Accepted,
    PartiallyFilled,
    Filled,
    Cancelled,
    /// The venue refused it, with a reason.
    Rejected,
}

impl OrderStatus {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            OrderStatus::Filled | OrderStatus::Cancelled | OrderStatus::Rejected
        )
    }

    pub fn as_str(self) -> &'static str {
        match self {
            OrderStatus::InFlight => "in_flight",
            OrderStatus::Accepted => "accepted",
            OrderStatus::PartiallyFilled => "partially_filled",
            OrderStatus::Filled => "filled",
            OrderStatus::Cancelled => "cancelled",
            OrderStatus::Rejected => "rejected",
        }
    }
}

/// An order as the engine tracks it.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Order {
    pub id: OrderId,
    pub instrument: InstrumentId,
    pub outcome: OutcomeId,
    pub side: Side,
    pub kind: OrderKind,
    pub quantity: Qty,
    pub filled: Qty,
    pub time_in_force: TimeInForce,
    pub status: OrderStatus,
    /// When the strategy submitted it.
    pub submitted_at: UnixNanos,
    /// When the venue saw it (submission plus latency).
    pub accepted_at: Option<UnixNanos>,
    /// Set when the venue refuses it.
    pub reject_reason: Option<String>,
    /// Free-form label the strategy attaches; carried onto fills so a run
    /// report can attribute them to intent (entry, exit, hedge).
    pub tag: Option<String>,
    /// Only reduces an existing position; never opens or flips one.
    pub reduce_only: bool,
    /// Add liquidity only: the venue refuses it rather than let it cross.
    ///
    /// Hyperliquid calls this ALO and Binance post-only; the behaviour is
    /// the same everywhere it exists, and it is the difference between a
    /// maker strategy and a very expensive taker one. A simulator without
    /// it either crosses silently at taker cost, or invents a maker fill on
    /// an order the venue would have rejected outright.
    pub post_only: bool,
}

impl Order {
    pub fn market(
        id: OrderId,
        instrument: InstrumentId,
        outcome: OutcomeId,
        side: Side,
        quantity: Qty,
        submitted_at: UnixNanos,
    ) -> Result<Self> {
        Self::new(
            id,
            instrument,
            outcome,
            side,
            OrderKind::Market,
            quantity,
            TimeInForce::ImmediateOrCancel,
            submitted_at,
        )
    }

    pub fn limit(
        id: OrderId,
        instrument: InstrumentId,
        outcome: OutcomeId,
        side: Side,
        limit: Price,
        quantity: Qty,
        submitted_at: UnixNanos,
    ) -> Result<Self> {
        Self::new(
            id,
            instrument,
            outcome,
            side,
            OrderKind::Limit { limit },
            quantity,
            TimeInForce::GoodTilCancel,
            submitted_at,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: OrderId,
        instrument: InstrumentId,
        outcome: OutcomeId,
        side: Side,
        kind: OrderKind,
        quantity: Qty,
        time_in_force: TimeInForce,
        submitted_at: UnixNanos,
    ) -> Result<Self> {
        if !quantity.is_positive() {
            return Err(BacktestError::invalid(
                "order quantity must be positive; direction is the side, not the sign",
            ));
        }
        Ok(Self {
            id,
            instrument,
            outcome,
            side,
            kind,
            quantity,
            filled: Qty::ZERO,
            time_in_force,
            status: OrderStatus::InFlight,
            submitted_at,
            accepted_at: None,
            reject_reason: None,
            tag: None,
            reduce_only: false,
            post_only: false,
        })
    }

    pub fn with_time_in_force(mut self, tif: TimeInForce) -> Self {
        self.time_in_force = tif;
        self
    }

    pub fn with_tag(mut self, tag: impl Into<String>) -> Self {
        self.tag = Some(tag.into());
        self
    }

    pub fn reduce_only(mut self) -> Self {
        self.reduce_only = true;
        self
    }

    pub fn post_only(mut self) -> Self {
        self.post_only = true;
        self
    }

    /// Quantity not yet filled.
    pub fn remaining(&self) -> Qty {
        Qty::from_raw(self.quantity.raw() - self.filled.raw())
    }

    pub fn is_open(&self) -> bool {
        !self.status.is_terminal()
    }

    /// The price cap this order will accept, if any.
    pub fn limit_price(&self) -> Option<Price> {
        match self.kind {
            OrderKind::Market => None,
            OrderKind::Limit { limit } => Some(limit),
        }
    }

    pub(crate) fn record_fill(&mut self, quantity: Qty) -> Result<()> {
        let filled = self.filled.checked_add(quantity)?;
        if filled > self.quantity {
            return Err(BacktestError::invalid(format!(
                "order {} would fill {} of {}",
                self.id, filled, self.quantity
            )));
        }
        self.filled = filled;
        self.status = if self.filled == self.quantity {
            OrderStatus::Filled
        } else {
            OrderStatus::PartiallyFilled
        };
        Ok(())
    }
}

/// One execution.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Fill {
    pub order_id: OrderId,
    pub instrument: InstrumentId,
    pub outcome: OutcomeId,
    pub side: Side,
    pub price: Price,
    pub quantity: Qty,
    pub commission: Money,
    /// True when this fill took liquidity.
    pub is_taker: bool,
    pub ts: UnixNanos,
    pub tag: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids() -> (InstrumentId, OutcomeId) {
        (InstrumentId::new("m").unwrap(), OutcomeId::FIRST)
    }

    #[test]
    fn quantity_must_be_positive_because_side_carries_direction() {
        let (instrument, outcome) = ids();
        assert!(
            Order::market(
                OrderId(1),
                instrument.clone(),
                outcome,
                Side::Buy,
                Qty::ZERO,
                UnixNanos::new(0)
            )
            .is_err()
        );
        assert!(
            Order::market(
                OrderId(1),
                instrument,
                outcome,
                Side::Sell,
                Qty::from_raw(-1),
                UnixNanos::new(0)
            )
            .is_err()
        );
    }

    #[test]
    fn fills_accumulate_and_close_the_order() {
        let (instrument, outcome) = ids();
        let mut order = Order::market(
            OrderId(1),
            instrument,
            outcome,
            Side::Buy,
            Qty::from_f64(10.0).unwrap(),
            UnixNanos::new(0),
        )
        .unwrap();
        order.record_fill(Qty::from_f64(4.0).unwrap()).unwrap();
        assert_eq!(order.status, OrderStatus::PartiallyFilled);
        assert_eq!(order.remaining(), Qty::from_f64(6.0).unwrap());
        assert!(order.is_open());

        order.record_fill(Qty::from_f64(6.0).unwrap()).unwrap();
        assert_eq!(order.status, OrderStatus::Filled);
        assert_eq!(order.remaining(), Qty::ZERO);
        assert!(!order.is_open());
    }

    #[test]
    fn overfilling_is_an_error_not_a_silent_clamp() {
        let (instrument, outcome) = ids();
        let mut order = Order::market(
            OrderId(1),
            instrument,
            outcome,
            Side::Buy,
            Qty::from_f64(5.0).unwrap(),
            UnixNanos::new(0),
        )
        .unwrap();
        assert!(order.record_fill(Qty::from_f64(6.0).unwrap()).is_err());
    }

    #[test]
    fn market_orders_have_no_limit_and_default_to_ioc() {
        let (instrument, outcome) = ids();
        let order = Order::market(
            OrderId(1),
            instrument,
            outcome,
            Side::Buy,
            Qty::from_f64(1.0).unwrap(),
            UnixNanos::new(0),
        )
        .unwrap();
        assert_eq!(order.limit_price(), None);
        assert_eq!(order.time_in_force, TimeInForce::ImmediateOrCancel);
    }

    #[test]
    fn limit_orders_carry_their_cap_and_rest_by_default() {
        let (instrument, outcome) = ids();
        let order = Order::limit(
            OrderId(2),
            instrument,
            outcome,
            Side::Sell,
            Price::from_f64(0.6).unwrap(),
            Qty::from_f64(3.0).unwrap(),
            UnixNanos::new(0),
        )
        .unwrap()
        .with_tag("exit")
        .reduce_only();
        assert_eq!(order.limit_price(), Some(Price::from_f64(0.6).unwrap()));
        assert_eq!(order.time_in_force, TimeInForce::GoodTilCancel);
        assert_eq!(order.tag.as_deref(), Some("exit"));
        assert!(order.reduce_only);
    }
}
