//! The run kernel: one deterministic pass over the merged data.
//!
//! The loop invariant, in this order per record:
//!
//! 1. advance the clock to the record's `ts_init`, firing due timers;
//! 2. let the **venue** see the data and update its books;
//! 3. let a print work through the passive queue;
//! 4. match any resting orders the new book crosses;
//! 5. let the **strategy** see the data;
//! 6. drain the commands the strategy queued;
//! 7. release orders whose latency has elapsed and match them.
//!
//! Two orderings in there are load-bearing. The venue sees data before the
//! strategy, so a strategy cannot act on a price the matching engine has
//! not processed. And strategy commands are *queued*, never executed inside
//! the callback, which removes reentrancy entirely and makes latency a
//! property of the queue rather than something each call site remembers.
//!
//! Nothing here reads a wall clock, iterates a hash map without ordering,
//! or draws a random number.

use std::collections::{BTreeMap, BinaryHeap, VecDeque};

use crate::account::{Liquidation, MarginModel, MarginState, margin_requirement, margin_state};
use crate::book::OrderBook;
use crate::clock::{Clock, TimeEvent};
use crate::currency::FxBook;
use crate::error::{BacktestError, Result};
use crate::event::{MarketEvent, Record};
use crate::execution::{ExecutionClient, ExecutionCommand, SimulatedExecution};
use crate::instrument::{InstrumentId, InstrumentSet, OutcomeId};
use crate::models::{BookFills, FeeContext, FeeModel, FillModel, LatencyModel, NoFees, NoLatency};
use crate::order::{Fill, Order, OrderId, OrderKind, OrderStatus, TimeInForce};
use crate::position::Portfolio;
use crate::types::{Money, Price, Qty, Side, UnixNanos, notional};

/// A key identifying one tradable book.
type BookKey = (InstrumentId, OutcomeId);

#[derive(Default)]
struct QueueLevels {
    buys: BTreeMap<i64, Vec<OrderId>>,
    sells: BTreeMap<i64, Vec<OrderId>>,
}

#[derive(Default)]
struct RestingIndex {
    markets: BTreeMap<BookKey, QueueLevels>,
}

impl RestingIndex {
    fn insert(&mut self, order: &Order) {
        let Some(limit) = order.limit_price() else {
            return;
        };
        let market = self
            .markets
            .entry((order.instrument.clone(), order.outcome))
            .or_default();
        let levels = match order.side {
            Side::Buy => &mut market.buys,
            Side::Sell => &mut market.sells,
        };
        let ids = levels.entry(limit.raw()).or_default();
        if let Err(at) = ids.binary_search(&order.id) {
            ids.insert(at, order.id);
        }
    }

    fn remove(&mut self, order: &Order) {
        let Some(limit) = order.limit_price() else {
            return;
        };
        let key = (order.instrument.clone(), order.outcome);
        let mut remove_market = false;
        if let Some(market) = self.markets.get_mut(&key) {
            let levels = match order.side {
                Side::Buy => &mut market.buys,
                Side::Sell => &mut market.sells,
            };
            if let Some(ids) = levels.get_mut(&limit.raw()) {
                if let Ok(at) = ids.binary_search(&order.id) {
                    ids.remove(at);
                }
                if ids.is_empty() {
                    levels.remove(&limit.raw());
                }
            }
            remove_market = market.buys.is_empty() && market.sells.is_empty();
        }
        if remove_market {
            self.markets.remove(&key);
        }
    }

    fn reached_by_trade(
        &self,
        key: &BookKey,
        passive: Side,
        trade_price: Price,
        out: &mut Vec<(Price, OrderId)>,
    ) {
        let Some(market) = self.markets.get(key) else {
            return;
        };
        match passive {
            Side::Buy => {
                for (price, ids) in market.buys.range(trade_price.raw()..).rev() {
                    out.extend(ids.iter().map(|id| (Price::from_raw(*price), *id)));
                }
            }
            Side::Sell => {
                for (price, ids) in market.sells.range(..=trade_price.raw()) {
                    out.extend(ids.iter().map(|id| (Price::from_raw(*price), *id)));
                }
            }
        }
    }

    fn crossing_top(
        &self,
        key: &BookKey,
        best_bid: Option<Price>,
        best_ask: Option<Price>,
        out: &mut Vec<OrderId>,
    ) {
        let Some(market) = self.markets.get(key) else {
            return;
        };
        if let Some(ask) = best_ask {
            for ids in market.buys.range(ask.raw()..).map(|(_, ids)| ids) {
                out.extend(ids.iter().copied());
            }
        }
        if let Some(bid) = best_bid {
            for ids in market.sells.range(..=bid.raw()).map(|(_, ids)| ids) {
                out.extend(ids.iter().copied());
            }
        }
    }

    fn all_for_market(&self, key: &BookKey, out: &mut Vec<OrderId>) {
        let Some(market) = self.markets.get(key) else {
            return;
        };
        for ids in market.buys.values().chain(market.sells.values()) {
            out.extend(ids.iter().copied());
        }
    }
}

/// Default equity sampling interval: one second of simulated time.
pub const DEFAULT_EQUITY_INTERVAL_NANOS: i64 = 1_000_000_000;

/// An order a strategy wants to place. Turned into an [`Order`] by the
/// engine, which assigns the id, so ids stay monotonic in submission order.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct OrderRequest {
    pub instrument: InstrumentId,
    pub outcome: OutcomeId,
    pub side: Side,
    pub kind: OrderKind,
    pub quantity: Qty,
    pub time_in_force: TimeInForce,
    pub tag: Option<String>,
    pub reduce_only: bool,
}

impl OrderRequest {
    pub fn market(instrument: InstrumentId, outcome: OutcomeId, side: Side, quantity: Qty) -> Self {
        Self {
            instrument,
            outcome,
            side,
            kind: OrderKind::Market,
            quantity,
            time_in_force: TimeInForce::ImmediateOrCancel,
            tag: None,
            reduce_only: false,
        }
    }

    pub fn limit(
        instrument: InstrumentId,
        outcome: OutcomeId,
        side: Side,
        limit: Price,
        quantity: Qty,
    ) -> Self {
        Self {
            instrument,
            outcome,
            side,
            kind: OrderKind::Limit { limit },
            quantity,
            time_in_force: TimeInForce::GoodTilCancel,
            tag: None,
            reduce_only: false,
        }
    }

    pub fn with_tag(mut self, tag: impl Into<String>) -> Self {
        self.tag = Some(tag.into());
        self
    }

    pub fn with_time_in_force(mut self, tif: TimeInForce) -> Self {
        self.time_in_force = tif;
        self
    }

    pub fn reduce_only(mut self) -> Self {
        self.reduce_only = true;
        self
    }
}

/// Which primitive of the venue's set contract a strategy invoked.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum SetOperationKind {
    /// Pay one unit of cash, receive one contract on every outcome.
    Mint,
    /// Surrender one contract on every outcome, receive one unit of cash.
    Redeem,
    /// Polymarket's negative-risk conversion: surrender the NO side of
    /// `held` and receive cash plus the YES side of everything else.
    ///
    /// `held` names the outcomes whose NO the strategy holds, which is how
    /// the venue's own adapter is addressed.
    Convert { held: Vec<OutcomeId> },
}

impl SetOperationKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            SetOperationKind::Mint => "mint",
            SetOperationKind::Redeem => "redeem",
            SetOperationKind::Convert { .. } => "convert",
        }
    }
}

/// A complete-set operation a strategy asked the venue to perform.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SetOperationRequest {
    pub instrument: InstrumentId,
    pub kind: SetOperationKind,
    /// Sets to mint or redeem; for a conversion, the number of NO contracts
    /// on each named outcome.
    pub quantity: Qty,
    pub tag: Option<String>,
}

impl SetOperationRequest {
    pub fn mint(instrument: InstrumentId, quantity: Qty) -> Self {
        Self {
            instrument,
            kind: SetOperationKind::Mint,
            quantity,
            tag: None,
        }
    }

    pub fn redeem(instrument: InstrumentId, quantity: Qty) -> Self {
        Self {
            instrument,
            kind: SetOperationKind::Redeem,
            quantity,
            tag: None,
        }
    }

    pub fn convert(instrument: InstrumentId, held: Vec<OutcomeId>, quantity: Qty) -> Self {
        Self {
            instrument,
            kind: SetOperationKind::Convert { held },
            quantity,
            tag: None,
        }
    }

    pub fn with_tag(mut self, tag: impl Into<String>) -> Self {
        self.tag = Some(tag.into());
        self
    }
}

/// What a complete-set operation did, or why it did nothing.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SetOperation {
    pub ts: UnixNanos,
    pub instrument: InstrumentId,
    pub kind: SetOperationKind,
    /// Complete sets actually exchanged. Zero on a rejection, and for a
    /// conversion this is the derived set count, not the NO quantity.
    pub sets: Qty,
    /// Cash the account paid (positive) or received (negative), before the
    /// flat operating cost.
    pub cash_delta: Money,
    /// The flat cost of performing it: a chain transaction, not a fee on
    /// size.
    pub cost: Money,
    /// Set when the venue refused the operation.
    pub rejected: Option<String>,
}

/// A probability the strategy stated, alongside what the market said.
///
/// This is the input a calibration report cannot reconstruct after the
/// fact. A run's fills say what a strategy *did*; only the strategy knows
/// what it *believed*, and scoring a forecaster against the market needs
/// both. Recording the market's own price at the same instant here, rather
/// than joining to a mark series later, is what keeps the pair honest: the
/// comparison is against the price the strategy was actually looking at.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Forecast {
    pub ts: UnixNanos,
    pub instrument: InstrumentId,
    pub outcome: OutcomeId,
    /// What the strategy thought the probability was.
    pub probability: Price,
    /// The market's mark at that instant, when a book had one.
    pub market: Option<Price>,
    pub tag: Option<String>,
}

/// One sample of a market's own probability, on the equity curve's clock.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct MarkPoint {
    pub ts: UnixNanos,
    pub instrument: InstrumentId,
    pub outcome: OutcomeId,
    pub price: Price,
}

/// A command a strategy queued. Never executed inside the callback that
/// produced it.
#[derive(Clone, PartialEq, Eq, Debug)]
enum Command {
    Submit(OrderId, OrderRequest),
    Cancel(OrderId),
    Amend {
        id: OrderId,
        quantity: Option<Qty>,
        limit: Option<Price>,
    },
    Set(SetOperationRequest),
}

/// What a strategy may do and see.
///
/// Note what is absent: any way to reach a [`crate::settlement::Resolution`],
/// or any data beyond the clock's current instant. The strategy cannot look
/// ahead because there is nothing here through which to do it.
pub struct Context<'a> {
    now: UnixNanos,
    books: &'a BTreeMap<BookKey, OrderBook>,
    marks: &'a BTreeMap<BookKey, Price>,
    oracles: &'a BTreeMap<BookKey, Price>,
    portfolio: &'a Portfolio,
    cash: Money,
    commands: &'a mut VecDeque<Command>,
    clock_requests: &'a mut Vec<(String, UnixNanos)>,
    forecasts: &'a mut Vec<Forecast>,
    next_order_id: &'a mut u64,
}

impl<'a> Context<'a> {
    #[inline]
    pub fn now(&self) -> UnixNanos {
        self.now
    }

    #[inline]
    pub fn cash(&self) -> Money {
        self.cash
    }

    pub fn book(&self, instrument: &InstrumentId, outcome: OutcomeId) -> Option<&OrderBook> {
        self.books.get(&(instrument.clone(), outcome))
    }

    pub fn best_bid(&self, instrument: &InstrumentId, outcome: OutcomeId) -> Option<Price> {
        self.book(instrument, outcome)
            .and_then(|b| b.best_bid())
            .map(|(price, _)| price)
    }

    pub fn best_ask(&self, instrument: &InstrumentId, outcome: OutcomeId) -> Option<Price> {
        self.book(instrument, outcome)
            .and_then(|b| b.best_ask())
            .map(|(price, _)| price)
    }

    pub fn position_quantity(&self, instrument: &InstrumentId, outcome: OutcomeId) -> Qty {
        self.portfolio
            .position(instrument, outcome)
            .map(|p| p.quantity)
            .unwrap_or(Qty::ZERO)
    }

    pub fn portfolio(&self) -> &Portfolio {
        self.portfolio
    }

    /// Queue an order. It reaches the venue after the latency model's delay.
    pub fn submit(&mut self, request: OrderRequest) {
        self.submit_tracked(request);
    }

    /// Queue an order and return the stable identifier used by later amend or
    /// cancel commands.
    pub fn submit_tracked(&mut self, request: OrderRequest) -> OrderId {
        *self.next_order_id += 1;
        let id = OrderId(*self.next_order_id);
        self.commands.push_back(Command::Submit(id, request));
        id
    }

    pub fn cancel(&mut self, id: OrderId) {
        self.commands.push_back(Command::Cancel(id));
    }

    /// Change a resting order's size or price.
    ///
    /// Queue priority follows the rule every real venue uses: a price
    /// change or a size *increase* goes to the back of the queue, while a
    /// size *decrease* keeps its place. Modelling amendment as free would
    /// let a strategy sit at the front of a queue and resize at will, which
    /// is the cheapest way to invent maker fills that never happened.
    pub fn amend(&mut self, id: OrderId, quantity: Option<Qty>, limit: Option<Price>) {
        self.commands.push_back(Command::Amend {
            id,
            quantity,
            limit,
        });
    }

    /// Ask for a timer. Honoured after the current record is processed.
    pub fn set_timer(&mut self, name: impl Into<String>, at: UnixNanos) {
        self.clock_requests.push((name.into(), at));
    }

    /// The price this run values the position at: the venue's mark where it
    /// publishes one, the book otherwise.
    pub fn mark(&self, instrument: &InstrumentId, outcome: OutcomeId) -> Option<Price> {
        self.marks.get(&(instrument.clone(), outcome)).copied()
    }

    /// The venue's oracle price, where it publishes one.
    ///
    /// Distinct from the mark and from the mid, and worth reading directly:
    /// the spread between oracle and book is the premium that funding is
    /// computed from, and a basis strategy is trading exactly that.
    pub fn oracle(&self, instrument: &InstrumentId, outcome: OutcomeId) -> Option<Price> {
        self.oracles.get(&(instrument.clone(), outcome)).copied()
    }

    /// Pay one unit of cash per set and receive one contract on every
    /// outcome.
    ///
    /// This is the venue's own primitive -- Polymarket's `split`, the
    /// conditional-token contract's mint -- and modelling it is what makes
    /// the sum-to-one relationship *transactable* rather than merely
    /// observable. Without it a strategy cannot supply both sides of a book
    /// without first buying them, so complete-set market making and the
    /// arbitrage that bounds a book to a dollar are both unreachable.
    ///
    /// Queued like an order, and subject to the same insertion latency: a
    /// set operation is a chain transaction, and one that lands instantly is
    /// an arbitrage nobody could have taken.
    pub fn mint(&mut self, instrument: &InstrumentId, sets: Qty) {
        self.commands
            .push_back(Command::Set(SetOperationRequest::mint(
                instrument.clone(),
                sets,
            )));
    }

    /// Surrender one contract on every outcome per set and receive one unit
    /// of cash.
    pub fn redeem(&mut self, instrument: &InstrumentId, sets: Qty) {
        self.commands
            .push_back(Command::Set(SetOperationRequest::redeem(
                instrument.clone(),
                sets,
            )));
    }

    /// Perform a negative-risk conversion on the outcomes whose NO side is
    /// held.
    ///
    /// See [`SetOperationKind::Convert`]. `held` must name at least two
    /// outcomes and leave at least one out, which is the residual the
    /// conversion pays you in.
    pub fn convert(&mut self, instrument: &InstrumentId, held: Vec<OutcomeId>, quantity: Qty) {
        self.commands
            .push_back(Command::Set(SetOperationRequest::convert(
                instrument.clone(),
                held,
                quantity,
            )));
    }

    /// Queue a complete-set operation directly.
    pub fn set_operation(&mut self, request: SetOperationRequest) {
        self.commands.push_back(Command::Set(request));
    }

    /// State the probability this strategy assigns to an outcome.
    ///
    /// Purely an observation: it moves no cash, places no order, and the
    /// engine never reads it back. It exists so a run can be *scored* --
    /// Brier, reliability, advantage over the market -- against what the
    /// strategy believed rather than against a rolling mean of the price it
    /// traded, which is a proxy for a forecast and not a forecast.
    ///
    /// The market's mark at this instant is captured alongside it.
    pub fn record_forecast(
        &mut self,
        instrument: &InstrumentId,
        outcome: OutcomeId,
        probability: Price,
    ) -> Result<()> {
        self.record_tagged_forecast(instrument, outcome, probability, None)
    }

    /// [`Context::record_forecast`], with a label carried onto the sample so
    /// a report can score several models from one run separately.
    pub fn record_tagged_forecast(
        &mut self,
        instrument: &InstrumentId,
        outcome: OutcomeId,
        probability: Price,
        tag: Option<String>,
    ) -> Result<()> {
        if probability < Price::PROBABILITY_MIN || probability > Price::PROBABILITY_MAX {
            return Err(BacktestError::invalid(format!(
                "a forecast probability must lie in [0, 1], got {probability}"
            )));
        }
        self.forecasts.push(Forecast {
            ts: self.now,
            instrument: instrument.clone(),
            outcome,
            probability,
            market: self.mark(instrument, outcome),
            tag,
        });
        Ok(())
    }
}

/// What a strategy implements. Every method has a default, so a strategy
/// that only cares about bars writes only `on_event`.
pub trait Strategy {
    fn on_start(&mut self, _ctx: &mut Context<'_>) -> Result<()> {
        Ok(())
    }
    fn on_event(&mut self, _ctx: &mut Context<'_>, _record: &Record) -> Result<()> {
        Ok(())
    }
    fn on_timer(&mut self, _ctx: &mut Context<'_>, _event: &TimeEvent) -> Result<()> {
        Ok(())
    }
    fn on_fill(&mut self, _ctx: &mut Context<'_>, _fill: &Fill) -> Result<()> {
        Ok(())
    }
    fn on_stop(&mut self, _ctx: &mut Context<'_>) -> Result<()> {
        Ok(())
    }
}

/// Stands in where the engine itself initiates a fill (a delisting cash
/// settlement), which no strategy asked for and none should be notified of
/// as if it had.
struct NoStrategy;

impl Strategy for NoStrategy {}

/// What is waiting out its latency: an order, or a set operation.
///
/// Both travel the same queue so that their arrival order is one total
/// order rather than two that happen to interleave. A mint that lands
/// before the order it was meant to hedge, or after, is a different run,
/// and which one it is should not depend on which heap was drained first.
#[derive(PartialEq, Eq)]
enum InFlightAction {
    Order(Box<Order>),
    Set(SetOperationRequest),
}

/// An action waiting out its latency before the venue sees it.
#[derive(PartialEq, Eq)]
struct InFlight {
    release_at: i64,
    sequence: u64,
    action: InFlightAction,
}

impl Ord for InFlight {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reversed: BinaryHeap is a max-heap and we want the earliest.
        other
            .release_at
            .cmp(&self.release_at)
            .then_with(|| other.sequence.cmp(&self.sequence))
    }
}
impl PartialOrd for InFlight {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// What a run did, in counts.
///
/// A backtest that produces no trades is the commonest outcome and the
/// least explicable one: the summary says zero and nothing says why. These
/// counters are the answer -- an order that never met a book, one refused
/// for margin, one that sat behind a queue that never cleared, and a feed
/// that went stale are four different silences with four different fixes.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct RunMetrics {
    /// Replayed records, by event kind.
    pub records_by_kind: BTreeMap<&'static str, u64>,
    pub orders_submitted: u64,
    pub orders_filled: u64,
    pub orders_partially_filled: u64,
    /// Cancelled with nothing filled: no book, or a limit never reached.
    pub orders_cancelled_unfilled: u64,
    pub orders_rejected_margin: u64,
    pub orders_rejected_risk: u64,
    pub orders_rejected_self_trade: u64,
    /// Sells that would have opened short exposure in an outcome nobody can
    /// borrow. See [`EngineBuilder::allow_naked_shorts`].
    pub orders_rejected_naked_short: u64,
    /// Orders submitted, or arriving after their latency, once the
    /// instrument had stopped trading.
    pub orders_rejected_expired: u64,
    pub orders_amended: u64,
    pub fills_taker: u64,
    pub fills_maker: u64,
    /// Feed gaps that invalidated a book.
    pub book_gaps: u64,
    /// Times a resting order sat behind queue volume it never cleared.
    pub queue_joins: u64,
    pub liquidations: u64,
    pub corporate_actions: u64,
    /// Complete sets minted, redeemed or converted.
    pub set_operations: u64,
    /// Set operations the account could not fund or did not hold.
    pub set_operations_rejected: u64,
    /// Instruments that stopped trading during the run.
    pub instruments_expired: u64,
}

impl RunMetrics {
    fn record(&mut self, kind: &'static str) {
        *self.records_by_kind.entry(kind).or_insert(0) += 1;
    }

    /// A one-line account of why a run may have done nothing.
    pub fn explain_silence(&self) -> Option<String> {
        if self.orders_submitted == 0 {
            return Some(
                "the strategy submitted no orders: check that its signals fall \
                 inside the replayed window"
                    .to_string(),
            );
        }
        if self.orders_filled == 0 && self.orders_partially_filled == 0 {
            let mut reasons = Vec::new();
            if self.orders_rejected_margin > 0 {
                reasons.push(format!(
                    "{} refused for margin",
                    self.orders_rejected_margin
                ));
            }
            if self.orders_rejected_risk > 0 {
                reasons.push(format!(
                    "{} refused by configured risk limits",
                    self.orders_rejected_risk
                ));
            }
            if self.orders_rejected_self_trade > 0 {
                reasons.push(format!(
                    "{} would have crossed the account's own book",
                    self.orders_rejected_self_trade
                ));
            }
            if self.orders_rejected_naked_short > 0 {
                reasons.push(format!(
                    "{} would have shorted an outcome the venue does not let \
                     you borrow; buy the complementary outcome instead",
                    self.orders_rejected_naked_short
                ));
            }
            if self.orders_rejected_expired > 0 {
                reasons.push(format!(
                    "{} arrived after their instrument had stopped trading",
                    self.orders_rejected_expired
                ));
            }
            if self.orders_cancelled_unfilled > 0 {
                reasons.push(format!(
                    "{} found no liquidity at their price",
                    self.orders_cancelled_unfilled
                ));
            }
            if self.book_gaps > 0 {
                reasons.push(format!("{} feed gaps invalidated books", self.book_gaps));
            }
            let detail = if reasons.is_empty() {
                "no order reached a matchable book".to_string()
            } else {
                reasons.join("; ")
            };
            return Some(format!(
                "{} orders, no fills: {detail}",
                self.orders_submitted
            ));
        }
        None
    }
}

/// Account-level order limits enforced before venue latency begins.
#[derive(Clone, Copy, Debug, Default)]
pub struct RiskLimits {
    pub max_order_quantity: Option<Qty>,
    pub max_abs_position: Option<Qty>,
    pub max_open_orders: Option<usize>,
}

impl RiskLimits {
    pub fn new(
        max_order_quantity: Option<Qty>,
        max_abs_position: Option<Qty>,
        max_open_orders: Option<usize>,
    ) -> Result<Self> {
        if max_order_quantity.is_some_and(|value| value.raw() <= 0) {
            return Err(BacktestError::invalid(
                "max_order_quantity must be positive",
            ));
        }
        if max_abs_position.is_some_and(|value| value.raw() <= 0) {
            return Err(BacktestError::invalid("max_abs_position must be positive"));
        }
        if max_open_orders == Some(0) {
            return Err(BacktestError::invalid("max_open_orders must be positive"));
        }
        Ok(Self {
            max_order_quantity,
            max_abs_position,
            max_open_orders,
        })
    }
}

/// One sample of the equity curve.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct EquityPoint {
    pub ts: UnixNanos,
    pub cash: Money,
    /// Marked value of open positions.
    pub position_value: Money,
    /// `cash + position_value`.
    pub equity: Money,
    pub realized_pnl: Money,
    pub unrealized_pnl: Money,
}

/// How a run ended.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct RunResult {
    pub fills: Vec<Fill>,
    pub orders: Vec<Order>,
    pub final_cash: Money,
    pub starting_cash: Money,
    pub realized_pnl: Money,
    pub commissions: Money,
    /// The last instant actually replayed. `None` when no data arrived.
    /// Settlement is gated on this, not on the requested window.
    pub simulated_through: Option<UnixNanos>,
    pub records_processed: u64,
    /// The last mark seen per book, for valuing what stayed open.
    pub marks: BTreeMap<BookKey, Price>,
    /// The equity curve, sampled on a fixed interval of simulated time.
    pub equity: Vec<EquityPoint>,
    /// Net funding paid out over the run. Negative means funding was
    /// received, which is the whole point of a carry strategy.
    pub funding_paid: Money,
    /// Positions the venue closed because the account fell below its
    /// maintenance requirement.
    /// Cash received from dividends, net of what a short paid.
    pub dividends_received: Money,
    pub liquidations: Vec<Liquidation>,
    /// Orders refused because the account could not fund them.
    pub rejected_for_margin: u64,
    /// Orders refused because they would have crossed with this account's
    /// own resting book.
    pub self_trades_prevented: u64,
    /// Every complete-set operation attempted, in order, including the ones
    /// the venue refused.
    pub set_operations: Vec<SetOperation>,
    /// Probabilities the strategy stated, each paired with the market's own
    /// price at that instant.
    pub forecasts: Vec<Forecast>,
    /// The market's own probabilities, sampled on the equity curve's clock.
    /// Empty unless [`EngineBuilder::record_mark_curve`] is on.
    pub mark_curve: Vec<MarkPoint>,
    /// Instruments that stopped trading during the run, with the instant.
    pub expirations: Vec<(InstrumentId, UnixNanos)>,
    /// Counters explaining what the run did and did not do.
    pub metrics: RunMetrics,
}

/// The engine.
pub struct Engine {
    clock: Clock,
    instruments: InstrumentSet,
    books: BTreeMap<BookKey, OrderBook>,
    portfolio: Portfolio,
    cash: Money,
    starting_cash: Money,
    orders: BTreeMap<OrderId, Order>,
    resting: Vec<OrderId>,
    resting_index: RestingIndex,
    matching_candidates: Vec<OrderId>,
    inflight: BinaryHeap<InFlight>,
    commands: VecDeque<Command>,
    clock_requests: Vec<(String, UnixNanos)>,
    fills: Vec<Fill>,
    marks: BTreeMap<BookKey, Price>,
    fee_model: Box<dyn FeeModel>,
    fill_model: Box<dyn FillModel>,
    latency_model: Box<dyn LatencyModel>,
    next_order_id: u64,
    sequence: u64,
    records: u64,
    simulated_through: Option<UnixNanos>,
    equity_interval: i64,
    next_equity_sample: Option<i64>,
    equity: Vec<EquityPoint>,
    funding_paid: Money,
    /// Displayed size still ahead of each resting order at its price.
    queue_ahead: BTreeMap<OrderId, i64>,
    /// Reused scratch space for price-prioritising orders reached by a trade.
    queue_candidates: Vec<(Price, OrderId)>,
    margin: Option<Box<dyn MarginModel>>,
    fx: FxBook,
    liquidations: Vec<Liquidation>,
    rejected_for_margin: u64,
    self_trades_prevented: u64,
    reporting_currency: crate::currency::Currency,
    dividends_received: Money,
    delisted: std::collections::BTreeSet<InstrumentId>,
    metrics: RunMetrics,
    execution: Box<dyn ExecutionClient>,
    risk_limits: RiskLimits,
    /// Whether a sell may open short exposure in a market outcome.
    allow_naked_shorts: bool,
    /// Every instrument's expiry, ascending, with a cursor into it.
    expiries: Vec<(i64, InstrumentId)>,
    next_expiry: usize,
    expired: std::collections::BTreeSet<InstrumentId>,
    expirations: Vec<(InstrumentId, UnixNanos)>,
    set_costs: SetOperationCosts,
    set_operations: Vec<SetOperation>,
    forecasts: Vec<Forecast>,
    record_mark_curve: bool,
    mark_curve: Vec<MarkPoint>,
    /// What the book itself last said: a mid, a print, or a bar close.
    book_marks: BTreeMap<BookKey, Price>,
    /// What the venue published as its mark, where it publishes one.
    venue_marks: BTreeMap<BookKey, Price>,
    /// What the venue published as its oracle, which funding is charged on.
    oracles: BTreeMap<BookKey, Price>,
    mark_source: MarkSource,
}

/// Which price a run values positions at.
///
/// A derivatives venue does not margin you against the mid. Hyperliquid
/// publishes a mark built from an oracle and the book, and margin,
/// unrealised PnL and liquidation all read that rather than the top of
/// book. Valuing at the mid instead means a thin book or a single wick can
/// liquidate a position the venue would never have touched -- and the
/// resulting loss then reads as a strategy result rather than as a
/// modelling artefact.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum MarkSource {
    /// Use the venue's published mark where there is one, and the book
    /// otherwise. The default, because it is a no-op on data that carries
    /// no reference prices and the right answer on data that does.
    #[default]
    VenueMark,
    /// Always value against the book, ignoring any published mark.
    /// For isolating what the venue's own mark was doing.
    BookMid,
}

/// What performing a complete-set operation costs, regardless of size.
///
/// A mint or redeem is one chain transaction. Its cost does not scale with
/// the number of sets, which is exactly why it matters: a flat cost is what
/// makes a one-cent-wide complete-set arbitrage unprofitable at small size
/// and profitable at large, and a model that omits it reports every such
/// edge as free money.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct SetOperationCosts {
    pub mint: Money,
    pub redeem: Money,
    pub convert: Money,
}

impl SetOperationCosts {
    /// The same flat cost for every operation.
    pub fn flat(cost: Money) -> Result<Self> {
        if cost.is_negative() {
            return Err(BacktestError::invalid(
                "a set operation cost must not be negative",
            ));
        }
        Ok(Self {
            mint: cost,
            redeem: cost,
            convert: cost,
        })
    }

    fn for_kind(&self, kind: &SetOperationKind) -> Money {
        match kind {
            SetOperationKind::Mint => self.mint,
            SetOperationKind::Redeem => self.redeem,
            SetOperationKind::Convert { .. } => self.convert,
        }
    }
}

/// Everything about a fill that is decided before it is booked.
///
/// Grouped rather than passed loose because the commission is already
/// settled by the time this is built -- the fee model priced a trade, or a
/// set operation charged its flat cost -- and a signature that takes a
/// price, a quantity and a commission side by side invites passing them in
/// the wrong order.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct FillTerms {
    price: Price,
    quantity: Qty,
    origin: FillOrigin,
    commission: Money,
}

/// Where a fill came from, which decides how it is priced and counted.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum FillOrigin {
    /// Matched against a book. The fee model prices it.
    Trade { is_taker: bool },
    /// A leg of a complete-set operation. Not a trade: no counterparty, no
    /// spread crossed, and no trading fee -- the operation's own flat cost
    /// is charged once, not per leg.
    CompleteSet,
}

impl Engine {
    pub fn builder(instruments: InstrumentSet) -> EngineBuilder {
        EngineBuilder {
            instruments,
            starting_cash: Money::ZERO,
            start: UnixNanos::new(0),
            fee_model: Box::new(NoFees),
            fill_model: Box::new(BookFills),
            latency_model: Box::new(NoLatency),
            equity_interval: DEFAULT_EQUITY_INTERVAL_NANOS,
            margin: None,
            fx: FxBook::new(),
            execution: Box::new(SimulatedExecution::new("simulated")),
            // The currency cash is held and results are reported in.
            // Instruments settling elsewhere need a rate to join it.
            reporting_currency: crate::currency::Currency::new("USDC")
                .expect("USDC is a valid code"),
            risk_limits: RiskLimits::default(),
            allow_naked_shorts: false,
            set_costs: SetOperationCosts::default(),
            record_mark_curve: true,
            mark_source: MarkSource::default(),
        }
    }

    /// Run a replay to exhaustion.
    pub fn run(
        &mut self,
        replay: &mut crate::replay::Replay,
        strategy: &mut dyn Strategy,
    ) -> Result<RunResult> {
        self.with_context(|ctx| strategy.on_start(ctx))?;
        self.drain_commands()?;

        while let Some(record) = replay.next_record()? {
            self.step(&record, strategy)?;
        }

        self.with_context(|ctx| strategy.on_stop(ctx))?;
        self.drain_commands()?;
        self.finish()
    }

    fn step(&mut self, record: &Record, strategy: &mut dyn Strategy) -> Result<()> {
        let ts = record.stamps.ts_init;

        // 1. Time moves, and timers due before this record fire first.
        for event in self.clock.advance_to(ts)? {
            self.with_context(|ctx| strategy.on_timer(ctx, &event))?;
            self.drain_commands()?;
        }

        // 1b. Anything whose trading window closed on the way here stops
        //     being tradable now, before the strategy gets another turn.
        self.expire_due(ts)?;

        // 2. The venue sees the data before anyone else.
        self.apply_to_books(record)?;

        // 3. A print works through the passive queue before anything else
        //    gets to react to it.
        if let MarketEvent::Trade {
            price,
            size,
            aggressor,
        } = &record.event
        {
            self.consume_queue(record, *price, *size, *aggressor, strategy)?;
        }

        // 4. Resting orders the new book crosses.
        self.match_resting(record, strategy)?;

        // 4b. A mark that moved may have made the account insolvent. The
        //     venue acts before the strategy gets another turn, which is
        //     the order a real one would use.
        self.check_liquidation(ts, strategy)?;

        // 5. Now the strategy.
        self.with_context(|ctx| strategy.on_event(ctx, record))?;

        // 6. Whatever it asked for.
        self.drain_commands()?;

        // 7. Orders whose latency has elapsed reach the venue.
        self.release_inflight(ts, strategy)?;

        self.records += 1;
        self.metrics.record(record.event.kind());
        self.simulated_through = Some(ts);
        self.sample_equity(ts)?;
        Ok(())
    }

    /// Stop trading anything whose expiry the clock has now passed.
    ///
    /// Trading stops; the data does not. A venue keeps publishing after a
    /// market closes -- late prints, a final book teardown, the settlement
    /// itself -- and those records are still legitimate to observe. What
    /// must not happen is a fill against them, which is what an unenforced
    /// `expiration` allows: an order matching a book that only exists
    /// because the market had already closed.
    ///
    /// Resting orders are cancelled rather than left to rot, because an
    /// order that can never fill and never terminates is indistinguishable
    /// in a report from one still working.
    fn expire_due(&mut self, ts: UnixNanos) -> Result<()> {
        while self.next_expiry < self.expiries.len()
            && self.expiries[self.next_expiry].0 <= ts.get()
        {
            let (at, instrument) = self.expiries[self.next_expiry].clone();
            self.next_expiry += 1;
            if !self.expired.insert(instrument.clone()) {
                continue;
            }
            let doomed: Vec<OrderId> = self
                .resting
                .iter()
                .copied()
                .filter(|id| {
                    self.orders
                        .get(id)
                        .is_some_and(|order| order.instrument == instrument)
                })
                .collect();
            for id in doomed {
                if let Some(order) = self.orders.get_mut(&id)
                    && order.is_open()
                {
                    order.status = OrderStatus::Cancelled;
                    order.reject_reason = Some(format!("{instrument} stopped trading at {at}"));
                }
                self.remove_resting(id);
            }
            self.expirations.push((instrument, UnixNanos::new(at)));
            self.metrics.instruments_expired += 1;
        }
        Ok(())
    }

    /// Whether this instrument has stopped trading as of `ts`.
    ///
    /// Reads the instrument rather than the expired set, so an order whose
    /// latency carries it past the expiry is caught even when no record has
    /// arrived to advance the sweep.
    fn has_expired(&self, instrument: &InstrumentId, ts: UnixNanos) -> Option<UnixNanos> {
        self.instruments
            .get(instrument)
            .ok()
            .and_then(|found| found.expiration)
            .filter(|at| ts >= *at)
    }

    /// The account's margin position right now, or `None` when no margin
    /// model is in force (a cash account cannot be called).
    pub fn margin_state(&self) -> Result<Option<MarginState>> {
        let Some(model) = self.margin.as_deref() else {
            return Ok(None);
        };
        let requirement = margin_requirement(
            model,
            self.portfolio.open_positions(),
            &self.instruments,
            &self.marks,
        )?;
        // Cash is held in the reporting currency. Positions settling in
        // another one need a rate to join it, and a position whose currency
        // has no rate is *not* folded in at par -- it is reported as
        // unconvertible, which suppresses any liquidation call, because
        // closing a book on a number known to be partial is worse than
        // waiting for the rate.
        let mut unconvertible = Vec::new();
        let mut unrealized = Money::ZERO;
        for position in self.portfolio.open_positions() {
            let key = (position.instrument.clone(), position.outcome);
            let Some(mark) = self.marks.get(&key) else {
                continue;
            };
            let pnl = position.unrealized_pnl(*mark)?;
            let settlement = &self
                .instruments
                .get(&position.instrument)?
                .settlement_currency;
            if settlement == &self.reporting_currency {
                unrealized = unrealized.checked_add(pnl)?;
            } else {
                match self.fx.convert(pnl, settlement, &self.reporting_currency) {
                    Ok(converted) => unrealized = unrealized.checked_add(converted)?,
                    Err(_) => unconvertible.push((settlement.clone(), pnl)),
                }
            }
        }
        let collateral = crate::account::Valuation {
            total: self.cash,
            currency: self.reporting_currency.clone(),
            unconvertible,
        };
        Ok(Some(margin_state(&collateral, unrealized, &requirement)?))
    }

    /// Close everything if the account has fallen below maintenance.
    ///
    /// Liquidation is all-or-nothing here rather than partial. A partial
    /// close needs a venue-specific rule for how much to take, and inventing
    /// one would be a guess dressed as a model; closing the book is the
    /// conservative reading and is what a margin call ultimately means.
    /// Positions are closed in instrument order so the sequence is the same
    /// on every run.
    fn check_liquidation(&mut self, ts: UnixNanos, strategy: &mut dyn Strategy) -> Result<()> {
        let Some(state) = self.margin_state()? else {
            return Ok(());
        };
        if !state.liquidatable {
            return Ok(());
        }
        let doomed: Vec<(InstrumentId, OutcomeId, Qty)> = self
            .portfolio
            .open_positions()
            .map(|p| (p.instrument.clone(), p.outcome, p.quantity))
            .collect();
        for (instrument, outcome, quantity) in doomed {
            let key = (instrument.clone(), outcome);
            let Some(mark) = self.marks.get(&key).copied() else {
                continue;
            };
            // Close by crossing the book in the opposite direction.
            let side = if quantity.is_positive() {
                Side::Sell
            } else {
                Side::Buy
            };
            self.next_order_id += 1;
            let id = OrderId(self.next_order_id);
            let mut order = Order::new(
                id,
                instrument.clone(),
                outcome,
                side,
                OrderKind::Market,
                Qty::from_raw(quantity.raw().abs()),
                TimeInForce::ImmediateOrCancel,
                ts,
            )?;
            order.status = OrderStatus::Accepted;
            order.accepted_at = Some(ts);
            order.tag = Some("liquidation".to_string());
            self.orders.insert(id, order);
            self.try_fill(id, ts, strategy)?;

            self.metrics.liquidations += 1;
            self.liquidations.push(Liquidation {
                instrument,
                outcome,
                quantity,
                mark,
                equity: state.equity,
                maintenance: state.maintenance,
            });
        }
        Ok(())
    }

    /// Whether the account can fund a new order.
    fn can_fund(&self, order: &Order) -> Result<bool> {
        let Some(model) = self.margin.as_deref() else {
            return Ok(true);
        };
        let Some(state) = self.margin_state()? else {
            return Ok(true);
        };
        // An incomplete valuation is not a licence to trade freely, but
        // neither is it grounds to refuse: it is reported and allowed, in
        // the same spirit as not liquidating on partial information.
        if state.incomplete {
            return Ok(true);
        }
        let key = (order.instrument.clone(), order.outcome);
        let Some(mark) = self.marks.get(&key) else {
            return Ok(true);
        };
        let instrument = self.instruments.get(&order.instrument)?;
        let needed = model.initial_margin(instrument, order.remaining(), *mark)?;
        Ok(state.available() >= needed)
    }

    /// Record an equity point when the sampling interval has elapsed.
    ///
    /// The first record always produces one, so a curve never starts after
    /// the strategy has already traded.
    fn sample_equity(&mut self, ts: UnixNanos) -> Result<()> {
        let due = match self.next_equity_sample {
            None => true,
            Some(next) => ts.get() >= next,
        };
        if !due {
            return Ok(());
        }
        self.push_equity(ts)?;
        self.next_equity_sample = Some(ts.get().saturating_add(self.equity_interval));
        Ok(())
    }

    fn push_equity(&mut self, ts: UnixNanos) -> Result<()> {
        let mut position_value = Money::ZERO;
        let mut unrealized = Money::ZERO;
        for position in self.portfolio.open_positions() {
            let key = (position.instrument.clone(), position.outcome);
            let Some(mark) = self.marks.get(&key) else {
                continue;
            };
            position_value = position_value.checked_add(position.exposure(*mark)?)?;
            unrealized = unrealized.checked_add(position.unrealized_pnl(*mark)?)?;
        }
        self.equity.push(EquityPoint {
            ts,
            cash: self.cash,
            position_value,
            equity: self.cash.checked_add(position_value)?,
            realized_pnl: self.portfolio.realized_pnl()?,
            unrealized_pnl: unrealized,
        });
        self.sample_marks(ts);
        Ok(())
    }

    /// Record what every probability book thought, on the equity clock.
    ///
    /// Only probability instruments, because this series exists to be
    /// scored: a market price on a prediction market *is* a forecast, and
    /// scoring a strategy against it is the comparison a calibration report
    /// is for. A perpetual's mark is a price, not a probability, and has no
    /// place in that sample.
    fn sample_marks(&mut self, ts: UnixNanos) {
        if !self.record_mark_curve {
            return;
        }
        for ((instrument, outcome), price) in &self.marks {
            let is_probability = self
                .instruments
                .get(instrument)
                .map(|found| found.kind.is_probability())
                .unwrap_or(false);
            if !is_probability {
                continue;
            }
            self.mark_curve.push(MarkPoint {
                ts,
                instrument: instrument.clone(),
                outcome: *outcome,
                price: *price,
            });
        }
    }

    fn with_context<T>(&mut self, f: impl FnOnce(&mut Context<'_>) -> Result<T>) -> Result<T> {
        let mut ctx = Context {
            now: self.clock.now(),
            books: &self.books,
            marks: &self.marks,
            oracles: &self.oracles,
            portfolio: &self.portfolio,
            cash: self.cash,
            commands: &mut self.commands,
            clock_requests: &mut self.clock_requests,
            forecasts: &mut self.forecasts,
            next_order_id: &mut self.next_order_id,
        };
        let out = f(&mut ctx)?;
        let requests = std::mem::take(&mut self.clock_requests);
        for (name, at) in requests {
            self.clock.set_timer(name, at)?;
        }
        Ok(out)
    }

    fn apply_to_books(&mut self, record: &Record) -> Result<()> {
        let key = (record.instrument.clone(), record.outcome);
        if !self.instruments.contains(&record.instrument) {
            return Err(BacktestError::UnknownInstrument(
                record.instrument.to_string(),
            ));
        }
        self.instruments
            .get(&record.instrument)?
            .check_outcome(record.outcome)?;
        let ts = record.stamps.ts_init;
        let book = self.books.entry(key.clone()).or_default();
        match &record.event {
            MarketEvent::BookSnapshot { bids, asks } => {
                book.apply_snapshot(bids, asks, ts)?;
            }
            MarketEvent::BookDelta(delta) => {
                book.apply_delta(record.instrument.as_str(), *delta, ts)?;
            }
            MarketEvent::Gap => {
                book.mark_gap(ts);
                self.metrics.book_gaps += 1;
            }
            MarketEvent::Trade { price, .. } => {
                self.book_marks.insert(key.clone(), *price);
            }
            MarketEvent::Bar { close, .. } => {
                self.book_marks.insert(key.clone(), *close);
            }
            MarketEvent::Reference { mark, oracle } => {
                if let Some(price) = mark {
                    self.venue_marks.insert(key.clone(), *price);
                }
                if let Some(price) = oracle {
                    self.oracles.insert(key.clone(), *price);
                }
            }
            MarketEvent::Funding { rate } => {
                self.apply_funding(&record.instrument, *rate)?;
            }
            MarketEvent::Corporate(action) => {
                self.apply_corporate_action(&record.instrument, *action, ts)?;
            }
        }
        if let Some(mid) = self.books.get(&key).and_then(|b| b.mid()) {
            self.book_marks.insert(key.clone(), mid);
        }
        self.refresh_mark(&key);
        Ok(())
    }

    /// Recompute the mark this run values positions at.
    ///
    /// `marks` is the *effective* mark, so every existing reader -- margin,
    /// equity, liquidation, settlement -- gets the configured price without
    /// having to know where it came from. The book-derived and
    /// venue-published prices are kept apart underneath so switching between
    /// them is a policy rather than a rewrite.
    fn refresh_mark(&mut self, key: &BookKey) {
        let effective = match self.mark_source {
            MarkSource::VenueMark => self
                .venue_marks
                .get(key)
                .or_else(|| self.book_marks.get(key))
                .copied(),
            MarkSource::BookMid => self.book_marks.get(key).copied(),
        };
        if let Some(price) = effective {
            self.marks.insert(key.clone(), price);
        }
    }

    /// Settle a funding payment against every open position in an
    /// instrument.
    ///
    /// `payment = position * price * rate`, charged to the holder, so a
    /// positive rate takes cash from longs and pays it to shorts. A position
    /// with no price cannot be valued and is skipped rather than funded at a
    /// guessed one -- silently funding at the entry price would make a
    /// stale position drift for free.
    ///
    /// The price is the venue's **oracle** where it publishes one, not the
    /// book. Hyperliquid charges funding on the oracle precisely so that a
    /// thin book cannot inflate or deflate a payment, and charging on the
    /// mid instead reintroduces the manipulation the oracle exists to
    /// prevent -- at hourly settlement, over a long carry, that compounds.
    fn apply_funding(&mut self, instrument: &InstrumentId, rate: Price) -> Result<()> {
        let mut total = Money::ZERO;
        for position in self.portfolio.open_positions() {
            if &position.instrument != instrument {
                continue;
            }
            let key = (position.instrument.clone(), position.outcome);
            let Some(mark) = self.oracles.get(&key).or_else(|| self.marks.get(&key)) else {
                continue;
            };
            let exposure = position.exposure(*mark)?;
            let payment = notional(rate, Qty::from_raw(exposure.raw()))?;
            total = total.checked_sub(payment)?;
        }
        if !total.is_zero() {
            self.cash = self.cash.checked_add(total)?;
            self.funding_paid = self.funding_paid.checked_sub(total)?;
        }
        Ok(())
    }

    /// Apply a corporate action to positions, resting orders and cash.
    ///
    /// Forward only: nothing already replayed is rewritten. A split scales
    /// the position and any resting order together -- a limit at 50 on a
    /// stock that just halved is a limit at 25 for twice the size, which is
    /// what the venue does and what stops an untouched order from becoming
    /// wildly marketable the instant the split lands.
    fn apply_corporate_action(
        &mut self,
        instrument: &InstrumentId,
        action: crate::corporate::CorporateAction,
        ts: UnixNanos,
    ) -> Result<()> {
        use crate::corporate::CorporateAction;
        action.validate()?;
        match action {
            CorporateAction::Split { ratio } => {
                self.portfolio.apply_split(instrument, ratio)?;
                let resting: Vec<OrderId> = self.resting.clone();
                for id in resting {
                    let Some(order) = self.orders.get(&id).cloned() else {
                        continue;
                    };
                    if &order.instrument != instrument {
                        continue;
                    }
                    let Some(limit) = order.limit_price() else {
                        continue;
                    };
                    let (quantity, price) =
                        CorporateAction::split_position(ratio, order.quantity, limit)?;
                    self.resting_index.remove(&order);
                    if let Some(order) = self.orders.get_mut(&id) {
                        order.quantity = quantity;
                        order.kind = OrderKind::Limit { limit: price };
                    }
                    if let Some(order) = self.orders.get(&id) {
                        self.resting_index.insert(order);
                    }
                }
                // Marks are quoted prices, and the quote is now post-split.
                let keys: Vec<BookKey> = self
                    .marks
                    .keys()
                    .filter(|(id, _)| id == instrument)
                    .cloned()
                    .collect();
                for key in keys {
                    if let Some(mark) = self.marks.get(&key).copied() {
                        let (_, adjusted) =
                            CorporateAction::split_position(ratio, Qty::ZERO, mark)?;
                        self.marks.insert(key, adjusted);
                    }
                }
                self.metrics.corporate_actions += 1;
            }
            CorporateAction::Dividend { per_share } => {
                let due = self.portfolio.dividend_due(instrument, per_share)?;
                if !due.is_zero() {
                    self.cash = self.cash.checked_add(due)?;
                    self.dividends_received = self.dividends_received.checked_add(due)?;
                }
                self.metrics.corporate_actions += 1;
            }
            CorporateAction::Delist { final_price } => {
                // Cash out whatever is open at the delisting price, then
                // the instrument is untradeable for the rest of the run.
                let open: Vec<(InstrumentId, OutcomeId, Qty)> = self
                    .portfolio
                    .open_positions()
                    .filter(|position| &position.instrument == instrument)
                    .map(|position| {
                        (
                            position.instrument.clone(),
                            position.outcome,
                            position.quantity,
                        )
                    })
                    .collect();
                for (id, outcome, quantity) in open {
                    let side = if quantity.is_positive() {
                        Side::Sell
                    } else {
                        Side::Buy
                    };
                    self.next_order_id += 1;
                    let order_id = OrderId(self.next_order_id);
                    let mut order = Order::new(
                        order_id,
                        id.clone(),
                        outcome,
                        side,
                        OrderKind::Market,
                        Qty::from_raw(quantity.raw().abs()),
                        TimeInForce::ImmediateOrCancel,
                        ts,
                    )?;
                    order.status = OrderStatus::Accepted;
                    order.accepted_at = Some(ts);
                    order.tag = Some("delisting".to_string());
                    self.orders.insert(order_id, order);
                    // Settled at the stated price, not against a book that
                    // may no longer exist.
                    self.execute(
                        order_id,
                        final_price,
                        Qty::from_raw(quantity.raw().abs()),
                        false,
                        ts,
                        &mut NoStrategy,
                    )?;
                }
                self.delisted.insert(instrument.clone());
                self.metrics.corporate_actions += 1;
            }
        }
        Ok(())
    }

    fn drain_commands(&mut self) -> Result<()> {
        while let Some(command) = self.commands.pop_front() {
            match command {
                Command::Submit(id, request) => self.accept(id, request)?,
                Command::Cancel(id) => {
                    let cancelled = if let Some(order) = self.orders.get_mut(&id)
                        && order.is_open()
                    {
                        order.status = OrderStatus::Cancelled;
                        true
                    } else {
                        false
                    };
                    if cancelled {
                        let at = self.clock.now();
                        self.remove_resting(id);
                        self.execution.send(ExecutionCommand::Cancel(id), at)?;
                    }
                }
                Command::Amend {
                    id,
                    quantity,
                    limit,
                } => self.amend(id, quantity, limit)?,
                Command::Set(request) => self.queue_set_operation(request)?,
            }
        }
        Ok(())
    }

    /// Send a set operation to the venue, which sees it after the same
    /// insertion latency an order faces.
    fn queue_set_operation(&mut self, request: SetOperationRequest) -> Result<()> {
        let now = self.clock.now();
        self.sequence += 1;
        let release_at = now.get().saturating_add(self.latency_model.insert_nanos());
        self.inflight.push(InFlight {
            release_at,
            sequence: self.sequence,
            action: InFlightAction::Set(request),
        });
        Ok(())
    }

    /// Apply an amendment, moving the order to the back of the queue when
    /// the change deserves it.
    fn amend(&mut self, id: OrderId, quantity: Option<Qty>, limit: Option<Price>) -> Result<()> {
        let Some(order) = self.orders.get(&id).cloned() else {
            return Ok(());
        };
        if !order.is_open() {
            return Ok(());
        }
        let mut updated = order.clone();
        let mut loses_priority = false;

        if let Some(new_limit) = limit {
            let instrument = self.instruments.get(&order.instrument)?;
            instrument.check_price(new_limit)?;
            if order.limit_price() != Some(new_limit) {
                updated.kind = OrderKind::Limit { limit: new_limit };
                loses_priority = true;
            }
        }
        if let Some(new_quantity) = quantity {
            if !new_quantity.is_positive() {
                return Err(BacktestError::invalid(
                    "an amended quantity must be positive; cancel instead",
                ));
            }
            if new_quantity < order.filled {
                return Err(BacktestError::invalid(format!(
                    "cannot amend order {id} below the {} already filled",
                    order.filled
                )));
            }
            // Growing the order is a new claim on the queue; shrinking it
            // is not.
            if new_quantity > order.quantity {
                loses_priority = true;
            }
            updated.quantity = new_quantity;
        }

        let was_resting = self.resting.contains(&id);
        if loses_priority && was_resting {
            self.resting_index.remove(&order);
        }
        self.orders.insert(id, updated);
        self.metrics.orders_amended += 1;
        self.execution.send(
            ExecutionCommand::Amend {
                id,
                quantity,
                limit,
            },
            self.clock.now(),
        )?;
        if loses_priority {
            self.queue_ahead.remove(&id);
            if let Some(order) = self.orders.get(&id).cloned() {
                self.join_queue(&order);
                if was_resting {
                    self.resting_index.insert(&order);
                }
            }
        }
        Ok(())
    }

    /// Would this order trade against the account's own resting book?
    ///
    /// Venues prevent wash trades, and a simulator that allows them lets a
    /// strategy cross with itself for free -- printing volume, paying two
    /// sides of a spread it never really crossed, and in a queue model
    /// filling its own passive orders on demand.
    fn would_self_trade(&self, order: &Order) -> Option<OrderId> {
        let limit = order.limit_price();
        self.resting.iter().copied().find(|id| {
            let Some(resting) = self.orders.get(id) else {
                return false;
            };
            if resting.instrument != order.instrument
                || resting.outcome != order.outcome
                || resting.side == order.side
                || !resting.is_open()
            {
                return false;
            }
            let Some(resting_price) = resting.limit_price() else {
                return false;
            };
            match (order.side, limit) {
                // A marketable order crosses whatever is resting.
                (_, None) => true,
                (Side::Buy, Some(cap)) => resting_price <= cap,
                (Side::Sell, Some(floor)) => resting_price >= floor,
            }
        })
    }

    fn accept(&mut self, id: OrderId, request: OrderRequest) -> Result<()> {
        let instrument = self.instruments.get(&request.instrument)?;
        instrument.check_outcome(request.outcome)?;
        if let OrderKind::Limit { limit } = request.kind {
            instrument.check_price(limit)?;
        }
        let now = self.clock.now();
        let mut order = Order::new(
            id,
            request.instrument,
            request.outcome,
            request.side,
            request.kind,
            request.quantity,
            request.time_in_force,
            now,
        )?;
        order.tag = request.tag;
        order.reduce_only = request.reduce_only;

        self.metrics.orders_submitted += 1;
        if let Some(at) = self.has_expired(&order.instrument, now) {
            order.status = OrderStatus::Rejected;
            order.reject_reason = Some(format!("{} stopped trading at {at}", order.instrument));
            self.orders.insert(id, order);
            self.metrics.orders_rejected_expired += 1;
            return Ok(());
        }
        if let Some(reason) = self.naked_short_rejection(&order) {
            order.status = OrderStatus::Rejected;
            order.reject_reason = Some(reason);
            self.orders.insert(id, order);
            self.metrics.orders_rejected_naked_short += 1;
            return Ok(());
        }
        if let Some(reason) = self.risk_rejection(&order) {
            order.status = OrderStatus::Rejected;
            order.reject_reason = Some(reason);
            self.orders.insert(id, order);
            self.metrics.orders_rejected_risk += 1;
            return Ok(());
        }
        self.execution
            .send(ExecutionCommand::Submit(Box::new(order.clone())), now)?;
        self.sequence += 1;
        let release_at = now.get().saturating_add(self.latency_model.insert_nanos());
        self.inflight.push(InFlight {
            release_at,
            sequence: self.sequence,
            action: InFlightAction::Order(Box::new(order.clone())),
        });
        self.orders.insert(id, order);
        Ok(())
    }

    /// Would this sell open short exposure in an outcome nobody can borrow?
    ///
    /// A prediction-market contract has no lending market. Selling one you
    /// do not hold is not a short, it is a position the venue would refuse
    /// to open, and letting a simulator open it produces two errors at once:
    /// a payoff that cannot be realised, and -- because a short's true
    /// exposure is `1 - p` rather than `p` -- a collateral requirement
    /// understated by up to the whole notional.
    ///
    /// The test is on the *worst case* across live orders, matching how
    /// `max_abs_position` is enforced: opposing orders may fill in any
    /// order, so a sell is only safe if it is covered even when every other
    /// live sell fills first. `reduce_only` sells are exempt because
    /// matching clamps them to what would close.
    fn naked_short_rejection(&self, order: &Order) -> Option<String> {
        if self.allow_naked_shorts || order.side != Side::Sell || order.reduce_only {
            return None;
        }
        let instrument = self.instruments.get(&order.instrument).ok()?;
        if !matches!(
            instrument.kind,
            crate::instrument::InstrumentKind::PredictionMarket
        ) {
            return None;
        }
        let held = self
            .portfolio
            .position(&order.instrument, order.outcome)
            .map(|position| position.quantity.raw())
            .unwrap_or(0);
        let pending: i64 = self
            .orders
            .values()
            .filter(|candidate| {
                candidate.is_open()
                    && !candidate.reduce_only
                    && candidate.side == Side::Sell
                    && candidate.instrument == order.instrument
                    && candidate.outcome == order.outcome
            })
            .fold(0, |total, candidate| {
                total.saturating_add(candidate.remaining().raw())
            });
        let projected = held
            .saturating_sub(pending)
            .saturating_sub(order.quantity.raw());
        if projected >= 0 {
            return None;
        }
        let complement = instrument
            .outcome_ids()
            .find(|candidate| *candidate != order.outcome)
            .map(|candidate| {
                instrument
                    .outcome(candidate)
                    .map(|label| format!("{label} (outcome {candidate})"))
                    .unwrap_or_else(|_| candidate.to_string())
            })
            .unwrap_or_else(|| "the other outcome".to_string());
        Some(format!(
            "selling {} of {} outcome {} would leave a short of {}, and a \
             prediction-market contract cannot be borrowed; buy {complement} \
             instead, which has the same payoff at a cost of one minus the \
             price",
            order.quantity,
            order.instrument,
            order.outcome,
            Qty::from_raw(-projected)
        ))
    }

    fn risk_rejection(&self, order: &Order) -> Option<String> {
        if let Some(maximum) = self.risk_limits.max_order_quantity
            && order.quantity > maximum
        {
            return Some(format!(
                "quantity {} exceeds max_order_quantity {}",
                order.quantity, maximum
            ));
        }
        if let Some(maximum) = self.risk_limits.max_open_orders {
            let open = self
                .orders
                .values()
                .filter(|candidate| candidate.is_open())
                .count();
            if open >= maximum {
                return Some(format!(
                    "open order count {open} reached max_open_orders {maximum}"
                ));
            }
        }
        if let Some(maximum) = self.risk_limits.max_abs_position {
            let current = self
                .portfolio
                .position(&order.instrument, order.outcome)
                .map(|position| position.quantity.raw())
                .unwrap_or(0);
            let mut pending_buys = 0_i64;
            let mut pending_sells = 0_i64;
            for candidate in self.orders.values().filter(|candidate| {
                candidate.is_open()
                    && candidate.instrument == order.instrument
                    && candidate.outcome == order.outcome
            }) {
                let target = match candidate.side {
                    Side::Buy => &mut pending_buys,
                    Side::Sell => &mut pending_sells,
                };
                *target = target.saturating_add(candidate.remaining().raw());
            }
            match order.side {
                Side::Buy => pending_buys = pending_buys.saturating_add(order.quantity.raw()),
                Side::Sell => pending_sells = pending_sells.saturating_add(order.quantity.raw()),
            }
            // Opposing live orders are not assumed to offset: either side may
            // fill independently. Check both possible exposure extremes.
            let projected_long = current.saturating_add(pending_buys).unsigned_abs();
            let projected_short = current.saturating_sub(pending_sells).unsigned_abs();
            let projected = projected_long.max(projected_short);
            if projected > maximum.raw() as u64 {
                return Some(format!(
                    "worst-case absolute position {} across live orders exceeds max_abs_position {}",
                    Qty::from_raw(projected.min(i64::MAX as u64) as i64),
                    maximum
                ));
            }
        }
        None
    }

    fn release_inflight(&mut self, ts: UnixNanos, strategy: &mut dyn Strategy) -> Result<()> {
        let mut released = Vec::new();
        while let Some(head) = self.inflight.peek() {
            if head.release_at > ts.get() {
                break;
            }
            released.push(self.inflight.pop().expect("peeked").action);
        }
        for action in released {
            let mut order = match action {
                InFlightAction::Order(order) => *order,
                InFlightAction::Set(request) => {
                    self.run_set_operation(request, ts, strategy)?;
                    continue;
                }
            };
            // The order was sent while the market was open and arrived after
            // it closed. That is a real way to miss a trade, and the honest
            // outcome is a rejection rather than a fill against a book the
            // venue was already tearing down.
            if let Some(at) = self.has_expired(&order.instrument, ts) {
                order.status = OrderStatus::Rejected;
                order.reject_reason = Some(format!(
                    "{} stopped trading at {at}, before this order arrived",
                    order.instrument
                ));
                self.orders.insert(order.id, order);
                self.metrics.orders_rejected_expired += 1;
                continue;
            }
            if let Some(other) = self.would_self_trade(&order) {
                order.status = OrderStatus::Rejected;
                order.reject_reason = Some(format!(
                    "would trade against this account's own resting order {other}"
                ));
                self.orders.insert(order.id, order);
                self.self_trades_prevented += 1;
                self.metrics.orders_rejected_self_trade += 1;
                continue;
            }
            if !self.can_fund(&order)? {
                order.status = OrderStatus::Rejected;
                order.reject_reason = Some("insufficient margin for this order".to_string());
                self.orders.insert(order.id, order);
                self.rejected_for_margin += 1;
                self.metrics.orders_rejected_margin += 1;
                continue;
            }
            order.status = OrderStatus::Accepted;
            order.accepted_at = Some(ts);
            self.orders.insert(order.id, order.clone());
            self.try_fill(order.id, ts, strategy)?;
        }
        Ok(())
    }

    fn match_resting(&mut self, record: &Record, strategy: &mut dyn Strategy) -> Result<()> {
        let key = (record.instrument.clone(), record.outcome);
        let Some(book) = self.books.get(&key) else {
            return Ok(());
        };
        let best_bid = book.best_bid().map(|(price, _)| price);
        let best_ask = book.best_ask().map(|(price, _)| price);
        let is_corporate = matches!(&record.event, MarketEvent::Corporate(_));
        let mut candidates = std::mem::take(&mut self.matching_candidates);
        candidates.clear();

        if is_corporate {
            // A split can reprice every outcome for the instrument. This is
            // rare and correctness matters more than indexing the unusual
            // cross-outcome case.
            candidates.extend(self.resting.iter().filter_map(|id| {
                let order = self.orders.get(id)?;
                (order.instrument == record.instrument && order.is_open()).then_some(*id)
            }));
        } else if self.fill_model.preserves_book_prices() {
            self.resting_index
                .crossing_top(&key, best_bid, best_ask, &mut candidates);
            // The old matcher used submission order. Keep that observable
            // liquidity-consumption order even though the index is priced.
            candidates.sort_unstable();
        } else {
            // A custom model may synthesize prices, so every order in this
            // market must still be offered to it.
            self.resting_index.all_for_market(&key, &mut candidates);
            candidates.sort_unstable();
        }
        if candidates.is_empty() {
            self.matching_candidates = candidates;
            return Ok(());
        }
        let ts = record.stamps.ts_init;
        for &id in &candidates {
            self.try_fill(id, ts, strategy)?;
        }
        candidates.clear();
        self.matching_candidates = candidates;
        self.prune_resting();
        Ok(())
    }

    /// Match one order against the current book.
    fn try_fill(&mut self, id: OrderId, ts: UnixNanos, strategy: &mut dyn Strategy) -> Result<()> {
        let Some(order) = self.orders.get(&id).cloned() else {
            return Ok(());
        };
        if !order.is_open() || order.status == OrderStatus::InFlight {
            return Ok(());
        }
        let key = (order.instrument.clone(), order.outcome);
        let Some(book) = self.books.get(&key) else {
            // No book for this instrument yet. That is not a reason to
            // strand the order: it is simply no liquidity, so the order's
            // own time-in-force decides. Returning early here instead left
            // the order accepted forever, matched by nothing, which is the
            // quietest possible way to lose a trade.
            self.no_liquidity(id);
            return Ok(());
        };
        // The fill model may substitute a synthetic book; matching does not
        // know the difference.
        let synthetic = self.fill_model.book_for_fill(book, &order);
        let effective = synthetic.as_ref().unwrap_or(book);

        let mut wanted = order.remaining();
        if order.reduce_only {
            // Never open or flip: clamp to what would close.
            let held = self
                .portfolio
                .position(&order.instrument, order.outcome)
                .map(|p| p.quantity.raw())
                .unwrap_or(0);
            let closable = match order.side {
                Side::Buy => (-held).max(0),
                Side::Sell => held.max(0),
            };
            wanted = Qty::from_raw(wanted.raw().min(closable));
            if !wanted.is_positive() {
                self.close(id, OrderStatus::Cancelled);
                return Ok(());
            }
        }

        let walk = effective.walk(order.side, wanted, order.limit_price());
        let filled = walk.filled();

        if order.time_in_force == TimeInForce::FillOrKill && filled != wanted {
            self.close(id, OrderStatus::Cancelled);
            return Ok(());
        }

        for (price, quantity) in &walk.fills {
            self.execute(id, *price, *quantity, true, ts, strategy)?;
        }

        let order = self.orders.get(&id).cloned().expect("order exists");
        if order.is_open() {
            match order.time_in_force {
                TimeInForce::ImmediateOrCancel | TimeInForce::FillOrKill => {
                    self.close(id, OrderStatus::Cancelled);
                }
                TimeInForce::GoodTilCancel => {
                    if matches!(order.kind, OrderKind::Limit { .. }) && !self.resting.contains(&id)
                    {
                        self.add_resting(&order);
                    }
                }
            }
        }
        Ok(())
    }

    /// Record how much size is already displayed at an order's price.
    ///
    /// The order joins the back of that queue. An L2 feed cannot say where
    /// in a level an order actually sits, so the pessimistic reading is the
    /// only one it supports; assuming the front is how backtests invent
    /// maker fills that never happened.
    fn join_queue(&mut self, order: &Order) {
        if !self.fill_model.uses_queue_position() || self.queue_ahead.contains_key(&order.id) {
            return;
        }
        let Some(limit) = order.limit_price() else {
            return;
        };
        let key = (order.instrument.clone(), order.outcome);
        let ahead = self
            .books
            .get(&key)
            .and_then(|book| book.quantity_at(order.side, limit))
            .map(Qty::raw)
            .unwrap_or(0);
        self.queue_ahead.insert(order.id, ahead);
        self.metrics.queue_joins += 1;
    }

    fn add_resting(&mut self, order: &Order) {
        if self.resting.contains(&order.id) {
            return;
        }
        self.join_queue(order);
        self.resting_index.insert(order);
        self.resting.push(order.id);
    }

    fn remove_resting(&mut self, id: OrderId) {
        if let Some(order) = self.orders.get(&id).cloned() {
            self.resting_index.remove(&order);
        }
        self.resting.retain(|open| *open != id);
        self.queue_ahead.remove(&id);
    }

    fn prune_resting(&mut self) {
        let closed: Vec<OrderId> = self
            .resting
            .iter()
            .copied()
            .filter(|id| self.orders.get(id).map(|order| order.is_open()) != Some(true))
            .collect();
        for id in closed {
            self.remove_resting(id);
        }
    }

    /// Let a print consume the queue, and fill what it reaches.
    ///
    /// A buy aggressor consumes offers, so it works through resting sells;
    /// a sell aggressor works through resting buys. Volume is applied to
    /// each eligible order in price priority, best first, and within a
    /// price by order id, which is submission order.
    fn consume_queue(
        &mut self,
        record: &Record,
        price: Price,
        size: Qty,
        aggressor: Option<Side>,
        strategy: &mut dyn Strategy,
    ) -> Result<()> {
        if !self.fill_model.uses_queue_position() {
            return Ok(());
        }
        let passive = match aggressor {
            Some(side) => side.opposite(),
            None if self.fill_model.fills_on_unknown_aggressor() => {
                // Without a side the print is ambiguous; the permissive
                // reading lets it work both queues.
                self.consume_queue_side(record, price, size, Side::Buy, strategy)?;
                return self.consume_queue_side(record, price, size, Side::Sell, strategy);
            }
            None => return Ok(()),
        };
        self.consume_queue_side(record, price, size, passive, strategy)
    }

    fn consume_queue_side(
        &mut self,
        record: &Record,
        price: Price,
        size: Qty,
        passive: Side,
        strategy: &mut dyn Strategy,
    ) -> Result<()> {
        // The maintained index is already in best-price/submission order.
        // This makes each print proportional to orders it can actually
        // reach, rather than to every resting order in the engine.
        let mut eligible = std::mem::take(&mut self.queue_candidates);
        eligible.clear();
        let key = (record.instrument.clone(), record.outcome);
        self.resting_index
            .reached_by_trade(&key, passive, price, &mut eligible);

        let mut remaining = size.raw();
        for &(limit, id) in &eligible {
            if remaining <= 0 {
                break;
            }
            let ahead = self.queue_ahead.entry(id).or_insert(0);
            if *ahead > 0 {
                let consumed = remaining.min(*ahead);
                *ahead -= consumed;
                remaining -= consumed;
                if remaining <= 0 {
                    break;
                }
            }
            let wanted = self
                .orders
                .get(&id)
                .map(|order| order.remaining().raw())
                .unwrap_or(0);
            let fill = remaining.min(wanted);
            if fill > 0 {
                // A passive fill happens at the resting price, not the
                // print's: the maker's own limit is what they agreed to.
                self.execute(id, limit, Qty::from_raw(fill), false, record.ts(), strategy)?;
                remaining -= fill;
            }
            if self.orders.get(&id).map(|o| o.is_open()) != Some(true) {
                self.queue_ahead.remove(&id);
            }
        }
        eligible.clear();
        self.queue_candidates = eligible;
        self.prune_resting();
        Ok(())
    }

    /// Apply the time-in-force rule when nothing could be matched.
    fn no_liquidity(&mut self, id: OrderId) {
        let Some(order) = self.orders.get(&id).cloned() else {
            return;
        };
        match order.time_in_force {
            TimeInForce::ImmediateOrCancel | TimeInForce::FillOrKill => {
                self.close(id, OrderStatus::Cancelled);
            }
            TimeInForce::GoodTilCancel => {
                if matches!(order.kind, OrderKind::Limit { .. }) && !self.resting.contains(&id) {
                    self.add_resting(&order);
                }
            }
        }
    }

    fn close(&mut self, id: OrderId, status: OrderStatus) {
        if let Some(order) = self.orders.get_mut(&id)
            && order.is_open()
        {
            order.status = status;
        }
        self.remove_resting(id);
        if status.is_terminal() {
            self.fee_model.order_closed(id);
        }
    }

    /// Perform a complete-set operation against the venue's set contract.
    ///
    /// The whole operation is expressed as one fill per outcome, priced so
    /// that the legs sum to exactly one unit of cash per set. That is not a
    /// convenience: [`crate::position::Portfolio::replay`] rebuilds every
    /// position from `bt_fills` alone, and a run whose positions moved
    /// without a fill to explain them is one where the stored result and
    /// the audit disagree. Minting through the same path keeps them equal.
    ///
    /// The division of the dollar across outcomes follows the market's own
    /// relative prices, normalised to sum to one, falling back to an even
    /// split where the books are not all quoting. Any division would settle
    /// to the same total; this one keeps per-outcome profit attribution
    /// close to what the market thought each leg was worth.
    fn run_set_operation(
        &mut self,
        request: SetOperationRequest,
        ts: UnixNanos,
        strategy: &mut dyn Strategy,
    ) -> Result<()> {
        let instrument = self.instruments.get(&request.instrument)?.clone();
        if !instrument.supports_complete_set() {
            return Err(BacktestError::invalid(format!(
                "{} does not trade as a complete set, so it cannot be minted \
                 or redeemed; a group of independent conditions is several \
                 instruments, not one with many outcomes",
                instrument.id
            )));
        }
        if !request.quantity.is_positive() {
            return Err(BacktestError::invalid(format!(
                "a {} needs a positive quantity",
                request.kind.as_str()
            )));
        }
        let cost = self.set_costs.for_kind(&request.kind);
        let outcomes = instrument.outcome_count();

        // A conversion is a redemption in disguise. Holding the NO side of
        // `k` outcomes is, in this model's long-only outcome space, `k - 1`
        // complete sets plus one contract on each outcome left out: NO(i) is
        // "everything except i", so outcome j appears in the basket once for
        // every i != j, which is `k - 1` times when j is named and `k` times
        // when it is not. The venue's conversion hands back `k - 1` in cash
        // and keeps the residual contracts, which is exactly what redeeming
        // `k - 1` sets does. Naming it separately is worth it because the
        // strategy reasons in NO contracts and the derivation is not
        // obvious; collapsing it to a redemption is what keeps the two from
        // drifting apart.
        let (sets, side) = match &request.kind {
            SetOperationKind::Mint => (request.quantity, Side::Buy),
            SetOperationKind::Redeem => (request.quantity, Side::Sell),
            SetOperationKind::Convert { held } => {
                if !instrument.neg_risk {
                    return Err(BacktestError::invalid(format!(
                        "{} is not a negative-risk market, so its outcomes \
                         cannot be converted",
                        instrument.id
                    )));
                }
                let mut sorted = held.clone();
                sorted.sort();
                sorted.dedup();
                if sorted.len() != held.len() {
                    return Err(BacktestError::invalid(
                        "a conversion must name each outcome at most once",
                    ));
                }
                for outcome in &sorted {
                    instrument.check_outcome(*outcome)?;
                }
                if sorted.len() < 2 || sorted.len() >= outcomes as usize {
                    return Err(BacktestError::invalid(format!(
                        "a conversion on {} must name between 2 and {} \
                         outcomes: fewer converts nothing, and naming every \
                         outcome leaves no residual to be paid in",
                        instrument.id,
                        outcomes.saturating_sub(1)
                    )));
                }
                let multiplier = (sorted.len() - 1) as i64;
                let sets = request
                    .quantity
                    .raw()
                    .checked_mul(multiplier)
                    .map(Qty::from_raw)
                    .ok_or(BacktestError::Overflow {
                        what: "conversion set count",
                    })?;
                (sets, Side::Sell)
            }
        };

        let mut record = SetOperation {
            ts,
            instrument: instrument.id.clone(),
            kind: request.kind.clone(),
            sets,
            cash_delta: Money::ZERO,
            cost,
            rejected: None,
        };

        // Runtime refusals, as opposed to the structural errors above: a
        // strategy can legitimately race into these, so they are recorded
        // and the run continues.
        if let Some(reason) = self.set_operation_refusal(&instrument, side, sets, cost)? {
            record.sets = Qty::ZERO;
            record.cash_delta = Money::ZERO;
            record.cost = Money::ZERO;
            record.rejected = Some(reason);
            self.set_operations.push(record);
            self.metrics.set_operations_rejected += 1;
            return Ok(());
        }

        let basis = self.set_basis(&instrument)?;
        let mut cash_delta = Money::ZERO;
        for (index, price) in basis.iter().enumerate() {
            let outcome = OutcomeId(index as u16);
            self.next_order_id += 1;
            let id = OrderId(self.next_order_id);
            let mut order = Order::new(
                id,
                instrument.id.clone(),
                outcome,
                side,
                OrderKind::Market,
                sets,
                TimeInForce::ImmediateOrCancel,
                ts,
            )?;
            order.status = OrderStatus::Accepted;
            order.accepted_at = Some(ts);
            order.tag = Some(
                request
                    .tag
                    .clone()
                    .unwrap_or_else(|| request.kind.as_str().to_string()),
            );
            self.orders.insert(id, order);
            // The flat cost rides on the first leg. Spreading it across
            // legs would divide a transaction fee by a number that has
            // nothing to do with it.
            let commission = if index == 0 { cost } else { Money::ZERO };
            let leg = notional(*price, sets)?;
            cash_delta = match side {
                Side::Buy => cash_delta.checked_add(leg)?,
                Side::Sell => cash_delta.checked_sub(leg)?,
            };
            self.book_fill(
                id,
                FillTerms {
                    price: *price,
                    quantity: sets,
                    origin: FillOrigin::CompleteSet,
                    commission,
                },
                ts,
                strategy,
            )?;
        }

        record.cash_delta = cash_delta;
        self.set_operations.push(record);
        self.metrics.set_operations += 1;
        Ok(())
    }

    /// Why the venue would refuse this set operation, if it would.
    fn set_operation_refusal(
        &self,
        instrument: &crate::instrument::Instrument,
        side: Side,
        sets: Qty,
        cost: Money,
    ) -> Result<Option<String>> {
        match side {
            Side::Buy => {
                // A set costs exactly one unit of cash, whatever the book
                // says the legs are worth.
                let needed = notional(Price::PROBABILITY_MAX, sets)?.checked_add(cost)?;
                if self.cash < needed {
                    return Ok(Some(format!(
                        "minting {sets} sets of {} costs {needed} but the \
                         account holds {}",
                        instrument.id, self.cash
                    )));
                }
                Ok(None)
            }
            Side::Sell => {
                // Redeeming needs the whole set in hand. Without this check
                // a redemption would manufacture short positions in the
                // outcomes it was missing and pay cash for them.
                for outcome in instrument.outcome_ids() {
                    let held = self
                        .portfolio
                        .position(&instrument.id, outcome)
                        .map(|position| position.quantity)
                        .unwrap_or(Qty::ZERO);
                    if held < sets {
                        return Ok(Some(format!(
                            "redeeming {sets} sets of {} needs {sets} of \
                             outcome {outcome} but only {held} are held",
                            instrument.id
                        )));
                    }
                }
                Ok(None)
            }
        }
    }

    /// How a set's single unit of cash is divided across its outcomes.
    fn set_basis(&self, instrument: &crate::instrument::Instrument) -> Result<Vec<Price>> {
        let quoted: Option<Vec<Price>> = instrument
            .outcome_ids()
            .map(|outcome| {
                self.marks
                    .get(&(instrument.id.clone(), outcome))
                    .copied()
                    .filter(|price| price.is_positive())
            })
            .collect();
        match quoted {
            Some(prices) => crate::instrument::normalise_to_one(&prices),
            None => crate::instrument::uniform_prices(instrument.outcome_count()),
        }
    }

    fn execute(
        &mut self,
        id: OrderId,
        price: Price,
        quantity: Qty,
        is_taker: bool,
        ts: UnixNanos,
        strategy: &mut dyn Strategy,
    ) -> Result<()> {
        let order = self.orders.get(&id).cloned().expect("order exists");
        let instrument = self.instruments.get(&order.instrument)?.clone();
        let commission = self.fee_model.commission(FeeContext {
            order_id: id,
            instrument: &instrument,
            side: order.side,
            price,
            quantity,
            is_taker,
        })?;
        self.book_fill(
            id,
            FillTerms {
                price,
                quantity,
                origin: FillOrigin::Trade { is_taker },
                commission,
            },
            ts,
            strategy,
        )
    }

    fn book_fill(
        &mut self,
        id: OrderId,
        terms: FillTerms,
        ts: UnixNanos,
        strategy: &mut dyn Strategy,
    ) -> Result<()> {
        let FillTerms {
            price,
            quantity,
            origin,
            commission,
        } = terms;
        let order = self.orders.get(&id).cloned().expect("order exists");
        let instrument = self.instruments.get(&order.instrument)?.clone();
        let is_taker = matches!(origin, FillOrigin::Trade { is_taker: true });

        let fill = Fill {
            order_id: id,
            instrument: order.instrument.clone(),
            outcome: order.outcome,
            side: order.side,
            price,
            quantity,
            commission,
            is_taker,
            ts,
            tag: order.tag.clone(),
        };
        // How cash moves depends on what kind of thing this is. A funded
        // instrument is bought: the notional leaves (or arrives) and the
        // asset is held. A derivative is collateralised: only realised
        // profit and fees touch cash, because nothing was purchased.
        let realized_before = self
            .portfolio
            .position(&order.instrument, order.outcome)
            .map(|position| position.realized_pnl)
            .unwrap_or(Money::ZERO);
        self.portfolio.apply(&fill)?;
        if instrument.kind.is_funded() {
            let gross = notional(price, quantity)?;
            let delta = match order.side {
                Side::Buy => Money::from_raw(-gross.raw()),
                Side::Sell => gross,
            };
            self.cash = self.cash.checked_add(delta)?.checked_sub(commission)?;
        } else {
            let realized_after = self
                .portfolio
                .position(&order.instrument, order.outcome)
                .map(|position| position.realized_pnl)
                .unwrap_or(Money::ZERO);
            // The position already netted the commission out of realised
            // profit, so this one delta carries both.
            self.cash = self
                .cash
                .checked_add(realized_after.checked_sub(realized_before)?)?;
        }
        if let Some(order) = self.orders.get_mut(&id) {
            order.record_fill(quantity)?;
        }
        if self.orders.get(&id).map(|order| order.is_open()) != Some(true) {
            self.remove_resting(id);
            self.fee_model.order_closed(id);
        }
        // A set leg took no liquidity and provided none, so counting it as
        // either would misstate the ratio a maker strategy is judged on.
        match origin {
            FillOrigin::Trade { is_taker: true } => self.metrics.fills_taker += 1,
            FillOrigin::Trade { is_taker: false } => self.metrics.fills_maker += 1,
            FillOrigin::CompleteSet => {}
        }
        self.fills.push(fill.clone());
        self.with_context(|ctx| strategy.on_fill(ctx, &fill))?;
        self.drain_commands()?;
        Ok(())
    }

    fn finish(&mut self) -> Result<RunResult> {
        for order in self.orders.values() {
            match order.status {
                OrderStatus::Filled => self.metrics.orders_filled += 1,
                OrderStatus::PartiallyFilled => self.metrics.orders_partially_filled += 1,
                OrderStatus::Cancelled if order.filled.is_zero() => {
                    self.metrics.orders_cancelled_unfilled += 1
                }
                OrderStatus::Cancelled => self.metrics.orders_partially_filled += 1,
                _ => {}
            }
        }
        // Always close the curve on the last instant actually simulated, so
        // the final equity is a point rather than something a reader has to
        // infer from the summary.
        if let Some(ts) = self.simulated_through
            && self.equity.last().map(|p| p.ts) != Some(ts)
        {
            self.push_equity(ts)?;
        }
        Ok(RunResult {
            fills: self.fills.clone(),
            orders: self.orders.values().cloned().collect(),
            final_cash: self.cash,
            starting_cash: self.starting_cash,
            realized_pnl: self.portfolio.realized_pnl()?,
            commissions: self.portfolio.commissions()?,
            simulated_through: self.simulated_through,
            records_processed: self.records,
            marks: self.marks.clone(),
            equity: self.equity.clone(),
            funding_paid: self.funding_paid,
            dividends_received: self.dividends_received,
            liquidations: self.liquidations.clone(),
            rejected_for_margin: self.rejected_for_margin,
            self_trades_prevented: self.self_trades_prevented,
            set_operations: self.set_operations.clone(),
            forecasts: self.forecasts.clone(),
            mark_curve: self.mark_curve.clone(),
            expirations: self.expirations.clone(),
            metrics: self.metrics.clone(),
        })
    }

    pub fn portfolio(&self) -> &Portfolio {
        &self.portfolio
    }

    /// The execution client, for reading back what was sent to the venue.
    pub fn execution(&self) -> &dyn ExecutionClient {
        self.execution.as_ref()
    }

    pub fn cash(&self) -> Money {
        self.cash
    }
}

pub struct EngineBuilder {
    instruments: InstrumentSet,
    starting_cash: Money,
    start: UnixNanos,
    fee_model: Box<dyn FeeModel>,
    fill_model: Box<dyn FillModel>,
    latency_model: Box<dyn LatencyModel>,
    equity_interval: i64,
    margin: Option<Box<dyn MarginModel>>,
    fx: FxBook,
    execution: Box<dyn ExecutionClient>,
    reporting_currency: crate::currency::Currency,
    risk_limits: RiskLimits,
    allow_naked_shorts: bool,
    set_costs: SetOperationCosts,
    record_mark_curve: bool,
    mark_source: MarkSource,
}

impl EngineBuilder {
    /// Which price positions are valued, margined and liquidated at.
    pub fn mark_source(mut self, source: MarkSource) -> Self {
        self.mark_source = source;
        self
    }

    /// Let a sell open short exposure in a prediction-market outcome.
    ///
    /// Off by default, and it should stay off for any venue that resembles
    /// a real one. A prediction-market contract cannot be borrowed: there is
    /// no stock loan for a share of "YES", so a venue that lets you sell one
    /// you do not hold is not modelling a venue. The bearish trade is to buy
    /// the complementary outcome, which costs `1 - p` and has the same
    /// payoff -- and a strategy that has to express it that way is one whose
    /// capital requirement is the real one.
    ///
    /// Turning this on is for isolating the effect of the constraint, not
    /// for producing a result anyone should act on.
    pub fn allow_naked_shorts(mut self, allow: bool) -> Self {
        self.allow_naked_shorts = allow;
        self
    }

    /// What a mint, redeem or conversion costs to perform.
    pub fn set_operation_costs(mut self, costs: SetOperationCosts) -> Self {
        self.set_costs = costs;
        self
    }

    /// Whether to sample every probability book onto the equity curve's
    /// clock, which is what a calibration or reliability report reads.
    ///
    /// On by default: the series is one point per book per equity sample,
    /// and a prediction-market run has few books. Turn it off for a run
    /// spanning thousands of markets, where the curve stops being small.
    pub fn record_mark_curve(mut self, record: bool) -> Self {
        self.record_mark_curve = record;
        self
    }
    pub fn starting_cash(mut self, cash: Money) -> Self {
        self.starting_cash = cash;
        self
    }

    pub fn start(mut self, at: UnixNanos) -> Self {
        self.start = at;
        self
    }

    pub fn fee_model(mut self, model: Box<dyn FeeModel>) -> Self {
        self.fee_model = model;
        self
    }

    pub fn fill_model(mut self, model: Box<dyn FillModel>) -> Self {
        self.fill_model = model;
        self
    }

    pub fn latency_model(mut self, model: Box<dyn LatencyModel>) -> Self {
        self.latency_model = model;
        self
    }

    /// Enforce a margin model: orders that would exceed available
    /// collateral are rejected, and a position that falls below maintenance
    /// is liquidated.
    ///
    /// Without one, leverage is infinite, and a strategy that would have
    /// been closed out by the venue instead reports a profit.
    pub fn margin_model(mut self, model: Box<dyn MarginModel>) -> Self {
        self.margin = Some(model);
        self
    }

    /// Where orders are sent.
    ///
    /// The simulator is the default. A live adapter implementing the same
    /// trait is what makes sim-versus-live reconcilable at all: the
    /// instruction stream is directly comparable, so a divergence shows up
    /// as a difference in what the strategy decided rather than as a
    /// difference in outcome nobody can attribute.
    pub fn execution_client(mut self, client: Box<dyn ExecutionClient>) -> Self {
        self.execution = client;
        self
    }

    pub fn risk_limits(mut self, limits: RiskLimits) -> Self {
        self.risk_limits = limits;
        self
    }

    /// Seed exchange rates for multi-currency valuation.
    pub fn fx(mut self, fx: FxBook) -> Self {
        self.fx = fx;
        self
    }

    /// The currency cash is held and equity reported in.
    pub fn reporting_currency(mut self, currency: crate::currency::Currency) -> Self {
        self.reporting_currency = currency;
        self
    }

    /// How often to sample the equity curve, in nanoseconds of simulated
    /// time. Sampling on an interval rather than per record keeps the curve
    /// a usable size on tick data, where a per-record curve would have as
    /// many points as there were quotes.
    pub fn equity_interval_nanos(mut self, nanos: i64) -> Result<Self> {
        if nanos <= 0 {
            return Err(BacktestError::invalid(
                "the equity sampling interval must be positive",
            ));
        }
        self.equity_interval = nanos;
        Ok(self)
    }

    pub fn build(self) -> Engine {
        // Expiries are resolved once, ascending, so the run only has to
        // compare the clock against the next one rather than sweep every
        // instrument on every record.
        let mut expiries: Vec<(i64, InstrumentId)> = self
            .instruments
            .ids()
            .into_iter()
            .filter_map(|id| {
                let instrument = self.instruments.get(&id).ok()?;
                instrument
                    .expiration
                    .map(|at| (at.get(), instrument.id.clone()))
            })
            .collect();
        expiries.sort();
        Engine {
            clock: Clock::new(self.start),
            instruments: self.instruments,
            books: BTreeMap::new(),
            portfolio: Portfolio::new(),
            cash: self.starting_cash,
            starting_cash: self.starting_cash,
            orders: BTreeMap::new(),
            resting: Vec::new(),
            resting_index: RestingIndex::default(),
            matching_candidates: Vec::new(),
            inflight: BinaryHeap::new(),
            commands: VecDeque::new(),
            clock_requests: Vec::new(),
            fills: Vec::new(),
            marks: BTreeMap::new(),
            fee_model: self.fee_model,
            fill_model: self.fill_model,
            latency_model: self.latency_model,
            next_order_id: 0,
            sequence: 0,
            records: 0,
            simulated_through: None,
            equity_interval: self.equity_interval,
            next_equity_sample: None,
            equity: Vec::new(),
            funding_paid: Money::ZERO,
            queue_ahead: BTreeMap::new(),
            queue_candidates: Vec::new(),
            margin: self.margin,
            fx: self.fx,
            liquidations: Vec::new(),
            rejected_for_margin: 0,
            self_trades_prevented: 0,
            reporting_currency: self.reporting_currency,
            dividends_received: Money::ZERO,
            delisted: Default::default(),
            metrics: RunMetrics::default(),
            execution: self.execution,
            risk_limits: self.risk_limits,
            allow_naked_shorts: self.allow_naked_shorts,
            expiries,
            next_expiry: 0,
            expired: Default::default(),
            expirations: Vec::new(),
            set_costs: self.set_costs,
            set_operations: Vec::new(),
            forecasts: Vec::new(),
            record_mark_curve: self.record_mark_curve,
            mark_curve: Vec::new(),
            book_marks: BTreeMap::new(),
            venue_marks: BTreeMap::new(),
            oracles: BTreeMap::new(),
            mark_source: self.mark_source,
        }
    }
}

/// Tier 1: the strategy is data.
///
/// A list of timestamped intents, replayed through the full matching, fee
/// and latency path. This covers most systematic research without any
/// callback code, is the shape an agent generates naturally (a query, not a
/// state machine), and has no language boundary in the hot loop.
pub struct SignalReplay {
    intents: VecDeque<(UnixNanos, OrderRequest)>,
}

impl SignalReplay {
    /// Intents must be in timestamp order; that is checked, not assumed.
    pub fn new(mut intents: Vec<(UnixNanos, OrderRequest)>) -> Result<Self> {
        let mut previous: Option<UnixNanos> = None;
        for (ts, _) in &intents {
            if let Some(prev) = previous
                && *ts < prev
            {
                return Err(BacktestError::OutOfOrder {
                    stream: "signals".to_string(),
                    ts: ts.get(),
                    previous: prev.get(),
                });
            }
            previous = Some(*ts);
        }
        intents.shrink_to_fit();
        Ok(Self {
            intents: intents.into(),
        })
    }
}

impl Strategy for SignalReplay {
    fn on_event(&mut self, ctx: &mut Context<'_>, _record: &Record) -> Result<()> {
        let now = ctx.now();
        while let Some((ts, _)) = self.intents.front() {
            if *ts > now {
                break;
            }
            let (_, request) = self.intents.pop_front().expect("peeked");
            ctx.submit(request);
        }
        Ok(())
    }
}

/// A lifecycle command addressed by a stable, strategy-defined order ID.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ReplayCommand {
    Submit {
        client_order_id: String,
        request: OrderRequest,
    },
    Cancel {
        client_order_id: String,
    },
    Amend {
        client_order_id: String,
        quantity: Option<Qty>,
        limit: Option<Price>,
    },
}

/// Declarative submit/amend/cancel replay with no language callback in the
/// hot loop.
pub struct CommandReplay {
    commands: VecDeque<(UnixNanos, ReplayCommand)>,
    order_ids: BTreeMap<String, OrderId>,
}

impl CommandReplay {
    pub fn new(mut commands: Vec<(UnixNanos, ReplayCommand)>) -> Result<Self> {
        let mut previous: Option<UnixNanos> = None;
        for (ts, command) in &commands {
            if let Some(prev) = previous
                && *ts < prev
            {
                return Err(BacktestError::OutOfOrder {
                    stream: "commands".to_string(),
                    ts: ts.get(),
                    previous: prev.get(),
                });
            }
            if let ReplayCommand::Amend {
                quantity: None,
                limit: None,
                ..
            } = command
            {
                return Err(BacktestError::invalid(
                    "amend command must change quantity or limit_price",
                ));
            }
            previous = Some(*ts);
        }
        commands.shrink_to_fit();
        Ok(Self {
            commands: commands.into(),
            order_ids: BTreeMap::new(),
        })
    }

    fn order_id(&self, client_order_id: &str, action: &str) -> Result<OrderId> {
        self.order_ids.get(client_order_id).copied().ok_or_else(|| {
            BacktestError::invalid(format!(
                "{action} references unknown client_order_id {client_order_id:?}"
            ))
        })
    }
}

impl Strategy for CommandReplay {
    fn on_event(&mut self, ctx: &mut Context<'_>, _record: &Record) -> Result<()> {
        let now = ctx.now();
        while self
            .commands
            .front()
            .is_some_and(|(timestamp, _)| *timestamp <= now)
        {
            let (_, command) = self.commands.pop_front().expect("peeked");
            match command {
                ReplayCommand::Submit {
                    client_order_id,
                    request,
                } => {
                    if self.order_ids.contains_key(&client_order_id) {
                        return Err(BacktestError::invalid(format!(
                            "duplicate client_order_id {client_order_id:?}"
                        )));
                    }
                    let order_id = ctx.submit_tracked(request);
                    self.order_ids.insert(client_order_id, order_id);
                }
                ReplayCommand::Cancel { client_order_id } => {
                    ctx.cancel(self.order_id(&client_order_id, "cancel")?);
                }
                ReplayCommand::Amend {
                    client_order_id,
                    quantity,
                    limit,
                } => {
                    ctx.amend(self.order_id(&client_order_id, "amend")?, quantity, limit);
                }
            }
        }
        Ok(())
    }
}
