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

use crate::account::{
    margin_requirement, margin_state, Liquidation, MarginModel, MarginState,
};
use crate::book::OrderBook;
use crate::clock::{Clock, TimeEvent};
use crate::currency::FxBook;
use crate::execution::{ExecutionClient, ExecutionCommand, SimulatedExecution};
use crate::error::{BacktestError, Result};
use crate::event::{MarketEvent, Record};
use crate::instrument::{InstrumentId, InstrumentSet, OutcomeId};
use crate::models::{
    BookFills, FeeContext, FeeModel, FillModel, LatencyModel, NoFees, NoLatency,
};
use crate::order::{Fill, Order, OrderId, OrderKind, OrderStatus, TimeInForce};
use crate::position::Portfolio;
use crate::types::{notional, Money, Price, Qty, Side, UnixNanos};

/// A key identifying one tradable book.
type BookKey = (InstrumentId, OutcomeId);

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
    pub fn market(
        instrument: InstrumentId,
        outcome: OutcomeId,
        side: Side,
        quantity: Qty,
    ) -> Self {
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

/// A command a strategy queued. Never executed inside the callback that
/// produced it.
#[derive(Clone, PartialEq, Eq, Debug)]
enum Command {
    Submit(OrderRequest),
    Cancel(OrderId),
    Amend {
        id: OrderId,
        quantity: Option<Qty>,
        limit: Option<Price>,
    },
}

/// What a strategy may do and see.
///
/// Note what is absent: any way to reach a [`crate::settlement::Resolution`],
/// or any data beyond the clock's current instant. The strategy cannot look
/// ahead because there is nothing here through which to do it.
pub struct Context<'a> {
    now: UnixNanos,
    books: &'a BTreeMap<BookKey, OrderBook>,
    portfolio: &'a Portfolio,
    cash: Money,
    commands: &'a mut VecDeque<Command>,
    clock_requests: &'a mut Vec<(String, UnixNanos)>,
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
        self.commands.push_back(Command::Submit(request));
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

/// An order waiting out its latency before the venue sees it.
#[derive(PartialEq, Eq)]
struct InFlight {
    release_at: i64,
    sequence: u64,
    order: Order,
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
    pub orders_rejected_self_trade: u64,
    pub orders_amended: u64,
    pub fills_taker: u64,
    pub fills_maker: u64,
    /// Feed gaps that invalidated a book.
    pub book_gaps: u64,
    /// Times a resting order sat behind queue volume it never cleared.
    pub queue_joins: u64,
    pub liquidations: u64,
    pub corporate_actions: u64,
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
                reasons.push(format!("{} refused for margin", self.orders_rejected_margin));
            }
            if self.orders_rejected_self_trade > 0 {
                reasons.push(format!(
                    "{} would have crossed the account's own book",
                    self.orders_rejected_self_trade
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
            return Some(format!("{} orders, no fills: {detail}", self.orders_submitted));
        }
        None
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
            let settlement = &self.instruments.get(&position.instrument)?.settlement_currency;
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
        Ok(())
    }

    fn with_context<T>(&mut self, f: impl FnOnce(&mut Context<'_>) -> Result<T>) -> Result<T> {
        let mut ctx = Context {
            now: self.clock.now(),
            books: &self.books,
            portfolio: &self.portfolio,
            cash: self.cash,
            commands: &mut self.commands,
            clock_requests: &mut self.clock_requests,
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
                self.marks.insert(key.clone(), *price);
            }
            MarketEvent::Bar { close, .. } => {
                self.marks.insert(key.clone(), *close);
            }
            MarketEvent::Funding { rate } => {
                self.apply_funding(&record.instrument, *rate)?;
            }
            MarketEvent::Corporate(action) => {
                self.apply_corporate_action(&record.instrument, *action, ts)?;
            }
        }
        if let Some(mid) = self.books.get(&key).and_then(|b| b.mid()) {
            self.marks.insert(key, mid);
        }
        Ok(())
    }

    /// Settle a funding payment against every open position in an
    /// instrument.
    ///
    /// `payment = position * mark * rate`, charged to the holder, so a
    /// positive rate takes cash from longs and pays it to shorts. A position
    /// with no mark cannot be valued and is skipped rather than funded at a
    /// guessed price -- silently funding at the entry price would make a
    /// stale position drift for free.
    fn apply_funding(&mut self, instrument: &InstrumentId, rate: Price) -> Result<()> {
        let mut total = Money::ZERO;
        for position in self.portfolio.open_positions() {
            if &position.instrument != instrument {
                continue;
            }
            let key = (position.instrument.clone(), position.outcome);
            let Some(mark) = self.marks.get(&key) else {
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
                    if let Some(order) = self.orders.get_mut(&id) {
                        order.quantity = quantity;
                        order.kind = OrderKind::Limit { limit: price };
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
                        (position.instrument.clone(), position.outcome, position.quantity)
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
                    self.execute(order_id, final_price, Qty::from_raw(quantity.raw().abs()), false, ts, &mut NoStrategy)?;
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
                Command::Submit(request) => self.accept(request)?,
                Command::Cancel(id) => {
                    if let Some(order) = self.orders.get_mut(&id)
                        && order.is_open()
                    {
                        order.status = OrderStatus::Cancelled;
                        let at = self.clock.now();
                        self.resting.retain(|open| *open != id);
                        self.queue_ahead.remove(&id);
                        self.execution.send(ExecutionCommand::Cancel(id), at)?;
                    }
                }
                Command::Amend {
                    id,
                    quantity,
                    limit,
                } => self.amend(id, quantity, limit)?,
            }
        }
        Ok(())
    }

    /// Apply an amendment, moving the order to the back of the queue when
    /// the change deserves it.
    fn amend(
        &mut self,
        id: OrderId,
        quantity: Option<Qty>,
        limit: Option<Price>,
    ) -> Result<()> {
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

    fn accept(&mut self, request: OrderRequest) -> Result<()> {
        let instrument = self.instruments.get(&request.instrument)?;
        instrument.check_outcome(request.outcome)?;
        if let OrderKind::Limit { limit } = request.kind {
            instrument.check_price(limit)?;
        }
        self.next_order_id += 1;
        let id = OrderId(self.next_order_id);
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
        self.execution
            .send(ExecutionCommand::Submit(Box::new(order.clone())), now)?;
        self.sequence += 1;
        let release_at = now.get().saturating_add(self.latency_model.insert_nanos());
        self.inflight.push(InFlight {
            release_at,
            sequence: self.sequence,
            order: order.clone(),
        });
        self.orders.insert(id, order);
        Ok(())
    }

    fn release_inflight(&mut self, ts: UnixNanos, strategy: &mut dyn Strategy) -> Result<()> {
        let mut released = Vec::new();
        while let Some(head) = self.inflight.peek() {
            if head.release_at > ts.get() {
                break;
            }
            released.push(self.inflight.pop().expect("peeked").order);
        }
        for mut order in released {
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
                order.reject_reason =
                    Some("insufficient margin for this order".to_string());
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
        let can_prefilter = self.fill_model.preserves_book_prices() && !is_corporate;

        // Most resting orders are deliberately away from the market. Do
        // not clone every open id and walk the whole book for each of them
        // on every feed record: only an order for this market whose limit
        // crosses the new top of book can fill here.
        let candidates: Vec<OrderId> = self
            .resting
            .iter()
            .filter_map(|id| {
                let order = self.orders.get(id)?;
                let affected_market = order.instrument == record.instrument
                    && (order.outcome == record.outcome || is_corporate);
                if !affected_market || !order.is_open() {
                    return None;
                }
                if !can_prefilter {
                    return Some(*id);
                }
                let limit = order.limit_price()?;
                let crosses = match order.side {
                    Side::Buy => best_ask.is_some_and(|ask| limit >= ask),
                    Side::Sell => best_bid.is_some_and(|bid| limit <= bid),
                };
                crosses.then_some(*id)
            })
            .collect();
        if candidates.is_empty() {
            return Ok(());
        }
        let ts = record.stamps.ts_init;
        for id in candidates {
            self.try_fill(id, ts, strategy)?;
        }
        self.resting.retain(|id| {
            self.orders
                .get(id)
                .map(|order| order.is_open())
                .unwrap_or(false)
        });
        Ok(())
    }

    /// Match one order against the current book.
    fn try_fill(
        &mut self,
        id: OrderId,
        ts: UnixNanos,
        strategy: &mut dyn Strategy,
    ) -> Result<()> {
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
                    if matches!(order.kind, OrderKind::Limit { .. })
                        && !self.resting.contains(&id)
                    {
                        self.join_queue(&order);
                        self.resting.push(id);
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
            .map(|book| {
                book.levels(order.side)
                    .into_iter()
                    .find(|(price, _)| *price == limit)
                    .map(|(_, size)| size.raw())
                    .unwrap_or(0)
            })
            .unwrap_or(0);
        self.queue_ahead.insert(order.id, ahead);
        self.metrics.queue_joins += 1;
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
        // Eligible orders, best price first, then submission order.
        let mut eligible: Vec<(Price, OrderId)> = self
            .resting
            .iter()
            .filter_map(|id| {
                let order = self.orders.get(id)?;
                if order.instrument != record.instrument
                    || order.outcome != record.outcome
                    || order.side != passive
                    || !order.is_open()
                {
                    return None;
                }
                let limit = order.limit_price()?;
                // A resting buy is reachable by a print at or below it; a
                // resting sell by a print at or above it.
                let reachable = match passive {
                    Side::Buy => price <= limit,
                    Side::Sell => price >= limit,
                };
                reachable.then_some((limit, *id))
            })
            .collect();
        eligible.sort_by(|a, b| match passive {
            Side::Buy => b.0.cmp(&a.0).then(a.1.cmp(&b.1)),
            Side::Sell => a.0.cmp(&b.0).then(a.1.cmp(&b.1)),
        });

        let mut remaining = size.raw();
        for (limit, id) in eligible {
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
        self.resting.retain(|id| {
            self.orders
                .get(id)
                .map(|order| order.is_open())
                .unwrap_or(false)
        });
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
                    self.resting.push(id);
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
        self.resting.retain(|open| *open != id);
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
            instrument: &instrument,
            side: order.side,
            price,
            quantity,
            is_taker,
        })?;

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
        if is_taker {
            self.metrics.fills_taker += 1;
        } else {
            self.metrics.fills_maker += 1;
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
}

impl EngineBuilder {
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
        Engine {
            clock: Clock::new(self.start),
            instruments: self.instruments,
            books: BTreeMap::new(),
            portfolio: Portfolio::new(),
            cash: self.starting_cash,
            starting_cash: self.starting_cash,
            orders: BTreeMap::new(),
            resting: Vec::new(),
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
