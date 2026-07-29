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

use crate::book::OrderBook;
use crate::clock::{Clock, TimeEvent};
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

        while let Some(record) = replay.next_record() {
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
        self.match_resting(ts, strategy)?;

        // 5. Now the strategy.
        self.with_context(|ctx| strategy.on_event(ctx, record))?;

        // 6. Whatever it asked for.
        self.drain_commands()?;

        // 7. Orders whose latency has elapsed reach the venue.
        self.release_inflight(ts, strategy)?;

        self.records += 1;
        self.simulated_through = Some(ts);
        self.sample_equity(ts)?;
        Ok(())
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
            MarketEvent::Gap => book.mark_gap(ts),
            MarketEvent::Trade { price, .. } => {
                self.marks.insert(key.clone(), *price);
            }
            MarketEvent::Bar { close, .. } => {
                self.marks.insert(key.clone(), *close);
            }
            MarketEvent::Funding { rate } => {
                self.apply_funding(&record.instrument, *rate)?;
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

    fn drain_commands(&mut self) -> Result<()> {
        while let Some(command) = self.commands.pop_front() {
            match command {
                Command::Submit(request) => self.accept(request)?,
                Command::Cancel(id) => {
                    if let Some(order) = self.orders.get_mut(&id)
                        && order.is_open()
                    {
                        order.status = OrderStatus::Cancelled;
                        self.resting.retain(|open| *open != id);
                    }
                }
            }
        }
        Ok(())
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
            order.status = OrderStatus::Accepted;
            order.accepted_at = Some(ts);
            self.orders.insert(order.id, order.clone());
            self.try_fill(order.id, ts, strategy)?;
        }
        Ok(())
    }

    fn match_resting(&mut self, ts: UnixNanos, strategy: &mut dyn Strategy) -> Result<()> {
        let candidates: Vec<OrderId> = self.resting.clone();
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
        let instrument = self.instruments.get(&order.instrument)?;
        let commission = self.fee_model.commission(FeeContext {
            instrument,
            side: order.side,
            price,
            quantity,
            is_taker,
        })?;

        let gross = notional(price, quantity)?;
        // Buying spends cash, selling receives it; commission always costs.
        let delta = match order.side {
            Side::Buy => Money::from_raw(-gross.raw()),
            Side::Sell => gross,
        };
        self.cash = self.cash.checked_add(delta)?.checked_sub(commission)?;

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
        self.portfolio.apply(&fill)?;
        if let Some(order) = self.orders.get_mut(&id) {
            order.record_fill(quantity)?;
        }
        self.fills.push(fill.clone());
        self.with_context(|ctx| strategy.on_fill(ctx, &fill))?;
        self.drain_commands()?;
        Ok(())
    }

    fn finish(&mut self) -> Result<RunResult> {
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
        })
    }

    pub fn portfolio(&self) -> &Portfolio {
        &self.portfolio
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
