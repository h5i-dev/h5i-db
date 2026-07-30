//! The pluggable models a venue is made of.
//!
//! Four small traits carry every behavioural variation a venue can have.
//! That is deliberate and is the one strong opinion this crate holds about
//! configuration: the reference implementation's simulated venue takes some
//! thirty-five booleans, several of which exist to undo earlier defaults,
//! and no reader can tell from a config which of them interact. Here, new
//! behaviour is a new implementation of one of these traits, and the venue
//! config stays a list of models.

use crate::book::OrderBook;
use crate::error::Result;
use crate::instrument::Instrument;
use crate::order::{Order, OrderId};
use crate::types::{Money, Price, Qty, SCALE, Side, UnixNanos, notional};

/// What a fee model needs to price a fill.
#[derive(Clone, Copy, Debug)]
pub struct FeeContext<'a> {
    pub order_id: OrderId,
    pub instrument: &'a Instrument,
    pub side: Side,
    pub price: Price,
    pub quantity: Qty,
    pub is_taker: bool,
    /// When the fill happened. A schedule that depends on rolling volume
    /// cannot be priced without it, and taking it from a wall clock rather
    /// than the replay would make the fee non-deterministic.
    pub ts: UnixNanos,
}

/// What a venue charges.
pub trait FeeModel: std::fmt::Debug {
    fn commission(&self, ctx: FeeContext<'_>) -> Result<Money>;

    /// Release any per-order state once an order can no longer fill.
    fn order_closed(&self, _order_id: OrderId) {}
}

/// No fees.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoFees;

impl FeeModel for NoFees {
    fn commission(&self, _ctx: FeeContext<'_>) -> Result<Money> {
        Ok(Money::ZERO)
    }
}

/// A flat proportion of notional, the usual spot/perp shape.
#[derive(Clone, Copy, Debug)]
pub struct ProportionalFees {
    pub maker: Price,
    pub taker: Price,
}

impl ProportionalFees {
    pub fn new(maker: f64, taker: f64) -> Result<Self> {
        Ok(Self {
            maker: Price::from_f64(maker)?,
            taker: Price::from_f64(taker)?,
        })
    }
}

impl FeeModel for ProportionalFees {
    fn commission(&self, ctx: FeeContext<'_>) -> Result<Money> {
        let rate = if ctx.is_taker { self.taker } else { self.maker };
        let gross = notional(ctx.price, ctx.quantity)?;
        notional(rate, Qty::from_raw(gross.raw()))
    }
}

/// The curved fee prediction-market venues charge.
///
/// `fee = rate * quantity * p * (1 - p)`, which peaks at even odds and
/// vanishes at certainty. A flat `notional * rate` is simply the wrong
/// shape here and overcharges the tails badly, which matters because tail
/// prices are where these markets trade most.
///
/// Maker rebates are modelled as a negative commission, a fraction of the
/// fee-equivalent, matching how the venues actually pay them.
#[derive(Clone, Copy, Debug)]
pub struct PredictionMarketFees {
    pub rate: Price,
    /// Fraction of the fee-equivalent paid back to makers, `0` for none.
    pub maker_rebate: Price,
}

impl PredictionMarketFees {
    pub fn new(rate: f64) -> Result<Self> {
        Ok(Self {
            rate: Price::from_f64(rate)?,
            maker_rebate: Price::ZERO,
        })
    }

    pub fn with_maker_rebate(mut self, fraction: f64) -> Result<Self> {
        self.maker_rebate = Price::from_f64(fraction)?;
        Ok(self)
    }

    /// `rate * qty * p * (1 - p)`, the fee a taker pays.
    fn curve(&self, price: Price, quantity: Qty) -> Result<Money> {
        let complement = price.complement()?;
        let pq = notional(price, Qty::from_raw(complement.raw()))?;
        let scaled = notional(Price::from_raw(pq.raw()), quantity)?;
        notional(self.rate, Qty::from_raw(scaled.raw()))
    }
}

impl FeeModel for PredictionMarketFees {
    fn commission(&self, ctx: FeeContext<'_>) -> Result<Money> {
        let fee = self.curve(ctx.price, ctx.quantity)?;
        if ctx.is_taker {
            Ok(fee)
        } else if self.maker_rebate.is_zero() {
            Ok(Money::ZERO)
        } else {
            // Negative commission: the venue pays the maker.
            let rebate = notional(self.maker_rebate, Qty::from_raw(fee.raw()))?;
            Ok(Money::from_raw(-rebate.raw()))
        }
    }
}

/// Kalshi's quadratic fees with exchange balance rounding.
///
/// The trade component is `rate * quantity * p * (1-p)`, rounded up to a
/// centicent. The resulting cash movement is then floored to a whole cent.
/// Rounding overpayment accumulates per order and is rebated in whole cents,
/// matching Kalshi's documented partial-fill treatment.
#[derive(Debug)]
pub struct KalshiFees {
    pub taker_rate: Price,
    /// `None` for series without maker fees.
    pub maker_rate: Option<Price>,
    rounding: std::sync::Mutex<std::collections::BTreeMap<OrderId, i64>>,
}

impl KalshiFees {
    pub fn new(taker_rate: f64) -> Result<Self> {
        Self::with_maker_rate(taker_rate, None)
    }

    pub fn with_maker_rate(taker_rate: f64, maker_rate: Option<f64>) -> Result<Self> {
        let taker_rate = Price::from_f64(taker_rate)?;
        let maker_rate = maker_rate.map(Price::from_f64).transpose()?;
        if taker_rate.raw() < 0 || maker_rate.is_some_and(|rate| rate.raw() < 0) {
            return Err(crate::error::BacktestError::invalid(
                "Kalshi fee rates must not be negative",
            ));
        }
        Ok(Self {
            taker_rate,
            maker_rate,
            rounding: std::sync::Mutex::new(std::collections::BTreeMap::new()),
        })
    }

    fn trade_fee(&self, rate: Price, price: Price, quantity: Qty) -> Result<Money> {
        let complement = price.complement()?;
        let pq = notional(price, Qty::from_raw(complement.raw()))?;
        let scaled = notional(Price::from_raw(pq.raw()), quantity)?;
        let raw = notional(rate, Qty::from_raw(scaled.raw()))?.raw();
        const CENTICENT: i64 = SCALE / 10_000;
        let rounded =
            raw.checked_add(CENTICENT - 1)
                .ok_or(crate::error::BacktestError::Overflow {
                    what: "rounding Kalshi trade fee",
                })?
                / CENTICENT
                * CENTICENT;
        Ok(Money::from_raw(rounded))
    }
}

impl FeeModel for KalshiFees {
    fn commission(&self, ctx: FeeContext<'_>) -> Result<Money> {
        let rate = if ctx.is_taker {
            Some(self.taker_rate)
        } else {
            self.maker_rate
        };
        let trade_fee = match rate {
            Some(rate) => self.trade_fee(rate, ctx.price, ctx.quantity)?,
            None => Money::ZERO,
        };
        let gross = notional(ctx.price, ctx.quantity)?;
        let revenue = match ctx.side {
            Side::Buy => -gross.raw(),
            Side::Sell => gross.raw(),
        };
        let after_trade_fee =
            revenue
                .checked_sub(trade_fee.raw())
                .ok_or(crate::error::BacktestError::Overflow {
                    what: "applying Kalshi trade fee",
                })?;
        const CENT: i64 = SCALE / 100;
        let cent_aligned = after_trade_fee.div_euclid(CENT) * CENT;
        let rounding_fee = after_trade_fee - cent_aligned;

        let mut accumulators = self.rounding.lock().map_err(|_| {
            crate::error::BacktestError::invalid("Kalshi fee accumulator lock poisoned")
        })?;
        let accumulated = accumulators.entry(ctx.order_id).or_default();
        *accumulated =
            accumulated
                .checked_add(rounding_fee)
                .ok_or(crate::error::BacktestError::Overflow {
                    what: "accumulating Kalshi rounding fee",
                })?;
        let rebate = (*accumulated / CENT) * CENT;
        *accumulated -= rebate;
        Ok(Money::from_raw(trade_fee.raw() + rounding_fee - rebate))
    }

    fn order_closed(&self, order_id: OrderId) {
        if let Ok(mut accumulated) = self.rounding.lock() {
            accumulated.remove(&order_id);
        }
    }
}

/// One step of a volume-tiered fee schedule.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct FeeTier {
    /// Rolling traded notional at or above which this tier applies.
    pub volume_from: Money,
    /// Signed: a negative maker fee is a rebate, which the top tiers of a
    /// real schedule actually pay.
    pub maker: Price,
    pub taker: Price,
}

/// Fees that fall as rolling traded volume rises.
///
/// Every serious venue prices this way and most simulators do not, which
/// matters more than it sounds. A market-making strategy lives on the maker
/// side of the schedule, where the difference between the entry tier and a
/// high one is often the difference between a positive and a negative
/// expectancy -- and at the top of a real schedule the maker fee is
/// *negative*. Backtesting such a strategy at a flat entry-tier fee answers
/// a question nobody asked.
///
/// The schedule is supplied rather than baked in: venues republish theirs,
/// and a stale table compiled into a backtester is worse than no table at
/// all because it looks authoritative.
///
/// Volume is counted over a trailing window and a fill is priced at the
/// tier the account was in *before* it, which is what a venue does.
#[derive(Debug)]
pub struct TieredFees {
    tiers: Vec<FeeTier>,
    window_nanos: i64,
    traded: std::sync::Mutex<std::collections::VecDeque<(i64, i64)>>,
}

impl TieredFees {
    /// `tiers` must start at zero volume and rise; `window_nanos` is how
    /// far back the volume is counted.
    pub fn new(mut tiers: Vec<FeeTier>, window_nanos: i64) -> Result<Self> {
        if tiers.is_empty() {
            return Err(crate::error::BacktestError::invalid(
                "a fee schedule needs at least one tier",
            ));
        }
        if window_nanos <= 0 {
            return Err(crate::error::BacktestError::invalid(
                "a rolling fee window must be positive",
            ));
        }
        tiers.sort_by_key(|tier| tier.volume_from.raw());
        if !tiers[0].volume_from.is_zero() {
            return Err(crate::error::BacktestError::invalid(
                "the first fee tier must start at zero volume; an account \
                 that has never traded still has to be charged something",
            ));
        }
        if tiers
            .windows(2)
            .any(|pair| pair[0].volume_from == pair[1].volume_from)
        {
            return Err(crate::error::BacktestError::invalid(
                "two fee tiers start at the same volume, so which one applies \
                 would depend on sort order",
            ));
        }
        Ok(Self {
            tiers,
            window_nanos,
            traded: std::sync::Mutex::new(std::collections::VecDeque::new()),
        })
    }

    /// The tier an account with this much rolling volume is in.
    pub fn tier_for(&self, volume: Money) -> FeeTier {
        *self
            .tiers
            .iter()
            .rev()
            .find(|tier| volume >= tier.volume_from)
            .unwrap_or(&self.tiers[0])
    }
}

impl FeeModel for TieredFees {
    fn commission(&self, ctx: FeeContext<'_>) -> Result<Money> {
        let gross = notional(ctx.price, ctx.quantity)?.abs();
        let mut traded = self
            .traded
            .lock()
            .map_err(|_| crate::error::BacktestError::invalid("fee volume state was poisoned"))?;
        let cutoff = ctx.ts.get().saturating_sub(self.window_nanos);
        while traded.front().is_some_and(|(at, _)| *at < cutoff) {
            traded.pop_front();
        }
        let rolling: i128 = traded.iter().map(|(_, amount)| *amount as i128).sum();
        let rolling = Money::from_raw(rolling.clamp(0, i64::MAX as i128) as i64);
        let tier = self.tier_for(rolling);
        // Priced at the tier reached *before* this fill, then the fill
        // counts towards the next one.
        traded.push_back((ctx.ts.get(), gross.raw()));

        let rate = if ctx.is_taker { tier.taker } else { tier.maker };
        notional(rate, Qty::from_raw(gross.raw()))
    }
}

/// How long the venue takes to hear about an order.
pub trait LatencyModel: std::fmt::Debug {
    fn insert_nanos(&self) -> i64;
    fn cancel_nanos(&self) -> i64 {
        self.insert_nanos()
    }
}

/// Instant. Unrealistic for any real venue, and the default only because a
/// test that wants to reason about ordering should not also have to reason
/// about latency.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoLatency;

impl LatencyModel for NoLatency {
    fn insert_nanos(&self) -> i64 {
        0
    }
}

/// A fixed delay each way.
#[derive(Clone, Copy, Debug)]
pub struct ConstantLatency {
    pub insert: i64,
    pub cancel: i64,
}

impl ConstantLatency {
    pub fn millis(insert: i64, cancel: i64) -> Self {
        Self {
            insert: insert * 1_000_000,
            cancel: cancel * 1_000_000,
        }
    }
}

impl LatencyModel for ConstantLatency {
    fn insert_nanos(&self) -> i64 {
        self.insert
    }
    fn cancel_nanos(&self) -> i64 {
        self.cancel
    }
}

/// How an order meets the book.
///
/// The escape hatch is [`FillModel::book_for_fill`]: a model may return a
/// *synthetic* book, and the matching engine then runs against it exactly
/// as it would against the real one. That single hook covers slippage,
/// bar-derived pseudo-quotes and synthetic depth without any of them
/// needing their own path through matching -- the most reusable idea in the
/// reference architecture.
pub trait FillModel: std::fmt::Debug {
    /// The book to match against, or `None` to use the venue's own.
    fn book_for_fill(&self, _book: &OrderBook, _order: &Order) -> Option<OrderBook> {
        None
    }

    /// Whether matching preserves the recorded book's prices.
    ///
    /// This lets the engine reject non-crossing resting orders from the
    /// top of book without invoking the model. The conservative default is
    /// `false` because a custom model may synthesize different prices.
    fn preserves_book_prices(&self) -> bool {
        false
    }

    /// Whether a resting order at the touch fills when a trade prints
    /// through its price. Conservative by default: a print at your price
    /// does not prove your order was ahead in the queue.
    fn passive_fills_on_trade_at_price(&self) -> bool {
        false
    }

    /// Whether the engine should track queue position for resting orders.
    ///
    /// With this on, an order accepted at a price is placed *behind* the
    /// size already displayed there, and only fills once trades have
    /// consumed that much. It is the honest L2 approximation: a level 2
    /// feed shows how much is at a price but not who is where in the queue,
    /// so assuming you are last is the conservative reading, and assuming
    /// you are first is how backtests invent maker fills.
    fn uses_queue_position(&self) -> bool {
        false
    }

    /// Whether a trade with no stated aggressor may fill a passive order.
    ///
    /// Many venues publish prints without a side. Treating those as filling
    /// both sides prevents queues stalling forever, but it overstates fill
    /// probability -- every print counts twice. The default refuses, because
    /// a maker fill that did not happen flatters a backtest and a missed one
    /// merely understates it.
    fn fills_on_unknown_aggressor(&self) -> bool {
        false
    }
}

/// Match against the book as recorded, with no embellishment.
#[derive(Clone, Copy, Debug, Default)]
pub struct BookFills;

impl FillModel for BookFills {
    fn preserves_book_prices(&self) -> bool {
        true
    }
}

/// Move the book adverse to the taker by a fixed number of ticks.
///
/// Slippage is applied by *rewriting the book*, not by adjusting the fill
/// price afterwards, so the size available at each level stays consistent
/// with the price the taker gets.
#[derive(Clone, Copy, Debug)]
pub struct TickSlippage {
    pub ticks: i64,
    pub tick_size: Price,
}

impl TickSlippage {
    pub fn new(ticks: i64, tick_size: Price) -> Self {
        Self { ticks, tick_size }
    }
}

impl FillModel for TickSlippage {
    fn book_for_fill(&self, book: &OrderBook, order: &Order) -> Option<OrderBook> {
        if self.ticks == 0 {
            return None;
        }
        let shift = self.ticks * self.tick_size.raw();
        // A buyer pays up; a seller receives less.
        let (bid_shift, ask_shift) = match order.side {
            Side::Buy => (0, shift),
            Side::Sell => (-shift, 0),
        };
        let bids: Vec<_> = book
            .levels(Side::Buy)
            .into_iter()
            .map(|(p, q)| (Price::from_raw(p.raw() + bid_shift), q))
            .collect();
        let asks: Vec<_> = book
            .levels(Side::Sell)
            .into_iter()
            .map(|(p, q)| (Price::from_raw(p.raw() + ask_shift), q))
            .collect();
        let mut synthetic = OrderBook::new();
        synthetic
            .apply_snapshot(
                &bids,
                &asks,
                book.last_update().unwrap_or(UnixNanos::new(0)),
            )
            .ok()?;
        Some(synthetic)
    }
}

/// Match passively, respecting queue position.
///
/// An order joining a price level starts behind everything already
/// displayed there and advances only as trades consume the level. Nothing
/// about an L2 feed says where in the queue an order actually sits, so this
/// takes the pessimistic reading; `optimistic()` takes the other one, and
/// the gap between two runs is a useful measure of how much a strategy
/// depends on queue luck.
#[derive(Clone, Copy, Debug, Default)]
pub struct QueuePositionFills {
    /// Let prints with no stated aggressor consume the queue.
    pub fill_on_unknown_aggressor: bool,
}

impl QueuePositionFills {
    pub fn new() -> Self {
        Self::default()
    }

    /// Also consume the queue on prints that name no aggressor.
    pub fn optimistic() -> Self {
        Self {
            fill_on_unknown_aggressor: true,
        }
    }
}

impl FillModel for QueuePositionFills {
    fn preserves_book_prices(&self) -> bool {
        true
    }

    fn uses_queue_position(&self) -> bool {
        true
    }

    fn fills_on_unknown_aggressor(&self) -> bool {
        self.fill_on_unknown_aggressor
    }
}

/// A periodic venue process: funding, fee waivers, liquidation checks.
///
/// Returns cash adjustments to apply to the account.
pub trait VenueModule: std::fmt::Debug {
    fn name(&self) -> &str;
    /// Interval between runs, in nanoseconds.
    fn interval_nanos(&self) -> i64;
    fn on_interval(&mut self, ts: UnixNanos) -> Result<Money>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::instrument::{Instrument, OutcomeId};
    use crate::order::{Order, OrderId};
    use crate::types::UnixNanos;

    fn market() -> Instrument {
        Instrument::binary("m", "v").unwrap()
    }

    fn ctx<'a>(instrument: &'a Instrument, price: f64, qty: f64, is_taker: bool) -> FeeContext<'a> {
        FeeContext {
            order_id: OrderId(1),
            instrument,
            side: Side::Buy,
            price: Price::from_f64(price).unwrap(),
            quantity: Qty::from_f64(qty).unwrap(),
            is_taker,
            ts: UnixNanos::new(0),
        }
    }

    #[test]
    fn prediction_market_fees_peak_at_even_odds() {
        let instrument = market();
        let fees = PredictionMarketFees::new(0.07).unwrap();
        // 0.07 * 100 * 0.5 * 0.5 = 1.75
        let even = fees
            .commission(ctx(&instrument, 0.50, 100.0, true))
            .unwrap();
        assert_eq!(even, Money::from_f64(1.75).unwrap());

        // The same notional far from even odds costs much less.
        let tail = fees
            .commission(ctx(&instrument, 0.05, 100.0, true))
            .unwrap();
        assert_eq!(tail, Money::from_f64(0.07 * 100.0 * 0.05 * 0.95).unwrap());
        assert!(tail < even);
    }

    #[test]
    fn prediction_market_fees_are_symmetric_about_a_half() {
        let instrument = market();
        let fees = PredictionMarketFees::new(0.07).unwrap();
        let low = fees.commission(ctx(&instrument, 0.30, 10.0, true)).unwrap();
        let high = fees.commission(ctx(&instrument, 0.70, 10.0, true)).unwrap();
        assert_eq!(low, high, "p(1-p) is symmetric");
    }

    #[test]
    fn kalshi_fees_apply_exchange_rounding() {
        let instrument = market();
        let fees = KalshiFees::new(0.07).unwrap();

        // The raw fee is $0.0175. Kalshi first rounds it to a centicent, then
        // rounds the cash movement to a cent.
        assert_eq!(
            fees.commission(ctx(&instrument, 0.50, 1.0, true)).unwrap(),
            Money::from_f64(0.02).unwrap()
        );
    }

    #[test]
    fn kalshi_fees_rebate_accumulated_partial_fill_rounding() {
        let instrument = market();
        let fees = KalshiFees::new(0.07).unwrap();
        let mut total_raw = 0;

        for _ in 0..3 {
            total_raw += fees
                .commission(ctx(&instrument, 0.50, 0.3, true))
                .unwrap()
                .raw();
        }

        // Three independent cash movements would each round to one cent. The
        // per-order accumulator returns the excess once it reaches a cent.
        assert_eq!(Money::from_raw(total_raw), Money::from_f64(0.02).unwrap());

        fees.order_closed(OrderId(1));
        assert_eq!(
            fees.commission(ctx(&instrument, 0.50, 0.3, true)).unwrap(),
            Money::from_f64(0.01).unwrap()
        );

        let mut other_order = ctx(&instrument, 0.50, 0.3, true);
        other_order.order_id = OrderId(2);
        assert_eq!(
            fees.commission(other_order).unwrap(),
            Money::from_f64(0.01).unwrap()
        );
    }

    #[test]
    fn fees_vanish_at_certainty() {
        let instrument = market();
        let fees = PredictionMarketFees::new(0.07).unwrap();
        assert_eq!(
            fees.commission(ctx(&instrument, 1.0, 100.0, true)).unwrap(),
            Money::ZERO
        );
        assert_eq!(
            fees.commission(ctx(&instrument, 0.0, 100.0, true)).unwrap(),
            Money::ZERO
        );
    }

    #[test]
    fn a_maker_rebate_is_a_negative_commission() {
        let instrument = market();
        let fees = PredictionMarketFees::new(0.07)
            .unwrap()
            .with_maker_rebate(0.25)
            .unwrap();
        let maker = fees
            .commission(ctx(&instrument, 0.5, 100.0, false))
            .unwrap();
        assert!(maker.is_negative(), "the venue pays the maker");
        assert_eq!(maker, Money::from_f64(-0.4375).unwrap());
        // Without a rebate configured a maker simply pays nothing.
        let plain = PredictionMarketFees::new(0.07).unwrap();
        assert_eq!(
            plain
                .commission(ctx(&instrument, 0.5, 100.0, false))
                .unwrap(),
            Money::ZERO
        );
    }

    #[test]
    fn proportional_fees_charge_the_notional() {
        let instrument = Instrument::perpetual("p", "v").unwrap();
        let fees = ProportionalFees::new(0.0002, 0.0005).unwrap();
        // 0.0005 * (100 * 10) = 0.5
        assert_eq!(
            fees.commission(ctx(&instrument, 100.0, 10.0, true))
                .unwrap(),
            Money::from_f64(0.5).unwrap()
        );
        assert_eq!(
            fees.commission(ctx(&instrument, 100.0, 10.0, false))
                .unwrap(),
            Money::from_f64(0.2).unwrap()
        );
    }

    #[test]
    fn no_fees_charges_nothing() {
        let instrument = market();
        assert_eq!(
            NoFees
                .commission(ctx(&instrument, 0.5, 1_000.0, true))
                .unwrap(),
            Money::ZERO
        );
    }

    #[test]
    fn slippage_rewrites_the_book_adverse_to_the_taker() {
        let mut book = OrderBook::new();
        book.apply_snapshot(
            &[(Price::from_f64(0.40).unwrap(), Qty::from_f64(10.0).unwrap())],
            &[(Price::from_f64(0.42).unwrap(), Qty::from_f64(10.0).unwrap())],
            UnixNanos::new(1),
        )
        .unwrap();
        let model = TickSlippage::new(2, Price::from_f64(0.01).unwrap());

        let buy = Order::market(
            OrderId(1),
            crate::instrument::InstrumentId::new("m").unwrap(),
            OutcomeId::FIRST,
            Side::Buy,
            Qty::from_f64(1.0).unwrap(),
            UnixNanos::new(1),
        )
        .unwrap();
        let synthetic = model.book_for_fill(&book, &buy).unwrap();
        assert_eq!(
            synthetic.best_ask().unwrap().0,
            Price::from_f64(0.44).unwrap(),
            "a buyer pays up"
        );
        assert_eq!(
            synthetic.best_bid().unwrap().0,
            Price::from_f64(0.40).unwrap(),
            "the side they are not taking is untouched"
        );

        let mut sell = buy.clone();
        sell.side = Side::Sell;
        let synthetic = model.book_for_fill(&book, &sell).unwrap();
        assert_eq!(
            synthetic.best_bid().unwrap().0,
            Price::from_f64(0.38).unwrap(),
            "a seller receives less"
        );
    }

    #[test]
    fn zero_slippage_uses_the_real_book() {
        let book = OrderBook::new();
        let model = TickSlippage::new(0, Price::from_f64(0.01).unwrap());
        let order = Order::market(
            OrderId(1),
            crate::instrument::InstrumentId::new("m").unwrap(),
            OutcomeId::FIRST,
            Side::Buy,
            Qty::from_f64(1.0).unwrap(),
            UnixNanos::new(1),
        )
        .unwrap();
        assert!(model.book_for_fill(&book, &order).is_none());
    }

    #[test]
    fn latency_models_report_their_delays() {
        assert_eq!(NoLatency.insert_nanos(), 0);
        let latency = ConstantLatency::millis(5, 10);
        assert_eq!(latency.insert_nanos(), 5_000_000);
        assert_eq!(latency.cancel_nanos(), 10_000_000);
    }

    #[test]
    fn the_default_fill_model_does_not_assume_queue_priority() {
        // A print at your price does not prove your order was ahead of it.
        assert!(!BookFills.passive_fills_on_trade_at_price());
    }
}
