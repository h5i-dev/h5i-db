//! Instruments, with N-outcome markets as the general case.
//!
//! A prediction market is one instrument with *N outcomes*, not N
//! instruments that happen to be related. Binary is the two-outcome case.
//!
//! The production stack studied for this design did the opposite: it modelled
//! only binary options and emulated multi-outcome markets by loading the YES
//! and NO tokens as two independent instruments paired positionally by the
//! strategy. That works until an outcome is added or resolved separately, and
//! it makes the invariant that actually defines these markets -- the outcome
//! prices sum to one -- inexpressible, because no single object owns all the
//! outcomes. So outcomes are a dimension here from the start.

use std::collections::HashMap;

use crate::error::{BacktestError, Result};
use crate::types::{Price, Qty, SCALE, UnixNanos};

/// A venue-qualified instrument identifier.
///
/// Backed by `Arc<str>` rather than `String` because every replayed record
/// carries one and cloning is on the hot path. A `String` id makes each
/// clone a heap allocation and each record a hundred-odd bytes wider, which
/// at a hundred million events is the difference between a replay that fits
/// in memory and one that does not.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Debug)]
pub struct InstrumentId(std::sync::Arc<str>);

impl InstrumentId {
    pub fn new(id: impl Into<String>) -> Result<Self> {
        let id = id.into();
        if id.trim().is_empty() {
            return Err(BacktestError::invalid("instrument id must not be empty"));
        }
        Ok(Self(id.into()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for InstrumentId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Which outcome of a multi-outcome instrument. Always 0 for single-outcome
/// instruments such as a perpetual future.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug, Default)]
pub struct OutcomeId(pub u16);

impl OutcomeId {
    pub const FIRST: Self = Self(0);

    #[inline]
    pub const fn index(self) -> usize {
        self.0 as usize
    }
}

impl std::fmt::Display for OutcomeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// What kind of thing is being traded, and the mechanics that follow from it.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum InstrumentKind {
    /// Outcomes that resolve to exactly one winner, priced as probabilities
    /// in `[0, 1]` that sum to one across the set.
    PredictionMarket,
    /// A perpetual swap: no expiry, periodic funding.
    Perpetual,
    /// Plain spot.
    Spot,
}

impl InstrumentKind {
    /// Whether prices are probabilities bounded to `[0, 1]`.
    pub fn is_probability(&self) -> bool {
        matches!(self, InstrumentKind::PredictionMarket)
    }

    /// Whether opening a position *buys* something or merely *collateralises*
    /// it.
    ///
    /// A prediction-market contract and a spot asset are paid for: cash
    /// leaves the account for the full notional and the asset arrives. A
    /// perpetual is not bought at all -- margin is posted, and cash moves
    /// only as profit is realised. Settling a perp as if it were purchased
    /// drives cash deeply negative on entry, which then reads as an
    /// insolvent account and liquidates a perfectly healthy position.
    pub fn is_funded(&self) -> bool {
        matches!(
            self,
            InstrumentKind::PredictionMarket | InstrumentKind::Spot
        )
    }
}

/// Which prices a venue will accept.
///
/// A uniform grid is the common case and the one every equity and
/// prediction market uses. It is not universal. Hyperliquid caps a price at
/// **five significant figures** as well as a decimal count, so the spacing
/// widens as the price rises: `0.0012345` is valid and `1.0012345` is not,
/// and no single tick expresses that. Rounding such a venue onto a flat
/// grid accepts orders it would reject at high prices and rejects orders it
/// would accept at low ones, which shows up as fills at prices the venue
/// could never have quoted -- exactly what [`Instrument::check_price`]
/// exists to prevent.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum PriceRule {
    /// Every price is a multiple of the instrument's tick.
    #[default]
    Tick,
    /// At most `significant_figures` significant digits *and* at most
    /// `max_decimals` decimal places.
    ///
    /// A whole number is always accepted regardless of its digit count,
    /// which is the venue's own carve-out: without it a price of 123456
    /// would be untradeable.
    SignificantFigures {
        significant_figures: u8,
        max_decimals: u8,
    },
}

/// One tradable instrument.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Instrument {
    pub id: InstrumentId,
    pub venue: String,
    pub kind: InstrumentKind,
    /// Human labels, one per outcome. Length is the outcome count and is
    /// at least one.
    pub outcomes: Vec<String>,
    pub tick_size: Price,
    pub lot_size: Qty,
    /// What cash this instrument settles in. A run holding instruments in
    /// several currencies keeps their balances apart, and needs a rate to
    /// report one number across them.
    pub settlement_currency: crate::currency::Currency,
    /// When trading stops, if it ever does.
    pub expiration: Option<UnixNanos>,
    /// The first instant at which this market's resolution could have been
    /// *observed*. Settlement is only applied to a run that reached it (see
    /// [`crate::settlement`]); without it a short replay of a long market
    /// would book resolution profit that nobody could have collected.
    pub settlement_observable: Option<UnixNanos>,
    /// Whether the venue itself will exchange a complete set of these
    /// outcomes for one unit of settlement currency, and back.
    ///
    /// Polymarket calls this a *negative-risk* market, and the flag travels
    /// under that name in its metadata; Kalshi and the plain Gnosis
    /// conditional-token contract give every two-outcome condition the same
    /// property without naming it. What it means here is narrower and more
    /// useful than the venue's spelling: the outcomes are not merely
    /// *believed* to be exclusive and exhaustive, the venue will trade a set
    /// against a dollar on that basis. That is the difference between a
    /// sum-to-one relationship a strategy may bet on and one it may
    /// *transact*, so it gates [`crate::engine::Context::mint`],
    /// [`crate::engine::Context::redeem`] and
    /// [`crate::engine::Context::convert`].
    ///
    /// A two-outcome market is set-exchangeable regardless: see
    /// [`Instrument::supports_complete_set`].
    pub neg_risk: bool,
    /// Which prices the venue accepts. `tick_size` is the finest increment
    /// under any rule; a non-`Tick` rule narrows what is legal further.
    pub price_rule: PriceRule,
}

impl Instrument {
    pub fn prediction_market(
        id: impl Into<String>,
        venue: impl Into<String>,
        outcomes: Vec<String>,
    ) -> Result<Self> {
        if outcomes.len() < 2 {
            return Err(BacktestError::invalid(
                "a prediction market needs at least two outcomes; a market \
                 with one possible result is not a market",
            ));
        }
        Ok(Self {
            id: InstrumentId::new(id)?,
            venue: venue.into(),
            kind: InstrumentKind::PredictionMarket,
            outcomes,
            tick_size: Price::from_raw(SCALE / 10_000), // 0.0001
            lot_size: Qty::from_raw(SCALE),             // one contract
            settlement_currency: crate::currency::Currency::new("USDC")?,
            expiration: None,
            settlement_observable: None,
            neg_risk: false,
            price_rule: PriceRule::Tick,
        })
    }

    /// The two-outcome case, spelled for the common path.
    pub fn binary(id: impl Into<String>, venue: impl Into<String>) -> Result<Self> {
        Self::prediction_market(id, venue, vec!["YES".into(), "NO".into()])
    }

    pub fn perpetual(id: impl Into<String>, venue: impl Into<String>) -> Result<Self> {
        Ok(Self {
            id: InstrumentId::new(id)?,
            venue: venue.into(),
            kind: InstrumentKind::Perpetual,
            outcomes: vec!["-".into()],
            tick_size: Price::from_raw(SCALE / 100),
            lot_size: Qty::from_raw(SCALE / 1_000),
            settlement_currency: crate::currency::Currency::new("USDC")?,
            expiration: None,
            settlement_observable: None,
            neg_risk: false,
            price_rule: PriceRule::Tick,
        })
    }

    pub fn with_tick_size(mut self, tick: Price) -> Self {
        self.tick_size = tick;
        self
    }

    pub fn with_lot_size(mut self, lot: Qty) -> Self {
        self.lot_size = lot;
        self
    }

    pub fn with_settlement_currency(mut self, currency: crate::currency::Currency) -> Self {
        self.settlement_currency = currency;
        self
    }

    pub fn with_expiration(mut self, at: UnixNanos) -> Self {
        self.expiration = Some(at);
        self
    }

    pub fn with_settlement_observable(mut self, at: UnixNanos) -> Self {
        self.settlement_observable = Some(at);
        self
    }

    /// Declare that the venue exchanges a complete set for one unit of cash.
    pub fn with_neg_risk(mut self, neg_risk: bool) -> Self {
        self.neg_risk = neg_risk;
        self
    }

    /// Which prices this venue accepts, when a flat tick does not say it.
    ///
    /// A significant-figure rule also fixes the tick at the finest legal
    /// increment, `10^-max_decimals`, since the two constraints are checked
    /// together and a tick coarser than that would silently dominate.
    pub fn with_price_rule(mut self, rule: PriceRule) -> Result<Self> {
        if let PriceRule::SignificantFigures {
            significant_figures,
            max_decimals,
        } = rule
        {
            if significant_figures == 0 || significant_figures > 18 {
                return Err(BacktestError::invalid(
                    "a significant-figure limit must lie in 1..=18",
                ));
            }
            if max_decimals > 9 {
                return Err(BacktestError::invalid(format!(
                    "{} decimal places exceeds the fixed-point scale's nine",
                    max_decimals
                )));
            }
            self.tick_size = Price::from_raw(10_i64.pow(9 - max_decimals as u32));
        }
        self.price_rule = rule;
        Ok(self)
    }

    #[inline]
    pub fn outcome_count(&self) -> u16 {
        self.outcomes.len() as u16
    }

    /// Every outcome id, in index order.
    pub fn outcome_ids(&self) -> impl Iterator<Item = OutcomeId> + use<> {
        (0..self.outcome_count()).map(OutcomeId)
    }

    /// Whether a complete set of these outcomes can be minted from, and
    /// redeemed for, one unit of the settlement currency.
    ///
    /// True for every two-outcome prediction market -- YES and NO of one
    /// condition are two halves of a dollar at the contract level, on every
    /// venue that lists them -- and for a wider market only when
    /// [`Instrument::neg_risk`] says the venue wired the outcomes into a
    /// single exclusive set.
    ///
    /// The asymmetry is not an oversight. A venue may group several
    /// independent binary conditions under one heading for display; their
    /// prices need not sum to one and no contract will trade them as a set,
    /// so minting across them would create a dollar out of nothing. Modelled
    /// here, such a group is several instruments, not one with many
    /// outcomes.
    pub fn supports_complete_set(&self) -> bool {
        matches!(self.kind, InstrumentKind::PredictionMarket)
            && (self.outcome_count() == 2 || self.neg_risk)
    }

    pub fn outcome(&self, id: OutcomeId) -> Result<&str> {
        self.outcomes
            .get(id.index())
            .map(String::as_str)
            .ok_or_else(|| BacktestError::UnknownOutcome {
                instrument: self.id.to_string(),
                outcome: id.0,
                count: self.outcome_count(),
            })
    }

    pub fn check_outcome(&self, id: OutcomeId) -> Result<()> {
        self.outcome(id).map(|_| ())
    }

    /// Validate a price for this instrument.
    ///
    /// Probability instruments are bounded to `[0, 1]`; everything is
    /// checked against the tick grid, because a price off the grid is a
    /// unit or scaling mistake, and letting it through produces fills at
    /// prices the venue could never have quoted.
    pub fn check_price(&self, price: Price) -> Result<()> {
        if self.kind.is_probability()
            && (price < Price::PROBABILITY_MIN || price > Price::PROBABILITY_MAX)
        {
            return Err(BacktestError::invalid(format!(
                "price {price} is outside [0, 1] for probability instrument {}",
                self.id
            )));
        }
        if self.tick_size.raw() > 0 && price.raw() % self.tick_size.raw() != 0 {
            return Err(BacktestError::invalid(format!(
                "price {price} is not a multiple of {}'s tick size {}",
                self.id, self.tick_size
            )));
        }
        if let PriceRule::SignificantFigures {
            significant_figures,
            ..
        } = self.price_rule
            && !is_whole(price)
            && count_significant_figures(price) > significant_figures
        {
            return Err(BacktestError::invalid(format!(
                "price {price} carries {} significant figures but {} accepts \
                 at most {significant_figures}",
                count_significant_figures(price),
                self.id
            )));
        }
        Ok(())
    }

    /// Round a price towards zero onto the grid this instrument accepts.
    ///
    /// Kept named for the tick because that is what it does under the common
    /// rule; under a significant-figure rule it also drops the digits the
    /// venue would refuse.
    pub fn round_to_tick(&self, price: Price) -> Price {
        let tick = self.tick_size.raw();
        let mut raw = price.raw();
        if tick > 0 {
            raw -= raw % tick;
        }
        if let PriceRule::SignificantFigures {
            significant_figures,
            ..
        } = self.price_rule
        {
            raw = truncate_to_significant_figures(raw, significant_figures);
        }
        Price::from_raw(raw)
    }

    /// How far a set of outcome prices is from summing to one.
    ///
    /// Zero on a perfectly consistent set. Real books never are, and the
    /// deviation is a tradable signal in itself, so this reports rather
    /// than enforces.
    pub fn completeness_error(&self, prices: &[Price]) -> Result<Price> {
        if prices.len() != self.outcomes.len() {
            return Err(BacktestError::invalid(format!(
                "{} has {} outcomes but {} prices were supplied",
                self.id,
                self.outcomes.len(),
                prices.len()
            )));
        }
        let sum: i64 = prices.iter().map(|p| p.raw()).sum();
        Ok(Price::from_raw(sum - SCALE))
    }
}

/// Whether a price is a whole number of units.
///
/// Venues that cap significant figures exempt integers, because otherwise a
/// price of 123456 would carry six of them and be untradeable.
fn is_whole(price: Price) -> bool {
    price.raw() % SCALE == 0
}

/// How many significant digits a fixed-point price carries.
///
/// Leading zeros after the point do not count and trailing zeros are not
/// significant, so `0.0012` has two and `1.2000` has two. Computed on the
/// raw integer rather than on a formatted string: the string form of a
/// binary float is where this kind of check usually goes wrong.
fn count_significant_figures(price: Price) -> u8 {
    let mut raw = price.raw().unsigned_abs();
    if raw == 0 {
        return 0;
    }
    while raw % 10 == 0 {
        raw /= 10;
    }
    let mut digits = 0_u8;
    while raw > 0 {
        raw /= 10;
        digits += 1;
    }
    digits
}

/// Drop the digits past `figures` significant ones, towards zero.
fn truncate_to_significant_figures(raw: i64, figures: u8) -> i64 {
    if figures == 0 || raw == 0 {
        return raw;
    }
    let magnitude = raw.unsigned_abs();
    // A whole number keeps every digit: that is the venue's carve-out.
    if magnitude % (SCALE as u64) == 0 {
        return raw;
    }
    let mut digits = 0_u32;
    let mut scan = magnitude;
    while scan > 0 {
        scan /= 10;
        digits += 1;
    }
    if digits <= figures as u32 {
        return raw;
    }
    let drop = 10_i64.pow(digits - figures as u32);
    raw - raw % drop
}

/// Split one unit across `outcomes` as evenly as fixed point allows.
///
/// Three outcomes cannot each take exactly a third of 1e9 raw units, so the
/// remainder goes to the first outcome rather than being dropped. Losing it
/// would leave a set worth 0.999999999, and a set that is not worth exactly
/// one is a slow leak through every mint, redeem and void settlement.
pub fn uniform_prices(outcomes: u16) -> Result<Vec<Price>> {
    if outcomes == 0 {
        return Err(BacktestError::invalid(
            "cannot split one unit across zero outcomes",
        ));
    }
    let count = outcomes as i64;
    let base = SCALE / count;
    let remainder = SCALE % count;
    let mut prices = vec![Price::from_raw(base); outcomes as usize];
    prices[0] = Price::from_raw(base + remainder);
    Ok(prices)
}

/// Scale a set of outcome prices so they sum to exactly one.
///
/// Real books never sum to one -- the deviation is the tradable signal
/// [`Instrument::completeness_error`] reports -- but an operation that
/// exchanges a whole set against a dollar has to divide that dollar somehow,
/// and the market's own relative view is the only division that needs no
/// invented input. The residual left by integer division is added to the
/// largest component (lowest index wins a tie), so the sum is exact and the
/// choice is the same on every run.
pub fn normalise_to_one(prices: &[Price]) -> Result<Vec<Price>> {
    if prices.is_empty() {
        return Err(BacktestError::invalid(
            "cannot normalise an empty price set",
        ));
    }
    let total: i128 = prices.iter().map(|price| price.raw() as i128).sum();
    if total <= 0 || prices.iter().any(|price| price.is_negative()) {
        return Err(BacktestError::invalid(
            "outcome prices must be non-negative and sum to more than zero to be normalised",
        ));
    }
    let mut scaled: Vec<Price> = prices
        .iter()
        .map(|price| Price::from_raw(((price.raw() as i128) * (SCALE as i128) / total) as i64))
        .collect();
    let residual = SCALE - scaled.iter().map(|price| price.raw()).sum::<i64>();
    if residual != 0 {
        let largest = scaled
            .iter()
            .enumerate()
            .max_by_key(|(index, price)| (price.raw(), std::cmp::Reverse(*index)))
            .map(|(index, _)| index)
            .expect("non-empty");
        scaled[largest] = Price::from_raw(scaled[largest].raw() + residual);
    }
    Ok(scaled)
}

/// The instruments a run knows about.
#[derive(Clone, Default, Debug)]
pub struct InstrumentSet {
    by_id: HashMap<InstrumentId, Instrument>,
}

impl InstrumentSet {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, instrument: Instrument) -> Result<()> {
        if self.by_id.contains_key(&instrument.id) {
            return Err(BacktestError::invalid(format!(
                "instrument {} is already registered",
                instrument.id
            )));
        }
        self.by_id.insert(instrument.id.clone(), instrument);
        Ok(())
    }

    pub fn get(&self, id: &InstrumentId) -> Result<&Instrument> {
        self.by_id
            .get(id)
            .ok_or_else(|| BacktestError::UnknownInstrument(id.to_string()))
    }

    pub fn contains(&self, id: &InstrumentId) -> bool {
        self.by_id.contains_key(id)
    }

    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }

    /// Ids in sorted order, so anything iterating instruments is
    /// deterministic rather than depending on hash order.
    pub fn ids(&self) -> Vec<InstrumentId> {
        let mut ids: Vec<_> = self.by_id.keys().cloned().collect();
        ids.sort();
        ids
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_prediction_market_needs_at_least_two_outcomes() {
        assert!(Instrument::prediction_market("m", "v", vec!["ONLY".into()]).is_err());
        assert!(Instrument::binary("m", "v").is_ok());
    }

    #[test]
    fn categorical_markets_are_the_general_case() {
        let market = Instrument::prediction_market(
            "election-2028",
            "polymarket",
            vec!["A".into(), "B".into(), "C".into(), "other".into()],
        )
        .unwrap();
        assert_eq!(market.outcome_count(), 4);
        assert_eq!(market.outcome(OutcomeId(2)).unwrap(), "C");
        let err = market.outcome(OutcomeId(9)).unwrap_err();
        assert!(matches!(
            err,
            BacktestError::UnknownOutcome { count: 4, .. }
        ));
    }

    #[test]
    fn binary_is_just_two_outcomes() {
        let market = Instrument::binary("will-it-rain", "kalshi").unwrap();
        assert_eq!(market.outcome_count(), 2);
        assert_eq!(market.outcome(OutcomeId::FIRST).unwrap(), "YES");
        assert_eq!(market.outcome(OutcomeId(1)).unwrap(), "NO");
    }

    #[test]
    fn probability_prices_are_bounded() {
        let market = Instrument::binary("m", "v").unwrap();
        assert!(market.check_price(Price::from_f64(0.5).unwrap()).is_ok());
        assert!(market.check_price(Price::from_f64(0.0).unwrap()).is_ok());
        assert!(market.check_price(Price::from_f64(1.0).unwrap()).is_ok());
        assert!(market.check_price(Price::from_f64(1.5).unwrap()).is_err());
        assert!(market.check_price(Price::from_f64(-0.1).unwrap()).is_err());
    }

    #[test]
    fn off_grid_prices_are_refused() {
        let market = Instrument::binary("m", "v").unwrap();
        // Tick is 0.0001; 0.00005 is half a tick.
        let off_grid = Price::from_f64(0.00005).unwrap();
        assert!(market.check_price(off_grid).is_err());
        assert_eq!(market.round_to_tick(off_grid), Price::from_raw(0));
        let on_grid = market.round_to_tick(Price::from_f64(0.37129).unwrap());
        assert!(market.check_price(on_grid).is_ok());
    }

    #[test]
    fn completeness_error_measures_the_deviation_from_one() {
        let market =
            Instrument::prediction_market("m", "v", vec!["A".into(), "B".into(), "C".into()])
                .unwrap();
        let exact = [
            Price::from_f64(0.5).unwrap(),
            Price::from_f64(0.3).unwrap(),
            Price::from_f64(0.2).unwrap(),
        ];
        assert_eq!(market.completeness_error(&exact).unwrap(), Price::ZERO);

        let rich = [
            Price::from_f64(0.55).unwrap(),
            Price::from_f64(0.3).unwrap(),
            Price::from_f64(0.2).unwrap(),
        ];
        assert_eq!(
            market.completeness_error(&rich).unwrap(),
            Price::from_f64(0.05).unwrap()
        );
        assert!(market.completeness_error(&exact[..2]).is_err());
    }

    /// A Hyperliquid perp: five significant figures, and decimals capped at
    /// `6 - szDecimals`. This is BTC, whose `szDecimals` is five.
    fn hyperliquid_perp() -> Instrument {
        Instrument::perpetual("BTC-PERP", "hyperliquid")
            .unwrap()
            .with_price_rule(PriceRule::SignificantFigures {
                significant_figures: 5,
                max_decimals: 1,
            })
            .unwrap()
    }

    #[test]
    fn significant_figures_widen_the_grid_as_the_price_rises() {
        // The whole point of the rule: one tick cannot express it. At 0.5 a
        // tenth is legal; at 50000 it is not, because five figures are
        // already spent.
        let perp = Instrument::perpetual("KPEPE-PERP", "hyperliquid")
            .unwrap()
            .with_price_rule(PriceRule::SignificantFigures {
                significant_figures: 5,
                max_decimals: 6,
            })
            .unwrap();
        // Near zero the six-decimal budget is the only binding constraint,
        // so four significant digits fit inside it.
        assert!(perp.check_price(Price::from_f64(0.001234).unwrap()).is_ok());
        // The same six decimals at a price of one carry seven significant
        // figures, which the venue will not quote.
        assert!(
            perp.check_price(Price::from_f64(1.001234).unwrap())
                .is_err(),
            "seven significant figures is more than the venue accepts"
        );
        assert!(perp.check_price(Price::from_f64(1.0012).unwrap()).is_ok());
    }

    #[test]
    fn a_whole_number_is_always_a_legal_price() {
        // Without the carve-out a six-figure price would be untradeable,
        // which would make most of the BTC book unquotable.
        let perp = hyperliquid_perp();
        assert!(
            perp.check_price(Price::from_units(123_456).unwrap())
                .is_ok()
        );
        assert!(
            perp.check_price(Price::from_f64(50_000.5).unwrap())
                .is_err()
        );
        assert!(perp.check_price(Price::from_units(50_001).unwrap()).is_ok());
    }

    #[test]
    fn the_decimal_cap_still_applies_under_a_figure_rule() {
        // szDecimals five leaves one decimal place, so a hundredth is off
        // the grid however few figures it carries.
        let perp = hyperliquid_perp();
        assert_eq!(perp.tick_size, Price::from_f64(0.1).unwrap());
        assert!(perp.check_price(Price::from_f64(1.05).unwrap()).is_err());
        assert!(perp.check_price(Price::from_f64(1.1).unwrap()).is_ok());
    }

    #[test]
    fn rounding_drops_the_digits_the_venue_would_refuse() {
        let perp = Instrument::perpetual("ETH-PERP", "hyperliquid")
            .unwrap()
            .with_price_rule(PriceRule::SignificantFigures {
                significant_figures: 5,
                max_decimals: 4,
            })
            .unwrap();
        let rounded = perp.round_to_tick(Price::from_f64(1234.5678).unwrap());
        assert_eq!(rounded, Price::from_f64(1234.5).unwrap());
        assert!(perp.check_price(rounded).is_ok());
        // Already legal prices are left alone.
        let legal = Price::from_f64(0.1234).unwrap();
        assert_eq!(perp.round_to_tick(legal), legal);
    }

    #[test]
    fn significant_figures_are_counted_on_the_integer_not_a_float_string() {
        for (value, expected) in [
            (0.0012, 2_u8),
            (1.2, 2),
            (1.0012345, 8),
            (12.345, 5),
            (0.5, 1),
        ] {
            assert_eq!(
                count_significant_figures(Price::from_f64(value).unwrap()),
                expected,
                "{value}"
            );
        }
        assert_eq!(count_significant_figures(Price::ZERO), 0);
    }

    #[test]
    fn a_price_rule_that_cannot_be_represented_is_refused() {
        let perp = Instrument::perpetual("p", "v").unwrap();
        assert!(
            perp.clone()
                .with_price_rule(PriceRule::SignificantFigures {
                    significant_figures: 0,
                    max_decimals: 2
                })
                .is_err()
        );
        assert!(
            perp.with_price_rule(PriceRule::SignificantFigures {
                significant_figures: 5,
                max_decimals: 12
            })
            .is_err()
        );
    }

    #[test]
    fn a_binary_market_is_always_set_exchangeable() {
        // YES and NO of one condition are two halves of a dollar on every
        // venue that lists them, whether or not it says "neg risk".
        assert!(
            Instrument::binary("m", "polymarket")
                .unwrap()
                .supports_complete_set()
        );
    }

    #[test]
    fn a_wide_market_needs_the_venue_to_say_the_set_is_tradable() {
        let market = Instrument::prediction_market(
            "m",
            "polymarket",
            vec!["A".into(), "B".into(), "C".into()],
        )
        .unwrap();
        assert!(
            !market.supports_complete_set(),
            "three grouped conditions are not a set until the venue wires them into one"
        );
        assert!(market.with_neg_risk(true).supports_complete_set());
    }

    #[test]
    fn nothing_but_a_prediction_market_has_a_complete_set() {
        assert!(
            !Instrument::perpetual("p", "v")
                .unwrap()
                .with_neg_risk(true)
                .supports_complete_set()
        );
    }

    #[test]
    fn a_uniform_split_still_sums_to_exactly_one() {
        for outcomes in 1..=17u16 {
            let prices = uniform_prices(outcomes).unwrap();
            assert_eq!(prices.len(), outcomes as usize);
            assert_eq!(
                prices.iter().map(|price| price.raw()).sum::<i64>(),
                SCALE,
                "{outcomes} outcomes must still divide one exactly"
            );
        }
        // Three-way: the indivisible remainder lands on the first outcome.
        let thirds = uniform_prices(3).unwrap();
        assert_eq!(thirds[0].raw(), 333_333_334);
        assert_eq!(thirds[1].raw(), 333_333_333);
    }

    #[test]
    fn normalising_a_book_that_does_not_sum_to_one_is_exact() {
        // An overpriced book: 0.55 + 0.30 + 0.20 = 1.05.
        let raw = [
            Price::from_f64(0.55).unwrap(),
            Price::from_f64(0.30).unwrap(),
            Price::from_f64(0.20).unwrap(),
        ];
        let normalised = normalise_to_one(&raw).unwrap();
        assert_eq!(
            normalised.iter().map(|price| price.raw()).sum::<i64>(),
            SCALE
        );
        // Order is preserved and the largest stays largest.
        assert!(normalised[0] > normalised[1] && normalised[1] > normalised[2]);
        // A set already summing to one is left alone.
        let exact = [
            Price::from_f64(0.5).unwrap(),
            Price::from_f64(0.3).unwrap(),
            Price::from_f64(0.2).unwrap(),
        ];
        assert_eq!(normalise_to_one(&exact).unwrap(), exact.to_vec());
    }

    #[test]
    fn normalising_refuses_a_set_it_cannot_divide() {
        assert!(normalise_to_one(&[]).is_err());
        assert!(normalise_to_one(&[Price::ZERO, Price::ZERO]).is_err());
        assert!(normalise_to_one(&[Price::from_raw(-1), Price::from_raw(SCALE + 1)]).is_err());
    }

    #[test]
    fn instrument_set_refuses_duplicates_and_iterates_deterministically() {
        let mut set = InstrumentSet::new();
        set.insert(Instrument::binary("b", "v").unwrap()).unwrap();
        set.insert(Instrument::binary("a", "v").unwrap()).unwrap();
        assert!(set.insert(Instrument::binary("a", "v").unwrap()).is_err());
        assert_eq!(
            set.ids().iter().map(|i| i.as_str()).collect::<Vec<_>>(),
            vec!["a", "b"]
        );
        assert!(set.get(&InstrumentId::new("missing").unwrap()).is_err());
    }
}
