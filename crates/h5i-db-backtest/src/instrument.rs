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

    #[inline]
    pub fn outcome_count(&self) -> u16 {
        self.outcomes.len() as u16
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
        Ok(())
    }

    /// Round a price down to the tick grid (towards zero).
    pub fn round_to_tick(&self, price: Price) -> Price {
        let tick = self.tick_size.raw();
        if tick <= 0 {
            return price;
        }
        Price::from_raw(price.raw() - price.raw() % tick)
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
