//! Corporate actions, and knowing which company a ticker meant.
//!
//! # Actions are applied forward, never backward
//!
//! The usual treatment of a split is to divide every historical price by
//! the ratio, producing an "adjusted" series. That is right for charting
//! and wrong for backtesting: nobody ever traded the adjusted price. A
//! strategy that bought at 50 the day before a 2-for-1 bought at 50, and
//! its order, its tick grid and its round-lot size were all denominated in
//! the pre-split price.
//!
//! So nothing here rewrites history. An action arrives as an event at the
//! instant it takes effect, and the engine applies it to *positions and
//! resting orders* -- doubling the shares, halving the basis -- exactly as
//! the venue does. Replayed prices stay as they were quoted.
//!
//! This also sidesteps the trap that consumed zipline: adjustment factors
//! are point-in-time data, and a backtest that reads today's factors has
//! read the future. An action that had not yet been announced simply is not
//! in the stream.
//!
//! # A ticker is not an identity
//!
//! Tickers are reused. `SymbolRegistry` maps a ticker to an instrument
//! *over a span*, so resolving one needs a date, and a lookup with no date
//! that could mean two companies is an error rather than a guess. That is
//! zipline's `MultipleSymbolsFound` rule, and it exists because the silent
//! alternative attributes one company's returns to another.

use std::collections::BTreeMap;

use crate::error::{BacktestError, Result};
use crate::instrument::InstrumentId;
use crate::types::{Money, Price, Qty, SCALE, UnixNanos};
use crate::window::TimeWindow;

/// Something a company did to its own shares.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CorporateAction {
    /// A share split. `ratio` is new shares per old share: a 2-for-1 is
    /// `2.0`, a 1-for-10 reverse split is `0.1`.
    Split { ratio: Price },
    /// A cash dividend, per share held.
    Dividend { per_share: Money },
    /// Trading ends and open positions are cashed out at `final_price`.
    Delist { final_price: Price },
}

impl CorporateAction {
    pub fn kind(&self) -> &'static str {
        match self {
            CorporateAction::Split { .. } => "split",
            CorporateAction::Dividend { .. } => "dividend",
            CorporateAction::Delist { .. } => "delist",
        }
    }

    /// Reject an action that cannot be applied.
    pub fn validate(&self) -> Result<()> {
        match self {
            CorporateAction::Split { ratio } => {
                if !ratio.is_positive() {
                    return Err(BacktestError::invalid(format!(
                        "a split ratio must be positive, got {ratio}"
                    )));
                }
                Ok(())
            }
            // A negative dividend is a capital call, which is a different
            // instrument's problem; refusing it here keeps a sign error
            // from quietly draining an account.
            CorporateAction::Dividend { per_share } => {
                if per_share.is_negative() {
                    return Err(BacktestError::invalid("a dividend must not be negative"));
                }
                Ok(())
            }
            CorporateAction::Delist { final_price } => {
                if final_price.is_negative() {
                    return Err(BacktestError::invalid(
                        "a delisting price must not be negative",
                    ));
                }
                Ok(())
            }
        }
    }

    /// Quantity and price after a split, leaving position value unchanged.
    pub fn split_position(ratio: Price, quantity: Qty, price: Price) -> Result<(Qty, Price)> {
        if !ratio.is_positive() {
            return Err(BacktestError::invalid("split ratio must be positive"));
        }
        let new_quantity = (quantity.raw() as i128 * ratio.raw() as i128) / SCALE as i128;
        let new_price = (price.raw() as i128 * SCALE as i128) / ratio.raw() as i128;
        Ok((
            Qty::from_raw(crate::types::narrow(
                new_quantity,
                "split_position quantity",
            )?),
            Price::from_raw(crate::types::narrow(new_price, "split_position price")?),
        ))
    }
}

/// One instrument's claim on a ticker, over a half-open span.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SymbolSpan {
    pub ticker: String,
    pub instrument: InstrumentId,
    pub span: TimeWindow,
}

/// Which instrument a ticker referred to, and when.
///
/// Spans are half-open and, within one ticker, contiguous: each period's
/// end is recomputed as its successor's start, so resolution is *total* --
/// there is no instant at which a reused ticker resolves to nothing merely
/// because two registrations left a gap between them.
#[derive(Clone, Default, Debug)]
pub struct SymbolRegistry {
    by_ticker: BTreeMap<String, Vec<SymbolSpan>>,
}

impl SymbolRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a ticker as belonging to an instrument from `from` onward.
    ///
    /// The previous holder's span is closed at this instant, which is what
    /// makes the timeline gapless.
    pub fn assign(
        &mut self,
        ticker: impl Into<String>,
        instrument: InstrumentId,
        from: UnixNanos,
    ) -> Result<()> {
        let ticker = ticker.into();
        let spans = self.by_ticker.entry(ticker.clone()).or_default();
        if let Some(previous) = spans.last_mut() {
            if from <= previous.span.start() {
                return Err(BacktestError::invalid(format!(
                    "ticker {ticker} is already held from {} onward; a later \
                     assignment must start after it",
                    previous.span.start()
                )));
            }
            previous.span = TimeWindow::new(previous.span.start(), from)?;
        }
        spans.push(SymbolSpan {
            ticker,
            instrument,
            span: TimeWindow::new(from, UnixNanos::MAX)?,
        });
        Ok(())
    }

    /// Which instrument this ticker meant at `at`.
    pub fn resolve(&self, ticker: &str, at: UnixNanos) -> Result<InstrumentId> {
        let spans = self
            .by_ticker
            .get(ticker)
            .ok_or_else(|| BacktestError::UnknownInstrument(ticker.to_string()))?;
        spans
            .iter()
            .find(|entry| entry.span.contains(at))
            .map(|entry| entry.instrument.clone())
            .ok_or_else(|| {
                BacktestError::invalid(format!(
                    "ticker {ticker} referred to nothing at {at}; its earliest \
                     registration begins at {}",
                    spans[0].span.start()
                ))
            })
    }

    /// Which instrument this ticker means, when that is unambiguous.
    ///
    /// A ticker held by two instruments over time has no single answer, and
    /// this refuses to invent one. Naming the candidates matters: the
    /// caller almost always has a date available and simply did not pass
    /// it.
    pub fn resolve_unique(&self, ticker: &str) -> Result<InstrumentId> {
        let spans = self
            .by_ticker
            .get(ticker)
            .ok_or_else(|| BacktestError::UnknownInstrument(ticker.to_string()))?;
        match spans.len() {
            1 => Ok(spans[0].instrument.clone()),
            _ => Err(BacktestError::invalid(format!(
                "ticker {ticker} referred to {} different instruments ({}); \
                 resolve it with a date",
                spans.len(),
                spans
                    .iter()
                    .map(|entry| entry.instrument.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ))),
        }
    }

    /// Every span, in ticker then time order.
    pub fn spans(&self) -> Vec<SymbolSpan> {
        self.by_ticker.values().flatten().cloned().collect()
    }

    pub fn is_empty(&self) -> bool {
        self.by_ticker.is_empty()
    }
}

/// Whether an instrument is a member of a universe at an instant.
///
/// Membership is a span, and a rolling calculation is *not* truncated at
/// the membership start: qlib's rule, and the reason it matters is that
/// filtering before a window computation silently shortens every lookback
/// that crosses an index entry.
#[derive(Clone, Default, Debug)]
pub struct Universe {
    members: BTreeMap<InstrumentId, Vec<TimeWindow>>,
}

impl Universe {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, instrument: InstrumentId, span: TimeWindow) {
        self.members.entry(instrument).or_default().push(span);
    }

    pub fn contains(&self, instrument: &InstrumentId, at: UnixNanos) -> bool {
        self.members
            .get(instrument)
            .map(|spans| spans.iter().any(|span| span.contains(at)))
            .unwrap_or(false)
    }

    /// Members at an instant, in deterministic order.
    pub fn members_at(&self, at: UnixNanos) -> Vec<InstrumentId> {
        self.members
            .iter()
            .filter(|(_, spans)| spans.iter().any(|span| span.contains(at)))
            .map(|(instrument, _)| instrument.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(value: i64) -> UnixNanos {
        UnixNanos::new(value)
    }

    fn id(name: &str) -> InstrumentId {
        InstrumentId::new(name).unwrap()
    }

    fn price(value: f64) -> Price {
        Price::from_f64(value).unwrap()
    }

    fn qty(value: f64) -> Qty {
        Qty::from_f64(value).unwrap()
    }

    // -- splits ------------------------------------------------------------

    #[test]
    fn a_two_for_one_split_doubles_shares_and_halves_the_basis() {
        let (quantity, basis) =
            CorporateAction::split_position(price(2.0), qty(100.0), price(50.0)).unwrap();
        assert_eq!(quantity, qty(200.0));
        assert_eq!(basis, price(25.0));
    }

    #[test]
    fn a_reverse_split_goes_the_other_way() {
        let (quantity, basis) =
            CorporateAction::split_position(price(0.1), qty(1_000.0), price(2.0)).unwrap();
        assert_eq!(quantity, qty(100.0));
        assert_eq!(basis, price(20.0));
    }

    #[test]
    fn a_split_leaves_position_value_unchanged() {
        // The invariant that makes a split a non-event economically.
        for ratio in [0.25, 0.5, 2.0, 3.0, 10.0] {
            let (quantity, basis) =
                CorporateAction::split_position(price(ratio), qty(120.0), price(45.0)).unwrap();
            let before = 120.0 * 45.0;
            let after = quantity.to_f64() * basis.to_f64();
            assert!(
                (before - after).abs() < 1e-6,
                "ratio {ratio}: {before} became {after}"
            );
        }
    }

    #[test]
    fn a_short_position_splits_too() {
        let (quantity, _) =
            CorporateAction::split_position(price(2.0), qty(-50.0), price(50.0)).unwrap();
        assert_eq!(quantity, qty(-100.0));
    }

    #[test]
    fn actions_validate_their_own_shape() {
        assert!(
            CorporateAction::Split { ratio: price(2.0) }
                .validate()
                .is_ok()
        );
        assert!(
            CorporateAction::Split { ratio: Price::ZERO }
                .validate()
                .is_err()
        );
        assert!(
            CorporateAction::Dividend {
                per_share: Money::from_f64(-1.0).unwrap()
            }
            .validate()
            .is_err(),
            "a negative dividend would quietly drain an account"
        );
        assert!(
            CorporateAction::Delist {
                final_price: Price::ZERO
            }
            .validate()
            .is_ok(),
            "a worthless delisting is a real outcome"
        );
    }

    // -- symbol identity ---------------------------------------------------

    #[test]
    fn a_ticker_resolves_to_whoever_held_it_at_the_time() {
        let mut registry = SymbolRegistry::new();
        registry.assign("XYZ", id("old-company"), ts(0)).unwrap();
        registry
            .assign("XYZ", id("new-company"), ts(1_000))
            .unwrap();

        assert_eq!(registry.resolve("XYZ", ts(500)).unwrap(), id("old-company"));
        assert_eq!(
            registry.resolve("XYZ", ts(1_500)).unwrap(),
            id("new-company")
        );
        // The handover instant belongs to the new holder: spans are
        // half-open, so there is no instant claimed by both.
        assert_eq!(
            registry.resolve("XYZ", ts(1_000)).unwrap(),
            id("new-company")
        );
        assert_eq!(registry.resolve("XYZ", ts(999)).unwrap(), id("old-company"));
    }

    #[test]
    fn the_timeline_has_no_gaps_between_holders() {
        let mut registry = SymbolRegistry::new();
        registry.assign("T", id("a"), ts(0)).unwrap();
        registry.assign("T", id("b"), ts(100)).unwrap();
        registry.assign("T", id("c"), ts(200)).unwrap();
        // Every instant from 0 onward resolves to exactly one holder.
        for at in [0, 50, 99, 100, 150, 199, 200, 10_000] {
            assert!(registry.resolve("T", ts(at)).is_ok(), "gap at {at}");
        }
    }

    #[test]
    fn an_ambiguous_lookup_without_a_date_is_refused() {
        // The silent alternative attributes one company's returns to
        // another, so this names the candidates instead of picking one.
        let mut registry = SymbolRegistry::new();
        registry.assign("XYZ", id("old-company"), ts(0)).unwrap();
        assert_eq!(registry.resolve_unique("XYZ").unwrap(), id("old-company"));

        registry
            .assign("XYZ", id("new-company"), ts(1_000))
            .unwrap();
        let error = registry.resolve_unique("XYZ").unwrap_err().to_string();
        assert!(error.contains("old-company"), "{error}");
        assert!(error.contains("new-company"), "{error}");
        assert!(error.contains("resolve it with a date"));
    }

    #[test]
    fn an_unknown_ticker_is_an_error_not_an_empty_answer() {
        let registry = SymbolRegistry::new();
        assert!(registry.resolve("NOPE", ts(1)).is_err());
        assert!(registry.resolve_unique("NOPE").is_err());
    }

    #[test]
    fn assignments_must_move_forward() {
        let mut registry = SymbolRegistry::new();
        registry.assign("T", id("a"), ts(100)).unwrap();
        assert!(
            registry.assign("T", id("b"), ts(50)).is_err(),
            "a ticker cannot be reassigned into the past"
        );
        assert!(registry.assign("T", id("b"), ts(100)).is_err());
    }

    #[test]
    fn a_ticker_before_its_first_registration_resolves_to_nothing() {
        let mut registry = SymbolRegistry::new();
        registry.assign("T", id("a"), ts(1_000)).unwrap();
        let error = registry.resolve("T", ts(500)).unwrap_err().to_string();
        assert!(error.contains("referred to nothing"), "{error}");
    }

    // -- universe ----------------------------------------------------------

    #[test]
    fn universe_membership_is_a_span() {
        let mut universe = Universe::new();
        universe.add(id("a"), TimeWindow::new(ts(0), ts(100)).unwrap());
        universe.add(id("b"), TimeWindow::new(ts(50), ts(200)).unwrap());

        assert!(universe.contains(&id("a"), ts(50)));
        assert!(
            !universe.contains(&id("a"), ts(100)),
            "half-open at the end"
        );
        assert_eq!(universe.members_at(ts(75)), vec![id("a"), id("b")]);
        assert_eq!(universe.members_at(ts(150)), vec![id("b")]);
        assert!(universe.members_at(ts(500)).is_empty());
    }

    #[test]
    fn an_instrument_can_leave_and_rejoin() {
        let mut universe = Universe::new();
        universe.add(id("a"), TimeWindow::new(ts(0), ts(100)).unwrap());
        universe.add(id("a"), TimeWindow::new(ts(300), ts(400)).unwrap());
        assert!(universe.contains(&id("a"), ts(50)));
        assert!(!universe.contains(&id("a"), ts(200)));
        assert!(universe.contains(&id("a"), ts(350)));
    }
}
