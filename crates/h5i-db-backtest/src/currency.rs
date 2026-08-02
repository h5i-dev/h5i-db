//! Currencies, exchange rates, and the refusal to guess one.
//!
//! A single-currency backtest is a special case, not the world. A perp
//! margined in USDC, a prediction market settling in USDC, and a European
//! equity quoted in EUR cannot be added together without a rate, and the
//! rate is *market data*: it moves, and using today's rate to value last
//! year's position is the same class of error as using tomorrow's price.
//!
//! So rates arrive as records like everything else, and conversion at an
//! instant uses the rate known at that instant. When no rate is known, the
//! answer is not zero and not the identity -- it is
//! [`FxError::NoRate`], reported the same way an unmarked position is,
//! because a portfolio that silently values EUR as USD is wrong in a way
//! that looks plausible for a long time.

use std::collections::BTreeMap;
use std::fmt;

use crate::error::{BacktestError, Result};
use crate::types::{Money, Price, Qty, notional};

/// An ISO-style currency code.
///
/// Compared and ordered as text, so anything iterating currencies is
/// deterministic rather than hash-ordered.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Currency(String);

impl Currency {
    pub fn new(code: impl Into<String>) -> Result<Self> {
        let code = code.into();
        let trimmed = code.trim();
        if trimmed.is_empty() || trimmed.len() > 12 {
            return Err(BacktestError::invalid(format!(
                "currency code {code:?} must be 1 to 12 characters"
            )));
        }
        if !trimmed
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '.')
        {
            return Err(BacktestError::invalid(format!(
                "currency code {code:?} must be alphanumeric"
            )));
        }
        Ok(Self(trimmed.to_ascii_uppercase()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Currency {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Debug for Currency {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Currency({})", self.0)
    }
}

/// Why a conversion could not be done.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum FxError {
    /// No rate is known between these two currencies.
    NoRate { from: Currency, to: Currency },
}

impl fmt::Display for FxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FxError::NoRate { from, to } => write!(
                f,
                "no exchange rate is known from {from} to {to}; the value \
                 cannot be converted and must not be assumed equal"
            ),
        }
    }
}

/// The exchange rates known right now.
///
/// Rates are *replaced* as new ones arrive, so the book always holds the
/// latest known rate and never a blend. Inverses are derived rather than
/// stored, so a feed publishing only `EUR/USD` still answers `USD/EUR`
/// without a second row that could drift out of step with the first.
#[derive(Clone, Default, Debug)]
pub struct FxBook {
    rates: BTreeMap<(Currency, Currency), Price>,
}

impl FxBook {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record `1 base = rate quote`.
    pub fn set(&mut self, base: Currency, quote: Currency, rate: Price) -> Result<()> {
        if !rate.is_positive() {
            return Err(BacktestError::invalid(format!(
                "exchange rate {base}/{quote} must be positive, got {rate}"
            )));
        }
        self.rates.insert((base, quote), rate);
        Ok(())
    }

    /// The rate to multiply a `from` amount by to get `to`.
    pub fn rate(&self, from: &Currency, to: &Currency) -> std::result::Result<Price, FxError> {
        if from == to {
            return Ok(Price::from_raw(crate::types::SCALE));
        }
        if let Some(rate) = self.rates.get(&(from.clone(), to.clone())) {
            return Ok(*rate);
        }
        // The inverse, derived rather than stored.
        if let Some(rate) = self.rates.get(&(to.clone(), from.clone()))
            && rate.is_positive()
        {
            let inverse =
                (crate::types::SCALE as i128 * crate::types::SCALE as i128) / rate.raw() as i128;
            // The rate table has its own error type, and an inverse that will
            // not fit is a missing rate as far as a caller is concerned.
            return crate::types::narrow(inverse, "Currency::inverse_rate")
                .map(Price::from_raw)
                .map_err(|_| FxError::NoRate {
                    from: from.clone(),
                    to: to.clone(),
                });
        }
        Err(FxError::NoRate {
            from: from.clone(),
            to: to.clone(),
        })
    }

    /// Convert an amount, or say why it could not be.
    pub fn convert(
        &self,
        amount: Money,
        from: &Currency,
        to: &Currency,
    ) -> std::result::Result<Money, FxError> {
        if from == to {
            return Ok(amount);
        }
        let rate = self.rate(from, to)?;
        notional(rate, Qty::from_raw(amount.raw())).map_err(|_| FxError::NoRate {
            from: from.clone(),
            to: to.clone(),
        })
    }

    pub fn is_empty(&self) -> bool {
        self.rates.is_empty()
    }

    /// Currency pairs currently known, in deterministic order.
    pub fn pairs(&self) -> Vec<(Currency, Currency)> {
        self.rates.keys().cloned().collect()
    }
}

/// How much of a balance counts as collateral.
///
/// A haircut is the discount a venue applies to non-settlement collateral:
/// 0.9 means a unit of that currency supports 0.9 units of margin. Held
/// separately from the rate because it is a *credit* decision, not a market
/// one, and conflating the two hides which is which when a position is
/// close to liquidation.
///
/// **Not yet wired into the engine.** Only [`crate::account::Account`] holds
/// one, and the engine does not hold an `Account`: it carries a single cash
/// balance in the reporting currency, which has no haircut to apply. Setting
/// factors here changes nothing about a run until the engine values several
/// balances at once.
#[derive(Clone, Default, Debug)]
pub struct Haircuts {
    factors: BTreeMap<Currency, Price>,
}

impl Haircuts {
    pub fn new() -> Self {
        Self::default()
    }

    /// `factor` in `(0, 1]`: the fraction of value that counts.
    pub fn set(&mut self, currency: Currency, factor: Price) -> Result<()> {
        if !factor.is_positive() || factor > Price::from_raw(crate::types::SCALE) {
            return Err(BacktestError::invalid(format!(
                "haircut factor for {currency} must be in (0, 1], got {factor}"
            )));
        }
        self.factors.insert(currency, factor);
        Ok(())
    }

    /// The factor for a currency; anything unlisted counts in full.
    pub fn factor(&self, currency: &Currency) -> Price {
        self.factors
            .get(currency)
            .copied()
            .unwrap_or(Price::from_raw(crate::types::SCALE))
    }

    /// Apply the haircut to an amount.
    pub fn apply(&self, amount: Money, currency: &Currency) -> Result<Money> {
        let factor = self.factor(currency);
        notional(factor, Qty::from_raw(amount.raw()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn usd() -> Currency {
        Currency::new("usd").unwrap()
    }

    fn eur() -> Currency {
        Currency::new("EUR").unwrap()
    }

    fn money(value: f64) -> Money {
        Money::from_f64(value).unwrap()
    }

    #[test]
    fn currency_codes_normalise_and_validate() {
        assert_eq!(usd().as_str(), "USD", "codes upper-case");
        assert_eq!(Currency::new("  eur ").unwrap(), eur(), "and trim");
        assert!(Currency::new("").is_err());
        assert!(Currency::new("with space").is_err());
        assert!(Currency::new("waytoolongacode").is_err());
        // Crypto tickers with dots are real.
        assert!(Currency::new("USDC.e").is_ok());
    }

    #[test]
    fn converting_to_the_same_currency_is_the_identity() {
        let book = FxBook::new();
        assert_eq!(
            book.convert(money(100.0), &usd(), &usd()).unwrap(),
            money(100.0)
        );
    }

    #[test]
    fn a_known_rate_converts_both_ways() {
        let mut book = FxBook::new();
        book.set(eur(), usd(), Price::from_f64(1.10).unwrap())
            .unwrap();
        assert_eq!(
            book.convert(money(100.0), &eur(), &usd()).unwrap(),
            money(110.0)
        );
        // The inverse is derived, not stored.
        let back = book.convert(money(110.0), &usd(), &eur()).unwrap();
        assert!(
            (back.to_f64() - 100.0).abs() < 1e-6,
            "round trip gave {back}"
        );
    }

    #[test]
    fn an_unknown_rate_is_refused_rather_than_assumed_to_be_one() {
        // The failure this prevents: silently valuing EUR as USD.
        let book = FxBook::new();
        let error = book.convert(money(100.0), &eur(), &usd()).unwrap_err();
        assert_eq!(
            error,
            FxError::NoRate {
                from: eur(),
                to: usd()
            }
        );
        assert!(error.to_string().contains("must not be assumed equal"));
    }

    #[test]
    fn a_later_rate_replaces_an_earlier_one() {
        let mut book = FxBook::new();
        book.set(eur(), usd(), Price::from_f64(1.10).unwrap())
            .unwrap();
        book.set(eur(), usd(), Price::from_f64(1.20).unwrap())
            .unwrap();
        assert_eq!(
            book.convert(money(100.0), &eur(), &usd()).unwrap(),
            money(120.0)
        );
    }

    #[test]
    fn a_non_positive_rate_is_refused() {
        let mut book = FxBook::new();
        assert!(book.set(eur(), usd(), Price::ZERO).is_err());
        assert!(
            book.set(eur(), usd(), Price::from_f64(-1.0).unwrap())
                .is_err()
        );
    }

    #[test]
    fn haircuts_discount_collateral_and_default_to_full_value() {
        let mut haircuts = Haircuts::new();
        haircuts.set(eur(), Price::from_f64(0.9).unwrap()).unwrap();
        assert_eq!(haircuts.apply(money(100.0), &eur()).unwrap(), money(90.0));
        // An unlisted currency counts in full rather than at zero.
        assert_eq!(haircuts.apply(money(100.0), &usd()).unwrap(), money(100.0));
    }

    #[test]
    fn a_haircut_outside_zero_to_one_is_refused() {
        let mut haircuts = Haircuts::new();
        assert!(haircuts.set(usd(), Price::ZERO).is_err());
        assert!(haircuts.set(usd(), Price::from_f64(1.5).unwrap()).is_err());
        assert!(haircuts.set(usd(), Price::from_f64(1.0).unwrap()).is_ok());
    }
}
