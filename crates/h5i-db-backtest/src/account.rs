//! Multi-currency balances, margin, and liquidation.
//!
//! Two things the earlier cash-only model got wrong by omission.
//!
//! **Balances are per currency.** A run holding USDC and EUR has two
//! balances, and reporting one number for both needs a rate at that
//! instant. Where no rate is known the equity is not reported as a
//! plausible wrong number; it is reported as unconvertible.
//!
//! **Leverage is finite.** Without a margin model a perpetual strategy can
//! hold any size at any price, so a position that would have been
//! liquidated instead shows a profit. That is not a rough edge, it is a
//! wrong answer, and it is the class of error this module exists to remove.

use std::collections::BTreeMap;

use crate::currency::{Currency, FxBook, FxError, Haircuts};
use crate::error::{BacktestError, Result};
use crate::instrument::{Instrument, InstrumentId, OutcomeId};
use crate::position::Position;
use crate::types::{Money, Price, Qty, SCALE, notional};

/// How much collateral a position requires.
///
/// Two levels, as every leveraged venue has: `initial` gates opening, and
/// `maintenance` (always the smaller) gates survival. Collapsing them into
/// one number makes a position that can be opened immediately liquidatable,
/// which no real venue does.
pub trait MarginModel: std::fmt::Debug {
    fn initial_margin(&self, instrument: &Instrument, quantity: Qty, mark: Price) -> Result<Money>;

    fn maintenance_margin(
        &self,
        instrument: &Instrument,
        quantity: Qty,
        mark: Price,
    ) -> Result<Money>;
}

/// Fully funded: a position costs its whole notional and is never
/// liquidated. The right model for spot and for prediction markets, where a
/// contract is prepaid and the worst case is already on deposit.
#[derive(Clone, Copy, Debug, Default)]
pub struct CashMargin;

impl MarginModel for CashMargin {
    fn initial_margin(
        &self,
        _instrument: &Instrument,
        quantity: Qty,
        mark: Price,
    ) -> Result<Money> {
        Ok(notional(mark, quantity)?.abs())
    }

    fn maintenance_margin(
        &self,
        _instrument: &Instrument,
        _quantity: Qty,
        _mark: Price,
    ) -> Result<Money> {
        // A prepaid position cannot be margin-called: the money is already
        // gone, and the worst outcome is that it is worth nothing.
        Ok(Money::ZERO)
    }
}

/// A fixed fraction of notional, the usual perpetual shape.
///
/// `initial = notional / leverage`, with maintenance a smaller fraction.
#[derive(Clone, Copy, Debug)]
pub struct LinearMargin {
    pub initial_rate: Price,
    pub maintenance_rate: Price,
}

impl LinearMargin {
    /// From a maximum leverage, with maintenance at half the initial rate.
    pub fn from_leverage(leverage: f64) -> Result<Self> {
        if !(leverage.is_finite() && leverage >= 1.0) {
            return Err(BacktestError::invalid(
                "leverage must be finite and at least 1",
            ));
        }
        let initial = Price::from_f64(1.0 / leverage)?;
        Ok(Self {
            initial_rate: initial,
            maintenance_rate: Price::from_raw(initial.raw() / 2),
        })
    }

    pub fn new(initial_rate: f64, maintenance_rate: f64) -> Result<Self> {
        let initial = Price::from_f64(initial_rate)?;
        let maintenance = Price::from_f64(maintenance_rate)?;
        if maintenance > initial {
            return Err(BacktestError::invalid(
                "maintenance margin must not exceed initial margin: a \
                 position that can be opened must not be immediately \
                 liquidatable",
            ));
        }
        Ok(Self {
            initial_rate: initial,
            maintenance_rate: maintenance,
        })
    }

    fn requirement(&self, rate: Price, quantity: Qty, mark: Price) -> Result<Money> {
        let gross = notional(mark, quantity)?.abs();
        notional(rate, Qty::from_raw(gross.raw()))
    }
}

impl MarginModel for LinearMargin {
    fn initial_margin(
        &self,
        _instrument: &Instrument,
        quantity: Qty,
        mark: Price,
    ) -> Result<Money> {
        self.requirement(self.initial_rate, quantity, mark)
    }

    fn maintenance_margin(
        &self,
        _instrument: &Instrument,
        quantity: Qty,
        mark: Price,
    ) -> Result<Money> {
        self.requirement(self.maintenance_rate, quantity, mark)
    }
}

/// What an equity calculation could and could not value.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Valuation {
    /// Total in the reporting currency, over everything convertible.
    pub total: Money,
    pub currency: Currency,
    /// Balances that had no rate to the reporting currency, and so are
    /// **not** in `total`. Present so a partial valuation cannot be read as
    /// a complete one.
    pub unconvertible: Vec<(Currency, Money)>,
}

impl Valuation {
    pub fn is_complete(&self) -> bool {
        self.unconvertible.is_empty()
    }
}

/// Cash balances in one or more currencies, plus the margin rules.
#[derive(Debug)]
pub struct Account {
    balances: BTreeMap<Currency, Money>,
    reporting: Currency,
    haircuts: Haircuts,
    margin: Box<dyn MarginModel>,
}

impl Account {
    pub fn new(reporting: Currency, margin: Box<dyn MarginModel>) -> Self {
        Self {
            balances: BTreeMap::new(),
            reporting,
            haircuts: Haircuts::new(),
            margin,
        }
    }

    /// A single-currency cash account, the common case.
    pub fn cash(reporting: Currency, starting: Money) -> Self {
        let mut account = Self::new(reporting.clone(), Box::new(CashMargin));
        account.deposit(reporting, starting);
        account
    }

    pub fn with_haircuts(mut self, haircuts: Haircuts) -> Self {
        self.haircuts = haircuts;
        self
    }

    pub fn reporting_currency(&self) -> &Currency {
        &self.reporting
    }

    pub fn deposit(&mut self, currency: Currency, amount: Money) {
        let balance = self.balances.entry(currency).or_insert(Money::ZERO);
        *balance = Money::from_raw(balance.raw() + amount.raw());
    }

    pub fn balance(&self, currency: &Currency) -> Money {
        self.balances.get(currency).copied().unwrap_or(Money::ZERO)
    }

    /// Balances in deterministic order.
    pub fn balances(&self) -> impl Iterator<Item = (&Currency, &Money)> {
        self.balances.iter()
    }

    /// Move cash, in whatever currency the leg settles in.
    pub fn apply(&mut self, currency: &Currency, delta: Money) -> Result<()> {
        let balance = self.balances.entry(currency.clone()).or_insert(Money::ZERO);
        *balance = balance.checked_add(delta)?;
        Ok(())
    }

    /// Total cash in the reporting currency, naming what could not convert.
    pub fn cash_value(&self, fx: &FxBook) -> Result<Valuation> {
        let mut total = Money::ZERO;
        let mut unconvertible = Vec::new();
        for (currency, amount) in &self.balances {
            if amount.is_zero() {
                continue;
            }
            match fx.convert(*amount, currency, &self.reporting) {
                Ok(converted) => total = total.checked_add(converted)?,
                Err(FxError::NoRate { .. }) => unconvertible.push((currency.clone(), *amount)),
            }
        }
        Ok(Valuation {
            total,
            currency: self.reporting.clone(),
            unconvertible,
        })
    }

    /// Cash usable as margin, after haircuts.
    pub fn collateral(&self, fx: &FxBook) -> Result<Valuation> {
        let mut total = Money::ZERO;
        let mut unconvertible = Vec::new();
        for (currency, amount) in &self.balances {
            if !amount.is_positive() {
                // A negative balance is a debt, not collateral, and it
                // counts against you in full regardless of haircut.
                match fx.convert(*amount, currency, &self.reporting) {
                    Ok(converted) => total = total.checked_add(converted)?,
                    Err(FxError::NoRate { .. }) => unconvertible.push((currency.clone(), *amount)),
                }
                continue;
            }
            let after_haircut = self.haircuts.apply(*amount, currency)?;
            match fx.convert(after_haircut, currency, &self.reporting) {
                Ok(converted) => total = total.checked_add(converted)?,
                Err(FxError::NoRate { .. }) => unconvertible.push((currency.clone(), *amount)),
            }
        }
        Ok(Valuation {
            total,
            currency: self.reporting.clone(),
            unconvertible,
        })
    }

    pub fn margin_model(&self) -> &dyn MarginModel {
        self.margin.as_ref()
    }
}

/// What margin the open book requires right now.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct MarginRequirement {
    pub initial: Money,
    pub maintenance: Money,
    /// Positions with no mark, whose requirement could not be computed and
    /// is therefore **not** included above.
    pub unmarked: Vec<(InstrumentId, OutcomeId)>,
}

/// Total margin required across a set of positions.
pub fn margin_requirement<'a>(
    model: &dyn MarginModel,
    positions: impl Iterator<Item = &'a Position>,
    instruments: &crate::instrument::InstrumentSet,
    marks: &BTreeMap<(InstrumentId, OutcomeId), Price>,
) -> Result<MarginRequirement> {
    let mut out = MarginRequirement::default();
    for position in positions {
        if position.is_flat() {
            continue;
        }
        let key = (position.instrument.clone(), position.outcome);
        let Some(mark) = marks.get(&key) else {
            out.unmarked.push(key);
            continue;
        };
        let instrument = instruments.get(&position.instrument)?;
        out.initial = out.initial.checked_add(model.initial_margin(
            instrument,
            position.quantity,
            *mark,
        )?)?;
        out.maintenance = out.maintenance.checked_add(model.maintenance_margin(
            instrument,
            position.quantity,
            *mark,
        )?)?;
    }
    Ok(out)
}

/// Whether an account is above water, and by how much.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct MarginState {
    /// Collateral plus unrealized profit, in the reporting currency.
    pub equity: Money,
    pub initial: Money,
    pub maintenance: Money,
    /// True when equity has fallen below the maintenance requirement.
    pub liquidatable: bool,
    /// Set when something could not be valued, which makes the rest of
    /// this a partial answer rather than a verdict.
    pub incomplete: bool,
}

impl MarginState {
    /// Equity over maintenance requirement. Higher is safer; below 1 is a
    /// liquidation.
    pub fn margin_ratio(&self) -> Option<f64> {
        if self.maintenance.is_zero() {
            return None;
        }
        Some(self.equity.to_f64() / self.maintenance.to_f64())
    }

    /// Free collateral available to open more.
    pub fn available(&self) -> Money {
        Money::from_raw(self.equity.raw() - self.initial.raw())
    }
}

/// Evaluate an account against its open positions.
///
/// `unrealized` is the marked profit already computed by the portfolio, in
/// the reporting currency. Liquidation is *not* declared when anything was
/// unconvertible or unmarked: closing someone's book on the strength of a
/// number known to be incomplete is worse than continuing.
pub fn margin_state(
    collateral: &Valuation,
    unrealized: Money,
    requirement: &MarginRequirement,
) -> Result<MarginState> {
    let equity = collateral.total.checked_add(unrealized)?;
    let incomplete = !collateral.is_complete() || !requirement.unmarked.is_empty();
    Ok(MarginState {
        equity,
        initial: requirement.initial,
        maintenance: requirement.maintenance,
        liquidatable: !incomplete
            && !requirement.maintenance.is_zero()
            && equity < requirement.maintenance,
        incomplete,
    })
}

/// A position closed by the venue rather than by the strategy.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Liquidation {
    pub instrument: InstrumentId,
    pub outcome: OutcomeId,
    pub quantity: Qty,
    pub mark: Price,
    pub equity: Money,
    pub maintenance: Money,
}

/// Scale a price by a fraction, for margin arithmetic.
pub fn scaled(price: Price, fraction: Price) -> Result<Price> {
    let product = (price.raw() as i128 * fraction.raw() as i128) / SCALE as i128;
    Ok(Price::from_raw(product as i64))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::instrument::InstrumentSet;
    use crate::order::{Fill, OrderId};
    use crate::position::Portfolio;
    use crate::types::{Side, UnixNanos};

    fn usd() -> Currency {
        Currency::new("USD").unwrap()
    }

    fn eur() -> Currency {
        Currency::new("EUR").unwrap()
    }

    fn money(value: f64) -> Money {
        Money::from_f64(value).unwrap()
    }

    fn price(value: f64) -> Price {
        Price::from_f64(value).unwrap()
    }

    fn qty(value: f64) -> Qty {
        Qty::from_f64(value).unwrap()
    }

    fn perp() -> Instrument {
        Instrument::perpetual("BTC-PERP", "hyperliquid").unwrap()
    }

    fn fill(side: Side, price_value: f64, quantity: f64) -> Fill {
        Fill {
            order_id: OrderId(1),
            instrument: InstrumentId::new("BTC-PERP").unwrap(),
            outcome: OutcomeId::FIRST,
            side,
            price: price(price_value),
            quantity: qty(quantity),
            commission: Money::ZERO,
            is_taker: true,
            ts: UnixNanos::new(0),
            tag: None,
        }
    }

    fn marks(value: f64) -> BTreeMap<(InstrumentId, OutcomeId), Price> {
        let mut out = BTreeMap::new();
        out.insert(
            (InstrumentId::new("BTC-PERP").unwrap(), OutcomeId::FIRST),
            price(value),
        );
        out
    }

    fn instruments() -> InstrumentSet {
        let mut set = InstrumentSet::new();
        set.insert(perp()).unwrap();
        set
    }

    // -- balances ----------------------------------------------------------

    #[test]
    fn balances_are_kept_per_currency() {
        let mut account = Account::cash(usd(), money(1_000.0));
        account.deposit(eur(), money(500.0));
        assert_eq!(account.balance(&usd()), money(1_000.0));
        assert_eq!(account.balance(&eur()), money(500.0));
        assert_eq!(account.balance(&Currency::new("JPY").unwrap()), Money::ZERO);
    }

    #[test]
    fn cash_value_converts_what_it_can_and_names_what_it_cannot() {
        let mut account = Account::cash(usd(), money(1_000.0));
        account.deposit(eur(), money(100.0));
        let mut fx = FxBook::new();

        // Without a rate the EUR is excluded and reported, not counted as USD.
        let partial = account.cash_value(&fx).unwrap();
        assert_eq!(partial.total, money(1_000.0));
        assert_eq!(partial.unconvertible, vec![(eur(), money(100.0))]);
        assert!(!partial.is_complete());

        fx.set(eur(), usd(), price(1.10)).unwrap();
        let complete = account.cash_value(&fx).unwrap();
        assert_eq!(complete.total, money(1_110.0));
        assert!(complete.is_complete());
    }

    #[test]
    fn collateral_applies_haircuts_but_debts_count_in_full() {
        let mut haircuts = Haircuts::new();
        haircuts.set(eur(), price(0.9)).unwrap();
        let mut account = Account::new(usd(), Box::new(CashMargin)).with_haircuts(haircuts);
        account.deposit(usd(), money(1_000.0));
        account.deposit(eur(), money(100.0));
        let mut fx = FxBook::new();
        fx.set(eur(), usd(), price(1.0)).unwrap();

        // 1000 USD at full value + 100 EUR haircut to 90.
        assert_eq!(account.collateral(&fx).unwrap().total, money(1_090.0));

        // A negative balance is a debt: no haircut softens it.
        let mut owing = Account::new(usd(), Box::new(CashMargin));
        owing.deposit(usd(), money(-100.0));
        assert_eq!(owing.collateral(&fx).unwrap().total, money(-100.0));
    }

    // -- margin models -----------------------------------------------------

    #[test]
    fn a_cash_account_prepays_and_is_never_liquidatable() {
        let model = CashMargin;
        let instrument = perp();
        assert_eq!(
            model
                .initial_margin(&instrument, qty(2.0), price(100.0))
                .unwrap(),
            money(200.0)
        );
        assert_eq!(
            model
                .maintenance_margin(&instrument, qty(2.0), price(100.0))
                .unwrap(),
            Money::ZERO
        );
    }

    #[test]
    fn linear_margin_is_notional_over_leverage() {
        let model = LinearMargin::from_leverage(10.0).unwrap();
        let instrument = perp();
        // 2 BTC at 100 = 200 notional; 10x leverage needs 20.
        assert_eq!(
            model
                .initial_margin(&instrument, qty(2.0), price(100.0))
                .unwrap(),
            money(20.0)
        );
        // Maintenance is half of initial by default.
        assert_eq!(
            model
                .maintenance_margin(&instrument, qty(2.0), price(100.0))
                .unwrap(),
            money(10.0)
        );
    }

    #[test]
    fn a_short_requires_the_same_margin_as_a_long() {
        let model = LinearMargin::from_leverage(5.0).unwrap();
        let instrument = perp();
        let long = model
            .initial_margin(&instrument, qty(3.0), price(50.0))
            .unwrap();
        let short = model
            .initial_margin(&instrument, qty(-3.0), price(50.0))
            .unwrap();
        assert_eq!(long, short);
    }

    #[test]
    fn maintenance_above_initial_is_refused() {
        // A position that can be opened must not be instantly liquidatable.
        assert!(LinearMargin::new(0.05, 0.10).is_err());
        assert!(LinearMargin::new(0.10, 0.05).is_ok());
        assert!(LinearMargin::from_leverage(0.5).is_err());
    }

    // -- requirement and state ---------------------------------------------

    #[test]
    fn requirement_sums_across_positions_and_names_unmarked_ones() {
        let portfolio = Portfolio::replay(&[fill(Side::Buy, 100.0, 2.0)]).unwrap();
        let model = LinearMargin::from_leverage(10.0).unwrap();

        let with_mark = margin_requirement(
            &model,
            portfolio.open_positions(),
            &instruments(),
            &marks(100.0),
        )
        .unwrap();
        assert_eq!(with_mark.initial, money(20.0));
        assert!(with_mark.unmarked.is_empty());

        let without = margin_requirement(
            &model,
            portfolio.open_positions(),
            &instruments(),
            &BTreeMap::new(),
        )
        .unwrap();
        assert_eq!(without.initial, Money::ZERO);
        assert_eq!(without.unmarked.len(), 1, "reported, not silently skipped");
    }

    #[test]
    fn an_account_above_maintenance_is_not_liquidatable() {
        let account = Account::cash(usd(), money(100.0));
        let fx = FxBook::new();
        let requirement = MarginRequirement {
            initial: money(20.0),
            maintenance: money(10.0),
            unmarked: Vec::new(),
        };
        let state =
            margin_state(&account.collateral(&fx).unwrap(), Money::ZERO, &requirement).unwrap();
        assert_eq!(state.equity, money(100.0));
        assert!(!state.liquidatable);
        assert_eq!(state.available(), money(80.0));
        assert_eq!(state.margin_ratio().unwrap(), 10.0);
    }

    #[test]
    fn losses_that_eat_the_maintenance_buffer_trigger_liquidation() {
        let account = Account::cash(usd(), money(20.0));
        let fx = FxBook::new();
        let requirement = MarginRequirement {
            initial: money(20.0),
            maintenance: money(10.0),
            unmarked: Vec::new(),
        };
        // A 15 loss leaves 5 of equity against a 10 requirement.
        let state = margin_state(
            &account.collateral(&fx).unwrap(),
            money(-15.0),
            &requirement,
        )
        .unwrap();
        assert_eq!(state.equity, money(5.0));
        assert!(state.liquidatable);
        assert!(state.margin_ratio().unwrap() < 1.0);
    }

    #[test]
    fn liquidation_is_never_declared_on_an_incomplete_valuation() {
        // Closing a book on a number known to be partial is worse than
        // waiting for the missing piece.
        let mut account = Account::cash(usd(), money(5.0));
        account.deposit(eur(), money(1_000.0));
        let fx = FxBook::new(); // no EUR rate
        let requirement = MarginRequirement {
            initial: money(20.0),
            maintenance: money(10.0),
            unmarked: Vec::new(),
        };
        let state =
            margin_state(&account.collateral(&fx).unwrap(), Money::ZERO, &requirement).unwrap();
        assert!(state.incomplete);
        assert!(
            !state.liquidatable,
            "an unconvertible balance must not become a liquidation"
        );

        // The same is true of an unmarked position.
        let unmarked = MarginRequirement {
            initial: money(20.0),
            maintenance: money(10.0),
            unmarked: vec![(InstrumentId::new("x").unwrap(), OutcomeId::FIRST)],
        };
        let state = margin_state(
            &Account::cash(usd(), money(1.0))
                .collateral(&FxBook::new())
                .unwrap(),
            Money::ZERO,
            &unmarked,
        )
        .unwrap();
        assert!(state.incomplete);
        assert!(!state.liquidatable);
    }

    #[test]
    fn a_cash_account_has_no_maintenance_and_so_no_liquidation() {
        let account = Account::cash(usd(), money(0.0));
        let requirement = MarginRequirement::default();
        let state = margin_state(
            &account.collateral(&FxBook::new()).unwrap(),
            money(-1_000.0),
            &requirement,
        )
        .unwrap();
        assert!(!state.liquidatable, "prepaid positions cannot be called");
        assert_eq!(state.margin_ratio(), None);
    }
}
