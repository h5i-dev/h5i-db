//! Positions as a fold over fills, and the portfolio as a projection.
//!
//! A [`Position`] holds no authoritative state of its own: it is the result
//! of applying every fill in order, and nothing else. That shape is not an
//! aesthetic preference. It means position state can be *rebuilt* from the
//! `bt_fills` table a run writes, so a stored run is auditable rather than
//! merely reported, and a disagreement between the two is detectable instead
//! of being resolved in favour of whichever was written last.
//!
//! [`Portfolio`] is the same idea one level up: a pure function of the
//! positions and the latest marks.

use std::collections::BTreeMap;

use crate::error::Result;
use crate::instrument::{InstrumentId, OutcomeId};
use crate::order::Fill;
use crate::types::{Money, Price, Qty, notional};

/// The key a position is held under: an instrument *and* an outcome.
pub type PositionKey = (InstrumentId, OutcomeId);

/// A netted position in one instrument outcome.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Position {
    pub instrument: InstrumentId,
    pub outcome: OutcomeId,
    /// Signed: positive is long, negative is short.
    pub quantity: Qty,
    /// Average price of the open quantity. Meaningless when flat, and set
    /// to zero there rather than left stale.
    pub average_price: Price,
    pub realized_pnl: Money,
    pub commissions: Money,
    pub fill_count: u64,
}

impl Position {
    pub fn new(instrument: InstrumentId, outcome: OutcomeId) -> Self {
        Self {
            instrument,
            outcome,
            quantity: Qty::ZERO,
            average_price: Price::ZERO,
            realized_pnl: Money::ZERO,
            commissions: Money::ZERO,
            fill_count: 0,
        }
    }

    pub fn is_flat(&self) -> bool {
        self.quantity.is_zero()
    }

    pub fn is_long(&self) -> bool {
        self.quantity.is_positive()
    }

    pub fn is_short(&self) -> bool {
        self.quantity.is_negative()
    }

    /// Apply a fill.
    ///
    /// Three cases: adding to a position (weighted-average the price),
    /// reducing one (realize the difference), and reversing through zero
    /// (realize the whole old position, then open the remainder at the fill
    /// price). The third is the one that is usually wrong in hand-rolled
    /// implementations, which tend to carry the old average across the flip.
    pub fn apply(&mut self, fill: &Fill) -> Result<()> {
        let signed = fill.quantity.raw() * fill.side.signum();
        let before = self.quantity.raw();
        let after = before + signed;

        if before == 0 || (before > 0) == (signed > 0) {
            // Opening or adding: weighted average of the two legs.
            let existing = (self.average_price.raw() as i128) * (before.abs() as i128);
            let incoming = (fill.price.raw() as i128) * (signed.abs() as i128);
            let total = before.abs() as i128 + signed.abs() as i128;
            self.average_price = if total == 0 {
                Price::ZERO
            } else {
                Price::from_raw(((existing + incoming) / total) as i64)
            };
        } else {
            // Reducing, closing, or reversing.
            let closing = signed.abs().min(before.abs());
            let direction = if before > 0 { 1 } else { -1 };
            // Long: gain is (exit - entry). Short: gain is (entry - exit).
            let per_unit = (fill.price.raw() - self.average_price.raw()) * direction;
            let realized = notional(Price::from_raw(per_unit), Qty::from_raw(closing))?;
            self.realized_pnl = self.realized_pnl.checked_add(realized)?;

            if after == 0 {
                self.average_price = Price::ZERO;
            } else if (after > 0) != (before > 0) {
                // Reversed through zero: the remainder is a new position
                // opened at this fill's price, not at the old average.
                self.average_price = fill.price;
            }
        }

        self.quantity = Qty::from_raw(after);
        self.commissions = self.commissions.checked_add(fill.commission)?;
        self.realized_pnl = self.realized_pnl.checked_sub(fill.commission)?;
        self.fill_count += 1;
        Ok(())
    }

    /// Apply a share split to this position.
    ///
    /// Quantity scales up and basis scales down, leaving the position's
    /// value unchanged. Realised profit is untouched: a split does not
    /// realise anything.
    pub fn apply_split(&mut self, ratio: Price) -> Result<()> {
        if self.is_flat() {
            return Ok(());
        }
        let (quantity, basis) = crate::corporate::CorporateAction::split_position(
            ratio,
            self.quantity,
            self.average_price,
        )?;
        self.quantity = quantity;
        self.average_price = basis;
        Ok(())
    }

    /// Mark-to-market profit on the open quantity at `mark`.
    pub fn unrealized_pnl(&self, mark: Price) -> Result<Money> {
        if self.is_flat() {
            return Ok(Money::ZERO);
        }
        let per_unit = mark.raw() - self.average_price.raw();
        notional(
            Price::from_raw(per_unit),
            Qty::from_raw(self.quantity.raw()),
        )
    }

    /// Signed exposure at `mark`.
    pub fn exposure(&self, mark: Price) -> Result<Money> {
        notional(mark, self.quantity)
    }
}

/// Every position in a run, plus the marks needed to value them.
#[derive(Clone, Default, Debug)]
pub struct Portfolio {
    positions: BTreeMap<PositionKey, Position>,
    marks: BTreeMap<PositionKey, Price>,
}

impl Portfolio {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn apply(&mut self, fill: &Fill) -> Result<()> {
        let key = (fill.instrument.clone(), fill.outcome);
        let position = self
            .positions
            .entry(key)
            .or_insert_with(|| Position::new(fill.instrument.clone(), fill.outcome));
        position.apply(fill)
    }

    /// Rebuild from a fill history. The inverse of writing `bt_fills`, and
    /// what makes a stored run checkable.
    pub fn replay(fills: &[Fill]) -> Result<Self> {
        let mut portfolio = Self::new();
        for fill in fills {
            portfolio.apply(fill)?;
        }
        Ok(portfolio)
    }

    /// Apply a split to every outcome of one instrument.
    pub fn apply_split(&mut self, instrument: &InstrumentId, ratio: Price) -> Result<()> {
        for position in self.positions.values_mut() {
            if &position.instrument == instrument {
                position.apply_split(ratio)?;
            }
        }
        Ok(())
    }

    /// Cash owed on a dividend: positive to a long, negative from a short.
    pub fn dividend_due(&self, instrument: &InstrumentId, per_share: Money) -> Result<Money> {
        let mut total = Money::ZERO;
        for position in self.positions.values() {
            if &position.instrument != instrument || position.is_flat() {
                continue;
            }
            let due = crate::types::notional(Price::from_raw(per_share.raw()), position.quantity)?;
            total = total.checked_add(due)?;
        }
        Ok(total)
    }

    pub fn mark(&mut self, instrument: &InstrumentId, outcome: OutcomeId, price: Price) {
        self.marks.insert((instrument.clone(), outcome), price);
    }

    pub fn position(&self, instrument: &InstrumentId, outcome: OutcomeId) -> Option<&Position> {
        self.positions.get(&(instrument.clone(), outcome))
    }

    /// Positions in key order, so any iteration is deterministic.
    pub fn positions(&self) -> impl Iterator<Item = &Position> {
        self.positions.values()
    }

    pub fn open_positions(&self) -> impl Iterator<Item = &Position> {
        self.positions.values().filter(|p| !p.is_flat())
    }

    pub fn realized_pnl(&self) -> Result<Money> {
        let mut total = Money::ZERO;
        for position in self.positions.values() {
            total = total.checked_add(position.realized_pnl)?;
        }
        Ok(total)
    }

    pub fn commissions(&self) -> Result<Money> {
        let mut total = Money::ZERO;
        for position in self.positions.values() {
            total = total.checked_add(position.commissions)?;
        }
        Ok(total)
    }

    /// Unrealized profit across marked positions.
    ///
    /// An open position with no mark contributes nothing and is reported by
    /// [`Self::unmarked`], rather than being valued at its entry price,
    /// which would quietly report a loss-making book as flat.
    pub fn unrealized_pnl(&self) -> Result<Money> {
        let mut total = Money::ZERO;
        for position in self.positions.values() {
            if let Some(mark) = self
                .marks
                .get(&(position.instrument.clone(), position.outcome))
            {
                total = total.checked_add(position.unrealized_pnl(*mark)?)?;
            }
        }
        Ok(total)
    }

    /// Open positions with no mark to value them at.
    pub fn unmarked(&self) -> Vec<&Position> {
        self.positions
            .values()
            .filter(|p| {
                !p.is_flat() && !self.marks.contains_key(&(p.instrument.clone(), p.outcome))
            })
            .collect()
    }

    /// Sum of absolute exposures.
    pub fn gross_exposure(&self) -> Result<Money> {
        let mut total = Money::ZERO;
        for position in self.positions.values() {
            if let Some(mark) = self
                .marks
                .get(&(position.instrument.clone(), position.outcome))
            {
                total = total.checked_add(position.exposure(*mark)?.abs())?;
            }
        }
        Ok(total)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::order::OrderId;
    use crate::types::{Side, UnixNanos};

    fn fill(side: Side, price: f64, quantity: f64, commission: f64) -> Fill {
        Fill {
            order_id: OrderId(1),
            instrument: InstrumentId::new("m").unwrap(),
            outcome: OutcomeId::FIRST,
            side,
            price: Price::from_f64(price).unwrap(),
            quantity: Qty::from_f64(quantity).unwrap(),
            commission: Money::from_f64(commission).unwrap(),
            is_taker: true,
            ts: UnixNanos::new(0),
            tag: None,
        }
    }

    fn position() -> Position {
        Position::new(InstrumentId::new("m").unwrap(), OutcomeId::FIRST)
    }

    fn money(value: f64) -> Money {
        Money::from_f64(value).unwrap()
    }

    #[test]
    fn opening_sets_the_average_to_the_fill_price() {
        let mut p = position();
        p.apply(&fill(Side::Buy, 0.40, 100.0, 0.0)).unwrap();
        assert!(p.is_long());
        assert_eq!(p.average_price, Price::from_f64(0.40).unwrap());
        assert_eq!(p.realized_pnl, Money::ZERO);
    }

    #[test]
    fn adding_weights_the_average() {
        let mut p = position();
        p.apply(&fill(Side::Buy, 0.40, 100.0, 0.0)).unwrap();
        p.apply(&fill(Side::Buy, 0.50, 100.0, 0.0)).unwrap();
        assert_eq!(p.quantity, Qty::from_f64(200.0).unwrap());
        assert_eq!(p.average_price, Price::from_f64(0.45).unwrap());
        assert_eq!(p.realized_pnl, Money::ZERO, "adding realizes nothing");
    }

    #[test]
    fn reducing_a_long_realizes_the_difference() {
        let mut p = position();
        p.apply(&fill(Side::Buy, 0.40, 100.0, 0.0)).unwrap();
        p.apply(&fill(Side::Sell, 0.50, 40.0, 0.0)).unwrap();
        assert_eq!(p.quantity, Qty::from_f64(60.0).unwrap());
        // 40 contracts * 0.10 gain
        assert_eq!(p.realized_pnl, money(4.0));
        assert_eq!(
            p.average_price,
            Price::from_f64(0.40).unwrap(),
            "reducing does not move the average"
        );
    }

    #[test]
    fn reducing_a_short_realizes_the_opposite_sign() {
        let mut p = position();
        p.apply(&fill(Side::Sell, 0.60, 100.0, 0.0)).unwrap();
        assert!(p.is_short());
        p.apply(&fill(Side::Buy, 0.50, 100.0, 0.0)).unwrap();
        assert!(p.is_flat());
        // Sold at 0.60, bought back at 0.50: a gain for a short.
        assert_eq!(p.realized_pnl, money(10.0));
        assert_eq!(
            p.average_price,
            Price::ZERO,
            "a flat position has no average"
        );
    }

    #[test]
    fn reversing_through_zero_reprices_the_remainder() {
        // The case hand-rolled implementations usually get wrong: the new
        // position must be priced at the fill, not at the old average.
        let mut p = position();
        p.apply(&fill(Side::Buy, 0.40, 100.0, 0.0)).unwrap();
        p.apply(&fill(Side::Sell, 0.50, 150.0, 0.0)).unwrap();
        assert!(p.is_short());
        assert_eq!(p.quantity, Qty::from_f64(-50.0).unwrap());
        // Closed 100 at +0.10, and the remaining short opened at 0.50.
        assert_eq!(p.realized_pnl, money(10.0));
        assert_eq!(p.average_price, Price::from_f64(0.50).unwrap());
    }

    #[test]
    fn commissions_reduce_realized_pnl_and_are_tracked_separately() {
        let mut p = position();
        p.apply(&fill(Side::Buy, 0.40, 100.0, 0.5)).unwrap();
        p.apply(&fill(Side::Sell, 0.50, 100.0, 0.5)).unwrap();
        assert_eq!(p.commissions, money(1.0));
        assert_eq!(p.realized_pnl, money(9.0), "10 gross less 1 of commission");
    }

    #[test]
    fn a_round_trip_at_the_same_price_realizes_nothing() {
        let mut p = position();
        p.apply(&fill(Side::Buy, 0.37, 250.0, 0.0)).unwrap();
        p.apply(&fill(Side::Sell, 0.37, 250.0, 0.0)).unwrap();
        assert!(p.is_flat());
        assert_eq!(p.realized_pnl, Money::ZERO);
    }

    #[test]
    fn unrealized_pnl_needs_an_open_position() {
        let mut p = position();
        assert_eq!(
            p.unrealized_pnl(Price::from_f64(0.9).unwrap()).unwrap(),
            Money::ZERO
        );
        p.apply(&fill(Side::Buy, 0.40, 100.0, 0.0)).unwrap();
        assert_eq!(
            p.unrealized_pnl(Price::from_f64(0.55).unwrap()).unwrap(),
            money(15.0)
        );
        p.apply(&fill(Side::Sell, 0.40, 200.0, 0.0)).unwrap();
        assert_eq!(
            p.unrealized_pnl(Price::from_f64(0.30).unwrap()).unwrap(),
            money(10.0),
            "a short gains as the mark falls"
        );
    }

    #[test]
    fn a_portfolio_is_a_fold_over_fills() {
        let fills = vec![
            fill(Side::Buy, 0.40, 100.0, 0.1),
            fill(Side::Sell, 0.50, 40.0, 0.1),
            fill(Side::Buy, 0.45, 10.0, 0.1),
        ];
        let replayed = Portfolio::replay(&fills).unwrap();

        let mut incremental = Portfolio::new();
        for f in &fills {
            incremental.apply(f).unwrap();
        }
        // Rebuilding from the fill log must equal applying them live: this
        // is what makes a stored run auditable.
        assert_eq!(
            replayed.realized_pnl().unwrap(),
            incremental.realized_pnl().unwrap()
        );
        assert_eq!(
            replayed.position(&InstrumentId::new("m").unwrap(), OutcomeId::FIRST),
            incremental.position(&InstrumentId::new("m").unwrap(), OutcomeId::FIRST)
        );
    }

    #[test]
    fn unmarked_positions_are_reported_not_valued_at_entry() {
        let mut portfolio = Portfolio::new();
        portfolio.apply(&fill(Side::Buy, 0.40, 100.0, 0.0)).unwrap();
        assert_eq!(portfolio.unrealized_pnl().unwrap(), Money::ZERO);
        assert_eq!(portfolio.unmarked().len(), 1);

        portfolio.mark(
            &InstrumentId::new("m").unwrap(),
            OutcomeId::FIRST,
            Price::from_f64(0.5).unwrap(),
        );
        assert_eq!(portfolio.unrealized_pnl().unwrap(), money(10.0));
        assert!(portfolio.unmarked().is_empty());
        assert_eq!(portfolio.gross_exposure().unwrap(), money(50.0));
    }

    #[test]
    fn positions_are_keyed_by_outcome_not_just_instrument() {
        let mut portfolio = Portfolio::new();
        let mut yes = fill(Side::Buy, 0.40, 10.0, 0.0);
        let mut no = fill(Side::Buy, 0.60, 10.0, 0.0);
        no.outcome = OutcomeId(1);
        yes.outcome = OutcomeId(0);
        portfolio.apply(&yes).unwrap();
        portfolio.apply(&no).unwrap();
        assert_eq!(portfolio.open_positions().count(), 2);
        assert_eq!(
            portfolio
                .position(&InstrumentId::new("m").unwrap(), OutcomeId(1))
                .unwrap()
                .average_price,
            Price::from_f64(0.60).unwrap()
        );
    }
}
