//! Settlement, applied after the run and only when it was observable.
//!
//! Settlement is **not** an event in the replay stream. It is a policy
//! applied to the result once the run has finished, and it is gated on one
//! question: did the run actually reach the point where the resolution
//! could have been seen?
//!
//! That gate is the whole design. A three-day replay of a six-month market
//! ends with an open position; marking it to the eventual winner books a
//! profit that nobody trading those three days could have collected. So
//! settlement applies only when `simulated_through >= observable_at`, and
//! otherwise the mark-to-market result stands and the report says why.
//!
//! Both numbers survive. `market_exit_pnl` is what the position was worth
//! at the last mark, `settled_pnl` is what it became at resolution, and the
//! difference is reported as an explicit adjustment rather than folded in
//! silently. An adjustment nobody can point at is one nobody can check.
//!
//! Resolutions never reach a strategy. They are held here, on the far side
//! of the run, precisely because a versioned store makes the leak easy: the
//! latest row for a settled market knows the answer, so the only safe place
//! for it is somewhere the strategy cannot reach.

use std::collections::BTreeMap;

use crate::error::{BacktestError, Result};
use crate::instrument::{InstrumentId, OutcomeId};
use crate::position::Portfolio;
use crate::types::{Money, Price, Qty, SCALE, UnixNanos, notional};

/// How a market resolved.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Resolution {
    pub instrument: InstrumentId,
    /// The outcome that paid out.
    pub winner: OutcomeId,
    /// The first instant this result could have been observed. Usually the
    /// venue's resolution timestamp, not the event's own date.
    pub observable_at: UnixNanos,
}

impl Resolution {
    pub fn new(instrument: InstrumentId, winner: OutcomeId, observable_at: UnixNanos) -> Self {
        Self {
            instrument,
            winner,
            observable_at,
        }
    }

    /// What one contract on `outcome` pays: 1 for the winner, 0 otherwise.
    pub fn settlement_price(&self, outcome: OutcomeId) -> Price {
        if outcome == self.winner {
            Price::from_raw(SCALE)
        } else {
            Price::ZERO
        }
    }
}

/// What settlement did to one position.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct PositionSettlement {
    pub instrument: InstrumentId,
    pub outcome: OutcomeId,
    pub quantity: Qty,
    /// Value of the open position at the last mark, if there was one.
    pub market_exit_pnl: Option<Money>,
    /// Value at the resolution price.
    pub settled_pnl: Money,
    /// `settled_pnl - market_exit_pnl`, the explicit delta.
    pub adjustment: Money,
}

/// The result of applying settlement to a finished run.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct SettlementReport {
    pub settled: Vec<PositionSettlement>,
    /// Positions left open because settlement was not observable, each with
    /// the reason.
    pub unsettled: Vec<(InstrumentId, OutcomeId, String)>,
    /// Total cash adjustment settlement introduced.
    pub total_adjustment: Money,
}

impl SettlementReport {
    pub fn was_applied(&self) -> bool {
        !self.settled.is_empty()
    }

    /// Warnings a report must carry, so a run whose settlement was skipped
    /// cannot be read as one where nothing was open.
    pub fn warnings(&self) -> Vec<String> {
        self.unsettled
            .iter()
            .map(|(instrument, outcome, reason)| {
                format!("{instrument} outcome {outcome} left unsettled: {reason}")
            })
            .collect()
    }
}

/// Apply settlement to the open positions of a finished run.
///
/// `simulated_through` is the last instant the run actually replayed --
/// not the window that was requested. A run that stopped early because its
/// data ran out settles nothing that happened after it stopped.
pub fn settle(
    portfolio: &Portfolio,
    resolutions: &[Resolution],
    simulated_through: Option<UnixNanos>,
    marks: &BTreeMap<(InstrumentId, OutcomeId), Price>,
) -> Result<SettlementReport> {
    let mut by_instrument: BTreeMap<&InstrumentId, &Resolution> = BTreeMap::new();
    for resolution in resolutions {
        if by_instrument
            .insert(&resolution.instrument, resolution)
            .is_some()
        {
            return Err(BacktestError::invalid(format!(
                "two resolutions supplied for {}",
                resolution.instrument
            )));
        }
    }

    let mut report = SettlementReport::default();
    for position in portfolio.open_positions() {
        let key = (position.instrument.clone(), position.outcome);
        let Some(resolution) = by_instrument.get(&position.instrument) else {
            report.unsettled.push((
                position.instrument.clone(),
                position.outcome,
                "no resolution is known for this market".to_string(),
            ));
            continue;
        };

        // The gate. Without a simulated-through the run cannot claim to
        // have reached anything, so it settles nothing.
        let Some(through) = simulated_through else {
            report.unsettled.push((
                position.instrument.clone(),
                position.outcome,
                "the run did not record how far it simulated".to_string(),
            ));
            continue;
        };
        if through < resolution.observable_at {
            report.unsettled.push((
                position.instrument.clone(),
                position.outcome,
                format!(
                    "the run simulated through {through} but this market \
                     resolved at {}; booking it would be profit nobody \
                     trading this window could have collected",
                    resolution.observable_at
                ),
            ));
            continue;
        }

        let settlement_price = resolution.settlement_price(position.outcome);
        let settled_pnl = position.unrealized_pnl(settlement_price)?;
        let market_exit_pnl = marks
            .get(&key)
            .map(|mark| position.unrealized_pnl(*mark))
            .transpose()?;
        let adjustment = match market_exit_pnl {
            Some(exit) => settled_pnl.checked_sub(exit)?,
            None => settled_pnl,
        };
        report.total_adjustment = report.total_adjustment.checked_add(adjustment)?;
        report.settled.push(PositionSettlement {
            instrument: position.instrument.clone(),
            outcome: position.outcome,
            quantity: position.quantity,
            market_exit_pnl,
            settled_pnl,
            adjustment,
        });
    }
    Ok(report)
}

/// Cash a settled position pays out: `quantity * settlement price`.
pub fn settlement_proceeds(quantity: Qty, settlement_price: Price) -> Result<Money> {
    notional(settlement_price, quantity)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::order::{Fill, OrderId};
    use crate::types::Side;

    fn instrument() -> InstrumentId {
        InstrumentId::new("market").unwrap()
    }

    fn fill(outcome: OutcomeId, side: Side, price: f64, quantity: f64) -> Fill {
        Fill {
            order_id: OrderId(1),
            instrument: instrument(),
            outcome,
            side,
            price: Price::from_f64(price).unwrap(),
            quantity: Qty::from_f64(quantity).unwrap(),
            commission: Money::ZERO,
            is_taker: true,
            ts: UnixNanos::new(0),
            tag: None,
        }
    }

    fn ts(value: i64) -> UnixNanos {
        UnixNanos::new(value)
    }

    fn money(value: f64) -> Money {
        Money::from_f64(value).unwrap()
    }

    /// Long 100 YES at 0.40.
    fn open_long() -> Portfolio {
        Portfolio::replay(&[fill(OutcomeId::FIRST, Side::Buy, 0.40, 100.0)]).unwrap()
    }

    #[test]
    fn a_winning_position_settles_at_one() {
        let portfolio = open_long();
        let resolution = Resolution::new(instrument(), OutcomeId::FIRST, ts(1_000));
        let report = settle(&portfolio, &[resolution], Some(ts(2_000)), &BTreeMap::new()).unwrap();
        assert!(report.was_applied());
        assert_eq!(report.settled.len(), 1);
        // 100 contracts bought at 0.40, worth 1.00 each: +60.
        assert_eq!(report.settled[0].settled_pnl, money(60.0));
        assert!(report.unsettled.is_empty());
    }

    #[test]
    fn a_losing_position_settles_at_zero() {
        let portfolio = open_long();
        // Outcome 1 won; the position is in outcome 0.
        let resolution = Resolution::new(instrument(), OutcomeId(1), ts(1_000));
        let report = settle(&portfolio, &[resolution], Some(ts(2_000)), &BTreeMap::new()).unwrap();
        assert_eq!(report.settled[0].settled_pnl, money(-40.0));
    }

    #[test]
    fn settlement_is_refused_when_the_run_stopped_before_resolution() {
        // The headline guarantee: a short replay of a long market must not
        // book resolution profit.
        let portfolio = open_long();
        let resolution = Resolution::new(instrument(), OutcomeId::FIRST, ts(10_000));
        let report = settle(&portfolio, &[resolution], Some(ts(2_000)), &BTreeMap::new()).unwrap();
        assert!(!report.was_applied());
        assert_eq!(report.total_adjustment, Money::ZERO);
        assert_eq!(report.unsettled.len(), 1);
        assert!(report.unsettled[0].2.contains("resolved at 10000"));
        assert!(!report.warnings().is_empty());
    }

    #[test]
    fn settlement_at_exactly_the_observable_instant_counts() {
        let portfolio = open_long();
        let resolution = Resolution::new(instrument(), OutcomeId::FIRST, ts(5_000));
        let report = settle(&portfolio, &[resolution], Some(ts(5_000)), &BTreeMap::new()).unwrap();
        assert!(report.was_applied());
    }

    #[test]
    fn a_run_that_does_not_know_how_far_it_got_settles_nothing() {
        let portfolio = open_long();
        let resolution = Resolution::new(instrument(), OutcomeId::FIRST, ts(1));
        let report = settle(&portfolio, &[resolution], None, &BTreeMap::new()).unwrap();
        assert!(!report.was_applied());
        assert!(report.unsettled[0].2.contains("how far it simulated"));
    }

    #[test]
    fn the_adjustment_against_the_last_mark_is_explicit() {
        let portfolio = open_long();
        let mut marks = BTreeMap::new();
        // Last seen at 0.90: the position was already showing +50.
        marks.insert(
            (instrument(), OutcomeId::FIRST),
            Price::from_f64(0.90).unwrap(),
        );
        let resolution = Resolution::new(instrument(), OutcomeId::FIRST, ts(1_000));
        let report = settle(&portfolio, &[resolution], Some(ts(2_000)), &marks).unwrap();
        let settled = &report.settled[0];
        assert_eq!(settled.market_exit_pnl, Some(money(50.0)));
        assert_eq!(settled.settled_pnl, money(60.0));
        assert_eq!(
            settled.adjustment,
            money(10.0),
            "only the last leg is attributed to settlement"
        );
        assert_eq!(report.total_adjustment, money(10.0));
    }

    #[test]
    fn a_market_with_no_resolution_is_left_open_and_named() {
        let portfolio = open_long();
        let report = settle(&portfolio, &[], Some(ts(9_999)), &BTreeMap::new()).unwrap();
        assert!(!report.was_applied());
        assert!(report.unsettled[0].2.contains("no resolution"));
    }

    #[test]
    fn flat_positions_are_not_settled() {
        let portfolio = Portfolio::replay(&[
            fill(OutcomeId::FIRST, Side::Buy, 0.40, 100.0),
            fill(OutcomeId::FIRST, Side::Sell, 0.60, 100.0),
        ])
        .unwrap();
        let resolution = Resolution::new(instrument(), OutcomeId::FIRST, ts(1));
        let report = settle(&portfolio, &[resolution], Some(ts(2)), &BTreeMap::new()).unwrap();
        assert!(report.settled.is_empty());
        assert!(report.unsettled.is_empty());
    }

    #[test]
    fn a_short_settles_the_other_way() {
        let portfolio =
            Portfolio::replay(&[fill(OutcomeId::FIRST, Side::Sell, 0.40, 100.0)]).unwrap();
        let resolution = Resolution::new(instrument(), OutcomeId::FIRST, ts(1));
        let report = settle(&portfolio, &[resolution], Some(ts(2)), &BTreeMap::new()).unwrap();
        // Sold at 0.40 what settled at 1.00: a 60 loss.
        assert_eq!(report.settled[0].settled_pnl, money(-60.0));
    }

    #[test]
    fn every_outcome_of_a_categorical_market_settles_together() {
        let portfolio = Portfolio::replay(&[
            fill(OutcomeId(0), Side::Buy, 0.50, 10.0),
            fill(OutcomeId(1), Side::Buy, 0.30, 10.0),
            fill(OutcomeId(2), Side::Buy, 0.20, 10.0),
        ])
        .unwrap();
        let resolution = Resolution::new(instrument(), OutcomeId(1), ts(1));
        let report = settle(&portfolio, &[resolution], Some(ts(2)), &BTreeMap::new()).unwrap();
        assert_eq!(report.settled.len(), 3);
        let total: i64 = report.settled.iter().map(|s| s.settled_pnl.raw()).sum();
        // Paid 0.50 + 0.30 + 0.20 = 1.00 per set, received 1.00: flat.
        assert_eq!(Money::from_raw(total), Money::ZERO);
    }

    #[test]
    fn duplicate_resolutions_are_refused() {
        let portfolio = open_long();
        let resolutions = vec![
            Resolution::new(instrument(), OutcomeId::FIRST, ts(1)),
            Resolution::new(instrument(), OutcomeId(1), ts(1)),
        ];
        assert!(settle(&portfolio, &resolutions, Some(ts(2)), &BTreeMap::new()).is_err());
    }

    #[test]
    fn proceeds_are_quantity_times_the_settlement_price() {
        assert_eq!(
            settlement_proceeds(Qty::from_f64(100.0).unwrap(), Price::from_raw(SCALE)).unwrap(),
            money(100.0)
        );
        assert_eq!(
            settlement_proceeds(Qty::from_f64(100.0).unwrap(), Price::ZERO).unwrap(),
            Money::ZERO
        );
    }
}
