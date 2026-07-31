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
use crate::instrument::{Instrument, InstrumentId, OutcomeId, uniform_prices};
use crate::position::Portfolio;
use crate::types::{Money, Price, Qty, Raw, SCALE, UnixNanos, notional};

/// What each outcome of a resolved market pays per contract.
///
/// One winner is the common case and the only one most backtesters model,
/// which is why the others are here. A market can be **voided** -- the
/// question turned out to be unanswerable, the venue refunds a complete set
/// at cost, and every outcome pays the same. It can be **split**: a
/// scalar or range market settles between the bounds, and Polymarket's
/// 50-50 outcome is the two-way case of exactly that.
///
/// Forcing those into a winner is not a rounding error, it is a sign error.
/// Booking a voided binary as `winner = YES` turns a refund into a doubling
/// for one side and a total loss for the other, and both are wrong by the
/// full notional.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Payout {
    /// Exactly one outcome pays one; every other pays nothing.
    Winner(OutcomeId),
    /// An explicit payout per outcome, in outcome order, summing to one.
    Split(Vec<Price>),
    /// The market was voided and a complete set refunded at cost, so every
    /// outcome pays the same fraction of one.
    Void { outcomes: u16 },
}

impl Payout {
    /// The payouts as a vector, when the arity is known from the payout
    /// itself. `Winner` alone does not know how many outcomes exist.
    fn as_prices(&self) -> Option<Vec<Price>> {
        match self {
            Payout::Winner(_) => None,
            Payout::Split(prices) => Some(prices.clone()),
            Payout::Void { outcomes } => uniform_prices(*outcomes).ok(),
        }
    }

    /// Whether this payout distinguishes between outcomes at all.
    ///
    /// A void pays every outcome the same, so a forecast of any probability
    /// scores identically against it. Feeding those into a Brier score does
    /// not measure a forecaster, it dilutes whatever the rest of the sample
    /// measured, which is why
    /// [`crate::run::RunReport::calibration_samples`] drops them.
    pub fn is_informative(&self) -> bool {
        match self {
            Payout::Winner(_) => true,
            Payout::Void { .. } => false,
            Payout::Split(prices) => {
                let first = prices.first().copied().unwrap_or(Price::ZERO);
                prices.iter().any(|price| *price != first)
            }
        }
    }
}

/// How a market resolved.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Resolution {
    pub instrument: InstrumentId,
    /// What each outcome pays per contract.
    pub payout: Payout,
    /// The first instant this result could have been observed. Usually the
    /// venue's resolution timestamp, not the event's own date.
    pub observable_at: UnixNanos,
}

impl Resolution {
    /// The single-winner case, which is what most markets do.
    pub fn new(instrument: InstrumentId, winner: OutcomeId, observable_at: UnixNanos) -> Self {
        Self {
            instrument,
            payout: Payout::Winner(winner),
            observable_at,
        }
    }

    /// An explicit payout per outcome, in outcome order.
    ///
    /// The payouts must sum to exactly one. That is not pedantry about
    /// rounding: a set that pays more than a dollar mints money at
    /// settlement and one that pays less burns it, and either turns the
    /// run's final cash into a number no ledger could produce.
    pub fn split(
        instrument: InstrumentId,
        payouts: Vec<Price>,
        observable_at: UnixNanos,
    ) -> Result<Self> {
        if payouts.len() < 2 {
            return Err(BacktestError::invalid(
                "a split resolution needs a payout for every outcome",
            ));
        }
        if payouts
            .iter()
            .any(|price| price.is_negative() || *price > Price::PROBABILITY_MAX)
        {
            return Err(BacktestError::invalid(format!(
                "every payout for {instrument} must lie in [0, 1]"
            )));
        }
        let total: Raw = payouts.iter().map(|price| price.raw()).sum();
        if total != SCALE {
            return Err(BacktestError::invalid(format!(
                "the payouts for {instrument} sum to {} rather than 1; a \
                 settlement that does not conserve a complete set mints or \
                 burns cash",
                Price::from_raw(total)
            )));
        }
        Ok(Self {
            instrument,
            payout: Payout::Split(payouts),
            observable_at,
        })
    }

    /// A voided market: a complete set refunds at cost, split evenly.
    pub fn void(instrument: InstrumentId, outcomes: u16, observable_at: UnixNanos) -> Result<Self> {
        if outcomes < 2 {
            return Err(BacktestError::invalid(
                "a voided market needs at least two outcomes",
            ));
        }
        Ok(Self {
            instrument,
            payout: Payout::Void { outcomes },
            observable_at,
        })
    }

    /// The outcome that took the whole dollar, when exactly one did.
    pub fn winner(&self) -> Option<OutcomeId> {
        match &self.payout {
            Payout::Winner(outcome) => Some(*outcome),
            Payout::Void { .. } => None,
            Payout::Split(prices) => {
                let mut winner = None;
                for (index, price) in prices.iter().enumerate() {
                    if price.raw() == SCALE {
                        winner = Some(OutcomeId(index as u16));
                    } else if !price.is_zero() {
                        return None;
                    }
                }
                winner
            }
        }
    }

    /// What one contract on `outcome` pays.
    ///
    /// An outcome outside the resolved set pays nothing rather than
    /// erroring, so a position in an outcome the resolution never mentions
    /// settles worthless rather than stalling the whole report. Arity
    /// disagreements are caught up front by [`Resolution::validate`], which
    /// is where a mismatch is a data problem worth naming.
    pub fn settlement_price(&self, outcome: OutcomeId) -> Price {
        match &self.payout {
            Payout::Winner(winner) => {
                if outcome == *winner {
                    Price::from_raw(SCALE)
                } else {
                    Price::ZERO
                }
            }
            Payout::Split(prices) => prices.get(outcome.index()).copied().unwrap_or(Price::ZERO),
            Payout::Void { outcomes } => uniform_prices(*outcomes)
                .ok()
                .and_then(|prices| prices.get(outcome.index()).copied())
                .unwrap_or(Price::ZERO),
        }
    }

    /// Check this resolution against the market it claims to resolve.
    ///
    /// A payout vector of the wrong length, or a winner that is not an
    /// outcome of the market, is a vendor-mapping mistake. Settling on it
    /// would silently pay the wrong leg, so it is refused where the
    /// instrument is known.
    pub fn validate(&self, instrument: &Instrument) -> Result<()> {
        let expected = instrument.outcome_count();
        match &self.payout {
            Payout::Winner(winner) => instrument.check_outcome(*winner),
            Payout::Split(prices) => {
                if prices.len() != expected as usize {
                    return Err(BacktestError::invalid(format!(
                        "{} has {expected} outcomes but its resolution supplies \
                         {} payouts",
                        instrument.id,
                        prices.len()
                    )));
                }
                Ok(())
            }
            Payout::Void { outcomes } => {
                if *outcomes != expected {
                    return Err(BacktestError::invalid(format!(
                        "{} has {expected} outcomes but its void resolution \
                         names {outcomes}",
                        instrument.id
                    )));
                }
                Ok(())
            }
        }
    }

    /// Payouts as a vector when the resolution knows its own arity.
    pub fn payouts(&self) -> Option<Vec<Price>> {
        self.payout.as_prices()
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

/// Check every resolution against the market it resolves.
///
/// Run before settlement, where the instruments are known. A resolution for
/// a market this run never loaded is not an error -- data is routinely
/// broader than a window -- but one that disagrees with a market it *does*
/// name is, because settling on it pays the wrong leg.
pub fn validate_resolutions(
    resolutions: &[Resolution],
    instruments: &crate::instrument::InstrumentSet,
) -> Result<()> {
    for resolution in resolutions {
        if let Ok(instrument) = instruments.get(&resolution.instrument) {
            resolution.validate(instrument)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::order::{Fill, OrderId};
    use crate::types::Side;

    use crate::instrument::Instrument;

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
        let total: Raw = report.settled.iter().map(|s| s.settled_pnl.raw()).sum();
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
    fn a_voided_binary_refunds_a_complete_set_at_cost() {
        // The case a winner-only model gets wrong by the full notional:
        // 100 YES bought at 0.40, market voided, each side pays 0.50.
        let portfolio = open_long();
        let resolution = Resolution::void(instrument(), 2, ts(1_000)).unwrap();
        let report = settle(&portfolio, &[resolution], Some(ts(2_000)), &BTreeMap::new()).unwrap();
        assert_eq!(report.settled[0].settled_pnl, money(10.0));
    }

    #[test]
    fn a_void_conserves_a_complete_set_however_many_outcomes() {
        // Three-way void: 1/3 each, and a set bought for exactly one comes
        // back for exactly one despite a third being inexact in fixed point.
        let portfolio = Portfolio::replay(&[
            fill(OutcomeId(0), Side::Buy, 0.50, 30.0),
            fill(OutcomeId(1), Side::Buy, 0.30, 30.0),
            fill(OutcomeId(2), Side::Buy, 0.20, 30.0),
        ])
        .unwrap();
        let resolution = Resolution::void(instrument(), 3, ts(1)).unwrap();
        let report = settle(&portfolio, &[resolution], Some(ts(2)), &BTreeMap::new()).unwrap();
        let total: Raw = report.settled.iter().map(|s| s.settled_pnl.raw()).sum();
        assert_eq!(
            Money::from_raw(total),
            Money::ZERO,
            "a refunded set must return exactly what it cost"
        );
    }

    #[test]
    fn a_scalar_market_settles_between_the_bounds() {
        let portfolio = open_long();
        let resolution = Resolution::split(
            instrument(),
            vec![
                Price::from_f64(0.70).unwrap(),
                Price::from_f64(0.30).unwrap(),
            ],
            ts(1),
        )
        .unwrap();
        let report = settle(&portfolio, &[resolution], Some(ts(2)), &BTreeMap::new()).unwrap();
        // 100 bought at 0.40, settled at 0.70.
        assert_eq!(report.settled[0].settled_pnl, money(30.0));
    }

    #[test]
    fn a_split_that_does_not_sum_to_one_is_refused() {
        let short = Resolution::split(
            instrument(),
            vec![
                Price::from_f64(0.70).unwrap(),
                Price::from_f64(0.20).unwrap(),
            ],
            ts(1),
        );
        assert!(short.is_err(), "0.9 of a dollar would burn cash");
        let long = Resolution::split(
            instrument(),
            vec![
                Price::from_f64(0.70).unwrap(),
                Price::from_f64(0.40).unwrap(),
            ],
            ts(1),
        );
        assert!(long.is_err(), "1.1 of a dollar would mint cash");
        assert!(Resolution::split(instrument(), vec![Price::from_raw(SCALE)], ts(1)).is_err());
    }

    #[test]
    fn a_one_hot_split_reads_back_as_a_winner() {
        let resolution = Resolution::split(
            instrument(),
            vec![Price::ZERO, Price::from_raw(SCALE)],
            ts(1),
        )
        .unwrap();
        assert_eq!(resolution.winner(), Some(OutcomeId(1)));
        assert_eq!(
            Resolution::new(instrument(), OutcomeId(1), ts(1)).winner(),
            Some(OutcomeId(1))
        );
        assert_eq!(
            Resolution::void(instrument(), 2, ts(1)).unwrap().winner(),
            None
        );
        let scalar = Resolution::split(
            instrument(),
            vec![
                Price::from_f64(0.70).unwrap(),
                Price::from_f64(0.30).unwrap(),
            ],
            ts(1),
        )
        .unwrap();
        assert_eq!(scalar.winner(), None, "a scalar settlement has no winner");
    }

    #[test]
    fn a_void_is_not_scored_as_a_forecast() {
        // The reference implementation's lesson: every forecast scores the
        // same against a 50-50, so including them flatters or dilutes the
        // sample without measuring anything.
        assert!(!Payout::Void { outcomes: 2 }.is_informative());
        assert!(Payout::Winner(OutcomeId::FIRST).is_informative());
        assert!(
            !Payout::Split(vec![
                Price::from_f64(0.5).unwrap(),
                Price::from_f64(0.5).unwrap()
            ])
            .is_informative()
        );
        assert!(
            Payout::Split(vec![
                Price::from_f64(0.7).unwrap(),
                Price::from_f64(0.3).unwrap()
            ])
            .is_informative()
        );
    }

    #[test]
    fn a_resolution_that_disagrees_with_its_market_is_refused() {
        let market =
            Instrument::prediction_market("market", "v", vec!["A".into(), "B".into(), "C".into()])
                .unwrap();
        let mut set = crate::instrument::InstrumentSet::new();
        set.insert(market).unwrap();

        // A winner the market does not have.
        let stray = Resolution::new(instrument(), OutcomeId(7), ts(1));
        assert!(validate_resolutions(&[stray], &set).is_err());

        // A payout vector of the wrong arity.
        let wrong_arity = Resolution::split(
            instrument(),
            vec![Price::from_f64(0.6).unwrap(), Price::from_f64(0.4).unwrap()],
            ts(1),
        )
        .unwrap();
        assert!(validate_resolutions(&[wrong_arity], &set).is_err());

        let void_arity = Resolution::void(instrument(), 2, ts(1)).unwrap();
        assert!(validate_resolutions(&[void_arity], &set).is_err());

        let good = Resolution::new(instrument(), OutcomeId(2), ts(1));
        assert!(validate_resolutions(&[good], &set).is_ok());

        // A resolution for a market this run never loaded is not an error.
        let elsewhere = Resolution::new(InstrumentId::new("other").unwrap(), OutcomeId(9), ts(1));
        assert!(validate_resolutions(&[elsewhere], &set).is_ok());
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
