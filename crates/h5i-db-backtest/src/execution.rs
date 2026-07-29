//! The seam between deciding to trade and trading.
//!
//! Every order a strategy produces leaves through this trait. The
//! simulator implements it; a live adapter would implement the same one.
//! That is the whole point: if simulation and production reach the venue
//! through different code, they diverge, and nothing detects it because
//! there is no shared surface to compare.
//!
//! What crosses here is *intent* -- submit, cancel, amend -- never a fill.
//! Fills come back the other way, from the venue, because in production
//! that is the only direction they can come from. A simulator that returns
//! a fill from `submit` is modelling a venue that does not exist, and every
//! strategy written against it learns a control flow that will not survive
//! contact with a real one.

use crate::error::Result;
use crate::order::{Order, OrderId};
use crate::types::{Price, Qty, UnixNanos};

/// One instruction sent to a venue.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ExecutionCommand {
    Submit(Box<Order>),
    Cancel(OrderId),
    Amend {
        id: OrderId,
        quantity: Option<Qty>,
        limit: Option<Price>,
    },
}

impl ExecutionCommand {
    pub fn order_id(&self) -> OrderId {
        match self {
            ExecutionCommand::Submit(order) => order.id,
            ExecutionCommand::Cancel(id) => *id,
            ExecutionCommand::Amend { id, .. } => *id,
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            ExecutionCommand::Submit(_) => "submit",
            ExecutionCommand::Cancel(_) => "cancel",
            ExecutionCommand::Amend { .. } => "amend",
        }
    }
}

/// Where orders go.
///
/// Implemented by the simulator and, in principle, by a live adapter. The
/// trait is deliberately small: anything a venue does *to* an order --
/// accept it, fill it, reject it -- arrives as data on the replay stream
/// rather than as a return value here.
pub trait ExecutionClient: std::fmt::Debug {
    /// A name for the venue, carried into run records.
    fn venue(&self) -> &str;

    /// Send one instruction. Returning `Ok` means the venue accepted the
    /// *instruction*, not that the order filled.
    fn send(&mut self, command: ExecutionCommand, ts: UnixNanos) -> Result<()>;

    /// Instructions sent so far, for reconciliation.
    fn sent(&self) -> usize;
}

/// The simulator's client: it records what was sent, and the engine's own
/// matching does the rest.
///
/// Deliberately not a matching engine itself. Matching needs books,
/// positions and fill models, all of which live in the engine; putting
/// them behind this trait too would mean a live adapter had to pretend to
/// own them.
#[derive(Clone, Debug, Default)]
pub struct SimulatedExecution {
    venue: String,
    log: Vec<(UnixNanos, ExecutionCommand)>,
}

impl SimulatedExecution {
    pub fn new(venue: impl Into<String>) -> Self {
        Self {
            venue: venue.into(),
            log: Vec::new(),
        }
    }

    /// Everything sent, in order.
    pub fn log(&self) -> &[(UnixNanos, ExecutionCommand)] {
        &self.log
    }

    /// The instruction stream as comparable text.
    ///
    /// This is what makes sim-versus-live reconcilable: run the same
    /// strategy against both, and compare these. A difference here is a
    /// difference in what the strategy decided, before any question of how
    /// the venue treated it.
    pub fn fingerprint(&self) -> Vec<String> {
        self.log
            .iter()
            .map(|(ts, command)| match command {
                ExecutionCommand::Submit(order) => format!(
                    "{ts} submit {} {} {} {}",
                    order.instrument,
                    order.outcome,
                    order.side.as_str(),
                    order.quantity
                ),
                ExecutionCommand::Cancel(id) => format!("{ts} cancel {id}"),
                ExecutionCommand::Amend {
                    id,
                    quantity,
                    limit,
                } => format!(
                    "{ts} amend {id} qty={} px={}",
                    quantity.map(|q| q.to_string()).unwrap_or_else(|| "-".into()),
                    limit.map(|p| p.to_string()).unwrap_or_else(|| "-".into())
                ),
            })
            .collect()
    }
}

impl ExecutionClient for SimulatedExecution {
    fn venue(&self) -> &str {
        &self.venue
    }

    fn send(&mut self, command: ExecutionCommand, ts: UnixNanos) -> Result<()> {
        self.log.push((ts, command));
        Ok(())
    }

    fn sent(&self) -> usize {
        self.log.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::instrument::{InstrumentId, OutcomeId};
    use crate::order::{OrderKind, TimeInForce};
    use crate::types::Side;

    fn order(id: u64, quantity: f64) -> Order {
        Order::new(
            OrderId(id),
            InstrumentId::new("m").unwrap(),
            OutcomeId::FIRST,
            Side::Buy,
            OrderKind::Market,
            Qty::from_f64(quantity).unwrap(),
            TimeInForce::ImmediateOrCancel,
            UnixNanos::new(0),
        )
        .unwrap()
    }

    #[test]
    fn a_client_records_what_it_was_asked_to_do() {
        let mut client = SimulatedExecution::new("sim");
        assert_eq!(client.venue(), "sim");
        client
            .send(
                ExecutionCommand::Submit(Box::new(order(1, 10.0))),
                UnixNanos::new(100),
            )
            .unwrap();
        client
            .send(ExecutionCommand::Cancel(OrderId(1)), UnixNanos::new(200))
            .unwrap();
        assert_eq!(client.sent(), 2);
        assert_eq!(client.log()[0].0, UnixNanos::new(100));
        assert_eq!(client.log()[1].1.kind(), "cancel");
    }

    #[test]
    fn the_fingerprint_is_what_two_runs_are_compared_on() {
        let mut first = SimulatedExecution::new("sim");
        let mut second = SimulatedExecution::new("live");
        for client in [&mut first, &mut second] {
            client
                .send(
                    ExecutionCommand::Submit(Box::new(order(1, 10.0))),
                    UnixNanos::new(100),
                )
                .unwrap();
        }
        assert_eq!(
            first.fingerprint(),
            second.fingerprint(),
            "the same decisions must compare equal across venues"
        );

        // A different decision shows up as a different fingerprint.
        second
            .send(
                ExecutionCommand::Submit(Box::new(order(2, 5.0))),
                UnixNanos::new(150),
            )
            .unwrap();
        assert_ne!(first.fingerprint(), second.fingerprint());
    }

    #[test]
    fn commands_report_the_order_they_concern() {
        assert_eq!(
            ExecutionCommand::Submit(Box::new(order(7, 1.0))).order_id(),
            OrderId(7)
        );
        assert_eq!(ExecutionCommand::Cancel(OrderId(9)).order_id(), OrderId(9));
        assert_eq!(
            ExecutionCommand::Amend {
                id: OrderId(3),
                quantity: None,
                limit: None
            }
            .order_id(),
            OrderId(3)
        );
    }

    #[test]
    fn an_amendment_fingerprint_names_what_changed() {
        let mut client = SimulatedExecution::new("sim");
        client
            .send(
                ExecutionCommand::Amend {
                    id: OrderId(1),
                    quantity: Some(Qty::from_f64(5.0).unwrap()),
                    limit: None,
                },
                UnixNanos::new(10),
            )
            .unwrap();
        let line = &client.fingerprint()[0];
        assert!(line.contains("qty=5"), "{line}");
        assert!(line.contains("px=-"), "an unchanged price reads as absent");
    }
}
