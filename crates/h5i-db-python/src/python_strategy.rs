//! Opt-in Python strategy callbacks.
//!
//! Declarative signal/command replays remain the fast default. This adapter
//! deliberately reacquires the GIL for each callback and is therefore for
//! path-dependent research where Python flexibility is worth that cost.

use std::collections::BTreeMap;

use h5i_db_backtest::clock::TimeEvent;
use h5i_db_backtest::engine::{Context, OrderRequest, Strategy, TwapRequest};
use h5i_db_backtest::event::{MarketEvent, Record};
use h5i_db_backtest::instrument::{InstrumentId, OutcomeId};
use h5i_db_backtest::order::{Fill, OrderId, TimeInForce, Trigger};
use h5i_db_backtest::types::{Price, Qty, Side, UnixNanos};
use h5i_db_backtest::{BacktestError, Result};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

pub(crate) struct PythonStrategy {
    object: Py<PyAny>,
    order_ids: BTreeMap<String, OrderId>,
}

impl PythonStrategy {
    pub(crate) fn new(object: Py<PyAny>) -> Self {
        Self {
            object,
            order_ids: BTreeMap::new(),
        }
    }

    fn invoke(
        &mut self,
        method: &str,
        ctx: &mut Context<'_>,
        event: Option<CallbackEvent<'_>>,
    ) -> Result<()> {
        Python::attach(|py| {
            let object = self.object.bind(py);
            if !object.hasattr(method).map_err(py_error)? {
                return Ok(());
            }
            let context = context_dict(py, ctx).map_err(py_error)?;
            let returned = match event {
                Some(event) => {
                    let payload = event.to_dict(py, ctx).map_err(py_error)?;
                    object
                        .call_method1(method, (context, payload))
                        .map_err(py_error)?
                }
                None => object.call_method1(method, (context,)).map_err(py_error)?,
            };
            self.apply_actions(ctx, &returned)
        })
    }

    fn apply_actions(&mut self, ctx: &mut Context<'_>, returned: &Bound<'_, PyAny>) -> Result<()> {
        if returned.is_none() {
            return Ok(());
        }
        if let Ok(command) = returned.cast::<PyDict>() {
            return self.apply_action(ctx, command);
        }
        for item in returned.try_iter().map_err(py_error)? {
            let item = item.map_err(py_error)?;
            let command = item.cast::<PyDict>().map_err(|_| {
                BacktestError::invalid(
                    "strategy callbacks must return mappings, an iterable of mappings, or None",
                )
            })?;
            self.apply_action(ctx, command)?;
        }
        Ok(())
    }

    fn apply_action(&mut self, ctx: &mut Context<'_>, command: &Bound<'_, PyDict>) -> Result<()> {
        let action: String = required(command, "action")?;
        match action.as_str() {
            "submit" => {
                let client_order_id: String = required(command, "client_order_id")?;
                if client_order_id.is_empty() {
                    return Err(BacktestError::invalid(
                        "submit client_order_id must not be empty",
                    ));
                }
                if self.order_ids.contains_key(&client_order_id) {
                    return Err(BacktestError::invalid(format!(
                        "duplicate client_order_id {client_order_id:?}"
                    )));
                }
                let instrument = InstrumentId::new(required::<String>(command, "instrument_id")?)?;
                let outcome = OutcomeId(optional(command, "outcome")?.unwrap_or(0_u16));
                let side = Side::parse(&required::<String>(command, "side")?)?;
                let quantity = Qty::from_f64(required::<f64>(command, "quantity")?)?;
                let kind =
                    optional::<String>(command, "kind")?.unwrap_or_else(|| "market".to_string());
                let mut request = match kind.as_str() {
                    "market" => OrderRequest::market(instrument, outcome, side, quantity),
                    "limit" => OrderRequest::limit(
                        instrument,
                        outcome,
                        side,
                        Price::from_f64(required::<f64>(command, "limit_price")?)?,
                        quantity,
                    ),
                    other => {
                        return Err(BacktestError::invalid(format!(
                            "unknown callback order kind {other:?}"
                        )));
                    }
                };
                if let Some(tif) = optional::<String>(command, "time_in_force")? {
                    request = request.with_time_in_force(match tif.as_str() {
                        "gtc" => TimeInForce::GoodTilCancel,
                        "ioc" => TimeInForce::ImmediateOrCancel,
                        "fok" => TimeInForce::FillOrKill,
                        other => {
                            return Err(BacktestError::invalid(format!(
                                "unknown callback time_in_force {other:?}"
                            )));
                        }
                    });
                }
                if let Some(tag) = optional::<String>(command, "tag")? {
                    request = request.with_tag(tag);
                }
                if optional::<bool>(command, "reduce_only")?.unwrap_or(false) {
                    request = request.reduce_only();
                }
                if optional::<bool>(command, "post_only")?.unwrap_or(false) {
                    request = request.post_only();
                }
                // A stop or take-profit: held off the book until the mark
                // reaches it, which is what makes it different from a limit
                // at the same price.
                if let Some(price) = optional::<f64>(command, "trigger_price")? {
                    let price = Price::from_f64(price)?;
                    let direction =
                        optional::<String>(command, "trigger_direction")?.unwrap_or_default();
                    request = request.with_trigger(match direction.as_str() {
                        "above" => Trigger::above(price),
                        "below" => Trigger::below(price),
                        "stop_loss" | "" => Trigger::stop_loss(side, price),
                        "take_profit" => Trigger::take_profit(side, price),
                        other => {
                            return Err(BacktestError::invalid(format!(
                                "unknown trigger_direction {other:?}"
                            )));
                        }
                    });
                }
                let id = ctx.submit_tracked(request);
                self.order_ids.insert(client_order_id, id);
            }
            "cancel" => {
                let client_order_id: String = required(command, "client_order_id")?;
                ctx.cancel(self.order_id(&client_order_id, "cancel")?);
            }
            "amend" => {
                let client_order_id: String = required(command, "client_order_id")?;
                let quantity = optional::<f64>(command, "quantity")?
                    .map(Qty::from_f64)
                    .transpose()?;
                let limit = optional::<f64>(command, "limit_price")?
                    .map(Price::from_f64)
                    .transpose()?;
                if quantity.is_none() && limit.is_none() {
                    return Err(BacktestError::invalid(
                        "amend callback action must change quantity or limit_price",
                    ));
                }
                ctx.amend(self.order_id(&client_order_id, "amend")?, quantity, limit);
            }
            "timer" => {
                let name: String = required(command, "name")?;
                let at: i64 = required(command, "ts")?;
                ctx.set_timer(name, UnixNanos::new(at));
            }
            // The venue's set contract: pay a dollar for one of every
            // outcome, or hand the set back for a dollar.
            "mint" | "redeem" => {
                let instrument = InstrumentId::new(required::<String>(command, "instrument_id")?)?;
                let sets = Qty::from_f64(required::<f64>(command, "quantity")?)?;
                if action == "mint" {
                    ctx.mint(&instrument, sets);
                } else {
                    ctx.redeem(&instrument, sets);
                }
            }
            // Worked over time by the venue, not by a client-side loop.
            "twap" => {
                let instrument = InstrumentId::new(required::<String>(command, "instrument_id")?)?;
                let outcome = OutcomeId(optional(command, "outcome")?.unwrap_or(0_u16));
                let side = Side::parse(&required::<String>(command, "side")?)?;
                let quantity = Qty::from_f64(required::<f64>(command, "quantity")?)?;
                let duration: i64 = required(command, "duration_nanos")?;
                let mut twap = TwapRequest::new(instrument, outcome, side, quantity, duration);
                if let Some(interval) = optional::<i64>(command, "interval_nanos")? {
                    twap = twap.interval_nanos(interval);
                }
                if let Some(tag) = optional::<String>(command, "tag")? {
                    twap = twap.with_tag(tag);
                }
                if optional::<bool>(command, "reduce_only")?.unwrap_or(false) {
                    twap = twap.reduce_only();
                }
                ctx.twap(twap);
            }
            "convert" => {
                let instrument = InstrumentId::new(required::<String>(command, "instrument_id")?)?;
                let quantity = Qty::from_f64(required::<f64>(command, "quantity")?)?;
                let held: Vec<u16> = required(command, "outcomes")?;
                ctx.convert(
                    &instrument,
                    held.into_iter().map(OutcomeId).collect(),
                    quantity,
                );
            }
            // Not an instruction to the venue: a statement of belief, kept
            // so the run can be scored against it afterwards.
            "forecast" => {
                let instrument = InstrumentId::new(required::<String>(command, "instrument_id")?)?;
                let outcome = OutcomeId(optional(command, "outcome")?.unwrap_or(0_u16));
                let probability = Price::from_f64(required::<f64>(command, "probability")?)?;
                ctx.record_tagged_forecast(
                    &instrument,
                    outcome,
                    probability,
                    optional::<String>(command, "tag")?,
                )?;
            }
            other => {
                return Err(BacktestError::invalid(format!(
                    "unknown callback action {other:?}"
                )));
            }
        }
        Ok(())
    }

    fn order_id(&self, client_order_id: &str, action: &str) -> Result<OrderId> {
        self.order_ids.get(client_order_id).copied().ok_or_else(|| {
            BacktestError::invalid(format!(
                "{action} references unknown client_order_id {client_order_id:?}"
            ))
        })
    }
}

impl Strategy for PythonStrategy {
    fn on_start(&mut self, ctx: &mut Context<'_>) -> Result<()> {
        self.invoke("on_start", ctx, None)
    }

    fn on_event(&mut self, ctx: &mut Context<'_>, record: &Record) -> Result<()> {
        self.invoke("on_event", ctx, Some(CallbackEvent::Market(record)))
    }

    fn on_timer(&mut self, ctx: &mut Context<'_>, event: &TimeEvent) -> Result<()> {
        self.invoke("on_timer", ctx, Some(CallbackEvent::Timer(event)))
    }

    fn on_fill(&mut self, ctx: &mut Context<'_>, fill: &Fill) -> Result<()> {
        self.invoke("on_fill", ctx, Some(CallbackEvent::Fill(fill)))
    }

    fn on_stop(&mut self, ctx: &mut Context<'_>) -> Result<()> {
        self.invoke("on_stop", ctx, None)
    }
}

enum CallbackEvent<'a> {
    Market(&'a Record),
    Timer(&'a TimeEvent),
    Fill(&'a Fill),
}

impl CallbackEvent<'_> {
    fn to_dict<'py>(&self, py: Python<'py>, ctx: &Context<'_>) -> PyResult<Bound<'py, PyDict>> {
        let out = PyDict::new(py);
        match self {
            Self::Market(record) => {
                out.set_item("type", "market")?;
                out.set_item("kind", record.event.kind())?;
                out.set_item("ts_init", record.stamps.ts_init.get())?;
                out.set_item("ts_event", record.stamps.ts_event.get())?;
                out.set_item("instrument_id", record.instrument.as_str())?;
                out.set_item("outcome", record.outcome.0)?;
                out.set_item(
                    "best_bid",
                    ctx.best_bid(&record.instrument, record.outcome)
                        .map(Price::to_f64),
                )?;
                out.set_item(
                    "best_ask",
                    ctx.best_ask(&record.instrument, record.outcome)
                        .map(Price::to_f64),
                )?;
                out.set_item(
                    "position",
                    ctx.position_quantity(&record.instrument, record.outcome)
                        .to_f64(),
                )?;
                add_market_fields(&out, &record.event)?;
            }
            Self::Timer(event) => {
                out.set_item("type", "timer")?;
                out.set_item("name", &event.name)?;
                out.set_item("scheduled_for", event.scheduled_for.get())?;
                out.set_item("sequence", event.sequence)?;
            }
            Self::Fill(fill) => {
                out.set_item("type", "fill")?;
                out.set_item("ts", fill.ts.get())?;
                out.set_item("order_id", fill.order_id.0)?;
                out.set_item("instrument_id", fill.instrument.as_str())?;
                out.set_item("outcome", fill.outcome.0)?;
                out.set_item("side", fill.side.as_str())?;
                out.set_item("price", fill.price.to_f64())?;
                out.set_item("quantity", fill.quantity.to_f64())?;
                out.set_item("commission", fill.commission.to_f64())?;
                out.set_item("is_taker", fill.is_taker)?;
                out.set_item("tag", fill.tag.as_deref())?;
            }
        }
        Ok(out)
    }
}

fn context_dict<'py>(py: Python<'py>, ctx: &Context<'_>) -> PyResult<Bound<'py, PyDict>> {
    let out = PyDict::new(py);
    out.set_item("now", ctx.now().get())?;
    out.set_item("cash", ctx.cash().to_f64())?;
    let positions = PyList::empty(py);
    for position in ctx.portfolio().positions() {
        let row = PyDict::new(py);
        row.set_item("instrument_id", position.instrument.as_str())?;
        row.set_item("outcome", position.outcome.0)?;
        row.set_item("quantity", position.quantity.to_f64())?;
        row.set_item("realized_pnl", position.realized_pnl.to_f64())?;
        positions.append(row)?;
    }
    out.set_item("positions", positions)?;
    Ok(out)
}

fn add_market_fields(out: &Bound<'_, PyDict>, event: &MarketEvent) -> PyResult<()> {
    match event {
        MarketEvent::Trade {
            price,
            size,
            aggressor,
        } => {
            out.set_item("price", price.to_f64())?;
            out.set_item("size", size.to_f64())?;
            out.set_item("aggressor", aggressor.map(Side::as_str))?;
        }
        MarketEvent::Funding { rate } => out.set_item("rate", rate.to_f64())?,
        MarketEvent::Bar {
            open,
            high,
            low,
            close,
            volume,
        } => {
            out.set_item("open", open.to_f64())?;
            out.set_item("high", high.to_f64())?;
            out.set_item("low", low.to_f64())?;
            out.set_item("close", close.to_f64())?;
            out.set_item("volume", volume.to_f64())?;
        }
        MarketEvent::BookDelta(delta) => {
            out.set_item("action", format!("{:?}", delta.action).to_lowercase())?;
            out.set_item("side", delta.side.as_str())?;
            out.set_item("price", delta.price.to_f64())?;
            out.set_item("size", delta.size.to_f64())?;
        }
        MarketEvent::BookSnapshot { bids, asks } => {
            out.set_item("bid_levels", bids.len())?;
            out.set_item("ask_levels", asks.len())?;
        }
        MarketEvent::Reference { mark, oracle } => {
            out.set_item("mark", mark.map(|price| price.to_f64()))?;
            out.set_item("oracle", oracle.map(|price| price.to_f64()))?;
        }
        MarketEvent::Gap | MarketEvent::Corporate(_) => {}
    }
    Ok(())
}

fn required<'py, T>(dict: &Bound<'py, PyDict>, key: &str) -> Result<T>
where
    for<'a> T: FromPyObject<'a, 'py, Error = PyErr>,
{
    dict.get_item(key)
        .map_err(py_error)?
        .ok_or_else(|| BacktestError::invalid(format!("callback action is missing {key:?}")))?
        .extract()
        .map_err(py_error)
}

fn optional<'py, T>(dict: &Bound<'py, PyDict>, key: &str) -> Result<Option<T>>
where
    for<'a> T: FromPyObject<'a, 'py, Error = PyErr>,
{
    match dict.get_item(key).map_err(py_error)? {
        Some(value) if !value.is_none() => value.extract().map(Some).map_err(py_error),
        _ => Ok(None),
    }
}

fn py_error(error: PyErr) -> BacktestError {
    BacktestError::invalid(format!("Python strategy callback failed: {error}"))
}
