//! Opt-in Python strategy callbacks.
//!
//! Declarative signal/command replays remain the fast default. This adapter
//! deliberately reacquires the GIL for each callback and is therefore for
//! path-dependent research where Python flexibility is worth that cost.
//!
//! What that cost is made of was measured rather than assumed
//! (`benchmarks/backtest_compare/h5i_callback_boundary.py`): most of it was
//! not the call, it was building argument dictionaries the strategy never
//! read. Two decisions follow.
//!
//! **Callbacks the strategy did not override are not called.** `EventStrategy`
//! defines all four as no-ops, so `hasattr` is always true and a strategy that
//! only wants fills was still paying a full crossing per market event to be
//! told `None`. The bound methods are resolved once, at construction.
//!
//! **Arguments are mappings that build Python objects on demand.** A market
//! event carried thirteen entries, three of them book and portfolio lookups,
//! whether or not the callback touched one. [`View`] owns a Rust snapshot and
//! converts per key, so an untouched field costs nothing. It owns rather than
//! borrows deliberately: a strategy that keeps an event past its callback
//! keeps a valid snapshot, which is what a plain dict gave it before.

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
    /// Bound callbacks, resolved once. `None` means "do not call": either the
    /// strategy has no such method, or it left `EventStrategy`'s do-nothing
    /// version in place, and crossing the boundary to be handed `None` back
    /// is the most expensive way to learn nothing.
    on_start: Option<Callable>,
    on_event: Option<Callable>,
    on_timer: Option<Callable>,
    on_fill: Option<Callable>,
    on_stop: Option<Callable>,
    order_ids: BTreeMap<String, OrderId>,
}

/// A resolved callback and whether it asked for the context.
///
/// Most strategies never read `context`, and building one is a pyclass and a
/// snapshot of every open position. Whether to build it is decided from the
/// signature, once, rather than every event: a callback declared
/// `on_event(self, event)` is not handed one.
struct Callable {
    function: Py<PyAny>,
    wants_context: bool,
}

impl PythonStrategy {
    /// Resolving the callbacks is where a signature this adapter cannot call
    /// correctly is caught, so it fails before the run rather than partway
    /// through one.
    pub(crate) fn new(object: Py<PyAny>) -> Result<Self> {
        Python::attach(|py| {
            let bound = object.bind(py);
            // The base class is only used to recognise an unoverridden
            // method. If it cannot be imported the strategy is some other
            // duck-typed object, and every method it defines gets called --
            // the behaviour before any of this existed.
            let base = py
                .import("h5i_db.backtest")
                .and_then(|module| module.getattr("EventStrategy"))
                .ok();
            Ok(Self {
                on_start: resolve(bound, base.as_ref(), "on_start", false)?,
                on_event: resolve(bound, base.as_ref(), "on_event", true)?,
                on_timer: resolve(bound, base.as_ref(), "on_timer", true)?,
                on_fill: resolve(bound, base.as_ref(), "on_fill", true)?,
                on_stop: resolve(bound, base.as_ref(), "on_stop", false)?,
                order_ids: BTreeMap::new(),
            })
        })
    }

    fn invoke(
        &mut self,
        method: Callback,
        ctx: &mut Context<'_>,
        event: Option<CallbackEvent<'_>>,
    ) -> Result<()> {
        // Checked before the GIL is taken: a strategy that does not want this
        // callback should not pay for an attach to find that out.
        if self.callback(method).is_none() {
            return Ok(());
        }
        Python::attach(|py| {
            let (callable, wants_context) = match self.callback(method) {
                Some(callable) => (callable.function.bind(py).clone(), callable.wants_context),
                None => return Ok(()),
            };
            // Only built if it was asked for: it is a pyclass plus a snapshot
            // of every open position, and most callbacks never read it.
            let context = wants_context
                .then(|| Py::new(py, context_view(ctx)))
                .transpose()
                .map_err(py_error)?;
            let returned = match event {
                Some(event) => {
                    let payload = Py::new(py, event.to_view(ctx)).map_err(py_error)?;
                    match context {
                        Some(context) => callable.call1((context, payload)),
                        None => callable.call1((payload,)),
                    }
                    .map_err(py_error)?
                }
                None => match context {
                    Some(context) => callable.call1((context,)),
                    None => callable.call0(),
                }
                .map_err(py_error)?,
            };
            self.apply_actions(ctx, &returned)
        })
    }

    fn callback(&self, method: Callback) -> Option<&Callable> {
        match method {
            Callback::Start => self.on_start.as_ref(),
            Callback::Event => self.on_event.as_ref(),
            Callback::Timer => self.on_timer.as_ref(),
            Callback::Fill => self.on_fill.as_ref(),
            Callback::Stop => self.on_stop.as_ref(),
        }
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

/// Which callback, resolved to a slot rather than looked up by name.
#[derive(Clone, Copy)]
enum Callback {
    Start,
    Event,
    Timer,
    Fill,
    Stop,
}

/// The bound method to call, or `None` where calling it would be a crossing
/// spent to learn nothing.
fn resolve(
    object: &Bound<'_, PyAny>,
    base: Option<&Bound<'_, PyAny>>,
    name: &str,
    carries_event: bool,
) -> Result<Option<Callable>> {
    let Ok(bound) = object.getattr(name) else {
        return Ok(None);
    };
    if let Some(base) = base
        && let Ok(inherited) = base.getattr(name)
        // Compare the *function* the lookup landed on, not the bound method:
        // attribute access builds a fresh bound method every time, so two of
        // them are never the same object. Going through `__func__` also keeps
        // a method assigned on the instance rather than the class an
        // override -- it has no `__func__`, so it never matches, and the one
        // thing this must not do is quietly stop calling something.
        && let Ok(found) = bound.getattr("__func__")
        && found.is(&inherited)
    {
        return Ok(None);
    }
    Ok(Some(Callable {
        wants_context: wants_context(&bound, carries_event, name)?,
        function: bound.unbind(),
    }))
}

/// Whether this callback declared a `context` parameter.
///
/// Decided from the *name* of the first parameter after `self`, not from the
/// parameter count. Counting worked only for the two documented shapes and
/// misbound anything else: `on_event(self, event, threshold=0.5)` counts three
/// parameters, so the context was passed in the `event` slot and the strategy
/// silently read a portfolio snapshot as a market event. Reading the name
/// gives the same answer for the documented shapes and the right one for the
/// rest.
///
/// `context` and `ctx` are the names that ask for one. A signature this
/// cannot call correctly is refused here rather than halfway through a run:
/// if the engine cannot fill every parameter that has no default --
/// `on_event(self, portfolio, event)`, where the context was asked for under
/// a name nothing recognises -- construction fails and says so.
///
/// Anything with no readable signature -- a C function, a `*args` wrapper, a
/// decorator that dropped `__code__` -- is given the context, which is what
/// `EventStrategy`'s docstring promises. The cost of guessing wrong that way
/// is a wasted argument.
fn wants_context(bound: &Bound<'_, PyAny>, carries_event: bool, name: &str) -> Result<bool> {
    // A bound method carries `self` in its code object; a function assigned
    // onto the instance is called without one, and both reach here.
    let implicit_self = bound.getattr("__func__").is_ok();
    let function = bound.getattr("__func__").unwrap_or_else(|_| bound.clone());
    let Ok(code) = function.getattr("__code__") else {
        return Ok(true);
    };
    // CO_VARARGS: the callback takes `*args`, so it names nothing and accepts
    // everything.
    const CO_VARARGS: usize = 0x04;
    if code
        .getattr("co_flags")
        .and_then(|flags| flags.extract::<usize>())
        .is_ok_and(|flags| flags & CO_VARARGS != 0)
    {
        return Ok(true);
    }
    let (Ok(argcount), Ok(names)) = (
        code.getattr("co_argcount")
            .and_then(|count| count.extract::<usize>()),
        code.getattr("co_varnames")
            .and_then(|names| names.extract::<Vec<String>>()),
    ) else {
        return Ok(true);
    };
    let skip = usize::from(implicit_self);
    let wants = matches!(names.get(skip).map(String::as_str), Some("context" | "ctx"));
    // Parameters with defaults are the caller's business, not the engine's:
    // only the ones it must fill are counted.
    let defaults = function
        .getattr("__defaults__")
        .ok()
        .and_then(|values| values.len().ok())
        .unwrap_or(0);
    // Both sides count `self`, however it arrives.
    let required = (argcount + 1 - skip).saturating_sub(defaults);
    let passed = 1 + usize::from(wants) + usize::from(carries_event);
    if required > passed {
        return Err(BacktestError::invalid(format!(
            "{name} declares {} parameter(s) without defaults, but the engine calls \
             it with {}. Name the first parameter `context` to be handed one, and \
             give any further parameter a default.",
            required - 1,
            passed - 1
        )));
    }
    Ok(wants)
}

impl Strategy for PythonStrategy {
    fn on_start(&mut self, ctx: &mut Context<'_>) -> Result<()> {
        self.invoke(Callback::Start, ctx, None)
    }

    fn on_event(&mut self, ctx: &mut Context<'_>, record: &Record) -> Result<()> {
        self.invoke(Callback::Event, ctx, Some(CallbackEvent::Market(record)))
    }

    fn on_timer(&mut self, ctx: &mut Context<'_>, event: &TimeEvent) -> Result<()> {
        self.invoke(Callback::Timer, ctx, Some(CallbackEvent::Timer(event)))
    }

    fn on_fill(&mut self, ctx: &mut Context<'_>, fill: &Fill) -> Result<()> {
        self.invoke(Callback::Fill, ctx, Some(CallbackEvent::Fill(fill)))
    }

    fn on_stop(&mut self, ctx: &mut Context<'_>) -> Result<()> {
        self.invoke(Callback::Stop, ctx, None)
    }
}

enum CallbackEvent<'a> {
    Market(&'a Record),
    Timer(&'a TimeEvent),
    Fill(&'a Fill),
}

impl CallbackEvent<'_> {
    fn to_view(&self, ctx: &Context<'_>) -> View {
        let mut out = View::with_capacity(13);
        match self {
            Self::Market(record) => {
                out.put("type", Val::Static("market"));
                out.put("kind", Val::Static(record.event.kind()));
                out.put("ts_init", Val::Int(record.stamps.ts_init.get()));
                out.put("ts_event", Val::Int(record.stamps.ts_event.get()));
                out.put("instrument_id", Val::Instrument(record.instrument.clone()));
                out.put("outcome", Val::Int(record.outcome.0 as i64));
                // Three lookups the old dict paid on every event. Cheap
                // enough to take now (they are map reads of `Copy` values);
                // it is the Python floats they became that were not.
                out.put(
                    "best_bid",
                    Val::OptFloat(
                        ctx.best_bid(&record.instrument, record.outcome)
                            .map(Price::to_f64),
                    ),
                );
                out.put(
                    "best_ask",
                    Val::OptFloat(
                        ctx.best_ask(&record.instrument, record.outcome)
                            .map(Price::to_f64),
                    ),
                );
                out.put(
                    "position",
                    Val::Float(
                        ctx.position_quantity(&record.instrument, record.outcome)
                            .to_f64(),
                    ),
                );
                add_market_fields(&mut out, &record.event);
            }
            Self::Timer(event) => {
                out.put("type", Val::Static("timer"));
                out.put("name", Val::Text(event.name.clone()));
                out.put("scheduled_for", Val::Int(event.scheduled_for.get()));
                out.put("sequence", Val::Int(event.sequence as i64));
            }
            Self::Fill(fill) => {
                out.put("type", Val::Static("fill"));
                out.put("ts", Val::Int(fill.ts.get()));
                out.put("order_id", Val::Int(fill.order_id.0 as i64));
                out.put("instrument_id", Val::Instrument(fill.instrument.clone()));
                out.put("outcome", Val::Int(fill.outcome.0 as i64));
                out.put("side", Val::Static(fill.side.as_str()));
                out.put("price", Val::Float(fill.price.to_f64()));
                out.put("quantity", Val::Float(fill.quantity.to_f64()));
                out.put("commission", Val::Float(fill.commission.to_f64()));
                out.put("is_taker", Val::Bool(fill.is_taker));
                out.put("tag", Val::OptText(fill.tag.clone()));
            }
        }
        out
    }
}

/// A position as the callback sees it, before it is any Python object.
struct PositionRow {
    instrument: InstrumentId,
    outcome: u16,
    quantity: f64,
    realized_pnl: f64,
}

/// One field, still in Rust.
enum Val {
    Int(i64),
    Float(f64),
    OptFloat(Option<f64>),
    Bool(bool),
    Static(&'static str),
    Text(String),
    OptText(Option<String>),
    Instrument(InstrumentId),
    Positions(Vec<PositionRow>),
}

impl Val {
    fn to_py(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        use pyo3::IntoPyObjectExt;
        match self {
            Self::Int(value) => value.into_py_any(py),
            Self::Float(value) => value.into_py_any(py),
            Self::OptFloat(value) => value.into_py_any(py),
            Self::Bool(value) => value.into_py_any(py),
            Self::Static(value) => value.into_py_any(py),
            Self::Text(value) => value.as_str().into_py_any(py),
            Self::OptText(value) => value.as_deref().into_py_any(py),
            Self::Instrument(value) => value.as_str().into_py_any(py),
            Self::Positions(rows) => {
                let out = PyList::empty(py);
                for row in rows {
                    let entry = PyDict::new(py);
                    entry.set_item("instrument_id", row.instrument.as_str())?;
                    entry.set_item("outcome", row.outcome)?;
                    entry.set_item("quantity", row.quantity)?;
                    entry.set_item("realized_pnl", row.realized_pnl)?;
                    out.append(entry)?;
                }
                out.into_py_any(py)
            }
        }
    }
}

/// The mapping a callback is handed.
///
/// A `dict` in all the ways a strategy uses one -- subscript, `get`, `in`,
/// `len`, iteration, and `dict(...)` -- but the values are Rust until asked
/// for. The lookup is a linear scan because the widest event carries
/// thirteen keys, and comparing thirteen static strings costs less than the
/// hashing a map would need.
#[pyclass(mapping, module = "h5i_db", name = "CallbackMapping")]
pub(crate) struct View {
    fields: Vec<(&'static str, Val)>,
}

impl View {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            fields: Vec::with_capacity(capacity),
        }
    }

    fn put(&mut self, key: &'static str, value: Val) {
        self.fields.push((key, value));
    }

    fn lookup(&self, key: &str) -> Option<&Val> {
        self.fields
            .iter()
            .find(|(name, _)| *name == key)
            .map(|(_, value)| value)
    }
}

#[pymethods]
impl View {
    fn __getitem__(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        match self.lookup(key) {
            Some(value) => value.to_py(py),
            None => Err(pyo3::exceptions::PyKeyError::new_err(key.to_string())),
        }
    }

    #[pyo3(signature = (key, default=None))]
    fn get(
        &self,
        py: Python<'_>,
        key: &str,
        default: Option<Py<PyAny>>,
    ) -> PyResult<Option<Py<PyAny>>> {
        match self.lookup(key) {
            Some(value) => value.to_py(py).map(Some),
            None => Ok(default),
        }
    }

    fn __contains__(&self, key: &str) -> bool {
        self.lookup(key).is_some()
    }

    fn __len__(&self) -> usize {
        self.fields.len()
    }

    fn keys(&self) -> Vec<&'static str> {
        self.fields.iter().map(|(name, _)| *name).collect()
    }

    fn values(&self, py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
        self.fields
            .iter()
            .map(|(_, value)| value.to_py(py))
            .collect()
    }

    fn items(&self, py: Python<'_>) -> PyResult<Vec<(&'static str, Py<PyAny>)>> {
        self.fields
            .iter()
            .map(|(name, value)| Ok((*name, value.to_py(py)?)))
            .collect()
    }

    fn __iter__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        use pyo3::IntoPyObjectExt;
        PyList::new(py, self.keys())?.into_py_any(py)
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let mut out = String::from("{");
        for (index, (name, value)) in self.fields.iter().enumerate() {
            if index > 0 {
                out.push_str(", ");
            }
            let rendered = value.to_py(py)?;
            out.push_str(&format!("{name:?}: {}", rendered.bind(py).repr()?));
        }
        out.push('}');
        Ok(out)
    }
}

/// The context a callback is handed.
///
/// `positions` used to be a `PyDict` per open position on every event, read
/// or not. It is a Rust row per position now -- one `Arc` bump and three
/// `Copy` fields each -- and becomes Python objects only if the callback
/// asks for them.
fn context_view(ctx: &Context<'_>) -> View {
    let mut out = View::with_capacity(3);
    out.put("now", Val::Int(ctx.now().get()));
    out.put("cash", Val::Float(ctx.cash().to_f64()));
    out.put(
        "positions",
        Val::Positions(
            ctx.portfolio()
                .positions()
                .map(|position| PositionRow {
                    instrument: position.instrument.clone(),
                    outcome: position.outcome.0,
                    quantity: position.quantity.to_f64(),
                    realized_pnl: position.realized_pnl.to_f64(),
                })
                .collect(),
        ),
    );
    out
}

fn add_market_fields(out: &mut View, event: &MarketEvent) {
    match event {
        MarketEvent::Trade {
            price,
            size,
            aggressor,
        } => {
            out.put("price", Val::Float(price.to_f64()));
            out.put("size", Val::Float(size.to_f64()));
            out.put(
                "aggressor",
                Val::OptText(aggressor.map(|side| side.as_str().to_string())),
            );
        }
        MarketEvent::Funding { rate } => out.put("rate", Val::Float(rate.to_f64())),
        MarketEvent::Bar {
            open,
            high,
            low,
            close,
            volume,
        } => {
            out.put("open", Val::Float(open.to_f64()));
            out.put("high", Val::Float(high.to_f64()));
            out.put("low", Val::Float(low.to_f64()));
            out.put("close", Val::Float(close.to_f64()));
            out.put("volume", Val::Float(volume.to_f64()));
        }
        MarketEvent::BookDelta(delta) => {
            out.put(
                "action",
                Val::Text(format!("{:?}", delta.action).to_lowercase()),
            );
            out.put("side", Val::Static(delta.side.as_str()));
            out.put("price", Val::Float(delta.price.to_f64()));
            out.put("size", Val::Float(delta.size.to_f64()));
        }
        MarketEvent::BookSnapshot { bids, asks } => {
            out.put("bid_levels", Val::Int(bids.len() as i64));
            out.put("ask_levels", Val::Int(asks.len() as i64));
        }
        MarketEvent::Reference { mark, oracle } => {
            out.put("mark", Val::OptFloat(mark.map(|price| price.to_f64())));
            out.put("oracle", Val::OptFloat(oracle.map(|price| price.to_f64())));
        }
        MarketEvent::Gap | MarketEvent::Corporate(_) => {}
    }
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

/// Carry a Python exception out of a callback as a backtest error.
///
/// Two things the plain `{error}` spelling lost. The **traceback** is where
/// the failure actually is: a `KeyError: 'price'` reported without one names
/// neither the callback nor the line, and the strategy is the caller's code,
/// not this crate's. And an **interrupt is not bad input**: `Ctrl-C` or an
/// exhausted heap arriving mid-callback surfaced as an `InvalidInputError`,
/// which reads as "your signals table is malformed" and sends the reader
/// looking in the wrong place. The variant is still `Invalid` -- the backtest
/// error model has no interrupt case -- so the message has to say it.
fn py_error(error: PyErr) -> BacktestError {
    Python::attach(|py| {
        let kind = if error.is_instance_of::<pyo3::exceptions::PyKeyboardInterrupt>(py)
            || error.is_instance_of::<pyo3::exceptions::PySystemExit>(py)
        {
            "the Python strategy callback was interrupted"
        } else if error.is_instance_of::<pyo3::exceptions::PyMemoryError>(py) {
            "the Python strategy callback ran out of memory"
        } else {
            "Python strategy callback failed"
        };
        let traceback = error
            .traceback(py)
            .and_then(|tb| tb.format().ok())
            .map(|text| format!("\n{text}"))
            .unwrap_or_default();
        BacktestError::invalid(format!("{kind}: {error}{traceback}"))
    })
}
