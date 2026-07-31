// Fixed-point arithmetic promotes to `i128` to hold a product. Under `wide`
// that promotion is already the raw type, so every `as i128` is a no-op and
// clippy says so, 67 times. The casts are not removable: the default build
// needs them, and this crate compiles both ways from one source. Suppressed
// only in the mode where they are redundant, so the default build still
// reports a genuinely pointless cast if one is ever written.
#![cfg_attr(feature = "wide", allow(clippy::unnecessary_cast))]

//! Deterministic event-driven backtesting on versioned h5i-db data.
//!
//! This crate is Part B of `ROADMAP_QUANT.md`. It simulates venues against
//! recorded data; it never routes a live order.
//!
//! Three properties shape everything here, and each is a test rather than an
//! intention:
//!
//! * **Determinism.** A run is a pure function of (data pin, strategy,
//!   config, seed). No wall clock, no unseeded randomness, no iteration over
//!   a hash map without sorting first. Two runs from the same inputs produce
//!   identical output, which is what makes an honest trial count -- and
//!   therefore an honest deflated Sharpe -- possible at all.
//! * **No look-ahead, structurally.** Records carry both `ts_event` and
//!   `ts_init` and replay in `ts_init` order, so late data arrives late.
//!   Strategy reads go through a pin advanced by the replay clock, so
//!   reading past "now" is not something a careful strategy avoids, it is
//!   something the storage layer refuses.
//! * **Data honesty.** Windows are half-open and owned in one place; a gap
//!   in incremental data invalidates the book rather than being replayed
//!   across; requested and loaded windows are separate facts.
//!
//! The dependency rule (ROADMAP_QUANT.md P6) runs one way: this crate uses
//! the engine, and the engine crates build and pass their tests with it
//! deleted.

pub mod account;
pub mod book;
pub mod clock;
pub mod corporate;
pub mod currency;
pub mod engine;
pub mod error;
pub mod event;
pub mod execution;
pub mod instrument;
pub mod l3;
pub mod models;
pub mod order;
pub mod position;
pub mod replay;
pub mod run;
pub mod schema;
pub mod settlement;
pub mod store;
pub mod types;
pub mod window;

pub use account::{
    Account, CashMargin, LinearMargin, MarginModel, MarginRequirement, MarginState,
    PerInstrumentMargin, Valuation,
};
pub use book::{BookAction, BookDelta, BookStatus, BookWalk, OrderBook};
pub use clock::{Clock, TimeEvent};
pub use corporate::{CorporateAction, SymbolRegistry, Universe};
pub use currency::{Currency, FxBook, Haircuts};
pub use engine::{
    CommandReplay, Context, Engine, EngineBuilder, Forecast, LiquidationPolicy, MarkPoint,
    MarkSource, OrderRequest, ReplayCommand, RiskLimits, RunResult, SetOperation,
    SetOperationCosts, SetOperationKind, SetOperationRequest, SignalReplay, Strategy, TwapProgress,
    TwapRequest,
};
pub use error::{BacktestError, Result};
pub use event::{MarketEvent, Record};
pub use execution::{ExecutionClient, ExecutionCommand, SimulatedExecution};
pub use instrument::{
    Instrument, InstrumentId, InstrumentKind, InstrumentSet, OutcomeId, PriceRule,
    normalise_to_one, uniform_prices,
};
pub use l3::{L3Book, MboMessage};
pub use models::{
    BookFills, ConstantLatency, FeeModel, FeeTier, FillModel, LatencyModel, NoFees, NoLatency,
    PredictionMarketFees, ProportionalFees, QueuePositionFills, TickSlippage, TieredFees,
    VenueModule,
};
pub use order::{
    Fill, Order, OrderId, OrderKind, OrderStatus, TimeInForce, Trigger, TriggerDirection,
};
pub use position::{Portfolio, Position};
pub use replay::{Replay, ReplayBuilder};
pub use run::{CalibrationSample, RunReport, RunSpec, UnscoredForecast, run_in_fork, run_in_place};
pub use settlement::{Payout, Resolution, SettlementReport, validate_resolutions};
pub use types::{Money, Price, Qty, SCALE, Side, Stamps, UnixNanos, notional};
pub use window::{Coverage, TimeWindow};
