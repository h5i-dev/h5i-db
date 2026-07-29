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

pub mod book;
pub mod clock;
pub mod error;
pub mod event;
pub mod instrument;
pub mod replay;
pub mod types;
pub mod window;

pub use book::{BookAction, BookDelta, BookStatus, BookWalk, OrderBook};
pub use clock::{Clock, TimeEvent};
pub use error::{BacktestError, Result};
pub use event::{MarketEvent, Record};
pub use instrument::{Instrument, InstrumentId, InstrumentKind, InstrumentSet, OutcomeId};
pub use replay::{Replay, ReplayBuilder};
pub use types::{notional, Money, Price, Qty, Side, Stamps, UnixNanos, SCALE};
pub use window::{Coverage, TimeWindow};
