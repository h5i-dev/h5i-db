//! Errors the backtest layer raises.
//!
//! Every variant names something the caller can act on. Data problems that
//! would otherwise produce a plausible wrong number -- a gap in an
//! incremental book, a window that means two different things -- are errors
//! here rather than warnings, because the failure mode they guard against
//! is silent.

use thiserror::Error;

pub type Result<T> = std::result::Result<T, BacktestError>;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum BacktestError {
    #[error("arithmetic overflow in {what}")]
    Overflow { what: &'static str },

    #[error("{what} must be finite")]
    NotFinite { what: &'static str },

    #[error("cannot parse {what}: {value:?}")]
    Parse { what: &'static str, value: String },

    #[error(
        "ts_init ({ts_init}) precedes ts_event ({ts_event}): the system \
         cannot have learned of an event before it happened. Check which \
         vendor field is mapped to which column."
    )]
    CausalityViolation { ts_event: i64, ts_init: i64 },

    #[error(
        "time window is empty or inverted: [{start}, {end}). Windows are \
         half-open; end must be strictly after start."
    )]
    EmptyWindow { start: i64, end: i64 },

    #[error(
        "book gap for {instrument}: an incremental update arrived at \
         {ts} with no snapshot since the gap at {gap_ts}. Replaying across \
         a hole would make the book more confident than the data supports; \
         wait for the next snapshot."
    )]
    BookGap {
        instrument: String,
        ts: i64,
        gap_ts: i64,
    },

    #[error(
        "record is out of order: {ts} follows {previous} on stream \
         {stream:?}. Replay requires each stream sorted by ts_init."
    )]
    OutOfOrder {
        stream: String,
        ts: i64,
        previous: i64,
    },

    #[error("unknown instrument {0:?}")]
    UnknownInstrument(String),

    #[error(
        "outcome {outcome} is out of range for {instrument}, which has \
         {count} outcomes"
    )]
    UnknownOutcome {
        instrument: String,
        outcome: u16,
        count: u16,
    },

    #[error("schema mismatch for {table}: {detail}")]
    Schema { table: &'static str, detail: String },

    #[error("{0}")]
    Invalid(String),
}

impl BacktestError {
    pub fn invalid(message: impl Into<String>) -> Self {
        BacktestError::Invalid(message.into())
    }
}
