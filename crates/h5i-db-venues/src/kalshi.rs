//! Kalshi Trade API v2 payloads into canonical backtest records.
//!
//! The production boundary is deliberately explicit:
//!
//! * REST market metadata defines instruments and observable settlement;
//! * REST order books and WebSocket `orderbook_snapshot` messages define L2;
//! * WebSocket `orderbook_delta` messages are sequence-checked and converted
//!   from relative quantity changes to absolute canonical levels;
//! * live and historical trade pages share one parser;
//! * candlesticks are research bars, not a substitute for historical L2.
//!
//! Kalshi does not publish historical order-book deltas. Queue-accurate
//! backtests therefore require prospective capture of the authenticated
//! WebSocket stream. A missing sequence emits an explicit [`MarketEvent::Gap`]
//! and no more deltas are accepted until a fresh snapshot arrives. So does a
//! stream that restarts its numbering after a failover: the new numbering is
//! adopted rather than mistaken for a resend, because a decoder still holding
//! the old high-water mark would refuse every later delta for the rest of the
//! session while reporting the frozen book as good.

use std::cmp::Reverse;
use std::collections::BTreeMap;

use chrono::DateTime;
use h5i_db_backtest::book::BookDelta;
use h5i_db_backtest::currency::Currency;
use h5i_db_backtest::error::{BacktestError, Result};
use h5i_db_backtest::event::{MarketEvent, Record};
use h5i_db_backtest::instrument::{Instrument, InstrumentId, OutcomeId};
use h5i_db_backtest::settlement::Resolution;
use h5i_db_backtest::types::{Price, Qty, Side, Stamps, UnixNanos};
use serde_json::Value;

/// Parsed market metadata and the resolution, when settlement is observable.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct MarketDefinition {
    pub instrument: Instrument,
    pub resolution: Option<Resolution>,
}

fn parse_json(body: &str, what: &'static str) -> Result<Value> {
    serde_json::from_str(body).map_err(|error| BacktestError::Parse {
        what,
        value: error.to_string(),
    })
}

fn required_str<'a>(value: &'a Value, field: &'static str) -> Result<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| BacktestError::Parse {
            what: field,
            value: "missing or not a string".to_string(),
        })
}

fn optional_decimal(value: &Value, field: &'static str) -> Result<Option<f64>> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) => {
            text.parse::<f64>()
                .map(Some)
                .map_err(|_| BacktestError::Parse {
                    what: field,
                    value: text.clone(),
                })
        }
        Some(Value::Number(number)) => number
            .as_f64()
            .map(Some)
            .ok_or(BacktestError::NotFinite { what: field }),
        Some(other) => Err(BacktestError::Parse {
            what: field,
            value: other.to_string(),
        }),
    }
}

fn decimal(value: &Value, field: &'static str) -> Result<f64> {
    optional_decimal(value, field)?.ok_or_else(|| BacktestError::Parse {
        what: field,
        value: "missing".to_string(),
    })
}

fn parse_time(value: &str, field: &'static str) -> Result<UnixNanos> {
    let parsed = DateTime::parse_from_rfc3339(value).map_err(|_| BacktestError::Parse {
        what: field,
        value: value.to_string(),
    })?;
    let nanos = parsed
        .timestamp_nanos_opt()
        .ok_or_else(|| BacktestError::Parse {
            what: field,
            value: "timestamp is outside nanosecond range".to_string(),
        })?;
    Ok(UnixNanos::new(nanos))
}

fn optional_time(value: &Value, field: &'static str) -> Result<Option<UnixNanos>> {
    match value.get(field).and_then(Value::as_str) {
        Some(text) => parse_time(text, field).map(Some),
        None => Ok(None),
    }
}

#[allow(clippy::collapsible_if)] // The collapsed let-chain requires Rust 2024.
fn uniform_tick(market: &Value) -> Result<Price> {
    if let Some(ranges) = market.get("price_ranges").and_then(Value::as_array) {
        if !ranges.is_empty() {
            let mut tick: Option<Price> = None;
            for range in ranges {
                let step = Price::from_f64(decimal(range, "step")?)?;
                if let Some(previous) = tick {
                    if previous != step {
                        return Err(BacktestError::invalid(
                            "Kalshi market has a variable tick schedule; the current \
                             instrument model cannot validate tapered price ranges",
                        ));
                    }
                }
                tick = Some(step);
            }
            if let Some(tick) = tick {
                return Ok(tick);
            }
        }
    }

    match market
        .get("price_level_structure")
        .and_then(Value::as_str)
        .unwrap_or("linear_cent")
    {
        "linear_cent" => Price::from_f64(0.01),
        "deci_cent" => Price::from_f64(0.001),
        "tapered_deci_cent" => Err(BacktestError::invalid(
            "Kalshi tapered_deci_cent requires a variable tick schedule",
        )),
        other => Err(BacktestError::invalid(format!(
            "unknown Kalshi price_level_structure {other}"
        ))),
    }
}

/// Parse `GET /markets/{ticker}` or one raw market object.
///
/// Only binary markets with a uniform tick schedule are accepted. Rejecting
/// tapered schedules is intentional: treating the smallest tick as valid
/// everywhere would let a strategy submit prices the venue refuses.
pub fn parse_market(body: &str) -> Result<MarketDefinition> {
    let root = parse_json(body, "Kalshi market")?;
    let market = root.get("market").unwrap_or(&root);
    if market
        .get("market_type")
        .and_then(Value::as_str)
        .unwrap_or("binary")
        != "binary"
    {
        return Err(BacktestError::invalid(
            "only binary Kalshi markets are currently replayable",
        ));
    }

    let ticker = required_str(market, "ticker")?;
    let tick = uniform_tick(market)?;
    let lot = if market
        .get("fractional_trading_enabled")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        Qty::from_f64(0.01)?
    } else {
        Qty::from_f64(1.0)?
    };
    let mut instrument = Instrument::binary(ticker, "kalshi")?
        .with_tick_size(tick)
        .with_lot_size(lot)
        .with_settlement_currency(Currency::new("USD")?);

    if let Some(expiration) = optional_time(market, "expiration_time")? {
        instrument = instrument.with_expiration(expiration);
    }
    let settlement_at = optional_time(market, "settlement_ts")?;
    if let Some(at) = settlement_at {
        instrument = instrument.with_settlement_observable(at);
    }

    let winner = match market.get("result").and_then(Value::as_str) {
        Some("yes") => Some(OutcomeId(0)),
        Some("no") => Some(OutcomeId(1)),
        Some("scalar") => {
            return Err(BacktestError::invalid(
                "scalar Kalshi settlement is not a binary resolution",
            ));
        }
        Some("") | None => None,
        Some(other) => {
            return Err(BacktestError::invalid(format!(
                "unknown Kalshi market result {other}"
            )));
        }
    };
    let resolution = match (winner, settlement_at) {
        (Some(winner), Some(at)) => Some(Resolution::new(instrument.id.clone(), winner, at)),
        _ => None,
    };
    Ok(MarketDefinition {
        instrument,
        resolution,
    })
}

fn parse_levels(value: Option<&Value>, what: &'static str) -> Result<Vec<(Price, Qty)>> {
    let Some(rows) = value.and_then(Value::as_array) else {
        return Ok(Vec::new());
    };
    rows.iter()
        .map(|row| {
            let pair = row.as_array().ok_or_else(|| BacktestError::Parse {
                what,
                value: row.to_string(),
            })?;
            if pair.len() != 2 {
                return Err(BacktestError::Parse {
                    what,
                    value: row.to_string(),
                });
            }
            Ok((
                Price::from_f64(decimal_pair(&pair[0], what)?)?,
                Qty::from_f64(decimal_pair(&pair[1], what)?)?,
            ))
        })
        .collect()
}

fn decimal_pair(value: &Value, what: &'static str) -> Result<f64> {
    match value {
        Value::String(text) => text.parse::<f64>().map_err(|_| BacktestError::Parse {
            what,
            value: text.clone(),
        }),
        Value::Number(number) => number.as_f64().ok_or(BacktestError::NotFinite { what }),
        other => Err(BacktestError::Parse {
            what,
            value: other.to_string(),
        }),
    }
}

type BookSide = Vec<(Price, Qty)>;

fn normalized_book(yes: BookSide, no: BookSide) -> Result<(BookSide, BookSide)> {
    let mut bids = yes;
    let mut asks = no
        .into_iter()
        .map(|(price, size)| Ok((price.complement()?, size)))
        .collect::<Result<Vec<_>>>()?;
    bids.sort_by_key(|(price, _)| Reverse(*price));
    asks.sort_by_key(|(price, _)| *price);
    Ok((bids, asks))
}

/// Parse `GET /markets/{ticker}/orderbook` as one full YES-outcome book.
pub fn parse_orderbook(body: &str, ticker: &str, received_at: UnixNanos) -> Result<Record> {
    let root = parse_json(body, "Kalshi orderbook")?;
    let book = root
        .get("orderbook_fp")
        .ok_or_else(|| BacktestError::Parse {
            what: "orderbook_fp",
            value: "missing".to_string(),
        })?;
    let yes = parse_levels(book.get("yes_dollars"), "yes_dollars")?;
    let no = parse_levels(book.get("no_dollars"), "no_dollars")?;
    let (bids, asks) = normalized_book(yes, no)?;
    Ok(Record::new(
        Stamps::immediate(received_at),
        InstrumentId::new(ticker)?,
        OutcomeId::FIRST,
        MarketEvent::BookSnapshot { bids, asks },
    ))
}

/// Stateful, single-market decoder for Kalshi's `orderbook_delta` channel.
#[derive(Clone, Debug)]
pub struct OrderbookDecoder {
    ticker: InstrumentId,
    last_seq: Option<i64>,
    desynced: bool,
    /// Consecutive messages carrying a sequence at or below `last_seq`.
    ///
    /// A venue resending its buffer produces a bounded run of these; a venue
    /// that restarted its numbering produces an unbounded one. See [`Step`].
    stale_run: u32,
    /// Whether the current desynced episode has already emitted its `Gap`.
    ///
    /// One notice per episode: repeating it for every message until the
    /// resubscribe buries the one that matters, and emitting none at all
    /// leaves a replay unable to tell a frozen book from a quiet market.
    gap_reported: bool,
    yes: BTreeMap<i64, i64>,
    no: BTreeMap<i64, i64>,
}

impl OrderbookDecoder {
    pub fn new(ticker: impl Into<String>) -> Result<Self> {
        Ok(Self {
            ticker: InstrumentId::new(ticker)?,
            last_seq: None,
            desynced: true,
            stale_run: 0,
            gap_reported: false,
            yes: BTreeMap::new(),
            no: BTreeMap::new(),
        })
    }

    /// Whether the book can still be rebuilt from the deltas being accepted.
    ///
    /// False whenever a delta is discarded for any reason other than being one
    /// already applied: a duplicate carries nothing new, but every other
    /// discard means the book has stopped tracking the venue, and a decoder
    /// that answers `true` while dropping deltas is the one failure a caller
    /// cannot detect for itself.
    pub fn is_synced(&self) -> bool {
        !self.desynced
    }

    /// Decode one WebSocket message.
    ///
    /// A sequence gap yields exactly one `Gap` record and marks the decoder
    /// stale. A later snapshot may yield `[Gap, Snapshot]` when its own
    /// sequence confirms messages were skipped.
    ///
    /// Every condition that makes the book untrustworthy is reported the same
    /// way, as a `Gap` record. Reporting one of them as an `Err` instead would
    /// make a caller that logs-and-continues silently drop the marker that
    /// tells a replay its book is no longer reconstructable. `Err` is reserved
    /// for messages this decoder cannot read at all.
    ///
    /// The invariant that follows from that: no delta is ever discarded
    /// without a `Gap` having been emitted for the episode that discards it,
    /// the sole exception being a sequence already applied, which by
    /// definition removes nothing from the book.
    ///
    /// Every record leaves on one clock. `ts_init` is `received_at`, because
    /// arrival is the order a replay of the capture has to reproduce and a
    /// gap has no other clock to be stamped from.
    pub fn decode(&mut self, body: &str, received_at: UnixNanos) -> Result<Vec<Record>> {
        let root = parse_json(body, "Kalshi WebSocket orderbook")?;
        let kind = required_str(&root, "type")?;
        let seq = root
            .get("seq")
            .and_then(Value::as_i64)
            .ok_or_else(|| BacktestError::Parse {
                what: "seq",
                value: "missing or not an integer".to_string(),
            })?;
        let msg = root.get("msg").ok_or_else(|| BacktestError::Parse {
            what: "msg",
            value: "missing".to_string(),
        })?;
        let ticker = required_str(msg, "market_ticker")?;
        if ticker != self.ticker.as_str() {
            return Err(BacktestError::invalid(format!(
                "Kalshi decoder for {} received {ticker}",
                self.ticker
            )));
        }

        let step = Step::of(self.last_seq, seq, self.stale_run);
        match kind {
            "orderbook_snapshot" => {
                // A snapshot carries the whole book, so it is accepted
                // whatever its sequence says. A resubscribe restarts the
                // numbering, and treating the restart as stale would leave
                // the decoder desynced for the rest of the session -- with
                // the fresh state it needs sitting in the message it just
                // dropped.
                let yes = parse_levels(msg.get("yes_dollars_fp"), "yes_dollars_fp")?;
                let no = parse_levels(msg.get("no_dollars_fp"), "no_dollars_fp")?;
                let raw_yes = raw_levels(&yes);
                let raw_no = raw_levels(&no);
                let (bids, asks) = normalized_book(yes, no)?;
                // Nothing above this line touches `self` and nothing below it
                // can fail. Advancing `last_seq` first would let a malformed
                // snapshot burn sequence N while the decoder still held the
                // book from before it, so the next delta would read as in
                // order and apply to levels the snapshot was meant to replace.
                self.last_seq = Some(seq);
                self.yes = raw_yes;
                self.no = raw_no;
                self.desynced = false;
                self.stale_run = 0;
                self.gap_reported = false;
                let snapshot = Record::new(
                    Stamps::immediate(received_at),
                    self.ticker.clone(),
                    OutcomeId::FIRST,
                    MarketEvent::BookSnapshot { bids, asks },
                );
                if step == Step::InOrder {
                    Ok(vec![snapshot])
                } else {
                    Ok(vec![self.gap(received_at), snapshot])
                }
            }
            "orderbook_delta" => {
                match step {
                    // A sequence already accepted is a duplicate or a
                    // reordered replay, and a Kalshi delta is a *relative*
                    // change: applying one twice moves the level by twice
                    // its size, which no later message corrects. Dropping it
                    // and leaving `last_seq` alone keeps the accepted stream
                    // contiguous, so the next in-order delta still applies.
                    // This is the one discard that needs no `Gap`: nothing
                    // was lost, so the book is still reconstructable.
                    Step::Stale => {
                        self.stale_run = self.stale_run.saturating_add(1);
                        return Ok(Vec::new());
                    }
                    // Skipped and Restarted differ only in how they were
                    // recognised; both mean the accepted history and the
                    // incoming one are not the same contiguous stream. Adopt
                    // the incoming numbering either way -- for a restart that
                    // is the whole point, since leaving `last_seq` at the old
                    // high value would read every later delta as stale too
                    // and the feed would be dead for the rest of the session.
                    Step::Skipped | Step::Restarted => {
                        self.stale_run = 0;
                        self.last_seq = Some(seq);
                        return Ok(self.desync(received_at));
                    }
                    Step::InOrder => {
                        self.stale_run = 0;
                        self.last_seq = Some(seq);
                    }
                }
                if self.desynced {
                    // Either a gap was reported earlier in this episode, or
                    // this is a delta arriving before the first snapshot and
                    // no gap has been reported at all. `desync` tells those
                    // apart: the book stays unreconstructable either way, but
                    // the discard has to be visible in the record stream at
                    // least once or a replay cannot tell that N deltas were
                    // thrown away.
                    return Ok(self.desync(received_at));
                }
                let event_at = message_time(msg)?;
                let side = required_str(msg, "side")?;
                let price = Price::from_f64(decimal(msg, "price_dollars")?)?;
                let change = Qty::from_f64(decimal(msg, "delta_fp")?)?.raw();
                // Which side, as a flag rather than as a borrow: the check
                // below answers through `self`, and a live `&mut` into one of
                // these maps would stop it doing so.
                let (yes_side, canonical_side, canonical_price) = match side {
                    "yes" => (true, Side::Buy, price),
                    "no" => (false, Side::Sell, price.complement()?),
                    other => {
                        return Err(BacktestError::invalid(format!(
                            "unknown Kalshi orderbook side {other}"
                        )));
                    }
                };
                let current = if yes_side { &self.yes } else { &self.no };
                let updated = current.get(&price.raw()).copied().unwrap_or(0) + change;
                if updated < 0 {
                    // Same class of failure as a sequence gap -- the book can
                    // no longer be reconstructed until a snapshot arrives --
                    // so it is reported through the same channel rather than
                    // as an error a caller might log and step over.
                    return Ok(self.desync(received_at));
                }
                let levels = if yes_side {
                    &mut self.yes
                } else {
                    &mut self.no
                };
                let delta = if updated == 0 {
                    levels.remove(&price.raw());
                    BookDelta::delete(canonical_side, canonical_price)
                } else {
                    levels.insert(price.raw(), updated);
                    BookDelta::set(canonical_side, canonical_price, Qty::from_raw(updated))
                };
                // One clock for every record this decoder emits, and it is
                // arrival. Snapshots and gaps have no other clock available,
                // `Record::ts()` is `ts_init`, and `IngestPlan::validate`
                // refuses a stream whose `ts` decreases -- so pushing a
                // delta's `ts_init` up to a venue clock running ahead lands it
                // after the gap or snapshot that follows it and gets the whole
                // stream rejected as out of order. The venue's stamp is kept
                // as `ts_event` only where it does not contradict arrival:
                // clamping it *down* keeps `Stamps::new`'s causality check
                // satisfied without inverting the order records arrived in.
                let ts_event = event_at.min(received_at);
                Ok(vec![Record::new(
                    Stamps::new(ts_event, received_at)?,
                    self.ticker.clone(),
                    OutcomeId::FIRST,
                    MarketEvent::BookDelta(delta),
                )])
            }
            other => Err(BacktestError::invalid(format!(
                "expected Kalshi orderbook message, got {other}"
            ))),
        }
    }

    /// Mark the book unreconstructable and report it, once per episode.
    ///
    /// Every path that reaches this has decided deltas can no longer be
    /// applied. The first one in an episode yields a `Gap`; the rest yield
    /// nothing, because repeating it for every message until the resubscribe
    /// would bury the one that matters. What must not happen is *none* of
    /// them yielding anything: a replay assembled from `decode` output would
    /// then show a book that simply stopped moving, with nothing in the
    /// stream saying why. A snapshot ends the episode and re-arms the notice.
    fn desync(&mut self, at: UnixNanos) -> Vec<Record> {
        self.desynced = true;
        if self.gap_reported {
            return Vec::new();
        }
        self.gap_reported = true;
        vec![self.gap(at)]
    }

    /// Stamped from the arrival clock, not the venue's: a gap is something
    /// *this* system noticed, and it is the record the deltas around it have
    /// to stay ordered against.
    fn gap(&self, at: UnixNanos) -> Record {
        Record::new(
            Stamps::immediate(at),
            self.ticker.clone(),
            OutcomeId::FIRST,
            MarketEvent::Gap,
        )
    }
}

/// How far below `last_seq` a sequence can be and still be a plausible resend.
///
/// A venue resends out of a bounded buffer, so a message from deep below the
/// accepted history was never part of this numbering at all.
const RESEND_WINDOW: i64 = 64;

/// How many consecutive at-or-below sequences are still readable as a resend.
/// The one after them is not.
///
/// This is the clause that catches a restart back to the *same* origin the
/// session began at, where the backwards jump is too small for
/// [`RESEND_WINDOW`] to see. A resend burst ends; a restarted stream keeps
/// counting up from its new origin forever.
const RESEND_RUN: u32 = 8;

/// How a message's sequence number relates to the last one accepted.
///
/// The four cases need different answers and the old single `skipped` flag
/// could not tell them apart: it read a duplicate as a gap and then moved
/// `last_seq` backwards onto it, so the rest of the stream looked skipped too.
///
/// Splitting `Stale` from `Restarted` is the other half of the same problem.
/// Both arrive as a sequence at or below one already accepted, but a duplicate
/// must be dropped with `last_seq` left alone while a restart must adopt the
/// new numbering, and calling a restart stale kills the feed permanently: no
/// later delta is ever a successor of the old high-water mark again.
///
/// The two are told apart by what a *resend* can look like. It replays
/// sequences already accepted, out of a buffer of bounded size and for a
/// bounded number of messages. So a sequence more than [`RESEND_WINDOW`] below
/// `last_seq`, or a run of more than [`RESEND_RUN`] consecutive at-or-below
/// messages, is a new origin rather than a resend. The asymmetry justifies
/// erring this way: calling a resend a restart costs one spurious `Gap` and a
/// wait for the next snapshot, while calling a restart a resend costs the rest
/// of the session, silently.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Step {
    /// The first message, or the successor of the last one accepted.
    InOrder,
    /// Messages between the last accepted one and this one were lost.
    Skipped,
    /// A sequence at or below one already accepted, close enough behind to be
    /// a duplicate or a reordered replay of a message already applied.
    Stale,
    /// A sequence at or below one already accepted, but from a numbering this
    /// decoder has not been following: the stream restarted.
    Restarted,
}

impl Step {
    fn of(last_seq: Option<i64>, seq: i64, stale_run: u32) -> Self {
        match last_seq {
            None => Step::InOrder,
            Some(previous) if seq == previous.saturating_add(1) => Step::InOrder,
            Some(previous) if seq <= previous => {
                if previous.saturating_sub(seq) > RESEND_WINDOW || stale_run >= RESEND_RUN {
                    Step::Restarted
                } else {
                    Step::Stale
                }
            }
            Some(_) => Step::Skipped,
        }
    }
}

fn raw_levels(levels: &[(Price, Qty)]) -> BTreeMap<i64, i64> {
    levels
        .iter()
        .map(|(price, size)| (price.raw(), size.raw()))
        .collect()
}

fn message_time(msg: &Value) -> Result<UnixNanos> {
    if let Some(ms) = msg.get("ts_ms").and_then(Value::as_i64) {
        return Ok(UnixNanos::new(ms.saturating_mul(1_000_000)));
    }
    if let Some(seconds) = msg.get("ts").and_then(Value::as_i64) {
        return Ok(UnixNanos::new(seconds.saturating_mul(1_000_000_000)));
    }
    Err(BacktestError::Parse {
        what: "ts_ms",
        value: "missing Kalshi event timestamp".to_string(),
    })
}

/// Parse a page from either `/markets/trades` or `/historical/trades`.
pub fn parse_trades(body: &str) -> Result<Vec<Record>> {
    let root = parse_json(body, "Kalshi trades")?;
    let rows = root
        .get("trades")
        .and_then(Value::as_array)
        .ok_or_else(|| BacktestError::Parse {
            what: "trades",
            value: "missing or not an array".to_string(),
        })?;
    let mut records = rows
        .iter()
        .map(|trade| {
            let ticker = required_str(trade, "ticker")?;
            let at = parse_time(required_str(trade, "created_time")?, "created_time")?;
            let price = Price::from_f64(decimal(trade, "yes_price_dollars")?)?;
            let size = Qty::from_f64(decimal(trade, "count_fp")?)?;
            let taker = trade
                .get("taker_outcome_side")
                .or_else(|| trade.get("taker_side"))
                .and_then(Value::as_str);
            let aggressor = match taker {
                Some("yes") => Some(Side::Buy),
                Some("no") => Some(Side::Sell),
                None => None,
                Some(other) => {
                    return Err(BacktestError::invalid(format!(
                        "unknown Kalshi taker side {other}"
                    )));
                }
            };
            Ok(Record::new(
                Stamps::immediate(at),
                InstrumentId::new(ticker)?,
                OutcomeId::FIRST,
                MarketEvent::Trade {
                    price,
                    size,
                    aggressor,
                },
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    records.sort_by_key(|record| record.ts().get());
    Ok(records)
}

/// Parse Kalshi market candlesticks as close-known bars.
///
/// Null-OHLC synthetic carry-forward candles are skipped. They are useful for
/// charts but are not trades and must not create executable bars.
pub fn parse_candlesticks(body: &str, ticker: &str, period_minutes: i64) -> Result<Vec<Record>> {
    if !matches!(period_minutes, 1 | 60 | 1440) {
        return Err(BacktestError::invalid(
            "Kalshi candlestick period must be 1, 60, or 1440 minutes",
        ));
    }
    let root = parse_json(body, "Kalshi candlesticks")?;
    let rows = root
        .get("candlesticks")
        .and_then(Value::as_array)
        .ok_or_else(|| BacktestError::Parse {
            what: "candlesticks",
            value: "missing or not an array".to_string(),
        })?;
    let instrument = InstrumentId::new(ticker)?;
    let width = period_minutes
        .saturating_mul(60)
        .saturating_mul(1_000_000_000);
    let mut records = Vec::new();
    for candle in rows {
        let price = candle.get("price").ok_or_else(|| BacktestError::Parse {
            what: "price",
            value: "missing".to_string(),
        })?;
        let Some(open) = optional_decimal(price, "open_dollars")? else {
            continue;
        };
        let Some(high) = optional_decimal(price, "high_dollars")? else {
            continue;
        };
        let Some(low) = optional_decimal(price, "low_dollars")? else {
            continue;
        };
        let Some(close) = optional_decimal(price, "close_dollars")? else {
            continue;
        };
        let end_seconds = candle
            .get("end_period_ts")
            .and_then(Value::as_i64)
            .ok_or_else(|| BacktestError::Parse {
                what: "end_period_ts",
                value: "missing or not an integer".to_string(),
            })?;
        let end = UnixNanos::new(end_seconds.saturating_mul(1_000_000_000));
        let volume = optional_decimal(candle, "volume_fp")?.unwrap_or(0.0);
        // A bar opens at `end - width` and is knowable only at its close, so
        // go through `Stamps::new`: it is the only place the causality check
        // lives, and a hand-built struct would let a bad `period_minutes` or a
        // negative `end` slip an impossible ordering into the store.
        //
        // Both stamps come from the venue's clock and there is no mix to fix
        // here: a REST page has no arrival clock per row, and every bar in the
        // stream is on that one clock, so the sort below is enough to keep it
        // monotonic.
        records.push(Record::new(
            Stamps::new(UnixNanos::new(end.get().saturating_sub(width)), end)?,
            instrument.clone(),
            OutcomeId::FIRST,
            MarketEvent::Bar {
                open: Price::from_f64(open)?,
                high: Price::from_f64(high)?,
                low: Price::from_f64(low)?,
                close: Price::from_f64(close)?,
                volume: Qty::from_f64(volume)?,
            },
        ));
    }
    records.sort_by_key(|record| record.ts().get());
    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ns(value: i64) -> UnixNanos {
        UnixNanos::new(value)
    }

    #[test]
    fn market_metadata_preserves_kalshi_contract_rules_and_settlement() {
        let parsed = parse_market(
            r#"{"market":{
                "ticker":"KXFED-26JUL-T4.50",
                "market_type":"binary",
                "price_level_structure":"deci_cent",
                "fractional_trading_enabled":true,
                "expiration_time":"2026-07-29T18:00:00Z",
                "settlement_ts":"2026-07-29T18:05:00Z",
                "result":"yes"
            }}"#,
        )
        .unwrap();
        assert_eq!(parsed.instrument.venue, "kalshi");
        assert_eq!(parsed.instrument.tick_size, Price::from_f64(0.001).unwrap());
        assert_eq!(parsed.instrument.lot_size, Qty::from_f64(0.01).unwrap());
        assert_eq!(parsed.instrument.settlement_currency.as_str(), "USD");
        assert_eq!(parsed.resolution.unwrap().winner(), Some(OutcomeId(0)));
    }

    #[test]
    fn tapered_ticks_fail_closed() {
        let error =
            parse_market(r#"{"ticker":"TAPERED","price_level_structure":"tapered_deci_cent"}"#)
                .unwrap_err();
        assert!(error.to_string().contains("variable tick"));
    }

    #[test]
    fn no_bids_become_yes_asks() {
        let record = parse_orderbook(
            r#"{"orderbook_fp":{
                "yes_dollars":[["0.4000","12.00"]],
                "no_dollars":[["0.5500","7.50"]]
            }}"#,
            "KXTEST",
            ns(10),
        )
        .unwrap();
        let MarketEvent::BookSnapshot { bids, asks } = record.event else {
            panic!("expected snapshot");
        };
        assert_eq!(
            bids[0],
            (Price::from_f64(0.4).unwrap(), Qty::from_f64(12.0).unwrap())
        );
        assert_eq!(
            asks[0],
            (Price::from_f64(0.45).unwrap(), Qty::from_f64(7.5).unwrap())
        );
    }

    #[test]
    fn websocket_deltas_are_absolute_and_sequence_gaps_require_a_snapshot() {
        let mut decoder = OrderbookDecoder::new("KXTEST").unwrap();
        let snapshot = decoder
            .decode(
                r#"{"type":"orderbook_snapshot","seq":10,"msg":{
                    "market_ticker":"KXTEST",
                    "yes_dollars_fp":[["0.40","10.00"]],
                    "no_dollars_fp":[["0.55","5.00"]]
                }}"#,
                ns(100),
            )
            .unwrap();
        assert_eq!(snapshot.len(), 1);
        assert!(decoder.is_synced());

        let update = decoder
            .decode(
                r#"{"type":"orderbook_delta","seq":11,"msg":{
                    "market_ticker":"KXTEST","price_dollars":"0.40",
                    "delta_fp":"-3.00","side":"yes","ts_ms":2
                }}"#,
                ns(3_000_000),
            )
            .unwrap();
        let MarketEvent::BookDelta(delta) = update[0].event else {
            panic!("expected delta");
        };
        assert_eq!(delta.size, Qty::from_f64(7.0).unwrap());

        let gap = decoder
            .decode(
                r#"{"type":"orderbook_delta","seq":13,"msg":{
                    "market_ticker":"KXTEST","price_dollars":"0.40",
                    "delta_fp":"1.00","side":"yes","ts_ms":4
                }}"#,
                ns(5_000_000),
            )
            .unwrap();
        assert!(matches!(gap[0].event, MarketEvent::Gap));
        assert_eq!(gap.len(), 1);
        // The gap is stamped from the arrival clock, not the venue's, so a
        // decoder's own output cannot go backwards in time.
        assert_eq!(gap[0].ts(), ns(5_000_000));
        assert!(!decoder.is_synced());
        // Deltas after the gap are not accepted, and say so the same way the
        // gap did: no records, not an error a caller could log and step over.
        let after = decoder
            .decode(
                r#"{"type":"orderbook_delta","seq":14,"msg":{
                    "market_ticker":"KXTEST","price_dollars":"0.40",
                    "delta_fp":"1.00","side":"yes","ts_ms":5
                }}"#,
                ns(6_000_000),
            )
            .unwrap();
        assert!(after.is_empty(), "one gap, not one per following message");
        assert!(!decoder.is_synced());
    }

    #[test]
    fn a_delta_is_stamped_from_arrival_like_every_other_record() {
        // Two clock mixes have to stay out, and this covers both. Stamping
        // `ts_event` from the venue regardless makes `Stamps::new` refuse the
        // record outright when the venue runs ahead. Pushing `ts_init` up to
        // the venue's clock instead passes that check but puts the delta after
        // the gap or snapshot that follows it, and `IngestPlan::validate`
        // rejects the whole stream. One clock for `ts_init`, arrival; the
        // venue's stamp survives in `ts_event` only where it agrees with it.
        let mut decoder = OrderbookDecoder::new("KXTEST").unwrap();
        decoder
            .decode(
                r#"{"type":"orderbook_snapshot","seq":1,"msg":{
                    "market_ticker":"KXTEST",
                    "yes_dollars_fp":[["0.40","10.00"]]
                }}"#,
                ns(0),
            )
            .unwrap();
        let records = decoder
            .decode(
                r#"{"type":"orderbook_delta","seq":2,"msg":{
                    "market_ticker":"KXTEST","price_dollars":"0.40",
                    "delta_fp":"-1.00","side":"yes","ts_ms":9
                }}"#,
                // Arrival reads earlier than the venue's own stamp.
                ns(1_000_000),
            )
            .unwrap();
        let stamps = records[0].stamps;
        assert_eq!(
            stamps.ts_init,
            ns(1_000_000),
            "arrival, never the venue's clock"
        );
        assert_eq!(
            stamps.ts_event,
            ns(1_000_000),
            "clamped down to arrival, not pushed past it"
        );
        assert!(stamps.ts_init >= stamps.ts_event);

        // A venue clock running *behind* contradicts nothing, so its stamp is
        // kept and the record records the delay it really had.
        let behind = decoder
            .decode(
                r#"{"type":"orderbook_delta","seq":3,"msg":{
                    "market_ticker":"KXTEST","price_dollars":"0.40",
                    "delta_fp":"-1.00","side":"yes","ts_ms":1
                }}"#,
                ns(5_000_000),
            )
            .unwrap();
        assert_eq!(behind[0].stamps.ts_event, ns(1_000_000));
        assert_eq!(behind[0].stamps.ts_init, ns(5_000_000));
    }

    #[test]
    fn records_stay_ordered_by_arrival_when_the_venue_clock_runs_ahead() {
        // `Record::ts()` is `ts_init` and `IngestPlan::validate` refuses a
        // stream whose `ts` decreases. Deltas used to carry whichever clock
        // read later while gaps and snapshots carried arrival, so a venue
        // running ahead by more than the message spacing put the first gap
        // *before* the delta it followed and the whole `book_events` stream
        // was rejected as out of order.
        let mut decoder = OrderbookDecoder::new("KXTEST").unwrap();
        let snapshot = decoder
            .decode(
                r#"{"type":"orderbook_snapshot","seq":1,"msg":{
                    "market_ticker":"KXTEST",
                    "yes_dollars_fp":[["0.40","10.00"]]
                }}"#,
                ns(1_000_000),
            )
            .unwrap();
        // The venue's clock is an hour ahead of local arrival.
        let delta = decoder
            .decode(
                r#"{"type":"orderbook_delta","seq":2,"msg":{
                    "market_ticker":"KXTEST","price_dollars":"0.40",
                    "delta_fp":"-1.00","side":"yes","ts_ms":3600000
                }}"#,
                ns(2_000_000),
            )
            .unwrap();
        let gap = decoder
            .decode(
                r#"{"type":"orderbook_delta","seq":9,"msg":{
                    "market_ticker":"KXTEST","price_dollars":"0.40",
                    "delta_fp":"-1.00","side":"yes","ts_ms":3600001
                }}"#,
                ns(3_000_000),
            )
            .unwrap();
        assert!(matches!(gap[0].event, MarketEvent::Gap));
        assert_eq!(delta[0].ts(), ns(2_000_000));
        assert!(snapshot[0].ts() <= delta[0].ts());
        assert!(
            delta[0].ts() <= gap[0].ts(),
            "arrival order, whatever the venue clock says"
        );
    }

    #[test]
    fn a_replayed_delta_is_dropped_rather_than_applied_twice() {
        // A Kalshi delta is a relative change, so applying a duplicate moves
        // the level by twice its size and nothing later corrects it. The old
        // code read any non-successor as a gap and then moved `last_seq` onto
        // it, which desynced the decoder on a message it should have ignored.
        let mut decoder = OrderbookDecoder::new("KXTEST").unwrap();
        decoder
            .decode(
                r#"{"type":"orderbook_snapshot","seq":10,"msg":{
                    "market_ticker":"KXTEST",
                    "yes_dollars_fp":[["0.40","10.00"]]
                }}"#,
                ns(100),
            )
            .unwrap();
        let first = decoder
            .decode(
                r#"{"type":"orderbook_delta","seq":11,"msg":{
                    "market_ticker":"KXTEST","price_dollars":"0.40",
                    "delta_fp":"-3.00","side":"yes","ts_ms":2
                }}"#,
                ns(3_000_000),
            )
            .unwrap();
        let MarketEvent::BookDelta(delta) = first[0].event else {
            panic!("expected delta");
        };
        assert_eq!(delta.size, Qty::from_f64(7.0).unwrap());

        let replay = decoder
            .decode(
                r#"{"type":"orderbook_delta","seq":11,"msg":{
                    "market_ticker":"KXTEST","price_dollars":"0.40",
                    "delta_fp":"-3.00","side":"yes","ts_ms":2
                }}"#,
                ns(3_500_000),
            )
            .unwrap();
        assert!(replay.is_empty(), "a duplicate applies nothing");
        assert!(decoder.is_synced(), "and does not desync the decoder");

        // The stream continues from where it was, not from the duplicate.
        let next = decoder
            .decode(
                r#"{"type":"orderbook_delta","seq":12,"msg":{
                    "market_ticker":"KXTEST","price_dollars":"0.40",
                    "delta_fp":"-2.00","side":"yes","ts_ms":4
                }}"#,
                ns(5_000_000),
            )
            .unwrap();
        let MarketEvent::BookDelta(delta) = next[0].event else {
            panic!("expected delta");
        };
        assert_eq!(
            delta.size,
            Qty::from_f64(5.0).unwrap(),
            "10 - 3 - 2, with the replay ignored"
        );
    }

    #[test]
    fn a_resubscribe_snapshot_restores_a_desynced_decoder() {
        // A resubscribe restarts Kalshi's numbering, so the snapshot that
        // carries the fresh book arrives with a sequence below the last one
        // seen. Refusing it as stale would leave the decoder desynced for the
        // rest of the session.
        let mut decoder = OrderbookDecoder::new("KXTEST").unwrap();
        decoder
            .decode(
                r#"{"type":"orderbook_snapshot","seq":40,"msg":{
                    "market_ticker":"KXTEST",
                    "yes_dollars_fp":[["0.40","10.00"]]
                }}"#,
                ns(100),
            )
            .unwrap();
        let gap = decoder
            .decode(
                r#"{"type":"orderbook_delta","seq":99,"msg":{
                    "market_ticker":"KXTEST","price_dollars":"0.40",
                    "delta_fp":"1.00","side":"yes","ts_ms":1
                }}"#,
                ns(2_000_000),
            )
            .unwrap();
        assert!(matches!(gap[0].event, MarketEvent::Gap));

        let fresh = decoder
            .decode(
                r#"{"type":"orderbook_snapshot","seq":1,"msg":{
                    "market_ticker":"KXTEST",
                    "yes_dollars_fp":[["0.41","8.00"]]
                }}"#,
                ns(3_000_000),
            )
            .unwrap();
        // A gap precedes it: the two streams are not one contiguous history.
        assert!(matches!(fresh[0].event, MarketEvent::Gap));
        assert!(matches!(fresh[1].event, MarketEvent::BookSnapshot { .. }));
        assert!(decoder.is_synced());
    }

    #[test]
    fn a_restarted_sequence_recovers_instead_of_dying_silently() {
        // A failover restarts Kalshi's numbering. If the fresh snapshot is
        // lost or late, every delta after it carries a sequence below
        // `last_seq`. Reading those as duplicates drops them forever while
        // `last_seq` stays at the old high-water mark, so no later message is
        // ever its successor: the book freezes and, worst of all, keeps
        // reporting itself trustworthy.
        let mut decoder = OrderbookDecoder::new("KXTEST").unwrap();
        decoder
            .decode(
                r#"{"type":"orderbook_snapshot","seq":500,"msg":{
                    "market_ticker":"KXTEST",
                    "yes_dollars_fp":[["0.40","10.00"]]
                }}"#,
                ns(100),
            )
            .unwrap();
        assert!(decoder.is_synced());

        // Sequence 1 is 499 below the accepted history, further back than any
        // resend buffer reaches, so it is a new origin and not a replay.
        let restart = decoder
            .decode(
                r#"{"type":"orderbook_delta","seq":1,"msg":{
                    "market_ticker":"KXTEST","price_dollars":"0.40",
                    "delta_fp":"1.00","side":"yes","ts_ms":1
                }}"#,
                ns(2_000_000),
            )
            .unwrap();
        assert!(matches!(restart[0].event, MarketEvent::Gap));
        assert_eq!(restart.len(), 1);
        assert!(
            !decoder.is_synced(),
            "a decoder dropping deltas must never claim its book is good"
        );

        // The new numbering was adopted, so this reads as the successor of
        // the restart rather than as one more duplicate. It is still refused
        // until a snapshot rebuilds the book, and refused without a second
        // gap, because the one above already said why.
        let following = decoder
            .decode(
                r#"{"type":"orderbook_delta","seq":2,"msg":{
                    "market_ticker":"KXTEST","price_dollars":"0.40",
                    "delta_fp":"1.00","side":"yes","ts_ms":2
                }}"#,
                ns(3_000_000),
            )
            .unwrap();
        assert!(
            following.is_empty(),
            "one gap, not one per following message"
        );
        assert!(!decoder.is_synced());

        // A snapshot in the new numbering is in order, so it needs no gap of
        // its own, and the stream is live again.
        let fresh = decoder
            .decode(
                r#"{"type":"orderbook_snapshot","seq":3,"msg":{
                    "market_ticker":"KXTEST",
                    "yes_dollars_fp":[["0.41","8.00"]]
                }}"#,
                ns(4_000_000),
            )
            .unwrap();
        assert_eq!(fresh.len(), 1);
        assert!(matches!(fresh[0].event, MarketEvent::BookSnapshot { .. }));
        assert!(decoder.is_synced());

        let applied = decoder
            .decode(
                r#"{"type":"orderbook_delta","seq":4,"msg":{
                    "market_ticker":"KXTEST","price_dollars":"0.41",
                    "delta_fp":"-3.00","side":"yes","ts_ms":5
                }}"#,
                ns(5_000_000),
            )
            .unwrap();
        let MarketEvent::BookDelta(delta) = applied[0].event else {
            panic!("expected delta");
        };
        assert_eq!(
            delta.size,
            Qty::from_f64(5.0).unwrap(),
            "8 - 3, off the new book"
        );
    }

    #[test]
    fn a_restart_to_a_nearby_origin_is_caught_by_the_run_of_stale_sequences() {
        // A short session that restarts back to the same origin it began at
        // jumps too little for the distance rule to see. What still gives it
        // away is that it never stops: a resend burst ends, a restarted stream
        // keeps counting up. The cost is bounded -- the deltas before the run
        // is long enough are indistinguishable from duplicates and are dropped
        // as such -- and it ends in a gap rather than in permanent silence.
        let mut decoder = OrderbookDecoder::new("KXTEST").unwrap();
        decoder
            .decode(
                r#"{"type":"orderbook_snapshot","seq":20,"msg":{
                    "market_ticker":"KXTEST",
                    "yes_dollars_fp":[["0.40","10.00"]]
                }}"#,
                ns(100),
            )
            .unwrap();
        for seq in 1i64..=8 {
            let dropped = decoder
                .decode(
                    &format!(
                        r#"{{"type":"orderbook_delta","seq":{seq},"msg":{{
                            "market_ticker":"KXTEST","price_dollars":"0.40",
                            "delta_fp":"1.00","side":"yes","ts_ms":1
                        }}}}"#
                    ),
                    ns(1_000_000 * seq),
                )
                .unwrap();
            assert!(dropped.is_empty(), "still readable as a resend");
        }
        let restart = decoder
            .decode(
                r#"{"type":"orderbook_delta","seq":9,"msg":{
                    "market_ticker":"KXTEST","price_dollars":"0.40",
                    "delta_fp":"1.00","side":"yes","ts_ms":1
                }}"#,
                ns(10_000_000),
            )
            .unwrap();
        assert!(matches!(restart[0].event, MarketEvent::Gap));
        assert!(!decoder.is_synced());

        // And the new numbering is adopted, so a snapshot on it is in order.
        let fresh = decoder
            .decode(
                r#"{"type":"orderbook_snapshot","seq":10,"msg":{
                    "market_ticker":"KXTEST",
                    "yes_dollars_fp":[["0.41","8.00"]]
                }}"#,
                ns(11_000_000),
            )
            .unwrap();
        assert_eq!(fresh.len(), 1);
        assert!(decoder.is_synced());
    }

    #[test]
    fn a_delta_before_the_first_snapshot_is_reported_not_swallowed() {
        // Nothing has been snapshotted, so the delta cannot be applied to
        // anything. Returning no records at all leaves a replay assembled from
        // `decode` output with no evidence that N deltas were discarded, and a
        // book that reads as a market nobody traded.
        let mut decoder = OrderbookDecoder::new("KXTEST").unwrap();
        assert!(!decoder.is_synced());
        let first = decoder
            .decode(
                r#"{"type":"orderbook_delta","seq":7,"msg":{
                    "market_ticker":"KXTEST","price_dollars":"0.40",
                    "delta_fp":"1.00","side":"yes","ts_ms":1
                }}"#,
                ns(1_000_000),
            )
            .unwrap();
        assert!(matches!(first[0].event, MarketEvent::Gap));
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].ts(), ns(1_000_000));
        assert!(!decoder.is_synced());

        // Still one notice per episode, not one per message.
        let second = decoder
            .decode(
                r#"{"type":"orderbook_delta","seq":8,"msg":{
                    "market_ticker":"KXTEST","price_dollars":"0.40",
                    "delta_fp":"1.00","side":"yes","ts_ms":2
                }}"#,
                ns(2_000_000),
            )
            .unwrap();
        assert!(second.is_empty(), "one gap, not one per following message");
    }

    #[test]
    fn a_malformed_snapshot_does_not_consume_its_sequence() {
        // Advancing `last_seq` before the levels parse leaves the decoder
        // believing it consumed sequence N while still holding the book from
        // before it, so the delta that really carries N reads as skipped and
        // the stream desyncs on a message that was never lost.
        let mut decoder = OrderbookDecoder::new("KXTEST").unwrap();
        decoder
            .decode(
                r#"{"type":"orderbook_snapshot","seq":10,"msg":{
                    "market_ticker":"KXTEST",
                    "yes_dollars_fp":[["0.40","10.00"]]
                }}"#,
                ns(100),
            )
            .unwrap();
        let error = decoder
            .decode(
                r#"{"type":"orderbook_snapshot","seq":11,"msg":{
                    "market_ticker":"KXTEST",
                    "yes_dollars_fp":[["oops","1.00"]]
                }}"#,
                ns(200),
            )
            .unwrap_err();
        assert!(matches!(error, BacktestError::Parse { .. }));

        // Sequence 11 was never consumed, so the delta carrying it is still
        // the next message in order and applies to the book the last good
        // snapshot left behind.
        let applied = decoder
            .decode(
                r#"{"type":"orderbook_delta","seq":11,"msg":{
                    "market_ticker":"KXTEST","price_dollars":"0.40",
                    "delta_fp":"-4.00","side":"yes","ts_ms":1
                }}"#,
                ns(300),
            )
            .unwrap();
        let MarketEvent::BookDelta(delta) = applied[0].event else {
            panic!("expected delta");
        };
        assert_eq!(delta.size, Qty::from_f64(6.0).unwrap());
        assert!(decoder.is_synced());
    }

    #[test]
    fn a_level_driven_negative_reports_a_gap_not_an_error() {
        // "The book can no longer be reconstructed" is one condition and it
        // gets one channel, whether a sequence or an arithmetic check found it.
        let mut decoder = OrderbookDecoder::new("KXTEST").unwrap();
        decoder
            .decode(
                r#"{"type":"orderbook_snapshot","seq":1,"msg":{
                    "market_ticker":"KXTEST",
                    "yes_dollars_fp":[["0.40","2.00"]]
                }}"#,
                ns(100),
            )
            .unwrap();
        let records = decoder
            .decode(
                r#"{"type":"orderbook_delta","seq":2,"msg":{
                    "market_ticker":"KXTEST","price_dollars":"0.40",
                    "delta_fp":"-5.00","side":"yes","ts_ms":1
                }}"#,
                ns(2_000_000),
            )
            .unwrap();
        assert!(matches!(records[0].event, MarketEvent::Gap));
        assert_eq!(records[0].ts(), ns(2_000_000));
        assert!(!decoder.is_synced());
    }

    #[test]
    fn trade_pages_map_no_takers_to_yes_sellers_and_sort() {
        let records = parse_trades(
            r#"{"trades":[
                {"ticker":"KXTEST","yes_price_dollars":"0.61","count_fp":"2.50",
                 "taker_side":"no","created_time":"2026-07-29T18:00:02Z"},
                {"ticker":"KXTEST","yes_price_dollars":"0.60","count_fp":"1.00",
                 "taker_outcome_side":"yes","created_time":"2026-07-29T18:00:01Z"}
            ],"cursor":""}"#,
        )
        .unwrap();
        assert!(records[0].ts() < records[1].ts());
        let MarketEvent::Trade { aggressor, .. } = records[1].event else {
            panic!("expected trade");
        };
        assert_eq!(aggressor, Some(Side::Sell));
    }

    #[test]
    fn candles_are_known_at_close_and_synthetic_rows_are_skipped() {
        let records = parse_candlesticks(
            r#"{"candlesticks":[
                {"end_period_ts":120,"price":{
                    "open_dollars":"0.40","high_dollars":"0.45",
                    "low_dollars":"0.39","close_dollars":"0.44"
                },"volume_fp":"12.00"},
                {"end_period_ts":180,"price":{"previous_dollars":"0.44"},"volume_fp":"0.00"}
            ]}"#,
            "KXTEST",
            1,
        )
        .unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].stamps.ts_event, ns(60_000_000_000));
        assert_eq!(records[0].stamps.ts_init, ns(120_000_000_000));
    }
}
