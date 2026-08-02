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
//! and no more deltas are accepted until a fresh snapshot arrives.

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
    yes: BTreeMap<i64, i64>,
    no: BTreeMap<i64, i64>,
}

impl OrderbookDecoder {
    pub fn new(ticker: impl Into<String>) -> Result<Self> {
        Ok(Self {
            ticker: InstrumentId::new(ticker)?,
            last_seq: None,
            desynced: true,
            yes: BTreeMap::new(),
            no: BTreeMap::new(),
        })
    }

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

        let step = Step::of(self.last_seq, seq);
        match kind {
            "orderbook_snapshot" => {
                // A snapshot carries the whole book, so it is accepted
                // whatever its sequence says. A resubscribe restarts the
                // numbering, and treating the restart as stale would leave
                // the decoder desynced for the rest of the session -- with
                // the fresh state it needs sitting in the message it just
                // dropped.
                self.last_seq = Some(seq);
                let yes = parse_levels(msg.get("yes_dollars_fp"), "yes_dollars_fp")?;
                let no = parse_levels(msg.get("no_dollars_fp"), "no_dollars_fp")?;
                self.yes = raw_levels(&yes);
                self.no = raw_levels(&no);
                self.desynced = false;
                let (bids, asks) = normalized_book(yes, no)?;
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
                    Step::Stale => return Ok(Vec::new()),
                    Step::Skipped => {
                        self.last_seq = Some(seq);
                        self.desynced = true;
                        // Stamped from the arrival clock like the snapshot
                        // path, because a gap is something *this* system
                        // noticed. Stamping it from the venue's clock instead
                        // lets a stream of records go backwards in time and
                        // trip `IngestPlan::validate`.
                        return Ok(vec![self.gap(received_at)]);
                    }
                    Step::InOrder => self.last_seq = Some(seq),
                }
                if self.desynced {
                    // The gap was already emitted once. Emitting it again for
                    // every delta until the resubscribe would bury the one
                    // that matters, and the book stays unreconstructable
                    // either way, so the delta is simply not accepted.
                    return Ok(Vec::new());
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
                    self.desynced = true;
                    return Ok(vec![self.gap(received_at)]);
                }
                let levels = if yes_side { &mut self.yes } else { &mut self.no };
                let delta = if updated == 0 {
                    levels.remove(&price.raw());
                    BookDelta::delete(canonical_side, canonical_price)
                } else {
                    levels.insert(price.raw(), updated);
                    BookDelta::set(canonical_side, canonical_price, Qty::from_raw(updated))
                };
                // Never earlier than the event: the venue stamps `ts_event`
                // from its clock and we stamp arrival from ours, so a skewed
                // venue clock reading later than local arrival would have the
                // record claim it was known before it happened. Clamping is
                // the honest reading of skew, and `Stamps::new` is the only
                // place that enforces it -- building the struct by hand, as
                // this did, walks straight past the causality check.
                let ts_init = received_at.max(event_at);
                Ok(vec![Record::new(
                    Stamps::new(event_at, ts_init)?,
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

    fn gap(&self, at: UnixNanos) -> Record {
        Record::new(
            Stamps::immediate(at),
            self.ticker.clone(),
            OutcomeId::FIRST,
            MarketEvent::Gap,
        )
    }
}

/// How a message's sequence number relates to the last one accepted.
///
/// The three cases need different answers and the old single `skipped` flag
/// could not tell them apart: it read a duplicate as a gap and then moved
/// `last_seq` backwards onto it, so the rest of the stream looked skipped too.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Step {
    /// The first message, or the successor of the last one accepted.
    InOrder,
    /// Messages between the last accepted one and this one were lost.
    Skipped,
    /// A sequence at or below one already accepted: a duplicate, a reordered
    /// replay, or a stream that restarted its numbering.
    Stale,
}

impl Step {
    fn of(last_seq: Option<i64>, seq: i64) -> Self {
        match last_seq {
            None => Step::InOrder,
            Some(previous) if seq == previous.saturating_add(1) => Step::InOrder,
            Some(previous) if seq <= previous => Step::Stale,
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
    fn a_delta_is_known_no_earlier_than_it_happened() {
        // The venue stamps `ts_ms` from its clock and we stamp arrival from
        // ours. A venue clock running ahead used to produce a record claiming
        // it was known before it happened, because the stamps were built as a
        // struct literal and never saw `Stamps::new`.
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
        assert_eq!(stamps.ts_event, ns(9_000_000));
        assert_eq!(
            stamps.ts_init,
            ns(9_000_000),
            "clamped to the event, never before it"
        );
        assert!(stamps.ts_init >= stamps.ts_event);
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
