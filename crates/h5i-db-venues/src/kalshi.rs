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

fn normalized_book(
    yes: Vec<(Price, Qty)>,
    no: Vec<(Price, Qty)>,
) -> Result<(Vec<(Price, Qty)>, Vec<(Price, Qty)>)> {
    let mut bids = yes;
    let mut asks = no
        .into_iter()
        .map(|(price, size)| Ok((price.complement()?, size)))
        .collect::<Result<Vec<_>>>()?;
    bids.sort_by(|a, b| b.0.cmp(&a.0));
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

        let skipped = self.last_seq.is_some_and(|previous| seq != previous + 1);
        self.last_seq = Some(seq);
        match kind {
            "orderbook_snapshot" => {
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
                if skipped {
                    Ok(vec![self.gap(received_at), snapshot])
                } else {
                    Ok(vec![snapshot])
                }
            }
            "orderbook_delta" => {
                let event_at = message_time(msg)?;
                if skipped {
                    self.desynced = true;
                    return Ok(vec![self.gap(event_at)]);
                }
                if self.desynced {
                    return Err(BacktestError::invalid(
                        "Kalshi orderbook delta received before a fresh snapshot",
                    ));
                }
                let side = required_str(msg, "side")?;
                let price = Price::from_f64(decimal(msg, "price_dollars")?)?;
                let change = Qty::from_f64(decimal(msg, "delta_fp")?)?.raw();
                let (levels, canonical_side, canonical_price) = match side {
                    "yes" => (&mut self.yes, Side::Buy, price),
                    "no" => (&mut self.no, Side::Sell, price.complement()?),
                    other => {
                        return Err(BacktestError::invalid(format!(
                            "unknown Kalshi orderbook side {other}"
                        )));
                    }
                };
                let updated = levels.get(&price.raw()).copied().unwrap_or(0) + change;
                if updated < 0 {
                    self.desynced = true;
                    return Err(BacktestError::invalid(
                        "Kalshi orderbook delta makes a level negative; resnapshot required",
                    ));
                }
                let delta = if updated == 0 {
                    levels.remove(&price.raw());
                    BookDelta::delete(canonical_side, canonical_price)
                } else {
                    levels.insert(price.raw(), updated);
                    BookDelta::set(canonical_side, canonical_price, Qty::from_raw(updated))
                };
                Ok(vec![Record::new(
                    Stamps {
                        ts_event: event_at,
                        ts_init: received_at,
                    },
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
        records.push(Record::new(
            Stamps {
                ts_event: UnixNanos::new(end.get().saturating_sub(width)),
                ts_init: end,
            },
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
        assert_eq!(parsed.resolution.unwrap().winner, OutcomeId(0));
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
        assert!(!decoder.is_synced());
        assert!(decoder
            .decode(
                r#"{"type":"orderbook_delta","seq":14,"msg":{
                        "market_ticker":"KXTEST","price_dollars":"0.40",
                        "delta_fp":"1.00","side":"yes","ts_ms":5
                    }}"#,
                ns(6_000_000),
            )
            .is_err());
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
