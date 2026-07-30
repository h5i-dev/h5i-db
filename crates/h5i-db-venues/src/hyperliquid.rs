//! Hyperliquid: request shapes and response parsing.
//!
//! This module parses; it does not fetch. Every function here takes bytes
//! that a caller already downloaded, which keeps the hard part (field
//! mapping, unit handling, the knowability of a bar) testable offline and
//! leaves HTTP to a script. [`candles_request`] and friends emit the exact
//! payloads to POST, so nothing is left to guess.
//!
//! Everything goes to one endpoint: `POST https://api.hyperliquid.xyz/info`
//! (`https://api.hyperliquid-testnet.xyz/info` for testnet), with a JSON
//! body whose `type` selects the query.
//!
//! Three details are worth stating because a widely-referenced open-source
//! Hyperliquid backtester gets each of them wrong:
//!
//! 1. **Timestamps are milliseconds.** That library documents its inputs as
//!    seconds and then converts candle times with a seconds constructor, so
//!    every timestamp lands in 1970.
//! 2. **Funding settles hourly**, and `fundingRate` is the rate for that
//!    hour. That library applies rates only at 00/08/16 UTC, which both
//!    mistimes them and drops seven eighths of the carry.
//! 3. **Prices and sizes arrive as JSON strings**, not numbers, and are
//!    parsed exactly rather than through a float that has already rounded.

use serde_json::Value;

use h5i_db_backtest::error::{BacktestError, Result};
use h5i_db_backtest::event::{MarketEvent, Record};
use h5i_db_backtest::instrument::{Instrument, InstrumentId, OutcomeId};
use h5i_db_backtest::types::{Price, Qty, Stamps, UnixNanos};

/// Mainnet info endpoint.
pub const MAINNET_INFO_URL: &str = "https://api.hyperliquid.xyz/info";
/// Testnet info endpoint.
pub const TESTNET_INFO_URL: &str = "https://api.hyperliquid-testnet.xyz/info";
/// Hyperliquid settles funding every hour.
pub const FUNDING_INTERVAL_HOURS: i64 = 1;

const MS: i64 = 1_000_000;

/// Body for a candle history request.
///
/// `start_ms` and `end_ms` are Unix **milliseconds**.
pub fn candles_request(coin: &str, interval: &str, start_ms: i64, end_ms: i64) -> String {
    serde_json::json!({
        "type": "candleSnapshot",
        "req": {
            "coin": coin,
            "interval": interval,
            "startTime": start_ms,
            "endTime": end_ms,
        }
    })
    .to_string()
}

/// Body for a funding history request.
pub fn funding_request(coin: &str, start_ms: i64, end_ms: Option<i64>) -> String {
    let mut req = serde_json::json!({ "coin": coin, "startTime": start_ms });
    if let Some(end) = end_ms {
        req["endTime"] = Value::from(end);
    }
    serde_json::json!({ "type": "fundingHistory", "req": req }).to_string()
}

/// Body for an L2 book snapshot request.
pub fn l2_book_request(coin: &str) -> String {
    serde_json::json!({ "type": "l2Book", "coin": coin }).to_string()
}

/// A perpetual instrument for a Hyperliquid coin.
pub fn instrument(coin: &str, tick_size: f64, lot_size: f64) -> Result<Instrument> {
    Ok(
        Instrument::perpetual(format!("{coin}-PERP"), "hyperliquid")?
            .with_tick_size(Price::from_f64(tick_size)?)
            .with_lot_size(Qty::from_f64(lot_size)?),
    )
}

fn parse_json(body: &str) -> Result<Value> {
    serde_json::from_str(body).map_err(|error| BacktestError::Parse {
        what: "hyperliquid response",
        value: error.to_string(),
    })
}

/// Parse a numeric field that the API sends as a string.
fn number(value: &Value, field: &'static str) -> Result<f64> {
    match value.get(field) {
        Some(Value::String(text)) => text.parse::<f64>().map_err(|_| BacktestError::Parse {
            what: field,
            value: text.clone(),
        }),
        Some(Value::Number(number)) => number
            .as_f64()
            .ok_or(BacktestError::NotFinite { what: field }),
        _ => Err(BacktestError::Parse {
            what: field,
            value: "missing".to_string(),
        }),
    }
}

fn millis(value: &Value, field: &'static str) -> Result<i64> {
    value
        .get(field)
        .and_then(Value::as_i64)
        .ok_or(BacktestError::Parse {
            what: field,
            value: "missing or not an integer".to_string(),
        })
}

/// Parse a `candleSnapshot` response into bar records.
///
/// A bar's `ts_event` is its **open** and its `ts_init` is its **close**,
/// because that is when the bar became knowable. Stamping both at the open
/// would let a strategy act at 09:00 on a 09:00-10:00 bar that includes the
/// hour it is about to trade through -- the single most common lookahead
/// bug in bar-driven backtests, and one the dual stamp removes for free.
pub fn parse_candles(body: &str, instrument_id: &str) -> Result<Vec<Record>> {
    let json = parse_json(body)?;
    let rows = json.as_array().ok_or(BacktestError::Parse {
        what: "candleSnapshot",
        value: "expected an array".to_string(),
    })?;
    let id = InstrumentId::new(instrument_id)?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let open_ms = millis(row, "t")?;
        let close_ms = millis(row, "T")?;
        out.push(Record::new(
            Stamps::new(UnixNanos::new(open_ms * MS), UnixNanos::new(close_ms * MS))?,
            id.clone(),
            OutcomeId::FIRST,
            MarketEvent::Bar {
                open: Price::from_f64(number(row, "o")?)?,
                high: Price::from_f64(number(row, "h")?)?,
                low: Price::from_f64(number(row, "l")?)?,
                close: Price::from_f64(number(row, "c")?)?,
                volume: Qty::from_f64(number(row, "v")?)?,
            },
        ));
    }
    out.sort_by_key(|record| record.ts().get());
    Ok(out)
}

/// Parse a `fundingHistory` response into funding records.
///
/// `fundingRate` is the rate for one settlement interval, which on
/// Hyperliquid is one hour. It is carried through unscaled; scaling an
/// already-per-interval rate is how a carry backtest ends up off by 8x.
pub fn parse_funding(body: &str, instrument_id: &str) -> Result<Vec<Record>> {
    let json = parse_json(body)?;
    let rows = json.as_array().ok_or(BacktestError::Parse {
        what: "fundingHistory",
        value: "expected an array".to_string(),
    })?;
    let id = InstrumentId::new(instrument_id)?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let at = UnixNanos::new(millis(row, "time")? * MS);
        out.push(Record::new(
            Stamps::immediate(at),
            id.clone(),
            OutcomeId::FIRST,
            MarketEvent::Funding {
                rate: Price::from_f64(number(row, "fundingRate")?)?,
            },
        ));
    }
    out.sort_by_key(|record| record.ts().get());
    Ok(out)
}

/// Parse an `l2Book` response into a book snapshot.
///
/// The payload is `{ coin, time, levels: [[bids], [asks]] }`, each level
/// `{ px, sz, n }` with `px` and `sz` as strings.
pub fn parse_l2_book(body: &str, instrument_id: &str) -> Result<Record> {
    let json = parse_json(body)?;
    let at = UnixNanos::new(millis(&json, "time")? * MS);
    let levels = json
        .get("levels")
        .and_then(Value::as_array)
        .ok_or(BacktestError::Parse {
            what: "l2Book.levels",
            value: "missing".to_string(),
        })?;
    if levels.len() != 2 {
        return Err(BacktestError::Parse {
            what: "l2Book.levels",
            value: format!("expected two sides, got {}", levels.len()),
        });
    }

    let side = |index: usize| -> Result<Vec<(Price, Qty)>> {
        let rows = levels[index].as_array().ok_or(BacktestError::Parse {
            what: "l2Book side",
            value: "expected an array".to_string(),
        })?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            out.push((
                Price::from_f64(number(row, "px")?)?,
                Qty::from_f64(number(row, "sz")?)?,
            ));
        }
        Ok(out)
    };

    Ok(Record::new(
        Stamps::immediate(at),
        InstrumentId::new(instrument_id)?,
        OutcomeId::FIRST,
        MarketEvent::BookSnapshot {
            bids: side(0)?,
            asks: side(1)?,
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    // Wire shapes as the API sends them: numbers-as-strings, times in ms.
    const CANDLES: &str = r#"[
        {"t":1640995200000,"T":1640998799999,"s":"BTC","i":"1h",
         "o":"46000.5","c":"46500.25","h":"46700.0","l":"45900.75","v":"120.5","n":842},
        {"t":1640998800000,"T":1641002399999,"s":"BTC","i":"1h",
         "o":"46500.25","c":"46200.0","h":"46800.0","l":"46100.0","v":"98.25","n":611}
    ]"#;

    const FUNDING: &str = r#"[
        {"coin":"BTC","fundingRate":"0.0000125","premium":"0.0001","time":1640995200000},
        {"coin":"BTC","fundingRate":"-0.0000075","premium":"-0.00005","time":1640998800000}
    ]"#;

    const BOOK: &str = r#"{"coin":"BTC","time":1640995200000,"levels":[
        [{"px":"46000.0","sz":"1.5","n":3},{"px":"45999.0","sz":"2.25","n":5}],
        [{"px":"46001.0","sz":"0.75","n":2},{"px":"46002.0","sz":"3.0","n":7}]
    ]}"#;

    #[test]
    fn request_bodies_match_the_documented_wire_format() {
        let candles = candles_request("BTC", "1h", 1640995200000, 1641002400000);
        let parsed: Value = serde_json::from_str(&candles).unwrap();
        assert_eq!(parsed["type"], "candleSnapshot");
        assert_eq!(parsed["req"]["coin"], "BTC");
        assert_eq!(parsed["req"]["interval"], "1h");
        assert_eq!(parsed["req"]["startTime"], 1640995200000i64);
        assert_eq!(parsed["req"]["endTime"], 1641002400000i64);

        let funding: Value = serde_json::from_str(&funding_request("BTC", 1, None)).unwrap();
        assert_eq!(funding["type"], "fundingHistory");
        assert!(
            funding["req"].get("endTime").is_none(),
            "omitted when absent"
        );

        let book: Value = serde_json::from_str(&l2_book_request("ETH")).unwrap();
        assert_eq!(book["type"], "l2Book");
        assert_eq!(book["coin"], "ETH");
    }

    #[test]
    fn candles_parse_with_millisecond_timestamps() {
        let records = parse_candles(CANDLES, "BTC-PERP").unwrap();
        assert_eq!(records.len(), 2);
        // 1640995200000 ms is 2022-01-01T00:00:00Z, not 1970.
        assert_eq!(records[0].stamps.ts_event.get(), 1_640_995_200_000 * MS);
        let MarketEvent::Bar {
            open,
            close,
            volume,
            ..
        } = &records[0].event
        else {
            panic!("expected a bar");
        };
        assert_eq!(*open, Price::from_f64(46000.5).unwrap());
        assert_eq!(*close, Price::from_f64(46500.25).unwrap());
        assert_eq!(*volume, Qty::from_f64(120.5).unwrap());
    }

    #[test]
    fn a_bar_becomes_knowable_at_its_close_not_its_open() {
        // The lookahead this mapping prevents: acting at 00:00 on a bar
        // that summarises 00:00-01:00.
        let records = parse_candles(CANDLES, "BTC-PERP").unwrap();
        let first = &records[0];
        assert_eq!(first.stamps.ts_event.get(), 1_640_995_200_000 * MS);
        assert_eq!(first.stamps.ts_init.get(), 1_640_998_799_999 * MS);
        assert!(
            first.stamps.ts_init > first.stamps.ts_event,
            "a bar is knowable only once it has closed"
        );
        // And replay orders by ts_init, so that is the instant it arrives.
        assert_eq!(first.ts(), first.stamps.ts_init);
    }

    #[test]
    fn funding_rates_parse_with_their_sign_intact() {
        let records = parse_funding(FUNDING, "BTC-PERP").unwrap();
        assert_eq!(records.len(), 2);
        let MarketEvent::Funding { rate } = &records[0].event else {
            panic!("expected funding");
        };
        assert_eq!(*rate, Price::from_f64(0.0000125).unwrap());
        let MarketEvent::Funding { rate } = &records[1].event else {
            panic!("expected funding");
        };
        assert!(rate.is_negative(), "a negative rate means shorts pay");
    }

    #[test]
    fn funding_is_hourly_and_carried_through_unscaled() {
        let records = parse_funding(FUNDING, "BTC-PERP").unwrap();
        let gap = records[1].ts().get() - records[0].ts().get();
        assert_eq!(
            gap,
            3_600 * 1_000 * MS,
            "Hyperliquid settles hourly, not every eight hours"
        );
        assert_eq!(FUNDING_INTERVAL_HOURS, 1);
    }

    #[test]
    fn l2_books_parse_both_sides_in_order() {
        let record = parse_l2_book(BOOK, "BTC-PERP").unwrap();
        let MarketEvent::BookSnapshot { bids, asks } = &record.event else {
            panic!("expected a snapshot");
        };
        assert_eq!(bids.len(), 2);
        assert_eq!(
            bids[0],
            (
                Price::from_f64(46000.0).unwrap(),
                Qty::from_f64(1.5).unwrap()
            )
        );
        assert_eq!(
            asks[0],
            (
                Price::from_f64(46001.0).unwrap(),
                Qty::from_f64(0.75).unwrap()
            )
        );
        assert!(bids[0].0 < asks[0].0, "the book must not be crossed");
    }

    #[test]
    fn the_parsed_book_reconstructs() {
        let record = parse_l2_book(BOOK, "BTC-PERP").unwrap();
        let book =
            h5i_db_backtest::store::replay_book(std::slice::from_ref(&record), "BTC-PERP").unwrap();
        assert_eq!(
            book.best_bid().unwrap().0,
            Price::from_f64(46000.0).unwrap()
        );
        assert_eq!(
            book.best_ask().unwrap().0,
            Price::from_f64(46001.0).unwrap()
        );
    }

    #[test]
    fn malformed_payloads_are_refused_rather_than_defaulted() {
        assert!(parse_candles("not json", "X").is_err());
        assert!(parse_candles(r#"{"not":"an array"}"#, "X").is_err());
        // A missing price must not silently become zero.
        assert!(parse_candles(r#"[{"t":1,"T":2,"o":"1","h":"1","l":"1","v":"1"}]"#, "X").is_err());
        // Nor an unparseable one.
        assert!(
            parse_candles(
                r#"[{"t":1,"T":2,"o":"x","h":"1","l":"1","c":"1","v":"1"}]"#,
                "X"
            )
            .is_err()
        );
        assert!(
            parse_l2_book(r#"{"time":1,"levels":[[]]}"#, "X").is_err(),
            "needs two sides"
        );
    }

    #[test]
    fn numbers_are_accepted_whether_quoted_or_not() {
        // The API quotes them; a cached or re-serialised copy may not.
        let unquoted = r#"[{"t":1,"T":2,"o":1.5,"h":2.0,"l":1.0,"c":1.75,"v":10.0}]"#;
        let records = parse_candles(unquoted, "X").unwrap();
        let MarketEvent::Bar { open, .. } = &records[0].event else {
            panic!()
        };
        assert_eq!(*open, Price::from_f64(1.5).unwrap());
    }

    #[test]
    fn an_instrument_is_a_perpetual_named_for_its_coin() {
        let btc = instrument("BTC", 0.5, 0.0001).unwrap();
        assert_eq!(btc.id.as_str(), "BTC-PERP");
        assert_eq!(btc.venue, "hyperliquid");
        assert_eq!(btc.outcome_count(), 1, "a perp has one outcome");
        assert_eq!(btc.tick_size, Price::from_f64(0.5).unwrap());
    }
}
