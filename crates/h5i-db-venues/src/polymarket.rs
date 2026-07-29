//! Polymarket: CLOB payloads and archive rows into canonical records.
//!
//! Like the Hyperliquid module, this parses rather than fetches. The shapes
//! handled here are the ones the public CLOB websocket and the hourly
//! archives use:
//!
//! * `book` — a full snapshot for one token, `{market, asset_id, timestamp,
//!   bids: [{price, size}], asks: [...]}`;
//! * `price_change` — incremental level updates,
//!   `{..., changes: [{price, size, side}]}`, where **size zero means the
//!   level is gone**;
//! * `last_trade_price` / trade rows — prints.
//!
//! Two Polymarket-specific things shape the mapping.
//!
//! A market is **one instrument with N outcomes**, not N instruments. The
//! CLOB addresses each outcome by its own `asset_id` (token), so the loader
//! takes the token ordering as the outcome ordering and keeps the market's
//! `condition_id` as the instrument. Loading tokens as unrelated
//! instruments -- what the reference stack does -- makes the invariant that
//! defines these markets, that outcome prices sum to one, impossible to
//! state.
//!
//! And **resolution metadata never becomes market data**. A market payload
//! carries fields that give the answer away (`winner`, `closed`,
//! `umaResolutionStatus`); [`resolution_from_market`] extracts them
//! deliberately, into the resolutions table, which the strategy path never
//! reads.

use serde_json::Value;

use h5i_db_backtest::book::BookDelta;
use h5i_db_backtest::error::{BacktestError, Result};
use h5i_db_backtest::event::{MarketEvent, Record};
use h5i_db_backtest::instrument::{Instrument, InstrumentId, OutcomeId};
use h5i_db_backtest::settlement::Resolution;
use h5i_db_backtest::types::{Price, Qty, Side, Stamps, UnixNanos};

/// The public CLOB host.
pub const CLOB_URL: &str = "https://clob.polymarket.com";
/// The Gamma metadata host, which is where market definitions live.
pub const GAMMA_URL: &str = "https://gamma-api.polymarket.com";

const MS: i64 = 1_000_000;

/// Which outcome each `asset_id` (token) refers to.
///
/// Constructed from the market's token list, whose order *is* the outcome
/// order. Passing tokens in a different order than the market defines them
/// silently relabels every outcome, so this is built once per market and
/// reused.
#[derive(Clone, Debug)]
pub struct TokenMap {
    instrument: InstrumentId,
    tokens: Vec<String>,
}

impl TokenMap {
    pub fn new(instrument: impl Into<String>, tokens: Vec<String>) -> Result<Self> {
        if tokens.len() < 2 {
            return Err(BacktestError::invalid(
                "a Polymarket market has at least two outcome tokens",
            ));
        }
        let mut sorted = tokens.clone();
        sorted.sort();
        sorted.dedup();
        if sorted.len() != tokens.len() {
            return Err(BacktestError::invalid("duplicate outcome token"));
        }
        Ok(Self {
            instrument: InstrumentId::new(instrument)?,
            tokens,
        })
    }

    pub fn instrument(&self) -> &InstrumentId {
        &self.instrument
    }

    pub fn outcome(&self, asset_id: &str) -> Result<OutcomeId> {
        self.tokens
            .iter()
            .position(|token| token == asset_id)
            .map(|index| OutcomeId(index as u16))
            .ok_or_else(|| BacktestError::Parse {
                what: "polymarket asset_id",
                value: asset_id.to_string(),
            })
    }
}

fn parse_json(body: &str) -> Result<Value> {
    serde_json::from_str(body).map_err(|error| BacktestError::Parse {
        what: "polymarket payload",
        value: error.to_string(),
    })
}

/// Polymarket sends decimals as strings; parse them exactly.
fn number(value: &Value, field: &'static str) -> Result<f64> {
    match value.get(field) {
        Some(Value::String(text)) => text.parse::<f64>().map_err(|_| BacktestError::Parse {
            what: field,
            value: text.clone(),
        }),
        Some(Value::Number(number)) => number.as_f64().ok_or(BacktestError::NotFinite { what: field }),
        _ => Err(BacktestError::Parse {
            what: field,
            value: "missing".to_string(),
        }),
    }
}

/// Timestamps arrive as millisecond strings or integers.
fn timestamp_ms(value: &Value, field: &'static str) -> Result<i64> {
    match value.get(field) {
        Some(Value::String(text)) => text.parse::<i64>().map_err(|_| BacktestError::Parse {
            what: field,
            value: text.clone(),
        }),
        Some(Value::Number(number)) => number.as_i64().ok_or(BacktestError::Parse {
            what: field,
            value: number.to_string(),
        }),
        _ => Err(BacktestError::Parse {
            what: field,
            value: "missing".to_string(),
        }),
    }
}

fn text<'a>(value: &'a Value, field: &'static str) -> Result<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .ok_or(BacktestError::Parse {
            what: field,
            value: "missing".to_string(),
        })
}

fn levels(value: &Value, field: &str) -> Result<Vec<(Price, Qty)>> {
    let Some(rows) = value.get(field).and_then(Value::as_array) else {
        return Ok(Vec::new());
    };
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        out.push((
            Price::from_f64(number(row, "price")?)?,
            Qty::from_f64(number(row, "size")?)?,
        ));
    }
    Ok(out)
}

/// Parse a `book` (full snapshot) message.
pub fn parse_book(body: &str, tokens: &TokenMap) -> Result<Record> {
    let json = parse_json(body)?;
    let at = UnixNanos::new(timestamp_ms(&json, "timestamp")? * MS);
    let outcome = tokens.outcome(text(&json, "asset_id")?)?;
    Ok(Record::new(
        Stamps::immediate(at),
        tokens.instrument().clone(),
        outcome,
        MarketEvent::BookSnapshot {
            bids: levels(&json, "bids")?,
            asks: levels(&json, "asks")?,
        },
    ))
}

/// Parse a `price_change` message into one record per changed level.
///
/// A change to size zero is a **delete**, which is how the venue spells
/// "this level is gone". Writing it as a set-to-zero would leave a level in
/// the book with no size, and a book model that then treats zero as
/// tradable quantity.
pub fn parse_price_change(body: &str, tokens: &TokenMap) -> Result<Vec<Record>> {
    let json = parse_json(body)?;
    let at = UnixNanos::new(timestamp_ms(&json, "timestamp")? * MS);
    let outcome = tokens.outcome(text(&json, "asset_id")?)?;
    let changes = json
        .get("changes")
        .and_then(Value::as_array)
        .ok_or(BacktestError::Parse {
            what: "price_change.changes",
            value: "missing".to_string(),
        })?;

    let mut out = Vec::with_capacity(changes.len());
    for change in changes {
        let side = Side::parse(text(change, "side")?)?;
        let price = Price::from_f64(number(change, "price")?)?;
        let size = Qty::from_f64(number(change, "size")?)?;
        let delta = if size.is_zero() {
            BookDelta::delete(side, price)
        } else {
            BookDelta::set(side, price, size)
        };
        out.push(Record::new(
            Stamps::immediate(at),
            tokens.instrument().clone(),
            outcome,
            MarketEvent::BookDelta(delta),
        ));
    }
    Ok(out)
}

/// Parse a trade message.
pub fn parse_trade(body: &str, tokens: &TokenMap) -> Result<Record> {
    let json = parse_json(body)?;
    let at = UnixNanos::new(timestamp_ms(&json, "timestamp")? * MS);
    let outcome = tokens.outcome(text(&json, "asset_id")?)?;
    // `side` is present on some feeds and absent on others. Absent means
    // unknown, and unknown must survive as unknown: an assumed aggressor
    // biases every queue-position fill that reads it.
    let aggressor = match json.get("side") {
        Some(Value::String(text)) => Some(Side::parse(text)?),
        _ => None,
    };
    Ok(Record::new(
        Stamps::immediate(at),
        tokens.instrument().clone(),
        outcome,
        MarketEvent::Trade {
            price: Price::from_f64(number(&json, "price")?)?,
            size: Qty::from_f64(number(&json, "size")?)?,
            aggressor,
        },
    ))
}

/// Build an instrument and its token map from a Gamma market payload.
///
/// The returned instrument carries **no** resolution information, by
/// construction: this function reads only the fields a trader could see
/// while the market was live.
pub fn instrument_from_market(body: &str) -> Result<(Instrument, TokenMap)> {
    let json = parse_json(body)?;
    let condition = text(&json, "condition_id")?;
    let tokens = json
        .get("tokens")
        .and_then(Value::as_array)
        .ok_or(BacktestError::Parse {
            what: "market.tokens",
            value: "missing".to_string(),
        })?;
    if tokens.len() < 2 {
        return Err(BacktestError::invalid(format!(
            "market {condition} has {} outcome tokens; a market needs at least two",
            tokens.len()
        )));
    }

    let mut labels = Vec::with_capacity(tokens.len());
    let mut ids = Vec::with_capacity(tokens.len());
    for token in tokens {
        labels.push(text(token, "outcome")?.to_string());
        ids.push(text(token, "token_id")?.to_string());
    }

    let mut instrument = Instrument::prediction_market(condition, "polymarket", labels)?;
    if let Ok(tick) = number(&json, "minimum_tick_size") {
        instrument = instrument.with_tick_size(Price::from_f64(tick)?);
    }
    if let Some(end) = json.get("end_date_iso").and_then(Value::as_str)
        && let Some(ns) = parse_iso8601_ns(end)
    {
        instrument = instrument.with_expiration(UnixNanos::new(ns));
    }
    let map = TokenMap::new(condition, ids)?;
    Ok((instrument, map))
}

/// Extract how a market resolved, if it has.
///
/// Kept separate from [`instrument_from_market`] on purpose. These fields
/// are the answer, and a versioned store makes them easy to leak: the
/// latest row for a settled market knows the winner. Routing them to the
/// resolutions table -- which only the post-run settlement policy reads --
/// is what keeps them off the strategy path.
pub fn resolution_from_market(body: &str, observable_at: UnixNanos) -> Result<Option<Resolution>> {
    let json = parse_json(body)?;
    let condition = text(&json, "condition_id")?;
    if !json.get("closed").and_then(Value::as_bool).unwrap_or(false) {
        return Ok(None);
    }
    let tokens = json
        .get("tokens")
        .and_then(Value::as_array)
        .ok_or(BacktestError::Parse {
            what: "market.tokens",
            value: "missing".to_string(),
        })?;
    let winners: Vec<usize> = tokens
        .iter()
        .enumerate()
        .filter(|(_, token)| token.get("winner").and_then(Value::as_bool).unwrap_or(false))
        .map(|(index, _)| index)
        .collect();
    match winners.len() {
        0 => Ok(None),
        1 => Ok(Some(Resolution::new(
            InstrumentId::new(condition)?,
            OutcomeId(winners[0] as u16),
            observable_at,
        ))),
        // Two winners is not an ambiguity to resolve by picking one; it is
        // a market that did not resolve the way this model assumes.
        _ => Err(BacktestError::invalid(format!(
            "market {condition} reports {} winning outcomes",
            winners.len()
        ))),
    }
}

/// Minimal RFC3339 to nanoseconds, for the `...Z` form Gamma emits.
fn parse_iso8601_ns(text: &str) -> Option<i64> {
    // Deliberately narrow: only the exact shape the API uses is accepted,
    // and anything else yields None rather than a plausible wrong instant.
    let bytes = text.as_bytes();
    if bytes.len() < 20 || bytes[10] != b'T' || !text.ends_with('Z') {
        return None;
    }
    let year: i64 = text[0..4].parse().ok()?;
    let month: i64 = text[5..7].parse().ok()?;
    let day: i64 = text[8..10].parse().ok()?;
    let hour: i64 = text[11..13].parse().ok()?;
    let minute: i64 = text[14..16].parse().ok()?;
    let second: i64 = text[17..19].parse().ok()?;
    let days = days_from_civil(year, month, day);
    Some(((days * 86_400) + hour * 3_600 + minute * 60 + second) * 1_000_000_000)
}

/// Howard Hinnant's civil-to-days algorithm.
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let mp = (month + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[cfg(test)]
mod tests {
    use super::*;

    const MARKET: &str = r#"{
        "condition_id": "0xabc",
        "minimum_tick_size": "0.001",
        "end_date_iso": "2024-11-05T00:00:00Z",
        "closed": false,
        "tokens": [
            {"token_id": "111", "outcome": "Yes", "winner": false},
            {"token_id": "222", "outcome": "No", "winner": false}
        ]
    }"#;

    const RESOLVED: &str = r#"{
        "condition_id": "0xabc",
        "closed": true,
        "tokens": [
            {"token_id": "111", "outcome": "Yes", "winner": true},
            {"token_id": "222", "outcome": "No", "winner": false}
        ]
    }"#;

    fn tokens() -> TokenMap {
        TokenMap::new("0xabc", vec!["111".into(), "222".into()]).unwrap()
    }

    #[test]
    fn a_market_is_one_instrument_with_outcomes() {
        let (instrument, map) = instrument_from_market(MARKET).unwrap();
        assert_eq!(instrument.id.as_str(), "0xabc");
        assert_eq!(instrument.outcome_count(), 2);
        assert_eq!(instrument.outcomes, vec!["Yes", "No"]);
        assert_eq!(instrument.tick_size, Price::from_f64(0.001).unwrap());
        // Token order is outcome order.
        assert_eq!(map.outcome("111").unwrap(), OutcomeId(0));
        assert_eq!(map.outcome("222").unwrap(), OutcomeId(1));
        assert!(map.outcome("999").is_err(), "an unknown token is an error");
    }

    #[test]
    fn categorical_markets_carry_every_outcome() {
        let body = r#"{
            "condition_id": "0xrace",
            "closed": false,
            "tokens": [
                {"token_id": "a", "outcome": "Alice", "winner": false},
                {"token_id": "b", "outcome": "Bob", "winner": false},
                {"token_id": "c", "outcome": "Carol", "winner": false}
            ]
        }"#;
        let (instrument, map) = instrument_from_market(body).unwrap();
        assert_eq!(instrument.outcome_count(), 3);
        assert_eq!(map.outcome("c").unwrap(), OutcomeId(2));
    }

    #[test]
    fn the_expiry_parses_from_the_iso_field() {
        let (instrument, _) = instrument_from_market(MARKET).unwrap();
        // 2024-11-05T00:00:00Z
        assert_eq!(
            instrument.expiration.unwrap().get(),
            1_730_764_800 * 1_000_000_000
        );
    }

    #[test]
    fn an_instrument_never_carries_the_answer() {
        // Even from a resolved market, the instrument says nothing.
        let (instrument, _) = instrument_from_market(RESOLVED).unwrap();
        assert!(instrument.settlement_observable.is_none());
        let rendered = format!("{instrument:?}");
        assert!(!rendered.contains("winner"), "{rendered}");
    }

    #[test]
    fn resolution_is_extracted_only_deliberately() {
        // An open market has no resolution to give.
        assert!(resolution_from_market(MARKET, UnixNanos::new(1)).unwrap().is_none());

        let resolution = resolution_from_market(RESOLVED, UnixNanos::new(500))
            .unwrap()
            .unwrap();
        assert_eq!(resolution.winner, OutcomeId(0));
        assert_eq!(resolution.observable_at, UnixNanos::new(500));
    }

    #[test]
    fn two_winners_is_an_error_not_a_coin_flip() {
        let body = r#"{
            "condition_id":"0xbad","closed":true,
            "tokens":[{"token_id":"1","outcome":"A","winner":true},
                      {"token_id":"2","outcome":"B","winner":true}]
        }"#;
        assert!(resolution_from_market(body, UnixNanos::new(1)).is_err());
    }

    #[test]
    fn a_book_snapshot_parses_for_the_right_outcome() {
        let body = r#"{
            "market":"0xabc","asset_id":"222","timestamp":"1700000000000",
            "bids":[{"price":"0.55","size":"100"},{"price":"0.54","size":"250"}],
            "asks":[{"price":"0.57","size":"80"}]
        }"#;
        let record = parse_book(body, &tokens()).unwrap();
        assert_eq!(record.outcome, OutcomeId(1), "asset 222 is the second outcome");
        assert_eq!(record.ts().get(), 1_700_000_000_000 * MS);
        let MarketEvent::BookSnapshot { bids, asks } = &record.event else {
            panic!()
        };
        assert_eq!(bids.len(), 2);
        assert_eq!(bids[0], (Price::from_f64(0.55).unwrap(), Qty::from_f64(100.0).unwrap()));
        assert_eq!(asks.len(), 1);
    }

    #[test]
    fn a_one_sided_book_is_allowed() {
        let body = r#"{"asset_id":"111","timestamp":1,"bids":[{"price":"0.1","size":"5"}]}"#;
        let record = parse_book(body, &tokens()).unwrap();
        let MarketEvent::BookSnapshot { asks, .. } = &record.event else {
            panic!()
        };
        assert!(asks.is_empty(), "an empty side is a state, not an error");
    }

    #[test]
    fn a_size_zero_change_is_a_delete_not_a_zero_sized_level() {
        let body = r#"{
            "asset_id":"111","timestamp":1700000000000,
            "changes":[
                {"price":"0.60","size":"120","side":"buy"},
                {"price":"0.61","size":"0","side":"sell"}
            ]
        }"#;
        let records = parse_price_change(body, &tokens()).unwrap();
        assert_eq!(records.len(), 2);
        let MarketEvent::BookDelta(set) = &records[0].event else {
            panic!()
        };
        assert_eq!(set.action, h5i_db_backtest::book::BookAction::Set);
        let MarketEvent::BookDelta(delete) = &records[1].event else {
            panic!()
        };
        assert_eq!(
            delete.action,
            h5i_db_backtest::book::BookAction::Delete,
            "size zero means the level is gone"
        );
    }

    #[test]
    fn a_trade_without_a_side_stays_unknown() {
        let known = r#"{"asset_id":"111","timestamp":1,"price":"0.5","size":"10","side":"buy"}"#;
        let unknown = r#"{"asset_id":"111","timestamp":1,"price":"0.5","size":"10"}"#;
        let MarketEvent::Trade { aggressor, .. } = parse_trade(known, &tokens()).unwrap().event
        else {
            panic!()
        };
        assert_eq!(aggressor, Some(Side::Buy));
        let MarketEvent::Trade { aggressor, .. } = parse_trade(unknown, &tokens()).unwrap().event
        else {
            panic!()
        };
        assert_eq!(aggressor, None, "unknown must survive as unknown");
    }

    #[test]
    fn token_maps_reject_shapes_that_would_relabel_outcomes() {
        assert!(TokenMap::new("m", vec!["only".into()]).is_err());
        assert!(TokenMap::new("m", vec!["a".into(), "a".into()]).is_err());
    }

    #[test]
    fn malformed_payloads_are_refused() {
        assert!(parse_book("{", &tokens()).is_err());
        assert!(parse_book(r#"{"asset_id":"111"}"#, &tokens()).is_err(), "no timestamp");
        assert!(
            parse_price_change(r#"{"asset_id":"111","timestamp":1}"#, &tokens()).is_err(),
            "no changes array"
        );
        assert!(instrument_from_market(r#"{"condition_id":"x","tokens":[]}"#).is_err());
    }

    #[test]
    fn a_reconstructed_book_matches_the_snapshot_then_the_changes() {
        let snapshot = parse_book(
            r#"{"asset_id":"111","timestamp":1,
                "bids":[{"price":"0.50","size":"100"}],
                "asks":[{"price":"0.52","size":"100"}]}"#,
            &tokens(),
        )
        .unwrap();
        let changes = parse_price_change(
            r#"{"asset_id":"111","timestamp":2,
                "changes":[{"price":"0.51","size":"50","side":"buy"},
                           {"price":"0.52","size":"0","side":"sell"}]}"#,
            &tokens(),
        )
        .unwrap();
        let mut all = vec![snapshot];
        all.extend(changes);
        let book = h5i_db_backtest::store::replay_book(&all, "0xabc").unwrap();
        assert_eq!(book.best_bid().unwrap().0, Price::from_f64(0.51).unwrap());
        assert_eq!(book.best_ask(), None, "the only ask was deleted");
    }
}
