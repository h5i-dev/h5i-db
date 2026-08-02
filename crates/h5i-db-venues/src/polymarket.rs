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
        Some(Value::Number(number)) => number
            .as_f64()
            .ok_or(BacktestError::NotFinite { what: field }),
        _ => Err(BacktestError::Parse {
            what: field,
            value: "missing".to_string(),
        }),
    }
}

/// Timestamps arrive as millisecond strings or integers.
///
/// Saturating rather than wrapping on the conversion to nanoseconds: an
/// absurd millisecond count is network input, and wrapping it would land a
/// far-future stamp somewhere in the past, where it reads as ordinary data.
fn millis_to_nanos(ms: i64) -> UnixNanos {
    UnixNanos::new(ms.saturating_mul(MS))
}

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
    let at = millis_to_nanos(timestamp_ms(&json, "timestamp")?);
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
    let at = millis_to_nanos(timestamp_ms(&json, "timestamp")?);
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
    let at = millis_to_nanos(timestamp_ms(&json, "timestamp")?);
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
    // Absent means "the payload does not say", and the default tick stands.
    // Present-but-unreadable is a different thing entirely: swallowing it
    // hands back an instrument whose tick came from nowhere, and a strategy
    // that quotes on it submits prices the venue refuses.
    match json.get("minimum_tick_size") {
        None | Some(Value::Null) => {}
        Some(_) => {
            instrument =
                instrument.with_tick_size(Price::from_f64(number(&json, "minimum_tick_size")?)?);
        }
    }
    // Gamma spells it `neg_risk` on the CLOB payload and `negRisk` on the
    // markets payload. Absent means no: a market wrongly believed to trade
    // as a set is one a strategy could mint cash out of. Either spelling
    // saying yes is yes -- taking only the last of them let a `false` under
    // one key mask a `true` under the other.
    let neg_risk = ["neg_risk", "negRisk"]
        .iter()
        .any(|key| json.get(*key).and_then(Value::as_bool).unwrap_or(false));
    instrument = instrument.with_neg_risk(neg_risk);
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
    let instrument = InstrumentId::new(condition)?;
    let outcomes = u16::try_from(tokens.len()).map_err(|_| BacktestError::Parse {
        what: "market.tokens",
        value: tokens.len().to_string(),
    })?;

    // An explicit payout vector settles the question outright, and is the
    // only form that can express a partial settlement. Gamma emits it as
    // strings, one per token, in token order.
    if let Some(rows) = json.get("payouts").and_then(Value::as_array) {
        if rows.len() != tokens.len() {
            return Err(BacktestError::invalid(format!(
                "market {instrument} has {} tokens but {} payouts",
                tokens.len(),
                rows.len()
            )));
        }
        let mut payouts = Vec::with_capacity(rows.len());
        for row in rows {
            let value = match row {
                Value::String(text) => text.parse::<f64>().map_err(|_| BacktestError::Parse {
                    what: "market.payouts",
                    value: text.clone(),
                })?,
                Value::Number(number) => number.as_f64().ok_or(BacktestError::NotFinite {
                    what: "market.payouts",
                })?,
                other => {
                    return Err(BacktestError::Parse {
                        what: "market.payouts",
                        value: other.to_string(),
                    });
                }
            };
            // Polymarket reports these as shares of the pot rather than as
            // per-contract prices, so a two-way split arrives as [1, 1]
            // rather than [0.5, 0.5]. Normalising covers both spellings and
            // is a no-op on one that already sums to a dollar.
            payouts.push(Price::from_f64(value)?);
        }
        if payouts.iter().all(|price| !price.is_positive()) {
            return Ok(None);
        }
        let normalised = h5i_db_backtest::instrument::normalise_to_one(&payouts)?;
        return Ok(Some(Resolution::split(
            instrument,
            normalised,
            observable_at,
        )?));
    }

    let winners: Vec<usize> = tokens
        .iter()
        .enumerate()
        .filter(|(_, token)| {
            token
                .get("winner")
                .and_then(Value::as_bool)
                .unwrap_or(false)
        })
        .map(|(index, _)| index)
        .collect();

    // A market UMA could not answer refunds a complete set at cost. Gamma
    // flags it, and the CLOB payload shows it as every token winning.
    // Recording it as a winner would turn a refund into a full loss for one
    // side and a doubling for the other.
    let voided = json
        .get("is_50_50_outcome")
        .and_then(Value::as_bool)
        .unwrap_or(false)
        || (!winners.is_empty() && winners.len() == tokens.len());
    if voided {
        return Ok(Some(Resolution::void(instrument, outcomes, observable_at)?));
    }

    match winners.len() {
        0 => Ok(None),
        1 => Ok(Some(Resolution::new(
            instrument,
            OutcomeId(winners[0] as u16),
            observable_at,
        ))),
        // Several winners but not all of them is neither a winner nor a
        // void, and picking one would be a guess dressed as a result.
        _ => Err(BacktestError::invalid(format!(
            "market {instrument} reports {} winning outcomes out of {}",
            winners.len(),
            tokens.len()
        ))),
    }
}

/// Minimal RFC3339 to nanoseconds, for the `...Z` form Gamma emits.
///
/// Works on bytes throughout. Slicing the `&str` at fixed offsets, as this
/// once did, panics with "byte index is not a char boundary" the moment a
/// multi-byte character straddles one of them -- and this reads a field off
/// the network, where "malformed" is a shape the caller must survive, not a
/// bug to abort on. Every rejection is a `None`, per the contract above.
fn parse_iso8601_ns(text: &str) -> Option<i64> {
    // Deliberately narrow: only the exact shape the API uses is accepted,
    // and anything else yields None rather than a plausible wrong instant.
    let bytes = text.as_bytes();
    if bytes.len() < 20 || bytes[10] != b'T' || bytes[bytes.len() - 1] != b'Z' {
        return None;
    }
    if bytes[4] != b'-' || bytes[7] != b'-' || bytes[13] != b':' || bytes[16] != b':' {
        return None;
    }
    let year = digits(bytes, 0, 4)?;
    let month = digits(bytes, 5, 2)?;
    let day = digits(bytes, 8, 2)?;
    let hour = digits(bytes, 11, 2)?;
    let minute = digits(bytes, 14, 2)?;
    let second = digits(bytes, 17, 2)?;
    // Bounds the calendar algorithm cannot check for us: it happily turns
    // month 47 into a date rather than saying no.
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    if hour > 23 || minute > 59 || second > 60 {
        return None;
    }
    let days = days_from_civil(year, month, day);
    days.checked_mul(86_400)?
        .checked_add(hour * 3_600 + minute * 60 + second)?
        .checked_mul(1_000_000_000)
}

/// `count` ASCII digits starting at `start`, or `None` for anything else.
///
/// Deliberately not `str::parse`, which accepts a leading sign and any
/// Unicode the slice happens to contain.
fn digits(bytes: &[u8], start: usize, count: usize) -> Option<i64> {
    let mut value: i64 = 0;
    for index in start..start + count {
        let byte = *bytes.get(index)?;
        if !byte.is_ascii_digit() {
            return None;
        }
        value = value * 10 + i64::from(byte - b'0');
    }
    Some(value)
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
        assert!(
            resolution_from_market(MARKET, UnixNanos::new(1))
                .unwrap()
                .is_none()
        );

        let resolution = resolution_from_market(RESOLVED, UnixNanos::new(500))
            .unwrap()
            .unwrap();
        assert_eq!(resolution.winner(), Some(OutcomeId(0)));
        assert_eq!(resolution.observable_at, UnixNanos::new(500));
    }

    #[test]
    fn every_token_winning_is_a_refund_not_a_coin_flip() {
        // This is how a voided market reaches us: UMA could not answer, the
        // venue refunds a complete set at cost, and both sides pay half.
        // Picking one as the winner would be wrong by the whole notional on
        // each side, in opposite directions.
        let body = r#"{
            "condition_id":"0xbad","closed":true,
            "tokens":[{"token_id":"1","outcome":"A","winner":true},
                      {"token_id":"2","outcome":"B","winner":true}]
        }"#;
        let resolution = resolution_from_market(body, UnixNanos::new(1))
            .unwrap()
            .unwrap();
        assert_eq!(resolution.winner(), None);
        assert_eq!(
            resolution.settlement_price(OutcomeId(0)),
            Price::from_f64(0.5).unwrap()
        );
    }

    #[test]
    fn the_fifty_fifty_flag_is_honoured_on_its_own() {
        let body = r#"{
            "condition_id":"0xbad","closed":true,"is_50_50_outcome":true,
            "tokens":[{"token_id":"1","outcome":"A","winner":true},
                      {"token_id":"2","outcome":"B","winner":false}]
        }"#;
        let resolution = resolution_from_market(body, UnixNanos::new(1))
            .unwrap()
            .unwrap();
        assert_eq!(resolution.winner(), None);
    }

    #[test]
    fn some_but_not_all_winners_is_still_an_error() {
        // Neither a winner nor a refund. Choosing between them would be a
        // guess dressed as a result.
        let body = r#"{
            "condition_id":"0xbad","closed":true,
            "tokens":[{"token_id":"1","outcome":"A","winner":true},
                      {"token_id":"2","outcome":"B","winner":true},
                      {"token_id":"3","outcome":"C","winner":false}]
        }"#;
        assert!(resolution_from_market(body, UnixNanos::new(1)).is_err());
    }

    #[test]
    fn an_explicit_payout_vector_settles_the_question() {
        // Gamma reports these as shares of the pot, so a scalar market that
        // paid 70/30 arrives as [7, 3] rather than [0.7, 0.3].
        let body = r#"{
            "condition_id":"0xscalar","closed":true,"payouts":["7","3"],
            "tokens":[{"token_id":"1","outcome":"A"},
                      {"token_id":"2","outcome":"B"}]
        }"#;
        let resolution = resolution_from_market(body, UnixNanos::new(1))
            .unwrap()
            .unwrap();
        assert_eq!(
            resolution.settlement_price(OutcomeId(0)),
            Price::from_f64(0.7).unwrap()
        );
        assert_eq!(
            resolution.settlement_price(OutcomeId(1)),
            Price::from_f64(0.3).unwrap()
        );
        assert_eq!(resolution.winner(), None);
    }

    #[test]
    fn a_payout_vector_of_the_wrong_length_is_refused() {
        let body = r#"{
            "condition_id":"0xbad","closed":true,"payouts":["1"],
            "tokens":[{"token_id":"1","outcome":"A"},
                      {"token_id":"2","outcome":"B"}]
        }"#;
        assert!(resolution_from_market(body, UnixNanos::new(1)).is_err());
    }

    #[test]
    fn a_negative_risk_market_is_recorded_as_one() {
        // Absent means no: a market wrongly believed to trade as a set is
        // one a strategy could mint cash out of.
        let plain = r#"{
            "condition_id":"0xabc",
            "tokens":[{"token_id":"111","outcome":"Yes"},
                      {"token_id":"222","outcome":"No"}]
        }"#;
        assert!(!instrument_from_market(plain).unwrap().0.neg_risk);

        for body in [
            r#"{"condition_id":"0xabc","neg_risk":true,
                "tokens":[{"token_id":"111","outcome":"A"},
                          {"token_id":"222","outcome":"B"},
                          {"token_id":"333","outcome":"C"}]}"#,
            r#"{"condition_id":"0xabc","negRisk":true,
                "tokens":[{"token_id":"111","outcome":"A"},
                          {"token_id":"222","outcome":"B"},
                          {"token_id":"333","outcome":"C"}]}"#,
        ] {
            let (instrument, _) = instrument_from_market(body).unwrap();
            assert!(instrument.neg_risk, "{body}");
            assert!(instrument.supports_complete_set());
        }
    }

    #[test]
    fn a_book_snapshot_parses_for_the_right_outcome() {
        let body = r#"{
            "market":"0xabc","asset_id":"222","timestamp":"1700000000000",
            "bids":[{"price":"0.55","size":"100"},{"price":"0.54","size":"250"}],
            "asks":[{"price":"0.57","size":"80"}]
        }"#;
        let record = parse_book(body, &tokens()).unwrap();
        assert_eq!(
            record.outcome,
            OutcomeId(1),
            "asset 222 is the second outcome"
        );
        assert_eq!(record.ts().get(), 1_700_000_000_000 * MS);
        let MarketEvent::BookSnapshot { bids, asks } = &record.event else {
            panic!()
        };
        assert_eq!(bids.len(), 2);
        assert_eq!(
            bids[0],
            (
                Price::from_f64(0.55).unwrap(),
                Qty::from_f64(100.0).unwrap()
            )
        );
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
    fn a_malformed_expiry_is_none_and_never_a_panic() {
        // The parser reads a field off the network, so every shape it does
        // not understand has to come back as None. Slicing the &str at fixed
        // byte offsets used to panic here: the multi-byte characters straddle
        // the offsets the parser reads.
        for text in [
            "",
            "2024-11-05",
            "2024-11-05T00:00:00",  // no zone
            "2024-11-05 00:00:00Z", // no T
            // Each of these three puts a two-byte character across one of the
            // fixed offsets the parser reads, which is exactly what "byte
            // index is not a char boundary" used to mean here.
            "200\u{00e9}-11-5T00:00:00Z", // across offset 4, in the year
            "2024-11-05T0\u{00e9}:00:00Z", // across offset 13, in the hour
            "2024-11-05T00:00:0\u{00e9}Z", // across offset 19, in the second
            "2024-13-05T00:00:00Z",       // month 13
            "2024-11-05T25:00:00Z",       // hour 25
            "+024-11-05T00:00:00Z",       // a sign str::parse would have taken
        ] {
            assert_eq!(parse_iso8601_ns(text), None, "{text:?}");
        }
        // And the shape it does understand still parses.
        assert_eq!(
            parse_iso8601_ns("2024-11-05T00:00:00Z"),
            Some(1_730_764_800 * 1_000_000_000)
        );
        // Fractional seconds are extra bytes before the Z, which the fixed
        // offsets simply do not read.
        assert_eq!(
            parse_iso8601_ns("2024-11-05T00:00:00.123Z"),
            Some(1_730_764_800 * 1_000_000_000)
        );
    }

    #[test]
    fn a_present_but_unreadable_tick_size_is_refused() {
        // Absent is "the payload does not say" and the default stands;
        // present-and-unreadable must not silently become the default, or a
        // strategy quotes prices the venue refuses.
        let body = r#"{
            "condition_id":"0xabc","minimum_tick_size":"not-a-number",
            "tokens":[{"token_id":"111","outcome":"Yes"},
                      {"token_id":"222","outcome":"No"}]
        }"#;
        assert!(instrument_from_market(body).is_err());
    }

    #[test]
    fn either_spelling_of_neg_risk_saying_yes_is_yes() {
        // Both keys present with different answers: the true one wins, rather
        // than whichever happens to be read last.
        let body = r#"{
            "condition_id":"0xabc","neg_risk":true,"negRisk":false,
            "tokens":[{"token_id":"111","outcome":"A"},
                      {"token_id":"222","outcome":"B"},
                      {"token_id":"333","outcome":"C"}]
        }"#;
        assert!(instrument_from_market(body).unwrap().0.neg_risk);
    }

    #[test]
    fn an_absurd_timestamp_saturates_rather_than_wrapping() {
        // Wrapping would land a far-future stamp somewhere in the past, where
        // it reads as ordinary data instead of as the nonsense it is.
        let body = format!(r#"{{"asset_id":"111","timestamp":{},"bids":[]}}"#, i64::MAX);
        let record = parse_book(&body, &tokens()).unwrap();
        assert_eq!(record.ts().get(), i64::MAX);
    }

    #[test]
    fn token_maps_reject_shapes_that_would_relabel_outcomes() {
        assert!(TokenMap::new("m", vec!["only".into()]).is_err());
        assert!(TokenMap::new("m", vec!["a".into(), "a".into()]).is_err());
    }

    #[test]
    fn malformed_payloads_are_refused() {
        assert!(parse_book("{", &tokens()).is_err());
        assert!(
            parse_book(r#"{"asset_id":"111"}"#, &tokens()).is_err(),
            "no timestamp"
        );
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
