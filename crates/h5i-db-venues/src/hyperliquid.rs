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

use h5i_db_backtest::account::PerInstrumentMargin;
use h5i_db_backtest::error::{BacktestError, Result};
use h5i_db_backtest::event::{MarketEvent, Record};
use h5i_db_backtest::instrument::{Instrument, InstrumentId, OutcomeId, PriceRule};
use h5i_db_backtest::types::{Price, Qty, Side, Stamps, UnixNanos};

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

/// Body for the perpetual universe request.
pub fn meta_request() -> String {
    serde_json::json!({ "type": "meta" }).to_string()
}

/// Body for the universe plus its live contexts (mark, oracle, funding).
pub fn meta_and_asset_ctxs_request() -> String {
    serde_json::json!({ "type": "metaAndAssetCtxs" }).to_string()
}

/// Hyperliquid's own limit on a perpetual price's significant figures.
pub const MAX_SIGNIFICANT_FIGURES: u8 = 5;
/// Decimal places available to a perpetual price before `szDecimals`.
pub const PERP_MAX_DECIMALS: u8 = 6;
/// The same budget for spot, which gets two more places.
pub const SPOT_MAX_DECIMALS: u8 = 8;

/// One coin's entry in the perpetual universe.
///
/// These are the fields that decide what a strategy may even *send*, and
/// they are per coin rather than per venue. Hard-coding a tick and a
/// leverage across a universe -- which is what
/// [`instrument`] leaves a caller to do -- gets both wrong for most of it.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct AssetMeta {
    pub name: String,
    /// Decimal places a *size* may carry. It also sets the price grid:
    /// a price gets `6 - sz_decimals` decimals on a perpetual.
    pub sz_decimals: u8,
    /// The most leverage the venue will grant on this coin.
    pub max_leverage: u32,
    /// Whether the venue refuses cross margin here.
    pub only_isolated: bool,
    pub is_delisted: bool,
}

impl AssetMeta {
    /// How many decimal places a price on this coin may carry.
    ///
    /// Hyperliquid spends one budget on both sides of the pair: a coin whose
    /// size is fine-grained gets a coarse price, and vice versa. A universal
    /// tick cannot express that, which is why this is derived per coin.
    pub fn price_decimals(&self) -> u8 {
        PERP_MAX_DECIMALS.saturating_sub(self.sz_decimals)
    }

    /// The canonical instrument for this coin.
    pub fn instrument(&self) -> Result<Instrument> {
        let lot = 10_f64.powi(-(self.sz_decimals as i32));
        Instrument::perpetual(format!("{}-PERP", self.name), "hyperliquid")?
            .with_lot_size(Qty::from_f64(lot)?)
            .with_price_rule(PriceRule::SignificantFigures {
                significant_figures: MAX_SIGNIFICANT_FIGURES,
                max_decimals: self.price_decimals(),
            })
    }
}

/// Parse a `meta` (or the first half of a `metaAndAssetCtxs`) response.
///
/// A delisted coin is kept rather than dropped: a backtest over a window in
/// which it still traded needs its instrument, and refusing to load it
/// would be survivorship bias applied at ingestion, which is the hardest
/// place to notice it.
pub fn parse_meta(body: &str) -> Result<Vec<AssetMeta>> {
    parse_meta_value(&parse_json(body)?)
}

fn parse_meta_value(json: &Value) -> Result<Vec<AssetMeta>> {
    let universe = json
        .get("universe")
        .and_then(Value::as_array)
        .ok_or(BacktestError::Parse {
            what: "meta.universe",
            value: "missing".to_string(),
        })?;
    let mut out = Vec::with_capacity(universe.len());
    for entry in universe {
        let name = entry
            .get("name")
            .and_then(Value::as_str)
            .ok_or(BacktestError::Parse {
                what: "meta.universe[].name",
                value: "missing".to_string(),
            })?;
        let sz_decimals =
            entry
                .get("szDecimals")
                .and_then(Value::as_u64)
                .ok_or(BacktestError::Parse {
                    what: "meta.universe[].szDecimals",
                    value: "missing".to_string(),
                })?;
        if sz_decimals > PERP_MAX_DECIMALS as u64 {
            return Err(BacktestError::invalid(format!(
                "{name}: szDecimals {sz_decimals} would leave a perpetual \
                 price no decimal places at all"
            )));
        }
        let max_leverage = entry
            .get("maxLeverage")
            .and_then(Value::as_u64)
            .unwrap_or(1)
            .max(1);
        out.push(AssetMeta {
            name: name.to_string(),
            sz_decimals: sz_decimals as u8,
            max_leverage: max_leverage as u32,
            only_isolated: entry
                .get("onlyIsolated")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            is_delisted: entry
                .get("isDelisted")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        });
    }
    Ok(out)
}

/// The window Hyperliquid's fee tiers count volume over: fourteen days.
pub const FEE_VOLUME_WINDOW_NANOS: i64 = 14 * 24 * 60 * 60 * 1_000_000_000;

/// Build a margin model from the universe's per-coin `maxLeverage`.
///
/// The venue grants forty times on the majors and three on the long tail.
/// One leverage across the universe either over-margins the majors or, far
/// worse, holds a position in an illiquid coin that the venue would have
/// refused to open at that size.
///
/// Maintenance lands at half the initial requirement at maximum leverage,
/// which is the rule Hyperliquid documents.
pub fn margin_from_meta(universe: &[AssetMeta]) -> Result<PerInstrumentMargin> {
    // The fallback is the tightest leverage in the universe rather than the
    // loosest: a coin that arrives without metadata should be harder to
    // hold than the majors, not easier.
    let floor = universe
        .iter()
        .map(|asset| asset.max_leverage)
        .min()
        .unwrap_or(1)
        .max(1);
    let mut margin = PerInstrumentMargin::new(floor as f64)?;
    for asset in universe {
        margin = margin.with_leverage(
            perp_instrument_id(&asset.name)?,
            asset.max_leverage.max(1) as f64,
        )?;
    }
    Ok(margin)
}

/// Parse a `metaAndAssetCtxs` response into the universe and its reference
/// prices.
///
/// The response is a two-element array: the `meta` object, then one context
/// per coin **in universe order**. That positional pairing is the whole
/// contract, so a length mismatch is refused rather than zipped to the
/// shorter one -- a short zip silently attributes one coin's mark to
/// another.
///
/// `at` is when the snapshot was taken. The payload carries no timestamp of
/// its own, and inventing one from the wall clock is how a reference price
/// ends up stamped before the book it was read alongside.
pub fn parse_meta_and_asset_ctxs(
    body: &str,
    at: UnixNanos,
) -> Result<(Vec<AssetMeta>, Vec<Record>)> {
    let json = parse_json(body)?;
    let pair = json.as_array().ok_or(BacktestError::Parse {
        what: "metaAndAssetCtxs",
        value: "expected a two-element array".to_string(),
    })?;
    if pair.len() != 2 {
        return Err(BacktestError::Parse {
            what: "metaAndAssetCtxs",
            value: format!("expected two elements, got {}", pair.len()),
        });
    }
    let universe = parse_meta_value(&pair[0])?;
    let contexts = pair[1].as_array().ok_or(BacktestError::Parse {
        what: "metaAndAssetCtxs[1]",
        value: "expected an array of contexts".to_string(),
    })?;
    if contexts.len() != universe.len() {
        return Err(BacktestError::invalid(format!(
            "the universe has {} coins but {} contexts; the pairing is \
             positional and a short zip would attribute one coin's mark to \
             another",
            universe.len(),
            contexts.len()
        )));
    }

    let mut records = Vec::new();
    for (asset, context) in universe.iter().zip(contexts) {
        let mark = optional_number(context, "markPx")?;
        let oracle = optional_number(context, "oraclePx")?;
        if mark.is_none() && oracle.is_none() {
            continue;
        }
        records.push(Record::new(
            Stamps::immediate(at),
            perp_instrument_id(&asset.name)?,
            OutcomeId::FIRST,
            MarketEvent::Reference { mark, oracle },
        ));
    }
    Ok((universe, records))
}

/// Parse an `activeAssetCtx` websocket payload into a reference record.
///
/// The live counterpart of [`parse_meta_and_asset_ctxs`], which is what a
/// recorder captures continuously rather than polling.
pub fn parse_asset_ctx(body: &str, at: UnixNanos) -> Result<Option<Record>> {
    let json = parse_json(body)?;
    let envelope = json.get("data").unwrap_or(&json);
    let Some(coin) = envelope.get("coin").and_then(Value::as_str) else {
        return Ok(None);
    };
    let context = envelope.get("ctx").unwrap_or(envelope);
    let mark = optional_number(context, "markPx")?;
    let oracle = optional_number(context, "oraclePx")?;
    if mark.is_none() && oracle.is_none() {
        return Ok(None);
    }
    Ok(Some(Record::new(
        Stamps::immediate(at),
        perp_instrument_id(coin)?,
        OutcomeId::FIRST,
        MarketEvent::Reference { mark, oracle },
    )))
}

/// A numeric field that may be absent, and must stay absent when it is.
///
/// A missing mark is not a zero mark. Defaulting it would value every
/// position in that coin at nothing and liquidate the account.
fn optional_number(value: &Value, field: &'static str) -> Result<Option<Price>> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(_) => Ok(Some(Price::from_f64(number(value, field)?)?)),
    }
}

/// A perpetual instrument for a Hyperliquid coin, with a hand-supplied grid.
///
/// Prefer [`parse_meta`] and [`AssetMeta::instrument`]: the venue publishes
/// the grid per coin, and a hand-supplied tick is a guess that the venue
/// will disagree with on most of the universe.
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
    book_from_value(&parse_json(body)?, &InstrumentId::new(instrument_id)?)
}

fn book_from_value(json: &Value, id: &InstrumentId) -> Result<Record> {
    let at = UnixNanos::new(millis(json, "time")? * MS);
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
        id.clone(),
        OutcomeId::FIRST,
        MarketEvent::BookSnapshot {
            bids: side(0)?,
            asks: side(1)?,
        },
    ))
}

/// Which side of a trade was the aggressor.
///
/// Hyperliquid labels a print with the side of the taker: `"B"` when a
/// buyer lifted, `"A"` when a seller hit. Anything else is left as unknown
/// rather than guessed, because a guessed aggressor biases every
/// queue-position fill that reads it -- and the queue model is the whole
/// reason to carry trades at all.
fn aggressor(value: &Value) -> Option<Side> {
    match value.get("side").and_then(Value::as_str) {
        Some("B") | Some("b") => Some(Side::Buy),
        Some("A") | Some("a") => Some(Side::Sell),
        _ => None,
    }
}

/// Parse a `trades` payload into print records.
///
/// Prints are what make a *maker* fill modellable. Without them the queue
/// model has nothing to consume the size ahead of a resting order, so every
/// touched limit fills immediately -- the single most flattering assumption
/// a market-making backtest can make.
///
/// Accepts either the bare array the websocket `trades` channel sends or a
/// single trade object.
pub fn parse_trades(body: &str, instrument_id: &str) -> Result<Vec<Record>> {
    let json = parse_json(body)?;
    let id = InstrumentId::new(instrument_id)?;
    trades_from_value(&json, &id)
}

fn trades_from_value(json: &Value, id: &InstrumentId) -> Result<Vec<Record>> {
    let rows = match json {
        Value::Array(rows) => rows.as_slice(),
        Value::Object(_) => std::slice::from_ref(json),
        other => {
            return Err(BacktestError::Parse {
                what: "trades",
                value: other.to_string(),
            });
        }
    };
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let at = UnixNanos::new(millis(row, "time")? * MS);
        out.push(Record::new(
            Stamps::immediate(at),
            id.clone(),
            OutcomeId::FIRST,
            MarketEvent::Trade {
                price: Price::from_f64(number(row, "px")?)?,
                size: Qty::from_f64(number(row, "sz")?)?,
                aggressor: aggressor(row),
            },
        ));
    }
    out.sort_by_key(|record| record.ts().get());
    Ok(out)
}

/// The coin a websocket payload is about, if it names one.
fn payload_coin(data: &Value) -> Option<&str> {
    data.get("coin")
        .and_then(Value::as_str)
        .or_else(|| data.as_array()?.first()?.get("coin")?.as_str())
}

/// How a coin becomes an instrument id.
///
/// The archive and the websocket both key by coin (`"BTC"`), while the
/// canonical instrument is `"BTC-PERP"`. Naming the mapping rather than
/// hard-coding a suffix at four call sites keeps a spot universe, whose
/// coins look like `"@1"`, expressible later.
pub fn perp_instrument_id(coin: &str) -> Result<InstrumentId> {
    InstrumentId::new(format!("{coin}-PERP"))
}

/// Parse one websocket message into records, or nothing for a channel this
/// module does not model.
///
/// Handles `l2Book` and `trades`. A `subscriptionResponse`, a `pong` or any
/// other channel yields an empty vector rather than an error: a reader
/// walking a live capture must not stop at the first heartbeat.
///
/// The instrument comes from the payload's own `coin`, mapped through
/// [`perp_instrument_id`], so one call handles a capture spanning many
/// markets.
pub fn parse_ws_message(body: &str) -> Result<Vec<Record>> {
    records_from_envelope(&parse_json(body)?)
}

fn records_from_envelope(json: &Value) -> Result<Vec<Record>> {
    // Archive lines wrap the live envelope; unwrap either shape.
    let envelope = json.get("raw").unwrap_or(json);
    let Some(channel) = envelope.get("channel").and_then(Value::as_str) else {
        return Ok(Vec::new());
    };
    let Some(data) = envelope.get("data") else {
        return Ok(Vec::new());
    };
    let Some(coin) = payload_coin(data) else {
        return Ok(Vec::new());
    };
    let id = perp_instrument_id(coin)?;
    match channel {
        "l2Book" => Ok(vec![book_from_value(data, &id)?]),
        "trades" => trades_from_value(data, &id),
        _ => Ok(Vec::new()),
    }
}

/// Read a decompressed archive stream: one JSON envelope per line.
///
/// Hyperliquid publishes its history as hourly LZ4 files of newline-
/// delimited messages, and it is the only source of book history at all --
/// `candleSnapshot` returns bars, and the REST `l2Book` returns only the
/// book as it is right now. A backtest that wants depth has to read this.
///
/// Takes already-decompressed bytes so the frame format stays the caller's
/// business; [`read_archive_lz4`] is the convenience wrapper.
///
/// A line that is blank, unparseable, or on a channel this module does not
/// model is skipped and counted. An archive with a few corrupt lines is
/// normal and refusing the whole hour over one of them would be worse than
/// reporting it -- but the count is returned, not swallowed, so a caller
/// can refuse a file that is mostly junk.
pub fn read_archive<R: std::io::BufRead>(reader: R) -> Result<ArchiveRead> {
    let mut out = ArchiveRead::default();
    for line in reader.lines() {
        let line = line.map_err(|error| BacktestError::Parse {
            what: "hyperliquid archive",
            value: error.to_string(),
        })?;
        if line.trim().is_empty() {
            continue;
        }
        out.lines += 1;
        match serde_json::from_str::<Value>(&line) {
            Ok(json) => match records_from_envelope(&json) {
                Ok(records) if records.is_empty() => out.skipped += 1,
                Ok(records) => out.records.extend(records),
                Err(_) => out.malformed += 1,
            },
            Err(_) => out.malformed += 1,
        }
    }
    out.records.sort_by_key(|record| record.ts().get());
    Ok(out)
}

/// [`read_archive`], decompressing an LZ4 frame first.
pub fn read_archive_lz4(bytes: &[u8]) -> Result<ArchiveRead> {
    let decoded = lz4_flex::frame::FrameDecoder::new(bytes);
    read_archive(std::io::BufReader::new(decoded))
}

/// What one archive file yielded, including what it did not.
#[derive(Clone, Default, Debug)]
pub struct ArchiveRead {
    pub records: Vec<Record>,
    /// Non-blank lines seen.
    pub lines: u64,
    /// Lines on a channel this module does not model.
    pub skipped: u64,
    /// Lines that were not readable as an envelope.
    pub malformed: u64,
}

impl ArchiveRead {
    /// The share of lines that produced nothing.
    ///
    /// A file whose lines are mostly skipped is usually the wrong channel
    /// or the wrong decompression, and is worth refusing rather than
    /// replaying as a thin book.
    pub fn barren_ratio(&self) -> f64 {
        if self.lines == 0 {
            return 0.0;
        }
        (self.skipped + self.malformed) as f64 / self.lines as f64
    }

    /// Refuse a read whose lines mostly produced nothing.
    pub fn require_yield(&self, minimum: f64) -> Result<()> {
        let produced = 1.0 - self.barren_ratio();
        if produced < minimum {
            return Err(BacktestError::invalid(format!(
                "only {:.1}% of {} archive lines produced records ({} skipped, \
                 {} malformed); check the channel and the decompression \
                 before replaying this as market data",
                produced * 100.0,
                self.lines,
                self.skipped,
                self.malformed
            )));
        }
        Ok(())
    }
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

    const META: &str = r#"{"universe":[
        {"name":"BTC","szDecimals":5,"maxLeverage":40},
        {"name":"ETH","szDecimals":4,"maxLeverage":25,"onlyIsolated":false},
        {"name":"KPEPE","szDecimals":0,"maxLeverage":10,"onlyIsolated":true,
         "isDelisted":true}
    ]}"#;

    #[test]
    fn the_universe_carries_a_grid_per_coin_not_per_venue() {
        let universe = parse_meta(META).unwrap();
        assert_eq!(universe.len(), 3);
        // The budget is shared between size and price: BTC's fine size
        // leaves one decimal on the price, KPEPE's whole-unit size leaves
        // six. One tick across the venue is wrong for both.
        assert_eq!(universe[0].price_decimals(), 1);
        assert_eq!(universe[1].price_decimals(), 2);
        assert_eq!(universe[2].price_decimals(), 6);
        assert_eq!(universe[0].max_leverage, 40);
        assert!(universe[2].only_isolated);
    }

    #[test]
    fn an_instrument_from_metadata_accepts_what_the_venue_accepts() {
        let universe = parse_meta(META).unwrap();
        let btc = universe[0].instrument().unwrap();
        assert_eq!(btc.id.as_str(), "BTC-PERP");
        assert_eq!(btc.lot_size, Qty::from_f64(0.00001).unwrap());
        assert_eq!(btc.tick_size, Price::from_f64(0.1).unwrap());
        // Five figures spent on the integer part, so a tenth is refused
        // here and accepted on a cheaper coin.
        assert!(btc.check_price(Price::from_f64(50_000.5).unwrap()).is_err());
        assert!(btc.check_price(Price::from_units(50_001).unwrap()).is_ok());

        let kpepe = universe[2].instrument().unwrap();
        assert!(
            kpepe
                .check_price(Price::from_f64(0.001234).unwrap())
                .is_ok()
        );
        assert!(
            kpepe
                .check_price(Price::from_f64(1.001234).unwrap())
                .is_err()
        );
    }

    #[test]
    fn a_delisted_coin_is_kept_rather_than_dropped() {
        // Dropping it would be survivorship bias applied at ingestion,
        // where it is hardest to notice.
        let universe = parse_meta(META).unwrap();
        assert!(universe[2].is_delisted);
        assert!(universe[2].instrument().is_ok());
    }

    #[test]
    fn malformed_universe_entries_are_refused() {
        assert!(parse_meta(r#"{"universe":[{"szDecimals":2}]}"#).is_err());
        assert!(parse_meta(r#"{"universe":[{"name":"X"}]}"#).is_err());
        assert!(parse_meta(r#"{}"#).is_err());
        // szDecimals larger than the price budget would leave no price grid.
        assert!(parse_meta(r#"{"universe":[{"name":"X","szDecimals":9}]}"#).is_err());
    }

    #[test]
    fn the_metadata_request_bodies_match_the_documented_wire_format() {
        assert_eq!(meta_request(), r#"{"type":"meta"}"#);
        assert_eq!(
            meta_and_asset_ctxs_request(),
            r#"{"type":"metaAndAssetCtxs"}"#
        );
    }

    // -- trades ------------------------------------------------------------

    const TRADES: &str = r#"[
        {"coin":"BTC","side":"B","px":"50000.0","sz":"0.1","time":1700000002000,"tid":2},
        {"coin":"BTC","side":"A","px":"49999.0","sz":"0.2","time":1700000001000,"tid":1},
        {"coin":"BTC","px":"49998.0","sz":"0.3","time":1700000003000,"tid":3}
    ]"#;

    #[test]
    fn trades_carry_the_aggressor_the_queue_model_needs() {
        let records = parse_trades(TRADES, "BTC-PERP").unwrap();
        assert_eq!(records.len(), 3);
        // Sorted by time, not by arrival in the payload.
        assert_eq!(records[0].ts(), UnixNanos::new(1_700_000_001_000 * MS));

        let sides: Vec<Option<Side>> = records
            .iter()
            .map(|record| match record.event {
                MarketEvent::Trade { aggressor, .. } => aggressor,
                _ => panic!("expected a trade"),
            })
            .collect();
        // "A" is a seller hitting, "B" a buyer lifting, and a print with no
        // side stays unknown rather than being guessed into one.
        assert_eq!(sides, vec![Some(Side::Sell), Some(Side::Buy), None]);
    }

    #[test]
    fn a_single_trade_object_parses_like_a_batch_of_one() {
        let one = r#"{"coin":"BTC","side":"B","px":"1.5","sz":"2","time":1700000000000}"#;
        assert_eq!(parse_trades(one, "BTC-PERP").unwrap().len(), 1);
        assert!(parse_trades("7", "BTC-PERP").is_err());
    }

    // -- websocket and archive --------------------------------------------

    const WS_BOOK: &str = r#"{"channel":"l2Book","data":{"coin":"BTC","time":1700000000000,
        "levels":[[{"px":"49999.0","sz":"1.0","n":1}],[{"px":"50001.0","sz":"2.0","n":1}]]}}"#;
    const WS_TRADES: &str = r#"{"channel":"trades","data":[
        {"coin":"ETH","side":"A","px":"3000.0","sz":"1.5","time":1700000000500}]}"#;

    #[test]
    fn a_websocket_message_names_its_own_instrument() {
        // One call handles a capture spanning many markets, because the
        // coin travels in the payload rather than in the call site.
        let book = parse_ws_message(WS_BOOK).unwrap();
        assert_eq!(book.len(), 1);
        assert_eq!(book[0].instrument.as_str(), "BTC-PERP");
        assert!(matches!(book[0].event, MarketEvent::BookSnapshot { .. }));

        let trades = parse_ws_message(WS_TRADES).unwrap();
        assert_eq!(trades[0].instrument.as_str(), "ETH-PERP");
    }

    #[test]
    fn a_channel_this_module_does_not_model_yields_nothing_rather_than_failing() {
        // A reader walking a live capture must not stop at the first
        // heartbeat or subscription acknowledgement.
        for body in [
            r#"{"channel":"pong"}"#,
            r#"{"channel":"subscriptionResponse","data":{"method":"subscribe"}}"#,
            r#"{"channel":"allMids","data":{"mids":{"BTC":"50000"}}}"#,
            r#"{"not":"an envelope"}"#,
        ] {
            assert!(parse_ws_message(body).unwrap().is_empty(), "{body}");
        }
    }

    #[test]
    fn an_archive_reads_newline_delimited_envelopes_in_time_order() {
        // The archive wraps the live envelope; both shapes must read.
        let lines = format!(
            "{}\n\n{}\n{{\"time\":\"x\",\"raw\":{}}}\n",
            WS_TRADES.replace('\n', ""),
            WS_BOOK.replace('\n', ""),
            WS_TRADES.replace('\n', "")
        );
        let read = read_archive(std::io::Cursor::new(lines)).unwrap();
        assert_eq!(read.lines, 3);
        assert_eq!(read.records.len(), 3);
        assert_eq!(read.malformed, 0);
        let stamps: Vec<i64> = read.records.iter().map(|r| r.ts().get()).collect();
        assert!(stamps.windows(2).all(|pair| pair[0] <= pair[1]));
    }

    #[test]
    fn a_mostly_barren_archive_is_reported_and_can_be_refused() {
        // Usually the wrong channel or the wrong decompression. Replaying
        // it as a thin book is worse than saying so.
        let lines = "{\"channel\":\"pong\"}\nnot json\n{\"channel\":\"pong\"}\n";
        let read = read_archive(std::io::Cursor::new(lines)).unwrap();
        assert_eq!(read.lines, 3);
        assert_eq!(read.skipped, 2);
        assert_eq!(read.malformed, 1);
        assert!(read.records.is_empty());
        assert_eq!(read.barren_ratio(), 1.0);
        assert!(read.require_yield(0.5).is_err());

        let good = read_archive(std::io::Cursor::new(WS_BOOK.replace('\n', ""))).unwrap();
        assert!(good.require_yield(0.5).is_ok());
    }

    // -- reference prices --------------------------------------------------

    const META_AND_CTXS: &str = r#"[
        {"universe":[{"name":"BTC","szDecimals":5,"maxLeverage":40},
                     {"name":"ETH","szDecimals":4,"maxLeverage":25}]},
        [{"markPx":"50000.0","oraclePx":"49995.0","funding":"0.0000125"},
         {"markPx":"3000.0","oraclePx":"3000.5","funding":"-0.00001"}]
    ]"#;

    #[test]
    fn a_venue_publishes_a_mark_and_an_oracle_that_are_not_the_mid() {
        let at = UnixNanos::new(1_700_000_000 * MS);
        let (universe, references) = parse_meta_and_asset_ctxs(META_AND_CTXS, at).unwrap();
        assert_eq!(universe.len(), 2);
        assert_eq!(references.len(), 2);
        assert_eq!(references[0].instrument.as_str(), "BTC-PERP");
        assert_eq!(references[0].ts(), at);
        match references[0].event {
            MarketEvent::Reference { mark, oracle } => {
                assert_eq!(mark, Some(Price::from_f64(50_000.0).unwrap()));
                assert_eq!(oracle, Some(Price::from_f64(49_995.0).unwrap()));
            }
            _ => panic!("expected a reference"),
        }
    }

    #[test]
    fn the_universe_and_its_contexts_are_paired_positionally_or_refused() {
        // A short zip would attribute one coin's mark to another, which is
        // undetectable downstream.
        let mismatched = r#"[
            {"universe":[{"name":"BTC","szDecimals":5},{"name":"ETH","szDecimals":4}]},
            [{"markPx":"50000.0"}]
        ]"#;
        let err = parse_meta_and_asset_ctxs(mismatched, UnixNanos::new(1)).unwrap_err();
        assert!(err.to_string().contains("positional"), "{err}");
        assert!(parse_meta_and_asset_ctxs(r#"[{"universe":[]}]"#, UnixNanos::new(1)).is_err());
    }

    #[test]
    fn a_missing_mark_stays_missing_rather_than_becoming_zero() {
        // Defaulting it would value every position in that coin at nothing
        // and liquidate the account.
        let body = r#"[
            {"universe":[{"name":"BTC","szDecimals":5}]},
            [{"oraclePx":"49995.0"}]
        ]"#;
        let (_, references) = parse_meta_and_asset_ctxs(body, UnixNanos::new(1)).unwrap();
        match references[0].event {
            MarketEvent::Reference { mark, oracle } => {
                assert_eq!(mark, None);
                assert!(oracle.is_some());
            }
            _ => panic!("expected a reference"),
        }

        // A context with neither price is not a record at all.
        let empty = r#"[{"universe":[{"name":"BTC","szDecimals":5}]},[{}]]"#;
        let (_, none) = parse_meta_and_asset_ctxs(empty, UnixNanos::new(1)).unwrap();
        assert!(none.is_empty());
    }

    #[test]
    fn the_live_asset_context_parses_the_same_way() {
        let body = r#"{"channel":"activeAssetCtx","data":{"coin":"BTC",
            "ctx":{"markPx":"50000.0","oraclePx":"49995.0"}}}"#;
        let record = parse_asset_ctx(body, UnixNanos::new(7)).unwrap().unwrap();
        assert_eq!(record.instrument.as_str(), "BTC-PERP");
        assert_eq!(record.ts(), UnixNanos::new(7));
        assert!(
            parse_asset_ctx(r#"{"data":{"ctx":{}}}"#, UnixNanos::new(1))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn an_lz4_frame_round_trips_into_records() {
        // The archive is published compressed; reading it must not need a
        // shell pipeline.
        let line = WS_BOOK.replace('\n', "");
        let mut encoder = lz4_flex::frame::FrameEncoder::new(Vec::new());
        std::io::Write::write_all(&mut encoder, line.as_bytes()).unwrap();
        let compressed = encoder.finish().unwrap();

        let read = read_archive_lz4(&compressed).unwrap();
        assert_eq!(read.records.len(), 1);
        assert_eq!(read.records[0].instrument.as_str(), "BTC-PERP");
    }
}
