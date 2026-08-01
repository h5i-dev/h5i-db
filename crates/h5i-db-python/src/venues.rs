//! The Rust venue parsers, reachable from Python.
//!
//! These are thin wrappers. The parsing, the sequence checking and the
//! relative-to-absolute conversion all stay in `h5i-db-venues`, so Python and
//! Rust ingest the same bytes into the same rows; a second parser written in
//! Python would be a second reading of each venue's wire format, and the two
//! would disagree on exactly the payloads nobody has recorded yet.
//!
//! Like the crate they wrap, nothing here fetches. Every function takes bytes
//! the caller already downloaded, which is what keeps credentials, pagination
//! and rate limits out of the parsing path and lets every case be tested
//! offline against a recorded payload.
//!
//! Rows come back as plain dicts keyed by the canonical column names, so a
//! caller can hand them straight to a DataFrame constructor without learning
//! an intermediate type. Fixed-point values become floats at this boundary
//! for the same reason the on-disk schema stores them that way: ordinary
//! Python arithmetic works on them without decoding anything.

use pyo3::prelude::*;
use pyo3::types::PyDict;

use h5i_db_backtest::book::BookAction;
use h5i_db_backtest::event::{MarketEvent, Record};
use h5i_db_backtest::instrument::{Instrument, InstrumentKind, PriceRule};
use h5i_db_backtest::settlement::{Payout, Resolution};
use h5i_db_backtest::types::{Price, Qty, Side, UnixNanos};
use h5i_db_venues::{kalshi, polymarket};

const KALSHI: &str = "kalshi";
const POLYMARKET: &str = "polymarket";

// -- row shapes -------------------------------------------------------------

/// Flatten book records into `book_deltas` rows.
///
/// A transcription of `h5i_db_backtest::store::write_book_events`, deliberately
/// rather than a second reading of `schema::book_deltas`: a snapshot becomes
/// one row per level, all sharing an `event_index`, with `is_last` on the last
/// of them; a delta becomes one row whose action names what to do; a gap
/// becomes a row of its own. Two mappings that drifted apart would put the
/// same feed on disk in two shapes, and only the half that came through Python
/// would be wrong.
///
/// `event_index` numbers records within this call, exactly as the Rust writer
/// numbers them within the slice it is given. A caller concatenating several
/// batches has to renumber, or the groups will collide.
fn book_rows<'py>(
    py: Python<'py>,
    records: &[Record],
    vendor: &str,
) -> PyResult<Vec<Bound<'py, PyDict>>> {
    let mut rows = Vec::with_capacity(records.len());
    for (index, record) in records.iter().enumerate() {
        let mut push = |action: &str,
                        side: Option<Side>,
                        price: Option<Price>,
                        size: Option<Qty>,
                        last: bool|
         -> PyResult<()> {
            let row = PyDict::new(py);
            row.set_item("ts_init", record.stamps.ts_init.get())?;
            row.set_item("ts_event", record.stamps.ts_event.get())?;
            row.set_item("instrument_id", record.instrument.as_str())?;
            row.set_item("outcome", record.outcome.0)?;
            row.set_item("action", action)?;
            row.set_item("side", side.map(Side::as_str))?;
            row.set_item("price", price.map(Price::to_f64))?;
            row.set_item("size", size.map(Qty::to_f64))?;
            row.set_item("event_index", index as i64)?;
            row.set_item("is_last", last)?;
            // The writer leaves this null because it does not know who sent
            // the bytes. Here we do, and a row that names its venue is what
            // makes a mixed table auditable.
            row.set_item("source_vendor", vendor)?;
            rows.push(row);
            Ok(())
        };

        match &record.event {
            MarketEvent::BookSnapshot { bids, asks } => {
                let total = bids.len() + asks.len();
                if total == 0 {
                    // An empty book is a real state, not an absence of one.
                    push("snapshot", None, None, None, true)?;
                } else {
                    let mut seen = 0;
                    for (price, size) in bids {
                        seen += 1;
                        push(
                            "snapshot",
                            Some(Side::Buy),
                            Some(*price),
                            Some(*size),
                            seen == total,
                        )?;
                    }
                    for (price, size) in asks {
                        seen += 1;
                        push(
                            "snapshot",
                            Some(Side::Sell),
                            Some(*price),
                            Some(*size),
                            seen == total,
                        )?;
                    }
                }
            }
            MarketEvent::BookDelta(delta) => {
                let action = match delta.action {
                    BookAction::Set => "set",
                    BookAction::Delete => "delete",
                    BookAction::Clear => "clear",
                };
                let (price, size) = match delta.action {
                    BookAction::Clear => (None, None),
                    BookAction::Delete => (Some(delta.price), None),
                    BookAction::Set => (Some(delta.price), Some(delta.size)),
                };
                push(action, Some(delta.side), price, size, true)?;
            }
            // A gap is the whole point of the sequence check: the caller lost
            // messages and the book cannot be reconstructed across the hole.
            // It gets its own row so that fact survives into the table rather
            // than being inferable only from a jump nobody looks for.
            MarketEvent::Gap => push("gap", None, None, None, true)?,
            other => {
                return Err(crate::invalid(format!(
                    "a {} record does not belong in book_deltas",
                    other.kind()
                )));
            }
        }
    }
    Ok(rows)
}

/// Flatten trade records into `trades` rows.
///
/// Mirrors `h5i_db_backtest::store::write_trades`. `aggressor` stays absent
/// where the vendor did not say: guessing it silently biases every fill model
/// that reads it.
fn trade_rows<'py>(
    py: Python<'py>,
    records: &[Record],
    vendor: &str,
) -> PyResult<Vec<Bound<'py, PyDict>>> {
    let mut rows = Vec::with_capacity(records.len());
    for record in records {
        let MarketEvent::Trade {
            price,
            size,
            aggressor,
        } = &record.event
        else {
            return Err(crate::invalid(format!(
                "a {} record does not belong in trades",
                record.event.kind()
            )));
        };
        let row = PyDict::new(py);
        row.set_item("ts_init", record.stamps.ts_init.get())?;
        row.set_item("ts_event", record.stamps.ts_event.get())?;
        row.set_item("instrument_id", record.instrument.as_str())?;
        row.set_item("outcome", record.outcome.0)?;
        row.set_item("price", price.to_f64())?;
        row.set_item("size", size.to_f64())?;
        row.set_item("aggressor", aggressor.map(Side::as_str))?;
        // No venue parsed here carries a per-print id yet, and a synthesised
        // one would deduplicate against nothing.
        row.set_item("trade_id", py.None())?;
        row.set_item("source_vendor", vendor)?;
        rows.push(row);
    }
    Ok(rows)
}

/// Flatten bar records into `bars` rows.
///
/// Taken from `schema::bars` field for field. There is no `write_bars` in the
/// store to mirror yet, so this is the one place a column list is read from
/// the schema rather than from an existing writer.
fn bar_rows<'py>(
    py: Python<'py>,
    records: &[Record],
    vendor: &str,
) -> PyResult<Vec<Bound<'py, PyDict>>> {
    let mut rows = Vec::with_capacity(records.len());
    for record in records {
        let MarketEvent::Bar {
            open,
            high,
            low,
            close,
            volume,
        } = &record.event
        else {
            return Err(crate::invalid(format!(
                "a {} record does not belong in bars",
                record.event.kind()
            )));
        };
        let row = PyDict::new(py);
        row.set_item("ts_init", record.stamps.ts_init.get())?;
        row.set_item("ts_event", record.stamps.ts_event.get())?;
        row.set_item("instrument_id", record.instrument.as_str())?;
        row.set_item("outcome", record.outcome.0)?;
        row.set_item("open", open.to_f64())?;
        row.set_item("high", high.to_f64())?;
        row.set_item("low", low.to_f64())?;
        row.set_item("close", close.to_f64())?;
        row.set_item("volume", volume.to_f64())?;
        row.set_item("source_vendor", vendor)?;
        rows.push(row);
    }
    Ok(rows)
}

/// One instrument as a dict.
///
/// Carries the `instruments` column names, except that the outcome labels stay
/// a list here instead of becoming one row each: the parser has no `ts_init`
/// to stamp those rows with, and inventing one would date the reference data
/// to whenever somebody happened to run the parse.
fn instrument_row<'py>(py: Python<'py>, instrument: &Instrument) -> PyResult<Bound<'py, PyDict>> {
    let row = PyDict::new(py);
    row.set_item("instrument_id", instrument.id.as_str())?;
    row.set_item("venue", instrument.venue.as_str())?;
    row.set_item(
        "kind",
        match instrument.kind {
            InstrumentKind::PredictionMarket => "prediction_market",
            InstrumentKind::Perpetual => "perpetual",
            InstrumentKind::Spot => "spot",
        },
    )?;
    row.set_item("outcomes", instrument.outcomes.clone())?;
    row.set_item("tick_size", instrument.tick_size.to_f64())?;
    row.set_item("lot_size", instrument.lot_size.to_f64())?;
    row.set_item(
        "settlement_currency",
        instrument.settlement_currency.as_str(),
    )?;
    row.set_item("expiration_ns", instrument.expiration.map(UnixNanos::get))?;
    row.set_item(
        "settlement_observable_ns",
        instrument.settlement_observable.map(UnixNanos::get),
    )?;
    row.set_item("neg_risk", instrument.neg_risk)?;
    let (figures, decimals) = match instrument.price_rule {
        PriceRule::Tick => (None, None),
        PriceRule::SignificantFigures {
            significant_figures,
            max_decimals,
        } => (Some(significant_figures), Some(max_decimals)),
    };
    row.set_item("price_significant_figures", figures)?;
    row.set_item("price_max_decimals", decimals)?;
    Ok(row)
}

/// One resolution as `resolutions` rows.
///
/// Mirrors `h5i_db_backtest::store::write_resolutions`: a winner and a void are
/// one row each, a split is one row per outcome. `kind` says which, so no row
/// has to be read in the light of another's absence.
fn resolution_rows<'py>(
    py: Python<'py>,
    resolution: &Resolution,
) -> PyResult<Vec<Bound<'py, PyDict>>> {
    let mut rows = Vec::new();
    let mut push = |kind: &str,
                    outcome: Option<u16>,
                    payout: Option<Price>,
                    outcome_count: Option<u16>|
     -> PyResult<()> {
        let row = PyDict::new(py);
        row.set_item("ts_init", resolution.observable_at.get())?;
        row.set_item("instrument_id", resolution.instrument.as_str())?;
        row.set_item("kind", kind)?;
        row.set_item("outcome", outcome)?;
        row.set_item("payout", payout.map(Price::to_f64))?;
        row.set_item("outcome_count", outcome_count)?;
        rows.push(row);
        Ok(())
    };
    match &resolution.payout {
        Payout::Winner(winner) => push("winner", Some(winner.0), None, None)?,
        Payout::Void { outcomes } => push("void", None, None, Some(*outcomes))?,
        Payout::Split(payouts) => {
            for (index, price) in payouts.iter().enumerate() {
                push("split", Some(index as u16), Some(*price), None)?;
            }
        }
    }
    Ok(rows)
}

// -- Kalshi -----------------------------------------------------------------

/// Stateful decoder for Kalshi's `orderbook_delta` WebSocket channel.
///
/// One per market ticker, and it must see that market's messages in the order
/// they arrived. It is stateful because Kalshi's deltas are *relative*: a
/// message says how much a level changed, not what it became, so the absolute
/// book only exists in whatever has been reading the stream from the last
/// snapshot onward.
#[pyclass]
pub struct KalshiOrderbookDecoder {
    inner: kalshi::OrderbookDecoder,
}

#[pymethods]
impl KalshiOrderbookDecoder {
    #[new]
    fn new(ticker: &str) -> PyResult<Self> {
        Ok(Self {
            inner: kalshi::OrderbookDecoder::new(ticker).map_err(crate::backtest_err)?,
        })
    }

    /// Whether the decoder currently holds a book it believes.
    ///
    /// False until the first snapshot, and again after a sequence gap. While
    /// false, deltas are refused rather than applied to a book that is already
    /// known to be wrong.
    #[getter]
    fn is_synced(&self) -> bool {
        self.inner.is_synced()
    }

    /// Decode one WebSocket message into `book_deltas` rows.
    ///
    /// `received_at` is when the caller's process saw the bytes, and becomes
    /// `ts_init`. It is required rather than read from a clock here because a
    /// parser that timestamps its own inputs turns a replay of a recorded
    /// capture into a different run every time.
    ///
    /// A missed sequence yields a row with `action == "gap"` and leaves the
    /// decoder unsynced. That row is the only notice a caller gets that the
    /// feed lost messages, so it is never merged into the surrounding book
    /// updates.
    fn decode<'py>(
        &mut self,
        py: Python<'py>,
        payload: &str,
        received_at: i64,
    ) -> PyResult<Vec<Bound<'py, PyDict>>> {
        let records = self
            .inner
            .decode(payload, UnixNanos::new(received_at))
            .map_err(crate::backtest_err)?;
        book_rows(py, &records, KALSHI)
    }
}

/// Parse `GET /markets/{ticker}/orderbook` into `book_deltas` snapshot rows.
#[pyfunction]
pub fn kalshi_parse_orderbook<'py>(
    py: Python<'py>,
    payload: &str,
    ticker: &str,
    received_at: i64,
) -> PyResult<Vec<Bound<'py, PyDict>>> {
    let record = kalshi::parse_orderbook(payload, ticker, UnixNanos::new(received_at))
        .map_err(crate::backtest_err)?;
    book_rows(py, std::slice::from_ref(&record), KALSHI)
}

/// Parse a page from `/markets/trades` or `/historical/trades` into `trades`
/// rows, oldest first.
#[pyfunction]
pub fn kalshi_parse_trades<'py>(
    py: Python<'py>,
    payload: &str,
) -> PyResult<Vec<Bound<'py, PyDict>>> {
    let records = kalshi::parse_trades(payload).map_err(crate::backtest_err)?;
    trade_rows(py, &records, KALSHI)
}

/// Parse market candlesticks into `bars` rows.
///
/// `period_minutes` must be 1, 60 or 1440, which is what the venue serves.
/// Synthetic carry-forward candles (null OHLC) are dropped: they are chart
/// furniture, not trades, and must not become executable bars.
#[pyfunction]
pub fn kalshi_parse_candlesticks<'py>(
    py: Python<'py>,
    payload: &str,
    ticker: &str,
    period_minutes: i64,
) -> PyResult<Vec<Bound<'py, PyDict>>> {
    let records =
        kalshi::parse_candlesticks(payload, ticker, period_minutes).map_err(crate::backtest_err)?;
    bar_rows(py, &records, KALSHI)
}

/// Parse `GET /markets/{ticker}` into `{"instrument": …, "resolution": …}`.
///
/// `resolution` is `None` while the market is open, and a list of
/// `resolutions` rows once settlement is observable. It is returned separately
/// from the instrument on purpose: those fields are the answer, and only the
/// post-run settlement policy is allowed to read them.
#[pyfunction]
pub fn kalshi_parse_market<'py>(py: Python<'py>, payload: &str) -> PyResult<Bound<'py, PyDict>> {
    let parsed = kalshi::parse_market(payload).map_err(crate::backtest_err)?;
    let out = PyDict::new(py);
    out.set_item("instrument", instrument_row(py, &parsed.instrument)?)?;
    match &parsed.resolution {
        Some(resolution) => out.set_item("resolution", resolution_rows(py, resolution)?)?,
        None => out.set_item("resolution", py.None())?,
    }
    Ok(out)
}

// -- Polymarket -------------------------------------------------------------

/// Which outcome each `asset_id` (token) refers to.
///
/// A Polymarket market is one instrument with N outcomes, and the CLOB
/// addresses each outcome by its own token. The token order *is* the outcome
/// order, so this is built once per market (ideally by
/// `polymarket_instrument_from_market`, which reads the order off the market
/// payload) and reused: passing tokens in a different order silently relabels
/// every outcome, which no later validation can catch.
#[pyclass]
pub struct PolymarketTokenMap {
    inner: polymarket::TokenMap,
}

#[pymethods]
impl PolymarketTokenMap {
    #[new]
    fn new(condition_id: &str, tokens: Vec<String>) -> PyResult<Self> {
        Ok(Self {
            inner: polymarket::TokenMap::new(condition_id, tokens).map_err(crate::backtest_err)?,
        })
    }

    #[getter]
    fn instrument_id(&self) -> String {
        self.inner.instrument().as_str().to_string()
    }

    /// The outcome index a token belongs to. An unknown token is an error, not
    /// a new outcome.
    fn outcome(&self, asset_id: &str) -> PyResult<u16> {
        Ok(self.inner.outcome(asset_id).map_err(crate::backtest_err)?.0)
    }
}

/// Parse a CLOB `book` message into `book_deltas` snapshot rows.
#[pyfunction]
pub fn polymarket_parse_book<'py>(
    py: Python<'py>,
    payload: &str,
    tokens: PyRef<'_, PolymarketTokenMap>,
) -> PyResult<Vec<Bound<'py, PyDict>>> {
    let record = polymarket::parse_book(payload, &tokens.inner).map_err(crate::backtest_err)?;
    book_rows(py, std::slice::from_ref(&record), POLYMARKET)
}

/// Parse a CLOB `price_change` message into one `book_deltas` row per level.
///
/// A change to size zero arrives as a `delete`, which is how the venue spells
/// "this level is gone". Recording it as a set-to-zero would leave a level in
/// the book with no size for a book model to treat as tradable quantity.
#[pyfunction]
pub fn polymarket_parse_price_change<'py>(
    py: Python<'py>,
    payload: &str,
    tokens: PyRef<'_, PolymarketTokenMap>,
) -> PyResult<Vec<Bound<'py, PyDict>>> {
    let records =
        polymarket::parse_price_change(payload, &tokens.inner).map_err(crate::backtest_err)?;
    book_rows(py, &records, POLYMARKET)
}

/// Parse a CLOB trade message into a `trades` row.
#[pyfunction]
pub fn polymarket_parse_trade<'py>(
    py: Python<'py>,
    payload: &str,
    tokens: PyRef<'_, PolymarketTokenMap>,
) -> PyResult<Vec<Bound<'py, PyDict>>> {
    let record = polymarket::parse_trade(payload, &tokens.inner).map_err(crate::backtest_err)?;
    trade_rows(py, std::slice::from_ref(&record), POLYMARKET)
}

/// Build an instrument and its token map from a Gamma market payload.
///
/// Returns `(instrument_dict, PolymarketTokenMap)`. The instrument carries no
/// resolution information by construction: this reads only fields a trader
/// could have seen while the market was live. Use
/// `polymarket_resolution_from_market` to extract the answer, deliberately.
#[pyfunction]
pub fn polymarket_instrument_from_market<'py>(
    py: Python<'py>,
    payload: &str,
) -> PyResult<(Bound<'py, PyDict>, PolymarketTokenMap)> {
    let (instrument, tokens) =
        polymarket::instrument_from_market(payload).map_err(crate::backtest_err)?;
    Ok((
        instrument_row(py, &instrument)?,
        PolymarketTokenMap { inner: tokens },
    ))
}

/// Extract how a market resolved, as `resolutions` rows, or `None` if it has
/// not.
///
/// `observable_at` is the first instant the result could have been seen, which
/// is what gates settlement. Kept a separate call from the instrument for the
/// reason the Rust module keeps it separate: these fields are the answer, and
/// routing them to the resolutions table is what keeps them off the strategy
/// path.
#[pyfunction]
pub fn polymarket_resolution_from_market<'py>(
    py: Python<'py>,
    payload: &str,
    observable_at: i64,
) -> PyResult<Option<Vec<Bound<'py, PyDict>>>> {
    let resolution = polymarket::resolution_from_market(payload, UnixNanos::new(observable_at))
        .map_err(crate::backtest_err)?;
    match resolution {
        Some(resolution) => Ok(Some(resolution_rows(py, &resolution)?)),
        None => Ok(None),
    }
}

/// Register everything this module exposes.
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<KalshiOrderbookDecoder>()?;
    m.add_class::<PolymarketTokenMap>()?;
    m.add_function(wrap_pyfunction!(kalshi_parse_orderbook, m)?)?;
    m.add_function(wrap_pyfunction!(kalshi_parse_trades, m)?)?;
    m.add_function(wrap_pyfunction!(kalshi_parse_candlesticks, m)?)?;
    m.add_function(wrap_pyfunction!(kalshi_parse_market, m)?)?;
    m.add_function(wrap_pyfunction!(polymarket_parse_book, m)?)?;
    m.add_function(wrap_pyfunction!(polymarket_parse_price_change, m)?)?;
    m.add_function(wrap_pyfunction!(polymarket_parse_trade, m)?)?;
    m.add_function(wrap_pyfunction!(polymarket_instrument_from_market, m)?)?;
    m.add_function(wrap_pyfunction!(polymarket_resolution_from_market, m)?)?;
    // The CLOB and Gamma hosts, so a capture script does not hardcode them
    // separately from the parsers that read what they return.
    m.add("POLYMARKET_CLOB_URL", polymarket::CLOB_URL)?;
    m.add("POLYMARKET_GAMMA_URL", polymarket::GAMMA_URL)?;
    Ok(())
}
