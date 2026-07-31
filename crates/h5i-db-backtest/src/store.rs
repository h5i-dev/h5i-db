//! Reading market data out of h5i-db and writing runs back into it.
//!
//! This is the seam that makes the kernel a *database* backtester rather
//! than a simulator that happens to be written in Rust. Records come from
//! pinned table reads, so a run is reproducible from its pin; results go
//! back as ordinary tables, so they are queryable with the same SQL as the
//! market data.
//!
//! Storage holds `f64` and the kernel holds fixed point. The conversion is
//! exact for everything the fixed-point type can represent, and
//! [`tests::float_storage_round_trips_exactly`] is the test that says so.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBuilder, Int64Array, Int64Builder, StringArray,
    StringBuilder, TimestampNanosecondArray, TimestampNanosecondBuilder, UInt8Array, UInt8Builder,
    UInt16Array, UInt16Builder,
};
use arrow::record_batch::RecordBatch;
use h5i_db_core::Database;
use h5i_db_core::database::{ReadAt, ScanOptions, WriteOptions};

use crate::book::{BookDelta, OrderBook};
use crate::decimal::{FixedBuilder, FixedEncoding, read_fixed, read_fixed_value};
use crate::engine::RunResult;
use crate::error::{BacktestError, Result};
use crate::event::{MarketEvent, Record};
use crate::instrument::{
    Instrument, InstrumentId, InstrumentKind, InstrumentSet, OutcomeId, PriceRule,
};
use crate::position::Portfolio;
use crate::schema;
use crate::settlement::{Payout, Resolution, SettlementReport};
use crate::types::{Money, Price, Qty, Raw, Side, Stamps, UnixNanos};
use crate::window::TimeWindow;

fn core_err(error: h5i_db_core::Error) -> BacktestError {
    BacktestError::invalid(error.to_string())
}

/// Create every market-data table that does not already exist.
///
/// `encoding` decides how the fixed-point columns are stored, and this is
/// the only place it is decided: a table's encoding is fixed when it is
/// created, and every writer afterwards reads it back off the table rather
/// than choosing again. `Float` is the default and what every existing table
/// uses; `Decimal` is for values that will outgrow what an `f64` holds
/// exactly, which starts at about nine million units.
///
/// Because creation is the only decision point, and it skips tables that
/// already exist, calling this before an ingest is enough to make the whole
/// pipeline write decimals -- `h5i_db_venues::write_plan` and every
/// `write_*` here will follow the tables they find:
///
/// ```no_run
/// # async fn example(db: &h5i_db_core::Database) -> h5i_db_backtest::Result<()> {
/// use h5i_db_backtest::{FixedEncoding, store};
/// store::create_market_data_tables_with(db, FixedEncoding::Decimal).await?;
/// # Ok(())
/// # }
/// ```
///
/// A table created one way and a table created the other are both readable
/// by the same build, so this is a per-database choice rather than a
/// per-binary one.
pub async fn create_market_data_tables_with(db: &Database, encoding: FixedEncoding) -> Result<()> {
    let mut tables = schema::market_data_tables(encoding);
    // The ingest log travels with the market data it describes.
    tables.push(schema::ingest_log_table());
    create_tables(db, tables).await
}

/// Create every run-output table that does not already exist.
///
/// See [`create_market_data_tables_with`] for what `encoding` decides.
pub async fn create_run_tables_with(db: &Database, encoding: FixedEncoding) -> Result<()> {
    create_tables(db, schema::run_output_tables(encoding)).await
}

/// Create every market-data table, storing fixed point as `f64`.
pub async fn create_market_data_tables(db: &Database) -> Result<()> {
    create_market_data_tables_with(db, FixedEncoding::Float).await
}

/// Create every run-output table, storing fixed point as `f64`.
pub async fn create_run_tables(db: &Database) -> Result<()> {
    create_run_tables_with(db, FixedEncoding::Float).await
}

/// Create whichever of `tables` do not exist yet, in one batch.
///
/// Batched because creating a table is metadata-bound: each one is a spec, an
/// empty manifest, a HEAD and a catalog entry, and the cost is almost entirely
/// the fsyncs behind them. Creating the run schema one table at a time was the
/// largest single item in a backtest run (`benches/replay_path.rs`), since
/// every run does it inside a fresh fork.
async fn create_tables(
    db: &Database,
    tables: Vec<(
        &'static str,
        arrow::datatypes::SchemaRef,
        h5i_db_core::spec::TableOptions,
    )>,
) -> Result<()> {
    let existing: Vec<String> = db
        .list_tables()
        .await
        .map_err(core_err)?
        .into_iter()
        .map(|entry| entry.name)
        .collect();
    let missing: Vec<(String, _, _)> = tables
        .into_iter()
        .filter(|(name, _, _)| !existing.iter().any(|e| e == name))
        .map(|(name, schema, options)| (name.to_string(), schema, options))
        .collect();
    if missing.is_empty() {
        return Ok(());
    }
    db.create_tables(missing).await.map_err(core_err)?;
    Ok(())
}

// -- column helpers ---------------------------------------------------------

fn column<'a, T: 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a T> {
    let index = batch
        .schema()
        .index_of(name)
        .map_err(|_| BacktestError::Schema {
            table: "read",
            detail: format!("missing column {name}"),
        })?;
    batch
        .column(index)
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| BacktestError::Schema {
            table: "read",
            detail: format!("column {name} has an unexpected type"),
        })
}

/// A fixed-point column, still in whatever encoding it was stored in.
///
/// Not downcast here, because the point is that the caller does not need to
/// know: [`read_fixed`] dispatches on the array. A reader written against
/// this reads a table created either way.
fn fixed<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a ArrayRef> {
    let index = batch
        .schema()
        .index_of(name)
        .map_err(|_| BacktestError::Schema {
            table: "read",
            detail: format!("missing column {name}"),
        })?;
    Ok(batch.column(index))
}

fn opt_str(array: &StringArray, row: usize) -> Option<&str> {
    array.is_valid(row).then(|| array.value(row))
}

fn opt_i64(array: &Int64Array, row: usize) -> Option<i64> {
    array.is_valid(row).then(|| array.value(row))
}

/// The `instrument_id` column of one batch, resolved to ids without one
/// allocation per row.
///
/// A batch holds thousands of rows and a handful of distinct instruments, so
/// `InstrumentId::new` per row allocates the same `Arc<str>` over and over --
/// 200k times for a single-instrument day. The archive reader took this fix
/// already (c4a5860b); the decode path is where the same waste was left.
///
/// The last hit is checked before the map because market data arrives grouped
/// by instrument far more often than not, which makes the common case a
/// pointer comparison rather than a hash.
struct Instruments<'a> {
    values: &'a StringArray,
    last: Option<(&'a str, InstrumentId)>,
    seen: std::collections::HashMap<&'a str, InstrumentId>,
}

impl<'a> Instruments<'a> {
    fn new(batch: &'a RecordBatch) -> Result<Self> {
        Ok(Self {
            values: column::<StringArray>(batch, "instrument_id")?,
            last: None,
            seen: std::collections::HashMap::new(),
        })
    }

    fn get(&mut self, row: usize) -> Result<InstrumentId> {
        // Bound to the batch, not to `&mut self`, so the cached keys outlive
        // this call.
        let name: &'a str = self.values.value(row);
        if let Some((cached, id)) = &self.last
            && *cached == name
        {
            return Ok(id.clone());
        }
        let id = match self.seen.get(name) {
            Some(id) => id.clone(),
            None => {
                let id = InstrumentId::new(name)?;
                self.seen.insert(name, id.clone());
                id
            }
        };
        self.last = Some((name, id.clone()));
        Ok(id)
    }
}

/// Scan a table that may legitimately not exist at this read point.
///
/// Two absences are facts rather than failures. A venue that publishes no
/// prints has no `trades` table, and a spot dataset has no `funding`; and a
/// table that exists today may have had no version at the pinned instant,
/// which is simply what "there was no funding data yet" looks like when
/// read from the past.
///
/// Only those two are tolerated. A corrupt segment or a schema mismatch
/// still propagates, because swallowing those is how a run silently trades
/// on half its data.
async fn scan_optional(
    db: &Database,
    table: &str,
    at: ReadAt,
    window: Option<TimeWindow>,
    columns: Option<&[&str]>,
) -> Result<Vec<RecordBatch>> {
    let options = scan_options(window, columns);
    match db.scan(table, at, options).await {
        Ok((batches, _report)) => Ok(batches),
        Err(error) if matches!(error.code(), "table_not_found" | "version_not_found") => {
            Ok(Vec::new())
        }
        Err(error) => Err(core_err(error)),
    }
}

async fn scan(
    db: &Database,
    table: &str,
    at: ReadAt,
    window: Option<TimeWindow>,
    columns: Option<&[&str]>,
) -> Result<Vec<RecordBatch>> {
    let (batches, _report) = db
        .scan(table, at, scan_options(window, columns))
        .await
        .map_err(core_err)?;
    Ok(batches)
}

/// Time bounds plus the columns the caller will actually read.
///
/// `columns` is `None` where a reader wants the whole row. Where it is set,
/// storage never decompresses the rest: `source_vendor` and `trade_id` are
/// carried for provenance and read by nobody on the replay path, and a scan
/// that materialises them pays for them on every run. A projection that
/// omits a column the decoder needs fails loudly in `column`, so the list
/// cannot silently drift away from the decoder it belongs to.
/// Exactly what [`read_book_events`] reads. `source_vendor` is not in it.
const BOOK_COLUMNS: &[&str] = &[
    "ts_init",
    "ts_event",
    "instrument_id",
    "outcome",
    "action",
    "side",
    "price",
    "size",
    "event_index",
    "is_last",
];

/// Exactly what the trade decoders read: no `trade_id`, no `source_vendor`.
const TRADE_COLUMNS: &[&str] = &[
    "ts_init",
    "ts_event",
    "instrument_id",
    "outcome",
    "price",
    "size",
    "aggressor",
];

const FUNDING_COLUMNS: &[&str] = &["ts_init", "ts_event", "instrument_id", "rate"];

const REFERENCE_COLUMNS: &[&str] = &[
    "ts_init",
    "ts_event",
    "instrument_id",
    "outcome",
    "mark",
    "oracle",
];

fn scan_options(window: Option<TimeWindow>, columns: Option<&[&str]>) -> ScanOptions {
    ScanOptions {
        time_start: window.map(|w| w.start().get()),
        // ScanOptions::time_end is exclusive, which is exactly what a
        // half-open window means. No adjustment, and none to get wrong.
        time_end: window.map(|w| w.end().get()),
        projection: columns.map(|c| c.iter().map(|name| (*name).to_string()).collect()),
        ..Default::default()
    }
}

// -- instruments ------------------------------------------------------------

/// Write instrument reference data: one row per outcome.
pub async fn write_instruments(
    db: &Database,
    instruments: &[Instrument],
    known_at: UnixNanos,
) -> Result<()> {
    let encoding = encoding_of(db, schema::INSTRUMENTS).await;
    let mut ts = TimestampNanosecondBuilder::new();
    let mut id = StringBuilder::new();
    let mut venue = StringBuilder::new();
    let mut kind = StringBuilder::new();
    let mut outcome = UInt16Builder::new();
    let mut label = StringBuilder::new();
    let mut tick = FixedBuilder::new(encoding);
    let mut lot = FixedBuilder::new(encoding);
    let mut expiration = Int64Builder::new();
    let mut observable = Int64Builder::new();
    let mut neg_risk = BooleanBuilder::new();
    let mut significant_figures = UInt8Builder::new();
    let mut max_decimals = UInt8Builder::new();

    for instrument in instruments {
        for (index, name) in instrument.outcomes.iter().enumerate() {
            ts.append_value(known_at.get());
            id.append_value(instrument.id.as_str());
            venue.append_value(&instrument.venue);
            kind.append_value(kind_name(&instrument.kind));
            outcome.append_value(index as u16);
            label.append_value(name);
            tick.append(instrument.tick_size.raw());
            lot.append(instrument.lot_size.raw());
            match instrument.expiration {
                Some(at) => expiration.append_value(at.get()),
                None => expiration.append_null(),
            }
            match instrument.settlement_observable {
                Some(at) => observable.append_value(at.get()),
                None => observable.append_null(),
            }
            neg_risk.append_value(instrument.neg_risk);
            match instrument.price_rule {
                PriceRule::Tick => {
                    significant_figures.append_null();
                    max_decimals.append_null();
                }
                PriceRule::SignificantFigures {
                    significant_figures: figures,
                    max_decimals: decimals,
                } => {
                    significant_figures.append_value(figures);
                    max_decimals.append_value(decimals);
                }
            }
        }
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(ts.finish()),
        Arc::new(id.finish()),
        Arc::new(venue.finish()),
        Arc::new(kind.finish()),
        Arc::new(outcome.finish()),
        Arc::new(label.finish()),
        tick.finish(),
        lot.finish(),
        Arc::new(expiration.finish()),
        Arc::new(observable.finish()),
        Arc::new(neg_risk.finish()),
        Arc::new(significant_figures.finish()),
        Arc::new(max_decimals.finish()),
    ];
    append(
        db,
        schema::INSTRUMENTS,
        schema::instruments(encoding),
        columns,
    )
    .await
}

fn kind_name(kind: &InstrumentKind) -> &'static str {
    match kind {
        InstrumentKind::PredictionMarket => "prediction_market",
        InstrumentKind::Perpetual => "perpetual",
        InstrumentKind::Spot => "spot",
    }
}

fn parse_kind(text: &str) -> Result<InstrumentKind> {
    match text {
        "prediction_market" => Ok(InstrumentKind::PredictionMarket),
        "perpetual" => Ok(InstrumentKind::Perpetual),
        "spot" => Ok(InstrumentKind::Spot),
        other => Err(BacktestError::Parse {
            what: "instrument kind",
            value: other.to_string(),
        }),
    }
}

/// Read instruments back, reassembling outcomes into whole instruments.
pub async fn read_instruments(db: &Database, at: ReadAt) -> Result<InstrumentSet> {
    let batches = scan(db, schema::INSTRUMENTS, at, None, None).await?;
    // Outcome rows arrive in whatever order the segments hold them, so
    // collect by (instrument, outcome index) and assemble in index order.
    let mut collected: BTreeMap<String, InstrumentDraft> = BTreeMap::new();
    for batch in &batches {
        let id = column::<StringArray>(batch, "instrument_id")?;
        let venue = column::<StringArray>(batch, "venue")?;
        let kind = column::<StringArray>(batch, "kind")?;
        let outcome = column::<UInt16Array>(batch, "outcome")?;
        let label = column::<StringArray>(batch, "outcome_label")?;
        let tick = fixed(batch, "tick_size")?;
        let lot = fixed(batch, "lot_size")?;
        let expiration = column::<Int64Array>(batch, "expiration_ns")?;
        let observable = column::<Int64Array>(batch, "settlement_observable_ns")?;
        // Absent for rows written before the column existed; false is the
        // reading that cannot invent a mintable set.
        let neg_risk = column::<BooleanArray>(batch, "neg_risk").ok();
        let significant_figures = column::<UInt8Array>(batch, "price_significant_figures").ok();
        let max_decimals = column::<UInt8Array>(batch, "price_max_decimals").ok();

        for row in 0..batch.num_rows() {
            // Read outside the closure: decoding a fixed-point column can
            // fail, and `?` does not cross into an `or_insert_with`.
            let tick_size = read_fixed_value(tick, row, "tick_size")?;
            let lot_size = read_fixed_value(lot, row, "lot_size")?;
            let draft = collected
                .entry(id.value(row).to_string())
                .or_insert_with(|| InstrumentDraft {
                    venue: venue.value(row).to_string(),
                    kind: kind.value(row).to_string(),
                    outcomes: BTreeMap::new(),
                    tick: tick_size,
                    lot: lot_size,
                    expiration: opt_i64(expiration, row),
                    observable: opt_i64(observable, row),
                    neg_risk: neg_risk
                        .map(|column| column.is_valid(row) && column.value(row))
                        .unwrap_or(false),
                    price_rule: match (significant_figures, max_decimals) {
                        (Some(figures), Some(decimals))
                            if figures.is_valid(row) && decimals.is_valid(row) =>
                        {
                            Some((figures.value(row), decimals.value(row)))
                        }
                        _ => None,
                    },
                });
            draft
                .outcomes
                .insert(outcome.value(row), label.value(row).to_string());
        }
    }

    let mut set = InstrumentSet::new();
    for (id, draft) in collected {
        let kind = parse_kind(&draft.kind)?;
        let expected = draft.outcomes.keys().copied().max().map(|m| m as usize + 1);
        if expected != Some(draft.outcomes.len()) {
            return Err(BacktestError::invalid(format!(
                "instrument {id} has gaps in its outcome indices: {:?}",
                draft.outcomes.keys().collect::<Vec<_>>()
            )));
        }
        let outcomes: Vec<String> = draft.outcomes.into_values().collect();
        let mut instrument = match kind {
            InstrumentKind::PredictionMarket => {
                Instrument::prediction_market(id.clone(), draft.venue.clone(), outcomes)?
            }
            InstrumentKind::Perpetual => Instrument::perpetual(id.clone(), draft.venue.clone())?,
            InstrumentKind::Spot => {
                let mut spot = Instrument::perpetual(id.clone(), draft.venue.clone())?;
                spot.kind = InstrumentKind::Spot;
                spot
            }
        };
        // The rule comes first: it fixes the tick at the finest legal
        // increment, and the stored tick must win over that default so a
        // venue quoting coarser than its own rule round-trips faithfully.
        if let Some((significant_figures, max_decimals)) = draft.price_rule {
            instrument = instrument.with_price_rule(PriceRule::SignificantFigures {
                significant_figures,
                max_decimals,
            })?;
        }
        instrument.tick_size = Price::from_raw(draft.tick);
        instrument.lot_size = Qty::from_raw(draft.lot);
        instrument.expiration = draft.expiration.map(UnixNanos::new);
        instrument.settlement_observable = draft.observable.map(UnixNanos::new);
        instrument.neg_risk = draft.neg_risk;
        set.insert(instrument)?;
    }
    Ok(set)
}

struct InstrumentDraft {
    venue: String,
    kind: String,
    outcomes: BTreeMap<u16, String>,
    tick: Raw,
    lot: Raw,
    expiration: Option<i64>,
    observable: Option<i64>,
    neg_risk: bool,
    price_rule: Option<(u8, u8)>,
}

// -- book events ------------------------------------------------------------

/// Write book records (snapshots, deltas, gaps).
///
/// A snapshot becomes one row per level, all sharing an `event_index`, with
/// `is_last` on the final row. Applying half a snapshot would leave a
/// crossed book, so the grouping is what lets the reader refuse a truncated
/// one instead of reconstructing nonsense from it.
pub async fn write_book_events(db: &Database, records: &[Record]) -> Result<()> {
    let encoding = encoding_of(db, schema::BOOK_DELTAS).await;
    let mut ts_init = TimestampNanosecondBuilder::new();
    let mut ts_event = TimestampNanosecondBuilder::new();
    let mut instrument = StringBuilder::new();
    let mut outcome = UInt16Builder::new();
    let mut action = StringBuilder::new();
    let mut side = StringBuilder::new();
    let mut price = FixedBuilder::new(encoding);
    let mut size = FixedBuilder::new(encoding);
    let mut event_index = Int64Builder::new();
    let mut is_last = BooleanBuilder::new();
    let mut vendor = StringBuilder::new();

    for (index, record) in records.iter().enumerate() {
        let mut push = |action_name: &str,
                        side_value: Option<Side>,
                        price_value: Option<Price>,
                        size_value: Option<Qty>,
                        last: bool| {
            ts_init.append_value(record.stamps.ts_init.get());
            ts_event.append_value(record.stamps.ts_event.get());
            instrument.append_value(record.instrument.as_str());
            outcome.append_value(record.outcome.0);
            action.append_value(action_name);
            match side_value {
                Some(value) => side.append_value(value.as_str()),
                None => side.append_null(),
            }
            match price_value {
                Some(value) => price.append(value.raw()),
                None => price.append_null(),
            }
            match size_value {
                Some(value) => size.append(value.raw()),
                None => size.append_null(),
            }
            event_index.append_value(index as i64);
            is_last.append_value(last);
            vendor.append_null();
        };

        match &record.event {
            MarketEvent::BookSnapshot { bids, asks } => {
                let total = bids.len() + asks.len();
                if total == 0 {
                    // An empty book is a real state and must survive the
                    // round trip, so it gets a row of its own.
                    push("snapshot", None, None, None, true);
                } else {
                    let mut seen = 0;
                    for (p, q) in bids {
                        seen += 1;
                        push(
                            "snapshot",
                            Some(Side::Buy),
                            Some(*p),
                            Some(*q),
                            seen == total,
                        );
                    }
                    for (p, q) in asks {
                        seen += 1;
                        push(
                            "snapshot",
                            Some(Side::Sell),
                            Some(*p),
                            Some(*q),
                            seen == total,
                        );
                    }
                }
            }
            MarketEvent::BookDelta(delta) => {
                let name = match delta.action {
                    crate::book::BookAction::Set => "set",
                    crate::book::BookAction::Delete => "delete",
                    crate::book::BookAction::Clear => "clear",
                };
                let (p, q) = match delta.action {
                    crate::book::BookAction::Clear => (None, None),
                    crate::book::BookAction::Delete => (Some(delta.price), None),
                    crate::book::BookAction::Set => (Some(delta.price), Some(delta.size)),
                };
                push(name, Some(delta.side), p, q, true);
            }
            MarketEvent::Gap => push("gap", None, None, None, true),
            MarketEvent::Trade { .. }
            | MarketEvent::Bar { .. }
            | MarketEvent::Funding { .. }
            | MarketEvent::Reference { .. }
            | MarketEvent::Corporate(_) => {
                return Err(BacktestError::invalid(
                    "trades, bars, funding, reference prices and corporate \
                     actions belong in their own tables, not book_deltas",
                ));
            }
        }
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(ts_init.finish()),
        Arc::new(ts_event.finish()),
        Arc::new(instrument.finish()),
        Arc::new(outcome.finish()),
        Arc::new(action.finish()),
        Arc::new(side.finish()),
        price.finish(),
        size.finish(),
        Arc::new(event_index.finish()),
        Arc::new(is_last.finish()),
        Arc::new(vendor.finish()),
    ];
    append(
        db,
        schema::BOOK_DELTAS,
        schema::book_deltas(encoding),
        columns,
    )
    .await
}

/// Read book records back, regrouping snapshot levels by `event_index`.
pub async fn read_book_events(
    db: &Database,
    at: ReadAt,
    window: Option<TimeWindow>,
) -> Result<Vec<Record>> {
    let batches = scan_optional(db, schema::BOOK_DELTAS, at, window, Some(BOOK_COLUMNS)).await?;
    let mut out: Vec<Record> = Vec::new();
    // Snapshot levels accumulate here until their `is_last` row arrives.
    type PendingSnapshot = (
        i64,
        Stamps,
        InstrumentId,
        OutcomeId,
        Vec<(Price, Qty)>,
        Vec<(Price, Qty)>,
    );
    let mut pending: Option<PendingSnapshot> = None;

    for batch in &batches {
        let ts_init = column::<TimestampNanosecondArray>(batch, "ts_init")?;
        let ts_event = column::<TimestampNanosecondArray>(batch, "ts_event")?;
        let mut instruments = Instruments::new(batch)?;
        let outcome = column::<UInt16Array>(batch, "outcome")?;
        let action = column::<StringArray>(batch, "action")?;
        let side = column::<StringArray>(batch, "side")?;
        let price = fixed(batch, "price")?;
        let size = fixed(batch, "size")?;
        let event_index = column::<Int64Array>(batch, "event_index")?;
        let is_last = column::<BooleanArray>(batch, "is_last")?;

        for row in 0..batch.num_rows() {
            let stamps = Stamps::new(
                UnixNanos::new(ts_event.value(row)),
                UnixNanos::new(ts_init.value(row)),
            )?;
            let id = instruments.get(row)?;
            let out_id = OutcomeId(outcome.value(row));

            match action.value(row) {
                "snapshot" => {
                    let index = event_index.value(row);
                    let entry = pending.get_or_insert_with(|| {
                        (index, stamps, id.clone(), out_id, Vec::new(), Vec::new())
                    });
                    if entry.0 != index {
                        return Err(BacktestError::invalid(format!(
                            "snapshot event {} was not terminated by an \
                             is_last row before event {index} began",
                            entry.0
                        )));
                    }
                    // The instrument and outcome come from the event's first
                    // row, so rows that disagree would be folded into that
                    // book silently: a YES book carrying NO levels, whose
                    // best ask belongs to the other outcome. One event is one
                    // book, and a row saying otherwise is a broken feed.
                    if entry.2 != id || entry.3 != out_id {
                        return Err(BacktestError::invalid(format!(
                            "snapshot event {index} mixes {}/{} with {}/{}; \
                             one event describes one outcome of one instrument",
                            entry.2.as_str(),
                            entry.3.0,
                            id.as_str(),
                            out_id.0
                        )));
                    }
                    if let (Some(side_text), Some(p), Some(q)) = (
                        opt_str(side, row),
                        read_fixed(price, row, "price")?,
                        read_fixed(size, row, "size")?,
                    ) {
                        let level = (Price::from_raw(p), Qty::from_raw(q));
                        match Side::parse(side_text)? {
                            Side::Buy => entry.4.push(level),
                            Side::Sell => entry.5.push(level),
                        }
                    }
                    if is_last.value(row) {
                        let (_, stamps, id, out_id, bids, asks) =
                            pending.take().expect("just inserted");
                        out.push(Record::new(
                            stamps,
                            id,
                            out_id,
                            MarketEvent::BookSnapshot { bids, asks },
                        ));
                    }
                }
                "gap" => out.push(Record::new(stamps, id, out_id, MarketEvent::Gap)),
                other => {
                    let side_text = opt_str(side, row).ok_or_else(|| {
                        BacktestError::invalid(format!("{other} row has no side"))
                    })?;
                    let parsed = Side::parse(side_text)?;
                    let delta =
                        match other {
                            "set" => BookDelta::set(
                                parsed,
                                Price::from_raw(read_fixed(price, row, "price")?.ok_or_else(
                                    || BacktestError::invalid("set row has no price"),
                                )?),
                                Qty::from_raw(read_fixed(size, row, "size")?.unwrap_or_default()),
                            ),
                            "delete" => BookDelta::delete(
                                parsed,
                                Price::from_raw(read_fixed(price, row, "price")?.ok_or_else(
                                    || BacktestError::invalid("delete row has no price"),
                                )?),
                            ),
                            "clear" => BookDelta::clear(parsed),
                            unknown => {
                                return Err(BacktestError::Parse {
                                    what: "book action",
                                    value: unknown.to_string(),
                                });
                            }
                        };
                    out.push(Record::new(
                        stamps,
                        id,
                        out_id,
                        MarketEvent::BookDelta(delta),
                    ));
                }
            }
        }
    }

    if let Some((index, _, _, _, _, _)) = pending {
        return Err(BacktestError::invalid(format!(
            "snapshot event {index} is truncated: no row carried is_last, so \
             the book it describes is incomplete"
        )));
    }
    out.sort_by_key(|record| record.ts().get());
    Ok(out)
}

// -- trades -----------------------------------------------------------------

pub async fn write_trades(db: &Database, records: &[Record]) -> Result<()> {
    let encoding = encoding_of(db, schema::TRADES).await;
    let mut ts_init = TimestampNanosecondBuilder::new();
    let mut ts_event = TimestampNanosecondBuilder::new();
    let mut instrument = StringBuilder::new();
    let mut outcome = UInt16Builder::new();
    let mut price = FixedBuilder::new(encoding);
    let mut size = FixedBuilder::new(encoding);
    let mut aggressor = StringBuilder::new();
    let mut trade_id = StringBuilder::new();
    let mut vendor = StringBuilder::new();

    for record in records {
        let MarketEvent::Trade {
            price: p,
            size: q,
            aggressor: side,
        } = &record.event
        else {
            return Err(BacktestError::invalid(
                "write_trades takes trade records only",
            ));
        };
        ts_init.append_value(record.stamps.ts_init.get());
        ts_event.append_value(record.stamps.ts_event.get());
        instrument.append_value(record.instrument.as_str());
        outcome.append_value(record.outcome.0);
        price.append(p.raw());
        size.append(q.raw());
        match side {
            Some(value) => aggressor.append_value(value.as_str()),
            None => aggressor.append_null(),
        }
        trade_id.append_null();
        vendor.append_null();
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(ts_init.finish()),
        Arc::new(ts_event.finish()),
        Arc::new(instrument.finish()),
        Arc::new(outcome.finish()),
        price.finish(),
        size.finish(),
        Arc::new(aggressor.finish()),
        Arc::new(trade_id.finish()),
        Arc::new(vendor.finish()),
    ];
    append(db, schema::TRADES, schema::trades(encoding), columns).await
}

pub async fn read_trades(
    db: &Database,
    at: ReadAt,
    window: Option<TimeWindow>,
) -> Result<Vec<Record>> {
    let batches = scan_optional(db, schema::TRADES, at, window, Some(TRADE_COLUMNS)).await?;
    let mut out = Vec::new();
    for batch in &batches {
        let ts_init = column::<TimestampNanosecondArray>(batch, "ts_init")?;
        let ts_event = column::<TimestampNanosecondArray>(batch, "ts_event")?;
        let mut instruments = Instruments::new(batch)?;
        let outcome = column::<UInt16Array>(batch, "outcome")?;
        let price = fixed(batch, "price")?;
        let size = fixed(batch, "size")?;
        let aggressor = column::<StringArray>(batch, "aggressor")?;
        for row in 0..batch.num_rows() {
            out.push(Record::new(
                Stamps::new(
                    UnixNanos::new(ts_event.value(row)),
                    UnixNanos::new(ts_init.value(row)),
                )?,
                instruments.get(row)?,
                OutcomeId(outcome.value(row)),
                MarketEvent::Trade {
                    price: Price::from_raw(read_fixed_value(price, row, "price")?),
                    size: Qty::from_raw(read_fixed_value(size, row, "size")?),
                    aggressor: opt_str(aggressor, row).map(Side::parse).transpose()?,
                },
            ));
        }
    }
    out.sort_by_key(|record| record.ts().get());
    Ok(out)
}

// -- funding ----------------------------------------------------------------

pub async fn write_funding(db: &Database, records: &[Record]) -> Result<()> {
    let encoding = encoding_of(db, schema::FUNDING).await;
    let mut ts_init = TimestampNanosecondBuilder::new();
    let mut ts_event = TimestampNanosecondBuilder::new();
    let mut instrument = StringBuilder::new();
    let mut rate = FixedBuilder::new(encoding);
    let mut vendor = StringBuilder::new();

    for record in records {
        let MarketEvent::Funding { rate: value } = &record.event else {
            return Err(BacktestError::invalid(
                "write_funding takes funding records only",
            ));
        };
        ts_init.append_value(record.stamps.ts_init.get());
        ts_event.append_value(record.stamps.ts_event.get());
        instrument.append_value(record.instrument.as_str());
        rate.append(value.raw());
        vendor.append_null();
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(ts_init.finish()),
        Arc::new(ts_event.finish()),
        Arc::new(instrument.finish()),
        rate.finish(),
        Arc::new(vendor.finish()),
    ];
    append(db, schema::FUNDING, schema::funding(encoding), columns).await
}

pub async fn read_funding(
    db: &Database,
    at: ReadAt,
    window: Option<TimeWindow>,
) -> Result<Vec<Record>> {
    let batches = scan_optional(db, schema::FUNDING, at, window, Some(FUNDING_COLUMNS)).await?;
    let mut out = Vec::new();
    for batch in &batches {
        let ts_init = column::<TimestampNanosecondArray>(batch, "ts_init")?;
        let ts_event = column::<TimestampNanosecondArray>(batch, "ts_event")?;
        let mut instruments = Instruments::new(batch)?;
        let rate = fixed(batch, "rate")?;
        for row in 0..batch.num_rows() {
            out.push(Record::new(
                Stamps::new(
                    UnixNanos::new(ts_event.value(row)),
                    UnixNanos::new(ts_init.value(row)),
                )?,
                instruments.get(row)?,
                OutcomeId::FIRST,
                MarketEvent::Funding {
                    rate: Price::from_raw(read_fixed_value(rate, row, "rate")?),
                },
            ));
        }
    }
    out.sort_by_key(|record| record.ts().get());
    Ok(out)
}

// -- resolutions ------------------------------------------------------------

pub async fn write_resolutions(db: &Database, resolutions: &[Resolution]) -> Result<()> {
    let encoding = encoding_of(db, schema::RESOLUTIONS).await;
    let mut ts = TimestampNanosecondBuilder::new();
    let mut instrument = StringBuilder::new();
    let mut kind = StringBuilder::new();
    let mut outcome = UInt16Builder::new();
    let mut payout = FixedBuilder::new(encoding);
    let mut outcome_count = UInt16Builder::new();

    for resolution in resolutions {
        let mut push = |kind_name: &str,
                        outcome_value: Option<u16>,
                        payout_value: Option<Price>,
                        count: Option<u16>| {
            ts.append_value(resolution.observable_at.get());
            instrument.append_value(resolution.instrument.as_str());
            kind.append_value(kind_name);
            match outcome_value {
                Some(value) => outcome.append_value(value),
                None => outcome.append_null(),
            }
            match payout_value {
                Some(value) => payout.append(value.raw()),
                None => payout.append_null(),
            }
            match count {
                Some(value) => outcome_count.append_value(value),
                None => outcome_count.append_null(),
            }
        };
        match &resolution.payout {
            Payout::Winner(winner) => push("winner", Some(winner.0), None, None),
            Payout::Void { outcomes } => push("void", None, None, Some(*outcomes)),
            Payout::Split(payouts) => {
                for (index, price) in payouts.iter().enumerate() {
                    push("split", Some(index as u16), Some(*price), None);
                }
            }
        }
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(ts.finish()),
        Arc::new(instrument.finish()),
        Arc::new(kind.finish()),
        Arc::new(outcome.finish()),
        payout.finish(),
        Arc::new(outcome_count.finish()),
    ];
    append(
        db,
        schema::RESOLUTIONS,
        schema::resolutions(encoding),
        columns,
    )
    .await
}

/// Read resolutions.
///
/// Called only by the post-run settlement policy. Nothing on the strategy
/// path reaches this function, which is the structural half of "a strategy
/// cannot see the answer".
///
/// A split arrives as one row per outcome and is reassembled by instrument,
/// with the payouts placed by their outcome index rather than by arrival
/// order, because segments hold rows in whatever order they were written.
pub async fn read_resolutions(db: &Database, at: ReadAt) -> Result<Vec<Resolution>> {
    let batches = scan_optional(db, schema::RESOLUTIONS, at, None, None).await?;
    let mut out = Vec::new();
    // Split rows, gathered by instrument until every outcome has arrived.
    let mut splits: BTreeMap<String, (i64, BTreeMap<u16, Price>)> = BTreeMap::new();

    for batch in &batches {
        let ts = column::<TimestampNanosecondArray>(batch, "ts_init")?;
        let instrument = column::<StringArray>(batch, "instrument_id")?;
        let kind = column::<StringArray>(batch, "kind")?;
        let outcome = column::<UInt16Array>(batch, "outcome")?;
        let payout = fixed(batch, "payout")?;
        let outcome_count = column::<UInt16Array>(batch, "outcome_count")?;
        for row in 0..batch.num_rows() {
            let id = instrument.value(row);
            let at = UnixNanos::new(ts.value(row));
            match kind.value(row) {
                "winner" => {
                    if !outcome.is_valid(row) {
                        return Err(BacktestError::invalid(format!(
                            "resolution for {id} names a winner but no outcome"
                        )));
                    }
                    out.push(Resolution::new(
                        InstrumentId::new(id)?,
                        OutcomeId(outcome.value(row)),
                        at,
                    ));
                }
                "void" => {
                    if !outcome_count.is_valid(row) {
                        return Err(BacktestError::invalid(format!(
                            "voided resolution for {id} does not say how many \
                             outcomes it refunds across"
                        )));
                    }
                    out.push(Resolution::void(
                        InstrumentId::new(id)?,
                        outcome_count.value(row),
                        at,
                    )?);
                }
                "split" => {
                    if !outcome.is_valid(row) || payout.is_null(row) {
                        return Err(BacktestError::invalid(format!(
                            "split resolution row for {id} is missing its \
                             outcome or payout"
                        )));
                    }
                    let entry = splits
                        .entry(id.to_string())
                        .or_insert((at.get(), BTreeMap::new()));
                    entry.1.insert(
                        outcome.value(row),
                        Price::from_raw(read_fixed_value(payout, row, "payout")?),
                    );
                }
                other => {
                    return Err(BacktestError::Parse {
                        what: "resolution kind",
                        value: other.to_string(),
                    });
                }
            }
        }
    }

    for (id, (at, payouts)) in splits {
        let expected = payouts.keys().copied().max().map(|last| last as usize + 1);
        if expected != Some(payouts.len()) {
            return Err(BacktestError::invalid(format!(
                "split resolution for {id} has gaps in its outcome indices: {:?}",
                payouts.keys().collect::<Vec<_>>()
            )));
        }
        out.push(Resolution::split(
            InstrumentId::new(id)?,
            payouts.into_values().collect(),
            UnixNanos::new(at),
        )?);
    }
    Ok(out)
}

// -- signals ----------------------------------------------------------------

/// Read a signal table as timestamped order intents.
///
/// This is what makes a Tier 1 strategy a *query result* rather than code:
/// whatever produced the table -- a factor pipeline, a notebook, an agent --
/// the kernel sees only intent, and executes it through the full matching,
/// fee and latency path.
pub async fn read_signals(
    db: &Database,
    table: &str,
    at: ReadAt,
    window: Option<TimeWindow>,
) -> Result<Vec<(UnixNanos, crate::engine::OrderRequest)>> {
    use crate::engine::OrderRequest;
    use crate::order::TimeInForce;

    let batches = scan(db, table, at, window, None).await?;
    let mut out = Vec::new();
    for batch in &batches {
        let ts = column::<TimestampNanosecondArray>(batch, "ts")?;
        let instrument = column::<StringArray>(batch, "instrument_id")?;
        let outcome = column::<UInt16Array>(batch, "outcome")?;
        let side = column::<StringArray>(batch, "side")?;
        let quantity = fixed(batch, "quantity")?;
        let kind = column::<StringArray>(batch, "kind")?;
        let limit = fixed(batch, "limit_price")?;
        let tif = column::<StringArray>(batch, "time_in_force")?;
        let tag = column::<StringArray>(batch, "tag")?;
        let reduce_only = column::<BooleanArray>(batch, "reduce_only")?;
        // Tolerated as absent so a table written before the column existed
        // still reads; false is the reading that cannot turn a taker into a
        // maker by accident.
        let post_only = column::<BooleanArray>(batch, "post_only").ok();

        for row in 0..batch.num_rows() {
            let id = InstrumentId::new(instrument.value(row))?;
            let out_id = OutcomeId(outcome.value(row));
            let parsed_side = Side::parse(side.value(row))?;
            let size = Qty::from_raw(read_fixed_value(quantity, row, "quantity")?);
            let mut request = match kind.value(row) {
                "market" => OrderRequest::market(id, out_id, parsed_side, size),
                "limit" => {
                    // A limit order without a price is a data error, not a
                    // market order: guessing which one the author meant is
                    // how a backtest quietly trades at the wrong price.
                    let price = read_fixed(limit, row, "limit_price")?.ok_or_else(|| {
                        BacktestError::invalid(format!(
                            "limit signal for {} at {} has no limit_price",
                            instrument.value(row),
                            ts.value(row)
                        ))
                    })?;
                    OrderRequest::limit(id, out_id, parsed_side, Price::from_raw(price), size)
                }
                other => {
                    return Err(BacktestError::Parse {
                        what: "signal kind",
                        value: other.to_string(),
                    });
                }
            };
            if let Some(text) = opt_str(tif, row) {
                request = request.with_time_in_force(match text {
                    "gtc" => TimeInForce::GoodTilCancel,
                    "ioc" => TimeInForce::ImmediateOrCancel,
                    "fok" => TimeInForce::FillOrKill,
                    other => {
                        return Err(BacktestError::Parse {
                            what: "time in force",
                            value: other.to_string(),
                        });
                    }
                });
            }
            if let Some(text) = opt_str(tag, row) {
                request = request.with_tag(text);
            }
            if reduce_only.is_valid(row) && reduce_only.value(row) {
                request = request.reduce_only();
            }
            if post_only.is_some_and(|column| column.is_valid(row) && column.value(row)) {
                request = request.post_only();
            }
            out.push((UnixNanos::new(ts.value(row)), request));
        }
    }
    // Signal replay requires timestamp order, and a table has none by
    // nature. Sorting here means a caller never has to think about it.
    out.sort_by_key(|(ts, _)| ts.get());
    Ok(out)
}

/// Read a lifecycle command table.
///
/// Unlike signals, commands retain a caller-defined identifier so later
/// rows can amend or cancel the exact order created by a submit row.
pub async fn read_commands(
    db: &Database,
    table: &str,
    at: ReadAt,
    window: Option<TimeWindow>,
) -> Result<Vec<(UnixNanos, crate::engine::ReplayCommand)>> {
    use crate::engine::{OrderRequest, ReplayCommand};
    use crate::order::TimeInForce;

    let batches = scan(db, table, at, window, None).await?;
    let mut out = Vec::new();
    for batch in &batches {
        let ts = column::<TimestampNanosecondArray>(batch, "ts")?;
        let action = column::<StringArray>(batch, "action")?;
        let client_id = column::<StringArray>(batch, "client_order_id")?;
        let instrument = column::<StringArray>(batch, "instrument_id")?;
        let outcome = column::<UInt16Array>(batch, "outcome")?;
        let side = column::<StringArray>(batch, "side")?;
        let quantity = fixed(batch, "quantity")?;
        let kind = column::<StringArray>(batch, "kind")?;
        let limit = fixed(batch, "limit_price")?;
        let tif = column::<StringArray>(batch, "time_in_force")?;
        let tag = column::<StringArray>(batch, "tag")?;
        let reduce_only = column::<BooleanArray>(batch, "reduce_only")?;
        // Tolerated as absent so a table written before the column existed
        // still reads; false is the reading that cannot turn a taker into a
        // maker by accident.
        let post_only = column::<BooleanArray>(batch, "post_only").ok();

        for row in 0..batch.num_rows() {
            let client_order_id = client_id.value(row).to_string();
            let command = match action.value(row) {
                "cancel" => ReplayCommand::Cancel { client_order_id },
                "amend" => {
                    let quantity = read_fixed(quantity, row, "quantity")?.map(Qty::from_raw);
                    let limit = read_fixed(limit, row, "limit_price")?.map(Price::from_raw);
                    if quantity.is_none() && limit.is_none() {
                        return Err(BacktestError::invalid(format!(
                            "amend command for {:?} at {} changes neither quantity nor limit_price",
                            client_id.value(row),
                            ts.value(row)
                        )));
                    }
                    ReplayCommand::Amend {
                        client_order_id,
                        quantity,
                        limit,
                    }
                }
                "submit" => {
                    let required = |present: bool, field: &str| {
                        present.then_some(()).ok_or_else(|| {
                            BacktestError::invalid(format!(
                                "submit command for {:?} at {} is missing {field}",
                                client_id.value(row),
                                ts.value(row)
                            ))
                        })
                    };
                    required(instrument.is_valid(row), "instrument_id")?;
                    required(outcome.is_valid(row), "outcome")?;
                    required(side.is_valid(row), "side")?;
                    required(!quantity.is_null(row), "quantity")?;
                    required(kind.is_valid(row), "kind")?;

                    let id = InstrumentId::new(instrument.value(row))?;
                    let outcome_id = OutcomeId(outcome.value(row));
                    let parsed_side = Side::parse(side.value(row))?;
                    let size = Qty::from_raw(read_fixed_value(quantity, row, "quantity")?);
                    let mut request = match kind.value(row) {
                        "market" => OrderRequest::market(id, outcome_id, parsed_side, size),
                        "limit" => {
                            let value =
                                read_fixed(limit, row, "limit_price")?.ok_or_else(|| {
                                    BacktestError::invalid(format!(
                                        "limit submit for {:?} at {} has no limit_price",
                                        client_id.value(row),
                                        ts.value(row)
                                    ))
                                })?;
                            OrderRequest::limit(
                                id,
                                outcome_id,
                                parsed_side,
                                Price::from_raw(value),
                                size,
                            )
                        }
                        other => {
                            return Err(BacktestError::Parse {
                                what: "command kind",
                                value: other.to_string(),
                            });
                        }
                    };
                    if let Some(text) = opt_str(tif, row) {
                        request = request.with_time_in_force(match text {
                            "gtc" => TimeInForce::GoodTilCancel,
                            "ioc" => TimeInForce::ImmediateOrCancel,
                            "fok" => TimeInForce::FillOrKill,
                            other => {
                                return Err(BacktestError::Parse {
                                    what: "time in force",
                                    value: other.to_string(),
                                });
                            }
                        });
                    }
                    if let Some(text) = opt_str(tag, row) {
                        request = request.with_tag(text);
                    }
                    if reduce_only.is_valid(row) && reduce_only.value(row) {
                        request = request.reduce_only();
                    }
                    if post_only.is_some_and(|column| column.is_valid(row) && column.value(row)) {
                        request = request.post_only();
                    }
                    ReplayCommand::Submit {
                        client_order_id,
                        request,
                    }
                }
                other => {
                    return Err(BacktestError::Parse {
                        what: "command action",
                        value: other.to_string(),
                    });
                }
            };
            out.push((UnixNanos::new(ts.value(row)), command));
        }
    }
    out.sort_by_key(|(ts, _)| ts.get());
    Ok(out)
}

// -- lazy sources -----------------------------------------------------------

/// Decode Arrow batches into records one batch at a time.
///
/// The batches themselves are columnar and compact; a `Record` is not. This
/// keeps at most one batch's worth of decoded records alive, so the peak
/// cost of a replay is the stored batches plus a batch of records, rather
/// than every record at once.
///
/// The remaining limit is honest and worth stating: the batches *are* held,
/// because the scan collects them. A window larger than memory still needs
/// chunking, which [`crate::window::TimeWindow::chunks`] exists for.
struct BatchDecoder<F> {
    batches: std::vec::IntoIter<RecordBatch>,
    buffer: std::collections::VecDeque<Record>,
    decode: F,
}

impl<F> Iterator for BatchDecoder<F>
where
    F: FnMut(&RecordBatch, &mut std::collections::VecDeque<Record>) -> Result<()>,
{
    type Item = Result<Record>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(record) = self.buffer.pop_front() {
                return Some(Ok(record));
            }
            let batch = self.batches.next()?;
            if let Err(error) = (self.decode)(&batch, &mut self.buffer) {
                return Some(Err(error));
            }
        }
    }
}

/// A lazy source of trade records.
pub async fn trade_source(
    db: &Database,
    at: ReadAt,
    window: Option<TimeWindow>,
) -> Result<crate::replay::RecordSource> {
    let batches = scan_optional(db, schema::TRADES, at, window, Some(TRADE_COLUMNS)).await?;
    Ok(Box::new(BatchDecoder {
        batches: batches.into_iter(),
        buffer: Default::default(),
        decode: decode_trades,
    }))
}

fn decode_trades(batch: &RecordBatch, out: &mut std::collections::VecDeque<Record>) -> Result<()> {
    let ts_init = column::<TimestampNanosecondArray>(batch, "ts_init")?;
    let ts_event = column::<TimestampNanosecondArray>(batch, "ts_event")?;
    let mut instruments = Instruments::new(batch)?;
    let outcome = column::<UInt16Array>(batch, "outcome")?;
    let price = fixed(batch, "price")?;
    let size = fixed(batch, "size")?;
    let aggressor = column::<StringArray>(batch, "aggressor")?;
    for row in 0..batch.num_rows() {
        out.push_back(Record::new(
            Stamps::new(
                UnixNanos::new(ts_event.value(row)),
                UnixNanos::new(ts_init.value(row)),
            )?,
            instruments.get(row)?,
            OutcomeId(outcome.value(row)),
            MarketEvent::Trade {
                price: Price::from_raw(read_fixed_value(price, row, "price")?),
                size: Qty::from_raw(read_fixed_value(size, row, "size")?),
                aggressor: opt_str(aggressor, row).map(Side::parse).transpose()?,
            },
        ));
    }
    Ok(())
}

/// How many trades a window holds, without decoding them.
///
/// Counting rows off the Arrow batches costs nothing, and lets a caller
/// report volume without draining the stream it is about to replay.
pub async fn count_trades(db: &Database, at: ReadAt, window: Option<TimeWindow>) -> Result<usize> {
    let batches = scan_optional(db, schema::TRADES, at, window, Some(TRADE_COLUMNS)).await?;
    Ok(batches.iter().map(|batch| batch.num_rows()).sum())
}

/// A lazy source of funding records.
pub async fn funding_source(
    db: &Database,
    at: ReadAt,
    window: Option<TimeWindow>,
) -> Result<crate::replay::RecordSource> {
    let batches = scan_optional(db, schema::FUNDING, at, window, Some(FUNDING_COLUMNS)).await?;
    Ok(Box::new(BatchDecoder {
        batches: batches.into_iter(),
        buffer: Default::default(),
        decode: decode_funding,
    }))
}

/// Stream the venue's published mark and oracle prices.
pub async fn reference_source(
    db: &Database,
    at: ReadAt,
    window: Option<TimeWindow>,
) -> Result<crate::replay::RecordSource> {
    let batches =
        scan_optional(db, schema::REFERENCES, at, window, Some(REFERENCE_COLUMNS)).await?;
    Ok(Box::new(BatchDecoder {
        batches: batches.into_iter(),
        buffer: Default::default(),
        decode: decode_reference,
    }))
}

fn decode_reference(
    batch: &RecordBatch,
    out: &mut std::collections::VecDeque<Record>,
) -> Result<()> {
    let ts_init = column::<TimestampNanosecondArray>(batch, "ts_init")?;
    let ts_event = column::<TimestampNanosecondArray>(batch, "ts_event")?;
    let mut instruments = Instruments::new(batch)?;
    let outcome = column::<UInt16Array>(batch, "outcome")?;
    let mark = fixed(batch, "mark")?;
    let oracle = fixed(batch, "oracle")?;
    for row in 0..batch.num_rows() {
        out.push_back(Record::new(
            Stamps::new(
                UnixNanos::new(ts_event.value(row)),
                UnixNanos::new(ts_init.value(row)),
            )?,
            instruments.get(row)?,
            OutcomeId(outcome.value(row)),
            MarketEvent::Reference {
                mark: opt_price(mark, row, "mark")?,
                oracle: opt_price(oracle, row, "oracle")?,
            },
        ));
    }
    Ok(())
}

fn opt_price(column: &ArrayRef, row: usize, name: &str) -> Result<Option<Price>> {
    Ok(read_fixed(column, row, name)?.map(Price::from_raw))
}

/// Write venue-published reference prices.
pub async fn write_references(db: &Database, records: &[Record]) -> Result<()> {
    let encoding = encoding_of(db, schema::REFERENCES).await;
    let mut ts_init = TimestampNanosecondBuilder::new();
    let mut ts_event = TimestampNanosecondBuilder::new();
    let mut instrument = StringBuilder::new();
    let mut outcome = UInt16Builder::new();
    let mut mark = FixedBuilder::new(encoding);
    let mut oracle = FixedBuilder::new(encoding);
    let mut vendor = StringBuilder::new();

    for record in records {
        let MarketEvent::Reference {
            mark: mark_value,
            oracle: oracle_value,
        } = &record.event
        else {
            return Err(BacktestError::invalid(
                "write_references takes reference records only",
            ));
        };
        ts_init.append_value(record.stamps.ts_init.get());
        ts_event.append_value(record.stamps.ts_event.get());
        instrument.append_value(record.instrument.as_str());
        outcome.append_value(record.outcome.0);
        match mark_value {
            Some(price) => mark.append(price.raw()),
            None => mark.append_null(),
        }
        match oracle_value {
            Some(price) => oracle.append(price.raw()),
            None => oracle.append_null(),
        }
        vendor.append_null();
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(ts_init.finish()),
        Arc::new(ts_event.finish()),
        Arc::new(instrument.finish()),
        Arc::new(outcome.finish()),
        mark.finish(),
        oracle.finish(),
        Arc::new(vendor.finish()),
    ];
    append(
        db,
        schema::REFERENCES,
        schema::references(encoding),
        columns,
    )
    .await
}

fn decode_funding(batch: &RecordBatch, out: &mut std::collections::VecDeque<Record>) -> Result<()> {
    let ts_init = column::<TimestampNanosecondArray>(batch, "ts_init")?;
    let ts_event = column::<TimestampNanosecondArray>(batch, "ts_event")?;
    let mut instruments = Instruments::new(batch)?;
    let rate = fixed(batch, "rate")?;
    for row in 0..batch.num_rows() {
        out.push_back(Record::new(
            Stamps::new(
                UnixNanos::new(ts_event.value(row)),
                UnixNanos::new(ts_init.value(row)),
            )?,
            instruments.get(row)?,
            OutcomeId::FIRST,
            MarketEvent::Funding {
                rate: Price::from_raw(read_fixed_value(rate, row, "rate")?),
            },
        ));
    }
    Ok(())
}

// -- ingest log -------------------------------------------------------------

/// One completed load, as recorded for idempotency.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct IngestEntry {
    pub ts: UnixNanos,
    pub vendor: String,
    /// Content hash of everything the load contained.
    pub digest: String,
    pub records: i64,
    pub window: Option<TimeWindow>,
    pub instruments: i64,
}

pub async fn write_ingest_log(db: &Database, entry: IngestEntry) -> Result<()> {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(TimestampNanosecondArray::from(vec![entry.ts.get()])),
        Arc::new(StringArray::from(vec![entry.vendor.as_str()])),
        Arc::new(StringArray::from(vec![entry.digest.as_str()])),
        Arc::new(Int64Array::from(vec![entry.records])),
        Arc::new(Int64Array::from(vec![
            entry.window.map(|w| w.start().get()),
        ])),
        Arc::new(Int64Array::from(vec![entry.window.map(|w| w.end().get())])),
        Arc::new(Int64Array::from(vec![entry.instruments])),
    ];
    append(db, schema::INGEST_LOG, schema::ingest_log(), columns).await
}

pub async fn read_ingest_log(db: &Database) -> Result<Vec<IngestEntry>> {
    let batches = scan_optional(db, schema::INGEST_LOG, ReadAt::Latest, None, None).await?;
    let mut out = Vec::new();
    for batch in &batches {
        let ts = column::<TimestampNanosecondArray>(batch, "ts")?;
        let vendor = column::<StringArray>(batch, "vendor")?;
        let digest = column::<StringArray>(batch, "digest")?;
        let records = column::<Int64Array>(batch, "records")?;
        let start = column::<Int64Array>(batch, "window_start_ns")?;
        let end = column::<Int64Array>(batch, "window_end_ns")?;
        let instruments = column::<Int64Array>(batch, "instruments")?;
        for row in 0..batch.num_rows() {
            let window = match (opt_i64(start, row), opt_i64(end, row)) {
                (Some(from), Some(to)) => {
                    TimeWindow::new(UnixNanos::new(from), UnixNanos::new(to)).ok()
                }
                _ => None,
            };
            out.push(IngestEntry {
                ts: UnixNanos::new(ts.value(row)),
                vendor: vendor.value(row).to_string(),
                digest: digest.value(row).to_string(),
                records: records.value(row),
                window,
                instruments: instruments.value(row),
            });
        }
    }
    Ok(out)
}

// -- run outputs ------------------------------------------------------------

/// Write a finished run's `bt_*` tables, atomically.
///
/// The five tables are one fact -- a run -- so they are committed as one
/// journaled transaction rather than five appends. That is a correctness fix
/// before it is a speed one: five sequential commits means a crash between
/// two of them leaves `bt_orders` written and `bt_fills` missing, which is a
/// run that reads as "submitted 400 orders, filled none". A transaction
/// leaves either all five or none.
///
/// It is also where a run's wall time was going. Each commit takes the
/// database-wide metadata lock, writes a manifest and fsyncs, so the cost is
/// per *commit*, not per row; five of them to store a few hundred rows was
/// measurably more expensive than replaying the market data that produced
/// them (`benches/replay_path.rs`).
pub async fn write_run(
    db: &Database,
    run_id: &str,
    config_digest: &str,
    result: &RunResult,
    settlement: &SettlementReport,
    started_at: UnixNanos,
) -> Result<()> {
    create_run_tables(db).await?;
    // Read after create, so a database whose run tables already exist in the
    // decimal encoding is written in that encoding rather than reverted.
    let encoding = encoding_of(db, schema::FILLS).await;
    let batches = vec![
        (
            schema::RUN,
            run_manifest_batch(
                run_id,
                config_digest,
                result,
                settlement,
                started_at,
                encoding,
            )?,
        ),
        (schema::ORDERS, orders_batch(result, encoding)?),
        (schema::FILLS, fills_batch(result, encoding)?),
        (
            schema::POSITIONS,
            positions_batch(result, settlement, encoding)?,
        ),
        (schema::EQUITY, equity_batch(result, encoding)?),
    ];
    commit_run_batches(db, batches).await
}

/// Commit every non-empty run table in one transaction.
///
/// Falls back to the per-table path when the rows land in the past, because
/// that is the one case a transaction cannot express: a backfill has to read
/// the affected range back and replace it, which is a different operation per
/// table. Re-running a run id into a database that already holds it is the
/// way to get there.
async fn commit_run_batches(
    db: &Database,
    batches: Vec<(&'static str, Option<RecordBatch>)>,
) -> Result<()> {
    let present: Vec<(&'static str, RecordBatch)> = batches
        .into_iter()
        .filter_map(|(table, batch)| batch.map(|batch| (table, batch)))
        .collect();
    if present.is_empty() {
        return Ok(());
    }

    let mut transaction = db.transaction();
    for (table, batch) in &present {
        transaction
            .append(table, vec![batch.clone()])
            .map_err(core_err)?;
    }
    match transaction.commit().await {
        Ok(_) => Ok(()),
        Err(error) if error.code() == "sort_order_violation" => {
            for (table, batch) in present {
                let schema = batch.schema();
                append_with(
                    db,
                    table,
                    schema,
                    batch.columns().to_vec(),
                    time_column_of(table),
                )
                .await?;
            }
            Ok(())
        }
        Err(error) => Err(core_err(error)),
    }
}

fn run_manifest_batch(
    run_id: &str,
    config_digest: &str,
    result: &RunResult,
    settlement: &SettlementReport,
    started_at: UnixNanos,
    encoding: FixedEncoding,
) -> Result<Option<RecordBatch>> {
    let warnings = settlement.warnings();
    let one = |value: Money| {
        let mut builder = FixedBuilder::new(encoding);
        builder.append(value.raw());
        builder.finish()
    };
    let columns: Vec<ArrayRef> = vec![
        Arc::new(TimestampNanosecondArray::from(vec![started_at.get()])),
        Arc::new(StringArray::from(vec![run_id])),
        Arc::new(StringArray::from(vec![config_digest])),
        one(result.starting_cash),
        one(result.final_cash),
        one(result.realized_pnl),
        one(result.commissions),
        Arc::new(Int64Array::from(vec![
            result.simulated_through.map(|t| t.get()),
        ])),
        Arc::new(Int64Array::from(vec![result.records_processed as i64])),
        Arc::new(BooleanArray::from(vec![settlement.was_applied()])),
        Arc::new(StringArray::from(vec![if warnings.is_empty() {
            None
        } else {
            Some(warnings.join("; "))
        }])),
    ];
    build(schema::RUN, schema::run(encoding), columns)
}

fn orders_batch(result: &RunResult, encoding: FixedEncoding) -> Result<Option<RecordBatch>> {
    let mut ts = TimestampNanosecondBuilder::new();
    let mut id = Int64Builder::new();
    let mut instrument = StringBuilder::new();
    let mut outcome = UInt16Builder::new();
    let mut side = StringBuilder::new();
    let mut kind = StringBuilder::new();
    let mut limit = FixedBuilder::new(encoding);
    let mut quantity = FixedBuilder::new(encoding);
    let mut filled = FixedBuilder::new(encoding);
    let mut tif = StringBuilder::new();
    let mut status = StringBuilder::new();
    let mut reject_reason = StringBuilder::new();
    let mut tag = StringBuilder::new();
    let mut reduce_only = BooleanBuilder::new();

    for order in &result.orders {
        ts.append_value(order.submitted_at.get());
        id.append_value(order.id.0 as i64);
        instrument.append_value(order.instrument.as_str());
        outcome.append_value(order.outcome.0);
        side.append_value(order.side.as_str());
        match order.limit_price() {
            Some(price) => {
                kind.append_value("limit");
                limit.append(price.raw());
            }
            None => {
                kind.append_value("market");
                limit.append_null();
            }
        }
        quantity.append(order.quantity.raw());
        filled.append(order.filled.raw());
        tif.append_value(match order.time_in_force {
            crate::order::TimeInForce::GoodTilCancel => "gtc",
            crate::order::TimeInForce::ImmediateOrCancel => "ioc",
            crate::order::TimeInForce::FillOrKill => "fok",
        });
        status.append_value(order.status.as_str());
        match &order.reject_reason {
            Some(value) => reject_reason.append_value(value),
            None => reject_reason.append_null(),
        }
        match &order.tag {
            Some(value) => tag.append_value(value),
            None => tag.append_null(),
        }
        reduce_only.append_value(order.reduce_only);
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(ts.finish()),
        Arc::new(id.finish()),
        Arc::new(instrument.finish()),
        Arc::new(outcome.finish()),
        Arc::new(side.finish()),
        Arc::new(kind.finish()),
        limit.finish(),
        quantity.finish(),
        filled.finish(),
        Arc::new(tif.finish()),
        Arc::new(status.finish()),
        Arc::new(reject_reason.finish()),
        Arc::new(tag.finish()),
        Arc::new(reduce_only.finish()),
    ];
    build(schema::ORDERS, schema::orders(encoding), columns)
}

fn fills_batch(result: &RunResult, encoding: FixedEncoding) -> Result<Option<RecordBatch>> {
    let mut ts = TimestampNanosecondBuilder::new();
    let mut order_id = Int64Builder::new();
    let mut instrument = StringBuilder::new();
    let mut outcome = UInt16Builder::new();
    let mut side = StringBuilder::new();
    let mut price = FixedBuilder::new(encoding);
    let mut quantity = FixedBuilder::new(encoding);
    let mut commission = FixedBuilder::new(encoding);
    let mut is_taker = BooleanBuilder::new();
    let mut tag = StringBuilder::new();

    for fill in &result.fills {
        ts.append_value(fill.ts.get());
        order_id.append_value(fill.order_id.0 as i64);
        instrument.append_value(fill.instrument.as_str());
        outcome.append_value(fill.outcome.0);
        side.append_value(fill.side.as_str());
        price.append(fill.price.raw());
        quantity.append(fill.quantity.raw());
        commission.append(fill.commission.raw());
        is_taker.append_value(fill.is_taker);
        match &fill.tag {
            Some(value) => tag.append_value(value),
            None => tag.append_null(),
        }
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(ts.finish()),
        Arc::new(order_id.finish()),
        Arc::new(instrument.finish()),
        Arc::new(outcome.finish()),
        Arc::new(side.finish()),
        price.finish(),
        quantity.finish(),
        commission.finish(),
        Arc::new(is_taker.finish()),
        Arc::new(tag.finish()),
    ];
    build(schema::FILLS, schema::fills(encoding), columns)
}

/// Read `bt_fills` back as [`Fill`]s, so a stored run can be re-folded into
/// positions and checked against what it claimed.
pub async fn read_fills(db: &Database, at: ReadAt) -> Result<Vec<crate::order::Fill>> {
    let batches = scan(db, schema::FILLS, at, None, None).await?;
    let mut out = Vec::new();
    for batch in &batches {
        let ts = column::<TimestampNanosecondArray>(batch, "ts")?;
        let order_id = column::<Int64Array>(batch, "order_id")?;
        let instrument = column::<StringArray>(batch, "instrument_id")?;
        let outcome = column::<UInt16Array>(batch, "outcome")?;
        let side = column::<StringArray>(batch, "side")?;
        let price = fixed(batch, "price")?;
        let quantity = fixed(batch, "quantity")?;
        let commission = fixed(batch, "commission")?;
        let is_taker = column::<BooleanArray>(batch, "is_taker")?;
        let tag = column::<StringArray>(batch, "tag")?;
        for row in 0..batch.num_rows() {
            out.push(crate::order::Fill {
                order_id: crate::order::OrderId(order_id.value(row) as u64),
                instrument: InstrumentId::new(instrument.value(row))?,
                outcome: OutcomeId(outcome.value(row)),
                side: Side::parse(side.value(row))?,
                price: Price::from_raw(read_fixed_value(price, row, "price")?),
                quantity: Qty::from_raw(read_fixed_value(quantity, row, "quantity")?),
                commission: Money::from_raw(read_fixed_value(commission, row, "commission")?),
                is_taker: is_taker.value(row),
                ts: UnixNanos::new(ts.value(row)),
                tag: opt_str(tag, row).map(str::to_string),
            });
        }
    }
    out.sort_by_key(|fill| fill.ts.get());
    Ok(out)
}

fn positions_batch(
    result: &RunResult,
    settlement: &SettlementReport,
    encoding: FixedEncoding,
) -> Result<Option<RecordBatch>> {
    let portfolio = Portfolio::replay(&result.fills)?;
    let settled: BTreeMap<(String, u16), &crate::settlement::PositionSettlement> = settlement
        .settled
        .iter()
        .map(|s| ((s.instrument.to_string(), s.outcome.0), s))
        .collect();

    let ts_value = result
        .simulated_through
        .map(|t| t.get())
        .unwrap_or_default();

    let mut ts = TimestampNanosecondBuilder::new();
    let mut instrument = StringBuilder::new();
    let mut outcome = UInt16Builder::new();
    let mut quantity = FixedBuilder::new(encoding);
    let mut average = FixedBuilder::new(encoding);
    let mut realized = FixedBuilder::new(encoding);
    let mut commissions = FixedBuilder::new(encoding);
    let mut settlement_pnl = FixedBuilder::new(encoding);
    let mut market_exit = FixedBuilder::new(encoding);

    for position in portfolio.positions() {
        ts.append_value(ts_value);
        instrument.append_value(position.instrument.as_str());
        outcome.append_value(position.outcome.0);
        quantity.append(position.quantity.raw());
        average.append(position.average_price.raw());
        realized.append(position.realized_pnl.raw());
        commissions.append(position.commissions.raw());
        match settled.get(&(position.instrument.to_string(), position.outcome.0)) {
            Some(entry) => {
                settlement_pnl.append(entry.settled_pnl.raw());
                match entry.market_exit_pnl {
                    Some(value) => market_exit.append(value.raw()),
                    None => market_exit.append_null(),
                }
            }
            None => {
                settlement_pnl.append_null();
                market_exit.append_null();
            }
        }
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(ts.finish()),
        Arc::new(instrument.finish()),
        Arc::new(outcome.finish()),
        quantity.finish(),
        average.finish(),
        realized.finish(),
        commissions.finish(),
        settlement_pnl.finish(),
        market_exit.finish(),
    ];
    build(schema::POSITIONS, schema::positions(encoding), columns)
}

fn equity_batch(result: &RunResult, encoding: FixedEncoding) -> Result<Option<RecordBatch>> {
    let mut ts = TimestampNanosecondBuilder::new();
    let mut cash = FixedBuilder::new(encoding);
    let mut position_value = FixedBuilder::new(encoding);
    let mut equity = FixedBuilder::new(encoding);
    let mut realized = FixedBuilder::new(encoding);
    let mut unrealized = FixedBuilder::new(encoding);

    for point in &result.equity {
        ts.append_value(point.ts.get());
        cash.append(point.cash.raw());
        position_value.append(point.position_value.raw());
        equity.append(point.equity.raw());
        realized.append(point.realized_pnl.raw());
        unrealized.append(point.unrealized_pnl.raw());
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(ts.finish()),
        cash.finish(),
        position_value.finish(),
        equity.finish(),
        realized.finish(),
        unrealized.finish(),
    ];
    build(schema::EQUITY, schema::equity(encoding), columns)
}

/// Assemble a batch, or `None` when there is nothing to write.
///
/// An empty run table is a legitimate outcome -- a strategy that never traded
/// has no fills -- so it is an absent batch rather than an error or a
/// zero-row commit.
fn build(
    table: &'static str,
    schema: arrow::datatypes::SchemaRef,
    columns: Vec<ArrayRef>,
) -> Result<Option<RecordBatch>> {
    if columns.first().map(|c| c.len()).unwrap_or(0) == 0 {
        return Ok(None);
    }
    RecordBatch::try_new(schema, columns)
        .map(Some)
        .map_err(|error| BacktestError::Schema {
            table,
            detail: error.to_string(),
        })
}

/// The encoding a table's fixed-point columns already use.
///
/// Read from the table rather than passed in, so a writer cannot disagree
/// with what was created. A table that does not exist yet has no encoding to
/// honour, so it takes the default and the caller's `create_*` decides.
pub(crate) async fn encoding_of(db: &Database, table: &str) -> FixedEncoding {
    let Ok(resolved) = db.resolve(table, ReadAt::Latest).await else {
        return FixedEncoding::default();
    };
    resolved
        .schema
        .fields()
        .iter()
        .find_map(|field| match FixedEncoding::of(field.data_type()) {
            // Only a fixed-point column answers the question; an Int64 or a
            // timestamp says nothing about the encoding either way.
            Some(FixedEncoding::Decimal) => Some(FixedEncoding::Decimal),
            _ => None,
        })
        .unwrap_or_default()
}

async fn append(
    db: &Database,
    table: &str,
    schema: arrow::datatypes::SchemaRef,
    columns: Vec<ArrayRef>,
) -> Result<()> {
    append_with(db, table, schema, columns, time_column_of(table)).await
}

/// Which column a table is time-indexed on.
fn time_column_of(table: &str) -> &'static str {
    if table.starts_with("bt_")
        || table == schema::INGEST_LOG
        || table == schema::SIGNALS
        || table == schema::COMMANDS
    {
        "ts"
    } else {
        "ts_init"
    }
}

/// Append, falling back to a merging replace when the rows land in the past.
///
/// Appends move forward in time. A load whose earliest record precedes what
/// is already stored is a **backfill**, and it must not be appended
/// blindly: a book reconstructed from interleaved appends is neither the
/// old one nor the new one. Instead the affected window is read back,
/// merged with the new rows, sorted, and written atomically over the same
/// range. The window is exactly the span the new rows cover, so untouched
/// history is never rewritten.
async fn append_with(
    db: &Database,
    table: &str,
    schema: arrow::datatypes::SchemaRef,
    columns: Vec<ArrayRef>,
    time_column: &str,
) -> Result<()> {
    if columns.first().map(|c| c.len()).unwrap_or(0) == 0 {
        return Ok(());
    }
    let batch =
        RecordBatch::try_new(schema.clone(), columns).map_err(|error| BacktestError::Schema {
            table: "write",
            detail: error.to_string(),
        })?;

    match db
        .append(table, vec![batch.clone()], WriteOptions::default())
        .await
    {
        Ok(_) => Ok(()),
        Err(error) if error.code() == "sort_order_violation" => {
            backfill(db, table, schema, batch, time_column).await
        }
        Err(error) => Err(core_err(error)),
    }
}

/// Merge new rows into an already-written range.
async fn backfill(
    db: &Database,
    table: &str,
    schema: arrow::datatypes::SchemaRef,
    incoming: RecordBatch,
    time_column: &str,
) -> Result<()> {
    let (start, end) = time_bounds(&incoming, time_column)?;
    let window = TimeWindow::new(UnixNanos::new(start), UnixNanos::new(end))?;
    let existing = scan_optional(db, table, ReadAt::Latest, Some(window), None).await?;

    let mut batches = existing;
    batches.push(incoming);
    let merged = arrow::compute::concat_batches(&schema, &batches).map_err(|error| {
        BacktestError::Schema {
            table: "backfill",
            detail: error.to_string(),
        }
    })?;
    let sorted = sort_by_time(&merged, time_column)?;

    db.replace_range(table, start, end, vec![sorted], WriteOptions::default())
        .await
        .map_err(core_err)?;
    Ok(())
}

/// The half-open span a batch covers on its time column.
fn time_bounds(batch: &RecordBatch, time_column: &str) -> Result<(i64, i64)> {
    let times = column::<TimestampNanosecondArray>(batch, time_column)?;
    let mut min = i64::MAX;
    let mut max = i64::MIN;
    for row in 0..batch.num_rows() {
        let value = times.value(row);
        min = min.min(value);
        max = max.max(value);
    }
    if min > max {
        return Err(BacktestError::invalid("cannot bound an empty batch"));
    }
    Ok((min, max + 1))
}

/// Stable sort of a batch by its time column.
///
/// Stable on purpose: rows sharing a timestamp keep the order they were
/// written in, so a backfill cannot silently reorder simultaneous events
/// that a replay's tie-break depends on.
fn sort_by_time(batch: &RecordBatch, time_column: &str) -> Result<RecordBatch> {
    let times = column::<TimestampNanosecondArray>(batch, time_column)?;
    let mut order: Vec<usize> = (0..batch.num_rows()).collect();
    order.sort_by_key(|row| times.value(*row));
    let indices =
        arrow::array::UInt32Array::from(order.iter().map(|row| *row as u32).collect::<Vec<_>>());
    let columns = batch
        .columns()
        .iter()
        .map(|column| arrow::compute::take(column.as_ref(), &indices, None))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| BacktestError::Schema {
            table: "backfill",
            detail: error.to_string(),
        })?;
    RecordBatch::try_new(batch.schema(), columns).map_err(|error| BacktestError::Schema {
        table: "backfill",
        detail: error.to_string(),
    })
}

/// Reconstruct a book from stored events, for checking a stored feed.
pub fn replay_book(records: &[Record], instrument: &str) -> Result<OrderBook> {
    let mut book = OrderBook::new();
    for record in records {
        match &record.event {
            MarketEvent::BookSnapshot { bids, asks } => {
                book.apply_snapshot(bids, asks, record.ts())?
            }
            MarketEvent::BookDelta(delta) => book.apply_delta(instrument, *delta, record.ts())?,
            MarketEvent::Gap => book.mark_gap(record.ts()),
            _ => {}
        }
    }
    Ok(book)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::SCALE;

    #[test]
    fn float_storage_round_trips_exactly() {
        // The claim the schema doc makes: every value the fixed-point type
        // can represent survives a trip through f64 storage unchanged.
        let cases = [
            0.0,
            1.0,
            0.5,
            0.42,
            0.0001,
            0.999999999,
            123.456789,
            1e6,
            -0.37,
            0.123456789,
            9_999_999.999999999,
        ];
        for value in cases {
            let original = Price::from_f64(value).unwrap();
            let stored = original.to_f64();
            let restored = Price::from_f64(stored).unwrap();
            assert_eq!(original, restored, "{value} did not survive the round trip");
        }
    }

    #[test]
    fn round_trip_holds_across_the_whole_tick_grid() {
        // Every 0.0001 tick a prediction market can quote.
        for tick in 0..=10_000 as crate::types::Raw {
            let original = Price::from_raw(tick * (SCALE / 10_000));
            let restored = Price::from_f64(original.to_f64()).unwrap();
            assert_eq!(original, restored, "tick {tick} failed");
        }
    }
}
