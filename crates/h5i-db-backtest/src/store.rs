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
    Array, ArrayRef, BooleanArray, BooleanBuilder, Float64Array, Float64Builder, Int64Array,
    Int64Builder, StringArray, StringBuilder, TimestampNanosecondArray, TimestampNanosecondBuilder,
    UInt16Array, UInt16Builder,
};
use arrow::record_batch::RecordBatch;
use h5i_db_core::Database;
use h5i_db_core::database::{ReadAt, ScanOptions, WriteOptions};

use crate::book::{BookDelta, OrderBook};
use crate::engine::RunResult;
use crate::error::{BacktestError, Result};
use crate::event::{MarketEvent, Record};
use crate::instrument::{Instrument, InstrumentId, InstrumentKind, InstrumentSet, OutcomeId};
use crate::position::Portfolio;
use crate::schema;
use crate::settlement::{Resolution, SettlementReport};
use crate::types::{Money, Price, Qty, Side, Stamps, UnixNanos};
use crate::window::TimeWindow;

fn core_err(error: h5i_db_core::Error) -> BacktestError {
    BacktestError::invalid(error.to_string())
}

/// Create every market-data table that does not already exist.
pub async fn create_market_data_tables(db: &Database) -> Result<()> {
    let mut tables = schema::market_data_tables();
    // The ingest log travels with the market data it describes.
    tables.push(schema::ingest_log_table());
    create_tables(db, tables).await
}

/// Create every run-output table that does not already exist.
pub async fn create_run_tables(db: &Database) -> Result<()> {
    create_tables(db, schema::run_output_tables()).await
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

fn opt_str(array: &StringArray, row: usize) -> Option<&str> {
    array.is_valid(row).then(|| array.value(row))
}

fn opt_f64(array: &Float64Array, row: usize) -> Option<f64> {
    array.is_valid(row).then(|| array.value(row))
}

fn opt_i64(array: &Int64Array, row: usize) -> Option<i64> {
    array.is_valid(row).then(|| array.value(row))
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
) -> Result<Vec<RecordBatch>> {
    let options = ScanOptions {
        time_start: window.map(|w| w.start().get()),
        time_end: window.map(|w| w.end().get()),
        ..Default::default()
    };
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
) -> Result<Vec<RecordBatch>> {
    let options = ScanOptions {
        time_start: window.map(|w| w.start().get()),
        // ScanOptions::time_end is exclusive, which is exactly what a
        // half-open window means. No adjustment, and none to get wrong.
        time_end: window.map(|w| w.end().get()),
        ..Default::default()
    };
    let (batches, _report) = db.scan(table, at, options).await.map_err(core_err)?;
    Ok(batches)
}

// -- instruments ------------------------------------------------------------

/// Write instrument reference data: one row per outcome.
pub async fn write_instruments(
    db: &Database,
    instruments: &[Instrument],
    known_at: UnixNanos,
) -> Result<()> {
    let mut ts = TimestampNanosecondBuilder::new();
    let mut id = StringBuilder::new();
    let mut venue = StringBuilder::new();
    let mut kind = StringBuilder::new();
    let mut outcome = UInt16Builder::new();
    let mut label = StringBuilder::new();
    let mut tick = Float64Builder::new();
    let mut lot = Float64Builder::new();
    let mut expiration = Int64Builder::new();
    let mut observable = Int64Builder::new();

    for instrument in instruments {
        for (index, name) in instrument.outcomes.iter().enumerate() {
            ts.append_value(known_at.get());
            id.append_value(instrument.id.as_str());
            venue.append_value(&instrument.venue);
            kind.append_value(kind_name(&instrument.kind));
            outcome.append_value(index as u16);
            label.append_value(name);
            tick.append_value(instrument.tick_size.to_f64());
            lot.append_value(instrument.lot_size.to_f64());
            match instrument.expiration {
                Some(at) => expiration.append_value(at.get()),
                None => expiration.append_null(),
            }
            match instrument.settlement_observable {
                Some(at) => observable.append_value(at.get()),
                None => observable.append_null(),
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
        Arc::new(tick.finish()),
        Arc::new(lot.finish()),
        Arc::new(expiration.finish()),
        Arc::new(observable.finish()),
    ];
    append(db, schema::INSTRUMENTS, schema::instruments(), columns).await
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
    let batches = scan(db, schema::INSTRUMENTS, at, None).await?;
    // Outcome rows arrive in whatever order the segments hold them, so
    // collect by (instrument, outcome index) and assemble in index order.
    let mut collected: BTreeMap<String, InstrumentDraft> = BTreeMap::new();
    for batch in &batches {
        let id = column::<StringArray>(batch, "instrument_id")?;
        let venue = column::<StringArray>(batch, "venue")?;
        let kind = column::<StringArray>(batch, "kind")?;
        let outcome = column::<UInt16Array>(batch, "outcome")?;
        let label = column::<StringArray>(batch, "outcome_label")?;
        let tick = column::<Float64Array>(batch, "tick_size")?;
        let lot = column::<Float64Array>(batch, "lot_size")?;
        let expiration = column::<Int64Array>(batch, "expiration_ns")?;
        let observable = column::<Int64Array>(batch, "settlement_observable_ns")?;

        for row in 0..batch.num_rows() {
            let draft = collected
                .entry(id.value(row).to_string())
                .or_insert_with(|| InstrumentDraft {
                    venue: venue.value(row).to_string(),
                    kind: kind.value(row).to_string(),
                    outcomes: BTreeMap::new(),
                    tick: tick.value(row),
                    lot: lot.value(row),
                    expiration: opt_i64(expiration, row),
                    observable: opt_i64(observable, row),
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
        instrument.tick_size = Price::from_f64(draft.tick)?;
        instrument.lot_size = Qty::from_f64(draft.lot)?;
        instrument.expiration = draft.expiration.map(UnixNanos::new);
        instrument.settlement_observable = draft.observable.map(UnixNanos::new);
        set.insert(instrument)?;
    }
    Ok(set)
}

struct InstrumentDraft {
    venue: String,
    kind: String,
    outcomes: BTreeMap<u16, String>,
    tick: f64,
    lot: f64,
    expiration: Option<i64>,
    observable: Option<i64>,
}

// -- book events ------------------------------------------------------------

/// Write book records (snapshots, deltas, gaps).
///
/// A snapshot becomes one row per level, all sharing an `event_index`, with
/// `is_last` on the final row. Applying half a snapshot would leave a
/// crossed book, so the grouping is what lets the reader refuse a truncated
/// one instead of reconstructing nonsense from it.
pub async fn write_book_events(db: &Database, records: &[Record]) -> Result<()> {
    let mut ts_init = TimestampNanosecondBuilder::new();
    let mut ts_event = TimestampNanosecondBuilder::new();
    let mut instrument = StringBuilder::new();
    let mut outcome = UInt16Builder::new();
    let mut action = StringBuilder::new();
    let mut side = StringBuilder::new();
    let mut price = Float64Builder::new();
    let mut size = Float64Builder::new();
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
                Some(value) => price.append_value(value.to_f64()),
                None => price.append_null(),
            }
            match size_value {
                Some(value) => size.append_value(value.to_f64()),
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
            | MarketEvent::Corporate(_) => {
                return Err(BacktestError::invalid(
                    "trades, bars, funding and corporate actions belong in \
                     their own tables, not book_deltas",
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
        Arc::new(price.finish()),
        Arc::new(size.finish()),
        Arc::new(event_index.finish()),
        Arc::new(is_last.finish()),
        Arc::new(vendor.finish()),
    ];
    append(db, schema::BOOK_DELTAS, schema::book_deltas(), columns).await
}

/// Read book records back, regrouping snapshot levels by `event_index`.
pub async fn read_book_events(
    db: &Database,
    at: ReadAt,
    window: Option<TimeWindow>,
) -> Result<Vec<Record>> {
    let batches = scan_optional(db, schema::BOOK_DELTAS, at, window).await?;
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
        let instrument = column::<StringArray>(batch, "instrument_id")?;
        let outcome = column::<UInt16Array>(batch, "outcome")?;
        let action = column::<StringArray>(batch, "action")?;
        let side = column::<StringArray>(batch, "side")?;
        let price = column::<Float64Array>(batch, "price")?;
        let size = column::<Float64Array>(batch, "size")?;
        let event_index = column::<Int64Array>(batch, "event_index")?;
        let is_last = column::<BooleanArray>(batch, "is_last")?;

        for row in 0..batch.num_rows() {
            let stamps = Stamps::new(
                UnixNanos::new(ts_event.value(row)),
                UnixNanos::new(ts_init.value(row)),
            )?;
            let id = InstrumentId::new(instrument.value(row))?;
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
                    if let (Some(side_text), Some(p), Some(q)) =
                        (opt_str(side, row), opt_f64(price, row), opt_f64(size, row))
                    {
                        let level = (Price::from_f64(p)?, Qty::from_f64(q)?);
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
                                Price::from_f64(opt_f64(price, row).ok_or_else(|| {
                                    BacktestError::invalid("set row has no price")
                                })?)?,
                                Qty::from_f64(opt_f64(size, row).unwrap_or(0.0))?,
                            ),
                            "delete" => BookDelta::delete(
                                parsed,
                                Price::from_f64(opt_f64(price, row).ok_or_else(|| {
                                    BacktestError::invalid("delete row has no price")
                                })?)?,
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
    let mut ts_init = TimestampNanosecondBuilder::new();
    let mut ts_event = TimestampNanosecondBuilder::new();
    let mut instrument = StringBuilder::new();
    let mut outcome = UInt16Builder::new();
    let mut price = Float64Builder::new();
    let mut size = Float64Builder::new();
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
        price.append_value(p.to_f64());
        size.append_value(q.to_f64());
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
        Arc::new(price.finish()),
        Arc::new(size.finish()),
        Arc::new(aggressor.finish()),
        Arc::new(trade_id.finish()),
        Arc::new(vendor.finish()),
    ];
    append(db, schema::TRADES, schema::trades(), columns).await
}

pub async fn read_trades(
    db: &Database,
    at: ReadAt,
    window: Option<TimeWindow>,
) -> Result<Vec<Record>> {
    let batches = scan_optional(db, schema::TRADES, at, window).await?;
    let mut out = Vec::new();
    for batch in &batches {
        let ts_init = column::<TimestampNanosecondArray>(batch, "ts_init")?;
        let ts_event = column::<TimestampNanosecondArray>(batch, "ts_event")?;
        let instrument = column::<StringArray>(batch, "instrument_id")?;
        let outcome = column::<UInt16Array>(batch, "outcome")?;
        let price = column::<Float64Array>(batch, "price")?;
        let size = column::<Float64Array>(batch, "size")?;
        let aggressor = column::<StringArray>(batch, "aggressor")?;
        for row in 0..batch.num_rows() {
            out.push(Record::new(
                Stamps::new(
                    UnixNanos::new(ts_event.value(row)),
                    UnixNanos::new(ts_init.value(row)),
                )?,
                InstrumentId::new(instrument.value(row))?,
                OutcomeId(outcome.value(row)),
                MarketEvent::Trade {
                    price: Price::from_f64(price.value(row))?,
                    size: Qty::from_f64(size.value(row))?,
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
    let mut ts_init = TimestampNanosecondBuilder::new();
    let mut ts_event = TimestampNanosecondBuilder::new();
    let mut instrument = StringBuilder::new();
    let mut rate = Float64Builder::new();
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
        rate.append_value(value.to_f64());
        vendor.append_null();
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(ts_init.finish()),
        Arc::new(ts_event.finish()),
        Arc::new(instrument.finish()),
        Arc::new(rate.finish()),
        Arc::new(vendor.finish()),
    ];
    append(db, schema::FUNDING, schema::funding(), columns).await
}

pub async fn read_funding(
    db: &Database,
    at: ReadAt,
    window: Option<TimeWindow>,
) -> Result<Vec<Record>> {
    let batches = scan_optional(db, schema::FUNDING, at, window).await?;
    let mut out = Vec::new();
    for batch in &batches {
        let ts_init = column::<TimestampNanosecondArray>(batch, "ts_init")?;
        let ts_event = column::<TimestampNanosecondArray>(batch, "ts_event")?;
        let instrument = column::<StringArray>(batch, "instrument_id")?;
        let rate = column::<Float64Array>(batch, "rate")?;
        for row in 0..batch.num_rows() {
            out.push(Record::new(
                Stamps::new(
                    UnixNanos::new(ts_event.value(row)),
                    UnixNanos::new(ts_init.value(row)),
                )?,
                InstrumentId::new(instrument.value(row))?,
                OutcomeId::FIRST,
                MarketEvent::Funding {
                    rate: Price::from_f64(rate.value(row))?,
                },
            ));
        }
    }
    out.sort_by_key(|record| record.ts().get());
    Ok(out)
}

// -- resolutions ------------------------------------------------------------

pub async fn write_resolutions(db: &Database, resolutions: &[Resolution]) -> Result<()> {
    let mut ts = TimestampNanosecondBuilder::new();
    let mut instrument = StringBuilder::new();
    let mut winner = UInt16Builder::new();
    for resolution in resolutions {
        ts.append_value(resolution.observable_at.get());
        instrument.append_value(resolution.instrument.as_str());
        winner.append_value(resolution.winner.0);
    }
    let columns: Vec<ArrayRef> = vec![
        Arc::new(ts.finish()),
        Arc::new(instrument.finish()),
        Arc::new(winner.finish()),
    ];
    append(db, schema::RESOLUTIONS, schema::resolutions(), columns).await
}

/// Read resolutions.
///
/// Called only by the post-run settlement policy. Nothing on the strategy
/// path reaches this function, which is the structural half of "a strategy
/// cannot see the answer".
pub async fn read_resolutions(db: &Database, at: ReadAt) -> Result<Vec<Resolution>> {
    let batches = scan_optional(db, schema::RESOLUTIONS, at, None).await?;
    let mut out = Vec::new();
    for batch in &batches {
        let ts = column::<TimestampNanosecondArray>(batch, "ts_init")?;
        let instrument = column::<StringArray>(batch, "instrument_id")?;
        let winner = column::<UInt16Array>(batch, "winner_outcome")?;
        for row in 0..batch.num_rows() {
            out.push(Resolution::new(
                InstrumentId::new(instrument.value(row))?,
                OutcomeId(winner.value(row)),
                UnixNanos::new(ts.value(row)),
            ));
        }
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

    let batches = scan(db, table, at, window).await?;
    let mut out = Vec::new();
    for batch in &batches {
        let ts = column::<TimestampNanosecondArray>(batch, "ts")?;
        let instrument = column::<StringArray>(batch, "instrument_id")?;
        let outcome = column::<UInt16Array>(batch, "outcome")?;
        let side = column::<StringArray>(batch, "side")?;
        let quantity = column::<Float64Array>(batch, "quantity")?;
        let kind = column::<StringArray>(batch, "kind")?;
        let limit = column::<Float64Array>(batch, "limit_price")?;
        let tif = column::<StringArray>(batch, "time_in_force")?;
        let tag = column::<StringArray>(batch, "tag")?;
        let reduce_only = column::<BooleanArray>(batch, "reduce_only")?;

        for row in 0..batch.num_rows() {
            let id = InstrumentId::new(instrument.value(row))?;
            let out_id = OutcomeId(outcome.value(row));
            let parsed_side = Side::parse(side.value(row))?;
            let size = Qty::from_f64(quantity.value(row))?;
            let mut request = match kind.value(row) {
                "market" => OrderRequest::market(id, out_id, parsed_side, size),
                "limit" => {
                    // A limit order without a price is a data error, not a
                    // market order: guessing which one the author meant is
                    // how a backtest quietly trades at the wrong price.
                    let price = opt_f64(limit, row).ok_or_else(|| {
                        BacktestError::invalid(format!(
                            "limit signal for {} at {} has no limit_price",
                            instrument.value(row),
                            ts.value(row)
                        ))
                    })?;
                    OrderRequest::limit(id, out_id, parsed_side, Price::from_f64(price)?, size)
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

    let batches = scan(db, table, at, window).await?;
    let mut out = Vec::new();
    for batch in &batches {
        let ts = column::<TimestampNanosecondArray>(batch, "ts")?;
        let action = column::<StringArray>(batch, "action")?;
        let client_id = column::<StringArray>(batch, "client_order_id")?;
        let instrument = column::<StringArray>(batch, "instrument_id")?;
        let outcome = column::<UInt16Array>(batch, "outcome")?;
        let side = column::<StringArray>(batch, "side")?;
        let quantity = column::<Float64Array>(batch, "quantity")?;
        let kind = column::<StringArray>(batch, "kind")?;
        let limit = column::<Float64Array>(batch, "limit_price")?;
        let tif = column::<StringArray>(batch, "time_in_force")?;
        let tag = column::<StringArray>(batch, "tag")?;
        let reduce_only = column::<BooleanArray>(batch, "reduce_only")?;

        for row in 0..batch.num_rows() {
            let client_order_id = client_id.value(row).to_string();
            let command = match action.value(row) {
                "cancel" => ReplayCommand::Cancel { client_order_id },
                "amend" => {
                    let quantity = opt_f64(quantity, row).map(Qty::from_f64).transpose()?;
                    let limit = opt_f64(limit, row).map(Price::from_f64).transpose()?;
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
                    required(quantity.is_valid(row), "quantity")?;
                    required(kind.is_valid(row), "kind")?;

                    let id = InstrumentId::new(instrument.value(row))?;
                    let outcome_id = OutcomeId(outcome.value(row));
                    let parsed_side = Side::parse(side.value(row))?;
                    let size = Qty::from_f64(quantity.value(row))?;
                    let mut request = match kind.value(row) {
                        "market" => OrderRequest::market(id, outcome_id, parsed_side, size),
                        "limit" => {
                            let value = opt_f64(limit, row).ok_or_else(|| {
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
                                Price::from_f64(value)?,
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
    let batches = scan_optional(db, schema::TRADES, at, window).await?;
    Ok(Box::new(BatchDecoder {
        batches: batches.into_iter(),
        buffer: Default::default(),
        decode: decode_trades,
    }))
}

fn decode_trades(batch: &RecordBatch, out: &mut std::collections::VecDeque<Record>) -> Result<()> {
    let ts_init = column::<TimestampNanosecondArray>(batch, "ts_init")?;
    let ts_event = column::<TimestampNanosecondArray>(batch, "ts_event")?;
    let instrument = column::<StringArray>(batch, "instrument_id")?;
    let outcome = column::<UInt16Array>(batch, "outcome")?;
    let price = column::<Float64Array>(batch, "price")?;
    let size = column::<Float64Array>(batch, "size")?;
    let aggressor = column::<StringArray>(batch, "aggressor")?;
    for row in 0..batch.num_rows() {
        out.push_back(Record::new(
            Stamps::new(
                UnixNanos::new(ts_event.value(row)),
                UnixNanos::new(ts_init.value(row)),
            )?,
            InstrumentId::new(instrument.value(row))?,
            OutcomeId(outcome.value(row)),
            MarketEvent::Trade {
                price: Price::from_f64(price.value(row))?,
                size: Qty::from_f64(size.value(row))?,
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
    let batches = scan_optional(db, schema::TRADES, at, window).await?;
    Ok(batches.iter().map(|batch| batch.num_rows()).sum())
}

/// A lazy source of funding records.
pub async fn funding_source(
    db: &Database,
    at: ReadAt,
    window: Option<TimeWindow>,
) -> Result<crate::replay::RecordSource> {
    let batches = scan_optional(db, schema::FUNDING, at, window).await?;
    Ok(Box::new(BatchDecoder {
        batches: batches.into_iter(),
        buffer: Default::default(),
        decode: decode_funding,
    }))
}

fn decode_funding(batch: &RecordBatch, out: &mut std::collections::VecDeque<Record>) -> Result<()> {
    let ts_init = column::<TimestampNanosecondArray>(batch, "ts_init")?;
    let ts_event = column::<TimestampNanosecondArray>(batch, "ts_event")?;
    let instrument = column::<StringArray>(batch, "instrument_id")?;
    let rate = column::<Float64Array>(batch, "rate")?;
    for row in 0..batch.num_rows() {
        out.push_back(Record::new(
            Stamps::new(
                UnixNanos::new(ts_event.value(row)),
                UnixNanos::new(ts_init.value(row)),
            )?,
            InstrumentId::new(instrument.value(row))?,
            OutcomeId::FIRST,
            MarketEvent::Funding {
                rate: Price::from_f64(rate.value(row))?,
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
    let batches = scan_optional(db, schema::INGEST_LOG, ReadAt::Latest, None).await?;
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
    let batches = vec![
        (
            schema::RUN,
            run_manifest_batch(run_id, config_digest, result, settlement, started_at)?,
        ),
        (schema::ORDERS, orders_batch(result)?),
        (schema::FILLS, fills_batch(result)?),
        (schema::POSITIONS, positions_batch(result, settlement)?),
        (schema::EQUITY, equity_batch(result)?),
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
) -> Result<Option<RecordBatch>> {
    let warnings = settlement.warnings();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(TimestampNanosecondArray::from(vec![started_at.get()])),
        Arc::new(StringArray::from(vec![run_id])),
        Arc::new(StringArray::from(vec![config_digest])),
        Arc::new(Float64Array::from(vec![result.starting_cash.to_f64()])),
        Arc::new(Float64Array::from(vec![result.final_cash.to_f64()])),
        Arc::new(Float64Array::from(vec![result.realized_pnl.to_f64()])),
        Arc::new(Float64Array::from(vec![result.commissions.to_f64()])),
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
    build(schema::RUN, schema::run(), columns)
}

fn orders_batch(result: &RunResult) -> Result<Option<RecordBatch>> {
    let mut ts = TimestampNanosecondBuilder::new();
    let mut id = Int64Builder::new();
    let mut instrument = StringBuilder::new();
    let mut outcome = UInt16Builder::new();
    let mut side = StringBuilder::new();
    let mut kind = StringBuilder::new();
    let mut limit = Float64Builder::new();
    let mut quantity = Float64Builder::new();
    let mut filled = Float64Builder::new();
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
                limit.append_value(price.to_f64());
            }
            None => {
                kind.append_value("market");
                limit.append_null();
            }
        }
        quantity.append_value(order.quantity.to_f64());
        filled.append_value(order.filled.to_f64());
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
        Arc::new(limit.finish()),
        Arc::new(quantity.finish()),
        Arc::new(filled.finish()),
        Arc::new(tif.finish()),
        Arc::new(status.finish()),
        Arc::new(reject_reason.finish()),
        Arc::new(tag.finish()),
        Arc::new(reduce_only.finish()),
    ];
    build(schema::ORDERS, schema::orders(), columns)
}

fn fills_batch(result: &RunResult) -> Result<Option<RecordBatch>> {
    let mut ts = TimestampNanosecondBuilder::new();
    let mut order_id = Int64Builder::new();
    let mut instrument = StringBuilder::new();
    let mut outcome = UInt16Builder::new();
    let mut side = StringBuilder::new();
    let mut price = Float64Builder::new();
    let mut quantity = Float64Builder::new();
    let mut commission = Float64Builder::new();
    let mut is_taker = BooleanBuilder::new();
    let mut tag = StringBuilder::new();

    for fill in &result.fills {
        ts.append_value(fill.ts.get());
        order_id.append_value(fill.order_id.0 as i64);
        instrument.append_value(fill.instrument.as_str());
        outcome.append_value(fill.outcome.0);
        side.append_value(fill.side.as_str());
        price.append_value(fill.price.to_f64());
        quantity.append_value(fill.quantity.to_f64());
        commission.append_value(fill.commission.to_f64());
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
        Arc::new(price.finish()),
        Arc::new(quantity.finish()),
        Arc::new(commission.finish()),
        Arc::new(is_taker.finish()),
        Arc::new(tag.finish()),
    ];
    build(schema::FILLS, schema::fills(), columns)
}

/// Read `bt_fills` back as [`Fill`]s, so a stored run can be re-folded into
/// positions and checked against what it claimed.
pub async fn read_fills(db: &Database, at: ReadAt) -> Result<Vec<crate::order::Fill>> {
    let batches = scan(db, schema::FILLS, at, None).await?;
    let mut out = Vec::new();
    for batch in &batches {
        let ts = column::<TimestampNanosecondArray>(batch, "ts")?;
        let order_id = column::<Int64Array>(batch, "order_id")?;
        let instrument = column::<StringArray>(batch, "instrument_id")?;
        let outcome = column::<UInt16Array>(batch, "outcome")?;
        let side = column::<StringArray>(batch, "side")?;
        let price = column::<Float64Array>(batch, "price")?;
        let quantity = column::<Float64Array>(batch, "quantity")?;
        let commission = column::<Float64Array>(batch, "commission")?;
        let is_taker = column::<BooleanArray>(batch, "is_taker")?;
        let tag = column::<StringArray>(batch, "tag")?;
        for row in 0..batch.num_rows() {
            out.push(crate::order::Fill {
                order_id: crate::order::OrderId(order_id.value(row) as u64),
                instrument: InstrumentId::new(instrument.value(row))?,
                outcome: OutcomeId(outcome.value(row)),
                side: Side::parse(side.value(row))?,
                price: Price::from_f64(price.value(row))?,
                quantity: Qty::from_f64(quantity.value(row))?,
                commission: Money::from_f64(commission.value(row))?,
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
    let mut quantity = Float64Builder::new();
    let mut average = Float64Builder::new();
    let mut realized = Float64Builder::new();
    let mut commissions = Float64Builder::new();
    let mut settlement_pnl = Float64Builder::new();
    let mut market_exit = Float64Builder::new();

    for position in portfolio.positions() {
        ts.append_value(ts_value);
        instrument.append_value(position.instrument.as_str());
        outcome.append_value(position.outcome.0);
        quantity.append_value(position.quantity.to_f64());
        average.append_value(position.average_price.to_f64());
        realized.append_value(position.realized_pnl.to_f64());
        commissions.append_value(position.commissions.to_f64());
        match settled.get(&(position.instrument.to_string(), position.outcome.0)) {
            Some(entry) => {
                settlement_pnl.append_value(entry.settled_pnl.to_f64());
                match entry.market_exit_pnl {
                    Some(value) => market_exit.append_value(value.to_f64()),
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
        Arc::new(quantity.finish()),
        Arc::new(average.finish()),
        Arc::new(realized.finish()),
        Arc::new(commissions.finish()),
        Arc::new(settlement_pnl.finish()),
        Arc::new(market_exit.finish()),
    ];
    build(schema::POSITIONS, schema::positions(), columns)
}

fn equity_batch(result: &RunResult) -> Result<Option<RecordBatch>> {
    let mut ts = TimestampNanosecondBuilder::new();
    let mut cash = Float64Builder::new();
    let mut position_value = Float64Builder::new();
    let mut equity = Float64Builder::new();
    let mut realized = Float64Builder::new();
    let mut unrealized = Float64Builder::new();

    for point in &result.equity {
        ts.append_value(point.ts.get());
        cash.append_value(point.cash.to_f64());
        position_value.append_value(point.position_value.to_f64());
        equity.append_value(point.equity.to_f64());
        realized.append_value(point.realized_pnl.to_f64());
        unrealized.append_value(point.unrealized_pnl.to_f64());
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(ts.finish()),
        Arc::new(cash.finish()),
        Arc::new(position_value.finish()),
        Arc::new(equity.finish()),
        Arc::new(realized.finish()),
        Arc::new(unrealized.finish()),
    ];
    build(schema::EQUITY, schema::equity(), columns)
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
    let existing = scan_optional(db, table, ReadAt::Latest, Some(window)).await?;

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
        for tick in 0..=10_000i64 {
            let original = Price::from_raw(tick * (SCALE / 10_000));
            let restored = Price::from_f64(original.to_f64()).unwrap();
            assert_eq!(original, restored, "tick {tick} failed");
        }
    }
}
