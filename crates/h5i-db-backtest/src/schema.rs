//! Canonical, venue-neutral table schemas (ROADMAP_QUANT.md §8.6).
//!
//! These are the tables a backtest reads and writes. They are venue-neutral
//! on purpose: a Polymarket loader and a Hyperliquid loader both produce
//! `book_deltas`, and the kernel never learns which vendor a row came from
//! beyond the provenance columns.
//!
//! **Numbers on disk are `Float64`; numbers in the kernel are fixed point.**
//! Storage matches the rest of this repository, so ordinary SQL and the
//! quant layer work on these tables without decoding anything. The
//! conversion at the boundary is exact for every value the fixed-point type
//! can represent -- nine decimal places and magnitudes below about 9e9 --
//! and [`crate::store`] has a round-trip test that says so rather than
//! leaving it as an assumption.
//!
//! Every market-data table carries both timestamps and is *time-indexed on
//! `ts_init`*, because that is the order replay reads them in, so a time
//! range scan prunes on exactly the column the merge sorts by.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use h5i_db_core::spec::TableOptions;

/// Market data: incremental book updates and full snapshots.
pub const BOOK_DELTAS: &str = "book_deltas";
/// Market data: prints.
pub const TRADES: &str = "trades";
/// Market data: aggregates.
pub const BARS: &str = "bars";
/// Reference data: one row per instrument outcome.
pub const INSTRUMENTS: &str = "instruments";
/// How markets resolved. Never read by a strategy; see [`crate::settlement`].
pub const RESOLUTIONS: &str = "resolutions";
/// Order intents for a Tier 1 signal replay.
pub const SIGNALS: &str = "signals";
/// Perpetual funding rates.
pub const FUNDING: &str = "funding";
/// What has already been ingested, so a reload is a no-op.
pub const INGEST_LOG: &str = "ingest_log";

/// Run output: one row describing the run.
pub const RUN: &str = "bt_run";
/// Run output: every order.
pub const ORDERS: &str = "bt_orders";
/// Run output: every fill. Positions are rebuildable from this alone.
pub const FILLS: &str = "bt_fills";
/// Run output: final positions.
pub const POSITIONS: &str = "bt_positions";
/// Run output: the equity curve, which the tearsheet layer consumes.
pub const EQUITY: &str = "bt_equity";

fn ts(name: &str) -> Field {
    Field::new(name, DataType::Timestamp(TimeUnit::Nanosecond, None), false)
}

fn text(name: &str) -> Field {
    Field::new(name, DataType::Utf8, false)
}

fn opt_text(name: &str) -> Field {
    Field::new(name, DataType::Utf8, true)
}

fn float(name: &str) -> Field {
    Field::new(name, DataType::Float64, false)
}

fn opt_float(name: &str) -> Field {
    Field::new(name, DataType::Float64, true)
}

fn int(name: &str) -> Field {
    Field::new(name, DataType::Int64, false)
}

fn opt_int(name: &str) -> Field {
    Field::new(name, DataType::Int64, true)
}

fn outcome() -> Field {
    Field::new("outcome", DataType::UInt16, false)
}

/// `book_deltas`: one row per price level change.
///
/// Rows sharing an `event_index` form one atomic book event -- a snapshot's
/// levels, or a burst of updates the venue published together -- and the
/// last of them carries `is_last`. Applying half an event would leave a
/// crossed or hollow book, so the grouping is part of the schema rather
/// than a convention each loader reinvents.
pub fn book_deltas() -> SchemaRef {
    Arc::new(Schema::new(vec![
        ts("ts_init"),
        ts("ts_event"),
        text("instrument_id"),
        outcome(),
        // "snapshot" | "set" | "delete" | "clear" | "gap"
        text("action"),
        // Null for clear, gap, and snapshot boundary rows.
        opt_text("side"),
        opt_float("price"),
        opt_float("size"),
        int("event_index"),
        Field::new("is_last", DataType::Boolean, false),
        opt_text("source_vendor"),
    ]))
}

/// `trades`: prints.
pub fn trades() -> SchemaRef {
    Arc::new(Schema::new(vec![
        ts("ts_init"),
        ts("ts_event"),
        text("instrument_id"),
        outcome(),
        float("price"),
        float("size"),
        // Null where the vendor does not say, which is common and must not
        // be guessed: an assumed aggressor silently biases every fill model
        // that reads it.
        opt_text("aggressor"),
        opt_text("trade_id"),
        opt_text("source_vendor"),
    ]))
}

/// `bars`: aggregates.
pub fn bars() -> SchemaRef {
    Arc::new(Schema::new(vec![
        ts("ts_init"),
        ts("ts_event"),
        text("instrument_id"),
        outcome(),
        float("open"),
        float("high"),
        float("low"),
        float("close"),
        float("volume"),
        opt_text("source_vendor"),
    ]))
}

/// `instruments`: one row per outcome, so a categorical market is N rows.
pub fn instruments() -> SchemaRef {
    Arc::new(Schema::new(vec![
        ts("ts_init"),
        text("instrument_id"),
        text("venue"),
        // "prediction_market" | "perpetual" | "spot"
        text("kind"),
        outcome(),
        text("outcome_label"),
        float("tick_size"),
        float("lot_size"),
        opt_int("expiration_ns"),
        opt_int("settlement_observable_ns"),
    ]))
}

/// `resolutions`: how a market ended.
pub fn resolutions() -> SchemaRef {
    Arc::new(Schema::new(vec![
        // The instant the result became observable, which is what gates
        // settlement -- not the instant the underlying event occurred.
        ts("ts_init"),
        text("instrument_id"),
        Field::new("winner_outcome", DataType::UInt16, false),
    ]))
}

/// `funding`: perpetual funding rates as they became due.
pub fn funding() -> SchemaRef {
    Arc::new(Schema::new(vec![
        ts("ts_init"),
        ts("ts_event"),
        text("instrument_id"),
        // Per-interval rate, not annualised: positive means longs pay.
        float("rate"),
        opt_text("source_vendor"),
    ]))
}

/// `ingest_log`: one row per completed load.
///
/// The `digest` is a content hash of everything the load contained, which
/// makes ingestion idempotent: re-running a loader over a window already
/// present is recognised and skipped rather than appending the rows a
/// second time. Without it the natural response to a partial failure --
/// run it again -- silently doubles the data, and a doubled book is not
/// obviously wrong until a fill happens at an impossible size.
pub fn ingest_log() -> SchemaRef {
    Arc::new(Schema::new(vec![
        ts("ts"),
        text("vendor"),
        text("digest"),
        int("records"),
        opt_int("window_start_ns"),
        opt_int("window_end_ns"),
        int("instruments"),
    ]))
}

/// `signals`: what a Tier 1 strategy wants to do, as data.
///
/// A signal table *is* the strategy for a signal replay: no callback code,
/// no state machine, just timestamped intent that the kernel executes
/// through the full matching, fee and latency path. It is also what a
/// factor pipeline naturally emits, so research output feeds simulation
/// without an adapter in between.
pub fn signals() -> SchemaRef {
    Arc::new(Schema::new(vec![
        // When the strategy wants to act. Replay submits the intent the
        // first time the clock reaches it.
        ts("ts"),
        text("instrument_id"),
        outcome(),
        // "buy" | "sell"
        text("side"),
        float("quantity"),
        // "market" | "limit"
        text("kind"),
        // Required for limit orders, ignored for market ones.
        opt_float("limit_price"),
        // "gtc" | "ioc" | "fok"; defaults per order kind when null.
        opt_text("time_in_force"),
        opt_text("tag"),
        Field::new("reduce_only", DataType::Boolean, true),
    ]))
}

/// Table options for the signals table.
pub fn signals_options() -> TableOptions {
    TableOptions {
        time_column: Some("ts".to_string()),
        sort_key: vec!["ts".to_string()],
        ..Default::default()
    }
}

/// `bt_run`: the run manifest.
pub fn run() -> SchemaRef {
    Arc::new(Schema::new(vec![
        ts("ts"),
        text("run_id"),
        text("config_digest"),
        float("starting_cash"),
        float("final_cash"),
        float("realized_pnl"),
        float("commissions"),
        opt_int("simulated_through_ns"),
        int("records_processed"),
        Field::new("settlement_applied", DataType::Boolean, false),
        opt_text("warnings"),
    ]))
}

/// `bt_orders`: every order the run produced.
pub fn orders() -> SchemaRef {
    Arc::new(Schema::new(vec![
        ts("ts"),
        int("order_id"),
        text("instrument_id"),
        outcome(),
        text("side"),
        text("kind"),
        opt_float("limit_price"),
        float("quantity"),
        float("filled"),
        text("time_in_force"),
        text("status"),
        opt_text("tag"),
        Field::new("reduce_only", DataType::Boolean, false),
    ]))
}

/// `bt_fills`: every execution. The authoritative record -- positions are a
/// fold over this table and nothing else.
pub fn fills() -> SchemaRef {
    Arc::new(Schema::new(vec![
        ts("ts"),
        int("order_id"),
        text("instrument_id"),
        outcome(),
        text("side"),
        float("price"),
        float("quantity"),
        float("commission"),
        Field::new("is_taker", DataType::Boolean, false),
        opt_text("tag"),
    ]))
}

/// `bt_positions`: where the run finished.
pub fn positions() -> SchemaRef {
    Arc::new(Schema::new(vec![
        ts("ts"),
        text("instrument_id"),
        outcome(),
        float("quantity"),
        float("average_price"),
        float("realized_pnl"),
        float("commissions"),
        opt_float("settlement_pnl"),
        opt_float("market_exit_pnl"),
    ]))
}

/// `bt_equity`: the equity curve, sampled on a fixed interval of simulated
/// time. This is the table `h5i_db.quant.returns()` reads.
pub fn equity() -> SchemaRef {
    Arc::new(Schema::new(vec![
        ts("ts"),
        float("cash"),
        float("position_value"),
        float("equity"),
        float("realized_pnl"),
        float("unrealized_pnl"),
    ]))
}

/// Table options for a market-data table: time-indexed on `ts_init`.
pub fn market_data_options() -> TableOptions {
    TableOptions {
        time_column: Some("ts_init".to_string()),
        sort_key: vec!["ts_init".to_string()],
        ..Default::default()
    }
}

/// Table options for a run-output table: time-indexed on `ts`.
pub fn run_output_options() -> TableOptions {
    TableOptions {
        time_column: Some("ts".to_string()),
        sort_key: vec!["ts".to_string()],
        ..Default::default()
    }
}

/// Every market-data table, with the options it wants.
pub fn market_data_tables() -> Vec<(&'static str, SchemaRef, TableOptions)> {
    vec![
        (BOOK_DELTAS, book_deltas(), market_data_options()),
        (TRADES, trades(), market_data_options()),
        (BARS, bars(), market_data_options()),
        (INSTRUMENTS, instruments(), market_data_options()),
        (RESOLUTIONS, resolutions(), market_data_options()),
        (FUNDING, funding(), market_data_options()),
    ]
}

/// The ingest log, which is metadata *about* loads rather than market data,
/// and so is indexed on when the load happened.
pub fn ingest_log_table() -> (&'static str, SchemaRef, TableOptions) {
    (INGEST_LOG, ingest_log(), run_output_options())
}

/// The signals table, which a caller creates only when driving a Tier 1
/// replay from stored intent.
pub fn signals_table() -> (&'static str, SchemaRef, TableOptions) {
    (SIGNALS, signals(), signals_options())
}

/// Every run-output table.
pub fn run_output_tables() -> Vec<(&'static str, SchemaRef, TableOptions)> {
    vec![
        (RUN, run(), run_output_options()),
        (ORDERS, orders(), run_output_options()),
        (FILLS, fills(), run_output_options()),
        (POSITIONS, positions(), run_output_options()),
        (EQUITY, equity(), run_output_options()),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn market_data_is_time_indexed_on_the_column_replay_sorts_by() {
        // Pruning must happen on ts_init, because that is the order the
        // merge reads records in.
        for (name, schema, options) in market_data_tables() {
            assert_eq!(
                options.time_column.as_deref(),
                Some("ts_init"),
                "{name} must be indexed on ts_init"
            );
            assert!(
                schema.field_with_name("ts_init").is_ok(),
                "{name} needs ts_init"
            );
        }
    }

    #[test]
    fn every_market_data_row_carries_both_timestamps() {
        for (name, schema, _) in market_data_tables() {
            if name == INSTRUMENTS || name == RESOLUTIONS {
                continue; // reference data: known-at only
            }
            for column in ["ts_init", "ts_event"] {
                let field = schema.field_with_name(column).expect(column);
                assert!(!field.is_nullable(), "{name}.{column} must be non-null");
            }
        }
    }

    #[test]
    fn optional_columns_are_the_ones_a_vendor_may_not_supply() {
        // An aggressor that is absent must stay absent rather than being
        // defaulted to a side.
        let trades = trades();
        assert!(trades.field_with_name("aggressor").unwrap().is_nullable());
        assert!(!trades.field_with_name("price").unwrap().is_nullable());

        // A clear or gap row has no side, price, or size.
        let deltas = book_deltas();
        for column in ["side", "price", "size"] {
            assert!(
                deltas.field_with_name(column).unwrap().is_nullable(),
                "{column} must be nullable"
            );
        }
        assert!(!deltas.field_with_name("action").unwrap().is_nullable());
    }

    #[test]
    fn run_outputs_are_time_indexed_and_named_consistently() {
        for (name, schema, options) in run_output_tables() {
            assert!(name.starts_with("bt_"), "{name} must be a bt_ table");
            assert_eq!(options.time_column.as_deref(), Some("ts"));
            assert!(schema.field_with_name("ts").is_ok());
        }
    }

    #[test]
    fn fills_carry_everything_needed_to_rebuild_a_position() {
        // The audit claim: bt_fills alone must reconstruct bt_positions.
        let fills = fills();
        for column in [
            "instrument_id",
            "outcome",
            "side",
            "price",
            "quantity",
            "commission",
        ] {
            assert!(fills.field_with_name(column).is_ok(), "fills needs {column}");
        }
    }

    #[test]
    fn table_names_do_not_collide() {
        let mut names: Vec<&str> = market_data_tables()
            .into_iter()
            .map(|(n, _, _)| n)
            .chain(run_output_tables().into_iter().map(|(n, _, _)| n))
            .chain(std::iter::once(signals_table().0))
            .chain(std::iter::once(ingest_log_table().0))
            .collect();
        let count = names.len();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), count, "table names must be unique");
    }
}
