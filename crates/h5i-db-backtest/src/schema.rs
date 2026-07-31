//! Canonical, venue-neutral table schemas (ROADMAP_QUANT.md §8.6).
//!
//! These are the tables a backtest reads and writes. They are venue-neutral
//! on purpose: a Polymarket loader and a Hyperliquid loader both produce
//! `book_deltas`, and the kernel never learns which vendor a row came from
//! beyond the provenance columns.
//!
//! **Numbers in the kernel are fixed point; on disk they are `Float64` by
//! default.** Storage matches the rest of this repository, so ordinary SQL
//! and the quant layer work on these tables without decoding anything. The
//! conversion at the boundary keeps all nine decimal places, and is exact up
//! to a magnitude of about 9e6 -- where an `f64` mantissa stops holding
//! consecutive scaled integers -- which covers every price a venue quotes.
//! [`crate::store`] has a round-trip test that says so rather than leaving
//! it as an assumption.
//!
//! A table that will hold values above that ceiling is created with
//! [`FixedEncoding::Decimal`] instead, and stores the same numbers as
//! `Decimal128(38, 9)` with no float in the path. The encoding is per table
//! and recorded in its schema, so both builds read both encodings; see
//! [`crate::decimal`]. Every schema function here takes it, and passing
//! [`FixedEncoding::Float`] reproduces the schema byte for byte.
//!
//! Every market-data table carries both timestamps and is *time-indexed on
//! `ts_init`*, because that is the order replay reads them in, so a time
//! range scan prunes on exactly the column the merge sorts by.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use h5i_db_core::spec::TableOptions;

use crate::decimal::FixedEncoding;

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
/// Submit, amend, and cancel commands for a lifecycle-aware replay.
pub const COMMANDS: &str = "commands";
/// Perpetual funding rates.
pub const FUNDING: &str = "funding";
/// Venue-published mark and oracle prices, which are not the book.
pub const REFERENCES: &str = "references";
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

/// A fixed-point column, in whichever encoding the table was created with.
///
/// Was `Float64` unconditionally. It still is by default, so an existing
/// table's schema is byte-identical; `FixedEncoding::Decimal` is what a
/// caller asks for when the values will outgrow what an `f64` holds exactly.
fn float(name: &str, encoding: FixedEncoding) -> Field {
    crate::decimal::fixed_field(name, encoding, false)
}

fn opt_float(name: &str, encoding: FixedEncoding) -> Field {
    crate::decimal::fixed_field(name, encoding, true)
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
pub fn book_deltas(encoding: FixedEncoding) -> SchemaRef {
    Arc::new(Schema::new(vec![
        ts("ts_init"),
        ts("ts_event"),
        text("instrument_id"),
        outcome(),
        // "snapshot" | "set" | "delete" | "clear" | "gap"
        text("action"),
        // Null for clear, gap, and snapshot boundary rows.
        opt_text("side"),
        opt_float("price", encoding),
        opt_float("size", encoding),
        int("event_index"),
        Field::new("is_last", DataType::Boolean, false),
        opt_text("source_vendor"),
    ]))
}

/// `trades`: prints.
pub fn trades(encoding: FixedEncoding) -> SchemaRef {
    Arc::new(Schema::new(vec![
        ts("ts_init"),
        ts("ts_event"),
        text("instrument_id"),
        outcome(),
        float("price", encoding),
        float("size", encoding),
        // Null where the vendor does not say, which is common and must not
        // be guessed: an assumed aggressor silently biases every fill model
        // that reads it.
        opt_text("aggressor"),
        opt_text("trade_id"),
        opt_text("source_vendor"),
    ]))
}

/// `bars`: aggregates.
pub fn bars(encoding: FixedEncoding) -> SchemaRef {
    Arc::new(Schema::new(vec![
        ts("ts_init"),
        ts("ts_event"),
        text("instrument_id"),
        outcome(),
        float("open", encoding),
        float("high", encoding),
        float("low", encoding),
        float("close", encoding),
        float("volume", encoding),
        opt_text("source_vendor"),
    ]))
}

/// `instruments`: one row per outcome, so a categorical market is N rows.
pub fn instruments(encoding: FixedEncoding) -> SchemaRef {
    Arc::new(Schema::new(vec![
        ts("ts_init"),
        text("instrument_id"),
        text("venue"),
        // "prediction_market" | "perpetual" | "spot"
        text("kind"),
        outcome(),
        text("outcome_label"),
        float("tick_size", encoding),
        float("lot_size", encoding),
        opt_int("expiration_ns"),
        opt_int("settlement_observable_ns"),
        // Whether the venue exchanges a complete set of these outcomes for
        // one unit of cash. Nullable because most instruments predate the
        // column and false is the safe reading of its absence: a market
        // that cannot be minted is only a missing capability, whereas a
        // market wrongly believed mintable creates cash.
        Field::new("neg_risk", DataType::Boolean, true),
        // A significant-figure price cap, where the venue has one. Absent
        // means the tick grid is the whole rule, which is the common case.
        Field::new("price_significant_figures", DataType::UInt8, true),
        Field::new("price_max_decimals", DataType::UInt8, true),
    ]))
}

/// `resolutions`: how a market ended.
///
/// Shaped for the payouts a market can actually produce, not just the
/// common one. A single winner is one row; a scalar or partial settlement
/// is one row per outcome carrying its payout; a void is one row naming the
/// arity it refunds across. `kind` says which, so no row has to be read in
/// the light of another's absence.
pub fn resolutions(encoding: FixedEncoding) -> SchemaRef {
    Arc::new(Schema::new(vec![
        // The instant the result became observable, which is what gates
        // settlement -- not the instant the underlying event occurred.
        ts("ts_init"),
        text("instrument_id"),
        // "winner" | "split" | "void"
        text("kind"),
        // The winning outcome on a `winner` row, the outcome this payout
        // belongs to on a `split` row, and absent on a `void`.
        Field::new("outcome", DataType::UInt16, true),
        // What one contract on `outcome` pays. Only a `split` carries it.
        opt_float("payout", encoding),
        // How many outcomes a `void` refunds across.
        Field::new("outcome_count", DataType::UInt16, true),
    ]))
}

/// `references`: the mark and oracle prices a venue publishes.
///
/// Separate from the book because they are separate facts. A derivatives
/// venue margins against its own mark and charges funding on its own
/// oracle; storing only the book leaves a replay to substitute the mid for
/// both, which liquidates positions the venue would not have.
pub fn references(encoding: FixedEncoding) -> SchemaRef {
    Arc::new(Schema::new(vec![
        ts("ts_init"),
        ts("ts_event"),
        text("instrument_id"),
        outcome(),
        // Either may be absent: a venue can publish one and not the other,
        // and a null must not be read as a zero price.
        opt_float("mark", encoding),
        opt_float("oracle", encoding),
        opt_text("source_vendor"),
    ]))
}

/// `funding`: perpetual funding rates as they became due.
pub fn funding(encoding: FixedEncoding) -> SchemaRef {
    Arc::new(Schema::new(vec![
        ts("ts_init"),
        ts("ts_event"),
        text("instrument_id"),
        // Per-interval rate, not annualised: positive means longs pay.
        float("rate", encoding),
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
/// Takes no encoding because it holds no fixed-point column: counts and a
/// digest, all of them integers or text.
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
pub fn signals(encoding: FixedEncoding) -> SchemaRef {
    Arc::new(Schema::new(vec![
        // When the strategy wants to act. Replay submits the intent the
        // first time the clock reaches it.
        ts("ts"),
        text("instrument_id"),
        outcome(),
        // "buy" | "sell"
        text("side"),
        float("quantity", encoding),
        // "market" | "limit"
        text("kind"),
        // Required for limit orders, ignored for market ones.
        opt_float("limit_price", encoding),
        // "gtc" | "ioc" | "fok"; defaults per order kind when null.
        opt_text("time_in_force"),
        opt_text("tag"),
        Field::new("reduce_only", DataType::Boolean, true),
        // Add-liquidity-only. Absent reads as false, which is the
        // reading that cannot silently turn a taker into a maker.
        Field::new("post_only", DataType::Boolean, true),
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

/// `commands`: a complete declarative order lifecycle.
///
/// Submit rows populate the order fields. Cancel rows only require
/// `client_order_id`; amend rows require at least one of `quantity` or
/// `limit_price`. Client IDs are strategy-local strings and are mapped to
/// engine order IDs without exposing implementation-dependent counters.
pub fn commands(encoding: FixedEncoding) -> SchemaRef {
    Arc::new(Schema::new(vec![
        ts("ts"),
        // "submit" | "cancel" | "amend"
        text("action"),
        text("client_order_id"),
        opt_text("instrument_id"),
        Field::new("outcome", DataType::UInt16, true),
        opt_text("side"),
        opt_float("quantity", encoding),
        opt_text("kind"),
        opt_float("limit_price", encoding),
        opt_text("time_in_force"),
        opt_text("tag"),
        Field::new("reduce_only", DataType::Boolean, true),
        // Add-liquidity-only. Absent reads as false, which is the
        // reading that cannot silently turn a taker into a maker.
        Field::new("post_only", DataType::Boolean, true),
    ]))
}

pub fn commands_options() -> TableOptions {
    TableOptions {
        time_column: Some("ts".to_string()),
        sort_key: vec!["ts".to_string()],
        ..Default::default()
    }
}

/// `bt_run`: the run manifest.
pub fn run(encoding: FixedEncoding) -> SchemaRef {
    Arc::new(Schema::new(vec![
        ts("ts"),
        text("run_id"),
        text("config_digest"),
        float("starting_cash", encoding),
        float("final_cash", encoding),
        float("realized_pnl", encoding),
        float("commissions", encoding),
        opt_int("simulated_through_ns"),
        int("records_processed"),
        Field::new("settlement_applied", DataType::Boolean, false),
        opt_text("warnings"),
    ]))
}

/// `bt_orders`: every order the run produced.
pub fn orders(encoding: FixedEncoding) -> SchemaRef {
    Arc::new(Schema::new(vec![
        ts("ts"),
        int("order_id"),
        text("instrument_id"),
        outcome(),
        text("side"),
        text("kind"),
        opt_float("limit_price", encoding),
        float("quantity", encoding),
        float("filled", encoding),
        text("time_in_force"),
        text("status"),
        opt_text("reject_reason"),
        opt_text("tag"),
        Field::new("reduce_only", DataType::Boolean, false),
    ]))
}

/// `bt_fills`: every execution. The authoritative record -- positions are a
/// fold over this table and nothing else.
pub fn fills(encoding: FixedEncoding) -> SchemaRef {
    Arc::new(Schema::new(vec![
        ts("ts"),
        int("order_id"),
        text("instrument_id"),
        outcome(),
        text("side"),
        float("price", encoding),
        float("quantity", encoding),
        float("commission", encoding),
        Field::new("is_taker", DataType::Boolean, false),
        opt_text("tag"),
    ]))
}

/// `bt_positions`: where the run finished.
pub fn positions(encoding: FixedEncoding) -> SchemaRef {
    Arc::new(Schema::new(vec![
        ts("ts"),
        text("instrument_id"),
        outcome(),
        float("quantity", encoding),
        float("average_price", encoding),
        float("realized_pnl", encoding),
        float("commissions", encoding),
        opt_float("settlement_pnl", encoding),
        opt_float("market_exit_pnl", encoding),
    ]))
}

/// `bt_equity`: the equity curve, sampled on a fixed interval of simulated
/// time. This is the table `h5i_db.quant.returns()` reads.
pub fn equity(encoding: FixedEncoding) -> SchemaRef {
    Arc::new(Schema::new(vec![
        ts("ts"),
        float("cash", encoding),
        float("position_value", encoding),
        float("equity", encoding),
        float("realized_pnl", encoding),
        float("unrealized_pnl", encoding),
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
pub fn market_data_tables(encoding: FixedEncoding) -> Vec<(&'static str, SchemaRef, TableOptions)> {
    vec![
        (BOOK_DELTAS, book_deltas(encoding), market_data_options()),
        (TRADES, trades(encoding), market_data_options()),
        (BARS, bars(encoding), market_data_options()),
        (INSTRUMENTS, instruments(encoding), market_data_options()),
        (RESOLUTIONS, resolutions(encoding), market_data_options()),
        (FUNDING, funding(encoding), market_data_options()),
        (REFERENCES, references(encoding), market_data_options()),
    ]
}

/// The ingest log, which is metadata *about* loads rather than market data,
/// and so is indexed on when the load happened.
pub fn ingest_log_table() -> (&'static str, SchemaRef, TableOptions) {
    (INGEST_LOG, ingest_log(), run_output_options())
}

/// The signals table, which a caller creates only when driving a Tier 1
/// replay from stored intent.
pub fn signals_table(encoding: FixedEncoding) -> (&'static str, SchemaRef, TableOptions) {
    (SIGNALS, signals(encoding), signals_options())
}

/// The optional lifecycle-aware strategy command table.
pub fn commands_table(encoding: FixedEncoding) -> (&'static str, SchemaRef, TableOptions) {
    (COMMANDS, commands(encoding), commands_options())
}

/// Every run-output table.
pub fn run_output_tables(encoding: FixedEncoding) -> Vec<(&'static str, SchemaRef, TableOptions)> {
    vec![
        (RUN, run(encoding), run_output_options()),
        (ORDERS, orders(encoding), run_output_options()),
        (FILLS, fills(encoding), run_output_options()),
        (POSITIONS, positions(encoding), run_output_options()),
        (EQUITY, equity(encoding), run_output_options()),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn market_data_is_time_indexed_on_the_column_replay_sorts_by() {
        // Pruning must happen on ts_init, because that is the order the
        // merge reads records in.
        for (name, schema, options) in market_data_tables(FixedEncoding::default()) {
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
        for (name, schema, _) in market_data_tables(FixedEncoding::default()) {
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
        let trades = trades(FixedEncoding::default());
        assert!(trades.field_with_name("aggressor").unwrap().is_nullable());
        assert!(!trades.field_with_name("price").unwrap().is_nullable());

        // A clear or gap row has no side, price, or size.
        let deltas = book_deltas(FixedEncoding::default());
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
        for (name, schema, options) in run_output_tables(FixedEncoding::default()) {
            assert!(name.starts_with("bt_"), "{name} must be a bt_ table");
            assert_eq!(options.time_column.as_deref(), Some("ts"));
            assert!(schema.field_with_name("ts").is_ok());
        }
    }

    #[test]
    fn fills_carry_everything_needed_to_rebuild_a_position() {
        // The audit claim: bt_fills alone must reconstruct bt_positions.
        let fills = fills(FixedEncoding::default());
        for column in [
            "instrument_id",
            "outcome",
            "side",
            "price",
            "quantity",
            "commission",
        ] {
            assert!(
                fills.field_with_name(column).is_ok(),
                "fills needs {column}"
            );
        }
    }

    #[test]
    fn table_names_do_not_collide() {
        let mut names: Vec<&str> = market_data_tables(FixedEncoding::default())
            .into_iter()
            .map(|(n, _, _)| n)
            .chain(
                run_output_tables(FixedEncoding::default())
                    .into_iter()
                    .map(|(n, _, _)| n),
            )
            .chain(std::iter::once(signals_table(FixedEncoding::default()).0))
            .chain(std::iter::once(ingest_log_table().0))
            .collect();
        let count = names.len();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), count, "table names must be unique");
    }
}
