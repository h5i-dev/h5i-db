//! Where a run's wall time actually goes: storage, decode, kernel, write.
//!
//! `tests/scale.rs` measures the kernel against *generated* records, which is
//! the right way to prove the replay streams but says nothing about the path a
//! real run takes. Every real run goes through storage first: a scan that
//! produces Arrow batches, then a decode that turns those batches into
//! `Record`s. Neither had ever been measured, so every claim about backtest
//! performance was an argument rather than a number.
//!
//! This splits one run into the phases that can be optimised independently:
//!
//! ```text
//!   scan    Arrow batches out of storage, nothing decoded
//!   decode  Arrow -> Record, the seam between the database and the kernel
//!   kernel  replay merge + engine, from records already in memory
//!   run     the whole thing end to end, including the fork and the writes
//! ```
//!
//! It then runs a *study*: N trials over the same window and the same pin.
//! `BacktestStudy` forbids varying any field that would change the data read
//! (`_NON_TUNABLE` in `backtest_study.py`), so every trial in a grid decodes a
//! byte-identical record stream. The `study_redundant_decode_share` figure is
//! how much of a sweep's wall time that repetition costs, which is the number
//! that says whether sharing the decode is worth building.
//!
//! ```sh
//! cargo bench -p h5i-db-backtest --bench replay_path
//! cargo bench -p h5i-db-backtest --bench replay_path -- --book 400000 --trials 8
//! ```
//!
//! Deterministic by construction: the generator is a pure function of the
//! arguments, so two invocations at the same scale compare directly.

use std::path::PathBuf;
use std::time::Instant;

use h5i_db_backtest::book::BookDelta;
use h5i_db_backtest::engine::{Engine, OrderRequest, SignalReplay};
use h5i_db_backtest::event::{MarketEvent, Record};
use h5i_db_backtest::instrument::{Instrument, InstrumentId, InstrumentSet, OutcomeId};
use h5i_db_backtest::replay::{Replay, priority};
use h5i_db_backtest::run::{RunSpec, run_in_fork};
use h5i_db_backtest::store;
use h5i_db_backtest::types::{Money, Price, Qty, Side, Stamps, UnixNanos};
use h5i_db_backtest::window::TimeWindow;
use h5i_db_core::Database;
use h5i_db_core::database::{ReadAt, ScanOptions};

/// Nanoseconds between consecutive book events.
const TICK_NANOS: i64 = 1_000_000;
/// Rows per write commit, so seeding never holds the whole dataset.
const COMMIT_ROWS: usize = 50_000;

struct Args {
    book_events: usize,
    trades: usize,
    instruments: usize,
    signals: usize,
    trials: usize,
    common_quotes: bool,
    dir: Option<PathBuf>,
}

impl Default for Args {
    fn default() -> Self {
        // Small enough to run on a laptop in under a minute, large enough that
        // the phase split is signal rather than noise.
        Self {
            book_events: 200_000,
            trades: 40_000,
            instruments: 8,
            signals: 200,
            trials: 4,
            common_quotes: false,
            dir: None,
        }
    }
}

fn parse_args() -> Args {
    let mut args = Args::default();
    let mut argv = std::env::args().skip(1);
    while let Some(flag) = argv.next() {
        // `cargo bench` forwards harness flags we do not implement; ignoring
        // them is friendlier than refusing to start.
        let value = |argv: &mut dyn Iterator<Item = String>| -> usize {
            argv.next()
                .and_then(|v| v.parse().ok())
                .unwrap_or_else(|| panic!("{flag} needs a numeric value"))
        };
        match flag.as_str() {
            "--book" => args.book_events = value(&mut argv),
            "--trades" => args.trades = value(&mut argv),
            "--instruments" => args.instruments = value(&mut argv),
            "--signals" => args.signals = value(&mut argv),
            "--trials" => args.trials = value(&mut argv),
            "--common-quotes" => args.common_quotes = true,
            "--dir" => args.dir = argv.next().map(PathBuf::from),
            "--bench" | "--nocapture" => {}
            other if other.starts_with("--") => {}
            _ => {}
        }
    }
    assert!(args.instruments > 0, "need at least one instrument");
    assert!(
        args.book_events >= args.instruments,
        "each instrument needs an opening snapshot"
    );
    args
}

// -- generation -------------------------------------------------------------

fn instrument_id(index: usize) -> String {
    format!("BENCH-PERP-{index:04}")
}

fn instrument_set(count: usize) -> InstrumentSet {
    let mut set = InstrumentSet::new();
    for index in 0..count {
        set.insert(
            Instrument::perpetual(instrument_id(index), "bench")
                .unwrap()
                .with_tick_size(Price::from_f64(0.01).unwrap()),
        )
        .unwrap();
    }
    set
}

/// A mid price that oscillates slowly, so fills stay plausible.
fn mid_at(step: usize) -> f64 {
    100.0 + ((step % 400) as f64 - 200.0) * 0.01
}

/// Book events in global timestamp order, instruments round-robin.
///
/// The first event per instrument is a snapshot, because a delta against a
/// book that was never opened is not a thing the engine should have to model.
fn book_record(step: usize, ids: &[InstrumentId], count: usize, common_quotes: bool) -> Record {
    let id = ids[step % count].clone();
    let ts = Stamps::immediate(UnixNanos::new(step as i64 * TICK_NANOS));
    let mid = mid_at(step);
    if common_quotes || step < count {
        Record::new(
            ts,
            id,
            OutcomeId::FIRST,
            MarketEvent::BookSnapshot {
                bids: vec![(
                    Price::from_f64(mid - 0.01).unwrap(),
                    Qty::from_f64(500.0).unwrap(),
                )],
                asks: vec![(
                    Price::from_f64(mid + 0.01).unwrap(),
                    Qty::from_f64(500.0).unwrap(),
                )],
            },
        )
    } else {
        let (side, price) = if step % 2 == 0 {
            (Side::Buy, mid - 0.01)
        } else {
            (Side::Sell, mid + 0.01)
        };
        Record::new(
            ts,
            id,
            OutcomeId::FIRST,
            MarketEvent::BookDelta(BookDelta::set(
                side,
                Price::from_f64(price).unwrap(),
                Qty::from_f64(500.0).unwrap(),
            )),
        )
    }
}

fn trade_record(index: usize, ids: &[InstrumentId], count: usize, spacing: usize) -> Record {
    let step = index * spacing;
    Record::new(
        Stamps::immediate(UnixNanos::new(step as i64 * TICK_NANOS)),
        ids[index % count].clone(),
        OutcomeId::FIRST,
        MarketEvent::Trade {
            price: Price::from_f64(mid_at(step)).unwrap(),
            size: Qty::from_f64(1.0).unwrap(),
            aggressor: Some(if index % 2 == 0 { Side::Buy } else { Side::Sell }),
        },
    )
}

/// Order intents spread evenly across the window, alternating direction so the
/// position stays near flat and margin never becomes the thing being measured.
fn signals(args: &Args, ids: &[InstrumentId]) -> Vec<(UnixNanos, OrderRequest)> {
    if args.signals == 0 {
        return Vec::new();
    }
    let spacing = (args.book_events / args.signals).max(1);
    (0..args.signals)
        .map(|index| {
            // Start after every instrument has an open book, or the order is
            // cancelled for want of anything to match against.
            let step = args.instruments + index * spacing;
            let side = if index % 2 == 0 { Side::Buy } else { Side::Sell };
            (
                UnixNanos::new(step as i64 * TICK_NANOS),
                OrderRequest::market(
                    ids[index % ids.len()].clone(),
                    OutcomeId::FIRST,
                    side,
                    Qty::from_f64(1.0).unwrap(),
                ),
            )
        })
        .collect()
}

async fn seed(db: &Database, args: &Args, ids: &[InstrumentId]) {
    store::create_market_data_tables(db).await.unwrap();
    let instruments: Vec<Instrument> = (0..args.instruments)
        .map(|index| {
            Instrument::perpetual(instrument_id(index), "bench")
                .unwrap()
                .with_tick_size(Price::from_f64(0.01).unwrap())
        })
        .collect();
    store::write_instruments(db, &instruments, UnixNanos::new(0))
        .await
        .unwrap();

    // Chunked so seeding a large run does not materialise the whole dataset,
    // which would make the bench's own memory the limiting factor.
    let mut chunk = Vec::with_capacity(COMMIT_ROWS);
    for step in 0..args.book_events {
        chunk.push(book_record(step, ids, args.instruments, args.common_quotes));
        if chunk.len() == COMMIT_ROWS {
            store::write_book_events(db, &chunk).await.unwrap();
            chunk.clear();
        }
    }
    if !chunk.is_empty() {
        store::write_book_events(db, &chunk).await.unwrap();
        chunk.clear();
    }

    let spacing = (args.book_events / args.trades.max(1)).max(1);
    for index in 0..args.trades {
        chunk.push(trade_record(index, ids, args.instruments, spacing));
        if chunk.len() == COMMIT_ROWS {
            store::write_trades(db, &chunk).await.unwrap();
            chunk.clear();
        }
    }
    if !chunk.is_empty() {
        store::write_trades(db, &chunk).await.unwrap();
    }
}

// -- measurement ------------------------------------------------------------

struct Phase {
    name: &'static str,
    millis: f64,
    rows: Option<u64>,
}

impl Phase {
    fn per_second(&self) -> Option<f64> {
        self.rows
            .filter(|_| self.millis > 0.0)
            .map(|rows| rows as f64 / (self.millis / 1000.0))
    }
}

async fn timed<F, T>(name: &'static str, rows: Option<u64>, f: F) -> (Phase, T)
where
    F: std::future::Future<Output = T>,
{
    let start = Instant::now();
    let out = f.await;
    let phase = Phase {
        name,
        millis: start.elapsed().as_secs_f64() * 1000.0,
        rows,
    };
    eprintln!(
        "  {:<38} {:>10.1} ms{}",
        phase.name,
        phase.millis,
        phase
            .per_second()
            .map(|r| format!("  ({r:>12.0} rows/s)"))
            .unwrap_or_default()
    );
    (phase, out)
}

#[tokio::main]
async fn main() {
    let args = parse_args();
    let ids: Vec<InstrumentId> = (0..args.instruments)
        .map(|index| InstrumentId::new(instrument_id(index)).unwrap())
        .collect();

    let temp = tempfile::tempdir().unwrap();
    let root = args
        .dir
        .clone()
        .unwrap_or_else(|| temp.path().to_path_buf())
        .join("replay-path.db");
    let db = Database::create(&root).await.unwrap();

    eprintln!(
        "replay path: {} book events, {} trades, {} instruments, {} signals, {} trials",
        args.book_events, args.trades, args.instruments, args.signals, args.trials
    );

    let mut phases = Vec::new();

    let (phase, ()) = timed(
        "seed: write market data",
        Some((args.book_events + args.trades) as u64),
        seed(&db, &args, &ids),
    )
    .await;
    phases.push(phase);

    // -- scan: what storage costs before anything is decoded ----------------

    let (phase, book_rows) = timed("scan: book_deltas (arrow only)", None, async {
        let (batches, _) = db
            .scan("book_deltas", ReadAt::Latest, ScanOptions::default())
            .await
            .unwrap();
        batches.iter().map(|b| b.num_rows() as u64).sum::<u64>()
    })
    .await;
    phases.push(Phase {
        rows: Some(book_rows),
        ..phase
    });

    let (phase, trade_rows) = timed("scan: trades (arrow only)", None, async {
        let (batches, _) = db
            .scan("trades", ReadAt::Latest, ScanOptions::default())
            .await
            .unwrap();
        batches.iter().map(|b| b.num_rows() as u64).sum::<u64>()
    })
    .await;
    phases.push(Phase {
        rows: Some(trade_rows),
        ..phase
    });

    // -- decode: the seam ---------------------------------------------------

    let (book_phase, book_records) = timed("decode: book_deltas -> Record", None, async {
        store::read_book_events(&db, ReadAt::Latest, None)
            .await
            .unwrap()
    })
    .await;
    let decoded_book = book_records.len() as u64;
    phases.push(Phase {
        rows: Some(decoded_book),
        ..book_phase
    });
    let book_decode_ms = phases.last().unwrap().millis;

    let (trade_phase, trade_records) = timed("decode: trades -> Record", None, async {
        let source = store::trade_source(&db, ReadAt::Latest, None).await.unwrap();
        source.collect::<Result<Vec<Record>, _>>().unwrap()
    })
    .await;
    phases.push(Phase {
        rows: Some(trade_records.len() as u64),
        ..trade_phase
    });
    let decode_ms = book_decode_ms + phases.last().unwrap().millis;

    // -- kernel: records already in memory ----------------------------------

    let instruments = instrument_set(args.instruments);
    let intents = signals(&args, &ids);
    let total_records = (book_records.len() + trade_records.len()) as u64;

    let (phase, result) = timed("kernel: replay + engine", Some(total_records), async {
        let mut replay = Replay::builder()
            .stream("book", priority::SNAPSHOT, book_records.clone())
            .stream("trades", priority::TRADE, trade_records.clone())
            .build()
            .unwrap();
        let mut strategy = SignalReplay::new(intents.clone()).unwrap();
        let mut engine = Engine::builder(instruments.clone())
            .starting_cash(Money::from_f64(1_000_000.0).unwrap())
            .start(UnixNanos::new(0))
            .equity_interval_nanos(60 * 1_000_000_000)
            .unwrap()
            .build();
        engine.run(&mut replay, &mut strategy).unwrap()
    })
    .await;
    let kernel_ms = phase.millis;
    phases.push(phase);
    eprintln!(
        "  ({} fills, so the kernel is doing work)",
        result.fills.len()
    );

    drop(book_records);
    drop(trade_records);

    // -- step breakdown of one warm run -------------------------------------
    //
    // The end-to-end number below is far larger than scan + decode + kernel,
    // and guessing at the gap is how the wrong thing gets optimised. These
    // steps mirror `run_in_fork` / `run_in_place` in order, on a base that
    // already holds the `bt_*` schema, which is the steady state a sweep runs
    // in. Their sum should account for the end-to-end figure.

    let (phase, fork) = timed("step: create + open fork", None, async {
        db.create_fork(
            "bench-steps",
            Some("phase split".to_string()),
            None,
            serde_json::Map::new(),
        )
        .await
        .unwrap();
        db.open_fork("bench-steps").await.unwrap()
    })
    .await;
    let fork_ms = phase.millis;
    phases.push(phase);

    // A fork inherits the market-data tables but not the `bt_*` ones, because
    // no run has ever written them anywhere a fork could inherit from. Five
    // table creations, five metadata commits, once per run.
    let (phase, ()) = timed("step: create_run_tables (in fork)", None, async {
        store::create_run_tables(&fork).await.unwrap();
    })
    .await;
    let create_ms = phase.millis;
    phases.push(phase);

    let (phase, ()) = timed("step: read_instruments", None, async {
        store::read_instruments(&fork, ReadAt::Latest).await.unwrap();
    })
    .await;
    let instruments_ms = phase.millis;
    phases.push(phase);

    // Funding and resolutions are absent for this dataset. An absent table is
    // a fact, not a failure, but the scan that discovers that still costs
    // something and a run performs both.
    let (phase, ()) = timed("step: scan absent funding + resolutions", None, async {
        let _ = store::funding_source(&fork, ReadAt::Latest, None)
            .await
            .unwrap();
        store::read_resolutions(&fork, ReadAt::Latest).await.unwrap();
    })
    .await;
    let absent_ms = phase.millis;
    phases.push(phase);

    // Folded here, and again inside `write_positions`, on top of the portfolio
    // the engine already maintained during the run.
    let (phase, ()) = timed(
        "step: Portfolio::replay (run does it 2x)",
        Some(result.fills.len() as u64),
        async {
            h5i_db_backtest::position::Portfolio::replay(&result.fills).unwrap();
        },
    )
    .await;
    let portfolio_ms = phase.millis;
    phases.push(phase);

    let settlement = h5i_db_backtest::settlement::settle(
        &h5i_db_backtest::position::Portfolio::replay(&result.fills).unwrap(),
        &[],
        result.simulated_through,
        &result.marks,
    )
    .unwrap();

    let (phase, ()) = timed("step: write_run (one transaction)", None, async {
        store::write_run(
            &fork,
            "bench-steps",
            "digest",
            &result,
            &settlement,
            UnixNanos::new(0),
        )
        .await
        .unwrap();
    })
    .await;
    let write_ms = phase.millis;
    phases.push(phase);

    // -- end to end, and then a study of it ---------------------------------

    let window = TimeWindow::new(
        UnixNanos::new(0),
        UnixNanos::new(args.book_events as i64 * TICK_NANOS + 1),
    )
    .unwrap();

    let run_spec = |run_id: String| {
        RunSpec::new(run_id, Money::from_f64(1_000_000.0).unwrap())
            .window(window)
            .equity_interval_nanos(60 * 1_000_000_000)
    };

    let (phase, records_processed) = timed("run: end to end (fork + write)", None, async {
        let mut strategy = SignalReplay::new(intents.clone()).unwrap();
        let report = run_in_fork(&db, run_spec("bench-single".to_string()), &mut strategy, |b| b)
            .await
            .unwrap();
        report.result.records_processed
    })
    .await;
    let run_ms = phase.millis;
    phases.push(Phase {
        rows: Some(records_processed),
        ..phase
    });

    let (phase, ()) = timed(
        "study: trials x run (same window, same pin)",
        Some(records_processed * args.trials as u64),
        async {
            for trial in 0..args.trials {
                let mut strategy = SignalReplay::new(intents.clone()).unwrap();
                run_in_fork(
                    &db,
                    run_spec(format!("bench-study-{trial:04}")),
                    &mut strategy,
                    |b| b,
                )
                .await
                .unwrap();
            }
        },
    )
    .await;
    let study_ms = phase.millis;
    phases.push(phase);

    // -- derived figures ----------------------------------------------------

    // Every trial re-reads and re-decodes a byte-identical record stream, so
    // all but the first decode is provably wasted work. This is the size of
    // the prize for sharing it.
    let redundant_ms = decode_ms * (args.trials.saturating_sub(1)) as f64;
    let redundant_share = if study_ms > 0.0 {
        redundant_ms / study_ms
    } else {
        0.0
    };
    let decode_share_of_run = if run_ms > 0.0 { decode_ms / run_ms } else { 0.0 };

    // Every step a run performs, so the end-to-end figure is accounted for
    // rather than assumed.
    let accounted = decode_ms
        + kernel_ms
        + write_ms
        + create_ms
        + fork_ms
        + instruments_ms
        + absent_ms
        + portfolio_ms;
    let unaccounted_share = if run_ms > 0.0 {
        (run_ms - accounted).max(0.0) / run_ms
    } else {
        0.0
    };

    eprintln!();
    eprintln!("  one warm run, by share of wall time:");
    for (label, ms) in [
        ("decode (arrow -> Record)", decode_ms),
        ("kernel (replay + engine)", kernel_ms),
        ("create_run_tables (5 commits)", create_ms),
        ("write_run (one transaction)", write_ms),
        ("fork create + open", fork_ms),
        ("read_instruments", instruments_ms),
        ("scan absent tables", absent_ms),
        ("Portfolio::replay", portfolio_ms),
    ] {
        eprintln!("    {:<32} {:>6.1} ms  {:>4.0}%", label, ms, ms / run_ms * 100.0);
    }
    eprintln!(
        "    {:<32} {:>6.1} ms  {:>4.0}%",
        "unaccounted",
        (run_ms - accounted).max(0.0),
        unaccounted_share * 100.0
    );
    eprintln!(
        "  {:.0}% of a {}-trial study is redundant decode",
        redundant_share * 100.0,
        args.trials
    );

    let report = serde_json::json!({
        "config": {
            "book_events": args.book_events,
            "trades": args.trades,
            "instruments": args.instruments,
            "signals": args.signals,
            "trials": args.trials,
            "common_quotes": args.common_quotes,
        },
        "phases": phases
            .iter()
            .map(|p| serde_json::json!({
                "name": p.name,
                "wall_ms": p.millis,
                "rows": p.rows,
                "rows_per_sec": p.per_second(),
            }))
            .collect::<Vec<_>>(),
        "derived": {
            "decode_ms": decode_ms,
            "kernel_ms": kernel_ms,
            "portfolio_replay_ms": portfolio_ms,
            "fork_create_ms": fork_ms,
            "create_run_tables_ms": create_ms,
            "read_instruments_ms": instruments_ms,
            "scan_absent_ms": absent_ms,
            "write_run_ms": write_ms,
            "run_ms": run_ms,
            "study_ms": study_ms,
            "decode_share_of_run": decode_share_of_run,
            "unaccounted_share_of_run": unaccounted_share,
            "study_redundant_decode_ms": redundant_ms,
            "study_redundant_decode_share": redundant_share,
        },
    });
    println!("{}", serde_json::to_string_pretty(&report).unwrap());
}
