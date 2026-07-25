//! `h5i-db demo`: the whole argument in thirty seconds, on real data.
//!
//! The scenario is a **vendor restatement**, chosen deliberately. A planted
//! event-time leak (a feature reading tomorrow's close) would show a *zero*
//! leakage delta, because those rows were always in the table — demonstrating
//! the flagship claim with a scenario the arrival-axis check cannot see would
//! be a lie told with a straight face. A restatement is the case the arrival
//! axis exists for, and it is an ordinary Tuesday in market data.
//!
//! Everything printed here is executed, not narrated: the numbers come from
//! the database that was just built.

use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use h5i_db_core::{Database, ReadAt, Result, TableOptions, WriteOptions};
use h5i_db_query::{H5iSession, ResearchPin, SessionOptions};

/// Minute bars, two symbols, one session — small enough to be instant.
const BARS_PER_SYMBOL: i64 = 120;
const SYMBOLS: [&str; 2] = ["AAPL", "MSFT"];
/// 2026-07-01T09:30:00Z in nanoseconds.
const SESSION_START_NS: i64 = 1_782_898_200_000_000_000;
const MINUTE_NS: i64 = 60_000_000_000;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
        Field::new("size", DataType::Int64, false),
    ]))
}

/// Deterministic bars. `bad_print` injects the erroneous tick that the vendor
/// will later correct — the thing the whole demo turns on.
fn bars(bad_print: bool) -> RecordBatch {
    let (mut ts, mut sym, mut px, mut sz) = (vec![], vec![], vec![], vec![]);
    for minute in 0..BARS_PER_SYMBOL {
        for (s, name) in SYMBOLS.iter().enumerate() {
            ts.push(SESSION_START_NS + minute * MINUTE_NS);
            sym.push(*name);
            // A gentle deterministic walk, distinct per symbol.
            let base = 100.0 + (s as f64) * 300.0;
            let mut price = base + (minute as f64) * 0.25;
            // The fat-finger print: minute 60 of AAPL comes in 10x too high.
            if bad_print && s == 0 && minute == 60 {
                price *= 10.0;
            }
            px.push(price);
            sz.push(100 + minute);
        }
    }
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(
                Int64Array::from(ts)
                    .reinterpret_cast::<arrow::datatypes::TimestampNanosecondType>()
                    .with_timezone("UTC"),
            ),
            Arc::new(StringArray::from(sym)),
            Arc::new(Float64Array::from(px)),
            Arc::new(Int64Array::from(sz)),
        ],
    )
    .map_err(|e| panic!("demo data: {e}"))
    .unwrap()
}

fn step(n: usize, title: &str, cmd: &str) {
    println!("\n[{n}] {title}");
    println!("    $ {cmd}");
}

async fn scalar(db: &Arc<Database>, sql: &str, pin: Option<ResearchPin>) -> Result<f64> {
    let session = match pin {
        Some(pin) => H5iSession::new_pinned(db.clone(), SessionOptions::default(), pin).await,
        None => H5iSession::new(db.clone(), SessionOptions::default()).await,
    }
    .map_err(|e| h5i_db_core::Error::internal(e.to_string()))?;
    let batches = session
        .sql(sql)
        .await
        .map_err(|e| h5i_db_core::Error::internal(e.to_string()))?
        .collect()
        .await
        .map_err(|e| h5i_db_core::Error::internal(e.to_string()))?;
    let value = batches
        .first()
        .filter(|b| b.num_rows() > 0)
        .and_then(|b| b.column(0).as_any().downcast_ref::<Float64Array>().cloned())
        .map(|a| a.value(0))
        .ok_or_else(|| h5i_db_core::Error::internal("demo query returned no scalar"))?;
    Ok(value)
}

/// Run the tour. Returns the directory the database was left in.
pub async fn run(dir: Option<PathBuf>, keep: bool) -> Result<PathBuf> {
    let base = match &dir {
        Some(d) => d.clone(),
        None => std::env::temp_dir().join(format!("h5i-db-demo-{}", uuid::Uuid::new_v4())),
    };
    std::fs::create_dir_all(&base).map_err(|e| h5i_db_core::Error::io(base.display(), e))?;
    let db_path = base.join("demo.db");

    println!("h5i-db demo — a vendor restatement, end to end");
    println!("    workspace: {}", base.display());

    step(
        1,
        "Create the database and a tick table",
        "h5i-db init demo.db",
    );
    let db = Arc::new(Database::create(&db_path).await?);
    db.create_table(
        "trades",
        schema(),
        TableOptions {
            time_column: Some("ts".into()),
            ..Default::default()
        },
    )
    .await?;

    step(
        2,
        "Ingest the session as the vendor first published it",
        "h5i-db ingest demo.db trades day1.parquet --idempotency-key demo-load-1",
    );
    let v1 = db
        .write(
            "trades",
            vec![bars(true)],
            WriteOptions {
                idempotency_key: Some("demo-load-1".into()),
                note: Some("vendor feed, first publication".into()),
                ..Default::default()
            },
        )
        .await?
        .sequence;
    println!("    -> version {v1}, {} rows", BARS_PER_SYMBOL * 2);
    println!("       (one AAPL print came in 10x too high — nobody has noticed yet)");

    let vwap_sql = "SELECT vwap(price, size) AS v FROM trades WHERE symbol = 'AAPL'";
    step(
        3,
        "Compute the metric a strategy would have traded on",
        "h5i-db query demo.db \"SELECT vwap(price, size) FROM trades WHERE symbol = 'AAPL'\"",
    );
    let vwap_before = scalar(&db, vwap_sql, None).await?;
    println!("    -> VWAP {vwap_before:.4}");

    step(
        4,
        "The vendor corrects the bad print — previewed before it lands",
        "h5i-db replace-range demo.db trades --input fix.parquet --start … --end … --plan",
    );
    let bad_ts = SESSION_START_NS + 60 * MINUTE_NS;
    let plan = db
        .plan_replace_range(
            "trades",
            bad_ts,
            bad_ts + MINUTE_NS,
            vec![corrected_minute()?],
            WriteOptions {
                note: Some("vendor restatement of the 10:30 print".into()),
                ..Default::default()
            },
        )
        .await?;
    println!(
        "    -> plan {} affects {} row(s); nothing visible has changed yet",
        plan.plan_id, plan.summary.rows_affected
    );

    step(
        5,
        "A human (or a rule) applies it",
        "h5i-db plan apply demo.db trades <plan_id>",
    );
    let v2 = db.apply_plan(&plan).await?.sequence;
    println!("    -> version {v2}");

    step(
        6,
        "The same metric, now that the correction has arrived",
        "h5i-db query demo.db \"SELECT vwap(price, size) FROM trades WHERE symbol = 'AAPL'\"",
    );
    let vwap_after = scalar(&db, vwap_sql, None).await?;
    println!(
        "    -> VWAP {vwap_after:.4}  ({:+.4} vs step 3)",
        vwap_after - vwap_before
    );

    step(
        7,
        "How much of the original number was data that had not arrived?",
        "h5i-db arrival-delta demo.db \"SELECT vwap(price, size) …\" --as-of 1",
    );
    let report = h5i_db_query::arrival_delta(
        db.clone(),
        vwap_sql,
        ReadAt::Version(v1),
        h5i_db_query::DEFAULT_TOLERANCE,
    )
    .await
    .map_err(|e| h5i_db_core::Error::internal(e.to_string()))?;
    let col = &report.columns[0];
    println!(
        "    -> changed: {}, head {:.4} vs as-of {:.4}, delta {:+.4}",
        report.changed,
        col.head.unwrap_or_default(),
        col.asof.unwrap_or_default(),
        col.delta.unwrap_or_default()
    );
    println!(
        "       vacuous: {} (there is real arrival history here)",
        report.vacuous
    );

    step(
        8,
        "The other axis: a session that cannot read past its decision instant",
        "h5i-db query demo.db \"SELECT max(price) …\" --decision-time 2026-07-01T10:00:00Z",
    );
    let cutoff_ns = SESSION_START_NS + 30 * MINUTE_NS;
    let max_sql = "SELECT max(price) AS v FROM trades WHERE symbol = 'AAPL'";
    let unpinned = scalar(&db, max_sql, None).await?;
    let pinned = scalar(
        &db,
        max_sql,
        Some(ResearchPin::default().with_event_time_cutoff_ns(cutoff_ns)),
    )
    .await?;
    println!("    -> unpinned max {unpinned:.4}; pinned at 10:00 max {pinned:.4}");
    println!("       the later bars are not filtered — inside that session they do not exist");

    println!("\nDone. Next:");
    println!("    h5i-db context {}", db_path.display());
    println!(
        "    h5i-db query {} \"SELECT * FROM trades\" --max-rows 5",
        db_path.display()
    );
    if !keep && dir.is_none() {
        let _ = std::fs::remove_dir_all(&base);
        println!("    (workspace removed; pass --keep to inspect it)");
    }
    Ok(base)
}

/// The corrected 10:30 AAPL print, plus the MSFT bar that shares the minute
/// (a range replacement covers the whole minute, not one symbol).
fn corrected_minute() -> Result<RecordBatch> {
    let ts = SESSION_START_NS + 60 * MINUTE_NS;
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(
                Int64Array::from(vec![ts, ts])
                    .reinterpret_cast::<arrow::datatypes::TimestampNanosecondType>()
                    .with_timezone("UTC"),
            ),
            Arc::new(StringArray::from(vec!["AAPL", "MSFT"])),
            Arc::new(Float64Array::from(vec![
                100.0 + 60.0 * 0.25,
                400.0 + 60.0 * 0.25,
            ])),
            Arc::new(Int64Array::from(vec![160i64, 160])),
        ],
    )
    .map_err(h5i_db_core::Error::Arrow)
}
