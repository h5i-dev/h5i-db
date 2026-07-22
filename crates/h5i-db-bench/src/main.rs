//! h5i-db benchmark harness.
//!
//! Generates a realistic tick dataset (random-walk trades + quotes across N
//! symbols), then measures the workloads from DESIGN_CLAUDE.md §9 Phase 2/4:
//!
//!  1. append ingest throughput
//!  2. full-table aggregation
//!  3. narrow time-range scans (pruning effectiveness: 0.01% / 1% / 100%)
//!  4. time_bucket OHLCV + VWAP rollup
//!  5. ASOF join trades × quotes
//!  6. cold version / as-of resolution after many commits
//!  7. baseline: raw DataFusion over the *identical* Parquet segment files
//!     (isolates h5i-db's metadata + planning overhead)
//!
//! Output: human summary on stderr, JSON report on stdout.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use clap::Parser;
use h5i_db_core::{Database, ReadAt, StorageOptions, TableOptions, WriteOptions};
use h5i_db_query::{
    AggregateStateMode, AggregateStateStore, FinanceAggregateSpec, H5iSession, SessionOptions,
};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::Serialize;

#[derive(Parser, Debug)]
#[command(name = "h5i-db-bench")]
struct Args {
    /// Total trade rows to generate.
    #[arg(long, default_value_t = 20_000_000)]
    trades: u64,
    /// Total quote rows.
    #[arg(long, default_value_t = 5_000_000)]
    quotes: u64,
    /// Number of symbols.
    #[arg(long, default_value_t = 64)]
    symbols: usize,
    /// Number of ingest batches (each becomes one commit → many versions).
    #[arg(long, default_value_t = 50)]
    commits: u64,
    /// Working directory (a temp dir when omitted).
    #[arg(long)]
    dir: Option<PathBuf>,
    /// RNG seed for reproducibility.
    #[arg(long, default_value_t = 42)]
    seed: u64,
}

#[derive(Debug, Serialize)]
struct BenchResult {
    name: String,
    wall_ms: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    rows_per_sec: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rows: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    detail: Option<serde_json::Value>,
}

fn trades_schema() -> SchemaRef {
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

fn quotes_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("bid", DataType::Float64, false),
        Field::new("ask", DataType::Float64, false),
    ]))
}

/// Generate time-ordered tick chunks: timestamps advance by random nanosecond
/// gaps; each row belongs to a random symbol whose price random-walks.
struct TickGen {
    rng: StdRng,
    symbols: Vec<String>,
    prices: Vec<f64>,
    t_ns: i64,
}

impl TickGen {
    fn new(seed: u64, n_symbols: usize, start_ns: i64) -> Self {
        let mut rng = StdRng::seed_from_u64(seed);
        let symbols: Vec<String> = (0..n_symbols).map(|i| format!("SYM{i:04}")).collect();
        let prices: Vec<f64> = (0..n_symbols)
            .map(|_| 20.0 + rng.random::<f64>() * 480.0)
            .collect();
        Self {
            rng,
            symbols,
            prices,
            t_ns: start_ns,
        }
    }

    fn trades_batch(&mut self, rows: usize) -> RecordBatch {
        let mut ts = Vec::with_capacity(rows);
        let mut syms: Vec<&str> = Vec::with_capacity(rows);
        let mut prices = Vec::with_capacity(rows);
        let mut sizes = Vec::with_capacity(rows);
        // Borrow symbols immutably via index list first to satisfy borrowck.
        let mut sym_idx = Vec::with_capacity(rows);
        for _ in 0..rows {
            self.t_ns += self.rng.random_range(1_000..2_000_000); // 1µs..2ms gaps
            let s = self.rng.random_range(0..self.symbols.len());
            let drift = (self.rng.random::<f64>() - 0.5) * 0.1;
            self.prices[s] = (self.prices[s] + drift).max(0.01);
            ts.push(self.t_ns);
            sym_idx.push(s);
            prices.push((self.prices[s] * 100.0).round() / 100.0);
            sizes.push(self.rng.random_range(1..1_000i64));
        }
        for &s in &sym_idx {
            syms.push(self.symbols[s].as_str());
        }
        RecordBatch::try_new(
            trades_schema(),
            vec![
                Arc::new(TimestampNanosecondArray::from(ts).with_timezone("UTC".to_string())),
                Arc::new(StringArray::from(syms)),
                Arc::new(Float64Array::from(prices)),
                Arc::new(Int64Array::from(sizes)),
            ],
        )
        .unwrap()
    }

    fn quotes_batch(&mut self, rows: usize) -> RecordBatch {
        let mut ts = Vec::with_capacity(rows);
        let mut sym_idx = Vec::with_capacity(rows);
        let mut bids = Vec::with_capacity(rows);
        let mut asks = Vec::with_capacity(rows);
        for _ in 0..rows {
            self.t_ns += self.rng.random_range(1_000..8_000_000);
            let s = self.rng.random_range(0..self.symbols.len());
            let mid = self.prices[s];
            let spread = mid * 0.0005;
            ts.push(self.t_ns);
            sym_idx.push(s);
            bids.push(mid - spread);
            asks.push(mid + spread);
        }
        let syms: Vec<&str> = sym_idx.iter().map(|&s| self.symbols[s].as_str()).collect();
        RecordBatch::try_new(
            quotes_schema(),
            vec![
                Arc::new(TimestampNanosecondArray::from(ts).with_timezone("UTC".to_string())),
                Arc::new(StringArray::from(syms)),
                Arc::new(Float64Array::from(bids)),
                Arc::new(Float64Array::from(asks)),
            ],
        )
        .unwrap()
    }
}

async fn timed<F, T>(name: &str, rows: Option<u64>, f: F) -> (BenchResult, T)
where
    F: std::future::Future<Output = T>,
{
    let start = Instant::now();
    let out = f.await;
    let wall_ms = start.elapsed().as_secs_f64() * 1000.0;
    let result = BenchResult {
        name: name.to_string(),
        wall_ms,
        rows_per_sec: rows.map(|r| r as f64 / (wall_ms / 1000.0)),
        rows,
        detail: None,
    };
    eprintln!(
        "  {:<40} {:>10.1} ms{}",
        name,
        wall_ms,
        result
            .rows_per_sec
            .map(|r| format!("  ({:.2} M rows/s)", r / 1e6))
            .unwrap_or_default()
    );
    (result, out)
}

async fn sql_rows(session: &H5iSession, sql: &str) -> u64 {
    let batches = session
        .sql(sql)
        .await
        .expect("sql")
        .collect()
        .await
        .expect("collect");
    batches.iter().map(|b| b.num_rows() as u64).sum()
}

/// One un-timed warmup execution, so measured numbers reflect warm caches on
/// both the h5i-db and baseline paths equally.
async fn warmup(session: &H5iSession, sql: &str) {
    let _ = sql_rows(session, sql).await;
    session.take_scan_metrics();
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let tmp;
    let dir = match &args.dir {
        Some(d) => {
            std::fs::create_dir_all(d).unwrap();
            d.clone()
        }
        None => {
            tmp = tempfile::tempdir().expect("tempdir");
            tmp.path().to_path_buf()
        }
    };
    let db_path = dir.join("bench.db");
    if db_path.exists() {
        std::fs::remove_dir_all(&db_path).unwrap();
    }
    eprintln!(
        "h5i-db bench: {} trades, {} quotes, {} symbols, {} commits, dir={}",
        args.trades,
        args.quotes,
        args.symbols,
        args.commits,
        dir.display()
    );

    let mut results: Vec<BenchResult> = Vec::new();
    let db = Arc::new(Database::create(&db_path).await.unwrap());
    let opts = TableOptions {
        time_column: Some("ts".into()),
        sort_key: vec![],
        storage: StorageOptions::default(),
        max_segments_per_manifest: None,
    };
    db.create_table("trades", trades_schema(), opts.clone())
        .await
        .unwrap();
    db.create_table("quotes", quotes_schema(), opts)
        .await
        .unwrap();

    // ------------------------------------------------------------------
    // 1. ingest
    // ------------------------------------------------------------------
    let start_ns = 1_750_000_000_000_000_000i64; // ~2025-06 in ns
    let mut gen = TickGen::new(args.seed, args.symbols, start_ns);
    let per_commit = (args.trades / args.commits).max(1) as usize;

    let (r, _) = timed("ingest trades (append commits)", Some(args.trades), async {
        for _ in 0..args.commits {
            let batch = gen.trades_batch(per_commit);
            db.append("trades", vec![batch], WriteOptions::default())
                .await
                .unwrap();
        }
    })
    .await;
    results.push(r);

    let mut qgen = TickGen::new(args.seed + 1, args.symbols, start_ns);
    let quote_commits = 10u64;
    let q_per_commit = (args.quotes / quote_commits).max(1) as usize;
    let (r, _) = timed("ingest quotes", Some(args.quotes), async {
        for _ in 0..quote_commits {
            let batch = qgen.quotes_batch(q_per_commit);
            db.append("quotes", vec![batch], WriteOptions::default())
                .await
                .unwrap();
        }
    })
    .await;
    results.push(r);

    let trades_meta = db.resolve("trades", ReadAt::Latest).await.unwrap();
    let (t_min, t_max) = trades_meta.manifest.time_range.unwrap();
    let span = t_max - t_min;
    eprintln!(
        "  trades: {} segments, {} MiB on disk",
        trades_meta.manifest.segments.len(),
        trades_meta.manifest.bytes / (1024 * 1024)
    );

    // ------------------------------------------------------------------
    // 2-5. queries
    // ------------------------------------------------------------------
    let session = H5iSession::new(db.clone(), SessionOptions::default())
        .await
        .unwrap();

    warmup(
        &session,
        "SELECT symbol, count(*), avg(price), sum(size) FROM trades GROUP BY symbol",
    )
    .await;
    // Split planning from execution to localize overhead.
    {
        let t0 = Instant::now();
        let df = session
            .sql("SELECT symbol, count(*), avg(price), sum(size) FROM trades GROUP BY symbol")
            .await
            .unwrap();
        let plan_ms = t0.elapsed().as_secs_f64() * 1000.0;
        let t1 = Instant::now();
        let phys = df.create_physical_plan().await.unwrap();
        let phys_ms = t1.elapsed().as_secs_f64() * 1000.0;
        let _ = phys;
        eprintln!("      [full-agg] logical: {plan_ms:.1} ms, physical: {phys_ms:.1} ms");
    }
    if std::env::var_os("H5I_BENCH_ANALYZE").is_some() {
        // Control: identical files via ListingTable on OUR session.
        let seg_dir = db_path
            .join("tables")
            .join(trades_meta.entry.table_id.to_string())
            .join("segments");
        session
            .context()
            .register_parquet(
                "trades_listing",
                seg_dir.to_str().unwrap(),
                h5i_db_query::datafusion::prelude::ParquetReadOptions::default(),
            )
            .await
            .unwrap();
        for _ in 0..2 {
            let t = Instant::now();
            let _ = sql_rows(
                &session,
                "SELECT symbol, count(*), avg(price), sum(size) FROM trades_listing GROUP BY symbol",
            )
            .await;
            eprintln!(
                "      [control listing-table on our session] {:.1} ms",
                t.elapsed().as_secs_f64() * 1000.0
            );
        }
        // Control 2: ListingTable through the h5i:// object store.
        {
            use h5i_db_query::datafusion::datasource::file_format::parquet::ParquetFormat;
            use h5i_db_query::datafusion::datasource::listing::{
                ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
            };
            let url = format!(
                "{}tables/{}/segments/",
                session.object_store_url().as_str(),
                trades_meta.entry.table_id
            );
            let lurl = ListingTableUrl::parse(&url).unwrap();
            let opts = ListingOptions::new(Arc::new(ParquetFormat::default()))
                .with_file_extension(".parquet");
            let config = ListingTableConfig::new(lurl)
                .with_listing_options(opts)
                .infer_schema(&session.context().state())
                .await
                .unwrap();
            session
                .context()
                .register_table(
                    "trades_h5istore",
                    Arc::new(ListingTable::try_new(config).unwrap()),
                )
                .unwrap();
            for _ in 0..2 {
                let t = Instant::now();
                let _ = sql_rows(
                    &session,
                    "SELECT symbol, count(*), avg(price), sum(size) FROM trades_h5istore GROUP BY symbol",
                )
                .await;
                eprintln!(
                    "      [control listing over h5i:// store] {:.1} ms",
                    t.elapsed().as_secs_f64() * 1000.0
                );
            }
        }
        for t in ["trades", "trades_listing"] {
            let b = session
                .sql(&format!("EXPLAIN ANALYZE SELECT symbol, count(*), avg(price), sum(size) FROM {t} GROUP BY symbol"))
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            let txt = arrow::util::pretty::pretty_format_batches(&b)
                .unwrap()
                .to_string();
            // print only structure lines, truncating file lists
            for line in txt.lines() {
                if line.contains("Exec") {
                    // Print operator name + full metrics blob.
                    let name = line
                        .trim_start_matches(['|', ' '])
                        .chars()
                        .take(30)
                        .collect::<String>();
                    let metrics = line
                        .find("metrics=")
                        .map(|i| &line[i..line.len().min(i + 700)])
                        .unwrap_or("");
                    eprintln!("      [{t}] {name} {metrics}");
                }
            }
        }
        let b = session
            .sql("EXPLAIN ANALYZE SELECT symbol, count(*), avg(price), sum(size) FROM trades GROUP BY symbol")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        eprintln!(
            "{}",
            arrow::util::pretty::pretty_format_batches(&b).unwrap()
        );
    }
    let (mut r, n) = timed("full aggregation (group by symbol)", None, async {
        sql_rows(
            &session,
            "SELECT symbol, count(*), avg(price), sum(size) FROM trades GROUP BY symbol",
        )
        .await
    })
    .await;
    r.rows = Some(n);
    results.push(r);
    session.take_scan_metrics();

    // P3 explicit state-store path. It intentionally reuses this existing
    // benchmark binary rather than creating another DataFusion-linked target.
    let aggregate_store = AggregateStateStore::new(db.clone(), AggregateStateMode::ReadWrite);
    let aggregate_spec = FinanceAggregateSpec::ohlcv("ts", "price", "size").grouped_by("symbol");
    let (mut r, cold_states) = timed("aggregate states: cold OHLCV + VWAP", None, async {
        aggregate_store
            .finance_rollup("trades", ReadAt::Latest, &aggregate_spec)
            .await
            .unwrap()
    })
    .await;
    r.rows = Some(cold_states.groups.len() as u64);
    r.detail = Some(serde_json::to_value(&cold_states.metrics).unwrap());
    results.push(r);

    let (mut r, warm_states) = timed("aggregate states: warm OHLCV + VWAP", None, async {
        aggregate_store
            .finance_rollup("trades", ReadAt::Latest, &aggregate_spec)
            .await
            .unwrap()
    })
    .await;
    assert_eq!(warm_states.groups, cold_states.groups);
    r.rows = Some(warm_states.groups.len() as u64);
    r.detail = Some(serde_json::to_value(&warm_states.metrics).unwrap());
    results.push(r);

    for (label, frac) in [("0.01%", 0.0001f64), ("1%", 0.01), ("100%", 1.0)] {
        let lo = t_min + (span as f64 * 0.4) as i64;
        let hi = lo + (span as f64 * frac) as i64;
        let sql = format!(
            "SELECT count(*), avg(price) FROM trades \
             WHERE ts >= to_timestamp_nanos({lo}) AND ts < to_timestamp_nanos({hi})"
        );
        warmup(&session, &sql).await;
        let (mut r, _) = timed(&format!("time-range scan {label}"), None, async {
            sql_rows(&session, &sql).await
        })
        .await;
        let metrics = session.take_scan_metrics();
        if let Some(m) = metrics.iter().find(|m| m.table == "trades") {
            r.detail = Some(serde_json::json!({
                "segments_total": m.segments_total,
                "segments_pruned": m.segments_pruned,
                "bytes_scheduled": m.bytes_scheduled,
            }));
            eprintln!(
                "      pruning: {}/{} segments pruned",
                m.segments_pruned, m.segments_total
            );
        }
        results.push(r);
    }

    let (mut r, n) = timed("1-minute OHLCV + VWAP rollup", None, async {
        sql_rows(
            &session,
            "SELECT symbol, time_bucket('1m', ts) AS bucket, \
                    first_value(price ORDER BY ts) AS open, max(price) AS high, \
                    min(price) AS low, last_value(price ORDER BY ts) AS close, \
                    sum(size) AS volume, vwap(price, size) AS vw \
             FROM trades GROUP BY symbol, bucket",
        )
        .await
    })
    .await;
    r.rows = Some(n);
    results.push(r);

    let (r, _) = timed("ASOF join trades x quotes (by symbol)", None, async {
        sql_rows(
            &session,
            "SELECT count(*) FROM ( \
                 SELECT symbol, price, bid, ask FROM \
                 asof_join('trades', 'quotes', 'ts', 'ts', 'symbol') WHERE bid IS NOT NULL)",
        )
        .await
    })
    .await;
    results.push(r);

    let (r, _) = timed("quant pipeline (asof->ohlcv->returns)", None, async {
        sql_rows(
            &session,
            "WITH enriched AS ( \
                 SELECT * FROM asof_join('trades', 'quotes', 'ts', 'ts', 'symbol') \
             ), bars AS ( \
                 SELECT symbol, time_bucket('1m', ts) AS bucket, \
                        first_value(price ORDER BY ts) AS open, \
                        last_value(price ORDER BY ts) AS close, \
                        vwap(price, size) AS vw \
                 FROM enriched GROUP BY symbol, bucket \
             ) \
             SELECT symbol, bucket, ln(close/open) AS ret, vw FROM bars",
        )
        .await
    })
    .await;
    results.push(r);

    // ------------------------------------------------------------------
    // 6. version resolution
    // ------------------------------------------------------------------
    let (r, _) = timed("cold read of version 3 (metadata only)", None, async {
        db.resolve("trades", ReadAt::Version(3)).await.unwrap()
    })
    .await;
    results.push(r);
    let versions = db.list_versions("trades").await.unwrap();
    let mid_ts = versions[versions.len() / 2].committed_at_ns;
    let (r, _) = timed("as_of resolution (binary search)", None, async {
        db.resolve("trades", ReadAt::AsOf(mid_ts)).await.unwrap()
    })
    .await;
    results.push(r);

    // ------------------------------------------------------------------
    // 7. baseline: raw DataFusion over the identical Parquet files
    // ------------------------------------------------------------------
    let seg_dir = db_path
        .join("tables")
        .join(trades_meta.entry.table_id.to_string())
        .join("segments");
    let ctx = h5i_db_query::datafusion::prelude::SessionContext::new();
    ctx.register_parquet(
        "trades_raw",
        seg_dir.to_str().unwrap(),
        h5i_db_query::datafusion::prelude::ParquetReadOptions::default(),
    )
    .await
    .unwrap();
    let _ = ctx
        .sql("SELECT symbol, count(*), avg(price), sum(size) FROM trades_raw GROUP BY symbol")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    if std::env::var_os("H5I_BENCH_ANALYZE").is_some() {
        let b = ctx
            .sql("EXPLAIN ANALYZE SELECT symbol, count(*), avg(price), sum(size) FROM trades_raw GROUP BY symbol")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        eprintln!(
            "BASELINE ANALYZE:\n{}",
            arrow::util::pretty::pretty_format_batches(&b).unwrap()
        );
    }
    let (r, _) = timed("BASELINE raw DF: full aggregation", None, async {
        let b = ctx
            .sql("SELECT symbol, count(*), avg(price), sum(size) FROM trades_raw GROUP BY symbol")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        b.iter().map(|x| x.num_rows() as u64).sum::<u64>()
    })
    .await;
    results.push(r);
    {
        let lo = t_min + (span as f64 * 0.4) as i64;
        let hi = lo + (span as f64 * 0.01) as i64;
        let sql = format!(
            "SELECT count(*), avg(price) FROM trades_raw \
             WHERE ts >= to_timestamp_nanos({lo}) AND ts < to_timestamp_nanos({hi})"
        );
        let (r, _) = timed("BASELINE raw DF: 1% time-range scan", None, async {
            let b = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
            b.len() as u64
        })
        .await;
        results.push(r);
    }

    println!("{}", serde_json::to_string_pretty(&results).unwrap());
}
