//! Fork microbenchmarks, shaped after BranchBench (ROADMAP Part X, X-D1).
//!
//! BranchBench (arXiv:2604.17180) measures branchable databases on primitives
//! rather than on whole queries, because the thing agentic workloads are
//! bottlenecked by is branch *lifecycle*, not data volume. Its finding was a
//! fork in the road that nobody had taken both sides of: systems with instant
//! branching read slowly as branches accumulate (Dolt, 5-4000x), and systems
//! that read fast branch slowly and cap out at ~20 live branches (Neon,
//! 25-1500x on create). This harness measures h5i-db on the same primitives so
//! the claim that it sits on neither horn is a number rather than an argument.
//!
//! What is measured, and why each one is here:
//!
//! - **create latency at the n-th fork** — the star topology (simulation
//!   workload). Must be flat: a fork is one metadata object.
//! - **create latency at depth n** — the chain topology (MCTS workload). Also
//!   must be flat, and is the one no benchmarked system could do at all.
//! - **resolve latency vs live fork count and vs depth** — the read side. This
//!   is where Dolt degrades and where the design's "manifests name segments by
//!   path" property is supposed to pay.
//! - **storage amplification** — bytes on disk after N forks over one base.
//! - **reclamation** — bytes released by dropping N forks and vacuuming, and
//!   how long it takes.
//! - **branching overhead ratio** — BranchBench's headline: the fraction of a
//!   branch-mutate-evaluate-prune loop spent on branch management rather than
//!   on work. A high ratio means the branching infrastructure, not the
//!   workload, is the bottleneck.
//! - **cross-fork aggregation** — `forks()` against the naive per-fork scan it
//!   replaces, which is the capability BranchBench found missing everywhere.
//!
//! Defaults are sized to run in CI on a small machine; pass `--forks` /
//! `--depth` / `--rows` to reproduce at the scale the paper uses.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use clap::Parser;
use h5i_db_core::{Database, ReadAt, ScanOptions, TableOptions, WriteOptions};
use h5i_db_query::{H5iSession, SessionOptions};
use serde::Serialize;

#[derive(Parser, Debug)]
#[command(name = "h5i-db-fork-bench")]
struct Args {
    /// Live forks in the star topology.
    #[arg(long, default_value_t = 64)]
    forks: usize,
    /// Depth of the chain topology.
    #[arg(long, default_value_t = 24)]
    depth: usize,
    /// Rows in the base table.
    #[arg(long, default_value_t = 50_000)]
    rows: usize,
    /// Segments in the base table (more segments = more manifest to copy).
    #[arg(long, default_value_t = 8)]
    segments: usize,
    /// Steps in the branch-mutate-evaluate-prune loop.
    #[arg(long, default_value_t = 32)]
    steps: usize,
    /// Where to build the scratch database (a temp dir by default).
    #[arg(long)]
    dir: Option<PathBuf>,
}

#[derive(Debug, Serialize)]
struct Percentiles {
    n: usize,
    p50_us: u64,
    p95_us: u64,
    max_us: u64,
}

impl Percentiles {
    fn of(mut samples: Vec<Duration>) -> Self {
        samples.sort();
        let at = |q: f64| -> u64 {
            if samples.is_empty() {
                return 0;
            }
            let idx = ((samples.len() as f64 - 1.0) * q).round() as usize;
            samples[idx].as_micros() as u64
        };
        Self {
            n: samples.len(),
            p50_us: at(0.50),
            p95_us: at(0.95),
            max_us: at(1.0),
        }
    }
}

/// One measurement of "does this get slower as the tree grows?".
///
/// Reported as first vs last decile rather than as a single number: the whole
/// question is the *slope*, and an average hides it.
#[derive(Debug, Serialize)]
struct Scaling {
    first_decile_us: u64,
    last_decile_us: u64,
    /// last / first. ~1.0 is flat; BranchBench measured 25-1500x here.
    ratio: f64,
}

impl Scaling {
    fn of(samples: &[Duration]) -> Self {
        let decile = (samples.len() / 10).max(1);
        let mean = |s: &[Duration]| -> f64 {
            s.iter().map(|d| d.as_secs_f64()).sum::<f64>() / s.len() as f64
        };
        let first = mean(&samples[..decile]);
        let last = mean(&samples[samples.len() - decile..]);
        Self {
            first_decile_us: (first * 1e6) as u64,
            last_decile_us: (last * 1e6) as u64,
            ratio: if first > 0.0 { last / first } else { 0.0 },
        }
    }
}

#[derive(Debug, Serialize)]
struct Report {
    config: ConfigReport,
    star_create: Percentiles,
    star_create_scaling: Scaling,
    chain_create: Percentiles,
    chain_create_scaling: Scaling,
    resolve_vs_fork_count: Scaling,
    resolve_vs_depth: Scaling,
    storage: StorageReport,
    reclamation: ReclamationReport,
    loop_workload: LoopReport,
    cross_fork: CrossForkReport,
}

#[derive(Debug, Serialize)]
struct ConfigReport {
    forks: usize,
    depth: usize,
    rows: usize,
    segments: usize,
    steps: usize,
}

#[derive(Debug, Serialize)]
struct StorageReport {
    base_bytes: u64,
    bytes_with_forks: u64,
    /// Total bytes / base bytes. A copying implementation would be ~N.
    amplification: f64,
    per_fork_overhead_bytes: u64,
}

#[derive(Debug, Serialize)]
struct ReclamationReport {
    drop_ms: u64,
    vacuum_ms: u64,
    bytes_after: u64,
    /// Whether the database returned to its pre-fork size.
    returned_to_base: bool,
}

#[derive(Debug, Serialize)]
struct LoopReport {
    steps: usize,
    total_ms: u64,
    branch_lifecycle_ms: u64,
    mutate_ms: u64,
    evaluate_ms: u64,
    /// BranchBench's headline metric: fraction of wall clock spent on branch
    /// management rather than on the workload.
    branching_overhead_ratio: f64,
}

/// `forks()` against the per-fork loop it replaces.
///
/// Read the ratio with care, and only at scale. The two sides do not run the
/// same machinery: the cross-fork side plans and executes SQL through
/// DataFusion, the loop side issues raw scans, so on a small base the constant
/// planning cost dominates and the ratio can sit below 1. What the design
/// actually claims is that a shared segment is *opened once* however many forks
/// reference it, which is asserted exactly (on counted segment reads, not on
/// time) in `h5i-db-query/tests/fork_scan.rs`. This number is the wall-clock
/// consequence of that, and it needs a base large enough to be I/O-bound
/// before it says anything.
#[derive(Debug, Serialize)]
struct CrossForkReport {
    forks_scanned: usize,
    cross_fork_ms: u64,
    per_fork_loop_ms: u64,
    wall_clock_ratio: f64,
    rows: usize,
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
    ]))
}

fn batch(start: i64, n: usize) -> RecordBatch {
    let ts: Vec<i64> = (0..n as i64).map(|i| start + i).collect();
    let symbols: Vec<&str> = (0..n)
        .map(|i| if i % 2 == 0 { "AAA" } else { "BBB" })
        .collect();
    let prices: Vec<f64> = (0..n).map(|i| 100.0 + (i % 97) as f64 * 0.01).collect();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ts)),
            Arc::new(StringArray::from(symbols)),
            Arc::new(Float64Array::from(prices)),
        ],
    )
    .unwrap()
}

fn options() -> TableOptions {
    TableOptions {
        time_column: Some("ts".into()),
        ..Default::default()
    }
}

fn dir_bytes(root: &Path) -> u64 {
    let mut total = 0;
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else if let Ok(m) = std::fs::metadata(&p) {
                total += m.len();
            }
        }
    }
    total
}

/// A base database with `rows` rows spread over `segments` commits.
async fn seed(root: &Path, rows: usize, segments: usize) -> Database {
    let db = Database::create(root).await.unwrap();
    db.create_table("trades", schema(), options())
        .await
        .unwrap();
    let per = (rows / segments).max(1);
    for s in 0..segments {
        let start = (s * per) as i64;
        let b = batch(start, per);
        if s == 0 {
            db.write("trades", vec![b], WriteOptions::default())
                .await
                .unwrap();
        } else {
            db.append("trades", vec![b], WriteOptions::default())
                .await
                .unwrap();
        }
    }
    db
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let tmp = tempfile::tempdir().unwrap();
    let root = args
        .dir
        .clone()
        .unwrap_or_else(|| tmp.path().join("fork-bench.db"));
    let db = seed(&root, args.rows, args.segments).await;
    let base_bytes = dir_bytes(&root);
    eprintln!(
        "seeded {} rows in {} segments ({:.1} MiB)",
        args.rows,
        args.segments,
        base_bytes as f64 / (1024.0 * 1024.0)
    );

    // -- star: create the n-th fork -------------------------------------
    let mut star_samples = Vec::with_capacity(args.forks);
    for i in 0..args.forks {
        let name = format!("star-{i:05}");
        let t = Instant::now();
        db.create_fork(&name, None, None, serde_json::Map::new())
            .await
            .unwrap();
        star_samples.push(t.elapsed());
    }
    let bytes_with_forks = dir_bytes(&root);

    // -- resolve latency against a database holding N live forks --------
    let mut resolve_samples = Vec::with_capacity(args.forks);
    for i in 0..args.forks {
        let handle = db.open_fork(&format!("star-{i:05}")).await.unwrap();
        let t = Instant::now();
        handle.resolve("trades", ReadAt::Latest).await.unwrap();
        resolve_samples.push(t.elapsed());
    }

    // -- storage + reclamation ------------------------------------------
    let names: Vec<String> = (0..args.forks).map(|i| format!("star-{i:05}")).collect();
    let t = Instant::now();
    db.drop_forks(&names).await.unwrap();
    let drop_ms = t.elapsed().as_millis() as u64;
    let t = Instant::now();
    db.vacuum(None, 0, true).await.unwrap();
    let vacuum_ms = t.elapsed().as_millis() as u64;
    let bytes_after = dir_bytes(&root);

    // -- chain: create the n-th nested fork ------------------------------
    let mut chain_samples = Vec::with_capacity(args.depth);
    let mut parent = String::new();
    for level in 0..args.depth {
        let name = format!("chain-{level:05}");
        let handle = if level == 0 {
            db.base()
        } else {
            db.open_fork(&parent).await.unwrap()
        };
        let t = Instant::now();
        handle
            .create_fork(&name, None, None, serde_json::Map::new())
            .await
            .unwrap();
        chain_samples.push(t.elapsed());
        // Each level writes, so deeper forks really do sit on more history.
        // Levels accumulate, so each one's rows must start past the last's.
        db.open_fork(&name)
            .await
            .unwrap()
            .append(
                "trades",
                vec![batch(1_000_000 + level as i64 * 1_000, 100)],
                WriteOptions::default(),
            )
            .await
            .unwrap();
        parent = name;
    }

    // -- resolve latency against depth ----------------------------------
    let mut depth_samples = Vec::with_capacity(args.depth);
    for level in 0..args.depth {
        let handle = db.open_fork(&format!("chain-{level:05}")).await.unwrap();
        let t = Instant::now();
        handle.resolve("trades", ReadAt::Latest).await.unwrap();
        depth_samples.push(t.elapsed());
    }
    db.drop_fork_tree("chain-00000").await.unwrap();

    // -- branch / mutate / evaluate / prune ------------------------------
    //
    // The loop BranchBench measures. Timed in three buckets so the overhead
    // ratio is a measurement rather than a guess.
    let mut branch_lifecycle = Duration::ZERO;
    let mut mutate = Duration::ZERO;
    let mut evaluate = Duration::ZERO;
    let loop_start = Instant::now();
    for step in 0..args.steps {
        let name = format!("loop-{step:05}");

        let t = Instant::now();
        db.create_fork(&name, None, None, serde_json::Map::new())
            .await
            .unwrap();
        let handle = db.open_fork(&name).await.unwrap();
        branch_lifecycle += t.elapsed();

        let t = Instant::now();
        handle
            .append(
                "trades",
                vec![batch(2_000_000 + step as i64 * 1_000, 500)],
                WriteOptions::default(),
            )
            .await
            .unwrap();
        mutate += t.elapsed();

        let t = Instant::now();
        handle
            .scan("trades", ReadAt::Latest, ScanOptions::default())
            .await
            .unwrap();
        evaluate += t.elapsed();

        let t = Instant::now();
        db.drop_fork(&name).await.unwrap();
        branch_lifecycle += t.elapsed();
    }
    let total = loop_start.elapsed();

    // -- cross-fork aggregation vs the loop it replaces ------------------
    let cross_forks = args.forks.min(16);
    db.fork_many("cmp", cross_forks, None, None, serde_json::Map::new())
        .await
        .unwrap();
    // Half of them write, so the scan mixes shared and private segments.
    for i in (0..cross_forks).step_by(2) {
        db.open_fork(&format!("cmp-{i:04}"))
            .await
            .unwrap()
            .append(
                "trades",
                vec![batch(3_000_000 + i as i64 * 1_000, 200)],
                WriteOptions::default(),
            )
            .await
            .unwrap();
    }
    let session = H5iSession::new(Arc::new(db.base()), SessionOptions::default())
        .await
        .unwrap();
    let t = Instant::now();
    let rows: usize = session
        .sql("SELECT __fork, count(*) AS n FROM forks('trades') GROUP BY __fork")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum();
    let cross_fork_ms = t.elapsed().as_millis() as u64;

    let t = Instant::now();
    for i in 0..cross_forks {
        db.open_fork(&format!("cmp-{i:04}"))
            .await
            .unwrap()
            .scan("trades", ReadAt::Latest, ScanOptions::default())
            .await
            .unwrap();
    }
    let per_fork_loop_ms = t.elapsed().as_millis() as u64;

    let report = Report {
        config: ConfigReport {
            forks: args.forks,
            depth: args.depth,
            rows: args.rows,
            segments: args.segments,
            steps: args.steps,
        },
        star_create_scaling: Scaling::of(&star_samples),
        star_create: Percentiles::of(star_samples),
        chain_create_scaling: Scaling::of(&chain_samples),
        chain_create: Percentiles::of(chain_samples),
        resolve_vs_fork_count: Scaling::of(&resolve_samples),
        resolve_vs_depth: Scaling::of(&depth_samples),
        storage: StorageReport {
            base_bytes,
            bytes_with_forks,
            amplification: bytes_with_forks as f64 / base_bytes.max(1) as f64,
            per_fork_overhead_bytes: bytes_with_forks.saturating_sub(base_bytes)
                / args.forks.max(1) as u64,
        },
        reclamation: ReclamationReport {
            drop_ms,
            vacuum_ms,
            bytes_after,
            // Within one segment's worth of slack: vacuum runs with a zero
            // grace period here, but the base itself has not changed.
            returned_to_base: bytes_after <= base_bytes + (base_bytes / 20).max(4096),
        },
        loop_workload: LoopReport {
            steps: args.steps,
            total_ms: total.as_millis() as u64,
            branch_lifecycle_ms: branch_lifecycle.as_millis() as u64,
            mutate_ms: mutate.as_millis() as u64,
            evaluate_ms: evaluate.as_millis() as u64,
            branching_overhead_ratio: branch_lifecycle.as_secs_f64()
                / total.as_secs_f64().max(1e-9),
        },
        cross_fork: CrossForkReport {
            forks_scanned: cross_forks,
            cross_fork_ms,
            per_fork_loop_ms,
            wall_clock_ratio: per_fork_loop_ms as f64 / cross_fork_ms.max(1) as f64,
            rows,
        },
    };

    eprintln!("\n--- fork lifecycle ---");
    eprintln!(
        "star create   p50 {:>6} us  p95 {:>6} us   scaling {:.2}x over {} forks",
        report.star_create.p50_us,
        report.star_create.p95_us,
        report.star_create_scaling.ratio,
        args.forks
    );
    eprintln!(
        "chain create  p50 {:>6} us  p95 {:>6} us   scaling {:.2}x over {} levels",
        report.chain_create.p50_us,
        report.chain_create.p95_us,
        report.chain_create_scaling.ratio,
        args.depth
    );
    eprintln!(
        "resolve       {:.2}x over {} live forks, {:.2}x over {} levels of depth",
        report.resolve_vs_fork_count.ratio, args.forks, report.resolve_vs_depth.ratio, args.depth
    );
    eprintln!("\n--- storage ---");
    eprintln!(
        "amplification {:.3}x at {} forks ({} bytes each)",
        report.storage.amplification, args.forks, report.storage.per_fork_overhead_bytes
    );
    eprintln!(
        "reclaim       drop {} ms + vacuum {} ms, back to base: {}",
        report.reclamation.drop_ms,
        report.reclamation.vacuum_ms,
        report.reclamation.returned_to_base
    );
    eprintln!("\n--- workload ---");
    eprintln!(
        "branch/mutate/evaluate/prune x{}: {} ms total, branching overhead ratio {:.3}",
        args.steps, report.loop_workload.total_ms, report.loop_workload.branching_overhead_ratio
    );
    eprintln!(
        "cross-fork aggregation over {} forks: {} ms (SQL) vs {} ms scanning each raw \
         ({:.2}x wall clock -- planning-bound below a few hundred MiB; the \
         segments-read-once claim is asserted in fork_scan.rs, not here)",
        report.cross_fork.forks_scanned,
        report.cross_fork.cross_fork_ms,
        report.cross_fork.per_fork_loop_ms,
        report.cross_fork.wall_clock_ratio
    );

    println!("{}", serde_json::to_string_pretty(&report).unwrap());
}
