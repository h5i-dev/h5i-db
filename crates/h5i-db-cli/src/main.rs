//! h5i-db CLI: non-interactive, machine-readable, resource-limited.
//!
//! Contract (DESIGN_CLAUDE.md §8): output formats via --format, machine
//! errors on stderr as `{code, message, retryable, hint}`, stable exit codes
//! (0 ok / 2 user error / 3 conflict / 4 limit / 5 internal), no prompts, no
//! pager, SQL from an argument or stdin.

mod context;
mod ingest;
mod output;
mod profile;

use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use clap::{Parser, Subcommand};
use futures::StreamExt;
use h5i_db_core::{
    Database, Error, ReadAt, Result, ScanOptions, StorageOptions, TableOptions, WriteOptions,
};
use h5i_db_query::{H5iSession, PredicateCacheMode, ResearchPin, SessionOptions};

use ingest::{InputFormat, align_batch, open_input};
use output::{
    BatchWriter, Format, Progress, is_broken_pipe, write_batches, write_error, write_value,
};
use profile::{AGENT_MAX_BYTES, AGENT_MAX_ROWS, Profile, ResultSpill, ResultSummary};

#[derive(Parser)]
#[command(
    name = "h5i-db",
    version,
    about = "Embedded versioned time-series database",
    disable_help_subcommand = true
)]
struct Cli {
    #[command(subcommand)]
    command: Command,

    /// Output format.
    #[arg(long, global = true, value_enum, default_value = "table")]
    format: Format,
}

#[derive(clap::Args, Debug, Clone)]
struct WriteFlags {
    /// Require the table head to be exactly this version (optimistic guard).
    #[arg(long)]
    expected_version: Option<u64>,
    /// Free-text note recorded in the version manifest.
    #[arg(long)]
    note: Option<String>,
}

impl WriteFlags {
    fn to_options(&self) -> WriteOptions {
        WriteOptions {
            expected_version: self.expected_version,
            note: self.note.clone(),
            user_meta: serde_json::Map::new(),
        }
    }
}

#[derive(Subcommand)]
enum Command {
    /// Create a new database directory.
    Init { db: PathBuf },

    /// Create a table. Schema comes from --schema JSON or --like a data file.
    CreateTable {
        db: PathBuf,
        table: String,
        /// JSON schema: [{"name":"ts","type":"timestamp_ns","nullable":false}, …]
        /// Types: int8..int64, uint8..uint64, float32/float64, utf8, bool,
        /// timestamp_s/ms/us/ns (UTC), date32, date64.
        #[arg(long, conflicts_with = "like")]
        schema: Option<String>,
        /// Infer the schema from a Parquet/CSV/Arrow file.
        #[arg(long)]
        like: Option<String>,
        /// Time index column (strongly recommended for time-series tables).
        #[arg(long)]
        time_column: Option<String>,
        /// Sort key columns (defaults to the time column).
        #[arg(long, value_delimiter = ',')]
        sort_key: Vec<String>,
        /// Target segment size in MiB of in-memory data.
        #[arg(long, default_value_t = 128)]
        target_segment_mb: u64,
    },

    /// Drop a table and its data. Refuses if pinned by a snapshot.
    DropTable {
        db: PathBuf,
        table: String,
        /// Required confirmation.
        #[arg(long)]
        yes: bool,
    },

    /// Rename a table (catalog edit; no data moves).
    Rename {
        db: PathBuf,
        from: String,
        to: String,
    },

    /// Everything needed to orient in one call: every table's schema, size,
    /// time range and head version, the operations policy gates, existing
    /// snapshots, and any staged plans awaiting review.
    ///
    /// Deterministic for a given database state, so it can be cached; only
    /// --stale-after makes it depend on the clock.
    Context {
        db: PathBuf,
        /// Approximate token ceiling. Detail is shed in a fixed order
        /// (columns, snapshots, notes, then whole tables smallest-first) and
        /// whatever was dropped is named under `omitted`.
        #[arg(long)]
        budget: Option<usize>,
        /// Report per-table commit age and flag tables whose head is older
        /// than this (e.g. "5m", "1h"). Omit to keep the output clock-free.
        #[arg(long)]
        stale_after: Option<humantime::Duration>,
    },

    /// List tables with row counts and time ranges.
    Tables { db: PathBuf },

    /// Show a table's schema and options.
    Schema { db: PathBuf, table: String },

    /// Show the first rows of a table.
    Sample {
        db: PathBuf,
        table: String,
        #[arg(short = 'n', long, default_value_t = 10)]
        rows: usize,
        /// Read at a specific version.
        #[arg(long)]
        version: Option<u64>,
    },

    /// Run SQL. Reads the query from the argument, or stdin when "-".
    Query {
        db: PathBuf,
        /// SQL text, or "-" for stdin.
        sql: String,
        /// Abort after this many rows have been produced.
        #[arg(long)]
        max_rows: Option<usize>,
        /// Stop after this many output bytes (checked at batch boundaries);
        /// truncation exits 4 with a limit_exceeded envelope.
        #[arg(long)]
        max_bytes: Option<u64>,
        /// Query timeout, e.g. "30s", "5m".
        #[arg(long)]
        timeout: Option<humantime::Duration>,
        /// Memory budget in MiB (enables disk spilling under pressure).
        #[arg(long)]
        memory_limit_mb: Option<usize>,
        /// Spill directory (with --memory-limit-mb).
        #[arg(long)]
        spill_dir: Option<PathBuf>,
        /// Number of threads / partitions.
        #[arg(long)]
        threads: Option<usize>,
        /// Print scan/pruning statistics to stderr after the query.
        #[arg(long)]
        stats: bool,
        /// Read and build disposable immutable predicate-cache sidecars.
        #[arg(long)]
        predicate_cache: bool,
        /// Research mode, arrival axis: pin **every** table at a decision
        /// point, so the query can only read data that had already been
        /// committed then. Accepts an RFC3339 timestamp (commit availability),
        /// an integer version, or a snapshot name. Set `H5I_DB_AS_OF` to pin a
        /// whole shell session.
        #[arg(long, env = "H5I_DB_AS_OF")]
        as_of: Option<String>,
        /// Research mode, event-time axis: an RFC3339 instant on the *data's*
        /// clock. Rows stamped after it are unreadable in this query.
        ///
        /// This is the half of the jail that works on bulk-loaded history,
        /// where nothing was ever withheld by arrival: it blocks the window
        /// that overruns into the future and the join that reads a later
        /// timestamp. Independent of --as-of on purpose — under a bulk ingest
        /// the two clocks are years apart. `H5I_DB_DECISION_TIME` pins a
        /// session. Defaults to the --as-of instant when that is a timestamp,
        /// which is the aligned case of a continuously-ingested database.
        #[arg(long, env = "H5I_DB_DECISION_TIME")]
        decision_time: Option<String>,
        /// Extra safety gap subtracted from the decision time, e.g. "1d".
        /// Needs a decision time to measure back from.
        #[arg(long, env = "H5I_DB_EMBARGO")]
        embargo: Option<humantime::Duration>,
    },

    /// Ingest data into a table from Parquet/CSV/Arrow (or stdin with "-").
    Ingest {
        db: PathBuf,
        table: String,
        /// Input file path or "-" for stdin.
        input: String,
        #[arg(long, value_enum, default_value = "auto")]
        input_format: InputFormat,
        /// write = replace table contents; append = strict ordered append.
        #[arg(long, value_enum, default_value = "append")]
        mode: IngestMode,
        /// Retry appends on version conflicts (safe for pure appends).
        #[arg(long, default_value_t = 5)]
        retries: usize,
        #[command(flatten)]
        write_flags: WriteFlags,
    },

    /// List a table's committed versions.
    Versions { db: PathBuf, table: String },

    /// Look-ahead-bias check: run a query against the current head and against
    /// an as-of read point, and report the "leakage delta" between them.
    LeakageCheck {
        db: PathBuf,
        /// SQL text, or "-" for stdin.
        sql: String,
        /// Decision point: an RFC3339 timestamp (as-of commit availability), an
        /// integer version, or a snapshot name.
        #[arg(long)]
        as_of: String,
        /// Absolute per-cell tolerance below which a delta is treated as noise.
        #[arg(long)]
        tolerance: Option<f64>,
    },

    /// Snapshot management.
    #[command(subcommand)]
    Snapshot(SnapshotCmd),

    /// Make a historical version current (history moves forward).
    Restore {
        db: PathBuf,
        table: String,
        version: u64,
        #[command(flatten)]
        write_flags: WriteFlags,
    },

    /// Replace all rows in a time range with the given input data.
    ReplaceRange {
        db: PathBuf,
        table: String,
        /// Range start (RFC3339 or raw integer in the column's unit), inclusive.
        #[arg(long)]
        start: String,
        /// Range end, exclusive.
        #[arg(long)]
        end: String,
        /// Input file (or "-"); omit to delete the range.
        #[arg(long)]
        input: Option<String>,
        #[arg(long, value_enum, default_value = "auto")]
        input_format: InputFormat,
        /// Prepare a previewable plan instead of committing immediately.
        #[arg(long)]
        plan: bool,
        #[command(flatten)]
        write_flags: WriteFlags,
    },

    /// Delete all rows in a time range.
    DeleteRange {
        db: PathBuf,
        table: String,
        #[arg(long)]
        start: String,
        #[arg(long)]
        end: String,
        /// Prepare a previewable plan instead of committing immediately.
        #[arg(long)]
        plan: bool,
        #[command(flatten)]
        write_flags: WriteFlags,
    },

    /// Previewable-mutation plans: list, show, apply, discard.
    #[command(subcommand)]
    Plan(PlanCmd),

    /// Mutation policy: which operations may commit without a reviewed plan.
    #[command(subcommand)]
    Policy(PolicyCmd),

    /// Data-safety policy: opt-in, per-table row constraints on writes.
    #[command(subcommand)]
    DataPolicy(DataPolicyCmd),

    /// Rewrite small segments into target-sized ones.
    Compact {
        db: PathBuf,
        table: String,
        /// Override the target segment size (MiB of in-memory data).
        #[arg(long)]
        target_mb: Option<u64>,
        #[command(flatten)]
        write_flags: WriteFlags,
    },

    /// Remove unreachable objects (dry-run unless --apply).
    Vacuum {
        db: PathBuf,
        /// Restrict to one table.
        table: Option<String>,
        /// Don't touch objects newer than this many seconds.
        #[arg(long, default_value_t = 3600)]
        grace_seconds: u64,
        /// Actually delete (default is a dry run).
        #[arg(long)]
        apply: bool,
    },

    /// Launch the local review UI (loopback only).
    Ui {
        db: PathBuf,
        #[arg(long, default_value_t = 7351)]
        port: u16,
        /// Enable plan apply/discard from the UI (default: read-only).
        #[arg(long)]
        allow_mutations: bool,
    },

    /// Check structural integrity (checksums, object existence).
    Verify {
        db: PathBuf,
        table: String,
        /// Also re-read every segment and verify content checksums.
        #[arg(long)]
        deep: bool,
    },
}

impl Command {
    /// The database this invocation targets, used to render `<db>` in error
    /// `next_actions` as a runnable path. Exhaustive on purpose: a new
    /// subcommand must decide what its database is rather than silently
    /// emitting placeholders.
    fn db_path(&self) -> Option<&PathBuf> {
        use Command::*;
        match self {
            Init { db }
            | CreateTable { db, .. }
            | DropTable { db, .. }
            | Rename { db, .. }
            | Context { db, .. }
            | Tables { db }
            | Schema { db, .. }
            | Sample { db, .. }
            | Query { db, .. }
            | Ingest { db, .. }
            | Versions { db, .. }
            | LeakageCheck { db, .. }
            | Restore { db, .. }
            | ReplaceRange { db, .. }
            | DeleteRange { db, .. }
            | Compact { db, .. }
            | Vacuum { db, .. }
            | Ui { db, .. }
            | Verify { db, .. } => Some(db),
            Snapshot(cmd) => Some(match cmd {
                SnapshotCmd::Create { db, .. }
                | SnapshotCmd::List { db }
                | SnapshotCmd::Delete { db, .. } => db,
            }),
            Plan(cmd) => Some(match cmd {
                PlanCmd::List { db, .. }
                | PlanCmd::Show { db, .. }
                | PlanCmd::Apply { db, .. }
                | PlanCmd::Discard { db, .. } => db,
            }),
            Policy(cmd) => Some(match cmd {
                PolicyCmd::Show { db } | PolicyCmd::Set { db, .. } => db,
            }),
            DataPolicy(cmd) => Some(match cmd {
                DataPolicyCmd::Get { db, .. }
                | DataPolicyCmd::Set { db, .. }
                | DataPolicyCmd::Clear { db, .. } => db,
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum IngestMode {
    Write,
    Append,
}

#[derive(Subcommand)]
enum SnapshotCmd {
    /// Pin current table versions under a name (all tables when omitted).
    Create {
        db: PathBuf,
        name: String,
        tables: Vec<String>,
        #[arg(long)]
        note: Option<String>,
    },
    List {
        db: PathBuf,
    },
    Delete {
        db: PathBuf,
        name: String,
    },
}

#[derive(Subcommand)]
enum PolicyCmd {
    /// Show the current mutation policy.
    Show { db: PathBuf },
    /// Set policy keys, e.g. `policy set m.db direct_delete=false`.
    Set {
        db: PathBuf,
        /// key=true|false pairs (keys: direct_append, direct_write,
        /// direct_replace, direct_delete, direct_restore, direct_compact).
        #[arg(required = true)]
        entries: Vec<String>,
    },
}

#[derive(Subcommand)]
enum DataPolicyCmd {
    /// Show a table's data-safety policy (empty when unset).
    Get { db: PathBuf, table: String },
    /// Install a table's data-safety policy from a typed JSON document.
    ///
    /// `policy` is inline JSON, `@path` to read a file, or `-` for stdin.
    /// Shape: `{"constraints":[{"name":"positive_price",
    /// "predicate":{"compare":{"column":"price","op":"gt",
    /// "value":{"float":0.0}}},"on_fail":"reject"}]}`
    Set {
        db: PathBuf,
        table: String,
        policy: String,
    },
    /// Remove a table's data-safety policy (writes become unconstrained).
    Clear { db: PathBuf, table: String },
}

#[derive(Subcommand)]
enum PlanCmd {
    /// List pending plans for a table.
    List { db: PathBuf, table: String },
    /// Show a plan: summary plus before/after samples.
    Show {
        db: PathBuf,
        table: String,
        plan_id: uuid::Uuid,
    },
    /// Publish a plan (fails with a conflict if the table head moved).
    Apply {
        db: PathBuf,
        table: String,
        plan_id: uuid::Uuid,
    },
    /// Drop a plan; its staged segments become vacuumable.
    Discard {
        db: PathBuf,
        table: String,
        plan_id: uuid::Uuid,
    },
}

// ---------------------------------------------------------------------------

fn main() {
    // Diagnostics go to stderr (stdout is machine output), volume via RUST_LOG.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_writer(std::io::stderr)
        .try_init();
    let cli = Cli::parse();
    // Captured before `run` consumes the command, so a failure can render its
    // recovery commands against the database the caller actually named.
    let db = cli.command.db_path().map(|p| p.display().to_string());
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let code = match runtime.block_on(run(cli)) {
        Ok(()) => 0,
        // Downstream closed stdout (`… | head`): quiet success, no envelope.
        Err(err) if is_broken_pipe(&err) => 0,
        Err(err) => {
            write_error(&err, db.as_deref());
            err.exit_category() as i32
        }
    };
    std::process::exit(code);
}

/// Classify a DataFusion error for the CLI contract: bad SQL and runtime
/// compute failures (casts, arithmetic) are user errors, resource exhaustion
/// is a limit, and only genuine engine faults stay internal. Errors raised by
/// the h5i table providers are unwrapped back into their core form so exit
/// codes survive the trip through DataFusion.
fn classify_df_error(e: datafusion::error::DataFusionError) -> Error {
    use arrow::error::ArrowError;
    use datafusion::error::DataFusionError as DfE;
    match e {
        DfE::Context(_, inner) => classify_df_error(*inner),
        DfE::Diagnostic(_, inner) => classify_df_error(*inner),
        DfE::Shared(inner) => match Arc::try_unwrap(inner) {
            Ok(inner) => classify_df_error(inner),
            Err(inner) => Error::invalid(inner.to_string()),
        },
        DfE::Collection(errors) => errors
            .into_iter()
            .next()
            .map(classify_df_error)
            .unwrap_or_else(|| Error::internal("empty DataFusion error collection")),
        DfE::External(err) => match err.downcast::<Error>() {
            Ok(core) => *core,
            Err(err) => Error::internal(err),
        },
        DfE::ResourcesExhausted(msg) => Error::LimitExceeded { detail: msg },
        DfE::Internal(msg) => Error::internal(msg),
        DfE::IoError(source) => Error::io("query execution", source),
        DfE::ObjectStore(err) => Error::ObjectStore(*err),
        DfE::ParquetError(err) => Error::Parquet(*err),
        DfE::ArrowError(err, ctx) => match *err {
            ArrowError::CastError(_)
            | ArrowError::ParseError(_)
            | ArrowError::ComputeError(_)
            | ArrowError::DivideByZero
            | ArrowError::ArithmeticOverflow(_)
            | ArrowError::InvalidArgumentError(_)
            | ArrowError::SchemaError(_) => match ctx {
                Some(ctx) => Error::invalid(format!("{err} ({ctx})")),
                None => Error::invalid(err.to_string()),
            },
            ArrowError::MemoryError(msg) => Error::LimitExceeded { detail: msg },
            other => Error::Arrow(other),
        },
        // SQL / Plan / SchemaError / Execution / NotImplemented / … : the
        // query (or the data it touched) is at fault, not the engine.
        other => Error::invalid(other.to_string()),
    }
}

/// What draining a result stream produced.
struct Drained {
    /// stdout does not hold the whole result.
    truncated: bool,
    /// Rows the query produced. Exact only when the whole stream was drained,
    /// which is the case under the agent profile.
    total_rows: u64,
    /// Rows actually rendered to stdout.
    returned_rows: u64,
    /// Present only under the agent profile.
    spill: Option<profile::SpillOutcome>,
}

/// Stream a result to stdout under a row/byte budget, and — under the agent
/// profile — into a recoverable Parquet spill.
///
/// The two profiles read the stream differently on purpose. By default the
/// row limit is pushed into the plan, so DataFusion stops early and nothing
/// here needs a row budget; only the byte cap applies and hitting it ends the
/// scan. Under the agent profile there is no plan limit, because reporting an
/// honest `total_rows` and spilling the withheld rows both require seeing the
/// whole result — the cost of that is the price of recoverability, and an
/// agent that writes `LIMIT` in its SQL avoids it entirely.
async fn drain_budgeted(
    stream: &mut (
             impl futures::Stream<Item = datafusion::error::Result<arrow::array::RecordBatch>> + Unpin
         ),
    schema: SchemaRef,
    format: Format,
    head_rows: Option<usize>,
    byte_cap: Option<u64>,
    agent: bool,
) -> Result<Drained> {
    let mut writer = BatchWriter::new(format, schema.clone(), byte_cap)?;
    // The spill opens itself once the result outgrows what stdout will render.
    let mut spill = agent.then(|| ResultSpill::new(schema, head_rows.unwrap_or(usize::MAX)));
    let mut returned_rows: u64 = 0;
    let mut total_rows: u64 = 0;
    // True once stdout has taken all it may; the stream keeps flowing in agent
    // mode so the spill and the row count stay complete.
    let mut stdout_full = false;

    while let Some(batch) = stream.next().await {
        let batch = batch.map_err(classify_df_error)?;
        total_rows += batch.num_rows() as u64;

        if let Some(spill) = spill.as_mut() {
            spill.push(&batch)?;
        }
        if stdout_full {
            continue;
        }

        let room = match head_rows {
            Some(h) => (h as u64).saturating_sub(returned_rows),
            None => u64::MAX,
        };
        if room == 0 {
            stdout_full = true;
        } else {
            let take = room.min(batch.num_rows() as u64) as usize;
            let slice = if take == batch.num_rows() {
                batch.clone()
            } else {
                batch.slice(0, take)
            };
            let within_bytes = writer.write(&slice)?;
            returned_rows += take as u64;
            // Either the byte cap tripped or this batch was cut short.
            if !within_bytes || take < batch.num_rows() {
                stdout_full = true;
                // A byte-capped result can be short of the row budget and
                // still be incomplete on stdout, so make sure the withheld
                // rows have somewhere to live.
                if !within_bytes && let Some(spill) = spill.as_mut() {
                    spill.force()?;
                }
            }
        }
        // Without a spill to fill there is nothing more to learn from the
        // stream, so stop reading it — this preserves the default profile's
        // early-exit behaviour exactly.
        if stdout_full && spill.is_none() {
            writer.finish()?;
            return Ok(Drained {
                truncated: true,
                total_rows,
                returned_rows,
                spill: None,
            });
        }
    }
    writer.finish()?;
    let spill = spill.map(|s| s.finish()).transpose()?;
    Ok(Drained {
        truncated: stdout_full,
        total_rows,
        returned_rows,
        spill,
    })
}

/// Parse a decision point shared by `query --as-of` and `leakage-check
/// --as-of`: an integer version, an RFC3339 timestamp resolved against commit
/// *availability* (`committed_at_ns`), or a snapshot name. Mirrors the `h5i()`
/// time-travel function so one spelling works everywhere.
///
/// Bare integers win over snapshot names by design: a snapshot named "42"
/// would otherwise silently shadow version 42. Name snapshots, not numbers.
fn parse_read_at(s: &str) -> Result<ReadAt> {
    if let Ok(v) = s.parse::<u64>() {
        return Ok(ReadAt::Version(v));
    }
    if let Ok(ts) = chrono::DateTime::parse_from_rfc3339(s) {
        let ns = ts
            .timestamp_nanos_opt()
            .ok_or_else(|| Error::invalid(format!("--as-of timestamp {s:?} out of range")))?;
        return Ok(ReadAt::AsOf(ns));
    }
    Ok(ReadAt::Snapshot(s.to_string()))
}

/// Build a research-mode pin from the two axes.
///
/// They are deliberately independent. Arrival is measured on the *commit*
/// clock and event time on the *data* clock, and under a bulk ingest those are
/// years apart: ten years of history committed this morning has a first commit
/// of today, so an arrival pin at a historical instant resolves to nothing
/// while an event-time cutoff at the same instant is exactly what is wanted.
/// Coupling them would make the event-time axis unusable on precisely the
/// databases it exists for.
///
/// When the two clocks *are* aligned — a continuously-ingested database — an
/// RFC3339 `--as-of` supplies the decision time as well, so the common case
/// stays one flag.
fn research_pin(
    as_of: Option<&str>,
    decision_time: Option<&str>,
    embargo: Option<std::time::Duration>,
) -> Result<ResearchPin> {
    let at = match as_of {
        Some(s) => parse_read_at(s)?,
        None => ReadAt::Latest,
    };
    // An explicit decision time wins; otherwise inherit an --as-of instant.
    let decision_ns = match decision_time {
        Some(s) => Some(parse_rfc3339_ns(s, "--decision-time")?),
        None => match at {
            ReadAt::AsOf(ns) => Some(ns),
            _ => None,
        },
    };

    if decision_time.is_none() && embargo.is_none() {
        // No event-time axis requested: arrival only, exactly as before.
        return Ok(ResearchPin::at(at));
    }
    let Some(decision_ns) = decision_ns else {
        return Err(Error::invalid(
            "--embargo needs an instant to measure back from; pass --decision-time with an \
             RFC3339 timestamp (or an RFC3339 --as-of, when the commit and event clocks agree)",
        ));
    };
    let embargo_ns = match embargo {
        Some(d) => i64::try_from(d.as_nanos())
            .map_err(|_| Error::invalid(format!("--embargo {d:?} is too long to represent")))?,
        None => 0,
    };
    let cutoff_ns = decision_ns.checked_sub(embargo_ns).ok_or_else(|| {
        Error::invalid("--embargo reaches back before the representable time range")
    })?;
    Ok(ResearchPin::at(at).with_event_time_cutoff_ns(cutoff_ns))
}

/// Parse an RFC3339 instant into nanoseconds since the epoch.
fn parse_rfc3339_ns(s: &str, flag: &str) -> Result<i64> {
    let ts = chrono::DateTime::parse_from_rfc3339(s)
        .map_err(|e| Error::invalid(format!("{flag} {s:?} is not an RFC3339 timestamp: {e}")))?;
    ts.timestamp_nanos_opt()
        .ok_or_else(|| Error::invalid(format!("{flag} {s:?} is outside the representable range")))
}

/// `--…-mb` flags → bytes, rejecting 0 and overflow.
fn mb_to_bytes(mb: u64) -> Result<u64> {
    if mb == 0 {
        return Err(Error::invalid("segment size must be at least 1 MiB"));
    }
    mb.checked_mul(1024 * 1024)
        .ok_or_else(|| Error::invalid(format!("segment size {mb} MiB overflows a byte count")))
}

/// Stream an input source into aligned batches, with TTY progress.
fn read_aligned(
    input: &str,
    format: InputFormat,
    schema: &SchemaRef,
    label: &'static str,
) -> Result<Vec<arrow::array::RecordBatch>> {
    let reader = open_input(input, format, Some(schema.clone()))?;
    let mut progress = Progress::start(label);
    let mut batches = Vec::new();
    let mut rows: u64 = 0;
    for batch in reader {
        let batch = align_batch(batch?, schema)?;
        rows += batch.num_rows() as u64;
        batches.push(batch);
        progress.update(rows);
    }
    progress.finish();
    Ok(batches)
}

async fn run(cli: Cli) -> Result<()> {
    let format = cli.format;
    match cli.command {
        Command::Init { db } => {
            Database::create(&db).await?;
            write_value(
                &serde_json::json!({"created": db.display().to_string()}),
                format,
            )
        }

        Command::CreateTable {
            db,
            table,
            schema,
            like,
            time_column,
            sort_key,
            target_segment_mb,
        } => {
            let schema: SchemaRef = match (schema, like) {
                (Some(json), None) => parse_schema_json(&json)?,
                (None, Some(path)) => {
                    // The reader knows its schema up front; no data is read.
                    let reader = open_input(&path, InputFormat::Auto, None)?;
                    if reader.schema.fields().is_empty() {
                        return Err(Error::invalid("--like file contains no data"));
                    }
                    reader.schema.clone()
                }
                _ => return Err(Error::invalid("provide exactly one of --schema or --like")),
            };
            // Inferred schemas (CSV/--like) mark everything nullable; the
            // time column must be non-nullable, so tighten it here.
            let schema: SchemaRef = if let Some(tc) = &time_column {
                Arc::new(Schema::new(
                    schema
                        .fields()
                        .iter()
                        .map(|f| {
                            if f.name() == tc {
                                Field::new(f.name(), f.data_type().clone(), false)
                            } else {
                                f.as_ref().clone()
                            }
                        })
                        .collect::<Vec<_>>(),
                ))
            } else {
                schema
            };
            let db = Database::open(&db).await?;
            let result = db
                .create_table(
                    &table,
                    schema,
                    TableOptions {
                        time_column,
                        sort_key,
                        storage: StorageOptions {
                            target_segment_bytes: mb_to_bytes(target_segment_mb)?,
                            ..Default::default()
                        },
                        max_segments_per_manifest: None,
                    },
                )
                .await?;
            write_value(&result, format)
        }

        Command::DropTable { db, table, yes } => {
            if !yes {
                return Err(Error::invalid(
                    "drop-table permanently deletes data; pass --yes to confirm",
                ));
            }
            let db = Database::open(&db).await?;
            db.drop_table(&table).await?;
            write_value(&serde_json::json!({"dropped": table}), format)
        }

        Command::Rename { db, from, to } => {
            let db = Database::open(&db).await?;
            db.rename_table(&from, &to).await?;
            write_value(
                &serde_json::json!({"renamed": {"from": from, "to": to}}),
                format,
            )
        }

        Command::Context {
            db,
            budget,
            stale_after,
        } => {
            let label = db.display().to_string();
            let database = Database::open_read_only(&db).await?;
            let doc =
                context::build(&database, &label, budget, stale_after.map(|d| d.as_secs())).await?;
            write_value(&doc, format)
        }

        Command::Tables { db } => {
            let db = Database::open_read_only(&db).await?;
            let mut rows = Vec::new();
            for entry in db.list_tables().await? {
                let resolved = db.resolve(&entry.name, ReadAt::Latest).await?;
                rows.push(serde_json::json!({
                    "table": entry.name,
                    "version": resolved.manifest.sequence,
                    "rows": resolved.manifest.rows,
                    "bytes": resolved.manifest.bytes,
                    "segments": resolved.manifest.segments.len(),
                    "time_range": resolved.manifest.time_range,
                    "time_column": resolved.spec.time_column,
                }));
            }
            write_value(&rows, format)
        }

        Command::Schema { db, table } => {
            let db = Database::open_read_only(&db).await?;
            let resolved = db.resolve(&table, ReadAt::Latest).await?;
            let fields: Vec<_> = resolved
                .schema
                .fields()
                .iter()
                .map(|f| {
                    serde_json::json!({
                        "name": f.name(),
                        "type": f.data_type().to_string(),
                        "nullable": f.is_nullable(),
                    })
                })
                .collect();
            write_value(
                &serde_json::json!({
                    "table": table,
                    "version": resolved.manifest.sequence,
                    "schema_revision": resolved.manifest.schema_revision,
                    "time_column": resolved.spec.time_column,
                    "sort_key": resolved.spec.sort_key,
                    "fields": fields,
                }),
                format,
            )
        }

        Command::Sample {
            db,
            table,
            rows,
            version,
        } => {
            let db = Database::open_read_only(&db).await?;
            let at = version.map(ReadAt::Version).unwrap_or(ReadAt::Latest);
            let resolved = db.resolve(&table, at).await?;
            let (batches, _) = db
                .scan_resolved(
                    &resolved,
                    ScanOptions {
                        limit: Some(rows),
                        ..Default::default()
                    },
                )
                .await?;
            write_batches(&batches, &resolved.schema, format)
        }

        Command::Query {
            db,
            sql,
            max_rows,
            max_bytes,
            timeout,
            memory_limit_mb,
            spill_dir,
            threads,
            stats,
            predicate_cache,
            as_of,
            decision_time,
            embargo,
        } => {
            let sql = if sql == "-" {
                let mut buf = String::new();
                std::io::stdin()
                    .lock()
                    .read_to_string(&mut buf)
                    .map_err(|e| Error::io("stdin", e))?;
                buf
            } else {
                sql
            };
            let db = Arc::new(Database::open_read_only(&db).await?);
            let options = SessionOptions {
                memory_limit: memory_limit_mb.map(|m| m * 1024 * 1024),
                spill_dir,
                target_partitions: threads,
                batch_size: None,
                telemetry_capacity: 0,
                predicate_cache: if predicate_cache {
                    PredicateCacheMode::ReadWrite
                } else {
                    PredicateCacheMode::Disabled
                },
            };
            // Research mode pins every table at the decision point; without
            // --as-of this is the same `new()` call as before, so the default
            // path is untouched.
            // Session construction resolves every table, so --as-of surfaces
            // real user errors here (unknown snapshot, version out of range).
            // Classify rather than blanket-internal them so exit codes hold.
            let pin = research_pin(
                as_of.as_deref(),
                decision_time.as_deref(),
                embargo.map(|d| *d),
            )?;
            let session = if pin.is_unpinned() {
                // Byte-for-byte the pre-research-mode path.
                H5iSession::new(db, options).await
            } else {
                H5iSession::new_pinned(db, options, pin).await
            }
            .map_err(classify_df_error)?;

            // The profile only fills in budgets the caller left unset, and it
            // never changes anything else about the output — in particular it
            // is read from the environment, never sniffed from the TTY, so a
            // piped run produces the same bytes as an interactive one.
            let agent = Profile::from_env().is_agent();
            let head_rows = if agent {
                Some(max_rows.unwrap_or(AGENT_MAX_ROWS))
            } else {
                max_rows
            };
            let byte_cap = if agent {
                Some(max_bytes.unwrap_or(AGENT_MAX_BYTES))
            } else {
                max_bytes
            };
            // A limit the caller asked for stays a hard error on breach; the
            // profile's own budget is soft, and truncation is reported instead.
            let hard_byte_limit = max_bytes;
            // Only the default profile pushes the row limit into the plan.
            let plan_limit = if agent { None } else { max_rows };

            let work = async {
                // Only pay for query-local performance reporting when the
                // caller asked for it; the default path plans and streams
                // without constructing a report.
                let (drained, report) = if stats {
                    let df = session
                        .sql_reported(&sql)
                        .await
                        .map_err(classify_df_error)?;
                    let df = match plan_limit {
                        Some(n) => df.limit(0, Some(n)).map_err(classify_df_error)?,
                        None => df,
                    };
                    let mut stream = df.execute_stream().await.map_err(classify_df_error)?;
                    let schema = stream.schema();
                    let drained =
                        drain_budgeted(&mut stream, schema, format, head_rows, byte_cap, agent)
                            .await?;
                    (drained, stream.report().cloned())
                } else {
                    let df = session.sql(&sql).await.map_err(classify_df_error)?;
                    let df = match plan_limit {
                        Some(n) => df.limit(0, Some(n)).map_err(classify_df_error)?,
                        None => df,
                    };
                    let schema: SchemaRef = Arc::new(df.schema().as_arrow().clone());
                    let mut stream = df.execute_stream().await.map_err(classify_df_error)?;
                    let drained =
                        drain_budgeted(&mut stream, schema, format, head_rows, byte_cap, agent)
                            .await?;
                    (drained, None)
                };

                // Tell the agent what it did not see — including on an
                // untruncated result, where an exact row count saves it a
                // second COUNT(*) round trip.
                if let Some(spill) = &drained.spill {
                    ResultSummary::build(
                        drained.total_rows,
                        drained.returned_rows,
                        drained.truncated,
                        head_rows.unwrap_or(AGENT_MAX_ROWS),
                        byte_cap.unwrap_or(AGENT_MAX_BYTES),
                        spill,
                    )
                    .emit();
                }

                // Breaching a caller-set --max-bytes keeps its exit-4 envelope.
                if drained.truncated
                    && let Some(limit) = hard_byte_limit
                {
                    return Err(Error::LimitExceeded {
                        detail: format!(
                            "result exceeded --max-bytes={limit}; output truncated at a batch \
                             boundary"
                        ),
                    });
                }
                Ok(report)
            };
            let report = match timeout {
                Some(t) => tokio::time::timeout(*t, work)
                    .await
                    .map_err(|_| Error::Timeout {
                        seconds: (*t).as_secs(),
                    })??,
                None => work.await?,
            };
            if stats && let Some(report) = report {
                eprintln!("{}", serde_json::to_string(&report)?);
            }
            Ok(())
        }

        Command::Ingest {
            db,
            table,
            input,
            input_format,
            mode,
            retries,
            write_flags,
        } => {
            let db = Database::open(&db).await?;
            let resolved = db.resolve(&table, ReadAt::Latest).await?;
            let batches = read_aligned(&input, input_format, &resolved.schema, "ingest")?;
            let opts = write_flags.to_options();
            let result = match mode {
                IngestMode::Write => db.write(&table, batches, opts).await?,
                IngestMode::Append => db.append_with_retry(&table, batches, opts, retries).await?,
            };
            write_value(&result, format)
        }

        Command::Versions { db, table } => {
            let db = Database::open_read_only(&db).await?;
            let versions = db.list_versions(&table).await?;
            let rows: Vec<_> = versions
                .iter()
                .map(|v| {
                    serde_json::json!({
                        "version": v.sequence,
                        "op": v.op,
                        "committed_at": chrono::DateTime::from_timestamp_nanos(v.committed_at_ns)
                            .to_rfc3339(),
                        "rows": v.rows,
                        "bytes": v.bytes,
                        "segments": v.segments,
                        "note": v.note,
                    })
                })
                .collect();
            write_value(&rows, format)
        }

        Command::LeakageCheck {
            db,
            sql,
            as_of,
            tolerance,
        } => {
            let sql = if sql == "-" {
                let mut buf = String::new();
                std::io::stdin()
                    .lock()
                    .read_to_string(&mut buf)
                    .map_err(|e| Error::io("stdin", e))?;
                buf
            } else {
                sql
            };
            let at = parse_read_at(&as_of)?;
            let db = Arc::new(Database::open_read_only(&db).await?);
            let report = h5i_db_query::check_leakage(
                db,
                &sql,
                at,
                tolerance.unwrap_or(h5i_db_query::DEFAULT_TOLERANCE),
            )
            .await
            .map_err(classify_df_error)?;
            write_value(&report, format)
        }

        Command::Snapshot(cmd) => match cmd {
            SnapshotCmd::Create {
                db,
                name,
                tables,
                note,
            } => {
                let db = Database::open(&db).await?;
                let snap = db.create_snapshot(&name, &tables, note).await?;
                write_value(&snap, format)
            }
            SnapshotCmd::List { db } => {
                let db = Database::open_read_only(&db).await?;
                write_value(&db.list_snapshots().await?, format)
            }
            SnapshotCmd::Delete { db, name } => {
                let db = Database::open(&db).await?;
                db.delete_snapshot(&name).await?;
                write_value(&serde_json::json!({"deleted": name}), format)
            }
        },

        Command::Restore {
            db,
            table,
            version,
            write_flags,
        } => {
            let db = Database::open(&db).await?;
            let result = db
                .restore(&table, version, write_flags.to_options())
                .await?;
            write_value(&result, format)
        }

        Command::ReplaceRange {
            db,
            table,
            start,
            end,
            input,
            input_format,
            plan,
            write_flags,
        } => {
            let db = Database::open(&db).await?;
            let resolved = db.resolve(&table, ReadAt::Latest).await?;
            let (start, end) = parse_range(&resolved, &start, &end)?;
            let batches = match input {
                Some(path) => read_aligned(&path, input_format, &resolved.schema, "replace-range")?,
                None => vec![],
            };
            if plan {
                let p = db
                    .plan_replace_range(&table, start, end, batches, write_flags.to_options())
                    .await?;
                print_plan(&p, format)
            } else {
                let result = db
                    .replace_range(&table, start, end, batches, write_flags.to_options())
                    .await?;
                write_value(&result, format)
            }
        }

        Command::DeleteRange {
            db,
            table,
            start,
            end,
            plan,
            write_flags,
        } => {
            let db = Database::open(&db).await?;
            let resolved = db.resolve(&table, ReadAt::Latest).await?;
            let (start, end) = parse_range(&resolved, &start, &end)?;
            if plan {
                let p = db
                    .plan_replace_range(&table, start, end, vec![], write_flags.to_options())
                    .await?;
                print_plan(&p, format)
            } else {
                let result = db
                    .delete_range(&table, start, end, write_flags.to_options())
                    .await?;
                write_value(&result, format)
            }
        }

        Command::Plan(cmd) => match cmd {
            PlanCmd::List { db, table } => {
                let db = Database::open_read_only(&db).await?;
                let plans = db.list_plans(&table).await?;
                let rows: Vec<_> = plans
                    .iter()
                    .map(|p| {
                        serde_json::json!({
                            "plan_id": p.plan_id,
                            "op": p.op.to_string(),
                            "base_version": p.base_version,
                            "created_at": chrono::DateTime::from_timestamp_nanos(p.created_at_ns)
                                .to_rfc3339(),
                            "expired": p.is_expired(),
                            "summary": p.summary,
                        })
                    })
                    .collect();
                write_value(&rows, format)
            }
            PlanCmd::Show { db, table, plan_id } => {
                let db = Database::open_read_only(&db).await?;
                let plan = db.load_plan(&table, plan_id).await?;
                print_plan(&plan, format)
            }
            PlanCmd::Apply { db, table, plan_id } => {
                let db = Database::open(&db).await?;
                let plan = db.load_plan(&table, plan_id).await?;
                let result = db.apply_plan(&plan).await?;
                write_value(&result, format)
            }
            PlanCmd::Discard { db, table, plan_id } => {
                let db = Database::open(&db).await?;
                db.discard_plan(&table, plan_id).await?;
                write_value(&serde_json::json!({"discarded": plan_id}), format)
            }
        },

        Command::Policy(cmd) => match cmd {
            PolicyCmd::Show { db } => {
                let db = Database::open_read_only(&db).await?;
                write_value(&db.policy().await?, format)
            }
            PolicyCmd::Set { db, entries } => {
                let db = Database::open(&db).await?;
                // Atomic read-modify-write; concurrent editors can't clobber
                // each other's keys.
                let policy = db
                    .update_policy(|policy| {
                        for entry in &entries {
                            let (key, value) = entry.split_once('=').ok_or_else(|| {
                                Error::invalid(format!(
                                    "policy entries are key=true|false, got {entry:?}"
                                ))
                            })?;
                            let value: bool = value.parse().map_err(|_| {
                                Error::invalid(format!(
                                    "policy value must be true or false, got {value:?}"
                                ))
                            })?;
                            policy.set(key.trim(), value)?;
                        }
                        Ok(())
                    })
                    .await?;
                write_value(&policy, format)
            }
        },

        Command::DataPolicy(cmd) => match cmd {
            DataPolicyCmd::Get { db, table } => {
                let db = Database::open_read_only(&db).await?;
                // `null` when unset — a stable, machine-readable "no policy".
                write_value(&db.data_policy(&table).await?, format)
            }
            DataPolicyCmd::Set { db, table, policy } => {
                let text = match policy.as_str() {
                    "-" => {
                        let mut buf = String::new();
                        std::io::stdin()
                            .lock()
                            .read_to_string(&mut buf)
                            .map_err(|e| Error::io("stdin", e))?;
                        buf
                    }
                    other if other.starts_with('@') => std::fs::read_to_string(&other[1..])
                        .map_err(|e| Error::io(other[1..].to_string(), e))?,
                    inline => inline.to_string(),
                };
                let parsed: h5i_db_core::DataPolicy = serde_json::from_str(&text)
                    .map_err(|e| Error::invalid(format!("data-policy JSON: {e}")))?;
                let db = Database::open(&db).await?;
                db.set_data_policy(&table, &parsed).await?;
                write_value(&parsed, format)
            }
            DataPolicyCmd::Clear { db, table } => {
                let db = Database::open(&db).await?;
                db.clear_data_policy(&table).await?;
                write_value(&serde_json::json!({"cleared": table}), format)
            }
        },

        Command::Compact {
            db,
            table,
            target_mb,
            write_flags,
        } => {
            let db = Database::open(&db).await?;
            let result = db
                .compact_with(
                    &table,
                    target_mb.map(mb_to_bytes).transpose()?,
                    write_flags.to_options(),
                )
                .await?;
            write_value(&result, format)
        }

        Command::Vacuum {
            db,
            table,
            grace_seconds,
            apply,
        } => {
            let db = Database::open(&db).await?;
            let report = db.vacuum(table.as_deref(), grace_seconds, apply).await?;
            write_value(&report, format)
        }

        Command::Ui {
            db,
            port,
            allow_mutations,
        } => {
            let label = db.display().to_string();
            let database = if allow_mutations {
                Database::open(&db).await?
            } else {
                Database::open_read_only(&db).await?
            };
            h5i_db_ui::serve(Arc::new(database), label, port, allow_mutations).await
        }

        Command::Verify { db, table, deep } => {
            let db = Database::open_read_only(&db).await?;
            let report = db.verify(&table, deep).await?;
            if report.problems.is_empty() {
                write_value(&report, format)
            } else {
                write_value(&report, format)?;
                Err(Error::corruption(
                    format!("table {table:?}"),
                    format!("{} problem(s) found", report.problems.len()),
                ))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

fn print_plan(plan: &h5i_db_core::MutationPlan, format: Format) -> Result<()> {
    write_value(
        &serde_json::json!({
            "plan_id": plan.plan_id,
            "table": plan.table,
            "op": plan.op.to_string(),
            "base_version": plan.base_version,
            "expires_at": chrono::DateTime::from_timestamp_nanos(plan.expires_at_ns).to_rfc3339(),
            "summary": plan.summary,
            "apply_with": format!("h5i-db plan apply <db> {} {}", plan.table, plan.plan_id),
        }),
        format,
    )?;
    if let Some(b64) = &plan.before_sample_ipc_b64 {
        eprintln!("before (sample):");
        let batches = h5i_db_core::MutationPlan::decode_sample(b64)?;
        let rendered =
            arrow::util::pretty::pretty_format_batches(&batches).map_err(Error::Arrow)?;
        eprintln!("{rendered}");
    }
    if let Some(b64) = &plan.after_sample_ipc_b64 {
        eprintln!("after (sample):");
        let batches = h5i_db_core::MutationPlan::decode_sample(b64)?;
        let rendered =
            arrow::util::pretty::pretty_format_batches(&batches).map_err(Error::Arrow)?;
        eprintln!("{rendered}");
    }
    Ok(())
}

/// Parse a time-range bound: RFC3339 timestamp or raw integer, converted to
/// the table's time-column unit.
fn parse_time_bound(resolved: &h5i_db_core::ResolvedTable, s: &str) -> Result<i64> {
    if let Ok(raw) = s.parse::<i64>() {
        return Ok(raw);
    }
    let dt = chrono::DateTime::parse_from_rfc3339(s).map_err(|e| {
        Error::invalid(format!(
            "time bound {s:?} is neither an integer nor RFC3339: {e}"
        ))
    })?;
    let ns = dt
        .timestamp_nanos_opt()
        .ok_or_else(|| Error::invalid(format!("time bound {s:?} out of range")))?;
    let tc = resolved
        .spec
        .time_column
        .as_ref()
        .ok_or_else(|| Error::invalid("table has no time column"))?;
    let field = resolved.schema.field_with_name(tc).map_err(Error::Arrow)?;
    let divisor = match field.data_type() {
        DataType::Timestamp(TimeUnit::Second, _) => 1_000_000_000,
        DataType::Timestamp(TimeUnit::Millisecond, _) => 1_000_000,
        DataType::Timestamp(TimeUnit::Microsecond, _) => 1_000,
        DataType::Timestamp(TimeUnit::Nanosecond, _) => 1,
        other => {
            return Err(Error::invalid(format!(
                "time column {tc:?} has integer type {other}; pass raw integer bounds"
            )));
        }
    };
    Ok(ns / divisor)
}

fn parse_range(
    resolved: &h5i_db_core::ResolvedTable,
    start: &str,
    end: &str,
) -> Result<(i64, i64)> {
    Ok((
        parse_time_bound(resolved, start)?,
        parse_time_bound(resolved, end)?,
    ))
}

/// Parse the create-table JSON schema.
fn parse_schema_json(json: &str) -> Result<SchemaRef> {
    #[derive(serde::Deserialize)]
    struct FieldSpec {
        name: String,
        #[serde(rename = "type")]
        ty: String,
        #[serde(default = "default_nullable")]
        nullable: bool,
    }
    fn default_nullable() -> bool {
        true
    }
    let specs: Vec<FieldSpec> = serde_json::from_str(json)
        .map_err(|e| Error::invalid(format!("bad --schema JSON: {e}")))?;
    if specs.is_empty() {
        return Err(Error::invalid("--schema must define at least one field"));
    }
    let fields: Vec<Field> = specs
        .iter()
        .map(|f| -> Result<Field> {
            let dt = match f.ty.to_ascii_lowercase().as_str() {
                "int8" => DataType::Int8,
                "int16" => DataType::Int16,
                "int32" | "int" => DataType::Int32,
                "int64" | "long" | "bigint" => DataType::Int64,
                "uint8" => DataType::UInt8,
                "uint16" => DataType::UInt16,
                "uint32" => DataType::UInt32,
                "uint64" => DataType::UInt64,
                "float32" | "float" => DataType::Float32,
                "float64" | "double" => DataType::Float64,
                "utf8" | "string" | "str" | "text" => DataType::Utf8,
                "bool" | "boolean" => DataType::Boolean,
                "date32" | "date" => DataType::Date32,
                "date64" => DataType::Date64,
                "timestamp_s" => DataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
                "timestamp_ms" => DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                "timestamp_us" => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                "timestamp_ns" | "timestamp" => {
                    DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
                }
                other => {
                    return Err(Error::invalid(format!(
                        "unknown type {other:?} for field {:?}; supported: int8..int64, \
                         uint8..uint64, float32/float64, utf8, bool, date32/date64, \
                         timestamp_s/ms/us/ns",
                        f.name
                    )));
                }
            };
            Ok(Field::new(&f.name, dt, f.nullable))
        })
        .collect::<Result<_>>()?;
    Ok(Arc::new(Schema::new(fields)))
}
