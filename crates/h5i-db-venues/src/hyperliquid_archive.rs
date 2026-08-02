//! Loading a downloaded Hyperliquid archive into a database.
//!
//! The parsers in [`crate::hyperliquid`] turn bytes into records; this turns
//! a *directory* into committed tables, which is the step that was missing.
//! Without it every caller reinvents the same walk: which hours are in the
//! window, which coins to keep, how a `.lz4` becomes a plan, and how to
//! avoid loading the same hour twice.
//!
//! # Still no fetching
//!
//! This reads local files. Syncing them is a shell command, and keeping it
//! there means the whole load is a pure function of a directory and is
//! testable without credentials or network:
//!
//! ```bash
//! aws s3 sync s3://hyperliquid-archive/market_data/20250101 \
//!     ./archive/market_data/20250101 --request-payer requester
//! aws s3 cp s3://hyperliquid-archive/asset_ctxs/20250101.csv.lz4 \
//!     ./archive/asset_ctxs/ --request-payer requester
//! ```
//!
//! The layout mirrors the bucket exactly, so a partial `aws s3 sync` of the
//! prefix is directly loadable:
//!
//! ```text
//! <root>/market_data/<YYYYMMDD>/<hour>/l2Book/<COIN>.lz4
//! <root>/asset_ctxs/<YYYYMMDD>.csv.lz4
//! ```
//!
//! Coins are filtered on the **file name**, not on the payload, so a load
//! for one coin skips a hundred and sixty-five files without decompressing
//! them -- which for an hour is the difference between reading 700 KB and
//! 77 MB. A live capture dropped into this layout must therefore be named
//! for its coin, and belongs in an hour of its own.
//!
//! # Hours are not what they look like
//!
//! The venue buckets a file by when its *archiver* received a message, so
//! the `0` file opens with a message the venue stamped just before
//! midnight the previous day. [`ArchiveLoad::window`] reports the span
//! actually loaded rather than the span requested, and a caller wanting a
//! clean day should sync an hour of slack on each side.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use h5i_db_backtest::error::{BacktestError, Result};
use h5i_db_backtest::event::Record;
use h5i_db_backtest::instrument::Instrument;
use h5i_db_backtest::types::UnixNanos;
use h5i_db_backtest::window::TimeWindow;
use h5i_db_core::Database;

use crate::hyperliquid::{self, AssetMeta};
use crate::{IngestOutcome, IngestPlan, write_plan};

/// Which slice of a downloaded archive to load.
#[derive(Clone, Debug)]
pub struct ArchiveSpec {
    /// The directory holding `market_data/` and `asset_ctxs/`.
    pub root: PathBuf,
    /// Dates to read, as the archive names them: `YYYYMMDD`.
    pub dates: Vec<String>,
    /// Hours within each date. Empty means every hour present.
    pub hours: Vec<u8>,
    /// Coins to keep. Empty means every coin present, which for one hour is
    /// a hundred and sixty-odd files and about 77 MB.
    pub coins: Vec<String>,
    /// Whether to read `asset_ctxs/<date>.csv.lz4` for marks and oracles.
    pub asset_ctxs: bool,
}

impl ArchiveSpec {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            dates: Vec::new(),
            hours: Vec::new(),
            coins: Vec::new(),
            asset_ctxs: true,
        }
    }

    pub fn date(mut self, date: impl Into<String>) -> Self {
        self.dates.push(date.into());
        self
    }

    pub fn hour(mut self, hour: u8) -> Self {
        self.hours.push(hour);
        self
    }

    pub fn coin(mut self, coin: impl Into<String>) -> Self {
        self.coins.push(coin.into());
        self
    }

    pub fn asset_ctxs(mut self, read: bool) -> Self {
        self.asset_ctxs = read;
        self
    }

    fn wants_coin(&self, coin: &str) -> bool {
        self.coins.is_empty() || self.coins.iter().any(|wanted| wanted == coin)
    }

    fn wants_hour(&self, hour: u8) -> bool {
        self.hours.is_empty() || self.hours.contains(&hour)
    }
}

/// What one load read, and what it could not.
#[derive(Clone, Debug, Default)]
pub struct ArchiveLoad {
    pub outcome: Option<IngestOutcome>,
    /// Files that produced records.
    pub files: Vec<PathBuf>,
    pub book_records: usize,
    pub trade_records: usize,
    pub reference_records: usize,
    /// Lines that were not readable as an envelope, across every file.
    pub malformed: u64,
    /// Lines on a channel this module does not model.
    pub skipped: u64,
    /// The span actually covered, which is not the span requested.
    pub window: Option<TimeWindow>,
    /// Named rather than swallowed: a date or hour the spec asked for that
    /// is not on disk is almost always an incomplete sync, and finding out
    /// at replay time -- as a thin book -- is much worse than here.
    pub missing: Vec<String>,
    /// What would be committed. Taken by [`load_archive`] when it commits.
    pub plan: Option<IngestPlan>,
}

impl ArchiveLoad {
    pub fn records(&self) -> usize {
        self.book_records + self.trade_records + self.reference_records
    }

    /// Refuse a load that is emptier than expected.
    pub fn require_files(&self, minimum: usize) -> Result<()> {
        if self.files.len() < minimum {
            return Err(BacktestError::invalid(format!(
                "loaded {} archive files, expected at least {minimum}; missing: {:?}",
                self.files.len(),
                self.missing
            )));
        }
        Ok(())
    }
}

/// Read a downloaded archive into an [`IngestPlan`], without writing.
///
/// Separated from committing so a caller can inspect or merge first, which
/// is the same shape every other loader here has.
///
/// `universe` supplies the instruments. It comes from a `meta` response
/// rather than from the archive, because the archive carries prices and no
/// metadata -- and a tick size guessed from observed prices would be wrong
/// for any coin whose book happens to be coarse that hour.
pub fn plan_from_archive(spec: &ArchiveSpec, universe: &[AssetMeta]) -> Result<ArchiveLoad> {
    let mut load = ArchiveLoad::default();
    let mut books: Vec<Record> = Vec::new();
    let mut trades: Vec<Record> = Vec::new();
    let mut references: Vec<Record> = Vec::new();
    let mut seen_coins: BTreeSet<String> = BTreeSet::new();

    if spec.dates.is_empty() {
        return Err(BacktestError::invalid(
            "an archive load needs at least one date; loading whatever \
             happens to be on disk makes a run depend on the shape of a \
             sync rather than on what was asked for",
        ));
    }

    for date in &spec.dates {
        let market_data = spec.root.join("market_data").join(date);
        if !market_data.is_dir() {
            load.missing.push(market_data.display().to_string());
        } else {
            for hour in 0..24_u8 {
                if !spec.wants_hour(hour) {
                    continue;
                }
                let dir = market_data.join(hour.to_string()).join("l2Book");
                if !dir.is_dir() {
                    if !spec.hours.is_empty() {
                        load.missing.push(dir.display().to_string());
                    }
                    continue;
                }
                for entry in sorted_files(&dir)? {
                    let Some(coin) = entry
                        .file_stem()
                        .and_then(|stem| stem.to_str())
                        .map(str::to_string)
                    else {
                        continue;
                    };
                    if !spec.wants_coin(&coin) {
                        continue;
                    }
                    let bytes = std::fs::read(&entry).map_err(|error| BacktestError::Parse {
                        what: "archive file",
                        value: format!("{}: {error}", entry.display()),
                    })?;
                    let read = hyperliquid::read_archive_lz4(&bytes)?;
                    load.malformed += read.malformed;
                    load.skipped += read.skipped;
                    if read.records.is_empty() {
                        continue;
                    }
                    seen_coins.insert(coin);
                    load.files.push(entry);
                    for record in read.records {
                        match record.event {
                            h5i_db_backtest::event::MarketEvent::Trade { .. } => {
                                trades.push(record)
                            }
                            _ => books.push(record),
                        }
                    }
                }
            }
        }

        if spec.asset_ctxs {
            let path = spec.root.join("asset_ctxs").join(format!("{date}.csv.lz4"));
            if path.is_file() {
                let bytes = std::fs::read(&path).map_err(|error| BacktestError::Parse {
                    what: "asset_ctxs file",
                    value: format!("{}: {error}", path.display()),
                })?;
                let contexts = hyperliquid::read_asset_ctxs_lz4(&bytes)?;
                let filter = if spec.coins.is_empty() {
                    None
                } else {
                    Some(spec.coins.as_slice())
                };
                let records = hyperliquid::asset_context_records(&contexts, filter)?;
                if !records.is_empty() {
                    load.files.push(path);
                    for context in &contexts {
                        if spec.wants_coin(&context.coin) {
                            seen_coins.insert(context.coin.clone());
                        }
                    }
                    references.extend(records);
                }
            } else {
                load.missing.push(path.display().to_string());
            }
        }
    }

    // A coin the spec asked for and the disk did not have is a hole in the
    // sync, and a run over it would silently be a run over nothing.
    for coin in &spec.coins {
        if !seen_coins.contains(coin) {
            load.missing.push(format!("no files for coin {coin}"));
        }
    }

    // The walk above is per file: one coin's hour, then the next coin's same
    // hour, then the next date. Each file is internally ordered, but the
    // concatenation of several is not, so a load of two coins -- or of two
    // dates -- hands the plan records that step backwards in time and is
    // rejected as out of order before a single row is written. Sorting here
    // is what makes a multi-coin, multi-date load work at all.
    //
    // Stable, so records sharing a timestamp keep the order their file gave
    // them: a snapshot and the delta that follows it within the same
    // millisecond must not swap.
    books.sort_by_key(|record| record.ts().get());
    trades.sort_by_key(|record| record.ts().get());
    references.sort_by_key(|record| record.ts().get());

    load.book_records = books.len();
    load.trade_records = trades.len();
    load.reference_records = references.len();

    let instruments: Vec<Instrument> = universe
        .iter()
        .filter(|asset| seen_coins.contains(&asset.name))
        .map(AssetMeta::instrument)
        .collect::<Result<_>>()?;
    let missing_meta: Vec<&String> = seen_coins
        .iter()
        .filter(|coin| !universe.iter().any(|asset| &&asset.name == coin))
        .collect();
    if !missing_meta.is_empty() {
        return Err(BacktestError::invalid(format!(
            "the archive holds data for {missing_meta:?} but the supplied \
             universe does not describe them; loading their books without a \
             tick size would accept prices the venue never quoted"
        )));
    }

    let plan = IngestPlan::new("hyperliquid")
        .with_instruments(instruments)
        .with_book_events(books)
        .with_trades(trades)
        .with_references(references);
    load.window = plan.loaded_window();
    load.plan = Some(plan);
    Ok(load)
}

/// Read a downloaded archive and commit it.
///
/// Idempotent through the ingest log, so re-running an interrupted sync is
/// the natural thing to do rather than a way to double a book.
pub async fn load_archive(
    db: &Database,
    spec: &ArchiveSpec,
    universe: &[AssetMeta],
    known_at: UnixNanos,
) -> Result<ArchiveLoad> {
    let mut load = plan_from_archive(spec, universe)?;
    let Some(plan) = load.plan.take() else {
        return Ok(load);
    };
    if plan.is_empty() {
        return Ok(load);
    }
    load.outcome = Some(write_plan(db, &plan, known_at).await?);
    Ok(load)
}

fn sorted_files(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut out: Vec<PathBuf> = std::fs::read_dir(dir)
        .map_err(|error| BacktestError::Parse {
            what: "archive directory",
            value: format!("{}: {error}", dir.display()),
        })?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("lz4"))
        .collect();
    // Sorted so a load is a pure function of the directory rather than of
    // the order the filesystem happened to hand entries back.
    out.sort();
    Ok(out)
}
