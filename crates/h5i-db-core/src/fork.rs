//! Forks: writable, pinned workspaces over a shared base (ROADMAP Part IX).
//!
//! A fork is **catalog aliasing plus a pin**, not a branched table history.
//! Three objects make one up, and none of them is data:
//!
//! - `forks/<hash>.json` — the pin set `{base table uuid → sequence}`, plus a
//!   freeform `user_meta` blob. This object is also a GC root: exactly like a
//!   snapshot, it forbids the retention floor from rising above what it pins,
//!   which is what keeps a fork's shared base segments alive.
//! - `catalog/forks/<hash>/<hash>.json` — the fork-scoped catalog. Name lookup
//!   inside a fork checks here first, then falls back to the pinned base.
//! - `tables/<uuid>/…` — ordinary tables. A fork owns no new *kind* of
//!   storage.
//!
//! That last point is the whole design. Because a fork's tables are ordinary
//! tables, the commit protocol, the per-table writer lock, retention, vacuum,
//! compaction, and time travel all work on them untouched: N agents writing
//! "the same" table in N forks are writing N different `table_id`s and never
//! contend. Nothing in the write path had to learn what a fork is.
//!
//! # Copy-on-write shadows
//!
//! The first write to a *base* table's name inside a fork materializes a
//! **shadow**: a fresh `table_id` whose version 0 manifest is a copy of the
//! pinned base manifest. Manifests are self-contained lists of segment
//! metadata, so this copies kilobytes of JSON and zero bytes of Parquet — the
//! shadow's segments are the base's segments, referenced by path.
//!
//! # The refinement invariant
//!
//! Those cross-directory references are the one genuinely delicate part, since
//! vacuum reclaims per table. They are safe because of a single invariant,
//! enforced at commit time and re-checked by `verify`:
//!
//! > A shadow manifest may reference only (a) segments under its own table
//! > prefix, or (b) segments listed in the base manifest at the pinned
//! > sequence.
//!
//! (b) is safe because the fork's pin holds the base's retention floor at or
//! below the pinned sequence, and base vacuum treats every segment in
//! `floor..=head` as reachable. So a fork's inherited segments are inside base
//! vacuum's referenced set for the fork's entire lifetime, without vacuum
//! knowing forks exist. Dropping the fork releases the pin, and the ordinary
//! retention machinery reclaims the storage.
//!
//! Note which way this cuts: the risk was never that a fork *retains* too much
//! (that is visible and boring), but that vacuum *deletes* segments a fork
//! still reads. The invariant is what makes that impossible rather than
//! unlikely.

use std::collections::{BTreeMap, BTreeSet};

use futures::stream::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::Backend;
use crate::catalog::CatalogEntry;
use crate::database::CommitInputs;
use crate::error::{Error, Result};
use crate::layout;

/// Manifest `user_meta` key recording a shadow's origin. Namespaced like
/// `IDEMPOTENCY_META_KEY` so it cannot collide with caller metadata.
///
/// This is an audit-trail copy: the fork catalog entry is the authority for
/// where a shadow came from. The manifest carries it too so the provenance
/// survives in an immutable object even if the fork is later dropped.
pub const FORKED_FROM_META_KEY: &str = "h5i.forked_from";

/// One pinned base table inside a fork.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ForkPin {
    pub table_name: String,
    pub sequence: u64,
    pub manifest_checksum: String,
}

/// A fork: a named, pinned, writable workspace.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Fork {
    pub name: String,
    pub created_at_ns: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    /// Wall-clock time the pins were resolved against (`None` = base head at
    /// creation). Recorded for provenance; the pins themselves are the truth.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub as_of_ns: Option<i64>,
    /// base table UUID → pinned version.
    pub pins: BTreeMap<Uuid, ForkPin>,
    /// Caller-owned metadata. Deliberately unstructured: it is the loose join
    /// point for a future run ledger, so the two features need not be designed
    /// together (ROADMAP Part IX "Do NOT build").
    #[serde(default, skip_serializing_if = "serde_json::Map::is_empty")]
    pub user_meta: serde_json::Map<String, serde_json::Value>,
    #[serde(default)]
    pub checksum: String,
}

impl Fork {
    fn compute_checksum(&self) -> Result<String> {
        let mut clone = self.clone();
        clone.checksum = String::new();
        Ok(crate::util::checksum_hex(&serde_json::to_vec(&clone)?))
    }

    pub fn seal(mut self) -> Result<Self> {
        self.checksum = self.compute_checksum()?;
        Ok(self)
    }

    pub fn verify(&self, object: &str) -> Result<()> {
        if self.checksum != self.compute_checksum()? {
            return Err(Error::corruption(object, "fork checksum mismatch"));
        }
        Ok(())
    }

    /// The pinned version of a base table, if this fork pins it.
    pub fn pin(&self, table_id: Uuid) -> Option<&ForkPin> {
        self.pins.get(&table_id)
    }
}

/// Where a shadow table came from. Absent on tables created inside the fork.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ForkOrigin {
    pub base_table_id: Uuid,
    /// Base version the shadow was copied from — also the `expected_version`
    /// that `promote` compare-and-swaps against.
    pub base_sequence: u64,
    pub base_manifest_checksum: String,
}

/// A fork-scoped catalog entry: name → the fork-owned table backing it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForkTableEntry {
    pub name: String,
    pub table_id: Uuid,
    pub created_at_ns: i64,
    pub spec_revision: u32,
    /// `Some` = copy-on-write shadow of a base table; `None` = created inside
    /// the fork and invisible to the base database.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub origin: Option<ForkOrigin>,
    #[serde(default)]
    pub checksum: String,
}

impl ForkTableEntry {
    fn compute_checksum(&self) -> Result<String> {
        let mut clone = self.clone();
        clone.checksum = String::new();
        Ok(crate::util::checksum_hex(&serde_json::to_vec(&clone)?))
    }

    pub fn seal(mut self) -> Result<Self> {
        self.checksum = self.compute_checksum()?;
        Ok(self)
    }

    pub fn verify(&self, object: &str) -> Result<()> {
        if self.checksum != self.compute_checksum()? {
            return Err(Error::corruption(
                object,
                "fork catalog entry checksum mismatch",
            ));
        }
        Ok(())
    }

    pub fn is_shadow(&self) -> bool {
        self.origin.is_some()
    }

    /// View this entry as an ordinary catalog entry.
    ///
    /// Everything downstream of name resolution (resolve, scan, the whole
    /// write path) takes a `CatalogEntry` and must not care whether the name
    /// came from the global catalog or a fork's.
    pub fn to_catalog_entry(&self) -> Result<CatalogEntry> {
        CatalogEntry {
            name: self.name.clone(),
            table_id: self.table_id,
            created_at_ns: self.created_at_ns,
            spec_revision: self.spec_revision,
            checksum: String::new(),
        }
        .seal()
    }
}

// ---------------------------------------------------------------------------
// fork object storage
// ---------------------------------------------------------------------------

pub async fn load(backend: &Backend, name: &str) -> Result<Fork> {
    match load_opt(backend, name).await? {
        Some(f) => Ok(f),
        None => {
            // Miss only: pay one listing to turn "not found" into "did you
            // mean …", the same bargain the table and snapshot lookups make.
            let existing = list(backend).await.unwrap_or_default();
            Err(Error::fork_not_found_among(
                name,
                existing.iter().map(|f| f.name.as_str()),
            ))
        }
    }
}

pub async fn load_opt(backend: &Backend, name: &str) -> Result<Option<Fork>> {
    let path = layout::fork_path(name);
    let Some(bytes) = backend.get_opt(&path).await? else {
        return Ok(None);
    };
    let fork: Fork = serde_json::from_slice(&bytes)
        .map_err(|e| Error::corruption(path.as_ref(), format!("fork parse: {e}")))?;
    fork.verify(path.as_ref())?;
    if fork.name != name {
        return Err(Error::corruption(
            path.as_ref(),
            format!("fork name {:?} != requested {:?}", fork.name, name),
        ));
    }
    Ok(Some(fork))
}

/// Store a new fork with atomic create-if-absent semantics.
pub async fn create(backend: &Backend, fork: &Fork) -> Result<()> {
    let path = layout::fork_path(&fork.name);
    let bytes = serde_json::to_vec_pretty(fork)?;
    if !backend.put_if_absent(&path, bytes.into()).await? {
        return Err(Error::ForkExists {
            name: fork.name.clone(),
        });
    }
    backend.sync_objects(&[path]).await
}

/// Store many new forks, then fsync them in one pass.
///
/// Two things make this cheaper than a `create` loop, and both are about
/// round trips rather than bytes: the puts are issued concurrently, and the
/// durability barrier is taken once for the whole batch instead of once per
/// fork. At a thousand branches the second dominates.
pub async fn create_many(backend: &Backend, forks: &[Fork]) -> Result<()> {
    let paths: Vec<object_store::path::Path> = futures::stream::iter(forks)
        .map(|fork| async move {
            let path = layout::fork_path(&fork.name);
            let bytes = serde_json::to_vec_pretty(fork)?;
            if !backend.put_if_absent(&path, bytes.into()).await? {
                return Err(Error::ForkExists {
                    name: fork.name.clone(),
                });
            }
            Ok::<_, Error>(path)
        })
        .buffer_unordered(crate::backend::METADATA_FETCH_CONCURRENCY)
        .try_collect()
        .await?;
    backend.sync_objects(&paths).await
}

/// Overwrite an existing fork object (metadata edits). Callers must hold the
/// database metadata lock.
///
/// Drops the fork index first. The index reuses an entry when the object is
/// still there at the same size, which an in-place edit can defeat — and the
/// failure mode is a pin the index no longer reports, which is the one that
/// loses data. Invalidating costs one rebuild and removes the whole question.
pub async fn store(backend: &Backend, fork: &Fork) -> Result<()> {
    crate::fork_index::invalidate(backend).await?;
    let path = layout::fork_path(&fork.name);
    let bytes = serde_json::to_vec_pretty(fork)?;
    backend.put(&path, bytes.into()).await?;
    backend.sync_objects(&[path]).await
}

pub async fn delete(backend: &Backend, name: &str) -> Result<()> {
    backend.delete(&layout::fork_path(name)).await
}

pub async fn list(backend: &Backend) -> Result<Vec<Fork>> {
    let metas = backend
        .list(&object_store::path::Path::from(layout::FORK_PREFIX))
        .await?;
    let mut out: Vec<Fork> = futures::stream::iter(metas)
        .map(|meta| async move {
            let bytes = backend.get(&meta.location).await?;
            let fork: Fork = serde_json::from_slice(&bytes).map_err(|e| {
                Error::corruption(meta.location.as_ref(), format!("fork parse: {e}"))
            })?;
            fork.verify(meta.location.as_ref())?;
            Ok::<_, Error>(fork)
        })
        .buffer_unordered(crate::backend::METADATA_FETCH_CONCURRENCY)
        .try_collect()
        .await?;
    out.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(out)
}

// ---------------------------------------------------------------------------
// fork-scoped catalog
// ---------------------------------------------------------------------------

pub async fn load_entry(
    backend: &Backend,
    fork_name: &str,
    table_name: &str,
) -> Result<Option<ForkTableEntry>> {
    let path = layout::fork_catalog_entry_path(fork_name, table_name);
    let Some(bytes) = backend.get_opt(&path).await? else {
        return Ok(None);
    };
    let entry: ForkTableEntry = serde_json::from_slice(&bytes)
        .map_err(|e| Error::corruption(path.as_ref(), format!("fork catalog parse: {e}")))?;
    entry.verify(path.as_ref())?;
    if entry.name != table_name {
        return Err(Error::corruption(
            path.as_ref(),
            format!(
                "fork catalog entry name {:?} does not match requested name {:?} \
                 (hash collision or tampering)",
                entry.name, table_name
            ),
        ));
    }
    Ok(Some(entry))
}

/// Create a fork catalog entry, failing if the name is already taken in this
/// fork. Atomic create-if-absent, so two racing shadow materializations of the
/// same name cannot both win.
pub async fn create_entry(
    backend: &Backend,
    fork_name: &str,
    entry: &ForkTableEntry,
) -> Result<()> {
    let path = layout::fork_catalog_entry_path(fork_name, &entry.name);
    let bytes = serde_json::to_vec_pretty(entry)?;
    if !backend.put_if_absent(&path, bytes.into()).await? {
        return Err(Error::TableExists {
            name: entry.name.clone(),
        });
    }
    backend.sync_objects(&[path]).await
}

/// Overwrite a fork catalog entry (spec-revision bumps). Callers must hold the
/// database metadata lock.
///
/// Invalidates the fork index for the same reason [`store`] does.
pub async fn store_entry(backend: &Backend, fork_name: &str, entry: &ForkTableEntry) -> Result<()> {
    crate::fork_index::invalidate(backend).await?;
    let path = layout::fork_catalog_entry_path(fork_name, &entry.name);
    let bytes = serde_json::to_vec_pretty(entry)?;
    backend.put(&path, bytes.into()).await?;
    backend.sync_objects(&[path]).await
}

pub async fn remove_entry(backend: &Backend, fork_name: &str, table_name: &str) -> Result<()> {
    backend
        .delete(&layout::fork_catalog_entry_path(fork_name, table_name))
        .await
}

pub async fn list_entries(backend: &Backend, fork_name: &str) -> Result<Vec<ForkTableEntry>> {
    let metas = backend
        .list(&layout::fork_catalog_prefix(fork_name))
        .await?;
    let mut out: Vec<ForkTableEntry> = futures::stream::iter(metas)
        .map(|meta| async move {
            let bytes = backend.get(&meta.location).await?;
            let entry: ForkTableEntry = serde_json::from_slice(&bytes).map_err(|e| {
                Error::corruption(meta.location.as_ref(), format!("fork catalog parse: {e}"))
            })?;
            entry.verify(meta.location.as_ref())?;
            Ok::<_, Error>(entry)
        })
        .buffer_unordered(crate::backend::METADATA_FETCH_CONCURRENCY)
        .try_collect()
        .await?;
    out.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(out)
}

// ---------------------------------------------------------------------------
// the refinement invariant
// ---------------------------------------------------------------------------

/// Check the refinement invariant for a shadow manifest.
///
/// `own_prefix` is the shadow's own table prefix; `base_paths` is the segment
/// path set of the base manifest at the pinned sequence. A violation means a
/// commit is about to reference a segment nothing keeps alive — the one bug
/// class in this design that loses data rather than merely wasting space, so
/// it is rejected at commit time rather than detected later.
pub fn check_refinement(
    own_prefix: &str,
    base_paths: &BTreeSet<String>,
    segment_paths: impl IntoIterator<Item = impl AsRef<str>>,
) -> Result<()> {
    for path in segment_paths {
        let path = path.as_ref();
        if path.starts_with(own_prefix) || base_paths.contains(path) {
            continue;
        }
        return Err(Error::corruption(
            path,
            format!(
                "fork refinement invariant violated: segment is neither under the fork \
                 table's own prefix {own_prefix:?} nor in the pinned base manifest; \
                 nothing would keep it alive"
            ),
        ));
    }
    Ok(())
}

/// Whether a segment path belongs to the table that owns `prefix` (as opposed
/// to being inherited from the base at fork time).
///
/// Path prefix, not `created_by_sequence`, is the authoritative test: inherited
/// segments keep the base's `created_by_sequence` (honest provenance — they
/// really were introduced by that base version), so only the path says which
/// directory's lifecycle governs the file.
pub fn is_own_segment(prefix: &str, segment_path: &str) -> bool {
    segment_path.starts_with(prefix)
}

// ---------------------------------------------------------------------------
// reported shapes
// ---------------------------------------------------------------------------

/// What a fork did to one name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ForkTableKind {
    /// A table that exists only in the fork.
    Created,
    /// A copy-on-write shadow of a base table.
    Shadowed,
}

/// One table's worth of `fork diff`. Metadata only: no segment is read.
#[derive(Debug, Clone, Serialize)]
pub struct ForkTableDiff {
    pub table: String,
    pub kind: ForkTableKind,
    /// Base version the shadow forked from (`None` for created tables).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_sequence: Option<u64>,
    /// Base head right now — differs from `base_sequence` when main moved and
    /// `promote` would therefore conflict.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_head_sequence: Option<u64>,
    /// Commits made inside the fork (the shadow's own head sequence).
    pub fork_versions: u64,
    pub rows_base: u64,
    pub rows_fork: u64,
    pub bytes_base: u64,
    pub bytes_fork: u64,
    /// Segments the fork wrote itself.
    pub segments_added: usize,
    /// Inherited segments the fork no longer references.
    pub segments_removed: usize,
    /// Inherited segments still referenced — the shared, un-copied bytes.
    pub segments_shared: usize,
    pub schema_revision_base: Option<u32>,
    pub schema_revision_fork: u32,
    /// True when the base moved since the fork was created, so `promote`
    /// would fail its compare-and-swap.
    pub base_moved: bool,
    /// Set when the base moved but every intervening commit was a compaction.
    pub base_moved_by_compaction_only: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct ForkDiff {
    pub fork: String,
    pub created_at_ns: i64,
    pub tables: Vec<ForkTableDiff>,
}

/// Outcome of promoting one fork table into the base.
#[derive(Debug, Clone, Serialize)]
pub struct PromoteResult {
    pub fork: String,
    pub table: String,
    pub kind: ForkTableKind,
    /// New base version, or `None` when the promote registered a
    /// fork-created table (which keeps its own version history verbatim).
    pub base_sequence: u64,
    pub rows: u64,
    /// Segments moved into the base table's directory.
    pub segments_linked: usize,
    /// Bytes that had to be *copied* because the backend could not link them.
    pub bytes_copied: u64,
}

/// A fork as reported by `fork list`, with the numbers that decide whether it
/// is still worth keeping.
#[derive(Debug, Clone, Serialize)]
pub struct ForkSummary {
    pub name: String,
    pub created_at_ns: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub as_of_ns: Option<i64>,
    pub tables_pinned: usize,
    pub tables_created: usize,
    pub tables_shadowed: usize,
    /// Bytes the fork wrote itself.
    pub bytes_own: u64,
    /// Base bytes the fork's pins hold back from reclamation. This is the
    /// number that matters when a database has quietly accumulated forks:
    /// they cost main nothing until main tries to reclaim.
    pub bytes_pinned: u64,
}

// ---------------------------------------------------------------------------
// operations
// ---------------------------------------------------------------------------

impl crate::database::Database {
    /// Create a fork pinning every table in the base database.
    ///
    /// O(1) in data: one small JSON object is written and nothing is copied.
    /// With `as_of`, each table is pinned at its last version committed at or
    /// before that time, and tables that did not yet exist are simply not
    /// pinned — a fork of the past contains what the past contained.
    pub async fn create_fork(
        &self,
        name: &str,
        note: Option<String>,
        as_of_ns: Option<i64>,
        user_meta: serde_json::Map<String, serde_json::Value>,
    ) -> Result<Fork> {
        let mut created = self
            .create_forks(
                std::slice::from_ref(&name.to_string()),
                note,
                as_of_ns,
                user_meta,
            )
            .await?;
        Ok(created.pop().expect("one name in, one fork out"))
    }

    /// Create many forks over **one** resolution of the base (ROADMAP X-B1).
    ///
    /// Every fork of the same base at the same instant pins the same versions,
    /// so resolving that pin set once and stamping it onto N objects is the
    /// whole optimisation: a thousand forks cost one pass over the catalog
    /// instead of a thousand. This is the shape agentic simulation produces —
    /// a wide star of short-lived branches off one root.
    ///
    /// All-or-nothing on *validation*: every name is checked for availability
    /// under the metadata lock before anything is written, so the ordinary
    /// "one of these already exists" case writes nothing. Writing itself is
    /// not transactional — a crash part-way leaves some forks created, each
    /// individually complete and droppable.
    pub async fn create_forks(
        &self,
        names: &[String],
        note: Option<String>,
        as_of_ns: Option<i64>,
        user_meta: serde_json::Map<String, serde_json::Value>,
    ) -> Result<Vec<Fork>> {
        self.ensure_writable("fork create")?;
        // No fork-of-fork in v1: nesting multiplies the promote and GC paths
        // before the primitive has users (ROADMAP IX "Do NOT build").
        self.ensure_base("fork create")?;
        if names.is_empty() {
            return Err(Error::invalid("fork create needs at least one name"));
        }
        for name in names {
            crate::database::validate_table_name(name)?;
        }
        // A repeated name would race itself: the first write wins and the
        // second reports ForkExists for a fork this very call created, which
        // reads as a spurious conflict.
        let unique: BTreeSet<&String> = names.iter().collect();
        if unique.len() != names.len() {
            return Err(Error::invalid(
                "fork create was given the same name twice; fork names must be distinct",
            ));
        }

        let _meta = self.backend().meta_lock().await?;
        for name in names {
            if load_opt(self.backend(), name).await?.is_some() {
                return Err(Error::ForkExists { name: name.clone() });
            }
        }

        let mut pins = BTreeMap::new();
        let base_tables = self.list_tables().await?;
        for entry in base_tables.iter().cloned() {
            // HEAD already carries the checksum of the manifest it points at,
            // so pinning the current version needs no manifest read at all.
            // Re-deriving it made "fork create copies nothing" true of bytes
            // written and false of bytes read: one full manifest per table.
            let (sequence, checksum) = match as_of_ns {
                None => {
                    let head = self.head(&entry.name, entry.table_id).await?;
                    (head.head.sequence, head.head.manifest_checksum)
                }
                Some(ts) => {
                    match self
                        .resolve(&entry.name, crate::database::ReadAt::AsOf(ts))
                        .await
                    {
                        Ok(resolved) => {
                            let seq = resolved.manifest.sequence;
                            // An as-of pin lands on an arbitrary past version,
                            // whose checksum no root of trust holds, so this
                            // one genuinely has to be computed.
                            (seq, self.manifest_checksum_at(entry.table_id, seq).await?)
                        }
                        // The table has no version at or before `ts`: it did
                        // not exist yet, so the fork does not pin it.
                        Err(Error::VersionNotFound { .. }) => continue,
                        Err(e) => return Err(e),
                    }
                }
            };
            pins.insert(
                entry.table_id,
                ForkPin {
                    table_name: entry.name.clone(),
                    sequence,
                    manifest_checksum: checksum,
                },
            );
        }

        // An `as_of` that predates the whole database pins nothing, and the
        // resulting fork looks empty for reasons that surface much later as a
        // bare "table not found". Say it here instead. (A database with no
        // tables yet is a different thing and is allowed: that fork is empty
        // because the database is.)
        if as_of_ns.is_some() && pins.is_empty() && !base_tables.is_empty() {
            return Err(Error::invalid(format!(
                "--as-of pins no version of any of the {} table(s) in this database: the \
                 timestamp precedes their oldest retained commit",
                base_tables.len()
            )));
        }

        // Fence before publishing the fork, not after: a reader that cannot
        // see forks must never get a window in which one exists and the
        // FORMAT file still says it is safe to vacuum freely.
        self.raise_min_reader_version(layout::FORK_MIN_READER_VERSION)
            .await?;

        let created_at_ns = crate::util::monotonic_commit_ts(None);
        let forks = names
            .iter()
            .map(|name| {
                Fork {
                    name: name.clone(),
                    created_at_ns,
                    note: note.clone(),
                    as_of_ns,
                    pins: pins.clone(),
                    user_meta: user_meta.clone(),
                    checksum: String::new(),
                }
                .seal()
            })
            .collect::<Result<Vec<_>>>()?;
        create_many(self.backend(), &forks).await?;
        Ok(forks)
    }

    /// `count` forks named `prefix-0000`, `prefix-0001`, … over one resolution
    /// of the base.
    ///
    /// The zero-padded suffix keeps name order equal to creation order, which
    /// matters because every listing in the system sorts by name.
    pub async fn fork_many(
        &self,
        prefix: &str,
        count: usize,
        note: Option<String>,
        as_of_ns: Option<i64>,
        user_meta: serde_json::Map<String, serde_json::Value>,
    ) -> Result<Vec<Fork>> {
        if count == 0 {
            return Err(Error::invalid("fork_many needs a count of at least 1"));
        }
        let names: Vec<String> = (0..count).map(|i| format!("{prefix}-{i:04}")).collect();
        self.create_forks(&names, note, as_of_ns, user_meta).await
    }

    /// Checksum of a table's manifest at an exact sequence.
    pub(crate) async fn manifest_checksum_at(
        &self,
        table_id: Uuid,
        sequence: u64,
    ) -> Result<String> {
        let path = layout::manifest_path(table_id, sequence);
        let bytes = self
            .backend()
            .get_opt(&path)
            .await?
            .ok_or_else(|| Error::corruption(path.as_ref(), "manifest missing"))?;
        Ok(crate::util::checksum_hex(&bytes))
    }

    pub async fn fork_info(&self, name: &str) -> Result<Fork> {
        load(self.backend(), name).await
    }

    /// Every fork's name, in name order.
    ///
    /// Answered from the visibility index (Part X, X-A1), so this costs one
    /// read rather than one per fork — which is what makes "query across every
    /// fork" a reasonable thing to ask for at a thousand of them. Use
    /// [`Self::list_forks`] when the sizes and table counts are wanted too;
    /// that one reads a manifest per table per fork.
    pub async fn fork_names(&self) -> Result<Vec<String>> {
        Ok(self.fork_index().await?.forks.into_keys().collect())
    }

    /// Every fork with the numbers that decide whether to keep it: what it
    /// wrote, and what it is holding back from reclamation.
    pub async fn list_forks(&self) -> Result<Vec<ForkSummary>> {
        let mut out = Vec::new();
        for fork in list(self.backend()).await? {
            let entries = list_entries(self.backend(), &fork.name).await?;
            let mut bytes_own = 0u64;
            let mut created = 0usize;
            let mut shadowed = 0usize;
            for fe in &entries {
                if fe.is_shadow() {
                    shadowed += 1;
                } else {
                    created += 1;
                }
                let head = self.head(&fe.name, fe.table_id).await?.head.sequence;
                let manifest = self.manifest_at(fe.table_id, head).await?;
                let own_prefix = format!("{}/", layout::table_prefix(fe.table_id));
                bytes_own += manifest
                    .segments
                    .iter()
                    .filter(|s| is_own_segment(&own_prefix, &s.path))
                    .map(|s| s.bytes)
                    .sum::<u64>();
            }
            // Bytes the pin holds: everything in the pinned base version. Not
            // all of it is *exclusively* held (main's head usually shares most
            // of it), so this is an upper bound on what dropping the fork
            // could release, and is labelled as pinned rather than wasted.
            let mut bytes_pinned = 0u64;
            for (table_id, pin) in &fork.pins {
                if let Ok(m) = self.manifest_at(*table_id, pin.sequence).await {
                    bytes_pinned += m.bytes;
                }
            }
            out.push(ForkSummary {
                name: fork.name.clone(),
                created_at_ns: fork.created_at_ns,
                note: fork.note.clone(),
                as_of_ns: fork.as_of_ns,
                tables_pinned: fork.pins.len(),
                tables_created: created,
                tables_shadowed: shadowed,
                bytes_own,
                bytes_pinned,
            });
        }
        Ok(out)
    }

    /// Drop a fork and everything it owns. Returns the number of tables
    /// deleted.
    ///
    /// Ordering is deliberate: tables first, the fork object (and with it the
    /// pin) last. A crash part-way leaves a fork that still pins its base and
    /// can simply be dropped again — never a fork whose pin is gone while its
    /// tables still reference base segments.
    pub async fn drop_fork(&self, name: &str) -> Result<usize> {
        self.ensure_writable("fork drop")?;
        self.ensure_base("fork drop")?;
        let _meta = self.backend().meta_lock().await?;
        self.drop_fork_locked(name).await
    }

    /// Drop many forks under one metadata lock (ROADMAP X-B1).
    ///
    /// Pruning is the other half of the branch-mutate-evaluate loop: an agent
    /// that forked a thousand hypotheses discards most of them, and taking the
    /// database metadata lock a thousand times to do it is the wrong shape.
    ///
    /// Each fork is dropped in the same order a single drop uses, so the crash
    /// story is unchanged: re-running completes, and a partially dropped fork
    /// is always still pinned rather than unpinned with live tables. A name
    /// that does not exist stops the batch — silently skipping it would hide a
    /// typo in a list the caller believes it deleted.
    pub async fn drop_forks(&self, names: &[String]) -> Result<usize> {
        self.ensure_writable("fork drop")?;
        self.ensure_base("fork drop")?;
        let _meta = self.backend().meta_lock().await?;
        let mut dropped = 0usize;
        for name in names {
            dropped += self.drop_fork_locked(name).await?;
        }
        Ok(dropped)
    }

    /// [`Self::drop_fork`] with the metadata lock already held.
    async fn drop_fork_locked(&self, name: &str) -> Result<usize> {
        let fork = load(self.backend(), name).await?;
        let entries = list_entries(self.backend(), &fork.name).await?;
        let dropped = entries.len();
        for fe in entries {
            // Catalog entry first, storage second: a crash between them leaves
            // an unreferenced table directory, which vacuum's orphan sweep
            // reclaims. The reverse order would leave the fork advertising a
            // table whose data is gone.
            remove_entry(self.backend(), &fork.name, &fe.name).await?;
            self.backend().heads.remove(fe.table_id).await?;
            let objects = self
                .backend()
                .list(&layout::table_prefix(fe.table_id))
                .await?;
            self.backend()
                .delete_many(objects.into_iter().map(|m| m.location).collect())
                .await?;
        }
        delete(self.backend(), name).await?;
        Ok(dropped)
    }

    /// What a fork changed, computed from manifests alone.
    pub async fn fork_diff(&self, name: &str, table: Option<&str>) -> Result<ForkDiff> {
        self.ensure_base("fork diff")?;
        let fork = load(self.backend(), name).await?;
        let entries = match table {
            None => list_entries(self.backend(), &fork.name).await?,
            Some(t) => match load_entry(self.backend(), &fork.name, t).await? {
                Some(e) => vec![e],
                // A name the fork never touched is not an error: it is an
                // empty diff, which is what "did this fork change X?" should
                // answer when it did not.
                None => vec![],
            },
        };
        let mut tables = Vec::with_capacity(entries.len());
        for fe in entries {
            tables.push(self.diff_one(&fork, &fe).await?);
        }
        Ok(ForkDiff {
            fork: fork.name,
            created_at_ns: fork.created_at_ns,
            tables,
        })
    }

    async fn diff_one(&self, fork: &Fork, fe: &ForkTableEntry) -> Result<ForkTableDiff> {
        let fork_head = self.head(&fe.name, fe.table_id).await?.head.sequence;
        let fork_manifest = self.manifest_at(fe.table_id, fork_head).await?;
        let own_prefix = format!("{}/", layout::table_prefix(fe.table_id));

        let Some(origin) = &fe.origin else {
            // Created in the fork: everything in it is new.
            return Ok(ForkTableDiff {
                table: fe.name.clone(),
                kind: ForkTableKind::Created,
                base_sequence: None,
                base_head_sequence: None,
                fork_versions: fork_head,
                rows_base: 0,
                rows_fork: fork_manifest.rows,
                bytes_base: 0,
                bytes_fork: fork_manifest.bytes,
                segments_added: fork_manifest.segments.len(),
                segments_removed: 0,
                segments_shared: 0,
                schema_revision_base: None,
                schema_revision_fork: fork_manifest.schema_revision,
                base_moved: false,
                base_moved_by_compaction_only: false,
            });
        };

        let base_manifest = self
            .manifest_at(origin.base_table_id, origin.base_sequence)
            .await?;
        let base_paths: BTreeSet<&str> = base_manifest
            .segments
            .iter()
            .map(|s| s.path.as_str())
            .collect();
        let fork_paths: BTreeSet<&str> = fork_manifest
            .segments
            .iter()
            .map(|s| s.path.as_str())
            .collect();

        let segments_added = fork_manifest
            .segments
            .iter()
            .filter(|s| is_own_segment(&own_prefix, &s.path))
            .count();
        let segments_shared = fork_paths.intersection(&base_paths).count();
        let segments_removed = base_paths.difference(&fork_paths).count();

        let base_head = self
            .head(
                &fork.pins[&origin.base_table_id].table_name,
                origin.base_table_id,
            )
            .await?
            .head
            .sequence;
        let base_moved = base_head != origin.base_sequence;
        let compaction_only = base_moved
            && self
                .intervening_commits_are_compaction(
                    origin.base_table_id,
                    origin.base_sequence,
                    base_head,
                )
                .await?;

        Ok(ForkTableDiff {
            table: fe.name.clone(),
            kind: ForkTableKind::Shadowed,
            base_sequence: Some(origin.base_sequence),
            base_head_sequence: Some(base_head),
            fork_versions: fork_head,
            rows_base: base_manifest.rows,
            rows_fork: fork_manifest.rows,
            bytes_base: base_manifest.bytes,
            bytes_fork: fork_manifest.bytes,
            segments_added,
            segments_removed,
            segments_shared,
            schema_revision_base: Some(base_manifest.schema_revision),
            schema_revision_fork: fork_manifest.schema_revision,
            base_moved,
            base_moved_by_compaction_only: compaction_only,
        })
    }

    /// Whether every base commit in `(from, to]` was a compaction — i.e. the
    /// base's *contents* did not change, only its physical layout.
    ///
    /// A promote blocked purely by compaction is the one conflict that is
    /// semantically spurious, so it is worth naming in the error rather than
    /// leaving an agent to diff two versions and work it out.
    async fn intervening_commits_are_compaction(
        &self,
        table_id: Uuid,
        from: u64,
        to: u64,
    ) -> Result<bool> {
        if to <= from {
            return Ok(false);
        }
        let floor = self.retention_min_seq(table_id).await?;
        for seq in (from + 1)..=to {
            if seq < floor {
                // History was expired; we cannot prove it was only compaction.
                return Ok(false);
            }
            if self.manifest_at(table_id, seq).await?.op != crate::manifest::OpKind::Compact {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Promote one of a fork's tables into the base database.
    ///
    /// For a shadow this is a compare-and-swap: it succeeds only if the base
    /// is still at the version the fork was created from. First promote wins;
    /// a loser gets `PromoteConflict` and nothing is written. The conflict unit
    /// is the whole table — this is a merge with a coarse conflict granularity,
    /// not the absence of one.
    ///
    /// For a table the fork created, promotion is a catalog move: the table is
    /// already self-contained, so it costs nothing and copies nothing.
    pub async fn promote(&self, fork_name: &str, table: &str) -> Result<PromoteResult> {
        self.ensure_writable("fork promote")?;
        self.ensure_base("fork promote")?;
        let _meta = self.backend().meta_lock().await?;
        let fork = load(self.backend(), fork_name).await?;
        let fe = load_entry(self.backend(), &fork.name, table)
            .await?
            .ok_or_else(|| {
                Error::invalid(format!(
                    "fork {fork_name:?} does not own table {table:?}; only tables a fork \
                     created or wrote to can be promoted"
                ))
            })?;

        let fork_head_state = self.head(&fe.name, fe.table_id).await?;
        let fork_manifest = self
            .manifest_at(fe.table_id, fork_head_state.head.sequence)
            .await?;

        let Some(origin) = fe.origin.clone() else {
            return self.promote_created(&fork, &fe, &fork_manifest).await;
        };

        // --- compare and swap against the base -------------------------
        let base_name = fork.pins[&origin.base_table_id].table_name.clone();
        let base_head_state = self.head(&base_name, origin.base_table_id).await?;
        let base_head = base_head_state.head.sequence;
        if base_head != origin.base_sequence {
            let compaction_only = self
                .intervening_commits_are_compaction(
                    origin.base_table_id,
                    origin.base_sequence,
                    base_head,
                )
                .await?;
            return Err(Error::PromoteConflict {
                fork: fork.name,
                table: table.to_string(),
                base: origin.base_sequence,
                actual: base_head,
                compaction_note: if compaction_only {
                    " (every intervening commit was a compaction, so the base's contents \
                     did not change; re-fork and re-run to promote onto the new layout)"
                        .to_string()
                } else {
                    String::new()
                },
            });
        }

        // --- move the fork's own segments into the base table ----------
        let own_prefix = format!("{}/", layout::table_prefix(fe.table_id));
        let base_paths: BTreeSet<String> = self
            .manifest_at(origin.base_table_id, origin.base_sequence)
            .await?
            .segments
            .iter()
            .map(|s| s.path.clone())
            .collect();
        // Re-check the invariant before writing into the base: this is the
        // other place a cross-table path could enter a manifest, and the base
        // is the one table where a dangling reference is unrecoverable.
        check_refinement(
            &own_prefix,
            &base_paths,
            fork_manifest.segments.iter().map(|s| s.path.as_str()),
        )?;

        let new_sequence = base_head + 1;
        let mut segments = Vec::with_capacity(fork_manifest.segments.len());
        let mut linked = 0usize;
        let mut bytes_copied = 0u64;
        for seg in &fork_manifest.segments {
            if !is_own_segment(&own_prefix, &seg.path) {
                // Inherited: already a base segment at a base path.
                segments.push(seg.clone());
                continue;
            }
            let from = object_store::path::Path::from(seg.path.as_str());
            let to = layout::segment_path(origin.base_table_id, seg.id);
            let copied = self.link_or_copy_segment(&from, &to).await?;
            bytes_copied += copied;
            linked += 1;
            let mut moved = seg.clone();
            moved.path = to.as_ref().to_string();
            // Introduced to the *base* table by this version, which is what
            // makes the commit fsync the newly linked files before HEAD moves.
            moved.created_by_sequence = new_sequence;
            segments.push(moved);
        }

        let mut manifest = fork_manifest.clone();
        manifest.table_id = origin.base_table_id;
        manifest.sequence = new_sequence;
        manifest.op = crate::manifest::OpKind::Promote;
        manifest.execution_mode = Some("direct".to_string());
        manifest.plan_hash = None;
        manifest.segments = segments;
        manifest.note = Some(format!(
            "promoted from fork {:?} (fork version {})",
            fork.name, fork_head_state.head.sequence
        ));
        manifest.user_meta.insert(
            FORKED_FROM_META_KEY.to_string(),
            serde_json::json!({
                "fork": fork.name,
                "fork_table_id": fe.table_id,
                "fork_sequence": fork_head_state.head.sequence,
                "base_sequence": origin.base_sequence,
            }),
        );
        manifest.recompute_rollups();

        let result = self
            .commit_manifest_locked(
                &base_name,
                origin.base_table_id,
                Some(&base_head_state),
                &mut manifest,
                linked,
                CommitInputs::default(),
            )
            .await?;

        Ok(PromoteResult {
            fork: fork.name,
            table: table.to_string(),
            kind: ForkTableKind::Shadowed,
            base_sequence: result.sequence,
            rows: manifest.rows,
            segments_linked: linked,
            bytes_copied,
        })
    }

    /// Promote a table the fork created: it has no base counterpart and every
    /// segment is already under its own prefix, so this is purely a move
    /// between catalogs. No data is touched and no version is created.
    async fn promote_created(
        &self,
        fork: &Fork,
        fe: &ForkTableEntry,
        manifest: &crate::manifest::VersionManifest,
    ) -> Result<PromoteResult> {
        if crate::catalog::load_entry(self.backend(), &fe.name)
            .await?
            .is_some()
        {
            return Err(Error::TableExists {
                name: fe.name.clone(),
            });
        }
        let entry = crate::catalog::CatalogEntry {
            name: fe.name.clone(),
            table_id: fe.table_id,
            created_at_ns: fe.created_at_ns,
            spec_revision: fe.spec_revision,
            checksum: String::new(),
        }
        .seal()?;
        // Global catalog first, then release it from the fork: a crash in
        // between leaves the table reachable from both, never from neither.
        crate::catalog::create_entry(self.backend(), &entry).await?;
        remove_entry(self.backend(), &fork.name, &fe.name).await?;
        Ok(PromoteResult {
            fork: fork.name.clone(),
            table: fe.name.clone(),
            kind: ForkTableKind::Created,
            base_sequence: manifest.sequence,
            rows: manifest.rows,
            segments_linked: 0,
            bytes_copied: 0,
        })
    }

    /// Put a segment at a second path, sharing storage when the backend can.
    ///
    /// Returns the bytes actually copied — zero on a filesystem hardlink,
    /// which is the whole point: promoting a fork's output should not rewrite
    /// its Parquet. Object stores have no link primitive, so there it is a
    /// real copy and the caller reports the cost honestly.
    async fn link_or_copy_segment(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
    ) -> Result<u64> {
        if let Some(root) = &self.backend().local_root {
            let src = root.join(from.as_ref());
            let dst = root.join(to.as_ref());
            if let Some(parent) = dst.parent() {
                std::fs::create_dir_all(parent).map_err(|e| Error::io(parent.display(), e))?;
            }
            match std::fs::hard_link(&src, &dst) {
                Ok(()) => return Ok(0),
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => return Ok(0),
                // Cross-device, a filesystem without links, or a hostile
                // umask: fall through to a copy rather than failing the
                // promote. Slower, never wrong.
                Err(_) => {
                    let bytes =
                        std::fs::copy(&src, &dst).map_err(|e| Error::io(dst.display(), e))?;
                    return Ok(bytes);
                }
            }
        }
        let bytes = self.backend().get(from).await?;
        let len = bytes.len() as u64;
        self.backend().put(to, bytes).await?;
        Ok(len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fork(name: &str) -> Fork {
        let mut pins = BTreeMap::new();
        pins.insert(
            Uuid::nil(),
            ForkPin {
                table_name: "trades".into(),
                sequence: 7,
                manifest_checksum: "abc".into(),
            },
        );
        Fork {
            name: name.into(),
            created_at_ns: 42,
            note: None,
            as_of_ns: None,
            pins,
            user_meta: serde_json::Map::new(),
            checksum: String::new(),
        }
    }

    #[test]
    fn seal_then_verify_round_trips() {
        let sealed = fork("agent-01").seal().unwrap();
        assert!(!sealed.checksum.is_empty());
        assert!(sealed.verify("forks/x.json").is_ok());
    }

    #[test]
    fn verify_detects_a_tampered_pin() {
        // The pin is the GC root: silently moving it is exactly how a fork
        // would come to reference reclaimed segments.
        let mut sealed = fork("agent-01").seal().unwrap();
        sealed.pins.get_mut(&Uuid::nil()).unwrap().sequence = 9;
        assert!(matches!(
            sealed.verify("forks/x.json").unwrap_err(),
            Error::Corruption { .. }
        ));
    }

    #[test]
    fn verify_detects_a_dropped_pin() {
        let mut sealed = fork("agent-01").seal().unwrap();
        sealed.pins.clear();
        assert!(sealed.verify("forks/x.json").is_err());
    }

    #[test]
    fn json_round_trip_preserves_pins_and_verifies() {
        let sealed = fork("agent-01").seal().unwrap();
        let back: Fork = serde_json::from_slice(&serde_json::to_vec(&sealed).unwrap()).unwrap();
        assert_eq!(back.pins, sealed.pins);
        assert!(back.verify("forks/x.json").is_ok());
        assert_eq!(back.pin(Uuid::nil()).unwrap().sequence, 7);
    }

    #[test]
    fn empty_optional_fields_stay_out_of_the_json() {
        let json = serde_json::to_string(&fork("f").seal().unwrap()).unwrap();
        assert!(!json.contains("note"), "{json}");
        assert!(!json.contains("as_of_ns"), "{json}");
        assert!(!json.contains("user_meta"), "{json}");
    }

    #[test]
    fn user_meta_is_covered_by_the_checksum() {
        let mut a = fork("f");
        a.user_meta.insert("run".into(), serde_json::json!("1"));
        let mut b = fork("f");
        b.user_meta.insert("run".into(), serde_json::json!("2"));
        assert_ne!(a.seal().unwrap().checksum, b.seal().unwrap().checksum);
    }

    fn entry(origin: Option<ForkOrigin>) -> ForkTableEntry {
        ForkTableEntry {
            name: "trades".into(),
            table_id: Uuid::nil(),
            created_at_ns: 1,
            spec_revision: 3,
            origin,
            checksum: String::new(),
        }
    }

    fn origin() -> ForkOrigin {
        ForkOrigin {
            base_table_id: Uuid::nil(),
            base_sequence: 5,
            base_manifest_checksum: "def".into(),
        }
    }

    #[test]
    fn fork_entry_seal_verify_and_shadow_flag() {
        let created = entry(None).seal().unwrap();
        assert!(!created.is_shadow());
        assert!(created.verify("e.json").is_ok());

        let shadow = entry(Some(origin())).seal().unwrap();
        assert!(shadow.is_shadow());
        assert!(shadow.verify("e.json").is_ok());
        // A created table and a shadow must not checksum alike, or a shadow's
        // promote base could be stripped without detection.
        assert_ne!(created.checksum, shadow.checksum);
    }

    #[test]
    fn fork_entry_verify_detects_a_moved_promote_base() {
        let mut shadow = entry(Some(origin())).seal().unwrap();
        shadow.origin.as_mut().unwrap().base_sequence = 6;
        assert!(shadow.verify("e.json").is_err());
    }

    #[test]
    fn to_catalog_entry_produces_a_self_consistent_entry() {
        let shadow = entry(Some(origin())).seal().unwrap();
        let ce = shadow.to_catalog_entry().unwrap();
        assert_eq!(ce.name, "trades");
        assert_eq!(ce.table_id, shadow.table_id);
        assert_eq!(ce.spec_revision, 3);
        // Must verify on its own terms — downstream code checks it.
        assert!(ce.verify("catalog.json").is_ok());
    }

    #[test]
    fn refinement_accepts_own_and_inherited_segments() {
        let base: BTreeSet<String> = ["tables/base/segments/a.parquet".to_string()]
            .into_iter()
            .collect();
        let own = "tables/shadow/";
        check_refinement(
            own,
            &base,
            [
                "tables/shadow/segments/new.parquet",
                "tables/base/segments/a.parquet",
            ],
        )
        .unwrap();
    }

    #[test]
    fn refinement_rejects_a_foreign_segment() {
        let base: BTreeSet<String> = ["tables/base/segments/a.parquet".to_string()]
            .into_iter()
            .collect();
        // A base segment that is NOT in the pinned manifest: it may already be
        // below the retention floor, so nothing keeps it alive.
        let err = check_refinement(
            "tables/shadow/",
            &base,
            ["tables/base/segments/unpinned.parquet"],
        )
        .unwrap_err();
        assert!(matches!(err, Error::Corruption { .. }));
        assert!(format!("{err}").contains("refinement invariant"));
    }

    #[test]
    fn refinement_rejects_another_forks_segment() {
        let base: BTreeSet<String> = BTreeSet::new();
        assert!(
            check_refinement(
                "tables/shadow/",
                &base,
                ["tables/other-fork/segments/x.parquet"]
            )
            .is_err()
        );
    }

    #[test]
    fn refinement_accepts_an_empty_segment_list() {
        // An empty table is a legitimate fork state (delete-range to zero).
        check_refinement("tables/shadow/", &BTreeSet::new(), Vec::<String>::new()).unwrap();
    }

    #[test]
    fn own_segment_test_is_prefix_based() {
        assert!(is_own_segment(
            "tables/abc/",
            "tables/abc/segments/x.parquet"
        ));
        assert!(!is_own_segment(
            "tables/abc/",
            "tables/def/segments/x.parquet"
        ));
        // Guard against a prefix that is a string prefix of another uuid: the
        // trailing slash is what makes this safe, so assert it stays.
        assert!(!is_own_segment(
            "tables/abc/",
            "tables/abcdef/segments/x.parquet"
        ));
    }
}
