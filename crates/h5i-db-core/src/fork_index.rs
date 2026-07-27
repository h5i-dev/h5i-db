//! The fork visibility index (ROADMAP Part X, X-A1).
//!
//! Three operations need to know what forks hold: the retention floor (may it
//! rise?), `drop_table` (is this table pinned?), and `vacuum` (which table
//! directories are reachable?). Each answered the question by reading every
//! fork object and every fork catalog entry, so all three were O(#forks) in
//! round trips. That is fine at ten forks and wrong at the thousand-branch
//! scale agentic exploration produces.
//!
//! This module answers the same questions from **one object**.
//!
//! # It is a cache, and correctness does not depend on maintaining it
//!
//! The index is never written by the operations that change forks. Nothing
//! calls "update the index"; there is no site to forget. Instead every read
//! revalidates it against storage, and the revalidation is cheap because it
//! uses *listings* rather than reads:
//!
//! - `forks/` and `catalog/forks/` are listed (two requests, whatever the fork
//!   count).
//! - An index entry is reused when its object is still present at the same
//!   size; anything present-but-unmatched is read, anything absent is dropped.
//! - The result is stored back only if it differs from what was there.
//!
//! So a stale index cannot survive a read, an index for forks that no longer
//! exist cannot outlive them, and a rebuild costs one read *per changed fork*
//! rather than per fork. Deleting `FORK_INDEX.json` is always safe; corrupting
//! it costs a rebuild ([`load_opt`] treats an unreadable index as absent).
//!
//! # Which way the danger runs
//!
//! Every consumer is protected against the index reporting **too much**: an
//! extra pin refuses a drop or holds a retention floor down, which is visible
//! and recoverable. Reporting **too little** is the direction that loses data —
//! a missed pin lets the floor rise past a live fork's version and the next
//! vacuum deletes segments it still reads. That asymmetry is why revalidation
//! is listing-based and unconditional rather than a generation counter someone
//! has to remember to bump, and why a corrupt *fork* (as opposed to a corrupt
//! index) fails the rebuild instead of being skipped.

use std::collections::{BTreeMap, BTreeSet};

use futures::stream::{StreamExt, TryStreamExt};
use object_store::path::Path as ObjPath;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::Backend;
use crate::error::{Error, Result};
use crate::layout;

/// A table a fork owns outright: a copy-on-write shadow, or one created inside
/// the fork. Absent from the global catalog, and therefore invisible to
/// vacuum's orphan sweep unless the sweep consults this index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexedTable {
    pub table_id: Uuid,
    /// Size of the fork catalog entry when this row was built. Together with
    /// the object's continued existence this is the reuse test: same path,
    /// same size, reuse; anything else is re-read.
    pub object_size: u64,
}

/// One fork, as the index sees it. Deliberately not a `Fork`: the index needs
/// pinned *sequences* and owned *table ids*, and copying the rest (notes,
/// checksums, user metadata) would make a large database's index large for no
/// consumer's benefit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexedFork {
    /// Size of `forks/<hash>.json` when this entry was built.
    pub object_size: u64,
    /// Base table id → pinned sequence.
    pub pins: BTreeMap<Uuid, u64>,
    /// Table name → the fork-owned table backing it.
    pub owned: BTreeMap<String, IndexedTable>,
}

/// The whole index: every fork, by name.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ForkIndex {
    pub built_at_ns: i64,
    pub forks: BTreeMap<String, IndexedFork>,
    #[serde(default)]
    pub checksum: String,
}

impl ForkIndex {
    fn compute_checksum(&self) -> Result<String> {
        let mut clone = self.clone();
        clone.checksum = String::new();
        Ok(crate::util::checksum_hex(&serde_json::to_vec(&clone)?))
    }

    pub fn seal(mut self) -> Result<Self> {
        self.checksum = self.compute_checksum()?;
        Ok(self)
    }

    fn is_intact(&self) -> bool {
        matches!(self.compute_checksum(), Ok(c) if c == self.checksum)
    }

    pub fn is_empty(&self) -> bool {
        self.forks.is_empty()
    }

    // -- consumer queries ------------------------------------------------
    //
    // These iterate the in-memory map rather than a precomputed inversion.
    // Each consumer calls one of them once per operation, so the inversion
    // would cost more to build than the scan it saves.

    /// The first fork (in name order) pinning `table_id` below `target`.
    ///
    /// Name order, and "first" rather than "lowest", so the message a user
    /// sees is the same one the pre-index full scan produced.
    pub fn first_pin_below(&self, table_id: Uuid, target: u64) -> Option<(&str, u64)> {
        self.forks.iter().find_map(|(name, f)| {
            f.pins
                .get(&table_id)
                .filter(|seq| **seq < target)
                .map(|seq| (name.as_str(), *seq))
        })
    }

    /// The first fork (in name order) pinning `table_id` at any version.
    pub fn first_pin(&self, table_id: Uuid) -> Option<(&str, u64)> {
        self.forks
            .iter()
            .find_map(|(name, f)| f.pins.get(&table_id).map(|seq| (name.as_str(), *seq)))
    }

    /// Every base table any fork pins — vacuum's "do not collect" root set.
    pub fn pinned_table_ids(&self) -> BTreeSet<Uuid> {
        self.forks
            .values()
            .flat_map(|f| f.pins.keys().copied())
            .collect()
    }

    /// Every table owned by a fork — the ids vacuum's orphan sweep would
    /// otherwise not find in any catalog.
    pub fn owned_table_ids(&self) -> BTreeSet<Uuid> {
        self.forks
            .values()
            .flat_map(|f| f.owned.values().map(|t| t.table_id))
            .collect()
    }

    /// Forks pinning `table_id`, with their pinned sequence, in name order.
    pub fn pins_of(&self, table_id: Uuid) -> Vec<(&str, u64)> {
        self.forks
            .iter()
            .filter_map(|(name, f)| f.pins.get(&table_id).map(|s| (name.as_str(), *s)))
            .collect()
    }
}

// ---------------------------------------------------------------------------
// storage
// ---------------------------------------------------------------------------

/// Read the stored index, or `None` when there is none *or it is unusable*.
///
/// Parse failures and checksum mismatches deliberately read as absent rather
/// than as errors: this object is a cache, and the recovery for a damaged
/// cache is to rebuild it, not to take the database down.
pub async fn load_opt(backend: &Backend) -> Result<Option<ForkIndex>> {
    let Some(bytes) = backend.get_opt(&layout::fork_index_path()).await? else {
        return Ok(None);
    };
    let Ok(index) = serde_json::from_slice::<ForkIndex>(&bytes) else {
        return Ok(None);
    };
    if !index.is_intact() {
        return Ok(None);
    }
    Ok(Some(index))
}

/// Compact, not pretty: this is the one metadata object whose size scales with
/// fork count, and nothing reads it by eye.
async fn store(backend: &Backend, index: &ForkIndex) -> Result<()> {
    let path = layout::fork_index_path();
    backend
        .put(&path, serde_json::to_vec(index)?.into())
        .await?;
    backend.sync_objects(&[path]).await
}

/// Drop the index object. Safe at any time; the next read rebuilds.
pub async fn invalidate(backend: &Backend) -> Result<()> {
    backend.delete(&layout::fork_index_path()).await
}

// ---------------------------------------------------------------------------
// refresh
// ---------------------------------------------------------------------------

/// The index, revalidated against storage and written back when it changed.
///
/// `writable` is the caller's permission to persist, not permission to be
/// correct: a read-only handle gets the same answers, it just cannot leave the
/// rebuilt index behind for the next reader.
pub async fn refresh(backend: &Backend, writable: bool) -> Result<ForkIndex> {
    let prior = load_opt(backend).await?;
    let rebuilt = rebuild(backend, prior.as_ref()).await?;

    if let Some(p) = &prior
        && p.forks == rebuilt.forks
    {
        return Ok(prior.expect("checked above"));
    }

    let sealed = rebuilt.seal()?;
    if writable {
        if sealed.forks.is_empty() {
            // A fork-free database keeps no index object at all, so enabling
            // this feature does not add a file to databases that never fork.
            if prior.is_some() {
                invalidate(backend).await?;
            }
        } else {
            store(backend, &sealed).await?;
        }
    }
    Ok(sealed)
}

/// Rebuild from listings, reading only the fork objects and catalog entries
/// that `prior` cannot account for.
async fn rebuild(backend: &Backend, prior: Option<&ForkIndex>) -> Result<ForkIndex> {
    let fork_metas = backend.list(&ObjPath::from(layout::FORK_PREFIX)).await?;
    let catalog_metas = backend
        .list(&ObjPath::from(layout::FORK_CATALOG_PREFIX))
        .await?;

    // -- pins ------------------------------------------------------------
    let mut prior_forks_by_path: BTreeMap<String, (&str, &IndexedFork)> = BTreeMap::new();
    if let Some(p) = prior {
        for (name, entry) in &p.forks {
            prior_forks_by_path.insert(
                layout::fork_path(name).as_ref().to_string(),
                (name.as_str(), entry),
            );
        }
    }

    let mut forks: BTreeMap<String, IndexedFork> = BTreeMap::new();
    let mut unknown: Vec<(ObjPath, u64)> = Vec::new();
    for meta in &fork_metas {
        match prior_forks_by_path.get(meta.location.as_ref()) {
            Some((name, entry)) if entry.object_size == meta.size => {
                forks.insert(
                    (*name).to_string(),
                    IndexedFork {
                        object_size: entry.object_size,
                        pins: entry.pins.clone(),
                        // Owned tables are rebuilt from the catalog listing
                        // below; carrying them here would double-count.
                        owned: BTreeMap::new(),
                    },
                );
            }
            _ => unknown.push((meta.location.clone(), meta.size)),
        }
    }
    for (name, size, pins) in read_forks(backend, unknown).await? {
        forks.insert(
            name,
            IndexedFork {
                object_size: size,
                pins,
                owned: BTreeMap::new(),
            },
        );
    }

    // -- owned tables ----------------------------------------------------
    // Fork catalog entries live at `catalog/forks/<hash(fork)>/<hash(table)>`,
    // so the directory component identifies the fork without reading anything.
    let by_hash: BTreeMap<String, String> = forks
        .keys()
        .map(|name| (layout::hash_name(name), name.clone()))
        .collect();

    let mut prior_owned_by_path: BTreeMap<String, (&str, &str, &IndexedTable)> = BTreeMap::new();
    if let Some(p) = prior {
        for (fork_name, entry) in &p.forks {
            for (table_name, owned) in &entry.owned {
                prior_owned_by_path.insert(
                    layout::fork_catalog_entry_path(fork_name, table_name)
                        .as_ref()
                        .to_string(),
                    (fork_name.as_str(), table_name.as_str(), owned),
                );
            }
        }
    }

    let mut unknown_entries: Vec<(ObjPath, String, u64)> = Vec::new();
    for meta in &catalog_metas {
        let Some(fork_hash) = meta.location.parts().nth(2) else {
            continue;
        };
        // A catalog entry whose fork object is gone belongs to no live fork.
        // The pre-index code reached these entries only by iterating existing
        // forks, so it never saw them either; skipping keeps the answer the
        // same rather than newly protecting orphans.
        let Some(fork_name) = by_hash.get(fork_hash.as_ref()) else {
            continue;
        };
        match prior_owned_by_path.get(meta.location.as_ref()) {
            Some((_, table_name, owned)) if owned.object_size == meta.size => {
                forks
                    .get_mut(fork_name)
                    .expect("fork name came from this map")
                    .owned
                    .insert((*table_name).to_string(), (*owned).clone());
            }
            _ => unknown_entries.push((meta.location.clone(), fork_name.clone(), meta.size)),
        }
    }
    for (fork_name, table_name, owned) in read_entries(backend, unknown_entries).await? {
        forks
            .get_mut(&fork_name)
            .expect("fork name came from this map")
            .owned
            .insert(table_name, owned);
    }

    Ok(ForkIndex {
        built_at_ns: crate::util::monotonic_commit_ts(None),
        forks,
        checksum: String::new(),
    })
}

/// Read fork objects concurrently, keeping the checksum verification: these
/// are GC roots, and a damaged one must fail the rebuild rather than quietly
/// contribute no pins.
async fn read_forks(
    backend: &Backend,
    paths: Vec<(ObjPath, u64)>,
) -> Result<Vec<(String, u64, BTreeMap<Uuid, u64>)>> {
    futures::stream::iter(paths)
        .map(|(path, size)| async move {
            let bytes = backend.get(&path).await?;
            let fork: crate::fork::Fork = serde_json::from_slice(&bytes)
                .map_err(|e| Error::corruption(path.as_ref(), format!("fork parse: {e}")))?;
            fork.verify(path.as_ref())?;
            let pins = fork.pins.iter().map(|(id, p)| (*id, p.sequence)).collect();
            Ok::<_, Error>((fork.name, size, pins))
        })
        .buffer_unordered(crate::backend::METADATA_FETCH_CONCURRENCY)
        .try_collect()
        .await
}

async fn read_entries(
    backend: &Backend,
    paths: Vec<(ObjPath, String, u64)>,
) -> Result<Vec<(String, String, IndexedTable)>> {
    futures::stream::iter(paths)
        .map(|(path, fork_name, size)| async move {
            let bytes = backend.get(&path).await?;
            let entry: crate::fork::ForkTableEntry =
                serde_json::from_slice(&bytes).map_err(|e| {
                    Error::corruption(path.as_ref(), format!("fork catalog parse: {e}"))
                })?;
            entry.verify(path.as_ref())?;
            Ok::<_, Error>((
                fork_name,
                entry.name,
                IndexedTable {
                    table_id: entry.table_id,
                    object_size: size,
                },
            ))
        })
        .buffer_unordered(crate::backend::METADATA_FETCH_CONCURRENCY)
        .try_collect()
        .await
}

impl crate::database::Database {
    /// The fork visibility index for this database, revalidated against
    /// storage. Always call this rather than caching an index across
    /// operations: revalidation is what makes it safe to trust.
    pub(crate) async fn fork_index(&self) -> Result<ForkIndex> {
        refresh(self.backend(), !self.is_read_only()).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn indexed(pins: &[(Uuid, u64)], owned: &[(&str, Uuid)]) -> IndexedFork {
        IndexedFork {
            object_size: 100,
            pins: pins.iter().copied().collect(),
            owned: owned
                .iter()
                .map(|(name, id)| {
                    (
                        (*name).to_string(),
                        IndexedTable {
                            table_id: *id,
                            object_size: 50,
                        },
                    )
                })
                .collect(),
        }
    }

    fn index(entries: &[(&str, IndexedFork)]) -> ForkIndex {
        ForkIndex {
            built_at_ns: 1,
            forks: entries
                .iter()
                .map(|(n, f)| ((*n).to_string(), f.clone()))
                .collect(),
            checksum: String::new(),
        }
    }

    #[test]
    fn seal_then_intact_round_trips() {
        let t = Uuid::new_v4();
        let sealed = index(&[("a", indexed(&[(t, 3)], &[]))]).seal().unwrap();
        assert!(!sealed.checksum.is_empty());
        assert!(sealed.is_intact());
    }

    #[test]
    fn tampering_is_detected_so_the_index_is_rebuilt_not_trusted() {
        let t = Uuid::new_v4();
        let mut sealed = index(&[("a", indexed(&[(t, 3)], &[]))]).seal().unwrap();
        // Lowering a pin is exactly the edit that would let a retention floor
        // rise past a live fork.
        sealed.forks.get_mut("a").unwrap().pins.insert(t, 1);
        assert!(!sealed.is_intact());
    }

    #[test]
    fn first_pin_below_reports_in_name_order() {
        let t = Uuid::new_v4();
        let idx = index(&[
            ("zeta", indexed(&[(t, 2)], &[])),
            ("alpha", indexed(&[(t, 4)], &[])),
        ]);
        // Both pin below 5; name order decides, matching the full-scan
        // behaviour this replaced (fork::list sorts by name).
        assert_eq!(idx.first_pin_below(t, 5), Some(("alpha", 4)));
        // Only zeta is below 3.
        assert_eq!(idx.first_pin_below(t, 3), Some(("zeta", 2)));
        assert_eq!(idx.first_pin_below(t, 2), None);
    }

    #[test]
    fn first_pin_ignores_the_sequence() {
        let t = Uuid::new_v4();
        let idx = index(&[("f", indexed(&[(t, 0)], &[]))]);
        assert_eq!(idx.first_pin(t), Some(("f", 0)));
        assert_eq!(idx.first_pin(Uuid::new_v4()), None);
    }

    #[test]
    fn root_sets_union_across_forks() {
        let (t1, t2, o1, o2) = (
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
        );
        let idx = index(&[
            ("a", indexed(&[(t1, 1)], &[("trades", o1)])),
            ("b", indexed(&[(t1, 2), (t2, 1)], &[("quotes", o2)])),
        ]);
        assert_eq!(idx.pinned_table_ids(), [t1, t2].into_iter().collect());
        assert_eq!(idx.owned_table_ids(), [o1, o2].into_iter().collect());
        assert_eq!(idx.pins_of(t1), vec![("a", 1), ("b", 2)]);
        assert_eq!(idx.pins_of(t2), vec![("b", 1)]);
    }

    #[test]
    fn an_empty_index_answers_nothing_rather_than_erroring() {
        let idx = ForkIndex::default();
        assert!(idx.is_empty());
        assert_eq!(idx.first_pin(Uuid::new_v4()), None);
        assert!(idx.pinned_table_ids().is_empty());
        assert!(idx.owned_table_ids().is_empty());
    }

    #[test]
    fn json_round_trip_preserves_pins_and_owned_tables() {
        let (t, o) = (Uuid::new_v4(), Uuid::new_v4());
        let sealed = index(&[("f", indexed(&[(t, 9)], &[("trades", o)]))])
            .seal()
            .unwrap();
        let back: ForkIndex =
            serde_json::from_slice(&serde_json::to_vec(&sealed).unwrap()).unwrap();
        assert!(back.is_intact());
        assert_eq!(back.first_pin(t), Some(("f", 9)));
        assert_eq!(back.owned_table_ids(), [o].into_iter().collect());
    }
}
