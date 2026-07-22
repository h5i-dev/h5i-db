//! Version retention / GC (ROADMAP 3.3).
//!
//! Without retention every historical version pins its segments forever:
//! storage grows without bound, compliance deletion is impossible, and
//! vacuum's reachability walk is O(all versions). Naive deletion of old
//! manifests would break the system, because:
//!
//! - `as_of` binary-searches directly-addressed sequences `0..=head`, so a
//!   missing low manifest turns time travel into `Corruption`;
//! - the parent-checksum chain assumes every ancestor exists;
//! - snapshots may pin arbitrary old versions.
//!
//! The fix is a **retention floor**: a per-table, monotonically increasing
//! `min_retained_sequence` stored in `tables/<uuid>/RETENTION.json`. The
//! floor is the anchor of the retained chain:
//!
//! - `resolve`/`as_of`/`list_versions` clamp to the floor and report expired
//!   versions as `VersionNotFound` with a retention hint;
//! - `verify` walks the checksum chain from HEAD down to the floor and stops
//!   there (the floor manifest is the trust anchor — its parent may be gone);
//! - `vacuum` computes segment reachability from `floor..=head` (plus plans
//!   and staging leases) and collects manifests below the floor as debris.
//!
//! Raising the floor never deletes data by itself — it makes versions below
//! it *unreachable*; the next `vacuum --apply` reclaims their storage. The
//! floor refuses to rise above any snapshot-pinned sequence (delete the
//! snapshot first), and it never decreases.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::database::Database;
use crate::error::{Error, Result};
use crate::snapshot;

/// Path of the per-table retention floor file.
pub(crate) fn retention_path(table_id: Uuid) -> object_store::path::Path {
    object_store::path::Path::from(format!("tables/{table_id}/RETENTION.json"))
}

/// Persisted retention floor. Self-checksummed like every other metadata
/// object (torn-write guard).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionFloor {
    pub table_id: Uuid,
    /// Versions below this sequence are expired: unreachable for reads and
    /// collectible by vacuum. The floor version itself is always retained.
    pub min_retained_sequence: u64,
    pub set_at_ns: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    #[serde(default)]
    pub checksum: String,
}

impl RetentionFloor {
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
            return Err(Error::corruption(object, "retention floor checksum mismatch"));
        }
        Ok(())
    }
}

/// How to choose the new floor.
#[derive(Debug, Clone)]
pub enum RetentionCut {
    /// Keep the newest `n` versions (floor = head + 1 - n, clamped at 0).
    KeepLast(u64),
    /// Expire all versions strictly below this sequence.
    BeforeSequence(u64),
    /// Expire all versions whose commit time is strictly before this
    /// wall-clock time (ns since epoch); the floor lands on the oldest
    /// version committed at or after it.
    BeforeTimestamp(i64),
}

impl Database {
    /// Current retention floor for a table, if one was ever set.
    pub async fn retention(&self, name: &str) -> Result<Option<RetentionFloor>> {
        let entry = crate::catalog::load_entry(self.backend(), name)
            .await?
            .ok_or_else(|| Error::TableNotFound { name: name.into() })?;
        self.retention_floor(entry.table_id).await
    }

    pub(crate) async fn retention_floor(&self, table_id: Uuid) -> Result<Option<RetentionFloor>> {
        let path = retention_path(table_id);
        match self.backend().get_opt(&path).await? {
            None => Ok(None),
            Some(bytes) => {
                let floor: RetentionFloor = serde_json::from_slice(&bytes).map_err(|e| {
                    Error::corruption(path.as_ref(), format!("retention parse: {e}"))
                })?;
                floor.verify(path.as_ref())?;
                Ok(Some(floor))
            }
        }
    }

    /// Effective minimum retained sequence (0 when no floor is set).
    pub(crate) async fn retention_min_seq(&self, table_id: Uuid) -> Result<u64> {
        Ok(self
            .retention_floor(table_id)
            .await?
            .map(|f| f.min_retained_sequence)
            .unwrap_or(0))
    }

    /// Raise a table's retention floor. Returns the stored floor.
    ///
    /// Refuses to expire the head version, any snapshot-pinned version, and
    /// never lowers an existing floor. Storage is reclaimed by the next
    /// `vacuum(apply=true)` — this call only moves the reachability anchor,
    /// so it is itself cheap and crash-safe (the floor file is the only
    /// write).
    pub async fn set_retention(
        &self,
        name: &str,
        cut: RetentionCut,
        note: Option<String>,
    ) -> Result<RetentionFloor> {
        if self.is_read_only() {
            return Err(Error::ReadOnly {
                op: "set_retention".into(),
            });
        }
        // Serialized with other metadata mutations; also keeps the
        // snapshot-pin check race-free against concurrent snapshot creation.
        let _meta = self.backend().meta_lock().await?;

        let resolved = self.resolve(name, crate::database::ReadAt::Latest).await?;
        let table_id = resolved.entry.table_id;
        let head_seq = resolved.head_sequence;

        let target = match cut {
            RetentionCut::KeepLast(n) => {
                if n == 0 {
                    return Err(Error::invalid("keep_last must retain at least 1 version"));
                }
                head_seq.saturating_sub(n - 1)
            }
            RetentionCut::BeforeSequence(s) => s,
            RetentionCut::BeforeTimestamp(ts) => {
                // Oldest retained = first version committed at/after ts.
                // Walk down from head until the commit time drops below ts.
                let current_floor = self.retention_min_seq(table_id).await?;
                let mut oldest_kept = head_seq;
                while oldest_kept > current_floor {
                    let m = self.manifest_at(table_id, oldest_kept - 1).await?;
                    if m.committed_at_ns < ts {
                        break;
                    }
                    oldest_kept -= 1;
                }
                oldest_kept
            }
        };
        // The head version is always retained.
        let target = target.min(head_seq);

        let current = self.retention_min_seq(table_id).await?;
        if target < current {
            return Err(Error::invalid(format!(
                "retention floor never decreases (current {current}, requested {target})"
            )));
        }

        // Snapshot pins win over retention: refuse to expire pinned versions.
        for snap in snapshot::list(self.backend()).await? {
            if let Some(se) = snap.entries.get(&table_id) {
                if se.sequence < target {
                    return Err(Error::invalid(format!(
                        "snapshot {:?} pins version {} of table {name:?}, below the requested \
                         floor {target}; delete the snapshot first",
                        snap.name, se.sequence
                    )));
                }
            }
        }

        let floor = RetentionFloor {
            table_id,
            min_retained_sequence: target,
            set_at_ns: crate::util::monotonic_commit_ts(None),
            note,
            checksum: String::new(),
        }
        .seal()?;
        let path = retention_path(table_id);
        self.backend()
            .put(&path, serde_json::to_vec_pretty(&floor)?.into())
            .await?;
        self.backend().sync_objects(&[path]).await?;
        Ok(floor)
    }
}
