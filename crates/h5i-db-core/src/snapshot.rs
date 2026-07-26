//! Named snapshots: an immutable map {table UUID → exact version} acting as
//! an extra GC root. Creating one is a reproducibility pin, not a claim of a
//! globally atomic multi-table write.

use std::collections::BTreeMap;

use futures::stream::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::Backend;
use crate::error::{Error, Result};
use crate::layout;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotEntry {
    pub table_name: String,
    pub sequence: u64,
    pub manifest_checksum: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub name: String,
    pub created_at_ns: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    /// table UUID → pinned version.
    pub entries: BTreeMap<Uuid, SnapshotEntry>,
    #[serde(default)]
    pub checksum: String,
}

impl Snapshot {
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
            return Err(Error::corruption(object, "snapshot checksum mismatch"));
        }
        Ok(())
    }
}

pub async fn load(backend: &Backend, name: &str) -> Result<Snapshot> {
    let path = layout::snapshot_path(name);
    let bytes = match backend.get_opt(&path).await? {
        Some(bytes) => bytes,
        None => {
            // Miss only: list the snapshots to offer a name suggestion.
            let existing = list(backend).await.unwrap_or_default();
            return Err(Error::snapshot_not_found_among(
                name,
                existing.iter().map(|s| s.name.as_str()),
            ));
        }
    };
    let snap: Snapshot = serde_json::from_slice(&bytes)
        .map_err(|e| Error::corruption(path.as_ref(), format!("snapshot parse: {e}")))?;
    snap.verify(path.as_ref())?;
    if snap.name != name {
        return Err(Error::corruption(
            path.as_ref(),
            format!("snapshot name {:?} != requested {:?}", snap.name, name),
        ));
    }
    Ok(snap)
}

pub async fn store(backend: &Backend, snapshot: &Snapshot) -> Result<()> {
    let path = layout::snapshot_path(&snapshot.name);
    let bytes = serde_json::to_vec_pretty(snapshot)?;
    // Atomic create-if-absent: two racing creators of the same name cannot
    // both succeed (snapshots are immutable, so overwrite is never right).
    if !backend.put_if_absent(&path, bytes.into()).await? {
        return Err(Error::invalid(format!(
            "snapshot {:?} already exists; snapshots are immutable — pick a new name or delete it first",
            snapshot.name
        )));
    }
    backend.sync_objects(&[path]).await
}

pub async fn delete(backend: &Backend, name: &str) -> Result<()> {
    // Ensure it exists so the CLI can report not-found precisely.
    let _ = load(backend, name).await?;
    backend.delete(&layout::snapshot_path(name)).await
}

pub async fn list(backend: &Backend) -> Result<Vec<Snapshot>> {
    let metas = backend
        .list(&object_store::path::Path::from(layout::SNAPSHOT_PREFIX))
        .await?;
    // Concurrent for the same reason as the table catalog: independent small
    // objects, and this runs on every retention change and every drop.
    let mut out: Vec<Snapshot> = futures::stream::iter(metas)
        .map(|meta| async move {
            let bytes = backend.get(&meta.location).await?;
            let snap: Snapshot = serde_json::from_slice(&bytes).map_err(|e| {
                Error::corruption(meta.location.as_ref(), format!("snapshot parse: {e}"))
            })?;
            snap.verify(meta.location.as_ref())?;
            Ok::<_, Error>(snap)
        })
        .buffer_unordered(crate::backend::METADATA_FETCH_CONCURRENCY)
        .try_collect()
        .await?;
    out.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(out)
}
