//! Named snapshots: an immutable map {table UUID → exact version} acting as
//! an extra GC root. Creating one is a reproducibility pin, not a claim of a
//! globally atomic multi-table write.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{Error, Result};
use crate::layout;
use crate::Backend;

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
    let bytes = backend
        .get_opt(&path)
        .await?
        .ok_or_else(|| Error::SnapshotNotFound { name: name.into() })?;
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
    if backend.get_opt(&path).await?.is_some() {
        return Err(Error::invalid(format!(
            "snapshot {:?} already exists; snapshots are immutable — pick a new name or delete it first",
            snapshot.name
        )));
    }
    let bytes = serde_json::to_vec_pretty(snapshot)?;
    backend.put(&path, bytes.into()).await?;
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
    let mut out = Vec::with_capacity(metas.len());
    for meta in metas {
        let bytes = backend.get(&meta.location).await?;
        let snap: Snapshot = serde_json::from_slice(&bytes).map_err(|e| {
            Error::corruption(meta.location.as_ref(), format!("snapshot parse: {e}"))
        })?;
        snap.verify(meta.location.as_ref())?;
        out.push(snap);
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(out)
}
