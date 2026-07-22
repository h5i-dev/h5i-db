//! Table catalog: name → table UUID indirection.
//!
//! One JSON object per table under `catalog/tables/<hash-of-name>.json`.
//! The raw name lives inside the JSON; renames are a catalog edit only.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{Error, Result};
use crate::layout;
use crate::Backend;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogEntry {
    pub name: String,
    pub table_id: Uuid,
    pub created_at_ns: i64,
    /// Current spec revision (spec files are immutable per revision).
    pub spec_revision: u32,
    #[serde(default)]
    pub checksum: String,
}

impl CatalogEntry {
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
        let expected = self.compute_checksum()?;
        if self.checksum != expected {
            return Err(Error::corruption(object, "catalog entry checksum mismatch"));
        }
        Ok(())
    }
}

pub async fn load_entry(backend: &Backend, name: &str) -> Result<Option<CatalogEntry>> {
    let path = layout::catalog_entry_path(name);
    match backend.get_opt(&path).await? {
        None => Ok(None),
        Some(bytes) => {
            let entry: CatalogEntry = serde_json::from_slice(&bytes)
                .map_err(|e| Error::corruption(path.as_ref(), format!("catalog parse: {e}")))?;
            entry.verify(path.as_ref())?;
            if entry.name != name {
                return Err(Error::corruption(
                    path.as_ref(),
                    format!(
                        "catalog entry name {:?} does not match requested name {:?} (hash collision or tampering)",
                        entry.name, name
                    ),
                ));
            }
            Ok(Some(entry))
        }
    }
}

pub async fn store_entry(backend: &Backend, entry: &CatalogEntry) -> Result<()> {
    let path = layout::catalog_entry_path(&entry.name);
    let bytes = serde_json::to_vec_pretty(entry)?;
    backend.put(&path, bytes.into()).await?;
    backend.sync_objects(&[path]).await
}

pub async fn remove_entry(backend: &Backend, name: &str) -> Result<()> {
    backend.delete(&layout::catalog_entry_path(name)).await
}

/// List all catalog entries (names are read from inside the JSON objects).
pub async fn list_entries(backend: &Backend) -> Result<Vec<CatalogEntry>> {
    let metas = backend
        .list(&object_store::path::Path::from(layout::CATALOG_PREFIX))
        .await?;
    let mut out = Vec::with_capacity(metas.len());
    for meta in metas {
        let bytes = backend.get(&meta.location).await?;
        let entry: CatalogEntry = serde_json::from_slice(&bytes).map_err(|e| {
            Error::corruption(meta.location.as_ref(), format!("catalog parse: {e}"))
        })?;
        entry.verify(meta.location.as_ref())?;
        out.push(entry);
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(out)
}
