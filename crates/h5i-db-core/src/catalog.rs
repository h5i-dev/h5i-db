//! Table catalog: name → table UUID indirection.
//!
//! One JSON object per table under `catalog/tables/<hash-of-name>.json`.
//! The raw name lives inside the JSON; renames are a catalog edit only.

use futures::stream::{self, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::Backend;
use crate::error::{Error, Result};
use crate::layout;

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

/// Store a catalog entry, overwriting any existing object (update semantics:
/// callers must hold the database metadata lock; see `Backend::meta_lock`).
///
/// **No in-tree caller.** Every catalog write in this crate goes through
/// [`create_entry`] or [`remove_entry`], because "create if absent" is what
/// makes two racing writers safe without the lock. This is kept as the
/// supported overwrite path for out-of-tree callers (an entry edited in place,
/// such as a spec-revision bump), and it is deliberately the only one: an
/// unlocked overwrite here would lose a concurrent create.
pub async fn store_entry(backend: &Backend, entry: &CatalogEntry) -> Result<()> {
    let path = layout::catalog_entry_path(&entry.name);
    let bytes = serde_json::to_vec_pretty(entry)?;
    backend.put(&path, bytes.into()).await?;
    backend.sync_objects(&[path]).await
}

/// Store a NEW catalog entry with atomic create-if-absent semantics; fails
/// with `TableExists` when the name is already cataloged. This closes the
/// check-then-put race at the storage layer even without the metadata lock.
pub async fn create_entry(backend: &Backend, entry: &CatalogEntry) -> Result<()> {
    let path = create_entry_unsynced(backend, entry).await?;
    backend.sync_objects(&[path]).await
}

/// Write a catalog entry without the durability barrier, returning the path the
/// caller must include in its own.
///
/// See [`crate::fork::create_entry_unsynced`] for why batching the barrier over
/// several entries is no weaker than one barrier each.
pub(crate) async fn create_entry_unsynced(
    backend: &Backend,
    entry: &CatalogEntry,
) -> Result<object_store::path::Path> {
    let path = layout::catalog_entry_path(&entry.name);
    let bytes = serde_json::to_vec_pretty(entry)?;
    if !backend.put_if_absent(&path, bytes.into()).await? {
        return Err(Error::TableExists {
            name: entry.name.clone(),
        });
    }
    Ok(path)
}

pub async fn remove_entry(backend: &Backend, name: &str) -> Result<()> {
    backend.delete(&layout::catalog_entry_path(name)).await
}

/// List all catalog entries (names are read from inside the JSON objects).
///
/// Fetched with bounded concurrency: the entries are independent objects, and
/// a serial loop made this cost one round trip per table. It is on the
/// per-query path (a query session registers every table before planning), so
/// on a wide catalog the latency was paid by every statement.
pub async fn list_entries(backend: &Backend) -> Result<Vec<CatalogEntry>> {
    let metas = backend
        .list(&object_store::path::Path::from(layout::CATALOG_PREFIX))
        .await?;
    let mut out: Vec<CatalogEntry> = stream::iter(metas)
        .map(|meta| async move {
            let bytes = backend.get(&meta.location).await?;
            let entry: CatalogEntry = serde_json::from_slice(&bytes).map_err(|e| {
                Error::corruption(meta.location.as_ref(), format!("catalog parse: {e}"))
            })?;
            entry.verify(meta.location.as_ref())?;
            Ok::<_, Error>(entry)
        })
        .buffer_unordered(crate::backend::METADATA_FETCH_CONCURRENCY)
        .try_collect()
        .await?;
    // `buffer_unordered` gives no ordering guarantee, and callers rely on this
    // being sorted by name.
    out.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(out)
}
