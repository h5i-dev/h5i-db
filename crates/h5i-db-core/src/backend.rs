//! Storage backend: an `object_store` for bulk immutable objects plus a
//! `HeadStore` providing the primitives object stores don't uniformly give
//! us: an exclusive per-table writer critical section and an atomic
//! compare-and-swap of the table HEAD.
//!
//! Invariants enforced here (DESIGN_CLAUDE.md §5):
//! - HEAD is the only mutable object under a table's path.
//! - The manifest for sequence n+1 is published *inside* the writer critical
//!   section, so two racing writers can never both write the same manifest
//!   slot: the loser fails head revalidation before it publishes anything.
//!   (A writer that crashed after publishing but before the swap leaves an
//!   uncommitted manifest > HEAD; the next writer overwrites that slot and
//!   vacuum treats such slots as debris.)
//! - A head swap only succeeds when the caller's expected generation matches;
//!   otherwise `VersionConflict` — never last-writer-wins.
//! - On the local backend, data is fsynced before HEAD moves, and the HEAD
//!   swap itself is write-temp + rename + directory fsync.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::TryStreamExt;
use object_store::{
    local::LocalFileSystem, path::Path as ObjPath, ObjectStore, ObjectStoreExt, PutPayload,
};
use url::Url;
use uuid::Uuid;

use crate::error::{Error, Result};
use crate::layout;
use crate::manifest::Head;

/// Opaque generation token for CAS. For the local backend it is the blake3
/// checksum of the HEAD bytes; for S3 it will be the object ETag/version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeadTag(pub String);

/// The current head of a table plus the tag needed to replace it atomically.
#[derive(Debug, Clone)]
pub struct HeadState {
    pub head: Head,
    pub tag: HeadTag,
}

#[async_trait]
pub trait HeadStore: Send + Sync + std::fmt::Debug {
    /// Read a table's HEAD. `None` when the table has no committed version.
    async fn read(&self, table_id: Uuid) -> Result<Option<HeadState>>;

    /// Run a full commit: enter the table's writer critical section,
    /// revalidate that HEAD still matches `expected` (`None` = "no HEAD yet",
    /// i.e. first commit), run `publish` (the caller writes + fsyncs the
    /// manifest there), then atomically swap HEAD to `new_head`.
    ///
    /// On expectation mismatch returns `VersionConflict` *before* `publish`
    /// runs, so a losing writer publishes nothing.
    async fn commit(
        &self,
        table_id: Uuid,
        table_name: &str,
        expected: Option<&HeadTag>,
        new_head: &Head,
        publish: BoxFuture<'_, Result<()>>,
    ) -> Result<HeadTag>;

    /// Remove a table's HEAD (drop-table path). Idempotent.
    async fn remove(&self, table_id: Uuid) -> Result<()>;
}

// ---------------------------------------------------------------------------
// local filesystem implementation
// ---------------------------------------------------------------------------

/// Local-filesystem HEAD store: lock file + write-temp + rename + fsync.
#[derive(Debug)]
pub struct LocalHeadStore {
    root: PathBuf,
    lock_timeout: Duration,
}

impl LocalHeadStore {
    pub fn new(root: PathBuf) -> Self {
        Self {
            root,
            lock_timeout: Duration::from_secs(10),
        }
    }

    fn head_fs_path(&self, table_id: Uuid) -> PathBuf {
        self.root.join(layout::head_path(table_id).as_ref())
    }

    fn lock_fs_path(&self, table_id: Uuid) -> PathBuf {
        self.root.join(format!("tables/{table_id}/HEAD.lock"))
    }
}

/// Exclusive advisory lock via `O_CREAT | O_EXCL`. Held for the duration of
/// one commit critical section. A stale lock older than `STALE_AFTER`
/// (crashed writer) is broken with a warning — head revalidation inside the
/// section still protects against the original holder waking up later.
struct FsLock {
    path: PathBuf,
}

const STALE_AFTER: Duration = Duration::from_secs(60);

impl FsLock {
    async fn acquire(path: &Path, timeout: Duration, table_name: &str) -> Result<Self> {
        let start = Instant::now();
        loop {
            match std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(path)
            {
                Ok(mut f) => {
                    use std::io::Write;
                    let _ = writeln!(f, "pid={} at={}", std::process::id(), chrono::Utc::now());
                    let _ = f.sync_all();
                    return Ok(Self {
                        path: path.to_path_buf(),
                    });
                }
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    if let Ok(meta) = std::fs::metadata(path) {
                        if let Ok(modified) = meta.modified() {
                            if modified.elapsed().unwrap_or_default() > STALE_AFTER {
                                tracing::warn!(
                                    lock = %path.display(),
                                    "breaking stale writer lock (older than {STALE_AFTER:?})"
                                );
                                let _ = std::fs::remove_file(path);
                                continue;
                            }
                        }
                    }
                    if start.elapsed() > timeout {
                        return Err(Error::LockTimeout {
                            table: table_name.to_string(),
                            waited_ms: start.elapsed().as_millis() as u64,
                        });
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(e) => return Err(Error::io(path.display(), e)),
            }
        }
    }
}

impl Drop for FsLock {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn fsync_dir(dir: &Path) -> Result<()> {
    let f = std::fs::File::open(dir).map_err(|e| Error::io(dir.display(), e))?;
    f.sync_all().map_err(|e| Error::io(dir.display(), e))
}

fn read_head_file(path: &Path) -> Result<Option<(Head, HeadTag)>> {
    match std::fs::read(path) {
        Ok(bytes) => {
            let head = Head::from_bytes(&bytes, &path.display().to_string())?;
            let tag = HeadTag(crate::util::checksum_hex(&bytes));
            Ok(Some((head, tag)))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(Error::io(path.display(), e)),
    }
}

fn write_head_file(path: &Path, head: &Head) -> Result<HeadTag> {
    let bytes = head.to_bytes()?;
    let tmp = path.with_extension(format!("tmp.{}", Uuid::new_v4()));
    {
        use std::io::Write;
        let mut f = std::fs::File::create(&tmp).map_err(|e| Error::io(tmp.display(), e))?;
        f.write_all(&bytes)
            .map_err(|e| Error::io(tmp.display(), e))?;
        f.sync_all().map_err(|e| Error::io(tmp.display(), e))?;
    }
    std::fs::rename(&tmp, path).map_err(|e| Error::io(path.display(), e))?;
    if let Some(parent) = path.parent() {
        fsync_dir(parent)?;
    }
    Ok(HeadTag(crate::util::checksum_hex(&bytes)))
}

#[async_trait]
impl HeadStore for LocalHeadStore {
    async fn read(&self, table_id: Uuid) -> Result<Option<HeadState>> {
        let path = self.head_fs_path(table_id);
        let res = tokio::task::spawn_blocking(move || read_head_file(&path))
            .await
            .map_err(Error::internal)??;
        Ok(res.map(|(head, tag)| HeadState { head, tag }))
    }

    async fn commit(
        &self,
        table_id: Uuid,
        table_name: &str,
        expected: Option<&HeadTag>,
        new_head: &Head,
        publish: BoxFuture<'_, Result<()>>,
    ) -> Result<HeadTag> {
        let head_path = self.head_fs_path(table_id);
        let lock_path = self.lock_fs_path(table_id);
        if let Some(parent) = head_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| Error::io(parent.display(), e))?;
        }

        let _lock = FsLock::acquire(&lock_path, self.lock_timeout, table_name).await?;

        // Revalidate the expectation inside the critical section.
        let current = read_head_file(&head_path)?;
        match (&expected, &current) {
            (None, None) => {}
            (Some(exp), Some((_, cur_tag))) if *exp == cur_tag => {}
            _ => {
                let actual = current.as_ref().map(|(h, _)| h.sequence).unwrap_or(0);
                return Err(Error::VersionConflict {
                    table: table_name.to_string(),
                    expected: new_head.sequence.saturating_sub(1),
                    actual,
                });
            }
        }

        // Publish the manifest (and any other objects) while holding the lock.
        publish.await?;

        write_head_file(&head_path, new_head)
    }

    async fn remove(&self, table_id: Uuid) -> Result<()> {
        let path = self.head_fs_path(table_id);
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(Error::io(path.display(), e)),
        }
    }
}

// ---------------------------------------------------------------------------
// backend
// ---------------------------------------------------------------------------

/// A database's storage backend.
#[derive(Debug, Clone)]
pub struct Backend {
    pub store: Arc<dyn ObjectStore>,
    pub heads: Arc<dyn HeadStore>,
    /// Base URL of the store (used to register with query engines).
    pub base_url: Url,
    /// Set when the backend is a local directory (enables fsync-before-swap
    /// durability and filesystem-level tooling).
    pub local_root: Option<PathBuf>,
}

impl Backend {
    pub fn local(root: &Path) -> Result<Self> {
        let canonical = root
            .canonicalize()
            .map_err(|e| Error::io(root.display(), e))?;
        let store = LocalFileSystem::new_with_prefix(&canonical)
            .map_err(Error::ObjectStore)?
            .with_automatic_cleanup(true);
        let base_url = Url::from_directory_path(&canonical)
            .map_err(|_| Error::invalid(format!("cannot build file URL for {canonical:?}")))?;
        Ok(Self {
            store: Arc::new(store),
            heads: Arc::new(LocalHeadStore::new(canonical.clone())),
            base_url,
            local_root: Some(canonical),
        })
    }

    pub async fn put(&self, path: &ObjPath, bytes: Bytes) -> Result<()> {
        self.store
            .put(path, PutPayload::from_bytes(bytes))
            .await
            .map_err(Error::ObjectStore)?;
        Ok(())
    }

    pub async fn get(&self, path: &ObjPath) -> Result<Bytes> {
        let res = self.store.get(path).await.map_err(Error::ObjectStore)?;
        res.bytes().await.map_err(Error::ObjectStore)
    }

    pub async fn get_opt(&self, path: &ObjPath) -> Result<Option<Bytes>> {
        match self.store.get(path).await {
            Ok(res) => Ok(Some(res.bytes().await.map_err(Error::ObjectStore)?)),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(Error::ObjectStore(e)),
        }
    }

    pub async fn delete(&self, path: &ObjPath) -> Result<()> {
        match self.store.delete(path).await {
            Ok(()) => Ok(()),
            Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(Error::ObjectStore(e)),
        }
    }

    /// List object metadata under a prefix.
    pub async fn list(&self, prefix: &ObjPath) -> Result<Vec<object_store::ObjectMeta>> {
        self.store
            .list(Some(prefix))
            .try_collect::<Vec<_>>()
            .await
            .map_err(Error::ObjectStore)
    }

    /// Durability barrier for the local backend: fsync the given objects (and
    /// their directories) so HEAD never references non-durable data. No-op on
    /// object stores, whose PUT is already durable-on-ack.
    pub async fn sync_objects(&self, paths: &[ObjPath]) -> Result<()> {
        let Some(root) = &self.local_root else {
            return Ok(());
        };
        let root = root.clone();
        let paths: Vec<PathBuf> = paths.iter().map(|p| root.join(p.as_ref())).collect();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut dirs = std::collections::BTreeSet::new();
            for p in &paths {
                let f = std::fs::File::open(p).map_err(|e| Error::io(p.display(), e))?;
                f.sync_all().map_err(|e| Error::io(p.display(), e))?;
                if let Some(dir) = p.parent() {
                    dirs.insert(dir.to_path_buf());
                }
            }
            for dir in dirs {
                fsync_dir(&dir)?;
            }
            Ok(())
        })
        .await
        .map_err(Error::internal)?
    }
}
