//! Storage backend: an `object_store` for bulk immutable objects plus a
//! `HeadStore` providing the primitives object stores don't uniformly give
//! us: an exclusive per-table writer critical section and an atomic
//! compare-and-swap of the table HEAD.
//!
//! Invariants enforced here (DESIGN_CLAUDE.md §5):
//! - HEAD is the only mutable object under a table's path.
//! - The writer critical section is an OS-level `flock` held on an open fd
//!   (never existence-based, never "broken" by age): a crashed writer's lock
//!   dies with its process, and a slow-but-alive writer keeps its lock — so
//!   two writers can never both reach the head swap.
//! - The manifest for sequence n+1 is published *inside* the writer critical
//!   section, so two racing writers can never both write the same manifest
//!   slot: the loser fails head revalidation before it publishes anything.
//!   (A writer that crashed after publishing but before the swap leaves an
//!   uncommitted manifest > HEAD; the next writer overwrites that slot and
//!   vacuum treats such slots as debris.)
//! - A head swap only succeeds when the caller's expected generation matches;
//!   otherwise `VersionConflict` — never last-writer-wins.
//! - On the local backend, all data a commit introduces (segments AND the
//!   manifest) is fsynced before HEAD moves, and the HEAD swap itself is
//!   write-temp + rename + directory fsync.

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

/// Exclusive advisory lock via OS-level `flock` on an open file descriptor.
/// Held for the duration of one commit critical section.
///
/// The lock file itself is never unlinked: existence carries no meaning, only
/// the kernel lock on the open fd does. A crashed holder's lock is released
/// by the kernel when its process dies, so there is no stale-lock breaking —
/// and therefore no window in which a slow-but-alive writer and a
/// lock-breaker can both commit. (`flock` is advisory and unreliable on NFS;
/// see docs/OPERATIONS.md.)
struct FsLock {
    /// Keeps the fd — and with it the kernel lock — alive. Dropping the
    /// handle closes the fd and releases the lock; the file is left behind
    /// on purpose (unlinking would let a later opener lock a fresh inode
    /// while an existing holder still locks the old one).
    _file: std::fs::File,
}

impl FsLock {
    async fn acquire(path: &Path, timeout: Duration, table_name: &str) -> Result<Self> {
        let start = Instant::now();
        loop {
            let p = path.to_path_buf();
            let attempt = tokio::task::spawn_blocking(move || FsLock::try_acquire(&p))
                .await
                .map_err(Error::internal)??;
            match attempt {
                Some(lock) => return Ok(lock),
                None => {
                    if start.elapsed() > timeout {
                        return Err(Error::LockTimeout {
                            table: table_name.to_string(),
                            waited_ms: start.elapsed().as_millis() as u64,
                        });
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        }
    }

    /// One non-blocking lock attempt. `None` = currently held elsewhere.
    fn try_acquire(path: &Path) -> Result<Option<FsLock>> {
        use fs4::fs_std::FileExt;
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .map_err(|e| Error::io(path.display(), e))?;
        match file.try_lock_exclusive() {
            Ok(true) => {}
            Ok(false) => return Ok(None),
            Err(e) => return Err(Error::io(path.display(), e)),
        }
        // Best-effort debug info for operators inspecting a held lock.
        use std::io::{Seek, Write};
        let _ = file.set_len(0);
        let mut f = &file;
        let _ = f.seek(std::io::SeekFrom::Start(0));
        let _ = writeln!(f, "pid={} at={}", std::process::id(), chrono::Utc::now());
        Ok(Some(FsLock { _file: file }))
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

        // Revalidate the expectation inside the critical section. All head
        // file I/O (reads, and the fsync-heavy swap below) runs on the
        // blocking pool, never inline on the async executor.
        let hp = head_path.clone();
        let current = tokio::task::spawn_blocking(move || read_head_file(&hp))
            .await
            .map_err(Error::internal)??;
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

        let nh = new_head.clone();
        tokio::task::spawn_blocking(move || write_head_file(&head_path, &nh))
            .await
            .map_err(Error::internal)?
    }

    async fn remove(&self, table_id: Uuid) -> Result<()> {
        let path = self.head_fs_path(table_id);
        // Idempotent: nothing to remove (and no lock file to create) when the
        // table directory is already gone.
        if !path.parent().is_some_and(|p| p.exists()) {
            return Ok(());
        }
        // Take the writer lock so removal cannot interleave with an in-flight
        // commit's head swap.
        let lock_path = self.lock_fs_path(table_id);
        let _lock = FsLock::acquire(&lock_path, self.lock_timeout, &table_id.to_string()).await?;
        tokio::task::spawn_blocking(move || match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(Error::io(path.display(), e)),
        })
        .await
        .map_err(Error::internal)?
    }
}

/// Guard for the database-level metadata critical section; see
/// [`Backend::meta_lock`]. Dropping it releases the lock.
pub struct MetaLockGuard {
    _lock: Option<FsLock>,
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

    /// Atomic create-if-absent put. Returns `false` (writing nothing) when an
    /// object already exists at `path` — the storage-level primitive behind
    /// catalog/snapshot create semantics, closing check-then-put races.
    pub async fn put_if_absent(&self, path: &ObjPath, bytes: Bytes) -> Result<bool> {
        use object_store::{PutMode, PutOptions};
        match self
            .store
            .put_opts(
                path,
                PutPayload::from_bytes(bytes),
                PutOptions::from(PutMode::Create),
            )
            .await
        {
            Ok(_) => Ok(true),
            Err(object_store::Error::AlreadyExists { .. }) => Ok(false),
            Err(e) => Err(Error::ObjectStore(e)),
        }
    }

    /// Acquire the database-level metadata lock serializing catalog, snapshot,
    /// and policy read-modify-write cycles (they have no per-table writer
    /// lock to piggyback on). Backed by `flock` on the local backend; object
    /// stores are expected to use conditional puts instead and get a no-op
    /// guard.
    pub async fn meta_lock(&self) -> Result<MetaLockGuard> {
        match &self.local_root {
            Some(root) => {
                let path = root.join("CATALOG.lock");
                let lock =
                    FsLock::acquire(&path, Duration::from_secs(10), "<catalog metadata>").await?;
                Ok(MetaLockGuard { _lock: Some(lock) })
            }
            None => Ok(MetaLockGuard { _lock: None }),
        }
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
