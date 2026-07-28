//! `Database`: open/create, table lifecycle, the commit protocol, version
//! resolution, scans, compaction, vacuum, and verify.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use futures::stream::{self, StreamExt, TryStreamExt};
use object_store::{ObjectStoreExt, path::Path as ObjPath};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::backend::{Backend, HeadState};
use crate::catalog::{self, CatalogEntry};
use crate::error::{Error, Result};
use crate::layout;
use crate::manifest::{Head, OpKind, SegmentMeta, VersionManifest};
use crate::segment::{
    MERGE_CHUNK_ROWS, SegmentWriter, batch_is_sorted, read_segment, sort_batches, time_values_i64,
};
use crate::snapshot::{self, Snapshot, SnapshotEntry};
use crate::spec::{SEGMENT_COUNT_WARN, TableOptions, TableSpec};

/// Which version of a table to read.
#[derive(Debug, Clone, PartialEq)]
pub enum ReadAt {
    Latest,
    /// Exact sequence number.
    Version(u64),
    /// Latest version whose commit wall-clock time (ns since epoch) is <= ts.
    AsOf(i64),
    /// The version pinned by a named snapshot.
    Snapshot(String),
}

/// A resolved, immutable view of one table version.
#[derive(Debug, Clone)]
pub struct ResolvedTable {
    pub entry: CatalogEntry,
    pub spec: TableSpec,
    pub schema: SchemaRef,
    pub manifest: VersionManifest,
    /// The head sequence at resolution time (== manifest.sequence for Latest).
    pub head_sequence: u64,
}

/// Options for a direct (engine-free) scan.
#[derive(Debug, Clone, Default)]
pub struct ScanOptions {
    /// Column names to read; `None` = all.
    pub projection: Option<Vec<String>>,
    /// Inclusive lower bound on the time column, raw units of the schema.
    pub time_start: Option<i64>,
    /// Exclusive upper bound on the time column, raw units of the schema.
    pub time_end: Option<i64>,
    /// Stop after this many rows.
    pub limit: Option<usize>,
    /// Concurrent segment reads (default 4).
    pub concurrency: Option<usize>,
    /// Verify each segment's full-file blake3 checksum against the manifest
    /// before decoding (3.6). Reads whole objects, so row-group pruning does
    /// not apply — integrity over speed.
    pub verify_checksums: bool,
}

/// What a scan touched — the observability half of pruning.
#[derive(Debug, Clone, Default, Serialize)]
pub struct ScanReport {
    pub segments_total: usize,
    pub segments_scanned: usize,
    pub segments_pruned: usize,
    pub bytes_scanned: u64,
    pub rows_returned: u64,
}

/// Result of a successful commit.
#[derive(Debug, Clone, Serialize)]
pub struct CommitResult {
    pub table: String,
    pub sequence: u64,
    pub op: String,
    pub rows_total: u64,
    pub segments_total: usize,
    pub segments_added: usize,
    /// Segments reused verbatim from the parent via content-hash dedup.
    pub segments_deduped: usize,
    pub committed_at_ns: i64,
}

/// A per-table commit prepared for a journaled multi-table transaction.
/// Segments are durable but unreachable until the transaction advances HEAD.
pub(crate) struct StagedCommit {
    pub(crate) entry: CatalogEntry,
    pub(crate) head: HeadState,
    pub(crate) manifest: VersionManifest,
    pub(crate) segments_added: usize,
    pub(crate) segments_deduped: usize,
    pub(crate) lease: Option<ObjPath>,
    /// Captured in the staging phase so the commit — which runs inside the
    /// database-wide metadata lock — need not re-read what staging just read.
    pub(crate) known: CommitInputs,
}

/// Objects the caller already holds, so the commit does not re-read them.
///
/// The commit runs inside the database-wide metadata lock, so anything it
/// fetches there is not merely a round trip — it is a round trip every other
/// writer in the database is queued behind. `write_prologue` has already loaded
/// the spec and the parent manifest a moment earlier; handing them down turns
/// two GETs plus two full manifest/spec deserializations per commit into zero.
///
/// Each field is validated before use (revision and sequence must match what
/// the commit actually needs) and falls back to a read when it does not, so a
/// stale or absent hint costs correctness nothing.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct CommitInputs {
    /// `max_segments_per_manifest` for the revision being committed.
    pub(crate) segment_limit: Option<usize>,
    /// `committed_at_ns` of the parent manifest.
    pub(crate) parent_committed_at_ns: Option<i64>,
}

/// Options common to write-path operations.
#[derive(Debug, Clone, Default)]
pub struct WriteOptions {
    /// Require the current head to be exactly this sequence; `None` = the
    /// head observed when the operation started.
    pub expected_version: Option<u64>,
    pub note: Option<String>,
    pub user_meta: serde_json::Map<String, serde_json::Value>,
    /// Caller-chosen token making this mutation replayable exactly once.
    ///
    /// A retry after an ambiguous failure — a timeout that may or may not have
    /// landed — carries the same key, finds the commit it already produced,
    /// and returns it instead of appending the rows a second time. Duplicated
    /// ticks are silent poison: nothing errors, the data is simply wrong from
    /// then on, so this is the one guard an unattended ingest loop needs.
    pub idempotency_key: Option<String>,
}

/// Manifest `user_meta` key under which an idempotency token is recorded.
/// Namespaced so it cannot collide with a caller's own metadata.
pub const IDEMPOTENCY_META_KEY: &str = "h5i.idempotency_key";

/// How many commits back a key is looked for.
///
/// A retry follows its original within moments, so the match is at or very
/// near the head; the bound keeps the guard from turning into a full history
/// scan on a long-lived table. Retries separated by more commits than this
/// are not deduplicated — stated plainly rather than implied, because the
/// guarantee has an edge and callers should know where it is.
pub const IDEMPOTENCY_LOOKBACK: u64 = 64;

/// Vacuum report (dry-run by default).
#[derive(Debug, Clone, Default, Serialize)]
pub struct VacuumReport {
    pub scanned_objects: usize,
    pub candidates: Vec<String>,
    pub candidate_bytes: u64,
    pub deleted: usize,
    pub dry_run: bool,
}

/// Verify report.
#[derive(Debug, Clone, Default, Serialize)]
pub struct VerifyReport {
    pub table: String,
    pub head_sequence: u64,
    pub manifests_checked: u64,
    pub segments_checked: u64,
    pub bytes_checked: u64,
    pub problems: Vec<String>,
}

/// One row of `list_versions`.
#[derive(Debug, Clone, Serialize)]
pub struct VersionSummary {
    pub sequence: u64,
    pub op: String,
    pub committed_at_ns: i64,
    pub rows: u64,
    pub bytes: u64,
    pub segments: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    /// `planned` when the version went through plan review, `direct`
    /// otherwise. Part of the summary because "how was this produced?" is an
    /// audit question asked of *every* row of a history, and answering it
    /// from the manifest the summary was built from costs nothing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_mode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plan_hash: Option<String>,
}

/// Fault-injection hook: called with a named commit step; returning an error
/// simulates a crash at that point. Test-only in spirit, but wired through
/// production code so the tested path IS the shipped path.
pub type CommitHook = Arc<dyn Fn(&str) -> Result<()> + Send + Sync>;

#[derive(Clone)]
pub struct Database {
    backend: Backend,
    read_only: bool,
    commit_hook: Option<CommitHook>,
    /// When set, every table lookup runs inside this fork: the fork's catalog
    /// shadows the global one, and base tables resolve at their pinned version
    /// instead of their head (ROADMAP Part IX).
    ///
    /// A handle, not a mode flag — `open_fork` returns a *new* `Database`, so a
    /// caller can hold base and fork handles side by side and neither can
    /// accidentally write through the other.
    fork: Option<Arc<crate::fork::Fork>>,
}

impl std::fmt::Debug for Database {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Database")
            .field("backend", &self.backend)
            .field("read_only", &self.read_only)
            .field("fork", &self.fork.as_ref().map(|f| &f.name))
            .finish()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FormatFile {
    format_version: u32,
    min_reader_version: u32,
    created_at_ns: i64,
    created_by: String,
}

impl Database {
    // ------------------------------------------------------------------
    // lifecycle
    // ------------------------------------------------------------------

    /// Create a new database directory. Fails if one already exists there.
    pub async fn create(path: &Path) -> Result<Self> {
        if path.join(layout::FORMAT_FILE).exists() {
            return Err(Error::DatabaseExists {
                path: path.display().to_string(),
            });
        }
        std::fs::create_dir_all(path).map_err(|e| Error::io(path.display(), e))?;
        let backend = Backend::local(path)?;
        let format = FormatFile {
            format_version: layout::FORMAT_VERSION,
            min_reader_version: layout::MIN_READER_VERSION,
            created_at_ns: crate::util::monotonic_commit_ts(None),
            created_by: format!("h5i-db {}", env!("CARGO_PKG_VERSION")),
        };
        backend
            .put(
                &layout::format_path(),
                serde_json::to_vec_pretty(&format)?.into(),
            )
            .await?;
        backend.sync_objects(&[layout::format_path()]).await?;
        Ok(Self {
            backend,
            read_only: false,
            commit_hook: None,
            fork: None,
        })
    }

    /// Create a database on a caller-supplied backend (for example S3,
    /// GCS, Azure, MinIO, or an in-memory object store).
    pub async fn create_with_backend(backend: Backend) -> Result<Self> {
        let format = FormatFile {
            format_version: layout::FORMAT_VERSION,
            min_reader_version: layout::MIN_READER_VERSION,
            created_at_ns: crate::util::monotonic_commit_ts(None),
            created_by: format!("h5i-db {}", env!("CARGO_PKG_VERSION")),
        };
        let bytes = serde_json::to_vec_pretty(&format)?;
        if !backend
            .put_if_absent(&layout::format_path(), bytes.into())
            .await?
        {
            return Err(Error::DatabaseExists {
                path: backend.base_url.to_string(),
            });
        }
        backend.sync_objects(&[layout::format_path()]).await?;
        Ok(Self {
            backend,
            read_only: false,
            commit_hook: None,
            fork: None,
        })
    }

    /// Open an existing database.
    pub async fn open(path: &Path) -> Result<Self> {
        Self::open_with(path, false).await
    }

    pub async fn open_read_only(path: &Path) -> Result<Self> {
        Self::open_with(path, true).await
    }

    /// Open an existing database on a caller-supplied backend.
    pub async fn open_backend(backend: Backend, read_only: bool) -> Result<Self> {
        let bytes = backend
            .get_opt(&layout::format_path())
            .await?
            .ok_or_else(|| Error::DatabaseNotFound {
                path: backend.base_url.to_string(),
            })?;
        let format: FormatFile = serde_json::from_slice(&bytes)
            .map_err(|e| Error::corruption(layout::FORMAT_FILE, format!("parse: {e}")))?;
        // Compared against READER_VERSION, not FORMAT_VERSION: the question
        // here is "can this binary reason about the whole database", which is
        // what a fork's extra GC root changes (layout.rs).
        if format.min_reader_version > layout::READER_VERSION {
            return Err(Error::FormatTooNew {
                found: format.min_reader_version,
                supported: layout::READER_VERSION,
            });
        }
        let db = Self {
            backend,
            read_only,
            commit_hook: None,
            fork: None,
        };
        if !read_only {
            crate::transaction::recover(&db).await?;
        }
        Ok(db)
    }

    async fn open_with(path: &Path, read_only: bool) -> Result<Self> {
        let backend = Backend::local(path).map_err(|_| Error::DatabaseNotFound {
            path: path.display().to_string(),
        })?;
        let bytes = backend
            .get_opt(&layout::format_path())
            .await?
            .ok_or_else(|| Error::DatabaseNotFound {
                path: path.display().to_string(),
            })?;
        let format: FormatFile = serde_json::from_slice(&bytes)
            .map_err(|e| Error::corruption(layout::FORMAT_FILE, format!("parse: {e}")))?;
        // Compared against READER_VERSION, not FORMAT_VERSION: the question
        // here is "can this binary reason about the whole database", which is
        // what a fork's extra GC root changes (layout.rs).
        if format.min_reader_version > layout::READER_VERSION {
            return Err(Error::FormatTooNew {
                found: format.min_reader_version,
                supported: layout::READER_VERSION,
            });
        }
        let db = Self {
            backend,
            read_only,
            commit_hook: None,
            fork: None,
        };
        if !read_only {
            crate::transaction::recover(&db).await?;
        }
        Ok(db)
    }

    /// Open, creating if absent.
    pub async fn open_or_create(path: &Path) -> Result<Self> {
        if path.join(layout::FORMAT_FILE).exists() {
            Self::open(path).await
        } else {
            Self::create(path).await
        }
    }

    pub fn backend(&self) -> &Backend {
        &self.backend
    }

    /// Current mutation policy (defaults when never configured).
    pub async fn policy(&self) -> Result<crate::policy::MutationPolicy> {
        crate::policy::load(&self.backend).await
    }

    /// Persist a new mutation policy (whole-value overwrite). Prefer
    /// [`Database::update_policy`] for read-modify-write edits.
    pub async fn set_policy(&self, policy: &crate::policy::MutationPolicy) -> Result<()> {
        self.ensure_writable("set_policy")?;
        let _meta = self.backend.meta_lock().await?;
        crate::policy::store(&self.backend, policy).await
    }

    /// Atomically read-modify-write the mutation policy under the database
    /// metadata lock, closing the load/store TOCTOU between concurrent
    /// policy editors (3.5).
    pub async fn update_policy(
        &self,
        f: impl FnOnce(&mut crate::policy::MutationPolicy) -> Result<()>,
    ) -> Result<crate::policy::MutationPolicy> {
        self.ensure_writable("set_policy")?;
        let _meta = self.backend.meta_lock().await?;
        let mut policy = crate::policy::load(&self.backend).await?;
        f(&mut policy)?;
        crate::policy::store(&self.backend, &policy).await?;
        Ok(policy)
    }

    // ------------------------------------------------------------------
    // data-safety policy (opt-in, per-table — ROADMAP V-B1)
    // ------------------------------------------------------------------

    /// The table's data-safety policy, or `None` when unset (the default —
    /// unset means no constraints and no write-path enforcement cost).
    pub async fn data_policy(&self, table: &str) -> Result<Option<crate::data_policy::DataPolicy>> {
        let entry = self.entry(table).await?;
        crate::data_policy::load(&self.backend, entry.table_id).await
    }

    /// Install (overwrite) a table's data-safety policy.
    pub async fn set_data_policy(
        &self,
        table: &str,
        policy: &crate::data_policy::DataPolicy,
    ) -> Result<()> {
        self.ensure_writable("set_data_policy")?;
        let entry = self.entry(table).await?;
        let _meta = self.backend.meta_lock().await?;
        crate::data_policy::store(&self.backend, entry.table_id, policy).await
    }

    /// Remove a table's data-safety policy (writes are unconstrained again).
    pub async fn clear_data_policy(&self, table: &str) -> Result<()> {
        self.ensure_writable("clear_data_policy")?;
        let entry = self.entry(table).await?;
        let _meta = self.backend.meta_lock().await?;
        crate::data_policy::clear(&self.backend, entry.table_id).await
    }

    /// Enforce the table's data policy (if any) against the rows a mutation
    /// would write. A no-op — a single metadata lookup — when no policy is set,
    /// so tables without a policy pay effectively nothing and the read path is
    /// never touched. Called from the write-path staging functions.
    pub(crate) async fn enforce_data_policy(
        &self,
        table_id: Uuid,
        batches: &[RecordBatch],
    ) -> Result<()> {
        if let Some(policy) = crate::data_policy::load(&self.backend, table_id).await? {
            policy.enforce(batches)?;
        }
        Ok(())
    }

    pub fn is_read_only(&self) -> bool {
        self.read_only
    }

    // ------------------------------------------------------------------
    // fork handles (ROADMAP Part IX)
    // ------------------------------------------------------------------

    /// Open a handle scoped to a fork. Table lookups then check the fork's
    /// catalog first and fall back to the fork's *pinned* view of the base.
    ///
    /// The returned handle shares this one's backend and read-only flag, so
    /// `open_read_only(..).open_fork(..)` is a read-only fork view.
    pub async fn open_fork(&self, fork_name: &str) -> Result<Self> {
        let fork = crate::fork::load(&self.backend, fork_name).await?;
        Ok(Self {
            backend: self.backend.clone(),
            read_only: self.read_only,
            commit_hook: self.commit_hook.clone(),
            fork: Some(Arc::new(fork)),
        })
    }

    /// A handle on the base database, dropping any fork scope.
    pub fn base(&self) -> Self {
        Self {
            backend: self.backend.clone(),
            read_only: self.read_only,
            commit_hook: self.commit_hook.clone(),
            fork: None,
        }
    }

    /// The fork this handle is scoped to, if any.
    pub fn fork(&self) -> Option<&crate::fork::Fork> {
        self.fork.as_deref()
    }

    pub fn fork_name(&self) -> Option<&str> {
        self.fork.as_ref().map(|f| f.name.as_str())
    }

    /// Raise the database's recorded `min_reader_version`, never lowering it.
    ///
    /// Callers must hold the metadata lock. Idempotent: re-running after the
    /// fence is already in place rewrites nothing.
    pub(crate) async fn raise_min_reader_version(&self, required: u32) -> Result<bool> {
        let path = layout::format_path();
        let bytes = self
            .backend
            .get_opt(&path)
            .await?
            .ok_or_else(|| Error::corruption(layout::FORMAT_FILE, "FORMAT missing"))?;
        let mut format: FormatFile = serde_json::from_slice(&bytes)
            .map_err(|e| Error::corruption(layout::FORMAT_FILE, format!("parse: {e}")))?;
        if format.min_reader_version >= required {
            return Ok(false);
        }
        format.min_reader_version = required;
        self.backend
            .put(&path, serde_json::to_vec_pretty(&format)?.into())
            .await?;
        self.backend.sync_objects(&[path]).await?;
        Ok(true)
    }

    /// Install a fault-injection hook (used by crash-safety tests).
    pub fn set_commit_hook(&mut self, hook: CommitHook) {
        self.commit_hook = Some(hook);
    }

    fn hook(&self, step: &str) -> Result<()> {
        if let Some(h) = &self.commit_hook {
            h(step)?;
        }
        Ok(())
    }

    pub(crate) fn ensure_writable(&self, op: &str) -> Result<()> {
        if self.read_only {
            return Err(Error::ReadOnly { op: op.into() });
        }
        Ok(())
    }

    // ------------------------------------------------------------------
    // table lifecycle
    // ------------------------------------------------------------------

    pub async fn create_table(
        &self,
        name: &str,
        schema: SchemaRef,
        options: TableOptions,
    ) -> Result<CommitResult> {
        self.ensure_writable("create_table")?;
        validate_table_name(name)?;
        // Serialize catalog mutations (3.5): the metadata lock closes the
        // check-then-put window, and `create_entry` below is additionally an
        // atomic create-if-absent as defense in depth.
        let _meta = self.backend.meta_lock().await?;
        // Uniqueness is checked against the *visible* namespace, which inside a
        // fork means the fork's own tables plus the base tables it pins. Two
        // forks may each hold a table called `features`; they are different
        // tables and never collide.
        //
        // `entry_opt`, not `entry`: the expected answer here is "absent", and
        // `entry` would build a did-you-mean suggestion out of the whole
        // catalog to describe a miss the caller is happy about.
        if self.entry_opt(name).await?.is_some() {
            return Err(Error::TableExists { name: name.into() });
        }
        let table_id = Uuid::new_v4();
        let spec = TableSpec::new(table_id, name, &schema, &options)?;

        // Write the spec, then the empty v0 manifest, then HEAD, then the
        // catalog entry last: a crash mid-way leaves an unreachable table dir
        // (vacuumable), never a visible broken table.
        let spec_path = layout::spec_path(table_id, spec.schema_revision);
        self.backend
            .put(&spec_path, serde_json::to_vec_pretty(&spec)?.into())
            .await?;
        self.backend.sync_objects(&[spec_path]).await?;

        let mut manifest = VersionManifest {
            format: layout::FORMAT_VERSION,
            table_id,
            sequence: 0,
            parent: None,
            parent_checksum: None,
            committed_at_ns: crate::util::monotonic_commit_ts(None),
            op: OpKind::Create,
            execution_mode: Some("direct".to_string()),
            plan_hash: None,
            note: None,
            user_meta: serde_json::Map::new(),
            schema_revision: spec.schema_revision,
            rows: 0,
            bytes: 0,
            time_range: None,
            segments: vec![],
        };
        manifest.recompute_rollups();
        let result = self
            .commit_manifest_locked(
                name,
                table_id,
                None,
                &mut manifest,
                0,
                CommitInputs {
                    segment_limit: Some(spec.max_segments_per_manifest),
                    parent_committed_at_ns: None,
                },
            )
            .await?;

        // Registration last, and into the fork's catalog when we are in one:
        // a table created inside a fork is invisible to the base database and
        // to base `vacuum`, which is what makes 20 agents' scratch tables cost
        // main nothing.
        match &self.fork {
            Some(fork) => {
                let entry = crate::fork::ForkTableEntry {
                    name: name.to_string(),
                    table_id,
                    created_at_ns: spec.created_at_ns,
                    spec_revision: spec.schema_revision,
                    origin: None,
                    checksum: String::new(),
                }
                .seal()?;
                crate::fork::create_entry(&self.backend, &fork.name, &entry).await?;
            }
            None => {
                let entry = CatalogEntry {
                    name: name.to_string(),
                    table_id,
                    created_at_ns: spec.created_at_ns,
                    spec_revision: spec.schema_revision,
                    checksum: String::new(),
                }
                .seal()?;
                catalog::create_entry(&self.backend, &entry).await?;
            }
        }
        Ok(result)
    }

    /// Drop a table: remove the catalog entry, HEAD, and all objects.
    pub async fn drop_table(&self, name: &str) -> Result<()> {
        self.ensure_writable("drop_table")?;
        // Catalog mutations are serialized (3.5); HEAD removal below
        // additionally takes the table's writer lock so it cannot interleave
        // with an in-flight commit.
        let _meta = self.backend.meta_lock().await?;
        self.drop_table_locked(name).await
    }

    /// `drop_table` while the caller already holds the metadata lock.
    pub(crate) async fn drop_table_locked(&self, name: &str) -> Result<()> {
        let entry = self.entry(name).await?;

        if let Some(fork) = &self.fork {
            // Inside a fork, only the fork's own tables can be dropped.
            // Dropping a shadow is the "undo my edits to this table" move: the
            // name reverts to the pinned base view. Dropping a *base* table
            // from a fork would need a tombstone in the fork catalog, which
            // buys nothing an agent asked for — say so instead of guessing.
            let Some(fe) = crate::fork::load_entry(&self.backend, &fork.name, name).await? else {
                return Err(Error::Unsupported {
                    detail: format!(
                        "table {name:?} belongs to the base database; a fork cannot drop it \
                         (drop only tables the fork created or shadowed)"
                    ),
                });
            };
            crate::fork::remove_entry(&self.backend, &fork.name, name).await?;
            self.backend.heads.remove(fe.table_id).await?;
            let objects = self
                .backend
                .list(&layout::table_prefix(fe.table_id))
                .await?;
            self.backend
                .delete_many(objects.into_iter().map(|m| m.location).collect())
                .await?;
            return Ok(());
        }

        // Refuse to drop a table pinned by any snapshot.
        for snap in snapshot::list(&self.backend).await? {
            if snap.entries.contains_key(&entry.table_id) {
                return Err(Error::invalid(format!(
                    "table {name:?} is pinned by snapshot {:?}; delete the snapshot first",
                    snap.name
                )));
            }
        }
        // …or by any fork. Same rule, same reason: a pin is a GC root, and
        // dropping under one would strand a live workspace's reads. One index
        // read, not one read per fork (Part X, X-A1).
        if let Some((fork_name, _)) = self.fork_index().await?.first_pin(entry.table_id) {
            return Err(Error::invalid(format!(
                "table {name:?} is pinned by fork {fork_name:?}; drop the fork first"
            )));
        }
        catalog::remove_entry(&self.backend, name).await?;
        self.backend.heads.remove(entry.table_id).await?;
        let objects = self
            .backend
            .list(&layout::table_prefix(entry.table_id))
            .await?;
        self.backend
            .delete_many(objects.into_iter().map(|m| m.location).collect())
            .await?;
        Ok(())
    }

    /// Rename = catalog edit only; no data moves.
    pub async fn rename_table(&self, from: &str, to: &str) -> Result<()> {
        self.ensure_writable("rename_table")?;
        validate_table_name(to)?;
        let _meta = self.backend.meta_lock().await?;
        let mut entry = self.entry(from).await?;
        entry.name = to.to_string();
        let entry = entry.seal()?;
        // Atomic create of the target name (fails TableExists on a race),
        // then removal of the source: a crash in between leaves the table
        // reachable under both names, never under none.
        catalog::create_entry(&self.backend, &entry).await?;
        catalog::remove_entry(&self.backend, from).await?;
        Ok(())
    }

    /// Tables visible through this handle: the global catalog on a base
    /// handle, or the fork's own tables layered over its pinned base tables.
    pub async fn list_tables(&self) -> Result<Vec<CatalogEntry>> {
        let Some(fork) = &self.fork else {
            return catalog::list_entries(&self.backend).await;
        };
        let mut by_name: BTreeMap<String, CatalogEntry> = BTreeMap::new();
        // Built from the pins rather than by filtering the global catalog, so
        // a nested fork lists the tables its *parent* owned (ROADMAP X-C1) and
        // so the listing costs no catalog read at all. A table created on main
        // after the fork is still invisible, for the same reason as before: a
        // fork is a frozen base, and a name appearing mid-run would make its
        // reads unreproducible.
        let mut needs_catalog = false;
        for (table_id, pin) in &fork.pins {
            match (pin.spec_revision, pin.created_at_ns) {
                (Some(spec_revision), Some(created_at_ns)) => {
                    by_name.insert(
                        pin.table_name.clone(),
                        CatalogEntry {
                            name: pin.table_name.clone(),
                            table_id: *table_id,
                            created_at_ns,
                            spec_revision,
                            checksum: String::new(),
                        }
                        .seal()?,
                    );
                }
                // Pre-X-C1 fork: fall back to the catalog for the whole set.
                _ => needs_catalog = true,
            }
        }
        if needs_catalog {
            for base in catalog::list_entries(&self.backend).await? {
                if fork.pin(base.table_id).is_some() {
                    by_name.insert(base.name.clone(), base);
                }
            }
        }
        // Fork-owned entries win: a shadow replaces the base table it shadows.
        for fe in crate::fork::list_entries(&self.backend, &fork.name).await? {
            by_name.insert(fe.name.clone(), fe.to_catalog_entry()?);
        }
        Ok(by_name.into_values().collect())
    }

    /// The fork catalog entry backing `name`, if this handle is in a fork and
    /// the fork owns that name.
    pub(crate) async fn fork_entry(
        &self,
        name: &str,
    ) -> Result<Option<crate::fork::ForkTableEntry>> {
        match &self.fork {
            None => Ok(None),
            Some(fork) => crate::fork::load_entry(&self.backend, &fork.name, name).await,
        }
    }

    /// Resolve a name in this handle's visible namespace, or `None`.
    ///
    /// Kept separate from [`Self::entry`] because a *miss* is not always a
    /// failure: `create_table` expects one, and the did-you-mean listing
    /// `entry` pays for on the error path would make creating a table O(all
    /// tables) — quadratic over a batch of creates.
    pub(crate) async fn entry_opt(&self, name: &str) -> Result<Option<CatalogEntry>> {
        let Some(fork) = &self.fork else {
            return catalog::load_entry(&self.backend, name).await;
        };
        if let Some(fe) = crate::fork::load_entry(&self.backend, &fork.name, name).await? {
            return Ok(Some(fe.to_catalog_entry()?));
        }
        // The *pin* answers what a name meant when this fork was made, and it
        // is the only thing that can: a nested fork's parent may already have
        // shadowed the name, so the global catalog would hand back the base
        // table the parent stopped using (ROADMAP X-C1).
        let Some((table_id, pin)) = fork.pin_by_name(name) else {
            return Ok(None);
        };
        match (pin.spec_revision, pin.created_at_ns) {
            (Some(spec_revision), Some(created_at_ns)) => Ok(Some(
                CatalogEntry {
                    name: name.to_string(),
                    table_id,
                    created_at_ns,
                    spec_revision,
                    checksum: String::new(),
                }
                .seal()?,
            )),
            // A fork written before pins carried these. Such a fork is
            // top-level by construction, so its tables are in the global
            // catalog and the pre-X-C1 lookup is still correct for it.
            _ => Ok(catalog::load_entry(&self.backend, name)
                .await?
                .filter(|base| base.table_id == table_id)),
        }
    }

    async fn entry(&self, name: &str) -> Result<CatalogEntry> {
        if let Some(entry) = self.entry_opt(name).await? {
            return Ok(entry);
        }
        // Miss: pay one catalog listing to turn "not found" into "did you mean
        // …". Only the error path does this, so the hit path is unchanged.
        let existing = self.list_tables().await.unwrap_or_default();
        Err(Error::table_not_found_among(
            name,
            existing.iter().map(|e| e.name.as_str()),
        ))
    }

    /// Reject an operation that only makes sense on the base database.
    ///
    /// These are the database-global roots (snapshots, retention, policy) plus
    /// fork management itself. Allowing them through a fork handle would let a
    /// speculative workspace mutate state its siblings depend on — precisely
    /// the coupling forks exist to remove.
    pub(crate) fn ensure_base(&self, op: &str) -> Result<()> {
        if let Some(fork) = &self.fork {
            return Err(Error::invalid(format!(
                "{op} operates on the whole database and is not available inside fork {:?}; \
                 run it on the base database",
                fork.name
            )));
        }
        Ok(())
    }

    pub(crate) async fn spec(&self, table_id: Uuid, revision: u32) -> Result<TableSpec> {
        let path = layout::spec_path(table_id, revision);
        let bytes = self
            .backend
            .get_opt(&path)
            .await?
            .ok_or_else(|| Error::corruption(path.as_ref(), "spec revision missing"))?;
        let spec: TableSpec = serde_json::from_slice(&bytes)
            .map_err(|e| Error::corruption(path.as_ref(), format!("spec parse: {e}")))?;
        spec.verify_checksum(path.as_ref())?;
        Ok(spec)
    }

    // ------------------------------------------------------------------
    // version resolution
    // ------------------------------------------------------------------

    pub(crate) async fn head(&self, name: &str, table_id: Uuid) -> Result<HeadState> {
        self.backend
            .heads
            .read(table_id)
            .await?
            .ok_or_else(|| Error::corruption(format!("tables/{table_id}/HEAD"), "missing HEAD"))
            .map_err(|e| match e {
                // A cataloged table without HEAD is corruption, but surface
                // the table name for the operator.
                Error::Corruption { object, detail } => Error::Corruption {
                    object: format!("{object} (table {name:?})"),
                    detail,
                },
                other => other,
            })
    }

    pub(crate) async fn manifest_at(
        &self,
        table_id: Uuid,
        sequence: u64,
    ) -> Result<VersionManifest> {
        let path = layout::manifest_path(table_id, sequence);
        let bytes = self.backend.get_opt(&path).await?.ok_or_else(|| {
            Error::corruption(path.as_ref(), "manifest missing for committed sequence")
        })?;
        VersionManifest::from_bytes(&bytes, path.as_ref())
    }

    /// The version this handle treats as "latest" for a table, plus the
    /// checksum that anchors trust in it.
    ///
    /// On a base handle that is the table's HEAD. Inside a fork, for a *base*
    /// table, it is the fork's pin — which is why a fork's reads neither
    /// observe nor contend with concurrent commits on main, and why a fork can
    /// look into the past but never into its own future. A fork-owned table
    /// (shadow or locally created) has its own HEAD and takes the normal path.
    pub(crate) async fn effective_head(
        &self,
        name: &str,
        entry: &CatalogEntry,
    ) -> Result<(u64, String)> {
        if let Some(fork) = &self.fork
            && let Some(pin) = fork.pin(entry.table_id)
        {
            return Ok((pin.sequence, pin.manifest_checksum.clone()));
        }
        let head = self.head(name, entry.table_id).await?;
        Ok((head.head.sequence, head.head.manifest_checksum))
    }

    /// Resolve a table at a given read point. The returned view is immutable:
    /// concurrent commits cannot affect it.
    pub async fn resolve(&self, name: &str, at: ReadAt) -> Result<ResolvedTable> {
        let entry = self.entry(name).await?;
        self.resolve_entry(entry, at).await
    }

    /// [`Self::resolve`] for a caller that already holds the catalog entry.
    ///
    /// A query session lists the whole catalog and then resolves each table,
    /// so resolving by name re-fetched, re-parsed and re-verified the very
    /// object the listing just produced — once per table, on every query.
    pub async fn resolve_entry(&self, entry: CatalogEntry, at: ReadAt) -> Result<ResolvedTable> {
        let name = entry.name.clone();
        let name = name.as_str();
        let (head_seq, head_checksum) = self.effective_head(name, &entry).await?;

        // The retention floor is read only by the arms that bound a read
        // against it. `Latest` — the default, and the hottest read in the
        // system — does not, and most tables have no RETENTION.json at all, so
        // fetching it unconditionally spent a round trip per scan on an object
        // that is usually a 404.
        let (sequence, verify_checksum) = match &at {
            ReadAt::Latest => (head_seq, Some(head_checksum.clone())),
            ReadAt::Version(v) => {
                let retention_floor = self.retention_min_seq(entry.table_id).await?;
                if *v < retention_floor || *v > head_seq {
                    return Err(Error::VersionNotFound {
                        table: name.into(),
                        requested: v.to_string(),
                        hint: format!("retained versions are {retention_floor}..={head_seq}"),
                    });
                }
                (*v, None)
            }
            ReadAt::AsOf(ts) => {
                let retention_floor = self.retention_min_seq(entry.table_id).await?;
                let seq = self
                    .as_of_sequence(entry.table_id, retention_floor, head_seq, *ts)
                    .await?;
                match seq {
                    Some(s) => (s, None),
                    None => {
                        return Err(Error::VersionNotFound {
                            table: name.into(),
                            requested: format!("as_of {ts}"),
                            hint: "timestamp precedes the oldest retained commit".into(),
                        });
                    }
                }
            }
            ReadAt::Snapshot(snap_name) => {
                let snap = snapshot::load(&self.backend, snap_name).await?;
                let se = snap.entries.get(&entry.table_id).ok_or_else(|| {
                    Error::invalid(format!(
                        "snapshot {snap_name:?} does not pin table {name:?}"
                    ))
                })?;
                (se.sequence, Some(se.manifest_checksum.clone()))
            }
        };

        // Integrity: HEAD (or snapshot) carries the manifest checksum. For
        // Version/AsOf reads no root of trust points at the manifest
        // directly, so verify it against its child's parent_checksum — a
        // one-hop slice of the chain that `verify` walks in full (3.6).
        let verify_checksum = match verify_checksum {
            Some(c) => Some(c),
            None if sequence == head_seq => Some(head_checksum.clone()),
            None => {
                self.manifest_at(entry.table_id, sequence + 1)
                    .await?
                    .parent_checksum
            }
        };
        let path = layout::manifest_path(entry.table_id, sequence);
        let bytes = self
            .backend
            .get_opt(&path)
            .await?
            .ok_or_else(|| Error::corruption(path.as_ref(), "manifest missing"))?;
        if let Some(expected) = verify_checksum {
            let actual = crate::util::checksum_hex(&bytes);
            if actual != expected {
                return Err(Error::corruption(
                    path.as_ref(),
                    format!("manifest checksum mismatch (expected {expected}, got {actual})"),
                ));
            }
        }
        let manifest = VersionManifest::from_bytes(&bytes, path.as_ref())?;
        let spec = self.spec(entry.table_id, manifest.schema_revision).await?;
        let schema = spec.schema()?;
        Ok(ResolvedTable {
            entry,
            spec,
            schema,
            manifest,
            head_sequence: head_seq,
        })
    }

    /// Largest sequence whose committed_at <= ts, via O(log V) binary search
    /// over directly-addressed manifests.
    async fn as_of_sequence(
        &self,
        table_id: Uuid,
        floor_seq: u64,
        head_seq: u64,
        ts: i64,
    ) -> Result<Option<u64>> {
        let mut lo = floor_seq;
        let mut hi = head_seq;
        // First check bounds to avoid degenerate loads.
        let first = self.manifest_at(table_id, floor_seq).await?;
        if ts < first.committed_at_ns {
            return Ok(None);
        }
        let last = self.manifest_at(table_id, head_seq).await?;
        if ts >= last.committed_at_ns {
            return Ok(Some(head_seq));
        }
        // Invariant: committed_at(lo) <= ts < committed_at(hi).
        while hi - lo > 1 {
            let mid = lo + (hi - lo) / 2;
            let m = self.manifest_at(table_id, mid).await?;
            if m.committed_at_ns <= ts {
                lo = mid;
            } else {
                hi = mid;
            }
        }
        Ok(Some(lo))
    }

    pub async fn list_versions(&self, name: &str) -> Result<Vec<VersionSummary>> {
        let entry = self.entry(name).await?;
        // Clamped to the *effective* head, so a fork's version list stops at
        // its pin rather than leaking commits main made after the fork.
        let (head_seq, _) = self.effective_head(name, &entry).await?;
        let retention_floor = self.retention_min_seq(entry.table_id).await?;
        let metas = self
            .backend
            .list(&layout::manifest_prefix(entry.table_id))
            .await?;
        let mut sequences: Vec<u64> = metas
            .iter()
            .filter_map(|m| layout::manifest_sequence_from_path(&m.location))
            .filter(|s| *s >= retention_floor && *s <= head_seq)
            .collect();
        sequences.sort_unstable();
        // One manifest read per version, and they are independent: a serial
        // loop costs one round trip per version, which is what makes a long
        // history expensive to list on object storage. Bounded concurrency
        // keeps the fan-out predictable; `buffered` preserves sequence order.
        use futures::StreamExt;
        let table_id = entry.table_id;
        let mut manifests = futures::stream::iter(
            sequences
                .into_iter()
                .map(|seq| async move { self.manifest_at(table_id, seq).await }),
        )
        .buffered(crate::backend::METADATA_FETCH_CONCURRENCY);
        let mut out = Vec::new();
        while let Some(m) = manifests.next().await {
            let m = m?;
            out.push(VersionSummary {
                sequence: m.sequence,
                op: m.op.to_string(),
                committed_at_ns: m.committed_at_ns,
                rows: m.rows,
                bytes: m.bytes,
                segments: m.segments.len(),
                note: m.note,
                // Carried here so an audit view does not have to re-resolve
                // every version one by one to learn how it was produced.
                execution_mode: m.execution_mode,
                plan_hash: m.plan_hash,
            });
        }
        Ok(out)
    }

    // ------------------------------------------------------------------
    // commit protocol
    // ------------------------------------------------------------------

    /// Publish `manifest` and swap HEAD, expecting `parent` as the current
    /// head state (None = first commit). Fills in parent linkage and the
    /// monotonic commit timestamp.
    async fn commit_manifest(
        &self,
        name: &str,
        table_id: Uuid,
        parent: Option<&HeadState>,
        manifest: &mut VersionManifest,
        segments_added: usize,
        known: CommitInputs,
    ) -> Result<CommitResult> {
        // Serialize every writer at the database level. Per-table HEAD CAS is
        // still the authority, while this outer lock lets a multi-table
        // transaction validate all bases and durably journal its roll-forward
        // before any ordinary writer can interleave. Object-store transactions
        // are rejected (their metadata guard is intentionally a no-op).
        let _meta = self.backend.meta_lock().await?;
        self.commit_manifest_locked(name, table_id, parent, manifest, segments_added, known)
            .await
    }

    /// Commit while the caller already holds the database metadata lock.
    pub(crate) async fn commit_manifest_locked(
        &self,
        name: &str,
        table_id: Uuid,
        parent: Option<&HeadState>,
        manifest: &mut VersionManifest,
        segments_added: usize,
        known: CommitInputs,
    ) -> Result<CommitResult> {
        // Segment-count guard rails.
        let spec_limit = match known.segment_limit {
            Some(limit) => limit,
            // spec may not exist yet during create_table's v0 commit
            None => self
                .spec(table_id, manifest.schema_revision)
                .await
                .map(|s| s.max_segments_per_manifest)
                .unwrap_or(crate::spec::SEGMENT_COUNT_HARD_DEFAULT),
        };
        if manifest.segments.len() > spec_limit {
            return Err(Error::LimitExceeded {
                detail: format!(
                    "manifest would reference {} segments (hard limit {spec_limit}); \
                     run `compact` first",
                    manifest.segments.len()
                ),
            });
        }
        if manifest.segments.len() > SEGMENT_COUNT_WARN {
            tracing::warn!(
                table = name,
                segments = manifest.segments.len(),
                "segment count is high; consider compaction"
            );
        }

        if let Some(p) = parent {
            manifest.parent = Some(p.head.sequence);
            manifest.parent_checksum = Some(p.head.manifest_checksum.clone());
            let parent_committed = match known.parent_committed_at_ns {
                Some(ts) => ts,
                None => {
                    self.manifest_at(table_id, p.head.sequence)
                        .await?
                        .committed_at_ns
                }
            };
            manifest.committed_at_ns = crate::util::monotonic_commit_ts(Some(parent_committed));
        } else {
            manifest.committed_at_ns = crate::util::monotonic_commit_ts(None);
        }
        manifest.recompute_rollups();

        let manifest_bytes = manifest.to_bytes()?;
        let manifest_checksum = crate::util::checksum_hex(&manifest_bytes);
        let manifest_path = layout::manifest_path(table_id, manifest.sequence);

        let new_head = Head {
            format: layout::FORMAT_VERSION,
            table_id,
            sequence: manifest.sequence,
            manifest_checksum: manifest_checksum.clone(),
        };

        self.hook("pre_publish")?;

        // Everything inside `publish` runs in the writer critical section,
        // after head revalidation.
        //
        // Durability (1.1): the segments this commit introduces are fsynced
        // *together with* the manifest before the head swap, so a committed
        // HEAD can never reference torn or unflushed Parquet objects after
        // power loss. Parent segments were made durable by their own commits.
        let backend = self.backend.clone();
        let hook = self.commit_hook.clone();
        let mp = manifest_path.clone();
        let manifest_sequence = new_head.sequence;
        let mut sync_paths: Vec<ObjPath> = manifest
            .segments
            .iter()
            .filter(|s| s.created_by_sequence == manifest.sequence)
            .map(|s| ObjPath::from(s.path.as_str()))
            .collect();
        let publish = Box::pin(async move {
            if backend.local_root.is_some() {
                backend.put(&mp, manifest_bytes.into()).await?;
            } else {
                crate::backend_object::create_manifest_slot(
                    &backend.store,
                    name,
                    table_id,
                    manifest_sequence,
                    manifest_bytes,
                )
                .await?;
            }
            if let Some(h) = &hook {
                h("post_manifest_put")?;
            }
            sync_paths.push(mp);
            backend.sync_objects(&sync_paths).await?;
            if let Some(h) = &hook {
                h("pre_head_swap")?;
            }
            Ok(())
        });

        let expected_tag = parent.map(|p| &p.tag);
        self.backend
            .heads
            .commit(table_id, name, expected_tag, &new_head, publish)
            .await?;
        self.hook("post_head_swap")?;

        Ok(CommitResult {
            table: name.to_string(),
            sequence: manifest.sequence,
            op: manifest.op.to_string(),
            rows_total: manifest.rows,
            segments_total: manifest.segments.len(),
            segments_added,
            segments_deduped: 0,
            committed_at_ns: manifest.committed_at_ns,
        })
    }

    /// Commit a manifest prepared by a `MutationPlan`: pure metadata CAS
    /// against the plan's base version. Segments were already uploaded at
    /// planning time.
    pub(crate) async fn commit_planned(
        &self,
        name: &str,
        table_id: Uuid,
        base_version: u64,
        base_manifest_checksum: &str,
        plan: &crate::plan::MutationPlan,
    ) -> Result<CommitResult> {
        self.ensure_writable("apply_plan")?;
        let head = self.head(name, table_id).await?;
        if head.head.sequence != base_version
            || head.head.manifest_checksum != base_manifest_checksum
        {
            return Err(Error::VersionConflict {
                table: name.into(),
                expected: base_version,
                actual: head.head.sequence,
            });
        }
        let mut manifest = VersionManifest {
            format: layout::FORMAT_VERSION,
            table_id,
            sequence: base_version + 1,
            parent: Some(base_version),
            parent_checksum: None, // filled by commit_manifest
            committed_at_ns: 0,    // filled by commit_manifest
            op: plan.op,
            execution_mode: Some("planned".to_string()),
            plan_hash: Some(plan.checksum.clone()),
            note: plan.note.clone(),
            user_meta: plan.user_meta.clone(),
            schema_revision: plan.schema_revision,
            rows: 0,
            bytes: 0,
            time_range: None,
            segments: plan.segments.clone(),
        };
        let mut res = self
            .commit_manifest(
                name,
                table_id,
                Some(&head),
                &mut manifest,
                plan.summary.segments_added,
                CommitInputs::default(),
            )
            .await?;
        res.segments_deduped = plan.summary.segments_reused;
        Ok(res)
    }

    /// Shared prologue for write-path ops: resolve entry/spec/head and check
    /// the caller's expected_version.
    /// If this mutation carries an idempotency key that a recent commit
    /// already recorded, return that commit instead of doing the work again.
    ///
    /// Deliberately a *read* of committed history rather than a lock or a
    /// reservation: the question "did my write land?" is answered by the
    /// version chain, which is the only thing that can answer it honestly
    /// after a crash. Costs nothing when no key is supplied.
    async fn idempotent_replay(
        &self,
        name: &str,
        opts: &WriteOptions,
    ) -> Result<Option<CommitResult>> {
        let Some(key) = opts.idempotency_key.as_deref() else {
            return Ok(None);
        };
        let entry = self.entry(name).await?;
        let head = self.head(name, entry.table_id).await?.head.sequence;
        let floor = head
            .saturating_sub(IDEMPOTENCY_LOOKBACK)
            .max(self.retention_min_seq(entry.table_id).await?);
        let mut seq = head;
        loop {
            let manifest = self.manifest_at(entry.table_id, seq).await?;
            if manifest
                .user_meta
                .get(IDEMPOTENCY_META_KEY)
                .and_then(|v| v.as_str())
                == Some(key)
            {
                return Ok(Some(CommitResult {
                    table: name.to_string(),
                    sequence: manifest.sequence,
                    op: manifest.op.to_string(),
                    rows_total: manifest.rows,
                    segments_total: manifest.segments.len(),
                    // The replay added nothing; saying otherwise would make a
                    // caller believe a second commit happened.
                    segments_added: 0,
                    segments_deduped: 0,
                    committed_at_ns: manifest.committed_at_ns,
                }));
            }
            if seq == 0 || seq <= floor {
                return Ok(None);
            }
            seq -= 1;
        }
    }

    /// Resolve a name for writing, materializing a copy-on-write shadow when
    /// this handle is in a fork and the name still points at the pinned base.
    ///
    /// This is the single place a fork's write path diverges from the base's.
    /// After it returns, every caller is holding an ordinary `CatalogEntry` for
    /// an ordinary table and the rest of the write path is fork-oblivious.
    pub(crate) async fn entry_for_write(&self, name: &str) -> Result<CatalogEntry> {
        let entry = self.entry(name).await?;
        let Some(fork) = &self.fork else {
            return Ok(entry);
        };
        // A pin keyed by this table_id means the name still resolves to the
        // base; anything else is already fork-owned and writable in place.
        if fork.pin(entry.table_id).is_none() {
            return Ok(entry);
        }
        self.materialize_shadow(name, &entry).await
    }

    /// Create the copy-on-write shadow backing `name` inside the current fork.
    ///
    /// Copies the pinned base manifest — a list of segment *metadata* — into a
    /// fresh `table_id`. No Parquet byte moves: the shadow's segments are the
    /// base's segments, referenced by path, kept alive by the fork's pin (see
    /// `fork.rs` for the refinement invariant this establishes).
    async fn materialize_shadow(&self, name: &str, base: &CatalogEntry) -> Result<CatalogEntry> {
        let fork = self
            .fork
            .as_ref()
            .ok_or_else(|| Error::internal("materialize_shadow outside a fork"))?;
        let _meta = self.backend.meta_lock().await?;
        // Re-check under the lock: a concurrent writer in this same fork may
        // have materialized it already, and two shadows for one name would
        // silently split the fork's history.
        if let Some(existing) = crate::fork::load_entry(&self.backend, &fork.name, name).await? {
            return existing.to_catalog_entry();
        }
        let pin = fork.pin(base.table_id).ok_or_else(|| {
            Error::internal(format!("fork {:?} does not pin table {name:?}", fork.name))
        })?;

        // Trust the pin before copying from it: a mismatch here means the base
        // manifest changed under an immutable reference, which must never be
        // propagated into a new table.
        let base_path = layout::manifest_path(base.table_id, pin.sequence);
        let base_bytes = self
            .backend
            .get_opt(&base_path)
            .await?
            .ok_or_else(|| Error::corruption(base_path.as_ref(), "pinned manifest missing"))?;
        let actual = crate::util::checksum_hex(&base_bytes);
        if actual != pin.manifest_checksum {
            return Err(Error::corruption(
                base_path.as_ref(),
                format!(
                    "fork {:?} pins version {} with checksum {}, found {actual}",
                    fork.name, pin.sequence, pin.manifest_checksum
                ),
            ));
        }
        let base_manifest = VersionManifest::from_bytes(&base_bytes, base_path.as_ref())?;

        let table_id = Uuid::new_v4();

        // The shadow's spec is the base's spec at the pinned schema revision,
        // re-keyed to the new table. Only that one revision is copied: every
        // manifest the shadow will ever hold is at that revision or later.
        let mut spec = self
            .spec(base.table_id, base_manifest.schema_revision)
            .await?;
        spec.table_id = table_id;
        spec.checksum = String::new();
        spec.checksum = spec.compute_checksum()?;
        let spec_path = layout::spec_path(table_id, spec.schema_revision);
        self.backend
            .put(&spec_path, serde_json::to_vec_pretty(&spec)?.into())
            .await?;
        self.backend.sync_objects(&[spec_path]).await?;

        // Version 0 of the shadow *is* the pinned base version's content.
        let mut manifest = base_manifest.clone();
        manifest.table_id = table_id;
        manifest.sequence = 0;
        manifest.parent = None;
        manifest.parent_checksum = None;
        manifest.op = OpKind::Create;
        manifest.execution_mode = Some("direct".to_string());
        manifest.plan_hash = None;
        manifest.note = Some(format!(
            "fork {:?}: shadow of {name:?} at base version {}",
            fork.name, pin.sequence
        ));
        // Provenance in an immutable object, so it outlives the fork itself.
        manifest.user_meta.insert(
            crate::fork::FORKED_FROM_META_KEY.to_string(),
            serde_json::json!({
                "fork": fork.name,
                "base_table_id": base.table_id,
                "base_sequence": pin.sequence,
                "base_manifest_checksum": pin.manifest_checksum,
            }),
        );

        // The inherited set must satisfy the refinement invariant by
        // construction; assert it rather than assume it, because this is one
        // of only two places a cross-table segment path is ever introduced.
        let own_prefix = format!("{}/", layout::table_prefix(table_id));
        let base_paths: BTreeSet<String> = base_manifest
            .segments
            .iter()
            .map(|s| s.path.clone())
            .collect();
        crate::fork::check_refinement(
            &own_prefix,
            &base_paths,
            manifest.segments.iter().map(|s| s.path.as_str()),
        )?;

        manifest.recompute_rollups();
        self.commit_manifest_locked(
            name,
            table_id,
            None,
            &mut manifest,
            0,
            CommitInputs {
                segment_limit: Some(spec.max_segments_per_manifest),
                parent_committed_at_ns: None,
            },
        )
        .await?;

        let entry = crate::fork::ForkTableEntry {
            name: name.to_string(),
            table_id,
            created_at_ns: crate::util::monotonic_commit_ts(None),
            spec_revision: spec.schema_revision,
            origin: Some(crate::fork::ForkOrigin {
                base_table_id: base.table_id,
                base_sequence: pin.sequence,
                base_manifest_checksum: pin.manifest_checksum.clone(),
            }),
            checksum: String::new(),
        }
        .seal()?;
        // Catalog entry last: a crash before this leaves an unreachable table
        // directory (collectible), never a half-registered shadow.
        crate::fork::create_entry(&self.backend, &fork.name, &entry).await?;
        entry.to_catalog_entry()
    }

    async fn write_prologue(
        &self,
        name: &str,
        op: OpKind,
        opts: &WriteOptions,
    ) -> Result<(CatalogEntry, TableSpec, HeadState, VersionManifest)> {
        self.ensure_writable(&op.to_string())?;
        // Policy gate: direct mutations may be forbidden per operation; the
        // reviewed plan/apply path (commit_planned) is always allowed.
        crate::policy::load(&self.backend).await?.check_direct(op)?;
        let entry = self.entry_for_write(name).await?;
        let head = self.head(name, entry.table_id).await?;
        if let Some(expected) = opts.expected_version
            && head.head.sequence != expected
        {
            return Err(Error::VersionConflict {
                table: name.into(),
                expected,
                actual: head.head.sequence,
            });
        }
        let manifest = self.manifest_at(entry.table_id, head.head.sequence).await?;
        let spec = self.spec(entry.table_id, manifest.schema_revision).await?;
        Ok((entry, spec, head, manifest))
    }

    /// Commit a metadata-only schema revision. Existing immutable segments
    /// remain in place and are adapted on read (nullable trailing columns are
    /// null-filled; supported numeric widenings are cast).
    pub async fn evolve_schema(
        &self,
        name: &str,
        new_schema: SchemaRef,
        opts: WriteOptions,
    ) -> Result<CommitResult> {
        self.ensure_writable("evolve_schema")?;
        let (entry, mut spec, head, parent_manifest) = self
            .write_prologue(name, OpKind::EvolveSchema, &opts)
            .await?;
        let old_schema = spec.schema()?;
        crate::evolution::validate_evolution(&old_schema, &new_schema)?;

        let _meta = self.backend.meta_lock().await?;
        let current = self.head(name, entry.table_id).await?;
        if current.tag != head.tag {
            return Err(Error::VersionConflict {
                table: name.into(),
                expected: head.head.sequence,
                actual: current.head.sequence,
            });
        }

        spec.schema_revision =
            spec.schema_revision
                .checked_add(1)
                .ok_or_else(|| Error::LimitExceeded {
                    detail: "schema revision overflow".into(),
                })?;
        spec.schema_ipc_b64 = crate::util::schema_to_b64(new_schema.as_ref());
        spec.checksum = spec.compute_checksum()?;
        let spec_path = layout::spec_path(entry.table_id, spec.schema_revision);
        self.backend
            .put(&spec_path, serde_json::to_vec_pretty(&spec)?.into())
            .await?;
        self.backend.sync_objects(&[spec_path]).await?;

        let next_seq = head.head.sequence + 1;
        let mut manifest = child_manifest(
            &parent_manifest,
            next_seq,
            OpKind::EvolveSchema,
            &opts,
            &spec,
        );
        manifest.segments = parent_manifest.segments.clone();
        self.commit_manifest_locked(
            name,
            entry.table_id,
            Some(&head),
            &mut manifest,
            0,
            CommitInputs {
                segment_limit: None,
                parent_committed_at_ns: Some(parent_manifest.committed_at_ns),
            },
        )
        .await
    }

    // ------------------------------------------------------------------
    // write operations
    // ------------------------------------------------------------------

    /// Replace the entire logical table. Input may be unsorted; it is sorted
    /// by the sort key in memory before segmentation.
    pub async fn write(
        &self,
        name: &str,
        batches: Vec<RecordBatch>,
        opts: WriteOptions,
    ) -> Result<CommitResult> {
        if let Some(replay) = self.idempotent_replay(name, &opts).await? {
            return Ok(replay);
        }
        let staged = self.stage_write(name, batches, &opts).await?;
        self.commit_staged(staged).await
    }

    pub(crate) async fn stage_write(
        &self,
        name: &str,
        batches: Vec<RecordBatch>,
        opts: &WriteOptions,
    ) -> Result<StagedCommit> {
        let (entry, spec, head, parent_manifest) =
            self.write_prologue(name, OpKind::Write, opts).await?;
        let schema = spec.schema()?;
        validate_batches_schema(&schema, &batches)?;
        validate_time_column(&spec, &batches)?;
        // Opt-in data-safety policy: reject the write if any row violates it
        // (no-op when the table has no policy).
        self.enforce_data_policy(entry.table_id, &batches).await?;

        let next_seq = head.head.sequence + 1;
        let mut writer = SegmentWriter::new(&self.backend, &spec, schema.clone(), next_seq);
        if spec.sort_key.is_empty() {
            for b in batches {
                writer.push(b).await?;
            }
        } else {
            // Chunked sort + k-way merge (2.4): sort each input batch, then
            // merge into bounded chunks — no full concatenation, and
            // `target_segment_bytes` actually splits the output.
            let sorted = crate::segment::sort_each_batch(&batches, &spec.sort_key)?;
            drop(batches);
            let mut merger =
                crate::segment::SortedBatchMerger::new(sorted, &spec.sort_key, MERGE_CHUNK_ROWS)?;
            while let Some(chunk) = merger.next_chunk()? {
                writer.push(chunk).await?;
            }
        }
        let (mut segments, _, lease) = writer.finish().await?;

        // Content-hash dedup against the parent version.
        let deduped = dedup_segments(&self.backend, &mut segments, &parent_manifest).await;

        let mut manifest = child_manifest(&parent_manifest, next_seq, OpKind::Write, opts, &spec);
        manifest.segments = segments;
        let added = manifest.segments.len() - deduped;
        Ok(StagedCommit {
            entry,
            head,
            manifest,
            segments_added: added,
            segments_deduped: deduped,
            known: CommitInputs {
                segment_limit: Some(spec.max_segments_per_manifest),
                parent_committed_at_ns: Some(parent_manifest.committed_at_ns),
            },
            lease,
        })
    }

    /// Strict ordered append: exact schema, input sorted by the sort key, and
    /// input min time >= current table max time.
    pub async fn append(
        &self,
        name: &str,
        batches: Vec<RecordBatch>,
        opts: WriteOptions,
    ) -> Result<CommitResult> {
        if let Some(replay) = self.idempotent_replay(name, &opts).await? {
            return Ok(replay);
        }
        self.append_inner(name, batches, opts, true).await
    }

    async fn append_inner(
        &self,
        name: &str,
        batches: Vec<RecordBatch>,
        opts: WriteOptions,
        auto_compact: bool,
    ) -> Result<CommitResult> {
        let staged = self
            .stage_append(name, batches, &opts, auto_compact)
            .await?;
        self.commit_staged(staged).await
    }

    pub(crate) async fn stage_append(
        &self,
        name: &str,
        batches: Vec<RecordBatch>,
        opts: &WriteOptions,
        auto_compact: bool,
    ) -> Result<StagedCommit> {
        let (entry, spec, head, parent_manifest) =
            self.write_prologue(name, OpKind::Append, opts).await?;
        let schema = spec.schema()?;
        validate_batches_schema(&schema, &batches)?;
        validate_time_column(&spec, &batches)?;
        // Opt-in data-safety policy (no-op when the table has no policy).
        self.enforce_data_policy(entry.table_id, &batches).await?;

        // Segment budget (3.13): fail — or compact — *before* uploading
        // anything; the commit-time check would only fire after the new
        // segments were already staged.
        if parent_manifest.segments.len() >= spec.max_segments_per_manifest {
            // At most ONE compaction attempt: if it cannot shrink the
            // segment count (nothing groupable), the retry below fails with
            // LimitExceeded instead of looping.
            let can_compact = auto_compact
                && opts.expected_version.is_none()
                && crate::policy::load(&self.backend)
                    .await?
                    .check_direct(OpKind::Compact)
                    .is_ok();
            if !can_compact {
                return Err(Error::LimitExceeded {
                    detail: format!(
                        "table already references {} segments (hard limit {}); \
                         run `compact` first",
                        parent_manifest.segments.len(),
                        spec.max_segments_per_manifest
                    ),
                });
            }
            tracing::warn!(
                table = name,
                segments = parent_manifest.segments.len(),
                "segment budget exhausted; compacting opportunistically before append"
            );
            self.compact(name, WriteOptions::default()).await?;
            // Head may have moved; restart against the compacted version.
            return Box::pin(self.stage_append(name, batches, opts, false)).await;
        }

        // Sortedness within and across input batches.
        if !spec.sort_key.is_empty() {
            let mut prev_last: Option<i64> = None;
            for b in &batches {
                if b.num_rows() == 0 {
                    continue;
                }
                if !batch_is_sorted(b, &spec.sort_key)? {
                    return Err(Error::SortOrderViolation {
                        table: name.into(),
                        detail: "append input batch is not sorted by the table sort key".into(),
                    });
                }
                if let Some(tc) = &spec.time_column {
                    // Batch is sorted, so min/max are first/last.
                    if let Some((bmin, bmax)) = crate::segment::time_min_max(b, tc)? {
                        if let Some(prev) = prev_last
                            && bmin < prev
                        {
                            return Err(Error::SortOrderViolation {
                                table: name.into(),
                                detail: "append input batches are not mutually ordered".into(),
                            });
                        }
                        prev_last = Some(bmax);
                    }
                }
            }
            // Input must start at or after the current table max.
            if let (Some((_, table_max)), Some(tc)) =
                (parent_manifest.time_range, &spec.time_column)
            {
                let input_min = batches
                    .iter()
                    .filter(|b| b.num_rows() > 0)
                    .map(|b| crate::segment::time_min_max(b, tc).map(|r| r.map(|(mn, _)| mn)))
                    .next()
                    .transpose()?
                    .flatten();
                if let Some(min) = input_min
                    && min < table_max
                {
                    return Err(Error::SortOrderViolation {
                        table: name.into(),
                        detail: format!(
                            "append input starts at {min} but the table already contains \
                                 rows up to {table_max}; use replace_range or write"
                        ),
                    });
                }
            }
        }

        let next_seq = head.head.sequence + 1;
        let mut writer = SegmentWriter::new(&self.backend, &spec, schema.clone(), next_seq);
        for b in batches {
            writer.push(b).await?;
        }
        let (mut new_segments, _, lease) = writer.finish().await?;
        let deduped = dedup_segments(&self.backend, &mut new_segments, &parent_manifest).await;

        let mut manifest = child_manifest(&parent_manifest, next_seq, OpKind::Append, opts, &spec);
        manifest.segments = parent_manifest.segments.clone();
        let added = new_segments.len() - deduped;
        manifest.segments.extend(new_segments);
        Ok(StagedCommit {
            entry,
            head,
            manifest,
            segments_added: added,
            segments_deduped: deduped,
            known: CommitInputs {
                segment_limit: Some(spec.max_segments_per_manifest),
                parent_committed_at_ns: Some(parent_manifest.committed_at_ns),
            },
            lease,
        })
    }

    async fn commit_staged(&self, mut staged: StagedCommit) -> Result<CommitResult> {
        let result = self
            .commit_manifest(
                &staged.entry.name,
                staged.entry.table_id,
                Some(&staged.head),
                &mut staged.manifest,
                staged.segments_added,
                staged.known,
            )
            .await;
        self.release_staging(staged.lease).await;
        let mut result = result?;
        result.segments_deduped = staged.segments_deduped;
        Ok(result)
    }

    pub(crate) async fn commit_staged_transaction(
        &self,
        mut staged: Vec<StagedCommit>,
    ) -> Result<Vec<CommitResult>> {
        if self.backend.local_root.is_none() {
            return Err(Error::Unsupported {
                detail: "multi-table transactions currently require the local backend".into(),
            });
        }
        let txn_id = Uuid::new_v4();
        let txn_path = crate::transaction::txn_path(txn_id);
        let result = self
            .commit_staged_transaction_inner(txn_id, &mut staged)
            .await;

        // Before the durable journal exists, failed staging is ordinary
        // unreachable debris and its leases can be released. Once journaled,
        // retain leases until open-time recovery completes the transaction.
        let journal_exists = self.backend.get_opt(&txn_path).await?.is_some();
        if result.is_ok() || !journal_exists {
            for commit in staged {
                self.release_staging(commit.lease).await;
            }
        }
        result
    }

    async fn commit_staged_transaction_inner(
        &self,
        txn_id: Uuid,
        staged: &mut [StagedCommit],
    ) -> Result<Vec<CommitResult>> {
        let _meta = self.backend.meta_lock().await?;

        // Validate every base while the global writer lock excludes ordinary
        // commits. A conflict aborts before a journal (the commit point) exists.
        for commit in staged.iter() {
            let current = self.backend.heads.read(commit.entry.table_id).await?;
            if current.as_ref().map(|h| &h.tag) != Some(&commit.head.tag) {
                return Err(Error::VersionConflict {
                    table: commit.entry.name.clone(),
                    expected: commit.head.head.sequence,
                    actual: current.map(|h| h.head.sequence).unwrap_or(0),
                });
            }
        }

        let mut new_heads = Vec::with_capacity(staged.len());
        let mut durable_paths = Vec::new();
        for commit in staged.iter_mut() {
            let spec = self
                .spec(commit.entry.table_id, commit.manifest.schema_revision)
                .await?;
            if commit.manifest.segments.len() > spec.max_segments_per_manifest {
                return Err(Error::LimitExceeded {
                    detail: format!(
                        "manifest would reference {} segments (hard limit {}); run `compact` first",
                        commit.manifest.segments.len(),
                        spec.max_segments_per_manifest
                    ),
                });
            }

            commit.manifest.parent = Some(commit.head.head.sequence);
            commit.manifest.parent_checksum = Some(commit.head.head.manifest_checksum.clone());
            let parent_committed = self
                .manifest_at(commit.entry.table_id, commit.head.head.sequence)
                .await?
                .committed_at_ns;
            commit.manifest.committed_at_ns =
                crate::util::monotonic_commit_ts(Some(parent_committed));
            commit.manifest.recompute_rollups();

            let bytes = commit.manifest.to_bytes()?;
            let manifest_checksum = crate::util::checksum_hex(&bytes);
            let manifest_path =
                layout::manifest_path(commit.entry.table_id, commit.manifest.sequence);
            self.backend.put(&manifest_path, bytes.into()).await?;
            durable_paths.extend(
                commit
                    .manifest
                    .segments
                    .iter()
                    .filter(|s| s.created_by_sequence == commit.manifest.sequence)
                    .map(|s| ObjPath::from(s.path.as_str())),
            );
            durable_paths.push(manifest_path);
            new_heads.push(Head {
                format: layout::FORMAT_VERSION,
                table_id: commit.entry.table_id,
                sequence: commit.manifest.sequence,
                manifest_checksum,
            });
        }
        self.backend.sync_objects(&durable_paths).await?;

        let journal = crate::transaction::TxnJournal {
            txn_id,
            created_at_ns: crate::util::monotonic_commit_ts(None),
            entries: staged
                .iter()
                .zip(&new_heads)
                .map(|(commit, new_head)| crate::transaction::TxnEntry {
                    table_id: commit.entry.table_id,
                    table_name: commit.entry.name.clone(),
                    base_sequence: commit.head.head.sequence,
                    new_head: new_head.clone(),
                })
                .collect(),
            checksum: String::new(),
        }
        .seal()?;
        let journal_path = crate::transaction::txn_path(txn_id);
        self.backend
            .put(&journal_path, serde_json::to_vec_pretty(&journal)?.into())
            .await?;
        self.backend
            .sync_objects(std::slice::from_ref(&journal_path))
            .await?;

        let mut results = Vec::with_capacity(staged.len());
        for (commit, new_head) in staged.iter().zip(new_heads) {
            self.backend
                .heads
                .commit(
                    commit.entry.table_id,
                    &commit.entry.name,
                    Some(&commit.head.tag),
                    &new_head,
                    Box::pin(async { Ok(()) }),
                )
                .await?;
            results.push(CommitResult {
                table: commit.entry.name.clone(),
                sequence: commit.manifest.sequence,
                op: commit.manifest.op.to_string(),
                rows_total: commit.manifest.rows,
                segments_total: commit.manifest.segments.len(),
                segments_added: commit.segments_added,
                segments_deduped: commit.segments_deduped,
                committed_at_ns: commit.manifest.committed_at_ns,
            });
        }
        self.backend.delete(&journal_path).await?;
        Ok(results)
    }

    /// Append with automatic rebase on `VersionConflict` (safe for pure
    /// appends: new segments never overlap other writers' commits logically,
    /// so the rebase is a re-validate + re-point, not a rewrite).
    pub async fn append_with_retry(
        &self,
        name: &str,
        batches: Vec<RecordBatch>,
        opts: WriteOptions,
        max_retries: usize,
    ) -> Result<CommitResult> {
        let mut attempt = 0;
        loop {
            match self.append(name, batches.clone(), opts.clone()).await {
                // LockTimeout is classified retryable and races exactly like
                // a conflict (another writer held the section) — retry both.
                Err(Error::VersionConflict { .. }) | Err(Error::LockTimeout { .. })
                    if attempt < max_retries =>
                {
                    attempt += 1;
                    tokio::time::sleep(std::time::Duration::from_millis(
                        10 * (1 << attempt.min(6)) as u64,
                    ))
                    .await;
                }
                other => return other,
            }
        }
    }

    /// Replace all rows in `[start, end)` (raw time units) with `new_batches`
    /// (which must lie inside the range). Boundary segments are rewritten;
    /// untouched segments are shared with the parent version.
    pub async fn replace_range(
        &self,
        name: &str,
        start: i64,
        end: i64,
        new_batches: Vec<RecordBatch>,
        opts: WriteOptions,
    ) -> Result<CommitResult> {
        self.replace_range_impl(name, start, end, new_batches, opts, OpKind::ReplaceRange)
            .await
    }

    /// Delete all rows in `[start, end)`.
    pub async fn delete_range(
        &self,
        name: &str,
        start: i64,
        end: i64,
        opts: WriteOptions,
    ) -> Result<CommitResult> {
        self.replace_range_impl(name, start, end, vec![], opts, OpKind::DeleteRange)
            .await
    }

    async fn replace_range_impl(
        &self,
        name: &str,
        start: i64,
        end: i64,
        new_batches: Vec<RecordBatch>,
        opts: WriteOptions,
        op: OpKind,
    ) -> Result<CommitResult> {
        if let Some(replay) = self.idempotent_replay(name, &opts).await? {
            return Ok(replay);
        }
        if start >= end {
            return Err(Error::invalid(format!(
                "empty range: start {start} must be < end {end}"
            )));
        }
        let (entry, spec, head, parent_manifest) = self.write_prologue(name, op, &opts).await?;
        let tc = spec.time_column.clone().ok_or_else(|| Error::Unsupported {
            detail: format!("{op} requires a table with a time column"),
        })?;
        let schema = spec.schema()?;
        validate_batches_schema(&schema, &new_batches)?;
        validate_time_column(&spec, &new_batches)?;
        // Opt-in data-safety policy on the replacement rows (no-op when unset).
        self.enforce_data_policy(entry.table_id, &new_batches)
            .await?;
        // New rows must fall inside the replaced range.
        for b in &new_batches {
            if b.num_rows() == 0 {
                continue;
            }
            for v in time_values_i64(b, &tc)? {
                if v < start || v >= end {
                    return Err(Error::invalid(format!(
                        "replacement row at time {v} falls outside [{start}, {end})"
                    )));
                }
            }
        }

        let next_seq = head.head.sequence + 1;
        let mut kept: Vec<SegmentMeta> = Vec::new();
        let mut boundary: Vec<SegmentMeta> = Vec::new();
        let mut dropped_whole = 0usize;
        for seg in &parent_manifest.segments {
            if !seg.overlaps_time(Some(start), Some(end)) {
                // Entirely outside the range: carried over untouched.
                kept.push(seg.clone());
            } else if seg.covered_by_time(start, end) {
                // Entirely *inside* it: every row is being replaced, so the
                // segment simply stops being referenced. It used to be read
                // and Parquet-decoded in full here, then filtered to zero
                // rows — for a wide range that is the whole table's bytes
                // moved to produce nothing.
                dropped_whole += 1;
            } else {
                // Straddles a boundary: must be rewritten minus the range.
                boundary.push(seg.clone());
            }
        }
        if dropped_whole > 0 {
            tracing::debug!(
                table = name,
                segments = dropped_whole,
                "range mutation dropped fully-covered segments without reading them"
            );
        }

        // Rewrite boundary segments minus the range, then add new data.
        let mut writer = SegmentWriter::new(&self.backend, &spec, schema.clone(), next_seq);
        for seg in &boundary {
            let batches = read_segment(&self.backend, seg, None, None).await?;
            // Keep rows OUTSIDE [start, end): t < start
            for b in
                crate::segment::filter_batches_by_time(batches.clone(), &tc, None, Some(start))?
            {
                writer.push(b).await?;
            }
            // and t >= end
            for b in crate::segment::filter_batches_by_time(batches, &tc, Some(end), None)? {
                writer.push(b).await?;
            }
        }
        if !new_batches.is_empty() {
            let sorted = sort_batches(&schema, &new_batches, &spec.sort_key)?;
            writer.push(sorted).await?;
        }
        let (mut rewritten, _, lease) = writer.finish().await?;
        let deduped = dedup_segments(&self.backend, &mut rewritten, &parent_manifest).await;

        let mut manifest = child_manifest(&parent_manifest, next_seq, op, &opts, &spec);
        manifest.segments = kept;
        let added = rewritten.len() - deduped;
        manifest.segments.extend(rewritten);
        let mut res = self
            .commit_manifest(
                name,
                entry.table_id,
                Some(&head),
                &mut manifest,
                added,
                CommitInputs {
                    segment_limit: Some(spec.max_segments_per_manifest),
                    parent_committed_at_ns: Some(parent_manifest.committed_at_ns),
                },
            )
            .await?;
        res.segments_deduped = deduped;
        self.release_staging(lease).await;
        Ok(res)
    }

    /// Make a historical version current by committing a new head that
    /// references the old segments. History is never rewound.
    pub async fn restore(
        &self,
        name: &str,
        version: u64,
        opts: WriteOptions,
    ) -> Result<CommitResult> {
        if let Some(replay) = self.idempotent_replay(name, &opts).await? {
            return Ok(replay);
        }
        let (entry, spec, head, parent_manifest) =
            self.write_prologue(name, OpKind::Restore, &opts).await?;
        if version > head.head.sequence {
            return Err(Error::VersionNotFound {
                table: name.into(),
                requested: version.to_string(),
                hint: format!("latest is {}", head.head.sequence),
            });
        }
        let target = self.manifest_at(entry.table_id, version).await?;
        let mut opts = opts;
        if opts.note.is_none() {
            opts.note = Some(format!("restore of version {version}"));
        }
        let mut manifest = child_manifest(
            &parent_manifest,
            head.head.sequence + 1,
            OpKind::Restore,
            &opts,
            &spec,
        );
        manifest.schema_revision = target.schema_revision;
        manifest.segments = target.segments;
        self.commit_manifest(
            name,
            entry.table_id,
            Some(&head),
            &mut manifest,
            0,
            CommitInputs {
                segment_limit: Some(spec.max_segments_per_manifest),
                parent_committed_at_ns: Some(parent_manifest.committed_at_ns),
            },
        )
        .await
    }

    // ------------------------------------------------------------------
    // reads
    // ------------------------------------------------------------------

    /// Collect matching batches. Returns the batches and a scan report.
    pub async fn scan(
        &self,
        name: &str,
        at: ReadAt,
        options: ScanOptions,
    ) -> Result<(Vec<RecordBatch>, ScanReport)> {
        let resolved = self.resolve(name, at).await?;
        self.scan_resolved(&resolved, options).await
    }

    pub async fn scan_resolved(
        &self,
        resolved: &ResolvedTable,
        options: ScanOptions,
    ) -> Result<(Vec<RecordBatch>, ScanReport)> {
        let (stream, mut report) = self.scan_stream_resolved(resolved, options)?;
        let batches: Vec<RecordBatch> = stream.try_collect().await?;
        report.rows_returned = batches.iter().map(|b| b.num_rows() as u64).sum();
        Ok((batches, report))
    }

    /// Streaming scan (2.4): batches are yielded as segments decode instead
    /// of being collected first, so memory stays bounded by
    /// `concurrency × segment size` regardless of result size.
    pub async fn scan_stream(
        &self,
        name: &str,
        at: ReadAt,
        options: ScanOptions,
    ) -> Result<(
        futures::stream::BoxStream<'static, Result<RecordBatch>>,
        ScanReport,
    )> {
        let resolved = self.resolve(name, at).await?;
        self.scan_stream_resolved(&resolved, options)
    }

    /// Streaming twin of [`Database::scan_resolved`]. The returned report
    /// carries the pruning counts up front; `rows_returned` stays 0 — the
    /// caller counts rows as it consumes the stream.
    pub fn scan_stream_resolved(
        &self,
        resolved: &ResolvedTable,
        options: ScanOptions,
    ) -> Result<(
        futures::stream::BoxStream<'static, Result<RecordBatch>>,
        ScanReport,
    )> {
        use futures::future;
        let spec = &resolved.spec;
        let time_filter_requested = options.time_start.is_some() || options.time_end.is_some();
        if time_filter_requested && spec.time_column.is_none() {
            return Err(Error::invalid(
                "time-range scan on a table without a time column",
            ));
        }

        // Prune segments by manifest time range.
        let mut report = ScanReport {
            segments_total: resolved.manifest.segments.len(),
            ..Default::default()
        };
        let survivors: Vec<SegmentMeta> = resolved
            .manifest
            .segments
            .iter()
            .filter(|s| {
                !time_filter_requested || s.overlaps_time(options.time_start, options.time_end)
            })
            .cloned()
            .collect();
        report.segments_scanned = survivors.len();
        report.segments_pruned = report.segments_total - survivors.len();
        report.bytes_scanned = survivors.iter().map(|s| s.bytes).sum();

        // If the projection excludes the time column but a filter needs it,
        // read it and drop it afterwards.
        let mut effective_projection = options.projection.clone();
        let mut drop_time_col = false;
        if let (Some(proj), Some(tc), true) = (
            &mut effective_projection,
            &spec.time_column,
            time_filter_requested,
        ) && !proj.contains(tc)
        {
            proj.push(tc.clone());
            drop_time_col = true;
        }

        let tc = spec.time_column.clone();
        let concurrency = options.concurrency.unwrap_or(4).max(1);
        let time_filter = if time_filter_requested {
            tc.as_deref()
                .map(|c| (c.to_string(), options.time_start, options.time_end))
        } else {
            None
        };

        let verify = options.verify_checksums;
        let backend = self.backend.clone();
        let target_schema = resolved.schema.clone();
        let target_revision = resolved.spec.schema_revision;
        let futures_iter = survivors.into_iter().map(move |seg| {
            let proj = effective_projection.clone();
            let tf = time_filter.clone();
            let backend = backend.clone();
            let target_schema = target_schema.clone();
            async move {
                let tf = tf.as_ref().map(|(c, s, e)| (c.as_str(), *s, *e));
                if seg.schema_revision != target_revision {
                    let batches = if verify {
                        crate::segment::read_segment_verified(&backend, &seg, None, tf).await?
                    } else {
                        read_segment(&backend, &seg, None, tf).await?
                    };
                    batches
                        .into_iter()
                        .map(|batch| {
                            let adapted = crate::evolution::adapt_batch(&target_schema, batch)?;
                            match &proj {
                                None => Ok(adapted),
                                Some(columns) => {
                                    let indices = columns
                                        .iter()
                                        .map(|name| {
                                            target_schema.index_of(name).map_err(Error::Arrow)
                                        })
                                        .collect::<Result<Vec<_>>>()?;
                                    adapted.project(&indices).map_err(Error::Arrow)
                                }
                            }
                        })
                        .collect()
                } else if verify {
                    crate::segment::read_segment_verified(&backend, &seg, proj.as_deref(), tf).await
                } else {
                    read_segment(&backend, &seg, proj.as_deref(), tf).await
                }
            }
        });

        let time_col = spec.time_column.clone();
        let limit = options.limit;
        let stream = stream::iter(futures_iter)
            .buffered(concurrency)
            .flat_map(|r: Result<Vec<RecordBatch>>| match r {
                Ok(batches) => stream::iter(batches.into_iter().map(Ok)).left_stream(),
                Err(e) => stream::once(future::ready(Err(e))).right_stream(),
            })
            .scan(0usize, move |rows, item| {
                let out = match item {
                    Err(e) => Some(Err(e)),
                    Ok(mut batch) => {
                        if let Some(lim) = limit {
                            if *rows >= lim {
                                return future::ready(None);
                            }
                            if *rows + batch.num_rows() > lim {
                                batch = batch.slice(0, lim - *rows);
                            }
                        }
                        *rows += batch.num_rows();
                        if drop_time_col {
                            match project_out(&batch, time_col.as_deref().unwrap()) {
                                Ok(b) => Some(Ok(b)),
                                Err(e) => Some(Err(e)),
                            }
                        } else {
                            Some(Ok(batch))
                        }
                    }
                };
                future::ready(out)
            })
            .filter(|r| {
                future::ready(match r {
                    Ok(b) => b.num_rows() > 0,
                    Err(_) => true,
                })
            });
        Ok((Box::pin(stream), report))
    }

    // ------------------------------------------------------------------
    // snapshots
    // ------------------------------------------------------------------

    /// Pin the current head of the given tables (all tables when empty)
    /// under a name.
    pub async fn create_snapshot(
        &self,
        name: &str,
        tables: &[String],
        note: Option<String>,
    ) -> Result<Snapshot> {
        self.ensure_writable("snapshot")?;
        // A snapshot is a database-global GC root; taking one from inside a
        // speculative workspace would pin tables main cannot see.
        self.ensure_base("snapshot")?;
        validate_table_name(name)?;
        // Snapshot creation is a catalog-level mutation (3.5): serialized so
        // the name-uniqueness check and the store cannot interleave (the
        // store itself is also an atomic create-if-absent).
        let _meta = self.backend.meta_lock().await?;
        let entries = if tables.is_empty() {
            self.list_tables().await?
        } else {
            let mut v = Vec::with_capacity(tables.len());
            for t in tables {
                v.push(self.entry(t).await?);
            }
            v
        };
        if entries.is_empty() {
            return Err(Error::invalid("cannot snapshot an empty database"));
        }
        let mut map = BTreeMap::new();
        for e in entries {
            let head = self.head(&e.name, e.table_id).await?;
            map.insert(
                e.table_id,
                SnapshotEntry {
                    table_name: e.name,
                    sequence: head.head.sequence,
                    manifest_checksum: head.head.manifest_checksum,
                },
            );
        }
        let snap = Snapshot {
            name: name.to_string(),
            created_at_ns: crate::util::monotonic_commit_ts(None),
            note,
            entries: map,
            checksum: String::new(),
        }
        .seal()?;
        snapshot::store(&self.backend, &snap).await?;
        Ok(snap)
    }

    pub async fn list_snapshots(&self) -> Result<Vec<Snapshot>> {
        snapshot::list(&self.backend).await
    }

    pub async fn delete_snapshot(&self, name: &str) -> Result<()> {
        self.ensure_writable("delete_snapshot")?;
        snapshot::delete(&self.backend, name).await
    }

    // ------------------------------------------------------------------
    // compaction
    // ------------------------------------------------------------------

    /// Rewrite runs of small segments into target-sized ones, using the
    /// table's configured target segment size. A no-op compaction returns the
    /// current head summary without committing a new version.
    pub async fn compact(&self, name: &str, opts: WriteOptions) -> Result<CommitResult> {
        self.compact_with(name, None, opts).await
    }

    /// `compact` with an explicit target for the rewritten segments'
    /// *in-memory* size (bytes). Overrides the table's configured target.
    pub async fn compact_with(
        &self,
        name: &str,
        target_bytes: Option<u64>,
        opts: WriteOptions,
    ) -> Result<CommitResult> {
        if let Some(replay) = self.idempotent_replay(name, &opts).await? {
            return Ok(replay);
        }
        let (entry, spec, head, parent_manifest) =
            self.write_prologue(name, OpKind::Compact, &opts).await?;
        let schema = spec.schema()?;
        let target = target_bytes.unwrap_or(spec.storage.target_segment_bytes);
        // Thresholds work on *encoded* bytes; encoded Parquet is typically
        // ~3x smaller than in-memory Arrow, so aim group sizes at target/3
        // and call a segment "small" below half of that.
        let target_encoded = (target / 3).max(1);
        let small_threshold = (target_encoded / 2).max(1);

        // Only segments this table owns are eligible. On a base table that is
        // every segment; on a fork's shadow it excludes the ones inherited from
        // the base, which makes "compaction never copies base bytes" a rule
        // rather than a happy accident of the size threshold (ROADMAP IX).
        // Rewriting an inherited segment would also duplicate it into the fork
        // for no gain: it is already target-sized, and the base copy stays
        // pinned regardless.
        let own_prefix = format!("{}/", layout::table_prefix(entry.table_id));
        let (owned, inherited): (Vec<SegmentMeta>, Vec<SegmentMeta>) = parent_manifest
            .segments
            .iter()
            .cloned()
            .partition(|s| crate::fork::is_own_segment(&own_prefix, &s.path));

        // Order segments by time (unknown ranges last) and find runs of
        // small segments.
        let mut ordered: Vec<SegmentMeta> = owned;
        ordered.sort_by_key(|s| s.time_range.map(|(min, _)| min).unwrap_or(i64::MAX));

        let mut groups: Vec<Vec<SegmentMeta>> = Vec::new();
        let mut current: Vec<SegmentMeta> = Vec::new();
        let mut current_bytes = 0u64;
        let mut untouched: Vec<SegmentMeta> = inherited;
        let close_current = |current: &mut Vec<SegmentMeta>,
                             untouched: &mut Vec<SegmentMeta>,
                             groups: &mut Vec<Vec<SegmentMeta>>| {
            if current.len() > 1 {
                groups.push(std::mem::take(current));
            } else {
                untouched.append(current);
            }
        };
        for seg in ordered {
            if seg.bytes < small_threshold {
                current_bytes += seg.bytes;
                current.push(seg);
                if current_bytes >= target_encoded {
                    close_current(&mut current, &mut untouched, &mut groups);
                    current_bytes = 0;
                }
            } else {
                close_current(&mut current, &mut untouched, &mut groups);
                current_bytes = 0;
                untouched.push(seg);
            }
        }
        close_current(&mut current, &mut untouched, &mut groups);

        if groups.is_empty() {
            // Nothing to do; report current state without a new version.
            return Ok(CommitResult {
                table: name.to_string(),
                sequence: head.head.sequence,
                op: "compact".into(),
                rows_total: parent_manifest.rows,
                segments_total: parent_manifest.segments.len(),
                segments_added: 0,
                segments_deduped: 0,
                committed_at_ns: parent_manifest.committed_at_ns,
            });
        }

        let next_seq = head.head.sequence + 1;
        let mut writer = SegmentWriter::new(&self.backend, &spec, schema.clone(), next_seq);
        for group in &groups {
            let mut batches: Vec<RecordBatch> = Vec::new();
            for seg in group {
                batches.extend(read_segment(&self.backend, seg, None, None).await?);
            }
            if spec.sort_key.is_empty() {
                for b in batches {
                    writer.push(b).await?;
                }
            } else {
                // Sort-each + k-way merge instead of concat + lexsort (2.4):
                // stored segments are typically already sorted, so this is
                // usually a pure merge with no per-batch sort at all.
                let sorted = crate::segment::sort_each_batch(&batches, &spec.sort_key)?;
                drop(batches);
                let mut merger = crate::segment::SortedBatchMerger::new(
                    sorted,
                    &spec.sort_key,
                    MERGE_CHUNK_ROWS,
                )?;
                while let Some(chunk) = merger.next_chunk()? {
                    writer.push(chunk).await?;
                }
            }
            // Flush per group so groups stay time-clustered.
            writer.flush().await?;
        }
        let (rewritten, _, lease) = writer.finish().await?;

        let mut manifest =
            child_manifest(&parent_manifest, next_seq, OpKind::Compact, &opts, &spec);
        manifest.segments = untouched;
        let added = rewritten.len();
        manifest.segments.extend(rewritten);
        manifest
            .segments
            .sort_by_key(|s| s.time_range.map(|(min, _)| min).unwrap_or(i64::MAX));

        // Compaction must preserve row count exactly.
        let new_rows: u64 = manifest.segments.iter().map(|s| s.rows).sum();
        if new_rows != parent_manifest.rows {
            return Err(Error::internal(format!(
                "compaction row-count mismatch: {} != {} — aborting commit",
                new_rows, parent_manifest.rows
            )));
        }
        let res = self
            .commit_manifest(
                name,
                entry.table_id,
                Some(&head),
                &mut manifest,
                added,
                CommitInputs {
                    segment_limit: Some(spec.max_segments_per_manifest),
                    parent_committed_at_ns: Some(parent_manifest.committed_at_ns),
                },
            )
            .await?;
        self.release_staging(lease).await;
        Ok(res)
    }

    /// Best-effort removal of a staging lease once its segments are reachable
    /// from a committed manifest (or a stored plan). Failure is harmless: the
    /// lease expires and vacuum collects it.
    async fn release_staging(&self, lease: Option<ObjPath>) {
        if let Some(path) = lease {
            let _ = self.backend.delete(&path).await;
        }
    }

    // ------------------------------------------------------------------
    // vacuum & verify
    // ------------------------------------------------------------------

    /// Remove unreachable objects (lost-CAS debris, orphaned segments from
    /// crashed writers, expired staging leases, orphaned table directories).
    /// Dry-run unless `apply` is set. Objects newer than `grace_seconds` are
    /// never touched, and staged-but-uncommitted segments are additionally
    /// protected by their staging lease regardless of age (3.4).
    pub async fn vacuum(
        &self,
        table: Option<&str>,
        grace_seconds: u64,
        apply: bool,
    ) -> Result<VacuumReport> {
        if apply {
            self.ensure_writable("vacuum")?;
        }
        // Vacuum reasons about reachability across the whole database, so it
        // runs from the base handle and treats every fork as a root. A fork's
        // own debris is reclaimed by `fork drop`, which deletes its tables
        // outright — there is no partial-reclamation story to get subtly wrong.
        self.ensure_base("vacuum")?;
        let all_entries = self.list_tables().await?;
        let entries = match table {
            Some(t) => vec![self.entry(t).await?],
            None => all_entries.clone(),
        };
        let mut report = VacuumReport {
            dry_run: !apply,
            ..Default::default()
        };
        let now = chrono::Utc::now();
        for entry in &entries {
            let head = self.head(&entry.name, entry.table_id).await?;
            let head_seq = head.head.sequence;
            let retention_floor = self.retention_min_seq(entry.table_id).await?;

            // Referenced set: every segment in every committed manifest,
            // plus segments staged by live (unexpired) mutation plans.
            let mut referenced: BTreeSet<String> = BTreeSet::new();
            for seq in retention_floor..=head_seq {
                let m = self.manifest_at(entry.table_id, seq).await?;
                for s in &m.segments {
                    referenced.insert(s.path.clone());
                }
            }
            referenced.extend(self.plan_protected_paths(entry.table_id).await?);

            // Staging leases: an unexpired lease protects its staged
            // segments no matter how old they are (large ingests stage long
            // before they commit); an expired lease is itself debris and its
            // segments fall through to normal orphan collection.
            let mut expired_leases: BTreeSet<String> = BTreeSet::new();
            for meta in self
                .backend
                .list(&layout::staging_prefix(entry.table_id))
                .await?
            {
                let bytes = self.backend.get(&meta.location).await?;
                let lease: crate::segment::StagingLeaseFile = serde_json::from_slice(&bytes)
                    .map_err(|e| {
                        // Fail closed: an unreadable lease aborts vacuum
                        // rather than risking collection of covered segments.
                        Error::corruption(
                            meta.location.as_ref(),
                            format!("staging lease parse: {e}"),
                        )
                    })?;
                if lease.is_expired() {
                    expired_leases.insert(meta.location.as_ref().to_string());
                } else {
                    referenced.extend(lease.segment_paths);
                }
            }

            let objects = self
                .backend
                .list(&layout::table_prefix(entry.table_id))
                .await?;
            // Collected and deleted in one batch below: deleting inside the
            // scan cost a round trip per object.
            let mut doomed: Vec<ObjPath> = Vec::new();
            for meta in objects {
                report.scanned_objects += 1;
                let loc = meta.location.as_ref();
                let age_ok = (now - meta.last_modified).num_seconds() >= grace_seconds as i64;
                if !age_ok {
                    continue;
                }
                let is_orphan_segment = loc.contains("/segments/") && !referenced.contains(loc);
                let is_uncommitted_manifest = loc.contains("/manifests/")
                    && layout::manifest_sequence_from_path(&meta.location)
                        .map(|s| s > head_seq)
                        .unwrap_or(true);
                let is_expired_manifest = loc.contains("/manifests/")
                    && layout::manifest_sequence_from_path(&meta.location)
                        .map(|s| s < retention_floor)
                        .unwrap_or(false);
                // NOTE: lock files are deliberately NOT debris — with the
                // flock-based writer lock (1.3), unlinking a held lock file
                // would let a later opener lock a fresh inode and break
                // mutual exclusion.
                let is_debris = loc.contains("HEAD.tmp") || expired_leases.contains(loc);
                if is_orphan_segment || is_uncommitted_manifest || is_expired_manifest || is_debris
                {
                    report.candidates.push(loc.to_string());
                    report.candidate_bytes += meta.size;
                    if apply {
                        doomed.push(meta.location.clone());
                    }
                }
            }
            report.deleted += doomed.len();
            self.backend.delete_many(doomed).await?;
        }

        // Orphaned table directories (3.4): a crashed create_table or a
        // lost drop race leaves a `tables/<uuid>/` dir no catalog entry
        // references — unreachable forever without this sweep. Snapshot-
        // pinned ids are protected, and a dir is only collected when EVERY
        // object in it is past the grace period (an in-flight create has
        // young objects).
        if table.is_none() {
            let mut cataloged: BTreeSet<Uuid> = all_entries.iter().map(|e| e.table_id).collect();
            let mut pinned: BTreeSet<Uuid> = BTreeSet::new();
            for snap in snapshot::list(&self.backend).await? {
                pinned.extend(snap.entries.keys().copied());
            }
            // Fork-owned tables are deliberately absent from the global
            // catalog — that is what keeps them invisible to main. Without
            // this they would read as orphaned directories and be deleted:
            // reachability must be computed over every catalog that exists,
            // not just the global one.
            //
            // Both root sets come from the fork index (Part X, X-A1), which
            // revalidates itself against storage on every read — a stale index
            // here would be exactly the data loss this sweep already avoids.
            let fork_index = self.fork_index().await?;
            pinned.extend(fork_index.pinned_table_ids());
            cataloged.extend(fork_index.owned_table_ids());
            let mut by_table: BTreeMap<Uuid, Vec<object_store::ObjectMeta>> = BTreeMap::new();
            for meta in self.backend.list(&ObjPath::from("tables")).await? {
                if let Some(id) = meta
                    .location
                    .parts()
                    .nth(1)
                    .and_then(|p| Uuid::parse_str(p.as_ref()).ok())
                {
                    by_table.entry(id).or_default().push(meta);
                }
            }
            for (id, metas) in by_table {
                if cataloged.contains(&id) || pinned.contains(&id) {
                    continue;
                }
                let all_old = metas
                    .iter()
                    .all(|m| (now - m.last_modified).num_seconds() >= grace_seconds as i64);
                if !all_old {
                    continue;
                }
                let mut doomed: Vec<ObjPath> = Vec::new();
                for meta in metas {
                    report.scanned_objects += 1;
                    report.candidates.push(meta.location.as_ref().to_string());
                    report.candidate_bytes += meta.size;
                    if apply {
                        doomed.push(meta.location);
                    }
                }
                report.deleted += doomed.len();
                self.backend.delete_many(doomed).await?;
            }
        }
        Ok(report)
    }

    /// Structural integrity check. `deep` additionally re-reads every segment
    /// and verifies its checksum.
    pub async fn verify(&self, name: &str, deep: bool) -> Result<VerifyReport> {
        let entry = self.entry(name).await?;
        let head = self.head(name, entry.table_id).await?;
        let retention_floor = self.retention_min_seq(entry.table_id).await?;
        let fork_entry = self.fork_entry(name).await?;

        let mut report = VerifyReport {
            table: name.to_string(),
            head_sequence: head.head.sequence,
            ..Default::default()
        };

        // Inside a fork, a shadow's manifest is the one place in the system
        // that references another table's storage. Re-derive the refinement
        // invariant below rather than trusting that the two writers who can
        // introduce such a path got it right: a violation is silent until the
        // base vacuums, and then it is data loss.
        //
        // Both inputs are loop-invariant — the origin is fixed for the whole
        // table — so they are built once here. Rebuilding them per version
        // re-read the base manifest and the base retention floor once for
        // every retained version of the shadow.
        let mut shadow_base: Option<(String, BTreeSet<String>)> = None;
        if let Some(origin) = fork_entry.as_ref().and_then(|fe| fe.origin.as_ref()) {
            let base_paths: BTreeSet<String> = self
                .manifest_at(origin.base_table_id, origin.base_sequence)
                .await?
                .segments
                .iter()
                .map(|s| s.path.clone())
                .collect();
            shadow_base = Some((
                format!("{}/", layout::table_prefix(entry.table_id)),
                base_paths,
            ));
            // The pin is what keeps those inherited segments alive, so a base
            // floor above it means they are already collectible. One check per
            // table, not per version.
            let base_floor = self.retention_min_seq(origin.base_table_id).await?;
            if base_floor > origin.base_sequence {
                report.problems.push(format!(
                    "base table retention floor {base_floor} is above this fork's pinned \
                     version {}; inherited segments are no longer protected",
                    origin.base_sequence
                ));
            }
        }

        // Verify manifests: checksum chain from head backwards.
        let mut expected_checksum = Some(head.head.manifest_checksum.clone());
        for seq in (retention_floor..=head.head.sequence).rev() {
            let path = layout::manifest_path(entry.table_id, seq);
            let bytes = match self.backend.get_opt(&path).await? {
                Some(b) => b,
                None => {
                    report
                        .problems
                        .push(format!("{}: manifest missing", path.as_ref()));
                    expected_checksum = None;
                    continue;
                }
            };
            report.manifests_checked += 1;
            let actual = crate::util::checksum_hex(&bytes);
            if let Some(exp) = &expected_checksum
                && &actual != exp
            {
                report.problems.push(format!(
                    "{}: checksum mismatch (chain expected {exp}, got {actual})",
                    path.as_ref()
                ));
            }
            let manifest = VersionManifest::from_bytes(&bytes, path.as_ref())?;
            expected_checksum = manifest.parent_checksum.clone();

            // Segment existence + size for the head version (and all
            // versions when deep).
            if deep || seq == head.head.sequence {
                for seg in &manifest.segments {
                    let seg_path = ObjPath::from(seg.path.as_str());
                    match self.backend.store.head(&seg_path).await {
                        Ok(meta) => {
                            report.segments_checked += 1;
                            if meta.size != seg.bytes {
                                report.problems.push(format!(
                                    "{}: size mismatch (manifest {} bytes, object {} bytes)",
                                    seg.path, seg.bytes, meta.size
                                ));
                            }
                            if deep {
                                let bytes = self.backend.get(&seg_path).await?;
                                report.bytes_checked += bytes.len() as u64;
                                let actual = crate::util::checksum_hex(&bytes);
                                if actual != seg.checksum {
                                    report
                                        .problems
                                        .push(format!("{}: content checksum mismatch", seg.path));
                                }
                            }
                        }
                        Err(object_store::Error::NotFound { .. }) => {
                            report
                                .problems
                                .push(format!("{}: segment object missing", seg.path));
                        }
                        Err(e) => return Err(Error::ObjectStore(e)),
                    }
                }
            }

            // Inside a fork, a shadow's manifest is the one place in the
            // system that references another table's storage. Re-derive the
            // refinement invariant here rather than trusting that the two
            // writers who can introduce such a path got it right: a violation
            // is silent until the base vacuums, and then it is data loss.
            if let Some((own_prefix, base_paths)) = &shadow_base
                && let Err(e) = crate::fork::check_refinement(
                    own_prefix,
                    base_paths,
                    manifest.segments.iter().map(|s| s.path.as_str()),
                )
            {
                report.problems.push(format!("version {seq}: {e}"));
            }
        }
        Ok(report)
    }
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

pub(crate) fn validate_table_name(name: &str) -> Result<()> {
    if name.is_empty() || name.len() > 256 {
        return Err(Error::invalid(
            "table/snapshot/fork names must be 1..=256 characters",
        ));
    }
    Ok(())
}

/// Exact schema check for append/replace inputs: same field names, types, and
/// no nullable input column feeding a non-nullable table column.
pub(crate) fn validate_batches_schema(schema: &SchemaRef, batches: &[RecordBatch]) -> Result<()> {
    for batch in batches {
        let got = batch.schema();
        if got.fields().len() != schema.fields().len() {
            return Err(Error::SchemaMismatch {
                detail: format!(
                    "expected {} columns, got {}",
                    schema.fields().len(),
                    got.fields().len()
                ),
            });
        }
        for (want, have) in schema.fields().iter().zip(got.fields()) {
            if want.name() != have.name() || want.data_type() != have.data_type() {
                return Err(Error::SchemaMismatch {
                    detail: format!(
                        "expected field {} {:?}, got {} {:?}",
                        want.name(),
                        want.data_type(),
                        have.name(),
                        have.data_type()
                    ),
                });
            }
            if !want.is_nullable() && have.is_nullable() {
                // Allowed only if the actual data has no nulls.
                let idx = got.index_of(have.name()).unwrap();
                if batch.column(idx).null_count() > 0 {
                    return Err(Error::SchemaMismatch {
                        detail: format!(
                            "column {} is non-nullable but input contains nulls",
                            want.name()
                        ),
                    });
                }
            }
        }
    }
    Ok(())
}

fn validate_time_column(spec: &TableSpec, batches: &[RecordBatch]) -> Result<()> {
    if let Some(tc) = &spec.time_column {
        for b in batches {
            if b.num_rows() > 0 {
                // time_values_i64 rejects nulls.
                let _ = time_values_i64(b, tc)?;
            }
        }
    }
    Ok(())
}

/// Start a child manifest inheriting identity fields from the parent.
fn child_manifest(
    parent: &VersionManifest,
    sequence: u64,
    op: OpKind,
    opts: &WriteOptions,
    spec: &TableSpec,
) -> VersionManifest {
    VersionManifest {
        format: layout::FORMAT_VERSION,
        table_id: parent.table_id,
        sequence,
        parent: Some(parent.sequence),
        parent_checksum: None, // filled by commit_manifest
        committed_at_ns: 0,    // filled by commit_manifest
        op,
        execution_mode: Some("direct".to_string()),
        plan_hash: None,
        note: opts.note.clone(),
        user_meta: {
            let mut meta = opts.user_meta.clone();
            // Recorded on the commit itself: the version chain is the only
            // record that survives the crash this guard exists for.
            if let Some(key) = &opts.idempotency_key {
                meta.insert(
                    crate::database::IDEMPOTENCY_META_KEY.to_string(),
                    serde_json::Value::String(key.clone()),
                );
            }
            meta
        },
        schema_revision: spec.schema_revision,
        rows: 0,
        bytes: 0,
        time_range: None,
        segments: vec![],
    }
}

/// Replace newly written segments identical (by content hash) to a parent
/// segment with a reference to the existing object, then delete each
/// redundant new object best-effort (a failed delete leaves an orphan for
/// vacuum). Returns how many were deduped.
pub(crate) async fn dedup_segments(
    backend: &Backend,
    new_segments: &mut [SegmentMeta],
    parent: &VersionManifest,
) -> usize {
    let by_hash = parent.segments_by_checksum();
    let mut deduped = 0;
    let mut redundant: Vec<String> = Vec::new();
    for seg in new_segments.iter_mut() {
        if let Some(existing) = by_hash.get(seg.checksum.as_str())
            && existing.bytes == seg.bytes
            && existing.rows == seg.rows
        {
            redundant.push(std::mem::replace(seg, (*existing).clone()).path);
            deduped += 1;
        }
    }
    for path in redundant {
        let _ = backend.delete(&ObjPath::from(path.as_str())).await;
    }
    deduped
}

/// Remove one column from a batch (used to drop an internally added time
/// column after filtering).
fn project_out(batch: &RecordBatch, column: &str) -> Result<RecordBatch> {
    let schema = batch.schema();
    let indices: Vec<usize> = (0..schema.fields().len())
        .filter(|&i| schema.field(i).name() != column)
        .collect();
    batch.project(&indices).map_err(Error::Arrow)
}
