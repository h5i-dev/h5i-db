//! `Database`: open/create, table lifecycle, the commit protocol, version
//! resolution, scans, compaction, vacuum, and verify.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use futures::stream::{self, StreamExt, TryStreamExt};
use object_store::{path::Path as ObjPath, ObjectStoreExt};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::backend::{Backend, HeadState};
use crate::catalog::{self, CatalogEntry};
use crate::error::{Error, Result};
use crate::layout;
use crate::manifest::{Head, OpKind, SegmentMeta, VersionManifest};
use crate::segment::{
    batch_is_sorted, read_segment, sort_batches, time_values_i64, SegmentWriter, MERGE_CHUNK_ROWS,
};
use crate::snapshot::{self, Snapshot, SnapshotEntry};
use crate::spec::{TableOptions, TableSpec, SEGMENT_COUNT_WARN};

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
}

/// Options common to write-path operations.
#[derive(Debug, Clone, Default)]
pub struct WriteOptions {
    /// Require the current head to be exactly this sequence; `None` = the
    /// head observed when the operation started.
    pub expected_version: Option<u64>,
    pub note: Option<String>,
    pub user_meta: serde_json::Map<String, serde_json::Value>,
}

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
}

impl std::fmt::Debug for Database {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Database")
            .field("backend", &self.backend)
            .field("read_only", &self.read_only)
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
        if format.min_reader_version > layout::FORMAT_VERSION {
            return Err(Error::FormatTooNew {
                found: format.min_reader_version,
                supported: layout::FORMAT_VERSION,
            });
        }
        let db = Self {
            backend,
            read_only,
            commit_hook: None,
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
        if format.min_reader_version > layout::FORMAT_VERSION {
            return Err(Error::FormatTooNew {
                found: format.min_reader_version,
                supported: layout::FORMAT_VERSION,
            });
        }
        let db = Self {
            backend,
            read_only,
            commit_hook: None,
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

    fn ensure_writable(&self, op: &str) -> Result<()> {
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
        if catalog::load_entry(&self.backend, name).await?.is_some() {
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
            .commit_manifest_locked(name, table_id, None, &mut manifest, 0)
            .await?;

        let entry = CatalogEntry {
            name: name.to_string(),
            table_id,
            created_at_ns: spec.created_at_ns,
            spec_revision: spec.schema_revision,
            checksum: String::new(),
        }
        .seal()?;
        catalog::create_entry(&self.backend, &entry).await?;
        Ok(result)
    }

    /// Drop a table: remove the catalog entry, HEAD, and all objects.
    pub async fn drop_table(&self, name: &str) -> Result<()> {
        self.ensure_writable("drop_table")?;
        // Catalog mutations are serialized (3.5); HEAD removal below
        // additionally takes the table's writer lock so it cannot interleave
        // with an in-flight commit.
        let _meta = self.backend.meta_lock().await?;
        let entry = self.entry(name).await?;
        // Refuse to drop a table pinned by any snapshot.
        for snap in snapshot::list(&self.backend).await? {
            if snap.entries.contains_key(&entry.table_id) {
                return Err(Error::invalid(format!(
                    "table {name:?} is pinned by snapshot {:?}; delete the snapshot first",
                    snap.name
                )));
            }
        }
        catalog::remove_entry(&self.backend, name).await?;
        self.backend.heads.remove(entry.table_id).await?;
        let objects = self
            .backend
            .list(&layout::table_prefix(entry.table_id))
            .await?;
        for meta in objects {
            self.backend.delete(&meta.location).await?;
        }
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

    pub async fn list_tables(&self) -> Result<Vec<CatalogEntry>> {
        catalog::list_entries(&self.backend).await
    }

    async fn entry(&self, name: &str) -> Result<CatalogEntry> {
        catalog::load_entry(&self.backend, name)
            .await?
            .ok_or_else(|| Error::TableNotFound { name: name.into() })
    }

    async fn spec(&self, table_id: Uuid, revision: u32) -> Result<TableSpec> {
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

    async fn head(&self, name: &str, table_id: Uuid) -> Result<HeadState> {
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

    /// Resolve a table at a given read point. The returned view is immutable:
    /// concurrent commits cannot affect it.
    pub async fn resolve(&self, name: &str, at: ReadAt) -> Result<ResolvedTable> {
        let entry = self.entry(name).await?;
        let head = self.head(name, entry.table_id).await?;
        let head_seq = head.head.sequence;
        let retention_floor = self.retention_min_seq(entry.table_id).await?;

        let (sequence, verify_checksum) = match &at {
            ReadAt::Latest => (head_seq, Some(head.head.manifest_checksum.clone())),
            ReadAt::Version(v) => {
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
                        })
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
            None if sequence == head_seq => Some(head.head.manifest_checksum.clone()),
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
        let head = self.head(name, entry.table_id).await?;
        let retention_floor = self.retention_min_seq(entry.table_id).await?;
        let metas = self
            .backend
            .list(&layout::manifest_prefix(entry.table_id))
            .await?;
        let mut sequences: Vec<u64> = metas
            .iter()
            .filter_map(|m| layout::manifest_sequence_from_path(&m.location))
            .filter(|s| *s >= retention_floor && *s <= head.head.sequence)
            .collect();
        sequences.sort_unstable();
        let mut out = Vec::with_capacity(sequences.len());
        for seq in sequences {
            let m = self.manifest_at(entry.table_id, seq).await?;
            out.push(VersionSummary {
                sequence: m.sequence,
                op: m.op.to_string(),
                committed_at_ns: m.committed_at_ns,
                rows: m.rows,
                bytes: m.bytes,
                segments: m.segments.len(),
                note: m.note,
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
    ) -> Result<CommitResult> {
        // Serialize every writer at the database level. Per-table HEAD CAS is
        // still the authority, while this outer lock lets a multi-table
        // transaction validate all bases and durably journal its roll-forward
        // before any ordinary writer can interleave. Object-store transactions
        // are rejected (their metadata guard is intentionally a no-op).
        let _meta = self.backend.meta_lock().await?;
        self.commit_manifest_locked(name, table_id, parent, manifest, segments_added)
            .await
    }

    /// Commit while the caller already holds the database metadata lock.
    async fn commit_manifest_locked(
        &self,
        name: &str,
        table_id: Uuid,
        parent: Option<&HeadState>,
        manifest: &mut VersionManifest,
        segments_added: usize,
    ) -> Result<CommitResult> {
        // Segment-count guard rails.
        let spec_limit = {
            // spec may not exist yet during create_table's v0 commit
            self.spec(table_id, manifest.schema_revision)
                .await
                .map(|s| s.max_segments_per_manifest)
                .unwrap_or(crate::spec::SEGMENT_COUNT_HARD_DEFAULT)
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
            let parent_committed = self
                .manifest_at(table_id, p.head.sequence)
                .await?
                .committed_at_ns;
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
            )
            .await?;
        res.segments_deduped = plan.summary.segments_reused;
        Ok(res)
    }

    /// Shared prologue for write-path ops: resolve entry/spec/head and check
    /// the caller's expected_version.
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
        let entry = self.entry(name).await?;
        let head = self.head(name, entry.table_id).await?;
        if let Some(expected) = opts.expected_version {
            if head.head.sequence != expected {
                return Err(Error::VersionConflict {
                    table: name.into(),
                    expected,
                    actual: head.head.sequence,
                });
            }
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
        self.commit_manifest_locked(name, entry.table_id, Some(&head), &mut manifest, 0)
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
                        detail: "append input batch is not sorted by the table sort key".into(),
                    });
                }
                if let Some(tc) = &spec.time_column {
                    // Batch is sorted, so min/max are first/last.
                    if let Some((bmin, bmax)) = crate::segment::time_min_max(b, tc)? {
                        if let Some(prev) = prev_last {
                            if bmin < prev {
                                return Err(Error::SortOrderViolation {
                                    detail: "append input batches are not mutually ordered".into(),
                                });
                            }
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
                if let Some(min) = input_min {
                    if min < table_max {
                        return Err(Error::SortOrderViolation {
                            detail: format!(
                                "append input starts at {min} but the table already contains \
                                 rows up to {table_max}; use replace_range or write"
                            ),
                        });
                    }
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
        for seg in &parent_manifest.segments {
            if seg.overlaps_time(Some(start), Some(end)) {
                boundary.push(seg.clone());
            } else {
                kept.push(seg.clone());
            }
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
            .commit_manifest(name, entry.table_id, Some(&head), &mut manifest, added)
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
        self.commit_manifest(name, entry.table_id, Some(&head), &mut manifest, 0)
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
        ) {
            if !proj.contains(tc) {
                proj.push(tc.clone());
                drop_time_col = true;
            }
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
        let (entry, spec, head, parent_manifest) =
            self.write_prologue(name, OpKind::Compact, &opts).await?;
        let schema = spec.schema()?;
        let target = target_bytes.unwrap_or(spec.storage.target_segment_bytes);
        // Thresholds work on *encoded* bytes; encoded Parquet is typically
        // ~3x smaller than in-memory Arrow, so aim group sizes at target/3
        // and call a segment "small" below half of that.
        let target_encoded = (target / 3).max(1);
        let small_threshold = (target_encoded / 2).max(1);

        // Order segments by time (unknown ranges last) and find runs of
        // small segments.
        let mut ordered: Vec<SegmentMeta> = parent_manifest.segments.clone();
        ordered.sort_by_key(|s| s.time_range.map(|(min, _)| min).unwrap_or(i64::MAX));

        let mut groups: Vec<Vec<SegmentMeta>> = Vec::new();
        let mut current: Vec<SegmentMeta> = Vec::new();
        let mut current_bytes = 0u64;
        let mut untouched: Vec<SegmentMeta> = Vec::new();
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
            .commit_manifest(name, entry.table_id, Some(&head), &mut manifest, added)
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
                        self.backend.delete(&meta.location).await?;
                        report.deleted += 1;
                    }
                }
            }
        }

        // Orphaned table directories (3.4): a crashed create_table or a
        // lost drop race leaves a `tables/<uuid>/` dir no catalog entry
        // references — unreachable forever without this sweep. Snapshot-
        // pinned ids are protected, and a dir is only collected when EVERY
        // object in it is past the grace period (an in-flight create has
        // young objects).
        if table.is_none() {
            let cataloged: BTreeSet<Uuid> = all_entries.iter().map(|e| e.table_id).collect();
            let mut pinned: BTreeSet<Uuid> = BTreeSet::new();
            for snap in snapshot::list(&self.backend).await? {
                pinned.extend(snap.entries.keys().copied());
            }
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
                for meta in metas {
                    report.scanned_objects += 1;
                    report.candidates.push(meta.location.as_ref().to_string());
                    report.candidate_bytes += meta.size;
                    if apply {
                        self.backend.delete(&meta.location).await?;
                        report.deleted += 1;
                    }
                }
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
        let mut report = VerifyReport {
            table: name.to_string(),
            head_sequence: head.head.sequence,
            ..Default::default()
        };

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
            if let Some(exp) = &expected_checksum {
                if &actual != exp {
                    report.problems.push(format!(
                        "{}: checksum mismatch (chain expected {exp}, got {actual})",
                        path.as_ref()
                    ));
                }
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
        }
        Ok(report)
    }
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

fn validate_table_name(name: &str) -> Result<()> {
    if name.is_empty() || name.len() > 256 {
        return Err(Error::invalid(
            "table/snapshot names must be 1..=256 characters",
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
        user_meta: opts.user_meta.clone(),
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
        if let Some(existing) = by_hash.get(seg.checksum.as_str()) {
            if existing.bytes == seg.bytes && existing.rows == seg.rows {
                redundant.push(std::mem::replace(seg, (*existing).clone()).path);
                deduped += 1;
            }
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
