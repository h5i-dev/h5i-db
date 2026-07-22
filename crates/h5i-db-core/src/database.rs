//! `Database`: open/create, table lifecycle, the commit protocol, version
//! resolution, scans, compaction, vacuum, and verify.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use futures::stream::{self, StreamExt};
use object_store::{path::Path as ObjPath, ObjectStoreExt};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::backend::{Backend, HeadState};
use crate::catalog::{self, CatalogEntry};
use crate::error::{Error, Result};
use crate::layout;
use crate::manifest::{Head, OpKind, SegmentMeta, VersionManifest};
use crate::segment::{batch_is_sorted, read_segment, sort_batches, time_values_i64, SegmentWriter};
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

    /// Open an existing database.
    pub async fn open(path: &Path) -> Result<Self> {
        Self::open_with(path, false).await
    }

    pub async fn open_read_only(path: &Path) -> Result<Self> {
        Self::open_with(path, true).await
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
        Ok(Self {
            backend,
            read_only,
            commit_hook: None,
        })
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
            .commit_manifest(name, table_id, None, &mut manifest, 0)
            .await?;

        let entry = CatalogEntry {
            name: name.to_string(),
            table_id,
            created_at_ns: spec.created_at_ns,
            spec_revision: spec.schema_revision,
            checksum: String::new(),
        }
        .seal()?;
        catalog::store_entry(&self.backend, &entry).await?;
        Ok(result)
    }

    /// Drop a table: remove the catalog entry, HEAD, and all objects.
    pub async fn drop_table(&self, name: &str) -> Result<()> {
        self.ensure_writable("drop_table")?;
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
        if catalog::load_entry(&self.backend, to).await?.is_some() {
            return Err(Error::TableExists { name: to.into() });
        }
        let mut entry = self.entry(from).await?;
        entry.name = to.to_string();
        let entry = entry.seal()?;
        catalog::store_entry(&self.backend, &entry).await?;
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

    async fn manifest_at(&self, table_id: Uuid, sequence: u64) -> Result<VersionManifest> {
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

        let (sequence, verify_checksum) = match &at {
            ReadAt::Latest => (head_seq, Some(head.head.manifest_checksum.clone())),
            ReadAt::Version(v) => {
                if *v > head_seq {
                    return Err(Error::VersionNotFound {
                        table: name.into(),
                        requested: v.to_string(),
                        hint: format!("latest is {head_seq}"),
                    });
                }
                (*v, None)
            }
            ReadAt::AsOf(ts) => {
                let seq = self.as_of_sequence(entry.table_id, head_seq, *ts).await?;
                match seq {
                    Some(s) => (s, None),
                    None => {
                        return Err(Error::VersionNotFound {
                            table: name.into(),
                            requested: format!("as_of {ts}"),
                            hint: "timestamp precedes the first commit".into(),
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

        // Integrity: HEAD (or snapshot) carries the manifest checksum.
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
    async fn as_of_sequence(&self, table_id: Uuid, head_seq: u64, ts: i64) -> Result<Option<u64>> {
        let mut lo = 0u64;
        let mut hi = head_seq;
        // First check bounds to avoid degenerate loads.
        let first = self.manifest_at(table_id, 0).await?;
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
        let metas = self
            .backend
            .list(&layout::manifest_prefix(entry.table_id))
            .await?;
        let mut sequences: Vec<u64> = metas
            .iter()
            .filter_map(|m| layout::manifest_sequence_from_path(&m.location))
            .filter(|s| *s <= head.head.sequence)
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
        let backend = self.backend.clone();
        let hook = self.commit_hook.clone();
        let mp = manifest_path.clone();
        let publish = Box::pin(async move {
            backend.put(&mp, manifest_bytes.into()).await?;
            if let Some(h) = &hook {
                h("post_manifest_put")?;
            }
            backend.sync_objects(&[mp]).await?;
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
        op: &str,
        opts: &WriteOptions,
    ) -> Result<(CatalogEntry, TableSpec, HeadState, VersionManifest)> {
        self.ensure_writable(op)?;
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
        let (entry, spec, head, parent_manifest) =
            self.write_prologue(name, "write", &opts).await?;
        let schema = spec.schema()?;
        validate_batches_schema(&schema, &batches)?;
        validate_time_column(&spec, &batches)?;

        let next_seq = head.head.sequence + 1;
        let sorted = if spec.sort_key.is_empty() {
            batches
        } else {
            vec![sort_batches(&schema, &batches, &spec.sort_key)?]
        };

        let mut writer = SegmentWriter::new(&self.backend, &spec, schema.clone(), next_seq);
        for b in sorted {
            writer.push(b).await?;
        }
        let (mut segments, _) = writer.finish().await?;

        // Content-hash dedup against the parent version.
        let deduped = dedup_segments(&mut segments, &parent_manifest);

        let mut manifest = child_manifest(&parent_manifest, next_seq, OpKind::Write, &opts, &spec);
        manifest.segments = segments;
        let added = manifest.segments.len() - deduped;
        let mut res = self
            .commit_manifest(name, entry.table_id, Some(&head), &mut manifest, added)
            .await?;
        res.segments_deduped = deduped;
        Ok(res)
    }

    /// Strict ordered append: exact schema, input sorted by the sort key, and
    /// input min time >= current table max time.
    pub async fn append(
        &self,
        name: &str,
        batches: Vec<RecordBatch>,
        opts: WriteOptions,
    ) -> Result<CommitResult> {
        let (entry, spec, head, parent_manifest) =
            self.write_prologue(name, "append", &opts).await?;
        let schema = spec.schema()?;
        validate_batches_schema(&schema, &batches)?;
        validate_time_column(&spec, &batches)?;

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
                    let vals = time_values_i64(b, tc)?;
                    if let (Some(prev), Some(first)) = (prev_last, vals.first()) {
                        if *first < prev {
                            return Err(Error::SortOrderViolation {
                                detail: "append input batches are not mutually ordered".into(),
                            });
                        }
                    }
                    prev_last = vals.last().copied();
                }
            }
            // Input must start at or after the current table max.
            if let (Some((_, table_max)), Some(tc)) =
                (parent_manifest.time_range, &spec.time_column)
            {
                let input_min = batches
                    .iter()
                    .filter(|b| b.num_rows() > 0)
                    .map(|b| time_values_i64(b, tc).map(|v| v[0]))
                    .next()
                    .transpose()?;
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
        let (mut new_segments, _) = writer.finish().await?;
        let deduped = dedup_segments(&mut new_segments, &parent_manifest);

        let mut manifest = child_manifest(&parent_manifest, next_seq, OpKind::Append, &opts, &spec);
        manifest.segments = parent_manifest.segments.clone();
        let added = new_segments.len() - deduped;
        manifest.segments.extend(new_segments);
        let mut res = self
            .commit_manifest(name, entry.table_id, Some(&head), &mut manifest, added)
            .await?;
        res.segments_deduped = deduped;
        Ok(res)
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
                Err(Error::VersionConflict { .. }) if attempt < max_retries => {
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
        let (entry, spec, head, parent_manifest) =
            self.write_prologue(name, &op.to_string(), &opts).await?;
        let tc = spec.time_column.clone().ok_or_else(|| Error::Unsupported {
            detail: format!("{op} requires a table with a time column"),
        })?;
        let schema = spec.schema()?;
        validate_batches_schema(&schema, &new_batches)?;
        validate_time_column(&spec, &new_batches)?;
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
        let (mut rewritten, _) = writer.finish().await?;
        let deduped = dedup_segments(&mut rewritten, &parent_manifest);

        let mut manifest = child_manifest(&parent_manifest, next_seq, op, &opts, &spec);
        manifest.segments = kept;
        let added = rewritten.len() - deduped;
        manifest.segments.extend(rewritten);
        let mut res = self
            .commit_manifest(name, entry.table_id, Some(&head), &mut manifest, added)
            .await?;
        res.segments_deduped = deduped;
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
            self.write_prologue(name, "restore", &opts).await?;
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

        let futures_iter = survivors.into_iter().map(|seg| {
            let proj = effective_projection.clone();
            let tf = time_filter.clone();
            let backend = self.backend.clone();
            async move {
                read_segment(
                    &backend,
                    &seg,
                    proj.as_deref(),
                    tf.as_ref().map(|(c, s, e)| (c.as_str(), *s, *e)),
                )
                .await
            }
        });
        let results: Vec<Result<Vec<RecordBatch>>> = stream::iter(futures_iter)
            .buffered(concurrency)
            .collect()
            .await;

        let mut out: Vec<RecordBatch> = Vec::new();
        let mut rows: usize = 0;
        'outer: for r in results {
            for mut batch in r? {
                if drop_time_col {
                    batch = project_out(&batch, spec.time_column.as_deref().unwrap())?;
                }
                if let Some(limit) = options.limit {
                    if rows + batch.num_rows() > limit {
                        let keep = limit - rows;
                        batch = batch.slice(0, keep);
                        rows += batch.num_rows();
                        if batch.num_rows() > 0 {
                            out.push(batch);
                        }
                        break 'outer;
                    }
                }
                rows += batch.num_rows();
                out.push(batch);
            }
        }
        report.rows_returned = rows as u64;
        Ok((out, report))
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
            self.write_prologue(name, "compact", &opts).await?;
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
        let mut close_current =
            |current: &mut Vec<SegmentMeta>,
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
            let sorted = sort_batches(&schema, &batches, &spec.sort_key)?;
            writer.push(sorted).await?;
            // Flush per group so groups stay time-clustered.
            writer.flush().await?;
        }
        let (rewritten, _) = writer.finish().await?;

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
        self.commit_manifest(name, entry.table_id, Some(&head), &mut manifest, added)
            .await
    }

    // ------------------------------------------------------------------
    // vacuum & verify
    // ------------------------------------------------------------------

    /// Remove unreachable objects (lost-CAS debris, orphaned segments from
    /// crashed writers). Dry-run unless `apply` is set. Objects newer than
    /// `grace_seconds` are never touched, protecting in-flight writers.
    pub async fn vacuum(
        &self,
        table: Option<&str>,
        grace_seconds: u64,
        apply: bool,
    ) -> Result<VacuumReport> {
        if apply {
            self.ensure_writable("vacuum")?;
        }
        let entries = match table {
            Some(t) => vec![self.entry(t).await?],
            None => self.list_tables().await?,
        };
        let mut report = VacuumReport {
            dry_run: !apply,
            ..Default::default()
        };
        let now = chrono::Utc::now();
        for entry in entries {
            let head = self.head(&entry.name, entry.table_id).await?;
            let head_seq = head.head.sequence;

            // Referenced set: every segment in every committed manifest,
            // plus segments staged by live (unexpired) mutation plans.
            let mut referenced: BTreeSet<String> = BTreeSet::new();
            for seq in 0..=head_seq {
                let m = self.manifest_at(entry.table_id, seq).await?;
                for s in &m.segments {
                    referenced.insert(s.path.clone());
                }
            }
            referenced.extend(self.plan_protected_paths(entry.table_id).await?);

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
                let is_debris = loc.ends_with(".lock") || loc.contains("HEAD.tmp");
                if is_orphan_segment || is_uncommitted_manifest || is_debris {
                    report.candidates.push(loc.to_string());
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
        let mut report = VerifyReport {
            table: name.to_string(),
            head_sequence: head.head.sequence,
            ..Default::default()
        };

        // Verify manifests: checksum chain from head backwards.
        let mut expected_checksum = Some(head.head.manifest_checksum.clone());
        for seq in (0..=head.head.sequence).rev() {
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
/// segment with a reference to the existing object; delete the redundant new
/// object best-effort. Returns how many were deduped.
pub(crate) fn dedup_segments(new_segments: &mut [SegmentMeta], parent: &VersionManifest) -> usize {
    let by_hash = parent.segments_by_checksum();
    let mut deduped = 0;
    for seg in new_segments.iter_mut() {
        if let Some(existing) = by_hash.get(seg.checksum.as_str()) {
            if existing.bytes == seg.bytes && existing.rows == seg.rows {
                *seg = (*existing).clone();
                deduped += 1;
            }
        }
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
