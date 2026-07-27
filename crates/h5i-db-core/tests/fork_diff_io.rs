//! What `fork diff` is allowed to read (ROADMAP Part IX acceptance, pinned
//! here as part of Part X's X-A3 review).
//!
//! `fork_diff` reports row and byte deltas and the added/removed/shared
//! segment counts. Those are *data* answers, and the only reason they are
//! cheap is that segments are immutable and their row counts, byte sizes and
//! column statistics already live in the manifest — so the difference between
//! two forks is a set difference over segment paths, not a scan.
//!
//! Part IX claimed "diff reads manifests only (no segment I/O)" as an
//! acceptance criterion but never asserted it, which meant the property could
//! regress silently the first time someone reached for a number the manifest
//! did not have. The test below fails if any Parquet byte is read.

use std::sync::{Arc, Mutex};

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use h5i_db_core::{Backend, Database, TableOptions, WriteOptions};

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
    ]))
}

fn batch(ts: &[i64], symbol: &[&str], price: &[f64]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ts.to_vec())),
            Arc::new(StringArray::from(symbol.to_vec())),
            Arc::new(Float64Array::from(price.to_vec())),
        ],
    )
    .unwrap()
}

/// An `ObjectStore` that records the path of every object read.
///
/// Recording paths rather than a count is what lets the assertion name the
/// offending segment instead of just reporting a number that grew.
#[derive(Debug)]
struct PathRecordingStore {
    inner: Arc<dyn object_store::ObjectStore>,
    reads: Mutex<Vec<String>>,
}

impl PathRecordingStore {
    fn new(inner: Arc<dyn object_store::ObjectStore>) -> Self {
        Self {
            inner,
            reads: Mutex::new(Vec::new()),
        }
    }
    fn take(&self) -> Vec<String> {
        std::mem::take(&mut self.reads.lock().unwrap())
    }
}

impl std::fmt::Display for PathRecordingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PathRecordingStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl object_store::ObjectStore for PathRecordingStore {
    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.reads
            .lock()
            .unwrap()
            .push(location.as_ref().to_string());
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<
            'static,
            object_store::Result<object_store::path::Path>,
        >,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

async fn recorded_db() -> (tempfile::TempDir, Database, Arc<PathRecordingStore>) {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("db");
    std::fs::create_dir_all(&root).unwrap();
    let plain = Backend::local(&root).unwrap();
    let recorder = Arc::new(PathRecordingStore::new(plain.store.clone()));
    let db = Database::create_with_backend(Backend {
        store: recorder.clone(),
        heads: plain.heads,
        base_url: plain.base_url,
        local_root: plain.local_root,
    })
    .await
    .unwrap();
    (dir, db, recorder)
}

/// A fork that added rows, removed rows and still shares most of the base.
/// The diff over it must report real numbers and read no Parquet.
#[tokio::test]
async fn fork_diff_reports_row_and_byte_deltas_without_reading_a_segment() {
    let (_dir, db, recorder) = recorded_db().await;
    db.create_table(
        "prices",
        schema(),
        TableOptions {
            time_column: Some("ts".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    // Several segments, so "shared" is a meaningful count.
    for i in 0..4i64 {
        db.append(
            "prices",
            vec![batch(
                &[i * 10, i * 10 + 1],
                &["A", "B"],
                &[1.0 + i as f64, 2.0],
            )],
            WriteOptions::default(),
        )
        .await
        .unwrap();
    }

    db.create_fork("agent", None, None, serde_json::Map::new())
        .await
        .unwrap();
    let fork = db.open_fork("agent").await.unwrap();
    fork.append(
        "prices",
        vec![batch(&[1000, 1001], &["Z", "Z"], &[9.0, 9.0])],
        WriteOptions::default(),
    )
    .await
    .unwrap();

    recorder.take();
    let diff = db.fork_diff("agent", None).await.unwrap();
    let reads = recorder.take();

    // The numbers are real: the fork holds the base's rows plus its own.
    let t = &diff.tables[0];
    assert_eq!(t.table, "prices");
    assert_eq!(t.rows_base, 8, "four appends of two rows");
    assert_eq!(t.rows_fork, 10, "plus the fork's two");
    assert!(t.bytes_fork > t.bytes_base, "the fork added bytes");
    assert_eq!(t.segments_added, 1, "the fork wrote one segment");
    assert_eq!(t.segments_removed, 0);
    assert_eq!(t.segments_shared, 4, "the base's segments are all shared");

    // And nothing was read to learn them.
    let parquet: Vec<&String> = reads.iter().filter(|p| p.ends_with(".parquet")).collect();
    assert!(
        parquet.is_empty(),
        "fork diff read {} segment(s): {parquet:?}",
        parquet.len()
    );
}

/// The same guarantee for the case where a fork *removed* data: a
/// delete-to-empty is a legitimate branch outcome, and its diff must still be
/// answerable from manifests alone.
#[tokio::test]
async fn a_fork_that_deleted_rows_diffs_without_reading_segments() {
    let (_dir, db, recorder) = recorded_db().await;
    db.create_table(
        "prices",
        schema(),
        TableOptions {
            time_column: Some("ts".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    for i in 0..3i64 {
        db.append(
            "prices",
            vec![batch(&[i * 10, i * 10 + 1], &["A", "B"], &[1.0, 2.0])],
            WriteOptions::default(),
        )
        .await
        .unwrap();
    }
    db.create_fork("pruner", None, None, serde_json::Map::new())
        .await
        .unwrap();
    let fork = db.open_fork("pruner").await.unwrap();
    // Drop a whole segment's worth of time range.
    fork.delete_range("prices", 0, 10, WriteOptions::default())
        .await
        .unwrap();

    recorder.take();
    let diff = db.fork_diff("pruner", None).await.unwrap();
    let reads = recorder.take();

    let t = &diff.tables[0];
    assert!(
        t.rows_fork < t.rows_base,
        "the fork should hold fewer rows ({} vs {})",
        t.rows_fork,
        t.rows_base
    );
    assert!(
        t.segments_removed > 0,
        "dropping a whole segment should show as removed"
    );

    let parquet: Vec<&String> = reads.iter().filter(|p| p.ends_with(".parquet")).collect();
    assert!(
        parquet.is_empty(),
        "fork diff read {} segment(s) on the delete path: {parquet:?}",
        parquet.len()
    );
}
