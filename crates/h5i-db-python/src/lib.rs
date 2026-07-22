//! Python native module for h5i-db.
//!
//! Interop is Arrow IPC streams (`bytes`) rather than pyo3↔pyarrow FFI
//! structs: it works with every pyarrow release, keeps this crate free of
//! version-locked bridge dependencies, and the copy it costs is one memcpy of
//! already-encoded buffers. The ergonomic API lives in the pure-Python
//! wrapper (`python/h5i_db/__init__.py`).
//!
//! Panic safety: wheels must be built with the `wheel` cargo profile
//! (`maturin build --profile wheel`, the default via `pyproject.toml`). It
//! inherits `release` but sets `panic = "unwind"` so pyo3 turns any residual
//! panic into a Python `PanicException` instead of aborting the host
//! interpreter — the workspace `release` profile is `panic = "abort"` and
//! must never be used for wheels.

use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use futures::StreamExt;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use h5i_db_core::{Database, Error, ReadAt, ScanOptions, TableOptions, WriteOptions};
use h5i_db_query::datafusion::error::DataFusionError;
use h5i_db_query::{H5iSession, SessionOptions};

// -- exceptions -------------------------------------------------------------
//
// One Python exception class per family of the core `{code, message,
// retryable, hint}` error envelope. Every raised instance carries `code`,
// `hint` and `retryable` attributes so callers can branch programmatically
// instead of parsing messages.

pyo3::create_exception!(
    _native,
    H5iError,
    pyo3::exceptions::PyException,
    "Base class for all h5i-db errors (attributes: code, hint, retryable)."
);
pyo3::create_exception!(
    _native,
    NotFoundError,
    H5iError,
    "Database, table, version or snapshot does not exist."
);
pyo3::create_exception!(
    _native,
    ConflictError,
    H5iError,
    "Concurrent-writer conflict or already-exists collision; usually retryable."
);
pyo3::create_exception!(
    _native,
    InvalidInputError,
    H5iError,
    "Bad argument, schema mismatch, sort-order violation or unsupported operation."
);
pyo3::create_exception!(
    _native,
    PolicyError,
    H5iError,
    "Operation forbidden by the mutation policy or a read-only handle."
);
pyo3::create_exception!(
    _native,
    CorruptionError,
    H5iError,
    "Checksum/format verification failed; data may be damaged or written by a newer h5i-db."
);
pyo3::create_exception!(
    _native,
    LimitError,
    H5iError,
    "A configured limit (memory, max_rows, segment count) was exceeded."
);
pyo3::create_exception!(
    _native,
    TimeoutError,
    H5iError,
    "The operation exceeded its deadline."
);
pyo3::create_exception!(
    _native,
    StorageError,
    H5iError,
    "Underlying storage / IO / encoding failure."
);

/// Attach the machine-readable envelope fields to an exception instance.
fn tagged(err: PyErr, code: &str, hint: Option<&str>, retryable: bool) -> PyErr {
    Python::attach(|py| {
        let value = err.value(py);
        let _ = value.setattr("code", code);
        let _ = value.setattr("hint", hint);
        let _ = value.setattr("retryable", retryable);
    });
    err
}

fn to_py_err(e: Error) -> PyErr {
    let code = e.code();
    let retryable = e.retryable();
    let hint = e.hint();
    let msg = format!(
        "[{code}] {e}{}",
        hint.as_deref()
            .map(|h| format!(" (hint: {h})"))
            .unwrap_or_default()
    );
    let err = match code {
        "database_not_found" | "table_not_found" | "version_not_found" | "snapshot_not_found" => {
            NotFoundError::new_err(msg)
        }
        "database_exists" | "table_exists" | "version_conflict" | "lock_timeout" => {
            ConflictError::new_err(msg)
        }
        "invalid_input" | "unsupported" | "schema_mismatch" | "sort_order_violation" => {
            InvalidInputError::new_err(msg)
        }
        "read_only" | "policy_violation" => PolicyError::new_err(msg),
        "corruption" | "format_too_new" => CorruptionError::new_err(msg),
        "limit_exceeded" => LimitError::new_err(msg),
        "timeout" => TimeoutError::new_err(msg),
        "storage" | "io" | "arrow" | "parquet" | "metadata" => StorageError::new_err(msg),
        _ => H5iError::new_err(msg),
    };
    tagged(err, code, hint.as_deref(), retryable)
}

fn df_err(e: DataFusionError) -> PyErr {
    let e = match e {
        // Unwrap so a core error surfaced through DataFusion keeps its code.
        DataFusionError::External(inner) => {
            return match inner.downcast::<Error>() {
                Ok(core) => to_py_err(*core),
                Err(other) => tagged(
                    H5iError::new_err(format!("[query] {other}")),
                    "query",
                    None,
                    false,
                ),
            }
        }
        DataFusionError::Context(_, inner) => return df_err(*inner),
        other => other,
    };
    let (exc, code): (fn(String) -> PyErr, &str) = match &e {
        DataFusionError::SQL(..) | DataFusionError::Plan(_) | DataFusionError::SchemaError(..) => {
            (|m| InvalidInputError::new_err(m), "invalid_input")
        }
        DataFusionError::ResourcesExhausted(_) => (|m| LimitError::new_err(m), "limit_exceeded"),
        _ => (|m| H5iError::new_err(m), "query"),
    };
    tagged(exc(format!("[{code}] {e}")), code, None, false)
}

fn invalid(msg: impl std::fmt::Display) -> PyErr {
    tagged(
        InvalidInputError::new_err(format!("[invalid_input] {msg}")),
        "invalid_input",
        None,
        false,
    )
}

fn encode_err(e: arrow::error::ArrowError) -> PyErr {
    tagged(
        StorageError::new_err(format!("[arrow] {e}")),
        "arrow",
        None,
        false,
    )
}

fn to_json<T: serde::Serialize>(v: &T) -> PyResult<String> {
    serde_json::to_string(v).map_err(|e| {
        tagged(
            StorageError::new_err(format!("[metadata] failed to encode result: {e}")),
            "metadata",
            None,
            false,
        )
    })
}

/// Encode batches as an Arrow IPC stream. The schema is always written, so
/// an empty result round-trips with its real schema instead of degrading to
/// a zero-column table.
fn batches_to_ipc(schema: &SchemaRef, batches: &[RecordBatch]) -> PyResult<Vec<u8>> {
    let mut buf = Vec::new();
    let mut writer =
        arrow::ipc::writer::StreamWriter::try_new(&mut buf, schema).map_err(encode_err)?;
    for b in batches {
        writer.write(b).map_err(encode_err)?;
    }
    writer.finish().map_err(encode_err)?;
    Ok(buf)
}

fn ipc_to_batches(bytes: &[u8]) -> PyResult<Vec<RecordBatch>> {
    let reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
        .map_err(|e| invalid(format!("invalid Arrow IPC stream: {e}")))?;
    reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| invalid(e.to_string()))
}

fn parse_read_at(
    version: Option<u64>,
    as_of: Option<&str>,
    snapshot: Option<&str>,
) -> PyResult<ReadAt> {
    match (version, as_of, snapshot) {
        (None, None, None) => Ok(ReadAt::Latest),
        (Some(v), None, None) => Ok(ReadAt::Version(v)),
        (None, Some(ts), None) => {
            let dt = chrono::DateTime::parse_from_rfc3339(ts)
                .map_err(|e| invalid(format!("bad as_of timestamp {ts:?}: {e}")))?;
            Ok(ReadAt::AsOf(
                dt.timestamp_nanos_opt()
                    .ok_or_else(|| invalid("as_of timestamp out of range"))?,
            ))
        }
        (None, None, Some(s)) => Ok(ReadAt::Snapshot(s.to_string())),
        _ => Err(invalid("specify at most one of version, as_of, snapshot")),
    }
}

fn check_timeout(timeout: Option<f64>) -> PyResult<()> {
    if let Some(secs) = timeout {
        if !secs.is_finite() || secs <= 0.0 {
            return Err(invalid("timeout must be a positive number of seconds"));
        }
    }
    Ok(())
}

/// Live handle state. All handles share one bounded multi-thread runtime;
/// database ownership and close state remain per handle.
#[derive(Clone)]
struct Inner {
    db: Arc<Database>,
    runtime: Arc<tokio::runtime::Runtime>,
}

#[pyclass]
struct NativeDatabase {
    inner: Mutex<Option<Inner>>,
}

fn shared_runtime() -> PyResult<Arc<tokio::runtime::Runtime>> {
    static RUNTIME: OnceLock<Arc<tokio::runtime::Runtime>> = OnceLock::new();
    if let Some(runtime) = RUNTIME.get() {
        return Ok(runtime.clone());
    }
    let workers = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(2)
        .min(8);
    let runtime = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(workers)
            .enable_all()
            .build()
            .map_err(|e| {
                tagged(
                    StorageError::new_err(format!("[io] failed to start runtime: {e}")),
                    "io",
                    None,
                    false,
                )
            })?,
    );
    // A racing constructor may win; use the installed runtime in that case.
    let _ = RUNTIME.set(runtime.clone());
    Ok(RUNTIME.get().cloned().unwrap_or(runtime))
}

impl NativeDatabase {
    fn inner(&self) -> PyResult<Inner> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .cloned()
            .ok_or_else(|| {
                tagged(
                    H5iError::new_err("[closed] database handle is closed"),
                    "closed",
                    Some("re-open the database"),
                    false,
                )
            })
    }

    /// Run a database future on the runtime with the GIL released, with an
    /// optional deadline in seconds.
    fn block<T, F, Fut>(&self, py: Python<'_>, timeout: Option<f64>, op: F) -> PyResult<T>
    where
        T: Send,
        F: FnOnce(Arc<Database>) -> Fut + Send,
        Fut: std::future::Future<Output = Result<T, Error>>,
    {
        check_timeout(timeout)?;
        let inner = self.inner()?;
        let db = inner.db.clone();
        py.detach(move || {
            inner.runtime.block_on(async move {
                match timeout {
                    Some(secs) => tokio::time::timeout(Duration::from_secs_f64(secs), op(db))
                        .await
                        .unwrap_or(Err(Error::Timeout {
                            seconds: secs.ceil() as u64,
                        })),
                    None => op(db).await,
                }
            })
        })
        .map_err(to_py_err)
    }
}

#[pymethods]
impl NativeDatabase {
    #[new]
    #[pyo3(signature = (path, create = false, read_only = false))]
    fn new(py: Python<'_>, path: PathBuf, create: bool, read_only: bool) -> PyResult<Self> {
        let runtime = shared_runtime()?;
        let db = py
            .detach(|| {
                runtime.block_on(async {
                    if read_only {
                        Database::open_read_only(&path).await
                    } else if create {
                        Database::open_or_create(&path).await
                    } else {
                        Database::open(&path).await
                    }
                })
            })
            .map_err(to_py_err)?;
        Ok(Self {
            inner: Mutex::new(Some(Inner {
                db: Arc::new(db),
                runtime,
            })),
        })
    }

    /// Release this database handle. The shared runtime stays alive for other
    /// handles and is reclaimed by the process at interpreter shutdown.
    /// Idempotent. In-flight operations on other threads hold their own
    /// reference and finish normally; later calls on this handle raise
    /// `H5iError` with `code == "closed"`.
    fn close(&self) {
        let taken = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        drop(taken);
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyResult<PyRef<'_, Self>> {
        slf.inner()?;
        Ok(slf)
    }

    fn __exit__(
        &self,
        _exc_type: &Bound<'_, PyAny>,
        _exc_value: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) {
        self.close();
    }

    #[getter]
    fn closed(&self) -> bool {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_none()
    }

    /// Create a table from an Arrow IPC schema (a zero-row stream is fine).
    #[pyo3(signature = (name, schema_ipc, time_column = None, sort_key = vec![]))]
    fn create_table(
        &self,
        py: Python<'_>,
        name: &str,
        schema_ipc: &[u8],
        time_column: Option<String>,
        sort_key: Vec<String>,
    ) -> PyResult<String> {
        // The stream itself carries the schema; a zero-batch stream (the
        // normal case for create_table) is valid.
        let reader =
            arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(schema_ipc), None)
                .map_err(|e| invalid(format!("invalid Arrow IPC schema stream: {e}")))?;
        let schema = reader.schema();
        let name = name.to_string();
        let result = self.block(py, None, move |db| async move {
            db.create_table(
                &name,
                schema,
                TableOptions {
                    time_column,
                    sort_key,
                    ..Default::default()
                },
            )
            .await
        })?;
        to_json(&result)
    }

    fn drop_table(&self, py: Python<'_>, name: &str) -> PyResult<()> {
        let name = name.to_string();
        self.block(
            py,
            None,
            move |db| async move { db.drop_table(&name).await },
        )
    }

    /// Arrow IPC schema-only stream for a table at a read point.
    #[pyo3(signature = (name, version = None, as_of = None, snapshot = None))]
    fn schema<'py>(
        &self,
        py: Python<'py>,
        name: &str,
        version: Option<u64>,
        as_of: Option<&str>,
        snapshot: Option<&str>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let at = parse_read_at(version, as_of, snapshot)?;
        let name = name.to_string();
        let schema = self.block(py, None, move |db| async move {
            Ok(db.resolve(&name, at).await?.schema)
        })?;
        Ok(PyBytes::new(py, &batches_to_ipc(&schema, &[])?))
    }

    /// Ingest an Arrow IPC stream. mode: "write" | "append".
    #[pyo3(signature = (name, ipc, mode = "append", expected_version = None, note = None))]
    fn ingest(
        &self,
        py: Python<'_>,
        name: &str,
        ipc: &[u8],
        mode: &str,
        expected_version: Option<u64>,
        note: Option<String>,
    ) -> PyResult<String> {
        let batches = ipc_to_batches(ipc)?;
        let opts = WriteOptions {
            expected_version,
            note,
            user_meta: serde_json::Map::new(),
        };
        let name = name.to_string();
        let result = match mode {
            "write" => self.block(py, None, move |db| async move {
                db.write(&name, batches, opts).await
            })?,
            "append" => self.block(py, None, move |db| async move {
                db.append_with_retry(&name, batches, opts, 5).await
            })?,
            other => {
                return Err(invalid(format!(
                    "mode must be 'write' or 'append', got {other:?}"
                )))
            }
        };
        to_json(&result)
    }

    /// Run SQL; returns an Arrow IPC stream (the schema is always included,
    /// even for empty results).
    ///
    /// `timeout` is a deadline in seconds; on expiry a `TimeoutError` is
    /// raised and execution is cancelled. `max_rows` raises `LimitError` as
    /// soon as the result exceeds it — the stream stops being pulled, so
    /// execution halts early instead of truncating silently.
    #[pyo3(signature = (query, memory_limit = None, timeout = None, max_rows = None))]
    fn sql<'py>(
        &self,
        py: Python<'py>,
        query: &str,
        memory_limit: Option<usize>,
        timeout: Option<f64>,
        max_rows: Option<usize>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        check_timeout(timeout)?;
        let inner = self.inner()?;
        let query = query.to_string();
        let bytes = py.detach(move || -> PyResult<Vec<u8>> {
            inner.runtime.block_on(async move {
                let run = async {
                    let session = H5iSession::new(
                        inner.db.clone(),
                        SessionOptions {
                            memory_limit,
                            ..Default::default()
                        },
                    )
                    .await?;
                    let df = session.sql(&query).await?;
                    let mut stream = df.execute_stream().await?;
                    let schema = stream.schema();
                    let mut batches = Vec::new();
                    let mut rows = 0usize;
                    while let Some(batch) = stream.next().await {
                        let batch = batch?;
                        rows += batch.num_rows();
                        if let Some(cap) = max_rows {
                            if rows > cap {
                                return Err(DataFusionError::ResourcesExhausted(format!(
                                    "result exceeded max_rows = {cap}; add a LIMIT or raise max_rows"
                                )));
                            }
                        }
                        batches.push(batch);
                    }
                    Ok::<_, DataFusionError>((schema, batches))
                };
                let (schema, batches) = match timeout {
                    Some(secs) => tokio::time::timeout(Duration::from_secs_f64(secs), run)
                        .await
                        .map_err(|_| {
                            tagged(
                                TimeoutError::new_err(format!(
                                    "[timeout] query exceeded {secs}s deadline"
                                )),
                                "timeout",
                                Some("raise the timeout or narrow the query"),
                                true,
                            )
                        })?
                        .map_err(df_err)?,
                    None => run.await.map_err(df_err)?,
                };
                batches_to_ipc(&schema, &batches)
            })
        })?;
        Ok(PyBytes::new(py, &bytes))
    }

    /// Direct scan of one table version; returns an Arrow IPC stream (the
    /// schema is always included, even for empty results).
    #[pyo3(signature = (name, version = None, as_of = None, snapshot = None,
                        columns = None, time_start = None, time_end = None,
                        limit = None, timeout = None))]
    #[allow(clippy::too_many_arguments)]
    fn read<'py>(
        &self,
        py: Python<'py>,
        name: &str,
        version: Option<u64>,
        as_of: Option<&str>,
        snapshot: Option<&str>,
        columns: Option<Vec<String>>,
        time_start: Option<i64>,
        time_end: Option<i64>,
        limit: Option<usize>,
        timeout: Option<f64>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let at = parse_read_at(version, as_of, snapshot)?;
        let name = name.to_string();
        let (schema, batches) = self.block(py, timeout, move |db| async move {
            let resolved = db.resolve(&name, at).await?;
            let schema: SchemaRef = match &columns {
                None => resolved.schema.clone(),
                Some(cols) => {
                    let indices = cols
                        .iter()
                        .map(|c| {
                            resolved
                                .schema
                                .index_of(c)
                                .map_err(|_| Error::invalid(format!("unknown column {c:?}")))
                        })
                        .collect::<Result<Vec<_>, Error>>()?;
                    Arc::new(resolved.schema.project(&indices).map_err(Error::from)?)
                }
            };
            let (batches, _) = db
                .scan_resolved(
                    &resolved,
                    ScanOptions {
                        projection: columns,
                        time_start,
                        time_end,
                        limit,
                        ..Default::default()
                    },
                )
                .await?;
            Ok((schema, batches))
        })?;
        Ok(PyBytes::new(py, &batches_to_ipc(&schema, &batches)?))
    }

    fn tables(&self, py: Python<'_>) -> PyResult<String> {
        let entries = self.block(py, None, |db| async move { db.list_tables().await })?;
        let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
        to_json(&names)
    }

    fn versions(&self, py: Python<'_>, name: &str) -> PyResult<String> {
        let name = name.to_string();
        let versions = self.block(
            py,
            None,
            move |db| async move { db.list_versions(&name).await },
        )?;
        to_json(&versions)
    }

    #[pyo3(signature = (name, tables = vec![], note = None))]
    fn create_snapshot(
        &self,
        py: Python<'_>,
        name: &str,
        tables: Vec<String>,
        note: Option<String>,
    ) -> PyResult<String> {
        let name = name.to_string();
        let snap = self.block(py, None, move |db| async move {
            db.create_snapshot(&name, &tables, note).await
        })?;
        to_json(&snap)
    }

    fn restore(&self, py: Python<'_>, name: &str, version: u64) -> PyResult<String> {
        let name = name.to_string();
        let result = self.block(py, None, move |db| async move {
            db.restore(&name, version, WriteOptions::default()).await
        })?;
        to_json(&result)
    }

    /// Plan a range replacement/delete; returns the plan as JSON.
    #[pyo3(signature = (name, start, end, ipc = None, note = None))]
    fn plan_replace_range(
        &self,
        py: Python<'_>,
        name: &str,
        start: i64,
        end: i64,
        ipc: Option<&[u8]>,
        note: Option<String>,
    ) -> PyResult<String> {
        let batches = match ipc {
            Some(b) => ipc_to_batches(b)?,
            None => vec![],
        };
        let name = name.to_string();
        let plan = self.block(py, None, move |db| async move {
            db.plan_replace_range(
                &name,
                start,
                end,
                batches,
                WriteOptions {
                    note,
                    ..Default::default()
                },
            )
            .await
        })?;
        to_json(&plan)
    }

    fn apply_plan(&self, py: Python<'_>, name: &str, plan_id: &str) -> PyResult<String> {
        let id =
            uuid::Uuid::parse_str(plan_id).map_err(|e| invalid(format!("bad plan id: {e}")))?;
        let name = name.to_string();
        let result = self.block(py, None, move |db| async move {
            let plan = db.load_plan(&name, id).await?;
            db.apply_plan(&plan).await
        })?;
        to_json(&result)
    }

    fn discard_plan(&self, py: Python<'_>, name: &str, plan_id: &str) -> PyResult<()> {
        let id =
            uuid::Uuid::parse_str(plan_id).map_err(|e| invalid(format!("bad plan id: {e}")))?;
        let name = name.to_string();
        self.block(py, None, move |db| async move {
            db.discard_plan(&name, id).await
        })
    }

    fn list_plans(&self, py: Python<'_>, name: &str) -> PyResult<String> {
        let name = name.to_string();
        let plans = self.block(
            py,
            None,
            move |db| async move { db.list_plans(&name).await },
        )?;
        to_json(&plans)
    }

    fn get_policy(&self, py: Python<'_>) -> PyResult<String> {
        let policy = self.block(py, None, |db| async move { db.policy().await })?;
        to_json(&policy)
    }

    /// Atomically merge a partial `{flag: bool}` JSON object into the
    /// mutation policy (read-modify-write under the metadata lock).
    /// Unknown flags and non-boolean values are rejected. Returns the
    /// stored policy as JSON.
    fn update_policy(&self, py: Python<'_>, updates_json: &str) -> PyResult<String> {
        let updates: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(updates_json)
                .map_err(|e| invalid(format!("bad policy JSON: {e}")))?;
        let policy = self.block(py, None, move |db| async move {
            db.update_policy(move |policy| {
                let mut value = serde_json::to_value(&*policy).map_err(Error::from)?;
                let obj = value
                    .as_object_mut()
                    .ok_or_else(|| Error::internal("policy did not serialize to an object"))?;
                for (k, v) in &updates {
                    if !obj.contains_key(k) {
                        return Err(Error::invalid(format!("unknown policy flag {k:?}")));
                    }
                    if !v.is_boolean() {
                        return Err(Error::invalid(format!(
                            "policy flag {k:?} must be a boolean"
                        )));
                    }
                    obj.insert(k.clone(), v.clone());
                }
                *policy = serde_json::from_value(value).map_err(Error::from)?;
                Ok(())
            })
            .await
        })?;
        to_json(&policy)
    }

    #[pyo3(signature = (name, note = None))]
    fn compact(&self, py: Python<'_>, name: &str, note: Option<String>) -> PyResult<String> {
        let name = name.to_string();
        let result = self.block(py, None, move |db| async move {
            db.compact(
                &name,
                WriteOptions {
                    note,
                    ..Default::default()
                },
            )
            .await
        })?;
        to_json(&result)
    }

    #[pyo3(signature = (table = None, grace_seconds = 3600, apply = false))]
    fn vacuum(
        &self,
        py: Python<'_>,
        table: Option<&str>,
        grace_seconds: u64,
        apply: bool,
    ) -> PyResult<String> {
        let table = table.map(str::to_string);
        let report = self.block(py, None, move |db| async move {
            db.vacuum(table.as_deref(), grace_seconds, apply).await
        })?;
        to_json(&report)
    }

    #[pyo3(signature = (name, deep = false))]
    fn verify(&self, py: Python<'_>, name: &str, deep: bool) -> PyResult<String> {
        let name = name.to_string();
        let report = self.block(
            py,
            None,
            move |db| async move { db.verify(&name, deep).await },
        )?;
        to_json(&report)
    }
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = m.py();
    m.add_class::<NativeDatabase>()?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add("H5iError", py.get_type::<H5iError>())?;
    m.add("NotFoundError", py.get_type::<NotFoundError>())?;
    m.add("ConflictError", py.get_type::<ConflictError>())?;
    m.add("InvalidInputError", py.get_type::<InvalidInputError>())?;
    m.add("PolicyError", py.get_type::<PolicyError>())?;
    m.add("CorruptionError", py.get_type::<CorruptionError>())?;
    m.add("LimitError", py.get_type::<LimitError>())?;
    m.add("TimeoutError", py.get_type::<TimeoutError>())?;
    m.add("StorageError", py.get_type::<StorageError>())?;
    Ok(())
}
