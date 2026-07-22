//! Python native module for h5i-db.
//!
//! Interop is Arrow IPC streams (`bytes`) rather than pyo3↔pyarrow FFI
//! structs: it works with every pyarrow release, keeps this crate free of
//! version-locked bridge dependencies, and the copy it costs is one memcpy of
//! already-encoded buffers. The ergonomic API lives in the pure-Python
//! wrapper (`python/h5i_db/__init__.py`).

use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::RecordBatch;
use pyo3::exceptions::{PyIOError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use h5i_db_core::{Database, Error, ReadAt, ScanOptions, TableOptions, WriteOptions};
use h5i_db_query::{H5iSession, SessionOptions};

fn to_py_err(e: Error) -> PyErr {
    let msg = format!("[{}] {}{}", e.code(), e, {
        e.hint()
            .map(|h| format!(" (hint: {h})"))
            .unwrap_or_default()
    });
    match e.exit_category() {
        h5i_db_core::ExitCategory::UserError => PyValueError::new_err(msg),
        h5i_db_core::ExitCategory::Internal => PyIOError::new_err(msg),
        _ => PyRuntimeError::new_err(msg),
    }
}

fn df_err(e: h5i_db_query::datafusion::error::DataFusionError) -> PyErr {
    PyValueError::new_err(e.to_string())
}

fn batches_to_ipc(batches: &[RecordBatch]) -> PyResult<Vec<u8>> {
    let mut buf = Vec::new();
    if batches.is_empty() {
        return Ok(buf);
    }
    let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &batches[0].schema())
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
    for b in batches {
        writer
            .write(b)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
    }
    writer
        .finish()
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
    Ok(buf)
}

fn ipc_to_batches(bytes: &[u8]) -> PyResult<Vec<RecordBatch>> {
    let reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
        .map_err(|e| PyValueError::new_err(format!("invalid Arrow IPC stream: {e}")))?;
    reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| PyValueError::new_err(e.to_string()))
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
                .map_err(|e| PyValueError::new_err(format!("bad as_of timestamp {ts:?}: {e}")))?;
            Ok(ReadAt::AsOf(dt.timestamp_nanos_opt().ok_or_else(|| {
                PyValueError::new_err("as_of timestamp out of range")
            })?))
        }
        (None, None, Some(s)) => Ok(ReadAt::Snapshot(s.to_string())),
        _ => Err(PyValueError::new_err(
            "specify at most one of version, as_of, snapshot",
        )),
    }
}

#[pyclass]
struct NativeDatabase {
    db: Arc<Database>,
    runtime: tokio::runtime::Runtime,
}

impl NativeDatabase {
    fn block<F, T>(&self, fut: F) -> PyResult<T>
    where
        F: std::future::Future<Output = Result<T, Error>>,
    {
        self.runtime.block_on(fut).map_err(to_py_err)
    }
}

#[pymethods]
impl NativeDatabase {
    #[new]
    #[pyo3(signature = (path, create = false, read_only = false))]
    fn new(path: PathBuf, create: bool, read_only: bool) -> PyResult<Self> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        let db = runtime
            .block_on(async {
                if read_only {
                    Database::open_read_only(&path).await
                } else if create {
                    Database::open_or_create(&path).await
                } else {
                    Database::open(&path).await
                }
            })
            .map_err(to_py_err)?;
        Ok(Self {
            db: Arc::new(db),
            runtime,
        })
    }

    /// Create a table from an Arrow IPC schema (a zero-row stream is fine).
    #[pyo3(signature = (name, schema_ipc, time_column = None, sort_key = vec![]))]
    fn create_table(
        &self,
        name: &str,
        schema_ipc: &[u8],
        time_column: Option<String>,
        sort_key: Vec<String>,
    ) -> PyResult<String> {
        let batches = ipc_to_batches(schema_ipc)?;
        let schema = batches
            .first()
            .map(|b| b.schema())
            .ok_or_else(|| PyValueError::new_err("schema_ipc stream carries no schema"))?;
        let result = self.block(self.db.create_table(
            name,
            schema,
            TableOptions {
                time_column,
                sort_key,
                ..Default::default()
            },
        ))?;
        Ok(serde_json::to_string(&result).unwrap())
    }

    /// Ingest an Arrow IPC stream. mode: "write" | "append".
    #[pyo3(signature = (name, ipc, mode = "append", expected_version = None, note = None))]
    fn ingest(
        &self,
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
        let result = match mode {
            "write" => self.block(self.db.write(name, batches, opts))?,
            "append" => self.block(self.db.append_with_retry(name, batches, opts, 5))?,
            other => {
                return Err(PyValueError::new_err(format!(
                    "mode must be 'write' or 'append', got {other:?}"
                )))
            }
        };
        Ok(serde_json::to_string(&result).unwrap())
    }

    /// Run SQL; returns an Arrow IPC stream.
    #[pyo3(signature = (query, memory_limit = None))]
    fn sql<'py>(
        &self,
        py: Python<'py>,
        query: &str,
        memory_limit: Option<usize>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let db = self.db.clone();
        let query = query.to_string();
        let batches = self
            .runtime
            .block_on(async move {
                let session = H5iSession::new(
                    db,
                    SessionOptions {
                        memory_limit,
                        ..Default::default()
                    },
                )
                .await?;
                session.sql(&query).await?.collect().await
            })
            .map_err(df_err)?;
        Ok(PyBytes::new(py, &batches_to_ipc(&batches)?))
    }

    /// Direct scan of one table version; returns an Arrow IPC stream.
    #[pyo3(signature = (name, version = None, as_of = None, snapshot = None,
                        columns = None, time_start = None, time_end = None,
                        limit = None))]
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
    ) -> PyResult<Bound<'py, PyBytes>> {
        let at = parse_read_at(version, as_of, snapshot)?;
        let (batches, _) = self.block(self.db.scan(
            name,
            at,
            ScanOptions {
                projection: columns,
                time_start,
                time_end,
                limit,
                concurrency: None,
            },
        ))?;
        Ok(PyBytes::new(py, &batches_to_ipc(&batches)?))
    }

    fn tables(&self) -> PyResult<String> {
        let entries = self.block(self.db.list_tables())?;
        let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
        Ok(serde_json::to_string(&names).unwrap())
    }

    fn versions(&self, name: &str) -> PyResult<String> {
        let versions = self.block(self.db.list_versions(name))?;
        Ok(serde_json::to_string(&versions).unwrap())
    }

    #[pyo3(signature = (name, tables = vec![], note = None))]
    fn create_snapshot(
        &self,
        name: &str,
        tables: Vec<String>,
        note: Option<String>,
    ) -> PyResult<String> {
        let snap = self.block(self.db.create_snapshot(name, &tables, note))?;
        Ok(serde_json::to_string(&snap).unwrap())
    }

    fn restore(&self, name: &str, version: u64) -> PyResult<String> {
        let result = self.block(self.db.restore(name, version, WriteOptions::default()))?;
        Ok(serde_json::to_string(&result).unwrap())
    }

    /// Plan a range replacement/delete; returns the plan as JSON.
    #[pyo3(signature = (name, start, end, ipc = None, note = None))]
    fn plan_replace_range(
        &self,
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
        let plan = self.block(self.db.plan_replace_range(
            name,
            start,
            end,
            batches,
            WriteOptions {
                note,
                ..Default::default()
            },
        ))?;
        Ok(serde_json::to_string(&plan).unwrap())
    }

    fn apply_plan(&self, name: &str, plan_id: &str) -> PyResult<String> {
        let id = uuid::Uuid::parse_str(plan_id)
            .map_err(|e| PyValueError::new_err(format!("bad plan id: {e}")))?;
        let plan = self.block(self.db.load_plan(name, id))?;
        let result = self.block(self.db.apply_plan(&plan))?;
        Ok(serde_json::to_string(&result).unwrap())
    }

    fn discard_plan(&self, name: &str, plan_id: &str) -> PyResult<()> {
        let id = uuid::Uuid::parse_str(plan_id)
            .map_err(|e| PyValueError::new_err(format!("bad plan id: {e}")))?;
        self.block(self.db.discard_plan(name, id))
    }

    #[pyo3(signature = (table = None, grace_seconds = 3600, apply = false))]
    fn vacuum(&self, table: Option<&str>, grace_seconds: u64, apply: bool) -> PyResult<String> {
        let report = self.block(self.db.vacuum(table, grace_seconds, apply))?;
        Ok(serde_json::to_string(&report).unwrap())
    }

    #[pyo3(signature = (name, deep = false))]
    fn verify(&self, name: &str, deep: bool) -> PyResult<String> {
        let report = self.block(self.db.verify(name, deep))?;
        Ok(serde_json::to_string(&report).unwrap())
    }
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<NativeDatabase>()?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
