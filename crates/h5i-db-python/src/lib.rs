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

use h5i_db_core::{DataPolicy, Database, Error, ReadAt, ScanOptions, TableOptions, WriteOptions};
use h5i_db_query::datafusion::error::DataFusionError;
use h5i_db_query::{DEFAULT_TOLERANCE, H5iSession, SessionOptions, arrival_delta};

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
fn tagged(
    err: PyErr,
    code: &str,
    hint: Option<&str>,
    retryable: bool,
    did_you_mean: Option<&str>,
    next_actions: Vec<(String, String)>,
) -> PyErr {
    Python::attach(|py| {
        let value = err.value(py);
        let _ = value.setattr("code", code);
        let _ = value.setattr("hint", hint);
        let _ = value.setattr("retryable", retryable);
        let _ = value.setattr("did_you_mean", did_you_mean);
        // List of {"cmd": …, "why": …} so Python callers get the same
        // machine-executable recovery steps the CLI envelope carries. The
        // commands keep the `<db>` placeholder: unlike the CLI, the bindings
        // are not invoked with a path on the command line.
        let actions: Vec<_> = next_actions
            .into_iter()
            .map(|(cmd, why)| {
                let d = pyo3::types::PyDict::new(py);
                let _ = d.set_item("cmd", cmd);
                let _ = d.set_item("why", why);
                d
            })
            .collect();
        let _ = value.setattr("next_actions", actions);
    });
    err
}

/// [`tagged`] for errors that carry no suggestion or recovery step (raised
/// outside the core error model: encoding faults, raw DataFusion failures).
fn tagged_plain(err: PyErr, code: &str, hint: Option<&str>, retryable: bool) -> PyErr {
    tagged(err, code, hint, retryable, None, Vec::new())
}

fn to_py_err(e: Error) -> PyErr {
    let code = e.code();
    let retryable = e.retryable();
    let hint = e.hint();
    let did_you_mean = e.did_you_mean().map(str::to_string);
    let next_actions: Vec<(String, String)> = e
        .next_actions()
        .into_iter()
        .map(|a| (a.cmd, a.why))
        .collect();
    let msg = format!(
        "[{code}] {e}{}",
        hint.as_deref()
            .map(|h| format!(" (hint: {h})"))
            .unwrap_or_default()
    );
    let err = match code {
        "database_not_found"
        | "table_not_found"
        | "version_not_found"
        | "snapshot_not_found"
        | "fork_not_found" => NotFoundError::new_err(msg),
        "database_exists"
        | "table_exists"
        | "version_conflict"
        | "lock_timeout"
        | "fork_exists"
        // A lost promote is a conflict in the ordinary sense — someone else
        // committed first — so it must be catchable as one.
        | "promote_conflict" => ConflictError::new_err(msg),
        "invalid_input"
        | "unsupported"
        | "schema_mismatch"
        | "sort_order_violation"
        | "data_policy_violation" => InvalidInputError::new_err(msg),
        "read_only" | "policy_violation" => PolicyError::new_err(msg),
        "corruption" | "format_too_new" => CorruptionError::new_err(msg),
        "limit_exceeded" => LimitError::new_err(msg),
        "timeout" => TimeoutError::new_err(msg),
        "storage" | "io" | "arrow" | "parquet" | "metadata" => StorageError::new_err(msg),
        _ => H5iError::new_err(msg),
    };
    tagged(
        err,
        code,
        hint.as_deref(),
        retryable,
        did_you_mean.as_deref(),
        next_actions,
    )
}

fn df_err(e: DataFusionError) -> PyErr {
    let e = match e {
        // Unwrap so a core error surfaced through DataFusion keeps its code.
        DataFusionError::External(inner) => {
            return match inner.downcast::<Error>() {
                Ok(core) => to_py_err(*core),
                Err(other) => tagged_plain(
                    H5iError::new_err(format!("[query] {other}")),
                    "query",
                    None,
                    false,
                ),
            };
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
    tagged_plain(exc(format!("[{code}] {e}")), code, None, false)
}

fn invalid(msg: impl std::fmt::Display) -> PyErr {
    tagged_plain(
        InvalidInputError::new_err(format!("[invalid_input] {msg}")),
        "invalid_input",
        None,
        false,
    )
}

fn encode_err(e: arrow::error::ArrowError) -> PyErr {
    tagged_plain(
        StorageError::new_err(format!("[arrow] {e}")),
        "arrow",
        None,
        false,
    )
}

/// Decode the caller-supplied fork metadata blob, which must be a JSON object.
///
/// Shared by the single- and batch-create bindings so the two cannot drift
/// into accepting different things.
fn parse_fork_meta(
    meta_json: Option<String>,
) -> PyResult<serde_json::Map<String, serde_json::Value>> {
    match meta_json {
        None => Ok(serde_json::Map::new()),
        Some(text) => match serde_json::from_str::<serde_json::Value>(&text)
            .map_err(|e| to_py_err(Error::invalid(format!("fork metadata JSON: {e}"))))?
        {
            serde_json::Value::Object(m) => Ok(m),
            _ => Err(to_py_err(Error::invalid(
                "fork metadata must be a JSON object",
            ))),
        },
    }
}

fn to_json<T: serde::Serialize>(v: &T) -> PyResult<String> {
    serde_json::to_string(v).map_err(|e| {
        tagged_plain(
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
    if let Some(secs) = timeout
        && (!secs.is_finite() || secs <= 0.0)
    {
        return Err(invalid("timeout must be a positive number of seconds"));
    }
    Ok(())
}

fn backtest_err(error: h5i_db_backtest::BacktestError) -> PyErr {
    // Backtest failures are input problems -- a bad window, an unsorted
    // signal table, a book gap -- so they surface as the same
    // invalid-input error the rest of this module raises.
    invalid(error.to_string())
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
                tagged_plain(
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
                tagged_plain(
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
    // pyo3 flattens the write options into keyword arguments, which is the
    // ergonomic shape on the Python side even though it trips the arity lint.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (name, ipc, mode = "append", expected_version = None, note = None, idempotency_key = None))]
    fn ingest(
        &self,
        py: Python<'_>,
        name: &str,
        ipc: &[u8],
        mode: &str,
        expected_version: Option<u64>,
        note: Option<String>,
        idempotency_key: Option<String>,
    ) -> PyResult<String> {
        let batches = ipc_to_batches(ipc)?;
        let opts = WriteOptions {
            expected_version,
            note,
            user_meta: serde_json::Map::new(),
            idempotency_key,
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
                )));
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
    /// Run a Tier 1 signal-replay backtest and return its report as JSON.
    ///
    /// The signals table *is* the strategy: the kernel reads timestamped
    /// intent and executes it through the full matching, fee, latency and
    /// queue path. Results land on a fork named `bt-<run_id>` as ordinary
    /// tables, so Python reads them back with the same query surface as
    /// market data rather than through a second API.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        run_id,
        starting_cash,
        signals_table = "signals".to_string(),
        fee_kind = None,
        fee_rate = None,
        maker_rebate = None,
        maker_fee_rate = None,
        queue_position = false,
        optimistic_queue = false,
        latency_nanos = None,
        slippage_ticks = None,
        window = None,
        version = None,
        as_of = None,
        snapshot = None,
        equity_interval_nanos = None,
        minimum_coverage = None,
    ))]
    fn run_backtest(
        &self,
        py: Python<'_>,
        run_id: &str,
        starting_cash: f64,
        signals_table: String,
        fee_kind: Option<String>,
        fee_rate: Option<f64>,
        maker_rebate: Option<f64>,
        maker_fee_rate: Option<f64>,
        queue_position: bool,
        optimistic_queue: bool,
        latency_nanos: Option<i64>,
        slippage_ticks: Option<i64>,
        window: Option<(i64, i64)>,
        version: Option<u64>,
        as_of: Option<String>,
        snapshot: Option<String>,
        equity_interval_nanos: Option<i64>,
        minimum_coverage: Option<f64>,
    ) -> PyResult<String> {
        use h5i_db_backtest::engine::SignalReplay;
        use h5i_db_backtest::models::{
            ConstantLatency, KalshiFees, PredictionMarketFees, ProportionalFees,
            QueuePositionFills, TickSlippage,
        };
        use h5i_db_backtest::run::{run_in_fork, RunSpec};
        use h5i_db_backtest::types::{Money, Price};
        use h5i_db_backtest::window::TimeWindow;

        let inner = self.inner()?;
        let run_id = run_id.to_string();
        py.detach(move || -> PyResult<String> {
            inner.runtime.block_on(async move {
                let read_at = parse_read_at(version, as_of.as_deref(), snapshot.as_deref())?;
                let mut spec = RunSpec::new(
                    run_id,
                    Money::from_f64(starting_cash).map_err(backtest_err)?,
                )
                .read_at(read_at.clone());
                if let Some((start, end)) = window {
                    spec = spec.window(
                        TimeWindow::new(
                            h5i_db_backtest::types::UnixNanos::new(start),
                            h5i_db_backtest::types::UnixNanos::new(end),
                        )
                        .map_err(backtest_err)?,
                    );
                }
                if let Some(nanos) = equity_interval_nanos {
                    spec = spec.equity_interval_nanos(nanos);
                }
                if let Some(minimum) = minimum_coverage {
                    spec = spec.minimum_coverage(minimum);
                }

                // Signals are read at the fork's own pin, not at the
                // market-data pin. The two are different axes: the pin says
                // what the strategy could have *known*, while the signals
                // table is the strategy itself, which is usually written
                // long after the data snapshot it trades against. Reading
                // both at one snapshot would mean a run could only ever
                // replay strategies that predate its data.
                //
                // Reproducibility is not lost: the run's fork pins every
                // table at creation, so `Latest` inside the fork is a fixed
                // version for the life of the run.
                let intents = h5i_db_backtest::store::read_signals(
                    &inner.db,
                    &signals_table,
                    h5i_db_core::ReadAt::Latest,
                    spec.window,
                )
                .await
                .map_err(backtest_err)?;
                let mut strategy = SignalReplay::new(intents).map_err(backtest_err)?;

                let report = run_in_fork(&inner.db, spec, &mut strategy, move |mut builder| {
                    if let Some(rate) = fee_rate {
                        match fee_kind.as_deref() {
                            Some("kalshi") => {
                                if let Ok(model) =
                                    KalshiFees::with_maker_rate(rate, maker_fee_rate)
                                {
                                    builder = builder.fee_model(Box::new(model));
                                }
                            }
                            Some("proportional") => {
                                if let Ok(model) =
                                    ProportionalFees::new(maker_rebate.unwrap_or(0.0), rate)
                                {
                                    builder = builder.fee_model(Box::new(model));
                                }
                            }
                            // Prediction-market curve is the default because
                            // it is the venue this layer leads with.
                            _ => {
                                if let Ok(mut model) = PredictionMarketFees::new(rate) {
                                    if let Some(rebate) = maker_rebate
                                        && let Ok(with_rebate) = model.with_maker_rebate(rebate)
                                    {
                                        model = with_rebate;
                                    }
                                    builder = builder.fee_model(Box::new(model));
                                }
                            }
                        }
                    }
                    if let Some(ticks) = slippage_ticks
                        && let Ok(tick) = Price::from_f64(0.0001)
                    {
                        builder = builder.fill_model(Box::new(TickSlippage::new(ticks, tick)));
                    } else if queue_position {
                        builder = builder.fill_model(Box::new(if optimistic_queue {
                            QueuePositionFills::optimistic()
                        } else {
                            QueuePositionFills::new()
                        }));
                    }
                    if let Some(nanos) = latency_nanos {
                        builder = builder.latency_model(Box::new(ConstantLatency {
                            insert: nanos,
                            cancel: nanos,
                        }));
                    }
                    builder
                })
                .await
                .map_err(backtest_err)?;

                Ok(serde_json::json!({
                    "run_id": report.run_id,
                    "fork": report.fork,
                    "digest": report.digest,
                    "starting_cash": report.result.starting_cash.to_f64(),
                    "final_cash": report.result.final_cash.to_f64(),
                    "realized_pnl": report.result.realized_pnl.to_f64(),
                    "commissions": report.result.commissions.to_f64(),
                    "funding_paid": report.result.funding_paid.to_f64(),
                    "fills": report.result.fills.len(),
                    "orders": report.result.orders.len(),
                    "records_processed": report.result.records_processed,
                    "simulated_through_ns": report.result.simulated_through.map(|t| t.get()),
                    "equity_points": report.result.equity.len(),
                    "settlement_applied": report.settlement.was_applied(),
                    "coverage": report.coverage.map(|c| c.ratio()),
                    "liquidations": report.result.liquidations.len(),
                    "rejected_for_margin": report.result.rejected_for_margin,
                    "self_trades_prevented": report.result.self_trades_prevented,
                    "metrics": {
                        "orders_submitted": report.result.metrics.orders_submitted,
                        "orders_filled": report.result.metrics.orders_filled,
                        "orders_cancelled_unfilled":
                            report.result.metrics.orders_cancelled_unfilled,
                        "orders_rejected_margin":
                            report.result.metrics.orders_rejected_margin,
                        "orders_rejected_self_trade":
                            report.result.metrics.orders_rejected_self_trade,
                        "fills_taker": report.result.metrics.fills_taker,
                        "fills_maker": report.result.metrics.fills_maker,
                        "book_gaps": report.result.metrics.book_gaps,
                        "liquidations": report.result.metrics.liquidations,
                    },
                    "warnings": report.warnings(),
                })
                .to_string())
            })
        })
    }

    #[pyo3(signature = (query, memory_limit = None, timeout = None, max_rows = None, target_partitions = None))]
    fn sql<'py>(
        &self,
        py: Python<'py>,
        query: &str,
        memory_limit: Option<usize>,
        timeout: Option<f64>,
        max_rows: Option<usize>,
        target_partitions: Option<usize>,
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
                            target_partitions,
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
                        if let Some(cap) = max_rows
                            && rows > cap
                        {
                            return Err(DataFusionError::ResourcesExhausted(format!(
                                "result exceeded max_rows = {cap}; add a LIMIT or raise max_rows"
                            )));
                        }
                        batches.push(batch);
                    }
                    Ok::<_, DataFusionError>((schema, batches))
                };
                let (schema, batches) = match timeout {
                    Some(secs) => tokio::time::timeout(Duration::from_secs_f64(secs), run)
                        .await
                        .map_err(|_| {
                            tagged_plain(
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

    // -- forks (ROADMAP Part IX) ---------------------------------------

    #[pyo3(signature = (name, note = None, as_of_ns = None, meta_json = None))]
    fn create_fork(
        &self,
        py: Python<'_>,
        name: &str,
        note: Option<String>,
        as_of_ns: Option<i64>,
        meta_json: Option<String>,
    ) -> PyResult<String> {
        let name = name.to_string();
        let user_meta = parse_fork_meta(meta_json)?;
        let fork = self.block(py, None, move |db| async move {
            db.create_fork(&name, note, as_of_ns, user_meta).await
        })?;
        to_json(&fork)
    }

    /// Create many forks over one resolution of the base (ROADMAP X-B1).
    #[pyo3(signature = (names, note = None, as_of_ns = None, meta_json = None))]
    fn create_forks(
        &self,
        py: Python<'_>,
        names: Vec<String>,
        note: Option<String>,
        as_of_ns: Option<i64>,
        meta_json: Option<String>,
    ) -> PyResult<String> {
        let user_meta = parse_fork_meta(meta_json)?;
        let forks = self.block(py, None, move |db| async move {
            db.create_forks(&names, note, as_of_ns, user_meta).await
        })?;
        to_json(&forks)
    }

    fn drop_forks(&self, py: Python<'_>, names: Vec<String>) -> PyResult<usize> {
        self.block(
            py,
            None,
            move |db| async move { db.drop_forks(&names).await },
        )
    }

    /// Every fork's name, from the visibility index.
    fn fork_names(&self, py: Python<'_>) -> PyResult<String> {
        let names = self.block(py, None, move |db| async move { db.fork_names().await })?;
        to_json(&names)
    }

    /// A handle scoped to a fork. Shares this handle's runtime; closing one
    /// does not close the other.
    fn open_fork(&self, py: Python<'_>, name: &str) -> PyResult<Self> {
        let runtime = self.inner()?.runtime.clone();
        let name = name.to_string();
        let forked = self.block(py, None, move |db| async move { db.open_fork(&name).await })?;
        Ok(Self {
            inner: Mutex::new(Some(Inner {
                db: Arc::new(forked),
                runtime,
            })),
        })
    }

    fn list_forks(&self, py: Python<'_>) -> PyResult<String> {
        let forks = self.block(py, None, move |db| async move { db.list_forks().await })?;
        to_json(&forks)
    }

    fn fork_info(&self, py: Python<'_>, name: &str) -> PyResult<String> {
        let name = name.to_string();
        let fork = self.block(py, None, move |db| async move { db.fork_info(&name).await })?;
        to_json(&fork)
    }

    #[pyo3(signature = (name, table = None))]
    fn fork_diff(&self, py: Python<'_>, name: &str, table: Option<String>) -> PyResult<String> {
        let name = name.to_string();
        let diff = self.block(py, None, move |db| async move {
            db.fork_diff(&name, table.as_deref()).await
        })?;
        to_json(&diff)
    }

    fn promote(&self, py: Python<'_>, fork: &str, table: &str) -> PyResult<String> {
        let fork = fork.to_string();
        let table = table.to_string();
        let result = self.block(py, None, move |db| async move {
            db.promote(&fork, &table).await
        })?;
        to_json(&result)
    }

    fn drop_fork(&self, py: Python<'_>, name: &str) -> PyResult<usize> {
        let name = name.to_string();
        self.block(py, None, move |db| async move { db.drop_fork(&name).await })
    }

    /// The fork this handle is scoped to, or `None` on a base handle.
    fn fork_name(&self, _py: Python<'_>) -> PyResult<Option<String>> {
        Ok(self.inner()?.db.fork_name().map(|s| s.to_string()))
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

    /// Look-ahead-bias check (V-A1): run `query` against head and against the
    /// earlier read point, and return the arrival-delta report as JSON.
    /// Exactly one of `version` / `as_of` / `snapshot` must pin the decision
    /// point; `tolerance` defaults to the crate default.
    #[pyo3(signature = (query, version = None, as_of = None, snapshot = None, tolerance = None))]
    fn arrival_delta(
        &self,
        py: Python<'_>,
        query: &str,
        version: Option<u64>,
        as_of: Option<&str>,
        snapshot: Option<&str>,
        tolerance: Option<f64>,
    ) -> PyResult<String> {
        if version.is_none() && as_of.is_none() && snapshot.is_none() {
            return Err(invalid(
                "arrival_delta needs a decision point: one of version, as_of, snapshot",
            ));
        }
        let at = parse_read_at(version, as_of, snapshot)?;
        let tol = tolerance.unwrap_or(DEFAULT_TOLERANCE);
        if !tol.is_finite() || tol < 0.0 {
            return Err(invalid("tolerance must be a non-negative, finite number"));
        }
        let inner = self.inner()?;
        let query = query.to_string();
        let report = py
            .detach(move || {
                inner
                    .runtime
                    .block_on(async move { arrival_delta(inner.db.clone(), &query, at, tol).await })
            })
            .map_err(df_err)?;
        to_json(&report)
    }

    /// A table's data-safety policy as JSON (`null` when unset).
    fn get_data_policy(&self, py: Python<'_>, table: &str) -> PyResult<String> {
        let table = table.to_string();
        let policy = self.block(
            py,
            None,
            move |db| async move { db.data_policy(&table).await },
        )?;
        to_json(&policy)
    }

    /// Install (overwrite) a table's data-safety policy from a typed JSON
    /// document; returns the stored policy as JSON.
    fn set_data_policy(&self, py: Python<'_>, table: &str, policy_json: &str) -> PyResult<String> {
        let policy: DataPolicy = serde_json::from_str(policy_json)
            .map_err(|e| invalid(format!("bad data-policy JSON: {e}")))?;
        let stored = policy.clone();
        let table = table.to_string();
        self.block(py, None, move |db| async move {
            db.set_data_policy(&table, &policy).await
        })?;
        to_json(&stored)
    }

    /// Remove a table's data-safety policy (writes become unconstrained).
    fn clear_data_policy(&self, py: Python<'_>, table: &str) -> PyResult<()> {
        let table = table.to_string();
        self.block(py, None, move |db| async move {
            db.clear_data_policy(&table).await
        })
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
