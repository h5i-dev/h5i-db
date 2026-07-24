//! Leakage-delta report (ROADMAP Part V, item V-A1).
//!
//! Runs one query twice — against the current head (**leaking**: every commit,
//! including data that only became available after the decision instant) and
//! against an as-of read point (**non-leaking**: only data available as of that
//! instant) — and diffs the two results. The difference is the "alpha that
//! evaporates": the portion of a metric that came from decision-time data
//! leakage rather than genuine signal (cf. the one-switch leaking/non-leaking
//! backtest diagnostic).
//!
//! This exists *because* h5i-db already resolves an as-of read point by commit
//! availability time (`ReadAt::AsOf`, `committed_at_ns`), so both runs are
//! deterministic, reproducible, and cheap (O(1) time-travel + reused aggregate
//! states). No new engine primitive is required — this is a thin surface over
//! [`H5iSession::new_at`].
//!
//! **Scope (state it honestly).** This measures *data-availability* leakage —
//! late-arriving or restated rows across commits. It does **not** detect
//! look-ahead *inside* a single snapshot (a window overrunning into future
//! rows — that needs an effect checker, V-A2), nor an LLM's pretraining
//! leakage. A non-zero delta proves availability leakage; a zero delta does not
//! prove its absence.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array, RecordBatch};
use arrow::compute::{cast, concat_batches};
use arrow::datatypes::{DataType, SchemaRef};
use arrow::util::display::array_value_to_string;
use datafusion::error::{DataFusionError, Result as DfResult};
use h5i_db_core::{Database, ReadAt};
use serde::Serialize;

use crate::session::{H5iSession, SessionOptions};

/// Default numeric tolerance below which a per-cell delta is treated as noise.
pub const DEFAULT_TOLERANCE: f64 = 1e-9;

/// Per-column comparison of the head vs as-of result.
#[derive(Debug, Clone, Serialize)]
pub struct ColumnDelta {
    pub name: String,
    /// Whether the column was compared numerically (else by string equality).
    pub numeric: bool,
    /// Scalar head/as-of values — only when both results are exactly one row
    /// (the common single-metric case), for a readable `head → asof (delta)`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub head: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub asof: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delta: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delta_pct: Option<f64>,
    /// Largest absolute per-row delta over the compared (overlapping) rows.
    pub max_abs_delta: f64,
    /// Rows (within the overlap) that differ beyond tolerance.
    pub mismatches: u64,
}

/// A table whose as-of version differs from head — i.e. commits were withheld
/// from the non-leaking run. Empty means the as-of point saw the same data.
#[derive(Debug, Clone, Serialize)]
pub struct TableVersionDelta {
    pub table: String,
    pub head_version: u64,
    pub asof_version: u64,
}

/// The full leakage-delta report (serialized as the CLI/Python envelope).
#[derive(Debug, Clone, Serialize)]
pub struct LeakageReport {
    /// Human-readable description of the as-of read point.
    pub decision: String,
    /// Whether the two results had the same schema and could be compared.
    pub comparable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub head_rows: u64,
    pub asof_rows: u64,
    pub row_count_differs: bool,
    pub columns: Vec<ColumnDelta>,
    /// Largest absolute delta across all numeric columns.
    pub max_abs_delta: f64,
    /// True if any availability leakage was detected (row-count change, a
    /// numeric delta beyond tolerance, or a non-numeric cell change).
    pub leakage_detected: bool,
    pub tolerance: f64,
    /// Per-table head-vs-as-of version gap (only tables that differ).
    pub withheld_versions: Vec<TableVersionDelta>,
}

/// Describe a read point for the report.
fn describe(at: &ReadAt) -> String {
    match at {
        ReadAt::Latest => "latest".into(),
        ReadAt::Version(v) => format!("version {v}"),
        ReadAt::AsOf(ts) => format!("as_of {ts} (ns since epoch)"),
        ReadAt::Snapshot(s) => format!("snapshot {s:?}"),
    }
}

/// Run `sql` against head and against `at`, returning the leakage-delta report.
pub async fn check_leakage(
    db: Arc<Database>,
    sql: &str,
    at: ReadAt,
    tolerance: f64,
) -> DfResult<LeakageReport> {
    // Head (leaking) session, then the as-of (non-leaking) session sharing its
    // runtime so the footer-metadata cache and memory pool are reused.
    let head_session = H5iSession::new(db.clone(), SessionOptions::default()).await?;
    let asof_session = H5iSession::new_with_runtime_at(
        db.clone(),
        SessionOptions::default(),
        head_session.runtime_env(),
        at.clone(),
    )
    .await?;

    let (head_schema, head_batches) = run(&head_session, sql).await?;
    let (asof_schema, asof_batches) = run(&asof_session, sql).await?;

    let withheld = withheld_versions(&db, &at).await?;
    Ok(compare(
        describe(&at),
        (head_schema, head_batches),
        (asof_schema, asof_batches),
        tolerance,
        withheld,
    ))
}

async fn run(session: &H5iSession, sql: &str) -> DfResult<(SchemaRef, Vec<RecordBatch>)> {
    let df = session.sql(sql).await?;
    let schema: SchemaRef = Arc::new(df.schema().as_arrow().clone());
    let batches = df.collect().await?;
    Ok((schema, batches))
}

/// Per-table head-vs-as-of resolved sequence, for tables where they differ.
async fn withheld_versions(db: &Arc<Database>, at: &ReadAt) -> DfResult<Vec<TableVersionDelta>> {
    let ext = |e: h5i_db_core::Error| DataFusionError::External(Box::new(e));
    let tables = db.list_tables().await.map_err(ext)?;
    let mut out = Vec::new();
    for entry in tables {
        let head = db
            .resolve(&entry.name, ReadAt::Latest)
            .await
            .map_err(ext)?
            .manifest
            .sequence;
        let asof = db
            .resolve(&entry.name, at.clone())
            .await
            .map_err(ext)?
            .manifest
            .sequence;
        if head != asof {
            out.push(TableVersionDelta {
                table: entry.name,
                head_version: head,
                asof_version: asof,
            });
        }
    }
    Ok(out)
}

fn is_numeric(dt: &DataType) -> bool {
    use DataType::*;
    matches!(
        dt,
        Int8 | Int16
            | Int32
            | Int64
            | UInt8
            | UInt16
            | UInt32
            | UInt64
            | Float16
            | Float32
            | Float64
            | Decimal128(_, _)
            | Decimal256(_, _)
            | Timestamp(_, _)
            | Date32
            | Date64
            | Time32(_)
            | Time64(_)
            | Duration(_)
    )
}

/// Cast a numeric/temporal column to `Float64` for delta arithmetic.
fn to_f64(col: &ArrayRef) -> Option<Float64Array> {
    cast(col, &DataType::Float64)
        .ok()
        .and_then(|c| c.as_any().downcast_ref::<Float64Array>().cloned())
}

fn schemas_match(a: &SchemaRef, b: &SchemaRef) -> bool {
    a.fields().len() == b.fields().len()
        && a.fields()
            .iter()
            .zip(b.fields())
            .all(|(x, y)| x.name() == y.name() && x.data_type() == y.data_type())
}

fn compare(
    decision: String,
    head: (SchemaRef, Vec<RecordBatch>),
    asof: (SchemaRef, Vec<RecordBatch>),
    tolerance: f64,
    withheld: Vec<TableVersionDelta>,
) -> LeakageReport {
    let (head_schema, head_batches) = head;
    let (asof_schema, asof_batches) = asof;
    let head_rows: u64 = head_batches.iter().map(|b| b.num_rows() as u64).sum();
    let asof_rows: u64 = asof_batches.iter().map(|b| b.num_rows() as u64).sum();
    let row_count_differs = head_rows != asof_rows;

    if !schemas_match(&head_schema, &asof_schema) {
        // A shape change between the two runs — cannot align columns, but the
        // change itself is a signal.
        return LeakageReport {
            decision,
            comparable: false,
            reason: Some(
                "head and as-of results have different schemas; cannot diff columns".into(),
            ),
            head_rows,
            asof_rows,
            row_count_differs,
            columns: Vec::new(),
            max_abs_delta: 0.0,
            leakage_detected: true,
            tolerance,
            withheld_versions: withheld,
        };
    }

    let head_cat = concat_batches(&head_schema, &head_batches)
        .unwrap_or_else(|_| RecordBatch::new_empty(head_schema.clone()));
    let asof_cat = concat_batches(&asof_schema, &asof_batches)
        .unwrap_or_else(|_| RecordBatch::new_empty(asof_schema.clone()));
    let overlap = (head_rows.min(asof_rows)) as usize;
    let single_row = head_rows == 1 && asof_rows == 1;

    let mut columns = Vec::with_capacity(head_schema.fields().len());
    let mut max_abs_delta = 0.0_f64;
    let mut any_cell_change = false;

    for (i, field) in head_schema.fields().iter().enumerate() {
        let a = head_cat.column(i);
        let b = asof_cat.column(i);
        let numeric = is_numeric(field.data_type());
        let mut col_max = 0.0_f64;
        let mut mismatches = 0_u64;
        let (mut scalar_head, mut scalar_asof, mut scalar_delta, mut scalar_pct) =
            (None, None, None, None);

        if numeric {
            if let (Some(af), Some(bf)) = (to_f64(a), to_f64(b)) {
                for row in 0..overlap {
                    let (av, bv) = (cell_opt(&af, row), cell_opt(&bf, row));
                    match (av, bv) {
                        (Some(x), Some(y)) => {
                            let d = (x - y).abs();
                            if d > col_max {
                                col_max = d;
                            }
                            if d > tolerance {
                                mismatches += 1;
                            }
                        }
                        (None, None) => {}
                        _ => mismatches += 1, // null appeared/disappeared
                    }
                }
                if single_row {
                    let (h, a0) = (cell_opt(&af, 0), cell_opt(&bf, 0));
                    scalar_head = h;
                    scalar_asof = a0;
                    if let (Some(h), Some(a0)) = (h, a0) {
                        let d = h - a0;
                        scalar_delta = Some(d);
                        scalar_pct = if a0 != 0.0 {
                            Some(d / a0.abs() * 100.0)
                        } else {
                            None
                        };
                    }
                }
            }
        } else {
            for row in 0..overlap {
                let sa = array_value_to_string(a, row).unwrap_or_default();
                let sb = array_value_to_string(b, row).unwrap_or_default();
                if sa != sb {
                    mismatches += 1;
                }
            }
        }

        if col_max > max_abs_delta {
            max_abs_delta = col_max;
        }
        if mismatches > 0 {
            any_cell_change = true;
        }
        columns.push(ColumnDelta {
            name: field.name().clone(),
            numeric,
            head: scalar_head,
            asof: scalar_asof,
            delta: scalar_delta,
            delta_pct: scalar_pct,
            max_abs_delta: col_max,
            mismatches,
        });
    }

    let leakage_detected = row_count_differs || any_cell_change || max_abs_delta > tolerance;
    LeakageReport {
        decision,
        comparable: true,
        reason: None,
        head_rows,
        asof_rows,
        row_count_differs,
        columns,
        max_abs_delta,
        leakage_detected,
        tolerance,
        withheld_versions: withheld,
    }
}

fn cell_opt(a: &Float64Array, row: usize) -> Option<f64> {
    if a.is_valid(row) {
        Some(a.value(row))
    } else {
        None
    }
}
