//! Opt-in, per-table **data-safety policy** (ROADMAP Part V, item V-B1).
//!
//! Where [`crate::policy::MutationPolicy`] decides *whether* a mutation may
//! commit directly or must go through plan/apply, a `DataPolicy` constrains
//! *what data* a mutation is allowed to write: a set of row-level predicates
//! that every written row must satisfy. It is the tractable, embedded-friendly
//! projection of Data Flow Control (Columbia DAPLab, CIDR 2026) onto h5i-db's
//! explicit-row write path — the `REQUIRED`/anti-hallucination idea ("every
//! sink row must derive from an approved source") becomes "every written row
//! must satisfy the declared constraints" (e.g. a non-null `source_id` drawn
//! from an allowed set, a positive price, a bounded timestamp).
//!
//! ## Design constraints (deliberate)
//!
//! - **Opt-in.** A table has no policy unless one is set; absence means no
//!   checks and no cost on the read path (reads are never touched — enforcement
//!   runs only on the write path, alongside the schema/time validation the
//!   write path already performs).
//! - **Fail-closed.** A corrupt/unparseable policy file is an error, not a
//!   silently-disabled check. A violating row rejects the whole mutation
//!   (`OnFail::Reject`) — the write never partially lands.
//! - **No engine dependency.** Constraints are evaluated directly over Arrow
//!   arrays here in `core`, so enforcement composes with the existing plan/apply
//!   choke points without pulling DataFusion into the storage kernel. The
//!   grammar is deliberately small (column-vs-literal comparisons, membership,
//!   nullness, boolean combinators) — enough for data-safety guarantees,
//!   and small enough to avoid the fragile string-parser class of bug the
//!   roadmap warns about (T0.4): policies are typed JSON, never parsed SQL.
//!
//! Row-dropping (`REMOVE` in the DFC paper) is intentionally **not** in v1:
//! silently discarding rows from a "safety" feature is surprising and would
//! need the plan/apply preview to show exactly what was dropped. v1 offers
//! `Reject` (abort) and `Warn` (audit-log only); dropping is a future item.

use arrow::array::{Array, RecordBatch};
use arrow::datatypes::DataType;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::Backend;

/// Current on-disk format for the per-table policy sidecar.
pub const FORMAT: u32 = 1;

fn data_policy_path(table_id: uuid::Uuid) -> object_store::path::Path {
    object_store::path::Path::from(format!("tables/{table_id}/DATA_POLICY.json"))
}

/// A literal value a column is compared against. Numeric literals coerce across
/// integer/float column types; `Str`/`Bool` match only their own kind.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ScalarLit {
    Int(i64),
    Float(f64),
    Str(String),
    Bool(bool),
}

/// Comparison operator for a [`Predicate::Compare`].
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl CmpOp {
    fn eval_ordering(self, ord: std::cmp::Ordering) -> bool {
        use std::cmp::Ordering::*;
        match self {
            CmpOp::Eq => ord == Equal,
            CmpOp::Ne => ord != Equal,
            CmpOp::Lt => ord == Less,
            CmpOp::Le => ord != Greater,
            CmpOp::Gt => ord == Greater,
            CmpOp::Ge => ord != Less,
        }
    }
}

/// A boolean predicate over a single row's columns. Every written row must
/// satisfy the constraint's predicate.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Predicate {
    /// The column value is present (not NULL).
    NotNull {
        column: String,
    },
    /// `column <op> value`. A NULL column value never satisfies a comparison
    /// (SQL three-valued logic collapses to "not satisfied" here — use
    /// `NotNull` explicitly if nullability is intended).
    Compare {
        column: String,
        op: CmpOp,
        value: ScalarLit,
    },
    /// `column IN (values…)`. NULL never satisfies membership.
    InSet {
        column: String,
        values: Vec<ScalarLit>,
    },
    And(Box<Predicate>, Box<Predicate>),
    Or(Box<Predicate>, Box<Predicate>),
    Not(Box<Predicate>),
}

/// What to do when a row fails a constraint.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum OnFail {
    /// Abort the whole mutation (nothing lands). The default and the DFC
    /// `KILL` analogue.
    #[default]
    Reject,
    /// Allow the write but emit an audit-log warning naming the constraint and
    /// the violation count.
    Warn,
}

/// One named constraint: a predicate every written row must satisfy, plus the
/// action on failure.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Constraint {
    pub name: String,
    pub predicate: Predicate,
    #[serde(default)]
    pub on_fail: OnFail,
}

/// A table's opt-in data-safety policy.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DataPolicy {
    #[serde(default = "default_format")]
    pub format: u32,
    pub constraints: Vec<Constraint>,
}

fn default_format() -> u32 {
    FORMAT
}

impl DataPolicy {
    pub fn new(constraints: Vec<Constraint>) -> Self {
        Self {
            format: FORMAT,
            constraints,
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec_pretty(self)?)
    }

    pub fn from_bytes(bytes: &[u8], object: &str) -> Result<Self> {
        let policy: DataPolicy = serde_json::from_slice(bytes)
            .map_err(|e| Error::corruption(object, format!("data policy parse: {e}")))?;
        if policy.format > FORMAT {
            return Err(Error::FormatTooNew {
                found: policy.format,
                supported: FORMAT,
            });
        }
        Ok(policy)
    }

    /// Enforce every constraint against the rows of `batches`.
    ///
    /// Returns `Err(DataPolicyViolation)` on the first `Reject` constraint that
    /// any row fails; `Warn` constraints only log. A predicate that references
    /// an unknown column or an unsupported type for the comparison is a policy
    /// *configuration* error (`InvalidInput`) surfaced here rather than
    /// silently passing — enforcement is total and fail-closed.
    pub fn enforce(&self, batches: &[RecordBatch]) -> Result<()> {
        for constraint in &self.constraints {
            let mut violations: u64 = 0;
            let mut first: Option<(usize, usize)> = None; // (batch, row)
            for (bi, batch) in batches.iter().enumerate() {
                if batch.num_rows() == 0 {
                    continue;
                }
                let mask = eval_mask(&constraint.predicate, batch)?;
                for (row, satisfied) in mask.iter().enumerate() {
                    if !satisfied {
                        violations += 1;
                        if first.is_none() {
                            first = Some((bi, row));
                        }
                    }
                }
            }
            if violations == 0 {
                continue;
            }
            match constraint.on_fail {
                OnFail::Warn => {
                    tracing::warn!(
                        constraint = constraint.name,
                        violations,
                        "data policy constraint violated (on_fail=warn); allowing write"
                    );
                }
                OnFail::Reject => {
                    let (bi, row) = first.unwrap_or((0, 0));
                    return Err(Error::DataPolicyViolation {
                        constraint: constraint.name.clone(),
                        detail: format!(
                            "{violations} row(s) violate constraint {:?}; \
                             first at batch {bi} row {row}",
                            constraint.name
                        ),
                    });
                }
            }
        }
        Ok(())
    }
}

pub async fn load(backend: &Backend, table_id: uuid::Uuid) -> Result<Option<DataPolicy>> {
    let path = data_policy_path(table_id);
    match backend.get_opt(&path).await? {
        None => Ok(None),
        Some(bytes) => Ok(Some(DataPolicy::from_bytes(&bytes, path.as_ref())?)),
    }
}

pub async fn store(backend: &Backend, table_id: uuid::Uuid, policy: &DataPolicy) -> Result<()> {
    let path = data_policy_path(table_id);
    backend.put(&path, policy.to_bytes()?.into()).await?;
    backend.sync_objects(&[path]).await
}

pub async fn clear(backend: &Backend, table_id: uuid::Uuid) -> Result<()> {
    backend.delete(&data_policy_path(table_id)).await
}

// ---------------------------------------------------------------------------
// Evaluation over Arrow arrays
// ---------------------------------------------------------------------------

/// A single cell value, normalized for comparison against a [`ScalarLit`].
#[derive(Debug, Clone, PartialEq)]
enum Cell {
    Null,
    Int(i64),
    Float(f64),
    Str(String),
    Bool(bool),
}

/// Evaluate a predicate over every row of `batch`, returning a satisfied-mask.
fn eval_mask(pred: &Predicate, batch: &RecordBatch) -> Result<Vec<bool>> {
    let n = batch.num_rows();
    match pred {
        Predicate::NotNull { column } => {
            let array = column_array(batch, column)?;
            Ok((0..n).map(|i| array.is_valid(i)).collect())
        }
        Predicate::Compare { column, op, value } => {
            let array = column_array(batch, column)?;
            (0..n)
                .map(|i| compare_cell(&cell_at(array, i)?, *op, value, column))
                .collect()
        }
        Predicate::InSet { column, values } => {
            let array = column_array(batch, column)?;
            (0..n)
                .map(|i| {
                    let cell = cell_at(array, i)?;
                    for v in values {
                        if compare_cell(&cell, CmpOp::Eq, v, column)? {
                            return Ok(true);
                        }
                    }
                    Ok(false)
                })
                .collect()
        }
        Predicate::And(a, b) => {
            let (ma, mb) = (eval_mask(a, batch)?, eval_mask(b, batch)?);
            Ok(ma.iter().zip(mb).map(|(x, y)| *x && y).collect())
        }
        Predicate::Or(a, b) => {
            let (ma, mb) = (eval_mask(a, batch)?, eval_mask(b, batch)?);
            Ok(ma.iter().zip(mb).map(|(x, y)| *x || y).collect())
        }
        Predicate::Not(a) => Ok(eval_mask(a, batch)?.iter().map(|x| !x).collect()),
    }
}

fn column_array<'a>(batch: &'a RecordBatch, column: &str) -> Result<&'a dyn Array> {
    batch
        .column_by_name(column)
        .map(|c| c.as_ref())
        .ok_or_else(|| Error::invalid(format!("data policy references unknown column {column:?}")))
}

/// Extract row `i` of `array` as a normalized [`Cell`].
fn cell_at(array: &dyn Array, i: usize) -> Result<Cell> {
    use arrow::array::*;
    if array.is_null(i) {
        return Ok(Cell::Null);
    }
    macro_rules! prim_int {
        ($ty:ty) => {{
            let a = array.as_any().downcast_ref::<$ty>().unwrap();
            Cell::Int(a.value(i) as i64)
        }};
    }
    let cell = match array.data_type() {
        DataType::Int8 => prim_int!(Int8Array),
        DataType::Int16 => prim_int!(Int16Array),
        DataType::Int32 => prim_int!(Int32Array),
        DataType::Int64 => prim_int!(Int64Array),
        DataType::UInt8 => prim_int!(UInt8Array),
        DataType::UInt16 => prim_int!(UInt16Array),
        DataType::UInt32 => prim_int!(UInt32Array),
        DataType::UInt64 => prim_int!(UInt64Array),
        DataType::Float32 => {
            let a = array.as_any().downcast_ref::<Float32Array>().unwrap();
            Cell::Float(a.value(i) as f64)
        }
        DataType::Float64 => {
            let a = array.as_any().downcast_ref::<Float64Array>().unwrap();
            Cell::Float(a.value(i))
        }
        DataType::Utf8 => {
            let a = array.as_any().downcast_ref::<StringArray>().unwrap();
            Cell::Str(a.value(i).to_string())
        }
        DataType::LargeUtf8 => {
            let a = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            Cell::Str(a.value(i).to_string())
        }
        DataType::Boolean => {
            let a = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            Cell::Bool(a.value(i))
        }
        // Time-like columns compare on their raw integer representation,
        // matching how the rest of the engine treats the time column. These
        // are physically integer arrays but not `Int64Array`, so reinterpret
        // through a temporary cast and read the single cell we need.
        DataType::Timestamp(_, _)
        | DataType::Date32
        | DataType::Date64
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Duration(_) => {
            let casted = arrow::compute::cast(array, &DataType::Int64).map_err(|e| {
                Error::invalid(format!("data policy: time column not i64-able: {e}"))
            })?;
            let a = casted
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| Error::invalid("data policy: time column cast did not yield i64"))?;
            Cell::Int(a.value(i))
        }
        other => {
            return Err(Error::invalid(format!(
                "data policy: unsupported column type {other:?} for comparison"
            )))
        }
    };
    Ok(cell)
}

/// Compare a cell against a literal under `op`. A NULL cell never satisfies a
/// comparison. A type mismatch (e.g. string column vs integer literal) is a
/// policy configuration error.
fn compare_cell(cell: &Cell, op: CmpOp, lit: &ScalarLit, column: &str) -> Result<bool> {
    let ordering = match (cell, lit) {
        (Cell::Null, _) => return Ok(false),
        // numeric coercions
        (Cell::Int(a), ScalarLit::Int(b)) => a.cmp(b),
        (Cell::Int(a), ScalarLit::Float(b)) => cmp_f64(*a as f64, *b),
        (Cell::Float(a), ScalarLit::Float(b)) => cmp_f64(*a, *b),
        (Cell::Float(a), ScalarLit::Int(b)) => cmp_f64(*a, *b as f64),
        (Cell::Str(a), ScalarLit::Str(b)) => a.as_str().cmp(b.as_str()),
        (Cell::Bool(a), ScalarLit::Bool(b)) => {
            if !matches!(op, CmpOp::Eq | CmpOp::Ne) {
                return Err(Error::invalid(format!(
                    "data policy: ordering comparison on boolean column {column:?}"
                )));
            }
            a.cmp(b)
        }
        _ => {
            return Err(Error::invalid(format!(
                "data policy: type mismatch comparing column {column:?} against literal {lit:?}"
            )))
        }
    };
    Ok(op.eval_ordering(ordering))
}

fn cmp_f64(a: f64, b: f64) -> std::cmp::Ordering {
    // NaN never compares equal/ordered; treat as "unordered" → not-equal, and
    // for ordering fall back to Greater so `< x`/`<= x` reject NaN (fail-safe).
    a.partial_cmp(&b).unwrap_or(std::cmp::Ordering::Greater)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("price", DataType::Float64, true),
            Field::new("qty", DataType::Int64, false),
            Field::new("source", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Float64Array::from(vec![Some(10.0), Some(-1.0), None])),
                Arc::new(Int64Array::from(vec![5, 5, 5])),
                Arc::new(StringArray::from(vec![Some("feed_a"), Some("evil"), None])),
            ],
        )
        .unwrap()
    }

    fn reject(name: &str, predicate: Predicate) -> DataPolicy {
        DataPolicy::new(vec![Constraint {
            name: name.into(),
            predicate,
            on_fail: OnFail::Reject,
        }])
    }

    #[test]
    fn compare_positive_price_rejects_negative_and_null() {
        // price > 0 fails: row1 (-1.0) and row2 (NULL).
        let p = reject(
            "positive_price",
            Predicate::Compare {
                column: "price".into(),
                op: CmpOp::Gt,
                value: ScalarLit::Float(0.0),
            },
        );
        let err = p.enforce(&[batch()]).unwrap_err();
        match err {
            Error::DataPolicyViolation { constraint, detail } => {
                assert_eq!(constraint, "positive_price");
                assert!(detail.contains("2 row(s)"), "detail: {detail}");
            }
            other => panic!("expected DataPolicyViolation, got {other:?}"),
        }
    }

    #[test]
    fn in_set_allows_members_rejects_others_and_null() {
        let p = reject(
            "allowed_source",
            Predicate::InSet {
                column: "source".into(),
                values: vec![
                    ScalarLit::Str("feed_a".into()),
                    ScalarLit::Str("feed_b".into()),
                ],
            },
        );
        // "evil" and NULL both violate.
        let err = p.enforce(&[batch()]).unwrap_err();
        assert!(matches!(err, Error::DataPolicyViolation { .. }));
    }

    #[test]
    fn not_null_only_flags_nulls() {
        let p = reject(
            "source_present",
            Predicate::NotNull {
                column: "source".into(),
            },
        );
        let err = p.enforce(&[batch()]).unwrap_err();
        match err {
            Error::DataPolicyViolation { detail, .. } => {
                assert!(detail.contains("1 row(s)"), "detail: {detail}");
            }
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn satisfied_policy_passes() {
        // qty == 5 for every row.
        let p = reject(
            "qty_five",
            Predicate::Compare {
                column: "qty".into(),
                op: CmpOp::Eq,
                value: ScalarLit::Int(5),
            },
        );
        assert!(p.enforce(&[batch()]).is_ok());
    }

    #[test]
    fn and_or_not_combine() {
        // (price > 0) AND source IN {feed_a} — row0 only passes; others fail.
        let and = Predicate::And(
            Box::new(Predicate::Compare {
                column: "price".into(),
                op: CmpOp::Gt,
                value: ScalarLit::Float(0.0),
            }),
            Box::new(Predicate::InSet {
                column: "source".into(),
                values: vec![ScalarLit::Str("feed_a".into())],
            }),
        );
        assert!(reject("both", and).enforce(&[batch()]).is_err());

        // NOT(price > 0) passes exactly the rows price>0 fails → still a
        // violation overall (row0 satisfies price>0 so NOT fails there).
        let not = Predicate::Not(Box::new(Predicate::Compare {
            column: "price".into(),
            op: CmpOp::Gt,
            value: ScalarLit::Float(0.0),
        }));
        assert!(reject("neg", not).enforce(&[batch()]).is_err());

        // Or that every row satisfies: qty==5 OR price>0 → all rows pass.
        let or = Predicate::Or(
            Box::new(Predicate::Compare {
                column: "qty".into(),
                op: CmpOp::Eq,
                value: ScalarLit::Int(5),
            }),
            Box::new(Predicate::Compare {
                column: "price".into(),
                op: CmpOp::Gt,
                value: ScalarLit::Float(0.0),
            }),
        );
        assert!(reject("either", or).enforce(&[batch()]).is_ok());
    }

    #[test]
    fn warn_does_not_reject() {
        let p = DataPolicy::new(vec![Constraint {
            name: "warn_neg".into(),
            predicate: Predicate::Compare {
                column: "price".into(),
                op: CmpOp::Gt,
                value: ScalarLit::Float(0.0),
            },
            on_fail: OnFail::Warn,
        }]);
        assert!(p.enforce(&[batch()]).is_ok());
    }

    #[test]
    fn unknown_column_is_config_error() {
        let p = reject(
            "bad",
            Predicate::NotNull {
                column: "nope".into(),
            },
        );
        let err = p.enforce(&[batch()]).unwrap_err();
        assert!(matches!(err, Error::InvalidInput { .. }));
    }

    #[test]
    fn type_mismatch_is_config_error() {
        // Comparing a string column against an int literal.
        let p = reject(
            "mismatch",
            Predicate::Compare {
                column: "source".into(),
                op: CmpOp::Eq,
                value: ScalarLit::Int(1),
            },
        );
        let err = p.enforce(&[batch()]).unwrap_err();
        assert!(matches!(err, Error::InvalidInput { .. }));
    }

    #[test]
    fn empty_batches_pass() {
        let p = reject(
            "positive_price",
            Predicate::Compare {
                column: "price".into(),
                op: CmpOp::Gt,
                value: ScalarLit::Float(0.0),
            },
        );
        assert!(p.enforce(&[]).is_ok());
    }

    #[test]
    fn serde_round_trip() {
        let p = reject(
            "positive_price",
            Predicate::And(
                Box::new(Predicate::NotNull {
                    column: "source".into(),
                }),
                Box::new(Predicate::Compare {
                    column: "price".into(),
                    op: CmpOp::Ge,
                    value: ScalarLit::Float(0.0),
                }),
            ),
        );
        let bytes = p.to_bytes().unwrap();
        let back = DataPolicy::from_bytes(&bytes, "DATA_POLICY").unwrap();
        assert_eq!(p, back);
    }

    #[test]
    fn future_format_is_rejected() {
        let json = br#"{"format": 9999, "constraints": []}"#;
        let err = DataPolicy::from_bytes(json, "DATA_POLICY").unwrap_err();
        assert!(matches!(err, Error::FormatTooNew { .. }));
    }
}
