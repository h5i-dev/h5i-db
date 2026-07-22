//! Schema evolution (ROADMAP §4.1).
//!
//! Supported changes — the ones that never require touching existing
//! segments:
//!
//! - **Add** a column (must be nullable, or have a metadata default of null):
//!   appended at the end of the schema; old segments read as all-null.
//! - **Widen** a numeric column within its family: Int8→Int16→Int32→Int64,
//!   UInt likewise, Float32→Float64. Old segments are cast on read.
//! - **Relax nullability**: a non-nullable column may become nullable.
//!
//! Everything else (drop, rename, reorder, narrow, cross-family type change,
//! tightening nullability, touching the time column's type) is rejected —
//! those DO require rewrites, which `write` already covers explicitly.
//!
//! Mechanics: `evolve_schema` writes spec revision N+1 and commits a
//! metadata-only version with `op = evolve_schema`. Segments keep the
//! revision they were written under; readers adapt each segment batch to the
//! resolved version's schema via [`adapt_batch`] (null-backfill + widening
//! cast). Manifests at schema revision > 1 carry `format: 2`, so readers too
//! old to adapt fail with `FormatTooNew` instead of silently mis-reading.
//!
//! Time travel interacts cleanly: a version resolved before the evolution
//! uses its own spec revision, so `as_of` reads see the old schema, and
//! `restore` of a pre-evolution version restores the old schema revision
//! with it.

use arrow::array::{new_null_array, ArrayRef, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use crate::error::{Error, Result};

/// Validate that `new` is a legal evolution of `old`. Returns a
/// human-readable change list (for commit notes / CLI output).
pub fn validate_evolution(old: &SchemaRef, new: &SchemaRef) -> Result<Vec<String>> {
    let mut changes = Vec::new();
    if new.fields().len() < old.fields().len() {
        return Err(Error::SchemaMismatch {
            detail: "schema evolution cannot drop columns; use `write` to rewrite the table"
                .into(),
        });
    }
    // Existing columns: same order, same name, compatible type/nullability.
    for (i, old_f) in old.fields().iter().enumerate() {
        let new_f = new.field(i);
        if new_f.name() != old_f.name() {
            return Err(Error::SchemaMismatch {
                detail: format!(
                    "schema evolution cannot rename or reorder columns \
                     (position {i}: {:?} -> {:?})",
                    old_f.name(),
                    new_f.name()
                ),
            });
        }
        if old_f.is_nullable() && !new_f.is_nullable() {
            return Err(Error::SchemaMismatch {
                detail: format!(
                    "column {:?}: cannot tighten nullability (nullable -> required)",
                    old_f.name()
                ),
            });
        }
        if !old_f.is_nullable() && new_f.is_nullable() {
            changes.push(format!("relax nullability of {:?}", old_f.name()));
        }
        if old_f.data_type() != new_f.data_type() {
            if !is_widening(old_f.data_type(), new_f.data_type()) {
                return Err(Error::SchemaMismatch {
                    detail: format!(
                        "column {:?}: {:?} -> {:?} is not a widening conversion; \
                         only Int8→…→Int64, UInt8→…→UInt64, Float32→Float64 are supported",
                        old_f.name(),
                        old_f.data_type(),
                        new_f.data_type()
                    ),
                });
            }
            changes.push(format!(
                "widen {:?}: {:?} -> {:?}",
                old_f.name(),
                old_f.data_type(),
                new_f.data_type()
            ));
        }
    }
    // Appended columns: nullable only (old rows must be representable).
    for new_f in new.fields().iter().skip(old.fields().len()) {
        if !new_f.is_nullable() {
            return Err(Error::SchemaMismatch {
                detail: format!(
                    "new column {:?} must be nullable (existing rows backfill as null)",
                    new_f.name()
                ),
            });
        }
        changes.push(format!(
            "add column {:?} {:?}",
            new_f.name(),
            new_f.data_type()
        ));
    }
    if changes.is_empty() {
        return Err(Error::invalid(
            "schema evolution requested but the schema is unchanged",
        ));
    }
    Ok(changes)
}

fn int_rank(dt: &DataType) -> Option<(u8, u8)> {
    // (family, rank); wider rank in the same family is a legal widening.
    match dt {
        DataType::Int8 => Some((0, 0)),
        DataType::Int16 => Some((0, 1)),
        DataType::Int32 => Some((0, 2)),
        DataType::Int64 => Some((0, 3)),
        DataType::UInt8 => Some((1, 0)),
        DataType::UInt16 => Some((1, 1)),
        DataType::UInt32 => Some((1, 2)),
        DataType::UInt64 => Some((1, 3)),
        DataType::Float32 => Some((2, 0)),
        DataType::Float64 => Some((2, 1)),
        _ => None,
    }
}

pub(crate) fn is_widening(from: &DataType, to: &DataType) -> bool {
    match (int_rank(from), int_rank(to)) {
        (Some((fa, fr)), Some((ta, tr))) => fa == ta && tr > fr,
        _ => false,
    }
}

/// Adapt a batch written under an older schema revision to `target`:
/// missing trailing columns backfill as nulls, widened columns are cast.
/// A no-op (same Arc) when the schemas already match.
pub fn adapt_batch(target: &SchemaRef, batch: RecordBatch) -> Result<RecordBatch> {
    if batch.schema() == *target {
        return Ok(batch);
    }
    let n = batch.num_rows();
    let old_schema = batch.schema();
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(target.fields().len());
    for field in target.fields() {
        match old_schema.index_of(field.name()) {
            Ok(idx) => {
                let col = batch.column(idx);
                if col.data_type() == field.data_type() {
                    cols.push(col.clone());
                } else {
                    cols.push(
                        arrow::compute::cast(col, field.data_type()).map_err(Error::Arrow)?,
                    );
                }
            }
            Err(_) => cols.push(new_null_array(field.data_type(), n)),
        }
    }
    RecordBatch::try_new(target.clone(), cols).map_err(Error::Arrow)
}

/// Build the target schema for a plain "add columns" evolution — a
/// convenience for CLI/Python callers who pass only the new columns.
pub fn schema_with_added(old: &SchemaRef, added: &[Field]) -> SchemaRef {
    let mut fields: Vec<Field> = old.fields().iter().map(|f| f.as_ref().clone()).collect();
    fields.extend(added.iter().cloned());
    std::sync::Arc::new(Schema::new(fields))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use std::sync::Arc;

    fn s(fields: Vec<Field>) -> SchemaRef {
        Arc::new(Schema::new(fields))
    }

    #[test]
    fn evolution_rules() {
        let old = s(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("px", DataType::Int32, false),
        ]);
        // add nullable + widen: ok
        let good = s(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("px", DataType::Int64, false),
            Field::new("venue", DataType::Utf8, true),
        ]);
        let changes = validate_evolution(&old, &good).unwrap();
        assert_eq!(changes.len(), 2);

        // add non-nullable: rejected
        let bad = s(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("px", DataType::Int32, false),
            Field::new("venue", DataType::Utf8, false),
        ]);
        assert!(validate_evolution(&old, &bad).is_err());

        // narrow: rejected
        let bad = s(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("px", DataType::Int16, false),
        ]);
        assert!(validate_evolution(&old, &bad).is_err());

        // rename: rejected
        let bad = s(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("price", DataType::Int32, false),
        ]);
        assert!(validate_evolution(&old, &bad).is_err());

        // drop: rejected
        let bad = s(vec![Field::new("ts", DataType::Int64, false)]);
        assert!(validate_evolution(&old, &bad).is_err());

        // unchanged: rejected
        assert!(validate_evolution(&old, &old).is_err());
    }

    #[test]
    fn adapt_backfills_and_widens() {
        let old = s(vec![Field::new("px", DataType::Int32, false)]);
        let new = s(vec![
            Field::new("px", DataType::Int64, false),
            Field::new("venue", DataType::Utf8, true),
        ]);
        let batch = RecordBatch::try_new(
            old,
            vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef],
        )
        .unwrap();
        let adapted = adapt_batch(&new, batch).unwrap();
        assert_eq!(adapted.schema(), new);
        assert_eq!(adapted.column(1).null_count(), 3);
    }
}
