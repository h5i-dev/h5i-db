//! Reading and writing fixed-point columns, in either encoding.
//!
//! Canonical tables have always stored `f64`. That is exact for every price a
//! prediction market or an equity quotes, and the tick-grid test proves it
//! over the whole grid, but it stops being exact above `2^53 / SCALE`, which
//! is about nine million units. Widening [`Raw`](crate::types::Raw) moves the
//! *arithmetic* ceiling to 1.7e29 and leaves that one where it was, so a wide
//! build could compute a value it could not store.
//!
//! `Decimal128(38, 9)` fixes it, and fits the type by construction: its
//! backing value is an `i128` scaled by `1e-9`, which is what a `Raw` already
//! is. Encoding is a widening move and decoding is a narrowing one. No float
//! appears in either direction, so nothing rounds.
//!
//! # Both encodings are readable in both builds
//!
//! The encoding is a property of the table, recorded in its spec, not of the
//! binary reading it. [`read_fixed`] dispatches on the array it was handed.
//!
//! That is deliberate and it is the mistake NautilusTrader is still paying
//! for: theirs is chosen by a compile-time feature, so a catalog written by
//! one build cannot be read by the other, and their decode path carries
//! correction code for the migration to this day. A reader here does not need
//! to know how it was built to read what is in front of it.
//!
//! A narrow build reading a `Decimal128` column can still meet a value too
//! large for its `Raw`. That is refused, not truncated, by the same
//! [`narrow`](crate::types::narrow) every other conversion goes through.

use arrow::array::{Array, ArrayRef, Decimal128Array, Decimal128Builder, Float64Array};
use arrow::datatypes::{DataType, Field};
use std::sync::Arc;

use crate::error::{BacktestError, Result};
use crate::types::{Raw, SCALE, narrow};

/// Decimal places every fixed-point column carries. Matches `SCALE`.
pub const DECIMAL_SCALE: i8 = 9;

/// Total digits. 38 is the most a `Decimal128` holds, and the whole point
/// here is range, so there is no reason to ask for less.
pub const DECIMAL_PRECISION: u8 = 38;

/// How a table stores its fixed-point columns.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum FixedEncoding {
    /// `f64`. The default, and what every existing table uses.
    ///
    /// Exact to about nine million units, which covers every price a venue
    /// quotes. Chosen for tables that will never hold more than that, and
    /// kept as the default so no existing data changes meaning.
    #[default]
    Float,
    /// `Decimal128(38, 9)`. Exact over the whole range a `Raw` can hold.
    ///
    /// Costs 16 bytes a value against 8, and loses min/max zone maps until
    /// core learns the type, so a scan over one prunes by time only. Pruning
    /// fails open, so that is slower and not wrong.
    Decimal,
}

impl FixedEncoding {
    /// The Arrow type a column in this encoding has.
    pub fn data_type(self) -> DataType {
        match self {
            Self::Float => DataType::Float64,
            Self::Decimal => DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE),
        }
    }

    /// Which encoding an existing column is in.
    ///
    /// `None` for a column that is neither, which is a schema mismatch the
    /// caller reports with the name in hand.
    pub fn of(data_type: &DataType) -> Option<Self> {
        match data_type {
            DataType::Float64 => Some(Self::Float),
            DataType::Decimal128(_, DECIMAL_SCALE) => Some(Self::Decimal),
            _ => None,
        }
    }
}

/// A fixed-point field.
pub fn fixed_field(name: &str, encoding: FixedEncoding, nullable: bool) -> Field {
    Field::new(name, encoding.data_type(), nullable)
}

/// Read row `row` of a fixed-point column as raw scaled units.
///
/// `None` where the value is null. Works on either encoding.
pub fn read_fixed(array: &ArrayRef, row: usize, name: &str) -> Result<Option<Raw>> {
    if array.is_null(row) {
        return Ok(None);
    }
    match array.data_type() {
        DataType::Float64 => {
            let values = downcast::<Float64Array>(array, name)?;
            let scaled = values.value(row) * SCALE as f64;
            if !scaled.is_finite() {
                return Err(BacktestError::NotFinite { what: "column" });
            }
            Ok(Some(narrow(scaled.round() as i128, "read_fixed")?))
        }
        DataType::Decimal128(_, DECIMAL_SCALE) => {
            let values = downcast::<Decimal128Array>(array, name)?;
            // The stored i128 *is* the raw, at the same scale.
            Ok(Some(narrow(values.value(row), "read_fixed")?))
        }
        other => Err(BacktestError::Schema {
            table: "read",
            detail: format!("column {name} is {other}, which is not a fixed-point encoding"),
        }),
    }
}

/// Read row `row` of a fixed-point column the schema declares non-null.
///
/// A null there is a row that disagrees with its own schema, so it is refused
/// rather than read as a zero -- a zero price or a zero size is a value a
/// matching engine will happily act on.
pub fn read_fixed_value(array: &ArrayRef, row: usize, name: &str) -> Result<Raw> {
    read_fixed(array, row, name)?.ok_or_else(|| BacktestError::Schema {
        table: "read",
        detail: format!("column {name} is null in a row where it must not be"),
    })
}

fn downcast<'a, T: 'static>(array: &'a ArrayRef, name: &str) -> Result<&'a T> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| BacktestError::Schema {
            table: "read",
            detail: format!("column {name} did not downcast to its declared type"),
        })
}

/// Accumulates a fixed-point column in whichever encoding the table declares.
pub enum FixedBuilder {
    Float(arrow::array::Float64Builder),
    Decimal(Box<Decimal128Builder>),
}

impl FixedBuilder {
    pub fn new(encoding: FixedEncoding) -> Self {
        match encoding {
            FixedEncoding::Float => Self::Float(arrow::array::Float64Builder::new()),
            FixedEncoding::Decimal => Self::Decimal(Box::new(
                Decimal128Builder::new()
                    .with_precision_and_scale(DECIMAL_PRECISION, DECIMAL_SCALE)
                    .expect("38/9 is a valid decimal128"),
            )),
        }
    }

    /// Append raw scaled units.
    pub fn append(&mut self, raw: Raw) {
        match self {
            Self::Float(builder) => builder.append_value(raw as f64 / SCALE as f64),
            Self::Decimal(builder) => builder.append_value(raw as i128),
        }
    }

    pub fn append_null(&mut self) {
        match self {
            Self::Float(builder) => builder.append_null(),
            Self::Decimal(builder) => builder.append_null(),
        }
    }

    pub fn finish(self) -> ArrayRef {
        match self {
            Self::Float(mut builder) => Arc::new(builder.finish()),
            Self::Decimal(mut builder) => Arc::new(builder.finish()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(encoding: FixedEncoding, raws: &[Raw]) -> Vec<Raw> {
        let mut builder = FixedBuilder::new(encoding);
        for raw in raws {
            builder.append(*raw);
        }
        let array = builder.finish();
        (0..raws.len())
            .map(|row| read_fixed(&array, row, "t").unwrap().unwrap())
            .collect()
    }

    #[test]
    fn decimal_is_exact_where_float_is_not() {
        // 2^53 is where an f64 mantissa stops holding consecutive integers,
        // which at this scale is about nine million units. Values above it
        // are the reason the decimal encoding exists.
        let above: Raw = 9_007_199_254_740_993;
        assert_eq!(round_trip(FixedEncoding::Decimal, &[above]), vec![above]);
        assert_ne!(
            round_trip(FixedEncoding::Float, &[above]),
            vec![above],
            "if f64 ever holds this exactly, the decimal encoding has no reason to exist"
        );
    }

    #[test]
    fn both_encodings_agree_below_the_float_ceiling() {
        // Every price a venue quotes lives here, so the two encodings must be
        // indistinguishable in the range that actually gets used.
        let cases: Vec<Raw> = vec![
            0,
            1,
            -1,
            SCALE,
            -SCALE,
            SCALE / 10_000,
            123_456_789,
            9_000_000 * SCALE / 1000,
        ];
        assert_eq!(
            round_trip(FixedEncoding::Float, &cases),
            round_trip(FixedEncoding::Decimal, &cases)
        );
    }

    #[test]
    fn nulls_survive_either_encoding() {
        for encoding in [FixedEncoding::Float, FixedEncoding::Decimal] {
            let mut builder = FixedBuilder::new(encoding);
            builder.append(SCALE);
            builder.append_null();
            let array = builder.finish();
            assert_eq!(read_fixed(&array, 0, "t").unwrap(), Some(SCALE));
            assert_eq!(read_fixed(&array, 1, "t").unwrap(), None);
        }
    }

    #[test]
    fn an_encoding_is_recognised_from_its_type() {
        assert_eq!(
            FixedEncoding::of(&FixedEncoding::Float.data_type()),
            Some(FixedEncoding::Float)
        );
        assert_eq!(
            FixedEncoding::of(&FixedEncoding::Decimal.data_type()),
            Some(FixedEncoding::Decimal)
        );
        assert_eq!(FixedEncoding::of(&DataType::Int64), None);
    }
}
