//! Fixed-point money and the two timestamps every record carries.
//!
//! Prices, quantities and cash are integers scaled by [`SCALE`], never
//! floats. A backtest sums millions of fills; binary floating point makes
//! that sum depend on the order it happened in, which is the same
//! reproducibility problem the analytics layer hit, except here it also
//! makes a position fail to close at exactly zero.
//!
//! The other decision that cannot be retrofitted is dual timestamps.
//! Every record has `ts_event` (when it happened at the venue) and
//! `ts_init` (when this system could first have known it). Replay orders by
//! `ts_init`, so a record that arrived late is delivered late, and a
//! strategy cannot act on knowledge it did not have. Ordering by `ts_event`
//! instead is a look-ahead bug that no amount of care elsewhere can undo.

use std::fmt;

use crate::error::{BacktestError, Result};

/// The integer every fixed-point value is stored in.
///
/// `i64` by default. The `wide` feature makes it `i128`, which buys range and
/// nothing else: the scale below does not move, so the tick grid, the `f64`
/// conversion and every value already written stay exactly as they were.
///
/// That is the opposite of NautilusTrader's choice, and deliberately. They
/// widen the scale with the integer (9 decimals to 16) because 18-decimal wei
/// needs the precision, which makes the two modes incompatible on disk and
/// cost them a catalog migration. Our ceiling is *range* -- a yen book, or a
/// meme-coin quantity -- so moving the scale would buy precision nobody asked
/// for and break the one thing that must not break.
#[cfg(feature = "wide")]
pub type Raw = i128;

/// See [`Raw`].
#[cfg(not(feature = "wide"))]
pub type Raw = i64;

/// Whether this build widened [`Raw`].
///
/// A compile-time choice is invisible to a caller holding a compiled
/// artifact, and reading a value at the wrong width is silent corruption
/// rather than an error. NautilusTrader exports the same fact as a linker
/// symbol for its FFI boundary; ours is reached through the Python module
/// and the run record, so a constant is enough.
pub const WIDE: bool = cfg!(feature = "wide");

/// Fixed-point scale: nine decimal places, in every mode.
///
/// Chosen to cover both ends of the venue roadmap without a per-venue
/// scale: prediction-market ticks are 1e-4, crypto quantities run to 1e-8,
/// and `i64` at 1e-9 still reaches ±9.2e9 units. Under `wide` the reach
/// becomes ±1.7e29 at the same nine decimals.
pub const SCALE: Raw = 1_000_000_000;
const SCALE_I128: i128 = SCALE as i128;

/// The largest magnitude [`Raw`] holds, as a float, for range checks at the
/// `f64` boundary.
const RAW_MAX_F64: f64 = Raw::MAX as f64;

// `notional`'s wide path splits both factors at the scale so the pieces
// multiply without a 256-bit type. That is only sound while the largest
// possible remainder squared still fits, which is a property of the scale,
// not of the inputs, so it is checked once at compile time.
const _: () = {
    assert!(SCALE > 0);
    let max_remainder = SCALE - 1;
    assert!(max_remainder.checked_mul(max_remainder).is_some());
};

macro_rules! fixed_point {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
        pub struct $name(pub Raw);

        impl $name {
            pub const ZERO: Self = Self(0);

            /// From raw scaled units.
            #[inline]
            pub const fn from_raw(raw: Raw) -> Self {
                Self(raw)
            }

            /// Raw scaled units.
            #[inline]
            pub const fn raw(self) -> Raw {
                self.0
            }

            /// From a whole number of units.
            pub fn from_units(units: Raw) -> Result<Self> {
                units
                    .checked_mul(SCALE)
                    .map(Self)
                    .ok_or_else(|| BacktestError::Overflow {
                        what: concat!(stringify!($name), "::from_units"),
                    })
            }

            /// From a float, rounding half away from zero.
            ///
            /// Floats are accepted only at the boundary -- parsing a vendor
            /// file, writing a test -- and become integers immediately.
            pub fn from_f64(value: f64) -> Result<Self> {
                if !value.is_finite() {
                    return Err(BacktestError::NotFinite {
                        what: stringify!($name),
                    });
                }
                let scaled = value * SCALE as f64;
                if scaled.abs() >= RAW_MAX_F64 {
                    return Err(BacktestError::Overflow {
                        what: concat!(stringify!($name), "::from_f64"),
                    });
                }
                Ok(Self(scaled.round() as Raw))
            }

            /// As a float. Lossy by construction; for display and reports.
            #[inline]
            pub fn to_f64(self) -> f64 {
                self.0 as f64 / SCALE as f64
            }

            #[inline]
            pub fn is_zero(self) -> bool {
                self.0 == 0
            }

            #[inline]
            pub fn is_positive(self) -> bool {
                self.0 > 0
            }

            #[inline]
            pub fn is_negative(self) -> bool {
                self.0 < 0
            }

            #[inline]
            pub fn abs(self) -> Self {
                Self(self.0.abs())
            }

            pub fn checked_add(self, other: Self) -> Result<Self> {
                self.0
                    .checked_add(other.0)
                    .map(Self)
                    .ok_or(BacktestError::Overflow {
                        what: concat!(stringify!($name), "::add"),
                    })
            }

            pub fn checked_sub(self, other: Self) -> Result<Self> {
                self.0
                    .checked_sub(other.0)
                    .map(Self)
                    .ok_or(BacktestError::Overflow {
                        what: concat!(stringify!($name), "::sub"),
                    })
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}({})", stringify!($name), self)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                let negative = self.0 < 0;
                let magnitude = (self.0 as i128).unsigned_abs();
                let whole = magnitude / SCALE_I128 as u128;
                let frac = magnitude % SCALE_I128 as u128;
                if negative {
                    write!(f, "-")?;
                }
                if frac == 0 {
                    write!(f, "{whole}")
                } else {
                    let text = format!("{frac:09}");
                    write!(f, "{whole}.{}", text.trim_end_matches('0'))
                }
            }
        }

        impl std::ops::Neg for $name {
            type Output = Self;
            fn neg(self) -> Self {
                Self(-self.0)
            }
        }
    };
}

fixed_point!(Price, "A price, in fixed point.");
fixed_point!(Qty, "A quantity, in fixed point.");
fixed_point!(Money, "A cash amount, in fixed point.");

impl Price {
    /// The bounds a probability price must satisfy: `[0, 1]`.
    pub const PROBABILITY_MIN: Price = Price(0);
    pub const PROBABILITY_MAX: Price = Price(SCALE);

    /// `1 - self`, the complementary probability.
    ///
    /// A binary prediction market's NO side prices at one minus YES; this
    /// is the operation that relates them, and it belongs here rather than
    /// being open-coded at each call site.
    pub fn complement(self) -> Result<Self> {
        Self::PROBABILITY_MAX.checked_sub(self)
    }
}

/// Narrow a wide intermediate back to [`Raw`], refusing rather than wrapping.
///
/// Fixed-point arithmetic promotes to `i128` to hold a product, then has to
/// come back. Writing that as `as Raw` wraps on overflow, which is how a
/// position silently becomes its own negation. Every narrowing in this crate
/// goes through here.
///
/// Under `wide` the conversion cannot fail and compiles to nothing.
#[inline]
pub fn narrow(value: i128, what: &'static str) -> Result<Raw> {
    Raw::try_from(value).map_err(|_| BacktestError::Overflow { what })
}

/// `price * qty`, rounded half away from zero.
///
/// Computed through `i128` because the intermediate product of two values
/// scaled by 1e9 overflows `i64` well inside the range either factor can
/// legitimately take.
#[cfg(not(feature = "wide"))]
pub fn notional(price: Price, qty: Qty) -> Result<Money> {
    let product = (price.0 as i128) * (qty.0 as i128);
    let rounded = if product >= 0 {
        (product + SCALE_I128 / 2) / SCALE_I128
    } else {
        (product - SCALE_I128 / 2) / SCALE_I128
    };
    narrow(rounded, "notional").map(Money)
}

/// `price * qty`, rounded half away from zero.
///
/// The narrow build promotes to `i128` and divides. That is not available
/// here, because `Raw` already *is* `i128` and the product of two of them
/// does not fit one. Rather than reach for a 256-bit type, split each factor
/// at the scale and recombine, which is the shape NautilusTrader uses
/// (`fixed.rs::checked_mul_div_fixed`):
///
/// ```text
///   a = aw·S + ar        a·b / S  =  aw·bw·S + aw·br + ar·bw + (ar·br)/S
///   b = bw·S + br
/// ```
///
/// Every partial product is bounded: `ar` and `br` are remainders below `S`,
/// so `ar·br` fits whatever `Raw` is, which the const assertion by `SCALE`
/// checks once at compile time.
///
/// Theirs truncates. Ours rounds half away from zero, so the final term keeps
/// its remainder and rounds on it rather than discarding it.
#[cfg(feature = "wide")]
pub fn notional(price: Price, qty: Qty) -> Result<Money> {
    let (a, b) = (price.0, qty.0);
    let (aw, ar) = (a / SCALE, a % SCALE);
    let (bw, br) = (b / SCALE, b % SCALE);

    let overflow = || BacktestError::Overflow { what: "notional" };
    let whole = aw
        .checked_mul(bw)
        .and_then(|v| v.checked_mul(SCALE))
        .ok_or_else(overflow)?;
    let cross = aw
        .checked_mul(br)
        .and_then(|v| v.checked_add(ar.checked_mul(bw)?))
        .ok_or_else(overflow)?;
    let sum = whole.checked_add(cross).ok_or_else(overflow)?;

    // `ar * br` is the only term with a fractional part left to round.
    let fine = ar * br;
    let rounded = if fine >= 0 {
        (fine + SCALE / 2) / SCALE
    } else {
        (fine - SCALE / 2) / SCALE
    };
    sum.checked_add(rounded).map(Money).ok_or_else(overflow)
}

/// Nanoseconds since the Unix epoch.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct UnixNanos(pub i64);

impl UnixNanos {
    pub const MIN: Self = Self(i64::MIN);
    pub const MAX: Self = Self(i64::MAX);

    #[inline]
    pub const fn new(nanos: i64) -> Self {
        Self(nanos)
    }

    #[inline]
    pub const fn get(self) -> i64 {
        self.0
    }

    pub fn checked_add_nanos(self, nanos: i64) -> Result<Self> {
        self.0
            .checked_add(nanos)
            .map(Self)
            .ok_or(BacktestError::Overflow { what: "UnixNanos" })
    }
}

impl fmt::Debug for UnixNanos {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "UnixNanos({})", self.0)
    }
}

impl fmt::Display for UnixNanos {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// The two timestamps every replayed record carries.
///
/// `ts_init >= ts_event` always: a system cannot learn of an event before
/// it happens. Data that violates it is rejected at ingestion rather than
/// silently reordered, because the usual cause is a vendor field mapped to
/// the wrong column, and replaying it would quietly grant look-ahead.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Stamps {
    pub ts_event: UnixNanos,
    pub ts_init: UnixNanos,
}

impl Stamps {
    pub fn new(ts_event: UnixNanos, ts_init: UnixNanos) -> Result<Self> {
        if ts_init < ts_event {
            return Err(BacktestError::CausalityViolation {
                ts_event: ts_event.get(),
                ts_init: ts_init.get(),
            });
        }
        Ok(Self { ts_event, ts_init })
    }

    /// Both stamps equal: the record was known the instant it happened.
    pub fn immediate(ts: UnixNanos) -> Self {
        Self {
            ts_event: ts,
            ts_init: ts,
        }
    }

    /// How late the system learned of it.
    pub fn delay_nanos(self) -> i64 {
        self.ts_init.get().saturating_sub(self.ts_event.get())
    }
}

/// Which side of the book.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash)]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    #[inline]
    pub fn opposite(self) -> Self {
        match self {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        }
    }

    /// +1 for a buy, -1 for a sell: the sign a fill applies to a position.
    #[inline]
    pub fn signum(self) -> i64 {
        match self {
            Side::Buy => 1,
            Side::Sell => -1,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Side::Buy => "buy",
            Side::Sell => "sell",
        }
    }

    pub fn parse(text: &str) -> Result<Self> {
        match text.to_ascii_lowercase().as_str() {
            "buy" | "bid" | "b" => Ok(Side::Buy),
            "sell" | "ask" | "offer" | "s" | "a" => Ok(Side::Sell),
            other => Err(BacktestError::Parse {
                what: "side",
                value: other.to_string(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_point_round_trips_through_f64() {
        for value in [0.0, 0.5, 1.0, 0.0001, 0.9999, -3.25, 12345.678901234] {
            let price = Price::from_f64(value).unwrap();
            assert!((price.to_f64() - value).abs() < 1e-9, "{value}");
        }
    }

    #[test]
    fn from_f64_rounds_half_away_from_zero() {
        // 1.5 raw units either side of zero.
        assert_eq!(Price::from_f64(1.5e-9).unwrap().raw(), 2);
        assert_eq!(Price::from_f64(-1.5e-9).unwrap().raw(), -2);
    }

    #[test]
    fn from_f64_rejects_non_finite_and_overflow() {
        assert!(Price::from_f64(f64::NAN).is_err());
        assert!(Price::from_f64(f64::INFINITY).is_err());
        assert!(Price::from_f64(1e30).is_err());
    }

    #[test]
    fn display_trims_trailing_zeros_but_keeps_precision() {
        assert_eq!(Price::from_f64(0.5).unwrap().to_string(), "0.5");
        assert_eq!(Price::from_units(3).unwrap().to_string(), "3");
        assert_eq!(Price::from_raw(-1).to_string(), "-0.000000001");
        assert_eq!(Price::from_f64(0.0001).unwrap().to_string(), "0.0001");
    }

    #[test]
    fn notional_is_exact_for_representable_values() {
        let price = Price::from_f64(0.37).unwrap();
        let qty = Qty::from_units(100).unwrap();
        assert_eq!(
            notional(price, qty).unwrap(),
            Money::from_f64(37.0).unwrap()
        );
    }

    #[test]
    fn notional_does_not_overflow_i64_intermediates() {
        // Both factors are large enough that price.raw * qty.raw exceeds i64.
        let price = Price::from_units(1_000_000).unwrap();
        let qty = Qty::from_units(1_000).unwrap();
        assert_eq!(
            notional(price, qty).unwrap(),
            Money::from_units(1_000_000_000).unwrap()
        );
    }

    #[test]
    fn notional_rounds_symmetrically() {
        // 0.5 raw units of product, positive and negative.
        let half = Price::from_raw(1);
        let qty = Qty::from_raw(SCALE / 2);
        assert_eq!(notional(half, qty).unwrap().raw(), 1);
        assert_eq!(notional(-half, qty).unwrap().raw(), -1);
    }

    #[test]
    fn probability_complement_is_exact() {
        let yes = Price::from_f64(0.37).unwrap();
        let no = yes.complement().unwrap();
        assert_eq!(no, Price::from_f64(0.63).unwrap());
        assert_eq!(no.complement().unwrap(), yes);
    }

    #[test]
    fn stamps_reject_learning_before_the_event() {
        let event = UnixNanos::new(1_000);
        assert!(Stamps::new(event, UnixNanos::new(999)).is_err());
        assert!(Stamps::new(event, event).is_ok());
        assert_eq!(
            Stamps::new(event, UnixNanos::new(1_500))
                .unwrap()
                .delay_nanos(),
            500
        );
    }

    #[test]
    fn side_parses_the_names_vendors_actually_use() {
        assert_eq!(Side::parse("BID").unwrap(), Side::Buy);
        assert_eq!(Side::parse("ask").unwrap(), Side::Sell);
        assert_eq!(Side::parse("Sell").unwrap(), Side::Sell);
        assert!(Side::parse("maybe").is_err());
        assert_eq!(Side::Buy.opposite(), Side::Sell);
    }

    #[test]
    fn checked_arithmetic_reports_overflow() {
        assert!(
            Money::from_raw(i64::MAX)
                .checked_add(Money::from_raw(1))
                .is_err()
        );
        assert!(Money::from_units(i64::MAX).is_err());
    }
}
