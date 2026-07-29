//! Vendor loaders: venue payloads into canonical records.
//!
//! Every loader here **parses**; none of them fetch. That split is
//! deliberate. The hard, wrong-able part of ingesting market data is the
//! mapping -- which field is the timestamp, what unit it is in, whether a
//! zero size means an empty level or a deleted one, when a bar became
//! knowable -- and all of it is pure, so all of it is tested offline
//! against recorded payload shapes. HTTP, retries, and pagination belong in
//! a script that hands bytes to these functions.
//!
//! The uniform output is what makes the kernel venue-neutral: a Polymarket
//! prediction market and a Hyperliquid perpetual both arrive as
//! [`crate::event::Record`]s and land in the same `book_deltas`, `trades`
//! and `funding` tables. Nothing downstream of this module knows which
//! venue a row came from.

pub mod hyperliquid;
pub mod polymarket;
