# h5i-db venue adapters

Vendor payloads into the canonical market-data tables — `instruments`,
`book_deltas`, `trades`, `bars`, `funding`, `references`, `resolutions` — so
the backtest kernel never learns a venue's shape.

**The extension point is the tables, not this crate.** Anything that can write
them is a loader, in any language and any process: ingest is a versioned
commit with provenance, a misbehaving loader corrupts a fork rather than a
process, and loaders and the kernel never run at the same time, so there is no
runtime channel to make fast. What this crate adds is the wrong-able part done
once and tested — which field is the timestamp, in what unit, whether a zero
size empties a level or deletes it, when a bar became knowable.

These functions parse; they do not fetch. Every one takes bytes a caller
already downloaded, which keeps HTTP, credentials, pagination and rate limits
in a script and leaves the whole mapping a pure function testable offline
against recorded payloads.

## What each venue supports today

| | Kalshi | Polymarket | Hyperliquid |
|---|---|---|---|
| Instruments | REST market metadata | one market, N outcomes | perp and spot universes |
| L2 book | REST + WS snapshots, sequence-checked deltas | `book` snapshots, `price_change` deltas | REST + WS |
| Trades | ✓ live and historical, one parser | ✓ | ✓ |
| Bars | ✓ candlesticks | ✗ | ✓ candles |
| Funding | n/a | n/a | ✓ |
| Mark and oracle price | n/a | n/a | ✓ per-asset contexts |
| Margin and leverage | n/a | n/a | ✓ per-coin cap, isolated-only flag |
| Settlement | ✓ once observable | ✓ from market resolution | n/a |
| Complete-set mint and redeem | ✗ | ✓ neg-risk markets | n/a |
| Fee model | quadratic, exchange rounding | proportional + maker rebate | 14-day rolling volume tiers |
| Historical archive | ✗¹ | pmxt, telonex, Kaggle layouts | hourly feed + asset contexts |

¹ Kalshi publishes no historical order-book deltas, so a queue-accurate
backtest needs prospective capture of the authenticated WebSocket stream. A
missing sequence number raises a gap rather than interpolating across it.

`n/a` means the venue has no such concept: a perpetual never resolves, and a
fully collateralized prediction market has no leverage or funding. That is a
different statement from `✗`, which means not implemented.

## Adding a venue

Write the canonical tables. A loader does not have to live here, and does not
have to be Rust — the Polymarket archive layouts under
`h5i_db/venues/_archive.py` are Python, and read third-party captures this
crate never sees.

If it does live here, note that venues do not implement a common trait. Their
inputs genuinely differ (Polymarket addresses outcomes by token, Hyperliquid
by coin and interval) and one signature would fit neither. What they share is
an output: `IngestPlan`, which `write_plan` commits.
