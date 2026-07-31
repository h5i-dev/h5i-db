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
| Bars | ✓ candlesticks | ✓ derived from trades | ✓ candles |
| Funding | n/a | n/a | ✓ |
| Mark and oracle price | n/a | n/a | ✓ per-asset contexts |
| Margin and leverage | n/a | n/a | ✓ per-coin cap, isolated-only flag |
| Settlement | ✓ once observable | ✓ from market resolution | n/a |
| Complete-set mint and redeem | ✗ | ✓ neg-risk markets | n/a |
| Fee model | quadratic, exchange rounding | proportional + maker rebate | 14-day rolling volume tiers |

History is worth splitting by what you are actually loading, because the three
answers differ per venue and so does where the data comes from:

| | Kalshi | Polymarket | Hyperliquid |
|---|---|---|---|
| Historical trades | official REST | official Data API | official archive |
| Historical bars | official candlesticks | derived from trades | official candles |
| Historical L2 | third-party archive¹ or self-capture | third-party² | official hourly archive |

¹ Neither venue publishes its own historical order book, so `official` is not
an option in that row for either of them. A third-party archive (pmxt) does now
carry Kalshi, which is a change from when this crate was written, and
`KALSHI_PMXT_LAYOUT` reads it. Three properties of those files were measured
rather than assumed, and they bound what the data can be used for:

* Its deltas are signed changes in resting size, not new sizes. Replaying them
  with exact decimal arithmetic reproduces a later vendor snapshot in about
  half the snapshot pairs in an hour; the misses are incomplete delta coverage
  within the hour, not arithmetic, and loading neighbouring hours reduces them.
* Its snapshot rows carry no venue timestamp, and the arrival stamp they do
  carry is a flush stamp running 8 to 34 minutes late with no stable offset. So
  snapshots seed the book and are then compared against it rather than replayed
  on a clock they do not share with the deltas.
* There are no per-market sequence numbers, so a dropped message cannot be
  detected the way the live decoder detects one. Divergence against later
  snapshots is measured and reported instead, which is weaker but honest.

Coverage also lags: at the time of writing the Kalshi files ran about seven
weeks behind, while the same host's Limitless and Opinion files were current.
Anything needing queue accuracy or recent data still wants prospective capture
of the authenticated WebSocket, where a missing sequence number raises a gap
rather than interpolating across it.

² pmxt, telonex and Kaggle layouts. The pmxt CLOB dialect is shared verbatim by
Limitless and Opinion, which `LIMITLESS_PMXT_LAYOUT` and `OPINION_PMXT_LAYOUT`
read. Those files carry a `receive_sequence`, but it counts the capture's own
messages across every market at once rather than one venue stream, so it is not
read as a per-instrument sequence check.

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
