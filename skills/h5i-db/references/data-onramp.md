# Getting vendor data into canonical tables

`h5i_db.venues` turns vendor files **already on disk** into the tables a replay
reads. It does not fetch: downloading belongs in a script, where credentials,
retries and rate limits belong, and this layer is the part that must be
reproducible and testable offline.

Three steps, each usable alone.

```python
from h5i_db import venues

specs  = venues.polymarket_markets_from_json(payloads)   # slug -> outcomes, tokens
venues.write_markets(db, specs)                          # instruments, resolutions
report = venues.ingest_archive(                          # book_deltas, trades
    db,
    files=venues.discover("/mnt/pmxt"),
    markets=specs,
    layout=venues.PMXT_LAYOUT,
    window=(start_ns, end_ns),
)
report.coverage, report.gaps, report.replayed, report.skipped
```

The same from a shell, with market definitions travelling as a JSON file rather
than as flags:

```bash
python -m h5i_db.venues markets market.db specs.json
python -m h5i_db.venues ingest  market.db specs.json --root /mnt/pmxt \
    --start-ns 1777000000000000000 --end-ns 1777003600000000000 --min-coverage 0.95
python -m h5i_db.venues inspect market.db
```

`--min-coverage` exits non-zero rather than letting a short load pass quietly,
which is what makes this usable in a scheduled backfill. Gating without a window
is an error (`2`), not a silent pass; falling short of the threshold is `3`.

## Market identity is positional and refused when ambiguous

`MarketSpec` pairs `outcome_labels` with `tokens` by index: index *i* of each
describes the same outcome. Getting that backwards attributes every fill to the
wrong side, so the constructor refuses every way of expressing it loosely.

Refused, each with a named reason: fewer than two outcomes; a token list that
does not match the outcome list; duplicate tokens; a token two markets both
claim; a `winner_outcome` with no `settlement_observable_ns` (settlement is gated
on when the result became knowable, so a resolution without that instant is
unusable); an observability instant before expiry.

`polymarket_markets_from_json` handles the awkward real shapes: list fields
arriving as JSON-encoded strings, a resolution expressed as settled
`outcomePrices` plus a `closed` flag, ISO-8601 or epoch times. Pass
`require_resolution=True` when a settlement study needs resolved markets only.

## A vendor dialect is data, not a code path

`ArchiveLayout` carries the column names, event vocabulary, timestamp unit and
level shape. `PMXT_LAYOUT`, `TELONEX_LAYOUT`, `KAGGLE_POLYMARKET_LAYOUT` and
`KAGGLE_POLYMARKET_TRADES_LAYOUT` are literals of that type, and a new vendor is
another literal rather than another module.

Three level shapes exist:

- `nested` — `bids`/`asks` are list columns of structs, one row per book state.
- `flat` — one row per level, side in its own column.
- `payload` — the whole event is a JSON string in one column, which is what a
  websocket capture written straight to Parquet looks like. The token usually
  lives inside it, so filtering happens in two passes: cheaply on the instrument
  column, then on the decoded token.

```python
layout = venues.ArchiveLayout(
    name="house-feed", timestamp_column="recv_ns", timestamp_unit="ns",
    token_column="token", event_type_column="channel", snapshot_events=("depth",),
    levels=venues.LevelLayout(style="nested", bids_column="buys", asks_column="sells",
                              price_field="px", size_field="qty"),
    max_levels=1,     # keep top of book; the drop count is reported
)
```

No directory convention is assumed. Pass explicit files, or a root plus a glob:
a mirror layout is not a data contract, and users mirror differently.

## Four properties worth knowing before pointing it at a mirror

**Re-running is a replay, not a duplicate.** Every commit is keyed by the hash of
the *normalised* rows, so identical inputs produce identical keys and h5i-db
recognises them. An interrupted backfill is safe to restart, and two sources
serving the same hour converge on one commit. `report.replayed` says whether the
whole ingest was already present.

**Requested and loaded windows stay separate facts.** `report.coverage` is the
loaded span over the requested one, and `None` when no window was asked for,
because a ratio against an unbounded request means nothing. It says nothing about
holes inside the span; those are `report.gaps`.

**Nothing is guessed.** An event type present in the file but absent from the
layout is counted in `report.skipped`, a file missing required columns is skipped
with its name and the missing columns, an unparseable payload is counted, and
`max_levels` records how many levels it dropped. A truncation nobody can see is
how a wrong conclusion arrives three steps later.

**Zero size means delete.** These venues spell "this level is gone" as a size-zero
change, so the importer writes `delete`, not a level with no quantity.

## Replaying an account's ledger

The strictest realism question available: given the trades an account actually
took, does the engine reproduce the same portfolio? Usually not, and that is the
point — a forced-fill simulator would reproduce the ledger by construction and
test nothing.

```python
commands = venues.commands_from_ledger(rows, specs)   # limit-IOC at the ledger price
db.append("commands", commands)                       # sells are reduce_only
result = backtest.execute(db, config)                 # DataConfig(commands="commands", ...)
venues.compare_to_ledger(result, typed_rows)          # per-market reconciliation
```

Compiled into *intent*, not fills, so the historical book accepts or refuses each
order on its merits. `reduce_only` on sells stops a replay inventing short
exposure the ledger never showed. The comparison reports per-market shortfalls
rather than one pass/fail, because *where* the book refused is the finding.

## Fetching, when you have network

Fetchers are scripts, not core API surface. The cookbook ships one:

```bash
python scripts/fetch_polymarket.py markets --open --limit 50 --out specs.json
python scripts/fetch_polymarket.py books --specs specs.json --out mirror/
```

Two limits are properties of the public API rather than of the tooling. The
book endpoint serves **live markets only**, so a resolved-market list is the
right input for definitions and the wrong one for books. And historical coverage
is price *points*, not books, so any depth or queue study needs a captured
archive.

One practical trap: these hosts answer `403` to the default `Python-urllib`
user agent, which reads exactly like a blocked network and is not one. Set any
`User-Agent`.

## Kalshi

Kalshi's hourly archive is outcome-major and its deltas are signed changes in
resting size, so it needs its own layout. Everything else is the same three
steps. A market needs no `tokens=`: the files name the instrument and pick the
outcome with a label, so the outcome order comes from `outcome_labels`.

```python
specs = [
    venues.MarketSpec(
        instrument_id="KXBTC15M-26JUN100815-15",
        venue="kalshi",
        outcome_labels=("yes", "no"),
    )
]
report = venues.ingest_archive(
    db,
    files=venues.discover("/mnt/kalshi", pattern="kalshi_orderbook_*.parquet"),
    markets=specs,
    layout=venues.KALSHI_PMXT_LAYOUT,
)
```

Read two numbers out of the report before trusting the result:

* `report.gaps` carries a `snapshot_divergence` entry saying how many vendor
  snapshots the reconstruction reproduced exactly. This feed has no sequence
  numbers, so that comparison is the only integrity check available. A low
  share almost always means the window's deltas are incomplete, and the fix is
  to load the neighbouring hours, not to lower expectations.
* `report.skipped` counts changes that arrived with no book to apply them to
  (`delta_before_snapshot`). An hour that was never snapshotted for a market
  contributes nothing rather than inventing a base of zero.

`LIMITLESS_PMXT_LAYOUT` and `OPINION_PMXT_LAYOUT` read the same host's other
venues, which share the Polymarket dialect.

## Bars

Anything shaped like OHLCV goes through one on-ramp, whatever produced it.

```python
# a vendor dump on disk
venues.ingest_bars(db, files=[...], layout=venues.BINANCE_KLINES_LAYOUT,
                   instrument_id="BINANCE:BTCUSDT")

# anything already in memory: a broker export, yfinance, your own frame
bars = venues.bars_from_dataframe(frame, instrument_id="AAPL")

# a venue that publishes no candles at all
venues.bars_from_trades(db, interval="1m")
```

`ts_init` is the bar **close** and `ts_event` the open, because a bar is not
knowable until its interval ends. That is why a layout must supply either a
close-time column or an `interval`: there is no safe default, so there is no
default. The same rule is why `references_from_series` makes you state
`published_after`. A daily rate for Monday is published on Tuesday, and
stamping it at Monday lets a strategy read it a day early.

Yahoo has had no official API since 2017, so `yfinance` is a scraper that
breaks when Yahoo changes internals. Fetch with it if you like, then hand the
frame to `bars_from_dataframe`: that keeps the breakage in your script instead
of in a parser here. Stooq downloads now sit behind a browser check, so fetch
those by hand and point `read_bars_csv` at the file.
