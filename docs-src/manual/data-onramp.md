---
title: Data on-ramp
description: "h5i_db.venues: turning vendor archives, bar files, trade dumps and live captures into the canonical tables a replay reads."
order: 7.5
seo_title: "h5i-db data on-ramp: vendor archives to canonical tables"
---

# Data on-ramp

`h5i_db.venues` turns vendor files **already on disk** into the canonical
tables a [backtest](backtest.html) reads. It does not fetch. Downloading
belongs in a script, where credentials, retries and rate limits belong; this
layer is the part that has to be reproducible and testable offline.

The canonical tables it writes are `book_deltas`, `trades`, `instruments`,
`resolutions`, `bars`, `funding`, `references` and `corporate_actions`.
`venues.CANONICAL_SCHEMAS` maps each name to its Arrow schema (the
individual `TRADES_SCHEMA`, `BOOK_DELTAS_SCHEMA`, … constants are the same
objects), and `venues.ensure_tables(db, names)` creates any that are
missing.

## Three steps, each usable alone

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

The same from a shell, with market definitions travelling as a JSON file
rather than as flags:

```bash
python -m h5i_db.venues markets market.db specs.json
python -m h5i_db.venues ingest  market.db specs.json --root /mnt/pmxt \
    --start-ns 1777000000000000000 --end-ns 1777003600000000000 --min-coverage 0.95
python -m h5i_db.venues bars    market.db --root /mnt/klines \
    --layout binance-klines --instrument BINANCE:BTCUSDT
python -m h5i_db.venues inspect market.db
```

The same four verbs are on the `h5i-venues` console script, and
`h5i-backtest` / `h5i-capture` are the equivalents for the other two
modules.

`--min-coverage` exits non-zero rather than letting a short load pass
quietly, which is what makes this usable in a scheduled backfill. Gating
without a window is an error (exit `2`), not a silent pass; falling short of
the threshold is `3`.

## Market identity is positional, and refused when ambiguous

`MarketSpec` pairs `outcome_labels` with `tokens` by index: index *i* of each
describes the same outcome. Getting that backwards attributes every fill to
the wrong side, so the constructor refuses every loose way of expressing it.

Refused, each with a named reason: fewer than two outcomes; a token list that
does not match the outcome list; duplicate tokens; a token two markets both
claim; a `winner_outcome` with no `settlement_observable_ns` (settlement is
gated on when the result became knowable, so a resolution without that
instant is unusable); an observability instant before expiry.

`polymarket_markets_from_json` handles the awkward real shapes: list fields
arriving as JSON-encoded strings, a resolution expressed as settled
`outcomePrices` plus a `closed` flag, ISO-8601 or epoch times. Pass
`require_resolution=True` when a settlement study needs resolved markets
only.

## A vendor dialect is data, not a code path

`ArchiveLayout` carries the column names, event vocabulary, timestamp unit
and level shape. `PMXT_LAYOUT`, `TELONEX_LAYOUT`, `KALSHI_PMXT_LAYOUT`,
`LIMITLESS_PMXT_LAYOUT`, `OPINION_PMXT_LAYOUT`,
`KAGGLE_POLYMARKET_LAYOUT` and `KAGGLE_POLYMARKET_TRADES_LAYOUT` are
literals of that type, and a new vendor is another literal rather than
another module.

Three level shapes exist:

| `LevelLayout(style=…)` | Shape |
|---|---|
| `nested` | `bids`/`asks` are list columns of structs, one row per book state |
| `flat` | One row per level, side in its own column |
| `payload` | The whole event is a JSON string in one column — what a websocket capture written straight to Parquet looks like |

Under `payload` the token usually lives inside the JSON, so filtering
happens in two passes: cheaply on the instrument column, then on the decoded
token.

```python
layout = venues.ArchiveLayout(
    name="house-feed", timestamp_column="recv_ns", timestamp_unit="ns",
    token_column="token", event_type_column="channel", snapshot_events=("depth",),
    levels=venues.LevelLayout(style="nested", bids_column="buys", asks_column="sells",
                              price_field="px", size_field="qty"),
    max_levels=1,     # keep top of book; the drop count is reported
)
```

No directory convention is assumed. Pass explicit `files=`, or a root plus a
glob to `venues.discover(root, pattern=…)`: a mirror layout is not a data
contract, and people mirror differently.

## Four properties worth knowing before pointing it at a mirror

**Re-running is a replay, not a duplicate.** Every commit is keyed by the
hash of the *normalised* rows, so identical inputs produce identical keys and
h5i-db recognises them. An interrupted backfill is safe to restart, and two
sources serving the same hour converge on one commit. `report.replayed` says
whether the whole ingest was already present.

**Requested and loaded windows stay separate facts.** `report.coverage` is
the loaded span over the requested one, and `None` when no window was asked
for, because a ratio against an unbounded request means nothing. It says
nothing about holes *inside* the span; those are `report.gaps`.

**Nothing is guessed.** An event type present in the file but absent from the
layout is counted in `report.skipped`; a file missing required columns is
skipped with its name and the missing columns; an unparseable payload is
counted; and `max_levels` records how many levels it dropped. A truncation
nobody can see is how a wrong conclusion arrives three steps later.

**Zero size means delete.** These venues spell "this level is gone" as a
size-zero change, so the importer writes `delete`, not a level with no
quantity.

`IngestReport` carries `vendor`, `tables` (a `TableWrite` per table: rows,
chunks, replayed chunks, idempotency keys), `sources` (a `SourceFile` each:
path, size, rows read, rows kept), `requested_window`, `loaded_window`,
`gaps`, `skipped` and `unknown_instruments`, plus the derived `coverage`,
`rows` and `replayed`. `to_dict()` renders the lot.

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
knowable until its interval ends. That is why a `BarLayout` must supply
either a close-time column or an `interval`: there is no safe default, so
there is no default. The same rule is why `references_from_series` makes you
state `published_after` — a daily rate for Monday is published on Tuesday,
and stamping it at Monday lets a strategy read it a day early.

`GENERIC_OHLCV_LAYOUT` and `BINANCE_KLINES_LAYOUT` cover the common files;
`read_bars_csv` and `bars_from_table` are the pieces underneath, for a file
you want to inspect before writing it. `parse_interval("1m")` is the same
interval parser the layouts use.

Yahoo has had no official API since 2017, so `yfinance` is a scraper that
breaks when Yahoo changes internals. Fetch with it if you like, then hand the
frame to `bars_from_dataframe`: that keeps the breakage in your script rather
than in a parser here. Stooq downloads now sit behind a browser check, so
fetch those by hand and point `read_bars_csv` at the file.

## Trades and corporate actions

A venue that publishes bulk trade files gives you real microstructure for
free.

```python
venues.ingest_trades(db, files=[...], layout=venues.BINANCE_TRADES_LAYOUT,
                     instrument_id="BINANCE:BTCUSDT")
```

The one field worth reading twice is the aggressor. Binance ships
`isBuyerMaker`, which is true when the **buyer** was resting, so the taker
was the seller. Read straight through, it inverts every trade sign, and the
result still balances and still sums to the right volume. `TradeLayout`
therefore takes either `buyer_is_maker_column` or `aggressor_column`, never
both. `BINANCE_TRADES_LAYOUT` and `BINANCE_AGG_TRADES_LAYOUT` are the
shipped literals, and `read_trades_csv` / `trades_from_table` are the
in-memory halves for a file you want to inspect first.

Corporate actions are what make an equity backtest correct rather than
plausible. Without them a 2-for-1 split reads as a 50% overnight crash.

```python
venues.ingest_corporate_actions(
    db,
    actions=[{"instrument_id": "AAPL", "kind": "split", "ratio": 2.0,
              "effective": "2026-03-02", "announced": "2026-02-01"}],
    known_by=simulated_now_ns,   # drop what had not been announced yet
)
```

`effective` is the replay clock, because that is when positions and resting
orders change. Past prices are never rewritten: nobody traded the adjusted
price, and a strategy that bought at 50 the day before a 2-for-1 bought at
50. `announced` is kept separately so `known_by` can reproduce what was
knowable on a past date; rows with no announcement are dropped under a cutoff
rather than assumed early enough, since assuming is how a run ends up
trading a split nobody had heard of.

`corporate_actions_from_rows` and `references_from_series` build the Arrow
tables without writing them, and `ingest_references` appends a reference
series (a benchmark, a rate, a vendor mark) that a strategy may read only
after its `published_after` instant.

## Kalshi and Predexon

Kalshi's hourly archive quotes both outcomes as bids, and its deltas are
signed changes in resting size, so it needs its own layout. Everything else
is the same three steps. A market needs no `tokens=`: the files name the
instrument and pick the outcome with a label, so the outcome order comes from
`outcome_labels`.

```python
specs = [venues.MarketSpec(instrument_id="KXBTC15M-26JUN100815-15",
                           venue="kalshi", outcome_labels=("yes", "no"))]
report = venues.ingest_archive(
    db,
    files=venues.discover("/mnt/kalshi", pattern="kalshi_orderbook_*.parquet"),
    markets=specs,
    layout=venues.KALSHI_PMXT_LAYOUT,
)
```

Read two numbers out of the report before trusting the result:

- `report.gaps` carries a `snapshot_divergence` entry saying how many vendor
  snapshots the reconstruction reproduced exactly. This feed has no sequence
  numbers, so that comparison is the only integrity check available. A low
  share almost always means the window's deltas are incomplete, and the fix
  is to load the neighbouring hours, not to lower expectations.
- `report.skipped` counts changes that arrived with no book to apply them to
  (`delta_before_snapshot`). An hour that was never snapshotted for a market
  contributes nothing rather than inventing a base of zero.

Predexon serves snapshots instead of an archive:

```python
snapshots = fetch_pages(...)          # your script, your API key
report = venues.ingest_predexon_orderbooks(db, snapshots=snapshots, markets=specs)
```

Check `report.gaps` for `snapshot_cadence` before trusting a window: it
carries the measured median and worst gap between samples, and the worst gap
is the number that matters. The vendor's `sequence` field is deliberately
unread, because it is not a per-market counter — it steps by a median of 45,
jumps by millions, and runs backwards — so differencing it invents holes that
are not there.

### One book, two sides

Kalshi publishes `yes_bids` and `no_bids`, two books of bids, because an ask
on YES is a bid on NO. Both the archive layout and the Predexon reader fold
them into a single two-sided book on outcome 0, at `1 - price` for the NO
side. A capture, an archive and a Predexon pull therefore give the same
canonical shape.

That fold is not cosmetic. Storing the two as separate one-sided books leaves
a market with no asks at all, and an order that cannot fill is **cancelled**
rather than rejected, so the run completes and the strategy simply reads as
having declined to trade.

## Manifold

Manifold publishes markets and bets as JSON rather than as book files, so it
has its own pair of readers:

```python
specs = venues.manifold_markets_from_json(payloads)
venues.write_markets(db, specs)
venues.ingest_manifold_bets(db, bets=bets, markets=specs)
```

`manifold_trades_from_json` is the same conversion without the write. Both
take a `skipped=` collector, so payloads they refused are countable rather
than silently absent.

## Recording a live feed

`h5i-capture` (from `pip install h5i-db[capture]`, or
`python -m h5i_db.capture`) records a venue websocket to lz4-compressed
newline-delimited JSON — the same format the archive readers consume, so a
capture and a vendor archive load through one path.

```bash
export KALSHI_API_KEY_ID=…            # never a flag: a flag lands in ps output
export KALSHI_PRIVATE_KEY_PATH=/path/to/key.pem
h5i-capture --venue kalshi --out ./capture --market KXBTCD-25DEC31
```

| Flag | Meaning |
|---|---|
| `--venue kalshi\|polymarket` | Kalshi needs credentials; Polymarket is public |
| `--out <dir>` | Files land in `<out>/<venue>/<date>/<hour>` |
| `--market <id>` | Repeatable, or comma-separated |
| `--channel <name>` | Override the venue's default channels |
| `--url <ws>` | Point at a demo or staging endpoint |
| `--flush-secs <n>` | How often completed lz4 blocks reach the file — this bounds what a `kill -9` can destroy |
| `--keepalive-secs`, `--max-backoff-secs` | Keepalive cadence and reconnect ceiling |

It stamps arrival in nanoseconds and writes the payload verbatim. Both rules
exist for the same reason: an arrival stamp cannot be reconstructed later,
and parsing on the write path means a parser bug costs the data rather than
an afternoon.

The same thing as a library, for a feed the CLI does not speak:

```python
from h5i_db.capture import CaptureWriter, archive_line, now_nanos, read_hour

with CaptureWriter("./capture", "kalshi", flush_after=5.0) as writer:
    received_at = now_nanos()
    writer.write_line(received_at, archive_line(received_at, frame))

read_hour("./capture/kalshi/2026-07-31", "14")   # or read_capture(path)
```

Credentials are read from `KALSHI_API_KEY_ID` + `KALSHI_PRIVATE_KEY_PATH`
(or `KALSHI_API_TOKEN`), never from an argument; `sign_kalshi` and
`kalshi_headers` are the signing pieces, and a missing credential raises
`MissingCredential` rather than connecting anonymously and failing later.

This is a separate package from `h5i_db.venues` on purpose. That package is
parse-only — every function takes bytes the caller already downloaded, which
is what makes the mapping a pure function testable offline against recorded
payloads. Sockets, credentials and reconnect policy would end that, so they
live here. The two meet at a file format, not at a function call. The extra's
dependencies (`websockets`, `cryptography`, `lz4`) are imported inside the
functions that need them, so a user who only reads archives never pays for a
TLS stack.

Record only what an archive cannot give you. For Kalshi after January 2026,
`predexon_book_from_snapshots` is usually the better answer, and recording
buys sub-cent precision, your own arrival clock, and independence from a free
service rather than access to otherwise-missing data.

## Replaying an account's ledger

The strictest realism question available: given the trades an account
actually took, does the engine reproduce the same portfolio? Usually not, and
that is the point — a forced-fill simulator would reproduce the ledger by
construction and test nothing.

```python
rows = [venues.LedgerRow(ts_ns=…, instrument_id=…, outcome=0,
                         side="buy", quantity=…, price=…), …]
commands = venues.commands_from_ledger(rows, specs)   # limit-IOC at the ledger price
db.append("commands", commands)                       # sells are reduce_only
result = backtest.execute(db, config)                 # DataConfig(commands="commands", …)
venues.compare_to_ledger(result, typed_rows)          # per-market reconciliation
```

Compiled into *intent*, not fills, so the historical book accepts or refuses
each order on its merits. `reduce_only` on sells stops a replay inventing
short exposure the ledger never showed. The comparison reports per-market
shortfalls rather than one pass/fail, because *where* the book refused is the
finding. `ledger_table(rows)` writes the ledger itself as a table when you
want the two side by side in SQL.

## Fetching, when you have network

Fetchers are scripts, not core API surface. Two limits are properties of the
public APIs rather than of the tooling: Polymarket's book endpoint serves
**live markets only**, so a resolved-market list is the right input for
definitions and the wrong one for books, and historical coverage is price
*points*, not books, so any depth or queue study needs a captured archive.

One practical trap: these hosts answer `403` to the default `Python-urllib`
user agent, which reads exactly like a blocked network and is not one. Set
any `User-Agent`.
