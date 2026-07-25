# Research mode: showing an agent the past and nothing else

Look-ahead bias is the correctness bug in backtesting, and it gets worse when
an agent runs forty backtests overnight: nobody reviews forty results closely
enough to notice that one of them quietly read tomorrow's close. So the
database withholds the future structurally instead of asking the query to.

There are two ways data can leak from the future, and they need different
defences.

## Event-time axis — `--decision-time`

Rows that were always in the table but are stamped after the moment you are
deciding at. This is the common case: a window that overruns forwards, a join
reading a later timestamp, a mean computed over the whole sample.

```bash
h5i-db query market.db "SELECT vwap(price, size) FROM trades" \
  --decision-time 2026-07-01T00:00:00Z \
  --embargo 1d
```

Every scan in that query is bounded by `time_column <= decision_time -
embargo`. It is part of the table, not a filter you compose with, so a query
that explicitly asks for later rows still gets none. Walk a backtest forward by
re-running with successive decision times.

Three refusals worth knowing, all deliberate, all following the same rule: a
bound that cannot be applied is an error, never a silent exemption.

* A table with no time column, and one whose time column is a bare integer
  carrying no unit, refuse the session outright.
* The table functions (`h5i()`, `asof_join()`, `gapfill()`, `resample()`,
  `tail()`, `latest_on()`) read their tables directly rather than through the
  catalog, so the cutoff cannot be pushed into them. Under `--decision-time`
  they refuse and tell you to reference the table by name, which is bounded.
  Under `--as-of` alone they work normally, resolving at the pinned version.

## Arrival axis — `--as-of`

Rows that exist now but had not arrived yet — a vendor restatement, a late
print, an index reconstitution. These are invisible on the event-time axis
because their timestamps are old; what is new is the *commit*.

```bash
h5i-db query market.db "<sql>" --as-of 2026-07-01T00:00:00Z   # availability
h5i-db query market.db "<sql>" --as-of 42                     # or a version
h5i-db query market.db "<sql>" --as-of pre-experiment         # or a snapshot
```

Every table is pinned, not just the one you name, and the table functions
resolve at the pin too. A pinned session picks the read point, so `h5i('t', 42)`
inside one is refused rather than quietly honoured.

The two axes are independent because they are measured on different clocks. On
a database built from one bulk load, ten years of history has a first commit of
this morning: the arrival axis has nothing to withhold, while the event-time
axis is doing all the work. On a continuously-ingested database both matter.

## Pinning a whole session

```bash
export H5I_DB_DECISION_TIME=2026-07-01T00:00:00Z
export H5I_DB_EMBARGO=1d
export H5I_DB_AS_OF=2026-07-01T00:00:00Z
```

Every subsequent `query` inherits the pin, which is what makes it a jail rather
than a flag you can forget. An explicit flag still wins over the environment.

## Auditing after the fact — `arrival-delta`

Runs one query twice, at head and at a decision point, and reports the delta:
the alpha that evaporates is the part that came from data that had not arrived.

```bash
h5i-db arrival-delta market.db "<sql>" --as-of 2026-07-01T00:00:00Z --format json
```

Read the report's `notes` before its numbers. A zero delta does not prove there
is no look-ahead: it only sees the arrival axis. And if the report says
`vacuous: true`, the check compared identical data (nothing was withheld,
typically because the database has no arrival history at all), so its zero
means nothing whatsoever.
