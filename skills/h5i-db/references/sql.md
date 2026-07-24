# h5i-db SQL extensions

Available inside any `h5i-db query` / `db.sql(...)` statement:

| Function | Purpose |
|---|---|
| `h5i('trades')`, `h5i('trades', 42)`, `h5i('trades', '2026-07-01T00:00:00Z')`, `h5i('trades', 'snapname')` | time travel: latest / version / as-of / snapshot |
| `asof_join('trades','quotes','ts','ts','symbol'[,'backward'\|'forward'[,tolerance]])` | most-recent-quote-per-trade joins |
| `time_bucket('1m', ts)` | bucketing (also '5s', '1h', '1d', '1mo'…) |
| `vwap(price, size)` / `wavg(w, x)` | weighted aggregates |
| `ewma(x, alpha) OVER (PARTITION BY sym ORDER BY ts)` | exponential smoothing |
| `first_value/last_value(price ORDER BY ts)` | OHLC open/close |

Add `--stats` to see pruning (segments skipped) on stderr.
