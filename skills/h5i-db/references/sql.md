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

## Keeping results small

Under `H5I_DB_PROFILE=agent` every query is capped (1000 rows, 1 MiB) and a
JSON summary on stderr reports the true `total_rows` plus a
`full_result_path` — a Parquet file holding the rows stdout withheld. Nothing
is lost, only withheld, so there is no need to guess a `LIMIT` defensively.

Explicit flags still win: `--max-rows` sets the rendered size, and a
`--max-bytes` you pass yourself remains a hard error (exit 4) rather than a
soft truncation.
