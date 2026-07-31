"""Vendor archives into canonical h5i-db market-data tables.

The on-ramp for anyone arriving with a local mirror of prediction-market data.
Three steps, each usable on its own:

```python
from h5i_db import venues

specs = venues.polymarket_markets_from_json(payloads)     # slug -> outcomes/tokens
venues.write_markets(db, specs)                            # instruments, resolutions
report = venues.ingest_archive(                            # book_deltas, trades
    db,
    files=venues.discover("/mnt/pmxt"),
    markets=specs,
    layout=venues.PMXT_LAYOUT,
)
print(report.coverage, report.gaps, report.replayed)
```

Nothing here fetches. Downloading belongs in a script, where credentials,
retries and rate limits belong; this module turns files already on disk into
tables, which is the part worth testing offline and the part that must be
byte-reproducible.

Re-running an import is a replay, not a duplicate: every commit is keyed by the
hash of the rows it carries, so the same inputs produce the same keys and
h5i-db recognises them. That makes an interrupted backfill safe to restart.
"""

from __future__ import annotations

from ._archive import (
    KAGGLE_POLYMARKET_LAYOUT,
    KAGGLE_POLYMARKET_TRADES_LAYOUT,
    KALSHI_PMXT_LAYOUT,
    LIMITLESS_PMXT_LAYOUT,
    OPINION_PMXT_LAYOUT,
    PMXT_LAYOUT,
    TELONEX_LAYOUT,
    ArchiveLayout,
    LevelLayout,
    discover,
    ingest_archive,
)
from ._bars import (
    BINANCE_KLINES_LAYOUT,
    GENERIC_OHLCV_LAYOUT,
    BarLayout,
    bars_from_dataframe,
    bars_from_table,
    bars_from_trades,
    ingest_bars,
    parse_interval,
    read_bars_csv,
)
from ._canonical import (
    BARS_SCHEMA,
    BOOK_DELTAS_SCHEMA,
    CANONICAL_SCHEMAS,
    CORPORATE_ACTIONS_SCHEMA,
    FUNDING_SCHEMA,
    INSTRUMENTS_SCHEMA,
    REFERENCES_SCHEMA,
    RESOLUTIONS_SCHEMA,
    TRADES_SCHEMA,
    IngestReport,
    SourceFile,
    TableWrite,
    content_key,
    ensure_tables,
)
from ._feeds import (
    corporate_actions_from_rows,
    ingest_corporate_actions,
    ingest_manifold_bets,
    ingest_predexon_orderbooks,
    predexon_book_from_snapshots,
    ingest_references,
    manifold_markets_from_json,
    manifold_trades_from_json,
    references_from_series,
)
from ._trades import (
    BINANCE_AGG_TRADES_LAYOUT,
    BINANCE_TRADES_LAYOUT,
    TradeLayout,
    ingest_trades,
    read_trades_csv,
    trades_from_table,
)
from ._ledger import (
    LedgerRow,
    commands_from_ledger,
    compare_to_ledger,
    ledger_table,
)
from ._markets import (
    MarketSpec,
    polymarket_markets_from_json,
    token_index,
    write_markets,
)

__all__ = [
    # layouts and ingest
    "ArchiveLayout",
    "LevelLayout",
    "KAGGLE_POLYMARKET_LAYOUT",
    "KAGGLE_POLYMARKET_TRADES_LAYOUT",
    "KALSHI_PMXT_LAYOUT",
    "LIMITLESS_PMXT_LAYOUT",
    "OPINION_PMXT_LAYOUT",
    "PMXT_LAYOUT",
    "TELONEX_LAYOUT",
    "discover",
    "ingest_archive",
    # trade dumps
    "TradeLayout",
    "BINANCE_AGG_TRADES_LAYOUT",
    "BINANCE_TRADES_LAYOUT",
    "ingest_trades",
    "read_trades_csv",
    "trades_from_table",
    # feeds that are not archives
    "corporate_actions_from_rows",
    "ingest_corporate_actions",
    "ingest_manifold_bets",
    "ingest_predexon_orderbooks",
    "predexon_book_from_snapshots",
    "ingest_references",
    "manifold_markets_from_json",
    "manifold_trades_from_json",
    "references_from_series",
    # bars
    "BarLayout",
    "BINANCE_KLINES_LAYOUT",
    "GENERIC_OHLCV_LAYOUT",
    "bars_from_dataframe",
    "bars_from_table",
    "bars_from_trades",
    "ingest_bars",
    "parse_interval",
    "read_bars_csv",
    # markets
    "MarketSpec",
    "polymarket_markets_from_json",
    "token_index",
    "write_markets",
    # ledger replay
    "LedgerRow",
    "commands_from_ledger",
    "compare_to_ledger",
    "ledger_table",
    # canonical layer
    "BARS_SCHEMA",
    "BOOK_DELTAS_SCHEMA",
    "CANONICAL_SCHEMAS",
    "CORPORATE_ACTIONS_SCHEMA",
    "FUNDING_SCHEMA",
    "INSTRUMENTS_SCHEMA",
    "REFERENCES_SCHEMA",
    "RESOLUTIONS_SCHEMA",
    "TRADES_SCHEMA",
    "IngestReport",
    "SourceFile",
    "TableWrite",
    "content_key",
    "ensure_tables",
]
