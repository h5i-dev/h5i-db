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
    PMXT_LAYOUT,
    TELONEX_LAYOUT,
    ArchiveLayout,
    LevelLayout,
    discover,
    ingest_archive,
)
from ._canonical import (
    BOOK_DELTAS_SCHEMA,
    CANONICAL_SCHEMAS,
    INSTRUMENTS_SCHEMA,
    RESOLUTIONS_SCHEMA,
    TRADES_SCHEMA,
    IngestReport,
    SourceFile,
    TableWrite,
    content_key,
    ensure_tables,
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
    "PMXT_LAYOUT",
    "TELONEX_LAYOUT",
    "discover",
    "ingest_archive",
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
    "BOOK_DELTAS_SCHEMA",
    "CANONICAL_SCHEMAS",
    "INSTRUMENTS_SCHEMA",
    "RESOLUTIONS_SCHEMA",
    "TRADES_SCHEMA",
    "IngestReport",
    "SourceFile",
    "TableWrite",
    "content_key",
    "ensure_tables",
]
