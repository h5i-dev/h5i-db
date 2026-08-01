"""Canonical market-data tables, and the only path that writes them.

Every venue importer normalises into the schemas here and hands the result to
:func:`commit`. That keeps three properties in one place instead of once per
vendor: the schemas match what the replay kernel reads, appends are
content-addressed so a re-run replays instead of double-appending, and each
commit records where its rows came from.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Optional, Sequence

import pyarrow as pa

__all__ = [
    "BOOK_DELTAS_SCHEMA",
    "TRADES_SCHEMA",
    "INSTRUMENTS_SCHEMA",
    "RESOLUTIONS_SCHEMA",
    "BARS_SCHEMA",
    "FUNDING_SCHEMA",
    "REFERENCES_SCHEMA",
    "CORPORATE_ACTIONS_SCHEMA",
    "CANONICAL_SCHEMAS",
    "IngestReport",
    "SourceFile",
    "TableWrite",
    "commit",
    "content_key",
    "ensure_tables",
]

# The canonical backtest schemas. Time is nanoseconds and tz-naive because that
# is what `h5i-db-backtest` reads; `ts_init` is the replay-order column and the
# time index of every table.
BOOK_DELTAS_SCHEMA = pa.schema(
    [
        pa.field("ts_init", pa.timestamp("ns"), nullable=False),
        pa.field("ts_event", pa.timestamp("ns"), nullable=False),
        pa.field("instrument_id", pa.string(), nullable=False),
        pa.field("outcome", pa.uint16(), nullable=False),
        pa.field("action", pa.string(), nullable=False),
        pa.field("side", pa.string()),
        pa.field("price", pa.float64()),
        pa.field("size", pa.float64()),
        pa.field("event_index", pa.int64(), nullable=False),
        pa.field("is_last", pa.bool_(), nullable=False),
        pa.field("source_vendor", pa.string()),
    ]
)

TRADES_SCHEMA = pa.schema(
    [
        pa.field("ts_init", pa.timestamp("ns"), nullable=False),
        pa.field("ts_event", pa.timestamp("ns"), nullable=False),
        pa.field("instrument_id", pa.string(), nullable=False),
        pa.field("outcome", pa.uint16(), nullable=False),
        pa.field("price", pa.float64(), nullable=False),
        pa.field("size", pa.float64(), nullable=False),
        pa.field("aggressor", pa.string()),
        pa.field("trade_id", pa.string()),
        pa.field("source_vendor", pa.string()),
    ]
)

INSTRUMENTS_SCHEMA = pa.schema(
    [
        pa.field("ts_init", pa.timestamp("ns"), nullable=False),
        pa.field("instrument_id", pa.string(), nullable=False),
        pa.field("venue", pa.string(), nullable=False),
        pa.field("kind", pa.string(), nullable=False),
        pa.field("outcome", pa.uint16(), nullable=False),
        pa.field("outcome_label", pa.string(), nullable=False),
        pa.field("tick_size", pa.float64(), nullable=False),
        pa.field("lot_size", pa.float64(), nullable=False),
        pa.field("expiration_ns", pa.int64()),
        pa.field("settlement_observable_ns", pa.int64()),
        # Whether the venue exchanges a complete set of these outcomes for
        # one unit of cash. Polymarket calls it negative risk.
        pa.field("neg_risk", pa.bool_()),
    ]
)

# A market does not always pick a winner. `kind` says which of the three
# things happened, so no row has to be read in the light of another's
# absence:
#
#   winner  one outcome took the dollar; `outcome` names it
#   split   one row per outcome, each carrying its `payout`
#   void    a complete set refunded at cost across `outcome_count` outcomes
RESOLUTIONS_SCHEMA = pa.schema(
    [
        pa.field("ts_init", pa.timestamp("ns"), nullable=False),
        pa.field("instrument_id", pa.string(), nullable=False),
        pa.field("kind", pa.string(), nullable=False),
        pa.field("outcome", pa.uint16()),
        pa.field("payout", pa.float64()),
        pa.field("outcome_count", pa.uint16()),
    ]
)

# Aggregates. `outcome` is not nullable because a bar is always a bar of
# something tradeable, and a binary market's two sides do not share one.
BARS_SCHEMA = pa.schema(
    [
        pa.field("ts_init", pa.timestamp("ns"), nullable=False),
        pa.field("ts_event", pa.timestamp("ns"), nullable=False),
        pa.field("instrument_id", pa.string(), nullable=False),
        pa.field("outcome", pa.uint16(), nullable=False),
        pa.field("open", pa.float64(), nullable=False),
        pa.field("high", pa.float64(), nullable=False),
        pa.field("low", pa.float64(), nullable=False),
        pa.field("close", pa.float64(), nullable=False),
        pa.field("volume", pa.float64(), nullable=False),
        pa.field("source_vendor", pa.string()),
    ]
)

# Perpetual funding as it became due. No `outcome`: funding is charged on a
# position in the instrument, and a perpetual has exactly one.
FUNDING_SCHEMA = pa.schema(
    [
        pa.field("ts_init", pa.timestamp("ns"), nullable=False),
        pa.field("ts_event", pa.timestamp("ns"), nullable=False),
        pa.field("instrument_id", pa.string(), nullable=False),
        pa.field("rate", pa.float64(), nullable=False),
        pa.field("source_vendor", pa.string()),
    ]
)

# Venue-published mark and oracle prices. Both are nullable because a venue
# can publish one and not the other, and a null must not read as a zero price.
REFERENCES_SCHEMA = pa.schema(
    [
        pa.field("ts_init", pa.timestamp("ns"), nullable=False),
        pa.field("ts_event", pa.timestamp("ns"), nullable=False),
        pa.field("instrument_id", pa.string(), nullable=False),
        pa.field("outcome", pa.uint16(), nullable=False),
        pa.field("mark", pa.float64()),
        pa.field("oracle", pa.float64()),
        pa.field("source_vendor", pa.string()),
    ]
)

# What a company did to its own shares. `ts_init` is the instant the action
# takes effect, because that is when a replay has to apply it to positions and
# resting orders; nothing here rewrites past prices, since nobody ever traded a
# split-adjusted price.
#
# `announced_ns` is what keeps a run honest. Adjustment data is point-in-time,
# and loading every action ever recorded means knowing about a split that, on
# the simulated date, had not been announced yet. Carrying the announcement
# instant makes "only what was known by then" a filter on this table rather
# than a discipline the caller has to remember. Null means unknown, never
# "always known".
CORPORATE_ACTIONS_SCHEMA = pa.schema(
    [
        pa.field("ts_init", pa.timestamp("ns"), nullable=False),
        pa.field("ts_event", pa.timestamp("ns"), nullable=False),
        pa.field("instrument_id", pa.string(), nullable=False),
        # "split" | "dividend" | "delist"
        pa.field("kind", pa.string(), nullable=False),
        # New shares per old share: a 2-for-1 is 2.0, a 1-for-10 reverse is 0.1.
        pa.field("ratio", pa.float64()),
        pa.field("per_share", pa.float64()),
        pa.field("final_price", pa.float64()),
        pa.field("announced_ns", pa.int64()),
        pa.field("source_vendor", pa.string()),
    ]
)

CANONICAL_SCHEMAS: Mapping[str, pa.Schema] = {
    "book_deltas": BOOK_DELTAS_SCHEMA,
    "trades": TRADES_SCHEMA,
    "instruments": INSTRUMENTS_SCHEMA,
    "resolutions": RESOLUTIONS_SCHEMA,
    "bars": BARS_SCHEMA,
    "funding": FUNDING_SCHEMA,
    "references": REFERENCES_SCHEMA,
    "corporate_actions": CORPORATE_ACTIONS_SCHEMA,
}

# Sorting a table before appending: h5i-db requires appended rows to carry
# timestamps at or after what is already stored, and one book event's rows must
# stay contiguous, so `event_index` is the tiebreak inside an instant.
_SORT_KEYS: Mapping[str, tuple[tuple[str, str], ...]] = {
    # `is_last` is the third key on purpose: Arrow's sort is not documented as
    # stable, so without it the terminating row of an event could land in the
    # middle and the store would refuse the event (or worse, mis-group it).
    # Level order within a side is irrelevant, since the book keys by price.
    "book_deltas": (
        ("ts_init", "ascending"),
        ("event_index", "ascending"),
        ("is_last", "ascending"),
    ),
    "trades": (("ts_init", "ascending"), ("instrument_id", "ascending")),
    "instruments": (
        ("ts_init", "ascending"),
        ("instrument_id", "ascending"),
        ("outcome", "ascending"),
    ),
    # `outcome` is the third key so a split market's per-outcome rows stay in
    # index order; the reader places them by index anyway, but a stable order
    # keeps a stored table readable.
    "resolutions": (
        ("ts_init", "ascending"),
        ("instrument_id", "ascending"),
        ("outcome", "ascending"),
    ),
    "bars": (
        ("ts_init", "ascending"),
        ("instrument_id", "ascending"),
        ("outcome", "ascending"),
    ),
    # Funding carries no outcome, so the instrument is the only tiebreak.
    "funding": (("ts_init", "ascending"), ("instrument_id", "ascending")),
    "references": (
        ("ts_init", "ascending"),
        ("instrument_id", "ascending"),
        ("outcome", "ascending"),
    ),
    # Two actions can take effect on one instrument at one instant (a dividend
    # and a split on the same open), so `kind` is the third key: without it the
    # order between them would depend on Arrow's sort stability, and applying a
    # split before or after a dividend gives different cash.
    "corporate_actions": (
        ("ts_init", "ascending"),
        ("instrument_id", "ascending"),
        ("kind", "ascending"),
    ),
}


@dataclass(frozen=True)
class SourceFile:
    """One input file, as the manifest records it."""

    path: str
    size_bytes: int
    rows_read: int
    rows_kept: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "size_bytes": self.size_bytes,
            "rows_read": self.rows_read,
            "rows_kept": self.rows_kept,
        }


@dataclass(frozen=True)
class TableWrite:
    """What one commit into one table did."""

    table: str
    rows: int
    chunks: int
    replayed_chunks: int
    idempotency_keys: tuple[str, ...]

    @property
    def replayed(self) -> bool:
        """True when every chunk was already stored under its content key."""
        return self.chunks > 0 and self.replayed_chunks == self.chunks

    def to_dict(self) -> dict[str, Any]:
        return {
            "table": self.table,
            "rows": self.rows,
            "chunks": self.chunks,
            "replayed_chunks": self.replayed_chunks,
            "replayed": self.replayed,
        }


@dataclass
class IngestReport:
    """The manifest for one ingest, and the only place coverage is reported.

    `requested` and `loaded` stay separate facts. A caller that asked for a
    window and got less needs to know that from the result rather than by
    querying afterwards and guessing.
    """

    vendor: str
    tables: dict[str, TableWrite] = field(default_factory=dict)
    sources: list[SourceFile] = field(default_factory=list)
    requested_window: Optional[tuple[int, int]] = None
    loaded_window: Optional[tuple[int, int]] = None
    gaps: list[dict[str, Any]] = field(default_factory=list)
    skipped: list[dict[str, Any]] = field(default_factory=list)
    unknown_instruments: list[str] = field(default_factory=list)

    @property
    def rows(self) -> int:
        return sum(write.rows for write in self.tables.values())

    @property
    def coverage(self) -> Optional[float]:
        """Loaded span over requested span, or None when nothing was requested.

        A ratio, not a guarantee: it says how much of the asked-for window the
        data actually spans, and says nothing about holes inside it. Holes are
        reported separately in `gaps`.
        """
        if self.requested_window is None or self.loaded_window is None:
            return None
        wanted = self.requested_window[1] - self.requested_window[0]
        if wanted <= 0:
            return None
        got = self.loaded_window[1] - self.loaded_window[0]
        return max(0.0, min(1.0, got / wanted))

    @property
    def replayed(self) -> bool:
        """True when the whole ingest was already present, byte for byte."""
        return bool(self.tables) and all(
            write.replayed for write in self.tables.values()
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "vendor": self.vendor,
            "rows": self.rows,
            "replayed": self.replayed,
            "tables": {name: write.to_dict() for name, write in self.tables.items()},
            "sources": [source.to_dict() for source in self.sources],
            "requested_window": list(self.requested_window)
            if self.requested_window
            else None,
            "loaded_window": list(self.loaded_window) if self.loaded_window else None,
            "coverage": self.coverage,
            "gaps": list(self.gaps),
            "skipped": list(self.skipped),
            "unknown_instruments": list(self.unknown_instruments),
        }

    def __repr__(self) -> str:  # pragma: no cover - display only
        parts = [f"{name}={write.rows}" for name, write in sorted(self.tables.items())]
        coverage = "n/a" if self.coverage is None else f"{self.coverage:.3f}"
        return (
            f"IngestReport(vendor={self.vendor!r}, {' '.join(parts)}, "
            f"coverage={coverage}, gaps={len(self.gaps)}, replayed={self.replayed})"
        )


def content_key(table: str, chunk: pa.Table) -> str:
    """A content-addressed idempotency key for one chunk.

    Hashing the *normalised* rows rather than the source file means a re-run
    over the same inputs produces the same key and h5i-db replays the commit
    instead of appending twice. It also means two vendors that describe the
    same book agree on the key, which is the behaviour you want when a local
    mirror and an archive host serve the same hour.
    """
    digest = hashlib.sha256()
    digest.update(table.encode("utf-8"))
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, chunk.schema) as writer:
        writer.write_table(chunk)
    digest.update(sink.getvalue().to_pybytes())
    return f"{table}-{digest.hexdigest()[:32]}"


def ensure_tables(db: Any, names: Iterable[str]) -> list[str]:
    """Create any canonical table that does not exist yet. Idempotent."""
    existing = set(db.tables())
    created = []
    for name in names:
        if name in existing:
            continue
        schema = CANONICAL_SCHEMAS.get(name)
        if schema is None:
            raise ValueError(
                f"{name!r} is not a canonical market-data table; "
                f"expected one of {sorted(CANONICAL_SCHEMAS)}"
            )
        db.create_table(name, schema, time_column="ts_init")
        created.append(name)
    return created


def _sorted_for_append(table: str, data: pa.Table) -> pa.Table:
    keys = _SORT_KEYS.get(table)
    if keys is None or data.num_rows == 0:
        return data
    return data.sort_by(list(keys))


def commit(
    db: Any,
    table: str,
    data: pa.Table,
    *,
    note: Optional[str] = None,
    chunk_rows: int = 250_000,
) -> TableWrite:
    """Append normalised rows, content-addressed and in replay order.

    Rows are sorted first because h5i-db rejects an append whose timestamps
    predate what is stored, and because one book event's rows must stay
    contiguous. Each chunk carries an idempotency key derived from its own
    bytes, so re-running an import is a replay rather than a duplicate.
    """
    if chunk_rows < 1:
        raise ValueError("chunk_rows must be a positive integer")
    schema = CANONICAL_SCHEMAS.get(table)
    if schema is None:
        raise ValueError(f"{table!r} is not a canonical market-data table")
    if data.schema != schema:
        data = data.cast(schema)
    ordered = _sorted_for_append(table, data)

    keys: list[str] = []
    replayed = 0
    for offset in range(0, ordered.num_rows, chunk_rows):
        chunk = ordered.slice(offset, chunk_rows)
        if chunk.num_rows == 0:
            continue
        key = content_key(table, chunk)
        keys.append(key)
        before = len(db.versions(table))
        db.append(table, chunk, idempotency_key=key, note=note)
        # A replayed commit adds no version. Counting versions is the only
        # signal that distinguishes a replay from a fresh write of identical
        # rows, because both report the same row totals.
        if len(db.versions(table)) == before:
            replayed += 1
    return TableWrite(
        table=table,
        rows=ordered.num_rows,
        chunks=len(keys),
        replayed_chunks=replayed,
        idempotency_keys=tuple(keys),
    )


def concat(tables: Sequence[pa.Table], schema: pa.Schema) -> pa.Table:
    """Concatenate normalised batches, tolerating an empty input list."""
    usable = [table for table in tables if table.num_rows]
    if not usable:
        return schema.empty_table()
    return pa.concat_tables(usable).cast(schema)
