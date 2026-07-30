"""Vendor Parquet archives into canonical tables.

Both public prediction-market archives ship Parquet: an hourly full-feed file
in one case, a per-day channel file in the other. They differ in column names,
event vocabulary, timestamp unit and how a book snapshot is laid out, and in
nothing else that matters. So the importer takes those differences as data
(:class:`ArchiveLayout`) rather than as a code path per vendor, and a third
vendor is a new layout literal instead of a new module.

Directory conventions are deliberately absent. A caller passes explicit files,
or a root plus a glob; nothing here assumes a `YYYY/MM/DD` tree, because that
is a mirror layout rather than a data contract, and users mirror differently.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Sequence

import pyarrow as pa
import pyarrow.compute as pc

from ._canonical import (
    BOOK_DELTAS_SCHEMA,
    TRADES_SCHEMA,
    IngestReport,
    SourceFile,
    commit,
    concat,
    ensure_tables,
)
from ._markets import MarketSpec, token_index

__all__ = [
    "ArchiveLayout",
    "LevelLayout",
    "PMXT_LAYOUT",
    "TELONEX_LAYOUT",
    "discover",
    "ingest_archive",
]


@dataclass(frozen=True)
class LevelLayout:
    """How one file spells a price level.

    Two shapes exist in the wild. `nested` means bids and asks are list columns
    of structs, one row per book state. `flat` means one row per level, with the
    side in its own column. Both reduce to the same canonical rows.
    """

    style: str = "nested"
    bids_column: str = "bids"
    asks_column: str = "asks"
    price_field: str = "price"
    size_field: str = "size"
    side_column: str = "side"
    price_column: str = "price"
    size_column: str = "size"
    bid_values: tuple[str, ...] = ("buy", "bid", "b")
    ask_values: tuple[str, ...] = ("sell", "ask", "offer", "s", "a")

    def __post_init__(self) -> None:
        if self.style not in ("nested", "flat"):
            raise ValueError("level style must be 'nested' or 'flat'")

    def side_of(self, raw: Any) -> str:
        text = str(raw).strip().lower()
        if text in self.bid_values:
            return "buy"
        if text in self.ask_values:
            return "sell"
        raise ValueError(
            f"side {raw!r} is neither a bid ({self.bid_values}) nor an ask "
            f"({self.ask_values})"
        )


@dataclass(frozen=True)
class ArchiveLayout:
    """One vendor's Parquet dialect, as data.

    `snapshot_events`, `delta_events` and `trade_events` are the event-type
    values that map to each canonical action. A value present in the file but
    in none of these lists is counted as skipped and reported, never guessed
    at: silently dropping an unknown event type is how a book ends up missing
    the update that mattered.
    """

    name: str
    timestamp_column: str = "timestamp"
    timestamp_unit: str = "ms"
    instrument_column: Optional[str] = None
    token_column: str = "asset_id"
    event_type_column: Optional[str] = "event_type"
    snapshot_events: tuple[str, ...] = ()
    delta_events: tuple[str, ...] = ()
    trade_events: tuple[str, ...] = ()
    gap_events: tuple[str, ...] = ()
    levels: LevelLayout = field(default_factory=LevelLayout)
    trade_price_column: str = "price"
    trade_size_column: str = "size"
    trade_side_column: Optional[str] = "side"
    trade_id_column: Optional[str] = None
    arrival_column: Optional[str] = None
    file_glob: str = "*.parquet"

    def __post_init__(self) -> None:
        if self.timestamp_unit not in ("s", "ms", "us", "ns"):
            raise ValueError("timestamp_unit must be one of s, ms, us, ns")
        if self.event_type_column is None and not (
            bool(self.snapshot_events) ^ bool(self.trade_events)
        ):
            raise ValueError(
                f"{self.name}: without an event-type column the layout must "
                "describe exactly one kind of row"
            )

    @property
    def scale_to_nanos(self) -> int:
        return {"s": 1_000_000_000, "ms": 1_000_000, "us": 1_000, "ns": 1}[
            self.timestamp_unit
        ]

    def columns(self) -> list[str]:
        """Columns to read. Reading only these is what keeps a wide feed cheap."""
        wanted = [self.timestamp_column, self.token_column]
        for optional in (
            self.instrument_column,
            self.event_type_column,
            self.arrival_column,
            self.trade_id_column,
        ):
            if optional:
                wanted.append(optional)
        if self.levels.style == "nested":
            wanted += [self.levels.bids_column, self.levels.asks_column]
        else:
            wanted += [
                self.levels.side_column,
                self.levels.price_column,
                self.levels.size_column,
            ]
        wanted += [self.trade_price_column, self.trade_size_column]
        if self.trade_side_column:
            wanted.append(self.trade_side_column)
        ordered: list[str] = []
        for name in wanted:
            if name not in ordered:
                ordered.append(name)
        return ordered


# The hourly full-feed archive: every channel in one file, book states as
# nested bid/ask lists, price changes as flat rows, milliseconds.
PMXT_LAYOUT = ArchiveLayout(
    name="pmxt",
    timestamp_column="timestamp",
    timestamp_unit="ms",
    instrument_column="market",
    token_column="asset_id",
    event_type_column="event_type",
    snapshot_events=("book",),
    delta_events=("price_change",),
    trade_events=("last_trade_price", "trade"),
    levels=LevelLayout(style="nested"),
)

# The per-day channel archive: one channel per file, so the event type is
# implied by which file you opened rather than carried in a column.
TELONEX_LAYOUT = ArchiveLayout(
    name="telonex",
    timestamp_column="timestamp",
    timestamp_unit="ns",
    instrument_column=None,
    token_column="asset_id",
    event_type_column=None,
    snapshot_events=("book_snapshot_full",),
    levels=LevelLayout(style="nested"),
)


def discover(
    root: str | os.PathLike[str],
    *,
    pattern: Optional[str] = None,
    layout: ArchiveLayout = PMXT_LAYOUT,
) -> list[Path]:
    """Files under `root` matching `pattern`, sorted by name.

    A convenience over `Path.rglob`, kept explicit so a caller with a different
    mirror layout can skip it and pass their own file list.
    """
    base = Path(root).expanduser()
    if not base.exists():
        raise FileNotFoundError(f"archive root does not exist: {base}")
    return sorted(base.rglob(pattern or layout.file_glob))


def _timestamps_ns(table: pa.Table, layout: ArchiveLayout) -> pa.Array:
    """The event-time column as int64 nanoseconds, whatever it arrived as."""
    column = table.column(layout.timestamp_column)
    if pa.types.is_timestamp(column.type):
        return pc.cast(pc.cast(column, pa.timestamp("ns")), pa.int64())
    values = pc.cast(column, pa.int64())
    if layout.scale_to_nanos == 1:
        return values
    return pc.multiply(values, pa.scalar(layout.scale_to_nanos, pa.int64()))


def _levels_from_nested(value: Any, layout: LevelLayout) -> list[tuple[float, float]]:
    """One side of a nested snapshot into (price, size) pairs."""
    if value is None:
        return []
    out: list[tuple[float, float]] = []
    for level in value:
        if level is None:
            continue
        if isinstance(level, Mapping):
            price = level.get(layout.price_field)
            size = level.get(layout.size_field)
        elif isinstance(level, (list, tuple)) and len(level) >= 2:
            price, size = level[0], level[1]
        else:
            raise ValueError(f"cannot read book level {level!r}")
        if price is None or size is None:
            continue
        out.append((float(price), float(size)))
    return out


@dataclass
class _Accumulator:
    """Canonical rows under construction, plus the event counter."""

    vendor: str
    book: dict[str, list[Any]] = field(default_factory=dict)
    trades: dict[str, list[Any]] = field(default_factory=dict)
    event_index: int = 0

    def __post_init__(self) -> None:
        self.book = {name: [] for name in BOOK_DELTAS_SCHEMA.names}
        self.trades = {name: [] for name in TRADES_SCHEMA.names}

    def book_event(
        self,
        *,
        ts_event: int,
        ts_init: int,
        instrument_id: str,
        outcome: int,
        action: str,
        levels: Sequence[tuple[str, float, float]],
    ) -> None:
        """Append one atomic book event.

        Rows share an `event_index` and the last carries `is_last`. An event
        with no levels still gets one row, because "the book is empty" is a
        state a replay must be able to apply; dropping it would leave the
        previous book standing.
        """
        self.event_index += 1
        payload = list(levels) or [("", None, None)]
        for position, (side, price, size) in enumerate(payload):
            self.book["ts_init"].append(ts_init)
            self.book["ts_event"].append(ts_event)
            self.book["instrument_id"].append(instrument_id)
            self.book["outcome"].append(outcome)
            self.book["action"].append(action)
            self.book["side"].append(side or None)
            self.book["price"].append(price)
            self.book["size"].append(size)
            self.book["event_index"].append(self.event_index)
            self.book["is_last"].append(position == len(payload) - 1)
            self.book["source_vendor"].append(self.vendor)

    def trade(
        self,
        *,
        ts_event: int,
        ts_init: int,
        instrument_id: str,
        outcome: int,
        price: float,
        size: float,
        aggressor: Optional[str],
        trade_id: Optional[str],
    ) -> None:
        self.trades["ts_init"].append(ts_init)
        self.trades["ts_event"].append(ts_event)
        self.trades["instrument_id"].append(instrument_id)
        self.trades["outcome"].append(outcome)
        self.trades["price"].append(price)
        self.trades["size"].append(size)
        self.trades["aggressor"].append(aggressor)
        self.trades["trade_id"].append(trade_id)
        self.trades["source_vendor"].append(self.vendor)

    def book_table(self) -> pa.Table:
        return pa.table(
            {
                name: pa.array(values, type=BOOK_DELTAS_SCHEMA.field(name).type)
                for name, values in self.book.items()
            },
            schema=BOOK_DELTAS_SCHEMA,
        )

    def trades_table(self) -> pa.Table:
        return pa.table(
            {
                name: pa.array(values, type=TRADES_SCHEMA.field(name).type)
                for name, values in self.trades.items()
            },
            schema=TRADES_SCHEMA,
        )


def _classify(layout: ArchiveLayout, event_type: Optional[str]) -> Optional[str]:
    """Vendor event type to canonical kind, or None when unrecognised."""
    if layout.event_type_column is None:
        if layout.snapshot_events:
            return "snapshot"
        return "trade"
    text = str(event_type)
    if text in layout.snapshot_events:
        return "snapshot"
    if text in layout.delta_events:
        return "delta"
    if text in layout.trade_events:
        return "trade"
    if text in layout.gap_events:
        return "gap"
    return None


def ingest_archive(
    db: Any,
    *,
    files: Iterable[str | os.PathLike[str]],
    markets: Sequence[MarketSpec],
    layout: ArchiveLayout = PMXT_LAYOUT,
    window: Optional[tuple[int, int]] = None,
    chunk_rows: int = 250_000,
    note: Optional[str] = None,
) -> IngestReport:
    """Normalise vendor archive files into `book_deltas` and `trades`.

    Rows are filtered to the tokens of `markets`, so a full-feed archive hour
    costs only the markets asked for. Files are read in whatever order they are
    given and the normalised result is ordered globally before it is committed,
    because an archive's file boundaries are not guaranteed to be time
    boundaries and h5i-db rejects an out-of-order append.

    `window` is a half-open `[start, end)` in epoch nanoseconds. It bounds what
    is read, and the returned report keeps the requested and the loaded window
    as separate facts so a short load is visible rather than inferred.
    """
    import pyarrow.parquet as pq

    if not markets:
        raise ValueError("ingest_archive needs at least one market to filter to")
    if window is not None:
        if len(window) != 2 or window[0] >= window[1]:
            raise ValueError("window must be a half-open (start, end) with start < end")

    tokens = token_index(markets)
    if not tokens:
        raise ValueError(
            "no token map: every market must carry tokens= for archive ingest, "
            "because archive rows are keyed by vendor token id"
        )
    by_instrument = {spec.instrument_id: spec for spec in markets}
    wanted_tokens = pa.array(sorted(tokens), pa.string())

    report = IngestReport(vendor=layout.name, requested_window=window)
    accumulator = _Accumulator(vendor=layout.name)
    unknown_events: dict[str, int] = {}
    unknown_tokens: set[str] = set()
    loaded_low: Optional[int] = None
    loaded_high: Optional[int] = None

    for raw_path in files:
        path = Path(raw_path)
        parquet = pq.ParquetFile(path)
        available = set(parquet.schema_arrow.names)
        needed = [name for name in layout.columns() if name in available]
        missing_required = [
            name
            for name in (layout.timestamp_column, layout.token_column)
            if name not in available
        ]
        if missing_required:
            report.skipped.append(
                {
                    "path": str(path),
                    "reason": "missing_columns",
                    "columns": missing_required,
                }
            )
            continue

        table = parquet.read(columns=needed)
        rows_read = table.num_rows
        stamps = _timestamps_ns(table, layout)
        keep = pc.is_in(table.column(layout.token_column), value_set=wanted_tokens)
        if window is not None:
            keep = pc.and_(
                keep,
                pc.and_(
                    pc.greater_equal(stamps, pa.scalar(window[0], pa.int64())),
                    pc.less(stamps, pa.scalar(window[1], pa.int64())),
                ),
            )
        table = table.append_column("__ts_ns", stamps).filter(keep)
        rows_kept = table.num_rows
        report.sources.append(
            SourceFile(
                path=str(path),
                size_bytes=path.stat().st_size,
                rows_read=rows_read,
                rows_kept=rows_kept,
            )
        )
        if rows_kept == 0:
            continue

        columns = {name: table.column(name).to_pylist() for name in table.column_names}
        event_types = (
            columns.get(layout.event_type_column)
            if layout.event_type_column
            else [None] * rows_kept
        )
        arrivals = columns.get(layout.arrival_column) if layout.arrival_column else None
        trade_ids = (
            columns.get(layout.trade_id_column) if layout.trade_id_column else None
        )
        bids = columns.get(layout.levels.bids_column)
        asks = columns.get(layout.levels.asks_column)
        flat_side = columns.get(layout.levels.side_column)
        flat_price = columns.get(layout.levels.price_column)
        flat_size = columns.get(layout.levels.size_column)
        trade_price = columns.get(layout.trade_price_column)
        trade_size = columns.get(layout.trade_size_column)
        trade_side = (
            columns.get(layout.trade_side_column) if layout.trade_side_column else None
        )
        token_values = columns[layout.token_column]
        stamp_values = columns["__ts_ns"]

        for row in range(rows_kept):
            token = token_values[row]
            resolved = tokens.get(str(token))
            if resolved is None:
                unknown_tokens.add(str(token))
                continue
            spec, outcome = resolved
            ts_event = int(stamp_values[row])
            ts_init = ts_event
            if arrivals is not None and arrivals[row] is not None:
                arrival = arrivals[row]
                ts_init = (
                    int(arrival.value if hasattr(arrival, "value") else arrival)
                    * layout.scale_to_nanos
                )
                # Replay orders by arrival, so an arrival before its own event
                # would reorder the feed. Clamp forward and record it.
                if ts_init < ts_event:
                    report.gaps.append(
                        {
                            "instrument_id": spec.instrument_id,
                            "outcome": outcome,
                            "ts_ns": ts_event,
                            "reason": "arrival_before_event",
                        }
                    )
                    ts_init = ts_event
            loaded_low = ts_event if loaded_low is None else min(loaded_low, ts_event)
            loaded_high = ts_event if loaded_high is None else max(loaded_high, ts_event)

            kind = _classify(layout, event_types[row] if event_types else None)
            if kind is None:
                key = str(event_types[row])
                unknown_events[key] = unknown_events.get(key, 0) + 1
                continue

            if kind == "gap":
                accumulator.book_event(
                    ts_event=ts_event,
                    ts_init=ts_init,
                    instrument_id=spec.instrument_id,
                    outcome=outcome,
                    action="gap",
                    levels=[],
                )
                report.gaps.append(
                    {
                        "instrument_id": spec.instrument_id,
                        "outcome": outcome,
                        "ts_ns": ts_event,
                        "reason": "vendor_gap",
                    }
                )
            elif kind == "snapshot":
                levels: list[tuple[str, float, float]] = []
                for side, source in (("buy", bids), ("sell", asks)):
                    if source is None:
                        continue
                    for price, size in _levels_from_nested(source[row], layout.levels):
                        levels.append((side, price, size))
                accumulator.book_event(
                    ts_event=ts_event,
                    ts_init=ts_init,
                    instrument_id=spec.instrument_id,
                    outcome=outcome,
                    action="snapshot",
                    levels=levels,
                )
            elif kind == "delta":
                if flat_price is None or flat_size is None or flat_side is None:
                    raise ValueError(
                        f"{layout.name}: a delta row needs side, price and size "
                        f"columns; {path} has none"
                    )
                size = float(flat_size[row] or 0.0)
                # Size zero is how these venues spell "this level is gone".
                # Writing it as a set would leave an untradeable level standing.
                action = "delete" if size == 0.0 else "set"
                accumulator.book_event(
                    ts_event=ts_event,
                    ts_init=ts_init,
                    instrument_id=spec.instrument_id,
                    outcome=outcome,
                    action=action,
                    levels=[
                        (
                            layout.levels.side_of(flat_side[row]),
                            float(flat_price[row]),
                            size,
                        )
                    ],
                )
            else:
                if trade_price is None or trade_size is None:
                    raise ValueError(
                        f"{layout.name}: a trade row needs price and size columns"
                    )
                aggressor = None
                if trade_side is not None and trade_side[row] is not None:
                    aggressor = layout.levels.side_of(trade_side[row])
                accumulator.trade(
                    ts_event=ts_event,
                    ts_init=ts_init,
                    instrument_id=spec.instrument_id,
                    outcome=outcome,
                    price=float(trade_price[row]),
                    size=float(trade_size[row]),
                    aggressor=aggressor,
                    trade_id=str(trade_ids[row])
                    if trade_ids is not None and trade_ids[row] is not None
                    else None,
                )

    if unknown_events:
        report.skipped.append({"reason": "unknown_event_types", "counts": unknown_events})
    report.unknown_instruments = sorted(unknown_tokens)
    if loaded_low is not None and loaded_high is not None:
        report.loaded_window = (loaded_low, loaded_high)

    book = concat([accumulator.book_table()], BOOK_DELTAS_SCHEMA)
    trades = concat([accumulator.trades_table()], TRADES_SCHEMA)
    wanted_tables = [
        name
        for name, table in (("book_deltas", book), ("trades", trades))
        if table.num_rows
    ]
    if wanted_tables:
        ensure_tables(db, wanted_tables)
    if book.num_rows:
        report.tables["book_deltas"] = commit(
            db, "book_deltas", book, note=note, chunk_rows=chunk_rows
        )
    if trades.num_rows:
        report.tables["trades"] = commit(
            db, "trades", trades, note=note, chunk_rows=chunk_rows
        )
    if by_instrument and not report.tables:
        report.skipped.append({"reason": "no_rows_matched", "markets": sorted(by_instrument)})
    return report
