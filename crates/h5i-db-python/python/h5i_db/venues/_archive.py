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
import zlib
from dataclasses import dataclass, field
from decimal import Decimal
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
    "KAGGLE_POLYMARKET_LAYOUT",
    "KAGGLE_POLYMARKET_TRADES_LAYOUT",
    "KALSHI_PMXT_LAYOUT",
    "PMXT_LAYOUT",
    "TELONEX_LAYOUT",
    "discover",
    "ingest_archive",
]


@dataclass(frozen=True)
class LevelLayout:
    """How one file spells a price level.

    Four shapes exist in the wild. `nested` means bids and asks are list
    columns of structs, one row per book state. `flat` means one row per level,
    with the side in its own column. `payload` means the whole event is a JSON
    string in one column, which is what a websocket capture written straight to
    Parquet looks like. `outcome_nested` means one list column *per outcome*,
    each holding that outcome's own book, which is how a venue that quotes each
    side of a binary market separately describes it: there is no ask column,
    because an ask on one outcome is a bid on the other. All four reduce to the
    same canonical rows.
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
    #: `outcome_nested` only: the level column for each outcome, in outcome
    #: order, so index i is outcome i.
    outcome_columns: tuple[str, ...] = ()
    #: The canonical side those per-outcome columns hold. A venue that quotes
    #: both outcomes separately is publishing bids on each.
    outcome_side: str = "buy"

    def __post_init__(self) -> None:
        if self.style not in ("nested", "flat", "payload", "outcome_nested"):
            raise ValueError(
                "level style must be 'nested', 'flat', 'payload' or 'outcome_nested'"
            )
        if self.style == "outcome_nested" and len(self.outcome_columns) < 2:
            raise ValueError(
                "an outcome_nested layout needs outcome_columns, one per outcome"
            )
        if self.outcome_side not in ("buy", "sell"):
            raise ValueError("outcome_side must be 'buy' or 'sell'")

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
    # A JSON-payload archive keeps the event inside one string column. The token
    # usually lives in there too, which is why filtering happens in two passes:
    # cheaply on `instrument_column` first, then on the decoded token.
    payload_column: Optional[str] = None
    payload_token_field: str = "token_id"
    payload_bids_field: str = "bids"
    payload_asks_field: str = "asks"
    payload_outcome_field: Optional[str] = None
    outcome_labels: tuple[str, ...] = ()
    # Keep only the best N levels per side. A full-depth capture can carry two
    # hundred levels per snapshot, which a top-of-book study never reads and
    # pays for in rows. Truncation is opt-in and always reported, because a
    # silently shallower book is a different book.
    max_levels: Optional[int] = None
    #: Which clock drives replay. `"arrival"` orders by when the capture saw
    #: the message, which is the causally safe reading *when the arrival stamp
    #: is really a receive stamp*. Some archives instead stamp rows when a
    #: batch was flushed to storage, which is minutes late and unevenly so; for
    #: those, `"event"` orders by the venue's own clock, which is the only
    #: monotone signal the file carries.
    time_basis: str = "arrival"
    #: Whether the delta column carries a signed *change* in resting size
    #: rather than the level's new size. A relative feed cannot be replayed
    #: level by level, so it is accumulated into absolute levels at ingest,
    #: which is also what the live decoder does; see `_reconstruct_events`.
    delta_relative: bool = False
    #: What a snapshot means for a relative feed. `"event"` emits every
    #: snapshot and re-seeds from it. `"seed"` emits only the first per
    #: outcome and treats later ones as divergence probes: use it when the
    #: snapshots are stamped on a different clock from the deltas, where
    #: re-seeding would overwrite good state with a stale book.
    snapshot_role: str = "event"

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
        if self.levels.style == "payload" and not self.payload_column:
            raise ValueError(
                f"{self.name}: a payload layout needs payload_column, the column "
                "holding the JSON event"
            )
        if self.max_levels is not None and self.max_levels < 1:
            raise ValueError(f"{self.name}: max_levels must be at least 1")
        if self.payload_outcome_field and not self.outcome_labels:
            raise ValueError(
                f"{self.name}: payload_outcome_field names a label, so "
                "outcome_labels must give the order those labels map to"
            )
        if self.time_basis not in ("arrival", "event"):
            raise ValueError(f"{self.name}: time_basis must be 'arrival' or 'event'")
        if self.snapshot_role not in ("event", "seed"):
            raise ValueError(f"{self.name}: snapshot_role must be 'event' or 'seed'")
        if self.snapshot_role == "seed" and not self.delta_relative:
            raise ValueError(
                f"{self.name}: snapshot_role='seed' only makes sense for a "
                "relative delta feed, where a snapshot supplies the base the "
                "changes accumulate onto"
            )
        if self.levels.style == "outcome_nested" and not self.outcome_labels:
            raise ValueError(
                f"{self.name}: an outcome_nested layout needs outcome_labels, "
                "because a delta row names its outcome rather than carrying a "
                "per-outcome token"
            )
        if (
            self.levels.style == "outcome_nested"
            and len(self.outcome_labels) != len(self.levels.outcome_columns)
        ):
            raise ValueError(
                f"{self.name}: {len(self.levels.outcome_columns)} outcome columns "
                f"for {len(self.outcome_labels)} outcome labels; index i of each "
                "must describe the same outcome"
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
        if self.levels.style == "payload":
            wanted.append(self.payload_column)  # type: ignore[arg-type]
        elif self.levels.style == "nested":
            wanted += [self.levels.bids_column, self.levels.asks_column]
        elif self.levels.style == "outcome_nested":
            # Both the per-outcome book columns and the flat delta columns: an
            # outcome-major feed carries snapshots and deltas in one file, in
            # different columns of the same row.
            wanted += list(self.levels.outcome_columns)
            wanted += [
                self.levels.side_column,
                self.levels.price_column,
                self.levels.size_column,
            ]
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


# A websocket capture written straight to Parquet: the event is a JSON string,
# the token and both sides live inside it, and the outcome is named rather than
# indexed. `outcome_labels` gives the order those names map to.
KAGGLE_POLYMARKET_LAYOUT = ArchiveLayout(
    name="kaggle-polymarket",
    timestamp_column="timestamp_received",
    timestamp_unit="ms",
    instrument_column="market_id",
    event_type_column="update_type",
    snapshot_events=("book_snapshot",),
    levels=LevelLayout(style="payload"),
    arrival_column=None,
    payload_column="data",
    payload_token_field="token_id",
    payload_bids_field="bids",
    payload_asks_field="asks",
    payload_outcome_field="side",
    outcome_labels=("YES", "NO"),
)

# The same dataset's trade file: flat columns, seconds, outcome named not indexed.
KAGGLE_POLYMARKET_TRADES_LAYOUT = ArchiveLayout(
    name="kaggle-polymarket-trades",
    timestamp_column="timestamp",
    timestamp_unit="s",
    instrument_column="condition_id",
    token_column="asset",
    event_type_column=None,
    trade_events=("trade",),
    levels=LevelLayout(style="flat"),
    trade_price_column="price",
    trade_size_column="size",
    trade_side_column="side",
)


# The hourly Kalshi orderbook archive. Three things make it its own shape
# rather than a variation on the Polymarket feed, and each is measured from the
# files rather than taken from the vendor's description:
#
#   * The book is outcome-major. A row carries `yes_bids` and `no_bids`, two
#     books of bids, because a Kalshi ask on YES is a bid on NO.
#   * `delta` is a signed change in resting size, not a new size. Replaying it
#     with exact decimal arithmetic reproduces a later snapshot exactly for
#     about half of the snapshot pairs in an hour; the rest fail because the
#     hour's deltas are incomplete, not because the arithmetic differs.
#   * `timestamp_received` is a flush stamp, not a receive stamp. It runs 8 to
#     34 minutes behind the venue clock with no stable offset, so ordering by
#     it would deliver events minutes late and out of order relative to each
#     other. `timestamp` (venue, microseconds) is the usable clock, and it is
#     null on snapshot rows, which is why snapshots seed rather than re-seed.
#
# There are no sequence numbers, so a dropped message cannot be detected the
# way the live decoder detects it. Divergence against later snapshots is
# reported instead, which is a weaker check but an honest one.
KALSHI_PMXT_LAYOUT = ArchiveLayout(
    name="kalshi-pmxt",
    timestamp_column="timestamp",
    timestamp_unit="us",
    arrival_column="timestamp_received",
    instrument_column="market_ticker",
    token_column="market_ticker",
    event_type_column="event_type",
    snapshot_events=("orderbook_snapshot",),
    delta_events=("orderbook_delta",),
    outcome_labels=("yes", "no"),
    time_basis="event",
    delta_relative=True,
    snapshot_role="seed",
    levels=LevelLayout(
        style="outcome_nested",
        outcome_columns=("yes_bids", "no_bids"),
        outcome_side="buy",
        # The struct fields are positional in the file and literally named
        # "1" and "2"; price first, size second.
        price_field="1",
        size_field="2",
        side_column="side",
        price_column="price",
        size_column="delta",
    ),
    file_glob="*.parquet",
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


def _column_ns(column: Any, scale_to_nanos: int) -> pa.Array:
    """One time column as int64 nanoseconds, whatever it arrived as.

    A real timestamp carries its own unit, so the layout's unit applies only to
    a bare integer column. Nulls survive: a feed that omits the venue clock on
    some rows is a fact the caller has to see, not one to paper over here.
    """
    if pa.types.is_timestamp(column.type):
        return pc.cast(pc.cast(column, pa.timestamp("ns")), pa.int64())
    values = pc.cast(column, pa.int64())
    if scale_to_nanos == 1:
        return values
    return pc.multiply(values, pa.scalar(scale_to_nanos, pa.int64()))


def _timestamps_ns(table: pa.Table, layout: ArchiveLayout) -> pa.Array:
    """The event-time column as int64 nanoseconds."""
    return _column_ns(table.column(layout.timestamp_column), layout.scale_to_nanos)


def _arrivals_ns(table: pa.Table, layout: ArchiveLayout) -> Optional[pa.Array]:
    """The arrival column as int64 nanoseconds, or None when there is not one."""
    if not layout.arrival_column or layout.arrival_column not in table.column_names:
        return None
    return _column_ns(table.column(layout.arrival_column), layout.scale_to_nanos)


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


def _decode_payload(raw: Any) -> Optional[Mapping[str, Any]]:
    """One JSON event, or None when the cell is empty or unparseable."""
    if raw is None:
        return None
    if isinstance(raw, Mapping):
        return raw
    import json

    try:
        decoded = json.loads(raw)
    except (TypeError, ValueError):
        return None
    return decoded if isinstance(decoded, Mapping) else None


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


@dataclass
class _Pending:
    """One book row of a relative feed, held until the whole set can be ordered.

    A relative feed cannot be normalised row by row the way an absolute one
    can, because a change is only meaningful against the level it changes. So
    the rows are collected, ordered once, and then accumulated.
    """

    ts_init: int
    ts_event: int
    instrument_id: str
    outcome: int
    kind: str
    levels: tuple[tuple[Decimal, Decimal], ...]
    sequence: int

    @property
    def key(self) -> tuple[str, int]:
        return (self.instrument_id, self.outcome)


def _levels_exact(value: Any, layout: LevelLayout) -> list[tuple[Decimal, Decimal]]:
    """One nested side as exact (price, size) pairs.

    Decimal rather than float: these arrive as decimal columns, a relative feed
    adds thousands of them together, and binary floating point would drift a
    level away from the zero that means "gone".
    """
    if value is None:
        return []
    out: list[tuple[Decimal, Decimal]] = []
    for level in value:
        if level is None:
            continue
        if isinstance(level, Mapping):
            price, size = level.get(layout.price_field), level.get(layout.size_field)
        elif isinstance(level, (list, tuple)) and len(level) >= 2:
            price, size = level[0], level[1]
        else:
            raise ValueError(f"cannot read book level {level!r}")
        if price is None or size is None:
            continue
        out.append((Decimal(str(price)), Decimal(str(size))))
    return out


_FINGERPRINT_MASK = (1 << 64) - 1


def _level_hash(price: Decimal, size: Decimal) -> int:
    """A stable hash of one price level.

    Summed across a book it gives an order-independent fingerprint that can be
    updated one level at a time, which is what makes it affordable to ask after
    every single change whether the book now matches a vendor snapshot.
    """
    return zlib.crc32(f"{price}:{size}".encode("utf-8"))


def _reconstruct_events(
    pending: list[_Pending],
    layout: ArchiveLayout,
    accumulator: "_Accumulator",
    report: IngestReport,
) -> None:
    """Accumulate a relative feed into absolute levels, in replay order.

    The order events are emitted in *is* the order a replay applies them, so
    reconstruction walks the same sequence the kernel will: sort once, then
    accumulate. Doing it in any other order would produce absolute levels that
    disagree with the timeline they are stored on.

    With `snapshot_role="seed"` only the first snapshot per outcome seeds the
    book. Later ones are compared against the reconstruction and reported, not
    applied: when snapshots and deltas are stamped on different clocks, a late
    snapshot carries a book that is *older* than the state built from deltas,
    and applying it would undo good updates. The comparison is the only
    integrity check available on a feed with no sequence numbers, so its result
    is reported rather than kept private.
    """
    if not pending:
        return

    seeding = layout.snapshot_role == "seed"
    first_delta: dict[tuple[str, int], int] = {}
    if seeding:
        for item in pending:
            if item.kind != "delta":
                continue
            current = first_delta.get(item.key)
            if current is None or item.ts_init < current:
                first_delta[item.key] = item.ts_init

    emit: list[_Pending] = []
    # Probe books are held as fingerprints rather than as events: the question
    # they answer is whether the reconstruction ever *passes through* the state
    # the vendor published, and that has to be asked without reference to when
    # either happened. Comparing at a shared instant would only measure the
    # clock skew between the two streams, which is already known to be large.
    probe_counts: dict[tuple[str, int], dict[int, int]] = {}
    probe_total = 0
    seeded_keys: set[tuple[str, int]] = set()
    for item in sorted(pending, key=lambda row: (row.ts_init, row.ts_event, row.sequence)):
        if item.kind != "snapshot" or not seeding:
            emit.append(item)
            continue
        if item.key in seeded_keys:
            fingerprint = 0
            for price, size in item.levels:
                if size != 0:
                    fingerprint = (fingerprint + _level_hash(price, size)) & _FINGERPRINT_MASK
            bucket = probe_counts.setdefault(item.key, {})
            bucket[fingerprint] = bucket.get(fingerprint, 0) + 1
            probe_total += 1
            continue
        seeded_keys.add(item.key)
        start = first_delta.get(item.key)
        if start is not None and item.ts_init >= start:
            # The seed has to land before the changes it is the base for. Its
            # own stamp is on the wrong clock (that is why it is a seed and not
            # an event), so it is placed one nanosecond ahead of the first
            # delta it seeds. The book state is the vendor's; only the instant
            # attributed to it is ours, and it is reported below.
            item.ts_init = start - 1
            item.ts_event = start - 1
        emit.append(item)

    state: dict[tuple[str, int], dict[Decimal, Decimal]] = {}
    marks: dict[tuple[str, int], int] = {}
    seen: dict[tuple[str, int], set[int]] = {}
    unseeded = 0
    clamped = 0

    def observe(key: tuple[str, int]) -> None:
        """Note that the book for `key` currently holds this exact state."""
        wanted = probe_counts.get(key)
        if wanted and marks.get(key, 0) in wanted:
            seen.setdefault(key, set()).add(marks[key])

    for item in sorted(emit, key=lambda row: (row.ts_init, row.ts_event, row.sequence)):
        if item.kind == "snapshot":
            book = {price: size for price, size in item.levels if size != 0}
            state[item.key] = book
            marks[item.key] = 0
            for price, size in book.items():
                marks[item.key] = (
                    marks[item.key] + _level_hash(price, size)
                ) & _FINGERPRINT_MASK
            observe(item.key)
            accumulator.book_event(
                ts_event=item.ts_event,
                ts_init=item.ts_init,
                instrument_id=item.instrument_id,
                outcome=item.outcome,
                action="snapshot",
                levels=[
                    (layout.levels.outcome_side, float(price), float(size))
                    for price, size in item.levels
                ],
            )
            continue

        book = state.get(item.key)
        if book is None:
            # A change with no base is not a book event, it is half of one.
            unseeded += 1
            continue
        mark = marks.get(item.key, 0)
        for price, change in item.levels:
            previous = book.get(price, Decimal(0))
            updated = previous + change
            if updated < 0:
                # Only reachable when a message was lost, which this feed gives
                # no other way to detect. Zero is the floor a real book has.
                clamped += 1
                updated = Decimal(0)
            if previous != 0:
                mark = (mark - _level_hash(price, previous)) & _FINGERPRINT_MASK
            if updated == 0:
                book.pop(price, None)
                action, size = "delete", Decimal(0)
            else:
                book[price] = updated
                mark = (mark + _level_hash(price, updated)) & _FINGERPRINT_MASK
                action, size = "set", updated
            marks[item.key] = mark
            observe(item.key)
            accumulator.book_event(
                ts_event=item.ts_event,
                ts_init=item.ts_init,
                instrument_id=item.instrument_id,
                outcome=item.outcome,
                action=action,
                levels=[(layout.levels.outcome_side, float(price), float(size))],
            )

    if probe_total:
        reproduced = 0
        for key, counts in probe_counts.items():
            hit = seen.get(key, ())
            reproduced += sum(count for mark, count in counts.items() if mark in hit)
        # How many vendor snapshots the reconstruction reproduced exactly at
        # some point in the walk. It measures this window, not the vendor: a
        # low number almost always means the window's deltas are incomplete,
        # and the fix is to load the neighbouring files as well. It is the only
        # integrity check a feed without sequence numbers admits, so it is
        # reported even when it is perfect.
        report.gaps.append(
            {
                "reason": "snapshot_divergence",
                "probes": probe_total,
                "reproduced": reproduced,
                "unreproduced": probe_total - reproduced,
            }
        )
    if unseeded:
        report.skipped.append({"reason": "delta_before_snapshot", "rows": unseeded})
    if clamped:
        report.skipped.append({"reason": "negative_size_clamped", "levels": clamped})
    if seeding and first_delta:
        restamped = sum(
            1 for key in seeded_keys if key in first_delta
        )
        report.skipped.append(
            {
                "reason": "seed_restamped_to_first_delta",
                "outcomes": restamped,
                "detail": "snapshot stamps are on the arrival clock; seeds were "
                "placed immediately before the deltas they seed",
            }
        )


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

    # An outcome-major feed names the instrument and picks the outcome with a
    # label, so it needs no per-outcome token and must not demand one.
    by_outcome_column = layout.levels.style == "outcome_nested"
    tokens = token_index(markets)
    if not tokens and not by_outcome_column:
        raise ValueError(
            "no token map: every market must carry tokens= for archive ingest, "
            "because archive rows are keyed by vendor token id"
        )
    by_instrument = {spec.instrument_id: spec for spec in markets}
    wanted_tokens = pa.array(sorted(tokens), pa.string())
    wanted_instruments = pa.array(sorted(by_instrument), pa.string())
    outcome_index_of = {
        str(label).strip().lower(): index
        for index, label in enumerate(layout.outcome_labels)
    }
    pending: list[_Pending] = []
    sequence = 0

    report = IngestReport(vendor=layout.name, requested_window=window)
    accumulator = _Accumulator(vendor=layout.name)
    unknown_events: dict[str, int] = {}
    unknown_tokens: set[str] = set()
    unparseable = 0
    dropped_levels = 0
    loaded_low: Optional[int] = None
    loaded_high: Optional[int] = None

    for raw_path in files:
        path = Path(raw_path)
        parquet = pq.ParquetFile(path)
        available = set(parquet.schema_arrow.names)
        needed = [name for name in layout.columns() if name in available]
        required = [layout.timestamp_column]
        if layout.levels.style == "payload":
            required.append(layout.payload_column)  # type: ignore[arg-type]
        else:
            required.append(layout.token_column)
        missing_required = [name for name in required if name not in available]
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
        arrival_stamps = _arrivals_ns(table, layout)
        if arrival_stamps is not None:
            # A capture that fetches snapshots itself has no venue clock to
            # quote for them. Falling back to the arrival stamp keeps the row
            # instead of dropping it for want of a timestamp, and keeps a
            # window filter from silently excluding every snapshot.
            stamps = pc.coalesce(stamps, arrival_stamps)
        if by_outcome_column:
            keep = pc.is_in(
                table.column(layout.instrument_column), value_set=wanted_instruments
            )
        elif layout.levels.style == "payload":
            # The token is inside the JSON, so pre-filter on whatever cheap
            # identifier the file does carry and decode only the survivors.
            if layout.instrument_column and layout.instrument_column in available:
                keep = pc.is_in(
                    table.column(layout.instrument_column),
                    value_set=pa.array(sorted(by_instrument), pa.string()),
                )
            else:
                keep = pc.scalar(True)
        else:
            keep = pc.is_in(table.column(layout.token_column), value_set=wanted_tokens)
        if window is not None:
            keep = pc.and_(
                keep,
                pc.and_(
                    pc.greater_equal(stamps, pa.scalar(window[0], pa.int64())),
                    pc.less(stamps, pa.scalar(window[1], pa.int64())),
                ),
            )
        table = table.append_column("__ts_ns", stamps)
        if arrival_stamps is not None:
            table = table.append_column("__arrival_ns", arrival_stamps)
        table = table.filter(keep) if not isinstance(keep, bool) else table
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
        arrivals = columns.get("__arrival_ns")
        outcome_levels = {
            name: columns.get(name) for name in layout.levels.outcome_columns
        }
        trade_ids = (
            columns.get(layout.trade_id_column) if layout.trade_id_column else None
        )
        payloads = (
            columns.get(layout.payload_column) if layout.payload_column else None
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
        token_values = columns.get(layout.token_column)
        instrument_values = (
            columns.get(layout.instrument_column) if layout.instrument_column else None
        )
        stamp_values = columns["__ts_ns"]

        def stamps_for(row: int, who: str, outcome: Optional[int]) -> tuple[int, int]:
            """`(ts_event, ts_init)` for one row, on the layout's clock."""
            ts_event = int(stamp_values[row])
            if (
                layout.time_basis == "event"
                or arrivals is None
                or arrivals[row] is None
            ):
                return ts_event, ts_event
            ts_init = int(arrivals[row])
            if ts_init < ts_event:
                # Replay orders by arrival, so an arrival before its own event
                # would reorder the feed. Clamp forward and record it.
                report.gaps.append(
                    {
                        "instrument_id": who,
                        "outcome": outcome,
                        "ts_ns": ts_event,
                        "reason": "arrival_before_event",
                    }
                )
                ts_init = ts_event
            return ts_event, ts_init

        if by_outcome_column:
            for row in range(rows_kept):
                name = (
                    str(instrument_values[row])
                    if instrument_values is not None
                    else None
                )
                spec = by_instrument.get(name or "")
                if spec is None:
                    unknown_tokens.add(str(name))
                    continue
                kind = _classify(layout, event_types[row] if event_types else None)
                if kind is None:
                    key = str(event_types[row]) if event_types else "<none>"
                    unknown_events[key] = unknown_events.get(key, 0) + 1
                    continue

                if kind == "snapshot":
                    # One row carries every outcome's book, so it becomes one
                    # event per outcome rather than one event.
                    for index, column_name in enumerate(layout.levels.outcome_columns):
                        if index >= spec.outcome_count:
                            continue
                        source = outcome_levels.get(column_name)
                        levels = _levels_exact(
                            source[row] if source is not None else None, layout.levels
                        )
                        if layout.max_levels is not None and len(levels) > layout.max_levels:
                            levels.sort(
                                key=lambda level: level[0],
                                reverse=layout.levels.outcome_side == "buy",
                            )
                            dropped_levels += len(levels) - layout.max_levels
                            levels = levels[: layout.max_levels]
                        ts_event, ts_init = stamps_for(row, spec.instrument_id, index)
                        loaded_low = ts_event if loaded_low is None else min(loaded_low, ts_event)
                        loaded_high = ts_event if loaded_high is None else max(loaded_high, ts_event)
                        sequence += 1
                        pending.append(
                            _Pending(
                                ts_init=ts_init,
                                ts_event=ts_event,
                                instrument_id=spec.instrument_id,
                                outcome=index,
                                kind="snapshot",
                                levels=tuple(levels),
                                sequence=sequence,
                            )
                        )
                elif kind == "delta":
                    if flat_side is None or flat_price is None or flat_size is None:
                        raise ValueError(
                            f"{layout.name}: a delta row needs "
                            f"{layout.levels.side_column}, "
                            f"{layout.levels.price_column} and "
                            f"{layout.levels.size_column} columns; {path} has none"
                        )
                    label = str(flat_side[row]).strip().lower()
                    index = outcome_index_of.get(label)
                    if index is None or index >= spec.outcome_count:
                        # The side names an outcome this market does not have.
                        # Guessing which one it meant would attribute one
                        # side's depth to the other.
                        unknown_events[f"outcome:{label}"] = (
                            unknown_events.get(f"outcome:{label}", 0) + 1
                        )
                        continue
                    if flat_price[row] is None or flat_size[row] is None:
                        unparseable += 1
                        continue
                    ts_event, ts_init = stamps_for(row, spec.instrument_id, index)
                    loaded_low = ts_event if loaded_low is None else min(loaded_low, ts_event)
                    loaded_high = ts_event if loaded_high is None else max(loaded_high, ts_event)
                    sequence += 1
                    pending.append(
                        _Pending(
                            ts_init=ts_init,
                            ts_event=ts_event,
                            instrument_id=spec.instrument_id,
                            outcome=index,
                            kind="delta",
                            levels=(
                                (
                                    Decimal(str(flat_price[row])),
                                    Decimal(str(flat_size[row])),
                                ),
                            ),
                            sequence=sequence,
                        )
                    )
                else:
                    unknown_events[str(kind)] = unknown_events.get(str(kind), 0) + 1
            continue

        for row in range(rows_kept):
            event: Optional[Mapping[str, Any]] = None
            if payloads is not None:
                event = _decode_payload(payloads[row])
                if event is None:
                    unparseable += 1
                    continue
                token = event.get(layout.payload_token_field)
            else:
                token = token_values[row] if token_values is not None else None
            resolved = tokens.get(str(token)) if token is not None else None
            if resolved is None and event is not None and layout.payload_outcome_field:
                # A capture may name the outcome rather than carry a known token.
                # Resolving through the instrument plus the label is exact when
                # the layout states the label order; guessing it would not be.
                label = event.get(layout.payload_outcome_field)
                instrument_id = (
                    str(instrument_values[row]) if instrument_values is not None else None
                )
                spec_by_id = by_instrument.get(instrument_id or "")
                if spec_by_id is not None and label is not None:
                    try:
                        index = layout.outcome_labels.index(str(label))
                    except ValueError:
                        index = None  # type: ignore[assignment]
                    if index is not None and index < spec_by_id.outcome_count:
                        resolved = (spec_by_id, index)
            if resolved is None:
                unknown_tokens.add(str(token))
                continue
            spec, outcome = resolved
            ts_event, ts_init = stamps_for(row, spec.instrument_id, outcome)
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
                if event is not None:
                    sides = (
                        ("buy", event.get(layout.payload_bids_field)),
                        ("sell", event.get(layout.payload_asks_field)),
                    )
                else:
                    sides = (
                        ("buy", bids[row] if bids is not None else None),
                        ("sell", asks[row] if asks is not None else None),
                    )
                for side, source in sides:
                    if source is None:
                        continue
                    parsed = _levels_from_nested(source, layout.levels)
                    if layout.max_levels is not None and len(parsed) > layout.max_levels:
                        # Best first: highest bids, lowest asks.
                        parsed.sort(key=lambda level: level[0], reverse=side == "buy")
                        dropped_levels += len(parsed) - layout.max_levels
                        parsed = parsed[: layout.max_levels]
                    for price, size in parsed:
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

    # A relative feed is normalised only once every file has been read: a
    # change is meaningless against a book assembled from a subset of them.
    _reconstruct_events(pending, layout, accumulator, report)

    if unknown_events:
        report.skipped.append({"reason": "unknown_event_types", "counts": unknown_events})
    if unparseable:
        report.skipped.append({"reason": "unparseable_payload", "rows": unparseable})
    if dropped_levels:
        report.skipped.append(
            {
                "reason": "depth_truncated",
                "max_levels": layout.max_levels,
                "levels_dropped": dropped_levels,
            }
        )
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
