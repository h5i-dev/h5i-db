"""OHLCV on-ramp: any bar source into the canonical `bars` table.

Most non-prediction-market data arrives as bars, and every vendor spells them
the same six ways with different column names. So this layer takes the naming
as data (:class:`BarLayout`), the same way :mod:`._archive` takes book dialects
as data, and a new vendor is a layout literal rather than a module.

One rule is enforced rather than configured. A bar becomes knowable when its
interval *closes*, not when it opens, so `ts_init` (the replay clock) is the
close and `ts_event` (when the priced activity happened) is the open. A loader
that stamps both at the open lets a strategy trade on a bar that has not
finished forming, which backtests beautifully and loses money live. That is why
a layout must supply either a close-time column or an explicit interval: there
is no safe default to guess, so this refuses to guess.

Nothing here fetches. A caller downloads with whatever tool they already use
(a broker export, `yfinance`, a browser) and hands over the rows, which keeps
credentials and retries in scripts and parsing here, where it is testable
offline.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence, Union

import pyarrow as pa
import pyarrow.compute as pc

from ._canonical import (
    BARS_SCHEMA,
    IngestReport,
    SourceFile,
    commit,
    concat,
    ensure_tables,
)

__all__ = [
    "BarLayout",
    "BINANCE_KLINES_LAYOUT",
    "GENERIC_OHLCV_LAYOUT",
    "bars_from_dataframe",
    "bars_from_table",
    "bars_from_trades",
    "ingest_bars",
    "parse_interval",
    "read_bars_csv",
]

_INTERVAL_UNITS = {
    "s": 1_000_000_000,
    "m": 60 * 1_000_000_000,
    "h": 3_600 * 1_000_000_000,
    "d": 86_400 * 1_000_000_000,
    "w": 7 * 86_400 * 1_000_000_000,
}

_TIME_UNIT_SCALE = {"s": 1_000_000_000, "ms": 1_000_000, "us": 1_000, "ns": 1}

# Epoch values only tell you their unit by magnitude. These bounds bracket
# 2001..2286 in seconds, which is the range any market data actually occupies,
# and each successive unit shifts it by three orders of magnitude.
_UNIT_BOUNDS = (
    ("s", 1_000_000_000, 10_000_000_000),
    ("ms", 1_000_000_000_000, 10_000_000_000_000),
    ("us", 1_000_000_000_000_000, 10_000_000_000_000_000),
    ("ns", 1_000_000_000_000_000_000, 10_000_000_000_000_000_000),
)


def parse_interval(interval: Union[str, int]) -> int:
    """A bar interval as nanoseconds. Accepts `"1m"`, `"4h"`, `"1d"` or an int.

    An integer is taken as nanoseconds already, so a caller with an exotic
    interval is never blocked by this spelling.
    """
    if isinstance(interval, int):
        if interval <= 0:
            raise ValueError("interval must be positive")
        return interval
    text = str(interval).strip().lower()
    match = re.fullmatch(r"(\d+)\s*([smhdw])", text)
    if not match:
        raise ValueError(
            f"cannot read interval {interval!r}; use a count and one of "
            f"{sorted(_INTERVAL_UNITS)} such as '1m', '4h', '1d', or an int of nanoseconds"
        )
    count = int(match.group(1))
    if count <= 0:
        raise ValueError("interval must be positive")
    return count * _INTERVAL_UNITS[match.group(2)]


@dataclass(frozen=True)
class BarLayout:
    """One vendor's bar dialect, as data.

    Columns may be named or, for a headerless file, positional: give
    `column_names` and refer to them by name thereafter, which keeps the rest
    of the layout readable instead of a list of indices.
    """

    name: str
    time_column: str = "time"
    open_column: str = "open"
    high_column: str = "high"
    low_column: str = "low"
    close_column: str = "close"
    volume_column: Optional[str] = "volume"
    #: The instant the bar closed. Preferred over `interval` when the vendor
    #: publishes it, because a vendor that ships a close time has already
    #: accounted for its own holidays and half days.
    close_time_column: Optional[str] = None
    #: Whether that close time is the last instant *inside* the bar rather than
    #: the first instant after it. Both spellings are common and they differ by
    #: one tick of the source resolution, which is exactly the window in which
    #: the bar is complete but not yet stamped as such. Getting it wrong is a
    #: one-tick look-ahead, so it is stated per vendor rather than sniffed.
    close_time_inclusive: bool = False
    #: Bar length, used to derive the close when the vendor omits it.
    interval: Optional[Union[str, int]] = None
    #: `"auto"` infers seconds/millis/micros/nanos from magnitude, which is
    #: unambiguous for any timestamp in the plausible range. Only relevant to
    #: numeric time columns; a real timestamp or date column carries its own.
    time_unit: str = "auto"
    #: Where the instrument id comes from, when it is in the data rather than
    #: supplied by the caller.
    instrument_column: Optional[str] = None
    #: Headerless files: the names to assign, in file order.
    column_names: tuple[str, ...] = ()
    csv_delimiter: str = ","
    file_glob: str = "*.csv"

    def __post_init__(self) -> None:
        if self.time_unit not in ("auto", "s", "ms", "us", "ns"):
            raise ValueError("time_unit must be auto, s, ms, us or ns")
        if self.close_time_column is None and self.interval is None:
            raise ValueError(
                f"{self.name}: give close_time_column or interval. A bar is only "
                "knowable once its interval closes, and without one of these there "
                "is no way to know when that was; stamping the open would let a "
                "backtest trade a bar that has not formed yet"
            )
        if self.interval is not None:
            parse_interval(self.interval)

    @property
    def interval_ns(self) -> Optional[int]:
        return None if self.interval is None else parse_interval(self.interval)

    def columns(self) -> list[str]:
        """Columns to read, deduplicated and in a stable order."""
        wanted = [
            self.time_column,
            self.open_column,
            self.high_column,
            self.low_column,
            self.close_column,
        ]
        for optional in (
            self.volume_column,
            self.close_time_column,
            self.instrument_column,
        ):
            if optional:
                wanted.append(optional)
        ordered: list[str] = []
        for name in wanted:
            if name not in ordered:
                ordered.append(name)
        return ordered


# The shape a broker export, a Stooq download and `yfinance.download()` all
# land in once the index is a column: named OHLCV with a date or datetime.
# Daily is the common case, so it is the stated default rather than a guess.
GENERIC_OHLCV_LAYOUT = BarLayout(
    name="ohlcv",
    time_column="date",
    open_column="open",
    high_column="high",
    low_column="low",
    close_column="close",
    volume_column="volume",
    interval="1d",
)

# Binance publishes free daily kline dumps at data.binance.vision: headerless
# CSV, twelve columns, and a close time in the file so no interval is assumed.
# The open and close times are microseconds, not the milliseconds older
# documentation describes, which `time_unit="auto"` would infer anyway; it is
# stated here so the layout does not silently change meaning if a future dump
# switches unit.
BINANCE_KLINES_LAYOUT = BarLayout(
    name="binance-klines",
    column_names=(
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "quote_volume",
        "trades",
        "taker_buy_base",
        "taker_buy_quote",
        "ignore",
    ),
    time_column="open_time",
    close_time_column="close_time",
    close_time_inclusive=True,
    volume_column="volume",
    time_unit="us",
)


def _infer_unit(values: pa.Array) -> str:
    """The epoch unit of a numeric time column, from its magnitude."""
    finite = pc.drop_null(values)
    if len(finite) == 0:
        raise ValueError("cannot infer a time unit from an empty column")
    probe = abs(pc.min(finite).as_py() or 0)
    for unit, low, high in _UNIT_BOUNDS:
        if low <= probe < high:
            return unit
    raise ValueError(
        f"epoch value {probe} is not in the plausible range for any of "
        "seconds, milliseconds, microseconds or nanoseconds; state time_unit "
        "explicitly if the column really is a timestamp"
    )


def _tick_ns(column: pa.ChunkedArray | pa.Array, unit: str) -> int:
    """One tick of a time column's own resolution, in nanoseconds."""
    if isinstance(column, pa.ChunkedArray):
        column = column.combine_chunks()
    if pa.types.is_timestamp(column.type):
        return _TIME_UNIT_SCALE[column.type.unit]
    if pa.types.is_date(column.type):
        return _TIME_UNIT_SCALE["s"]
    resolved = unit
    if resolved == "auto":
        resolved = _infer_unit(pc.cast(column, pa.int64()))
    return _TIME_UNIT_SCALE[resolved]


def _to_nanos(column: pa.ChunkedArray | pa.Array, unit: str) -> pa.Array:
    """Any time column as int64 epoch nanoseconds.

    Timestamps and dates carry their own unit. A numeric column does not, so
    `unit` (or inference from magnitude) supplies it.
    """
    if isinstance(column, pa.ChunkedArray):
        column = column.combine_chunks()
    if pa.types.is_timestamp(column.type) or pa.types.is_date(column.type):
        return pc.cast(pc.cast(column, pa.timestamp("ns")), pa.int64())
    if pa.types.is_string(column.type) or pa.types.is_large_string(column.type):
        # A string date is common in CSV exports and unambiguous once parsed.
        parsed = pc.strptime(column, format="%Y-%m-%d", unit="s", error_is_null=True)
        if pc.sum(pc.cast(pc.is_null(parsed), pa.int64())).as_py():
            parsed = pc.cast(column, pa.timestamp("ns"))
        return pc.cast(pc.cast(parsed, pa.timestamp("ns")), pa.int64())
    values = pc.cast(column, pa.int64())
    resolved = _infer_unit(values) if unit == "auto" else unit
    scale = _TIME_UNIT_SCALE[resolved]
    return values if scale == 1 else pc.multiply(values, pa.scalar(scale, pa.int64()))


def bars_from_table(
    table: pa.Table,
    *,
    instrument_id: Optional[str] = None,
    layout: BarLayout = GENERIC_OHLCV_LAYOUT,
    outcome: int = 0,
    source_vendor: Optional[str] = None,
) -> pa.Table:
    """Normalise an OHLCV table into canonical `bars` rows.

    Column lookup is case-insensitive, because "Date"/"date"/"DATE" is a
    formatting difference between exports of the same data and not worth a
    layout each.

    `ts_init` is the bar close and `ts_event` the bar open. See the module
    docstring for why that asymmetry is not configurable.
    """
    if instrument_id is None and layout.instrument_column is None:
        raise ValueError(
            "give instrument_id, or a layout with instrument_column when the "
            "instrument varies row by row"
        )
    if not 0 <= int(outcome) <= 65535:
        raise ValueError("outcome must fit in uint16")

    lookup = {name.lower(): name for name in table.column_names}

    def column(name: str, required: bool = True) -> Optional[pa.Array]:
        actual = lookup.get(name.lower())
        if actual is None:
            if required:
                raise KeyError(
                    f"{layout.name}: column {name!r} is not in the data; "
                    f"available columns are {sorted(table.column_names)}"
                )
            return None
        found = table.column(actual)
        return found.combine_chunks() if isinstance(found, pa.ChunkedArray) else found

    opens = _to_nanos(column(layout.time_column), layout.time_unit)
    if layout.close_time_column:
        raw_close = column(layout.close_time_column)
        closes = _to_nanos(raw_close, layout.time_unit)
        if layout.close_time_inclusive:
            # Advance to the exclusive boundary: the bar is readable one tick
            # of the vendor's own resolution after its last contained instant.
            closes = pc.add(
                closes, pa.scalar(_tick_ns(raw_close, layout.time_unit), pa.int64())
            )
    else:
        span = layout.interval_ns
        assert span is not None  # guaranteed by BarLayout.__post_init__
        closes = pc.add(opens, pa.scalar(span, pa.int64()))

    rows = table.num_rows
    if layout.instrument_column:
        instruments = pc.cast(column(layout.instrument_column), pa.string())
    else:
        instruments = pa.array([instrument_id] * rows, pa.string())

    volume_column = (
        column(layout.volume_column, required=False) if layout.volume_column else None
    )
    volume = (
        pc.cast(volume_column, pa.float64())
        if volume_column is not None
        else pa.array([0.0] * rows, pa.float64())
    )
    # Volume is non-null in the schema: a bar with unknown volume is a real
    # thing (an index has no shares traded), and zero is the honest reading,
    # whereas a null would make every downstream sum ambiguous.
    volume = pc.fill_null(volume, 0.0)

    normalised = pa.table(
        {
            "ts_init": pc.cast(closes, pa.timestamp("ns")),
            "ts_event": pc.cast(opens, pa.timestamp("ns")),
            "instrument_id": instruments,
            "outcome": pa.array([int(outcome)] * rows, pa.uint16()),
            "open": pc.cast(column(layout.open_column), pa.float64()),
            "high": pc.cast(column(layout.high_column), pa.float64()),
            "low": pc.cast(column(layout.low_column), pa.float64()),
            "close": pc.cast(column(layout.close_column), pa.float64()),
            "volume": volume,
            "source_vendor": pa.array(
                [source_vendor or layout.name] * rows, pa.string()
            ),
        },
        schema=BARS_SCHEMA,
    )
    return normalised


def bars_from_dataframe(
    frame: Any,
    *,
    instrument_id: Optional[str] = None,
    layout: BarLayout = GENERIC_OHLCV_LAYOUT,
    outcome: int = 0,
    source_vendor: Optional[str] = None,
) -> pa.Table:
    """Canonical bars from a pandas (or any Arrow-convertible) frame.

    A `DatetimeIndex` is promoted to a column first, because that is the shape
    `yfinance.download()` and most broker exports return and requiring the
    caller to `reset_index()` would be a papercut on the most common path. A
    `yfinance` frame with grouped columns should be flattened by the caller,
    since which level names the field is theirs to decide, not ours to guess.
    """
    if isinstance(frame, pa.Table):
        table = frame
    else:
        reset = frame
        index = getattr(frame, "index", None)
        if index is not None and getattr(index, "name", None) is not None:
            reset = frame.reset_index()
        elif index is not None and str(type(index)).find("DatetimeIndex") >= 0:
            reset = frame.rename_axis("date").reset_index()
        table = pa.Table.from_pandas(reset, preserve_index=False)
    return bars_from_table(
        table,
        instrument_id=instrument_id,
        layout=layout,
        outcome=outcome,
        source_vendor=source_vendor,
    )


def read_bars_csv(
    path: str | os.PathLike[str], *, layout: BarLayout = GENERIC_OHLCV_LAYOUT
) -> pa.Table:
    """Read one CSV as an Arrow table, honouring a headerless layout."""
    from pyarrow import csv as pa_csv

    read_options = pa_csv.ReadOptions()
    if layout.column_names:
        read_options = pa_csv.ReadOptions(
            column_names=list(layout.column_names), autogenerate_column_names=False
        )
    parse_options = pa_csv.ParseOptions(delimiter=layout.csv_delimiter)
    return pa_csv.read_csv(
        Path(path), read_options=read_options, parse_options=parse_options
    )


def ingest_bars(
    db: Any,
    *,
    files: Optional[Iterable[str | os.PathLike[str]]] = None,
    frames: Optional[Iterable[tuple[str, Any]]] = None,
    instrument_id: Optional[str] = None,
    layout: BarLayout = GENERIC_OHLCV_LAYOUT,
    outcome: int = 0,
    source_vendor: Optional[str] = None,
    window: Optional[tuple[int, int]] = None,
    chunk_rows: int = 250_000,
    note: Optional[str] = None,
) -> IngestReport:
    """Normalise bar files or in-memory frames into the `bars` table.

    `files` are CSVs read with `layout`; `frames` are `(instrument_id, frame)`
    pairs already in memory, which is the path for anything fetched in Python.
    Either may be given, and both together is fine when backfilling a download
    with a live pull.

    `window` is a half-open `[start, end)` in epoch nanoseconds applied to
    `ts_init`, so a bar is included when it *closed* inside the window.
    """
    if not files and not frames:
        raise ValueError("ingest_bars needs files= or frames=")
    if window is not None and (len(window) != 2 or window[0] >= window[1]):
        raise ValueError("window must be a half-open (start, end) with start < end")

    report = IngestReport(vendor=layout.name, requested_window=window)
    batches: list[pa.Table] = []

    def absorb(table: pa.Table, who: Optional[str]) -> pa.Table:
        normalised = bars_from_table(
            table,
            instrument_id=who or instrument_id,
            layout=layout,
            outcome=outcome,
            source_vendor=source_vendor,
        )
        if window is not None:
            stamps = pc.cast(normalised.column("ts_init"), pa.int64())
            normalised = normalised.filter(
                pc.and_(
                    pc.greater_equal(stamps, pa.scalar(window[0], pa.int64())),
                    pc.less(stamps, pa.scalar(window[1], pa.int64())),
                )
            )
        return normalised

    for raw_path in files or ():
        path = Path(raw_path)
        table = read_bars_csv(path, layout=layout)
        rows_read = table.num_rows
        normalised = absorb(table, None)
        report.sources.append(
            SourceFile(
                path=str(path),
                size_bytes=path.stat().st_size,
                rows_read=rows_read,
                rows_kept=normalised.num_rows,
            )
        )
        if normalised.num_rows:
            batches.append(normalised)

    for who, frame in frames or ():
        normalised = absorb(
            frame if isinstance(frame, pa.Table) else pa.Table.from_pandas(
                frame.reset_index() if getattr(frame, "index", None) is not None
                and getattr(frame.index, "name", None) is not None else frame,
                preserve_index=False,
            ),
            who,
        )
        report.sources.append(
            SourceFile(
                path=f"<frame:{who}>",
                size_bytes=0,
                rows_read=normalised.num_rows,
                rows_kept=normalised.num_rows,
            )
        )
        if normalised.num_rows:
            batches.append(normalised)

    bars = concat(batches, BARS_SCHEMA)
    if bars.num_rows:
        stamps = pc.cast(bars.column("ts_init"), pa.int64())
        report.loaded_window = (pc.min(stamps).as_py(), pc.max(stamps).as_py())
        ensure_tables(db, ["bars"])
        report.tables["bars"] = commit(
            db, "bars", bars, note=note, chunk_rows=chunk_rows
        )
    else:
        report.skipped.append({"reason": "no_rows_matched"})
    return report


def bars_from_trades(
    db: Any,
    *,
    interval: Union[str, int],
    instruments: Optional[Sequence[str]] = None,
    source_vendor: str = "derived-trades",
    chunk_rows: int = 250_000,
    note: Optional[str] = None,
) -> IngestReport:
    """Aggregate the stored `trades` table into `bars`.

    This is how a venue that publishes no OHLCV gets bars: they are derived
    from its own prints rather than fetched from somewhere that derived them
    differently. Only intervals that actually contain a trade produce a bar,
    so gaps stay visible as missing bars instead of being forward-filled into
    flat candles that imply a quiet market rather than an empty one.

    `ts_init` is the interval end, matching every other bar in the table: the
    aggregate is not knowable until the interval closes.
    """
    span = parse_interval(interval)
    if "trades" not in set(db.tables()):
        raise ValueError("no `trades` table to aggregate; ingest trades first")

    predicate = ""
    if instruments:
        quoted = ", ".join("'" + str(name).replace("'", "''") + "'" for name in instruments)
        predicate = f" WHERE instrument_id IN ({quoted})"
    trades = db.sql(
        "SELECT instrument_id, outcome, ts_event, price, size FROM trades"
        + predicate
        + " ORDER BY instrument_id, outcome, ts_event",
        target_partitions=1,
    ).to_arrow()

    report = IngestReport(vendor=source_vendor)
    if trades.num_rows == 0:
        report.skipped.append({"reason": "no_trades_matched"})
        return report

    stamps = pc.cast(pc.cast(trades.column("ts_event"), pa.timestamp("ns")), pa.int64())
    bucket = pc.multiply(
        pc.cast(
            pc.floor(pc.divide(pc.cast(stamps, pa.float64()), float(span))), pa.int64()
        ),
        pa.scalar(span, pa.int64()),
    )
    working = pa.table(
        {
            "instrument_id": trades.column("instrument_id"),
            "outcome": trades.column("outcome"),
            "bucket": bucket,
            "price": pc.cast(trades.column("price"), pa.float64()),
            "size": pc.cast(trades.column("size"), pa.float64()),
        }
    )
    # Rows arrive ordered by time, so "first" and "last" within a group are the
    # open and the close. Grouping does not reorder within a group.
    grouped = working.group_by(
        ["instrument_id", "outcome", "bucket"], use_threads=False
    ).aggregate(
        [
            ("price", "first"),
            ("price", "max"),
            ("price", "min"),
            ("price", "last"),
            ("size", "sum"),
        ]
    )
    ends = pc.add(grouped.column("bucket"), pa.scalar(span, pa.int64()))
    bars = pa.table(
        {
            "ts_init": pc.cast(ends, pa.timestamp("ns")),
            "ts_event": pc.cast(grouped.column("bucket"), pa.timestamp("ns")),
            "instrument_id": pc.cast(grouped.column("instrument_id"), pa.string()),
            "outcome": pc.cast(grouped.column("outcome"), pa.uint16()),
            "open": grouped.column("price_first"),
            "high": grouped.column("price_max"),
            "low": grouped.column("price_min"),
            "close": grouped.column("price_last"),
            "volume": pc.fill_null(grouped.column("size_sum"), 0.0),
            "source_vendor": pa.array(
                [source_vendor] * grouped.num_rows, pa.string()
            ),
        },
        schema=BARS_SCHEMA,
    )
    ensure_tables(db, ["bars"])
    report.tables["bars"] = commit(db, "bars", bars, note=note, chunk_rows=chunk_rows)
    ends_int = pc.cast(bars.column("ts_init"), pa.int64())
    report.loaded_window = (pc.min(ends_int).as_py(), pc.max(ends_int).as_py())
    return report
