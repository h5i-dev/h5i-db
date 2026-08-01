"""Tabular trade dumps into the canonical `trades` table.

The sibling of :mod:`._bars` for prints rather than aggregates. A venue that
publishes bulk trade files gives you real microstructure for free, and the only
thing that varies between vendors is column naming and how they spell which
side crossed the spread.

That last part is where the money is. A trade file almost never says "the
aggressor was the buyer": it says something equivalent, and the two common
spellings mean opposite things unless you read them carefully. Binance ships
`isBuyerMaker`, which is true when the *buyer* was resting, so the taker was
the seller. Reading that flag as the aggressor's side inverts every trade sign
in the file, and the result still balances, still sums to the right volume, and
is wrong in exactly the way order-flow research cannot survive. So the layout
names the convention rather than assuming one.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional

import pyarrow as pa
import pyarrow.compute as pc

from ._canonical import (
    TRADES_SCHEMA,
    IngestReport,
    SourceFile,
    commit,
    concat,
    ensure_tables,
)

__all__ = [
    "TradeLayout",
    "BINANCE_AGG_TRADES_LAYOUT",
    "BINANCE_TRADES_LAYOUT",
    "ingest_trades",
    "read_trades_csv",
    "trades_from_table",
]


@dataclass(frozen=True)
class TradeLayout:
    """One vendor's trade-file dialect, as data."""

    name: str
    time_column: str = "time"
    price_column: str = "price"
    size_column: str = "size"
    time_unit: str = "auto"
    trade_id_column: Optional[str] = None
    instrument_column: Optional[str] = None
    #: A column naming the side that crossed the spread, in the vendor's own
    #: words (`buy`/`sell`, `b`/`s`, …).
    aggressor_column: Optional[str] = None
    buy_values: tuple[str, ...] = ("buy", "b", "bid", "taker_buy")
    sell_values: tuple[str, ...] = ("sell", "s", "ask", "taker_sell")
    #: A boolean column that is true when the *buyer* was the resting side. It
    #: is the inverse of the aggressor, which is why it is a separate field
    #: instead of another spelling in `buy_values`.
    buyer_is_maker_column: Optional[str] = None
    #: Headerless files: the names to assign, in file order.
    column_names: tuple[str, ...] = ()
    csv_delimiter: str = ","
    file_glob: str = "*.csv"

    def __post_init__(self) -> None:
        if self.time_unit not in ("auto", "s", "ms", "us", "ns"):
            raise ValueError("time_unit must be auto, s, ms, us or ns")
        if self.aggressor_column and self.buyer_is_maker_column:
            raise ValueError(
                f"{self.name}: give aggressor_column or buyer_is_maker_column, "
                "not both; they describe the same fact in opposite directions "
                "and honouring both would make the file's meaning depend on "
                "which one this code read first"
            )

    def columns(self) -> list[str]:
        wanted = [self.time_column, self.price_column, self.size_column]
        for optional in (
            self.trade_id_column,
            self.instrument_column,
            self.aggressor_column,
            self.buyer_is_maker_column,
        ):
            if optional:
                wanted.append(optional)
        ordered: list[str] = []
        for name in wanted:
            if name not in ordered:
                ordered.append(name)
        return ordered


# Binance's free bulk dumps at data.binance.vision. Headerless, and the time
# column is microseconds rather than the milliseconds older documentation
# describes. `isBuyerMaker` is true when the buyer rested, so the aggressor is
# the seller; see the module docstring for why that is stated and not inferred.
BINANCE_TRADES_LAYOUT = TradeLayout(
    name="binance-trades",
    column_names=(
        "trade_id",
        "price",
        "qty",
        "quote_qty",
        "time",
        "is_buyer_maker",
        "is_best_match",
    ),
    time_column="time",
    price_column="price",
    size_column="qty",
    time_unit="us",
    trade_id_column="trade_id",
    buyer_is_maker_column="is_buyer_maker",
)

# The aggregated file: consecutive fills of one order at one price collapsed
# into a row. Same columns either side of the trade-id pair it carries instead
# of a single id.
BINANCE_AGG_TRADES_LAYOUT = TradeLayout(
    name="binance-agg-trades",
    column_names=(
        "agg_trade_id",
        "price",
        "qty",
        "first_trade_id",
        "last_trade_id",
        "time",
        "is_buyer_maker",
        "is_best_match",
    ),
    time_column="time",
    price_column="price",
    size_column="qty",
    time_unit="us",
    trade_id_column="agg_trade_id",
    buyer_is_maker_column="is_buyer_maker",
)


def _as_bool(column: pa.Array) -> pa.Array:
    """A maker flag as booleans, however the file spelt it.

    A CSV writer may emit `True`, `true` or `1`, and pyarrow infers a different
    type for each. Anything unrecognised becomes null rather than false: an
    unreadable flag means the aggressor is unknown, and false would silently
    assert that every such trade was buyer-initiated.
    """
    if pa.types.is_boolean(column.type):
        return column
    if pa.types.is_integer(column.type):
        return pc.not_equal(column, pa.scalar(0, column.type))
    text = pc.utf8_lower(pc.cast(column, pa.string()))
    truthy = pc.is_in(text, value_set=pa.array(["true", "1", "t", "yes"], pa.string()))
    falsy = pc.is_in(text, value_set=pa.array(["false", "0", "f", "no"], pa.string()))
    return pc.if_else(pc.or_(truthy, falsy), truthy, pa.scalar(None, pa.bool_()))


def trades_from_table(
    table: pa.Table,
    *,
    instrument_id: Optional[str] = None,
    layout: TradeLayout,
    outcome: int = 0,
    source_vendor: Optional[str] = None,
) -> pa.Table:
    """Normalise a trade table into canonical `trades` rows.

    `ts_init` equals `ts_event`: a print is knowable the moment it prints,
    unlike a bar, which is only knowable once its interval closes.
    """
    from ._bars import _to_nanos

    if instrument_id is None and layout.instrument_column is None:
        raise ValueError(
            "give instrument_id, or a layout with instrument_column when the "
            "instrument varies row by row"
        )
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

    stamps = _to_nanos(column(layout.time_column), layout.time_unit)
    rows = table.num_rows

    if layout.buyer_is_maker_column:
        maker = _as_bool(column(layout.buyer_is_maker_column))
        # Buyer resting means the seller crossed, so the flag inverts.
        aggressor = pc.if_else(
            pc.is_valid(maker),
            pc.if_else(maker, pa.scalar("sell"), pa.scalar("buy")),
            pa.scalar(None, pa.string()),
        )
    elif layout.aggressor_column:
        text = pc.utf8_lower(pc.cast(column(layout.aggressor_column), pa.string()))
        is_buy = pc.is_in(text, value_set=pa.array(layout.buy_values, pa.string()))
        is_sell = pc.is_in(text, value_set=pa.array(layout.sell_values, pa.string()))
        aggressor = pc.if_else(
            pc.or_(is_buy, is_sell),
            pc.if_else(is_buy, pa.scalar("buy"), pa.scalar("sell")),
            pa.scalar(None, pa.string()),
        )
    else:
        aggressor = pa.nulls(rows, pa.string())

    trade_id = (
        pc.cast(column(layout.trade_id_column, required=False), pa.string())
        if layout.trade_id_column
        else None
    )
    instruments = (
        pc.cast(column(layout.instrument_column), pa.string())
        if layout.instrument_column
        else pa.array([instrument_id] * rows, pa.string())
    )

    return pa.table(
        {
            "ts_init": pc.cast(stamps, pa.timestamp("ns")),
            "ts_event": pc.cast(stamps, pa.timestamp("ns")),
            "instrument_id": instruments,
            "outcome": pa.array([int(outcome)] * rows, pa.uint16()),
            "price": pc.cast(column(layout.price_column), pa.float64()),
            "size": pc.cast(column(layout.size_column), pa.float64()),
            "aggressor": aggressor,
            "trade_id": trade_id if trade_id is not None else pa.nulls(rows, pa.string()),
            "source_vendor": pa.array(
                [source_vendor or layout.name] * rows, pa.string()
            ),
        },
        schema=TRADES_SCHEMA,
    )


def read_trades_csv(
    path: str | os.PathLike[str], *, layout: TradeLayout
) -> pa.Table:
    """Read one trade CSV as an Arrow table, honouring a headerless layout."""
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


def ingest_trades(
    db: Any,
    *,
    files: Iterable[str | os.PathLike[str]],
    layout: TradeLayout,
    instrument_id: Optional[str] = None,
    outcome: int = 0,
    source_vendor: Optional[str] = None,
    window: Optional[tuple[int, int]] = None,
    chunk_rows: int = 250_000,
    note: Optional[str] = None,
) -> IngestReport:
    """Normalise trade files into the `trades` table.

    `window` is a half-open `[start, end)` in epoch nanoseconds.
    """
    if window is not None and (len(window) != 2 or window[0] >= window[1]):
        raise ValueError("window must be a half-open (start, end) with start < end")

    report = IngestReport(vendor=layout.name, requested_window=window)
    batches: list[pa.Table] = []
    for raw_path in files:
        path = Path(raw_path)
        table = read_trades_csv(path, layout=layout)
        rows_read = table.num_rows
        normalised = trades_from_table(
            table,
            instrument_id=instrument_id,
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

    trades = concat(batches, TRADES_SCHEMA)
    if not trades.num_rows:
        report.skipped.append({"reason": "no_rows_matched"})
        return report
    unknown = pc.sum(
        pc.cast(pc.is_null(trades.column("aggressor")), pa.int64())
    ).as_py()
    if unknown:
        # Order-flow work reads this column, so an unreadable side is reported
        # rather than left to be discovered as a suspiciously balanced tape.
        report.skipped.append({"reason": "aggressor_unreadable", "rows": int(unknown)})
    ensure_tables(db, ["trades"])
    report.tables["trades"] = commit(
        db, "trades", trades, note=note, chunk_rows=chunk_rows
    )
    stamps = pc.cast(trades.column("ts_init"), pa.int64())
    report.loaded_window = (pc.min(stamps).as_py(), pc.max(stamps).as_py())
    return report
