"""Bulk trade dumps, and the one field in them that inverts.

A trade file is easy to load and easy to load backwards. Every vendor agrees on
price, size and time; they disagree on how to say which side crossed the
spread, and the two common spellings mean opposite things. A file read with the
sign inverted still balances, still sums to the right volume, and is wrong in
the only way order-flow research cares about, so that is what these pin down.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

import h5i_db
from h5i_db import venues

# Binance's dump: headerless, microseconds, and `is_buyer_maker` true when the
# BUYER rested, which makes the seller the aggressor.
_TRADES_CSV = (
    "6517796134,64722.55000000,0.00008000,5.17780400,1784505600137657,False,True\n"
    "6517796135,64722.60000000,0.00002000,1.29445100,1784505600137657,True,True\n"
)


def _write(tmp: Path, text: str, name: str = "BTCUSDT-trades-2026-07-20.csv") -> Path:
    path = tmp / name
    path.write_text(text, encoding="utf-8")
    return path


def test_a_resting_buyer_means_the_seller_was_the_aggressor():
    with tempfile.TemporaryDirectory() as tmp:
        path = _write(Path(tmp), _TRADES_CSV)
        table = venues.read_trades_csv(path, layout=venues.BINANCE_TRADES_LAYOUT)
        trades = venues.trades_from_table(
            table, instrument_id="BTCUSDT", layout=venues.BINANCE_TRADES_LAYOUT
        )

        # Row 0 has is_buyer_maker=False: the buyer took, so the aggressor is
        # the buyer. Row 1 is true, so it inverts. Reading the flag straight
        # through would swap both.
        assert trades.column("aggressor").to_pylist() == ["buy", "sell"]
        assert trades.column("price").to_pylist() == [64722.55, 64722.60]
        assert trades.column("trade_id").to_pylist() == ["6517796134", "6517796135"]


def test_the_time_column_is_microseconds_not_milliseconds():
    with tempfile.TemporaryDirectory() as tmp:
        path = _write(Path(tmp), _TRADES_CSV)
        trades = venues.trades_from_table(
            venues.read_trades_csv(path, layout=venues.BINANCE_TRADES_LAYOUT),
            instrument_id="BTCUSDT",
            layout=venues.BINANCE_TRADES_LAYOUT,
        )
        stamped = trades.column("ts_init")[0].as_py()

        # Read as milliseconds this lands in the year 58415, which is the kind
        # of wrong that a window filter turns into an empty result rather than
        # an error.
        assert stamped.year == 2026 and stamped.month == 7 and stamped.day == 20
        # A print is knowable the moment it prints, unlike a bar.
        assert trades.column("ts_init").to_pylist() == trades.column("ts_event").to_pylist()


def test_an_unreadable_side_is_null_and_reported_rather_than_assumed():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        path = _write(
            root,
            "1,100.0,1.0,100.0,1784505600137657,,True\n"
            "2,101.0,2.0,202.0,1784505600137658,False,True\n",
        )
        db = h5i_db.Database(str(root / "m.db"), create=True)
        report = venues.ingest_trades(
            db,
            files=[path],
            layout=venues.BINANCE_TRADES_LAYOUT,
            instrument_id="BTCUSDT",
        )
        stored = db.sql("SELECT * FROM trades ORDER BY trade_id").to_pandas()

        # Defaulting the missing flag to false would assert the trade was
        # buyer-initiated, which is a claim the file never made.
        assert stored.aggressor.isna().sum() == 1
        unreadable = [s for s in report.skipped if s["reason"] == "aggressor_unreadable"]
        assert unreadable and unreadable[0]["rows"] == 1


def test_the_two_ways_of_naming_the_aggressor_cannot_both_be_given():
    # They describe the same fact in opposite directions, so honouring both
    # would make the file's meaning depend on which field this code read first.
    with pytest.raises(ValueError, match="not both"):
        venues.TradeLayout(
            name="ambiguous",
            aggressor_column="side",
            buyer_is_maker_column="is_buyer_maker",
        )


def test_a_side_column_is_read_by_the_names_the_layout_states():
    table = __import__("pyarrow").table(
        {
            "time": [1784505600137657, 1784505600137658],
            "price": [1.0, 2.0],
            "size": [3.0, 4.0],
            "side": ["SELL", "BUY"],
        }
    )
    layout = venues.TradeLayout(
        name="sided", time_column="time", size_column="size", aggressor_column="side"
    )
    trades = venues.trades_from_table(table, instrument_id="X", layout=layout)
    assert trades.column("aggressor").to_pylist() == ["sell", "buy"]


def test_aggregated_and_raw_dumps_agree_on_the_same_tape():
    # The two Binance files describe one tape at different granularities, so a
    # layout that read either one backwards would disagree with the other.
    agg_csv = "4017553738,64722.55000000,0.00010000,6517796134,6517796135,1784505600137657,False,True\n"
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        agg_path = _write(root, agg_csv, name="agg.csv")
        agg = venues.trades_from_table(
            venues.read_trades_csv(agg_path, layout=venues.BINANCE_AGG_TRADES_LAYOUT),
            instrument_id="BTCUSDT",
            layout=venues.BINANCE_AGG_TRADES_LAYOUT,
        )
        raw_path = _write(root, _TRADES_CSV, name="raw.csv")
        raw = venues.trades_from_table(
            venues.read_trades_csv(raw_path, layout=venues.BINANCE_TRADES_LAYOUT),
            instrument_id="BTCUSDT",
            layout=venues.BINANCE_TRADES_LAYOUT,
        )
        assert agg.column("aggressor")[0].as_py() == raw.column("aggressor")[0].as_py()
        assert agg.column("ts_init")[0].as_py() == raw.column("ts_init")[0].as_py()
