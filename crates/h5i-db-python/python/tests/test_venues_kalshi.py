"""The outcome-major, relative-delta archive shape, as Kalshi ships it.

These build Parquet matching the column contract measured from real pmxt files
rather than shipping a sample, so they run offline and state the contract in
one place. What they check is the part that silently corrupts a study when
wrong: that a change in resting size is accumulated rather than written as a
level, that a book with no base is refused rather than invented, and that the
order events are emitted in is the order a replay applies them.
"""

from __future__ import annotations

import tempfile
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import h5i_db
from h5i_db import venues

SECOND_NS = 1_000_000_000
BASE_MS = 1_777_000_000_000
TICKER = "KXTEST-26DEC31"

# Prices and sizes arrive as decimals, and the struct fields are positional in
# the file: price first, size second, named "1" and "2".
_LEVEL = pa.struct(
    [pa.field("1", pa.decimal128(9, 4)), pa.field("2", pa.decimal128(18, 6))]
)
_SCHEMA = pa.schema(
    [
        pa.field("timestamp_received", pa.timestamp("ms", tz="UTC"), nullable=False),
        pa.field("timestamp", pa.timestamp("us", tz="UTC")),
        pa.field("market_ticker", pa.string(), nullable=False),
        pa.field("event_type", pa.string(), nullable=False),
        pa.field("yes_bids", pa.list_(_LEVEL), nullable=False),
        pa.field("no_bids", pa.list_(_LEVEL), nullable=False),
        pa.field("price", pa.decimal128(9, 4)),
        pa.field("delta", pa.decimal128(18, 6)),
        pa.field("side", pa.string(), nullable=False),
    ]
)


def _levels(pairs):
    return [{"1": Decimal(str(p)), "2": Decimal(str(s))} for p, s in pairs]


def _snapshot(received_ms, yes, no):
    return {
        "timestamp_received": received_ms,
        # A snapshot carries no venue clock. That is the whole reason it seeds
        # the book rather than being replayed as an event at its own stamp.
        "timestamp": None,
        "market_ticker": TICKER,
        "event_type": "orderbook_snapshot",
        "yes_bids": _levels(yes),
        "no_bids": _levels(no),
        "price": None,
        "delta": None,
        "side": "",
    }


def _delta(received_ms, venue_us, side, price, change):
    return {
        "timestamp_received": received_ms,
        "timestamp": venue_us,
        "market_ticker": TICKER,
        "event_type": "orderbook_delta",
        "yes_bids": [],
        "no_bids": [],
        "price": Decimal(str(price)),
        "delta": Decimal(str(change)),
        "side": side,
    }


def _write(path: Path, rows) -> None:
    columns = {name: [row[name] for row in rows] for name in _SCHEMA.names}
    table = pa.table(
        {
            name: pa.array(values, type=_SCHEMA.field(name).type)
            for name, values in columns.items()
        },
        schema=_SCHEMA,
    )
    pq.write_table(table, path)


def _spec():
    return venues.MarketSpec(
        instrument_id=TICKER, venue="kalshi", outcome_labels=("yes", "no")
    )


def _ingest(rows, tmp: Path):
    _write(tmp / "hour.parquet", rows)
    db = h5i_db.Database(str(tmp / "market.db"), create=True)
    report = venues.ingest_archive(
        db,
        files=[tmp / "hour.parquet"],
        markets=[_spec()],
        layout=venues.KALSHI_PMXT_LAYOUT,
    )
    return db, report


def test_one_snapshot_row_becomes_one_event_per_outcome():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        db, _ = _ingest(
            [_snapshot(BASE_MS, [(0.40, 100), (0.41, 50)], [(0.58, 70)])], root
        )
        book = db.sql("SELECT * FROM book_deltas ORDER BY event_index, is_last").to_pandas()

        # The row holds two books, so it is two events, never one mixed event.
        assert set(book.action) == {"snapshot"}
        assert (book.groupby("event_index").outcome.nunique() == 1).all()
        assert set(book.outcome) == {0, 1}
        # Both outcomes are quoted as bids: an ask on YES is a bid on NO, and
        # calling one of them an ask would double-count the same resting order.
        assert set(book.side) == {"buy"}
        assert sorted(book[book.outcome == 1].price.tolist()) == [0.58]
        assert (book.groupby("event_index").is_last.sum() == 1).all()


def test_a_delta_is_a_change_in_size_not_a_new_size():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        db, _ = _ingest(
            [
                _snapshot(BASE_MS, [(0.40, 100.0)], [(0.58, 70.0)]),
                # +25 on a level resting at 100 must store 125, not 25.
                _delta(BASE_MS + 1000, BASE_MS * 1000 + 10, "yes", 0.40, 25.0),
                # And taking the rest of it away is a delete, not a zero level.
                _delta(BASE_MS + 2000, BASE_MS * 1000 + 20, "yes", 0.40, -125.0),
            ],
            root,
        )
        book = db.sql(
            "SELECT * FROM book_deltas WHERE outcome = 0 ORDER BY event_index"
        ).to_pandas()

        applied = book[book.action.isin(["set", "delete"])]
        assert applied.action.tolist() == ["set", "delete"]
        assert float(applied[applied.action == "set"]["size"].iloc[0]) == 125.0
        assert float(applied[applied.action == "delete"]["size"].iloc[0]) == 0.0


def test_a_change_with_no_base_is_refused_rather_than_assumed_zero():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        # An hour in which this market published changes but was never
        # snapshotted. A snapshot row would seed *both* books at once, since it
        # carries a column per outcome, so the only way to have no base is to
        # have no snapshot row at all.
        db, report = _ingest(
            [
                _delta(BASE_MS + 1000, BASE_MS * 1000 + 10, "no", 0.58, -40.0),
                _delta(BASE_MS + 2000, BASE_MS * 1000 + 20, "yes", 0.40, 25.0),
            ],
            root,
        )

        # Treating the absent base as zero would invent a level resting at -40,
        # so the rows are dropped and counted instead of priced.
        assert "book_deltas" not in db.tables()
        unseeded = [s for s in report.skipped if s["reason"] == "delta_before_snapshot"]
        assert unseeded and unseeded[0]["rows"] == 2


def test_the_seed_lands_before_the_deltas_it_is_the_base_for():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        # The snapshot's arrival stamp is far LATER than the venue stamps of
        # the deltas, which is how this vendor actually writes its files.
        db, report = _ingest(
            [
                _snapshot(BASE_MS + 1_800_000, [(0.40, 100.0)], []),
                _delta(BASE_MS + 1_800_000, BASE_MS * 1000 + 10, "yes", 0.40, 5.0),
            ],
            root,
        )
        book = db.sql(
            "SELECT * FROM book_deltas WHERE outcome = 0 ORDER BY ts_init"
        ).to_pandas()

        # Ordered by the replay clock, the base must precede the change. If the
        # snapshot kept its own stamp it would sort after, and the change would
        # have nothing to apply to.
        assert book.action.tolist() == ["snapshot", "set"]
        assert book.ts_init.is_monotonic_increasing
        assert float(book[book.action == "set"]["size"].iloc[0]) == 105.0
        restamped = [s for s in report.skipped if s["reason"] == "seed_restamped_to_first_delta"]
        assert restamped and restamped[0]["outcomes"] == 1


def test_divergence_against_later_snapshots_is_measured_and_reported():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        db, report = _ingest(
            [
                _snapshot(BASE_MS, [(0.40, 100.0)], []),
                _delta(BASE_MS + 1000, BASE_MS * 1000 + 10, "yes", 0.40, 25.0),
                # A later snapshot agreeing with the reconstruction: 125.
                _snapshot(BASE_MS + 2000, [(0.40, 125.0)], []),
                # And one that does not, which is the only way a feed with no
                # sequence numbers can reveal a lost message.
                _snapshot(BASE_MS + 3000, [(0.40, 999.0)], []),
            ],
            root,
        )
        divergence = [g for g in report.gaps if g["reason"] == "snapshot_divergence"]
        assert divergence
        # Two probes per snapshot row (one per outcome); the YES book is
        # reproduced once and missed once, and both empty NO books match.
        assert divergence[0]["probes"] == 4
        assert divergence[0]["reproduced"] == 3
        assert divergence[0]["unreproduced"] == 1


def test_replay_order_is_the_order_the_rows_were_reconstructed_in():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        # Deltas out of file order: the venue clock, not the file, is the truth.
        db, _ = _ingest(
            [
                _snapshot(BASE_MS, [(0.40, 100.0)], []),
                _delta(BASE_MS + 9000, BASE_MS * 1000 + 30, "yes", 0.40, -10.0),
                _delta(BASE_MS + 9000, BASE_MS * 1000 + 10, "yes", 0.40, 5.0),
                _delta(BASE_MS + 9000, BASE_MS * 1000 + 20, "yes", 0.40, 5.0),
            ],
            root,
        )
        book = db.sql(
            "SELECT * FROM book_deltas WHERE action = 'set' ORDER BY ts_init, event_index"
        ).to_pandas()

        # 100 +5 +5 -10 walked in venue order leaves 100, and every intermediate
        # level must be the running total rather than the file's order.
        assert [float(v) for v in book["size"]] == [105.0, 110.0, 100.0]
        assert book.ts_init.is_monotonic_increasing


def test_an_unknown_outcome_label_is_counted_not_guessed():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        db, report = _ingest(
            [
                _snapshot(BASE_MS, [(0.40, 100.0)], []),
                _delta(BASE_MS + 1000, BASE_MS * 1000 + 10, "maybe", 0.40, 5.0),
            ],
            root,
        )
        book = db.sql("SELECT * FROM book_deltas").to_pandas()
        assert "set" not in set(book.action)
        unknown = [s for s in report.skipped if s["reason"] == "unknown_event_types"]
        assert unknown and unknown[0]["counts"]["outcome:maybe"] == 1


def test_the_layout_refuses_a_configuration_that_cannot_mean_anything():
    from dataclasses import replace

    # Seeding is only meaningful for a relative feed: on an absolute one there
    # is nothing to accumulate onto, so the combination is rejected rather than
    # quietly ignored.
    with pytest.raises(ValueError, match="relative delta feed"):
        replace(venues.KALSHI_PMXT_LAYOUT, delta_relative=False)
    with pytest.raises(ValueError, match="time_basis"):
        replace(venues.KALSHI_PMXT_LAYOUT, time_basis="whenever")
