"""Predexon's Kalshi orderbook history, and the two conversions it needs.

This source publishes one YES book with both sides, in whole cents, as full
snapshots. The tables here are outcome-major and priced in dollars, so both
have to be converted, and both conversions are exact rather than approximate.
The third thing it publishes is a `sequence` field that looks like a per-market
counter and is not one, which is the trap these pin down.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import h5i_db
from h5i_db import venues

TICKER = "KXTEST-26DEC31"
MS = 1_000_000


def _spec():
    return venues.MarketSpec(
        instrument_id=TICKER, venue="kalshi", outcome_labels=("yes", "no")
    )


def _snapshot(timestamp_ms, sequence, yes_bids, yes_asks):
    return {
        "ticker": TICKER,
        "timestamp": timestamp_ms,
        "sequence": sequence,
        "yes_bids": [{"price": p, "size": s} for p, s in yes_bids],
        "yes_asks": [{"price": p, "size": s} for p, s in yes_asks],
    }


def test_the_yes_book_keeps_both_of_its_sides():
    book = venues.predexon_book_from_snapshots(
        [_snapshot(1_785_000_000_000, 1, [(90, 494)], [(91, 1196)])],
        markets=[_spec()],
    )
    rows = book.to_pylist()

    # Cents become dollars and the sides stay as the vendor gives them, which
    # is already the shape the live decoder produces. Splitting the ask off
    # into a second outcome would leave a book nothing can buy against.
    assert {r["outcome"] for r in rows} == {0}
    assert [(r["price"], r["size"]) for r in rows if r["side"] == "buy"] == [
        (0.90, 494.0)
    ]
    assert [(r["price"], r["size"]) for r in rows if r["side"] == "sell"] == [
        (0.91, 1196.0)
    ]


def test_each_record_is_a_whole_book_so_nothing_accumulates():
    book = venues.predexon_book_from_snapshots(
        [
            _snapshot(1_785_000_000_000, 1, [(90, 100)], [(91, 200)]),
            _snapshot(1_785_000_005_000, 2, [(89, 300)], [(92, 400)]),
        ],
        markets=[_spec()],
    )
    rows = book.to_pylist()

    # A full snapshot per record means a dropped record costs one sample rather
    # than corrupting every level after it, which is the reason to prefer this
    # source over an archive of relative changes.
    assert {r["action"] for r in rows} == {"snapshot"}
    # One event per record, carrying that record's whole two-sided book.
    assert len({r["event_index"] for r in rows}) == 2
    groups: dict[int, list] = {}
    for row in rows:
        groups.setdefault(row["event_index"], []).append(row)
    assert all(sum(1 for r in rs if r["is_last"]) == 1 for rs in groups.values())
    # One event never mixes outcomes, and carries both sides of the one it has.
    assert all(len({r["outcome"] for r in rs}) == 1 for rs in groups.values())
    assert all({r["side"] for r in rs} == {"buy", "sell"} for rs in groups.values())


def test_a_wild_sequence_field_produces_no_phantom_gaps():
    report = venues.IngestReport(vendor="predexon")
    venues.predexon_book_from_snapshots(
        [
            # Real values from this vendor: a step of 7, then a reset, then a
            # jump of two million. Differencing them would claim millions of
            # missing updates on a market that had a few dozen.
            _snapshot(1_785_000_000_000, 528_246, [(90, 1)], [(91, 1)]),
            _snapshot(1_785_000_001_000, 528_253, [(90, 1)], [(91, 1)]),
            _snapshot(1_785_000_002_000, 123, [(90, 1)], [(91, 1)]),
            _snapshot(1_785_000_003_000, 2_306_209, [(90, 1)], [(91, 1)]),
        ],
        markets=[_spec()],
        report=report,
    )
    reasons = {gap["reason"] for gap in report.gaps}
    assert "sequence_reset" not in reasons
    assert not any("update" in reason for reason in reasons)


def test_the_sampling_cadence_is_measured_rather_than_claimed():
    report = venues.IngestReport(vendor="predexon")
    venues.predexon_book_from_snapshots(
        [
            _snapshot(1_785_000_000_000, 1, [(90, 1)], [(91, 1)]),
            _snapshot(1_785_000_005_000, 2, [(90, 1)], [(91, 1)]),
            _snapshot(1_785_000_010_000, 3, [(90, 1)], [(91, 1)]),
            # A long hole: the touch could have moved unobserved right here.
            _snapshot(1_785_000_610_000, 4, [(90, 1)], [(91, 1)]),
        ],
        markets=[_spec()],
        report=report,
    )
    cadence = [g for g in report.gaps if g["reason"] == "snapshot_cadence"]

    assert cadence
    assert cadence[0]["median_ns"] == 5_000 * MS
    # The worst hole matters more than the median: a strategy reading a
    # ten-minute gap as continuous fills at prices nobody quoted.
    assert cadence[0]["max_ns"] == 600_000 * MS


def test_an_unrequested_ticker_is_reported_not_ingested():
    report = venues.IngestReport(vendor="predexon")
    book = venues.predexon_book_from_snapshots(
        [
            {"ticker": "SOMETHING-ELSE", "timestamp": 1_785_000_000_000,
             "sequence": 1, "yes_bids": [{"price": 5, "size": 1}], "yes_asks": []},
        ],
        markets=[_spec()],
        report=report,
    )
    assert book.num_rows == 0
    assert report.unknown_instruments == ["SOMETHING-ELSE"]


def test_snapshots_ingest_and_reingest_as_a_replay():
    with tempfile.TemporaryDirectory() as tmp:
        db = h5i_db.Database(str(Path(tmp) / "m.db"), create=True)
        snapshots = [
            _snapshot(1_785_000_000_000, 1, [(90, 100)], [(91, 200)]),
            _snapshot(1_785_000_005_000, 2, [(89, 300)], [(92, 400)]),
        ]
        first = venues.ingest_predexon_orderbooks(
            db, snapshots=snapshots, markets=[_spec()]
        )
        second = venues.ingest_predexon_orderbooks(
            db, snapshots=snapshots, markets=[_spec()]
        )
        rows = db.sql("SELECT count(*) AS n FROM book_deltas").to_pandas()["n"][0]

        assert first.replayed is False and second.replayed is True
        assert int(rows) == 4
