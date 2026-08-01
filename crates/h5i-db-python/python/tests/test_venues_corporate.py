"""Corporate actions, and the two ways they quietly ruin an equity backtest.

The first is arithmetic: a 2-for-1 split that never reaches the run looks like
a 50% overnight crash, and the strategy that "survived" it is measuring a
bookkeeping artefact. The second is time: adjustment data is point-in-time, and
a run that loads every action ever recorded knows about splits nobody had
announced on the simulated date. Both fail silently, which is why the loader
refuses ambiguity here rather than resolving it.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

import h5i_db
from h5i_db import venues

DAY_NS = 86_400 * 1_000_000_000
EFFECTIVE = "2026-03-02"
ANNOUNCED = "2026-02-01"


def _rows():
    return [
        {"instrument_id": "AAPL", "kind": "split", "ratio": 2.0,
         "effective": EFFECTIVE, "announced": ANNOUNCED},
        {"instrument_id": "MSFT", "kind": "dividend", "per_share": 0.75,
         "effective": EFFECTIVE, "announced": ANNOUNCED},
        {"instrument_id": "XYZ", "kind": "delist", "final_price": 3.25,
         "effective": EFFECTIVE, "announced": ANNOUNCED},
    ]


def test_each_kind_carries_its_own_value_and_no_other():
    table = venues.corporate_actions_from_rows(_rows())
    by_id = {row["instrument_id"]: row for row in table.to_pylist()}

    # A ratio and a per-share amount are different quantities. Sharing one
    # column would let a reader take a dividend for a split.
    assert by_id["AAPL"]["ratio"] == 2.0
    assert by_id["AAPL"]["per_share"] is None and by_id["AAPL"]["final_price"] is None
    assert by_id["MSFT"]["per_share"] == 0.75 and by_id["MSFT"]["ratio"] is None
    assert by_id["XYZ"]["final_price"] == 3.25
    assert table.schema == venues.CORPORATE_ACTIONS_SCHEMA


def test_the_replay_clock_is_when_the_action_takes_effect():
    table = venues.corporate_actions_from_rows(_rows())
    # The engine has to apply a split to positions and resting orders at the
    # instant it happens, not when it was announced a month earlier.
    assert table.column("ts_init").to_pylist() == table.column("ts_event").to_pylist()
    stamped = table.column("ts_init")[0].as_py()
    assert (stamped.year, stamped.month, stamped.day) == (2026, 3, 2)
    # The announcement is kept as a separate fact, which is what makes a
    # point-in-time filter expressible at all.
    assert table.column("announced_ns")[0].as_py() < int(
        table.column("ts_init")[0].as_py().timestamp() * 1_000_000_000
    )


@pytest.mark.parametrize(
    "row, message",
    [
        ({"instrument_id": "A", "kind": "split", "ratio": 0.0,
          "effective": EFFECTIVE}, "positive"),
        ({"instrument_id": "A", "kind": "split", "ratio": -2.0,
          "effective": EFFECTIVE}, "positive"),
        # A negative dividend is a capital call, a different instrument's
        # problem, and a sign error here quietly drains an account.
        ({"instrument_id": "A", "kind": "dividend", "per_share": -0.5,
          "effective": EFFECTIVE}, "must not be negative"),
        ({"instrument_id": "A", "kind": "split", "effective": EFFECTIVE}, "needs ratio"),
        ({"instrument_id": "A", "kind": "buyback", "effective": EFFECTIVE},
         "not a corporate action"),
    ],
)
def test_an_unusable_action_is_refused_at_load(row, message):
    with pytest.raises(ValueError, match=message):
        venues.corporate_actions_from_rows([row])


def test_a_value_belonging_to_another_kind_is_a_mapping_error():
    # Far likelier to be a mis-mapped column than a harmless extra, and
    # ignoring it would load the dividend at whatever happened to be there.
    with pytest.raises(ValueError, match="mapped wrong"):
        venues.corporate_actions_from_rows(
            [{"instrument_id": "A", "kind": "dividend", "per_share": 1.0,
              "ratio": 2.0, "effective": EFFECTIVE}]
        )


def test_an_action_announced_after_it_happened_is_a_swapped_column():
    with pytest.raises(ValueError, match="swapped"):
        venues.corporate_actions_from_rows(
            [{"instrument_id": "A", "kind": "split", "ratio": 2.0,
              "effective": "2026-03-02", "announced": "2026-04-01"}]
        )


def test_a_cutoff_keeps_only_what_had_been_announced_by_then():
    with tempfile.TemporaryDirectory() as tmp:
        db = h5i_db.Database(str(Path(tmp) / "m.db"), create=True)
        rows = _rows() + [
            # Announced after the cutoff: on the simulated date nobody knew.
            {"instrument_id": "LATE", "kind": "split", "ratio": 3.0,
             "effective": "2026-06-01", "announced": "2026-05-01"},
            # No announcement at all, so it cannot be placed on that axis.
            {"instrument_id": "UNDATED", "kind": "dividend", "per_share": 1.0,
             "effective": "2026-03-02"},
        ]
        cutoff = int(
            __import__("datetime").datetime(2026, 3, 1).timestamp() * 1_000_000_000
        )
        report = venues.ingest_corporate_actions(db, actions=rows, known_by=cutoff)
        stored = db.sql("SELECT * FROM corporate_actions").to_pandas()

        assert set(stored.instrument_id) == {"AAPL", "MSFT", "XYZ"}
        dropped = [s for s in report.skipped if s["reason"] == "announced_after_cutoff"]
        # The undated row is dropped too: assuming it was known early enough is
        # exactly how a backtest ends up trading a split nobody had heard of.
        assert dropped and dropped[0]["rows"] == 2
        assert dropped[0]["without_announcement"] == 1


def test_reingesting_the_same_actions_replays_instead_of_duplicating():
    with tempfile.TemporaryDirectory() as tmp:
        db = h5i_db.Database(str(Path(tmp) / "m.db"), create=True)
        first = venues.ingest_corporate_actions(db, actions=_rows())
        second = venues.ingest_corporate_actions(db, actions=_rows())
        rows = db.sql("SELECT count(*) AS n FROM corporate_actions").to_pandas()["n"][0]

        assert first.replayed is False and second.replayed is True
        assert int(rows) == 3


def test_a_run_replays_actions_written_by_this_loader():
    """The cross-language seam: Python writes the table, Rust reads it.

    The two schemas are maintained separately, so a drift in nullability or
    column order would not fail either side's own tests. It would fail here,
    when the replay asks the store for the stream.
    """
    import pyarrow as pa

    from h5i_db import backtest

    second = 1_000_000_000
    base = 1_777_000_000 * second
    with tempfile.TemporaryDirectory() as tmp:
        db = h5i_db.Database(str(Path(tmp) / "run.db"), create=True)
        spec = venues.MarketSpec(
            instrument_id="EQ", venue="test", outcome_labels=("YES", "NO")
        )
        venues.write_markets(db, [spec])
        venues.ensure_tables(db, ["book_deltas"])

        rows = []
        for step in range(6):
            stamp = base + step * 30 * second
            for outcome, (bid, ask) in enumerate(((0.40, 0.42), (0.57, 0.59))):
                for index, (side, price) in enumerate((("buy", bid), ("sell", ask))):
                    rows.append(
                        {
                            "ts_init": stamp,
                            "ts_event": stamp,
                            "instrument_id": "EQ",
                            "outcome": outcome,
                            "action": "snapshot",
                            "side": side,
                            "price": price,
                            "size": 500.0,
                            "event_index": step * 2 + outcome + 1,
                            "is_last": index == 1,
                            "source_vendor": "test",
                        }
                    )
        book = pa.table(
            {
                name: pa.array(
                    [row[name] for row in rows],
                    type=venues.BOOK_DELTAS_SCHEMA.field(name).type,
                )
                for name in venues.BOOK_DELTAS_SCHEMA.names
            },
            schema=venues.BOOK_DELTAS_SCHEMA,
        )
        db.append("book_deltas", book)

        venues.ingest_corporate_actions(
            db,
            actions=[
                {"instrument_id": "EQ", "kind": "dividend", "per_share": 0.01,
                 "effective": base + 90 * second, "announced": base},
            ],
        )
        db.snapshot("loaded", tables=["instruments", "book_deltas", "corporate_actions"])

        backtest.create_signal_table(db, "signals")
        db.append(
            "signals",
            backtest.signal_table(
                [
                    {
                        "ts": base + 60 * second + 1_000,
                        "instrument_id": "EQ",
                        "outcome": 0,
                        "side": "buy",
                        "quantity": 10.0,
                        "tag": "yes",
                    }
                ]
            ),
        )
        result = backtest.execute(
            db,
            backtest.BacktestConfig(
                run_id="with-actions",
                data=backtest.DataConfig(signals="signals", snapshot="loaded"),
                portfolio=backtest.PortfolioConfig(starting_cash=1_000.0),
            ),
        )
        # Reaching a fill means the replay assembled every source, including
        # the corporate stream, from tables this loader wrote.
        assert len(result.fills.to_pandas()) == 1
        db.close()
