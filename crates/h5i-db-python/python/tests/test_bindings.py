"""Smoke tests for the h5i_db Python bindings.

Runnable under pytest, or directly (``python test_bindings.py``) in any
environment with the wheel and pyarrow installed — used by the wheel-install
smoke step.
"""

from __future__ import annotations

import tempfile

import pyarrow as pa

import h5i_db

SCHEMA = pa.schema(
    [
        pa.field("ts", pa.timestamp("ns"), nullable=False),
        pa.field("symbol", pa.string()),
        pa.field("px", pa.float64()),
    ]
)


def _sample(n: int = 5) -> pa.Table:
    return pa.table(
        {
            "ts": pa.array(range(n), type=pa.timestamp("ns")),
            "symbol": ["A", "B", "A", "B", "A"][:n],
            "px": [float(i) for i in range(n)],
        },
        schema=SCHEMA,
    )


def _open_db(tmp: str) -> h5i_db.Database:
    db = h5i_db.Database(f"{tmp}/t.db", create=True)
    db.create_table("trades", SCHEMA, time_column="ts")
    return db


def test_empty_result_keeps_schema():
    with tempfile.TemporaryDirectory() as tmp, _open_db(tmp) as db:
        # Empty table read: full schema, zero rows (ROADMAP 3.9).
        empty = db.read("trades")
        assert empty.num_rows == 0
        assert empty.schema.equals(SCHEMA), empty.schema
        # Column projection keeps the projected schema even when empty.
        proj = db.read("trades", columns=["px", "symbol"])
        assert proj.schema.names == ["px", "symbol"]
        # SQL with an always-false predicate keeps the query schema.
        db.append("trades", _sample())
        res = db.sql("SELECT ts, px FROM h5i('trades') WHERE px < -1").to_arrow()
        assert res.num_rows == 0
        assert res.schema.names == ["ts", "px"]


def test_exception_types_and_attributes():
    with tempfile.TemporaryDirectory() as tmp, _open_db(tmp) as db:
        try:
            db.read("nope")
            raise AssertionError("expected NotFoundError")
        except h5i_db.NotFoundError as e:
            assert isinstance(e, h5i_db.H5iError)
            assert e.code == "table_not_found"
            assert e.retryable is False
            assert e.hint  # points at listing tables
        try:
            db.sql("SELEC nonsense")
            raise AssertionError("expected InvalidInputError")
        except h5i_db.InvalidInputError as e:
            assert e.code == "invalid_input"


def test_sql_max_rows_and_timeout_args():
    with tempfile.TemporaryDirectory() as tmp, _open_db(tmp) as db:
        db.append("trades", _sample())
        # Under the cap: fine (timeout generous, exercises the knob).
        assert len(db.sql("SELECT * FROM h5i('trades')", timeout=60, max_rows=100)) == 5
        try:
            db.sql("SELECT * FROM h5i('trades')", max_rows=2)
            raise AssertionError("expected LimitError")
        except h5i_db.LimitError as e:
            assert e.code == "limit_exceeded"
        try:
            db.sql("SELECT 1", timeout=-1)
            raise AssertionError("expected InvalidInputError")
        except h5i_db.InvalidInputError:
            pass


def test_close_and_context_manager():
    with tempfile.TemporaryDirectory() as tmp:
        db = _open_db(tmp)
        assert not db.closed
        with db:
            db.append("trades", _sample())
        assert db.closed
        db.close()  # idempotent
        try:
            db.tables()
            raise AssertionError("expected H5iError(closed)")
        except h5i_db.H5iError as e:
            assert e.code == "closed"


def test_new_methods():
    with tempfile.TemporaryDirectory() as tmp, _open_db(tmp) as db:
        db.append("trades", _sample())
        assert db.schema("trades").equals(SCHEMA)
        # Policy round-trip.
        pol = db.policy()
        assert pol.get("direct_delete") is True
        assert db.set_policy(direct_delete=False)["direct_delete"] is False
        assert db.policy()["direct_delete"] is False
        try:
            db.set_policy(bogus_flag=True)
            raise AssertionError("expected InvalidInputError")
        except h5i_db.InvalidInputError:
            pass
        db.set_policy(direct_delete=True)
        # Plans listing.
        plan = db.plan_delete_range("trades", 0, 2)
        listed = db.list_plans("trades")
        assert plan.plan_id in [p.plan_id for p in listed]
        plan.discard()
        assert plan.plan_id not in [p.plan_id for p in db.list_plans("trades")]
        # Compact + drop_table. Append a second, strictly-later batch (ordered
        # append requires min ts >= the table's current max) to give compaction
        # more than one segment to merge.
        db.append(
            "trades",
            pa.table(
                {
                    "ts": pa.array(range(5, 10), type=pa.timestamp("ns")),
                    "symbol": ["A", "B", "A", "B", "A"],
                    "px": [float(i) for i in range(5, 10)],
                },
                schema=SCHEMA,
            ),
        )
        db.compact("trades")
        db.drop_table("trades")
        assert "trades" not in db.tables()


def test_leakage_check_detects_withheld_rows():
    with tempfile.TemporaryDirectory() as tmp, _open_db(tmp) as db:
        db.append("trades", _sample(3))  # version 1: 3 rows
        # Append two later rows as version 2 (ts strictly increasing).
        later = pa.table(
            {
                "ts": pa.array(range(3, 5), type=pa.timestamp("ns")),
                "symbol": ["B", "A"],
                "px": [3.0, 4.0],
            },
            schema=SCHEMA,
        )
        db.append("trades", later)

        report = db.leakage_check(
            "SELECT count(*) AS c FROM trades", version=1
        )
        assert report["comparable"] is True
        assert report["leakage_detected"] is True
        col = report["columns"][0]
        assert col["name"] == "c"
        assert col["head"] == 5.0 and col["asof"] == 3.0 and col["delta"] == 2.0
        assert report["withheld_versions"][0]["table"] == "trades"

        # A decision point is required.
        try:
            db.leakage_check("SELECT count(*) FROM trades")
            raise AssertionError("expected InvalidInputError")
        except h5i_db.InvalidInputError:
            pass


def test_data_policy_round_trip_and_enforcement():
    with tempfile.TemporaryDirectory() as tmp, _open_db(tmp) as db:
        assert db.data_policy("trades") is None  # unset by default

        policy = {
            "constraints": [
                {
                    "name": "positive_px",
                    "predicate": {
                        "compare": {
                            "column": "px",
                            "op": "gt",
                            "value": {"float": 0.0},
                        }
                    },
                    "on_fail": "reject",
                }
            ]
        }
        stored = db.set_data_policy("trades", policy)
        assert stored["constraints"][0]["name"] == "positive_px"
        assert db.data_policy("trades")["constraints"][0]["name"] == "positive_px"

        # A row with px <= 0 is rejected fail-closed.
        bad = pa.table(
            {
                "ts": pa.array([0], type=pa.timestamp("ns")),
                "symbol": ["A"],
                "px": [-1.0],
            },
            schema=SCHEMA,
        )
        try:
            db.append("trades", bad)
            raise AssertionError("expected the data policy to reject px=-1")
        except h5i_db.InvalidInputError as e:
            assert e.code == "data_policy_violation"

        # A conforming batch (all px > 0) is accepted.
        good = pa.table(
            {
                "ts": pa.array([0, 1, 2], type=pa.timestamp("ns")),
                "symbol": ["A", "B", "A"],
                "px": [1.0, 2.0, 3.0],
            },
            schema=SCHEMA,
        )
        db.append("trades", good)

        # Clearing removes the constraint entirely: a negative-px row (later
        # timestamp, to satisfy ordered append) now writes without error.
        db.clear_data_policy("trades")
        assert db.data_policy("trades") is None
        bad_later = pa.table(
            {
                "ts": pa.array([10], type=pa.timestamp("ns")),
                "symbol": ["A"],
                "px": [-1.0],
            },
            schema=SCHEMA,
        )
        db.append("trades", bad_later)


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"ok  {name}")
    print("all bindings tests passed")
