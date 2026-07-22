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
        # Compact + drop_table.
        db.append("trades", _sample())
        db.compact("trades")
        db.drop_table("trades")
        assert "trades" not in db.tables()


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"ok  {name}")
    print("all bindings tests passed")
