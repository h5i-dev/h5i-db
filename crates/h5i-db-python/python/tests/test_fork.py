"""Fork bindings (ROADMAP Part IX).

Runnable under pytest, or directly (``python test_fork.py``) in any
environment with the wheel and pyarrow installed.

The Python surface is deliberately thin over the engine, so these assert the
things a *notebook* user would trip over -- that a fork handle behaves like a
`Database`, that `as_of` accepts what a Python caller naturally has, and that
a lost promote raises rather than silently merging -- while the storage-level
guarantees are proven in the Rust suites.
"""

from __future__ import annotations

import datetime
import tempfile

import pyarrow as pa
import pytest

import h5i_db

SCHEMA = pa.schema(
    [
        pa.field("ts", pa.timestamp("ns"), nullable=False),
        pa.field("symbol", pa.string()),
        pa.field("px", pa.float64()),
    ]
)


def _sample(start: int = 0, n: int = 5) -> pa.Table:
    return pa.table(
        {
            "ts": pa.array(range(start, start + n), type=pa.timestamp("ns")),
            "symbol": ["A"] * n,
            "px": [float(i) for i in range(start, start + n)],
        },
        schema=SCHEMA,
    )


def _open_db(tmp: str) -> h5i_db.Database:
    db = h5i_db.Database(f"{tmp}/t.db", create=True)
    db.create_table("trades", SCHEMA, time_column="ts")
    db.write("trades", _sample())
    return db


def _rows(db: h5i_db.Database, table: str = "trades") -> int:
    return db.sql(f"select count(*) c from {table}").to_arrow()["c"][0].as_py()


def test_a_fork_handle_is_a_database_handle():
    with tempfile.TemporaryDirectory() as tmp:
        db = _open_db(tmp)
        db.create_fork("agent-01", note="hypothesis 1")
        fork = db.fork("agent-01")

        assert fork.fork_name == "agent-01"
        assert db.fork_name is None
        assert _rows(fork) == 5

        # Ordinary write API, no fork-specific calls: this is the point.
        fork.append("trades", _sample(start=100, n=3))
        assert _rows(fork) == 8
        assert _rows(db) == 5, "the base must not move"


def test_forks_are_isolated_from_each_other():
    with tempfile.TemporaryDirectory() as tmp:
        db = _open_db(tmp)
        for i in range(3):
            db.create_fork(f"agent-{i}")
            db.fork(f"agent-{i}").append("trades", _sample(start=100, n=i + 1))
        for i in range(3):
            assert _rows(db.fork(f"agent-{i}")) == 5 + i + 1
        assert _rows(db) == 5


def test_tables_created_in_a_fork_stay_there():
    with tempfile.TemporaryDirectory() as tmp:
        db = _open_db(tmp)
        db.create_fork("agent-01")
        fork = db.fork("agent-01")
        fork.create_table("features", SCHEMA, time_column="ts")
        fork.write("features", _sample(n=2))

        assert sorted(fork.tables()) == ["features", "trades"]
        assert db.tables() == ["trades"]


def test_forks_listing_reports_ownership_and_what_is_pinned():
    with tempfile.TemporaryDirectory() as tmp:
        db = _open_db(tmp)
        db.create_fork("idle")
        db.create_fork("busy")
        db.fork("busy").append("trades", _sample(start=100, n=1))

        forks = {f["name"]: f for f in db.forks()}
        assert set(forks) == {"idle", "busy"}
        assert forks["idle"]["bytes_own"] == 0
        assert forks["idle"]["bytes_pinned"] > 0
        assert forks["busy"]["tables_shadowed"] == 1
        assert forks["busy"]["bytes_own"] > 0


def test_diff_then_promote_then_the_loser_raises():
    with tempfile.TemporaryDirectory() as tmp:
        db = _open_db(tmp)
        for name in ("agent-a", "agent-b"):
            db.create_fork(name)
            db.fork(name).append("trades", _sample(start=100, n=2))

        diff = db.fork_diff("agent-a")["tables"][0]
        assert diff["kind"] == "shadowed"
        assert (diff["rows_base"], diff["rows_fork"]) == (5, 7)
        assert diff["segments_shared"] == 1, "the base segment is shared, not copied"

        promoted = db.promote("agent-a", "trades")
        assert promoted["rows"] == 7
        assert _rows(db) == 7

        # First commit wins; the loser is rejected, not merged.
        with pytest.raises(h5i_db.ConflictError):
            db.promote("agent-b", "trades")
        assert _rows(db) == 7


def test_dropping_a_fork_releases_the_base():
    with tempfile.TemporaryDirectory() as tmp:
        db = _open_db(tmp)
        db.create_fork("agent-01")
        db.fork("agent-01").append("trades", _sample(start=100, n=1))

        with pytest.raises(h5i_db.H5iError):
            db.drop_table("trades")

        assert db.drop_fork("agent-01") == 1
        assert db.forks() == []
        db.drop_table("trades")


def test_as_of_accepts_the_shapes_a_python_caller_has():
    with tempfile.TemporaryDirectory() as tmp:
        db = _open_db(tmp)
        cutoff_ns = db.versions("trades")[-1]["committed_at_ns"]
        db.append("trades", _sample(start=100, n=4))
        assert _rows(db) == 9

        # datetime resolves to microseconds, commits to nanoseconds, so round
        # *up* to the next microsecond: that is still at or after the commit we
        # want to pin and well before the next one, which keeps this
        # deterministic instead of depending on where the ns digits fell.
        cutoff_us = -(-cutoff_ns // 1000)
        as_datetime = datetime.datetime(
            1970, 1, 1, tzinfo=datetime.timezone.utc
        ) + datetime.timedelta(microseconds=cutoff_us)
        for label, value in [
            ("int ns", cutoff_ns),
            ("datetime", as_datetime),
            ("rfc3339", as_datetime.isoformat().replace("+00:00", "Z")),
        ]:
            name = f"backtest-{label.replace(' ', '-')}"
            db.create_fork(name, as_of=value)
            assert _rows(db.fork(name)) == 5, f"as_of via {label} did not pin the past"


def test_an_as_of_fork_is_writable_but_has_no_look_ahead():
    with tempfile.TemporaryDirectory() as tmp:
        db = _open_db(tmp)
        cutoff = db.versions("trades")[-1]["committed_at_ns"]
        db.append("trades", _sample(start=100, n=4))

        db.create_fork("backtest", as_of=cutoff)
        fork = db.fork("backtest")
        assert _rows(fork) == 5

        fork.create_table("signals", SCHEMA, time_column="ts")
        fork.write("signals", _sample(n=1))
        assert _rows(fork, "signals") == 1
        assert _rows(fork) == 5, "writing must not reveal the future"


def test_bad_as_of_is_rejected_with_a_useful_message():
    with tempfile.TemporaryDirectory() as tmp:
        db = _open_db(tmp)
        with pytest.raises(h5i_db.InvalidInputError):
            db.create_fork("bad", as_of="not a timestamp")
        with pytest.raises(h5i_db.InvalidInputError):
            db.create_fork("bad", as_of=[1, 2, 3])
        # A cutoff before all history pins nothing; fail there, not later.
        with pytest.raises(h5i_db.InvalidInputError):
            db.create_fork("bad", as_of=1)


def test_fork_metadata_round_trips():
    with tempfile.TemporaryDirectory() as tmp:
        db = _open_db(tmp)
        db.create_fork("agent-01", meta={"run_id": "r-42", "hypothesis": 3})
        info = db.fork_info("agent-01")
        assert info["user_meta"] == {"run_id": "r-42", "hypothesis": 3}


def test_database_wide_operations_refuse_a_fork_handle():
    with tempfile.TemporaryDirectory() as tmp:
        db = _open_db(tmp)
        db.create_fork("agent-01")
        fork = db.fork("agent-01")
        for call in (
            lambda: fork.create_fork("nested"),
            lambda: fork.snapshot("snap"),
            lambda: fork.vacuum(apply=True),
        ):
            with pytest.raises(h5i_db.InvalidInputError):
                call()


def test_closing_a_fork_handle_leaves_the_base_open():
    with tempfile.TemporaryDirectory() as tmp:
        db = _open_db(tmp)
        db.create_fork("agent-01")
        fork = db.fork("agent-01")
        fork.close()
        assert fork.closed
        assert not db.closed
        assert _rows(db) == 5


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
