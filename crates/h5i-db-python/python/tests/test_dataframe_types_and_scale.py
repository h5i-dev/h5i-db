"""Type breadth and scale for the DataFrame builder (ROADMAP Part VIII).

The other builder tests all run on one shape of data: naive microsecond
timestamps, float64, int64 and string, a dozen rows in a single segment.
Two things go unexercised as a result.

**Types.** Timezone-aware timestamps, dates, booleans, decimals and the
narrow numeric types each have their own literal syntax, comparison rules
and promotion behaviour. A builder that renders literals is exactly the
component that can get them wrong.

**Scale.** Segment pruning, spilling under a memory budget, and query
deadlines only appear once there is more than one segment and more than a
screenful of rows. Pruning in particular is a claim the manual makes, and
one the builder could quietly defeat by wrapping queries in subqueries.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import decimal
import re
import tempfile

import pyarrow as pa

import h5i_db
from h5i_db import col, count_star, lit, time_bucket, when

SECOND = 1_000_000
EPOCH = dt.datetime(1970, 1, 1)
UTC = dt.timezone.utc


def _raises(exc, fn, *args, **kwargs):
    try:
        fn(*args, **kwargs)
    except exc as caught:
        return caught
    raise AssertionError(f"expected {exc.__name__}")


@contextlib.contextmanager
def _database(name: str, schema: pa.Schema, data: dict, time_column="ts"):
    with tempfile.TemporaryDirectory() as tmp:
        db = h5i_db.Database(f"{tmp}/{name}.db", create=True)
        db.create_table("t", schema, time_column=time_column)
        db.append("t", pa.table(data, schema=schema))
        try:
            yield db
        finally:
            db.close()


# ---------------------------------------------------------------------------
# Timezone-aware timestamps
# ---------------------------------------------------------------------------

TZ_SCHEMA = pa.schema(
    [
        pa.field("ts", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("v", pa.float64()),
    ]
)

TZ_START = dt.datetime(2026, 7, 1, 12, 0, tzinfo=UTC)


@contextlib.contextmanager
def open_tz(rows: int = 12):
    yield from _tz_rows(rows)


def _tz_rows(rows):
    data = {
        "ts": pa.array(
            [TZ_START + dt.timedelta(minutes=i) for i in range(rows)],
            type=pa.timestamp("us", tz="UTC"),
        ),
        "v": [float(i) for i in range(rows)],
    }
    with _database("tz", TZ_SCHEMA, data) as db:
        yield db


def test_timezone_aware_timestamps_round_trip_and_compare():
    with open_tz() as db:
        out = db.table("t").to_arrow()
        assert out.schema.field("ts").type == pa.timestamp("us", tz="UTC")
        cut = TZ_START + dt.timedelta(minutes=6)
        # An aware literal renders with its offset and compares correctly.
        aware = db.table("t").filter(col("ts") >= cut).to_arrow()
        assert aware.num_rows == 6, aware.num_rows
        assert "+00:00" in (col("ts") >= cut)._render(0)
        # A naive literal is read as the same instant here, since the column
        # is UTC. Pinned so a change in coercion is visible rather than silent.
        naive = db.table("t").filter(col("ts") >= cut.replace(tzinfo=None)).to_arrow()
        assert naive.num_rows == 6, naive.num_rows
        # A non-UTC literal must resolve by instant, not by wall clock.
        east = cut.astimezone(dt.timezone(dt.timedelta(hours=9)))
        assert db.table("t").filter(col("ts") >= east).to_arrow().num_rows == 6


def test_time_bucket_and_range_frames_on_an_aware_column():
    with open_tz() as db:
        buckets = (
            db.table("t")
            .group_by(time_bucket("5m", col("ts")).alias("bar"))
            .agg(count_star().alias("n"))
            .sort("bar")
            .to_arrow()
        )
        assert buckets.num_rows == 3, buckets.to_pylist()
        assert [n for n in buckets.column("n").to_pylist()] == [5, 5, 2]
        assert buckets.schema.field("bar").type == pa.timestamp("us", tz="UTC")
        # Local-time bucketing via the timezone argument.
        local = (
            db.table("t")
            .group_by(
                time_bucket("1d", col("ts"), timezone="America/New_York").alias("d")
            )
            .agg(count_star().alias("n"))
            .to_arrow()
        )
        assert local.num_rows == 1, local.to_pylist()
        # A RANGE frame needs an INTERVAL against a timestamp, so this is the
        # combination most likely to break on an aware column.
        rolled = (
            db.table("t")
            .select(col("ts"), col("v").rolling_mean("2m", order_by="ts").alias("m"))
            .sort("ts")
            .to_arrow()
        )
        assert rolled.num_rows == 12
        # Three-minute window (t-2m .. t) over v = 0,1,2,...
        assert rolled.column("m").to_pylist()[3] == (1 + 2 + 3) / 3


def test_timestamp_units_other_than_microseconds():
    for unit, step in (("ns", 1_000_000_000), ("ms", 1_000)):
        schema = pa.schema(
            [
                pa.field("ts", pa.timestamp(unit), nullable=False),
                pa.field("v", pa.float64()),
            ]
        )
        data = {
            "ts": pa.array([i * step for i in range(6)], type=pa.timestamp(unit)),
            "v": [float(i) for i in range(6)],
        }
        with _database(f"u{unit}", schema, data) as db:
            out = db.table("t").filter(col("ts") > EPOCH).sort("ts").to_arrow()
            assert out.schema.field("ts").type == pa.timestamp(unit), unit
            assert out.num_rows == 5, (unit, out.num_rows)
            assert (
                db.table("t")
                .group_by(time_bucket("1m", col("ts")).alias("b"))
                .agg(count_star().alias("n"))
                .to_arrow()
                .num_rows
                >= 1
            ), unit


# ---------------------------------------------------------------------------
# Dates, booleans, decimals, narrow numerics
# ---------------------------------------------------------------------------

MIXED_SCHEMA = pa.schema(
    [
        pa.field("ts", pa.timestamp("us"), nullable=False),
        pa.field("d", pa.date32()),
        pa.field("b", pa.bool_()),
        pa.field("m", pa.decimal128(18, 4)),
        pa.field("i", pa.int32()),
        pa.field("f", pa.float32()),
    ]
)

FLAGS = [True, False, True, None, False, True]


@contextlib.contextmanager
def open_mixed():
    data = {
        "ts": pa.array([i * SECOND for i in range(6)], type=pa.timestamp("us")),
        "d": pa.array([dt.date(2026, 7, 1 + i) for i in range(6)], type=pa.date32()),
        "b": FLAGS,
        "m": pa.array(
            [decimal.Decimal(f"{10 + i}.5000") for i in range(6)],
            type=pa.decimal128(18, 4),
        ),
        "i": pa.array(list(range(6)), type=pa.int32()),
        "f": pa.array([1.5 * i for i in range(6)], type=pa.float32()),
    }
    with _database("mixed", MIXED_SCHEMA, data) as db:
        yield db


def test_date_columns_compare_and_aggregate():
    with open_mixed() as db:
        assert (col("d") > dt.date(2026, 7, 3))._render(0) == (
            "\"d\" > DATE '2026-07-03'"
        )
        out = db.table("t").filter(col("d") > dt.date(2026, 7, 3)).to_arrow()
        assert out.num_rows == 3, out.to_pylist()
        assert out.schema.field("d").type == pa.date32()
        extremes = (
            db.table("t")
            .group_by(lit(1).alias("k"))
            .agg(col("d").min().alias("lo"), col("d").max().alias("hi"))
            .to_arrow()
            .to_pylist()[0]
        )
        assert extremes["lo"] == dt.date(2026, 7, 1)
        assert extremes["hi"] == dt.date(2026, 7, 6)
        assert (
            db.table("t").group_by("d").agg(count_star().alias("n")).to_arrow().num_rows
            == 6
        )


def test_boolean_columns_are_predicates_in_their_own_right():
    with open_mixed() as db:
        assert db.table("t").filter(col("b")).to_arrow().num_rows == 3
        # NOT NULL is NULL, so the negation drops the missing flag too.
        assert db.table("t").filter(~col("b")).to_arrow().num_rows == 2
        assert db.table("t").filter(col("b") == True).to_arrow().num_rows == 3  # noqa: E712
        assert db.table("t").filter(col("b") == False).to_arrow().num_rows == 2  # noqa: E712
        assert db.table("t").filter(col("b").is_null()).to_arrow().num_rows == 1
        groups = (
            db.table("t")
            .group_by("b")
            .agg(count_star().alias("n"))
            .to_arrow()
            .to_pylist()
        )
        assert {(g["b"], g["n"]) for g in groups} == {(True, 3), (False, 2), (None, 1)}
        # A boolean produced by the builder round-trips as a boolean.
        made = (
            db.table("t")
            .select(when(col("i") > 2).then(lit(True)).otherwise(lit(False)).alias("x"))
            .to_arrow()
        )
        assert made.schema.field("x").type == pa.bool_()
        assert made.column("x").to_pylist() == [False] * 3 + [True] * 3


def test_decimal_columns_keep_their_precision():
    with open_mixed() as db:
        assert (col("m") > decimal.Decimal("12.5"))._render(0) == '"m" > 12.5'
        for threshold in (decimal.Decimal("12.5"), 12.5):
            out = db.table("t").filter(col("m") > threshold).to_arrow()
            assert out.num_rows == 3, (threshold, out.num_rows)
        out = db.table("t").to_arrow()
        assert out.schema.field("m").type == pa.decimal128(18, 4)
        assert out.column("m")[0].as_py() == decimal.Decimal("10.5000")
        totals = (
            db.table("t")
            .group_by(lit(1).alias("k"))
            .agg(col("m").sum().alias("s"), col("m").mean().alias("a"))
            .to_arrow()
        )
        # Summing widens the precision rather than falling back to float.
        assert pa.types.is_decimal(totals.schema.field("s").type)
        values = [decimal.Decimal(f"{10 + i}.5000") for i in range(6)]
        assert totals.column("s")[0].as_py() == sum(values)
        assert decimal.Decimal(str(totals.column("a")[0].as_py())) == sum(values) / 6


def test_narrow_numeric_types_promote_the_way_sql_says():
    """Pinned, because the result type is not the input type.

    A caller who writes ``col("i") * 2`` over an int32 column gets int64
    back, and a rolling mean over float32 comes back double. Neither is
    wrong, but both are worth being explicit about.
    """
    with open_mixed() as db:
        out = (
            db.table("t")
            .select(
                col("i").alias("i"),
                (col("i") * 2).alias("i2"),
                col("f").alias("f"),
                col("f").rolling_mean(3, order_by="ts").alias("fm"),
                col("i").cast("BIGINT").alias("cast"),
            )
            .sort("i")
            .to_arrow()
        )
        assert out.schema.field("i").type == pa.int32()
        assert out.schema.field("i2").type == pa.int64()
        assert out.schema.field("f").type == pa.float32()
        assert out.schema.field("fm").type == pa.float64()
        assert out.schema.field("cast").type == pa.int64()
        assert out.column("i2").to_pylist() == [0, 2, 4, 6, 8, 10]


def test_every_literal_type_survives_a_round_trip():
    """Each literal the builder can render, compared against its own value."""
    with open_mixed() as db:
        cases = [
            ("bool", lit(True), True),
            ("int", lit(7), 7),
            ("float", lit(1.5), 1.5),
            ("string", lit("hi"), "hi"),
            ("date", lit(dt.date(2026, 7, 1)), dt.date(2026, 7, 1)),
            ("datetime", lit(dt.datetime(2026, 7, 1, 12, 30)), dt.datetime(2026, 7, 1, 12, 30)),
            ("decimal", lit(decimal.Decimal("1.25")), 1.25),
            ("null", lit(None), None),
        ]
        projection = [expr.alias(name) for name, expr, _ in cases]
        row = db.table("t").select(*projection).limit(1).to_arrow().to_pylist()[0]
        for name, _, expected in cases:
            got = row[name]
            if isinstance(expected, float):
                assert abs(float(got) - expected) < 1e-9, name
            else:
                assert got == expected, (name, got, expected)


# ---------------------------------------------------------------------------
# Scale: pruning, memory budgets, deadlines
# ---------------------------------------------------------------------------

SCALE_SCHEMA = pa.schema(
    [
        pa.field("ts", pa.timestamp("us"), nullable=False),
        pa.field("sym", pa.string()),
        pa.field("px", pa.float64()),
    ]
)

BATCH = 2000
BATCHES = 8
TOTAL = BATCH * BATCHES


@contextlib.contextmanager
def open_scale():
    """Several appends, so there is more than one segment to prune."""
    with tempfile.TemporaryDirectory() as tmp:
        db = h5i_db.Database(f"{tmp}/scale.db", create=True)
        db.create_table("t", SCALE_SCHEMA, time_column="ts")
        for b in range(BATCHES):
            base = b * BATCH
            db.append(
                "t",
                pa.table(
                    {
                        "ts": pa.array(
                            [(base + i) * 1000 for i in range(BATCH)],
                            type=pa.timestamp("us"),
                        ),
                        "sym": [["A", "B", "C", "D"][i % 4] for i in range(BATCH)],
                        "px": [100.0 + (i % 997) for i in range(BATCH)],
                    },
                    schema=SCALE_SCHEMA,
                ),
            )
        try:
            yield db
        finally:
            db.close()


def _file_groups(frame) -> int:
    """How many segment groups the scan actually opened."""
    plan = frame.explain(analyze=True).to_arrow()
    text = "\n".join(str(v) for v in plan.column(plan.num_columns - 1).to_pylist())
    match = re.search(r"file_groups=\{(\d+) group", text)
    assert match, f"no file_groups in plan:\n{text}"
    return int(match.group(1))


def _cut(micros: int) -> dt.datetime:
    return EPOCH + dt.timedelta(microseconds=micros)


def test_a_time_range_filter_prunes_segments():
    """The manual's claim, checked from Python rather than the CLI."""
    with open_scale() as db:
        full = _file_groups(db.table("t"))
        assert full > 1, full
        narrow = _file_groups(db.table("t").filter(col("ts") < _cut(BATCH * 1000)))
        assert narrow < full, (narrow, full)
        # An unsatisfiable predicate prunes everything, touching no data.
        assert _file_groups(db.table("t").filter(col("px") > 1e9)) == 0
        # Pruning must not cost correctness.
        kept = (
            db.table("t")
            .filter(col("ts") < _cut(BATCH * 1000))
            .select(count_star().alias("n"))
            .to_arrow()
            .column("n")[0]
            .as_py()
        )
        assert kept == BATCH, kept


def test_pruning_survives_the_builders_subquery_wrapping():
    """A wrap must not hide the predicate from the scan.

    The builder inserts subqueries to keep semantics right. If that stopped
    predicates reaching the scan, every wrapped pipeline would quietly read
    the whole table, which is the kind of regression only a plan assertion
    catches.
    """
    with open_scale() as db:
        flat = db.table("t").filter(col("ts") < _cut(BATCH * 1000))
        wrapped = (
            db.table("t")
            .with_columns(scaled=col("px") * 2)
            .filter(col("scaled") > 0)
            .filter(col("ts") < _cut(BATCH * 1000))
        )
        assert wrapped.sql().count("SELECT") > 1, wrapped.sql()
        assert _file_groups(wrapped) == _file_groups(flat)


def test_a_memory_budget_turns_an_overrun_into_a_typed_error():
    with open_scale() as db:
        heavy = db.table("t").sort("px")
        err = _raises(h5i_db.LimitError, heavy.collect, 64 * 1024)
        assert err.code == "limit_exceeded"
        # The same query completes when the budget is realistic.
        assert len(heavy.collect(memory_limit=256 * 1024 * 1024)) == TOTAL

        # And an aggregation is bounded the same way. Group on `ts`, which is
        # unique per row, rather than on a low-cardinality column.
        #
        # The budget is split across DataFusion partitions, and the partition
        # count defaults to the host's CPU count -- so both the per-partition
        # allowance AND the per-partition group state shrink as 1/cores. What
        # decides the outcome is the *ratio* between them, and that ratio is
        # only stable when it is far from 1. Grouping on a 997-value column put
        # it at roughly 1:1, which made this assertion depend on the core count
        # of whatever machine ran it: it passed on 1, 4 and 8 cores and failed
        # on 2 (the standard CI runner size). TOTAL groups puts the requirement
        # an order of magnitude over the budget at any partition count.
        grouped = db.table("t").group_by("ts").agg(count_star().alias("n"))
        _raises(h5i_db.LimitError, grouped.collect, 64 * 1024)
        assert len(grouped.collect(memory_limit=256 * 1024 * 1024)) == TOTAL


def test_a_deadline_cancels_a_query_that_would_not_finish():
    with open_scale() as db:
        # A self cross join is quadratic, so it reliably outlives the
        # deadline instead of racing it.
        runaway = db.table("t").join(db.table("t"), how="cross")
        err = _raises(h5i_db.TimeoutError, runaway.collect, None, 0.5)
        assert err.code == "timeout"
        assert "deadline" in str(err)
        # A generous deadline on a finite query is not disturbed by any of it.
        assert len(db.table("t").sort("px").collect(timeout=60)) == TOTAL


def test_max_rows_stops_at_the_boundary():
    with open_scale() as db:
        assert len(db.table("t").limit(10).collect(max_rows=10)) == 10
        _raises(h5i_db.LimitError, db.table("t").limit(11).collect, None, None, 10)
        err = _raises(h5i_db.LimitError, db.table("t").collect, None, None, 10)
        assert err.code == "limit_exceeded"


def test_results_stay_correct_at_scale():
    with open_scale() as db:
        out = (
            db.table("t")
            .group_by("sym")
            .agg(count_star().alias("n"), col("px").mean().alias("avg"))
            .sort("sym")
            .to_arrow()
            .to_pylist()
        )
        assert [r["sym"] for r in out] == ["A", "B", "C", "D"]
        assert sum(r["n"] for r in out) == TOTAL
        assert all(r["n"] == TOTAL // 4 for r in out), out
        # A window function over every row, checked at both ends.
        ranked = (
            db.table("t")
            .with_columns(rn=count_star().over(order_by="ts", rows=(None, 0)))
            .sort("ts")
            .to_arrow()
        )
        assert ranked.column("rn").to_pylist()[0] == 1
        assert ranked.column("rn").to_pylist()[-1] == TOTAL


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"ok  {name}")
    print("all type and scale tests passed")
