"""Data semantics for the DataFrame builder (ROADMAP Part VIII).

The other two test files use a fixture with one row per timestamp. That is
fine for lowering and for verb composition, but it makes every
cross-sectional operator vacuous: a ``PARTITION BY ts`` bucket of one row
ranks 1.0, demeans to 0.0 and has no sample variance, so those tests would
pass even against a badly broken operator.

This file supplies what a cross-section actually needs -- a **panel**, many
entities sharing each timestamp -- and the other thing the fixtures lacked
entirely: **NULLs**. Expected values come from references written here in
plain Python from the documented specification (`cs_rank` is pandas
``rank(pct=True)`` with ``method="average"``; `cs_zscore` uses sample
stddev, ddof=1), not from the SQL the builder generates, so these check
behaviour rather than self-consistency.
"""

from __future__ import annotations

import contextlib
import math
import tempfile

import pyarrow as pa

import h5i_db
from h5i_db import col, count_star, lit

SCHEMA = pa.schema(
    [
        pa.field("ts", pa.timestamp("us"), nullable=False),
        pa.field("sym", pa.string()),
        pa.field("tag", pa.string()),
        pa.field("factor", pa.float64()),
        pa.field("nf", pa.float64()),
        pa.field("qty", pa.int64()),
    ]
)

SECOND = 1_000_000
SYMBOLS = ["A", "B", "C", "D", "E", "F"]

# One list per timestamp, one entry per symbol. Chosen to exercise the cases
# a cross-section can present: a clean spread, a tie, a heavy outlier, and a
# bucket with no variance at all.
FACTORS = [
    [10.0, 20.0, 30.0, 40.0, 50.0, 60.0],
    [5.0, 5.0, 7.0, 9.0, 11.0, 13.0],
    [-3.0, -1.0, 0.0, 1.0, 3.0, 100.0],
    [2.0, 2.0, 2.0, 2.0, 2.0, 2.0],
    [1.5, 2.5, 3.5, 4.5, 5.5, 6.5],
]

# Which entries of `nf` are NULL: none, one, two, all-but-one, all. The last
# two exist so "excluded from the statistic" is tested where exclusion leaves
# too little to compute anything.
NULL_MASK = [
    [],
    [2],
    [0, 5],
    [0, 1, 2, 3, 4],
    [0, 1, 2, 3, 4, 5],
]

BUCKETS = len(FACTORS)


def _panel_columns():
    ts, sym, tag, factor, nf, qty = [], [], [], [], [], []
    for b, values in enumerate(FACTORS):
        for i, value in enumerate(values):
            ts.append(b * SECOND)
            sym.append(SYMBOLS[i])
            # A nullable string column, for grouping and IN over NULLs.
            tag.append(None if (b + i) % 4 == 0 else f"g{i % 3}")
            factor.append(value)
            nf.append(None if i in NULL_MASK[b] else value)
            qty.append(None if (b == 1 and i == 0) else 10 + i)
    return ts, sym, tag, factor, nf, qty


@contextlib.contextmanager
def open_panel():
    ts, sym, tag, factor, nf, qty = _panel_columns()
    with tempfile.TemporaryDirectory() as tmp:
        db = h5i_db.Database(f"{tmp}/panel.db", create=True)
        db.create_table("panel", SCHEMA, time_column="ts")
        db.append(
            "panel",
            pa.table(
                {
                    "ts": pa.array(ts, type=pa.timestamp("us")),
                    "sym": sym,
                    "tag": tag,
                    "factor": factor,
                    "nf": nf,
                    "qty": qty,
                },
                schema=SCHEMA,
            ),
        )
        try:
            yield db
        finally:
            db.close()


def _raises(exc, fn, *args, **kwargs):
    try:
        fn(*args, **kwargs)
    except exc as caught:
        return caught
    raise AssertionError(f"expected {exc.__name__}")


def _by_bucket(db, column: str, alias: str, expr):
    """Run one expression and regroup the result by timestamp bucket.

    Sorted by (ts, sym) with a full grid, so chunking by symbol count
    recovers the buckets exactly, without converting timestamps to numbers.
    """
    rows = (
        db.table("panel")
        .select(col("ts"), col("sym"), expr.alias(alias))
        .sort(["ts", "sym"])
        .to_arrow()
        .to_pylist()
    )
    width = len(SYMBOLS)
    assert len(rows) == BUCKETS * width
    buckets = [rows[b * width : (b + 1) * width] for b in range(BUCKETS)]
    for bucket in buckets:
        assert len({r["ts"] for r in bucket}) == 1, bucket
        assert [r["sym"] for r in bucket] == SYMBOLS, bucket
    return [[r[alias] for r in bucket] for bucket in buckets]


def _values(column: str, bucket: int):
    return (
        FACTORS[bucket]
        if column == "factor"
        else [
            None if i in NULL_MASK[bucket] else FACTORS[bucket][i]
            for i in range(len(SYMBOLS))
        ]
    )


def _close(a, b, tol=1e-9):
    if a is None or b is None:
        return a is None and b is None
    return abs(a - b) <= tol


# ---------------------------------------------------------------------------
# References, from the documented specification
# ---------------------------------------------------------------------------


def ref_pct_rank(values):
    """pandas ``rank(pct=True)``, method="average"; NULLs excluded."""
    present = [v for v in values if v is not None]
    n = len(present)
    if n == 0:
        return [None] * len(values)
    out = []
    for v in values:
        if v is None:
            out.append(None)
            continue
        less = sum(1 for u in present if u < v)
        tied = sum(1 for u in present if u == v)
        out.append((less + (tied + 1) / 2) / n)
    return out


def ref_demean(values):
    present = [v for v in values if v is not None]
    if not present:
        return [None] * len(values)
    mean = sum(present) / len(present)
    return [None if v is None else v - mean for v in values]


def ref_zscore(values):
    """(x - mean) / sample stddev, ddof=1. Undefined below two observations."""
    present = [v for v in values if v is not None]
    n = len(present)
    if n < 2:
        return [None] * len(values)
    mean = sum(present) / n
    variance = sum((u - mean) ** 2 for u in present) / (n - 1)
    sd = math.sqrt(variance)
    if sd == 0.0:
        return None  # the caller decides; SQL has no one answer here
    return [None if v is None else (v - mean) / sd for v in values]


# ---------------------------------------------------------------------------
# Cross-sectional operators, on an actual cross-section
# ---------------------------------------------------------------------------


def test_the_panel_fixture_is_actually_a_cross_section():
    """Guard the guard: these tests are worthless on one row per bucket."""
    with open_panel() as db:
        rows = db.table("panel").to_arrow().to_pylist()
        per_bucket = {}
        for row in rows:
            per_bucket.setdefault(row["ts"], []).append(row)
        assert len(per_bucket) == BUCKETS
        for ts, group in per_bucket.items():
            assert len(group) == len(SYMBOLS), (ts, len(group))
        # And the operators must not be degenerate on it.
        ranks = _by_bucket(db, "factor", "r", col("factor").cs_rank("ts"))
        assert len(set(ranks[0])) == len(SYMBOLS), ranks[0]


def test_cs_rank_matches_a_percentile_rank_reference():
    with open_panel() as db:
        for column in ("factor", "nf"):
            got = _by_bucket(db, column, "r", col(column).cs_rank("ts"))
            for b in range(BUCKETS):
                want = ref_pct_rank(_values(column, b))
                for a, e in zip(got[b], want):
                    assert _close(a, e), (column, b, got[b], want)


def test_cs_rank_averages_tied_ranks():
    """The tie in bucket 1 is the point: two 5.0s share rank 1.5 of 6."""
    with open_panel() as db:
        got = _by_bucket(db, "factor", "r", col("factor").cs_rank("ts"))[1]
        assert _close(got[0], 1.5 / 6) and _close(got[1], 1.5 / 6), got
        assert _close(got[2], 3 / 6), got
        assert _close(got[5], 1.0), got


def test_cs_demean_and_zscore_match_their_references():
    with open_panel() as db:
        for column in ("factor", "nf"):
            demeaned = _by_bucket(db, column, "d", col(column).cs_demean("ts"))
            zscored = _by_bucket(db, column, "z", col(column).cs_zscore("ts"))
            for b in range(BUCKETS):
                values = _values(column, b)
                for a, e in zip(demeaned[b], ref_demean(values)):
                    assert _close(a, e), ("demean", column, b, demeaned[b])
                want = ref_zscore(values)
                if want is None:
                    continue  # zero variance, asserted separately
                for a, e in zip(zscored[b], want):
                    assert _close(a, e), ("zscore", column, b, zscored[b], want)


def test_cs_zscore_on_a_bucket_with_no_variance():
    """Every value identical: the standard deviation is zero.

    Recorded rather than assumed. Whatever SQL does here, a factor pipeline
    will meet it on a day when every name moves the same amount, so the
    behaviour needs to be pinned rather than discovered in production.
    """
    with open_panel() as db:
        flat = _by_bucket(db, "factor", "z", col("factor").cs_zscore("ts"))[3]
        assert all(v is None or math.isnan(v) for v in flat), flat


def test_cross_sectional_operators_exclude_nulls_from_the_statistic():
    """A missing value must not shift its peers' ranks (zipline's rule)."""
    with open_panel() as db:
        # Bucket 2 has two NULLs; the four survivors must rank among
        # themselves exactly as they would if the NULL rows did not exist.
        got = _by_bucket(db, "nf", "r", col("nf").cs_rank("ts"))[2]
        present = [v for v in _values("nf", 2) if v is not None]
        assert len(present) == 4
        expected = ref_pct_rank(_values("nf", 2))
        for a, e in zip(got, expected):
            assert _close(a, e), (got, expected)
        assert sorted(v for v in got if v is not None) == [0.25, 0.5, 0.75, 1.0]
        # A NULL input is NULL out, never a fabricated rank.
        for value, rank in zip(_values("nf", 2), got):
            assert (value is None) == (rank is None)
        # A bucket with a single observation has no sample variance...
        assert all(v is None for v in _by_bucket(
            db, "nf", "z", col("nf").cs_zscore("ts")
        )[3])
        # ... and an all-NULL bucket yields NULL throughout, not an error.
        assert all(
            v is None
            for v in _by_bucket(db, "nf", "r", col("nf").cs_rank("ts"))[4]
        )


def test_cs_winsorize_clips_the_tails_of_a_real_cross_section():
    """Property-checked, so the test does not restate the implementation."""
    with open_panel() as db:
        clipped = _by_bucket(
            db, "factor", "w", col("factor").cs_winsorize(0.2, 0.8, "ts")
        )
        identity = _by_bucket(
            db, "factor", "w", col("factor").cs_winsorize(0.0, 1.0, "ts")
        )
        for b in range(BUCKETS):
            values = _values("factor", b)
            out = clipped[b]
            assert all(v is not None for v in out), (b, out)
            # Tails are replaced by a surviving value, never invented.
            assert set(out) <= set(values), (b, out, values)
            # Clipping pulls the extremes inward and preserves order.
            assert min(out) >= min(values) and max(out) <= max(values), (b, out)
            for x, y in zip(values, out):
                for x2, y2 in zip(values, out):
                    if x < x2:
                        assert y <= y2, (b, values, out)
            # A full-width band changes nothing.
            for a, e in zip(identity[b], values):
                assert _close(a, e), (b, identity[b], values)
        # The outlier bucket really is clipped, or the property test above
        # would be satisfied by an identity function.
        assert max(clipped[2]) < 100.0, clipped[2]
        # NULLs pass through untouched.
        with_nulls = _by_bucket(
            db, "nf", "w", col("nf").cs_winsorize(0.2, 0.8, "ts")
        )
        for b in range(BUCKETS):
            for value, out in zip(_values("nf", b), with_nulls[b]):
                assert (value is None) == (out is None), (b, with_nulls[b])


def test_a_cross_sectional_pipeline_end_to_end():
    """Rank within each date, keep the top half, average by symbol."""
    with open_panel() as db:
        built = (
            db.table("panel")
            .with_columns(r=col("factor").cs_rank("ts"))
            .filter(col("r") > 0.5)
            .group_by("sym")
            .agg(count_star().alias("n"))
            .sort("sym")
        )
        out = built.to_arrow().to_pylist()
        # Derived, not guessed: the flat bucket ranks every name at 3.5/6, so
        # a "top half" filter keeps all six of them, not three.
        expected = sum(
            1
            for b in range(BUCKETS)
            for r in ref_pct_rank(_values("factor", b))
            if r > 0.5
        )
        assert expected == 18, expected
        assert sum(r["n"] for r in out) == expected, out
        # That count is itself the proof the rank was computed over the whole
        # cross-section: ranking the survivors instead would renormalise, and
        # every bucket would keep about half of what it kept here. The shape
        # of the SQL says the same thing structurally -- the window sits in a
        # subquery, the filter outside it.
        sql = built.sql()
        assert sql.count("SELECT") == 2, sql
        close_subquery = sql.index(') AS "_s1"')
        assert sql.index("cs_rank") < close_subquery, sql
        assert close_subquery < sql.index('WHERE "r"'), sql


# ---------------------------------------------------------------------------
# NULL semantics
# ---------------------------------------------------------------------------


def test_a_predicate_and_its_negation_do_not_partition_the_rows():
    """SQL's three-valued logic, pinned because it surprises DataFrame users.

    ``NOT NULL`` is NULL, so a row whose value is missing satisfies neither
    the predicate nor its negation. Anyone expecting polars' two-valued
    behaviour will lose those rows from both sides without a warning.
    """
    with open_panel() as db:
        total = db.table("panel").to_arrow().num_rows
        kept = db.table("panel").filter(col("nf") > 5).to_arrow().num_rows
        dropped = db.table("panel").filter(~(col("nf") > 5)).to_arrow().num_rows
        missing = db.table("panel").filter(col("nf").is_null()).to_arrow().num_rows
        assert missing == sum(len(m) for m in NULL_MASK) == 14
        assert kept + dropped != total, "the fixture must contain NULLs"
        assert kept + dropped + missing == total
        # is_null and is_not_null do partition, which is the way to be exact.
        present = db.table("panel").filter(col("nf").is_not_null()).to_arrow()
        assert present.num_rows + missing == total


def test_is_in_never_matches_null():
    with open_panel() as db:
        tagged = db.table("panel").filter(col("tag").is_in(["g0", None])).to_arrow()
        assert tagged.num_rows > 0
        assert all(v == "g0" for v in tagged.column("tag").to_pylist())
        # Matching NULLs is what is_null is for.
        both = (
            db.table("panel")
            .filter(col("tag").is_in(["g0"]) | col("tag").is_null())
            .to_arrow()
        )
        assert both.num_rows > tagged.num_rows


def test_aggregates_skip_nulls_and_count_distinguishes_them():
    with open_panel() as db:
        out = (
            db.table("panel")
            .group_by("ts")
            .agg(
                count_star().alias("rows"),
                col("nf").count().alias("present"),
                col("nf").mean().alias("mean"),
                col("nf").sum().alias("total"),
                col("nf").min().alias("lo"),
                col("nf").max().alias("hi"),
            )
            .sort("ts")
            .to_arrow()
            .to_pylist()
        )
        for b, row in enumerate(out):
            values = [v for v in _values("nf", b) if v is not None]
            assert row["rows"] == len(SYMBOLS)
            assert row["present"] == len(values), b
            if values:
                assert _close(row["mean"], sum(values) / len(values)), b
                assert _close(row["total"], sum(values)), b
                assert _close(row["lo"], min(values)), b
                assert _close(row["hi"], max(values)), b
            else:
                # An all-NULL group aggregates to NULL, not to zero.
                assert row["mean"] is None and row["lo"] is None, b
                assert row["total"] is None, b


def test_grouping_keeps_a_null_key_as_its_own_group():
    with open_panel() as db:
        groups = (
            db.table("panel")
            .group_by("tag")
            .agg(count_star().alias("n"))
            .to_arrow()
            .to_pylist()
        )
        keys = [g["tag"] for g in groups]
        assert None in keys, keys
        assert sum(g["n"] for g in groups) == BUCKETS * len(SYMBOLS)


def test_coalesce_and_null_arithmetic():
    with open_panel() as db:
        rows = (
            db.table("panel")
            .select(
                col("nf"),
                col("nf").coalesce(lit(0.0)).alias("filled"),
                (col("nf") + 1).alias("plus"),
                col("qty").alias("qty"),
            )
            .sort(["ts", "sym"])
            .to_arrow()
            .to_pylist()
        )
        for row in rows:
            if row["nf"] is None:
                assert row["filled"] == 0.0
                # Arithmetic on a NULL stays NULL rather than treating it as 0.
                assert row["plus"] is None
            else:
                assert _close(row["filled"], row["nf"])
                assert _close(row["plus"], row["nf"] + 1)


def test_rolling_operators_over_a_column_with_nulls():
    """A rolling mean skips NULLs; it does not treat them as zero."""
    with open_panel() as db:
        rows = (
            db.table("panel")
            .filter(col("sym") == "A")
            .with_columns(m=col("nf").rolling_mean(5, order_by="ts"))
            .sort("ts")
            .to_arrow()
            .to_pylist()
        )
        assert len(rows) == BUCKETS
        seen = []
        for row in rows:
            if row["nf"] is not None:
                seen.append(row["nf"])
            expected = sum(seen) / len(seen) if seen else None
            assert _close(row["m"], expected, 1e-9), (row, seen)


def test_null_ordering_is_stable_across_sort_directions():
    with open_panel() as db:
        ascending = (
            db.table("panel").sort("nf").to_arrow().column("nf").to_pylist()
        )
        descending = (
            db.table("panel")
            .sort("nf", descending=True)
            .to_arrow()
            .column("nf")
            .to_pylist()
        )
        assert sum(v is None for v in ascending) == 14
        assert sum(v is None for v in descending) == 14
        present_asc = [v for v in ascending if v is not None]
        assert present_asc == sorted(present_asc)
        present_desc = [v for v in descending if v is not None]
        assert present_desc == sorted(present_desc, reverse=True)


def test_null_values_survive_a_round_trip_through_the_builder():
    with open_panel() as db:
        out = db.table("panel").select("nf", "qty", "tag").to_arrow()
        assert out.column("nf").null_count == 14
        assert out.column("qty").null_count == 1
        assert out.column("tag").null_count > 0


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"ok  {name}")
    print("all semantics tests passed")
