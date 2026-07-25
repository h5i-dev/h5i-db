"""Tests for the lazy DataFrame builder (ROADMAP Part VIII).

Runnable under pytest, or directly (``python test_dataframe.py``) in any
environment with the wheel and pyarrow installed.

Two kinds of test live here. SQL-generation tests build frames against a
stand-in database that raises if it is ever asked to execute, which both keeps
them fast and proves the builder is lazy. Differential tests run a built query
and the hand-written SQL it claims to compile to, and compare Arrow output.
"""

from __future__ import annotations

import ast
import contextlib
import pathlib
import sys
import tempfile

import pyarrow as pa

import h5i_db
from h5i_db import col, count_star, lit, sql_expr, time_bucket, vwap, when

SCHEMA = pa.schema(
    [
        pa.field("ts", pa.timestamp("us"), nullable=False),
        pa.field("symbol", pa.string()),
        pa.field("price", pa.float64()),
        pa.field("size", pa.int64()),
    ]
)

SECOND = 1_000_000  # the ts column is timestamp[us], so raw units are micros


def _batch(start: int, n: int = 12, price_offset: float = 0.0) -> pa.Table:
    return pa.table(
        {
            "ts": pa.array(
                [(start + i) * SECOND for i in range(n)], type=pa.timestamp("us")
            ),
            "symbol": [["AAPL", "MSFT", "GOOG"][i % 3] for i in range(n)],
            "price": [100.0 + i + price_offset for i in range(n)],
            "size": [10 + i for i in range(n)],
        },
        schema=SCHEMA,
    )


@contextlib.contextmanager
def open_db(versions: int = 1):
    """A populated database; each extra version appends a later batch."""
    with tempfile.TemporaryDirectory() as tmp:
        db = h5i_db.Database(f"{tmp}/t.db", create=True)
        db.create_table("trades", SCHEMA, time_column="ts")
        db.create_table("quotes", SCHEMA, time_column="ts")
        for v in range(versions):
            db.append("trades", _batch(v * 100))
        db.append("quotes", _batch(0))
        try:
            yield db
        finally:
            db.close()


class _NoDb:
    """Stand-in database that fails if a frame tries to execute."""

    def sql(self, *args, **kwargs):  # pragma: no cover -- must never run
        raise AssertionError("building a query must not execute anything")


_NODB = _NoDb()


def frame(name: str = "trades", **pin) -> h5i_db.LazyFrame:
    return h5i_db.LazyFrame._from_table(_NODB, name, **pin)


def _raises(exc, fn, *args, **kwargs):
    try:
        fn(*args, **kwargs)
    except exc as caught:
        return caught
    raise AssertionError(f"expected {exc.__name__}")


# ---------------------------------------------------------------------------
# The documented interface compiles to the documented SQL
# ---------------------------------------------------------------------------


def test_flagship_example_compiles_to_expected_sql():
    built = (
        frame("trades", as_of="2026-07-01T00:00:00Z")
        .filter(col("symbol").is_in(["AAPL", "MSFT"]))
        .group_by("symbol")
        .agg(col("price").mean().alias("px"))
    )
    assert built.sql() == (
        'SELECT "symbol", avg("price") AS "px"\n'
        "FROM h5i('trades', '2026-07-01T00:00:00Z')\n"
        "WHERE \"symbol\" IN ('AAPL', 'MSFT')\n"
        'GROUP BY "symbol"'
    ), built.sql()


def test_sql_generation_is_byte_stable():
    def build():
        return (
            frame("trades", version=7)
            .filter(col("price") > 1, col("size") > 2)
            .with_columns(notional=col("price") * col("size"))
            .filter(col("notional") > 100)
            .group_by(time_bucket("5m", col("ts")).alias("bar"))
            .agg(vwap(col("price"), col("size")).alias("v"))
            .sort("bar", descending=True)
            .limit(10)
        )

    assert build().sql() == build().sql()
    # Rebuilding from a shared prefix must not mutate the prefix.
    base = frame("trades").filter(col("price") > 1)
    before = base.sql()
    base.select("symbol")
    base.group_by("symbol").count()
    assert base.sql() == before


def test_unpinned_table_lowers_to_the_bare_name():
    assert frame("trades").sql() == 'SELECT *\nFROM "trades"'


def test_pins_lower_to_the_h5i_table_function():
    assert "FROM h5i('trades', 42)" in frame("trades", version=42).sql()
    assert (
        "FROM h5i('trades', '2026-07-01T00:00:00Z')"
        in frame("trades", as_of="2026-07-01T00:00:00Z").sql()
    )
    assert (
        "FROM h5i('trades', 'eod-2026-07-18')"
        in frame("trades", snapshot="eod-2026-07-18").sql()
    )


def test_conflicting_pins_are_rejected():
    err = _raises(
        h5i_db.InvalidInputError, frame, "trades", version=1, as_of="2026-01-01T00:00:00Z"
    )
    assert "at most one" in str(err)
    assert err.code == "invalid_input"
    _raises(h5i_db.InvalidInputError, frame, "trades", version=-1)
    _raises(h5i_db.InvalidInputError, frame, "trades", version="1")
    _raises(h5i_db.InvalidInputError, frame, "trades", snapshot="")


# ---------------------------------------------------------------------------
# Expression rendering
# ---------------------------------------------------------------------------


def test_precedence_avoids_redundant_parentheses():
    cases = [
        (col("a") + col("b") * col("c"), '"a" + "b" * "c"'),
        ((col("a") + col("b")) * col("c"), '("a" + "b") * "c"'),
        (col("a") - (col("b") - col("c")), '"a" - ("b" - "c")'),
        (col("a") - col("b") - col("c"), '"a" - "b" - "c"'),
        (col("a") / (col("b") * col("c")), '"a" / ("b" * "c")'),
        (-col("a") + col("b"), '-"a" + "b"'),
        ((col("a") > 1) & (col("b") < 2), '"a" > 1 AND "b" < 2'),
        (
            (col("a") > 1) | ((col("b") < 2) & (col("c") == 3)),
            '"a" > 1 OR "b" < 2 AND "c" = 3',
        ),
        (
            ((col("a") > 1) | (col("b") < 2)) & (col("c") == 3),
            '("a" > 1 OR "b" < 2) AND "c" = 3',
        ),
        (~(col("a") > 1), 'NOT "a" > 1'),
        (col("a") + 1, '"a" + 1'),
        (1 - col("a"), '1 - "a"'),
        (2 * col("a"), '2 * "a"'),
    ]
    for expr, expected in cases:
        assert expr._render(0) == expected, (expr._render(0), expected)


def test_literals_render_by_type():
    cases = [
        (None, "NULL"),
        (True, "TRUE"),
        (False, "FALSE"),
        (3, "3"),
        (-3, "-3"),
        (1.5, "1.5"),
        (1e20, "1e+20"),
        ("abc", "'abc'"),
    ]
    for value, expected in cases:
        assert lit(value)._render(0) == expected, value
    import datetime

    assert (
        lit(datetime.date(2026, 7, 1))._render(0) == "DATE '2026-07-01'"
    )
    assert lit(datetime.datetime(2026, 7, 1, 12, 30))._render(0) == (
        "TIMESTAMP '2026-07-01T12:30:00'"
    )
    # NaN and infinity have no SQL literal form and must not be faked.
    _raises(h5i_db.InvalidInputError, lit, float("nan"))
    _raises(h5i_db.InvalidInputError, lit, float("inf"))
    _raises(h5i_db.InvalidInputError, lit, object())


def test_empty_is_in_is_false_not_a_syntax_error():
    assert col("a").is_in([])._render(0) == "FALSE"
    # A bare string is a likely mistake, not a collection of characters.
    _raises(h5i_db.InvalidInputError, col("a").is_in, "AAPL")


def test_none_comparison_points_at_is_null():
    err = _raises(h5i_db.InvalidInputError, lambda: col("a") == None)  # noqa: E711
    assert "is_null" in err.hint
    err = _raises(h5i_db.InvalidInputError, lambda: col("a") != None)  # noqa: E711
    assert "is_not_null" in err.hint


def test_expr_has_no_truth_value():
    err = _raises(TypeError, bool, col("a") > 1)
    assert "&" in str(err)
    # The classic mistake this guards: `(a > 1) and (b < 2)`.
    _raises(TypeError, lambda: (col("a") > 1) and (col("b") < 2))


def test_case_when_chain():
    expr = (
        when(col("a") > 1)
        .then(lit("hi"))
        .when(col("a") > 0)
        .then(lit("mid"))
        .otherwise(lit("lo"))
    )
    assert expr._render(0) == (
        "CASE WHEN \"a\" > 1 THEN 'hi' WHEN \"a\" > 0 THEN 'mid' ELSE 'lo' END"
    )


def test_cast_rejects_arbitrary_text():
    assert col("a").cast("DECIMAL(10, 2)")._render(0) == 'CAST("a" AS DECIMAL(10, 2))'
    err = _raises(h5i_db.InvalidInputError, col("a").cast, "INT); DROP TABLE t; --")
    assert "sql_expr" in err.hint


# ---------------------------------------------------------------------------
# Window / rolling / cross-sectional lowering
# ---------------------------------------------------------------------------


def test_rolling_sugar_generates_the_documented_window_frame():
    expr = col("price").rolling_mean(20, order_by="ts", partition_by="symbol")
    assert expr._render(0) == (
        'avg("price") OVER (PARTITION BY "symbol" ORDER BY "ts" '
        "ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)"
    )


def test_rolling_duration_frame_uses_range_interval():
    expr = col("price").rolling_mean("30m", order_by="ts", partition_by="symbol")
    assert expr._render(0) == (
        'avg("price") OVER (PARTITION BY "symbol" ORDER BY "ts" '
        "RANGE BETWEEN INTERVAL '30 minutes' PRECEDING AND CURRENT ROW)"
    )
    # A fractional duration is normalised down to a whole smaller unit.
    assert "INTERVAL '90 minutes'" in col("p").rolling_mean(
        "1.5h", order_by="ts"
    )._render(0)
    assert "INTERVAL '36 hours'" in col("p").rolling_mean(
        "1.5d", order_by="ts"
    )._render(0)
    # Months have no fixed-size smaller unit, so a fraction is refused.
    _raises(h5i_db.InvalidInputError, col("p").rolling_mean, "1.5mo", "ts")
    _raises(h5i_db.InvalidInputError, col("p").rolling_mean, "30 minutes", "ts")


def test_rolling_operators_lower_to_their_engine_names():
    expected = {
        "rolling_mean": "avg",
        "rolling_sum": "sum",
        "rolling_min": "min",
        "rolling_max": "max",
        "rolling_std": "stddev",
        "rolling_var": "var_samp",
        "rolling_count": "count",
        "rolling_mad": "mad",
        "rolling_skew": "skew",
        "rolling_kurt": "kurt",
        "rolling_rank": "ts_rank",
        "rolling_idxmax": "idxmax",
        "rolling_idxmin": "idxmin",
    }
    for method, fn in expected.items():
        sql = getattr(col("price"), method)(5, order_by="ts")._render(0)
        assert sql.startswith(f'{fn}("price") OVER ('), sql
        assert "ROWS BETWEEN 4 PRECEDING AND CURRENT ROW" in sql


def test_pair_rolling_operators_take_both_arguments():
    sql = col("price").rolling_corr(col("size"), 5, order_by="ts")._render(0)
    assert sql.startswith('ts_corr("price", "size") OVER (')
    sql = col("price").rolling_cov(col("size"), 5, order_by="ts")._render(0)
    assert sql.startswith('ts_cov("price", "size") OVER (')


def test_rolling_requires_an_ordering():
    err = _raises(h5i_db.InvalidInputError, col("p").rolling_mean, 5, None)
    assert "order_by" in str(err)
    _raises(h5i_db.InvalidInputError, col("p").rolling_mean, 0, "ts")
    _raises(h5i_db.InvalidInputError, col("p").rolling_mean, 1.5, "ts")


def test_window_frame_bounds():
    assert "ROWS BETWEEN CURRENT ROW AND CURRENT ROW" in col("p").sum().over(
        order_by="ts", rows=1
    )._render(0)
    assert "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW" in col(
        "p"
    ).sum().over(order_by="ts", rows=(None, 0))._render(0)
    assert "ROWS BETWEEN 2 PRECEDING AND 3 FOLLOWING" in col("p").sum().over(
        order_by="ts", rows=(2, 3)
    )._render(0)
    # A frame without an ordering is meaningless.
    _raises(h5i_db.InvalidInputError, col("p").sum().over, None, None, False, 5)
    _raises(
        h5i_db.InvalidInputError,
        lambda: col("p").sum().over(order_by="ts", rows=5, duration="30m"),
    )


def test_cross_sectional_lowering():
    assert col("f").cs_rank("ts")._render(0) == (
        'cs_rank("f") OVER (PARTITION BY "ts")'
    )
    assert col("f").cs_winsorize(0.05, 0.95, "ts")._render(0) == (
        'cs_winsorize("f", 0.05, 0.95) OVER (PARTITION BY "ts")'
    )
    # cs_demean / cs_zscore are plain SQL: the engine ships no UDF for them.
    assert col("f").cs_demean("ts")._render(0) == (
        '"f" - avg("f") OVER (PARTITION BY "ts")'
    )
    assert col("f").cs_zscore("ts")._render(0) == (
        '("f" - avg("f") OVER (PARTITION BY "ts")) '
        '/ stddev("f") OVER (PARTITION BY "ts")'
    )


def test_cross_sectional_requires_a_bucket():
    err = _raises(h5i_db.InvalidInputError, col("f").cs_rank, None)
    assert "partition_by" in str(err)
    _raises(h5i_db.InvalidInputError, col("f").cs_winsorize, 0.9, 0.1, "ts")
    _raises(h5i_db.InvalidInputError, col("f").cs_winsorize, -0.1, 0.9, "ts")


def test_ewma_validates_alpha_at_build_time():
    assert col("p").ewma(0.06, order_by="ts", partition_by="symbol")._render(0) == (
        'ewma("p", 0.06) OVER (PARTITION BY "symbol" ORDER BY "ts")'
    )
    _raises(h5i_db.InvalidInputError, col("p").ewma, 1.5, "ts")
    _raises(h5i_db.InvalidInputError, col("p").ewma, -0.1, "ts")


def test_time_bucket_rejects_unparseable_intervals():
    assert time_bucket("5m", col("ts"))._render(0) == "time_bucket('5m', \"ts\")"
    assert time_bucket("1d", col("ts"), timezone="America/New_York")._render(0) == (
        "time_bucket('1d', \"ts\", 'America/New_York')"
    )
    err = _raises(h5i_db.InvalidInputError, time_bucket, "5 minutes", col("ts"))
    assert "sql_expr" in err.hint


# ---------------------------------------------------------------------------
# Subquery wrapping: the rules that keep a pipeline correct
# ---------------------------------------------------------------------------


def test_filter_on_a_computed_column_wraps():
    built = frame().with_columns(ret=col("close") / col("open") - 1).filter(
        col("ret") > 0
    )
    assert built.sql() == (
        "SELECT *\n"
        "FROM (\n"
        '  SELECT *, "close" / "open" - 1 AS "ret"\n'
        '  FROM "trades"\n'
        ') AS "_s1"\n'
        'WHERE "ret" > 0'
    ), built.sql()


def test_filter_on_a_base_column_stays_flat():
    built = frame().with_columns(ret=col("close") - 1).filter(col("open") > 0)
    assert built.sql().count("SELECT") == 1, built.sql()
    assert built.sql() == (
        'SELECT *, "close" - 1 AS "ret"\n'
        'FROM "trades"\n'
        'WHERE "open" > 0'
    )


def test_independent_with_columns_do_not_nest():
    built = frame().with_columns(a=col("x") + 1).with_columns(b=col("y") + 2)
    assert built.sql().count("SELECT") == 1, built.sql()
    # ... but a dependent one does.
    built = frame().with_columns(a=col("x") + 1).with_columns(b=col("a") + 2)
    assert built.sql().count("SELECT") == 2, built.sql()


def test_aggregation_limit_and_distinct_close_a_level():
    agg = frame().group_by("symbol").agg(col("price").mean().alias("px"))
    assert agg.filter(col("px") > 1).sql().count("SELECT") == 2
    assert frame().limit(5).filter(col("a") > 1).sql().count("SELECT") == 2
    assert frame().select("a").unique().filter(col("a") > 1).sql().count("SELECT") == 2
    # Consecutive limits compose rather than overwrite.
    assert frame().limit(10).limit(3).sql().count("SELECT") == 2


def test_opaque_sql_expr_forces_a_wrap_only_when_aliases_exist():
    # No computed names in scope: the fragment resolves against the FROM.
    assert frame().select("a").filter(sql_expr('"b" > 1')).sql().count("SELECT") == 1
    # A computed name exists and the fragment might reach for it, so wrap.
    built = frame().with_columns(r=col("x") + 1).filter(sql_expr('"r" > 1'))
    assert built.sql().count("SELECT") == 2, built.sql()


def test_select_after_select_resolves_against_the_previous_output():
    assert frame().select(col("a").alias("b")).select("b").sql().count("SELECT") == 2
    # Selecting a passthrough base column needs no extra level.
    assert frame().select("a", "b").select("a").sql().count("SELECT") == 1


def test_sort_and_group_by_reference_aliases():
    built = (
        frame()
        .group_by(time_bucket("5m", col("ts")).alias("bar"))
        .agg(col("price").max().alias("hi"))
        .sort("bar", descending=True)
    )
    assert 'GROUP BY "bar"' in built.sql()
    assert 'ORDER BY "bar" DESC' in built.sql()


def test_multi_key_sort_directions():
    built = frame().sort(["a", "b"], descending=[False, True])
    assert 'ORDER BY "a", "b" DESC' in built.sql()
    _raises(h5i_db.InvalidInputError, frame().sort, ["a", "b"], [True])


# ---------------------------------------------------------------------------
# Joins
# ---------------------------------------------------------------------------


def test_join_renders_both_sides_as_aliased_subqueries():
    built = frame("trades").join(frame("quotes"), on="ts")
    assert built.sql() == (
        "SELECT *\n"
        "FROM (\n"
        "  SELECT *\n"
        '  FROM "trades"\n'
        ') AS "l"\n'
        "INNER JOIN (\n"
        "  SELECT *\n"
        '  FROM "quotes"\n'
        ') AS "r"\n'
        '  ON "l"."ts" = "r"."ts"'
    ), built.sql()


def test_join_kinds_and_keys():
    assert "LEFT JOIN" in frame().join(frame("quotes"), on="ts", how="left").sql()
    assert "CROSS JOIN" in frame().join(frame("quotes"), how="cross").sql()
    sql = frame().join(frame("quotes"), left_on=["a", "b"], right_on=["c", "d"]).sql()
    assert '"l"."a" = "r"."c" AND "l"."b" = "r"."d"' in sql
    assert 'ON l."x" = r."y"' in frame().join(
        frame("quotes"), predicate=sql_expr('l."x" = r."y"')
    ).sql()


def test_unknown_join_type_is_rejected_with_a_suggestion():
    err = _raises(
        h5i_db.InvalidInputError, frame().join, frame("quotes"), on="ts", how="lefty"
    )
    assert "left" in str(err.hint), err.hint
    _raises(
        h5i_db.InvalidInputError, frame().join, frame("quotes"), how="cross", on="ts"
    )
    _raises(h5i_db.InvalidInputError, frame().join, frame("quotes"))


def test_join_asof_lowers_to_the_table_function():
    assert frame("trades").join_asof(frame("quotes"), on="ts").sql() == (
        "SELECT *\nFROM asof_join('trades', 'quotes', 'ts', 'ts')"
    )
    assert "'ts', 'ts', 'symbol'" in frame("trades").join_asof(
        frame("quotes"), on="ts", by="symbol"
    ).sql()
    assert "'symbol,venue', 'forward'" in frame("trades").join_asof(
        frame("quotes"), on="ts", by=["symbol", "venue"], direction="forward"
    ).sql()
    # A tolerance needs the positional by_cols slot filled; empty is ignored.
    assert "'ts', 'ts', '', 'backward', 5000000" in frame("trades").join_asof(
        frame("quotes"), on="ts", tolerance=5 * SECOND
    ).sql()
    assert "'lts', 'rts'" in frame("trades").join_asof(
        frame("quotes"), left_on="lts", right_on="rts"
    ).sql()


def test_join_asof_refuses_a_pin_it_cannot_honour():
    err = _raises(
        h5i_db.InvalidInputError,
        frame("trades", version=3).join_asof,
        frame("quotes"),
        on="ts",
    )
    assert "silently ignored" in str(err)
    err = _raises(
        h5i_db.InvalidInputError,
        frame("trades").join_asof,
        frame("quotes", as_of="2026-01-01T00:00:00Z"),
        on="ts",
    )
    assert "right-side pin" in str(err)


def test_join_asof_refuses_a_side_that_is_no_longer_a_plain_table():
    err = _raises(
        h5i_db.InvalidInputError,
        frame("trades").filter(col("price") > 1).join_asof,
        frame("quotes"),
        on="ts",
    )
    assert "plain table on the left side" in str(err)
    _raises(
        h5i_db.InvalidInputError,
        frame("trades").join_asof,
        frame("quotes").select("ts"),
        on="ts",
    )


def test_join_asof_argument_validation():
    _raises(
        h5i_db.InvalidInputError,
        frame().join_asof,
        frame("quotes"),
        on="ts",
        direction="sideways",
    )
    _raises(
        h5i_db.InvalidInputError,
        frame().join_asof,
        frame("quotes"),
        on="ts",
        tolerance=1.5,
    )
    _raises(
        h5i_db.InvalidInputError,
        frame().join_asof,
        frame("quotes"),
        on="ts",
        by="a,b",
    )


# ---------------------------------------------------------------------------
# Quoting: identifiers and literals are structurally safe
# ---------------------------------------------------------------------------

ADVERSARIAL_NAMES = [
    "Symbol",              # case is preserved, unlike bare SQL identifiers
    "select",              # reserved word
    "group by",            # spaces and a keyword
    'he said "hi"',        # embedded double quote
    "it's",                # embedded single quote
    "price; DROP TABLE trades; --",
    "日本語",
    "a.b",                 # a dot is part of the name, not a qualifier
    "col--comment",
]

ADVERSARIAL_VALUES = [
    "O'Brien",
    'a "quoted" value',
    "'; DROP TABLE trades; --",
    "back\\slash",
    "new\nline",
    "100%",
    "日本語",
    "",
]


def test_adversarial_identifiers_round_trip():
    schema = pa.schema(
        [pa.field("ts", pa.timestamp("us"), nullable=False)]
        + [pa.field(name, pa.int64()) for name in ADVERSARIAL_NAMES]
    )
    with tempfile.TemporaryDirectory() as tmp:
        with h5i_db.Database(f"{tmp}/t.db", create=True) as db:
            db.create_table("weird", schema, time_column="ts")
            data = {"ts": pa.array([0, SECOND], type=pa.timestamp("us"))}
            for i, name in enumerate(ADVERSARIAL_NAMES):
                data[name] = [i, i + 100]
            db.append("weird", pa.table(data, schema=schema))

            for i, name in enumerate(ADVERSARIAL_NAMES):
                out = (
                    db.table("weird")
                    .filter(col(name) == i)
                    .select(col(name).alias("v"))
                    .to_arrow()
                )
                assert out.num_rows == 1, (name, out)
                assert out.column("v")[0].as_py() == i, name

            # Every name survives as a projected alias too.
            out = db.table("weird").select(
                *[col(n).alias(n) for n in ADVERSARIAL_NAMES]
            ).to_arrow()
            assert out.schema.names == ADVERSARIAL_NAMES, out.schema.names

            # The table is still there: none of those names ran as SQL.
            assert "weird" in db.tables()


def test_adversarial_string_literals_round_trip():
    schema = pa.schema(
        [
            pa.field("ts", pa.timestamp("us"), nullable=False),
            pa.field("v", pa.string()),
        ]
    )
    with tempfile.TemporaryDirectory() as tmp:
        with h5i_db.Database(f"{tmp}/t.db", create=True) as db:
            db.create_table("vals", schema, time_column="ts")
            db.append(
                "vals",
                pa.table(
                    {
                        "ts": pa.array(
                            [i * SECOND for i in range(len(ADVERSARIAL_VALUES))],
                            type=pa.timestamp("us"),
                        ),
                        "v": ADVERSARIAL_VALUES,
                    },
                    schema=schema,
                ),
            )
            for value in ADVERSARIAL_VALUES:
                out = db.table("vals").filter(col("v") == value).to_arrow()
                assert out.num_rows == 1, (value, out.num_rows)
                assert out.column("v")[0].as_py() == value
            # is_in carries the same escaping.
            out = db.table("vals").filter(col("v").is_in(ADVERSARIAL_VALUES)).to_arrow()
            assert out.num_rows == len(ADVERSARIAL_VALUES)
            assert db.table("vals").to_arrow().num_rows == len(ADVERSARIAL_VALUES)


def test_quoting_helpers_double_the_delimiter():
    assert h5i_db.dataframe.quote_ident('a"b') == '"a""b"'
    assert h5i_db.dataframe.quote_string("a'b") == "'a''b'"
    _raises(h5i_db.InvalidInputError, h5i_db.dataframe.quote_ident, "")
    _raises(h5i_db.InvalidInputError, h5i_db.dataframe.quote_ident, "a\x00b")
    _raises(h5i_db.InvalidInputError, h5i_db.dataframe.quote_ident, 7)


# ---------------------------------------------------------------------------
# Differential: the builder and the SQL it claims to compile to agree
# ---------------------------------------------------------------------------


def _same(db, built, sql: str):
    from_builder = built.to_arrow()
    from_sql = db.sql(sql).to_arrow()
    assert from_builder.equals(from_sql), (
        f"\nbuilder SQL:\n{built.sql()}\n\ngolden SQL:\n{sql}\n\n"
        f"builder:\n{from_builder}\n\ngolden:\n{from_sql}"
    )
    return from_builder


def test_differential_group_by_agg():
    with open_db() as db:
        _same(
            db,
            db.table("trades")
            .filter(col("symbol").is_in(["AAPL", "MSFT"]))
            .group_by("symbol")
            .agg(col("price").mean().alias("px"))
            .sort("symbol"),
            "SELECT symbol, avg(price) AS px FROM trades "
            "WHERE symbol IN ('AAPL', 'MSFT') GROUP BY symbol ORDER BY symbol",
        )


def test_differential_ohlcv_query():
    with open_db() as db:
        built = (
            db.table("trades")
            .group_by(time_bucket("5m", col("ts")).alias("bar"), "symbol")
            .agg(
                col("price").first("ts").alias("open"),
                col("price").max().alias("high"),
                col("price").min().alias("low"),
                col("price").last("ts").alias("close"),
                col("size").sum().alias("volume"),
                vwap(col("price"), col("size")).alias("vwap"),
            )
            .sort(["bar", "symbol"])
        )
        _same(
            db,
            built,
            "SELECT time_bucket('5m', ts) AS bar, symbol, "
            "first_value(price ORDER BY ts) AS open, max(price) AS high, "
            "min(price) AS low, last_value(price ORDER BY ts) AS close, "
            "sum(size) AS volume, vwap(price, size) AS vwap "
            "FROM trades GROUP BY bar, symbol ORDER BY bar, symbol",
        )


def test_differential_filter_project_sort_limit():
    with open_db() as db:
        _same(
            db,
            db.table("trades")
            .filter(col("price") > 103, col("size").between(12, 20))
            .with_columns(notional=col("price") * col("size"))
            .select("symbol", "notional")
            .sort("notional", descending=True)
            .limit(3),
            "SELECT symbol, notional FROM ("
            "  SELECT *, price * size AS notional FROM trades"
            "  WHERE price > 103 AND size BETWEEN 12 AND 20) "
            "ORDER BY notional DESC LIMIT 3",
        )


def test_differential_window_operators():
    with open_db() as db:
        cases = [
            (
                col("price").rolling_mean(3, "ts", "symbol").alias("v"),
                "avg(price) OVER (PARTITION BY symbol ORDER BY ts "
                "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS v",
            ),
            (
                col("price").rolling_std(4, "ts", "symbol").alias("v"),
                "stddev(price) OVER (PARTITION BY symbol ORDER BY ts "
                "ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS v",
            ),
            (
                col("price").rolling_rank(4, "ts", "symbol").alias("v"),
                "ts_rank(price) OVER (PARTITION BY symbol ORDER BY ts "
                "ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS v",
            ),
            (
                col("price").rolling_idxmax(4, "ts", "symbol").alias("v"),
                "idxmax(price) OVER (PARTITION BY symbol ORDER BY ts "
                "ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS v",
            ),
            (
                col("price").rolling_mad(4, "ts", "symbol").alias("v"),
                "mad(price) OVER (PARTITION BY symbol ORDER BY ts "
                "ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS v",
            ),
            (
                col("price").rolling_corr(col("size"), 4, "ts", "symbol").alias("v"),
                "ts_corr(price, size) OVER (PARTITION BY symbol ORDER BY ts "
                "ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS v",
            ),
            (
                col("price").rolling_mean("30m", "ts", "symbol").alias("v"),
                "avg(price) OVER (PARTITION BY symbol ORDER BY ts "
                "RANGE BETWEEN INTERVAL '30 minutes' PRECEDING AND CURRENT ROW) AS v",
            ),
            (
                col("price").ewma(0.5, "ts", "symbol").alias("v"),
                "ewma(price, 0.5) OVER (PARTITION BY symbol ORDER BY ts) AS v",
            ),
            (
                col("price").cs_rank("ts").alias("v"),
                "cs_rank(price) OVER (PARTITION BY ts) AS v",
            ),
            (
                col("price").cs_winsorize(0.1, 0.9, "ts").alias("v"),
                "cs_winsorize(price, 0.1, 0.9) OVER (PARTITION BY ts) AS v",
            ),
            (
                col("price").cs_zscore("ts").alias("v"),
                "(price - avg(price) OVER (PARTITION BY ts)) "
                "/ stddev(price) OVER (PARTITION BY ts) AS v",
            ),
        ]
        for expr, golden in cases:
            _same(
                db,
                db.table("trades").select(col("ts"), expr).sort("ts"),
                f"SELECT ts, {golden} FROM trades ORDER BY ts",
            )


def test_differential_scalar_and_aggregate_functions():
    with open_db() as db:
        _same(
            db,
            db.table("trades")
            .group_by("symbol")
            .agg(
                count_star().alias("n"),
                col("price").sum().alias("s"),
                col("price").min().alias("lo"),
                col("price").max().alias("hi"),
                col("price").median().alias("med"),
                col("price").quantile(0.25).alias("q1"),
                col("price").std().alias("sd"),
                col("price").var().alias("vr"),
                col("size").n_unique().alias("u"),
            )
            .sort("symbol"),
            "SELECT symbol, count(*) AS n, sum(price) AS s, min(price) AS lo, "
            "max(price) AS hi, median(price) AS med, "
            "percentile_cont(0.25) WITHIN GROUP (ORDER BY price) AS q1, "
            "stddev(price) AS sd, var_samp(price) AS vr, "
            "count(DISTINCT size) AS u FROM trades GROUP BY symbol ORDER BY symbol",
        )
        _same(
            db,
            db.table("trades")
            .select(
                col("price").abs().alias("a"),
                col("price").log().alias("l"),
                col("price").sqrt().alias("q"),
                col("price").round(1).alias("r"),
                col("price").floor().alias("f"),
                col("size").cast("DOUBLE").alias("c"),
                when(col("price") > 105).then(lit(1)).otherwise(lit(0)).alias("w"),
            )
            .sort("a"),
            "SELECT abs(price) AS a, ln(price) AS l, sqrt(price) AS q, "
            "round(price, 1) AS r, floor(price) AS f, "
            "CAST(size AS DOUBLE) AS c, "
            "CASE WHEN price > 105 THEN 1 ELSE 0 END AS w "
            "FROM trades ORDER BY a",
        )


def test_differential_distinct_and_offset():
    with open_db() as db:
        _same(
            db,
            db.table("trades").select("symbol").unique().sort("symbol"),
            "SELECT DISTINCT symbol FROM trades ORDER BY symbol",
        )
        _same(
            db,
            db.table("trades").sort("ts").limit(3, offset=4),
            "SELECT * FROM trades ORDER BY ts LIMIT 3 OFFSET 4",
        )


def test_differential_joins():
    with open_db() as db:
        _same(
            db,
            db.table("trades")
            .join(db.table("quotes"), on="ts")
            .select(
                col("ts", relation="l").alias("ts"),
                col("price", relation="l").alias("tp"),
                col("price", relation="r").alias("qp"),
            )
            .sort("ts"),
            "SELECT l.ts AS ts, l.price AS tp, r.price AS qp "
            "FROM trades l JOIN quotes r ON l.ts = r.ts ORDER BY ts",
        )
        _same(
            db,
            db.table("trades").join_asof(db.table("quotes"), on="ts", by="symbol"),
            "SELECT * FROM asof_join('trades', 'quotes', 'ts', 'ts', 'symbol')",
        )


def test_join_asof_stays_one_to_one_with_the_left_side():
    with open_db() as db:
        left = db.table("trades").to_arrow().num_rows
        joined = db.table("trades").join_asof(db.table("quotes"), on="ts").to_arrow()
        assert joined.num_rows == left


# ---------------------------------------------------------------------------
# Version pins reach the engine
# ---------------------------------------------------------------------------


def test_pinned_frame_reads_the_pinned_version():
    with open_db(versions=3) as db:
        assert db.table("trades").to_arrow().num_rows == 36
        for version, expected in [(1, 12), (2, 24), (3, 36)]:
            # Ordered on both sides: an unordered SELECT has no guaranteed
            # row order, and the pinned and session-bound scans need not
            # return segments in the same sequence.
            built = db.table("trades", version=version).sort("ts").to_arrow()
            assert built.num_rows == expected, version
            golden = db.sql(
                f"SELECT * FROM h5i('trades', {version}) ORDER BY ts"
            ).to_arrow()
            assert built.equals(golden), version


def test_pin_survives_the_whole_pipeline():
    with open_db(versions=3) as db:
        built = (
            db.table("trades", version=1)
            .filter(col("price") > 100)
            .group_by("symbol")
            .agg(count_star().alias("n"))
            .sort("symbol")
        )
        _same(
            db,
            built,
            "SELECT symbol, count(*) AS n FROM h5i('trades', 1) "
            "WHERE price > 100 GROUP BY symbol ORDER BY symbol",
        )
        # The pin is doing real work: the same query at head differs.
        at_head = (
            db.table("trades")
            .filter(col("price") > 100)
            .group_by("symbol")
            .agg(count_star().alias("n"))
            .sort("symbol")
            .to_arrow()
        )
        assert not at_head.equals(built.to_arrow())


def test_snapshot_pin_reads_the_snapshot():
    with open_db(versions=1) as db:
        db.snapshot("eod", tables=["trades"])
        db.append("trades", _batch(500))
        assert db.table("trades").to_arrow().num_rows == 24
        pinned = db.table("trades", snapshot="eod").to_arrow()
        assert pinned.num_rows == 12
        assert pinned.equals(db.sql("SELECT * FROM h5i('trades', 'eod')").to_arrow())


def test_the_same_table_at_two_versions_joins():
    """The Part V "one query across N pinned versions" surface."""
    with open_db(versions=1) as db:
        # The second version adds richer bars, so the per-symbol mean moves.
        db.append("trades", _batch(500, price_offset=50.0))
        old = db.table("trades", version=1).group_by("symbol").agg(
            col("price").mean().alias("px")
        )
        new = db.table("trades", version=2).group_by("symbol").agg(
            col("price").mean().alias("px")
        )
        drift = (
            old.join(new, on="symbol")
            .select(
                col("symbol", relation="l").alias("symbol"),
                (col("px", relation="r") - col("px", relation="l")).alias("drift"),
            )
            .sort("symbol")
        )
        out = drift.to_arrow()
        assert out.num_rows == 3
        assert all(v.as_py() > 0 for v in out.column("drift"))
        # Both pins survive into the generated SQL.
        assert "h5i('trades', 1)" in drift.sql()
        assert "h5i('trades', 2)" in drift.sql()


# ---------------------------------------------------------------------------
# Terminal methods
# ---------------------------------------------------------------------------


def test_collect_returns_a_query_result_and_converters_agree():
    with open_db() as db:
        built = db.table("trades").select("symbol", "price").sort("price")
        result = built.collect()
        assert isinstance(result, h5i_db.QueryResult)
        assert len(result) == 12
        assert built.to_arrow().equals(result.to_arrow())
        try:
            import pandas  # noqa: F401
        except ImportError:
            return  # pandas is not a dependency of the base wheel
        assert list(built.to_pandas().columns) == ["symbol", "price"]


def test_collect_passes_limits_through():
    with open_db() as db:
        _raises(h5i_db.LimitError, db.table("trades").collect, None, None, 3)
        _raises(
            h5i_db.LimitError, db.table("trades").to_arrow, max_rows=3
        )


def test_schema_needs_no_rows():
    with open_db() as db:
        schema = (
            db.table("trades")
            .group_by("symbol")
            .agg(col("price").mean().alias("px"))
            .schema()
        )
        assert schema.names == ["symbol", "px"]
        assert schema.field("px").type == pa.float64()
        assert schema.field("symbol").type == pa.string()


def test_explain_reports_a_plan():
    with open_db() as db:
        plan = db.table("trades").filter(col("price") > 100).explain().to_arrow()
        assert plan.num_rows > 0
        text = "\n".join(str(v) for v in plan.column(plan.num_columns - 1))
        assert "trades" in text
        assert db.table("trades").explain(analyze=True).to_arrow().num_rows > 0


def test_repr_shows_the_generated_sql():
    text = repr(frame("trades").filter(col("price") > 1))
    assert "LazyFrame" in text
    assert 'FROM "trades"' in text


def test_pipe_composes_reusable_helpers():
    def top(f, n):
        return f.sort("price", descending=True).limit(n)

    with open_db() as db:
        out = db.table("trades").pipe(top, 2).to_arrow()
        assert out.num_rows == 2


def test_verbs_reject_empty_input():
    for call in (
        lambda: frame().filter(),
        lambda: frame().select(),
        lambda: frame().with_columns(),
        lambda: frame().group_by(),
        lambda: frame().group_by("symbol").agg(),
    ):
        _raises(h5i_db.InvalidInputError, call)
    _raises(h5i_db.InvalidInputError, frame().limit, -1)
    _raises(h5i_db.InvalidInputError, frame().limit, 5, -1)
    # An aliased predicate is a sign of a misplaced .alias().
    _raises(h5i_db.InvalidInputError, frame().filter, (col("a") > 1).alias("x"))
    # with_columns needs a name it can project under.
    _raises(h5i_db.InvalidInputError, frame().with_columns, col("a") + 1)


def test_unknown_operators_fail_at_build_time():
    # Not an engine parse error at collect() time: a plain AttributeError,
    # which is what makes autocomplete and type checkers useful here.
    _raises(AttributeError, lambda: col("price").rolling_median)
    _raises(AttributeError, lambda: frame().groupby)


# ---------------------------------------------------------------------------
# Packaging
# ---------------------------------------------------------------------------


def test_builder_adds_no_third_party_dependency():
    """The base wheel stays pyarrow-only (ROADMAP VII-D packaging rule)."""
    source = pathlib.Path(h5i_db.dataframe.__file__).read_text(encoding="utf-8")
    imported = set()
    for node in ast.parse(source).body:
        if isinstance(node, ast.Import):
            imported.update(a.name.split(".")[0] for a in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            imported.add(node.module.split(".")[0])
    allowed = set(sys.stdlib_module_names) | {"h5i_db", "__future__"}
    assert imported <= allowed, imported - allowed


def test_public_names_are_exported():
    for name in [
        "LazyFrame",
        "GroupBy",
        "Expr",
        "col",
        "lit",
        "sql_expr",
        "count_star",
        "when",
        "time_bucket",
        "vwap",
        "wavg",
    ]:
        assert name in h5i_db.__all__, name
        assert hasattr(h5i_db, name), name


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"ok  {name}")
    print("all dataframe tests passed")
