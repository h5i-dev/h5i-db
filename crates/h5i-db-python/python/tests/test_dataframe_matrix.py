"""Generated coverage for the DataFrame builder (ROADMAP Part VIII).

`test_dataframe.py` holds hand-written cases for specific behaviours. This
file holds the two techniques that actually found bugs, kept as permanent
tests rather than one-off investigations:

* **Expression fuzzing** — random operator trees evaluated twice, once as
  generated SQL and once by a Python reference. This is what caught
  ``a * (b / c)`` rendering as ``a * b / c``.
* **A verb matrix** — every ordered pair and triple of pipeline verbs,
  executed. This is what caught the wrap-rule bugs, including the one where
  ``LIMIT`` landed after the grouping it was supposed to feed.

Both are seeded, so a failure reproduces exactly.

There is also a rejection table pinning the error contract: every input the
builder refuses, and the wording it refuses it with.
"""

from __future__ import annotations

import contextlib
import itertools
import operator
import random
import tempfile

import pyarrow as pa

import h5i_db
from h5i_db import col, count_star, lit, sql_expr, time_bucket, vwap, wavg, when

SCHEMA = pa.schema(
    [
        pa.field("ts", pa.timestamp("us"), nullable=False),
        pa.field("symbol", pa.string()),
        pa.field("price", pa.float64()),
        pa.field("size", pa.int64()),
    ]
)

SECOND = 1_000_000
ROWS = 12


@contextlib.contextmanager
def open_db():
    with tempfile.TemporaryDirectory() as tmp:
        db = h5i_db.Database(f"{tmp}/t.db", create=True)
        db.create_table("trades", SCHEMA, time_column="ts")
        db.create_table("quotes", SCHEMA, time_column="ts")
        batch = pa.table(
            {
                "ts": pa.array(
                    [i * SECOND for i in range(ROWS)], type=pa.timestamp("us")
                ),
                "symbol": [["AAPL", "MSFT", "GOOG"][i % 3] for i in range(ROWS)],
                "price": [100.0 + i for i in range(ROWS)],
                "size": [10 + i for i in range(ROWS)],
            },
            schema=SCHEMA,
        )
        db.append("trades", batch)
        db.append("quotes", batch)
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


# ---------------------------------------------------------------------------
# Expression fuzzing
# ---------------------------------------------------------------------------

def _sql_div(a: int, b: int) -> int:
    """SQL truncates integer division toward zero; Python floors it."""
    quotient = abs(a) // abs(b)
    return quotient if (a >= 0) == (b >= 0) else -quotient


def _sql_mod(a: int, b: int) -> int:
    """The remainder that matches _sql_div, so sign follows the dividend."""
    return a - b * _sql_div(a, b)


# The reference implements SQL's semantics, not Python's. Subtraction makes
# intermediates negative even though the data is positive, which is where the
# two conventions part company.
_INT_OPS = [
    ("+", operator.add),
    ("-", operator.sub),
    ("*", operator.mul),
    ("/", _sql_div),
    ("%", _sql_mod),
]

_FLOAT_OPS = [
    ("+", operator.add),
    ("-", operator.sub),
    ("*", operator.mul),
    ("/", operator.truediv),
]

_EXPR_OPS = {
    "+": operator.add,
    "-": operator.sub,
    "*": operator.mul,
    "/": operator.truediv,
    "%": operator.mod,
}


def _build_tree(rng, depth, column, ops, literal):
    """A random operator tree as (Expr, python evaluator)."""
    if depth == 0 or rng.random() < 0.3:
        if rng.random() < 0.5:
            return col(column), operator.itemgetter(column)
        value = literal(rng)
        return lit(value), (lambda row, v=value: v)
    symbol, apply = rng.choice(ops)
    left, left_fn = _build_tree(rng, depth - 1, column, ops, literal)
    right, right_fn = _build_tree(rng, depth - 1, column, ops, literal)
    expr = _EXPR_OPS[symbol](left, right)
    return expr, (lambda row, f=apply, a=left_fn, b=right_fn: f(a(row), b(row)))


def _usable(evaluator, rows):
    """Reject trees that divide by zero or overflow — noise, not signal."""
    try:
        values = [evaluator(row) for row in rows]
    except (ZeroDivisionError, OverflowError):
        return None
    for value in values:
        if isinstance(value, float) and (value != value or abs(value) > 1e15):
            return None
        if abs(value) > 2**62:
            return None
    return values


def _fuzz(db, rows, column, ops, literal, seed, count, compare):
    rng = random.Random(seed)
    trees = []
    while len(trees) < count:
        expr, evaluator = _build_tree(rng, 3, column, ops, literal)
        expected = _usable(evaluator, rows)
        if expected is None:
            continue
        trees.append((expr, expected))
    # One query for the whole batch: per-expression queries make this test
    # slow enough that it would get run less often, which defeats the point.
    projection = [e.alias(f"v{i}") for i, (e, _) in enumerate(trees)]
    got = (
        db.table("trades").select(col("ts"), *projection).sort("ts").to_arrow()
    )
    for i, (expr, expected) in enumerate(trees):
        actual = got.column(f"v{i}").to_pylist()
        for a, b in zip(actual, expected):
            assert compare(a, b), (
                f"tree {i} disagrees with the reference\n"
                f"SQL: {expr._render(0)}\nactual {a} != expected {b}"
            )
    return len(trees)


def test_fuzz_integer_arithmetic_matches_the_reference():
    """Integer trees, where SQL's truncating division makes grouping visible."""
    with open_db() as db:
        rows = (
            db.table("trades").select("price", "size").sort("ts").to_arrow()
        ).to_pylist()
        total = 0
        for seed in (1, 2, 3, 4):
            total += _fuzz(
                db,
                rows,
                "size",
                _INT_OPS,
                lambda rng: rng.randint(1, 9),
                seed,
                60,
                lambda a, b: a == b,
            )
        assert total >= 240


def test_fuzz_float_arithmetic_matches_the_reference():
    with open_db() as db:
        rows = (
            db.table("trades").select("price", "size").sort("ts").to_arrow()
        ).to_pylist()
        total = 0
        for seed in (11, 12, 13):
            total += _fuzz(
                db,
                rows,
                "price",
                _FLOAT_OPS,
                lambda rng: round(rng.uniform(0.5, 9.5), 3),
                seed,
                60,
                lambda a, b: abs(a - b) <= 1e-6 * max(1.0, abs(b)),
            )
        assert total >= 180


def _bool_tree(rng, depth):
    """A random predicate as (Expr, python evaluator)."""
    if depth == 0 or rng.random() < 0.35:
        column = rng.choice(["price", "size"])
        threshold = (
            round(rng.uniform(100.0, 111.0), 2)
            if column == "price"
            else rng.randint(10, 21)
        )
        symbol, apply = rng.choice(
            [
                ("<", operator.lt),
                ("<=", operator.le),
                (">", operator.gt),
                (">=", operator.ge),
                ("==", operator.eq),
                ("!=", operator.ne),
            ]
        )
        expr = {
            "<": operator.lt,
            "<=": operator.le,
            ">": operator.gt,
            ">=": operator.ge,
            "==": operator.eq,
            "!=": operator.ne,
        }[symbol](col(column), threshold)
        return expr, (
            lambda row, f=apply, c=column, t=threshold: bool(f(row[c], t))
        )
    choice = rng.random()
    if choice < 0.25:
        inner, inner_fn = _bool_tree(rng, depth - 1)
        return ~inner, (lambda row, f=inner_fn: not f(row))
    left, left_fn = _bool_tree(rng, depth - 1)
    right, right_fn = _bool_tree(rng, depth - 1)
    if choice < 0.65:
        return (left & right), (
            lambda row, a=left_fn, b=right_fn: a(row) and b(row)
        )
    return (left | right), (lambda row, a=left_fn, b=right_fn: a(row) or b(row))


def test_fuzz_boolean_predicates_select_the_same_rows():
    with open_db() as db:
        rows = (
            db.table("trades").select("ts", "price", "size").sort("ts").to_arrow()
        ).to_pylist()
        checked = 0
        for seed in (21, 22, 23):
            rng = random.Random(seed)
            for _ in range(40):
                predicate, evaluator = _bool_tree(rng, 3)
                got = (
                    db.table("trades")
                    .filter(predicate)
                    .select("ts")
                    .sort("ts")
                    .to_arrow()
                    .column("ts")
                    .to_pylist()
                )
                want = [r["ts"] for r in rows if evaluator(r)]
                assert got == want, (
                    f"predicate disagrees with the reference\n"
                    f"SQL: {predicate._render(0)}\n{got} != {want}"
                )
                checked += 1
        assert checked == 120


# ---------------------------------------------------------------------------
# Verb matrix
# ---------------------------------------------------------------------------

# Every verb here preserves the four base columns, so every ordering is a
# legitimate query and any failure is the builder's fault, not the caller's.
VERBS = {
    "filter": lambda f: f.filter(col("price") > 102),
    "with_columns": lambda f: f.with_columns(w=col("price") * 2),
    "select": lambda f: f.select("ts", "symbol", "price", "size"),
    "sort": lambda f: f.sort("price", descending=True),
    "limit": lambda f: f.limit(8),
    "head": lambda f: f.head(6),
    "unique": lambda f: f.unique(),
}

FILTER_FLOOR = 102  # what the `filter` verb above enforces


def _check_pipeline(db, names):
    frame = db.table("trades")
    for name in names:
        frame = VERBS[name](frame)
    try:
        table = frame.to_arrow()
    except Exception as err:  # pragma: no cover -- failure path
        raise AssertionError(
            f"{' -> '.join(names)} does not run: {type(err).__name__}: {err}\n"
            f"{frame.sql()}"
        ) from err

    # Every row limit in the pipeline still bounds the result. The
    # limit-after-grouping bug was exactly this invariant being violated.
    caps = [8 if n == "limit" else 6 for n in names if n in ("limit", "head")]
    if caps:
        assert table.num_rows <= min(caps), (
            f"{' -> '.join(names)} returned {table.num_rows} rows despite "
            f"a limit of {min(caps)}\n{frame.sql()}"
        )
    # A filter anywhere in the pipeline still holds at the end, since no verb
    # here adds rows back.
    if "filter" in names and "price" in table.schema.names:
        for value in table.column("price").to_pylist():
            assert value > FILTER_FLOOR, (
                f"{' -> '.join(names)} kept price={value}\n{frame.sql()}"
            )
    if "unique" == names[-1]:
        rows = [tuple(r.values()) for r in table.to_pylist()]
        assert len(rows) == len(set(rows)), f"{' -> '.join(names)} left duplicates"
    return table


def test_every_verb_pair_runs_and_respects_its_limits():
    with open_db() as db:
        for names in itertools.product(VERBS, repeat=2):
            _check_pipeline(db, list(names))


def test_every_verb_triple_runs_and_respects_its_limits():
    with open_db() as db:
        for names in itertools.product(VERBS, repeat=3):
            _check_pipeline(db, list(names))


def test_a_limit_bounds_what_the_next_stage_sees():
    """`.limit(k)` then a reducing verb must feed that verb only k rows.

    Stated as an invariant because the failure mode is a plausible number,
    not an error: grouping all rows and then limiting the *groups* returns a
    result that looks entirely reasonable.
    """
    with open_db() as db:
        for k in (1, 3, 5, 12, 20):
            grouped = (
                db.table("trades")
                .limit(k)
                .group_by("symbol")
                .agg(count_star().alias("n"))
                .to_arrow()
            )
            seen = sum(grouped.column("n").to_pylist())
            assert seen == min(k, ROWS), (k, seen)

            assert db.table("trades").limit(k).unique().to_arrow().num_rows <= k
            assert (
                db.table("trades")
                .limit(k)
                .filter(col("price") > 0)
                .to_arrow()
                .num_rows
                <= k
            )
            assert (
                db.table("trades")
                .limit(k)
                .join(db.table("quotes"), on="ts")
                .to_arrow()
                .num_rows
                <= k
            )


def test_aggregation_after_each_verb():
    """Aggregation reads what the stage before it emitted, in every order."""
    with open_db() as db:
        for name, verb in VERBS.items():
            built = (
                verb(db.table("trades"))
                .group_by("symbol")
                .agg(
                    count_star().alias("n"),
                    col("price").mean().alias("px"),
                )
                .sort("symbol")
            )
            table = built.to_arrow()
            assert table.num_rows >= 1, (name, built.sql())
            if name == "filter":
                assert sum(table.column("n").to_pylist()) == sum(
                    1
                    for p in range(ROWS)
                    if 100.0 + p > FILTER_FLOOR
                ), name


def test_window_expressions_survive_each_verb():
    """A window column must not be recomputed by a later stage."""
    with open_db() as db:
        reference = {
            r["ts"]: r["ma"]
            for r in db.table("trades")
            .with_columns(ma=col("price").rolling_mean(3, order_by="ts"))
            .to_arrow()
            .to_pylist()
        }
        for name, verb in VERBS.items():
            if name in ("select", "unique"):
                continue  # these drop or collapse the added column
            table = (
                verb(
                    db.table("trades").with_columns(
                        ma=col("price").rolling_mean(3, order_by="ts")
                    )
                )
                .to_arrow()
                .to_pylist()
            )
            for row in table:
                assert abs(row["ma"] - reference[row["ts"]]) < 1e-9, (
                    f"{name} changed the window value at {row['ts']}"
                )


# ---------------------------------------------------------------------------
# Operator coverage
# ---------------------------------------------------------------------------


def test_every_scalar_and_aggregate_lowering():
    """Each operator against the SQL it claims to compile to."""
    scalars = [
        (col("price").abs(), "abs(price)"),
        (col("price").log(), "ln(price)"),
        (col("price").log10(), "log10(price)"),
        (col("price").exp() / lit(1e40), "exp(price) / 1e+40"),
        (col("price").sqrt(), "sqrt(price)"),
        (col("price").sign(), "signum(price)"),
        (col("price").round(2), "round(price, 2)"),
        (col("price").floor(), "floor(price)"),
        (col("price").ceil(), "ceil(price)"),
        (col("price").coalesce(lit(0.0)), "coalesce(price, 0.0)"),
        (col("price").greatest(lit(105.0)), "greatest(price, 105.0)"),
        (col("price").least(lit(105.0)), "least(price, 105.0)"),
        (col("size").cast("DOUBLE"), "CAST(size AS DOUBLE)"),
        (-col("price"), "-price"),
        (col("price") % lit(7.0), "price % 7.0"),
        # Plain Python numbers on the left, so the reflected operators run.
        (1.5 + col("price"), "1.5 + price"),
        (200.0 - col("price"), "200.0 - price"),
        (2.0 * col("price"), "2.0 * price"),
        (1000.0 / col("price"), "1000.0 / price"),
        (1000.0 % col("price"), "1000.0 % price"),
        (
            lit(__import__("decimal").Decimal("1.25")) * col("price"),
            "1.25 * price",
        ),
        (
            when(col("price") > 105).then(lit(1)).otherwise(lit(0)),
            "CASE WHEN price > 105 THEN 1 ELSE 0 END",
        ),
        # A when/then chain with no otherwise is SQL's implicit ELSE NULL.
        (
            when(col("price") > 105).then(lit(1)),
            "CASE WHEN price > 105 THEN 1 END",
        ),
        (sql_expr("price + 1"), "price + 1"),
    ]
    predicates = [
        (col("price") != 105.0, "price != 105.0"),
        (col("price") <= 105.0, "price <= 105.0"),
        (col("price") >= 105.0, "price >= 105.0"),
        (col("symbol").like("A%"), "symbol LIKE 'A%'"),
        (col("symbol").not_like("A%"), "symbol NOT LIKE 'A%'"),
        (col("symbol").ilike("a%"), "symbol ILIKE 'a%'"),
        (col("price").is_null(), "price IS NULL"),
        (col("price").is_not_null(), "price IS NOT NULL"),
        (col("price").between(102, 108), "price BETWEEN 102 AND 108"),
        (col("symbol").is_in(["AAPL"]), "symbol IN ('AAPL')"),
        (col("symbol").is_in(col("symbol")), "symbol IN (symbol)"),
        (
            (col("price") > 102) & ~(col("size") < 15),
            "price > 102 AND NOT size < 15",
        ),
        (
            True & (col("price") > 102),
            "TRUE AND price > 102",
        ),
        (
            False | (col("price") > 102),
            "FALSE OR price > 102",
        ),
    ]
    aggregates = [
        (col("price").count(), "count(price)"),
        (col("price").first(), "first_value(price)"),
        (col("price").last(), "last_value(price)"),
        (col("price").first("ts", descending=True), "first_value(price ORDER BY ts DESC)"),
        (wavg(col("size"), col("price")), "wavg(size, price)"),
        (vwap(col("price"), col("size")), "vwap(price, size)"),
        (col("price").quantile(0.9), "percentile_cont(0.9) WITHIN GROUP (ORDER BY price)"),
    ]
    with open_db() as db:
        for expr, golden in scalars + predicates:
            _same(
                db,
                db.table("trades").select(col("ts"), expr.alias("v")).sort("ts"),
                f"SELECT ts, {golden} AS v FROM trades ORDER BY ts",
            )
        for expr, golden in aggregates:
            _same(
                db,
                db.table("trades")
                .group_by("symbol")
                .agg(expr.alias("v"))
                .sort("symbol"),
                f"SELECT symbol, {golden} AS v FROM trades "
                "GROUP BY symbol ORDER BY symbol",
            )


def test_time_bucket_forms():
    with open_db() as db:
        _same(
            db,
            db.table("trades")
            .group_by(time_bucket("1m", col("ts")).alias("bar"))
            .agg(count_star().alias("n"))
            .sort("bar"),
            "SELECT time_bucket('1m', ts) AS bar, count(*) AS n "
            "FROM trades GROUP BY bar ORDER BY bar",
        )
        # A raw INTERVAL via the escape hatch, and an explicit origin.
        _same(
            db,
            db.table("trades")
            .select(
                col("ts"),
                time_bucket(sql_expr("INTERVAL '1 minute'"), col("ts")).alias("b"),
            )
            .sort("ts"),
            "SELECT ts, time_bucket(INTERVAL '1 minute', ts) AS b "
            "FROM trades ORDER BY ts",
        )
        assert "time_bucket('1d', \"ts\", TIMESTAMP" in (
            time_bucket(
                "1d", col("ts"), origin=__import__("datetime").datetime(2000, 1, 3)
            )._render(0)
        )


def test_output_name_is_known_only_when_it_really_is():
    assert col("price").output_name == "price"
    assert col("price").alias("p").output_name == "p"
    assert (col("price") + 1).output_name is None
    # A qualified reference names a side, not an output column, so
    # with_columns cannot guess what to call it.
    assert col("price", relation="l").output_name is None
    err = _raises(
        h5i_db.InvalidInputError,
        _frame().with_columns,
        col("price", relation="l"),
    )
    assert "needs a name" in str(err)


def test_terminal_methods_and_head():
    with open_db() as db:
        assert db.table("trades").head(3).to_arrow().num_rows == 3
        assert repr(col("price").alias("p")).startswith("<Expr")
        assert "AS 'p'" in repr(col("price").alias("p"))
        # to_pandas / to_polars only when the optional dependency is present.
        for name, convert in [
            ("pandas", lambda f: f.to_pandas()),
            ("polars", lambda f: f.to_polars()),
        ]:
            try:
                __import__(name)
            except ImportError:
                continue
            assert len(convert(db.table("trades").limit(2))) == 2


def _same(db, built, sql: str):
    from_builder = built.to_arrow()
    from_sql = db.sql(sql).to_arrow()
    assert from_builder.equals(from_sql), (
        f"\nbuilder SQL:\n{built.sql()}\n\ngolden SQL:\n{sql}\n\n"
        f"builder:\n{from_builder}\n\ngolden:\n{from_sql}"
    )


# ---------------------------------------------------------------------------
# The rejection contract
# ---------------------------------------------------------------------------


def _frame(name="trades", **pin):
    class _NoDb:
        def sql(self, *a, **k):  # pragma: no cover -- must never run
            raise AssertionError("validation must not execute anything")

    if not hasattr(_frame, "_db"):
        _frame._db = _NoDb()
    return h5i_db.LazyFrame._from_table(_frame._db, name, **pin)


REJECTIONS = [
    # (label, callable, fragment expected in message or hint)
    ("nul in literal", lambda: lit("a\x00b"), "NUL"),
    ("nul in identifier", lambda: col("a\x00b"), "NUL"),
    ("empty identifier", lambda: col(""), "empty"),
    ("non-string identifier", lambda: col(7), "string"),
    ("bytes literal", lambda: lit(b"x"), "bytes"),
    ("nan literal", lambda: lit(float("nan")), "no SQL literal form"),
    ("infinite decimal", lambda: lit(__import__("decimal").Decimal("Infinity")), "no SQL literal form"),
    ("zero duration", lambda: col("p").rolling_mean("0m", "ts"), "positive"),
    ("fractional month", lambda: col("p").rolling_mean("1.5mo", "ts"), "whole number"),
    ("unparseable duration", lambda: col("p").rolling_mean("soon", "ts"), "duration"),
    ("duration not a string", lambda: col("p").sum().over(order_by="ts", duration=30), "string"),
    ("bad frame bound", lambda: col("p").sum().over(order_by="ts", rows=(1.5, 0)), "non-negative"),
    ("frame bound triple", lambda: col("p").sum().over(order_by="ts", rows=(1, 2, 3)), "preceding"),
    ("rows and duration", lambda: col("p").sum().over(order_by="ts", rows=2, duration="1m"), "either"),
    ("empty order_by", lambda: col("p").sum().over(order_by=[]), "at least one"),
    ("like non-string", lambda: col("s").like(7), "pattern"),
    ("round non-int", lambda: col("p").round(1.5), "integer"),
    ("quantile out of range", lambda: col("p").quantile(2), "[0, 1]"),
    ("ewma non-numeric alpha", lambda: col("p").ewma("fast", "ts"), "number"),
    ("empty sql_expr", lambda: sql_expr("   "), "non-empty"),
    ("cast injection", lambda: col("p").cast("INT); DROP TABLE t; --"), "sql_expr"),
    ("timezone non-string", lambda: time_bucket("1d", col("ts"), timezone=7), "string"),
    ("when non-expr", lambda: when(True), "boolean Expr"),
    ("bad expr type", lambda: _frame().select(object()), "column name or Expr"),
    ("empty table name", lambda: _frame(""), "non-empty"),
    ("bad version type", lambda: _frame(version=1.5), "integer"),
    ("join non-frame", lambda: _frame().join("quotes", on="ts"), "LazyFrame"),
    ("join predicate and on", lambda: _frame().join(_frame("q"), on="ts", predicate=sql_expr("1=1")), "either"),
    ("join predicate type", lambda: _frame().join(_frame("q"), predicate="1=1"), "Expr"),
    ("join on and left_on", lambda: _frame().join(_frame("q"), on="ts", left_on="ts", right_on="ts"), "either"),
    ("join non-string key", lambda: _frame().join(_frame("q"), on=[1]), "strings"),
    ("asof non-frame", lambda: _frame().join_asof("quotes", on="ts"), "LazyFrame"),
    ("asof on and left_on", lambda: _frame().join_asof(_frame("q"), on="ts", left_on="ts"), "either"),
    ("asof on non-string", lambda: _frame().join_asof(_frame("q"), on=["ts"]), "single column"),
    ("asof missing keys", lambda: _frame().join_asof(_frame("q")), "left_on"),
    ("asof negative tolerance", lambda: _frame().join_asof(_frame("q"), on="ts", tolerance=-1), "negative"),
    ("with_columns duplicate name", lambda: _frame().with_columns(x=col("a"), **{"x ": col("b")}) if False else _frame().with_columns(col("a").alias("x"), col("b").alias("x")), "same name twice"),
]


def test_the_rejection_contract():
    """Every refusal raises InvalidInputError and says something useful."""
    for label, call, fragment in REJECTIONS:
        err = _raises(h5i_db.InvalidInputError, call)
        text = f"{err} {getattr(err, 'hint', '') or ''}"
        assert fragment in text, (label, text)
        assert err.code == "invalid_input", label


def test_frames_from_different_databases_do_not_join():
    with open_db() as first, open_db() as second:
        left, right = first.table("trades"), second.table("trades")
        for call in (
            lambda: left.join(right, on="ts"),
            lambda: left.join_asof(right, on="ts"),
        ):
            err = _raises(h5i_db.InvalidInputError, call)
            assert "different databases" in str(err)


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"ok  {name}")
    print("all matrix tests passed")
