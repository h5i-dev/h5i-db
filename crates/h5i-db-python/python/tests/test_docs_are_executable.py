"""Execute the Python examples in the DataFrame builder documentation.

The Rust ``docs_are_executable`` test only runs lines beginning ``h5i-db``,
so nothing checks the Python fences. This does: every ```python fence on the
builder's manual page is executed against a real database, and every ```sql
fence that follows a Python fence is compared against what that Python
actually compiles to.

That second half is the point. A doc page can claim any lowering it likes;
this asserts the claim.
"""

from __future__ import annotations

import pathlib
import re
import tempfile

import pyarrow as pa

import h5i_db
import h5i_db.dataframe

DOC = (
    pathlib.Path(__file__).resolve().parents[4]
    / "docs-src"
    / "api"
    / "dataframe.md"
)

TRADES = pa.schema(
    [
        pa.field("ts", pa.timestamp("us"), nullable=False),
        pa.field("symbol", pa.string()),
        pa.field("price", pa.float64()),
        pa.field("size", pa.int64()),
    ]
)

BARS = pa.schema(
    [
        pa.field("ts", pa.timestamp("us"), nullable=False),
        pa.field("symbol", pa.string()),
        pa.field("open", pa.float64()),
        pa.field("close", pa.float64()),
    ]
)

SECOND = 1_000_000


def _fences(text: str):
    """(language, body) for every fenced block, in document order."""
    return [
        (m.group(1) or "", m.group(2))
        for m in re.finditer(r"^```(\w*)\n(.*?)^```", text, re.M | re.S)
    ]


# Signature blocks, the convention the other API pages use:
# ``to_arrow() -> pyarrow.Table``. Declarations, not statements.
_SIGNATURE = re.compile(r"^[\w.]+\(.*\)\s*->\s*[\w.\[\]|, \"']+$", re.S)


def _is_signature(body: str) -> bool:
    return bool(_SIGNATURE.match(body.strip()))


def _seed(db):
    db.create_table("trades", TRADES, time_column="ts")
    db.create_table("quotes", TRADES, time_column="ts")
    db.create_table("bars", BARS, time_column="ts")
    for start in (0, 100):
        db.append(
            "trades",
            pa.table(
                {
                    "ts": pa.array(
                        [(start + i) * SECOND for i in range(6)],
                        type=pa.timestamp("us"),
                    ),
                    "symbol": [["AAPL", "MSFT", "GOOG"][i % 3] for i in range(6)],
                    "price": [100.0 + i for i in range(6)],
                    "size": [10 + i for i in range(6)],
                },
                schema=TRADES,
            ),
        )
    db.append(
        "quotes",
        pa.table(
            {
                "ts": pa.array([i * SECOND for i in range(6)], type=pa.timestamp("us")),
                "symbol": [["AAPL", "MSFT", "GOOG"][i % 3] for i in range(6)],
                "price": [100.0 + i for i in range(6)],
                "size": [10 + i for i in range(6)],
            },
            schema=TRADES,
        ),
    )
    db.append(
        "bars",
        pa.table(
            {
                "ts": pa.array([i * SECOND for i in range(6)], type=pa.timestamp("us")),
                "symbol": [["AAPL", "MSFT", "GOOG"][i % 3] for i in range(6)],
                "open": [100.0 + i for i in range(6)],
                "close": [101.0 + i for i in range(6)],
            },
            schema=BARS,
        ),
    )


def _normalise(sql: str) -> str:
    """Compare SQL by tokens: the docs wrap long lines for readability."""
    return " ".join(sql.split())


def test_documented_python_examples_run_and_lower_as_claimed():
    text = DOC.read_text(encoding="utf-8")
    fences = _fences(text)
    runnable = [
        body
        for lang, body in fences
        if lang == "python" and not _is_signature(body)
    ]
    assert len(runnable) >= 8, f"only {len(runnable)} runnable python fences found"

    with tempfile.TemporaryDirectory() as tmp:
        with h5i_db.Database(f"{tmp}/doc.db", create=True) as db:
            _seed(db)
            env = {
                "db": db,
                "t0": 0,
                "t1": 5 * SECOND,
                **{
                    n: getattr(h5i_db.dataframe, n)
                    for n in h5i_db.dataframe.__all__
                },
            }
            checked_sql = 0
            for index, (lang, body) in enumerate(fences):
                if lang != "python" or _is_signature(body):
                    continue
                value = _run(body, env, index)

                # A ```sql fence straight after a ```python one is a claim
                # about that example's lowering. Hold it to the claim.
                following = fences[index + 1] if index + 1 < len(fences) else None
                if following and following[0] == "sql":
                    if isinstance(value, h5i_db.LazyFrame):
                        actual = value.sql()
                    elif isinstance(value, h5i_db.Expr):
                        actual = value._render(0)
                    else:
                        raise AssertionError(
                            f"fence {index} is followed by a SQL claim but "
                            f"evaluates to {type(value).__name__}:\n{body}"
                        )
                    assert _normalise(actual) == _normalise(following[1]), (
                        f"fence {index} does not compile to the SQL shown.\n"
                        f"claimed:\n{following[1]}\nactual:\n{actual}"
                    )
                    checked_sql += 1
            assert checked_sql >= 5, f"only {checked_sql} lowering claims checked"


TERMINALS = {"collect", "to_arrow", "to_pandas", "to_polars", "sql", "explain", "schema"}


def _tolerated(err: Exception) -> bool:
    """The one failure a fresh fixture cannot avoid.

    The ``as_of`` example pins to a date older than any commit a temporary
    database can have. Reaching that error means the query was built and the
    engine resolved the pin, which is exactly what the page claims.
    """
    return isinstance(err, h5i_db.NotFoundError) and (
        getattr(err, "code", None) == "version_not_found"
    )


def _run(body: str, env: dict, index: int):
    """Execute a fence and return the LazyFrame it builds, if any.

    A trailing terminal call is evaluated *and* stripped, so an example
    ending in ``.collect()`` still yields the frame whose lowering the page
    claims.
    """
    import ast

    def fail(err, stage, detail=""):
        raise AssertionError(
            f"documented example {index} {stage}: "
            f"{type(err).__name__}: {err}\n{detail or body}"
        ) from err

    tree = ast.parse(body)
    last = tree.body[-1] if tree.body else None
    trailing_expr = isinstance(last, ast.Expr)
    head = tree.body[:-1] if trailing_expr else tree.body
    try:
        exec(compile(ast.Module(head, []), "<doc>", "exec"), env)
    except Exception as err:
        fail(err, "failed")

    receiver = None
    if trailing_expr:
        # A trailing terminal call is evaluated, but the frame it was called
        # on is what carries the lowering the page claims.
        if (
            isinstance(last.value, ast.Call)
            and isinstance(last.value.func, ast.Attribute)
            and last.value.func.attr in TERMINALS
        ):
            receiver = last.value.func.value
        try:
            frame = eval(
                compile(ast.Expression(receiver or last.value), "<doc>", "eval"), env
            )
        except Exception as err:
            fail(err, "failed")
    elif (
        isinstance(last, ast.Assign)
        and len(last.targets) == 1
        and isinstance(last.targets[0], ast.Name)
    ):
        frame = env.get(last.targets[0].id)
    else:
        return None

    if not isinstance(frame, h5i_db.LazyFrame):
        return frame
    # Run it, so a bad column name or an unplannable query fails here rather
    # than passing on the strength of string comparison alone.
    try:
        frame.limit(5).collect()
    except Exception as err:
        if not _tolerated(err):
            fail(err, "builds but does not run", frame.sql())
    if receiver is not None:
        try:
            eval(compile(ast.Expression(last.value), "<doc>", "eval"), env)
        except Exception as err:
            if not _tolerated(err):
                fail(err, "failed at its terminal call", frame.sql())
    return frame


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"ok  {name}")
    print("all doc tests passed")
