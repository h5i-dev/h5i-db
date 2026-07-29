"""Execute every Python example on the quant manual page.

Same principle as ``test_docs_are_executable``: a doc page can claim
anything, so the claims are run. Each ```python fence on
``docs-src/manual/quant.md`` is executed in order against a real database
holding the tables the page talks about.
"""

from __future__ import annotations

import datetime as dt
import pathlib
import re
import tempfile

import numpy as np
import pyarrow as pa
import pytest

import h5i_db
from h5i_db import quant

DOC = (
    pathlib.Path(__file__).resolve().parents[4] / "docs-src" / "manual" / "quant.md"
)

PRICE_SCHEMA = pa.schema(
    [
        pa.field("ts", pa.timestamp("us"), nullable=False),
        pa.field("asset", pa.string()),
        pa.field("price", pa.float64()),
    ]
)
FACTOR_SCHEMA = pa.schema(
    [
        pa.field("ts", pa.timestamp("us"), nullable=False),
        pa.field("asset", pa.string()),
        pa.field("factor", pa.float64()),
    ]
)
RETURN_SCHEMA = pa.schema(
    [
        pa.field("ts", pa.timestamp("us"), nullable=False),
        pa.field("ret", pa.float64()),
    ]
)


def _python_fences(text: str) -> list:
    return re.findall(r"```python\n(.*?)```", text, re.DOTALL)


@pytest.fixture(scope="module")
def doc_db():
    rng = np.random.default_rng(202)
    n_dates, n_assets = 90, 20
    assets = [f"A{i:02d}" for i in range(n_assets)]
    dates = [dt.datetime(2024, 1, 1) + dt.timedelta(days=d) for d in range(n_dates)]
    steps = rng.normal(0.0004, 0.012, size=(n_dates, n_assets))
    levels = 100.0 * np.exp(np.cumsum(steps, axis=0))
    factors = rng.normal(size=(n_dates, n_assets))
    ts = [d for d in dates for _ in assets]
    asset = [a for _ in dates for a in assets]

    with tempfile.TemporaryDirectory() as tmp:
        db = h5i_db.Database(f"{tmp}/research.db", create=True)
        db.create_table("prices", PRICE_SCHEMA, time_column="ts")
        db.create_table("signals", FACTOR_SCHEMA, time_column="ts")
        db.create_table("strategy_returns", RETURN_SCHEMA, time_column="ts")
        db.append(
            "prices",
            pa.table(
                {"ts": ts, "asset": asset, "price": levels.reshape(-1).tolist()},
                schema=PRICE_SCHEMA,
            ),
        )
        db.append(
            "signals",
            pa.table(
                {"ts": ts, "asset": asset, "factor": factors.reshape(-1).tolist()},
                schema=FACTOR_SCHEMA,
            ),
        )
        db.append(
            "strategy_returns",
            pa.table(
                {
                    "ts": dates,
                    "ret": rng.normal(0.0005, 0.011, size=n_dates).tolist(),
                },
                schema=RETURN_SCHEMA,
            ),
        )
        # The page's examples name this snapshot and a second signals version.
        db.snapshot("2024-q1")
        db.write(
            "signals",
            pa.table(
                {
                    "ts": ts,
                    "asset": asset,
                    "factor": (-factors).reshape(-1).tolist(),
                },
                schema=FACTOR_SCHEMA,
            ),
        )
        yield db
        db.close()


def test_the_page_has_examples():
    assert len(_python_fences(DOC.read_text(encoding="utf-8"))) >= 4


def test_every_python_example_runs(doc_db, tmp_path):
    fences = _python_fences(DOC.read_text(encoding="utf-8"))
    namespace = {
        "db": doc_db,
        "quant": quant,
        "h5i_db": h5i_db,
        "__name__": "doc_example",
    }
    for index, source in enumerate(fences):
        # Reports go to the temp directory rather than the repo root.
        source = source.replace('path="factor.html"', f'path="{tmp_path}/factor.html"')
        source = source.replace(
            'path="tearsheet.html"', f'path="{tmp_path}/tearsheet.html"'
        )
        try:
            exec(compile(source, f"quant.md[fence {index}]", "exec"), namespace)
        except Exception as exc:  # noqa: BLE001 -- the failure must name the fence
            raise AssertionError(
                f"example {index} on docs-src/manual/quant.md failed: {exc!r}\n"
                f"--- source ---\n{source}"
            ) from exc


def test_the_divergence_table_covers_every_documented_difference():
    """Each divergence has an id, and the ids are contiguous."""
    text = DOC.read_text(encoding="utf-8")
    ids = re.findall(r"^\| (D\d+) \|", text, re.MULTILINE)
    assert ids, "the divergence ledger must not be empty"
    assert ids == [f"D{i + 1}" for i in range(len(ids))], "divergence ids must be dense"
