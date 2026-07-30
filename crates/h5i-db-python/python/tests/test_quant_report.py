"""Report rendering tests (ROADMAP_QUANT.md M2, §6).

The contract for a report is narrow and checkable: it is one file, it makes
no network requests, it leads with provenance, and its numbers are the same
ones the API returns.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import json
import re
import tempfile

import numpy as np
import pyarrow as pa
import pytest

import h5i_db
from h5i_db import quant

from test_quant_sweep import FACTOR_SCHEMA, PRICE_SCHEMA, _panel_data

RETURN_SCHEMA = pa.schema(
    [
        pa.field("ts", pa.timestamp("us"), nullable=False),
        pa.field("ret", pa.float64()),
    ]
)


@contextlib.contextmanager
def report_db():
    prices, factors, dates = _panel_data(seed=29, n_dates=60, n_assets=20)
    rng = np.random.default_rng(3)
    with tempfile.TemporaryDirectory() as tmp:
        db = h5i_db.Database(f"{tmp}/report.db", create=True)
        db.create_table("prices", PRICE_SCHEMA, time_column="ts")
        db.create_table("signals", FACTOR_SCHEMA, time_column="ts")
        db.create_table("rets", RETURN_SCHEMA, time_column="ts")
        db.append("prices", prices)
        db.append("signals", factors)
        db.append(
            "rets",
            pa.table(
                {
                    "ts": dates,
                    "ret": rng.normal(0.0005, 0.01, size=len(dates)).tolist(),
                },
                schema=RETURN_SCHEMA,
            ),
        )
        db.snapshot("v1")
        try:
            yield db
        finally:
            db.close()


def _panel(db, **kwargs):
    kwargs.setdefault("periods", (1, 5, 10))
    kwargs.setdefault("quantiles", 5)
    kwargs.setdefault("filter_zscore", None)
    kwargs.setdefault("max_loss", 1.0)
    return quant.build_panel(db, "signals", "prices", **kwargs)


# -- the self-containment contract -------------------------------------------

_EXTERNAL = re.compile(
    r"""(src|href)\s*=\s*["']\s*(https?:|//)""", re.IGNORECASE
)
_FETCHERS = re.compile(r"\b(fetch|XMLHttpRequest|WebSocket|importScripts)\s*\(")


@pytest.mark.parametrize("kind", ["factor", "tearsheet"])
def test_report_is_self_contained(kind):
    with report_db() as db:
        html = (
            quant.factor_report(_panel(db))
            if kind == "factor"
            else quant.tearsheet(quant.returns(db, "rets"))
        )
    assert _EXTERNAL.search(html) is None, "a report must not load anything remote"
    assert _FETCHERS.search(html) is None, "a report must not make requests"
    assert html.lstrip().startswith("<!doctype html>")
    assert html.count("<script") == 1 and html.count("<style") == 1


@pytest.mark.parametrize("kind", ["factor", "tearsheet"])
def test_report_leads_with_provenance(kind):
    with report_db() as db:
        subject = _panel(db, snapshot="v1") if kind == "factor" else quant.returns(
            db, "rets", snapshot="v1"
        )
        html = (
            quant.factor_report(subject)
            if kind == "factor"
            else quant.tearsheet(subject)
        )
    assert "Provenance" in html
    assert subject.provenance.digest in html
    assert html.index("Provenance") < html.index("Summary")


def test_unpinned_report_carries_the_warning():
    with report_db() as db:
        html = quant.factor_report(_panel(db))
    assert "unpinned" in html


def test_report_writes_a_file(tmp_path):
    with report_db() as db:
        out = tmp_path / "factor.html"
        html = quant.factor_report(_panel(db, snapshot="v1"), path=str(out))
    assert out.read_text(encoding="utf-8") == html
    assert len(html) > 5000


# -- the payload is the API's own numbers ------------------------------------


def test_factor_payload_matches_the_api():
    with report_db() as db:
        panel = _panel(db, snapshot="v1")
        payload = quant.report_payload(panel)
        decay = panel.ic_decay().to_arrow().to_pylist()
    table = next(t for t in payload["tables"] if t["id"] == "decay-table")
    assert len(table["rows"]) == len(decay)
    for got, expected in zip(table["rows"], decay):
        np.testing.assert_allclose(got["mean_ic"], expected["mean_ic"], rtol=1e-12)
    assert payload["provenance"]["digest"] == panel.provenance.digest


def test_tearsheet_payload_matches_the_api():
    with report_db() as db:
        series = quant.returns(db, "rets", snapshot="v1")
        payload = quant.report_payload(series)
        stats = series.stats()
    tiles = {t["label"]: t["value"] for t in payload["headline"]}
    np.testing.assert_allclose(tiles["Sharpe"], stats["sharpe_ratio"], rtol=1e-12)
    np.testing.assert_allclose(
        tiles["Max drawdown"], stats["max_drawdown"], rtol=1e-12
    )


def test_payload_is_json_serializable_without_nan():
    """NaN is not valid JSON; a report must never emit it."""
    with report_db() as db:
        # A monotone series has no drawdown and produces a NULL Calmar.
        flat = pa.table(
            {
                "ts": [dt.datetime(2024, 5, 1) + dt.timedelta(days=i) for i in range(30)],
                "ret": [0.001] * 30,
            },
            schema=RETURN_SCHEMA,
        )
        db.create_table("flat", RETURN_SCHEMA, time_column="ts")
        db.append("flat", flat)
        payload = quant.report_payload(quant.returns(db, "flat"))
    json.dumps(payload, allow_nan=False)


def test_report_escapes_a_closing_script_tag():
    """A table name containing markup must not break out of the payload."""
    with report_db() as db:
        panel = _panel(db, snapshot="v1")
        payload = quant.report_payload(panel)
        payload["title"] = "</script><script>alert(1)</script>"
        html = quant.report.render(payload)
    assert "<script>alert(1)</script>" not in html
    assert html.count("<script") == 1


def test_report_renders_both_themes():
    with report_db() as db:
        html = quant.tearsheet(quant.returns(db, "rets", snapshot="v1"))
    assert "prefers-color-scheme: dark" in html
    assert 'data-theme="dark"' in html
    assert 'data-theme="light"' in html


def test_report_offers_a_table_view_for_every_chart():
    with report_db() as db:
        payload = quant.report_payload(_panel(db, snapshot="v1"))
        html = quant.report.render(payload)
    assert html.count("View as table") == 1  # the template renders one per chart
    assert len(payload["charts"]) >= 5
    for chart in payload["charts"]:
        assert chart["series"], f"chart {chart['id']} has no series"
        assert chart["kind"] in {"line", "bar", "area"}


def test_factor_report_caps_series_at_the_validated_palette_size():
    """No chart may need a fourth categorical hue."""
    with report_db() as db:
        payload = quant.report_payload(
            _panel(db, snapshot="v1", periods=(1, 2, 3, 5, 10))
        )
    for chart in payload["charts"]:
        assert len(chart["series"]) <= 3, f"{chart['id']} needs more than 3 hues"
