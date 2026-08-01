"""The HTML report for one backtest run.

Same contract as every other report: one file, no network, provenance before
numbers. What is specific here is that the report must survive a run with no
equity curve, and must lead with the evidence that says how far the numbers
can be trusted -- replay fidelity, the pin, coverage, preflight issues.
"""

from __future__ import annotations

import datetime as dt
import json
import re
import tempfile

import pytest
from h5i_db import backtest, quant

from test_backtest_bindings import MARKET, SECOND, _seeded, _signals

_EXTERNAL = re.compile(r"""(src|href)\s*=\s*["']\s*(https?:|//)""", re.IGNORECASE)
_FETCHERS = re.compile(r"\b(fetch|XMLHttpRequest|WebSocket|importScripts)\s*\(")

_BUY = {
    "ts": dt.datetime(2024, 1, 1, 0, 0, 3),
    "instrument_id": MARKET,
    "side": "buy",
    "quantity": 100.0,
    "tag": "entry",
}


def _run(tmp, run_id, *, equity=True, signals=(_BUY,), **config_kwargs):
    db = _seeded(tmp)
    _signals(db, list(signals))
    config = backtest.BacktestConfig(
        run_id=run_id,
        portfolio=backtest.PortfolioConfig(starting_cash=1_000.0),
        data=backtest.DataConfig(snapshot="seed", **config_kwargs),
        output=backtest.OutputConfig(
            equity_interval_nanos=SECOND if equity else None
        ),
    )
    return db, backtest.execute(db, config)


# -- the contract every report shares ----------------------------------------


def test_report_is_self_contained():
    with tempfile.TemporaryDirectory() as tmp:
        db, result = _run(tmp, "rep-self")
        html = result.report()
        db.close()
    assert _EXTERNAL.search(html) is None, "a report must not load anything remote"
    assert _FETCHERS.search(html) is None, "a report must not make requests"
    assert html.lstrip().startswith("<!doctype html>")
    assert html.count("<script") == 1 and html.count("<style") == 1


def test_report_leads_with_the_evidence():
    with tempfile.TemporaryDirectory() as tmp:
        db, result = _run(tmp, "rep-order")
        html = result.report()
        digest = result["digest"]
        db.close()
    assert html.index("Provenance") < html.index("Summary")
    assert digest in html
    assert result.config.digest in html
    assert "snapshot_l2" in html or "periodic L2 snapshots" in html


def test_report_writes_the_file_it_returns(tmp_path):
    with tempfile.TemporaryDirectory() as tmp:
        db, result = _run(tmp, "rep-file")
        out = tmp_path / "run.html"
        html = result.report(out)
        db.close()
    assert out.read_text(encoding="utf-8") == html
    assert len(html) > 5000


def test_report_renders_both_themes():
    with tempfile.TemporaryDirectory() as tmp:
        db, result = _run(tmp, "rep-theme")
        html = result.report()
        db.close()
    assert "prefers-color-scheme: dark" in html
    assert 'data-theme="dark"' in html
    assert 'data-theme="light"' in html


def test_report_escapes_a_closing_script_tag():
    with tempfile.TemporaryDirectory() as tmp:
        db, result = _run(tmp, "rep-escape")
        payload = quant.backtest_payload(result)
        db.close()
    payload["title"] = "</script><script>alert(1)</script>"
    html = quant.report.render(payload)
    assert "<script>alert(1)</script>" not in html
    assert html.count("<script") == 1


# -- the numbers are the API's own -------------------------------------------


def test_payload_matches_the_api():
    with tempfile.TemporaryDirectory() as tmp:
        db, result = _run(tmp, "rep-numbers")
        payload = quant.backtest_payload(result)
        summary = result.summary()
        stats = result.stats()
        fills = result.fills.to_pylist()
        db.close()

    tiles = {tile["label"]: tile["value"] for tile in payload["headline"]}
    assert tiles["Fills"] == summary["fills"]
    assert tiles["Orders"] == summary["orders"]
    assert tiles["Commissions"] == summary["commissions"]
    assert tiles["Realized P&L"] == summary["realized_pnl"]
    assert tiles["Sharpe"] == pytest.approx(stats["sharpe_ratio"], rel=1e-12)

    manifest = next(t for t in payload["tables"] if t["id"] == "run-manifest")
    recorded = {row["field"]: row["value"] for row in manifest["rows"]}
    assert recorded["digest"] == summary["digest"]

    scatter = next(c for c in payload["charts"] if c["id"] == "fills")
    plotted = [p for series in scatter["series"] for p in series["points"]]
    assert len(plotted) == len(fills)
    assert {p["y"] for p in plotted} == {row["price"] for row in fills}


def test_payload_is_json_serializable_without_nan():
    """NaN is not valid JSON; a report must never emit it."""
    with tempfile.TemporaryDirectory() as tmp:
        db, result = _run(tmp, "rep-json")
        payload = quant.backtest_payload(result)
        db.close()
    json.dumps(payload, allow_nan=False)


def test_charts_stay_inside_the_validated_palette():
    with tempfile.TemporaryDirectory() as tmp:
        db, result = _run(tmp, "rep-palette")
        payload = quant.backtest_payload(result)
        db.close()
    for chart in payload["charts"]:
        assert chart["series"], f"chart {chart['id']} has no series"
        assert len(chart["series"]) <= 3, f"{chart['id']} needs more than 3 hues"
        assert chart["kind"] in {"line", "bar", "area", "scatter"}
    # Buy and sell must not rest on hue alone.
    scatter = next(c for c in payload["charts"] if c["id"] == "fills")
    assert all(series["marker"] for series in scatter["series"])


# -- what this report has that a tearsheet does not --------------------------


def test_the_configuration_is_carried_verbatim():
    """The report must be enough to re-run the run it describes."""
    with tempfile.TemporaryDirectory() as tmp:
        db, result = _run(tmp, "rep-config")
        payload = quant.backtest_payload(result)
        config = result.config
        db.close()
    block = next(b for b in payload["code"] if b["id"] == "config-json")
    assert backtest.BacktestConfig.from_json(block["text"]) == config
    assert any(b["id"] == "preflight-json" for b in payload["code"])


def test_preflight_issues_reach_the_status_banner():
    with tempfile.TemporaryDirectory() as tmp:
        db, result = _run(tmp, "rep-status")
        payload = quant.backtest_payload(result)
        issues = result.inspection.issues
        db.close()
    status = payload["status"]
    assert status["tone"] in {"ok", "warn", "bad"}
    assert len(status["items"]) >= len(issues)
    for issue in issues:
        assert any(item["message"] == issue.message for item in status["items"])


def test_an_unpinned_run_says_so():
    with tempfile.TemporaryDirectory() as tmp:
        db = _seeded(tmp)
        _signals(db, [_BUY])
        result = backtest.execute(
            db,
            backtest.BacktestConfig(
                run_id="rep-unpinned",
                portfolio=backtest.PortfolioConfig(starting_cash=1_000.0),
                output=backtest.OutputConfig(equity_interval_nanos=SECOND),
            ),
        )
        payload = quant.backtest_payload(result)
        html = result.report()
        db.close()
    assert payload["status"]["tone"] != "ok"
    assert "Unpinned" in payload["status"]["detail"]
    assert "unpinned" in html.lower()


def test_a_run_without_an_equity_curve_still_reports():
    """A window the data does not reach replays nothing, and must say so."""
    with tempfile.TemporaryDirectory() as tmp:
        db, result = _run(
            tmp,
            "rep-noequity",
            signals=(),
            window=(dt.datetime(2025, 1, 1), dt.datetime(2025, 1, 2)),
        )
        payload = quant.backtest_payload(result)
        html = result.report()
        equity_rows = result.equity.num_rows
        db.close()
    assert equity_rows == 0
    assert not [c for c in payload["charts"] if c["id"] in {"equity", "drawdown"}]
    assert any("no equity curve" in note for note in payload["notes"])
    assert "Run manifest" in html
    assert [t for t in payload["tables"] if t["id"] == "order-status"]


def test_a_silent_run_reports_its_silence():
    with tempfile.TemporaryDirectory() as tmp:
        db, result = _run(tmp, "rep-silent", signals=())
        payload = quant.backtest_payload(result)
        html = result.report()
        order_rows = result.orders.num_rows
        db.close()
    assert order_rows == 0
    assert not [c for c in payload["charts"] if c["id"] == "fills"]
    assert "Run manifest" in html


def test_html_summary_is_the_report():
    with tempfile.TemporaryDirectory() as tmp:
        db, result = _run(tmp, "rep-alias")
        assert result.html_summary() == result.report()
        assert "Run manifest" in result.html_summary()
        db.close()


def test_the_cli_report_verb_writes_the_run_report(tmp_path, capsys):
    with tempfile.TemporaryDirectory() as tmp:
        database = f"{tmp}/bt.db"
        db, _ = _run(tmp, "rep-cli")
        db.close()
        out = tmp_path / "cli.html"
        assert backtest.main(["report", database, "rep-cli", "--output", str(out)]) == 0
        assert capsys.readouterr().out.strip() == str(out)
        report = out.read_text(encoding="utf-8")

        # The retired flag still selects a page that contains the manifest.
        legacy = tmp_path / "legacy.html"
        assert (
            backtest.main(
                ["report", database, "rep-cli", "--output", str(legacy),
                 "--execution-only"]
            )
            == 0
        )
        capsys.readouterr()

        tear = tmp_path / "tear.html"
        assert (
            backtest.main(
                ["report", database, "rep-cli", "--output", str(tear), "--tearsheet"]
            )
            == 0
        )
    assert "Run manifest" in report
    assert legacy.read_text(encoding="utf-8") == report
    assert "Run manifest" not in tear.read_text(encoding="utf-8")


def test_a_notebook_gets_the_report_in_an_iframe():
    with tempfile.TemporaryDirectory() as tmp:
        db, result = _run(tmp, "rep-notebook")
        embedded = result._repr_html_()
        db.close()
    assert embedded.startswith("<iframe srcdoc=")
    # Escaped, so the document cannot terminate the attribute or the iframe.
    assert "<!doctype html>" not in embedded
    assert "&lt;!doctype html&gt;" in embedded
