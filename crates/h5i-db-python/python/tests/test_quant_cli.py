"""Command line surface tests (ROADMAP_QUANT.md P4).

The CLI must produce the *same numbers* as the Python API, not merely run:
one implementation reachable from two surfaces is the whole point of the
rule, so the parity is asserted rather than assumed.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import io
import json
import tempfile

import numpy as np
import pyarrow as pa
import pytest

import h5i_db
from h5i_db import quant
from h5i_db.quant.__main__ import main

from test_quant_sweep import FACTOR_SCHEMA, PRICE_SCHEMA, _panel_data

RETURN_SCHEMA = pa.schema(
    [
        pa.field("ts", pa.timestamp("us"), nullable=False),
        pa.field("ret", pa.float64()),
    ]
)


@pytest.fixture(scope="module")
def db_path():
    prices, factors, dates = _panel_data(seed=131, n_dates=80, n_assets=20)
    rng = np.random.default_rng(5)
    with tempfile.TemporaryDirectory() as tmp:
        path = f"{tmp}/cli.db"
        db = h5i_db.Database(path, create=True)
        db.create_table("prices", PRICE_SCHEMA, time_column="ts")
        db.create_table("signals", FACTOR_SCHEMA, time_column="ts")
        db.create_table("rets", RETURN_SCHEMA, time_column="ts")
        db.append("prices", prices)
        db.append("signals", factors)
        db.append(
            "rets",
            pa.table(
                {"ts": dates, "ret": rng.normal(0.0004, 0.01, len(dates)).tolist()},
                schema=RETURN_SCHEMA,
            ),
        )
        db.snapshot("v1")
        db.close()
        yield path


def run(argv):
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        code = main(argv)
    return code, out.getvalue()


def test_factor_json_matches_the_api(db_path):
    code, out = run(
        [
            "factor", "--db", db_path, "--factor", "signals", "--prices", "prices",
            "--periods", "1,5", "--quantiles", "5", "--snapshot", "v1",
            "--filter-zscore", "20.0", "--max-loss", "1.0",
        ]
    )
    assert code == 0
    payload = json.loads(out)
    with h5i_db.Database(db_path) as db:
        panel = quant.build_panel(
            db, "signals", "prices", periods=(1, 5), quantiles=5,
            filter_zscore=20.0, max_loss=1.0, snapshot="v1",
        )
        expected = panel.ic_decay().to_arrow().to_pylist()
        assert payload["provenance"]["digest"] == panel.provenance.digest
    table = next(t for t in payload["tables"] if t["id"] == "decay-table")
    for got, want in zip(table["rows"], expected):
        np.testing.assert_allclose(got["mean_ic"], want["mean_ic"], rtol=1e-12)


def test_stats_matches_the_api(db_path):
    code, out = run(["stats", "--db", db_path, "--returns", "rets", "--snapshot", "v1"])
    assert code == 0
    stats = json.loads(out)
    with h5i_db.Database(db_path) as db:
        expected = quant.returns(db, "rets", snapshot="v1").stats()
    for key, value in expected.items():
        if isinstance(value, float):
            np.testing.assert_allclose(stats[key], value, rtol=1e-12)


def test_tearsheet_writes_a_report(db_path, tmp_path):
    out_file = tmp_path / "t.html"
    code, out = run(
        ["tearsheet", "--db", db_path, "--returns", "rets", "--snapshot", "v1",
         "--out", str(out_file)]
    )
    assert code == 0
    assert json.loads(out)["written"] == str(out_file)
    html = out_file.read_text(encoding="utf-8")
    assert html.lstrip().startswith("<!doctype html>")


def test_html_to_stdout(db_path):
    code, out = run(
        ["tearsheet", "--db", db_path, "--returns", "rets", "--snapshot", "v1",
         "--format", "html"]
    )
    assert code == 0
    assert out.lstrip().startswith("<!doctype html>")


def test_verify_passes_on_a_pinned_panel_and_honours_expect(db_path):
    code, out = run(
        ["verify", "--db", db_path, "--factor", "signals", "--prices", "prices",
         "--periods", "1", "--snapshot", "v1", "--max-loss", "1.0"]
    )
    assert code == 0
    digest = json.loads(out)["digest"]

    code, _ = run(
        ["verify", "--db", db_path, "--factor", "signals", "--prices", "prices",
         "--periods", "1", "--snapshot", "v1", "--max-loss", "1.0",
         "--expect", digest]
    )
    assert code == 0

    code, out = run(
        ["verify", "--db", db_path, "--factor", "signals", "--prices", "prices",
         "--periods", "1", "--snapshot", "v1", "--max-loss", "1.0",
         "--expect", "0" * 64]
    )
    assert code == 1
    assert json.loads(out)["verified"] is False


def test_verify_refuses_an_unpinned_panel(db_path):
    code, out = run(
        ["verify", "--db", db_path, "--factor", "signals", "--prices", "prices",
         "--periods", "1", "--max-loss", "1.0"]
    )
    assert code == 1
    assert "unpinned" in json.loads(out)["reason"]


def test_per_table_version_pins_parse(db_path):
    code, out = run(
        ["factor", "--db", db_path, "--factor", "signals", "--prices", "prices",
         "--periods", "1", "--version", "signals:1,prices:1", "--max-loss", "1.0"]
    )
    assert code == 0
    pin = json.loads(out)["provenance"]["pin"]
    assert pin["version"] == {"prices": 1, "signals": 1}
    assert pin["pinned"] is True


def test_bad_input_exits_with_a_message(db_path, capsys):
    code = main(
        ["factor", "--db", db_path, "--factor", "signals", "--prices", "prices",
         "--periods", "1", "--max-loss", "0.0001"]
    )
    assert code == 2
    assert "max_loss" in capsys.readouterr().err


def test_event_time_cutoff_flag_is_recorded(db_path):
    code, out = run(
        ["factor", "--db", db_path, "--factor", "signals", "--prices", "prices",
         "--periods", "1", "--snapshot", "v1", "--max-loss", "1.0",
         "--event-time-cutoff", "2024-02-01T00:00:00"]
    )
    assert code == 0
    payload = json.loads(out)
    assert payload["provenance"]["pin"]["event_time_cutoff"] == "2024-02-01T00:00:00"
