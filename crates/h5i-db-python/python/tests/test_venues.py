"""The vendor on-ramp: archives and market payloads into canonical tables.

These tests build Parquet files matching each vendor's documented column
contract rather than shipping vendor samples, so they run offline and state the
contract in one place. What they check is the part that silently corrupts a
study when wrong: outcome attribution, event grouping, level semantics,
idempotency, and whether a short load is visible in the result.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import h5i_db
from h5i_db import backtest, venues

SECOND_NS = 1_000_000_000
BASE_NS = 1_777_000_000 * SECOND_NS
YES_TOKEN = "token-yes"
NO_TOKEN = "token-no"
CONDITION = "0xcondition"


def _specs(resolved: bool = True) -> list[venues.MarketSpec]:
    return [
        venues.MarketSpec(
            instrument_id=CONDITION,
            venue="polymarket",
            outcome_labels=("Yes", "No"),
            tokens=(YES_TOKEN, NO_TOKEN),
            expiration_ns=BASE_NS + 600 * SECOND_NS,
            settlement_observable_ns=BASE_NS + 900 * SECOND_NS,
            winner_outcome=0 if resolved else None,
        )
    ]


def _pmxt_file(path: Path, rows: list[dict]) -> Path:
    """A file in the hourly full-feed shape: nested book levels, ms stamps."""
    level = pa.struct([("price", pa.float64()), ("size", pa.float64())])
    table = pa.table(
        {
            "event_type": pa.array([row["event_type"] for row in rows], pa.string()),
            "timestamp": pa.array([row["timestamp"] for row in rows], pa.int64()),
            "market": pa.array([row.get("market", CONDITION) for row in rows], pa.string()),
            "asset_id": pa.array([row["asset_id"] for row in rows], pa.string()),
            "bids": pa.array([row.get("bids") for row in rows], pa.list_(level)),
            "asks": pa.array([row.get("asks") for row in rows], pa.list_(level)),
            "price": pa.array([row.get("price") for row in rows], pa.float64()),
            "size": pa.array([row.get("size") for row in rows], pa.float64()),
            "side": pa.array([row.get("side") for row in rows], pa.string()),
        }
    )
    pq.write_table(table, path)
    return path


def _base_ms() -> int:
    return BASE_NS // 1_000_000


def _feed_rows() -> list[dict]:
    ms = _base_ms()
    return [
        {
            "event_type": "book",
            "timestamp": ms,
            "asset_id": YES_TOKEN,
            "bids": [{"price": 0.40, "size": 100.0}],
            "asks": [{"price": 0.42, "size": 90.0}],
        },
        {
            "event_type": "book",
            "timestamp": ms,
            "asset_id": NO_TOKEN,
            "bids": [{"price": 0.57, "size": 120.0}],
            "asks": [{"price": 0.59, "size": 110.0}],
        },
        {
            "event_type": "price_change",
            "timestamp": ms + 1_000,
            "asset_id": YES_TOKEN,
            "price": 0.41,
            "size": 55.0,
            "side": "BUY",
        },
        {
            "event_type": "price_change",
            "timestamp": ms + 2_000,
            "asset_id": YES_TOKEN,
            "price": 0.40,
            "size": 0.0,
            "side": "buy",
        },
        {
            "event_type": "last_trade_price",
            "timestamp": ms + 3_000,
            "asset_id": YES_TOKEN,
            "price": 0.42,
            "size": 25.0,
            "side": "sell",
        },
        {
            "event_type": "tick_size_change",
            "timestamp": ms + 4_000,
            "asset_id": YES_TOKEN,
        },
        {
            "event_type": "book",
            "timestamp": ms + 5_000,
            "asset_id": "token-of-another-market",
            "bids": [{"price": 0.10, "size": 10.0}],
            "asks": [{"price": 0.12, "size": 10.0}],
        },
    ]


def test_market_specs_refuse_the_mistakes_that_corrupt_a_study():
    with pytest.raises(ValueError, match="at least two outcomes"):
        venues.MarketSpec(instrument_id="m", venue="v", outcome_labels=("Yes",))
    with pytest.raises(ValueError, match="index i of each"):
        venues.MarketSpec(
            instrument_id="m", venue="v", outcome_labels=("Yes", "No"), tokens=("a",)
        )
    with pytest.raises(ValueError, match="token ids must be distinct"):
        venues.MarketSpec(
            instrument_id="m",
            venue="v",
            outcome_labels=("Yes", "No"),
            tokens=("a", "a"),
        )
    with pytest.raises(ValueError, match="settlement_observable_ns"):
        venues.MarketSpec(
            instrument_id="m", venue="v", outcome_labels=("Yes", "No"), winner_outcome=0
        )
    with pytest.raises(ValueError, match="before trading stops"):
        venues.MarketSpec(
            instrument_id="m",
            venue="v",
            outcome_labels=("Yes", "No"),
            expiration_ns=100,
            settlement_observable_ns=50,
        )
    # A token claimed by two markets makes every row keyed by it ambiguous.
    left = venues.MarketSpec(
        instrument_id="a", venue="v", outcome_labels=("Yes", "No"), tokens=("t1", "t2")
    )
    right = venues.MarketSpec(
        instrument_id="b", venue="v", outcome_labels=("Yes", "No"), tokens=("t1", "t3")
    )
    with pytest.raises(ValueError, match="claimed by both"):
        venues.token_index([left, right])


def test_polymarket_payloads_become_specs_with_positional_outcomes():
    payload = {
        "condition_id": CONDITION,
        # The Gamma API returns these list fields as JSON-encoded strings.
        "outcomes": '["Yes", "No"]',
        "clobTokenIds": f'["{YES_TOKEN}", "{NO_TOKEN}"]',
        "outcomePrices": '["1", "0"]',
        "closed": True,
        "umaResolutionTime": "2026-05-01T12:15:00Z",
        "endDate": "2026-05-01T12:10:00Z",
        "slug": "will-x-happen",
    }
    spec = venues.polymarket_markets_from_json([payload])[0]
    assert spec.outcome_labels == ("Yes", "No")
    assert spec.tokens == (YES_TOKEN, NO_TOKEN)
    assert spec.outcome_of_token(NO_TOKEN) == 1
    assert spec.winner_outcome == 0
    assert spec.settlement_observable_ns > spec.expiration_ns
    assert spec.metadata["slug"] == "will-x-happen"

    # The CLOB shape carries outcome names on the token objects, which is the
    # ordering that must win: it cannot disagree with the token order.
    clob = {
        "condition_id": CONDITION,
        "tokens": [
            {"token_id": NO_TOKEN, "outcome": "No"},
            {"token_id": YES_TOKEN, "outcome": "Yes"},
        ],
    }
    spec = venues.polymarket_markets_from_json([clob])[0]
    assert spec.outcome_labels == ("No", "Yes")
    assert spec.outcome_of_token(YES_TOKEN) == 1

    # A resolution with no resolution time cannot be settled honestly.
    with pytest.raises(ValueError, match="became knowable"):
        venues.polymarket_markets_from_json(
            [
                {
                    "condition_id": CONDITION,
                    "outcomes": '["Yes","No"]',
                    "winning_outcome": "Yes",
                }
            ]
        )
    # An unresolved market is fine unless the caller demands one.
    live = venues.polymarket_markets_from_json(
        [{"condition_id": CONDITION, "outcomes": '["Yes","No"]'}]
    )[0]
    assert not live.is_resolved
    with pytest.raises(ValueError, match="no resolution"):
        venues.polymarket_markets_from_json(
            [{"condition_id": CONDITION, "outcomes": '["Yes","No"]'}],
            require_resolution=True,
        )


def test_archive_ingest_normalises_every_event_kind():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _pmxt_file(root / "hour-0.parquet", _feed_rows())
        db = h5i_db.Database(str(root / "market.db"), create=True)
        specs = _specs()
        venues.write_markets(db, specs)
        report = venues.ingest_archive(
            db,
            files=venues.discover(root),
            markets=specs,
            layout=venues.PMXT_LAYOUT,
            window=(BASE_NS, BASE_NS + 600 * SECOND_NS),
        )

        book = db.sql(
            "SELECT * FROM book_deltas ORDER BY event_index, is_last"
        ).to_pandas()
        # Two snapshots (one per outcome), then a set, then a delete.
        assert list(book.action.unique()) == ["snapshot", "set", "delete"]
        assert set(book[book.action == "snapshot"].outcome) == {0, 1}
        # Outcome attribution is positional, so the NO book must carry NO prices.
        no_side = book[(book.outcome == 1) & (book.action == "snapshot")]
        assert sorted(no_side.price.tolist()) == [0.57, 0.59]
        # Size zero is a delete, not a level with no quantity.
        assert float(book[book.action == "delete"]["size"].iloc[0]) == 0.0
        # Every event is terminated exactly once.
        assert (book.groupby("event_index").is_last.sum() == 1).all()
        # And one event never mixes outcomes, which the engine now refuses.
        assert (book.groupby("event_index").outcome.nunique() == 1).all()

        trades = db.sql("SELECT * FROM trades").to_pandas()
        assert len(trades) == 1
        assert trades.aggressor.iloc[0] == "sell"

        # Unrecognised event types are counted, never silently dropped, and the
        # token of a market we did not ask for is reported rather than ingested.
        unknown = [item for item in report.skipped if item["reason"] == "unknown_event_types"]
        assert unknown and unknown[0]["counts"]["tick_size_change"] == 1
        assert report.sources[0].rows_read == 7
        assert report.sources[0].rows_kept == 6
        assert report.coverage is not None and 0.0 < report.coverage <= 1.0
        db.close()


def test_reingesting_the_same_files_replays_instead_of_duplicating():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _pmxt_file(root / "hour-0.parquet", _feed_rows())
        db = h5i_db.Database(str(root / "market.db"), create=True)
        specs = _specs()
        first = venues.ingest_archive(
            db, files=venues.discover(root), markets=specs, layout=venues.PMXT_LAYOUT
        )
        rows_after_first = db.sql("SELECT count(*) AS n FROM book_deltas").to_pandas()["n"][0]
        versions_after_first = len(db.versions("book_deltas"))

        second = venues.ingest_archive(
            db, files=venues.discover(root), markets=specs, layout=venues.PMXT_LAYOUT
        )
        rows_after_second = db.sql("SELECT count(*) AS n FROM book_deltas").to_pandas()["n"][0]

        assert not first.replayed
        assert second.replayed
        assert rows_after_second == rows_after_first
        assert len(db.versions("book_deltas")) == versions_after_first
        db.close()


def test_window_bounds_the_read_and_coverage_reports_the_shortfall():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _pmxt_file(root / "hour-0.parquet", _feed_rows())
        db = h5i_db.Database(str(root / "market.db"), create=True)
        # Ask for ten minutes; the file holds three seconds of the markets we want.
        report = venues.ingest_archive(
            db,
            files=venues.discover(root),
            markets=_specs(),
            layout=venues.PMXT_LAYOUT,
            window=(BASE_NS, BASE_NS + 600 * SECOND_NS),
        )
        assert report.requested_window == (BASE_NS, BASE_NS + 600 * SECOND_NS)
        assert report.loaded_window is not None
        assert report.coverage < 0.02
        # Bounding tighter drops the later rows entirely.
        db.close()

        db = h5i_db.Database(str(root / "narrow.db"), create=True)
        narrow = venues.ingest_archive(
            db,
            files=venues.discover(root),
            markets=_specs(),
            layout=venues.PMXT_LAYOUT,
            window=(BASE_NS, BASE_NS + 1 * SECOND_NS),
        )
        assert narrow.sources[0].rows_kept == 2
        assert "trades" not in narrow.tables
        db.close()


def test_a_file_missing_required_columns_is_skipped_not_guessed():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        pq.write_table(pa.table({"nonsense": pa.array([1, 2])}), root / "bad.parquet")
        db = h5i_db.Database(str(root / "market.db"), create=True)
        report = venues.ingest_archive(
            db, files=venues.discover(root), markets=_specs(), layout=venues.PMXT_LAYOUT
        )
        skipped = [item for item in report.skipped if item["reason"] == "missing_columns"]
        assert skipped and "timestamp" in skipped[0]["columns"]
        assert report.rows == 0
        db.close()


def test_a_layout_carries_the_vendor_dialect_so_a_third_vendor_is_data():
    """A custom layout ingests a differently-spelled feed with no new code."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        level = pa.struct([("px", pa.float64()), ("qty", pa.float64())])
        pq.write_table(
            pa.table(
                {
                    "channel": pa.array(["depth", "depth"], pa.string()),
                    "recv_ns": pa.array([BASE_NS, BASE_NS + SECOND_NS], pa.int64()),
                    "token": pa.array([YES_TOKEN, NO_TOKEN], pa.string()),
                    "buys": pa.array(
                        [[{"px": 0.30, "qty": 5.0}], [{"px": 0.68, "qty": 6.0}]],
                        pa.list_(level),
                    ),
                    "sells": pa.array(
                        [[{"px": 0.32, "qty": 4.0}], [{"px": 0.70, "qty": 3.0}]],
                        pa.list_(level),
                    ),
                }
            ),
            root / "day.parquet",
        )
        layout = venues.ArchiveLayout(
            name="house-feed",
            timestamp_column="recv_ns",
            timestamp_unit="ns",
            token_column="token",
            event_type_column="channel",
            snapshot_events=("depth",),
            levels=venues.LevelLayout(
                style="nested",
                bids_column="buys",
                asks_column="sells",
                price_field="px",
                size_field="qty",
            ),
        )
        db = h5i_db.Database(str(root / "market.db"), create=True)
        report = venues.ingest_archive(
            db, files=[root / "day.parquet"], markets=_specs(), layout=layout
        )
        book = db.sql("SELECT * FROM book_deltas ORDER BY event_index").to_pandas()
        assert report.vendor == "house-feed"
        assert len(book) == 4
        assert sorted(book[book.outcome == 0].price.tolist()) == [0.30, 0.32]
        assert set(book.source_vendor) == {"house-feed"}
        db.close()


def test_ingested_data_replays_through_the_backtest_kernel():
    """The point of all of it: the tables a run can actually read."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        ms = _base_ms()
        rows = []
        # A book wide enough to fill against, quoted through the session, then
        # a resolved tail so a full-window replay reaches settlement.
        for step in range(12):
            for token, bid, ask in ((YES_TOKEN, 0.40, 0.42), (NO_TOKEN, 0.57, 0.59)):
                rows.append(
                    {
                        "event_type": "book",
                        "timestamp": ms + step * 30_000,
                        "asset_id": token,
                        "bids": [{"price": bid, "size": 500.0}],
                        "asks": [{"price": ask, "size": 500.0}],
                    }
                )
        for tail in range(2):
            for token, bid, ask in ((YES_TOKEN, 0.99, 0.999), (NO_TOKEN, 0.001, 0.01)):
                rows.append(
                    {
                        "event_type": "book",
                        "timestamp": ms + 900_000 + tail * 30_000,
                        "asset_id": token,
                        "bids": [{"price": bid, "size": 500.0}],
                        "asks": [{"price": ask, "size": 500.0}],
                    }
                )
        _pmxt_file(root / "hour-0.parquet", rows)

        db = h5i_db.Database(str(root / "market.db"), create=True)
        specs = _specs()
        venues.write_markets(db, specs)
        venues.ingest_archive(
            db, files=venues.discover(root), markets=specs, layout=venues.PMXT_LAYOUT
        )
        db.snapshot("ingested", tables=["instruments", "book_deltas", "resolutions"])

        backtest.create_signal_table(db, "signals")
        db.append(
            "signals",
            backtest.signal_table(
                [
                    {
                        "ts": BASE_NS + 60 * SECOND_NS + 1_000,
                        "instrument_id": CONDITION,
                        "outcome": 0,
                        "side": "buy",
                        "quantity": 10.0,
                        "tag": "yes",
                    }
                ]
            ),
        )
        result = backtest.execute(
            db,
            backtest.BacktestConfig(
                run_id="from-archive",
                data=backtest.DataConfig(signals="signals", snapshot="ingested"),
                portfolio=backtest.PortfolioConfig(starting_cash=1_000.0),
            ),
        )
        fills = result.fills.to_pandas()
        assert len(fills) == 1
        # It paid the ask that the archive carried, not the bid, not a mid.
        assert fills.price.iloc[0] == pytest.approx(0.42)
        positions = result.positions.to_pandas()
        # YES won, so 10 contracts bought at 0.42 settle to 10 * (1 - 0.42).
        assert bool(result.run.to_pandas().settlement_applied.iloc[0])
        assert positions.settlement_pnl.sum() == pytest.approx(10 * (1 - 0.42))
        db.close()


def test_cli_round_trips_specs_and_gates_on_coverage(capsys):
    from h5i_db.venues.__main__ import main

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _pmxt_file(root / "hour-0.parquet", _feed_rows())
        database = str(root / "market.db")
        spec_path = root / "specs.json"
        spec_path.write_text(
            json.dumps(
                [
                    {
                        "condition_id": CONDITION,
                        "outcomes": '["Yes","No"]',
                        "clobTokenIds": f'["{YES_TOKEN}","{NO_TOKEN}"]',
                        "outcomePrices": '["1","0"]',
                        "closed": True,
                        "umaResolutionTime": 1_777_000_900,
                    }
                ]
            ),
            encoding="utf-8",
        )

        assert main(["markets", database, str(spec_path)]) == 0
        assert json.loads(capsys.readouterr().out)["tables"]["instruments"]["rows"] == 2

        assert main(["ingest", database, str(spec_path), "--root", str(root)]) == 0
        payload = json.loads(capsys.readouterr().out)
        assert payload["tables"]["book_deltas"]["rows"] > 0

        assert main(["inspect", database]) == 0
        summary = json.loads(capsys.readouterr().out)["tables"]
        assert summary["book_deltas"]["rows"] > 0

        # Coverage gating: a ten-minute request over a three-second file fails.
        code = main(
            [
                "ingest",
                database,
                str(spec_path),
                "--root",
                str(root),
                "--start-ns",
                str(BASE_NS),
                "--end-ns",
                str(BASE_NS + 600 * SECOND_NS),
                "--min-coverage",
                "0.9",
            ]
        )
        assert code == 3
        # And gating without a window is an error, not a silent pass.
        assert main(
            ["ingest", database, str(spec_path), "--root", str(root), "--min-coverage", "0.5"]
        ) == 2
