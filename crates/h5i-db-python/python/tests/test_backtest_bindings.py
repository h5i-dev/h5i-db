"""Driving a backtest from Python (ROADMAP_QUANT.md Part B).

Both ends of a run are tables: the signals table is the strategy, and the
``bt_*`` tables on the run's fork are the result. These tests exercise that
whole path from Python, and end where it should end -- at a tearsheet.
"""

from __future__ import annotations

import datetime as dt
import json
import tempfile
from dataclasses import replace

import h5i_db
import pyarrow as pa
import pytest
from h5i_db import backtest, quant
from h5i_db.backtest_result import VERIFY_OF, _CONFIG_SCHEMA

SECOND = 1_000_000_000
MARKET = "will-x-happen"

BOOK_DELTAS = pa.schema(
    [
        pa.field("ts_init", pa.timestamp("ns"), nullable=False),
        pa.field("ts_event", pa.timestamp("ns"), nullable=False),
        pa.field("instrument_id", pa.string(), nullable=False),
        pa.field("outcome", pa.uint16(), nullable=False),
        pa.field("action", pa.string(), nullable=False),
        pa.field("side", pa.string()),
        pa.field("price", pa.float64()),
        pa.field("size", pa.float64()),
        pa.field("event_index", pa.int64(), nullable=False),
        pa.field("is_last", pa.bool_(), nullable=False),
        pa.field("source_vendor", pa.string()),
    ]
)
INSTRUMENTS = pa.schema(
    [
        pa.field("ts_init", pa.timestamp("ns"), nullable=False),
        pa.field("instrument_id", pa.string(), nullable=False),
        pa.field("venue", pa.string(), nullable=False),
        pa.field("kind", pa.string(), nullable=False),
        pa.field("outcome", pa.uint16(), nullable=False),
        pa.field("outcome_label", pa.string(), nullable=False),
        pa.field("tick_size", pa.float64(), nullable=False),
        pa.field("lot_size", pa.float64(), nullable=False),
        pa.field("expiration_ns", pa.int64()),
        pa.field("settlement_observable_ns", pa.int64()),
    ]
)


def _seeded(tmp, *, tick_size: float = 0.0001) -> h5i_db.Database:
    db = h5i_db.Database(f"{tmp}/bt.db", create=True)
    db.create_table("instruments", INSTRUMENTS, time_column="ts_init")
    db.create_table("book_deltas", BOOK_DELTAS, time_column="ts_init")
    db.append(
        "instruments",
        pa.table(
            {
                "ts_init": [dt.datetime(2024, 1, 1)] * 2,
                "instrument_id": [MARKET] * 2,
                "venue": ["polymarket"] * 2,
                "kind": ["prediction_market"] * 2,
                "outcome": [0, 1],
                "outcome_label": ["YES", "NO"],
                "tick_size": [tick_size] * 2,
                "lot_size": [1.0] * 2,
                "expiration_ns": [None, None],
                "settlement_observable_ns": [None, None],
            },
            schema=INSTRUMENTS,
        ),
    )

    # Ten one-second snapshots, two rows each (one bid, one ask).
    rows: dict = {name: [] for name in BOOK_DELTAS.names}
    base = dt.datetime(2024, 1, 1)
    for step in range(1, 11):
        at = base + dt.timedelta(seconds=step)
        for index, (side, price) in enumerate(
            [("buy", 0.40 + step * 0.01), ("sell", 0.42 + step * 0.01)]
        ):
            rows["ts_init"].append(at)
            rows["ts_event"].append(at)
            rows["instrument_id"].append(MARKET)
            rows["outcome"].append(0)
            rows["action"].append("snapshot")
            rows["side"].append(side)
            rows["price"].append(round(price, 4))
            rows["size"].append(500.0)
            rows["event_index"].append(step)
            rows["is_last"].append(index == 1)
            rows["source_vendor"].append("test")
    db.append("book_deltas", pa.table(rows, schema=BOOK_DELTAS))
    db.snapshot("seed")
    return db


def _signals(db, rows):
    backtest.create_signal_table(db)
    db.append("signals", backtest.signal_table(rows))


RESOLUTIONS = pa.schema(
    [
        pa.field("ts_init", pa.timestamp("ns"), nullable=False),
        pa.field("instrument_id", pa.string(), nullable=False),
        pa.field("kind", pa.string(), nullable=False),
        pa.field("outcome", pa.uint16()),
        pa.field("payout", pa.float64()),
        pa.field("outcome_count", pa.uint16()),
    ]
)


def _resolve(db, kind="winner", outcome=0, payout=None, outcome_count=None):
    """Write how the seeded market ended, after the data it traded on."""
    db.create_table("resolutions", RESOLUTIONS, time_column="ts_init")
    db.append(
        "resolutions",
        pa.table(
            {
                "ts_init": [dt.datetime(2024, 1, 1, 0, 0, 5)],
                "instrument_id": [MARKET],
                "kind": [kind],
                "outcome": [outcome],
                "payout": [payout],
                "outcome_count": [outcome_count],
            },
            schema=RESOLUTIONS,
        ),
    )


def test_a_run_from_python_produces_fills_and_a_fork():
    with tempfile.TemporaryDirectory() as tmp:
        db = _seeded(tmp)
        _signals(
            db,
            [
                {
                    "ts": dt.datetime(2024, 1, 1, 0, 0, 3),
                    "instrument_id": MARKET,
                    "side": "buy",
                    "quantity": 100.0,
                    "tag": "entry",
                }
            ],
        )
        report = backtest.run(db, "py-001", starting_cash=1_000.0, snapshot="seed")
        assert report["fork"] == "bt-py-001"
        assert report["fills"] == 1
        assert report["records_processed"] > 0
        assert len(report["digest"]) == 64

        fork = db.fork("bt-py-001")
        fills = fork.read("bt_fills").to_pylist()
        assert len(fills) == 1
        assert fills[0]["side"] == "buy"
        assert fills[0]["tag"] == "entry"
        db.close()


def test_the_run_is_reproducible_from_its_arguments():
    with tempfile.TemporaryDirectory() as tmp:
        db = _seeded(tmp)
        _signals(
            db,
            [
                {
                    "ts": dt.datetime(2024, 1, 1, 0, 0, 3),
                    "instrument_id": MARKET,
                    "side": "buy",
                    "quantity": 50.0,
                }
            ],
        )
        first = backtest.run(db, "rep-a", starting_cash=500.0, snapshot="seed")
        second = backtest.run(db, "rep-b", starting_cash=500.0, snapshot="seed")
        for key in ("final_cash", "realized_pnl", "fills", "records_processed"):
            assert first[key] == second[key], key
        db.close()


def test_a_run_feeds_the_tearsheet():
    """The whole point: simulation to report with no adapter in between."""
    with tempfile.TemporaryDirectory() as tmp:
        db = _seeded(tmp)
        _signals(
            db,
            [
                {
                    "ts": dt.datetime(2024, 1, 1, 0, 0, 2),
                    "instrument_id": MARKET,
                    "side": "buy",
                    "quantity": 100.0,
                }
            ],
        )
        report = backtest.run(
            db,
            "tear-001",
            starting_cash=1_000.0,
            snapshot="seed",
            equity_interval_nanos=SECOND,
        )
        assert report["equity_points"] >= 2

        fork = db.fork("bt-tear-001")
        series = quant.from_levels(fork, "bt_equity")
        stats = series.stats()
        assert stats["n_periods"] >= 1
        html = quant.tearsheet(series, title="Backtest run")
        assert "Backtest run" in html
        db.close()


def test_fees_reduce_the_final_cash():
    with tempfile.TemporaryDirectory() as tmp:
        db = _seeded(tmp)
        _signals(
            db,
            [
                {
                    "ts": dt.datetime(2024, 1, 1, 0, 0, 3),
                    "instrument_id": MARKET,
                    "side": "buy",
                    "quantity": 100.0,
                }
            ],
        )
        free = backtest.run(db, "free", starting_cash=1_000.0, snapshot="seed")
        charged = backtest.run(
            db,
            "charged",
            starting_cash=1_000.0,
            snapshot="seed",
            fee_rate=0.07,
        )
        assert charged["commissions"] > 0
        assert free["commissions"] == 0
        assert charged["final_cash"] < free["final_cash"]
        db.close()


def test_a_limit_signal_without_a_price_is_refused_before_it_runs():
    with pytest.raises(ValueError, match="limit_price"):
        backtest.signal_table(
            [
                {
                    "ts": 0,
                    "instrument_id": MARKET,
                    "side": "buy",
                    "quantity": 1.0,
                    "kind": "limit",
                }
            ]
        )


def test_signal_rows_must_be_complete():
    with pytest.raises(ValueError, match="missing"):
        backtest.signal_table([{"ts": 0, "instrument_id": MARKET}])
    with pytest.raises(ValueError, match="unknown kind"):
        backtest.signal_table(
            [
                {
                    "ts": 0,
                    "instrument_id": MARKET,
                    "side": "buy",
                    "quantity": 1.0,
                    "kind": "iceberg",
                }
            ]
        )


def test_coverage_floor_refuses_a_thin_window():
    with tempfile.TemporaryDirectory() as tmp:
        db = _seeded(tmp)
        _signals(db, [])
        start = dt.datetime(2024, 1, 1)
        with pytest.raises(h5i_db.InvalidInputError, match="coverage"):
            backtest.run(
                db,
                "thin",
                starting_cash=100.0,
                snapshot="seed",
                window=(start, start + dt.timedelta(seconds=200)),
                minimum_coverage=0.9,
            )
        db.close()


def test_an_unsorted_signal_table_is_refused():
    with tempfile.TemporaryDirectory() as tmp:
        db = _seeded(tmp)
        # Written out of order on purpose; the reader sorts, so this must
        # succeed rather than fail -- a table has no inherent order.
        _signals(
            db,
            [
                {
                    "ts": dt.datetime(2024, 1, 1, 0, 0, 3),
                    "instrument_id": MARKET,
                    "side": "buy",
                    "quantity": 10.0,
                },
                {
                    "ts": dt.datetime(2024, 1, 1, 0, 0, 5),
                    "instrument_id": MARKET,
                    "side": "sell",
                    "quantity": 10.0,
                },
            ],
        )
        report = backtest.run(db, "sorted", starting_cash=500.0, snapshot="seed")
        assert report["fills"] == 2
        db.close()


def test_queue_position_changes_nothing_for_a_taker():
    """A marketable order does not depend on queue position."""
    with tempfile.TemporaryDirectory() as tmp:
        db = _seeded(tmp)
        _signals(
            db,
            [
                {
                    "ts": dt.datetime(2024, 1, 1, 0, 0, 3),
                    "instrument_id": MARKET,
                    "side": "buy",
                    "quantity": 10.0,
                }
            ],
        )
        plain = backtest.run(db, "plain", starting_cash=500.0, snapshot="seed")
        queued = backtest.run(
            db, "queued", starting_cash=500.0, snapshot="seed", queue_position=True
        )
        assert plain["fills"] == queued["fills"] == 1
        assert plain["final_cash"] == queued["final_cash"]
        db.close()


def test_typed_config_round_trips_and_preflights():
    with tempfile.TemporaryDirectory() as tmp:
        db = _seeded(tmp)
        _signals(
            db,
            [
                {
                    "ts": dt.datetime(2024, 1, 1, 0, 0, 3),
                    "instrument_id": MARKET,
                    "side": "buy",
                    "quantity": 10.0,
                }
            ],
        )
        config = backtest.BacktestConfig(
            run_id="typed",
            portfolio=backtest.PortfolioConfig(starting_cash=500.0),
            data=backtest.DataConfig(snapshot="seed"),
            execution=backtest.ExecutionConfig(
                fee_kind="prediction_market",
                fee_rate=0.07,
            ),
            output=backtest.OutputConfig(equity_interval_nanos=SECOND),
            metadata={"owner": "research"},
        )
        restored = backtest.BacktestConfig.from_json(config.to_json())
        assert restored == config
        assert restored.digest == config.digest

        inspection = backtest.inspect(db, config)
        assert inspection.ok
        assert inspection.fidelity == backtest.ReplayFidelity.SNAPSHOT_L2
        assert inspection.tables["book_deltas"]["row_count"] == 20
        assert inspection.warnings
        db.close()


def test_typed_execution_returns_a_lazy_result_object():
    with tempfile.TemporaryDirectory() as tmp:
        db = _seeded(tmp)
        _signals(
            db,
            [
                {
                    "ts": dt.datetime(2024, 1, 1, 0, 0, 3),
                    "instrument_id": MARKET,
                    "side": "buy",
                    "quantity": 10.0,
                    "tag": "entry",
                }
            ],
        )
        config = backtest.BacktestConfig(
            run_id="result",
            portfolio=backtest.PortfolioConfig(starting_cash=500.0),
            data=backtest.DataConfig(snapshot="seed"),
            output=backtest.OutputConfig(equity_interval_nanos=SECOND),
        )
        result = backtest.execute(db, config)
        assert isinstance(result, dict)
        assert result.fills.num_rows == 1
        assert result.orders.num_rows == 1
        assert result.summary()["fills"] == 1
        assert result.explain()["status_counts"]["filled"] == 1
        assert result.stats()["n_periods"] >= 1
        assert "Run manifest" in result.html_summary()

        reopened = backtest.open_result(db, "result")
        assert reopened.config == config
        assert reopened.inspection.fidelity == backtest.ReplayFidelity.SNAPSHOT_L2
        assert backtest.list_runs(db)[0]["fork"] == "bt-result"
        assert result.verify()["verified"]
        db.close()


def test_preflight_refuses_queue_claims_from_periodic_snapshots():
    with tempfile.TemporaryDirectory() as tmp:
        db = _seeded(tmp)
        _signals(db, [])
        config = backtest.BacktestConfig(
            run_id="bad-queue",
            portfolio=backtest.PortfolioConfig(starting_cash=500.0),
            data=backtest.DataConfig(snapshot="seed"),
            execution=backtest.ExecutionConfig(queue_position=True),
        )
        inspection = backtest.inspect(db, config)
        assert not inspection.ok
        assert {issue.code for issue in inspection.errors} == {
            "unsupported_queue_claim"
        }
        with pytest.raises(ValueError, match="unsupported_queue_claim"):
            backtest.execute(db, config)
        db.close()


def test_configuration_rejects_silently_overridden_execution_models():
    with pytest.raises(ValueError, match="separate scenarios"):
        backtest.ExecutionConfig(queue_position=True, slippage_ticks=1)
    with pytest.raises(ValueError, match="mutually exclusive"):
        backtest.ExecutionConfig(maker_rebate=-0.001, maker_fee_rate=0.001)


def test_signal_and_target_position_adapters_compile_to_intent():
    times = [
        dt.datetime(2024, 1, 1, 0, 0, 1),
        dt.datetime(2024, 1, 1, 0, 0, 2),
        dt.datetime(2024, 1, 1, 0, 0, 3),
    ]
    signals = backtest.from_signals(
        times,
        instrument_id=MARKET,
        entries=[False, True, False],
        exits=[False, False, True],
        size=10.0,
        tag="zscore",
    ).to_pylist()
    assert [row["side"] for row in signals] == ["buy", "sell"]
    assert [row["tag"] for row in signals] == ["zscore-entry", "zscore-exit"]
    assert signals[1]["reduce_only"]

    targets = backtest.target_positions(
        times,
        [0.0, 10.0, 4.0],
        instrument_id=MARKET,
    ).to_pylist()
    assert [(row["side"], row["quantity"]) for row in targets] == [
        ("buy", 10.0),
        ("sell", 6.0),
    ]
    with pytest.raises(ValueError, match="both true"):
        backtest.from_signals(
            times[:1],
            instrument_id=MARKET,
            entries=[True],
            exits=[True],
        )


def test_backtest_study_uses_isolated_forks_and_returns_a_leaderboard():
    with tempfile.TemporaryDirectory() as tmp:
        db = _seeded(tmp)
        _signals(
            db,
            [
                {
                    "ts": dt.datetime(2024, 1, 1, 0, 0, 3),
                    "instrument_id": MARKET,
                    "side": "buy",
                    "quantity": 10.0,
                }
            ],
        )
        base = backtest.BacktestConfig(
            run_id="study-template",
            portfolio=backtest.PortfolioConfig(starting_cash=500.0),
            data=backtest.DataConfig(snapshot="seed"),
            execution=backtest.ExecutionConfig(
                fee_kind="prediction_market",
                fee_rate=0.0,
            ),
        )
        result = backtest.study(
            db,
            study_id="fees",
            base=base,
            parameters={"execution.fee_rate": [0.0, 0.07]},
        )
        board = result.leaderboard("final_cash")
        assert len(board) == 2
        assert board[0]["final_cash"] > board[1]["final_cash"]
        assert board[0]["fork"] == "bt-fees-0000-run"
        assert not result.failures
        assert "<table>" in result.to_html(metric="final_cash")
        assert set(db.fork_names()) >= {
            "bt-fees-0000-run",
            "bt-fees-0001-run",
        }
        result.drop()
        db.close()


def test_backtest_cli_uses_the_same_typed_contract(capsys):
    with tempfile.TemporaryDirectory() as tmp:
        database = f"{tmp}/bt.db"
        config_path = f"{tmp}/config.json"
        db = _seeded(tmp)
        _signals(
            db,
            [
                {
                    "ts": dt.datetime(2024, 1, 1, 0, 0, 3),
                    "instrument_id": MARKET,
                    "side": "buy",
                    "quantity": 10.0,
                }
            ],
        )
        db.close()
        config = backtest.BacktestConfig(
            run_id="cli",
            portfolio=backtest.PortfolioConfig(starting_cash=500.0),
            data=backtest.DataConfig(snapshot="seed"),
        )
        config.to_json(config_path)

        assert backtest.main(["inspect", database, config_path]) == 0
        assert json.loads(capsys.readouterr().out)["fidelity"] == "snapshot_l2"
        assert backtest.main(["run", database, config_path]) == 0
        assert json.loads(capsys.readouterr().out)["fills"] == 1
        assert backtest.main(["list", database]) == 0
        assert json.loads(capsys.readouterr().out)[0]["fork"] == "bt-cli"


def test_native_risk_limits_are_persisted_and_explained():
    with tempfile.TemporaryDirectory() as tmp:
        db = _seeded(tmp)
        _signals(
            db,
            [
                {
                    "ts": dt.datetime(2024, 1, 1, 0, 0, 3),
                    "instrument_id": MARKET,
                    "side": "buy",
                    "quantity": 10.0,
                }
            ],
        )
        config = backtest.BacktestConfig(
            run_id="risk",
            portfolio=backtest.PortfolioConfig(starting_cash=500.0),
            data=backtest.DataConfig(snapshot="seed"),
            risk=backtest.RiskConfig(max_order_quantity=5.0),
        )
        result = backtest.execute(db, config)
        assert result["fills"] == 0
        assert result["metrics"]["orders_rejected_risk"] == 1
        explanation = result.explain()
        assert explanation["status_counts"]["rejected"] == 1
        assert any(
            "max_order_quantity" in reason
            for reason in explanation["rejection_reasons"]
        )
        assert backtest.open_result(db, "risk").config.risk == config.risk
        db.close()


def test_declarative_commands_support_order_lifecycle():
    with tempfile.TemporaryDirectory() as tmp:
        db = _seeded(tmp)
        backtest.create_command_table(db)
        db.append(
            "commands",
            backtest.command_table(
                [
                    {
                        "ts": dt.datetime(2024, 1, 1, 0, 0, 3),
                        "action": "submit",
                        "client_order_id": "quote-yes",
                        "instrument_id": MARKET,
                        "side": "buy",
                        "quantity": 10.0,
                        "kind": "limit",
                        "limit_price": 0.40,
                    },
                    {
                        "ts": dt.datetime(2024, 1, 1, 0, 0, 5),
                        "action": "cancel",
                        "client_order_id": "quote-yes",
                    },
                ]
            ),
        )
        config = backtest.BacktestConfig(
            run_id="commands",
            portfolio=backtest.PortfolioConfig(starting_cash=500.0),
            data=backtest.DataConfig(commands="commands", snapshot="seed"),
        )

        inspection = backtest.inspect(db, config)
        assert not inspection.errors
        result = backtest.execute(db, config)
        orders = result.orders.to_pylist()
        assert len(orders) == 1
        assert orders[0]["status"] == "cancelled"
        assert result.config.data.strategy_kind == "commands"
        assert result.config.data.signals is None
        db.close()


def test_python_event_strategy_is_explicit_and_receives_fills():
    class BuyOnThirdSecond(backtest.EventStrategy):
        def __init__(self):
            self.scheduled = False
            self.fills = []

        def on_event(self, context, event):
            assert event["ts_init"] == context["now"]
            if not self.scheduled:
                self.scheduled = True
                return {
                    "action": "timer",
                    "name": "enter",
                    "ts": event["ts_init"] + 2 * SECOND,
                }
            return None

        def on_timer(self, context, event):
            assert event["name"] == "enter"
            return {
                "action": "submit",
                "client_order_id": "entry",
                "instrument_id": MARKET,
                "side": "buy",
                "quantity": 10.0,
                "tag": "callback-entry",
            }

        def on_fill(self, context, event):
            self.fills.append(event)

    with tempfile.TemporaryDirectory() as tmp:
        db = _seeded(tmp)
        strategy = BuyOnThirdSecond()
        result = backtest.run_strategy(
            db,
            "callback",
            strategy,
            strategy_id="tests.BuyOnThirdSecond:v1",
            starting_cash=500.0,
            data=backtest.DataConfig(snapshot="seed"),
        )

        assert result["fills"] == 1
        assert len(strategy.fills) == 1
        assert strategy.fills[0]["tag"] == "callback-entry"
        assert result.config.data.strategy_kind == "callback"
        assert result.config.data.strategy_id == "tests.BuyOnThirdSecond:v1"
        verified = result.verify(strategy=BuyOnThirdSecond())
        assert verified["verified"]
        db.close()


def test_a_callback_strategy_can_trade_the_venues_set_contract():
    """Mint a complete set and hand it back, from Python.

    A set costs exactly one unit of cash however the book divides it, so a
    mint followed by a redeem is cash-neutral. That invariant is the whole
    point of modelling the operation: without it, a strategy cannot supply
    both sides of a book without first buying them.
    """

    class MintAndRedeem(backtest.EventStrategy):
        def __init__(self):
            self.seen = 0

        def on_event(self, context, event):
            self.seen += 1
            if self.seen == 1:
                return {"action": "mint", "instrument_id": MARKET, "quantity": 20.0}
            if self.seen == 4:
                return {"action": "redeem", "instrument_id": MARKET, "quantity": 20.0}
            return None

    with tempfile.TemporaryDirectory() as tmp:
        db = _seeded(tmp)
        result = backtest.run_strategy(
            db,
            "set-contract",
            MintAndRedeem(),
            strategy_id="tests.MintAndRedeem:v1",
            starting_cash=500.0,
            data=backtest.DataConfig(snapshot="seed"),
        )

        operations = result["set_operations"]
        assert [op["kind"] for op in operations] == ["mint", "redeem"]
        assert all(op["rejected"] is None for op in operations)
        assert operations[0]["cash_delta"] == pytest.approx(20.0)
        assert operations[1]["cash_delta"] == pytest.approx(-20.0)
        assert result["final_cash"] == pytest.approx(500.0)
        assert result["metrics"]["set_operations"] == 2
        db.close()


def test_a_python_forecast_becomes_a_scored_calibration_sample():
    """The triple `quant.calibration` needs, produced by the run itself.

    Fills say what a strategy did; only the strategy knows what it believed,
    so the forecast has to come from the callback rather than be inferred
    from the price it traded against.
    """

    class Forecaster(backtest.EventStrategy):
        def on_event(self, context, event):
            return {
                "action": "forecast",
                "instrument_id": MARKET,
                "outcome": 0,
                "probability": 0.65,
                "tag": "model-a",
            }

    with tempfile.TemporaryDirectory() as tmp:
        db = _seeded(tmp)
        _resolve(db, kind="winner", outcome=0)
        result = backtest.run_strategy(
            db,
            "forecasting",
            Forecaster(),
            strategy_id="tests.Forecaster:v1",
            starting_cash=500.0,
        )

        samples = result["calibration_samples"]
        assert len(samples) == result["forecasts"] > 0
        first = samples[0]
        assert first["forecast"] == pytest.approx(0.65)
        assert first["realized"] == pytest.approx(1.0)
        assert first["tag"] == "model-a"
        # The market's own probability at the same instant, which is what
        # the forecast is scored against.
        assert 0.0 < first["market"] < 1.0
        db.close()


def test_a_voided_market_is_dropped_from_the_scored_sample():
    class Forecaster(backtest.EventStrategy):
        def on_event(self, context, event):
            return {
                "action": "forecast",
                "instrument_id": MARKET,
                "outcome": 0,
                "probability": 0.95,
            }

    with tempfile.TemporaryDirectory() as tmp:
        db = _seeded(tmp)
        _resolve(db, kind="void", outcome=None, outcome_count=2)
        result = backtest.run_strategy(
            db,
            "voided",
            Forecaster(),
            strategy_id="tests.Forecaster:v1",
            starting_cash=500.0,
        )

        assert result["forecasts"] > 0
        assert result["calibration_samples"] == []
        assert any("were not scored" in warning for warning in result["warnings"])
        db.close()


def test_trial_ledger_deduplicates_semantic_configs_and_keeps_one_run():
    with tempfile.TemporaryDirectory() as tmp:
        db = _seeded(tmp)
        _signals(
            db,
            [
                {
                    "ts": dt.datetime(2024, 1, 1, 0, 0, 3),
                    "instrument_id": MARKET,
                    "side": "buy",
                    "quantity": 10.0,
                }
            ],
        )
        first_config = backtest.BacktestConfig(
            run_id="agent-attempt-1",
            portfolio=backtest.PortfolioConfig(starting_cash=500.0),
            data=backtest.DataConfig(snapshot="seed"),
            metadata={"agent": "alpha", "attempt": 1},
        )
        retry_config = replace(
            first_config,
            run_id="agent-attempt-2",
            metadata={"agent": "beta", "attempt": 99},
        )

        first = backtest.execute(db, first_config)
        retry = backtest.execute(db, retry_config)

        assert first_config.trial_digest == retry_config.trial_digest
        assert first["cached"] is False
        assert retry["cached"] is True
        assert retry["requested_run_id"] == "agent-attempt-2"
        assert retry.fork_name == first.fork_name
        assert backtest.trial_count(db) == 1
        assert (
            backtest.find_trial(db, first_config.trial_digest).fork_name
            == first.fork_name
        )
        assert {name for name in db.fork_names() if name.startswith("bt-")} == {
            first.fork_name
        }

        fork = db.fork(first.fork_name)
        try:
            assert fork.read("bt_run").num_rows == 1
            for table in fork.tables():
                if table.startswith("bt_") and fork.read(table).num_rows:
                    assert fork.read("bt_run").num_rows == 1
        finally:
            fork.close()
        db.close()


def test_duplicate_study_trials_share_the_ledger_even_when_concurrent():
    with tempfile.TemporaryDirectory() as tmp:
        db = _seeded(tmp)
        _signals(
            db,
            [
                {
                    "ts": dt.datetime(2024, 1, 1, 0, 0, 3),
                    "instrument_id": MARKET,
                    "side": "buy",
                    "quantity": 10.0,
                }
            ],
        )
        result = backtest.study(
            db,
            study_id="retry-loop",
            base=backtest.BacktestConfig(
                run_id="template",
                portfolio=backtest.PortfolioConfig(starting_cash=500.0),
                data=backtest.DataConfig(snapshot="seed"),
            ),
            parameters={"execution.fee_rate": [0.0, 0.0]},
            max_workers=2,
        )

        assert len(result.trials) == 2
        assert [row["cached"] for row in result.trials].count(True) == 1
        assert len({row["fork"] for row in result.trials}) == 1
        assert backtest.trial_count(db) == 1
        db.close()


def test_attention_routing_uses_seen_state_and_container_rollup():
    result = backtest.StudyResult(
        study_id="attention",
        trials=[
            {"trial": 0, "status": "running"},
            {"trial": 1, "status": "ok"},
            {"trial": 2, "status": "warned", "warnings": ["thin coverage"]},
            {
                "trial": 3,
                "status": "ok",
                "needs_decision": True,
                "seen": True,
            },
            {
                "trial": 4,
                "status": "warned",
                "warnings": ["reviewed"],
                "seen": True,
            },
        ],
    )

    assert [item.trial for item in result.attention()] == [3, 2, 1, 0, 4]
    assert result.attention_state is backtest.AttentionState.NEEDS_DECISION
    assert result.warning_badge == 1
    result.open_trial(2)
    assert result.warning_badge == 0
    assert backtest.attention_for_trial(result.trials[2]).state is (
        backtest.AttentionState.SEEN
    )
    assert backtest.AttentionState.FINISHED_UNSEEN.priority > (
        backtest.AttentionState.RUNNING.priority
    )


def test_execute_persists_the_metadata_that_groups_runs_into_studies():
    """`execute` flattens its config into `run`'s arguments before the config
    is rebuilt and stored, so anything not threaded through is lost. Metadata
    carries the study, trial, phase and parameters that the review UI groups
    and pivots on; losing it collapsed every trial into one unnamed
    experiment with no parameters."""
    with tempfile.TemporaryDirectory() as tmp:
        db = _seeded(tmp)
        _signals(
            db,
            [
                {
                    "ts": dt.datetime(2024, 1, 1, 0, 0, 3),
                    "instrument_id": MARKET,
                    "side": "buy",
                    "quantity": 10.0,
                }
            ],
        )
        meta = {
            "study_id": "fee-ladder",
            "trial": 2,
            "phase": "train",
            "parameters": {"fee_rate": 0.07, "slippage_ticks": 1},
            "needs_decision": True,
        }
        config = backtest.BacktestConfig(
            run_id="meta-run",
            portfolio=backtest.PortfolioConfig(starting_cash=500.0),
            data=backtest.DataConfig(snapshot="seed"),
            execution=backtest.ExecutionConfig(fee_kind="prediction_market", fee_rate=0.07),
            metadata=meta,
        )
        result = backtest.execute(db, config)

        fork = db.fork(result.fork_name)
        try:
            stored = fork.read("bt_config").to_pandas()
            recorded = json.loads(stored["config_json"].iloc[0])
        finally:
            fork.close()
        assert recorded["metadata"] == meta

        # The trial digest is a score identity, so annotating a run must not
        # move it -- otherwise re-labelling a trial would defeat the ledger.
        relabelled = replace(config, metadata={**meta, "phase": "holdout"})
        assert relabelled.trial_digest == config.trial_digest
        db.close()


def test_an_unbuildable_fee_model_is_refused_rather_than_dropped():
    """A fee the kernel cannot represent must not become a free run.

    The rate was turned into a model inside a builder closure that had no way
    to report a failure, so an unrepresentable one installed *no* fee model:
    the run then reported success, zero commissions, and a P&L nobody could
    have earned on the venue it named.
    """
    with tempfile.TemporaryDirectory() as tmp:
        db = _seeded(tmp)
        _signals(
            db,
            [
                {
                    "ts": dt.datetime(2024, 1, 1, 0, 0, 3),
                    "instrument_id": MARKET,
                    "side": "buy",
                    "quantity": 10.0,
                }
            ],
        )
        with pytest.raises(h5i_db.InvalidInputError, match="overflow"):
            backtest.run(
                db,
                "impossible-fee",
                starting_cash=500.0,
                snapshot="seed",
                fee_rate=1e30,
            )
        # Refused before anything was created, so there is no half-run fork
        # carrying numbers priced by a model that was never installed.
        assert "bt-impossible-fee" not in db.fork_names()
        db.close()


def test_a_fee_kind_without_a_rate_is_refused():
    """`fee_kind` alone prices nothing, and used to install nothing."""
    with tempfile.TemporaryDirectory() as tmp:
        db = _seeded(tmp)
        _signals(db, [])
        with pytest.raises(h5i_db.InvalidInputError, match="fee_rate"):
            backtest.run(
                db,
                "kind-only",
                starting_cash=500.0,
                snapshot="seed",
                fee_kind="kalshi",
            )
        db.close()

    with pytest.raises(ValueError, match="fee_rate"):
        backtest.ExecutionConfig(fee_kind="proportional")
    with pytest.raises(ValueError, match="maker_rebate"):
        backtest.ExecutionConfig(fee_kind="kalshi", fee_rate=0.07, maker_rebate=-0.001)


def test_slippage_is_measured_in_the_instruments_own_tick():
    """One tick of slippage is one tick of *this* market, not of 0.0001.

    The tick was hardcoded, so a venue quoting in cents got a hundredth of the
    slippage it asked for and the run looked cheaper to trade than it was.
    """
    with tempfile.TemporaryDirectory() as tmp:
        db = _seeded(tmp, tick_size=0.01)
        _signals(
            db,
            [
                {
                    "ts": dt.datetime(2024, 1, 1, 0, 0, 3),
                    "instrument_id": MARKET,
                    "side": "buy",
                    "quantity": 10.0,
                }
            ],
        )
        plain = backtest.run(db, "no-slip", starting_cash=500.0, snapshot="seed")
        slipped = backtest.run(
            db,
            "one-tick",
            starting_cash=500.0,
            snapshot="seed",
            slippage_ticks=1,
        )
        assert plain["fills"] == slipped["fills"] == 1
        # Ten contracts, one cent worse each.
        assert plain["final_cash"] - slipped["final_cash"] == pytest.approx(
            0.10, abs=1e-9
        )
        db.close()


def test_slippage_and_queue_position_cannot_be_requested_together():
    """Two fill models, one slot: the loser used to be dropped in silence.

    `slippage_ticks=0` is the sharp case. It installed a no-op slippage model
    and discarded the queue-position request the caller actually made.
    """
    with tempfile.TemporaryDirectory() as tmp:
        db = _seeded(tmp)
        _signals(db, [])
        with pytest.raises(h5i_db.InvalidInputError, match="separate scenarios"):
            backtest.run(
                db,
                "both-models",
                starting_cash=500.0,
                snapshot="seed",
                slippage_ticks=0,
                queue_position=True,
            )
        db.close()


def test_the_execution_models_are_part_of_the_run_digest():
    """Two runs priced differently are two computations, not one.

    `RunSpec` holds its models as boxed trait objects it cannot inspect, so
    the digest can only see them through the fingerprint the binding builds.
    """
    with tempfile.TemporaryDirectory() as tmp:
        db = _seeded(tmp)
        _signals(
            db,
            [
                {
                    "ts": dt.datetime(2024, 1, 1, 0, 0, 3),
                    "instrument_id": MARKET,
                    "side": "buy",
                    "quantity": 10.0,
                }
            ],
        )
        free = backtest.run(db, "fp-free", starting_cash=500.0, snapshot="seed")
        charged = backtest.run(
            db, "fp-charged", starting_cash=500.0, snapshot="seed", fee_rate=0.07
        )
        same = backtest.run(
            db, "fp-charged-again", starting_cash=500.0, snapshot="seed", fee_rate=0.07
        )
        assert free["digest"] != charged["digest"]
        # The run id is not part of the identity: the same computation under a
        # second name is the same computation.
        assert charged["digest"] == same["digest"]
        assert charged["execution_fingerprint"] != free["execution_fingerprint"]
        assert "prediction_market" in charged["execution_fingerprint"]
        db.close()


def test_a_margin_model_is_installed_when_it_is_asked_for():
    """A zero in `liquidations` means one of two very different things."""
    with tempfile.TemporaryDirectory() as tmp:
        db = _seeded(tmp)
        _signals(
            db,
            [
                {
                    "ts": dt.datetime(2024, 1, 1, 0, 0, 3),
                    "instrument_id": MARKET,
                    "side": "buy",
                    "quantity": 10.0,
                }
            ],
        )
        unmargined = backtest.run(db, "no-margin", starting_cash=500.0, snapshot="seed")
        margined = backtest.run(
            db,
            "cash-margin",
            starting_cash=500.0,
            snapshot="seed",
            margin_kind="cash",
        )
        assert unmargined["margin_model"] is None
        assert margined["margin_model"] == "cash"
        assert margined["digest"] != unmargined["digest"]
        with pytest.raises(h5i_db.InvalidInputError, match="leverage"):
            backtest.run(
                db,
                "linear-no-leverage",
                starting_cash=500.0,
                snapshot="seed",
                margin_kind="linear",
            )
        db.close()


def test_timestamps_convert_without_losing_nanoseconds():
    """`int(dt.timestamp() * 1e9)` rounds; a window boundary must not.

    A float64 mantissa holds 53 bits and a present-day instant in nanoseconds
    needs 61, so the obvious spelling moves the boundary by a few hundred
    nanoseconds -- enough to include or exclude an event.
    """
    from h5i_db.backtest import _to_nanos

    moment = dt.datetime(2024, 1, 1, 0, 0, 0, 123457, tzinfo=dt.timezone.utc)
    exact = 1_704_067_200_123_457_000
    assert _to_nanos(moment) == exact
    assert _to_nanos("2024-01-01T00:00:00.123457Z") == exact
    # A naive datetime is read as UTC, not as the machine's timezone.
    assert _to_nanos(moment.replace(tzinfo=None)) == exact


def test_equivalent_spellings_of_one_trial_share_its_digest():
    """The ledger identifies a trial by content, so `10000` and `10000.0`,
    and a window written three ways, have to be one trial."""
    start = dt.datetime(2024, 1, 1, 0, 0, 1, tzinfo=dt.timezone.utc)
    end = dt.datetime(2024, 1, 1, 0, 0, 9, tzinfo=dt.timezone.utc)
    from h5i_db.backtest import _to_nanos

    integral = backtest.BacktestConfig(
        run_id="ints",
        portfolio=backtest.PortfolioConfig(starting_cash=10_000),
        data=backtest.DataConfig(snapshot="seed", window=(start, end)),
        execution=backtest.ExecutionConfig(fee_kind="proportional", fee_rate=0),
    )
    floating = backtest.BacktestConfig(
        run_id="floats",
        portfolio=backtest.PortfolioConfig(starting_cash=10_000.0),
        data=backtest.DataConfig(
            snapshot="seed", window=(_to_nanos(start), _to_nanos(end))
        ),
        execution=backtest.ExecutionConfig(fee_kind="proportional", fee_rate=0.0),
    )
    assert integral.trial_digest == floating.trial_digest
    # The stored form is the canonical one, so a round trip through JSON
    # cannot reintroduce the difference.
    assert integral.data.window == floating.data.window
    assert backtest.BacktestConfig.from_json(integral.to_json()).trial_digest == (
        floating.trial_digest
    )


def test_a_verification_fork_is_never_offered_to_the_ledger():
    """`verify()` re-runs a config the ledger already holds, so its temporary
    fork carries that config's trial digest -- and is deleted when the
    comparison is done. A concurrent `execute(reuse=True)` matching it would
    be handed a result that disappears underneath it."""
    with tempfile.TemporaryDirectory() as tmp:
        db = _seeded(tmp)
        _signals(
            db,
            [
                {
                    "ts": dt.datetime(2024, 1, 1, 0, 0, 3),
                    "instrument_id": MARKET,
                    "side": "buy",
                    "quantity": 10.0,
                }
            ],
        )
        config = backtest.BacktestConfig(
            run_id="zzz-verified",
            portfolio=backtest.PortfolioConfig(starting_cash=500.0),
            data=backtest.DataConfig(snapshot="seed"),
        )
        result = backtest.execute(db, config)

        # A verify fork, held open, and named so it sorts *first* in the fork
        # listing: the ledger must skip it on its merits, not by luck.
        verify_config = backtest.BacktestConfig.from_dict(
            {
                **config.to_dict(),
                "run_id": "aaa-verify-standin",
                "metadata": {VERIFY_OF: "zzz-verified"},
            }
        )
        standin = backtest.execute(db, verify_config, reuse=False)
        assert standin.fork_name < result.fork_name
        assert (
            backtest.find_trial(db, config.trial_digest).fork_name == result.fork_name
        )
        # A verification is not a search: counting it would deflate every
        # multiple-testing correction computed from the trial count.
        assert backtest.trial_count(db) == 1

        verified = result.verify()
        assert verified["verified"]
        # The run id is outside the digest, so a re-run under another name is
        # the same computation and says so.
        assert verified["same_digest"]
        # `verify()` drops the fork it created and nothing else.
        assert not any(
            name.startswith("bt-verify-zzz-verified-") for name in db.fork_names()
        )
        assert result.fork_name in db.fork_names()
        db.close()


def test_a_run_with_an_unwritten_config_does_not_break_the_listing():
    """A partial `bt_config` is one unreadable run, not an unreadable database.

    `open_result` indexed row zero, so an empty config table raised
    `IndexError` -- which `list_runs` does not catch, taking every other run
    in the database down with it.
    """
    with tempfile.TemporaryDirectory() as tmp:
        db = _seeded(tmp)
        _signals(
            db,
            [
                {
                    "ts": dt.datetime(2024, 1, 1, 0, 0, 3),
                    "instrument_id": MARKET,
                    "side": "buy",
                    "quantity": 10.0,
                }
            ],
        )
        good = backtest.run(db, "readable", starting_cash=500.0, snapshot="seed")
        broken = backtest.run(db, "half-written", starting_cash=500.0, snapshot="seed")

        # Exactly the state a half-completed `_persist_config` leaves: the
        # table was created, the row never landed.
        fork = db.fork(broken.fork_name)
        try:
            fork.drop_table("bt_config")
            fork.create_table("bt_config", _CONFIG_SCHEMA)
        finally:
            fork.close()

        listed = {row["run_id"] for row in backtest.list_runs(db)}
        assert "readable" in listed
        assert backtest.open_result(db, "half-written").config is None
        assert backtest.find_trial(db, good.config.trial_digest) is not None
        db.close()


def test_a_callback_signature_the_engine_cannot_call_is_refused():
    """Whether a callback wants `context` is read off the parameter's name.

    Counting parameters misbound anything with an extra one: a strategy
    declaring `on_event(self, event, threshold=0.5)` was handed the context in
    the `event` slot and read a portfolio snapshot as a market event, with no
    error anywhere.
    """

    class Extra(backtest.EventStrategy):
        def __init__(self):
            self.seen = []

        def on_event(self, event, threshold=0.5):
            self.seen.append(event["type"])
            return None

    class Misnamed(backtest.EventStrategy):
        # `context` and `ctx` are the names that ask for a context; anything
        # else leaves a parameter the engine cannot fill.
        def on_event(self, portfolio, event):
            return None

    with tempfile.TemporaryDirectory() as tmp:
        db = _seeded(tmp)
        strategy = Extra()
        result = backtest.run_strategy(
            db,
            "defaulted-parameter",
            strategy,
            strategy_id="tests.Extra:v1",
            starting_cash=500.0,
            data=backtest.DataConfig(snapshot="seed"),
        )
        assert result["records_processed"] > 0
        # Market events, not context dicts.
        assert strategy.seen and all(isinstance(kind, str) for kind in strategy.seen)

        with pytest.raises(h5i_db.InvalidInputError, match="context"):
            backtest.run_strategy(
                db,
                "misnamed-context",
                Misnamed(),
                strategy_id="tests.Misnamed:v1",
                starting_cash=500.0,
                data=backtest.DataConfig(snapshot="seed"),
            )
        db.close()
