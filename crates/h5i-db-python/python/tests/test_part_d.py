"""Walk-forward selection, the strategy pack, basket reports, ledger replay.

What these check is the part that makes a search honest: that a holdout is spent
on a shortlist rather than mined by every candidate, that folds are scored by
median so one lucky window cannot carry a candidate, and that a strategy
expressed as data can be reproduced from its own table.
"""

from __future__ import annotations

import datetime as dt
import json
import math
import re
import tempfile
from pathlib import Path

import pyarrow as pa
import pytest

import h5i_db
from h5i_db import backtest, quant, venues

SECOND = 1_000_000_000
MARKETS = ("EVENT-A", "EVENT-B", "EVENT-C", "EVENT-D")


def _panel(tmp: str, *, steps: int = 40, tail: int = 3) -> h5i_db.Database:
    """A small multi-market panel with a resolved tail, built by hand.

    Deterministic and readable on purpose: each market gets its own drift so the
    strategies have something to disagree about, and the tail lets a full-window
    replay settle.
    """
    base = dt.datetime(2026, 5, 1, 12, 0, 0)
    specs = []
    book: dict[str, list] = {name: [] for name in venues.BOOK_DELTAS_SCHEMA.names}
    event = 0
    expiry = base + dt.timedelta(minutes=15 * (steps - 1))
    observable = expiry + dt.timedelta(minutes=45)

    drifts = {"EVENT-A": 0.006, "EVENT-B": -0.004, "EVENT-C": 0.002, "EVENT-D": -0.001}
    winners = {"EVENT-A": 0, "EVENT-B": 1, "EVENT-C": 0, "EVENT-D": 1}
    for name in MARKETS:
        specs.append(
            venues.MarketSpec(
                instrument_id=name,
                venue="example",
                outcome_labels=("Yes", "No"),
                tokens=(f"{name}-yes", f"{name}-no"),
                expiration_ns=int(expiry.replace(tzinfo=dt.timezone.utc).timestamp() * SECOND),
                settlement_observable_ns=int(
                    observable.replace(tzinfo=dt.timezone.utc).timestamp() * SECOND
                ),
                winner_outcome=winners[name],
            )
        )

    def snapshot(at, instrument, outcome, bid, ask, depth_bid, depth_ask):
        nonlocal event
        event += 1
        for position, (side, price, size) in enumerate(
            (("buy", bid, depth_bid), ("sell", ask, depth_ask))
        ):
            book["ts_init"].append(at)
            book["ts_event"].append(at)
            book["instrument_id"].append(instrument)
            book["outcome"].append(outcome)
            book["action"].append("snapshot")
            book["side"].append(side)
            # Snap to the 0.001 tick these markets declare. The engine refuses
            # a limit off the grid, so an off-grid fixture is not a valid book.
            book["price"].append(round(price, 3))
            book["size"].append(size)
            book["event_index"].append(event)
            book["is_last"].append(position == 1)
            book["source_vendor"].append("test")

    for step in range(steps):
        at = base + dt.timedelta(minutes=15 * step)
        for index, name in enumerate(MARKETS):
            mid = 0.5 + drifts[name] * step + 0.01 * math.sin(step / 3 + index)
            mid = min(0.95, max(0.05, mid))
            half = 0.006
            depth_bid = 100.0 + 40.0 * math.sin(step / 4 + index)
            depth_ask = 100.0 + 40.0 * math.cos(step / 5 + index)
            snapshot(at, name, 0, mid - half, mid + half, depth_bid, depth_ask)
            snapshot(at, name, 1, 1 - mid - half, 1 - mid + half, depth_ask, depth_bid)
    for extra in range(tail):
        at = observable + dt.timedelta(minutes=15 * extra)
        for name in MARKETS:
            for outcome in (0, 1):
                won = outcome == winners[name]
                snapshot(
                    at,
                    name,
                    outcome,
                    0.99 if won else 0.001,
                    0.999 if won else 0.01,
                    500.0,
                    500.0,
                )

    db = h5i_db.Database(str(Path(tmp) / "panel.db"), create=True)
    venues.write_markets(db, specs)
    venues.ensure_tables(db, ["book_deltas"])
    db.append(
        "book_deltas",
        pa.table(
            {
                name: pa.array(values, type=venues.BOOK_DELTAS_SCHEMA.field(name).type)
                for name, values in book.items()
            },
            schema=venues.BOOK_DELTAS_SCHEMA,
        ).sort_by([("ts_init", "ascending"), ("event_index", "ascending")]),
    )
    db.snapshot("panel", tables=["instruments", "book_deltas", "resolutions"])
    return db


def _stamps(db) -> list:
    rows = db.sql(
        "SELECT DISTINCT ts_init FROM book_deltas ORDER BY ts_init"
    ).to_pandas()
    return list(rows.ts_init)


def _signal_config(db, table: str, run_id: str) -> backtest.BacktestConfig:
    return backtest.BacktestConfig(
        run_id=run_id,
        data=backtest.DataConfig(signals=table, snapshot="panel"),
        portfolio=backtest.PortfolioConfig(starting_cash=100_000.0),
        execution=backtest.ExecutionConfig(fee_kind="kalshi", fee_rate=0.07),
        output=backtest.OutputConfig(equity_interval_nanos=15 * 60 * SECOND),
    )


# -- D1: walk-forward and selection -----------------------------------------


def test_walk_forward_folds_must_be_ordered_and_score_by_median():
    windows = [
        backtest.ValidationWindows(train=(0, 100), holdout=(100, 200)),
        backtest.ValidationWindows(train=(200, 300), holdout=(300, 400)),
    ]
    walk = backtest.WalkForward.of(*windows)
    assert len(walk) == 2
    # An embargo is reported rather than enforced: only the caller knows the
    # label horizon that would justify one.
    assert windows[0].embargo_ns == 0
    gap = backtest.ValidationWindows(train=(0, 100), holdout=(150, 200))
    assert gap.embargo_ns == 50
    with pytest.raises(ValueError, match="time order"):
        backtest.WalkForward.of(windows[1], windows[0])
    with pytest.raises(ValueError, match="at least one fold"):
        backtest.WalkForward(folds=())


def test_top_k_spends_the_holdout_on_a_shortlist_only():
    with tempfile.TemporaryDirectory() as tmp:
        db = _panel(tmp)
        stamps = _stamps(db)
        panel = backtest.quote_panel(db, snapshot="panel")
        plan = backtest.strategies.late_favorite_hold(
            panel, min_price=0.5, tail_fraction=1.0, quantity=10.0
        )
        db.create_table("signals", plan.signals.schema, time_column="ts")
        db.append("signals", plan.signals)

        walk = backtest.WalkForward.of(
            backtest.ValidationWindows(train=(stamps[0], stamps[12]), holdout=(stamps[12], stamps[20])),
            backtest.ValidationWindows(train=(stamps[20], stamps[30]), holdout=(stamps[30], stamps[-1])),
        )
        result = backtest.study(
            db,
            study_id="topk",
            base=_signal_config(db, "signals", "topk"),
            parameters={"execution.fee_rate": [0.0, 0.02, 0.04, 0.07]},
            validation=walk,
            selection=backtest.TopK(k=2, metric="final_cash"),
        )
        assert len(result.trials) == 4
        # Only the shortlist reached the holdout.
        assert len(result.selected) == 2
        for row in result.trials:
            has_holdout = any(key.startswith("fold0_holdout_") for key in row)
            assert has_holdout == bool(row["selected"])
        # Multi-fold studies carry per-fold columns and the medians over them.
        winner = result.selected[0]
        assert "fold0_train_final_cash" in winner
        assert "fold1_train_final_cash" in winner
        assert winner["train_median_final_cash"] == pytest.approx(
            (winner["fold0_train_final_cash"] + winner["fold1_train_final_cash"]) / 2
        )
        # ranked() applies the study's own policy: holdout median, train tiebreak.
        ranking = result.ranked()
        assert len(ranking) == 2
        assert ranking[0]["holdout_median_final_cash"] >= ranking[1][
            "holdout_median_final_cash"
        ]
        db.close()


def test_a_selection_without_a_holdout_is_refused():
    config = backtest.BacktestConfig(
        run_id="x",
        data=backtest.DataConfig(signals="signals"),
        portfolio=backtest.PortfolioConfig(starting_cash=1.0),
    )
    with pytest.raises(ValueError, match="needs validation windows"):
        backtest.BacktestStudy(
            study_id="s",
            base=config,
            parameters={"execution.fee_rate": [0.0]},
            selection=backtest.TopK(k=1),
        )


def test_single_fold_studies_keep_the_flat_column_names():
    """The columns callers already rank on must not move."""
    with tempfile.TemporaryDirectory() as tmp:
        db = _panel(tmp, steps=24)
        stamps = _stamps(db)
        panel = backtest.quote_panel(db, snapshot="panel")
        plan = backtest.strategies.late_favorite_hold(
            panel, min_price=0.5, tail_fraction=1.0
        )
        db.create_table("signals", plan.signals.schema, time_column="ts")
        db.append("signals", plan.signals)
        result = backtest.study(
            db,
            study_id="flat",
            base=_signal_config(db, "signals", "flat"),
            parameters={"execution.fee_rate": [0.0, 0.07]},
            validation=backtest.ValidationWindows(
                train=(stamps[0], stamps[10]), holdout=(stamps[10], stamps[-1])
            ),
        )
        row = result.trials[0]
        assert "train_final_cash" in row and "holdout_final_cash" in row
        assert not any(key.startswith("fold0_") for key in row)
        db.close()


def test_random_search_is_seeded_and_ranges_need_a_step_for_grids():
    space = {"execution.fee_rate": backtest.Range(0.0, 0.08)}
    first = backtest.RandomSearch(trials=6, seed=7).plan(space)
    second = backtest.RandomSearch(trials=6, seed=7).plan(space)
    third = backtest.RandomSearch(trials=6, seed=8).plan(space)
    assert first == second
    assert first != third
    assert all(0.0 <= point["execution.fee_rate"] <= 0.08 for point in first)
    # A continuous range has nothing to enumerate, so a grid must say so.
    with pytest.raises(ValueError, match="a grid needs a step"):
        backtest.GridSearch().plan(space)
    stepped = backtest.Range(0.0, 0.06, step=0.02)
    assert stepped.enumerate() == pytest.approx([0.0, 0.02, 0.04, 0.06])
    assert backtest.Range(1, 4, step=1, integer=True).enumerate() == [1, 2, 3, 4]
    with pytest.raises(ValueError, match="high must exceed low"):
        backtest.Range(1.0, 1.0)
    with pytest.raises(ValueError, match="strictly positive"):
        backtest.Range(0.0, 1.0, log=True)


def test_random_search_drives_a_real_study():
    with tempfile.TemporaryDirectory() as tmp:
        db = _panel(tmp, steps=24)
        panel = backtest.quote_panel(db, snapshot="panel")
        plan = backtest.strategies.late_favorite_hold(
            panel, min_price=0.5, tail_fraction=1.0
        )
        db.create_table("signals", plan.signals.schema, time_column="ts")
        db.append("signals", plan.signals)
        result = backtest.study(
            db,
            study_id="random",
            base=_signal_config(db, "signals", "random"),
            parameters={"execution.fee_rate": backtest.Range(0.0, 0.08)},
            search=backtest.RandomSearch(trials=4, seed=3),
        )
        assert len(result.trials) == 4
        assert all(row["status"] == "ok" for row in result.trials)
        # Higher fees cannot produce more cash, which is the sanity check that
        # the sampled parameter actually reached the engine.
        board = result.leaderboard("final_cash")
        assert board[0]["final_cash"] >= board[-1]["final_cash"]
        db.close()


def test_tpe_without_optuna_names_the_install():
    search = backtest.TPESearch(trials=2)
    try:
        import optuna  # noqa: F401
    except ImportError:
        with pytest.raises(ImportError, match="pip install optuna"):
            search.sampler()
    else:  # pragma: no cover - only when the extra is installed
        assert search.sampler() is optuna


# -- D2: the strategy pack ---------------------------------------------------


def test_every_panel_strategy_produces_replayable_signals():
    with tempfile.TemporaryDirectory() as tmp:
        db = _panel(tmp)
        panel = backtest.quote_panel(db, snapshot="panel")
        assert set(panel.columns) >= {"ts", "instrument_id", "bid", "ask", "bid_size", "ask_size"}
        # The panel stops at expiry, so no strategy can read the resolution jump.
        expiry = db.sql("SELECT max(expiration_ns) AS e FROM instruments").to_pandas()["e"][0]
        import pandas as pd

        assert panel.ts.max() <= pd.Timestamp(int(expiry), unit="ns")

        produced = {}
        for name, generator in sorted(backtest.STRATEGIES.items()):
            plan = generator(panel)
            produced[name] = plan.num_signals
            assert plan.strategy == name
            assert plan.signals.schema == backtest.SIGNAL_SCHEMA
            if plan.num_signals:
                stamps = plan.signals.column("ts").to_pylist()
                assert stamps == sorted(stamps)
                # Every order is stamped after a quote instant, never on one.
                quote_instants = set(panel.ts)
                assert not any(pd.Timestamp(item) in quote_instants for item in stamps)
        # A pack where nothing fires is not a pack.
        assert sum(produced.values()) > 0
        firing = [name for name, count in produced.items() if count]
        assert len(firing) >= 6, produced
        db.close()


def test_pair_arbitrage_prices_the_fee_curve_into_the_decision():
    with tempfile.TemporaryDirectory() as tmp:
        db = _panel(tmp, steps=16)
        free = backtest.strategies.pair_arbitrage(db, snapshot="panel", fee_rate=0.0)
        charged = backtest.strategies.pair_arbitrage(db, snapshot="panel", fee_rate=0.07)
        # The panel quotes a 1.2-cent-wide pair, so it never clears a 7% fee at
        # even odds; the same book with no fee is a different decision.
        assert charged.num_signals <= free.num_signals
        if free.num_signals:
            legs = free.signals.column("outcome").to_pylist()
            assert set(legs) == {0, 1}
            assert legs.count(0) == legs.count(1)
        with pytest.raises(ValueError, match="two distinct outcomes"):
            backtest.strategies.pair_arbitrage(db, outcomes=(0, 0))
        db.close()


def test_strategy_parameter_validation_refuses_incoherent_rules():
    with tempfile.TemporaryDirectory() as tmp:
        db = _panel(tmp, steps=12)
        panel = backtest.quote_panel(db, snapshot="panel")
        with pytest.raises(ValueError, match="shorter than slow"):
            backtest.strategies.ema_crossover(panel, fast=20, slow=10)
        with pytest.raises(ValueError, match="must be negative"):
            backtest.strategies.mean_reversion(panel, entry_z=1.0)
        with pytest.raises(ValueError, match="oversold < overbought"):
            backtest.strategies.rsi_reversion(panel, oversold=80, overbought=20)
        with pytest.raises(ValueError, match="probability in"):
            backtest.strategies.deep_value(panel, max_price=1.5)
        with pytest.raises(ValueError, match="needs columns"):
            backtest.strategies.microprice_imbalance(panel.drop(columns=["bid_size"]))
        db.close()


def test_a_strategy_plan_round_trips_through_a_run():
    with tempfile.TemporaryDirectory() as tmp:
        db = _panel(tmp)
        panel = backtest.quote_panel(db, snapshot="panel")
        plan = backtest.strategies.late_favorite_hold(
            panel, min_price=0.5, tail_fraction=1.0, quantity=20.0
        )
        assert plan.num_signals > 0
        db.create_table("signals", plan.signals.schema, time_column="ts")
        db.append("signals", plan.signals)
        config = backtest.BacktestConfig(
            run_id="pack",
            data=backtest.DataConfig(signals="signals", snapshot="panel"),
            portfolio=backtest.PortfolioConfig(starting_cash=100_000.0),
            metadata=plan.to_metadata(),
        )
        result = backtest.execute(db, config)
        assert result.summary()["fills"] > 0
        # Signals-as-data means the run is reproducible from its own tables.
        assert result.verify()["verified"]
        db.close()


# -- D1: brier advantage and the basket report ------------------------------


def test_brier_advantage_is_signed_against_the_market():
    # A forecast that is closer to the truth than the price scores positive.
    better = quant.brier_advantage([0.9, 0.1], [0.6, 0.4], [1.0, 0.0])
    assert better.advantage > 0
    assert better.win_rate == 1.0
    assert 0 < better.skill_score <= 1
    worse = quant.brier_advantage([0.2, 0.8], [0.6, 0.4], [1.0, 0.0])
    assert worse.advantage < 0
    # Identical forecasts cannot beat the market.
    same = quant.brier_advantage([0.5, 0.5], [0.5, 0.5], [1.0, 0.0])
    assert same.advantage == pytest.approx(0.0)
    assert same.skill_score == pytest.approx(0.0)
    # The cumulative path is what the report draws.
    assert better.cumulative[-1] == pytest.approx(
        sum(better.advantage_per_observation)
    )
    with pytest.raises(ValueError, match="same length"):
        quant.brier_advantage([0.5], [0.5, 0.5], [1.0, 0.0])
    with pytest.raises(ValueError, match="outcome must be 0 or 1"):
        quant.brier_advantage([0.5], [0.5], [0.4])


def test_brier_decomposition_satisfies_its_identity():
    forecasts = [0.05, 0.15, 0.25, 0.45, 0.55, 0.75, 0.85, 0.95] * 6
    outcomes = [0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0] * 6
    parts = quant.brier_decomposition(forecasts, outcomes)
    assert parts["identity"] == pytest.approx(parts["brier"], abs=0.02)
    assert parts["resolution"] > 0
    curve = quant.reliability_curve(forecasts, outcomes)
    assert len(curve) >= 4
    assert all(row["observations"] > 0 for row in curve)
    assert all(-1.0 <= row["gap"] <= 1.0 for row in curve)


def test_the_two_bucketed_views_agree_about_the_same_forecasts():
    """A forecast outside the edges belongs to neither view, so both say so.

    It used to land in the top bucket in the decomposition and be dropped
    from the curve, so the same input produced two different accounts of it.
    """
    forecasts = [0.05, 0.3, 0.62, 0.91]
    outcomes = [0.0, 1.0, 1.0, 1.0]
    narrow = (0.1, 0.5, 0.9)
    with pytest.raises(ValueError, match="outside the bucket edges"):
        quant.brier_decomposition(forecasts, outcomes, edges=narrow)
    with pytest.raises(ValueError, match="outside the bucket edges"):
        quant.reliability_curve(forecasts, outcomes, edges=narrow)
    # Edges that cover them: every observation lands in exactly one bucket,
    # and the two views count the same ones.
    wide = (0.0, 0.5, 1.0)
    parts = quant.brier_decomposition(forecasts, outcomes, edges=wide)
    curve = quant.reliability_curve(forecasts, outcomes, edges=wide)
    assert sum(row["observations"] for row in curve) == parts["observations"]


def test_probability_series_can_arrive_as_arrays():
    """`if not strategy:` raises on anything numpy-shaped; len() does not."""
    np = pytest.importorskip("numpy")
    scored = quant.brier_advantage(
        np.array([0.9, 0.1]), np.array([0.6, 0.4]), np.array([1.0, 0.0])
    )
    assert scored.observations == 2
    with pytest.raises(ValueError, match="empty"):
        quant.brier_advantage(np.array([]), np.array([]), np.array([]))


def test_basket_report_renders_from_stored_tables_only():
    with tempfile.TemporaryDirectory() as tmp:
        db = _panel(tmp)
        panel = backtest.quote_panel(db, snapshot="panel")
        runs = {}
        for index, threshold in enumerate((0.45, 0.50, 0.55)):
            plan = backtest.strategies.late_favorite_hold(
                panel, min_price=threshold, tail_fraction=1.0, quantity=10.0
            )
            if not plan.num_signals:
                continue
            table = f"signals_{index}"
            db.create_table(table, plan.signals.schema, time_column="ts")
            db.append(table, plan.signals)
            runs[f"th{int(threshold * 100)}"] = backtest.execute(
                db, _signal_config(db, table, f"run-{index}")
            )
        assert len(runs) >= 2

        report = quant.basket_payload(
            db,
            runs,
            basket_id="favorites",
            panels=quant.PORTFOLIO_PANELS + ("equity", "price", "allocation"),
            snapshot="panel",
        )
        assert report.totals["runs"] == len(runs)
        assert report.totals["equity_start"] is not None
        assert "total_equity" in report.panels
        # Fill markers hang off the price panel, keyed by instrument.
        assert report.panels["price"]["paths"]
        assert any(report.panels["price"]["fills"].values())
        # The basket total holds each run's last value between its own samples,
        # so it never dips as runs come online.
        series = report.panels["total_equity"]["series"]
        assert len(series) > 1
        assert min(row["equity"] for row in series) > 0

        document = report.to_html()
        assert "<svg" in document and "favorites" in document
        assert "Basket equity" in document
        # Self-contained: no external fetches, and the payload travels with it.
        assert "http://" not in document and "https://" not in document
        assert 'id="payload"' in document or "id='payload'" in document

        # brier_advantage needs probabilities nobody else can infer.
        skipped = quant.basket_payload(
            db, runs, panels=("brier_advantage",), snapshot="panel"
        )
        assert skipped.skipped[0]["reason"] == "no_probabilities_supplied"
        scored = quant.basket_payload(
            db,
            runs,
            panels=("brier_advantage",),
            snapshot="panel",
            brier={
                label: {"strategy": [0.8, 0.2], "market": [0.6, 0.4], "outcome": [1.0, 0.0]}
                for label in runs
            },
        )
        assert scored.panels["brier_advantage"]["runs"]
        with pytest.raises(ValueError, match="unknown panels"):
            quant.basket_payload(db, runs, panels=("nonexistent",))
        db.close()


def test_per_run_panels_are_dropped_loudly_when_the_basket_is_large():
    with tempfile.TemporaryDirectory() as tmp:
        db = _panel(tmp, steps=16)
        panel = backtest.quote_panel(db, snapshot="panel")
        plan = backtest.strategies.late_favorite_hold(
            panel, min_price=0.5, tail_fraction=1.0
        )
        db.create_table("signals", plan.signals.schema, time_column="ts")
        db.append("signals", plan.signals)
        runs = {
            f"r{index}": backtest.execute(db, _signal_config(db, "signals", f"big-{index}"))
            for index in range(3)
        }
        report = quant.basket_payload(
            db, runs, panels=("total_equity", "equity"), per_run_limit=2
        )
        assert "equity" not in report.drawn
        dropped = [item for item in report.skipped if item["panel"] == "equity"]
        assert dropped and dropped[0]["reason"] == "too_many_runs"
        db.close()


def test_the_basket_payload_is_json_inside_the_script_tag_it_travels_in():
    """`JSON.parse(textContent)` has to see JSON, not HTML entities.

    Escaped with `html.escape`, every quote in the payload arrives as
    `&quot;` and the parse throws; escaping only the angle brackets keeps the
    parser out of the string while leaving it valid JSON.
    """
    report = quant.BasketReport(
        basket_id="tags <b>&</b>",
        runs=[{"label": "r0 <script>"}],
        totals={"runs": 1},
    )
    document = report.to_html()
    payload = re.search(
        r"<script type='application/json' id='payload'>(.*)</script>",
        document,
        re.DOTALL,
    )
    assert payload is not None
    parsed = json.loads(payload.group(1))
    assert parsed["basket_id"] == "tags <b>&</b>"
    # The parser must never see a tag inside the script element.
    assert "<" not in payload.group(1)


# -- the price panel under the fill markers ---------------------------------


def _depth_book(tmp: str, instrument: str = "EVENT-A") -> h5i_db.Database:
    """A book with real depth, a deleted level, and a clear, written by hand.

    `book_deltas` is one row per level, so a panel that reduces a side with
    the wrong aggregate reads a price nobody quoted. Every number here is
    chosen so the right answer differs from the wrong ones: the best bid is
    not the only bid, the best ask is not the widest ask, and the deleted ask
    is the cheapest row in the table.
    """
    base = dt.datetime(2026, 6, 1, 9, 0, 0)
    rows: list[tuple] = [
        # (offset, action, side, price)
        (0, "snapshot", "buy", 0.52),
        (0, "snapshot", "buy", 0.45),
        (0, "snapshot", "sell", 0.54),
        (0, "snapshot", "sell", 0.70),
        (1, "set", "buy", 0.53),
        (1, "set", "sell", 0.56),
        # Cheaper than every live ask, and gone: a panel that ignores `action`
        # prices the book off a level that no longer exists.
        (1, "delete", "sell", 0.50),
        (1, "clear", None, None),
    ]
    book: dict[str, list] = {name: [] for name in venues.BOOK_DELTAS_SCHEMA.names}
    for index, (offset, action, side, price) in enumerate(rows):
        at = base + dt.timedelta(minutes=offset)
        book["ts_init"].append(at)
        book["ts_event"].append(at)
        book["instrument_id"].append(instrument)
        book["outcome"].append(0)
        book["action"].append(action)
        book["side"].append(side)
        book["price"].append(price)
        book["size"].append(None if price is None else 100.0)
        book["event_index"].append(index)
        book["is_last"].append(True)
        book["source_vendor"].append("test")

    db = h5i_db.Database(str(Path(tmp) / "depth.db"), create=True)
    venues.ensure_tables(db, ["book_deltas"])
    db.append(
        "book_deltas",
        pa.table(
            {
                name: pa.array(values, type=venues.BOOK_DELTAS_SCHEMA.field(name).type)
                for name, values in book.items()
            },
            schema=venues.BOOK_DELTAS_SCHEMA,
        ).sort_by([("ts_init", "ascending"), ("event_index", "ascending")]),
    )
    db.snapshot("depth", tables=["book_deltas"])
    return db


def test_the_price_panel_mids_the_best_bid_against_the_best_ask():
    """The worst ask is not a price; neither is a level that was deleted."""
    with tempfile.TemporaryDirectory() as tmp:
        db = _depth_book(tmp)
        paths = quant.basket._price_path(
            db, ["EVENT-A"], snapshot="depth", outcome=0, max_points=100
        )
        db.close()
    mids = [point["mid"] for point in paths["EVENT-A"]]
    assert len(mids) == 2
    # (0.52 + 0.54) / 2, not (0.52 + 0.70) / 2: max() over the sell side is the
    # widest quote in the book, not the top of it.
    assert mids[0] == pytest.approx(0.53)
    # (0.53 + 0.56) / 2, not (0.53 + 0.50) / 2: the 0.50 ask was deleted.
    assert mids[1] == pytest.approx(0.545)


def test_the_price_panel_quotes_the_instruments_it_is_given():
    """An instrument id is vendor data, so it goes through the quoter."""
    hostile = "EVT-A'; DROP TABLE book_deltas; --"
    with tempfile.TemporaryDirectory() as tmp:
        db = _depth_book(tmp, instrument=hostile)
        paths = quant.basket._price_path(
            db, [hostile], snapshot="depth", outcome=0, max_points=100
        )
        still_there = db.sql("SELECT count(*) AS n FROM book_deltas").to_pandas()["n"][0]
        db.close()
    assert [point["mid"] for point in paths[hostile]] == [
        pytest.approx(0.53),
        pytest.approx(0.545),
    ]
    assert still_there == 8


# -- D2: account ledger replay ----------------------------------------------


def test_ledger_replay_lets_the_book_refuse_and_reports_where():
    with tempfile.TemporaryDirectory() as tmp:
        db = _panel(tmp)
        stamps = _stamps(db)
        specs = [
            venues.MarketSpec(
                instrument_id=name,
                venue="example",
                outcome_labels=("Yes", "No"),
                tokens=(f"{name}-yes", f"{name}-no"),
            )
            for name in MARKETS
        ]
        quotes = db.sql(
            f"""
            SELECT instrument_id,
                   max(CASE WHEN side='sell' THEN price END) AS ask
            FROM h5i('book_deltas', 'panel')
            WHERE outcome = 0 AND ts_init = to_timestamp_nanos({int(stamps[6].value)})
            GROUP BY instrument_id ORDER BY instrument_id
            """
        ).to_pandas()

        rows = []
        for record in quotes.itertuples():
            # One achievable price and one that the book never offered.
            rows.append(
                {
                    "timestamp": int(stamps[6].value),
                    "asset_id": f"{record.instrument_id}-yes",
                    "side": "buy",
                    "size": 5.0,
                    "price": float(record.ask),
                    "transaction_hash": f"0x{record.instrument_id}-fair",
                }
            )
        rows.append(
            {
                "timestamp": int(stamps[6].value),
                "asset_id": "EVENT-A-yes",
                "side": "buy",
                "size": 5.0,
                "price": 0.01,
                "transaction_hash": "0ximpossible",
            }
        )
        commands = venues.commands_from_ledger(rows, specs)
        assert commands.num_rows == len(rows)
        assert set(commands.column("time_in_force").to_pylist()) == {"ioc"}
        assert set(commands.column("kind").to_pylist()) == {"limit"}

        backtest.create_command_table(db, "commands")
        db.append("commands", commands)
        result = backtest.execute(
            db,
            backtest.BacktestConfig(
                run_id="ledger",
                data=backtest.DataConfig(commands="commands", snapshot="panel"),
                portfolio=backtest.PortfolioConfig(starting_cash=100_000.0),
            ),
        )
        typed = venues._ledger._coerce_rows(rows, specs)
        comparison = venues.compare_to_ledger(result, typed)
        assert comparison["ledger_rows"] == len(rows)
        # The achievable orders filled; the 1-cent limit did not, so the replay
        # reproduces less than the ledger and says which market fell short.
        assert 0 < comparison["fill_ratio"] < 1
        assert comparison["markets_reproduced"] < len(comparison["markets"])
        shortfall = [row for row in comparison["markets"] if not row["reproduced"]]
        assert shortfall and shortfall[0]["instrument_id"] == "EVENT-A"
        db.close()


def test_ledger_rows_refuse_what_cannot_be_resolved():
    specs = [
        venues.MarketSpec(
            instrument_id="EVENT-A",
            venue="example",
            outcome_labels=("Yes", "No"),
            tokens=("EVENT-A-yes", "EVENT-A-no"),
        )
    ]
    with pytest.raises(KeyError, match="cannot resolve to a market"):
        venues._ledger._coerce_rows(
            [{"timestamp": 1, "asset_id": "unknown", "side": "buy", "size": 1, "price": 0.5}],
            specs,
        )
    with pytest.raises(KeyError, match="names none"):
        venues._ledger._coerce_rows(
            [
                {
                    "timestamp": 1,
                    "instrument_id": "EVENT-A",
                    "side": "buy",
                    "size": 1,
                    "price": 0.5,
                }
            ],
            specs,
        )
    with pytest.raises(ValueError, match="outside"):
        venues.LedgerRow(
            ts_ns=1, instrument_id="EVENT-A", outcome=0, side="buy", quantity=1, price=1.5
        )
    with pytest.raises(ValueError, match="side must be"):
        venues.LedgerRow(
            ts_ns=1, instrument_id="EVENT-A", outcome=0, side="hold", quantity=1, price=0.5
        )
    # An outcome named by label resolves positionally.
    resolved = venues._ledger._coerce_rows(
        [
            {
                "timestamp": 1,
                "instrument_id": "EVENT-A",
                "outcome": "No",
                "side": "sell",
                "size": 2,
                "price": 0.4,
            }
        ],
        specs,
    )
    assert resolved[0].outcome == 1


def test_basket_net_counts_closed_round_trips_not_just_settlement():
    """`net` must be realized + settlement, not settlement - commissions.

    The two agree only when nothing closed, so a hold-to-resolution book hides
    the bug and a strategy that round-trips shows it. Guarded because the wrong
    form is the intuitive one and was shipped once.
    """
    with tempfile.TemporaryDirectory() as tmp:
        db = _panel(tmp)
        panel = backtest.quote_panel(db, snapshot="panel")
        # A rule that opens *and closes* positions, so realized_pnl is not
        # merely the negative of commissions.
        plan = backtest.strategies.ema_crossover(panel, fast=3, slow=8, quantity=10.0)
        assert plan.num_signals > 4
        db.create_table("signals_rt", plan.signals.schema, time_column="ts")
        db.append("signals_rt", plan.signals)
        result = backtest.execute(db, _signal_config(db, "signals_rt", "round-trip"))

        summary = result.summary()
        positions = result.positions.to_pandas()
        settled = float(positions.settlement_pnl.fillna(0.0).sum())
        realized = float(summary["realized_pnl"])
        commissions = float(summary["commissions"])
        assert result.fills.num_rows > 4

        report = quant.basket_payload(db, {"rt": result}, panels=("leaderboard",))
        reported = report.runs[0]["net"]
        assert reported == pytest.approx(realized + settled)

        # And the wrong form really is different here, which is what makes this
        # test meaningful rather than tautological.
        if abs(realized + commissions) > 1e-9:
            assert reported != pytest.approx(settled - commissions)
        db.close()


def test_the_quote_panel_tops_the_book_the_way_the_report_does():
    """The panel and the report chart have to describe the same book.

    `quote_panel` reduced the sell side with `max`, so it quoted the *worst*
    ask in the market and fed that to `_mid` and therefore to every built-in
    signal generator, while `basket._price_path` had already been corrected to
    the lowest live sell. The report chart and the signals then disagreed
    about the same instant.
    """
    with tempfile.TemporaryDirectory() as tmp:
        db = _depth_book(tmp)
        panel = backtest.quote_panel(db, snapshot="depth")
        db.close()
    assert len(panel) == 2
    # 0.54, not 0.70: the widest quote in the book is not the top of it.
    assert list(panel["ask"]) == [pytest.approx(0.54), pytest.approx(0.56)]
    # 0.53 at the second instant, and the deleted 0.50 ask is not an ask.
    assert list(panel["bid"]) == [pytest.approx(0.52), pytest.approx(0.53)]


def test_a_zero_size_set_is_a_delete_to_the_panel_too():
    """The feed spells a delete two ways and the action filter sees only one.

    A `set` of size zero is applied as a delete by the kernel's book
    (`crates/h5i-db-backtest/src/book.rs`), so a panel filtering on `action`
    alone still prices the market off a level that is not there.
    """
    at = dt.datetime(2026, 6, 1, 9, 1, 0)
    row = {
        "ts_init": at,
        "ts_event": at,
        "instrument_id": "EVENT-A",
        "outcome": 0,
        "action": "set",
        "side": "sell",
        "price": 0.30,
        "size": 0.0,
        "event_index": 8,
        "is_last": True,
        "source_vendor": "test",
    }
    with tempfile.TemporaryDirectory() as tmp:
        db = _depth_book(tmp)
        db.append(
            "book_deltas",
            pa.table(
                {
                    name: pa.array(
                        [row[name]], type=venues.BOOK_DELTAS_SCHEMA.field(name).type
                    )
                    for name in venues.BOOK_DELTAS_SCHEMA.names
                },
                schema=venues.BOOK_DELTAS_SCHEMA,
            ),
        )
        panel = backtest.quote_panel(db)
        paths = quant.basket._price_path(
            db, ["EVENT-A"], snapshot=None, outcome=0, max_points=100
        )
        db.close()
    # 0.56 is the cheapest ask anyone can actually lift; 0.30 was withdrawn.
    assert list(panel["ask"])[-1] == pytest.approx(0.56)
    assert [point["mid"] for point in paths["EVENT-A"]][-1] == pytest.approx(0.545)


def test_the_basket_payload_survives_the_panels_it_is_rendered_beside():
    """The escaped JSON has to reach the script tag, not be shadowed on the way.

    The panel loop bound the same name as the payload, so what the block
    emitted was the last drawn panel's Python dict repr: not JSON at all, and
    with every `<` in an instrument or strategy name unescaped, which is a
    live injection into the document.
    """
    report = quant.BasketReport(
        basket_id="basket",
        runs=[{"label": "r0"}],
        totals={"runs": 1},
        panels={"leaderboard": {"rows": [{"run": "<img src=x onerror=alert(1)>"}]}},
        drawn=("leaderboard",),
    )
    document = report.to_html()
    block = re.search(
        r"<script type='application/json' id='payload'>(.*)</script>",
        document,
        re.DOTALL,
    )
    assert block is not None
    parsed = json.loads(block.group(1))
    assert parsed["basket_id"] == "basket"
    assert parsed["panels"]["leaderboard"]["rows"][0]["run"].startswith("<img")
    # Nothing the HTML parser could read as a tag survives inside the element.
    assert "<" not in block.group(1)
