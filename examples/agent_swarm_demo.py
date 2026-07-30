#!/usr/bin/env python
"""A fleet of agents researching strategies, and the review surface for it.

What this shows, in about a minute:

    three agents work the same pinned dataset in parallel, each running a
    different kind of study -- a threshold sweep, an execution-cost sensitivity
    check, and a walk-forward validation -- and every trial lands in the
    database as its own fork with its own result tables. Then the UI opens on
    the pile and sorts it by what needs a human next.

The point is not that backtests run. It is what happens when *many* of them run
and nobody is watching each one:

- every trial is a fork, so trials cannot contaminate each other and a bad one
  is dropped rather than unwound;
- a trial is identified by the hash of its inputs, so an agent that retries a
  config it already ran gets the recorded result back instead of burning a
  second run (demonstrated below, not asserted);
- runs that hit something a human should decide are flagged *by the agent*, and
  the review surface floats them above the merely-finished ones;
- warnings the engine raises, such as a position it refused to settle, travel
  with the trial instead of scrolling past in a log.

Run it:

    python examples/agent_swarm_demo.py                # build, run, open the UI
    python examples/agent_swarm_demo.py --no-ui        # just the runs
    python examples/agent_swarm_demo.py --keep         # keep the database

Needs the `h5i_db` package importable and the `h5i-db` binary on PATH or under
`target/{debug,release}`. Nothing here reaches the network.
"""

from __future__ import annotations

import argparse
import datetime as dt
import math
import random
import re
import shutil
import subprocess
import sys
import threading
import time
import webbrowser
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional, Sequence

import pyarrow as pa

import h5i_db
from h5i_db import backtest

REPO = Path(__file__).resolve().parent.parent
SECOND_NS = 1_000_000_000

# ---------------------------------------------------------------- the market

BOOK_SCHEMA = pa.schema(
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
INSTRUMENT_SCHEMA = pa.schema(
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
RESOLUTION_SCHEMA = pa.schema(
    [
        pa.field("ts_init", pa.timestamp("ns"), nullable=False),
        pa.field("instrument_id", pa.string(), nullable=False),
        pa.field("winner_outcome", pa.uint16(), nullable=False),
    ]
)

TICK = 0.001


def _snap(value: float) -> float:
    """Prices live on the venue's tick grid; the engine refuses limits off it."""
    return round(round(value / TICK) * TICK, 4)


def build_panel(markets: int, steps: int, seed: int) -> dict[str, pa.Table]:
    """A panel of binary event contracts with a known answer.

    Deliberately small and deliberately honest: quotes are a mean-reverting walk
    around a latent probability, spreads widen at the tails the way real event
    books do, and each market resolves after trading stops. Nothing here is
    tuned to make a strategy look good.
    """
    rng = random.Random(seed)
    base = dt.datetime(2026, 3, 2, 13, 30, 0)
    step = dt.timedelta(minutes=5)
    expiry = base + step * (steps - 1)
    observable = expiry + step * 2

    def nanos(moment: dt.datetime) -> int:
        return int(moment.replace(tzinfo=dt.timezone.utc).timestamp() * SECOND_NS)

    questions = [
        "Fed holds rates at the March meeting",
        "CPI prints below 3.0% year over year",
        "Nonfarm payrolls beat consensus",
        "Brent closes above $95 this month",
        "ECB cuts before the Fed",
        "Unemployment ticks up in the next print",
        "Ten-year yield ends the week above 4%",
        "Jobless claims come in under 220k",
    ]
    book: dict[str, list] = {name: [] for name in BOOK_SCHEMA.names}
    instruments: dict[str, list] = {name: [] for name in INSTRUMENT_SCHEMA.names}
    resolutions: dict[str, list] = {name: [] for name in RESOLUTION_SCHEMA.names}
    event = 0

    plan = []
    for index in range(markets):
        p_true = 0.08 + 0.84 * (index + 0.5) / markets
        plan.append(
            {
                "id": f"EVT-{index:03d}",
                "question": questions[index % len(questions)],
                "p_true": p_true,
                "mid": p_true,
                "phase": rng.random() * math.tau,
                "wins": rng.random() < p_true,
            }
        )

    for market in plan:
        for outcome, label in ((0, "YES"), (1, "NO")):
            instruments["ts_init"].append(base)
            instruments["instrument_id"].append(market["id"])
            instruments["venue"].append("demo-exchange")
            instruments["kind"].append("prediction_market")
            instruments["outcome"].append(outcome)
            instruments["outcome_label"].append(label)
            instruments["tick_size"].append(TICK)
            instruments["lot_size"].append(1.0)
            instruments["expiration_ns"].append(nanos(expiry))
            instruments["settlement_observable_ns"].append(nanos(observable))
        resolutions["ts_init"].append(observable)
        resolutions["instrument_id"].append(market["id"])
        resolutions["winner_outcome"].append(0 if market["wins"] else 1)

    def emit(at, instrument, outcome, bid, ask, bid_size, ask_size):
        nonlocal event
        event += 1
        for position, (side, price, size) in enumerate(
            (("buy", bid, bid_size), ("sell", ask, ask_size))
        ):
            book["ts_init"].append(at)
            book["ts_event"].append(at)
            book["instrument_id"].append(instrument)
            book["outcome"].append(outcome)
            book["action"].append("snapshot")
            book["side"].append(side)
            book["price"].append(_snap(price))
            book["size"].append(size)
            book["event_index"].append(event)
            book["is_last"].append(position == 1)
            book["source_vendor"].append("demo")

    for index in range(steps):
        at = base + step * index
        for market in plan:
            drift = 0.12 * (market["p_true"] - market["mid"])
            shock = (rng.random() - 0.5) * 0.05
            market["mid"] = min(
                0.97,
                max(0.03, market["mid"] + drift + shock * math.sqrt(
                    market["mid"] * (1 - market["mid"])
                )),
            )
            mid = market["mid"]
            half = 0.004 + 0.012 * abs(mid - 0.5)
            depth = 140.0 + 60.0 * math.sin(market["phase"] + index / 4)
            ask_depth = 140.0 + 60.0 * math.cos(market["phase"] + index / 6)
            emit(at, market["id"], 0, mid - half, mid + half, depth, ask_depth)
            emit(at, market["id"], 1, 1 - mid - half, 1 - mid + half, ask_depth, depth)

    # After the result is observable the book collapses onto the winner. Without
    # these rows a replay could never reach settlement, and the demo's point
    # about refused settlement would be a fabrication rather than a fact.
    for tail in range(2):
        at = observable + step * tail
        for market in plan:
            winner = 0 if market["wins"] else 1
            for outcome in (0, 1):
                won = outcome == winner
                emit(at, market["id"], outcome,
                     0.99 if won else 0.001, 0.999 if won else 0.01, 600.0, 600.0)

    return {
        "instruments": pa.table(
            {k: pa.array(v, type=INSTRUMENT_SCHEMA.field(k).type)
             for k, v in instruments.items()}, schema=INSTRUMENT_SCHEMA
        ).sort_by([("ts_init", "ascending"), ("instrument_id", "ascending"),
                   ("outcome", "ascending")]),
        "book_deltas": pa.table(
            {k: pa.array(v, type=BOOK_SCHEMA.field(k).type) for k, v in book.items()},
            schema=BOOK_SCHEMA,
        ),
        "resolutions": pa.table(
            {k: pa.array(v, type=RESOLUTION_SCHEMA.field(k).type)
             for k, v in resolutions.items()}, schema=RESOLUTION_SCHEMA
        ).sort_by([("ts_init", "ascending"), ("instrument_id", "ascending")]),
        "_questions": {market["id"]: market["question"] for market in plan},
    }


# ------------------------------------------------------------------- agents


@dataclass
class Trial:
    """One unit of work an agent decided to run."""

    agent: str
    study_id: str
    run_id: str
    signals_table: str
    parameters: dict[str, Any]
    trial_index: int
    phase: str = "run"
    window: Optional[tuple] = None
    needs_decision: bool = False
    reason: str = ""
    fee_rate: float = 0.07
    slippage_ticks: Optional[int] = None


@dataclass
class Outcome:
    trial: Trial
    fills: int = 0
    net: float = 0.0
    cached: bool = False
    warnings: int = 0
    settled: int = 0
    refused: int = 0
    error: str = ""
    seconds: float = 0.0


class Console:
    """Thread-safe, aligned progress. Agents write to it as work completes."""

    COLOURS = {"quant-1": "\033[36m", "quant-2": "\033[35m", "risk-1": "\033[33m"}
    RESET = "\033[0m"

    def __init__(self, colour: bool) -> None:
        self._lock = threading.Lock()
        self._colour = colour

    def line(self, agent: str, message: str) -> None:
        with self._lock:
            tint = self.COLOURS.get(agent, "") if self._colour else ""
            reset = self.RESET if self._colour and tint else ""
            stamp = dt.datetime.now().strftime("%H:%M:%S")
            print(f"  {stamp}  {tint}{agent:<8}{reset} {message}", flush=True)

    def rule(self, title: str) -> None:
        with self._lock:
            print(f"\n\033[1m{title}\033[0m" if self._colour else f"\n{title}", flush=True)


def run_trial(db_path: str, trial: Trial, console: Console) -> Outcome:
    """Execute one trial in its own database handle.

    Each agent opens its own handle rather than sharing one, which is how
    separate processes would do it and keeps the demo honest about concurrency.
    """
    started = time.monotonic()
    db = h5i_db.Database(db_path)
    try:
        execution = backtest.ExecutionConfig(
            fee_kind="kalshi",
            fee_rate=trial.fee_rate,
            slippage_ticks=trial.slippage_ticks,
        )
        config = backtest.BacktestConfig(
            run_id=trial.run_id,
            data=backtest.DataConfig(
                signals=trial.signals_table, snapshot="research-pin", window=trial.window
            ),
            portfolio=backtest.PortfolioConfig(starting_cash=250_000.0),
            execution=execution,
            output=backtest.OutputConfig(equity_interval_nanos=5 * 60 * SECOND_NS),
            metadata={
                "study_id": trial.study_id,
                "trial": trial.trial_index,
                "phase": trial.phase,
                "agent": trial.agent,
                "parameters": trial.parameters,
                "needs_decision": trial.needs_decision,
                "reason": trial.reason,
            },
        )
        result = backtest.execute(db, config)
        summary = result.summary()
        positions = result.positions.to_pandas()
        settled = int(positions.settlement_pnl.notna().sum()) if len(positions) else 0
        # realized_pnl is already net of commissions, so the total is
        # realized + settlement. Subtracting commissions again would
        # double-count them, and using settlement alone would drop every closed
        # round trip -- which is most of what a crossover strategy does.
        net = float(summary.get("realized_pnl") or 0.0) + float(
            positions.settlement_pnl.fillna(0.0).sum()
        )
        outcome = Outcome(
            trial=trial,
            # bt_fills is authoritative, and a result served from the ledger is
            # rebuilt from stored tables rather than from a fresh run payload,
            # so counting the table is right for both paths.
            fills=int(result.fills.num_rows),
            net=net,
            cached=bool(result.get("cached", False)),
            warnings=len(result.get("warnings", []) or []),
            settled=settled,
            refused=len(positions) - settled,
            seconds=time.monotonic() - started,
        )
        flag = " [CACHED]" if outcome.cached else ""
        decision = "  <needs human sign-off>" if trial.needs_decision else ""
        console.line(
            trial.agent,
            f"{trial.run_id:<26} fills={outcome.fills:<4} net={outcome.net:>9,.2f}"
            f"  {outcome.seconds:4.1f}s{flag}{decision}",
        )
        return outcome
    except Exception as error:  # a failed trial is data, not a crash
        console.line(trial.agent, f"{trial.run_id:<26} FAILED: {error}")
        return Outcome(trial=trial, error=str(error), seconds=time.monotonic() - started)
    finally:
        db.close()


def plan_work(db: Any, panel: Any, console: Console) -> list[Trial]:
    """What each agent decided to try, and why.

    Three agents with genuinely different jobs, which is what makes the review
    surface worth having: one is exploring, one is stress-testing an assumption,
    and one is validating a candidate before it can be promoted.
    """
    trials: list[Trial] = []

    # quant-1 sweeps an entry threshold. Each threshold is a different signals
    # table, because the signals are the strategy: a study cannot vary them.
    console.line("quant-1", "exploring: favourite-momentum entry threshold")
    for index, threshold in enumerate((0.55, 0.62, 0.70, 0.78)):
        plan = backtest.strategies.late_favorite_hold(
            panel, min_price=threshold, tail_fraction=0.5, quantity=40.0
        )
        table = f"signals_fav_{int(threshold * 100)}"
        if plan.num_signals == 0:
            continue
        db.create_table(table, plan.signals.schema, time_column="ts")
        db.append(table, plan.signals, note=f"late_favorite_hold min_price={threshold}")
        trials.append(
            Trial(
                agent="quant-1",
                study_id="favourite-threshold",
                run_id=f"fav-th{int(threshold * 100)}",
                signals_table=table,
                parameters={"min_price": threshold, "strategy": plan.strategy},
                trial_index=index,
            )
        )

    # quant-2 asks how much execution cost the best-looking rule can absorb. Same
    # signals every time; only the cost assumption moves.
    console.line("quant-2", "stress-testing: how much cost does the edge survive?")
    momentum = backtest.strategies.ema_crossover(panel, fast=4, slow=12, quantity=40.0)
    db.create_table("signals_ema", momentum.signals.schema, time_column="ts")
    db.append("signals_ema", momentum.signals, note=str(momentum.parameters))
    for index, (fee, slip) in enumerate(((0.0, None), (0.035, None), (0.07, None), (0.07, 2))):
        trials.append(
            Trial(
                agent="quant-2",
                study_id="execution-sensitivity",
                run_id=f"exec-fee{int(fee * 1000):03d}-slip{slip or 0}",
                signals_table="signals_ema",
                parameters={"fee_rate": fee, "slippage_ticks": slip},
                trial_index=index,
                fee_rate=fee,
                slippage_ticks=slip,
            )
        )

    # risk-1 validates one candidate out of sample and, per policy, cannot
    # promote it alone. It marks the trial for sign-off rather than deciding.
    console.line("risk-1", "validating a promotion candidate (policy: human signs off)")
    stamps = list(
        db.sql("SELECT DISTINCT ts_init FROM book_deltas ORDER BY ts_init")
        .to_pandas()
        .ts_init
    )
    split = len(stamps) // 2
    for index, (phase, window) in enumerate(
        (("train", (stamps[0], stamps[split])), ("holdout", (stamps[split], stamps[-1])))
    ):
        trials.append(
            Trial(
                agent="risk-1",
                study_id="promotion-review",
                run_id=f"promo-{phase}",
                signals_table="signals_ema",
                parameters={"phase": phase, "candidate": "ema_crossover"},
                trial_index=index,
                phase=phase,
                window=window,
                needs_decision=phase == "holdout",
                reason="promotion to the blessed set requires human sign-off",
            )
        )

    # quant-1 re-submits a config it already ran. Nothing is recomputed: the
    # ledger recognises the inputs and returns the recorded result.
    repeat = next((t for t in trials if t.agent == "quant-1"), None)
    if repeat is not None:
        trials.append(
            Trial(
                agent="quant-1",
                study_id="favourite-threshold",
                run_id=repeat.run_id,
                signals_table=repeat.signals_table,
                parameters=repeat.parameters,
                trial_index=repeat.trial_index,
            )
        )
    return trials


# ---------------------------------------------------------------------- main


def find_binary() -> Optional[Path]:
    """The repo's own binary first, then whatever is on PATH.

    Order matters. A globally installed older h5i-db cannot read a database this
    source version writes, and it fails with `format_too_new` rather than
    anything a reader would connect to their PATH.
    """
    for profile in ("release", "debug"):
        candidate = REPO / "target" / profile / "h5i-db"
        if candidate.exists():
            return candidate
    found = shutil.which("h5i-db")
    return Path(found) if found else None


def wait_for_url(process: "subprocess.Popen[str]", port: int,
                 timeout: float = 15.0) -> Optional[str]:
    """Read the tokenised URL the UI prints, then keep its stderr drained.

    Draining matters: leaving a piped stream unread would eventually block the
    child once the pipe fills.
    """
    seen: list[str] = []
    deadline = time.monotonic() + timeout
    assert process.stderr is not None
    while time.monotonic() < deadline:
        line = process.stderr.readline()
        if not line:
            break
        seen.append(line.rstrip())
        match = re.search(r"(http://\S+token=\w+)", line)
        if match:
            threading.Thread(
                target=lambda: [None for _ in process.stderr], daemon=True
            ).start()
            return match.group(1)
    # No URL: whatever it did say is the useful thing to show.
    for line in seen[-4:]:
        print(f"    {line}")
    return None


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--markets", type=int, default=24)
    parser.add_argument("--steps", type=int, default=60)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--workers", type=int, default=3,
                        help="agents running at once; 1 makes the log sequential")
    parser.add_argument("--port", type=int, default=7351)
    parser.add_argument("--db", default=None, help="where to build it")
    parser.add_argument("--keep", action="store_true", help="do not delete on exit")
    parser.add_argument("--no-ui", action="store_true")
    parser.add_argument("--no-browser", action="store_true")
    parser.add_argument("--no-colour", action="store_true")
    args = parser.parse_args(argv)

    # Line-buffer stdout. Piped to a file or a pager, a block-buffered demo
    # shows nothing until it exits, which for a long-running UI step is never.
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except (AttributeError, ValueError):  # pragma: no cover - exotic streams
        pass
    colour = not args.no_colour and sys.stdout.isatty()
    console = Console(colour)
    db_path = Path(args.db or (REPO / "target" / "agent-swarm-demo.db"))
    if db_path.exists():
        shutil.rmtree(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    console.rule("1. One pinned dataset, shared by every agent")
    panel_tables = build_panel(args.markets, args.steps, args.seed)
    questions = panel_tables.pop("_questions")
    db = h5i_db.Database(str(db_path), create=True)
    for name, table in panel_tables.items():
        db.create_table(name, table.schema, time_column="ts_init")
        db.append(name, table, note="demo market data")
    db.snapshot("research-pin", tables=list(panel_tables), note="frozen research input")
    rows = db.sql("SELECT count(*) AS n FROM book_deltas").to_pandas()["n"][0]
    print(f"  {args.markets} markets, {rows:,} book rows, pinned as 'research-pin'")
    print(f"  e.g. {list(questions.values())[0]!r}")

    console.rule("2. Three agents decide what to try")
    panel = backtest.quote_panel(db, snapshot="research-pin")
    trials = plan_work(db, panel, console)
    print(f"  {len(trials)} trials queued across "
          f"{len({t.agent for t in trials})} agents")

    console.rule(f"3. Running them ({args.workers} at a time, each its own fork)")
    from concurrent.futures import ThreadPoolExecutor

    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        outcomes = list(
            pool.map(lambda trial: run_trial(str(db_path), trial, console), trials)
        )
    elapsed = time.monotonic() - started

    console.rule("4. What the fleet produced")
    ok = [o for o in outcomes if not o.error]
    cached = [o for o in ok if o.cached]
    warned = [o for o in ok if o.warnings]
    flagged = [o for o in ok if o.trial.needs_decision]
    refused = sum(o.refused for o in ok)
    print(f"  {len(outcomes)} trials in {elapsed:.1f}s across "
          f"{len({t.agent for t in trials})} agents")
    print(f"  {len(ok) - len(cached)} executed, {len(cached)} served from the "
          f"trial ledger without recomputing")
    print(f"  {len(warned)} carry engine warnings, {refused} positions the engine "
          f"refused to settle")
    print(f"  {len(flagged)} waiting on a human decision")
    forks = [name for name in db.fork_names() if name.startswith("bt-")]
    print(f"  {len(forks)} result forks, each holding its own bt_* tables")

    if cached:
        example = cached[0]
        print(f"\n  Trial dedup: {example.trial.run_id!r} was submitted twice and ran"
              f" once.\n  The second submission matched on content, not on name.")

    console.rule("5. Query the pile like any other data")
    board = db.sql(
        """
        SELECT run_id, records_processed, realized_pnl, commissions, settlement_applied
        FROM forks('bt_run')
        ORDER BY realized_pnl DESC
        LIMIT 5
        """
    ).to_pandas()
    print("  best five by realized P&L, read across every fork in one query.")
    print("  (realized_pnl is net of commissions; add settlement_pnl from")
    print("   bt_positions for anything still open at the end.)\n")
    print("   " + board.to_string(index=False).replace("\n", "\n   "))
    totals = db.sql(
        """
        SELECT count(DISTINCT __fork) AS forks, count(*) AS fills,
               round(sum(price * quantity), 2) AS notional
        FROM forks('bt_fills')
        """
    ).to_pandas()
    print(f"\n  and every fill the fleet produced, aggregated across forks:")
    print("   " + totals.to_string(index=False).replace("\n", "\n   "))
    db.close()

    if args.no_ui:
        print(f"\n  Database kept at {db_path}")
        print(f"  Open it yourself with:  h5i-db ui {db_path}")
        return 0

    binary = find_binary()
    if binary is None:
        print("\n  Could not find the h5i-db binary. Build it with:")
        print("     cargo build -p h5i-db-cli")
        print(f"  Then:  h5i-db ui {db_path}")
        return 0

    console.rule("6. The review surface")
    print(f"  serving with {binary}")
    process = subprocess.Popen(
        [str(binary), "ui", str(db_path), "--port", str(args.port)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    # The UI mints a per-session access token and prints the URL carrying it;
    # the API refuses anything else, so the bare host:port is not usable.
    url = wait_for_url(process, args.port)
    if url is None:
        process.terminate()
        print("  The UI did not report a URL. Start it yourself with:")
        print(f"     {binary} ui {db_path}")
        return 1
    print(f"  {url}\n")
    print("  (that URL carries this session's token; the API refuses requests without it)\n")
    print("  The Experiments tab sorts by what needs a human next:")
    print("    1. needs a decision   <- risk-1's promotion candidate")
    print("    2. failed or warned   <- runs the engine refused to settle")
    print("    3. finished, unseen")
    print("    4. still running")
    print("    5. already seen       <- opening a detail marks it, a list view never does")
    print("\n  Also worth a look: the fork monitor (one per trial), the version")
    print("  timeline, and the SQL scratchpad, which reports pruning per query.")
    print("\n  Ctrl-C to stop.\n")

    try:
        if not args.no_browser:
            webbrowser.open(url)
        process.wait()
    except KeyboardInterrupt:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
        print("\n  stopped.")
    finally:
        if not args.keep and db_path.exists():
            shutil.rmtree(db_path, ignore_errors=True)
            print(f"  removed {db_path} (pass --keep to retain it)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
