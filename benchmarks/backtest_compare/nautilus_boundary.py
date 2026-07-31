#!/usr/bin/env python3
"""What a Python strategy callback costs NautilusTrader, per event.

`h5i_callback_boundary.py` splits h5i's callback cost from its engine cost by
running the same events with and without Python in the loop. Comparing h5i's
total against Nautilus's total without doing the same to Nautilus answers
"which is faster end to end" and not "which part is faster", so this is the
matching split.

Nautilus has no declarative path to use as the control, but it has something
equivalent: a strategy that never subscribes. The engine still streams every
quote through the message bus and the venue; it just never calls into Python.

    subscribed    on_quote_tick per event, counter and two comparisons
    unsubscribed  same data through the same engine, no Python per event

    per-event boundary cost = (subscribed - unsubscribed) / events

Arms alternate within one process, medians after one warm-up, matching the
h5i harness so the two numbers are built the same way.

    /tmp/h5i-nautilus-wheel-venv/bin/python \
        benchmarks/backtest_compare/nautilus_boundary.py \
        --workload benchmarks/backtest_compare/workload.json --rounds 5
"""

from __future__ import annotations

import argparse
import json
import statistics
import time
from pathlib import Path

from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.config import BacktestEngineConfig, LoggingConfig
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.enums import AccountType, OmsType, OrderSide
from nautilus_trader.model.identifiers import InstrumentId, Venue
from nautilus_trader.model.objects import Money, Price, Quantity
from nautilus_trader.test_kit.providers import TestInstrumentProvider
from nautilus_trader.trading.strategy import Strategy


def mid_at(step: int) -> float:
    return 100.0 + ((step % 400) - 200) * 0.01


class ComparisonStrategy(Strategy):
    """The cross-engine strategy, with subscription made optional.

    Not subscribing is the whole control: the engine's work is unchanged and
    the per-event trip into Python is gone.
    """

    def __init__(
        self,
        instrument_id: InstrumentId,
        spacing: int,
        signal_count: int,
        subscribe: bool,
    ):
        super().__init__()
        self.instrument_id = instrument_id
        self.spacing = spacing
        self.signal_count = signal_count
        self.subscribe = subscribe
        self.events_seen = 0
        self.orders_submitted = 0

    def on_start(self) -> None:
        if self.subscribe:
            self.subscribe_quote_ticks(self.instrument_id)

    def on_quote_tick(self, tick: QuoteTick) -> None:
        del tick
        self.events_seen += 1
        if (
            self.orders_submitted >= self.signal_count
            or self.events_seen != 2 + self.orders_submitted * self.spacing
        ):
            return
        side = OrderSide.BUY if self.orders_submitted % 2 == 0 else OrderSide.SELL
        order = self.order_factory.market(
            instrument_id=self.instrument_id,
            order_side=side,
            quantity=Quantity.from_int(1),
        )
        self.submit_order(order)
        self.orders_submitted += 1


def build_events(instrument_id: InstrumentId, event_count: int) -> list:
    return [
        QuoteTick(
            instrument_id=instrument_id,
            bid_price=Price.from_str(f"{mid_at(step) - 0.01:.5f}"),
            ask_price=Price.from_str(f"{mid_at(step) + 0.01:.5f}"),
            bid_size=Quantity.from_int(500),
            ask_size=Quantity.from_int(500),
            ts_event=step * 1_000_000_000,
            ts_init=step * 1_000_000_000,
        )
        for step in range(event_count)
    ]


def run_arm(workload: dict, events: list, instrument, subscribe: bool):
    """One `BacktestEngine.run()`, timed at the same boundary as the adapter."""
    event_count = int(workload["quote_events"])
    signal_count = int(workload["signals"])
    engine = BacktestEngine(
        config=BacktestEngineConfig(logging=LoggingConfig(bypass_logging=True)),
    )
    engine.add_venue(
        venue=Venue("SIM"),
        oms_type=OmsType.NETTING,
        account_type=AccountType.MARGIN,
        base_currency=USD,
        starting_balances=[Money(int(workload["initial_cash"]), USD)],
    )
    engine.add_instrument(instrument)
    strategy = ComparisonStrategy(
        instrument.id,
        max(event_count // max(signal_count, 1), 1),
        signal_count,
        subscribe,
    )
    engine.add_strategy(strategy)
    engine.add_data(events)

    started = time.perf_counter_ns()
    engine.run()
    elapsed = (time.perf_counter_ns() - started) / 1_000_000
    seen, sent = strategy.events_seen, strategy.orders_submitted
    engine.dispose()
    return elapsed, seen, sent


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workload", type=Path, required=True)
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--output", type=str, default=None)
    args = parser.parse_args()

    workload = json.loads(args.workload.read_text())
    event_count = int(workload["quote_events"])
    instrument = TestInstrumentProvider.default_fx_ccy("AUD/USD")
    events = build_events(instrument.id, event_count)

    arms = ("unsubscribed", "subscribed")
    samples: dict[str, list[float]] = {arm: [] for arm in arms}
    for round_index in range(args.rounds + 1):
        for arm in arms:
            ms, seen, sent = run_arm(workload, events, instrument, arm == "subscribed")
            if round_index == 0:
                print(
                    f"  warm-up {arm:13} {ms:8.1f} ms  seen={seen} orders={sent}",
                    flush=True,
                )
                continue
            samples[arm].append(ms)

    medians = {arm: statistics.median(samples[arm]) for arm in arms}
    boundary_us = (medians["subscribed"] - medians["unsubscribed"]) / event_count * 1000

    print()
    print(f"{'arm':14} {'median ms':>10} {'min':>9} {'max':>9}")
    for arm in arms:
        print(
            f"{arm:14} {medians[arm]:10.1f} {min(samples[arm]):9.1f} "
            f"{max(samples[arm]):9.1f}"
        )
    print()
    print(f"engine without Python: {medians['unsubscribed'] / event_count * 1000:.3f} us/event")
    print(f"boundary cost:         {boundary_us:.3f} us/event")

    if args.output:
        with open(args.output, "w") as handle:
            json.dump(
                {
                    "engine": "nautilus_trader",
                    "engine_version": __import__("nautilus_trader").__version__,
                    "event_count": event_count,
                    "rounds": args.rounds,
                    "samples_ms": samples,
                    "median_ms": medians,
                    "engine_us_per_event": medians["unsubscribed"] / event_count * 1000,
                    "boundary_us_per_event": boundary_us,
                },
                handle,
                indent=1,
            )


if __name__ == "__main__":
    main()
