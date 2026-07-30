#!/usr/bin/env python3
"""Run the common backtest workload through NautilusTrader.

Data construction is deliberately outside ``engine_ms``. ``load_ms`` covers
copying the already-created native model objects into the engine, and
``engine_ms`` covers BacktestEngine.run(). This is the closest public
Nautilus boundary to h5i-db's decoded-record kernel timing.
"""

from __future__ import annotations

import argparse
import json
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


class ComparisonStrategy(Strategy):
    def __init__(self, instrument_id: InstrumentId, spacing: int, signal_count: int):
        super().__init__()
        self.instrument_id = instrument_id
        self.spacing = spacing
        self.signal_count = signal_count
        self.events_seen = 0
        self.orders_submitted = 0

    def on_start(self) -> None:
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


def mid_at(step: int) -> float:
    return 100.0 + ((step % 400) - 200) * 0.01


def load_workload(path: Path) -> dict:
    workload = json.loads(path.read_text())
    if workload.get("schema_version") != 1:
        raise ValueError("unsupported workload schema")
    if workload.get("instruments") != 1:
        raise ValueError("the Nautilus adapter currently requires one instrument")
    return workload


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workload", type=Path, required=True)
    args = parser.parse_args()
    workload = load_workload(args.workload)
    event_count = int(workload["quote_events"])
    signal_count = int(workload["signals"])

    instrument = TestInstrumentProvider.default_fx_ccy("AUD/USD")
    instrument_id = instrument.id
    events = [
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

    setup_started = time.perf_counter_ns()
    engine = BacktestEngine(
        config=BacktestEngineConfig(
            logging=LoggingConfig(bypass_logging=True),
        ),
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
        instrument_id,
        max(event_count // max(signal_count, 1), 1),
        signal_count,
    )
    engine.add_strategy(strategy)
    setup_ms = (time.perf_counter_ns() - setup_started) / 1_000_000

    load_started = time.perf_counter_ns()
    engine.add_data(events)
    load_ms = (time.perf_counter_ns() - load_started) / 1_000_000

    run_started = time.perf_counter_ns()
    engine.run()
    engine_ms = (time.perf_counter_ns() - run_started) / 1_000_000

    result = {
        "schema_version": 1,
        "engine": "nautilus_trader",
        "engine_version": __import__("nautilus_trader").__version__,
        "workload": workload["name"],
        "event_count": event_count,
        "signals_requested": signal_count,
        "events_seen": strategy.events_seen,
        "orders_submitted": strategy.orders_submitted,
        "timings_ms": {
            "setup": setup_ms,
            "load": load_ms,
            "engine": engine_ms,
        },
        "throughput_events_per_sec": event_count / (engine_ms / 1000),
        "boundary": "in-memory model objects -> BacktestEngine.run",
    }
    engine.dispose()
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
