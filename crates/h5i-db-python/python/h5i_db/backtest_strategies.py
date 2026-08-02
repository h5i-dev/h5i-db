"""A pack of standard strategies, expressed as signal generators.

Every function here takes a quote panel and returns *signals*, never a callback
object. That is the point: a signals table is data, so the trial ledger can
identify it, `verify()` can reproduce it, and the replay loop stays free of a
language boundary. A callback strategy can do things these cannot, and pays for
it by having no content identity.

The shared input is a panel with one row per instrument per instant:

| column | meaning |
|---|---|
| `ts` | the quote instant |
| `instrument_id` | the market |
| `bid`, `ask` | best prices, as probabilities |
| `bid_size`, `ask_size` | displayed depth (optional; needed by microprice) |

:func:`quote_panel` builds one from `book_deltas`. Every generator stamps its
orders strictly after the instant they were decided from, because an order
sharing a timestamp with a book event may match the previous snapshot.

These are reference implementations of well-known rules, sized to be readable.
They are not tuned, and a plateau in :func:`~h5i_db.backtest.study` is the only
evidence that a parameter choice means anything.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Optional, Sequence

import pyarrow as pa

from .dataframe import quote_string

__all__ = [
    "STRATEGIES",
    "SignalPlan",
    "breakout",
    "deep_value",
    "ema_crossover",
    "final_period_momentum",
    "late_favorite_hold",
    "mean_reversion",
    "microprice_imbalance",
    "pair_arbitrage",
    "panic_fade",
    "quote_panel",
    "rsi_reversion",
    "threshold_momentum",
    "vwap_reversion",
]

#: One microsecond. An order is stamped this far after the quote it was decided
#: from, so it cannot match the snapshot it was reacting to.
SUBMIT_DELAY_NS = 1_000


@dataclass(frozen=True)
class SignalPlan:
    """Signals plus what produced them.

    The parameters travel with the table so a study's metadata and the signals
    it ran cannot drift apart.
    """

    strategy: str
    parameters: Mapping[str, Any]
    signals: pa.Table

    @property
    def num_signals(self) -> int:
        return self.signals.num_rows

    def to_metadata(self) -> dict[str, Any]:
        return {"strategy": self.strategy, "parameters": dict(self.parameters)}


def quote_panel(
    db: Any,
    *,
    snapshot: Optional[str] = None,
    outcome: int = 0,
    instruments: Optional[Sequence[str]] = None,
    until_expiry: bool = True,
) -> "Any":
    """Top of book per instrument per instant, as a pandas frame.

    `until_expiry` stops at `instruments.expiration_ns`. A panel that keeps
    quoting after resolution has a jump to 0 or 1 in it, and every momentum or
    volatility rule would read that jump as a signal.
    """
    # Snapshot and instrument names reach here from callers, config files and
    # vendor archives, so they are quoted rather than interpolated: an
    # apostrophe in a market name is enough to make this SQL mean something
    # else.
    source = (
        f"h5i('book_deltas', {quote_string(snapshot)})" if snapshot else "book_deltas"
    )
    filters = [f"outcome = {int(outcome)}"]
    if instruments:
        listed = ", ".join(quote_string(str(name)) for name in instruments)
        filters.append(f"instrument_id IN ({listed})")
    if until_expiry and "instruments" in db.tables():
        expiry = db.sql(
            "SELECT max(expiration_ns) AS expiry FROM instruments"
        ).to_pandas()["expiry"][0]
        if expiry is not None and not (isinstance(expiry, float) and math.isnan(expiry)):
            filters.append(f"ts_init <= to_timestamp_nanos({int(expiry)})")
    where = " AND ".join(filters)
    return db.sql(
        f"""
        SELECT instrument_id, ts_init AS ts,
               max(CASE WHEN side = 'buy'  THEN price END) AS bid,
               max(CASE WHEN side = 'sell' THEN price END) AS ask,
               max(CASE WHEN side = 'buy'  THEN size  END) AS bid_size,
               max(CASE WHEN side = 'sell' THEN size  END) AS ask_size
        FROM {source}
        WHERE {where}
        GROUP BY instrument_id, ts_init
        ORDER BY instrument_id, ts_init
        """
    ).to_pandas()


def _require(panel: Any, columns: Sequence[str], strategy: str) -> None:
    missing = [name for name in columns if name not in panel.columns]
    if missing:
        raise ValueError(
            f"{strategy} needs columns {missing}; the panel has "
            f"{sorted(panel.columns)}"
        )
    if panel.empty:
        raise ValueError(f"{strategy}: the panel is empty")


def _mid(panel: Any) -> Any:
    return (panel["bid"] + panel["ask"]) / 2.0


def _plan(
    strategy: str,
    parameters: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
) -> SignalPlan:
    from .backtest import signal_table

    table = signal_table(list(rows))
    if table.num_rows:
        table = table.sort_by([("ts", "ascending")])
    return SignalPlan(strategy=strategy, parameters=dict(parameters), signals=table)


def _order(
    ts: Any,
    instrument_id: str,
    side: str,
    quantity: float,
    *,
    outcome: int = 0,
    tag: str,
    reduce_only: bool = False,
    limit_price: Optional[float] = None,
) -> dict[str, Any]:
    import pandas as pd

    stamp = pd.Timestamp(ts)
    row: dict[str, Any] = {
        "ts": stamp.to_pydatetime() + pd.Timedelta(nanoseconds=SUBMIT_DELAY_NS).to_pytimedelta(),
        "instrument_id": str(instrument_id),
        "outcome": int(outcome),
        "side": side,
        "quantity": float(quantity),
        "tag": tag,
        "reduce_only": reduce_only,
    }
    if limit_price is not None:
        row["kind"] = "limit"
        row["limit_price"] = float(limit_price)
        row["time_in_force"] = "gtc"
    return row


def _crossing_signals(
    panel: Any,
    *,
    strategy: str,
    parameters: Mapping[str, Any],
    indicator: Callable[[Any], Any],
    quantity: float,
    outcome: int,
    long_only: bool,
) -> SignalPlan:
    """Shared driver for rules that enter on a sign change of one indicator.

    The state machine is deliberately minimal: flat to long on a positive
    crossing, long to flat on a negative one, and the exit is `reduce_only` so a
    rule cannot accidentally open a short it never intended.
    """
    rows: list[dict[str, Any]] = []
    for instrument_id, group in panel.groupby("instrument_id", sort=True):
        ordered = group.sort_values("ts").reset_index(drop=True)
        signal = indicator(ordered)
        held = False
        previous = None
        for index in range(len(ordered)):
            value = signal.iloc[index] if hasattr(signal, "iloc") else signal[index]
            if value is None or (isinstance(value, float) and math.isnan(value)):
                previous = None
                continue
            if previous is not None:
                crossed_up = previous <= 0 < value
                crossed_down = previous >= 0 > value
                if crossed_up and not held:
                    rows.append(
                        _order(
                            ordered.ts.iloc[index],
                            instrument_id,
                            "buy",
                            quantity,
                            outcome=outcome,
                            tag=f"{strategy}-entry",
                        )
                    )
                    held = True
                elif crossed_down and held:
                    rows.append(
                        _order(
                            ordered.ts.iloc[index],
                            instrument_id,
                            "sell",
                            quantity,
                            outcome=outcome,
                            tag=f"{strategy}-exit",
                            reduce_only=True,
                        )
                    )
                    held = False
                elif crossed_down and not held and not long_only:
                    rows.append(
                        _order(
                            ordered.ts.iloc[index],
                            instrument_id,
                            "sell",
                            quantity,
                            outcome=outcome,
                            tag=f"{strategy}-short",
                        )
                    )
            previous = value
    return _plan(strategy, parameters, rows)


# -- trend ------------------------------------------------------------------


def ema_crossover(
    panel: Any,
    *,
    fast: int = 8,
    slow: int = 24,
    quantity: float = 10.0,
    outcome: int = 0,
    long_only: bool = True,
) -> SignalPlan:
    """Enter when a fast EMA of the mid crosses above a slow one."""
    _require(panel, ("ts", "instrument_id", "bid", "ask"), "ema_crossover")
    if fast >= slow:
        raise ValueError("fast span must be shorter than slow")

    def indicator(frame: Any) -> Any:
        mid = _mid(frame)
        return mid.ewm(span=fast, adjust=False).mean() - mid.ewm(
            span=slow, adjust=False
        ).mean()

    return _crossing_signals(
        panel,
        strategy="ema_crossover",
        parameters={"fast": fast, "slow": slow, "quantity": quantity},
        indicator=indicator,
        quantity=quantity,
        outcome=outcome,
        long_only=long_only,
    )


def threshold_momentum(
    panel: Any,
    *,
    lookback: int = 12,
    threshold: float = 0.02,
    quantity: float = 10.0,
    outcome: int = 0,
) -> SignalPlan:
    """Buy after the mid rises by `threshold` over `lookback` samples."""
    _require(panel, ("ts", "instrument_id", "bid", "ask"), "threshold_momentum")
    if lookback < 1:
        raise ValueError("lookback must be at least one sample")

    def indicator(frame: Any) -> Any:
        mid = _mid(frame)
        return mid.diff(lookback) - threshold

    return _crossing_signals(
        panel,
        strategy="threshold_momentum",
        parameters={"lookback": lookback, "threshold": threshold, "quantity": quantity},
        indicator=indicator,
        quantity=quantity,
        outcome=outcome,
        long_only=True,
    )


def breakout(
    panel: Any,
    *,
    lookback: int = 24,
    quantity: float = 10.0,
    outcome: int = 0,
) -> SignalPlan:
    """Buy a new `lookback`-sample high, exit on a new low."""
    _require(panel, ("ts", "instrument_id", "bid", "ask"), "breakout")

    def indicator(frame: Any) -> Any:
        mid = _mid(frame)
        high = mid.shift(1).rolling(lookback).max()
        low = mid.shift(1).rolling(lookback).min()
        # Positive above the prior high, negative below the prior low, zero in
        # between, so one crossing series drives both entry and exit.
        return (mid - high).clip(lower=0) + (mid - low).clip(upper=0)

    return _crossing_signals(
        panel,
        strategy="breakout",
        parameters={"lookback": lookback, "quantity": quantity},
        indicator=indicator,
        quantity=quantity,
        outcome=outcome,
        long_only=True,
    )


def final_period_momentum(
    panel: Any,
    *,
    tail_fraction: float = 0.25,
    lookback: int = 6,
    quantity: float = 10.0,
    outcome: int = 0,
) -> SignalPlan:
    """Momentum, restricted to the last `tail_fraction` of each market's life.

    Event contracts behave differently near resolution: the probability path is
    pinned by an approaching answer rather than by flow. Trading only the tail
    is the cheapest way to test whether a rule's edge lives there.
    """
    _require(panel, ("ts", "instrument_id", "bid", "ask"), "final_period_momentum")
    if not 0 < tail_fraction <= 1:
        raise ValueError("tail_fraction must be in (0, 1]")
    frames = []
    for _, group in panel.groupby("instrument_id", sort=True):
        ordered = group.sort_values("ts")
        keep = max(1, int(round(len(ordered) * tail_fraction)))
        frames.append(ordered.tail(keep))
    import pandas as pd

    tail = pd.concat(frames) if frames else panel
    plan = threshold_momentum(
        tail, lookback=lookback, threshold=0.0, quantity=quantity, outcome=outcome
    )
    return SignalPlan(
        strategy="final_period_momentum",
        parameters={
            "tail_fraction": tail_fraction,
            "lookback": lookback,
            "quantity": quantity,
        },
        signals=plan.signals,
    )


# -- mean reversion ---------------------------------------------------------


def mean_reversion(
    panel: Any,
    *,
    lookback: int = 24,
    entry_z: float = -1.5,
    quantity: float = 10.0,
    outcome: int = 0,
) -> SignalPlan:
    """Buy when the mid is `entry_z` standard deviations below its mean."""
    _require(panel, ("ts", "instrument_id", "bid", "ask"), "mean_reversion")
    if entry_z >= 0:
        raise ValueError("entry_z must be negative: this buys weakness")

    def indicator(frame: Any) -> Any:
        mid = _mid(frame)
        mean = mid.rolling(lookback).mean()
        deviation = mid.rolling(lookback).std()
        z = (mid - mean) / deviation
        # Positive when cheap enough to buy, negative once back to the mean.
        return (entry_z - z).where(z < 0, -z)

    return _crossing_signals(
        panel,
        strategy="mean_reversion",
        parameters={"lookback": lookback, "entry_z": entry_z, "quantity": quantity},
        indicator=indicator,
        quantity=quantity,
        outcome=outcome,
        long_only=True,
    )


def rsi_reversion(
    panel: Any,
    *,
    period: int = 14,
    oversold: float = 30.0,
    overbought: float = 70.0,
    quantity: float = 10.0,
    outcome: int = 0,
) -> SignalPlan:
    """Buy below `oversold` RSI, exit above `overbought`."""
    _require(panel, ("ts", "instrument_id", "bid", "ask"), "rsi_reversion")
    if not 0 < oversold < overbought < 100:
        raise ValueError("need 0 < oversold < overbought < 100")

    def indicator(frame: Any) -> Any:
        mid = _mid(frame)
        change = mid.diff()
        gain = change.clip(lower=0).ewm(alpha=1 / period, adjust=False).mean()
        loss = (-change.clip(upper=0)).ewm(alpha=1 / period, adjust=False).mean()
        strength = gain / loss.replace(0.0, float("nan"))
        rsi = 100 - 100 / (1 + strength)
        rsi = rsi.fillna(50.0)
        # Positive below `oversold` (buy) and negative above `overbought`
        # (exit), which is the sign convention `_crossing_signals` reads.
        return (oversold - rsi).where(rsi < 50, overbought - rsi)

    return _crossing_signals(
        panel,
        strategy="rsi_reversion",
        parameters={
            "period": period,
            "oversold": oversold,
            "overbought": overbought,
            "quantity": quantity,
        },
        indicator=indicator,
        quantity=quantity,
        outcome=outcome,
        long_only=True,
    )


def vwap_reversion(
    panel: Any,
    *,
    lookback: int = 24,
    band: float = 0.02,
    quantity: float = 10.0,
    outcome: int = 0,
) -> SignalPlan:
    """Buy `band` below a rolling size-weighted average price.

    Falls back to an unweighted mean when the panel carries no depth, and says
    so in the parameters rather than pretending the weighting happened.
    """
    _require(panel, ("ts", "instrument_id", "bid", "ask"), "vwap_reversion")
    weighted = "bid_size" in panel.columns and "ask_size" in panel.columns

    def indicator(frame: Any) -> Any:
        mid = _mid(frame)
        if weighted:
            size = frame["bid_size"].fillna(0) + frame["ask_size"].fillna(0)
            numerator = (mid * size).rolling(lookback).sum()
            denominator = size.rolling(lookback).sum()
            reference = numerator / denominator.replace(0.0, float("nan"))
        else:
            reference = mid.rolling(lookback).mean()
        gap = reference - mid
        return gap - band

    return _crossing_signals(
        panel,
        strategy="vwap_reversion",
        parameters={
            "lookback": lookback,
            "band": band,
            "quantity": quantity,
            "size_weighted": weighted,
        },
        indicator=indicator,
        quantity=quantity,
        outcome=outcome,
        long_only=True,
    )


def panic_fade(
    panel: Any,
    *,
    drop: float = 0.06,
    lookback: int = 3,
    quantity: float = 10.0,
    outcome: int = 0,
) -> SignalPlan:
    """Buy after an abrupt fall, on the view that it overshot."""
    _require(panel, ("ts", "instrument_id", "bid", "ask"), "panic_fade")
    if drop <= 0:
        raise ValueError("drop must be positive: it is the size of the fall to fade")

    def indicator(frame: Any) -> Any:
        mid = _mid(frame)
        return (-mid.diff(lookback)) - drop

    return _crossing_signals(
        panel,
        strategy="panic_fade",
        parameters={"drop": drop, "lookback": lookback, "quantity": quantity},
        indicator=indicator,
        quantity=quantity,
        outcome=outcome,
        long_only=True,
    )


# -- microstructure and event-contract structure ----------------------------


def microprice_imbalance(
    panel: Any,
    *,
    threshold: float = 0.15,
    quantity: float = 10.0,
    outcome: int = 0,
) -> SignalPlan:
    """Buy when depth leans to the bid by more than `threshold`.

    Imbalance is `(bid_size - ask_size) / (bid_size + ask_size)`, computable
    from a single snapshot, which is why it is the honest microstructure signal
    for snapshot data. It says nothing about queue position, and no amount of
    care recovers that from a periodic feed.
    """
    _require(
        panel,
        ("ts", "instrument_id", "bid", "ask", "bid_size", "ask_size"),
        "microprice_imbalance",
    )
    if not 0 < threshold < 1:
        raise ValueError("threshold must be in (0, 1)")

    def indicator(frame: Any) -> Any:
        total = frame["bid_size"].fillna(0) + frame["ask_size"].fillna(0)
        imbalance = (frame["bid_size"].fillna(0) - frame["ask_size"].fillna(0)) / (
            total.replace(0.0, float("nan"))
        )
        return imbalance.fillna(0.0) - threshold

    return _crossing_signals(
        panel,
        strategy="microprice_imbalance",
        parameters={"threshold": threshold, "quantity": quantity},
        indicator=indicator,
        quantity=quantity,
        outcome=outcome,
        long_only=True,
    )


def deep_value(
    panel: Any,
    *,
    max_price: float = 0.15,
    quantity: float = 10.0,
    outcome: int = 0,
    once_per_market: bool = True,
) -> SignalPlan:
    """Buy anything trading below `max_price` and hold it to resolution.

    The longshot side of the favorite-longshot bias, and usually the losing one.
    Included because it is the natural counterparty to
    :func:`late_favorite_hold`, and a pack that only ships the profitable side
    of a documented anomaly is marketing rather than a toolbox.
    """
    _require(panel, ("ts", "instrument_id", "bid", "ask"), "deep_value")
    if not 0 < max_price < 1:
        raise ValueError("max_price must be a probability in (0, 1)")
    rows = []
    for instrument_id, group in panel.groupby("instrument_id", sort=True):
        ordered = group.sort_values("ts")
        hits = ordered[ordered["ask"] <= max_price]
        if hits.empty:
            continue
        chosen = hits.head(1) if once_per_market else hits
        for row in chosen.itertuples():
            rows.append(
                _order(
                    row.ts,
                    instrument_id,
                    "buy",
                    quantity,
                    outcome=outcome,
                    tag="deep_value",
                )
            )
    return _plan(
        "deep_value",
        {"max_price": max_price, "quantity": quantity, "once_per_market": once_per_market},
        rows,
    )


def late_favorite_hold(
    panel: Any,
    *,
    min_price: float = 0.75,
    tail_fraction: float = 0.3,
    quantity: float = 10.0,
    outcome: int = 0,
) -> SignalPlan:
    """Buy strong favorites late in a market's life and hold to resolution.

    The favorite side of the same bias, restricted to the tail where the fee
    curve is cheapest relative to the edge. One entry per market.
    """
    _require(panel, ("ts", "instrument_id", "bid", "ask"), "late_favorite_hold")
    if not 0 < min_price < 1:
        raise ValueError("min_price must be a probability in (0, 1)")
    if not 0 < tail_fraction <= 1:
        raise ValueError("tail_fraction must be in (0, 1]")
    rows = []
    for instrument_id, group in panel.groupby("instrument_id", sort=True):
        ordered = group.sort_values("ts").reset_index(drop=True)
        start = int(len(ordered) * (1.0 - tail_fraction))
        tail = ordered.iloc[start:]
        hits = tail[tail["ask"] >= min_price]
        if hits.empty:
            continue
        row = hits.iloc[0]
        rows.append(
            _order(
                row.ts,
                instrument_id,
                "buy",
                quantity,
                outcome=outcome,
                tag="late_favorite_hold",
            )
        )
    return _plan(
        "late_favorite_hold",
        {"min_price": min_price, "tail_fraction": tail_fraction, "quantity": quantity},
        rows,
    )


def pair_arbitrage(
    db: Any,
    *,
    snapshot: Optional[str] = None,
    fee_rate: float = 0.0,
    min_edge: float = 0.0,
    quantity: float = 10.0,
    outcomes: tuple[int, int] = (0, 1),
    once_per_market: bool = True,
) -> SignalPlan:
    """Buy both outcomes when the pair costs less than it settles for.

    A binary market's two outcomes settle to exactly 1.00 between them, so a
    pair bought below that is a locked-in gain before costs. `fee_rate` applies
    the quadratic venue fee, `rate * p * (1 - p)` per leg, which is the whole
    question on these venues: near even odds the fee exceeds any basis the book
    offers.

    Reads both outcomes itself rather than taking a panel, because a pair trade
    is the one rule here that is not a function of one side's quotes.
    """
    if not 0.0 <= fee_rate < 1.0:
        raise ValueError("fee_rate must be in [0, 1)")
    if len(set(outcomes)) != 2:
        raise ValueError("a pair needs two distinct outcomes")
    source = (
        f"h5i('book_deltas', {quote_string(snapshot)})" if snapshot else "book_deltas"
    )
    first, second = outcomes
    frame = db.sql(
        f"""
        WITH tops AS (
            SELECT instrument_id, ts_init AS ts, outcome,
                   max(CASE WHEN side = 'sell' THEN price END) AS ask
            FROM {source}
            WHERE outcome IN ({int(first)}, {int(second)})
            GROUP BY instrument_id, ts_init, outcome
        )
        SELECT a.instrument_id, a.ts, a.ask AS ask_a, b.ask AS ask_b
        FROM tops a
        JOIN tops b
          ON a.instrument_id = b.instrument_id AND a.ts = b.ts
        WHERE a.outcome = {int(first)} AND b.outcome = {int(second)}
          AND a.ask IS NOT NULL AND b.ask IS NOT NULL
        ORDER BY a.instrument_id, a.ts
        """
    ).to_pandas()
    rows = []
    for instrument_id, group in frame.groupby("instrument_id", sort=True):
        ordered = group.sort_values("ts")
        for row in ordered.itertuples():
            cost = float(row.ask_a) + float(row.ask_b)
            fee = fee_rate * (
                row.ask_a * (1 - row.ask_a) + row.ask_b * (1 - row.ask_b)
            )
            if (1.0 - cost - fee) <= min_edge:
                continue
            for outcome in (first, second):
                rows.append(
                    _order(
                        row.ts,
                        instrument_id,
                        "buy",
                        quantity,
                        outcome=outcome,
                        tag="pair_arbitrage",
                    )
                )
            if once_per_market:
                break
    return _plan(
        "pair_arbitrage",
        {
            "fee_rate": fee_rate,
            "min_edge": min_edge,
            "quantity": quantity,
            "outcomes": list(outcomes),
            "once_per_market": once_per_market,
        },
        rows,
    )


#: Every panel-driven generator, for sweeping the pack itself.
#: `pair_arbitrage` is absent because it reads the database rather than a panel.
STRATEGIES: Mapping[str, Callable[..., SignalPlan]] = {
    "breakout": breakout,
    "deep_value": deep_value,
    "ema_crossover": ema_crossover,
    "final_period_momentum": final_period_momentum,
    "late_favorite_hold": late_favorite_hold,
    "mean_reversion": mean_reversion,
    "microprice_imbalance": microprice_imbalance,
    "panic_fade": panic_fade,
    "rsi_reversion": rsi_reversion,
    "threshold_momentum": threshold_momentum,
    "vwap_reversion": vwap_reversion,
}
