"""JSON feeds that are not archives: an AMM's bet log, a published series.

Two shapes that do not fit :mod:`._archive`, which assumes a columnar file of
book events. A venue with no order book still has prints, and a macro series is
not market data at all but is read alongside it. Both arrive as already-fetched
JSON, for the same reason as everywhere else here: fetching belongs in a script
with the credentials and the retries, parsing belongs somewhere testable.

Both converters take publication time seriously. A bet is knowable when it is
placed, but a published series describes a period that ended before anyone
could read the number, so `references_from_series` makes the caller state that
lag rather than defaulting it to zero and quietly backdating the knowledge.
"""

from __future__ import annotations

from typing import Any, Iterable, Mapping, Optional, Sequence

import pyarrow as pa
import pyarrow.compute as pc

from ._bars import parse_interval
from ._canonical import (
    REFERENCES_SCHEMA,
    TRADES_SCHEMA,
    IngestReport,
    commit,
    concat,
    ensure_tables,
)
from ._markets import MarketSpec

__all__ = [
    "manifold_markets_from_json",
    "manifold_trades_from_json",
    "ingest_manifold_bets",
    "references_from_series",
    "ingest_references",
]

# Manifold's binary markets are a constant-product AMM. Anything else it hosts
# (multiple choice, numeric, free response) has a different payout rule, and
# reading one as the other misprices every position, so they are skipped and
# reported rather than approximated.
_MANIFOLD_BINARY = ("BINARY", "PSEUDO_NUMERIC", "STONK")
_MANIFOLD_OUTCOMES = ("YES", "NO")


def manifold_markets_from_json(
    payloads: Iterable[Mapping[str, Any]],
    *,
    skipped: Optional[list[dict[str, Any]]] = None,
) -> list[MarketSpec]:
    """Market specs from `GET /v0/markets` (or `/v0/market/{id}`) payloads.

    `id` is the instrument id rather than `slug`, because a slug can be edited
    after the fact and the bet log keys on `contractId`.
    """
    specs: list[MarketSpec] = []
    for payload in payloads:
        kind = str(payload.get("outcomeType") or "")
        if kind not in _MANIFOLD_BINARY:
            if skipped is not None:
                skipped.append(
                    {
                        "reason": "unsupported_outcome_type",
                        "id": payload.get("id"),
                        "outcomeType": kind,
                    }
                )
            continue
        created = payload.get("createdTime")
        close = payload.get("closeTime")
        resolved_at = payload.get("resolutionTime")
        resolution = payload.get("resolution")
        winner: Optional[int] = None
        payouts: Optional[tuple[float, ...]] = None
        voided = False
        if payload.get("isResolved"):
            if resolved_at is None:
                # Settlement is gated on when the result became knowable. A
                # resolved market that does not say when cannot be settled
                # against, and inventing the instant would move every payout.
                if skipped is not None:
                    skipped.append(
                        {
                            "reason": "resolved_without_resolution_time",
                            "id": payload.get("id"),
                        }
                    )
                continue
            if resolution in _MANIFOLD_OUTCOMES:
                winner = _MANIFOLD_OUTCOMES.index(resolution)
            elif resolution == "CANCEL":
                voided = True
            elif resolution == "MKT":
                # A partial resolution pays each side its share, which is a
                # split and not a winner: recording it as a winner would be
                # wrong by the full notional on both sides.
                probability = payload.get("resolutionProbability")
                if probability is not None:
                    share = float(probability)
                    payouts = (share, 1.0 - share)
        specs.append(
            MarketSpec(
                instrument_id=str(payload["id"]),
                venue="manifold",
                outcome_labels=_MANIFOLD_OUTCOMES,
                kind="prediction_market",
                defined_ns=int(created) * 1_000_000 if created else 0,
                expiration_ns=int(close) * 1_000_000 if close else None,
                settlement_observable_ns=(
                    int(resolved_at) * 1_000_000 if resolved_at else None
                ),
                winner_outcome=winner,
                payouts=payouts,
                voided=voided,
                metadata={"slug": payload.get("slug"), "question": payload.get("question")},
            )
        )
    return specs


def manifold_trades_from_json(
    bets: Iterable[Mapping[str, Any]],
    *,
    markets: Optional[Sequence[MarketSpec]] = None,
    source_vendor: str = "manifold",
    skipped: Optional[dict[str, int]] = None,
) -> pa.Table:
    """Canonical `trades` rows from `GET /v0/bets` payloads.

    The executed price is `amount / shares`, the mana actually paid per share,
    not `probAfter`. On an automated market maker those differ: `probAfter` is
    the marginal price the *next* trade would start from, so pricing fills at
    it overstates a buyer's cost and understates a seller's, by more the larger
    the bet. Using the realised average is the only reading a profit and loss
    calculation can reproduce.

    Redemptions, cancellations and unfilled limit orders are not prints and are
    counted out rather than priced.
    """
    wanted = {spec.instrument_id for spec in markets} if markets else None
    counters = skipped if skipped is not None else {}
    rows: dict[str, list[Any]] = {name: [] for name in TRADES_SCHEMA.names}
    for bet in bets:
        contract = str(bet.get("contractId") or "")
        if wanted is not None and contract not in wanted:
            counters["other_market"] = counters.get("other_market", 0) + 1
            continue
        if bet.get("isRedemption"):
            # The automatic YES+NO to mana conversion moves no risk between
            # participants, so it is not a trade.
            counters["redemption"] = counters.get("redemption", 0) + 1
            continue
        if bet.get("isCancelled") or bet.get("isFilled") is False:
            counters["unfilled"] = counters.get("unfilled", 0) + 1
            continue
        outcome_label = str(bet.get("outcome") or "")
        if outcome_label not in _MANIFOLD_OUTCOMES:
            counters[f"outcome:{outcome_label}"] = (
                counters.get(f"outcome:{outcome_label}", 0) + 1
            )
            continue
        try:
            amount = float(bet.get("amount") or 0.0)
            shares = float(bet.get("shares") or 0.0)
        except (TypeError, ValueError):
            counters["unparseable"] = counters.get("unparseable", 0) + 1
            continue
        if shares == 0.0 or amount == 0.0:
            counters["zero_size"] = counters.get("zero_size", 0) + 1
            continue
        created = bet.get("createdTime")
        if created is None:
            counters["no_timestamp"] = counters.get("no_timestamp", 0) + 1
            continue
        stamp = int(created) * 1_000_000
        # A negative amount is a sale: the direction belongs in `aggressor`,
        # while price and size stay positive the way every other venue writes
        # them.
        aggressor = "sell" if amount < 0 or shares < 0 else "buy"
        rows["ts_init"].append(stamp)
        rows["ts_event"].append(stamp)
        rows["instrument_id"].append(contract)
        rows["outcome"].append(_MANIFOLD_OUTCOMES.index(outcome_label))
        rows["price"].append(abs(amount) / abs(shares))
        rows["size"].append(abs(shares))
        rows["aggressor"].append(aggressor)
        rows["trade_id"].append(str(bet.get("id")) if bet.get("id") else None)
        rows["source_vendor"].append(source_vendor)

    return pa.table(
        {
            name: pa.array(values, type=TRADES_SCHEMA.field(name).type)
            for name, values in rows.items()
        },
        schema=TRADES_SCHEMA,
    )


def ingest_manifold_bets(
    db: Any,
    *,
    bets: Iterable[Mapping[str, Any]],
    markets: Optional[Sequence[MarketSpec]] = None,
    chunk_rows: int = 250_000,
    note: Optional[str] = None,
) -> IngestReport:
    """Normalise a Manifold bet log into the `trades` table."""
    counters: dict[str, int] = {}
    trades = manifold_trades_from_json(bets, markets=markets, skipped=counters)
    report = IngestReport(vendor="manifold")
    if counters:
        report.skipped.append({"reason": "not_a_print", "counts": counters})
    if trades.num_rows:
        ensure_tables(db, ["trades"])
        report.tables["trades"] = commit(
            db, "trades", trades, note=note, chunk_rows=chunk_rows
        )
        stamps = pc.cast(trades.column("ts_init"), pa.int64())
        report.loaded_window = (pc.min(stamps).as_py(), pc.max(stamps).as_py())
    else:
        report.skipped.append({"reason": "no_rows_matched"})
    return report


def references_from_series(
    observations: Iterable[tuple[Any, Any]],
    *,
    instrument_id: str,
    published_after: str | int,
    field: str = "mark",
    outcome: int = 0,
    source_vendor: str = "series",
    time_unit: str = "auto",
) -> pa.Table:
    """Canonical `references` rows from a published `(time, value)` series.

    This is the on-ramp for a macro or index series read alongside market data:
    a policy rate, a benchmark yield, an index level. Whatever publishes it,
    the shape is the same and so is the trap.

    `published_after` is that trap, and it has no default. A series value is
    stamped with the period it *describes*, and that period has already ended by
    the time anybody can read the number: a daily rate for Monday is published
    on Tuesday. Stamping `ts_init` at the period would let a strategy read
    Monday's close on Monday morning, which is not a subtle edge. So `ts_event`
    is the period and `ts_init` is the period plus this lag, and the caller has
    to say what the lag is because only they know their publisher's schedule.

    Values that do not parse as a number are dropped rather than zeroed: a
    missing observation is a hole, and a hole read as zero is a rate cut.
    """
    from ._bars import _to_nanos

    if field not in ("mark", "oracle"):
        raise ValueError("field must be 'mark' or 'oracle'")
    lag = parse_interval(published_after)

    stamps_raw: list[Any] = []
    values: list[float] = []
    for when, value in observations:
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            continue
        if numeric != numeric:  # NaN, which a publisher uses for "no reading"
            continue
        stamps_raw.append(when)
        values.append(numeric)

    if not stamps_raw:
        return REFERENCES_SCHEMA.empty_table()

    column = pa.array(stamps_raw)
    events = _to_nanos(column, time_unit)
    inits = pc.add(events, pa.scalar(lag, pa.int64()))
    count = len(values)
    other = "oracle" if field == "mark" else "mark"
    return pa.table(
        {
            "ts_init": pc.cast(inits, pa.timestamp("ns")),
            "ts_event": pc.cast(events, pa.timestamp("ns")),
            "instrument_id": pa.array([instrument_id] * count, pa.string()),
            "outcome": pa.array([int(outcome)] * count, pa.uint16()),
            field: pa.array(values, pa.float64()),
            other: pa.nulls(count, pa.float64()),
            "source_vendor": pa.array([source_vendor] * count, pa.string()),
        },
        schema=REFERENCES_SCHEMA,
    )


def ingest_references(
    db: Any,
    *,
    tables: Iterable[pa.Table],
    chunk_rows: int = 250_000,
    note: Optional[str] = None,
) -> IngestReport:
    """Commit one or more reference tables, ordered and content-addressed."""
    combined = concat(list(tables), REFERENCES_SCHEMA)
    report = IngestReport(vendor="references")
    if not combined.num_rows:
        report.skipped.append({"reason": "no_rows_matched"})
        return report
    ensure_tables(db, ["references"])
    report.tables["references"] = commit(
        db, "references", combined, note=note, chunk_rows=chunk_rows
    )
    stamps = pc.cast(combined.column("ts_init"), pa.int64())
    report.loaded_window = (pc.min(stamps).as_py(), pc.max(stamps).as_py())
    return report
