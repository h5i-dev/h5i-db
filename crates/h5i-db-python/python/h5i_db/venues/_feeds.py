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
    BOOK_DELTAS_SCHEMA,
    CORPORATE_ACTIONS_SCHEMA,
    REFERENCES_SCHEMA,
    TRADES_SCHEMA,
    IngestReport,
    commit,
    concat,
    ensure_tables,
)
from ._markets import MarketSpec

__all__ = [
    "corporate_actions_from_rows",
    "ingest_corporate_actions",
    "ingest_predexon_orderbooks",
    "predexon_book_from_snapshots",
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


def predexon_book_from_snapshots(
    snapshots: Iterable[Mapping[str, Any]],
    *,
    markets: Sequence[MarketSpec],
    source_vendor: str = "predexon",
    report: Optional[IngestReport] = None,
) -> pa.Table:
    """Canonical `book_deltas` rows from Predexon's Kalshi orderbook history.

    Every record is a *full* book, so nothing accumulates: each becomes one
    snapshot event per outcome, and a dropped record costs one sample rather
    than corrupting every level after it. That is the main reason to prefer
    this source over an archive of relative deltas.

    Two conversions happen here, both exact.

    Prices arrive as whole cents, so they are divided by 100. Predexon's own
    documentation notes this endpoint rounds to whole cents, which is lossy now
    that Kalshi quotes sub-cent; the loss is the vendor's, not this function's,
    and it is worth knowing before pricing anything at the touch.

    Predexon publishes one YES book with bids and asks, while the outcome-major
    tables here give each outcome its own book of bids. A YES ask at 91 cents
    is a NO bid at 9 cents, the same resting interest described from the other
    side, so asks are folded into outcome 1 at `100 - price`. That keeps this
    source directly comparable with `KALSHI_PMXT_LAYOUT`, which is the point of
    having two sources for one venue.

    `sequence` is deliberately not read. It looks like a per-market update
    counter and is not one: over a single ticker's day it steps by a median of
    45, jumps by as much as 21 million, and runs backwards nine times. Whatever
    it counts is not this market's updates, so differencing it would report
    holes that do not exist. What *is* reported is the sampling cadence, the
    time between consecutive snapshots, which is measured rather than inferred
    and tells you the resolution you actually have.
    """
    by_ticker = {spec.instrument_id: spec for spec in markets}
    rows: dict[str, list[Any]] = {name: [] for name in BOOK_DELTAS_SCHEMA.names}
    event_index = 0
    cadence: dict[str, list[int]] = {}
    last_stamp: dict[str, int] = {}
    unknown: set[str] = set()

    def emit(
        stamp: int, instrument: str, outcome: int, levels: list[tuple[float, float]]
    ) -> None:
        nonlocal event_index
        event_index += 1
        payload = levels or [(None, None)]
        for position, (price, size) in enumerate(payload):
            rows["ts_init"].append(stamp)
            rows["ts_event"].append(stamp)
            rows["instrument_id"].append(instrument)
            rows["outcome"].append(outcome)
            rows["action"].append("snapshot")
            rows["side"].append("buy" if price is not None else None)
            rows["price"].append(price)
            rows["size"].append(size)
            rows["event_index"].append(event_index)
            rows["is_last"].append(position == len(payload) - 1)
            rows["source_vendor"].append(source_vendor)

    for record in snapshots:
        ticker = str(record.get("ticker") or "")
        spec = by_ticker.get(ticker)
        if spec is None:
            unknown.add(ticker)
            continue
        stamp = record.get("timestamp")
        if stamp is None:
            continue
        stamp_ns = int(stamp) * 1_000_000

        previous = last_stamp.get(ticker)
        if previous is not None and stamp_ns > previous:
            cadence.setdefault(ticker, []).append(stamp_ns - previous)
        last_stamp[ticker] = stamp_ns

        yes = [
            (float(level["price"]) / 100.0, float(level["size"]))
            for level in (record.get("yes_bids") or [])
            if level.get("price") is not None and level.get("size") is not None
        ]
        no = [
            ((100.0 - float(level["price"])) / 100.0, float(level["size"]))
            for level in (record.get("yes_asks") or [])
            if level.get("price") is not None and level.get("size") is not None
        ]
        emit(stamp_ns, spec.instrument_id, 0, yes)
        if spec.outcome_count > 1:
            emit(stamp_ns, spec.instrument_id, 1, no)

    if report is not None:
        spans = sorted(span for gaps in cadence.values() for span in gaps)
        if spans:
            # The resolution of this source, measured rather than claimed. A
            # median of seconds is a usable book; a median of minutes means the
            # touch moved unobserved between samples, and a strategy reading it
            # as continuous will fill at prices nobody quoted.
            report.gaps.append(
                {
                    "reason": "snapshot_cadence",
                    "samples": len(spans) + len(cadence),
                    "median_ns": spans[len(spans) // 2],
                    "max_ns": spans[-1],
                }
            )
        if unknown:
            report.unknown_instruments = sorted(unknown)

    return pa.table(
        {
            name: pa.array(values, type=BOOK_DELTAS_SCHEMA.field(name).type)
            for name, values in rows.items()
        },
        schema=BOOK_DELTAS_SCHEMA,
    )


def ingest_predexon_orderbooks(
    db: Any,
    *,
    snapshots: Iterable[Mapping[str, Any]],
    markets: Sequence[MarketSpec],
    chunk_rows: int = 250_000,
    note: Optional[str] = None,
) -> IngestReport:
    """Normalise Predexon orderbook snapshots into `book_deltas`."""
    report = IngestReport(vendor="predexon")
    book = predexon_book_from_snapshots(snapshots, markets=markets, report=report)
    if not book.num_rows:
        report.skipped.append({"reason": "no_rows_matched"})
        return report
    ensure_tables(db, ["book_deltas"])
    report.tables["book_deltas"] = commit(
        db, "book_deltas", book, note=note, chunk_rows=chunk_rows
    )
    stamps = pc.cast(book.column("ts_init"), pa.int64())
    report.loaded_window = (pc.min(stamps).as_py(), pc.max(stamps).as_py())
    return report


#: Which value column each kind carries, and what makes it valid. The engine
#: checks the same things at replay; checking here too means a bad row fails
#: the load rather than a run that is already hours in.
_CORPORATE_KINDS: Mapping[str, tuple[str, str]] = {
    "split": ("ratio", "positive"),
    "dividend": ("per_share", "non-negative"),
    "delist": ("final_price", "non-negative"),
}


def corporate_actions_from_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    source_vendor: str = "corporate",
    time_unit: str = "auto",
) -> pa.Table:
    """Canonical `corporate_actions` rows from vendor records.

    Each row needs `instrument_id`, `kind`, an `effective` instant, and the one
    value its kind carries: `ratio` for a split (new shares per old, so a
    2-for-1 is `2.0`), `per_share` for a dividend, `final_price` for a delist.
    An optional `announced` instant records when it was disclosed.

    `effective` is the replay clock, because that is when the action has to be
    applied to positions and resting orders. Prices are never rewritten: a
    strategy that bought at 50 the day before a 2-for-1 bought at 50, and an
    adjusted series would claim it bought at 25.

    Supplying a value that belongs to a different kind is an error rather than
    an ignored field. A dividend row carrying `ratio` is far more likely to be
    a mis-mapped column than a harmless extra, and silently dropping it would
    load the dividend at whatever `per_share` happened to be there.
    """
    from ._bars import _to_nanos

    effective_raw: list[Any] = []
    announced_raw: list[Any] = []
    has_announced = False
    instruments: list[str] = []
    kinds: list[str] = []
    values: dict[str, list[Optional[float]]] = {
        name: [] for name, _ in _CORPORATE_KINDS.values()
    }

    for index, row in enumerate(rows):
        kind = str(row.get("kind") or "").strip().lower()
        if kind not in _CORPORATE_KINDS:
            raise ValueError(
                f"row {index}: {kind!r} is not a corporate action; expected one "
                f"of {sorted(_CORPORATE_KINDS)}"
            )
        column, rule = _CORPORATE_KINDS[kind]
        if column not in row or row[column] is None:
            raise ValueError(f"row {index}: a {kind} needs {column}")
        try:
            value = float(row[column])
        except (TypeError, ValueError) as error:
            raise ValueError(f"row {index}: {column} is not a number") from error
        if rule == "positive" and value <= 0:
            raise ValueError(
                f"row {index}: a split ratio must be positive, got {value}"
            )
        if rule == "non-negative" and value < 0:
            # A negative dividend is a capital call, which is a different
            # instrument's problem, and a sign error here drains an account.
            raise ValueError(f"row {index}: {column} must not be negative, got {value}")
        for other, _ in _CORPORATE_KINDS.values():
            if other != column and row.get(other) is not None:
                raise ValueError(
                    f"row {index}: a {kind} carries {column}, but this row also "
                    f"has {other}; one of the two columns is mapped wrong"
                )

        effective = row.get("effective")
        if effective is None:
            raise ValueError(f"row {index}: an action needs an effective instant")
        announced = row.get("announced")
        if announced is not None:
            has_announced = True
        effective_raw.append(effective)
        announced_raw.append(announced)
        instruments.append(str(row["instrument_id"]))
        kinds.append(kind)
        for name in values:
            values[name].append(value if name == column else None)

    if not instruments:
        return CORPORATE_ACTIONS_SCHEMA.empty_table()

    effective_ns = _to_nanos(pa.array(effective_raw), time_unit)
    if has_announced:
        # Announced and effective are read on one axis, so they are converted
        # together: a mixed pair (a date string and an epoch int) would
        # otherwise land centuries apart and the comparison below would pass.
        announced_ns = _to_nanos(
            pa.array([a if a is not None else None for a in announced_raw]), time_unit
        )
        late = pc.and_(
            pc.is_valid(announced_ns), pc.greater(announced_ns, effective_ns)
        )
        if pc.any(late).as_py():
            raise ValueError(
                "an action is announced after it takes effect, which usually "
                "means the announced and effective columns are swapped"
            )
    else:
        announced_ns = pa.nulls(len(instruments), pa.int64())

    count = len(instruments)
    return pa.table(
        {
            "ts_init": pc.cast(effective_ns, pa.timestamp("ns")),
            "ts_event": pc.cast(effective_ns, pa.timestamp("ns")),
            "instrument_id": pa.array(instruments, pa.string()),
            "kind": pa.array(kinds, pa.string()),
            "ratio": pa.array(values["ratio"], pa.float64()),
            "per_share": pa.array(values["per_share"], pa.float64()),
            "final_price": pa.array(values["final_price"], pa.float64()),
            "announced_ns": pc.cast(announced_ns, pa.int64()),
            "source_vendor": pa.array([source_vendor] * count, pa.string()),
        },
        schema=CORPORATE_ACTIONS_SCHEMA,
    )


def ingest_corporate_actions(
    db: Any,
    *,
    actions: Iterable[Mapping[str, Any]],
    source_vendor: str = "corporate",
    time_unit: str = "auto",
    known_by: Optional[int] = None,
    chunk_rows: int = 250_000,
    note: Optional[str] = None,
) -> IngestReport:
    """Normalise corporate actions into the `corporate_actions` table.

    `known_by` is an epoch-nanosecond cutoff: rows announced after it are left
    out, which is how a run reproduces what was knowable on a past date instead
    of what is recorded now. Rows with no announcement instant cannot be placed
    on that axis, so they are dropped and counted rather than assumed early
    enough, because assuming is what produces a backtest that traded a split
    nobody had heard of.
    """
    table = corporate_actions_from_rows(
        actions, source_vendor=source_vendor, time_unit=time_unit
    )
    report = IngestReport(vendor=source_vendor)
    if known_by is not None and table.num_rows:
        announced = table.column("announced_ns")
        unknown = pc.sum(pc.cast(pc.is_null(announced), pa.int64())).as_py() or 0
        keep = pc.and_(
            pc.is_valid(announced),
            pc.less_equal(announced, pa.scalar(int(known_by), pa.int64())),
        )
        dropped = table.num_rows - (pc.sum(pc.cast(keep, pa.int64())).as_py() or 0)
        table = table.filter(pc.fill_null(keep, False))
        if dropped:
            report.skipped.append(
                {
                    "reason": "announced_after_cutoff",
                    "rows": int(dropped),
                    "without_announcement": int(unknown),
                }
            )
    if not table.num_rows:
        report.skipped.append({"reason": "no_rows_matched"})
        return report
    ensure_tables(db, ["corporate_actions"])
    report.tables["corporate_actions"] = commit(
        db, "corporate_actions", table, note=note, chunk_rows=chunk_rows
    )
    stamps = pc.cast(table.column("ts_init"), pa.int64())
    report.loaded_window = (pc.min(stamps).as_py(), pc.max(stamps).as_py())
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
