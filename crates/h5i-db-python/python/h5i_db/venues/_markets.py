"""Market definitions: the step between "I have a slug" and a replayable table.

A market spec is the identity of one tradeable event: which instrument, how
many outcomes, which vendor token maps to which outcome, when trading stops,
and when the result became knowable. Everything downstream depends on it, and
getting the outcome order wrong silently attributes one side's fills to the
other, so this layer refuses ambiguity rather than resolving it.

Nothing here reaches the network. Vendor payloads arrive as already-fetched
JSON, which keeps fetching in scripts (where credentials and retries belong)
and parsing here (where it can be tested offline).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Optional, Sequence

import pyarrow as pa

from ._canonical import (
    INSTRUMENTS_SCHEMA,
    RESOLUTIONS_SCHEMA,
    IngestReport,
    commit,
    ensure_tables,
)

__all__ = [
    "MarketSpec",
    "polymarket_markets_from_json",
    "write_markets",
]


@dataclass(frozen=True)
class MarketSpec:
    """One market, resolved to the fields the replay kernel needs.

    `outcome_labels` and `tokens` are positional: index `i` of each describes
    outcome `i`. That is the whole contract, and it is why both are required
    together when tokens are supplied.
    """

    instrument_id: str
    venue: str
    outcome_labels: tuple[str, ...]
    tokens: tuple[str, ...] = ()
    kind: str = "prediction_market"
    tick_size: float = 0.001
    lot_size: float = 1.0
    defined_ns: int = 0
    expiration_ns: Optional[int] = None
    settlement_observable_ns: Optional[int] = None
    winner_outcome: Optional[int] = None
    metadata: Mapping[str, Any] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        object.__setattr__(self, "outcome_labels", tuple(self.outcome_labels))
        object.__setattr__(self, "tokens", tuple(self.tokens))
        object.__setattr__(self, "metadata", dict(self.metadata or {}))
        if not self.instrument_id:
            raise ValueError("instrument_id must be non-empty")
        if not self.venue:
            raise ValueError("venue must be non-empty")
        if len(self.outcome_labels) < 2:
            raise ValueError(
                f"{self.instrument_id}: a prediction market needs at least two "
                "outcomes; a market with one possible result is not a market"
            )
        if len(set(self.outcome_labels)) != len(self.outcome_labels):
            raise ValueError(
                f"{self.instrument_id}: outcome labels must be distinct, got "
                f"{self.outcome_labels}"
            )
        if self.tokens and len(self.tokens) != len(self.outcome_labels):
            raise ValueError(
                f"{self.instrument_id}: {len(self.tokens)} tokens for "
                f"{len(self.outcome_labels)} outcomes; index i of each must "
                "describe the same outcome"
            )
        if self.tokens and len(set(self.tokens)) != len(self.tokens):
            raise ValueError(
                f"{self.instrument_id}: token ids must be distinct, otherwise "
                "one token would resolve to two outcomes"
            )
        if self.tick_size <= 0 or self.lot_size <= 0:
            raise ValueError(f"{self.instrument_id}: tick_size and lot_size must be > 0")
        if self.winner_outcome is not None and not (
            0 <= self.winner_outcome < len(self.outcome_labels)
        ):
            raise ValueError(
                f"{self.instrument_id}: winner_outcome {self.winner_outcome} is not "
                f"one of 0..{len(self.outcome_labels) - 1}"
            )
        if self.winner_outcome is not None and self.settlement_observable_ns is None:
            raise ValueError(
                f"{self.instrument_id}: a resolved market needs "
                "settlement_observable_ns; settlement is gated on when the "
                "result became knowable, not on when it was recorded"
            )
        if (
            self.expiration_ns is not None
            and self.settlement_observable_ns is not None
            and self.settlement_observable_ns < self.expiration_ns
        ):
            raise ValueError(
                f"{self.instrument_id}: the result cannot become observable "
                "before trading stops"
            )

    @property
    def outcome_count(self) -> int:
        return len(self.outcome_labels)

    def outcome_of_token(self, token: str) -> int:
        """Map a vendor token id to its outcome index."""
        if not self.tokens:
            raise ValueError(
                f"{self.instrument_id}: no token map; supply tokens= to ingest "
                "vendor data keyed by token id"
            )
        try:
            return self.tokens.index(token)
        except ValueError as error:
            raise KeyError(
                f"token {token!r} is not one of {self.instrument_id}'s "
                f"{len(self.tokens)} tokens"
            ) from error

    @property
    def is_resolved(self) -> bool:
        return self.winner_outcome is not None


def _token_index(specs: Sequence[MarketSpec]) -> dict[str, tuple[MarketSpec, int]]:
    """Token id to (market, outcome), refusing a token claimed twice.

    A token that two markets both claim would make every row keyed by it
    ambiguous. Naming the collision is the only safe response: the silent
    alternative attributes one market's book to another.
    """
    index: dict[str, tuple[MarketSpec, int]] = {}
    for spec in specs:
        for outcome, token in enumerate(spec.tokens):
            existing = index.get(token)
            if existing is not None and existing[0].instrument_id != spec.instrument_id:
                raise ValueError(
                    f"token {token!r} is claimed by both "
                    f"{existing[0].instrument_id!r} and {spec.instrument_id!r}"
                )
            index[token] = (spec, outcome)
    return index


def _as_nanos(value: Any) -> Optional[int]:
    """Vendor timestamps to epoch nanoseconds, or None when absent.

    Accepts what the public payloads actually carry: ISO-8601 strings, epoch
    seconds, epoch milliseconds, and datetimes. Integers are disambiguated by
    magnitude, which is unavoidable and worth stating: values under 1e11 are
    read as seconds, under 1e14 as milliseconds, otherwise as nanoseconds.
    """
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise TypeError("a timestamp must not be a bool")
    import datetime as _dt

    if isinstance(value, _dt.datetime):
        moment = value if value.tzinfo else value.replace(tzinfo=_dt.timezone.utc)
        return int(moment.timestamp() * 1_000_000_000)
    if isinstance(value, (int, float)):
        magnitude = abs(float(value))
        if magnitude < 1e11:
            return int(value * 1_000_000_000)
        if magnitude < 1e14:
            return int(value * 1_000_000)
        return int(value)
    if isinstance(value, str):
        text = value.strip()
        if text.isdigit() or (text.startswith("-") and text[1:].isdigit()):
            return _as_nanos(int(text))
        moment = _dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
        return _as_nanos(moment)
    raise TypeError(f"cannot read {value!r} as a timestamp")


def polymarket_markets_from_json(
    payloads: Iterable[Mapping[str, Any]],
    *,
    venue: str = "polymarket",
    default_tick_size: float = 0.001,
    default_lot_size: float = 1.0,
    require_resolution: bool = False,
) -> list[MarketSpec]:
    """Build market specs from already-fetched Polymarket market payloads.

    Handles the field spellings the public market endpoints use, including the
    JSON-encoded-string form of the list fields that the Gamma API returns.
    A market whose outcome list and token list disagree in length is refused
    rather than zipped to the shorter one, because a short zip silently drops
    an outcome.

    `require_resolution=True` refuses an unresolved market, which is what a
    settlement study wants; the default keeps live markets usable for
    execution-only work.
    """
    import json as _json

    def _listish(value: Any) -> list[Any]:
        if value is None:
            return []
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return []
            try:
                decoded = _json.loads(text)
            except ValueError:
                return [text]
            return list(decoded) if isinstance(decoded, list) else [decoded]
        if isinstance(value, (list, tuple)):
            return list(value)
        return [value]

    def _first(payload: Mapping[str, Any], *names: str) -> Any:
        for name in names:
            if name in payload and payload[name] not in (None, ""):
                return payload[name]
        return None

    specs: list[MarketSpec] = []
    for payload in payloads:
        instrument_id = _first(
            payload, "condition_id", "conditionId", "id", "slug", "market"
        )
        if instrument_id is None:
            raise ValueError(
                "market payload has no condition_id, id or slug to identify it"
            )
        labels = [str(item) for item in _listish(_first(payload, "outcomes", "outcome"))]
        tokens_raw = _listish(
            _first(payload, "clobTokenIds", "clob_token_ids", "tokens", "token_ids")
        )
        tokens: list[str] = []
        token_labels: list[str] = []
        for item in tokens_raw:
            if isinstance(item, Mapping):
                token_id = _first(item, "token_id", "tokenId", "id")
                if token_id is None:
                    raise ValueError(
                        f"{instrument_id}: a token entry carries no token_id"
                    )
                tokens.append(str(token_id))
                label = _first(item, "outcome", "label", "name")
                if label is not None:
                    token_labels.append(str(label))
            else:
                tokens.append(str(item))
        # The token objects carry their own outcome names on the CLOB endpoint;
        # prefer them, because they are ordered with the tokens by construction.
        if token_labels and len(token_labels) == len(tokens):
            labels = token_labels
        if not labels:
            raise ValueError(
                f"{instrument_id}: no outcome labels; a market needs named outcomes"
            )
        if tokens and len(tokens) != len(labels):
            raise ValueError(
                f"{instrument_id}: {len(tokens)} tokens against {len(labels)} "
                "outcomes; the payload is not self-consistent"
            )

        winner: Optional[int] = None
        winner_label = _first(payload, "winning_outcome", "winningOutcome")
        prices = _listish(_first(payload, "outcomePrices", "outcome_prices"))
        if winner_label is not None:
            wanted = str(winner_label)
            if wanted not in labels:
                raise ValueError(
                    f"{instrument_id}: winning outcome {wanted!r} is not one of "
                    f"{labels}"
                )
            winner = labels.index(wanted)
        elif prices and len(prices) == len(labels):
            # A closed market reports its settled prices as 1 and 0. Only treat
            # that as a resolution when exactly one outcome is at 1: anything
            # else is a live quote, not a result.
            settled = [index for index, price in enumerate(prices) if float(price) == 1.0]
            zeros = [index for index, price in enumerate(prices) if float(price) == 0.0]
            closed = bool(_first(payload, "closed", "is_closed", "resolved"))
            if closed and len(settled) == 1 and len(zeros) == len(labels) - 1:
                winner = settled[0]

        observable = _as_nanos(
            _first(
                payload,
                "settlement_observable_ns",
                "umaResolutionTime",
                "uma_resolution_time",
                "resolvedTime",
                "resolved_time",
                "closedTime",
                "closed_time",
            )
        )
        expiration = _as_nanos(
            _first(payload, "expiration_ns", "endDate", "end_date_iso", "end_date")
        )
        if winner is not None and observable is None:
            # Settlement is gated on observability, so a resolution with no
            # observability instant is unusable. Falling back to expiry would
            # book a result at an instant nobody could have traded on.
            raise ValueError(
                f"{instrument_id}: resolved to {labels[winner]!r} but the payload "
                "carries no resolution time; settlement needs the instant the "
                "result became knowable"
            )
        if require_resolution and winner is None:
            raise ValueError(f"{instrument_id}: no resolution in the payload")

        specs.append(
            MarketSpec(
                instrument_id=str(instrument_id),
                venue=venue,
                outcome_labels=tuple(labels),
                tokens=tuple(tokens),
                tick_size=float(_first(payload, "tick_size", "tickSize") or default_tick_size),
                lot_size=float(
                    _first(payload, "lot_size", "minimum_order_size") or default_lot_size
                ),
                defined_ns=_as_nanos(_first(payload, "startDate", "start_date_iso")) or 0,
                expiration_ns=expiration,
                settlement_observable_ns=observable,
                winner_outcome=winner,
                metadata={
                    key: payload[key]
                    for key in ("slug", "question", "market_slug")
                    if key in payload
                },
            )
        )
    return specs


def write_markets(
    db: Any,
    specs: Sequence[MarketSpec],
    *,
    note: Optional[str] = None,
) -> IngestReport:
    """Write `instruments`, and `resolutions` for the markets that resolved.

    One row per outcome in `instruments`; one row per resolved market in
    `resolutions`, dated by observability. Unresolved markets are reported in
    the result rather than written with a placeholder winner.
    """
    if not specs:
        raise ValueError("write_markets needs at least one market")
    _token_index(specs)  # refuse duplicate tokens before writing anything

    seen: dict[str, MarketSpec] = {}
    for spec in specs:
        previous = seen.get(spec.instrument_id)
        if previous is not None and previous != spec:
            raise ValueError(
                f"{spec.instrument_id} was supplied twice with different definitions"
            )
        seen[spec.instrument_id] = spec
    ordered = [seen[key] for key in sorted(seen)]

    instrument_rows: dict[str, list[Any]] = {
        name: [] for name in INSTRUMENTS_SCHEMA.names
    }
    resolution_rows: dict[str, list[Any]] = {
        name: [] for name in RESOLUTIONS_SCHEMA.names
    }
    unresolved: list[str] = []
    for spec in ordered:
        for outcome, label in enumerate(spec.outcome_labels):
            instrument_rows["ts_init"].append(spec.defined_ns)
            instrument_rows["instrument_id"].append(spec.instrument_id)
            instrument_rows["venue"].append(spec.venue)
            instrument_rows["kind"].append(spec.kind)
            instrument_rows["outcome"].append(outcome)
            instrument_rows["outcome_label"].append(label)
            instrument_rows["tick_size"].append(spec.tick_size)
            instrument_rows["lot_size"].append(spec.lot_size)
            instrument_rows["expiration_ns"].append(spec.expiration_ns)
            instrument_rows["settlement_observable_ns"].append(
                spec.settlement_observable_ns
            )
        if spec.is_resolved:
            resolution_rows["ts_init"].append(spec.settlement_observable_ns)
            resolution_rows["instrument_id"].append(spec.instrument_id)
            resolution_rows["winner_outcome"].append(spec.winner_outcome)
        else:
            unresolved.append(spec.instrument_id)

    wanted = ["instruments"] + (["resolutions"] if resolution_rows["ts_init"] else [])
    ensure_tables(db, wanted)
    report = IngestReport(vendor="markets")
    if unresolved:
        # Not an error: a live market has no winner yet. Reported so a
        # settlement study can refuse to run against it.
        report.skipped.append({"reason": "unresolved", "markets": unresolved})
    instruments = pa.table(
        {
            name: pa.array(values, type=INSTRUMENTS_SCHEMA.field(name).type)
            for name, values in instrument_rows.items()
        },
        schema=INSTRUMENTS_SCHEMA,
    )
    report.tables["instruments"] = commit(db, "instruments", instruments, note=note)
    if resolution_rows["ts_init"]:
        resolutions = pa.table(
            {
                name: pa.array(values, type=RESOLUTIONS_SCHEMA.field(name).type)
                for name, values in resolution_rows.items()
            },
            schema=RESOLUTIONS_SCHEMA,
        )
        report.tables["resolutions"] = commit(db, "resolutions", resolutions, note=note)
    return report


def token_index(specs: Sequence[MarketSpec]) -> dict[str, tuple[MarketSpec, int]]:
    """Public wrapper: token id to (market, outcome index)."""
    return _token_index(specs)
