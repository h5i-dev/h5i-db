"""Semantic attention state for agent-native backtest experiments."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Any

__all__ = [
    "AttentionState",
    "TrialAttention",
    "attention_for_trial",
    "rollup_attention",
]


class AttentionState(str, Enum):
    NEEDS_DECISION = "needs-decision"
    FAILED_WARNED = "failed/warned"
    FINISHED_UNSEEN = "finished-unseen"
    RUNNING = "running"
    SEEN = "seen"

    @property
    def priority(self) -> int:
        return {
            AttentionState.NEEDS_DECISION: 5,
            AttentionState.FAILED_WARNED: 4,
            AttentionState.FINISHED_UNSEEN: 3,
            AttentionState.RUNNING: 2,
            AttentionState.SEEN: 1,
        }[self]


@dataclass(frozen=True)
class TrialAttention:
    trial: int
    state: AttentionState
    seen: bool
    warnings: tuple[str, ...] = ()
    needs_decision: bool = False

    @property
    def priority(self) -> int:
        return self.state.priority


def _warnings(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,) if value else ()
    if isinstance(value, Sequence):
        return tuple(str(item) for item in value if item)
    return (str(value),)


def attention_for_trial(row: Mapping[str, Any]) -> TrialAttention:
    seen = bool(row.get("seen", False))
    warnings = _warnings(row.get("warnings"))
    needs_decision = bool(row.get("needs_decision", False))
    status = str(row.get("status", "ok")).lower()
    if needs_decision:
        state = AttentionState.NEEDS_DECISION
    elif status == "running":
        state = AttentionState.RUNNING
    elif seen:
        state = AttentionState.SEEN
    elif status in {"failed", "warned", "error"} or warnings:
        state = AttentionState.FAILED_WARNED
    else:
        state = AttentionState.FINISHED_UNSEEN
    return TrialAttention(
        trial=int(row.get("trial", 0)),
        state=state,
        seen=seen,
        warnings=warnings,
        needs_decision=needs_decision,
    )


def rollup_attention(trials: Iterable[TrialAttention]) -> AttentionState:
    return max(
        (trial.state for trial in trials),
        key=lambda state: state.priority,
        default=AttentionState.SEEN,
    )
