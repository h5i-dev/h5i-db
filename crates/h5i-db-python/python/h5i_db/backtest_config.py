"""Typed, serializable configuration and preflight inspection for backtests."""

from __future__ import annotations

import datetime as _dt
import hashlib
import json
from dataclasses import asdict, dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Mapping, Optional

__all__ = [
    "BacktestConfig",
    "DataConfig",
    "ExecutionConfig",
    "InspectionIssue",
    "OutputConfig",
    "PortfolioConfig",
    "PreflightInspection",
    "ReplayFidelity",
    "inspect",
]

_SCHEMA_VERSION = 1
_MARKET_TABLES = ("book_deltas", "trades", "bars", "funding", "resolutions")
_REQUIRED_COLUMNS = {
    "instruments": {
        "ts_init",
        "instrument_id",
        "venue",
        "kind",
        "outcome",
        "outcome_label",
        "tick_size",
        "lot_size",
    },
    "book_deltas": {
        "ts_init",
        "ts_event",
        "instrument_id",
        "outcome",
        "action",
        "event_index",
        "is_last",
    },
    "trades": {
        "ts_init",
        "ts_event",
        "instrument_id",
        "outcome",
        "price",
        "size",
    },
    "bars": {
        "ts_init",
        "ts_event",
        "instrument_id",
        "outcome",
        "open",
        "high",
        "low",
        "close",
    },
    "signals": {"ts", "instrument_id", "outcome", "side", "quantity", "kind"},
}


def _json_value(value: Any) -> Any:
    if isinstance(value, _dt.datetime):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_json_value(item) for item in value]
    return value


def _coerce_window(value: Any) -> Optional[tuple[Any, Any]]:
    if value is None:
        return None
    if not isinstance(value, (list, tuple)) or len(value) != 2:
        raise ValueError("window must contain exactly (start, end)")
    return (value[0], value[1])


@dataclass(frozen=True)
class DataConfig:
    """Pinned canonical inputs for one replay."""

    signals: str = "signals"
    snapshot: Optional[str] = None
    version: Optional[int] = None
    as_of: Optional[str] = None
    window: Optional[tuple[Any, Any]] = None
    minimum_coverage: Optional[float] = None

    def __post_init__(self) -> None:
        if not self.signals or not isinstance(self.signals, str):
            raise ValueError("signals must be a non-empty table name")
        pins = [
            self.snapshot is not None,
            self.version is not None,
            self.as_of is not None,
        ]
        if sum(pins) > 1:
            raise ValueError("set at most one of snapshot, version, or as_of")
        if self.version is not None and (
            isinstance(self.version, bool) or self.version < 0
        ):
            raise ValueError("version must be a non-negative integer")
        window = _coerce_window(self.window)
        object.__setattr__(self, "window", window)
        if window is not None:
            from .backtest import _to_nanos

            if _to_nanos(window[0]) >= _to_nanos(window[1]):
                raise ValueError("window start must be before window end")
        coverage = self.minimum_coverage
        if coverage is not None and not 0.0 <= coverage <= 1.0:
            raise ValueError("minimum_coverage must be between zero and one")

    @property
    def is_pinned(self) -> bool:
        return (
            self.snapshot is not None
            or self.version is not None
            or self.as_of is not None
        )

    def read_kwargs(self) -> dict[str, Any]:
        return {
            "snapshot": self.snapshot,
            "version": self.version,
            "as_of": self.as_of,
        }


@dataclass(frozen=True)
class ExecutionConfig:
    """Execution assumptions. No venue fee or latency is guessed."""

    fee_kind: Optional[str] = None
    fee_rate: Optional[float] = None
    maker_rebate: Optional[float] = None
    maker_fee_rate: Optional[float] = None
    queue_position: bool = False
    optimistic_queue: bool = False
    latency_nanos: Optional[int] = None
    slippage_ticks: Optional[int] = None

    def __post_init__(self) -> None:
        if self.fee_kind not in (None, "prediction_market", "proportional", "kalshi"):
            raise ValueError(
                "fee_kind must be prediction_market, proportional, kalshi, or None"
            )
        for name in ("fee_rate", "maker_fee_rate"):
            value = getattr(self, name)
            if value is not None and value < 0:
                raise ValueError(f"{name} must be non-negative")
        if self.maker_rebate is not None and self.maker_rebate > 0:
            raise ValueError("maker_rebate must be zero or negative")
        if self.maker_rebate is not None and self.maker_fee_rate is not None:
            raise ValueError("maker_rebate and maker_fee_rate are mutually exclusive")
        if self.maker_fee_rate is not None and self.fee_kind != "kalshi":
            raise ValueError("maker_fee_rate is only meaningful for fee_kind='kalshi'")
        if self.optimistic_queue and not self.queue_position:
            raise ValueError("optimistic_queue requires queue_position=True")
        if self.queue_position and (self.slippage_ticks or 0) != 0:
            raise ValueError(
                "queue_position and slippage_ticks model different fill assumptions; "
                "run them as separate scenarios"
            )
        if self.latency_nanos is not None and self.latency_nanos < 0:
            raise ValueError("latency_nanos must be non-negative")
        if self.slippage_ticks is not None and self.slippage_ticks < 0:
            raise ValueError("slippage_ticks must be non-negative")


@dataclass(frozen=True)
class PortfolioConfig:
    """Portfolio state at the start of a replay."""

    starting_cash: float

    def __post_init__(self) -> None:
        if not isinstance(self.starting_cash, (int, float)) or isinstance(
            self.starting_cash, bool
        ):
            raise TypeError("starting_cash must be numeric")
        if not 0 < float(self.starting_cash) < float("inf"):
            raise ValueError("starting_cash must be finite and positive")


@dataclass(frozen=True)
class OutputConfig:
    """Run-output controls."""

    equity_interval_nanos: Optional[int] = None

    def __post_init__(self) -> None:
        if self.equity_interval_nanos is not None and self.equity_interval_nanos <= 0:
            raise ValueError("equity_interval_nanos must be positive")


@dataclass(frozen=True)
class BacktestConfig:
    """Complete immutable specification for one backtest."""

    run_id: str
    portfolio: PortfolioConfig
    data: DataConfig = field(default_factory=DataConfig)
    execution: ExecutionConfig = field(default_factory=ExecutionConfig)
    output: OutputConfig = field(default_factory=OutputConfig)
    schema_version: int = _SCHEMA_VERSION
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.run_id or not isinstance(self.run_id, str):
            raise ValueError("run_id must be a non-empty string")
        if self.schema_version != _SCHEMA_VERSION:
            raise ValueError(
                f"unsupported backtest config schema_version {self.schema_version}; "
                f"expected {_SCHEMA_VERSION}"
            )
        try:
            json.dumps(_json_value(dict(self.metadata)), sort_keys=True)
        except (TypeError, ValueError) as exc:
            raise TypeError("metadata must be JSON-serializable") from exc
        object.__setattr__(self, "metadata", dict(self.metadata))

    def to_dict(self) -> dict[str, Any]:
        return _json_value(asdict(self))

    @property
    def digest(self) -> str:
        payload = json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def to_json(self, path: Optional[str | Path] = None, *, indent: int = 2) -> str:
        text = json.dumps(self.to_dict(), sort_keys=True, indent=indent)
        if path is not None:
            Path(path).write_text(text + "\n", encoding="utf-8")
        return text

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "BacktestConfig":
        known = {
            "run_id",
            "portfolio",
            "data",
            "execution",
            "output",
            "schema_version",
            "metadata",
        }
        unknown = set(payload) - known
        if unknown:
            raise ValueError(f"unknown backtest config fields: {sorted(unknown)}")
        data_payload = dict(payload.get("data", {}))
        data_payload["window"] = _coerce_window(data_payload.get("window"))
        return cls(
            run_id=payload["run_id"],
            portfolio=PortfolioConfig(**dict(payload["portfolio"])),
            data=DataConfig(**data_payload),
            execution=ExecutionConfig(**dict(payload.get("execution", {}))),
            output=OutputConfig(**dict(payload.get("output", {}))),
            schema_version=int(payload.get("schema_version", _SCHEMA_VERSION)),
            metadata=dict(payload.get("metadata", {})),
        )

    @classmethod
    def from_json(cls, value: str | Path) -> "BacktestConfig":
        candidate = Path(value) if isinstance(value, Path) else None
        if candidate is not None or (
            isinstance(value, str)
            and not value.lstrip().startswith(("{", "["))
            and Path(value).is_file()
        ):
            text = (candidate or Path(value)).read_text(encoding="utf-8")
        else:
            text = str(value)
        payload = json.loads(text)
        if not isinstance(payload, dict):
            raise ValueError("backtest config JSON must contain an object")
        return cls.from_dict(payload)


class ReplayFidelity(str, Enum):
    """What execution claims the available market data can support."""

    TICK_L2 = "tick_l2"
    SNAPSHOT_L2 = "snapshot_l2"
    TRADES_ONLY = "trades_only"
    BARS_SYNTHETIC = "bars_synthetic"
    NONE = "none"


@dataclass(frozen=True)
class InspectionIssue:
    severity: str
    code: str
    message: str
    remediation: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PreflightInspection:
    config_digest: str
    fidelity: ReplayFidelity
    tables: Mapping[str, Mapping[str, Any]]
    issues: tuple[InspectionIssue, ...] = ()
    capabilities: Mapping[str, bool] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return not any(issue.severity == "error" for issue in self.issues)

    @property
    def errors(self) -> tuple[InspectionIssue, ...]:
        return tuple(issue for issue in self.issues if issue.severity == "error")

    @property
    def warnings(self) -> tuple[InspectionIssue, ...]:
        return tuple(issue for issue in self.issues if issue.severity == "warning")

    def raise_for_errors(self) -> "PreflightInspection":
        if self.errors:
            detail = "; ".join(
                f"{issue.code}: {issue.message}" for issue in self.errors
            )
            raise ValueError(f"backtest preflight failed: {detail}")
        return self

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "config_digest": self.config_digest,
            "fidelity": self.fidelity.value,
            "tables": {name: dict(stats) for name, stats in self.tables.items()},
            "capabilities": dict(self.capabilities),
            "issues": [issue.to_dict() for issue in self.issues],
        }

    def __repr__(self) -> str:
        return (
            f"PreflightInspection(ok={self.ok}, fidelity={self.fidelity.value!r}, "
            f"errors={len(self.errors)}, warnings={len(self.warnings)})"
        )


def _table_source_sql(
    db: Any, name: str, config: DataConfig, *, market_pin: bool = True
) -> str:
    kwargs = config.read_kwargs() if market_pin else {}
    return db.table(name, **kwargs).sql()


def _scalar_stats(
    db: Any,
    name: str,
    time_column: str,
    config: DataConfig,
    *,
    market_pin: bool = True,
) -> dict:
    source = _table_source_sql(db, name, config, market_pin=market_pin)
    table = db.sql(
        "SELECT COUNT(*) AS row_count, "
        f"MIN({time_column}) AS min_time, MAX({time_column}) AS max_time "
        f"FROM (\n{source}\n) AS _h5i_backtest_source"
    )
    row = table.to_arrow().to_pylist()[0]
    return {
        "row_count": int(row["row_count"]),
        "min_time": row["min_time"],
        "max_time": row["max_time"],
    }


def inspect(db: Any, config: BacktestConfig) -> PreflightInspection:
    """Validate pinned inputs and state the strongest defensible replay fidelity."""
    if not isinstance(config, BacktestConfig):
        raise TypeError("config must be a BacktestConfig")
    available = set(db.tables())
    issues: list[InspectionIssue] = []
    stats: dict[str, dict[str, Any]] = {}
    if not config.data.is_pinned:
        issues.append(
            InspectionIssue(
                "warning",
                "unpinned_data",
                "the run reads current table heads and cannot be reproduced "
                "from its config",
                "set data.snapshot, data.as_of, or data.version",
            )
        )

    requested = {"instruments", config.data.signals, *_MARKET_TABLES}
    for name in sorted(requested):
        if name not in available:
            if name in ("instruments", config.data.signals):
                issues.append(
                    InspectionIssue(
                        "error",
                        "missing_table",
                        f"required table {name!r} does not exist",
                    )
                )
            continue
        try:
            is_signals = name == config.data.signals
            read_kwargs = {} if is_signals else config.data.read_kwargs()
            schema = db.read(name, limit=1, **read_kwargs).schema
        except (
            Exception
        ) as exc:  # database errors retain their actionable type in run()
            issues.append(
                InspectionIssue(
                    "error",
                    "unreadable_table",
                    f"cannot read {name!r} at the configured pin: {exc}",
                )
            )
            continue
        columns = set(schema.names)
        standard = "signals" if name == config.data.signals else name
        missing = _REQUIRED_COLUMNS.get(standard, set()) - columns
        if missing:
            issues.append(
                InspectionIssue(
                    "error",
                    "schema_mismatch",
                    f"{name!r} is missing canonical columns {sorted(missing)}",
                )
            )
            continue
        time_column = "ts" if name == config.data.signals else "ts_init"
        table_stats = _scalar_stats(
            db,
            name,
            time_column,
            config.data,
            market_pin=not is_signals,
        )
        table_stats["columns"] = tuple(schema.names)
        stats[name] = table_stats
        if table_stats["row_count"] == 0 and name in (
            "instruments",
            config.data.signals,
        ):
            severity = "error" if name == "instruments" else "warning"
            issues.append(
                InspectionIssue(
                    severity,
                    "empty_table",
                    f"{name!r} contains no rows at the configured pin",
                )
            )

    fidelity = ReplayFidelity.NONE
    actions: dict[str, int] = {}
    if "book_deltas" in stats and stats["book_deltas"]["row_count"] > 0:
        source = _table_source_sql(db, "book_deltas", config.data)
        action_rows = (
            db.sql(
                "SELECT action, COUNT(*) AS n "
                f"FROM (\n{source}\n) AS _h5i_books GROUP BY action"
            )
            .to_arrow()
            .to_pylist()
        )
        actions = {str(row["action"]): int(row["n"]) for row in action_rows}
        stats["book_deltas"]["actions"] = actions
        if any(action in actions for action in ("set", "delete")):
            fidelity = ReplayFidelity.TICK_L2
        else:
            fidelity = ReplayFidelity.SNAPSHOT_L2
        if actions.get("gap", 0):
            issues.append(
                InspectionIssue(
                    "error",
                    "book_gaps",
                    f"book_deltas contains {actions['gap']} explicit gap event(s)",
                    "repair or exclude stale intervals before replay",
                )
            )
    elif "trades" in stats and stats["trades"]["row_count"] > 0:
        fidelity = ReplayFidelity.TRADES_ONLY
    elif "bars" in stats and stats["bars"]["row_count"] > 0:
        fidelity = ReplayFidelity.BARS_SYNTHETIC

    queue_supported = (
        fidelity == ReplayFidelity.TICK_L2
        and stats.get("trades", {}).get("row_count", 0) > 0
    )
    if config.execution.queue_position and not queue_supported:
        issues.append(
            InspectionIssue(
                "error",
                "unsupported_queue_claim",
                f"queue-position replay is not defensible with {fidelity.value} inputs",
                "provide tick L2 deltas plus trades, or disable queue_position",
            )
        )
    if fidelity == ReplayFidelity.SNAPSHOT_L2:
        issues.append(
            InspectionIssue(
                "warning",
                "snapshot_only",
                "periodic snapshots cannot reconstruct queue transitions "
                "between events",
            )
        )
    if fidelity in (ReplayFidelity.TRADES_ONLY, ReplayFidelity.NONE):
        issues.append(
            InspectionIssue(
                "error",
                "no_executable_book",
                "the default matching model needs canonical book data",
            )
        )

    return PreflightInspection(
        config_digest=config.digest,
        fidelity=fidelity,
        tables=stats,
        issues=tuple(issues),
        capabilities={
            "market_orders": fidelity
            in (ReplayFidelity.TICK_L2, ReplayFidelity.SNAPSHOT_L2),
            "limit_orders": fidelity
            in (ReplayFidelity.TICK_L2, ReplayFidelity.SNAPSHOT_L2),
            "queue_position": queue_supported,
            "exact_intraday_path": fidelity == ReplayFidelity.TICK_L2,
        },
    )
