"""Rich, lazy access to a backtest run stored on an h5i-db fork."""

from __future__ import annotations

import html
import json
import secrets
from pathlib import Path
from typing import Any, Iterable, Optional, Union

import pyarrow as pa

from .backtest_config import BacktestConfig, PreflightInspection

__all__ = [
    "BacktestResult",
    "find_trial",
    "list_runs",
    "open_result",
    "trial_count",
]

_RESULT_TABLES = (
    "bt_run",
    "bt_orders",
    "bt_fills",
    "bt_positions",
    "bt_equity",
    "bt_config",
)
_CONFIG_SCHEMA = pa.schema(
    [
        pa.field("run_id", pa.string(), nullable=False),
        pa.field("config_digest", pa.string(), nullable=False),
        pa.field("trial_digest", pa.string(), nullable=False),
        pa.field("config_json", pa.string(), nullable=False),
        pa.field("fidelity", pa.string(), nullable=False),
        pa.field("inspection_json", pa.string(), nullable=False),
    ]
)


def _persist_config(
    db: Any,
    fork_name: str,
    config: BacktestConfig,
    inspection: PreflightInspection,
) -> None:
    fork = db.fork(fork_name)
    try:
        if "bt_config" not in fork.tables():
            fork.create_table("bt_config", _CONFIG_SCHEMA)
            fork.append(
                "bt_config",
                pa.table(
                    {
                        "run_id": [config.run_id],
                        "config_digest": [config.digest],
                        "trial_digest": [config.trial_digest],
                        "config_json": [config.to_json(indent=None)],
                        "fidelity": [inspection.fidelity.value],
                        "inspection_json": [
                            json.dumps(
                                inspection.to_dict(), sort_keys=True, default=str
                            )
                        ],
                    },
                    schema=_CONFIG_SCHEMA,
                ),
                note="typed backtest configuration and preflight evidence",
            )
    finally:
        fork.close()


class BacktestResult(dict):
    """Run manifest plus lazy accessors for all fork-resident output tables.

    It subclasses ``dict`` so existing code using ``report["fills"]`` remains
    source-compatible while new code gains discoverable methods.
    """

    def __init__(
        self,
        report: dict[str, Any],
        *,
        db: Any,
        config: Optional[BacktestConfig] = None,
        inspection: Optional[PreflightInspection] = None,
    ) -> None:
        super().__init__(report)
        self._db = db
        self.config = config
        self.inspection = inspection

    @property
    def fork_name(self) -> str:
        return str(self["fork"])

    @property
    def run_id(self) -> str:
        if self.config is not None:
            return self.config.run_id
        return self.fork_name.removeprefix("bt-")

    def table(self, name: str) -> pa.Table:
        if name not in _RESULT_TABLES:
            raise ValueError(
                f"unknown result table {name!r}; expected one of {_RESULT_TABLES}"
            )
        fork = self._db.fork(self.fork_name)
        try:
            if name not in fork.tables():
                return pa.table({})
            return fork.read(name)
        finally:
            fork.close()

    @property
    def run(self) -> pa.Table:
        return self.table("bt_run")

    @property
    def orders(self) -> pa.Table:
        return self.table("bt_orders")

    @property
    def fills(self) -> pa.Table:
        return self.table("bt_fills")

    @property
    def positions(self) -> pa.Table:
        return self.table("bt_positions")

    @property
    def equity(self) -> pa.Table:
        return self.table("bt_equity")

    def summary(self) -> dict[str, Any]:
        keys = (
            "run_id",
            "fork",
            "digest",
            "records_processed",
            "orders",
            "fills",
            "final_cash",
            "realized_pnl",
            "commissions",
            "equity_points",
            "coverage",
            "warnings",
        )
        return {key: self[key] for key in keys if key in self}

    def stats(self) -> dict[str, Any]:
        from . import quant

        fork = self._db.fork(self.fork_name)
        try:
            if (
                "bt_equity" not in fork.tables()
                or fork.read("bt_equity", limit=1).num_rows == 0
            ):
                return {}
            return quant.from_levels(fork, "bt_equity").stats()
        finally:
            fork.close()

    def tearsheet(
        self,
        path: Optional[Union[str, Path]] = None,
        *,
        title: Optional[str] = None,
    ) -> str:
        from . import quant

        fork = self._db.fork(self.fork_name)
        try:
            series = quant.from_levels(fork, "bt_equity")
            return quant.tearsheet(
                series,
                path=path,
                title=title or f"Backtest: {self.run_id}",
            )
        finally:
            fork.close()

    def plot(self, *, ax: Any = None) -> Any:
        """Plot equity without making matplotlib a mandatory dependency."""
        try:
            import matplotlib.pyplot as plt
        except ImportError as exc:
            raise ImportError("plot() requires matplotlib") from exc
        equity = self.equity.to_pandas()
        if equity.empty:
            raise ValueError("the run has no equity samples to plot")
        if ax is None:
            _, ax = plt.subplots(figsize=(10, 4))
        ax.plot(equity["ts"], equity["equity"], label=self.run_id)
        ax.set(title=f"Backtest equity: {self.run_id}", xlabel="time", ylabel="equity")
        return ax

    def explain(self, order_id: Optional[int] = None) -> dict[str, Any]:
        """Explain run silence, rejections, cancellations, and fill fragmentation."""
        orders = self.orders.to_pylist()
        fills = self.fills.to_pylist()
        if order_id is not None:
            matched = [row for row in orders if row.get("order_id") == order_id]
            if not matched:
                raise KeyError(f"order {order_id} is not present in {self.fork_name}")
            order_fills = [row for row in fills if row.get("order_id") == order_id]
            return {
                "order": matched[0],
                "fills": order_fills,
                "fill_count": len(order_fills),
                "filled_quantity": sum(float(row["quantity"]) for row in order_fills),
            }
        status_counts: dict[str, int] = {}
        rejection_reasons: dict[str, int] = {}
        fills_per_order: dict[int, int] = {}
        for row in orders:
            status = str(row.get("status"))
            status_counts[status] = status_counts.get(status, 0) + 1
            reason = row.get("reject_reason")
            if reason:
                rejection_reasons[str(reason)] = (
                    rejection_reasons.get(str(reason), 0) + 1
                )
        for row in fills:
            key = int(row["order_id"])
            fills_per_order[key] = fills_per_order.get(key, 0) + 1
        return {
            "warnings": list(self.get("warnings", [])),
            "status_counts": status_counts,
            "rejection_reasons": rejection_reasons,
            "orders_without_fills": sum(
                1 for row in orders if int(row["order_id"]) not in fills_per_order
            ),
            "orders_sweeping_multiple_levels": sum(
                1 for count in fills_per_order.values() if count > 1
            ),
            "fidelity": (
                self.inspection.fidelity.value if self.inspection is not None else None
            ),
        }

    def compare(self, other: "BacktestResult") -> dict[str, Any]:
        if not isinstance(other, BacktestResult):
            raise TypeError("other must be a BacktestResult")
        numeric = (
            "records_processed",
            "orders",
            "fills",
            "final_cash",
            "realized_pnl",
            "commissions",
            "equity_points",
            "coverage",
        )
        changes = {}
        for key in numeric:
            if key in self and key in other:
                before, after = self[key], other[key]
                delta = (
                    after - before
                    if isinstance(before, (int, float))
                    and not isinstance(before, bool)
                    and isinstance(after, (int, float))
                    and not isinstance(after, bool)
                    else None
                )
                changes[key] = {
                    "left": before,
                    "right": after,
                    "delta": delta,
                    "equal": before == after,
                }
        return {
            "left": self.run_id,
            "right": other.run_id,
            "same_digest": self.get("digest") == other.get("digest"),
            "metrics": changes,
        }

    def verify(self, *, strategy: Any = None) -> dict[str, Any]:
        """Re-execute from persisted configuration and compare deterministic output."""
        if self.config is None:
            raise ValueError("this result has no persisted BacktestConfig")
        from .backtest import execute

        suffix = secrets.token_hex(6)
        verify_config = BacktestConfig.from_dict(
            {**self.config.to_dict(), "run_id": f"verify-{self.run_id}-{suffix}"}
        )
        candidate: Optional[BacktestResult] = None
        try:
            candidate = execute(self._db, verify_config, strategy=strategy)
            compared = self.compare(candidate)
            compared["tables_equal"] = {
                name: self.table(name).equals(candidate.table(name))
                for name in ("bt_orders", "bt_fills", "bt_positions", "bt_equity")
            }
            compared["verified"] = all(
                item["equal"] for item in compared["metrics"].values()
            ) and all(compared["tables_equal"].values())
            return compared
        finally:
            if candidate is not None:
                self._db.drop_fork(candidate.fork_name)

    def promote(self, tables: Optional[Iterable[str]] = None) -> list[dict[str, Any]]:
        """Promote explicitly selected result tables from the run fork."""
        selected = tuple(tables) if tables is not None else _RESULT_TABLES
        unknown = set(selected) - set(_RESULT_TABLES)
        if unknown:
            raise ValueError(f"unknown result tables: {sorted(unknown)}")
        return [self._db.promote(self.fork_name, table) for table in selected]

    def drop(self) -> int:
        return self._db.drop_fork(self.fork_name)

    def report(
        self,
        path: Optional[Union[str, Path]] = None,
        *,
        title: Optional[str] = None,
    ) -> str:
        """One self-contained HTML file: evidence, performance, executions.

        No network access at view time and no dependencies, so the file can
        be attached to a review, committed beside the run, or opened years
        later. A run without an equity curve still reports: the performance
        panels drop out and the execution evidence remains.
        """
        from . import quant

        return quant.report.backtest_report(
            self, path=str(path) if path is not None else None, title=title
        )

    def html_summary(self, path: Optional[Union[str, Path]] = None) -> str:
        """The report, under its original name."""
        return self.report(path)

    def _repr_html_(self) -> str:
        """Render inside a notebook without leaking styles into it.

        The report is a whole document with its own theme; an iframe is what
        keeps that from repainting the notebook around it.
        """
        document = html.escape(self.report(), quote=True)
        return (
            f'<iframe srcdoc="{document}" style="width:100%;height:820px;'
            'border:1px solid rgba(127,127,127,.35);border-radius:8px" '
            'loading="lazy"></iframe>'
        )


def open_result(db: Any, run_id: str) -> BacktestResult:
    fork_name = run_id if run_id.startswith("bt-") else f"bt-{run_id}"
    fork = db.fork(fork_name)
    try:
        if "bt_run" not in fork.tables():
            raise ValueError(f"fork {fork_name!r} is not a backtest run")
        rows = fork.read("bt_run").to_pylist()
        if len(rows) != 1:
            raise ValueError(f"{fork_name!r} has {len(rows)} bt_run rows; expected one")
        report = dict(rows[0])
        report["fork"] = fork_name
        config = None
        inspection = None
        if "bt_config" in fork.tables():
            metadata = fork.read("bt_config").to_pylist()[0]
            config = BacktestConfig.from_json(metadata["config_json"])
            from .backtest_config import InspectionIssue, ReplayFidelity

            raw = json.loads(metadata["inspection_json"])
            inspection = PreflightInspection(
                config_digest=raw["config_digest"],
                fidelity=ReplayFidelity(raw["fidelity"]),
                tables=raw["tables"],
                capabilities=raw["capabilities"],
                issues=tuple(InspectionIssue(**issue) for issue in raw["issues"]),
            )
        return BacktestResult(report, db=db, config=config, inspection=inspection)
    finally:
        fork.close()


def list_runs(db: Any) -> list[dict[str, Any]]:
    runs = []
    for fork_name in sorted(name for name in db.fork_names() if name.startswith("bt-")):
        try:
            result = open_result(db, fork_name)
        except (ValueError, KeyError):
            continue
        runs.append(result.summary())
    return runs


def find_trial(db: Any, trial_digest: str) -> Optional[BacktestResult]:
    """Return the recorded trial for a semantic config digest, if any."""
    match = None
    for fork_name in sorted(
        name for name in db.fork_names() if name.startswith("bt-")
    ):
        fork = db.fork(fork_name)
        try:
            if "bt_config" not in fork.tables():
                continue
            rows = fork.read("bt_config").to_pylist()
            if len(rows) != 1:
                continue
            row = rows[0]
            recorded = row.get("trial_digest")
            if recorded is None:
                recorded = BacktestConfig.from_json(row["config_json"]).trial_digest
            if recorded == trial_digest:
                match = fork_name
                break
        finally:
            fork.close()
    return open_result(db, match) if match is not None else None


def trial_count(db: Any) -> int:
    """Count distinct score-producing configurations in the trial ledger."""
    digests: set[str] = set()
    for fork_name in sorted(
        name for name in db.fork_names() if name.startswith("bt-")
    ):
        fork = db.fork(fork_name)
        try:
            tables = fork.tables()
            if "bt_config" in tables:
                for row in fork.read("bt_config").to_pylist():
                    digest = row.get("trial_digest")
                    if digest is None:
                        digest = BacktestConfig.from_json(
                            row["config_json"]
                        ).trial_digest
                    digests.add(str(digest))
            elif "bt_run" in tables:
                # Native/Rust-created runs may not carry the Python typed
                # config table. They are still ledger trials and must never
                # disappear from the search budget.
                for row in fork.read("bt_run").to_pylist():
                    digest = row.get("config_digest") or row.get("digest")
                    if digest is not None:
                        digests.add(str(digest))
        finally:
            fork.close()
    return len(digests)
