"""One report over many runs, built from stored tables only.

A sweep produces one fork per trial, and comparing twenty of them by opening
twenty tearsheets is not comparing them. This assembles a single document:
portfolio-level panels first, per-market panels for baskets small enough to
read, and fill markers on the price path so an execution question has somewhere
to land.

Nothing here re-simulates. Every panel is a query over `bt_equity`, `bt_fills`,
`bt_positions` and the pinned market data, so a report is cheap, and rebuilding
it a year later against the same forks produces the same document.

The panel set is explicit rather than automatic. A twenty-market basket with
per-market lines is unreadable, and silently dropping series to make it fit
would misrepresent the basket, so the caller chooses and the report records what
it drew.
"""

from __future__ import annotations

import html
import json
import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence, Union

from ._common import quote_string

__all__ = [
    "PANELS",
    "PORTFOLIO_PANELS",
    "BasketReport",
    "basket_payload",
    "basket_report",
]

#: Panels that describe the whole basket. Safe at any size.
PORTFOLIO_PANELS = (
    "total_equity",
    "total_drawdown",
    "total_rolling_sharpe",
    "total_cash_equity",
    "periodic_pnl",
    "leaderboard",
)

#: Panels that draw one series per run. Readable up to roughly a dozen runs.
PER_RUN_PANELS = (
    "equity",
    "run_pnl",
    "allocation",
    "price",
    "drawdown",
    "brier_advantage",
)

PANELS = PORTFOLIO_PANELS + PER_RUN_PANELS


@dataclass
class BasketReport:
    """The assembled payload, plus the document it renders to."""

    basket_id: str
    runs: list[dict[str, Any]] = field(default_factory=list)
    panels: dict[str, Any] = field(default_factory=dict)
    totals: dict[str, Any] = field(default_factory=dict)
    drawn: tuple[str, ...] = ()
    skipped: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "basket_id": self.basket_id,
            "runs": self.runs,
            "totals": self.totals,
            "panels": self.panels,
            "drawn": list(self.drawn),
            "skipped": self.skipped,
        }

    def to_html(self, path: Optional[Union[str, Path]] = None) -> str:
        document = _render(self)
        if path is not None:
            Path(path).write_text(document, encoding="utf-8")
        return document


def _ts_ns(value: Any) -> int:
    """Nanoseconds since the epoch, from a pandas ``Timestamp`` or a raw int.

    Arrow hands a timestamp column back as ``Timestamp`` under pandas but as
    int64 when the column was already integral, so reading ``.value``
    unconditionally couples the report to one of the two.
    """
    return int(getattr(value, "value", value))


def _equity_frame(result: Any) -> list[dict[str, float]]:
    """The equity curve as plain rows, in simulated-time order."""
    table = result.equity
    if table.num_rows == 0:
        return []
    frame = table.to_pandas().sort_values("ts")
    return [
        {
            "ts_ns": _ts_ns(row.ts),
            "equity": float(row.equity),
            "cash": float(row.cash),
            "position_value": float(row.position_value),
            "realized_pnl": float(row.realized_pnl),
            "unrealized_pnl": float(row.unrealized_pnl),
        }
        for row in frame.itertuples()
    ]


def _drawdown(series: Sequence[float]) -> list[float]:
    out: list[float] = []
    peak = -math.inf
    for value in series:
        peak = max(peak, value)
        out.append(0.0 if peak <= 0 else (value - peak) / peak)
    return out


def _rolling_sharpe(returns: Sequence[float], window: int) -> list[Optional[float]]:
    """Trailing Sharpe on the sampled curve, unannualised.

    Unannualised on purpose: `bt_equity` is sampled on an interval the run
    chose, so there is no bars-per-year constant that is right for every
    caller. Scale it yourself if you know your sampling.
    """
    if window < 2:
        raise ValueError(
            f"a rolling Sharpe needs a window of at least 2, got {window}; "
            "one sample has no dispersion to divide by"
        )
    out: list[Optional[float]] = []
    for index in range(len(returns)):
        if index + 1 < window:
            out.append(None)
            continue
        chunk = returns[index + 1 - window : index + 1]
        mean = sum(chunk) / window
        variance = sum((value - mean) ** 2 for value in chunk) / (window - 1)
        deviation = math.sqrt(variance)
        out.append(None if deviation == 0 else mean / deviation)
    return out


def _returns(series: Sequence[float]) -> list[float]:
    out = []
    for index in range(1, len(series)):
        previous = series[index - 1]
        out.append(0.0 if previous == 0 else (series[index] - previous) / previous)
    return out


def _aligned_total(
    curves: Mapping[str, Sequence[Mapping[str, float]]],
) -> list[dict[str, float]]:
    """Sum equity across runs on the union of their sample instants.

    Each run's contribution is held at its last known value between its own
    samples, which is the only defensible carry for an equity level. A run that
    has not started yet contributes its starting equity, not zero, otherwise the
    basket total would show a phantom ramp as runs come online.
    """
    instants = sorted({row["ts_ns"] for rows in curves.values() for row in rows})
    if not instants:
        return []
    cursors = {name: 0 for name in curves}
    held = {
        name: (rows[0]["equity"] if rows else 0.0) for name, rows in curves.items()
    }
    held_cash = {name: (rows[0]["cash"] if rows else 0.0) for name, rows in curves.items()}
    out = []
    for moment in instants:
        for name, rows in curves.items():
            index = cursors[name]
            while index < len(rows) and rows[index]["ts_ns"] <= moment:
                held[name] = rows[index]["equity"]
                held_cash[name] = rows[index]["cash"]
                index += 1
            cursors[name] = index
        out.append(
            {
                "ts_ns": moment,
                "equity": sum(held.values()),
                "cash": sum(held_cash.values()),
            }
        )
    return out


def _fills(result: Any) -> list[dict[str, Any]]:
    table = result.fills
    if table.num_rows == 0:
        return []
    frame = table.to_pandas()
    return [
        {
            "ts_ns": _ts_ns(row.ts),
            "instrument_id": str(row.instrument_id),
            "outcome": int(row.outcome),
            "side": str(row.side),
            "price": float(row.price),
            "quantity": float(row.quantity),
        }
        for row in frame.itertuples()
    ]


def _price_path(
    db: Any,
    instruments: Sequence[str],
    *,
    snapshot: Optional[str],
    outcome: int,
    max_points: int,
) -> dict[str, list[dict[str, float]]]:
    """Mid path per instrument, read from the pinned book.

    Reads at the same pin the runs used, so the line under a fill marker is the
    book the fill actually met rather than whatever the table holds now.

    `book_deltas` carries one row per *level*, so the mid is the best bid
    (highest live buy) against the best ask (lowest live sell), not the extreme
    of each side. Rows whose action is `delete`, `clear` or `gap` describe
    levels that are going away and are excluded: a deleted level is not a
    price anyone can trade at. So is a `set` of size zero, which is the other
    spelling of the same thing -- the kernel's book applies it as a delete
    (`crates/h5i-db-backtest/src/book.rs`) and the action filter alone misses
    it.

    The instants are the book events themselves, so a line point is the best
    price quoted by that event rather than the resting best across the whole
    book. Carrying untouched levels forward is a stateful replay, which is not
    something a report query should be doing.

    Every literal is routed through `quote_string`, like the rest of the
    package: an instrument id is vendor data, and vendor data has no business
    being spliced into SQL text unquoted.
    """
    if not instruments:
        return {}
    source = (
        f"h5i({quote_string('book_deltas')}, {quote_string(snapshot)})"
        if snapshot
        else "book_deltas"
    )
    quoted = ", ".join(quote_string(str(name)) for name in instruments)
    rows = db.sql(
        f"""
        SELECT instrument_id, ts_init, (best_bid + best_ask) / 2 AS mid
        FROM (
          SELECT instrument_id, ts_init,
                 max(CASE WHEN side = 'buy'  THEN price END) AS best_bid,
                 min(CASE WHEN side = 'sell' THEN price END) AS best_ask
          FROM {source}
          WHERE outcome = {int(outcome)}
            AND instrument_id IN ({quoted})
            AND action IN ({quote_string('snapshot')}, {quote_string('set')})
            AND size > 0
            AND price IS NOT NULL
          GROUP BY instrument_id, ts_init
        ) AS levels
        ORDER BY instrument_id, ts_init
        """
    ).to_pandas()
    paths: dict[str, list[dict[str, float]]] = {}
    for name, group in rows.groupby("instrument_id"):
        ordered = group.sort_values("ts_init")
        step = max(1, len(ordered) // max_points)
        paths[str(name)] = [
            {"ts_ns": _ts_ns(row.ts_init), "mid": float(row.mid)}
            for row in ordered.iloc[::step].itertuples()
            if row.mid is not None and not math.isnan(row.mid)
        ]
    return paths


def basket_payload(
    db: Any,
    runs: Union[Sequence[str], Mapping[str, Any]],
    *,
    basket_id: str = "basket",
    panels: Sequence[str] = PORTFOLIO_PANELS,
    snapshot: Optional[str] = None,
    per_run_limit: int = 12,
    rolling_window: int = 12,
    price_outcome: int = 0,
    max_price_points: int = 400,
    brier: Optional[Mapping[str, Mapping[str, float]]] = None,
) -> BasketReport:
    """Assemble the payload for a basket of runs.

    `runs` is either a sequence of run ids to open, or a mapping of label to
    already-open :class:`~h5i_db.backtest.BacktestResult`. Passing results
    avoids reopening forks when a study just produced them.

    `brier`, when given, maps a run label to `{"strategy": p, "market": p,
    "outcome": y}` so the report can show whether each run's implied probability
    beat the price it paid. It is supplied rather than derived because only the
    caller knows what its strategy's probability *was*.
    """
    from ..backtest import open_result
    from .calibration import brier_advantage

    unknown = [name for name in panels if name not in PANELS]
    if unknown:
        raise ValueError(
            f"unknown panels {unknown}; choose from {sorted(PANELS)}"
        )
    if isinstance(runs, Mapping):
        opened = dict(runs)
    else:
        opened = {str(name): open_result(db, str(name)) for name in runs}
    if not opened:
        raise ValueError("a basket needs at least one run")

    report = BasketReport(basket_id=basket_id)
    curves: dict[str, list[dict[str, float]]] = {}
    fills: dict[str, list[dict[str, Any]]] = {}
    instruments: set[str] = set()

    for label, result in opened.items():
        rows = _equity_frame(result)
        curves[label] = rows
        fills[label] = _fills(result)
        summary = result.summary()
        positions = result.positions.to_pandas()
        settled = (
            float(positions.settlement_pnl.fillna(0.0).sum())
            if "settlement_pnl" in positions
            else 0.0
        )
        instruments.update(str(name) for name in positions.get("instrument_id", []))
        report.runs.append(
            {
                "label": label,
                "run_id": result.run_id,
                "fork": result.fork_name,
                "fills": summary.get("fills"),
                "orders": summary.get("orders"),
                "final_cash": summary.get("final_cash"),
                "realized_pnl": summary.get("realized_pnl"),
                "commissions": summary.get("commissions"),
                "settlement_pnl": settled,
                # `realized_pnl` is already net of commissions, so the total is
                # realized + settlement. Subtracting commissions again would
                # double-count them, and settlement alone would drop every
                # closed round trip, which is most of what a crossover rule does.
                # The two agree only when nothing closed, which is why a
                # hold-to-resolution book hides the difference.
                "net": float(summary.get("realized_pnl") or 0.0) + settled,
                "equity_samples": len(rows),
                "settlement_applied": bool(
                    result.run.to_pandas().settlement_applied.iloc[0]
                )
                if result.run.num_rows
                else None,
            }
        )

    total = _aligned_total(curves)
    report.totals = {
        "runs": len(opened),
        "equity_start": total[0]["equity"] if total else None,
        "equity_end": total[-1]["equity"] if total else None,
        "net": sum(row["net"] for row in report.runs),
        "commissions": sum(float(row["commissions"] or 0.0) for row in report.runs),
        "fills": sum(int(row["fills"] or 0) for row in report.runs),
    }
    if total:
        levels = [row["equity"] for row in total]
        report.totals["max_drawdown"] = min(_drawdown(levels), default=0.0)

    drawn: list[str] = []
    too_many = len(opened) > per_run_limit
    for panel in panels:
        if panel in PER_RUN_PANELS and too_many:
            report.skipped.append(
                {
                    "panel": panel,
                    "reason": "too_many_runs",
                    "runs": len(opened),
                    "limit": per_run_limit,
                }
            )
            continue
        if panel == "total_equity":
            report.panels[panel] = {"series": total}
        elif panel == "total_drawdown":
            levels = [row["equity"] for row in total]
            report.panels[panel] = {
                "series": [
                    {"ts_ns": row["ts_ns"], "drawdown": value}
                    for row, value in zip(total, _drawdown(levels))
                ]
            }
        elif panel == "total_cash_equity":
            report.panels[panel] = {
                "series": [
                    {"ts_ns": row["ts_ns"], "cash": row["cash"], "equity": row["equity"]}
                    for row in total
                ]
            }
        elif panel == "total_rolling_sharpe":
            levels = [row["equity"] for row in total]
            rets = _returns(levels)
            values = _rolling_sharpe(rets, rolling_window)
            report.panels[panel] = {
                "window": rolling_window,
                "series": [
                    {"ts_ns": total[index + 1]["ts_ns"], "sharpe": value}
                    for index, value in enumerate(values)
                    if value is not None
                ],
            }
        elif panel == "periodic_pnl":
            report.panels[panel] = {
                "series": [
                    {"label": row["label"], "net": row["net"]}
                    for row in sorted(report.runs, key=lambda item: item["label"])
                ]
            }
        elif panel == "leaderboard":
            report.panels[panel] = {
                "rows": sorted(
                    report.runs, key=lambda item: item["net"], reverse=True
                )
            }
        elif panel == "equity":
            report.panels[panel] = {"series": curves}
        elif panel == "drawdown":
            report.panels[panel] = {
                "series": {
                    label: [
                        {"ts_ns": row["ts_ns"], "drawdown": value}
                        for row, value in zip(
                            rows, _drawdown([item["equity"] for item in rows])
                        )
                    ]
                    for label, rows in curves.items()
                }
            }
        elif panel == "run_pnl":
            report.panels[panel] = {
                "series": {
                    label: [
                        {
                            "ts_ns": row["ts_ns"],
                            "pnl": row["realized_pnl"] + row["unrealized_pnl"],
                        }
                        for row in rows
                    ]
                    for label, rows in curves.items()
                }
            }
        elif panel == "allocation":
            report.panels[panel] = {
                "series": {
                    label: [
                        {
                            "ts_ns": row["ts_ns"],
                            "invested": (
                                0.0
                                if row["equity"] == 0
                                else row["position_value"] / row["equity"]
                            ),
                        }
                        for row in rows
                    ]
                    for label, rows in curves.items()
                }
            }
        elif panel == "price":
            paths = _price_path(
                db,
                sorted(instruments),
                snapshot=snapshot,
                outcome=price_outcome,
                max_points=max_price_points,
            )
            report.panels[panel] = {"paths": paths, "fills": fills}
        elif panel == "brier_advantage":
            if not brier:
                report.skipped.append(
                    {
                        "panel": panel,
                        "reason": "no_probabilities_supplied",
                        "hint": "pass brier={label: {'strategy': [...], "
                        "'market': [...], 'outcome': [...]}}",
                    }
                )
                continue
            scored = {}
            for label, payload in brier.items():
                advantage = brier_advantage(
                    payload["strategy"], payload["market"], payload["outcome"]
                )
                scored[label] = {
                    **advantage.to_dict(),
                    "cumulative": list(advantage.cumulative),
                }
            report.panels[panel] = {"runs": scored}
        drawn.append(panel)
    report.drawn = tuple(drawn)
    return report


def _svg_line(
    series: Mapping[str, Sequence[Mapping[str, float]]],
    *,
    x_key: str,
    y_key: str,
    width: int = 860,
    height: int = 240,
    markers: Optional[Mapping[str, Sequence[Mapping[str, Any]]]] = None,
    zero_line: bool = False,
) -> str:
    """A dependency-free line chart.

    Inline SVG rather than a plotting library, because a report that needs
    matplotlib installed to be *read* is not a report. It also keeps the
    document self-contained, which matters when it is the artifact attached to
    a decision.
    """
    points = [
        (float(row[x_key]), float(row[y_key]))
        for rows in series.values()
        for row in rows
        if row.get(y_key) is not None
    ]
    if not points:
        return "<p class='empty'>no data for this panel</p>"
    xs = [point[0] for point in points]
    ys = [point[1] for point in points]
    x_low, x_high = min(xs), max(xs)
    y_low, y_high = min(ys), max(ys)
    if zero_line:
        y_low, y_high = min(y_low, 0.0), max(y_high, 0.0)
    x_span = (x_high - x_low) or 1.0
    y_span = (y_high - y_low) or 1.0
    pad = 28

    def project(x: float, y: float) -> tuple[float, float]:
        px = pad + (x - x_low) / x_span * (width - 2 * pad)
        py = height - pad - (y - y_low) / y_span * (height - 2 * pad)
        return px, py

    palette = (
        "#2c7fb8",
        "#c0392b",
        "#27ae60",
        "#8e44ad",
        "#d35400",
        "#16a085",
        "#7f8c8d",
        "#2980b9",
        "#c2185b",
        "#f39c12",
        "#00838f",
        "#5d4037",
    )
    parts = [
        f"<svg viewBox='0 0 {width} {height}' role='img' "
        f"preserveAspectRatio='xMidYMid meet'>"
    ]
    if zero_line and y_low <= 0 <= y_high:
        _, zero_y = project(x_low, 0.0)
        parts.append(
            f"<line x1='{pad}' y1='{zero_y:.1f}' x2='{width - pad}' "
            f"y2='{zero_y:.1f}' stroke='#999' stroke-dasharray='3 3'/>"
        )
    for index, (label, rows) in enumerate(sorted(series.items())):
        usable = [row for row in rows if row.get(y_key) is not None]
        if not usable:
            continue
        colour = palette[index % len(palette)]
        path = " ".join(
            ("M" if position == 0 else "L")
            + "{:.1f},{:.1f}".format(*project(float(row[x_key]), float(row[y_key])))
            for position, row in enumerate(usable)
        )
        parts.append(
            f"<path d='{path}' fill='none' stroke='{colour}' stroke-width='1.6'>"
            f"<title>{html.escape(str(label))}</title></path>"
        )
        for marker in (markers or {}).get(label, ()):  # fill markers
            mx, my = project(
                float(marker["ts_ns"]),
                float(marker.get(y_key, marker.get("price", 0.0))),
            )
            shape = "#27ae60" if str(marker.get("side")) == "buy" else "#c0392b"
            parts.append(
                f"<circle cx='{mx:.1f}' cy='{my:.1f}' r='3' fill='{shape}' "
                f"fill-opacity='0.85'><title>"
                f"{html.escape(str(marker.get('side','')))} "
                f"{html.escape(str(marker.get('quantity','')))} @ "
                f"{html.escape(str(marker.get('price','')))}"
                f"</title></circle>"
            )
    parts.append("</svg>")
    return "".join(parts)


def _table(rows: Sequence[Mapping[str, Any]]) -> str:
    if not rows:
        return "<p class='empty'>nothing to show</p>"
    columns = list(rows[0].keys())
    header = "".join(f"<th>{html.escape(name)}</th>" for name in columns)
    body = []
    for row in rows:
        cells = []
        for name in columns:
            value = row.get(name)
            if isinstance(value, float):
                cells.append(f"<td>{value:,.4f}</td>")
            else:
                cells.append(f"<td>{html.escape(str(value))}</td>")
        body.append("<tr>" + "".join(cells) + "</tr>")
    return (
        f"<table><thead><tr>{header}</tr></thead>"
        f"<tbody>{''.join(body)}</tbody></table>"
    )


_TITLES = {
    "total_equity": "Basket equity",
    "total_drawdown": "Basket drawdown",
    "total_cash_equity": "Basket cash against equity",
    "total_rolling_sharpe": "Basket rolling Sharpe (unannualised)",
    "periodic_pnl": "Net result by run",
    "leaderboard": "Leaderboard",
    "equity": "Equity by run",
    "drawdown": "Drawdown by run",
    "run_pnl": "P&L by run",
    "allocation": "Invested fraction by run",
    "price": "Price path with fills",
    "brier_advantage": "Brier advantage over the market",
}


def _render(report: BasketReport) -> str:
    # Inside a <script> element the HTML parser looks for the closing tag
    # before JavaScript ever sees the string, so `<` must not survive; as a
    # unicode escape it is inert to the parser and the same string to JSON.
    # html.escape() would instead hand JSON.parse a document full of `&quot;`,
    # which is not JSON. Same treatment as report.py's `render`.
    #
    # The loop below binds its own name. It used to reuse this one, so the
    # escaped JSON never reached the <script> block: what was emitted was the
    # last panel's Python dict repr, which is not JSON at all and carries
    # every `<` an instrument or strategy name contains straight into the
    # parser.
    payload = (
        json.dumps(report.to_dict(), default=str)
        .replace("<", "\\u003c")
        .replace(">", "\\u003e")
    )
    blocks = []
    for panel in report.drawn:
        drawn = report.panels.get(panel, {})
        title = html.escape(_TITLES.get(panel, panel))
        if panel == "total_equity":
            body = _svg_line({"basket": drawn["series"]}, x_key="ts_ns", y_key="equity")
        elif panel == "total_drawdown":
            body = _svg_line(
                {"basket": drawn["series"]}, x_key="ts_ns", y_key="drawdown", zero_line=True
            )
        elif panel == "total_cash_equity":
            body = _svg_line(
                {
                    "cash": [
                        {"ts_ns": row["ts_ns"], "value": row["cash"]}
                        for row in drawn["series"]
                    ],
                    "equity": [
                        {"ts_ns": row["ts_ns"], "value": row["equity"]}
                        for row in drawn["series"]
                    ],
                },
                x_key="ts_ns",
                y_key="value",
            )
        elif panel == "total_rolling_sharpe":
            body = _svg_line(
                {"sharpe": drawn["series"]},
                x_key="ts_ns",
                y_key="sharpe",
                zero_line=True,
            )
        elif panel in ("periodic_pnl",):
            body = _table(drawn["series"])
        elif panel == "leaderboard":
            body = _table(drawn["rows"])
        elif panel in ("equity", "drawdown", "run_pnl", "allocation"):
            key = {
                "equity": "equity",
                "drawdown": "drawdown",
                "run_pnl": "pnl",
                "allocation": "invested",
            }[panel]
            body = _svg_line(
                drawn["series"],
                x_key="ts_ns",
                y_key=key,
                zero_line=panel in ("drawdown", "run_pnl"),
            )
        elif panel == "price":
            markers = {
                instrument: [
                    fill
                    for rows in drawn["fills"].values()
                    for fill in rows
                    if fill["instrument_id"] == instrument
                ]
                for instrument in drawn["paths"]
            }
            body = _svg_line(
                drawn["paths"], x_key="ts_ns", y_key="mid", markers=markers
            )
        elif panel == "brier_advantage":
            body = _svg_line(
                {
                    label: [
                        {"index": position, "cumulative": value}
                        for position, value in enumerate(scored["cumulative"])
                    ]
                    for label, scored in drawn["runs"].items()
                },
                x_key="index",
                y_key="cumulative",
                zero_line=True,
            ) + _table(
                [
                    {"run": label, **{k: v for k, v in scored.items() if k != "cumulative"}}
                    for label, scored in drawn["runs"].items()
                ]
            )
        else:  # pragma: no cover - guarded by the panel whitelist
            continue
        blocks.append(f"<section><h2>{title}</h2>{body}</section>")

    skipped = ""
    if report.skipped:
        skipped = (
            "<section><h2>Not drawn</h2>"
            + _table(
                [
                    {key: value for key, value in item.items()}
                    for item in report.skipped
                ]
            )
            + "</section>"
        )
    totals = _table([report.totals]) if report.totals else ""
    return (
        "<!doctype html><meta charset='utf-8'>"
        f"<title>{html.escape(report.basket_id)}</title>"
        "<style>"
        ":root{color-scheme:light dark}"
        "body{font:14px/1.5 system-ui,sans-serif;margin:0 auto;max-width:980px;padding:32px}"
        "h1{font-size:22px;margin:0 0 4px}h2{font-size:15px;margin:28px 0 8px}"
        "section{border-top:1px solid #8883;padding-top:4px}"
        "table{border-collapse:collapse;font-variant-numeric:tabular-nums;"
        "display:block;overflow-x:auto;max-width:100%}"
        "th,td{border:1px solid #8884;padding:4px 8px;text-align:right;white-space:nowrap}"
        "th{background:#8881}svg{width:100%;height:auto}"
        ".empty{color:#888;font-style:italic}"
        ".meta{color:#666;font-size:12px}"
        "</style>"
        f"<h1>{html.escape(report.basket_id)}</h1>"
        f"<p class='meta'>{len(report.runs)} runs &middot; "
        f"panels: {html.escape(', '.join(report.drawn))}</p>"
        f"{totals}{''.join(blocks)}{skipped}"
        f"<script type='application/json' id='payload'>"
        f"{payload}</script>"
    )


def basket_report(
    db: Any,
    runs: Union[Sequence[str], Mapping[str, Any]],
    *,
    path: Optional[Union[str, Path]] = None,
    **kwargs: Any,
) -> str:
    """Build and render a basket report in one call."""
    return basket_payload(db, runs, **kwargs).to_html(path)
