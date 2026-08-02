"""Self-contained HTML reports (ROADMAP_QUANT.md §6, M2).

One renderer serves every report kind. Output is a single file with inline
CSS and JS and the data embedded as JSON: no CDN, no build step, no network
access at view time, matching the cookbook pages and the review UI's
single-asset convention.

Section one of every report is the provenance header, so a reader sees which
data version produced the numbers before seeing the numbers.

    html = factor_report(panel, path="factor.html")
    html = tearsheet(series, benchmark=bench, path="tearsheet.html")
    html = backtest_report(result, path="run.html")

``report_payload`` returns the same content as a dict, which is what the CLI
and any agent consumer should read instead of scraping the HTML.
"""

from __future__ import annotations

import datetime as _dt
import html as _html
import json
import math
import re as _re
from typing import Any, Optional, Sequence

__all__ = [
    "backtest_payload",
    "backtest_report",
    "factor_report",
    "render",
    "report_payload",
    "tearsheet",
]

# Categorical slots 1-3, stepped from the review UI's hues (mint signature,
# amber, violet) onto each mode's chart surface and validated per mode:
# lightness band, chroma floor, adjacent CVD separation, contrast. Three is
# the documented all-pairs safe count; a report never draws more than three
# horizons as distinct series, and folds the rest into a table.
#
# The UI's own accents (#42e2d0 and friends) sit above the dark lightness
# band -- correct for chrome on near-black, too light for a data mark -- so
# marks use these steps and chrome keeps the neon.
_SERIES_LIGHT = ("#00958a", "#b2660d", "#6a4fb3")
_SERIES_DARK = ("#1e9f93", "#c9812f", "#8f6dc6")
_MAX_SERIES = 3


def _fmt_ts(value: Any) -> Any:
    if isinstance(value, (_dt.datetime, _dt.date)):
        return value.isoformat()
    return value


def _clean(value: Any) -> Any:
    """JSON cannot carry NaN or infinity; they become null."""
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    if isinstance(value, (_dt.datetime, _dt.date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: _clean(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_clean(v) for v in value]
    return value


def _rows(result) -> list:
    """Arrow result to a list of plain dicts."""
    if result is None:
        return []
    table = result.to_arrow() if hasattr(result, "to_arrow") else result
    return [_clean(row) for row in table.to_pylist()]


def _provenance_block(provenance) -> dict:
    data = provenance.to_dict()
    data["warnings"] = list(provenance.warnings())
    return _clean(data)


# -- payload builders --------------------------------------------------------


def factor_payload(
    panel,
    *,
    title: Optional[str] = None,
    turnover_period: int = 1,
) -> dict:
    """Everything a factor report shows, as plain data."""
    periods = list(panel.periods)[:_MAX_SERIES]
    ic_rows = _rows(panel.ic())
    decay_rows = _rows(panel.ic_decay())
    quantile_rows = _rows(panel.quantile_returns())
    turnover_rows = _rows(panel.turnover(period=turnover_period))
    autocorr_rows = _rows(panel.rank_autocorrelation(period=turnover_period))
    curve_rows = _rows(panel.cumulative_returns(period=panel.periods[0]))
    loss = _clean(panel.loss_report())

    headline = []
    for row in decay_rows:
        headline.append(
            {
                "label": f"Mean IC ({row['period']}b)",
                "value": row["mean_ic"],
                "format": "decimal4",
            }
        )
    for row in decay_rows:
        headline.append(
            {
                "label": f"ICIR ({row['period']}b)",
                "value": row["icir"],
                "format": "decimal2",
            }
        )
    headline.append(
        {"label": "Rows kept", "value": loss["after_binning"], "format": "integer"}
    )
    headline.append(
        # A fraction of the input rows, so a percent format: "percent" is not
        # one of the formats the document knows and fell through to a bare
        # decimal, printing "0.123" next to a count of 84,120.
        {"label": "Rows dropped", "value": loss["total"], "format": "percent1"}
    )

    # Turnover is per quantile per date; the chart shows the mean across
    # quantiles, and the table keeps the detail.
    turnover_by_date: dict = {}
    for row in turnover_rows:
        turnover_by_date.setdefault(row["ts"], []).append(row["turnover"])
    turnover_series = [
        {"x": ts, "y": sum(v) / len(v) if v else None}
        for ts, v in sorted(turnover_by_date.items())
    ]

    charts = [
        {
            "kind": "line",
            "id": "ic",
            "title": "Information coefficient over time",
            "subtitle": "Per-date Spearman rank correlation of factor to forward return",
            "yFormat": "decimal3",
            "series": [
                {
                    "name": f"{p} bar" + ("s" if p != 1 else ""),
                    "points": [
                        {"x": r["ts"], "y": r.get(f"ic_{p}")} for r in ic_rows
                    ],
                }
                for p in periods
            ],
        },
        {
            "kind": "bar",
            "id": "ic-decay",
            "title": "IC decay by horizon",
            "subtitle": "Mean IC as the forward-return horizon lengthens",
            "yFormat": "decimal4",
            "categories": [f"{r['period']}b" for r in decay_rows],
            "series": [
                {
                    "name": "Mean IC",
                    "points": [
                        {"x": f"{r['period']}b", "y": r["mean_ic"]}
                        for r in decay_rows
                    ],
                }
            ],
        },
        {
            "kind": "bar",
            "id": "quantile-returns",
            "title": "Mean forward return by quantile",
            "subtitle": "Demeaned, equal weight per date",
            "yFormat": "percent3",
            "categories": [f"Q{r['factor_quantile']}" for r in quantile_rows],
            "series": [
                {
                    "name": f"{p} bar" + ("s" if p != 1 else ""),
                    "points": [
                        {
                            "x": f"Q{r['factor_quantile']}",
                            "y": r.get(f"mean_{p}"),
                            "err": r.get(f"stderr_{p}"),
                        }
                        for r in quantile_rows
                    ],
                }
                for p in periods
            ],
        },
        {
            "kind": "line",
            "id": "turnover",
            "title": "Quantile turnover",
            "subtitle": f"Mean across quantiles, {turnover_period}-bar lag",
            "yFormat": "percent1",
            "series": [{"name": "Turnover", "points": turnover_series}],
        },
        {
            "kind": "line",
            "id": "autocorrelation",
            "title": "Factor rank autocorrelation",
            "subtitle": f"Stability of the ranking, {turnover_period}-bar lag",
            "yFormat": "decimal3",
            "series": [
                {
                    "name": "Autocorrelation",
                    "points": [
                        {"x": r["ts"], "y": r["autocorrelation"]}
                        for r in autocorr_rows
                    ],
                }
            ],
        },
        {
            "kind": "line",
            "id": "cumulative",
            "title": "Factor portfolio cumulative return",
            "subtitle": f"Gross leverage 1, {panel.periods[0]}-bar horizon",
            "yFormat": "percent1",
            "series": [
                {
                    "name": "Cumulative return",
                    "points": [
                        {"x": r["ts"], "y": r["cumulative_return"]}
                        for r in curve_rows
                    ],
                }
            ],
        },
    ]

    return {
        "kind": "factor",
        "title": title or "Factor evaluation",
        "provenance": _provenance_block(panel.provenance),
        "headline": headline,
        "charts": charts,
        "tables": [
            {
                "id": "decay-table",
                "title": "IC by horizon",
                "columns": ["period", "mean_ic", "std_ic", "icir", "t_stat", "n"],
                "rows": decay_rows,
            },
            {
                "id": "quantile-table",
                "title": "Returns by quantile",
                "columns": list(quantile_rows[0]) if quantile_rows else [],
                "rows": quantile_rows,
            },
        ],
        "notes": [
            f"{loss['after_binning']} of {loss['initial']} factor rows survived "
            f"({loss['forward_returns']:.1%} lost computing forward returns, "
            f"{loss['binning']:.1%} lost binning)."
        ],
    }


def tearsheet_payload(
    series,
    *,
    benchmark=None,
    title: Optional[str] = None,
    rolling_window: int = 63,
    top_drawdowns: int = 5,
) -> dict:
    """Everything a performance tearsheet shows, as plain data."""
    stats = _clean(series.stats(benchmark=benchmark))
    curve = _rows(series.equity_curve())
    under = _rows(series.underwater())
    rolling = _rows(series.rolling_sharpe(rolling_window))
    drawdowns = _clean(series.drawdown_table(top=top_drawdowns))

    headline = [
        {"label": "Annual return", "value": stats.get("annual_return"), "format": "percent1"},
        {"label": "Cumulative return", "value": stats.get("cumulative_return"), "format": "percent1"},
        {"label": "Annual volatility", "value": stats.get("annual_volatility"), "format": "percent1"},
        {"label": "Sharpe", "value": stats.get("sharpe_ratio"), "format": "decimal2"},
        {"label": "Sortino", "value": stats.get("sortino_ratio"), "format": "decimal2"},
        {"label": "Calmar", "value": stats.get("calmar_ratio"), "format": "decimal2"},
        {"label": "Max drawdown", "value": stats.get("max_drawdown"), "format": "percent1"},
        {"label": "Stability", "value": stats.get("stability"), "format": "decimal2"},
        {"label": "Tail ratio", "value": stats.get("tail_ratio"), "format": "decimal2"},
        {"label": "Daily VaR", "value": stats.get("daily_value_at_risk"), "format": "percent2"},
    ]
    if benchmark is not None:
        headline.append({"label": "Alpha", "value": stats.get("alpha"), "format": "percent1"})
        headline.append({"label": "Beta", "value": stats.get("beta"), "format": "decimal2"})

    charts = [
        {
            "kind": "line",
            "id": "equity",
            "title": "Cumulative return",
            "subtitle": "Compounded from the returns series",
            "yFormat": "percent1",
            "series": [
                {
                    "name": "Strategy",
                    "points": [
                        {"x": r["ts"], "y": r["cumulative_return"]} for r in curve
                    ],
                }
            ],
        },
        {
            "kind": "area",
            "id": "underwater",
            "title": "Drawdown",
            "subtitle": "Distance below the running peak",
            "yFormat": "percent1",
            "series": [
                {
                    "name": "Drawdown",
                    "points": [{"x": r["ts"], "y": r["drawdown"]} for r in under],
                }
            ],
        },
        {
            "kind": "line",
            "id": "rolling-sharpe",
            "title": f"Rolling Sharpe ({rolling_window} bars)",
            "subtitle": "Annualized",
            "yFormat": "decimal2",
            "series": [
                {
                    "name": "Rolling Sharpe",
                    "points": [
                        {"x": r["ts"], "y": r["rolling_sharpe"]} for r in rolling
                    ],
                }
            ],
        },
    ]
    if benchmark is not None:
        bench_curve = _rows(benchmark.equity_curve())
        charts[0]["series"].append(
            {
                "name": "Benchmark",
                "points": [
                    {"x": r["ts"], "y": r["cumulative_return"]} for r in bench_curve
                ],
            }
        )

    return {
        "kind": "tearsheet",
        "title": title or "Performance tearsheet",
        "provenance": _provenance_block(series.provenance),
        "headline": headline,
        "charts": charts,
        "tables": [
            {
                "id": "drawdown-table",
                "title": f"Worst {len(drawdowns)} drawdowns",
                "columns": [
                    "net_drawdown",
                    "peak_date",
                    "valley_date",
                    "recovery_date",
                    "duration",
                ],
                "rows": drawdowns,
            },
            {
                "id": "stats-table",
                "title": "All statistics",
                "columns": ["statistic", "value"],
                "rows": [{"statistic": k, "value": v} for k, v in stats.items()],
            },
        ],
        "notes": [],
    }


# A run can emit millions of equity samples and fills. Charts are sampled to
# these caps and tables to `max_table_rows`; every cut is reported in `notes`
# rather than silently shortening the picture.
_POINT_CAP = 1_500
_MARK_CAP = 1_500
_TABLE_CAP = 200

_FIDELITY_TEXT = {
    "tick_l2": "tick-by-tick L2 book",
    "snapshot_l2": "periodic L2 snapshots",
    "trades_only": "trades only, no book",
    "bars_synthetic": "synthetic bars",
    "none": "no usable market data",
}


def _sample(rows: list, cap: int) -> tuple[list, int]:
    """Evenly thin `rows` to `cap`, always keeping the last one."""
    if len(rows) <= cap:
        return rows, 0
    stride = math.ceil(len(rows) / cap)
    kept = rows[::stride]
    if kept[-1] is not rows[-1]:
        kept.append(rows[-1])
    return kept, len(rows) - len(kept)


def _x_format(stamps: Sequence[Any]) -> str:
    """Clock labels inside a single day, dates across more than one."""
    marks = [s for s in stamps if isinstance(s, str)]
    if len(marks) < 2:
        return "datetime"
    return "time" if marks[0][:10] == marks[-1][:10] else "date"


def _drawdown(levels: Sequence[float]) -> list:
    peak = -math.inf
    out = []
    for value in levels:
        peak = max(peak, value)
        out.append(value / peak - 1.0 if peak > 0 else 0.0)
    return out


def _counts_table(counts: dict, key: str, title: str, ident: str) -> dict:
    rows = [
        {key: name, "count": count}
        for name, count in sorted(counts.items(), key=lambda kv: -kv[1])
    ]
    return {"id": ident, "title": title, "columns": [key, "count"], "rows": rows}


def backtest_payload(
    result,
    *,
    title: Optional[str] = None,
    max_table_rows: int = _TABLE_CAP,
) -> dict:
    """Everything a backtest report shows, as plain data.

    Reads only what ``BacktestResult`` already exposes, so the report's
    numbers are the ones ``summary()``, ``stats()`` and ``explain()`` return.
    A run without an equity curve still gets a report: the performance
    panels drop out and the execution evidence remains.
    """
    summary = _clean(result.summary())
    explanation = _clean(result.explain())
    config = getattr(result, "config", None)
    inspection = getattr(result, "inspection", None)

    equity = _rows(result.equity)
    fills = _rows(result.fills)
    orders = _rows(result.orders)
    positions = _rows(result.positions)
    notes: list[str] = []

    stats: dict = {}
    if len(equity) >= 2:
        stats = _clean(result.stats())

    starting_cash = None
    if config is not None:
        starting_cash = config.portfolio.starting_cash
    final_equity = equity[-1]["equity"] if equity else None
    net_pnl = (
        final_equity - starting_cash
        if final_equity is not None and starting_cash is not None
        else None
    )

    # -- provenance: what was replayed, at which version, under which model --
    data = config.data if config is not None else None
    execution = config.execution if config is not None else None
    # No event-time cutoff row: a run's read point is the pin, and its
    # simulated span is the window, which gets its own entry below.
    pin = {
        "pinned": bool(data.is_pinned) if data is not None else None,
        "version": data.version if data is not None else None,
        "as_of": data.as_of if data is not None else None,
        "snapshot": data.snapshot if data is not None else None,
    }
    entries: list = [["Run", result.run_id], ["Fork", result.fork_name]]
    if config is not None:
        entries.append(["Config digest", config.digest])
        entries.append(["Trial digest", config.trial_digest])
        entries.append(["Strategy", _strategy_name(data)])
        if data.window is not None:
            entries.append(["Window", f"{data.window[0]} .. {data.window[1]} (ns)"])
    if "coverage" in summary and summary["coverage"] is not None:
        floor = data.minimum_coverage if data is not None else None
        entries.append(
            [
                "Coverage",
                f"{summary['coverage']:.4f}"
                + (f" (floor {floor})" if floor is not None else ""),
            ]
        )
    entries.append(["Records replayed", f"{summary.get('records_processed', 0):,}"])
    if execution is not None:
        entries.append(["Execution model", _execution_text(execution)])
    if result.get("cached"):
        entries.append(["Source", "cached: an identical trial had already run"])

    # The run's warnings belong to the status banner directly above, not to
    # both places: one condition, one line.
    run_warnings = list(summary.get("warnings") or [])
    provenance = {
        "digest": summary.get("digest"),
        "pin": pin,
        "entries": entries,
        "parameters": {},
        "warnings": [],
    }

    # -- the status banner: can these numbers be believed, and how far -------
    issues = []
    fidelity = None
    if inspection is not None:
        fidelity = inspection.fidelity.value
        issues = [
            {
                "severity": issue.severity,
                "code": issue.code,
                "message": issue.message,
                "remediation": issue.remediation,
            }
            for issue in inspection.issues
        ]
    tone = "ok"
    if any(issue["severity"] == "error" for issue in issues):
        tone = "bad"
    elif issues or run_warnings or not pin["pinned"]:
        tone = "warn"
    headline_text = (
        f"Replay fidelity: {_FIDELITY_TEXT.get(fidelity, fidelity)}"
        if fidelity
        else "Replay fidelity was not recorded for this run"
    )
    detail = (
        "Pinned to an immutable data version, so this run is reproducible."
        if pin["pinned"]
        else "Unpinned: read at the latest version, so a later write changes it."
    )
    status = {
        "tone": tone,
        "headline": headline_text,
        "detail": detail,
        "items": issues
        + [
            {"severity": "warning", "code": "run", "message": warning}
            for warning in run_warnings
        ],
    }

    # -- headline tiles ------------------------------------------------------
    orders_count = summary.get("orders") or 0
    fills_count = summary.get("fills") or 0
    filled = explanation["status_counts"].get("filled", 0)
    tiles = [
        {"label": "Net P&L", "value": net_pnl, "format": "money", "tone": "signed"},
        {
            "label": "Return",
            "value": (
                net_pnl / starting_cash
                if net_pnl is not None and starting_cash
                else None
            ),
            "format": "percent2",
            "tone": "signed",
        },
        {"label": "Final equity", "value": final_equity, "format": "money"},
        {
            "label": "Realized P&L",
            "value": summary.get("realized_pnl"),
            "format": "money",
            "tone": "signed",
        },
        {
            "label": "Commissions",
            "value": summary.get("commissions"),
            "format": "money",
        },
        {"label": "Final cash", "value": summary.get("final_cash"), "format": "money"},
    ]
    if stats:
        tiles += [
            {
                "label": "Sharpe",
                "value": stats.get("sharpe_ratio"),
                "format": "decimal2",
            },
            {
                "label": "Max drawdown",
                "value": stats.get("max_drawdown"),
                "format": "percent1",
            },
            {
                "label": "Annual volatility",
                "value": stats.get("annual_volatility"),
                "format": "percent1",
            },
        ]
    tiles += [
        {"label": "Orders", "value": orders_count, "format": "integer"},
        {"label": "Fills", "value": fills_count, "format": "integer"},
        {
            "label": "Orders filled",
            "value": filled / orders_count if orders_count else None,
            "format": "percent0",
            "hint": f"{filled} of {orders_count}",
        },
    ]

    # -- charts --------------------------------------------------------------
    charts: list = []
    if len(equity) >= 2:
        sampled, dropped = _sample(equity, _POINT_CAP)
        if dropped:
            notes.append(
                f"The equity charts plot {len(sampled):,} of {len(equity):,} "
                "samples, thinned evenly; the tables carry the full run."
            )
        stamps = [row["ts"] for row in sampled]
        x_format = _x_format(stamps)
        charts.append(
            {
                "kind": "line",
                "id": "equity",
                "title": "Equity",
                "subtitle": "Cash plus marked position value, on simulated time",
                "yFormat": "money",
                "xFormat": x_format,
                "series": [
                    {
                        "name": "Equity",
                        "points": [
                            {"x": row["ts"], "y": row["equity"]} for row in sampled
                        ],
                    }
                ],
            }
        )
        # Computed on the whole curve and thinned afterwards, not computed on
        # the thinned curve: thinning first drops the samples that made the
        # troughs, and the chart would draw a shallower worst drawdown than
        # the "Max drawdown" tile prints beside it. The trough itself is put
        # back if the even thinning happened to drop it, for the same reason.
        underwater = [
            {"ts": row["ts"], "drawdown": value}
            for row, value in zip(
                equity, _drawdown([row["equity"] for row in equity])
            )
        ]
        drawdown_points, _thinned = _sample(underwater, _POINT_CAP)
        trough = min(underwater, key=lambda row: row["drawdown"])
        if not any(row is trough for row in drawdown_points):
            drawdown_points = sorted(
                drawdown_points + [trough], key=lambda row: row["ts"]
            )
        charts.append(
            {
                "kind": "area",
                "id": "drawdown",
                "title": "Drawdown",
                "subtitle": "Distance below the running peak",
                "yFormat": "percent1",
                "xFormat": x_format,
                "series": [
                    {
                        "name": "Drawdown",
                        "points": [
                            {"x": row["ts"], "y": row["drawdown"]}
                            for row in drawdown_points
                        ],
                    }
                ],
            }
        )
        if any(row.get("position_value") for row in sampled):
            # As a share of equity, not as cash beside position value: those
            # two are the same unit but not the same order of magnitude, and
            # on one axis the smaller is a flat line against the bottom. The
            # share answers the question the pair was there to answer -- how
            # invested was this run, and when -- on a scale that fits it.
            exposure = [
                {
                    "x": row["ts"],
                    "y": (
                        row["position_value"] / row["equity"]
                        if row.get("equity")
                        else None
                    ),
                }
                for row in sampled
            ]
            peak = max(
                (abs(point["y"]) for point in exposure if point["y"] is not None),
                default=0.0,
            )
            charts.append(
                {
                    "kind": "line",
                    "id": "exposure",
                    "title": "Exposure",
                    "subtitle": "Position value as a share of equity",
                    "yFormat": "percent2" if peak < 0.02 else "percent1",
                    "xFormat": x_format,
                    "series": [{"name": "Exposure", "points": exposure}],
                }
            )
    if fills:
        marks, dropped = _sample(fills, _MARK_CAP)
        if dropped:
            notes.append(
                f"The fills chart plots {len(marks):,} of {len(fills):,} fills, "
                "thinned evenly."
            )
        # Buy and sell carry a marker shape as well as a hue: the conventional
        # green/red pair is the one categorical pair red-green readers cannot
        # separate, so identity never rests on color here.
        sides = [
            ("buy", "Buy", "up"),
            ("sell", "Sell", "down"),
        ]
        series = []
        for side, name, marker in sides:
            points = [
                {
                    "x": row["ts"],
                    "y": row["price"],
                    "meta": (
                        f"{row['quantity']:g} @ {row['price']:g} "
                        f"({'taker' if row.get('is_taker') else 'maker'})"
                    ),
                }
                for row in marks
                if row.get("side") == side
            ]
            if points:
                series.append({"name": name, "marker": marker, "points": points})
        if series:
            charts.append(
                {
                    "kind": "scatter",
                    "id": "fills",
                    "title": "Fills",
                    "subtitle": "Execution price over simulated time",
                    "yFormat": "price",
                    "xFormat": _x_format([row["ts"] for row in marks]),
                    "series": series,
                }
            )

    # -- tables --------------------------------------------------------------
    manifest = [
        {"field": key, "value": _scalar(value)}
        for key, value in summary.items()
        if key != "warnings"
    ]
    tables = [
        {
            "id": "run-manifest",
            "title": "Run manifest",
            "columns": ["field", "value"],
            "rows": manifest,
        },
        _counts_table(
            explanation["status_counts"], "status", "Order lifecycle", "order-status"
        ),
    ]
    if explanation["rejection_reasons"]:
        tables.append(
            _counts_table(
                explanation["rejection_reasons"],
                "reason",
                "Rejections",
                "order-rejections",
            )
        )
    if positions:
        tables.append(
            {
                "id": "positions",
                "title": "Final positions",
                "columns": [
                    name
                    for name in (
                        "instrument_id",
                        "outcome",
                        "quantity",
                        "average_price",
                        "realized_pnl",
                        "commissions",
                        "settlement_pnl",
                    )
                    if name in positions[0]
                ],
                "rows": positions,
                "formats": {
                    "quantity": "decimal4",
                    "average_price": "price",
                    "realized_pnl": "money",
                    "commissions": "money",
                    "settlement_pnl": "money",
                },
            }
        )
    if fills:
        shown = fills[:max_table_rows]
        tables.append(
            {
                "id": "fills-table",
                "title": "Fills",
                "columns": [
                    name
                    for name in (
                        "ts",
                        "order_id",
                        "instrument_id",
                        "side",
                        "price",
                        "quantity",
                        "commission",
                        "is_taker",
                        "tag",
                    )
                    if name in fills[0]
                ],
                "rows": shown,
                "formats": {
                    "price": "price",
                    "quantity": "decimal4",
                    "commission": "money",
                },
                "note": (
                    f"The first {len(shown):,} of {len(fills):,} fills. "
                    "Read the whole table with result.fills."
                    if len(fills) > len(shown)
                    else None
                ),
            }
        )
    if orders and explanation["rejection_reasons"]:
        rejected = [row for row in orders if row.get("reject_reason")][:max_table_rows]
        tables.append(
            {
                "id": "rejected-orders",
                "title": "Rejected orders",
                "columns": [
                    name
                    for name in (
                        "ts",
                        "order_id",
                        "instrument_id",
                        "side",
                        "quantity",
                        "limit_price",
                        "reject_reason",
                    )
                    if name in orders[0]
                ],
                "rows": rejected,
                "formats": {"quantity": "decimal4", "limit_price": "price"},
            }
        )
    if stats:
        tables.append(
            {
                "id": "stats-table",
                "title": "All statistics",
                "columns": ["statistic", "value"],
                "rows": [{"statistic": k, "value": v} for k, v in stats.items()],
            }
        )

    # -- the evidence, verbatim ---------------------------------------------
    code = []
    if config is not None:
        code.append(
            {
                "id": "config-json",
                "title": "Backtest configuration (re-runnable)",
                "text": config.to_json(indent=2),
            }
        )
    if inspection is not None:
        code.append(
            {
                "id": "preflight-json",
                "title": "Preflight evidence",
                "text": json.dumps(inspection.to_dict(), indent=2, default=str),
            }
        )

    if not equity:
        notes.append(
            "This run recorded no equity curve, so there are no performance "
            "panels. Set output.equity_interval_nanos to sample one."
        )
    if explanation["orders_without_fills"]:
        notes.append(
            f"{explanation['orders_without_fills']} order(s) never filled."
        )
    if explanation["orders_sweeping_multiple_levels"]:
        notes.append(
            f"{explanation['orders_sweeping_multiple_levels']} order(s) swept "
            "more than one price level."
        )

    return {
        "kind": "backtest",
        "title": title or f"Backtest: {result.run_id}",
        "subtitle": _subtitle(summary, pin, fidelity),
        "status": status,
        "provenance": provenance,
        "headline": tiles,
        "charts": charts,
        "tables": tables,
        "code": code,
        "notes": notes,
    }


def _strategy_name(data) -> str:
    if data.strategy_kind == "callback":
        return f"Python callback {data.strategy_id!r}"
    table = data.commands if data.commands is not None else data.signals
    return f"{data.strategy_kind} table {table!r}"


def _execution_text(execution) -> str:
    parts = []
    if execution.fee_kind:
        parts.append(f"{execution.fee_kind} fees")
    if execution.fee_rate is not None:
        parts.append(f"taker {execution.fee_rate:g}")
    if execution.maker_fee_rate is not None:
        parts.append(f"maker {execution.maker_fee_rate:g}")
    if execution.maker_rebate is not None:
        parts.append(f"rebate {execution.maker_rebate:g}")
    parts.append(
        "queue position modelled"
        if execution.queue_position
        else "no queue model (fills at the touch)"
    )
    if execution.optimistic_queue:
        parts.append("optimistic queue")
    if execution.latency_nanos:
        parts.append(f"latency {execution.latency_nanos:,} ns")
    if execution.slippage_ticks:
        parts.append(f"slippage {execution.slippage_ticks} tick(s)")
    return ", ".join(parts)


def _subtitle(summary: dict, pin: dict, fidelity: Optional[str]) -> str:
    where = (
        f"snapshot {pin['snapshot']!r}"
        if pin.get("snapshot")
        else f"version {pin['version']}"
        if pin.get("version") is not None
        else f"as of {pin['as_of']}"
        if pin.get("as_of")
        else "the latest version (unpinned)"
    )
    return (
        f"{summary.get('records_processed', 0):,} records replayed from {where}, "
        f"producing {summary.get('orders', 0):,} order(s) and "
        f"{summary.get('fills', 0):,} fill(s)."
    )


def _scalar(value: Any) -> Any:
    if isinstance(value, (list, tuple, dict)):
        return json.dumps(value, default=str)
    # A manifest field that was never measured says so, rather than
    # rendering as a blank cell that reads like a zero.
    return "n/a" if value is None else value


def report_payload(subject, **kwargs) -> dict:
    """The report's data for whatever kind of subject it is."""
    if hasattr(subject, "periods"):
        return factor_payload(subject, **kwargs)
    if hasattr(subject, "annualization"):
        return tearsheet_payload(subject, **kwargs)
    if hasattr(subject, "fork_name") and hasattr(subject, "explain"):
        return backtest_payload(subject, **kwargs)
    raise TypeError(f"cannot build a report for {type(subject).__name__}")


# -- rendering ----------------------------------------------------------------


def factor_report(panel, *, path: Optional[str] = None, **kwargs) -> str:
    return render(factor_payload(panel, **kwargs), path=path)


def tearsheet(series, *, path: Optional[str] = None, **kwargs) -> str:
    return render(tearsheet_payload(series, **kwargs), path=path)


def backtest_report(result, *, path: Optional[str] = None, **kwargs) -> str:
    return render(backtest_payload(result, **kwargs), path=path)


def render(payload: dict, path: Optional[str] = None) -> str:
    """Turn a payload into a single self-contained HTML file."""
    data = json.dumps(_clean(payload), allow_nan=False, default=str)
    # Escape every angle bracket, not just `</script>`: inside a script
    # element the HTML parser looks for the tag sequence before JavaScript
    # ever sees the string, so a table or column name carrying markup could
    # otherwise close the element. `<` is the same string to JSON and
    # inert to the parser.
    data = data.replace("<", "\\u003c").replace(">", "\\u003e")
    title = _html.escape(str(payload.get("title", "Report")))
    # One pass over the template, not two: substituting the title first lets a
    # title containing the other placeholder swallow the payload, and doing it
    # the other way round lets the payload swallow the title. A single pass
    # means neither substitution can see the other's output.
    substitutions = {"__TITLE__": title, "__DATA__": data}
    html = _re.sub(
        "__TITLE__|__DATA__", lambda match: substitutions[match.group()], _TEMPLATE
    )
    if path is not None:
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(html)
    return html


# Dark is a selected mode, not an inverted light one: the steps come from the
# review UI (near-black surface tiers, mint chrome) and the series hues are
# re-stepped for that surface. Declared twice so the OS preference and an
# explicit `data-theme` stamp both win in the right direction.
_DARK_TOKENS = """
  color-scheme: dark;
  --page: #0a0c10;
  --surface-1: #12161c;
  --surface-2: #1a1f26;
  --text-primary: #f6f7f9;
  --text-secondary: #abb3bf;
  --muted: #8590a0;
  --grid: #1e242b;
  --axis: #3a424b;
  --border: #2a3138;
  --accent: #42e2d0;
  --series-1: #1e9f93;
  --series-2: #c9812f;
  --series-3: #8f6dc6;
  --gain: #3fbc83;
  --loss: #ff7b7b;
  --warn: #f5b264;
  --fail: #ff6b6b;
  --shadow: rgba(0,0,0,.45);
"""

_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>__TITLE__</title>
<style>
:root {
  color-scheme: light;
  --page: #eef1f4;
  --surface-1: #ffffff;
  --surface-2: #f6f8fa;
  --text-primary: #0d1117;
  --text-secondary: #414b57;
  --muted: #6b7683;
  --grid: #e4e8ec;
  --axis: #c2c9d1;
  --border: rgba(13,17,23,0.13);
  --accent: #0d7d72;
  --series-1: #00958a;
  --series-2: #b2660d;
  --series-3: #6a4fb3;
  --gain: #0f7a4f;
  --loss: #b4262a;
  --warn: #a2650a;
  --fail: #b4262a;
  --shadow: rgba(13,17,23,.14);
  /* Named first so a machine that has the review UI's faces renders
     identically to it; the stacks below are what everyone else gets. No
     report embeds a font file: it would double the size of every run. */
  --font-ui: "Space Grotesk", system-ui, -apple-system, "Segoe UI", sans-serif;
  --font-mono: "Space Mono", ui-monospace, SFMono-Regular, Menlo, monospace;
}
@media (prefers-color-scheme: dark) {
  :root:not([data-theme="light"]) {__DARK_TOKENS__  }
}
:root[data-theme="dark"] {__DARK_TOKENS__}
* { box-sizing: border-box; }
body {
  margin: 0; padding: 30px 20px 64px;
  background: var(--page); color: var(--text-primary);
  font: 14.5px/1.55 var(--font-ui);
  -webkit-font-smoothing: antialiased;
}
main { max-width: 1080px; margin: 0 auto; }
h1 { font-size: 25px; margin: 0 0 5px; letter-spacing: -0.015em; }
h2 { font-size: 15.5px; margin: 0 0 2px; letter-spacing: -0.005em; }
p.sub { margin: 0 0 20px; color: var(--text-secondary); font-size: 13.5px; }
.micro {
  font-family: var(--font-mono); font-size: 10.5px; text-transform: lowercase;
  letter-spacing: .14em; color: var(--muted); margin: 0 0 9px;
}
section {
  background: var(--surface-1); border: 1px solid var(--border);
  border-radius: 8px; padding: 16px 18px; margin: 0 0 14px;
}
.status {
  display: flex; gap: 11px; align-items: flex-start;
  background: var(--surface-1); border: 1px solid var(--border);
  border-left-width: 3px; border-radius: 8px; padding: 14px 16px; margin: 0 0 14px;
}
.status .glyph { font-family: var(--font-mono); font-weight: 700; line-height: 1.4; }
.status.ok { border-left-color: var(--gain); }
.status.ok .glyph { color: var(--gain); }
.status.warn { border-left-color: var(--warn); }
.status.warn .glyph { color: var(--warn); }
.status.bad { border-left-color: var(--fail); }
.status.bad .glyph { color: var(--fail); }
.status .headline { font-weight: 600; }
.status .detail { color: var(--text-secondary); font-size: 13px; margin-top: 2px; }
.status ul { margin: 9px 0 0; padding-left: 18px; font-size: 13px; color: var(--text-secondary); }
.status li { margin-bottom: 3px; }
.status .code { font-family: var(--font-mono); font-size: 11.5px; color: var(--muted); }
.prov { font-size: 13px; color: var(--text-secondary); }
.prov dl { display: grid; grid-template-columns: max-content 1fr; gap: 4px 16px; margin: 0; }
.prov dt { color: var(--muted); }
.prov dd {
  margin: 0; font-family: var(--font-mono); font-size: 12px;
  color: var(--text-primary); word-break: break-all;
}
.warn {
  margin-top: 12px; padding: 8px 12px; border-radius: 6px;
  border: 1px solid var(--warn); color: var(--text-primary); font-size: 13px;
}
.tiles { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 10px; }
.tile { border: 1px solid var(--border); border-radius: 8px; padding: 11px 13px; background: var(--surface-2); }
.tile .label { font-size: 12px; color: var(--muted); }
.tile .value {
  font-family: var(--font-mono); font-size: 20px; margin-top: 3px;
  letter-spacing: -0.02em; font-variant-numeric: tabular-nums;
}
.tile .value.gain { color: var(--gain); }
.tile .value.loss { color: var(--loss); }
.tile .hint { font-size: 11.5px; color: var(--muted); margin-top: 2px; }
.legend { display: flex; flex-wrap: wrap; gap: 14px; margin: 2px 0 10px; font-size: 13px; color: var(--text-secondary); }
.legend span { display: inline-flex; align-items: center; gap: 6px; }
.swatch { width: 10px; height: 10px; border-radius: 2px; display: inline-block; }
.chart-wrap { overflow-x: auto; }
svg { display: block; width: 100%; height: auto; }
.tick { fill: var(--muted); font-size: 11px; font-family: var(--font-mono); }
.gridline { stroke: var(--grid); stroke-width: 1; }
.baseline { stroke: var(--axis); stroke-width: 1; }
details { margin-top: 12px; }
summary { cursor: pointer; color: var(--text-secondary); font-size: 13px; }
table { border-collapse: collapse; width: 100%; margin-top: 10px; font-size: 13px; }
th, td {
  text-align: right; padding: 5px 8px; border-bottom: 1px solid var(--border);
  font-family: var(--font-mono); font-size: 12px; font-variant-numeric: tabular-nums;
}
th:first-child, td:first-child { text-align: left; }
th { color: var(--muted); font-weight: 600; font-size: 11px; text-transform: lowercase; letter-spacing: .06em; }
.table-wrap { overflow-x: auto; }
pre {
  font-family: var(--font-mono); font-size: 11.5px; line-height: 1.5;
  background: var(--surface-2); border: 1px solid var(--border);
  border-radius: 6px; padding: 12px; overflow: auto; max-height: 460px;
  margin: 10px 0 0; color: var(--text-secondary);
}
.tooltip {
  position: fixed; pointer-events: none; opacity: 0; transition: opacity .08s;
  background: var(--surface-1); border: 1px solid var(--border);
  border-radius: 6px; padding: 7px 10px; font-size: 12px;
  box-shadow: 0 4px 14px var(--shadow); z-index: 10; max-width: 280px;
}
.tooltip .row { display: flex; align-items: center; gap: 6px; }
.tooltip .k { color: var(--muted); }
.tooltip strong { font-family: var(--font-mono); }
.note { color: var(--text-secondary); font-size: 13px; margin: 8px 0 0; }
</style>
</head>
<body>
<main id="app"></main>
<div class="tooltip" id="tip"></div>
<script>
const DATA = __DATA__;
const PALETTE = ["var(--series-1)", "var(--series-2)", "var(--series-3)"];
const tip = document.getElementById("tip");

const group = v => Math.abs(v) >= 1000 ? v.toLocaleString(undefined, {
  minimumFractionDigits: 2, maximumFractionDigits: 2 }) : v.toFixed(2);
const FMT = {
  percent0: v => (v * 100).toFixed(0) + "%",
  percent1: v => (v * 100).toFixed(1) + "%",
  percent2: v => (v * 100).toFixed(2) + "%",
  percent3: v => (v * 100).toFixed(3) + "%",
  decimal2: v => v.toFixed(2),
  decimal3: v => v.toFixed(3),
  decimal4: v => v.toFixed(4),
  integer: v => Math.round(v).toLocaleString(),
  // No currency symbol: a run is denominated in whatever its data was.
  money: v => (v < 0 ? "-" : "") + group(Math.abs(v)),
  money0: v => Math.round(v).toLocaleString(),
  price: v => v.toFixed(4),
};
function fmt(v, kind) {
  if (v === null || v === undefined || Number.isNaN(v)) return "n/a";
  if (typeof v !== "number") return String(v);
  return (FMT[kind] || FMT.decimal3)(v);
}
function esc(s) {
  return String(s).replace(/[&<>"]/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]));
}
function el(tag, attrs, children) {
  const n = document.createElementNS(
    tag === "svg" || SVG_TAGS.has(tag) ? "http://www.w3.org/2000/svg" : "http://www.w3.org/1999/xhtml", tag);
  for (const k in (attrs || {})) n.setAttribute(k, attrs[k]);
  for (const c of (children || [])) n.appendChild(typeof c === "string" ? document.createTextNode(c) : c);
  return n;
}
const SVG_TAGS = new Set(["svg","g","path","rect","circle","line","text","polyline","clipPath","defs"]);

function showTip(evt, html) {
  tip.innerHTML = html;
  tip.style.opacity = "1";
  const pad = 14;
  let x = evt.clientX + pad, y = evt.clientY + pad;
  const r = tip.getBoundingClientRect();
  if (x + r.width > window.innerWidth) x = evt.clientX - r.width - pad;
  if (y + r.height > window.innerHeight) y = evt.clientY - r.height - pad;
  tip.style.left = x + "px"; tip.style.top = y + "px";
}
function hideTip() { tip.style.opacity = "0"; }

function niceTicks(lo, hi, count) {
  if (lo === hi) { lo -= 0.5; hi += 0.5; }
  const span = hi - lo;
  const raw = span / Math.max(1, count);
  const mag = Math.pow(10, Math.floor(Math.log10(raw)));
  const norm = raw / mag;
  const step = (norm >= 5 ? 10 : norm >= 2 ? 5 : norm >= 1 ? 2 : 1) * mag;
  const start = Math.ceil(lo / step) * step;
  const out = [];
  for (let v = start; v <= hi + step * 1e-9; v += step) out.push(v);
  return out;
}
function isDate(v) { return typeof v === "string" && /^\\d{4}-\\d{2}-\\d{2}/.test(v); }
function labelX(v, kind) {
  if (!isDate(v)) return String(v);
  // A run inside one day is read on the clock; a longer one, by date.
  if (kind === "time") return v.slice(11, 19) || v.slice(0, 10);
  if (kind === "datetime") return v.slice(0, 19).replace("T", " ");
  return v.slice(0, 10);
}

const W = 960, H = 300, M = { t: 12, r: 16, b: 30, l: 64 };

function lineChart(spec) {
  const wrap = el("div", { class: "chart-wrap" }, []);
  const svg = el("svg", { viewBox: `0 0 ${W} ${H}`, role: "img",
                          "aria-label": spec.title });
  const series = spec.series.filter(s => s.points.some(p => p.y !== null));
  if (!series.length) { wrap.appendChild(el("p", { class: "note" }, ["No data."])); return wrap; }
  const xs = series[0].points.map(p => p.x);
  const n = xs.length;
  let lo = Infinity, hi = -Infinity;
  for (const s of series) for (const p of s.points) {
    if (p.y === null || p.y === undefined) continue;
    lo = Math.min(lo, p.y); hi = Math.max(hi, p.y);
  }
  if (spec.kind === "area") hi = Math.max(hi, 0);
  const pad = (hi - lo) * 0.08 || 0.01;
  lo -= pad; hi += pad;
  const px = i => M.l + (n <= 1 ? 0 : (i * (W - M.l - M.r)) / (n - 1));
  const py = v => M.t + (H - M.t - M.b) * (1 - (v - lo) / (hi - lo));

  for (const t of niceTicks(lo, hi, 5)) {
    svg.appendChild(el("line", { class: "gridline", x1: M.l, x2: W - M.r, y1: py(t), y2: py(t) }));
    svg.appendChild(el("text", { class: "tick", x: M.l - 8, y: py(t) + 4, "text-anchor": "end" },
                       [fmt(t, spec.yFormat)]));
  }
  const step = Math.max(1, Math.floor(n / 6));
  for (let i = 0; i < n; i += step) {
    // Anchor the edge labels inward so they cannot be clipped by the viewBox.
    const anchor = i === 0 ? "start" : (px(i) > W - M.r - 40 ? "end" : "middle");
    svg.appendChild(el("text", { class: "tick", x: px(i), y: H - 10, "text-anchor": anchor },
                       [labelX(xs[i], spec.xFormat)]));
  }
  if (lo < 0 && hi > 0) {
    svg.appendChild(el("line", { class: "baseline", x1: M.l, x2: W - M.r, y1: py(0), y2: py(0) }));
  }
  series.forEach((s, si) => {
    const color = PALETTE[si % PALETTE.length];
    const pts = [];
    s.points.forEach((p, i) => { if (p.y !== null && p.y !== undefined) pts.push([px(i), py(p.y)]); });
    if (spec.kind === "area" && pts.length) {
      const d = "M" + pts.map(p => p.join(",")).join("L") +
                `L${pts[pts.length - 1][0]},${py(0)}L${pts[0][0]},${py(0)}Z`;
      svg.appendChild(el("path", { d, fill: color, "fill-opacity": "0.16", stroke: "none" }));
    }
    svg.appendChild(el("path", {
      d: "M" + pts.map(p => p.join(",")).join("L"),
      fill: "none", stroke: color, "stroke-width": "2",
      "stroke-linejoin": "round", "stroke-linecap": "round",
    }));
  });
  // Crosshair + tooltip across all series at the hovered index.
  const hair = el("line", { class: "baseline", y1: M.t, y2: H - M.b, x1: 0, x2: 0, opacity: "0" });
  svg.appendChild(hair);
  const overlay = el("rect", { x: M.l, y: M.t, width: W - M.l - M.r, height: H - M.t - M.b,
                               fill: "transparent" });
  overlay.addEventListener("mousemove", evt => {
    const box = svg.getBoundingClientRect();
    const rel = ((evt.clientX - box.left) / box.width) * W;
    let i = Math.round(((rel - M.l) / (W - M.l - M.r)) * (n - 1));
    i = Math.max(0, Math.min(n - 1, i));
    hair.setAttribute("x1", px(i)); hair.setAttribute("x2", px(i));
    hair.setAttribute("opacity", "1");
    let html = `<div class="k">${esc(labelX(xs[i], spec.xFormat === "date" ? "date" : "datetime"))}</div>`;
    series.forEach((s, si) => {
      const p = s.points[i];
      html += `<div class="row"><span class="swatch" style="background:${PALETTE[si % PALETTE.length]}"></span>` +
              `${esc(s.name)}: <strong>${fmt(p && p.y, spec.yFormat)}</strong></div>`;
    });
    showTip(evt, html);
  });
  overlay.addEventListener("mouseleave", () => { hideTip(); hair.setAttribute("opacity", "0"); });
  svg.appendChild(overlay);
  wrap.appendChild(svg);
  return wrap;
}

// Marker shapes carry identity alongside hue, so a scatter stays readable
// for a red-green reader and in print.
function marker(kind, x, y, r) {
  if (kind === "up") return el("path", { d: `M${x},${y - r}L${x + r},${y + r * 0.75}L${x - r},${y + r * 0.75}Z` });
  if (kind === "down") return el("path", { d: `M${x},${y + r}L${x + r},${y - r * 0.75}L${x - r},${y - r * 0.75}Z` });
  return el("circle", { cx: x, cy: y, r: r * 0.9 });
}

function scatterChart(spec) {
  const wrap = el("div", { class: "chart-wrap" }, []);
  const svg = el("svg", { viewBox: `0 0 ${W} ${H}`, role: "img", "aria-label": spec.title });
  const at = v => { const t = Date.parse(v); return Number.isNaN(t) ? Number(v) : t; };
  let xlo = Infinity, xhi = -Infinity, lo = Infinity, hi = -Infinity;
  for (const s of spec.series) for (const p of s.points) {
    if (p.y === null || p.y === undefined) continue;
    const t = at(p.x);
    xlo = Math.min(xlo, t); xhi = Math.max(xhi, t);
    lo = Math.min(lo, p.y); hi = Math.max(hi, p.y);
  }
  if (!Number.isFinite(xlo)) { wrap.appendChild(el("p", { class: "note" }, ["No data."])); return wrap; }
  const ypad = (hi - lo) * 0.12 || 0.01;
  lo -= ypad; hi += ypad;
  const xpad = (xhi - xlo) * 0.03 || 1;
  const x0 = xlo - xpad, x1 = xhi + xpad;
  const px = t => M.l + ((t - x0) / (x1 - x0)) * (W - M.l - M.r);
  const py = v => M.t + (H - M.t - M.b) * (1 - (v - lo) / (hi - lo));

  for (const t of niceTicks(lo, hi, 5)) {
    svg.appendChild(el("line", { class: "gridline", x1: M.l, x2: W - M.r, y1: py(t), y2: py(t) }));
    svg.appendChild(el("text", { class: "tick", x: M.l - 8, y: py(t) + 4, "text-anchor": "end" },
                       [fmt(t, spec.yFormat)]));
  }
  const stamps = spec.series[0].points.map(p => p.x);
  const slots = 6;
  for (let i = 0; i <= slots; i++) {
    const t = x0 + ((x1 - x0) * i) / slots;
    const near = stamps.reduce((best, s) =>
      Math.abs(at(s) - t) < Math.abs(at(best) - t) ? s : best, stamps[0]);
    const anchor = i === 0 ? "start" : (i === slots ? "end" : "middle");
    svg.appendChild(el("text", { class: "tick", x: px(t), y: H - 10, "text-anchor": anchor },
                       [labelX(near, spec.xFormat)]));
  }
  spec.series.forEach((s, si) => {
    const color = PALETTE[si % PALETTE.length];
    for (const p of s.points) {
      if (p.y === null || p.y === undefined) continue;
      const node = marker(s.marker, px(at(p.x)), py(p.y), 6);
      node.setAttribute("fill", color);
      node.setAttribute("fill-opacity", "0.85");
      // A 2px surface ring keeps overlapping marks countable.
      node.setAttribute("stroke", "var(--surface-1)");
      node.setAttribute("stroke-width", "1.5");
      node.addEventListener("mousemove", evt => showTip(evt,
        `<div class="k">${esc(labelX(p.x, "datetime"))}</div>` +
        `<div class="row"><span class="swatch" style="background:${color}"></span>` +
        `${esc(s.name)}: <strong>${fmt(p.y, spec.yFormat)}</strong></div>` +
        (p.meta ? `<div class="k">${esc(p.meta)}</div>` : "")));
      node.addEventListener("mouseleave", hideTip);
      svg.appendChild(node);
    }
  });
  wrap.appendChild(svg);
  return wrap;
}

function barChart(spec) {
  const wrap = el("div", { class: "chart-wrap" }, []);
  const svg = el("svg", { viewBox: `0 0 ${W} ${H}`, role: "img", "aria-label": spec.title });
  const cats = [...new Set(spec.categories)];
  const series = spec.series;
  let lo = 0, hi = 0;
  for (const s of series) for (const p of s.points) {
    if (p.y === null || p.y === undefined) continue;
    const top = p.y + (p.err || 0), bot = p.y - (p.err || 0);
    lo = Math.min(lo, bot); hi = Math.max(hi, top);
  }
  const pad = (hi - lo) * 0.1 || 0.01;
  lo -= pad; hi += pad;
  const py = v => M.t + (H - M.t - M.b) * (1 - (v - lo) / (hi - lo));
  const band = (W - M.l - M.r) / Math.max(1, cats.length);
  const gap = 2; // the 2px surface gap between adjacent fills
  const bw = Math.max(3, (band - 18) / series.length - gap);

  for (const t of niceTicks(lo, hi, 5)) {
    svg.appendChild(el("line", { class: "gridline", x1: M.l, x2: W - M.r, y1: py(t), y2: py(t) }));
    svg.appendChild(el("text", { class: "tick", x: M.l - 8, y: py(t) + 4, "text-anchor": "end" },
                       [fmt(t, spec.yFormat)]));
  }
  svg.appendChild(el("line", { class: "baseline", x1: M.l, x2: W - M.r, y1: py(0), y2: py(0) }));
  cats.forEach((cat, ci) => {
    const cx = M.l + band * ci + band / 2;
    svg.appendChild(el("text", { class: "tick", x: cx, y: H - 10, "text-anchor": "middle" }, [cat]));
    series.forEach((s, si) => {
      const p = s.points.find(q => q.x === cat);
      if (!p || p.y === null || p.y === undefined) return;
      const color = PALETTE[si % PALETTE.length];
      const total = series.length * (bw + gap) - gap;
      const x = cx - total / 2 + si * (bw + gap);
      const y0 = py(0), y1 = py(p.y);
      const rect = el("rect", {
        x, y: Math.min(y0, y1), width: bw, height: Math.max(1, Math.abs(y1 - y0)),
        fill: color, rx: 4, ry: 4,
      });
      rect.addEventListener("mousemove", evt => showTip(evt,
        `<div class="k">${esc(cat)}</div><div class="row">` +
        `<span class="swatch" style="background:${color}"></span>${esc(s.name)}: ` +
        `<strong>${fmt(p.y, spec.yFormat)}</strong></div>` +
        (p.err ? `<div class="k">± ${fmt(p.err, spec.yFormat)} (s.e.)</div>` : "")));
      rect.addEventListener("mouseleave", hideTip);
      svg.appendChild(rect);
      if (p.err) {
        const top = py(p.y + p.err), bot = py(p.y - p.err), mid = x + bw / 2;
        svg.appendChild(el("line", { x1: mid, x2: mid, y1: top, y2: bot,
                                     stroke: "var(--text-secondary)", "stroke-width": "1" }));
      }
    });
  });
  wrap.appendChild(svg);
  return wrap;
}

const MARK_GLYPH = { up: "▲", down: "▼" };

function legend(spec) {
  if (spec.series.length < 2) return null;
  const box = el("div", { class: "legend" }, []);
  spec.series.forEach((s, i) => {
    const item = el("span", {}, []);
    item.appendChild(el("span", { class: "swatch", style: `background:${PALETTE[i % PALETTE.length]}` }, []));
    item.appendChild(document.createTextNode(
      s.marker && MARK_GLYPH[s.marker] ? `${MARK_GLYPH[s.marker]} ${s.name}` : s.name));
    box.appendChild(item);
  });
  return box;
}

function dataTable(spec) {
  const cols = spec.columns && spec.columns.length
    ? spec.columns
    : (spec.rows.length ? Object.keys(spec.rows[0]) : []);
  const formats = spec.formats || {};
  const table = el("table", {}, []);
  const head = el("tr", {}, cols.map(c => el("th", {}, [c])));
  table.appendChild(head);
  for (const row of spec.rows) {
    table.appendChild(el("tr", {}, cols.map(c => {
      const v = row[c];
      let text;
      if (typeof v === "number" && formats[c]) text = fmt(v, formats[c]);
      else if (typeof v === "number") text = Number.isInteger(v) ? String(v) : v.toFixed(6);
      else if (v === null || v === undefined) text = "";
      else text = String(v).replace(/T00:00:00(\\.0+)?$/, "");  // a date, not an instant
      return el("td", {}, [text]);
    })));
  }
  const wrap = el("div", { class: "table-wrap" }, [table]);
  return wrap;
}

function chartSection(spec) {
  const sec = el("section", {}, []);
  sec.appendChild(el("h2", {}, [spec.title]));
  if (spec.subtitle) sec.appendChild(el("p", { class: "sub" }, [spec.subtitle]));
  const lg = legend(spec);
  if (lg) sec.appendChild(lg);
  sec.appendChild(spec.kind === "bar" ? barChart(spec)
                : spec.kind === "scatter" ? scatterChart(spec)
                : lineChart(spec));
  const det = el("details", {}, []);
  det.appendChild(el("summary", {}, ["View as table"]));
  const rows = [];
  if (spec.kind === "bar") {
    for (const cat of [...new Set(spec.categories)]) {
      const row = { category: cat };
      spec.series.forEach(s => {
        const p = s.points.find(q => q.x === cat);
        row[s.name] = p ? p.y : null;
      });
      rows.push(row);
    }
  } else if (spec.kind === "scatter") {
    // Every mark stands alone here; there is no shared x to align on.
    spec.series.forEach(s => s.points.forEach(p => {
      rows.push({ x: labelX(p.x, "datetime"), series: s.name, y: p.y, detail: p.meta || "" });
    }));
    rows.sort((a, b) => (a.x < b.x ? -1 : a.x > b.x ? 1 : 0));
  } else {
    const xs = spec.series[0] ? spec.series[0].points.map(p => p.x) : [];
    xs.forEach((x, i) => {
      const row = { x: labelX(x, "datetime") };
      spec.series.forEach(s => { row[s.name] = s.points[i] ? s.points[i].y : null; });
      rows.push(row);
    });
  }
  det.appendChild(dataTable({ rows, columns: rows.length ? Object.keys(rows[0]) : [] }));
  sec.appendChild(det);
  return sec;
}

function statusBanner(status) {
  const glyph = { ok: "OK", warn: "!", bad: "X" }[status.tone] || "?";
  const box = el("div", { class: `status ${status.tone || "ok"}` }, []);
  box.appendChild(el("div", { class: "glyph" }, [glyph]));
  const body = el("div", {}, []);
  body.appendChild(el("div", { class: "headline" }, [status.headline]));
  if (status.detail) body.appendChild(el("div", { class: "detail" }, [status.detail]));
  if ((status.items || []).length) {
    const list = el("ul", {}, []);
    for (const item of status.items) {
      const li = el("li", {}, []);
      li.appendChild(el("span", { class: "code" }, [`${item.severity}/${item.code} `]));
      li.appendChild(document.createTextNode(item.message));
      if (item.remediation) {
        li.appendChild(el("div", { class: "code" }, [item.remediation]));
      }
      list.appendChild(li);
    }
    body.appendChild(list);
  }
  box.appendChild(body);
  return box;
}

function render() {
  const app = document.getElementById("app");
  app.appendChild(el("h1", {}, [DATA.title]));
  const p = DATA.provenance || {};
  const pinned = p.pin && p.pin.pinned;
  app.appendChild(el("p", { class: "sub" }, [DATA.subtitle ||
    (pinned ? "Pinned to an immutable data version." : "Unpinned: read at the latest version.")]));

  if (DATA.status) app.appendChild(statusBanner(DATA.status));

  const prov = el("section", { class: "prov" }, []);
  prov.appendChild(el("h2", {}, ["Provenance"]));
  const dl = el("dl", {}, []);
  const pin = p.pin || {};
  // Show the read point that is actually in force, not every axis that is
  // not: printing "Version: latest" beside "Snapshot: v1" reads as a
  // contradiction.
  const entries = [["Digest", p.digest]];
  if (pin.version !== null && pin.version !== undefined) {
    entries.push(["Version", typeof pin.version === "object"
      ? JSON.stringify(pin.version) : String(pin.version)]);
  }
  if (pin.as_of) entries.push(["As of", pin.as_of]);
  if (pin.snapshot) entries.push(["Snapshot", pin.snapshot]);
  if (!pin.pinned) entries.push(["Read point", "latest (unpinned)"]);
  if (pin.event_time_cutoff !== undefined) {
    entries.push(["Event-time cutoff", pin.event_time_cutoff || "none"]);
  }
  for (const [k, v] of (p.entries || [])) entries.push([k, v]);
  for (const [k, v] of entries) {
    dl.appendChild(el("dt", {}, [k]));
    dl.appendChild(el("dd", {}, [String(v === undefined || v === null ? "-" : v)]));
  }
  for (const [k, v] of Object.entries(p.parameters || {})) {
    dl.appendChild(el("dt", {}, [k]));
    dl.appendChild(el("dd", {}, [JSON.stringify(v)]));
  }
  prov.appendChild(dl);
  for (const w of (p.warnings || [])) {
    prov.appendChild(el("div", { class: "warn" }, ["⚠ " + w]));
  }
  app.appendChild(prov);

  if ((DATA.headline || []).length) {
    const sec = el("section", {}, []);
    sec.appendChild(el("h2", {}, ["Summary"]));
    const tiles = el("div", { class: "tiles" }, []);
    for (const t of DATA.headline) {
      const tile = el("div", { class: "tile" }, []);
      tile.appendChild(el("div", { class: "label" }, [t.label]));
      // A signed figure keeps its sign in the text; the hue only reinforces it.
      const signed = t.tone === "signed" && typeof t.value === "number" && t.value !== 0;
      const cls = "value" + (signed ? (t.value > 0 ? " gain" : " loss") : "");
      const text = fmt(t.value, t.format);
      tile.appendChild(el("div", { class: cls },
        [signed && t.value > 0 && !text.startsWith("+") ? "+" + text : text]));
      if (t.hint) tile.appendChild(el("div", { class: "hint" }, [t.hint]));
      tiles.appendChild(tile);
    }
    sec.appendChild(tiles);
    for (const note of (DATA.notes || [])) sec.appendChild(el("p", { class: "note" }, [note]));
    app.appendChild(sec);
  }

  for (const spec of (DATA.charts || [])) app.appendChild(chartSection(spec));

  for (const t of (DATA.tables || [])) {
    if (!t.rows || !t.rows.length) continue;
    const sec = el("section", {}, []);
    sec.appendChild(el("h2", {}, [t.title]));
    if (t.note) sec.appendChild(el("p", { class: "note" }, [t.note]));
    sec.appendChild(dataTable(t));
    app.appendChild(sec);
  }

  for (const block of (DATA.code || [])) {
    if (!block.text) continue;
    const sec = el("section", {}, []);
    sec.appendChild(el("h2", {}, [block.title]));
    const det = el("details", {}, []);
    det.appendChild(el("summary", {}, ["Show"]));
    det.appendChild(el("pre", {}, [block.text]));
    sec.appendChild(det);
    app.appendChild(sec);
  }
}
render();
</script>
</body>
</html>
""".replace("__DARK_TOKENS__", _DARK_TOKENS)
