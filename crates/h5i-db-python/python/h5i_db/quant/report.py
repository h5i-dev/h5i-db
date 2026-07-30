"""Self-contained HTML reports (ROADMAP_QUANT.md §6, M2).

One renderer serves every report kind. Output is a single file with inline
CSS and JS and the data embedded as JSON: no CDN, no build step, no network
access at view time, matching the cookbook pages and the review UI's
single-asset convention.

Section one of every report is the provenance header, so a reader sees which
data version produced the numbers before seeing the numbers.

    html = factor_report(panel, path="factor.html")
    html = tearsheet(series, benchmark=bench, path="tearsheet.html")

``report_payload`` returns the same content as a dict, which is what the CLI
and any agent consumer should read instead of scraping the HTML.
"""

from __future__ import annotations

import datetime as _dt
import html as _html
import json
import math
from typing import Any, Optional, Sequence

__all__ = ["factor_report", "tearsheet", "render", "report_payload"]

# Categorical slots 1-3 from the validated reference palette. Three is the
# documented all-pairs safe count; a report never draws more than three
# horizons as distinct series, and folds the rest into a table.
_SERIES_LIGHT = ("#2a78d6", "#eb6834", "#1baf7a")
_SERIES_DARK = ("#3987e5", "#d95926", "#199e70")
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
        {"label": "Rows dropped", "value": loss["total"], "format": "percent"}
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


def report_payload(subject, **kwargs) -> dict:
    """The report's data for whatever kind of subject it is."""
    if hasattr(subject, "periods"):
        return factor_payload(subject, **kwargs)
    if hasattr(subject, "annualization"):
        return tearsheet_payload(subject, **kwargs)
    raise TypeError(f"cannot build a report for {type(subject).__name__}")


# -- rendering ----------------------------------------------------------------


def factor_report(panel, *, path: Optional[str] = None, **kwargs) -> str:
    return render(factor_payload(panel, **kwargs), path=path)


def tearsheet(series, *, path: Optional[str] = None, **kwargs) -> str:
    return render(tearsheet_payload(series, **kwargs), path=path)


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
    html = _TEMPLATE.replace("__TITLE__", title).replace("__DATA__", data)
    if path is not None:
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(html)
    return html


_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>__TITLE__</title>
<style>
:root {
  color-scheme: light;
  --surface-1: #fcfcfb;
  --page: #f9f9f7;
  --text-primary: #0b0b0b;
  --text-secondary: #52514e;
  --muted: #898781;
  --grid: #e1e0d9;
  --axis: #c3c2b7;
  --border: rgba(11,11,11,0.10);
  --series-1: #2a78d6;
  --series-2: #eb6834;
  --series-3: #1baf7a;
  --warn: #ec835a;
}
@media (prefers-color-scheme: dark) {
  :root:not([data-theme="light"]) {
    color-scheme: dark;
    --surface-1: #1a1a19;
    --page: #0d0d0d;
    --text-primary: #ffffff;
    --text-secondary: #c3c2b7;
    --muted: #898781;
    --grid: #2c2c2a;
    --axis: #383835;
    --border: rgba(255,255,255,0.10);
    --series-1: #3987e5;
    --series-2: #d95926;
    --series-3: #199e70;
  }
}
:root[data-theme="dark"] {
  color-scheme: dark;
  --surface-1: #1a1a19;
  --page: #0d0d0d;
  --text-primary: #ffffff;
  --text-secondary: #c3c2b7;
  --muted: #898781;
  --grid: #2c2c2a;
  --axis: #383835;
  --border: rgba(255,255,255,0.10);
  --series-1: #3987e5;
  --series-2: #d95926;
  --series-3: #199e70;
}
* { box-sizing: border-box; }
body {
  margin: 0; padding: 32px 20px 64px;
  background: var(--page); color: var(--text-primary);
  font: 15px/1.55 system-ui, -apple-system, "Segoe UI", sans-serif;
}
main { max-width: 1040px; margin: 0 auto; }
h1 { font-size: 26px; margin: 0 0 4px; letter-spacing: -0.01em; }
h2 { font-size: 17px; margin: 0 0 2px; letter-spacing: -0.005em; }
p.sub { margin: 0 0 18px; color: var(--text-secondary); font-size: 14px; }
section {
  background: var(--surface-1); border: 1px solid var(--border);
  border-radius: 10px; padding: 18px 20px; margin: 0 0 18px;
}
.prov { font-size: 13px; color: var(--text-secondary); }
.prov dl { display: grid; grid-template-columns: max-content 1fr; gap: 4px 16px; margin: 0; }
.prov dt { color: var(--muted); }
.prov dd { margin: 0; font-variant-numeric: tabular-nums; word-break: break-all; }
.warn {
  margin-top: 12px; padding: 8px 12px; border-radius: 6px;
  border: 1px solid var(--warn); color: var(--text-primary); font-size: 13px;
}
.tiles { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 12px; }
.tile { border: 1px solid var(--border); border-radius: 8px; padding: 12px 14px; }
.tile .label { font-size: 12px; color: var(--muted); }
.tile .value { font-size: 22px; margin-top: 2px; letter-spacing: -0.01em; }
.legend { display: flex; flex-wrap: wrap; gap: 14px; margin: 2px 0 10px; font-size: 13px; color: var(--text-secondary); }
.legend span { display: inline-flex; align-items: center; gap: 6px; }
.swatch { width: 10px; height: 10px; border-radius: 2px; display: inline-block; }
.chart-wrap { overflow-x: auto; }
svg { display: block; width: 100%; height: auto; }
.tick { fill: var(--muted); font-size: 11px; }
.gridline { stroke: var(--grid); stroke-width: 1; }
.baseline { stroke: var(--axis); stroke-width: 1; }
details { margin-top: 12px; }
summary { cursor: pointer; color: var(--text-secondary); font-size: 13px; }
table { border-collapse: collapse; width: 100%; margin-top: 10px; font-size: 13px; }
th, td { text-align: right; padding: 5px 8px; border-bottom: 1px solid var(--border); font-variant-numeric: tabular-nums; }
th:first-child, td:first-child { text-align: left; }
th { color: var(--muted); font-weight: 600; }
.tooltip {
  position: fixed; pointer-events: none; opacity: 0; transition: opacity .08s;
  background: var(--surface-1); border: 1px solid var(--border);
  border-radius: 6px; padding: 7px 10px; font-size: 12px;
  box-shadow: 0 4px 14px rgba(0,0,0,.14); z-index: 10; max-width: 260px;
}
.tooltip .row { display: flex; align-items: center; gap: 6px; }
.tooltip .k { color: var(--muted); }
.note { color: var(--text-secondary); font-size: 13px; margin: 6px 0 0; }
</style>
</head>
<body>
<main id="app"></main>
<div class="tooltip" id="tip"></div>
<script>
const DATA = __DATA__;
const PALETTE = ["var(--series-1)", "var(--series-2)", "var(--series-3)"];
const tip = document.getElementById("tip");

const FMT = {
  percent1: v => (v * 100).toFixed(1) + "%",
  percent2: v => (v * 100).toFixed(2) + "%",
  percent3: v => (v * 100).toFixed(3) + "%",
  decimal2: v => v.toFixed(2),
  decimal3: v => v.toFixed(3),
  decimal4: v => v.toFixed(4),
  integer: v => Math.round(v).toLocaleString(),
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
const SVG_TAGS = new Set(["g","path","rect","circle","line","text","polyline","clipPath","defs"]);

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
function labelX(v) { return isDate(v) ? v.slice(0, 10) : String(v); }

const W = 960, H = 300, M = { t: 12, r: 16, b: 30, l: 58 };

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
                       [labelX(xs[i])]));
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
    let html = `<div class="k">${esc(labelX(xs[i]))}</div>`;
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

function legend(spec) {
  if (spec.series.length < 2) return null;
  const box = el("div", { class: "legend" }, []);
  spec.series.forEach((s, i) => {
    const item = el("span", {}, []);
    item.appendChild(el("span", { class: "swatch", style: `background:${PALETTE[i % PALETTE.length]}` }, []));
    item.appendChild(document.createTextNode(s.name));
    box.appendChild(item);
  });
  return box;
}

function dataTable(spec) {
  const cols = spec.columns && spec.columns.length
    ? spec.columns
    : (spec.rows.length ? Object.keys(spec.rows[0]) : []);
  const table = el("table", {}, []);
  const head = el("tr", {}, cols.map(c => el("th", {}, [c])));
  table.appendChild(head);
  for (const row of spec.rows) {
    table.appendChild(el("tr", {}, cols.map(c => {
      const v = row[c];
      let text;
      if (typeof v === "number") text = Number.isInteger(v) ? String(v) : v.toFixed(6);
      else if (v === null || v === undefined) text = "";
      else text = String(v).replace(/T00:00:00(\\.0+)?$/, "");  // a date, not an instant
      return el("td", {}, [text]);
    })));
  }
  return table;
}

function chartSection(spec) {
  const sec = el("section", {}, []);
  sec.appendChild(el("h2", {}, [spec.title]));
  if (spec.subtitle) sec.appendChild(el("p", { class: "sub" }, [spec.subtitle]));
  const lg = legend(spec);
  if (lg) sec.appendChild(lg);
  sec.appendChild(spec.kind === "bar" ? barChart(spec) : lineChart(spec));
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
  } else {
    const xs = spec.series[0] ? spec.series[0].points.map(p => p.x) : [];
    xs.forEach((x, i) => {
      const row = { x: labelX(x) };
      spec.series.forEach(s => { row[s.name] = s.points[i] ? s.points[i].y : null; });
      rows.push(row);
    });
  }
  det.appendChild(dataTable({ rows, columns: rows.length ? Object.keys(rows[0]) : [] }));
  sec.appendChild(det);
  return sec;
}

function render() {
  const app = document.getElementById("app");
  app.appendChild(el("h1", {}, [DATA.title]));
  const p = DATA.provenance || {};
  const pinned = p.pin && p.pin.pinned;
  app.appendChild(el("p", { class: "sub" },
    [pinned ? "Pinned to an immutable data version." : "Unpinned: read at the latest version."]));

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
  entries.push(["Event-time cutoff", pin.event_time_cutoff || "none"]);
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
      tile.appendChild(el("div", { class: "value" }, [fmt(t.value, t.format)]));
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
    sec.appendChild(dataTable(t));
    app.appendChild(sec);
  }
}
render();
</script>
</body>
</html>
"""
