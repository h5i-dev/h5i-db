"""Portfolio performance statistics, computed in the engine (ROADMAP_QUANT.md Q2).

The shape follows ``pyfolio``, but the arithmetic follows ``empyrical``:
pyfolio's ratio functions are deprecated shims over it, so empyrical is the
definition of record and what these are tested against.

    series = returns(db, "strategy_returns")
    series.stats()
    series.drawdown_table()

Everything here takes a *returns series* -- one row per bar, a simple
(non-cumulative) decimal return -- which is the minimum input pyfolio's own
tear sheet needs. A factor panel produces one directly
(:meth:`~h5i_db.quant.FactorPanel.returns`), and so will a backtest run.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional, Union

from ..dataframe import LazyFrame, quote_ident
from ._common import Pin, Provenance, indent, resolve_source, sql_number

__all__ = [
    "returns",
    "from_levels",
    "ReturnSeries",
    "DAILY",
    "WEEKLY",
    "MONTHLY",
    "YEARLY",
]

# empyrical's ANNUALIZATION_FACTORS, spelled out so a caller can pass a bar
# count directly for anything else (24 * 365 for hourly crypto, say).
DAILY = 252
WEEKLY = 52
MONTHLY = 12
YEARLY = 1

TS = "ts"
RET = "ret"


def returns(
    db: Any,
    source: Union[str, LazyFrame],
    *,
    ts: str = TS,
    ret: str = RET,
    annualization: float = DAILY,
    pin: Optional[Pin] = None,
    version: Optional[Any] = None,
    as_of: Optional[str] = None,
    snapshot: Optional[str] = None,
    event_time_cutoff: Optional[Any] = None,
    deterministic: bool = True,
) -> "ReturnSeries":
    """Open a returns series for analysis.

    ``source`` is a table name or a :class:`~h5i_db.LazyFrame` with a
    timestamp column and a return column. ``annualization`` is the number of
    bars per year (252 for daily bars, the empyrical default).
    """
    read_pin = Pin.coerce(
        pin,
        version=version,
        as_of=as_of,
        snapshot=snapshot,
        event_time_cutoff=event_time_cutoff,
    )
    sql_text, described = resolve_source(db, source, read_pin, ts, "returns")
    base = (
        f"SELECT {quote_ident(ts)} AS {quote_ident(TS)}, "
        f"CAST({quote_ident(ret)} AS DOUBLE) AS {quote_ident(RET)}\n"
        f"FROM (\n{indent(sql_text)}\n) AS {quote_ident('_s')}\n"
        f"WHERE {quote_ident(ret)} IS NOT NULL"
    )
    provenance = Provenance(
        kind="return_series",
        pin=read_pin,
        parameters={"annualization": annualization, "deterministic": deterministic},
        sources={"returns": described},
        sql={"returns": base},
    )
    return ReturnSeries(
        db=db,
        _sql=base,
        annualization=float(annualization),
        provenance=provenance,
        deterministic=deterministic,
    )


def from_levels(
    db: Any,
    source: Union[str, LazyFrame],
    *,
    ts: str = TS,
    level: str = "equity",
    annualization: float = DAILY,
    pin: Optional[Pin] = None,
    version: Optional[Any] = None,
    as_of: Optional[str] = None,
    snapshot: Optional[str] = None,
    event_time_cutoff: Optional[Any] = None,
    deterministic: bool = True,
) -> "ReturnSeries":
    """Open a returns series from a *level* series such as an equity curve.

    This is what turns a backtest run into a tearsheet: a run writes
    ``bt_equity`` into its fork, and

        series = quant.perf.from_levels(fork, "bt_equity")
        quant.tearsheet(series, path="run.html")

    is the whole path from simulation to report. The first bar has no prior
    level and so has no return; it is dropped rather than being called zero,
    which would put a fake flat bar at the start of every curve.
    """
    read_pin = Pin.coerce(
        pin,
        version=version,
        as_of=as_of,
        snapshot=snapshot,
        event_time_cutoff=event_time_cutoff,
    )
    sql_text, described = resolve_source(db, source, read_pin, ts, "levels")
    q_ts, q_level = quote_ident(ts), quote_ident(level)
    base = (
        f"SELECT {quote_ident(TS)}, {quote_ident(RET)} FROM (\n"
        f"  SELECT {q_ts} AS {quote_ident(TS)},\n"
        f"         CAST({q_level} AS DOUBLE) / NULLIF(lag(CAST({q_level} AS DOUBLE)) "
        f"OVER (ORDER BY {q_ts}), 0) - 1 AS {quote_ident(RET)}\n"
        f"  FROM (\n{indent(sql_text, 4)}\n  ) AS {quote_ident('_lv')}\n"
        f") AS {quote_ident('_r')}\n"
        f"WHERE {quote_ident(RET)} IS NOT NULL"
    )
    provenance = Provenance(
        kind="return_series",
        pin=read_pin,
        parameters={
            "annualization": annualization,
            "deterministic": deterministic,
            "derived_from": "levels",
            "level_column": level,
        },
        sources={"levels": described},
        sql={"returns": base},
    )
    return ReturnSeries(
        db=db,
        _sql=base,
        annualization=float(annualization),
        provenance=provenance,
        deterministic=deterministic,
    )


@dataclass
class ReturnSeries:
    """A pinned returns series. Statistics are queries, not cached frames."""

    db: Any
    _sql: str
    annualization: float
    provenance: Provenance
    deterministic: bool = True

    def _run(self, sql: str, **limits: Any):
        if self.deterministic:
            limits.setdefault("target_partitions", 1)
        return self.db.sql(sql, **limits)

    def sql(self) -> str:
        return self._sql

    @property
    def frame(self) -> LazyFrame:
        from ..dataframe import _Query

        return LazyFrame(
            self.db,
            _Query(f"(\n{indent(self._sql)}\n) AS {quote_ident('returns')}"),
        )

    def __repr__(self) -> str:
        return (
            f"ReturnSeries(annualization={self.annualization:g}, "
            f"digest={self.provenance.digest[:12]})"
        )

    # -- the enriched series every statistic reads from ---------------------

    def _base_ctes(self, benchmark: Optional["ReturnSeries"] = None) -> list:
        """Cumulative value, running peak, drawdown and log returns.

        The running peak starts at the initial capital rather than at the
        first bar's value, which is what makes a drawdown that begins on bar
        one count: empyrical prepends the starting value before taking the
        cumulative maximum.
        """
        ctes = [("_r0", self._sql)]
        if benchmark is not None:
            ctes.append(("_b0", benchmark._sql))
            ctes.append(
                (
                    "_o",
                    f"SELECT r.{quote_ident(TS)}, r.{quote_ident(RET)}, "
                    f"b.{quote_ident(RET)} AS _bench\n"
                    f"FROM _r0 r JOIN _b0 b "
                    f"ON r.{quote_ident(TS)} = b.{quote_ident(TS)}",
                )
            )
        else:
            ctes.append(("_o", "SELECT * FROM _r0"))
        ctes.append(
            (
                "_n",
                f"SELECT *, row_number() OVER (ORDER BY {quote_ident(TS)}) AS _i,\n"
                f"       ln(1 + {quote_ident(RET)}) AS _lr\n"
                f"FROM _o",
            )
        )
        ctes.append(
            (
                "_c",
                f"SELECT *, exp(sum(_lr) OVER (ORDER BY {quote_ident(TS)})) AS _cum,\n"
                f"       sum(_lr) OVER (ORDER BY {quote_ident(TS)}) AS _clr\n"
                f"FROM _n",
            )
        )
        ctes.append(
            (
                "_d",
                f"SELECT *, greatest(1.0, max(_cum) OVER "
                f"(ORDER BY {quote_ident(TS)})) AS _peak\n"
                f"FROM _c",
            )
        )
        ctes.append(
            (
                "_u",
                "SELECT *, (_cum - _peak) / _peak AS _dd,\n"
                f"       avg({quote_ident(RET)}) OVER () AS _mu\n"
                "FROM _d",
            )
        )
        return ctes

    # -- headline statistics ------------------------------------------------

    def stats(self, benchmark: Optional["ReturnSeries"] = None) -> dict:
        """The headline performance statistics, as one row of SQL.

        Matches ``empyrical`` (and therefore pyfolio's ``perf_stats``) on
        every value. With a ``benchmark`` series, alpha and beta are added;
        the two series are joined on their timestamps, so only overlapping
        bars contribute.
        """
        ann = sql_number(float(self.annualization))
        ctes = self._base_ctes(benchmark)
        pieces = [
            "count(*) AS n_periods",
            f"min({quote_ident(TS)}) AS period_start",
            f"max({quote_ident(TS)}) AS period_end",
            "exp(sum(_lr)) - 1 AS cumulative_return",
            f"power(exp(sum(_lr)), {ann} / count(*)) - 1 AS annual_return",
            f"stddev({quote_ident(RET)}) * sqrt({ann}) AS annual_volatility",
            f"avg({quote_ident(RET)}) / NULLIF(stddev({quote_ident(RET)}), 0) "
            f"* sqrt({ann}) AS sharpe_ratio",
            # Sortino's denominator uses the mean square of the *clipped*
            # series over every bar, not only the losing ones.
            f"(avg({quote_ident(RET)}) * {ann}) / NULLIF(sqrt(avg(power("
            f"least({quote_ident(RET)}, 0), 2))) * sqrt({ann}), 0) "
            f"AS sortino_ratio",
            f"sqrt(avg(power(least({quote_ident(RET)}, 0), 2))) * sqrt({ann}) "
            f"AS downside_risk",
            "min(_dd) AS max_drawdown",
            f"CASE WHEN min(_dd) < 0 THEN (power(exp(sum(_lr)), {ann} / count(*)) - 1)"
            f" / abs(min(_dd)) END AS calmar_ratio",
            # An all-losing series has no positive part; empyrical sums an
            # empty list to 0.0, so the numerator must not collapse to NULL.
            f"coalesce(sum(CASE WHEN {quote_ident(RET)} > 0 THEN "
            f"{quote_ident(RET)} END), 0) / "
            f"NULLIF(-coalesce(sum(CASE WHEN {quote_ident(RET)} < 0 THEN "
            f"{quote_ident(RET)} END), 0), 0) AS omega_ratio",
            "power(corr(_clr, _i), 2) AS stability",
            f"avg(power({quote_ident(RET)} - _mu, 3)) / "
            f"NULLIF(power(avg(power({quote_ident(RET)} - _mu, 2)), 1.5), 0) AS skew",
            f"avg(power({quote_ident(RET)} - _mu, 4)) / "
            f"NULLIF(power(avg(power({quote_ident(RET)} - _mu, 2)), 2), 0) - 3 "
            f"AS kurtosis",
            f"avg({quote_ident(RET)}) - 2.0 * stddev({quote_ident(RET)}) "
            f"AS daily_value_at_risk",
        ]
        if benchmark is not None:
            pieces.append(f"regr_slope({quote_ident(RET)}, _bench) AS beta")
            # mean(r - beta*b) == mean(r) - beta*mean(b). Spelling it the
            # second way keeps every aggregate at the top level; an aggregate
            # nested inside another is not a valid expression.
            pieces.append(
                f"avg({quote_ident(RET)}) - regr_slope({quote_ident(RET)}, _bench) "
                f"* avg(_bench) AS _alpha_mean"
            )
        ctes.append(("_stats", "SELECT\n  " + ",\n  ".join(pieces) + "\nFROM _u"))
        ctes.extend(_percentile_ctes({"p95": 0.95, "p05": 0.05}))
        body = (
            "SELECT s.*,\n"
            "  abs(p.p95) / NULLIF(abs(p.p05), 0) AS tail_ratio\n"
            "FROM _stats s CROSS JOIN _pctl p"
        )
        row = self._run(_with(ctes, body)).to_arrow().to_pylist()[0]
        out = dict(row)
        if benchmark is not None:
            mean_alpha = out.pop("_alpha_mean", None)
            out["alpha"] = (
                (1.0 + mean_alpha) ** self.annualization - 1.0
                if mean_alpha is not None
                else None
            )
        return out

    # -- drawdowns ----------------------------------------------------------

    def underwater(self):
        """The drawdown series: how far below the running peak, per bar."""
        ctes = self._base_ctes()
        body = (
            f"SELECT {quote_ident(TS)}, _cum AS value, _peak AS peak, "
            f"_dd AS drawdown\nFROM _u ORDER BY {quote_ident(TS)}"
        )
        return self._run(_with(ctes, body))

    def equity_curve(self):
        """Cumulative return per bar, compounded from the returns series."""
        ctes = self._base_ctes()
        body = (
            f"SELECT {quote_ident(TS)}, {quote_ident(RET)} AS period_return,\n"
            f"       _cum - 1 AS cumulative_return\n"
            f"FROM _u ORDER BY {quote_ident(TS)}"
        )
        return self._run(_with(ctes, body))

    def drawdown_table(self, top: int = 10) -> list:
        """The worst ``top`` non-overlapping drawdown episodes.

        Peak, valley and recovery dates plus the net drawdown, matching
        pyfolio's ``gen_drawdown_table``. Episode segmentation is a
        sequential scan over the (small) underwater series, so it happens
        here rather than in SQL; the series itself is computed in the engine.
        """
        if top < 1:
            raise ValueError("top must be >= 1")
        rows = self.underwater().to_arrow().to_pylist()
        if not rows:
            return []

        episodes = []
        remaining = [(i, r) for i, r in enumerate(rows)]
        # pyfolio removes each worst episode and re-searches what is left, so
        # the episodes it reports never overlap.
        alive = [True] * len(rows)
        for _ in range(top):
            worst = None
            for i, row in enumerate(rows):
                if not alive[i] or row["drawdown"] is None:
                    continue
                if worst is None or row["drawdown"] < rows[worst]["drawdown"]:
                    worst = i
            if worst is None or rows[worst]["drawdown"] >= 0:
                break
            # Walk back to the peak that started it, forward to recovery.
            start = worst
            while start > 0 and alive[start - 1] and rows[start - 1]["drawdown"] < 0:
                start -= 1
            peak_index = start - 1 if start > 0 else start
            end = worst
            recovery = None
            while end < len(rows) - 1:
                end += 1
                if not alive[end]:
                    break
                if rows[end]["drawdown"] >= 0:
                    recovery = rows[end]["ts"]
                    break
            episodes.append(
                {
                    "net_drawdown": -rows[worst]["drawdown"],
                    "peak_date": rows[peak_index]["ts"],
                    "valley_date": rows[worst]["ts"],
                    "recovery_date": recovery,
                    "duration": (end - peak_index + 1) if recovery else None,
                }
            )
            for i in range(peak_index, end + 1):
                alive[i] = False
            if not any(alive):
                break
        return episodes

    # -- rolling statistics --------------------------------------------------

    def rolling_volatility(self, window: int = 63):
        ann = sql_number(float(self.annualization))
        frame = _frame(window)
        ctes = [("_r", self._sql)]
        body = (
            f"SELECT {quote_ident(TS)},\n"
            f"       {_full_window(window, frame)}\n"
            f"       stddev({quote_ident(RET)}) {frame} * sqrt({ann}) END "
            f"AS rolling_volatility\n"
            f"FROM _r ORDER BY {quote_ident(TS)}"
        )
        return self._run(_with(ctes, body))

    def rolling_sharpe(self, window: int = 63):
        ann = sql_number(float(self.annualization))
        frame = _frame(window)
        ctes = [("_r", self._sql)]
        body = (
            f"SELECT {quote_ident(TS)},\n"
            f"       {_full_window(window, frame)}\n"
            f"       avg({quote_ident(RET)}) {frame} / "
            f"NULLIF(stddev({quote_ident(RET)}) {frame}, 0) * sqrt({ann}) END "
            f"AS rolling_sharpe\n"
            f"FROM _r ORDER BY {quote_ident(TS)}"
        )
        return self._run(_with(ctes, body))

    def rolling_beta(self, benchmark: "ReturnSeries", window: int = 126):
        """Rolling beta to a benchmark.

        Uses ``ts_cov`` and a rolling variance rather than ``covar_samp``
        over a sliding frame: DataFusion cannot retract from the built-in
        covariance aggregate, which is exactly why the engine ships
        ``ts_cov``.
        """
        frame = _frame(window)
        ctes = [
            ("_r", self._sql),
            ("_b", benchmark._sql),
            (
                "_j",
                f"SELECT r.{quote_ident(TS)}, r.{quote_ident(RET)} AS _r, "
                f"b.{quote_ident(RET)} AS _b\n"
                f"FROM _r r JOIN _b b ON r.{quote_ident(TS)} = b.{quote_ident(TS)}",
            ),
        ]
        body = (
            f"SELECT {quote_ident(TS)},\n"
            f"       CASE WHEN count(_r) {frame} < {window} THEN NULL ELSE\n"
            f"       ts_cov(_r, _b) {frame} / NULLIF(var_samp(_b) {frame}, 0) END "
            f"AS rolling_beta\n"
            f"FROM _j ORDER BY {quote_ident(TS)}"
        )
        return self._run(_with(ctes, body))


def _frame(window: int) -> str:
    if not isinstance(window, int) or isinstance(window, bool) or window < 2:
        raise ValueError(f"window must be an integer >= 2, got {window!r}")
    return (
        f"OVER (ORDER BY {quote_ident(TS)} ROWS BETWEEN {window - 1} "
        f"PRECEDING AND CURRENT ROW)"
    )


def _full_window(window: int, frame: str) -> str:
    """Suppress a rolling statistic until its window is actually full.

    A SQL frame happily returns a value on row two; a "63-bar Sharpe"
    computed from two observations is not one, and plotting it puts a
    meaningless spike at the left edge of every chart. pandas defaults to
    ``min_periods=window`` for the same reason, so this also keeps the two
    comparable.
    """
    return f"CASE WHEN count({quote_ident(RET)}) {frame} < {window} THEN NULL ELSE"


def _percentile_ctes(fractions: dict) -> list:
    """Exact linear-interpolation percentiles, numpy's default method.

    DataFusion's ``percentile_cont`` is approximate (it disagrees with numpy
    around the eighth significant digit), which is invisible in a chart and
    unacceptable in a statistic we claim matches a reference implementation.
    This computes the interpolation directly off the sorted rank: for a
    fraction ``p``, ``h = (n - 1) * p`` and the value is
    ``x[floor(h)] + (h - floor(h)) * (x[floor(h) + 1] - x[floor(h)])``.
    """
    r = quote_ident(RET)
    ord_cte = (
        "_ord",
        f"SELECT {r}, row_number() OVER (ORDER BY {r}) AS _rn,\n"
        f"       count(*) OVER () AS _n\nFROM _u",
    )
    parts = []
    for name, p in fractions.items():
        frac = sql_number(float(p))
        h = f"(_n - 1) * {frac}"
        lo_rank = f"floor({h}) + 1"
        parts.append(
            f"  max(CASE WHEN _rn = {lo_rank} THEN {r} END) AS {quote_ident(name + '_lo')}"
        )
        parts.append(
            f"  max(CASE WHEN _rn = {lo_rank} + 1 THEN {r} END) AS "
            f"{quote_ident(name + '_hi')}"
        )
        parts.append(f"  max({h} - floor({h})) AS {quote_ident(name + '_f')}")
    raw = ("_pctl_raw", "SELECT\n" + ",\n".join(parts) + "\nFROM _ord")
    finals = []
    for name in fractions:
        lo = quote_ident(name + "_lo")
        hi = quote_ident(name + "_hi")
        f = quote_ident(name + "_f")
        finals.append(
            f"  {lo} + {f} * (coalesce({hi}, {lo}) - {lo}) AS {quote_ident(name)}"
        )
    final = ("_pctl", "SELECT\n" + ",\n".join(finals) + "\nFROM _pctl_raw")
    return [ord_cte, raw, final]


def _with(ctes, body: str) -> str:
    parts = [f"{quote_ident(name)} AS (\n{indent(sql)}\n)" for name, sql in ctes]
    return "WITH " + ",\n".join(parts) + "\n" + body
