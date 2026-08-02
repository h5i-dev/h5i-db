"""Factor evaluation, computed in the engine (ROADMAP_QUANT.md Q1).

The shape follows ``alphalens``: build a panel of factor values joined to
forward returns and quantile buckets, then read statistics off it. The
difference is where the work happens. A panel is a *pinned query*, not a
materialized pandas frame, so every statistic below compiles to SQL and runs
inside h5i-db, and every result carries the data version it was computed
against.

    panel = build_panel(db, "signals", "prices", periods=(1, 5, 10))
    panel.ic().to_pandas()
    panel.quantile_returns()

Divergences from alphalens are deliberate, enumerated, and tested; see the
divergence table in ``docs-src/manual/quant.md``.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Mapping, Optional, Sequence, Union

from ..dataframe import LazyFrame, quote_ident, quote_string
from ._common import Pin, Provenance, indent, resolve_source, sql_number

__all__ = ["build_panel", "FactorPanel", "MaxLossExceededError"]


class MaxLossExceededError(Exception):
    """Too many factor rows were dropped building the panel.

    Mirrors ``alphalens.utils.MaxLossExceededError``: the panel is refused
    rather than returned quietly, because a factor that loses most of its
    rows produces statistics that look fine and mean nothing.
    """


# The panel's own column names. Forward returns are named by bar count
# (``fwd_5`` = five bars ahead), not by a pandas frequency string.
TS = "ts"
ASSET = "asset"
FACTOR = "factor"
GROUP = "group"
QUANTILE = "factor_quantile"


def _fwd(period: int) -> str:
    return f"fwd_{period}"


def _finite(expr: str) -> str:
    """SQL predicate for ``np.isfinite`` (alphalens filters the factor this way)."""
    return (
        f"({expr} IS NOT NULL AND {expr} = {expr} "
        f"AND abs({expr}) != CAST('inf' AS DOUBLE))"
    )


def _partition(keys: Sequence[str]) -> str:
    return ", ".join(quote_ident(k) for k in keys)


def build_panel(
    db: Any,
    factor: Union[str, LazyFrame],
    prices: Union[str, LazyFrame],
    *,
    periods: Sequence[int] = (1, 5, 10),
    quantiles: Optional[int] = 5,
    bins: Optional[int] = None,
    group: Union[str, Mapping[str, str], None] = None,
    binning_by_group: bool = False,
    zero_aware: bool = False,
    filter_zscore: Optional[float] = 20.0,
    max_loss: float = 0.35,
    cumulative_returns: bool = True,
    ts: str = TS,
    asset: str = ASSET,
    factor_column: str = FACTOR,
    price_column: str = "price",
    pin: Optional[Pin] = None,
    version: Optional[int] = None,
    as_of: Optional[str] = None,
    snapshot: Optional[str] = None,
    event_time_cutoff: Optional[Any] = None,
    deterministic: bool = True,
) -> "FactorPanel":
    """Join a factor to forward returns and quantile buckets.

    ``factor`` and ``prices`` are table names (read through the pin) or
    :class:`~h5i_db.LazyFrame` objects (taken as given, so columns can be
    renamed or derived first). Both are long format: ``(ts, asset, value)``.

    ``periods`` are forward-return horizons **in bars**, and the resulting
    columns are ``fwd_<n>``. With ``cumulative_returns`` (the default) the
    horizon return is ``price[t+n]/price[t] - 1``; without it, it is the
    single-bar return ``n`` bars ahead.

    Exactly one of ``quantiles`` (equal count) and ``bins`` (equal width)
    applies. ``group`` is a column already on the factor source or a mapping
    ``{asset: group}``.

    ``max_loss`` is the fraction of factor rows that may be dropped before
    the panel is refused; :meth:`FactorPanel.loss_report` breaks the loss
    down by phase.
    """
    read_pin = Pin.coerce(
        pin,
        version=version,
        as_of=as_of,
        snapshot=snapshot,
        event_time_cutoff=event_time_cutoff,
    )
    periods = _check_periods(periods)
    quantiles, bins = _check_binning(quantiles, bins, zero_aware)
    if not 0.0 <= max_loss <= 1.0:
        raise ValueError(f"max_loss must be in [0, 1], got {max_loss}")

    factor_sql, factor_src = resolve_source(db, factor, read_pin, ts, "factor")
    price_sql, price_src = resolve_source(db, prices, read_pin, ts, "prices")

    group_column: Optional[str] = None
    group_map: Optional[Mapping[str, str]] = None
    if isinstance(group, str):
        group_column = group
    elif isinstance(group, Mapping):
        group_map = dict(group)
    elif group is not None:
        raise TypeError("group must be a column name or a {asset: group} mapping")
    if binning_by_group and group is None:
        raise ValueError("binning_by_group=True needs a group")

    sql = _panel_sql(
        factor_sql=factor_sql,
        price_sql=price_sql,
        periods=periods,
        quantiles=quantiles,
        bins=bins,
        group_column=group_column,
        group_map=group_map,
        binning_by_group=binning_by_group,
        zero_aware=zero_aware,
        filter_zscore=filter_zscore,
        cumulative_returns=cumulative_returns,
        ts=ts,
        asset=asset,
        factor_column=factor_column,
        price_column=price_column,
    )
    counts_sql = _loss_sql(sql["stages"])

    provenance = Provenance(
        kind="factor_panel",
        pin=read_pin,
        parameters={
            "periods": list(periods),
            "quantiles": quantiles,
            "bins": bins,
            "group": group_column
            if group_column
            else ("mapping" if group_map else None),
            "binning_by_group": binning_by_group,
            "zero_aware": zero_aware,
            "filter_zscore": filter_zscore,
            "max_loss": max_loss,
            "cumulative_returns": cumulative_returns,
            "deterministic": deterministic,
        },
        sources={"factor": factor_src, "prices": price_src},
        sql={"panel": sql["panel"]},
    )

    panel = FactorPanel(
        db=db,
        _sql=sql["panel"],
        _counts_sql=counts_sql,
        periods=tuple(periods),
        has_group=group is not None,
        max_loss=max_loss,
        provenance=provenance,
        deterministic=deterministic,
    )
    panel._check_loss()
    return panel


def _check_periods(periods: Sequence[int]) -> tuple:
    if isinstance(periods, int):
        periods = (periods,)
    out = []
    for p in periods:
        if isinstance(p, bool) or not isinstance(p, int):
            raise TypeError("periods must be integers (bar counts)")
        if p < 1:
            raise ValueError(f"periods must be >= 1, got {p}")
        out.append(p)
    if not out:
        raise ValueError("periods must not be empty")
    if len(set(out)) != len(out):
        raise ValueError(f"duplicate periods: {out}")
    return tuple(sorted(out))


def _check_binning(quantiles, bins, zero_aware) -> tuple:
    if (quantiles is None) == (bins is None):
        raise ValueError("pass exactly one of quantiles= or bins=")
    n = quantiles if quantiles is not None else bins
    if isinstance(n, bool) or not isinstance(n, int):
        raise TypeError("quantiles/bins must be an integer count")
    if n < 2:
        raise ValueError("quantiles/bins must be at least 2")
    if zero_aware and n % 2 != 0:
        raise ValueError("zero_aware needs an even number of quantiles/bins")
    return quantiles, bins


def _panel_sql(
    *,
    factor_sql: str,
    price_sql: str,
    periods: Sequence[int],
    quantiles: Optional[int],
    bins: Optional[int],
    group_column: Optional[str],
    group_map: Optional[Mapping[str, str]],
    binning_by_group: bool,
    zero_aware: bool,
    filter_zscore: Optional[float],
    cumulative_returns: bool,
    ts: str,
    asset: str,
    factor_column: str,
    price_column: str,
) -> dict:
    """Compile the panel pipeline.

    The stage order reproduces alphalens exactly, which matters because the
    z-score filter's statistics depend on it: they are per asset, over the
    price grid already restricted to factor dates, and computed *before* the
    join back to factor rows.
    """
    q_ts, q_asset = quote_ident(ts), quote_ident(asset)
    q_factor, q_price = quote_ident(factor_column), quote_ident(price_column)
    has_group = group_column is not None or group_map is not None

    ctes = []
    ctes.append(("_factor_src", factor_sql))
    ctes.append(("_price_src", price_sql))

    # Prices restricted to the factor's assets (alphalens: prices.filter(items=...)).
    ctes.append(
        (
            "_px",
            f"SELECT {q_ts} AS {quote_ident(TS)}, {q_asset} AS {quote_ident(ASSET)},\n"
            f"       CAST({q_price} AS DOUBLE) AS price\n"
            f"FROM _price_src\n"
            f"WHERE {q_asset} IN (SELECT DISTINCT {q_asset} FROM _factor_src)",
        )
    )

    # Forward returns on the price grid, per asset in time order.
    fwd_exprs = []
    for p in periods:
        window = f"OVER (PARTITION BY {quote_ident(ASSET)} ORDER BY {quote_ident(TS)})"
        if cumulative_returns:
            expr = f"lead(price, {p}) {window} / price - 1"
        elif p == 1:
            expr = f"lead(price, 1) {window} / price - 1"
        else:
            expr = (
                f"lead(price, {p}) {window} / lead(price, {p - 1}) {window} - 1"
            )
        fwd_exprs.append(f"       {expr} AS {quote_ident(_fwd(p))}")
    ctes.append(
        (
            "_fwd_raw",
            f"SELECT {quote_ident(TS)}, {quote_ident(ASSET)},\n"
            + ",\n".join(fwd_exprs)
            + "\nFROM _px",
        )
    )

    # Restrict to the factor's dates before the z-score statistics, because
    # alphalens computes them on exactly this grid.
    ctes.append(
        (
            "_fwd_dates",
            f"SELECT * FROM _fwd_raw\n"
            f"WHERE {quote_ident(TS)} IN (SELECT DISTINCT {q_ts} FROM _factor_src)",
        )
    )

    if filter_zscore is None:
        ctes.append(("_fwd", "SELECT * FROM _fwd_dates"))
    else:
        z = sql_number(float(filter_zscore))
        cols = []
        for p in periods:
            c = quote_ident(_fwd(p))
            part = f"OVER (PARTITION BY {quote_ident(ASSET)})"
            cols.append(
                f"       CASE WHEN abs({c} - avg({c}) {part}) > "
                f"{z} * stddev({c}) {part} THEN NULL ELSE {c} END AS {c}"
            )
        ctes.append(
            (
                "_fwd",
                f"SELECT {quote_ident(TS)}, {quote_ident(ASSET)},\n"
                + ",\n".join(cols)
                + "\nFROM _fwd_dates",
            )
        )

    # Factor rows, with the group resolved.
    if group_map is not None:
        values = ",\n         ".join(
            f"({quote_string(str(k))}, {quote_string(str(v))})"
            for k, v in sorted(group_map.items())
        )
        ctes.append(
            (
                "_group_map",
                f"SELECT * FROM (VALUES\n         {values}\n"
                f"       ) AS t({quote_ident(ASSET)}, {quote_ident(GROUP)})",
            )
        )
        group_select = f",\n       g.{quote_ident(GROUP)} AS {quote_ident(GROUP)}"
        group_join = (
            f"\nLEFT JOIN _group_map g ON f.{q_asset} = g.{quote_ident(ASSET)}"
        )
    elif group_column is not None:
        group_select = (
            f",\n       CAST(f.{quote_ident(group_column)} AS VARCHAR) "
            f"AS {quote_ident(GROUP)}"
        )
        group_join = ""
    else:
        group_select = ""
        group_join = ""

    ctes.append(
        (
            "_factor_rows",
            f"SELECT f.{q_ts} AS {quote_ident(TS)}, "
            f"f.{q_asset} AS {quote_ident(ASSET)},\n"
            f"       CAST(f.{q_factor} AS DOUBLE) AS {quote_ident(FACTOR)}"
            f"{group_select}\n"
            f"FROM _factor_src f{group_join}",
        )
    )

    fwd_cols = [quote_ident(_fwd(p)) for p in periods]
    joined_cols = ",\n       ".join(f"w.{c} AS {c}" for c in fwd_cols)
    group_col_sql = f",\n       f.{quote_ident(GROUP)}" if has_group else ""
    ctes.append(
        (
            "_joined",
            f"SELECT f.{quote_ident(TS)}, f.{quote_ident(ASSET)}, "
            f"f.{quote_ident(FACTOR)}{group_col_sql},\n       {joined_cols}\n"
            f"FROM _factor_rows f\n"
            f"JOIN _fwd w ON f.{quote_ident(TS)} = w.{quote_ident(TS)} "
            f"AND f.{quote_ident(ASSET)} = w.{quote_ident(ASSET)}",
        )
    )

    # alphalens drops any row with a non-finite factor or a missing forward
    # return, and counts what is left as the forward-return phase survivors.
    conditions = [_finite(quote_ident(FACTOR))] + [
        f"{c} IS NOT NULL" for c in fwd_cols
    ]
    if has_group:
        conditions.append(f"{quote_ident(GROUP)} IS NOT NULL")
    ctes.append(
        (
            "_clean",
            "SELECT * FROM _joined\nWHERE " + "\n  AND ".join(conditions),
        )
    )

    bucket_keys = [TS] + ([GROUP] if binning_by_group else [])
    ctes.append(
        (
            "_binned",
            "SELECT *,\n"
            f"       {_bucket_expr(quantiles, bins, zero_aware, bucket_keys)} "
            f"AS {quote_ident(QUANTILE)}\n"
            "FROM _clean",
        )
    )

    panel_body = (
        f"SELECT * FROM _binned WHERE {quote_ident(QUANTILE)} IS NOT NULL"
    )
    ctes.append(("_panel", panel_body))

    panel_sql = _with(ctes, "SELECT * FROM _panel")
    return {
        "panel": panel_sql,
        "stages": ctes,
    }


def _bucket_expr(
    quantiles: Optional[int],
    bins: Optional[int],
    zero_aware: bool,
    keys: Sequence[str],
) -> str:
    """Quantile (equal count) or bin (equal width) assignment, 1-based."""
    factor = quote_ident(FACTOR)
    part = _partition(keys)
    if quantiles is not None:
        if not zero_aware:
            return f"ntile({quantiles}) OVER (PARTITION BY {part} ORDER BY {factor})"
        half = quantiles // 2
        sign_part = f"{part}, ({factor} >= 0)"
        return (
            f"ntile({half}) OVER (PARTITION BY {sign_part} ORDER BY {factor})"
            f" + CASE WHEN {factor} >= 0 THEN {half} ELSE 0 END"
        )
    # Equal width. pandas' cut() is right-closed with the lowest edge nudged
    # down, so the minimum lands in bucket 1: ceil((x - min) / width), floored
    # at 1 and capped at the bin count.
    if not zero_aware:
        lo = f"min({factor}) OVER (PARTITION BY {part})"
        hi = f"max({factor}) OVER (PARTITION BY {part})"
        width = f"(({hi}) - ({lo})) / {bins}"
        idx = f"ceil(({factor} - ({lo})) / NULLIF({width}, 0))"
        return f"CAST(least({bins}, greatest(1, {idx})) AS BIGINT)"
    half = bins // 2
    sign_part = f"{part}, ({factor} >= 0)"
    lo = f"min({factor}) OVER (PARTITION BY {sign_part})"
    hi = f"max({factor}) OVER (PARTITION BY {sign_part})"
    width = f"(({hi}) - ({lo})) / {half}"
    idx = f"ceil(({factor} - ({lo})) / NULLIF({width}, 0))"
    return (
        f"CAST(least({half}, greatest(1, {idx})) "
        f"+ CASE WHEN {factor} >= 0 THEN {half} ELSE 0 END AS BIGINT)"
    )


def _with(ctes: Sequence, body: str) -> str:
    parts = []
    for name, sql in ctes:
        parts.append(f"{quote_ident(name)} AS (\n{indent(sql)}\n)")
    return "WITH " + ",\n".join(parts) + "\n" + body


def _loss_sql(stages: Sequence) -> str:
    """Row counts for each phase alphalens attributes losses to."""
    body = (
        "SELECT\n"
        "  (SELECT count(*) FROM _factor_src) AS initial,\n"
        "  (SELECT count(*) FROM _clean) AS after_forward_returns,\n"
        "  (SELECT count(*) FROM _panel) AS after_binning"
    )
    return _with(stages, body)


@dataclass
class FactorPanel:
    """A pinned factor panel: the query, not the rows.

    Every method compiles to SQL and returns a
    :class:`~h5i_db.QueryResult`, so the heavy work stays in the engine and
    only the small aggregate comes back.
    """

    db: Any
    _sql: str
    _counts_sql: str
    periods: tuple
    has_group: bool
    max_loss: float
    provenance: Provenance
    deterministic: bool = True
    _counts: Optional[dict] = None

    def _run(self, sql: str, **limits: Any):
        """Execute one of the panel's queries.

        With ``deterministic`` (the default) execution is single-partition,
        which fixes the order partial aggregates are combined in. Floating
        point addition is not associative, so a parallel plan is free to
        return a slightly different sum every run; that is invisible in a
        chart and fatal to a number you intend to cite. The cost is
        parallelism inside a single query, not across runs, so a sweep of
        forks still uses every core.
        """
        if self.deterministic:
            limits.setdefault("target_partitions", 1)
        return self.db.sql(sql, **limits)

    # -- introspection -----------------------------------------------------

    def sql(self) -> str:
        """The compiled panel query."""
        return self._sql

    @property
    def frame(self) -> LazyFrame:
        """The panel as a :class:`~h5i_db.LazyFrame`, for further composition."""
        from ..dataframe import _Query

        query = _Query(f"(\n{indent(self._sql)}\n) AS {quote_ident('panel')}")
        return LazyFrame(self.db, query)

    def collect(self, **limits: Any):
        """Run the panel query and return the rows."""
        return self._run(self._sql, **limits)

    def __repr__(self) -> str:
        return (
            f"FactorPanel(periods={list(self.periods)}, "
            f"group={self.has_group}, digest={self.provenance.digest[:12]})"
        )

    # -- loss accounting ---------------------------------------------------

    def loss_report(self) -> dict:
        """How many factor rows survived each phase.

        Mirrors alphalens' printed accounting: ``forward_returns`` is the
        fraction lost joining to forward returns (missing prices, non-finite
        factor values, the z-score filter), ``binning`` the fraction lost
        assigning quantiles.
        """
        if self._counts is None:
            row = self._run(self._counts_sql).to_arrow().to_pylist()[0]
            initial = float(row["initial"])
            after_fwd = float(row["after_forward_returns"])
            after_bin = float(row["after_binning"])
            total = (initial - after_bin) / initial if initial else 0.0
            fwd = (initial - after_fwd) / initial if initial else 0.0
            self._counts = {
                "initial": int(row["initial"]),
                "after_forward_returns": int(row["after_forward_returns"]),
                "after_binning": int(row["after_binning"]),
                "forward_returns": fwd,
                "binning": total - fwd,
                "total": total,
                "max_loss": self.max_loss,
            }
        return dict(self._counts)

    def _check_loss(self) -> None:
        report = self.loss_report()
        if report["total"] > self.max_loss:
            raise MaxLossExceededError(
                f"max_loss ({self.max_loss:.1%}) exceeded "
                f"{report['total']:.1%}: {report['after_binning']} of "
                f"{report['initial']} factor rows survived "
                f"({report['forward_returns']:.1%} lost computing forward "
                f"returns, {report['binning']:.1%} lost binning). Consider "
                "raising max_loss, widening the price history, or checking "
                "that the factor and price calendars line up."
            )

    # -- information coefficient -------------------------------------------

    def ic(self, group_adjust: bool = False, by_group: bool = False):
        """Per-date Spearman rank correlation of factor to forward returns.

        One column per horizon (``ic_<n>``). Ranks use pandas' tie rule
        (``cs_rank``), so this matches ``scipy.stats.spearmanr`` rather than
        SQL's ``percent_rank``.
        """
        return self._run(self._ic_sql(group_adjust, by_group))

    def _ic_sql(self, group_adjust: bool, by_group: bool) -> str:
        self._require_group(group_adjust or by_group)
        keys = [TS] + ([GROUP] if by_group else [])
        part = _partition(keys)
        source = self._demeaned_cte("_src", group_adjust)
        ranked = [
            f"       cs_rank({quote_ident(FACTOR)}) OVER (PARTITION BY {part}) "
            f"AS _rank_factor"
        ]
        for p in self.periods:
            c = quote_ident(_fwd(p))
            ranked.append(
                f"       cs_rank({c}) OVER (PARTITION BY {part}) AS _rank_{p}"
            )
        select_keys = ", ".join(quote_ident(k) for k in keys)
        ctes = source + [
            (
                "_ranked",
                f"SELECT {select_keys},\n" + ",\n".join(ranked) + "\nFROM _src",
            )
        ]
        aggs = ",\n".join(
            f"       corr(_rank_factor, _rank_{p}) AS {quote_ident(f'ic_{p}')}"
            for p in self.periods
        )
        body = (
            f"SELECT {select_keys},\n{aggs}\n"
            f"FROM _ranked GROUP BY {select_keys} ORDER BY {select_keys}"
        )
        return _with(ctes, body)

    def mean_ic(
        self,
        by: Optional[str] = None,
        group_adjust: bool = False,
        by_group: bool = False,
    ):
        """Mean IC, optionally resampled (``by='1mo'``) and/or per group."""
        ic_sql = self._ic_sql(group_adjust, by_group)
        keys = []
        if by is not None:
            keys.append("bucket")
        if by_group:
            keys.append(GROUP)
        aggs = ",\n".join(
            f"       avg({quote_ident(f'ic_{p}')}) AS {quote_ident(f'ic_{p}')}"
            for p in self.periods
        )
        if not keys:
            body = f"SELECT\n{aggs}\nFROM _ic"
            return self._run(_with([("_ic", ic_sql)], body))
        select = []
        if by is not None:
            select.append(
                f"time_bucket({quote_string(by)}, {quote_ident(TS)}) AS bucket"
            )
        if by_group:
            select.append(quote_ident(GROUP))
        key_sql = ", ".join(quote_ident(k) for k in keys)
        body = (
            f"SELECT {', '.join(select)},\n{aggs}\n"
            f"FROM _ic GROUP BY {key_sql} ORDER BY {key_sql}"
        )
        return self._run(_with([("_ic", ic_sql)], body))

    def ic_decay(self, group_adjust: bool = False):
        """IC as a function of horizon: one query, one row per horizon.

        Returns mean IC, its standard deviation, the information ratio
        (``ICIR``), and the t-statistic of the mean, per horizon.
        """
        ic_sql = self._ic_sql(group_adjust, by_group=False)
        parts = []
        for p in self.periods:
            c = quote_ident(f"ic_{p}")
            parts.append(
                "SELECT "
                f"{p} AS period, avg({c}) AS mean_ic, stddev({c}) AS std_ic, "
                f"count({c}) AS n, "
                f"avg({c}) / NULLIF(stddev({c}), 0) AS icir, "
                f"avg({c}) / NULLIF(stddev({c}) / sqrt(count({c})), 0) AS t_stat "
                "FROM _ic"
            )
        body = "\nUNION ALL\n".join(parts) + "\nORDER BY period"
        return self._run(_with([("_ic", ic_sql)], body))

    # -- returns by quantile -----------------------------------------------

    def quantile_returns(
        self,
        by_date: bool = False,
        by_group: bool = False,
        demeaned: bool = True,
        group_adjust: bool = False,
    ):
        """Mean forward return per quantile bucket, with standard errors.

        With ``by_date=False`` the mean is taken over dates of the per-date
        bucket means (equal weight per date), matching alphalens; the
        standard error is the cross-date standard deviation over
        ``sqrt(#dates)``.
        """
        return self._run(
            self._quantile_returns_sql(by_date, by_group, demeaned, group_adjust)
        )

    def _quantile_returns_sql(
        self, by_date: bool, by_group: bool, demeaned: bool, group_adjust: bool
    ) -> str:
        self._require_group(group_adjust or by_group)
        ctes = self._demeaned_cte("_src", group_adjust, demeaned)
        stage1_keys = [QUANTILE, TS] + ([GROUP] if by_group else [])
        keys_sql = ", ".join(quote_ident(k) for k in stage1_keys)
        aggs = []
        for p in self.periods:
            c = quote_ident(_fwd(p))
            aggs.append(f"       avg({c}) AS {quote_ident(f'mean_{p}')}")
            aggs.append(f"       stddev({c}) AS {quote_ident(f'std_{p}')}")
            aggs.append(f"       count({c}) AS {quote_ident(f'count_{p}')}")
        ctes.append(
            (
                "_stage1",
                f"SELECT {keys_sql},\n" + ",\n".join(aggs) + f"\nFROM _src "
                f"GROUP BY {keys_sql}",
            )
        )
        if by_date:
            cols = []
            for p in self.periods:
                m, s, n = (
                    quote_ident(f"mean_{p}"),
                    quote_ident(f"std_{p}"),
                    quote_ident(f"count_{p}"),
                )
                cols.append(f"       {m} AS {quote_ident(f'mean_{p}')}")
                cols.append(
                    f"       {s} / NULLIF(sqrt({n}), 0) AS "
                    f"{quote_ident(f'stderr_{p}')}"
                )
            body = (
                f"SELECT {keys_sql},\n" + ",\n".join(cols) + "\nFROM _stage1 "
                f"ORDER BY {keys_sql}"
            )
            return _with(ctes, body)
        stage2_keys = [QUANTILE] + ([GROUP] if by_group else [])
        keys2 = ", ".join(quote_ident(k) for k in stage2_keys)
        cols = []
        for p in self.periods:
            m = quote_ident(f"mean_{p}")
            cols.append(f"       avg({m}) AS {quote_ident(f'mean_{p}')}")
            cols.append(
                f"       stddev({m}) / NULLIF(sqrt(count({m})), 0) AS "
                f"{quote_ident(f'stderr_{p}')}"
            )
        body = (
            f"SELECT {keys2},\n" + ",\n".join(cols) + f"\nFROM _stage1 "
            f"GROUP BY {keys2} ORDER BY {keys2}"
        )
        return _with(ctes, body)

    def spread(
        self,
        upper: Optional[int] = None,
        lower: int = 1,
        by_date: bool = True,
        demeaned: bool = True,
        group_adjust: bool = False,
    ):
        """Top-minus-bottom quantile return spread, with a joint standard error."""
        if upper is None:
            upper = self.quantile_count()
        inner = self._quantile_returns_sql(
            by_date=by_date, by_group=False, demeaned=demeaned,
            group_adjust=group_adjust,
        )
        keys = [TS] if by_date else []
        key_sql = ", ".join(quote_ident(k) for k in keys)
        select_keys = (f"u.{quote_ident(TS)} AS {quote_ident(TS)}," if by_date else "")
        cols = []
        for p in self.periods:
            m = quote_ident(f"mean_{p}")
            se = quote_ident(f"stderr_{p}")
            cols.append(f"       u.{m} - l.{m} AS {quote_ident(f'spread_{p}')}")
            cols.append(
                f"       sqrt(power(u.{se}, 2) + power(l.{se}, 2)) AS "
                f"{quote_ident(f'stderr_{p}')}"
            )
        on = (
            f"u.{quote_ident(TS)} = l.{quote_ident(TS)}"
            if by_date
            else "1 = 1"
        )
        body = (
            f"SELECT {select_keys}\n" + ",\n".join(cols) + "\n"
            f"FROM (SELECT * FROM _q WHERE {quote_ident(QUANTILE)} = {upper}) u\n"
            f"JOIN (SELECT * FROM _q WHERE {quote_ident(QUANTILE)} = {lower}) l\n"
            f"  ON {on}"
        )
        if by_date:
            body += f"\nORDER BY u.{quote_ident(TS)}"
        return self._run(_with([("_q", inner)], body))

    # -- turnover and stability --------------------------------------------

    def turnover(self, period: int = 1):
        """Fraction of each quantile's names that were not in it ``period`` bars ago."""
        if period < 1:
            raise ValueError("period must be >= 1")
        # dense_rank over the panel's own rows gives each *date* its ordinal
        # directly, because equal timestamps share a rank. Ranking distinct
        # dates in a separate CTE and joining back reads the panel twice,
        # and a panel is the expensive part of this pipeline.
        ctes = [("_panel", self._sql)]
        ctes.append(
            (
                "_m",
                f"SELECT {quote_ident(TS)}, {quote_ident(ASSET)}, "
                f"{quote_ident(QUANTILE)},\n"
                f"       dense_rank() OVER (ORDER BY {quote_ident(TS)}) AS _r\n"
                f"FROM _panel",
            )
        )
        body = (
            f"SELECT c.{quote_ident(TS)}, c.{quote_ident(QUANTILE)},\n"
            f"       sum(CASE WHEN p.{quote_ident(ASSET)} IS NULL THEN 1.0 "
            f"ELSE 0.0 END) / count(*) AS turnover\n"
            f"FROM _m c\n"
            f"LEFT JOIN _m p ON c._r = p._r + {period}\n"
            f"  AND c.{quote_ident(QUANTILE)} = p.{quote_ident(QUANTILE)}\n"
            f"  AND c.{quote_ident(ASSET)} = p.{quote_ident(ASSET)}\n"
            f"WHERE c._r > {period}\n"
            f"GROUP BY c.{quote_ident(TS)}, c.{quote_ident(QUANTILE)}\n"
            f"ORDER BY c.{quote_ident(TS)}, c.{quote_ident(QUANTILE)}"
        )
        return self._run(_with(ctes, body))

    def rank_autocorrelation(self, period: int = 1):
        """Per-date correlation of factor ranks with their ranks ``period`` bars ago."""
        if period < 1:
            raise ValueError("period must be >= 1")
        # Same single-pass ranking as turnover(): equal timestamps share a
        # dense_rank, so the date ordinal falls out without a second read of
        # the panel.
        ctes = [("_panel", self._sql)]
        ctes.append(
            (
                "_ranked",
                f"SELECT {quote_ident(TS)}, {quote_ident(ASSET)},\n"
                f"       dense_rank() OVER (ORDER BY {quote_ident(TS)}) AS _r,\n"
                f"       cs_rank({quote_ident(FACTOR)}) OVER "
                f"(PARTITION BY {quote_ident(TS)}) AS _rank\n"
                f"FROM _panel",
            )
        )
        body = (
            f"SELECT c.{quote_ident(TS)}, corr(c._rank, p._rank) AS autocorrelation\n"
            f"FROM _ranked c\n"
            f"JOIN _ranked p ON c._r = p._r + {period} "
            f"AND c.{quote_ident(ASSET)} = p.{quote_ident(ASSET)}\n"
            f"GROUP BY c.{quote_ident(TS)} ORDER BY c.{quote_ident(TS)}"
        )
        return self._run(_with(ctes, body))

    # -- factor portfolio ---------------------------------------------------

    def weights(
        self,
        demeaned: bool = True,
        group_adjust: bool = False,
        equal_weight: bool = False,
    ):
        """Factor values as portfolio weights, gross leverage 1 per date."""
        return self._run(
            self._weights_sql(demeaned, group_adjust, equal_weight)
        )

    def _weights_sql(
        self, demeaned: bool, group_adjust: bool, equal_weight: bool
    ) -> str:
        self._require_group(group_adjust)
        keys = [TS] + ([GROUP] if group_adjust else [])
        part = _partition(keys)
        factor = quote_ident(FACTOR)
        ctes = [("_panel", self._sql)]
        carry = (
            f"{quote_ident(TS)}, {quote_ident(ASSET)}, {factor}"
            + (f", {quote_ident(GROUP)}" if self.has_group else "")
        )
        fwd_carry = "".join(
            f", {quote_ident(_fwd(p))}" for p in self.periods
        )

        if equal_weight:
            if demeaned:
                ctes.append(
                    (
                        "_median",
                        f"SELECT {part}, median({factor}) AS _med\n"
                        f"FROM _panel GROUP BY {part}",
                    )
                )
                on = " AND ".join(
                    f"p.{quote_ident(k)} = m.{quote_ident(k)}" for k in keys
                )
                centered = f"p.{factor} - m._med"
                ctes.append(
                    (
                        "_centered",
                        f"SELECT {', '.join('p.' + quote_ident(c) for c in [TS, ASSET, FACTOR] + ([GROUP] if self.has_group else []))}"
                        + "".join(
                            f", p.{quote_ident(_fwd(p_))}" for p_ in self.periods
                        )
                        + f",\n       {centered} AS _c\n"
                        f"FROM _panel p JOIN _median m ON {on}",
                    )
                )
            else:
                ctes.append(
                    (
                        "_centered",
                        f"SELECT {carry}{fwd_carry}, {factor} AS _c FROM _panel",
                    )
                )
            # sign(), then each side scaled by its own count so the two sides
            # carry equal gross exposure (alphalens' equal-weight rule).
            ctes.append(
                (
                    "_signed",
                    f"SELECT {carry}{fwd_carry},\n"
                    f"       CASE WHEN _c > 0 THEN 1.0 WHEN _c < 0 THEN -1.0 "
                    f"ELSE 0.0 END AS _s,\n"
                    f"       sum(CASE WHEN _c > 0 THEN 1 ELSE 0 END) OVER "
                    f"(PARTITION BY {part}) AS _npos,\n"
                    f"       sum(CASE WHEN _c < 0 THEN 1 ELSE 0 END) OVER "
                    f"(PARTITION BY {part}) AS _nneg\n"
                    f"FROM _centered",
                )
            )
            if demeaned:
                raw = (
                    "CASE WHEN _s > 0 THEN 1.0 / NULLIF(_npos, 0) "
                    "WHEN _s < 0 THEN -1.0 / NULLIF(_nneg, 0) ELSE 0.0 END"
                )
            else:
                raw = "_s"
            ctes.append(
                ("_raw", f"SELECT {carry}{fwd_carry}, {raw} AS _w FROM _signed")
            )
        else:
            centered = (
                f"{factor} - avg({factor}) OVER (PARTITION BY {part})"
                if demeaned
                else factor
            )
            ctes.append(
                (
                    "_raw",
                    f"SELECT {carry}{fwd_carry}, {centered} AS _w FROM _panel",
                )
            )

        ctes.append(
            (
                "_norm",
                f"SELECT {carry}{fwd_carry},\n"
                f"       _w / NULLIF(sum(abs(_w)) OVER (PARTITION BY {part}), 0) "
                f"AS _w\nFROM _raw",
            )
        )
        if group_adjust:
            # Re-normalize across the whole date so gross leverage is 1.
            ctes.append(
                (
                    "_final",
                    f"SELECT {carry}{fwd_carry},\n"
                    f"       _w / NULLIF(sum(abs(_w)) OVER "
                    f"(PARTITION BY {quote_ident(TS)}), 0) AS weight\n"
                    f"FROM _norm",
                )
            )
        else:
            ctes.append(
                (
                    "_final",
                    f"SELECT {carry}{fwd_carry}, _w AS weight FROM _norm",
                )
            )
        return _with(ctes, "SELECT * FROM _final")

    def returns(
        self,
        demeaned: bool = True,
        group_adjust: bool = False,
        equal_weight: bool = False,
    ):
        """Per-date return of the factor-weighted portfolio, one column per horizon."""
        return self._run(
            self._returns_sql(demeaned, group_adjust, equal_weight)
        )

    def _returns_sql(
        self, demeaned: bool, group_adjust: bool, equal_weight: bool
    ) -> str:
        weights = self._weights_sql(demeaned, group_adjust, equal_weight)
        aggs = ",\n".join(
            f"       sum(weight * {quote_ident(_fwd(p))}) AS "
            f"{quote_ident(f'ret_{p}')}"
            for p in self.periods
        )
        body = (
            f"SELECT {quote_ident(TS)},\n{aggs}\n"
            f"FROM _w GROUP BY {quote_ident(TS)} ORDER BY {quote_ident(TS)}"
        )
        return _with([("_w", weights)], body)

    def alpha_beta(
        self,
        demeaned: bool = True,
        group_adjust: bool = False,
        equal_weight: bool = False,
        annualization: float = 252.0,
    ):
        """Annualized alpha and beta of the factor portfolio vs the universe.

        The universe return is the equal-weighted mean forward return across
        the panel. Annualization uses ``annualization / period`` compounding
        periods per year, with ``period`` measured in bars.
        """
        returns_sql = self._returns_sql(demeaned, group_adjust, equal_weight)
        universe_aggs = ",\n".join(
            f"       avg({quote_ident(_fwd(p))}) AS {quote_ident(f'uni_{p}')}"
            for p in self.periods
        )
        # The panel is inlined as a subquery rather than a CTE: the nested
        # returns query defines its own `_panel`, and a name may only be
        # bound once across the whole statement.
        ctes = [
            ("_ret", returns_sql),
            (
                "_uni",
                f"SELECT {quote_ident(TS)},\n{universe_aggs}\n"
                f"FROM (\n{indent(self._sql)}\n) AS {quote_ident('_p')} "
                f"GROUP BY {quote_ident(TS)}",
            ),
            (
                "_j",
                f"SELECT r.{quote_ident(TS)},\n"
                + ",\n".join(
                    f"       r.{quote_ident(f'ret_{p}')}, "
                    f"u.{quote_ident(f'uni_{p}')}"
                    for p in self.periods
                )
                + f"\nFROM _ret r JOIN _uni u "
                f"ON r.{quote_ident(TS)} = u.{quote_ident(TS)}",
            ),
        ]
        parts = []
        for p in self.periods:
            y = quote_ident(f"ret_{p}")
            x = quote_ident(f"uni_{p}")
            parts.append(
                f"SELECT {p} AS period, regr_intercept({y}, {x}) AS alpha, "
                f"regr_slope({y}, {x}) AS beta FROM _j"
            )
        body = "\nUNION ALL\n".join(parts) + "\nORDER BY period"
        result = self._run(_with(ctes, body))
        rows = result.to_arrow().to_pylist()
        out = []
        for row in rows:
            alpha = row["alpha"]
            period = row["period"]
            ann = None
            if alpha is not None:
                exponent = annualization / period
                base = 1.0 + alpha
                ann = base**exponent - 1 if base > 0 else float("nan")
            out.append(
                {
                    "period": period,
                    "alpha": alpha,
                    "ann_alpha": ann,
                    "beta": row["beta"],
                }
            )
        return out

    def cumulative_returns(
        self,
        period: Optional[int] = None,
        demeaned: bool = True,
        group_adjust: bool = False,
        equal_weight: bool = False,
    ):
        """Compounded equity curve of the factor portfolio for one horizon.

        The series compounds the per-date horizon returns in date order. For
        ``period > 1`` those horizons overlap, so the curve reads as
        rebalancing at every bar into a ``period``-bar holding, which is the
        alphalens convention, not a tradable schedule.
        """
        if period is None:
            period = self.periods[0]
        if period not in self.periods:
            raise ValueError(
                f"period {period} is not in the panel's periods {list(self.periods)}"
            )
        returns_sql = self._returns_sql(demeaned, group_adjust, equal_weight)
        r = quote_ident(f"ret_{period}")
        body = (
            f"SELECT {quote_ident(TS)}, {r} AS period_return,\n"
            f"       exp(sum(ln(1 + {r})) OVER (ORDER BY {quote_ident(TS)})) - 1 "
            f"AS cumulative_return\n"
            f"FROM _r WHERE {r} IS NOT NULL AND {r} > -1 "
            f"ORDER BY {quote_ident(TS)}"
        )
        return self._run(_with([("_r", returns_sql)], body))

    # -- helpers ------------------------------------------------------------

    def quantile_count(self) -> int:
        """The highest quantile label present in the panel.

        An empty panel has no highest label, and `max()` over no rows is NULL;
        that is a state the caller has to handle rather than an integer, so it
        is named rather than turned into `int(None)` three frames down.
        """
        row = (
            self._run(
                _with(
                    [("_p", self._sql)],
                    f"SELECT max({quote_ident(QUANTILE)}) AS q FROM _p",
                )
            )
            .to_arrow()
            .to_pylist()[0]
        )
        if row["q"] is None:
            raise ValueError(
                "this panel has no binned rows, so it has no quantiles; check "
                "loss_report() for where they went"
            )
        return int(row["q"])

    def _require_group(self, needed: bool) -> None:
        if needed and not self.has_group:
            raise ValueError(
                "this needs a group; pass group= to build_panel()"
            )

    def _demeaned_cte(
        self, name: str, group_adjust: bool, demeaned: bool = True
    ) -> list:
        """Source CTEs with forward returns demeaned as alphalens does."""
        ctes = [("_panel", self._sql)]
        if not demeaned and not group_adjust:
            ctes.append((name, "SELECT * FROM _panel"))
            return ctes
        keys = [TS] + ([GROUP] if group_adjust else [])
        part = _partition(keys)
        carry = [TS, ASSET, FACTOR, QUANTILE] + ([GROUP] if self.has_group else [])
        cols = [f"       {quote_ident(c)}" for c in carry]
        for p in self.periods:
            c = quote_ident(_fwd(p))
            cols.append(
                f"       {c} - avg({c}) OVER (PARTITION BY {part}) AS {c}"
            )
        ctes.append((name, "SELECT\n" + ",\n".join(cols) + "\nFROM _panel"))
        return ctes
