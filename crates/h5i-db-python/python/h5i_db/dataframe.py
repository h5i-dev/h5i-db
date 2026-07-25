"""Lazy DataFrame builder for the Python API (ROADMAP Part VIII).

A polars-shaped query builder that **compiles to SQL** and executes through
the same path as :meth:`h5i_db.Database.sql`::

    from h5i_db import col

    (db.table("trades", as_of="2026-07-01T00:00:00Z")
       .filter(col("symbol").is_in(["AAPL", "MSFT"]))
       .group_by("symbol")
       .agg(col("price").mean().alias("px"))
       .collect())

The builder is a *compiler*, never an *evaluator*. Every verb lowers to SQL
text run by the engine, so a builder query sees exactly the same session,
table functions and version pins as the equivalent hand-written string. No
step of a pipeline is evaluated in Python; a verb that cannot be expressed in
SQL does not exist here.

Two consequences worth knowing:

* ``db.table(name, version=…)`` lowers to the ``h5i()`` table function, so
  research-mode pins apply to builder queries. Unpinned, it lowers to the
  bare table name, which is snapshot-bound for the duration of the query --
  two references to one table inside a single query always agree.
* :meth:`LazyFrame.sql` returns the generated SQL. It is stable, formatted
  and meant to be read, logged and diffed; copying it into ``db.sql()`` is
  the supported way to graduate out of the builder.

Coverage of SQL through builder verbs is deliberately partial.
:func:`sql_expr` embeds a raw fragment anywhere an expression is accepted,
and is the intended escape hatch rather than a sign of a missing feature.
"""

from __future__ import annotations

import datetime as _datetime
import decimal as _decimal
import math as _math
import re as _re
from typing import Any, Iterable, Optional, Sequence, Union

from h5i_db._native import InvalidInputError

# Expression precedence. Rendering a child at a required minimum precedence
# and parenthesising only when it binds more loosely is what keeps generated
# SQL readable: `a + b * c` rather than `((a) + ((b) * (c)))`.
_P_RAW = 0  # sql_expr(): unknown shape, so always parenthesised when nested
_P_OR = 1
_P_AND = 2
_P_NOT = 3
_P_CMP = 4
_P_ADD = 5
_P_MUL = 6
_P_UNARY = 7
_P_ATOM = 8

_CAST_TYPE_RE = _re.compile(r"^[A-Za-z][A-Za-z0-9_ ]*(\(\s*\d+\s*(,\s*\d+\s*)?\))?$")
_DURATION_RE = _re.compile(r"^\s*(\d+(?:\.\d+)?)\s*(s|m|h|d|w|mo|y)\s*$")

# Duration suffix -> (SQL interval unit, next smaller unit, multiplier).
# The chain is what lets '1.5h' become INTERVAL '90 minutes': months and years
# have no fixed-size smaller unit, so they stop the chain and a fractional
# value is rejected rather than silently rounded.
_DURATION_UNITS = {
    "s": ("seconds", None, 0),
    "m": ("minutes", "s", 60),
    "h": ("hours", "m", 60),
    "d": ("days", "h", 24),
    "w": ("weeks", "d", 7),
    "mo": ("months", None, 0),
    "y": ("years", "mo", 12),
}

_JOIN_KINDS = {
    "inner": "INNER JOIN",
    "left": "LEFT JOIN",
    "right": "RIGHT JOIN",
    "full": "FULL OUTER JOIN",
    "cross": "CROSS JOIN",
    "semi": "LEFT SEMI JOIN",
    "anti": "LEFT ANTI JOIN",
}

ExprLike = Union["Expr", str, int, float, bool, None]


def _invalid(message: str, hint: Optional[str] = None) -> InvalidInputError:
    """Build the same error shape the native layer raises for bad input."""
    err = InvalidInputError(f"[invalid_input] {message}")
    try:
        err.code = "invalid_input"
        err.hint = hint
        err.retryable = False
        err.did_you_mean = None
        err.next_actions = []
    except AttributeError:  # pragma: no cover -- attribute-less exception type
        pass
    return err


def _did_you_mean(name: str, candidates: Iterable[str]) -> Optional[str]:
    """Closest candidate by edit distance, or None when nothing is close."""
    import difflib

    matches = difflib.get_close_matches(name, list(candidates), n=1, cutoff=0.6)
    return matches[0] if matches else None


# ---------------------------------------------------------------------------
# Quoting -- the single site where user text becomes SQL
# ---------------------------------------------------------------------------


def quote_ident(name: str) -> str:
    """Quote an identifier. Always quoted, so case is preserved exactly.

    Arrow schemas are case-sensitive while unquoted SQL identifiers are
    normalised to lowercase, so quoting is what makes ``col("Symbol")`` find a
    field actually named ``Symbol``. A literal ``"`` is doubled; no other
    character needs escaping inside a quoted identifier.
    """
    if not isinstance(name, str):
        raise _invalid(f"identifier must be a string, got {type(name).__name__}")
    if not name:
        raise _invalid("identifier must not be empty")
    if "\x00" in name:
        raise _invalid("identifier must not contain a NUL character")
    return '"' + name.replace('"', '""') + '"'


def quote_string(value: str) -> str:
    """Quote a string literal by doubling embedded single quotes."""
    if "\x00" in value:
        raise _invalid("string literal must not contain a NUL character")
    return "'" + value.replace("'", "''") + "'"


def _fmt_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    # bool before int: bool is a subclass of int and TRUE/FALSE is not 1/0.
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if _math.isnan(value) or _math.isinf(value):
            raise _invalid(
                f"{value!r} has no SQL literal form",
                hint="filter on the column instead, or use sql_expr(...)",
            )
        return repr(value)
    if isinstance(value, _decimal.Decimal):
        if not value.is_finite():
            raise _invalid(f"{value!r} has no SQL literal form")
        return str(value)
    if isinstance(value, str):
        return quote_string(value)
    # datetime before date: datetime is a subclass of date.
    if isinstance(value, _datetime.datetime):
        return f"TIMESTAMP {quote_string(value.isoformat())}"
    if isinstance(value, _datetime.date):
        return f"DATE {quote_string(value.isoformat())}"
    raise _invalid(
        f"cannot use {type(value).__name__} as a SQL literal",
        hint="supported: None, bool, int, float, Decimal, str, date, datetime",
    )


def _parse_duration(text: str) -> str:
    """``'30m'`` -> ``INTERVAL '30 minutes'`` for RANGE window frames."""
    match = _DURATION_RE.match(text)
    if not match:
        raise _invalid(
            f"cannot parse duration {text!r}",
            hint="use a number and one of s, m, h, d, w, mo, y (e.g. '30m')",
        )
    amount = float(match.group(1))
    unit = match.group(2)
    # Walk down to a smaller unit while the amount is fractional.
    while amount != int(amount):
        _, smaller, factor = _DURATION_UNITS[unit]
        if smaller is None:
            raise _invalid(
                f"duration {text!r} is not a whole number of a fixed-size unit",
                hint="use a whole number, e.g. '90m' rather than '1.5h'",
            )
        amount *= factor
        unit = smaller
    if amount <= 0:
        raise _invalid(f"duration {text!r} must be positive")
    return f"INTERVAL {quote_string(f'{int(amount)} {_DURATION_UNITS[unit][0]}')}"


# ---------------------------------------------------------------------------
# Expressions
# ---------------------------------------------------------------------------


class Expr:
    """A SQL expression fragment.

    Build these with :func:`col`, :func:`lit` and :func:`sql_expr` rather than
    constructing directly. Operators compose: ``&``, ``|`` and ``~`` are the
    boolean connectives (Python's ``and``/``or``/``not`` cannot be overloaded
    and raise a :class:`TypeError` here).
    """

    __slots__ = ("_sql", "_prec", "_alias", "_refs")

    def __init__(
        self,
        sql: str,
        prec: int = _P_ATOM,
        alias: Optional[str] = None,
        refs: Optional[frozenset] = frozenset(),
    ):
        self._sql = sql
        self._prec = prec
        self._alias = alias
        # None means "references unknown" (an opaque sql_expr fragment), which
        # makes the frame conservatively wrap rather than risk a dangling
        # reference to a column the previous stage did not project.
        self._refs = refs

    # -- rendering ---------------------------------------------------------

    def _render(self, min_prec: int = 0) -> str:
        return f"({self._sql})" if self._prec < min_prec else self._sql

    def _projection_sql(self) -> str:
        if self._alias is None:
            return self._sql
        return f"{self._sql} AS {quote_ident(self._alias)}"

    def __repr__(self) -> str:
        suffix = f" AS {self._alias!r}" if self._alias else ""
        return f"<Expr {self._sql}{suffix}>"

    # -- naming ------------------------------------------------------------

    def alias(self, name: str) -> "Expr":
        """Name this expression in the output (``expr AS name``)."""
        quote_ident(name)  # validate eagerly, at the call site
        return Expr(self._sql, self._prec, name, self._refs)

    @property
    def output_name(self) -> Optional[str]:
        """The column name this expression produces, when it is knowable.

        An alias, or a plain column reference. ``None`` for a computed
        expression with no alias, whose name the engine derives.
        """
        if self._alias is not None:
            return self._alias
        return _plain_name(self)

    # -- operators ---------------------------------------------------------

    def __bool__(self):
        raise TypeError(
            "an Expr has no truth value; combine with & | ~ "
            "(and remember to parenthesise: (a > 1) & (b < 2))"
        )

    def __add__(self, other):
        return _binary(self, "+", other, _P_ADD)

    def __radd__(self, other):
        return _binary(other, "+", self, _P_ADD)

    def __sub__(self, other):
        return _binary(self, "-", other, _P_ADD)

    def __rsub__(self, other):
        return _binary(other, "-", self, _P_ADD)

    def __mul__(self, other):
        return _binary(self, "*", other, _P_MUL)

    def __rmul__(self, other):
        return _binary(other, "*", self, _P_MUL)

    def __truediv__(self, other):
        return _binary(self, "/", other, _P_MUL)

    def __rtruediv__(self, other):
        return _binary(other, "/", self, _P_MUL)

    def __mod__(self, other):
        return _binary(self, "%", other, _P_MUL)

    def __rmod__(self, other):
        return _binary(other, "%", self, _P_MUL)

    def __neg__(self):
        return Expr(f"-{self._render(_P_UNARY)}", _P_UNARY, refs=self._refs)

    def __eq__(self, other):  # type: ignore[override]
        if other is None:
            raise _invalid(
                "comparing to None is never true in SQL",
                hint="use .is_null() instead of == None",
            )
        return _binary(self, "=", other, _P_CMP, non_assoc=True)

    def __ne__(self, other):  # type: ignore[override]
        if other is None:
            raise _invalid(
                "comparing to None is never true in SQL",
                hint="use .is_not_null() instead of != None",
            )
        return _binary(self, "!=", other, _P_CMP, non_assoc=True)

    def __lt__(self, other):
        return _binary(self, "<", other, _P_CMP, non_assoc=True)

    def __le__(self, other):
        return _binary(self, "<=", other, _P_CMP, non_assoc=True)

    def __gt__(self, other):
        return _binary(self, ">", other, _P_CMP, non_assoc=True)

    def __ge__(self, other):
        return _binary(self, ">=", other, _P_CMP, non_assoc=True)

    def __and__(self, other):
        return _binary(self, "AND", other, _P_AND)

    def __rand__(self, other):
        return _binary(other, "AND", self, _P_AND)

    def __or__(self, other):
        return _binary(self, "OR", other, _P_OR)

    def __ror__(self, other):
        return _binary(other, "OR", self, _P_OR)

    def __invert__(self):
        return Expr(f"NOT {self._render(_P_NOT)}", _P_NOT, refs=self._refs)

    __hash__ = None  # type: ignore[assignment]  # __eq__ builds SQL, not a bool

    # -- predicates --------------------------------------------------------

    def is_null(self) -> "Expr":
        return Expr(f"{self._render(_P_CMP + 1)} IS NULL", _P_CMP, refs=self._refs)

    def is_not_null(self) -> "Expr":
        return Expr(f"{self._render(_P_CMP + 1)} IS NOT NULL", _P_CMP, refs=self._refs)

    def is_in(self, values: Union[Iterable[Any], "Expr"]) -> "Expr":
        """``x IN (…)``. An empty collection lowers to ``FALSE``."""
        if isinstance(values, Expr):
            items = [values._render(0)]
            refs = _merge_refs(self._refs, values._refs)
        else:
            if isinstance(values, (str, bytes)):
                raise _invalid(
                    "is_in() takes a collection of values, not a single string",
                    hint='use is_in(["AAPL"]) or == "AAPL"',
                )
            items = [_to_expr(v)._render(0) for v in values]
            refs = self._refs
            if not items:
                return Expr("FALSE", _P_ATOM, refs=frozenset())
        return Expr(
            f"{self._render(_P_CMP + 1)} IN ({', '.join(items)})", _P_CMP, refs=refs
        )

    def between(self, low: ExprLike, high: ExprLike) -> "Expr":
        lo, hi = _to_expr(low), _to_expr(high)
        return Expr(
            f"{self._render(_P_CMP + 1)} BETWEEN "
            f"{lo._render(_P_CMP + 1)} AND {hi._render(_P_CMP + 1)}",
            _P_CMP,
            refs=_merge_refs(self._refs, lo._refs, hi._refs),
        )

    def like(self, pattern: str) -> "Expr":
        return self._like("LIKE", pattern)

    def not_like(self, pattern: str) -> "Expr":
        return self._like("NOT LIKE", pattern)

    def ilike(self, pattern: str) -> "Expr":
        return self._like("ILIKE", pattern)

    def _like(self, op: str, pattern: str) -> "Expr":
        if not isinstance(pattern, str):
            raise _invalid(f"{op} pattern must be a string")
        return Expr(
            f"{self._render(_P_CMP + 1)} {op} {quote_string(pattern)}",
            _P_CMP,
            refs=self._refs,
        )

    def cast(self, sql_type: str) -> "Expr":
        """``CAST(x AS <type>)``; the type is a SQL type name."""
        if not isinstance(sql_type, str) or not _CAST_TYPE_RE.match(sql_type):
            raise _invalid(
                f"{sql_type!r} is not a valid SQL type name",
                hint="e.g. 'BIGINT', 'DOUBLE', 'DECIMAL(10, 2)'; "
                "for exotic types use sql_expr(...)",
            )
        return Expr(f"CAST({self._sql} AS {sql_type})", _P_ATOM, refs=self._refs)

    # -- scalar functions --------------------------------------------------

    def abs(self) -> "Expr":
        return _call("abs", self)

    def log(self) -> "Expr":
        return _call("ln", self)

    def log10(self) -> "Expr":
        return _call("log10", self)

    def exp(self) -> "Expr":
        return _call("exp", self)

    def sqrt(self) -> "Expr":
        return _call("sqrt", self)

    def sign(self) -> "Expr":
        return _call("sign", self)

    def round(self, decimals: int = 0) -> "Expr":
        if not isinstance(decimals, int) or isinstance(decimals, bool):
            raise _invalid("round(decimals=) must be an integer")
        return _call("round", self, lit(decimals))

    def floor(self) -> "Expr":
        return _call("floor", self)

    def ceil(self) -> "Expr":
        return _call("ceil", self)

    def coalesce(self, *others: ExprLike) -> "Expr":
        return _call("coalesce", self, *others)

    def greatest(self, *others: ExprLike) -> "Expr":
        return _call("greatest", self, *others)

    def least(self, *others: ExprLike) -> "Expr":
        return _call("least", self, *others)

    # -- aggregates --------------------------------------------------------

    def sum(self) -> "Expr":
        return _call("sum", self)

    def mean(self) -> "Expr":
        return _call("avg", self)

    def min(self) -> "Expr":
        return _call("min", self)

    def max(self) -> "Expr":
        return _call("max", self)

    def count(self) -> "Expr":
        return _call("count", self)

    def n_unique(self) -> "Expr":
        return Expr(f"count(DISTINCT {self._sql})", _P_ATOM, refs=self._refs)

    def std(self) -> "Expr":
        return _call("stddev", self)

    def var(self) -> "Expr":
        return _call("var_samp", self)

    def median(self) -> "Expr":
        return _call("median", self)

    def quantile(self, q: float) -> "Expr":
        """Exact percentile (``percentile_cont``)."""
        if not isinstance(q, (int, float)) or isinstance(q, bool) or not 0 <= q <= 1:
            raise _invalid("quantile(q) must be a number in [0, 1]")
        return Expr(
            f"percentile_cont({_fmt_literal(float(q))}) "
            f"WITHIN GROUP (ORDER BY {self._sql})",
            _P_ATOM,
            refs=self._refs,
        )

    def first(self, order_by: Optional[Any] = None, descending: bool = False) -> "Expr":
        """``first_value(x ORDER BY …)`` -- the "opening value" idiom."""
        return self._bookend("first_value", order_by, descending)

    def last(self, order_by: Optional[Any] = None, descending: bool = False) -> "Expr":
        """``last_value(x ORDER BY …)`` -- the "closing value" idiom."""
        return self._bookend("last_value", order_by, descending)

    def _bookend(self, fn: str, order_by, descending) -> "Expr":
        if order_by is None:
            return _call(fn, self)
        items, refs = _order_items(order_by, descending)
        return Expr(
            f"{fn}({self._sql} ORDER BY {', '.join(items)})",
            _P_ATOM,
            refs=_merge_refs(self._refs, refs),
        )

    # -- window functions --------------------------------------------------

    def over(
        self,
        partition_by: Optional[Any] = None,
        order_by: Optional[Any] = None,
        descending: bool = False,
        rows: Optional[Union[int, Sequence[Optional[int]]]] = None,
        duration: Optional[str] = None,
    ) -> "Expr":
        """Attach an ``OVER (…)`` clause to this (aggregate) expression.

        ``rows`` is either a trailing row count ``n`` (``ROWS BETWEEN n-1
        PRECEDING AND CURRENT ROW``) or a ``(preceding, following)`` pair
        where ``None`` means unbounded. ``duration`` is a time-based frame
        such as ``'30m'``, which needs ``order_by`` on a timestamp column.
        """
        clause, refs = _window_clause(
            partition_by, order_by, descending, rows, duration
        )
        return Expr(
            f"{self._sql} OVER ({clause})",
            _P_ATOM,
            refs=_merge_refs(self._refs, refs),
        )

    def ewma(
        self,
        alpha: float,
        order_by: Any,
        partition_by: Optional[Any] = None,
    ) -> "Expr":
        """``ewma(x, alpha)`` -- pandas ``ewm(alpha=…, adjust=False)``."""
        if not isinstance(alpha, (int, float)) or isinstance(alpha, bool):
            raise _invalid("ewma(alpha) must be a number")
        if not 0 <= alpha <= 1:
            raise _invalid(f"ewma(alpha) must be in [0, 1], got {alpha}")
        base = Expr(
            f"ewma({self._sql}, {_fmt_literal(float(alpha))})",
            _P_ATOM,
            refs=self._refs,
        )
        return base.over(partition_by=partition_by, order_by=order_by)

    # -- rolling sugar (DESIGN.md 6.6) -------------------------------------

    def _rolling(
        self, fn: str, window, order_by, partition_by, extra: Sequence["Expr"] = ()
    ) -> "Expr":
        if order_by is None:
            raise _invalid(
                f"rolling_{fn} needs order_by -- a rolling window is only "
                "defined against an ordering",
                hint="order_by='ts'",
            )
        rows, duration = _split_window(window)
        args = ", ".join([self._sql] + [e._render(0) for e in extra])
        base = Expr(
            f"{fn}({args})",
            _P_ATOM,
            refs=_merge_refs(self._refs, *[e._refs for e in extra]),
        )
        return base.over(
            partition_by=partition_by, order_by=order_by, rows=rows, duration=duration
        )

    def rolling_mean(self, window, order_by, partition_by=None) -> "Expr":
        return self._rolling("avg", window, order_by, partition_by)

    def rolling_sum(self, window, order_by, partition_by=None) -> "Expr":
        return self._rolling("sum", window, order_by, partition_by)

    def rolling_min(self, window, order_by, partition_by=None) -> "Expr":
        return self._rolling("min", window, order_by, partition_by)

    def rolling_max(self, window, order_by, partition_by=None) -> "Expr":
        return self._rolling("max", window, order_by, partition_by)

    def rolling_std(self, window, order_by, partition_by=None) -> "Expr":
        return self._rolling("stddev", window, order_by, partition_by)

    def rolling_var(self, window, order_by, partition_by=None) -> "Expr":
        return self._rolling("var_samp", window, order_by, partition_by)

    def rolling_count(self, window, order_by, partition_by=None) -> "Expr":
        return self._rolling("count", window, order_by, partition_by)

    def rolling_mad(self, window, order_by, partition_by=None) -> "Expr":
        """Mean absolute deviation about the frame mean (``mad``)."""
        return self._rolling("mad", window, order_by, partition_by)

    def rolling_skew(self, window, order_by, partition_by=None) -> "Expr":
        """Sample skewness (``skew``); NULL for frames of fewer than 3 rows."""
        return self._rolling("skew", window, order_by, partition_by)

    def rolling_kurt(self, window, order_by, partition_by=None) -> "Expr":
        """Excess kurtosis (``kurt``); NULL for frames of fewer than 4 rows."""
        return self._rolling("kurt", window, order_by, partition_by)

    def rolling_rank(self, window, order_by, partition_by=None) -> "Expr":
        """Percentile of the current value within the frame (``ts_rank``)."""
        return self._rolling("ts_rank", window, order_by, partition_by)

    def rolling_idxmax(self, window, order_by, partition_by=None) -> "Expr":
        """1-based position of the frame maximum (``idxmax``)."""
        return self._rolling("idxmax", window, order_by, partition_by)

    def rolling_idxmin(self, window, order_by, partition_by=None) -> "Expr":
        """1-based position of the frame minimum (``idxmin``)."""
        return self._rolling("idxmin", window, order_by, partition_by)

    def rolling_corr(self, other, window, order_by, partition_by=None) -> "Expr":
        """Rolling correlation (``ts_corr``); NULL when either side is flat."""
        return self._rolling(
            "ts_corr", window, order_by, partition_by, extra=(_to_expr(other),)
        )

    def rolling_cov(self, other, window, order_by, partition_by=None) -> "Expr":
        """Rolling covariance (``ts_cov``)."""
        return self._rolling(
            "ts_cov", window, order_by, partition_by, extra=(_to_expr(other),)
        )

    # -- cross-sectional (VII-B2) ------------------------------------------

    def cs_rank(self, partition_by: Any) -> "Expr":
        """Percentile rank against peers in the same bucket (``cs_rank``)."""
        return self._cross_sectional("cs_rank", partition_by)

    def cs_winsorize(self, lower: float, upper: float, partition_by: Any) -> "Expr":
        """Clip cross-sectional tails at percentile cutoffs."""
        for name, value in (("lower", lower), ("upper", upper)):
            if (
                not isinstance(value, (int, float))
                or isinstance(value, bool)
                or not 0 <= value <= 1
            ):
                raise _invalid(f"cs_winsorize({name}=) must be a number in [0, 1]")
        if lower > upper:
            raise _invalid("cs_winsorize needs lower <= upper")
        return self._cross_sectional(
            "cs_winsorize",
            partition_by,
            extra=(lit(float(lower)), lit(float(upper))),
        )

    def cs_demean(self, partition_by: Any) -> "Expr":
        """``x - avg(x) OVER (PARTITION BY …)`` -- plain SQL, no UDF."""
        clause, refs = _window_clause(partition_by, None, False, None, None)
        return Expr(
            f"{self._render(_P_ADD)} - avg({self._sql}) OVER ({clause})",
            _P_ADD,
            refs=_merge_refs(self._refs, refs),
        )

    def cs_zscore(self, partition_by: Any) -> "Expr":
        """Cross-sectional standardisation -- plain SQL, no UDF."""
        clause, refs = _window_clause(partition_by, None, False, None, None)
        return Expr(
            f"({self._render(_P_ADD)} - avg({self._sql}) OVER ({clause})) "
            f"/ stddev({self._sql}) OVER ({clause})",
            _P_MUL,
            refs=_merge_refs(self._refs, refs),
        )

    def _cross_sectional(
        self, fn: str, partition_by: Any, extra: Sequence["Expr"] = ()
    ) -> "Expr":
        if partition_by is None:
            raise _invalid(
                f"{fn} needs partition_by -- a cross-section is defined by the "
                "bucket its peers share",
                hint="partition_by='ts'",
            )
        clause, refs = _window_clause(partition_by, None, False, None, None)
        args = ", ".join([self._sql] + [e._render(0) for e in extra])
        return Expr(
            f"{fn}({args}) OVER ({clause})",
            _P_ATOM,
            refs=_merge_refs(self._refs, refs),
        )


def _merge_refs(*refsets) -> Optional[frozenset]:
    out: set = set()
    for refs in refsets:
        if refs is None:
            return None
        out |= refs
    return frozenset(out)


def _to_expr(value: ExprLike) -> Expr:
    if isinstance(value, Expr):
        return value
    if isinstance(value, _WhenThen):
        return value._as_expr()
    return lit(value)


def _binary(
    left: ExprLike, op: str, right: ExprLike, prec: int, non_assoc: bool = False
) -> Expr:
    lhs, rhs = _to_expr(left), _to_expr(right)
    right_prec = prec + 1 if (non_assoc or op in {"-", "/", "%"}) else prec
    return Expr(
        f"{lhs._render(prec)} {op} {rhs._render(right_prec)}",
        prec,
        refs=_merge_refs(lhs._refs, rhs._refs),
    )


def _call(fn: str, *args: ExprLike) -> Expr:
    exprs = [_to_expr(a) for a in args]
    rendered = ", ".join(e._render(0) for e in exprs)
    return Expr(f"{fn}({rendered})", _P_ATOM, refs=_merge_refs(*[e._refs for e in exprs]))


# ---------------------------------------------------------------------------
# Expression constructors
# ---------------------------------------------------------------------------


def col(name: str, relation: Optional[str] = None) -> Expr:
    """Reference a column.

    The name is one identifier and is never split on ``.``; a column really
    named ``a.b`` works. To qualify a side of a join, pass
    ``relation="l"`` or ``relation="r"``.

    ``col("*")`` is the wildcard.
    """
    if name == "*" and relation is None:
        return Expr("*", _P_ATOM, refs=None)
    ident = quote_ident(name)
    if relation is not None:
        return Expr(f"{quote_ident(relation)}.{ident}", _P_ATOM, refs=frozenset({name}))
    return Expr(ident, _P_ATOM, refs=frozenset({name}))


def lit(value: Any) -> Expr:
    """A literal value."""
    return Expr(_fmt_literal(value), _P_ATOM, refs=frozenset())


def sql_expr(sql: str) -> Expr:
    """Embed a raw SQL fragment -- the escape hatch.

    The text is inserted verbatim, so it is the one place the builder cannot
    guarantee quoting; never build it from untrusted input. It is treated as
    referencing unknown columns, so a frame conservatively wraps the previous
    stage in a subquery rather than assume the fragment is safe to inline.
    """
    if not isinstance(sql, str) or not sql.strip():
        raise _invalid("sql_expr() needs a non-empty SQL string")
    return Expr(sql.strip(), _P_RAW, refs=None)


def count_star() -> Expr:
    """``count(*)``."""
    return Expr("count(*)", _P_ATOM, refs=frozenset())


def vwap(price: ExprLike, size: ExprLike) -> Expr:
    """``vwap(price, size)`` -- volume-weighted mean (value first)."""
    return _call("vwap", price, size)


def wavg(weight: ExprLike, value: ExprLike) -> Expr:
    """``wavg(weight, value)`` -- kdb argument order (weight first)."""
    return _call("wavg", weight, value)


def time_bucket(
    interval: Union[str, Expr],
    ts: ExprLike,
    origin: Optional[Any] = None,
    timezone: Optional[str] = None,
) -> Expr:
    """``time_bucket('5m', ts)`` -- floor timestamps into fixed buckets."""
    if isinstance(interval, str):
        if not _DURATION_RE.match(interval):
            raise _invalid(
                f"cannot parse time_bucket interval {interval!r}",
                hint="use a number and one of s, m, h, d, w, mo, y (e.g. '5m'); "
                "for an SQL INTERVAL use sql_expr(...)",
            )
        interval_expr: ExprLike = lit(interval)
    else:
        interval_expr = interval
    args: list = [interval_expr, ts]
    if origin is not None:
        args.append(origin)
    if timezone is not None:
        if not isinstance(timezone, str):
            raise _invalid("time_bucket(timezone=) must be a string")
        args.append(lit(timezone))
    return _call("time_bucket", *args)


class _WhenThen:
    """Intermediate state of a ``when(...).then(...)`` chain."""

    __slots__ = ("_pairs",)

    def __init__(self, pairs):
        self._pairs = pairs

    def when(self, condition: Expr) -> "_When":
        return _When(self._pairs, condition)

    def otherwise(self, value: ExprLike) -> Expr:
        return self._build(_to_expr(value))

    def _as_expr(self) -> Expr:
        """No ``otherwise`` -- SQL's implicit ``ELSE NULL``."""
        return self._build(None)

    def _build(self, default: Optional[Expr]) -> Expr:
        parts = ["CASE"]
        refsets = []
        for cond, value in self._pairs:
            parts.append(f"WHEN {cond._render(0)} THEN {value._render(0)}")
            refsets += [cond._refs, value._refs]
        if default is not None:
            parts.append(f"ELSE {default._render(0)}")
            refsets.append(default._refs)
        parts.append("END")
        return Expr(" ".join(parts), _P_ATOM, refs=_merge_refs(*refsets))


class _When:
    """A pending ``when(...)`` awaiting its ``then(...)``."""

    __slots__ = ("_pairs", "_cond")

    def __init__(self, pairs, cond):
        if not isinstance(cond, Expr):
            raise _invalid("when() takes a boolean Expr")
        self._pairs = pairs
        self._cond = cond

    def then(self, value: ExprLike) -> _WhenThen:
        return _WhenThen(self._pairs + [(self._cond, _to_expr(value))])


def when(condition: Expr) -> _When:
    """Start a ``CASE WHEN`` chain: ``when(c).then(a).otherwise(b)``."""
    return _When([], condition)


# ---------------------------------------------------------------------------
# Window / ordering clauses
# ---------------------------------------------------------------------------


def _as_expr_list(value: Any) -> list:
    """Normalise ``"a"`` / ``col("a")`` / a sequence of either into Exprs."""
    if value is None:
        return []
    if isinstance(value, (str, Expr)):
        value = [value]
    out = []
    for item in value:
        if isinstance(item, str):
            out.append(col(item))
        elif isinstance(item, Expr):
            out.append(item)
        elif isinstance(item, _WhenThen):
            out.append(item._as_expr())
        else:
            raise _invalid(
                f"expected a column name or Expr, got {type(item).__name__}"
            )
    return out


def _order_items(order_by: Any, descending: Union[bool, Sequence[bool]]):
    exprs = _as_expr_list(order_by)
    if not exprs:
        raise _invalid("order_by must name at least one column")
    if isinstance(descending, bool):
        flags = [descending] * len(exprs)
    else:
        flags = list(descending)
        if len(flags) != len(exprs):
            raise _invalid(
                f"descending has {len(flags)} entries but order_by has {len(exprs)}"
            )
    items = []
    for expr, desc in zip(exprs, flags):
        # An aliased ordering key refers to the projected name, which is what
        # the engine resolves in ORDER BY -- and is far more readable.
        target = (
            quote_ident(expr._alias) if expr._alias is not None else expr._render(0)
        )
        items.append(f"{target} DESC" if desc else target)
    return items, _merge_refs(*[e._refs for e in exprs])


def _split_window(window: Union[int, str]):
    """A rolling ``window`` is either a row count or a duration string."""
    if isinstance(window, bool) or not isinstance(window, (int, str)):
        raise _invalid(
            f"window must be a row count or a duration string, "
            f"got {type(window).__name__}",
            hint="window=20 (rows) or window='30m' (time)",
        )
    return (window, None) if isinstance(window, int) else (None, window)


def _frame_clause(rows, duration) -> str:
    if rows is not None and duration is not None:
        raise _invalid("pass either rows= or duration=, not both")
    if duration is not None:
        if not isinstance(duration, str):
            raise _invalid("duration must be a string such as '30m'")
        return f"RANGE BETWEEN {_parse_duration(duration)} PRECEDING AND CURRENT ROW"
    if isinstance(rows, int) and not isinstance(rows, bool):
        if rows < 1:
            raise _invalid(f"rows must be at least 1, got {rows}")
        if rows == 1:
            return "ROWS BETWEEN CURRENT ROW AND CURRENT ROW"
        return f"ROWS BETWEEN {rows - 1} PRECEDING AND CURRENT ROW"
    # (preceding, following); None on either side means unbounded.
    try:
        preceding, following = rows
    except (TypeError, ValueError):
        raise _invalid(
            "rows must be an int or a (preceding, following) pair"
        ) from None
    return f"ROWS BETWEEN {_bound(preceding, 'PRECEDING')} AND {_bound(following, 'FOLLOWING')}"


def _bound(value, direction: str) -> str:
    if value is None:
        return f"UNBOUNDED {direction}"
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise _invalid(f"frame bound must be a non-negative int or None, got {value!r}")
    return "CURRENT ROW" if value == 0 else f"{value} {direction}"


def _window_clause(partition_by, order_by, descending, rows, duration):
    parts = []
    refsets = []
    partitions = _as_expr_list(partition_by)
    if partitions:
        parts.append(
            "PARTITION BY " + ", ".join(e._render(0) for e in partitions)
        )
        refsets += [e._refs for e in partitions]
    if order_by is not None:
        items, refs = _order_items(order_by, descending)
        parts.append("ORDER BY " + ", ".join(items))
        refsets.append(refs)
    if rows is not None or duration is not None:
        if order_by is None:
            raise _invalid(
                "a window frame needs order_by",
                hint="order_by='ts'",
            )
        parts.append(_frame_clause(rows, duration))
    return " ".join(parts), _merge_refs(*refsets)


# ---------------------------------------------------------------------------
# Query state
# ---------------------------------------------------------------------------


def _indent(text: str, spaces: int = 2) -> str:
    pad = " " * spaces
    return "\n".join(pad + line if line else line for line in text.split("\n"))


class _Query:
    """One SELECT level. Verbs that cannot extend it wrap it as a subquery."""

    __slots__ = (
        "source",
        "projection",
        "filters",
        "group_by",
        "order_by",
        "limit",
        "offset",
        "distinct",
        "base_table",
        "base_pin",
        "depth",
    )

    def __init__(self, source: str, base_table=None, base_pin=None):
        self.source = source
        self.projection: Optional[list] = None
        self.filters: list = []
        self.group_by: Optional[list] = None
        self.order_by: Optional[list] = None
        self.limit: Optional[int] = None
        self.offset: Optional[int] = None
        self.distinct = False
        self.base_table = base_table
        self.base_pin = base_pin
        self.depth = 0

    def copy(self) -> "_Query":
        other = _Query(self.source, self.base_table, self.base_pin)
        other.projection = None if self.projection is None else list(self.projection)
        other.filters = list(self.filters)
        other.group_by = None if self.group_by is None else list(self.group_by)
        other.order_by = None if self.order_by is None else list(self.order_by)
        other.limit = self.limit
        other.offset = self.offset
        other.distinct = self.distinct
        other.depth = self.depth
        return other

    # -- introspection used by the wrap rules ------------------------------

    def defined_aliases(self) -> frozenset:
        """Names this level *computes* (so a later stage cannot inline them).

        A pass-through column reference is not included: selecting ``a`` does
        not redefine ``a``, so a later filter on ``a`` needs no subquery.
        """
        if self.projection is None:
            return frozenset()
        return frozenset(
            e._alias for e in self.projection if e._alias is not None
        )

    def is_bare_table(self) -> bool:
        return (
            self.base_table is not None
            and self.projection is None
            and not self.filters
            and self.group_by is None
            and self.order_by is None
            and self.limit is None
            and self.offset is None
            and not self.distinct
        )

    # -- rendering ---------------------------------------------------------

    def render(self) -> str:
        select = "SELECT DISTINCT" if self.distinct else "SELECT"
        if self.projection is None:
            projection = "*"
        else:
            projection = ", ".join(e._projection_sql() for e in self.projection)
        lines = [f"{select} {projection}", f"FROM {self.source}"]
        if self.filters:
            lines.append("WHERE " + " AND ".join(f._render(_P_AND) for f in self.filters))
        if self.group_by:
            lines.append("GROUP BY " + ", ".join(self.group_by))
        if self.order_by:
            lines.append("ORDER BY " + ", ".join(self.order_by))
        if self.limit is not None:
            lines.append(f"LIMIT {self.limit}")
        if self.offset:
            lines.append(f"OFFSET {self.offset}")
        return "\n".join(lines)

    def wrapped(self) -> "_Query":
        """This level as the FROM of a fresh one."""
        alias = quote_ident(f"_s{self.depth + 1}")
        inner = _indent(self.render())
        out = _Query(f"(\n{inner}\n) AS {alias}")
        out.depth = self.depth + 1
        return out


def _plain_name(expr: Expr) -> Optional[str]:
    if expr._prec == _P_ATOM and expr._refs and len(expr._refs) == 1:
        only = next(iter(expr._refs))
        if expr._sql == quote_ident(only):
            return only
    return None


# ---------------------------------------------------------------------------
# LazyFrame
# ---------------------------------------------------------------------------


class LazyFrame:
    """A query under construction. Nothing runs until a terminal method.

    Build one with :meth:`h5i_db.Database.table`. Every method returns a new
    frame, so a partially-built pipeline is safe to reuse as a base for
    several queries.
    """

    __slots__ = ("_db", "_q")

    def __init__(self, db, query: _Query):
        self._db = db
        self._q = query

    # -- construction ------------------------------------------------------

    @classmethod
    def _from_table(
        cls,
        db,
        name: str,
        version: Optional[int] = None,
        as_of: Optional[str] = None,
        snapshot: Optional[str] = None,
    ) -> "LazyFrame":
        if not isinstance(name, str) or not name:
            raise _invalid("table name must be a non-empty string")
        pins = [
            ("version", version),
            ("as_of", as_of),
            ("snapshot", snapshot),
        ]
        given = [(k, v) for k, v in pins if v is not None]
        if len(given) > 1:
            raise _invalid(
                "pass at most one of version / as_of / snapshot, got "
                + ", ".join(k for k, _ in given)
            )
        if not given:
            # A bare name is snapshot-bound for the whole query, so two
            # references to one table inside a query always agree. h5i() would
            # re-resolve per reference.
            return cls(db, _Query(quote_ident(name), base_table=name, base_pin=None))
        key, value = given[0]
        if key == "version":
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise _invalid(f"version must be a non-negative integer, got {value!r}")
            arg = str(value)
        else:
            if not isinstance(value, str) or not value:
                raise _invalid(f"{key} must be a non-empty string")
            arg = quote_string(value)
        source = f"h5i({quote_string(name)}, {arg})"
        return cls(db, _Query(source, base_table=name, base_pin=(key, value)))

    def _next(self, query: _Query) -> "LazyFrame":
        return LazyFrame(self._db, query)

    def _level(self, wrap: bool) -> _Query:
        return self._q.wrapped() if wrap else self._q.copy()

    # -- verbs -------------------------------------------------------------

    def filter(self, *predicates: Expr) -> "LazyFrame":
        """Keep rows matching every predicate (they are ANDed)."""
        exprs = [_to_expr(p) for p in predicates]
        if not exprs:
            raise _invalid("filter() needs at least one predicate")
        for expr in exprs:
            if expr._alias is not None:
                raise _invalid("a filter predicate cannot be aliased")
        q = self._level(self._needs_wrap_for(exprs))
        q.filters.extend(exprs)
        return self._next(q)

    def select(self, *exprs: Union[str, Expr], **named: Union[str, Expr]) -> "LazyFrame":
        """Replace the projection with exactly these expressions."""
        projection = _projection_list(exprs, named)
        if not projection:
            raise _invalid("select() needs at least one column")
        q = self._level(self._needs_wrap_for(projection))
        q.projection = projection
        return self._next(q)

    def with_columns(
        self, *exprs: Expr, **named: Union[str, Expr]
    ) -> "LazyFrame":
        """Add or replace columns, keeping everything already projected."""
        additions = _projection_list(exprs, named)
        if not additions:
            raise _invalid("with_columns() needs at least one column")
        for expr in additions:
            if expr.output_name is None:
                raise _invalid(
                    "with_columns() needs a name for every expression",
                    hint="pass it as a keyword (name=expr) or use .alias(...)",
                )
        q = self._level(self._needs_wrap_for(additions))
        if q.projection is None:
            q.projection = [col("*")] + additions
        else:
            q.projection = q.projection + additions
        return self._next(q)

    def _needs_wrap_for(self, exprs: Sequence[Expr]) -> bool:
        """Can these expressions be added to the current level as-is?

        Aggregation, LIMIT and DISTINCT all close a level: anything after
        them acts on their *output*. Otherwise the only hazard is referring to
        a name this level computes, since SQL resolves WHERE and SELECT
        against the FROM, not against sibling projections.
        """
        return (
            self._q.group_by is not None
            or self._q.limit is not None
            or self._q.distinct
            or _conflicts(exprs, self._q.defined_aliases())
        )

    def sort(
        self,
        by: Union[str, Expr, Sequence[Union[str, Expr]]],
        descending: Union[bool, Sequence[bool]] = False,
    ) -> "LazyFrame":
        """Order the result."""
        items, _ = _order_items(by, descending)
        q = self._level(self._q.limit is not None)
        q.order_by = items
        return self._next(q)

    def limit(self, n: int, offset: int = 0) -> "LazyFrame":
        """Keep at most ``n`` rows, skipping ``offset``."""
        if not isinstance(n, int) or isinstance(n, bool) or n < 0:
            raise _invalid(f"limit must be a non-negative integer, got {n!r}")
        if not isinstance(offset, int) or isinstance(offset, bool) or offset < 0:
            raise _invalid(f"offset must be a non-negative integer, got {offset!r}")
        q = self._level(self._q.limit is not None)
        q.limit = n
        q.offset = offset or None
        return self._next(q)

    def head(self, n: int = 10) -> "LazyFrame":
        """Alias for :meth:`limit` -- still lazy."""
        return self.limit(n)

    def unique(self) -> "LazyFrame":
        """``SELECT DISTINCT`` over the current projection."""
        q = self._level(self._q.limit is not None)
        q.distinct = True
        return self._next(q)

    def group_by(self, *keys: Union[str, Expr]) -> "GroupBy":
        """Start an aggregation; finish it with :meth:`GroupBy.agg`."""
        exprs = _as_expr_list(list(keys))
        if not exprs:
            raise _invalid("group_by() needs at least one key")
        return GroupBy(self, exprs)

    # -- joins -------------------------------------------------------------

    def join(
        self,
        other: "LazyFrame",
        on: Optional[Union[str, Sequence[str]]] = None,
        how: str = "inner",
        left_on: Optional[Union[str, Sequence[str]]] = None,
        right_on: Optional[Union[str, Sequence[str]]] = None,
        predicate: Optional[Expr] = None,
    ) -> "LazyFrame":
        """Join two frames.

        Both sides become subqueries aliased ``l`` and ``r``. Those aliases
        are part of the contract: ``col("price", relation="l")`` and
        ``sql_expr('l."a" > r."b"')`` are how you reach a specific side.

        Column names are not deduplicated, so a ``SELECT *`` over a join of
        two tables sharing a name yields both. Project explicitly to avoid
        the ambiguity.
        """
        if not isinstance(other, LazyFrame):
            raise _invalid("join() takes another LazyFrame")
        if other._db is not self._db:
            raise _invalid("cannot join frames from different databases")
        if how not in _JOIN_KINDS:
            suggestion = _did_you_mean(how, _JOIN_KINDS)
            raise _invalid(
                f"unknown join type {how!r}; expected one of "
                + ", ".join(sorted(_JOIN_KINDS)),
                hint=f"did you mean {suggestion!r}?" if suggestion else None,
            )
        condition = _join_condition(how, on, left_on, right_on, predicate)
        left = _indent(self._q.render())
        right = _indent(other._q.render())
        source = (
            f"(\n{left}\n) AS {quote_ident('l')}\n"
            f"{_JOIN_KINDS[how]} (\n{right}\n) AS {quote_ident('r')}"
        )
        if condition:
            source += f"\n  ON {condition}"
        q = _Query(source)
        q.depth = max(self._q.depth, other._q.depth) + 1
        return self._next(q)

    def join_asof(
        self,
        other: "LazyFrame",
        on: Optional[str] = None,
        by: Optional[Union[str, Sequence[str]]] = None,
        direction: str = "backward",
        tolerance: Optional[int] = None,
        left_on: Optional[str] = None,
        right_on: Optional[str] = None,
    ) -> "LazyFrame":
        """ASOF join via the ``asof_join`` table function.

        For each left row, take the most recent right row at or before it
        (``'backward'``) or the first at or after it (``'forward'``). The
        result is LEFT and 1:1 with the left side.

        ``tolerance`` is an integer in the time column's raw units (for a
        ``timestamp[us]`` column, ``5000000`` is five seconds).

        Both sides must be plain tables with nothing applied yet, and neither
        may be pinned: the table function resolves both at latest, so a
        version pin here would be silently ignored rather than honoured.
        Filter or pin after the join, or materialise first.
        """
        if not isinstance(other, LazyFrame):
            raise _invalid("join_asof() takes another LazyFrame")
        if other._db is not self._db:
            raise _invalid("cannot join frames from different databases")
        for side, frame in (("left", self), ("right", other)):
            if not frame._q.is_bare_table():
                raise _invalid(
                    f"join_asof() needs a plain table on the {side} side, but "
                    "operations have already been applied",
                    hint="join first and filter afterwards, or materialise the "
                    "side with .collect() and write it back",
                )
            if frame._q.base_pin is not None:
                key, value = frame._q.base_pin
                raise _invalid(
                    f"join_asof() cannot honour the {side}-side pin "
                    f"{key}={value!r}: asof_join reads both tables at latest, "
                    "so the pin would be silently ignored",
                    hint="use db.sql() with the ASOF JOIN keyword form over "
                    "session-bound names, or materialise the pinned version "
                    "into its own table first",
                )
        if direction not in ("backward", "forward"):
            raise _invalid(
                f"direction must be 'backward' or 'forward', got {direction!r}"
            )
        left_key, right_key = _asof_keys(on, left_on, right_on)
        by_cols = _asof_by(by)
        args = [
            quote_string(self._q.base_table),
            quote_string(other._q.base_table),
            quote_string(left_key),
            quote_string(right_key),
        ]
        # The table function is positional, so a tolerance needs the by_cols
        # slot filled; the engine ignores empty entries.
        if by_cols or direction != "backward" or tolerance is not None:
            args.append(quote_string(by_cols))
        if direction != "backward" or tolerance is not None:
            args.append(quote_string(direction))
        if tolerance is not None:
            if not isinstance(tolerance, int) or isinstance(tolerance, bool):
                raise _invalid(
                    f"tolerance must be an integer in raw time units, "
                    f"got {type(tolerance).__name__}"
                )
            if tolerance < 0:
                raise _invalid(f"tolerance must not be negative, got {tolerance}")
            args.append(str(tolerance))
        return self._next(_Query(f"asof_join({', '.join(args)})"))

    # -- composition -------------------------------------------------------

    def pipe(self, function, *args, **kwargs):
        """``frame.pipe(f, ...)`` == ``f(frame, ...)`` -- for factor helpers."""
        return function(self, *args, **kwargs)

    # -- terminal ----------------------------------------------------------

    def sql(self) -> str:
        """The SQL this frame compiles to."""
        return self._q.render()

    def explain(self, analyze: bool = False, **limits) -> Any:
        """``EXPLAIN`` (or ``EXPLAIN ANALYZE``) for the compiled query."""
        prefix = "EXPLAIN ANALYZE" if analyze else "EXPLAIN"
        return self._db.sql(f"{prefix} {self.sql()}", **limits)

    def collect(
        self,
        memory_limit: Optional[int] = None,
        timeout: Optional[float] = None,
        max_rows: Optional[int] = None,
    ) -> Any:
        """Run the query and return a :class:`h5i_db.QueryResult`."""
        return self._db.sql(
            self.sql(),
            memory_limit=memory_limit,
            timeout=timeout,
            max_rows=max_rows,
        )

    def to_arrow(self, **limits):
        return self.collect(**limits).to_arrow()

    def to_pandas(self, **limits):
        return self.collect(**limits).to_pandas()

    def to_polars(self, **limits):
        return self.collect(**limits).to_polars()

    def schema(self):
        """The result schema, without reading any data (a ``LIMIT 0`` run)."""
        inner = _indent(self.sql())
        probe = (
            f"SELECT * FROM (\n{inner}\n) AS {quote_ident('_schema')} LIMIT 0"
        )
        return self._db.sql(probe).to_arrow().schema

    def __repr__(self) -> str:
        return f"<LazyFrame>\n{self.sql()}"


def _conflicts(exprs: Sequence[Expr], aliases: frozenset) -> bool:
    """Do these expressions reach for a name the current level computes?

    An opaque :func:`sql_expr` counts as reaching for everything, which is why
    it forces a subquery whenever the level defines any alias at all.
    """
    if not aliases:
        return False
    return any(e._refs is None or (e._refs & aliases) for e in exprs)


def _projection_list(exprs: Sequence[Any], named: dict) -> list:
    out = _as_expr_list(list(exprs))
    for name, value in named.items():
        expr = col(value) if isinstance(value, str) else _to_expr(value)
        out.append(expr.alias(name))
    return out


def _as_name_list(value, what: str) -> list:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    names = list(value)
    for name in names:
        if not isinstance(name, str):
            raise _invalid(f"{what} must be column names (strings)")
    return names


def _join_condition(how, on, left_on, right_on, predicate) -> str:
    if how == "cross":
        if on or left_on or right_on or predicate is not None:
            raise _invalid("a cross join takes no join keys")
        return ""
    if predicate is not None:
        if on or left_on or right_on:
            raise _invalid("pass either predicate= or on=/left_on=/right_on=")
        if not isinstance(predicate, Expr):
            raise _invalid("predicate must be an Expr")
        return predicate._render(0)
    if on is not None:
        if left_on or right_on:
            raise _invalid("pass either on= or left_on=/right_on=")
        left_names = right_names = _as_name_list(on, "on")
    else:
        left_names = _as_name_list(left_on, "left_on")
        right_names = _as_name_list(right_on, "right_on")
        if len(left_names) != len(right_names) or not left_names:
            raise _invalid(
                "left_on and right_on must name the same number of columns"
            )
    return " AND ".join(
        f'{quote_ident("l")}.{quote_ident(left)} = '
        f'{quote_ident("r")}.{quote_ident(right)}'
        for left, right in zip(left_names, right_names)
    )


def _asof_keys(on, left_on, right_on):
    if on is not None:
        if left_on or right_on:
            raise _invalid("pass either on= or left_on=/right_on=")
        if not isinstance(on, str):
            raise _invalid("join_asof(on=) must be a single column name")
        return on, on
    if not isinstance(left_on, str) or not isinstance(right_on, str):
        raise _invalid(
            "join_asof() needs on=, or both left_on= and right_on= as column names"
        )
    return left_on, right_on


def _asof_by(by) -> str:
    names = _as_name_list(by, "by")
    for name in names:
        if "," in name:
            raise _invalid(
                f"join_asof(by=) entry {name!r} must not contain a comma",
                hint="pass a list of names instead",
            )
    return ",".join(names)


class GroupBy:
    """Pending aggregation from :meth:`LazyFrame.group_by`."""

    __slots__ = ("_frame", "_keys")

    def __init__(self, frame: LazyFrame, keys: list):
        self._frame = frame
        self._keys = keys

    def agg(self, *exprs: Expr, **named: Expr) -> LazyFrame:
        """Aggregate. Group keys are projected alongside the aggregates."""
        aggs = _projection_list(exprs, named)
        if not aggs:
            raise _invalid(
                "agg() needs at least one aggregate",
                hint="use .count() for a plain group count",
            )
        # Aggregation is a hard boundary: always start a fresh level so the
        # keys and aggregates resolve against the previous stage's output.
        q = self._frame._level(self._frame._q.projection is not None)
        q.projection = list(self._keys) + aggs
        q.group_by = [
            quote_ident(k._alias) if k._alias is not None else k._render(0)
            for k in self._keys
        ]
        return self._frame._next(q)

    def count(self, name: str = "count") -> LazyFrame:
        """Rows per group."""
        return self.agg(count_star().alias(name))


__all__ = [
    "Expr",
    "GroupBy",
    "LazyFrame",
    "col",
    "count_star",
    "lit",
    "quote_ident",
    "quote_string",
    "sql_expr",
    "time_bucket",
    "vwap",
    "wavg",
    "when",
]
