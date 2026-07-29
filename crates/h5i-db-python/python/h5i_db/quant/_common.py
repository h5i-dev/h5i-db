"""Shared plumbing for the quant layer: pins, provenance, SQL helpers.

Everything user-facing in :mod:`h5i_db.quant` runs against a *pinned* read
(ROADMAP_QUANT.md P1) and carries a provenance header (P2). Those two ideas
live here so every module spells them the same way.
"""

from __future__ import annotations

import datetime as _datetime
import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Mapping, Optional, Sequence, Union

from ..dataframe import LazyFrame, quote_ident, quote_string

__all__ = [
    "Pin",
    "Provenance",
    "quote_ident",
    "quote_string",
    "resolve_source",
    "sql_number",
]


def sql_number(value: Union[int, float]) -> str:
    """Render a Python number as SQL text.

    Floats go through ``repr`` so a round trip is exact; the SQL text is the
    shortest string that reads back as the same double.
    """
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"expected a number, got {type(value).__name__}")
    if isinstance(value, int):
        return str(value)
    if value != value or value in (float("inf"), float("-inf")):
        raise ValueError(f"cannot render {value!r} as SQL")
    return repr(float(value))


@dataclass(frozen=True)
class Pin:
    """The read point every quant computation runs against.

    At most one of ``version`` / ``as_of`` / ``snapshot`` may be set; that is
    the storage-level pin and it is passed straight through to
    :meth:`h5i_db.Database.table`. ``event_time_cutoff`` is the decision-time
    embargo: every source read is additionally restricted to
    ``time_column <= cutoff``, so nothing a strategy or factor could not have
    known at that instant enters the computation.

    An unpinned read is allowed (research against head), and reports say so
    rather than implying reproducibility that is not there.
    """

    version: Optional[Union[int, Mapping[str, int]]] = None
    as_of: Optional[str] = None
    snapshot: Optional[str] = None
    event_time_cutoff: Optional[Union[int, str, _datetime.datetime]] = None

    def __post_init__(self) -> None:
        set_axes = [
            name
            for name in ("version", "as_of", "snapshot")
            if getattr(self, name) is not None
        ]
        if len(set_axes) > 1:
            raise ValueError(
                "pass at most one read point; got " + ", ".join(sorted(set_axes))
            )

    @property
    def is_pinned(self) -> bool:
        """True when the storage read point is explicit (reproducible)."""
        return any(
            getattr(self, name) is not None
            for name in ("version", "as_of", "snapshot")
        )

    def version_for(self, table: Optional[str]) -> Optional[int]:
        """The version pin for one table.

        Versions are *per table* in h5i: table A's version 2 has nothing to
        do with table B's. A bare integer therefore means "read every source
        at that version", which is only meaningful when they really do share
        a lineage; a mapping pins them independently. To pin several tables
        to one instant, prefer ``as_of`` or ``snapshot``, which are global by
        construction.
        """
        version = self.version
        if version is None or isinstance(version, int):
            return version
        if table is None:
            raise ValueError(
                "a per-table version mapping needs a named table; pass the "
                "source as a table name rather than a frame"
            )
        if table not in version:
            raise ValueError(
                f"no version pinned for table {table!r}; the mapping covers "
                + ", ".join(sorted(map(repr, version)))
            )
        return version[table]

    def table_kwargs_for(self, table: Optional[str]) -> dict:
        """Keyword arguments for :meth:`h5i_db.Database.table`."""
        return {
            "version": self.version_for(table),
            "as_of": self.as_of,
            "snapshot": self.snapshot,
        }

    def cutoff_sql(self) -> Optional[str]:
        """The cutoff rendered as a SQL literal, or None."""
        cutoff = self.event_time_cutoff
        if cutoff is None:
            return None
        if isinstance(cutoff, bool):
            raise TypeError("event_time_cutoff must be a timestamp or integer")
        if isinstance(cutoff, _datetime.datetime):
            return f"TIMESTAMP {quote_string(cutoff.isoformat())}"
        if isinstance(cutoff, str):
            return f"TIMESTAMP {quote_string(cutoff)}"
        if isinstance(cutoff, int):
            return str(cutoff)
        raise TypeError(
            "event_time_cutoff must be a datetime, RFC3339 string, or integer"
        )

    def to_dict(self) -> dict:
        cutoff = self.event_time_cutoff
        if isinstance(cutoff, _datetime.datetime):
            cutoff = cutoff.isoformat()
        version = self.version
        if isinstance(version, Mapping):
            version = dict(sorted(version.items()))
        return {
            "version": version,
            "as_of": self.as_of,
            "snapshot": self.snapshot,
            "event_time_cutoff": cutoff,
            "pinned": self.is_pinned,
        }

    @classmethod
    def coerce(cls, value: Optional["Pin"], **kwargs: Any) -> "Pin":
        """Accept either a ``Pin`` or loose keyword arguments."""
        if value is not None:
            if not isinstance(value, Pin):
                raise TypeError("pin= takes a Pin")
            if any(v is not None for v in kwargs.values()):
                raise ValueError(
                    "pass either pin= or the individual read-point arguments, "
                    "not both"
                )
            return value
        return cls(**kwargs)


@dataclass(frozen=True)
class Provenance:
    """The header that makes a number citable (ROADMAP_QUANT.md P2).

    ``digest`` is a stable hash of everything that determines the result:
    the pin, the parameters, and the compiled SQL. Two runs that agree on the
    digest are the same computation, so a report can be regenerated and
    checked rather than trusted.
    """

    kind: str
    pin: Pin
    parameters: dict = field(default_factory=dict)
    sources: dict = field(default_factory=dict)
    sql: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "kind": self.kind,
            "pin": self.pin.to_dict(),
            "parameters": self.parameters,
            "sources": self.sources,
            "digest": self.digest,
        }

    @property
    def digest(self) -> str:
        payload = json.dumps(
            {
                "kind": self.kind,
                "pin": self.pin.to_dict(),
                "parameters": self.parameters,
                "sources": self.sources,
                "sql": self.sql,
            },
            sort_keys=True,
            default=str,
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def warnings(self) -> list:
        out = []
        if not self.pin.is_pinned:
            out.append(
                "unpinned read: this ran against the latest version, so the "
                "numbers are not reproducible from this header alone"
            )
        return out


def resolve_source(
    db: Any,
    source: Union[str, LazyFrame],
    pin: Pin,
    time_column: str,
    what: str,
) -> tuple:
    """Turn a table name or frame into ``(sql_text, description)``.

    A string is read through the pin. A :class:`LazyFrame` is taken as given
    (the caller pinned it, or deliberately did not) and only the event-time
    cutoff is applied on top, so an already-projected frame can be handed in
    when column names do not match the defaults.
    """
    if isinstance(source, LazyFrame):
        frame = source
        described: dict = {"kind": "frame"}
    elif isinstance(source, str):
        frame = db.table(source, **pin.table_kwargs_for(source))
        described = {"kind": "table", "name": source}
    else:
        raise TypeError(
            f"{what} must be a table name or a LazyFrame, got "
            f"{type(source).__name__}"
        )
    sql_text = frame.sql()
    cutoff = pin.cutoff_sql()
    if cutoff is not None:
        sql_text = (
            f"SELECT * FROM (\n{_indent(sql_text)}\n) AS {quote_ident('_src')}\n"
            f"WHERE {quote_ident(time_column)} <= {cutoff}"
        )
    described["pin"] = pin.to_dict()
    return sql_text, described


def _indent(text: str, spaces: int = 2) -> str:
    pad = " " * spaces
    return "\n".join(pad + line if line else line for line in text.splitlines())


def indent(text: str, spaces: int = 2) -> str:
    return _indent(text, spaces)


def as_columns(value: Union[str, Sequence[str], None]) -> list:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    return list(value)
