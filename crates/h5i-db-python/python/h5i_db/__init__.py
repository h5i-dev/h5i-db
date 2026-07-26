"""h5i-db: embedded versioned time-series database.

Ergonomic wrapper over the native module. All tabular data crosses the
boundary as Arrow IPC streams, so any pyarrow >= 14 works.

    import h5i_db

    db = h5i_db.Database("market.db", create=True)
    db.create_table("trades", schema, time_column="ts")
    db.append("trades", table)                    # pyarrow.Table / batches
    df = db.sql("SELECT * FROM trades").to_pandas()
    old = db.read("trades", version=3)            # time travel
    plan = db.plan_delete_range("trades", start, end)   # previewable mutation
    plan.apply()                                  # or plan.discard()

Queries can also be built up rather than written as strings; the builder
compiles to the same SQL surface (see :mod:`h5i_db.dataframe`)::

    from h5i_db import col

    (db.table("trades", as_of="2026-07-01T00:00:00Z")
       .filter(col("symbol") == "AAPL")
       .group_by("symbol")
       .agg(col("price").mean().alias("px"))
       .collect())
"""

from __future__ import annotations

import datetime
import json
from dataclasses import dataclass
from typing import Any, Iterable, Optional, Sequence, Union

import pyarrow as pa
import pyarrow.ipc

from h5i_db._native import (  # noqa: F401
    ConflictError,
    CorruptionError,
    H5iError,
    InvalidInputError,
    LimitError,
    NativeDatabase,
    NotFoundError,
    PolicyError,
    StorageError,
    TimeoutError,  # noqa: A001 -- deliberate: h5i_db.TimeoutError subclasses H5iError
    __version__,
)
from h5i_db.dataframe import (  # noqa: F401
    Expr,
    GroupBy,
    LazyFrame,
    col,
    count_star,
    lit,
    sql_expr,
    time_bucket,
    vwap,
    wavg,
    when,
)

TableLike = Union[pa.Table, pa.RecordBatch, Sequence[pa.RecordBatch]]


def _to_ipc(data: TableLike) -> bytes:
    if isinstance(data, pa.RecordBatch):
        data = pa.Table.from_batches([data])
    elif not isinstance(data, pa.Table):
        data = pa.Table.from_batches(list(data))
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, data.schema) as writer:
        writer.write_table(data)
    return sink.getvalue().to_pybytes()


def _from_ipc(data: bytes) -> pa.Table:
    if not data:
        return pa.table({})
    with pa.ipc.open_stream(data) as reader:
        return reader.read_all()


def _schema_ipc(schema: pa.Schema) -> bytes:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, schema):
        pass
    return sink.getvalue().to_pybytes()


_EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)


def _datetime_to_ns(dt: datetime.datetime) -> int:
    """Exact nanoseconds since the epoch for an aware ``datetime``.

    Done with integer arithmetic on the timedelta rather than
    ``dt.timestamp() * 1e9``: a float64 holds 53 bits of mantissa, and a
    present-day instant in nanoseconds needs 61, so the obvious spelling
    silently rounds by a few hundred nanoseconds. That is enough to land a
    cutoff on the wrong side of a commit and hand back a different version of
    the world, which is the kind of wrong that looks like a data bug for a day.
    """
    delta = dt - _EPOCH
    return (delta.days * 86_400 + delta.seconds) * 1_000_000_000 + delta.microseconds * 1_000


def _as_of_ns(value: Optional[Union[str, int, datetime.datetime]]) -> Optional[int]:
    """Normalise a decision instant to nanoseconds since the epoch.

    A naive ``datetime`` is read as UTC rather than as local time: these
    timestamps select data, and silently applying the machine's timezone would
    move the cutoff by hours depending on where the code ran.
    """
    if value is None:
        return value
    # bool is an int subclass, and `as_of=True` is never what anyone meant.
    if isinstance(value, bool):
        raise InvalidInputError("as_of must be a string, datetime, or int ns; got bool")
    if isinstance(value, int):
        return value
    if isinstance(value, datetime.datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=datetime.timezone.utc)
        return _datetime_to_ns(dt)
    if isinstance(value, str):
        text = value.replace("Z", "+00:00")
        try:
            dt = datetime.datetime.fromisoformat(text)
        except ValueError as exc:
            raise InvalidInputError(f"as_of {value!r} is not an RFC3339 timestamp") from exc
        if not dt.tzinfo:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        return _datetime_to_ns(dt)
    raise InvalidInputError(f"as_of must be a string, datetime, or int ns; got {type(value).__name__}")


class QueryResult:
    """A materialised query result with convenience converters.

    The query has already run by the time you hold one of these. For a
    query that has *not* run yet, see :class:`h5i_db.LazyFrame`.
    """

    def __init__(self, table: pa.Table):
        self._table = table

    def to_arrow(self) -> pa.Table:
        return self._table

    def to_pandas(self):
        return self._table.to_pandas()

    def to_polars(self):
        import polars as pl  # optional dependency

        return pl.from_arrow(self._table)

    def __len__(self) -> int:
        return self._table.num_rows

    def __repr__(self) -> str:
        return repr(self._table)


@dataclass
class MutationPlan:
    """A previewable, not-yet-published mutation (plan/apply flow)."""

    _db: "Database"
    table: str
    plan_id: str
    summary: dict
    raw: dict

    @property
    def before_sample(self) -> Optional[pa.Table]:
        b64 = self.raw.get("before_sample_ipc_b64")
        if not b64:
            return None
        import base64

        return _from_ipc(base64.b64decode(b64))

    @property
    def after_sample(self) -> Optional[pa.Table]:
        b64 = self.raw.get("after_sample_ipc_b64")
        if not b64:
            return None
        import base64

        return _from_ipc(base64.b64decode(b64))

    def apply(self) -> dict:
        """Publish the plan. Raises on VersionConflict if the head moved."""
        return json.loads(self._db._native.apply_plan(self.table, self.plan_id))

    def discard(self) -> None:
        self._db._native.discard_plan(self.table, self.plan_id)


class Database:
    """An h5i-db database directory."""

    def __init__(self, path: str, create: bool = False, read_only: bool = False):
        self._native = NativeDatabase(path, create=create, read_only=read_only)
        self.path = path

    # -- lifecycle --------------------------------------------------------

    def close(self) -> None:
        """Release the native handle (idempotent).

        Later operations on this object raise ``H5iError`` with
        ``code == "closed"``.
        """
        self._native.close()

    @property
    def closed(self) -> bool:
        return self._native.closed

    def __enter__(self) -> "Database":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    # -- schema & tables --------------------------------------------------

    def create_table(
        self,
        name: str,
        schema: pa.Schema,
        time_column: Optional[str] = None,
        sort_key: Optional[Iterable[str]] = None,
    ) -> dict:
        if time_column is not None:
            # The time column must be non-nullable.
            idx = schema.get_field_index(time_column)
            if idx >= 0 and schema.field(idx).nullable:
                schema = schema.set(idx, schema.field(idx).with_nullable(False))
        return json.loads(
            self._native.create_table(
                name, _schema_ipc(schema), time_column, list(sort_key or [])
            )
        )

    def drop_table(self, name: str) -> None:
        self._native.drop_table(name)

    def schema(
        self,
        name: str,
        version: Optional[int] = None,
        as_of: Optional[str] = None,
        snapshot: Optional[str] = None,
    ) -> pa.Schema:
        """Schema of a table at a read point (latest by default)."""
        data = self._native.schema(name, version, as_of, snapshot)
        with pa.ipc.open_stream(data) as reader:
            return reader.schema

    def tables(self) -> list[str]:
        return json.loads(self._native.tables())

    def versions(self, name: str) -> list[dict]:
        return json.loads(self._native.versions(name))

    # -- writes -----------------------------------------------------------

    def write(self, name: str, data: TableLike, **kw: Any) -> dict:
        return json.loads(self._native.ingest(name, _to_ipc(data), mode="write", **kw))

    def append(self, name: str, data: TableLike, **kw: Any) -> dict:
        return json.loads(self._native.ingest(name, _to_ipc(data), mode="append", **kw))

    def restore(self, name: str, version: int) -> dict:
        return json.loads(self._native.restore(name, version))

    # -- previewable mutations ---------------------------------------------

    def plan_replace_range(
        self,
        name: str,
        start: int,
        end: int,
        data: Optional[TableLike] = None,
        note: Optional[str] = None,
    ) -> MutationPlan:
        raw = json.loads(
            self._native.plan_replace_range(
                name, start, end, _to_ipc(data) if data is not None else None, note
            )
        )
        return MutationPlan(
            _db=self,
            table=name,
            plan_id=raw["plan_id"],
            summary=raw["summary"],
            raw=raw,
        )

    def plan_delete_range(
        self, name: str, start: int, end: int, note: Optional[str] = None
    ) -> MutationPlan:
        return self.plan_replace_range(name, start, end, None, note)

    def list_plans(self, name: str) -> list[MutationPlan]:
        """Pending (not yet applied/discarded) mutation plans for a table."""
        raws = json.loads(self._native.list_plans(name))
        return [
            MutationPlan(
                _db=self,
                table=name,
                plan_id=r["plan_id"],
                summary=r.get("summary", {}),
                raw=r,
            )
            for r in raws
        ]

    # -- reads --------------------------------------------------------------

    def sql(
        self,
        query: str,
        memory_limit: Optional[int] = None,
        timeout: Optional[float] = None,
        max_rows: Optional[int] = None,
    ) -> QueryResult:
        """Run SQL.

        ``timeout`` is a deadline in seconds (raises :class:`TimeoutError`
        and cancels execution). ``max_rows`` raises :class:`LimitError` as
        soon as the result exceeds it — execution stops early rather than
        silently truncating.
        """
        return QueryResult(
            _from_ipc(self._native.sql(query, memory_limit, timeout, max_rows))
        )

    def table(
        self,
        name: str,
        version: Optional[int] = None,
        as_of: Optional[str] = None,
        snapshot: Optional[str] = None,
    ) -> LazyFrame:
        """Start a lazy query against a table.

        Returns a :class:`LazyFrame` that builds SQL; nothing runs until
        ``.collect()`` (or another terminal method). ``.sql()`` shows what
        it compiles to.

        Pass at most one read point. With one, the frame reads through the
        ``h5i()`` table function, so version pins apply exactly as they do to
        hand-written SQL. Without one, it reads the bare table name, which is
        snapshot-bound for the duration of the query.

            db.table("trades").filter(col("px") > 0).collect()
            db.table("trades", version=42).sql()
        """
        return LazyFrame._from_table(self, name, version, as_of, snapshot)

    def read(
        self,
        name: str,
        version: Optional[int] = None,
        as_of: Optional[str] = None,
        snapshot: Optional[str] = None,
        columns: Optional[list[str]] = None,
        time_start: Optional[int] = None,
        time_end: Optional[int] = None,
        limit: Optional[int] = None,
        timeout: Optional[float] = None,
    ) -> pa.Table:
        return _from_ipc(
            self._native.read(
                name,
                version,
                as_of,
                snapshot,
                columns,
                time_start,
                time_end,
                limit,
                timeout,
            )
        )

    def arrival_delta(
        self,
        query: str,
        version: Optional[int] = None,
        as_of: Optional[str] = None,
        snapshot: Optional[str] = None,
        tolerance: Optional[float] = None,
    ) -> dict:
        """How much this query's answer moved because data arrived late.

        Runs ``query`` twice, at the current head and at an earlier read
        point, and returns the difference as a dict. Specify exactly one of
        ``version`` / ``as_of`` (RFC3339 availability timestamp) / ``snapshot``
        as the earlier point; ``tolerance`` is the per-cell numeric noise floor
        (default ``1e-9``).

        A measurement, not a verdict. ``changed`` says the answer moved, which
        means late-arriving or restated rows affected it. It sees only that
        shape of look-ahead: a signal reading its own bar leaves it unmoved, so
        no value of the delta clears a query. Read ``vacuous`` first - when the
        two read points resolve to the same version the zero is arithmetic, not
        evidence - and ``notes`` alongside the numbers.
        """
        return json.loads(
            self._native.arrival_delta(query, version, as_of, snapshot, tolerance)
        )

    # -- forks ---------------------------------------------------------------

    def create_fork(
        self,
        name: str,
        note: Optional[str] = None,
        as_of: Optional[Union[str, int, "datetime.datetime"]] = None,
        meta: Optional[dict] = None,
    ) -> dict:
        """Create a writable workspace over a pinned view of every table.

        Costs one small metadata object and copies no data, so one dataset can
        back many parallel agents. ``as_of`` (an RFC3339 string, a datetime, or
        nanoseconds since the epoch) pins each table at its last version
        committed at or before that instant, giving a workspace over a frozen
        past; tables that did not exist then are not pinned. Note that strings
        and datetimes carry microsecond resolution while commits are stamped in
        nanoseconds -- pass an int of nanoseconds when you need to name one
        exact commit rather than a moment.

        ``meta`` is carried verbatim and never interpreted -- use it to tie a
        fork back to whatever run or hypothesis produced it.
        """
        return json.loads(
            self._native.create_fork(
                name,
                note,
                _as_of_ns(as_of),
                json.dumps(meta) if meta is not None else None,
            )
        )

    def fork(self, name: str) -> "Database":
        """A handle scoped to ``name``.

        Table lookups then resolve to the fork's own tables first and fall back
        to its pinned view of the base, so existing code runs unchanged inside
        a fork. Writes to a base table's name transparently copy-on-write into
        the fork; the base is never modified.
        """
        forked = Database.__new__(Database)
        forked._native = self._native.open_fork(name)
        forked.path = self.path
        return forked

    @property
    def fork_name(self) -> Optional[str]:
        """The fork this handle is scoped to, or ``None`` on a base handle."""
        return self._native.fork_name()

    def forks(self) -> list[dict]:
        """Every fork, with what it owns and what it holds back from
        reclamation (``bytes_pinned``)."""
        return json.loads(self._native.list_forks())

    def fork_info(self, name: str) -> dict:
        return json.loads(self._native.fork_info(name))

    def fork_diff(self, name: str, table: Optional[str] = None) -> dict:
        """What a fork changed, computed from manifests alone."""
        return json.loads(self._native.fork_diff(name, table))

    def promote(self, fork: str, table: str) -> dict:
        """Promote one of a fork's tables into the base database.

        Compare-and-swap against the version the fork was created from: the
        first promote wins, and a later one raises rather than merging. The
        conflict unit is the whole table.
        """
        return json.loads(self._native.promote(fork, table))

    def drop_fork(self, name: str) -> int:
        """Delete a fork and everything it owns; returns the table count."""
        return self._native.drop_fork(name)

    # -- maintenance ----------------------------------------------------------

    def snapshot(self, name: str, tables: Optional[list[str]] = None, note: Optional[str] = None) -> dict:
        return json.loads(self._native.create_snapshot(name, tables or [], note))

    def vacuum(self, table: Optional[str] = None, grace_seconds: int = 3600, apply: bool = False) -> dict:
        return json.loads(self._native.vacuum(table, grace_seconds, apply))

    def verify(self, name: str, deep: bool = False) -> dict:
        return json.loads(self._native.verify(name, deep))

    def compact(self, name: str, note: Optional[str] = None) -> dict:
        return json.loads(self._native.compact(name, note))

    # -- mutation policy -----------------------------------------------------

    def policy(self) -> dict:
        """The database mutation policy as a dict of boolean flags."""
        return json.loads(self._native.get_policy())

    def set_policy(self, policy: Optional[dict] = None, **flags: bool) -> dict:
        """Update the mutation policy; unspecified flags keep their value.

        The merge is atomic (read-modify-write under the database metadata
        lock). Unknown flags raise :class:`InvalidInputError`. Returns the
        merged policy that was stored.
        """
        updates = dict(policy or {})
        updates.update(flags)
        return json.loads(self._native.update_policy(json.dumps(updates)))

    # -- data-safety policy --------------------------------------------------

    def data_policy(self, table: str) -> Optional[dict]:
        """A table's data-safety policy as a dict, or ``None`` when unset.

        An unset policy is the default: writes are unconstrained and pay no
        enforcement cost.
        """
        return json.loads(self._native.get_data_policy(table))

    def set_data_policy(self, table: str, policy: dict) -> dict:
        """Install (overwrite) a table's data-safety policy; returns it.

        ``policy`` is a typed document, e.g.::

            db.set_data_policy("trades", {"constraints": [
                {"name": "positive_price",
                 "predicate": {"compare": {"column": "price", "op": "gt",
                                           "value": {"float": 0.0}}},
                 "on_fail": "reject"}]})

        Predicates compose ``not_null`` / ``compare`` / ``in_set`` with
        ``and`` / ``or`` / ``not``; ``on_fail`` is ``"reject"`` or ``"warn"``.
        Constraints are enforced fail-closed on every write and at plan time; a
        violation raises :class:`InvalidInputError` (``data_policy_violation``).
        """
        return json.loads(self._native.set_data_policy(table, json.dumps(policy)))

    def clear_data_policy(self, table: str) -> None:
        """Remove a table's data-safety policy (writes become unconstrained)."""
        self._native.clear_data_policy(table)


__all__ = [
    "Database",
    "QueryResult",
    "MutationPlan",
    "LazyFrame",
    "GroupBy",
    "Expr",
    "col",
    "lit",
    "sql_expr",
    "count_star",
    "when",
    "time_bucket",
    "vwap",
    "wavg",
    "H5iError",
    "NotFoundError",
    "ConflictError",
    "InvalidInputError",
    "PolicyError",
    "CorruptionError",
    "LimitError",
    "TimeoutError",
    "StorageError",
    "__version__",
]
