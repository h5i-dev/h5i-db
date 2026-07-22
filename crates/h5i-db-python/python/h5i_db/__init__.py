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
"""

from __future__ import annotations

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


class QueryResult:
    """Lazy holder of a query result with convenience converters."""

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


__all__ = [
    "Database",
    "QueryResult",
    "MutationPlan",
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
