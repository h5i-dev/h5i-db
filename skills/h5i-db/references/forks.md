# Forks: parallel workspaces over one dataset

A fork is a writable workspace pinned to a version of the base database. Use
one when you are about to try something you may want to throw away, or when
several lines of work need to run against the same data at once without
tripping over each other.

```bash
h5i-db fork create market.db agent-01 --note "hypothesis 1"
h5i-db ingest market.db features out.parquet --fork agent-01
h5i-db query market.db "SELECT count(*) FROM features" --fork agent-01
```

Creating a fork copies no data and takes constant time regardless of dataset
size, so there is no reason to hesitate over making one. Twenty forks of a
50 GB table store 50 GB once.

## The `--fork` flag

Every data command takes `--fork <name>`. Inside a fork:

- names resolve to the fork's own tables first, then to its pinned view of the
  base, so existing commands work unchanged;
- writing to a base table's name transparently copies that table into the fork
  first — the base is never modified;
- reads see the base exactly as it was when the fork was created. Commits
  landing on the base meanwhile are invisible, which is what makes a long
  analysis reproducible.

Database-wide commands (`snapshot`, `vacuum`, `fork create`) refuse `--fork`
and say so. There is no fork of a fork.

## Finishing: promote or drop

Most speculative work gets discarded, and that is the cheap path:

```bash
h5i-db fork drop market.db agent-02       # deletes the fork and its tables
```

To keep it, look first and then promote one table:

```bash
h5i-db fork diff market.db agent-01                       # what changed
h5i-db fork promote market.db agent-01 --table features   # land it on the base
```

`fork diff` is computed from metadata alone and reports rows and bytes before
and after, how many segments the fork added, and how many it still shares with
the base. `base_moved` tells you whether a promote would currently conflict.

**Promotion is first-commit-wins.** It compare-and-swaps against the version
the fork started from. If the base moved, you get exit code 3 with
`code: "promote_conflict"` and `retryable: false` — do not retry it. The work
was computed against a base that no longer exists, so either re-fork from the
current head and re-run, or drop the fork.

**Compaction does not cost you the promote.** If every intervening base commit
was a compaction, the base's layout changed but its rows did not, so the
promote is replayed onto the new layout automatically and the result carries
`rebased_from`. The exception is a fork that *deleted* rows it inherited:
those edits cannot be replayed from metadata alone, so that case still
conflicts and the message says so.

The conflict unit is the whole table. There is no row-level merge, so do not
plan a workflow around two forks landing complementary changes to one table.

## Forking the past

`--as-of` pins the base at a past instant instead of the present:

```bash
h5i-db fork create market.db backtest --as-of 2026-03-01T00:00:00Z
```

That is a workspace where the base is frozen but you can still materialise
features and intermediate results — the writable counterpart of a read-only
historical pin. Note that this freezes *commits*, which is not the same as the
leakage guarantee: a fork does not stop a query from reading rows stamped in
the future. For look-ahead protection keep using `--decision-time` /
`--embargo` inside the fork (→ [research-mode.md](research-mode.md)).

A timestamp before the database's whole history is rejected rather than
producing an empty workspace.

## Housekeeping

A live fork pins the base versions it reads, so those versions cannot be
expired and their storage cannot be reclaimed while it exists. Nothing breaks;
reclamation is simply deferred. `h5i-db fork list market.db` shows every fork
with `bytes_own` (what it wrote) and `bytes_pinned` (what it is holding back),
which is how you find workspaces someone forgot to drop.

Dropping the base table under a live fork is refused; drop the fork first.

## Python

```python
db.create_fork("agent-01", note="hypothesis 1")
work = db.fork("agent-01")          # a Database handle, scoped to the fork
work.append("features", table)
db.fork_diff("agent-01")
db.promote("agent-01", "features")  # raises ConflictError if the base moved
db.drop_fork("agent-01")
```

`create_fork(..., as_of=...)` accepts an RFC3339 string, a `datetime`, or an
int of nanoseconds. Strings and datetimes carry microsecond resolution; pass
nanoseconds when you need to name one exact commit.
