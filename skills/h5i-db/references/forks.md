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

Database-wide commands (`snapshot`, `vacuum`, `set-retention`) refuse `--fork`
and say so: they move state a fork's siblings depend on. `fork create` is not
one of them — with `--fork` it nests (see [Nesting forks](#nesting-forks)).

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

## Nesting forks

A fork can be forked. Pass `--fork` to `fork create` and the child is created
inside that fork, pinning its tables the way a top-level fork pins the
database's:

```bash
h5i-db fork create market.db trunk
h5i-db fork create market.db branch-a --fork trunk    # child of trunk
```

The child sees the database, plus whatever the parent had done at the moment
it was taken, plus its own work. It stays frozen against parent commits made
afterwards, so a parent and its children never disturb each other.

This is the shape to use for a search: refine a branch, evaluate it, fork the
promising child, discard the rest. Flat forks off the database cannot express
it, because each level needs to build on the last.

Rules worth knowing before you plan around it:

- **Promote moves work up exactly one level.** `promote branch-a --table t`
  lands on `trunk`, never on the database. A table the child *created*
  likewise appears in the parent, not in the base catalog. To get work all the
  way to main, promote at each level.
- **A parent with live children cannot be dropped.** The error names them.
  Drop from the leaves up, or drop the whole subtree at once.
- **Depth is capped at 32.** Reads do not slow down with depth — a fork names
  its segments by path, so there is no chain to walk — the cap is only a
  runaway guard.
- `fork show` reports `parent` and `depth`.

## Comparing forks in one query

`forks('table')` reads a table across every fork at once and adds a `__fork`
column naming the branch each row came from:

```bash
h5i-db query market.db "SELECT __fork, count(*), avg(price) FROM forks('trades') GROUP BY __fork"
```

Use this instead of scanning each fork and stitching the results together. It
is not just shorter: forks share their segments, so a segment several forks
can see is read once however many reference it, and only what each fork
actually changed is read separately. Comparing fifty branches costs about what
reading one does, plus their deltas.

Narrow it with a comma-separated list — `forks('trades', 'branch-a,branch-b')`
— and note that named forks must exist (a typo is an error, not an omission).
Forks whose schema for the table has diverged are refused; use `fork diff` to
see how they differ.

From Python: `db.fork_scan("trades")` returns a lazy frame, and
`db.fork_scan("trades", ["branch-a"])` narrows it.

## Creating and discarding many at once

For fanouts, batch both ends rather than looping:

```bash
h5i-db fork create market.db trial --count 200    # trial-0000 … trial-0199
h5i-db fork drop market.db trial-0000 trial-0001  # several names at once
```

`--count` resolves the base once and stamps one pin set onto every fork, so
the cost is one pass over the catalog however many branches you make. Names
must all be free before anything is written, so a collision leaves no partial
fanout. A batch drop stops at a name that does not exist rather than skipping
it — forks dropped before that point stay dropped, so re-run with what
`fork list` still shows.

Python: `db.fork_many("trial", 200)`, `db.create_forks([...])`,
`db.drop_forks([...])`, and `db.fork_names()` for a cheap list of names.
