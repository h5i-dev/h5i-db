# h5i-db Operations Guide

How to run an h5i-db database in production: backup and restore, vacuum and
compaction cadence, plan hygiene, disk-usage math, filesystem caveats, and
the torn-HEAD recovery runbook.

Everything below refers to the on-disk layout (see `crates/h5i-db-core/src/layout.rs`):

```text
<root>/
  FORMAT                              # format + minimum reader version
  catalog/tables/<hash-of-name>.json  # table name -> table UUID
  snapshots/<hash-of-name>.json       # snapshot name -> {table uuid: version}
  tables/<table-uuid>/
    HEAD                              # the ONLY mutable object per table
    spec/<revision>.json              # schema revisions
    manifests/<seq>.json              # one immutable manifest per version
    segments/<segment-uuid>.parquet   # immutable data
```

Two properties make operations simple:

- **Everything except `HEAD` is immutable.** Segments, manifests, specs,
  catalog entries, and snapshots are write-once. Only `HEAD` (one small JSON
  file per table) ever changes, and it changes by atomic rename.
- **History is a hash chain.** Each manifest records the blake3 checksum of
  its parent's bytes, and `HEAD` records the checksum of the manifest it
  points at, so any prefix of history is self-verifying
  (`h5i-db verify <db> <table> [--deep]`).

---

## Backup

Immutable objects mean a plain file copy is a correct backup **if you copy in
the right order and don't run destructive maintenance concurrently**.

### Procedure

1. **Don't run `vacuum --apply` (or `drop-table`) during the backup window.**
   Vacuum is the only thing that deletes objects; a copy that races it can
   miss files it already indexed. Plain writers are safe to leave running.
2. Copy in this order (older references first, `HEAD` before the objects it
   references is the one order that is *wrong* — copy `HEAD` **first**, per
   table, then its immutable objects):

   ```bash
   DB=/data/market.db BK=/backup/market.db-$(date +%F)
   mkdir -p "$BK"
   cp "$DB/FORMAT" "$BK/"
   cp -r "$DB/catalog" "$DB/snapshots" "$BK/" 2>/dev/null || true
   for t in "$DB"/tables/*/; do
     b="$BK/tables/$(basename "$t")"; mkdir -p "$b"
     cp "$t/HEAD" "$b/"                        # 1. pin the version to back up
     cp -r "$t/spec" "$t/manifests" "$b/"      # 2. immutable metadata
     cp -r "$t/segments" "$b/"                 # 3. immutable data
   done
   ```

   Why this order works: the copied `HEAD` names some sequence *S*. Every
   manifest `0..=S` and every segment they reference already existed when
   `HEAD` was copied, and (with vacuum paused) nothing deletes them — so all
   of them are present in the later copy steps. Commits that land *during*
   the backup produce manifests `> S`; they may be half-copied, which is
   harmless: they are unreachable from the copied `HEAD` and are exactly what
   `vacuum` classifies as debris.
3. Skip transient files if you meet them: `HEAD.lock`, `HEAD.tmp.*`.
4. Validate the backup before trusting it:

   ```bash
   h5i-db tables "$BK"
   h5i-db verify "$BK" <table> --deep   # re-reads every segment checksum
   ```

Filesystem/LVM/ZFS snapshots are also fine (crash-consistent is enough: the
commit protocol fsyncs data before `HEAD` moves, so any point-in-time image
is a valid database).

### Restore

A backup **is** a database. Point the CLI at it, or copy it back into place
and run `h5i-db verify` per table. There is no replay/WAL step.

Note that restoring an old backup rewinds *all* tables to the backup time;
to rewind a single table inside a live database, prefer
`h5i-db restore <db> <table> <version>` — that is what versioning is for.

---

## Vacuum

`h5i-db vacuum <db> [table] [--grace-seconds N] [--apply]` removes
unreachable objects: segments referenced by no committed manifest and no
live mutation plan, manifests above `HEAD` (crashed-writer leftovers), and
`*.lock` / `HEAD.tmp.*` debris. Without `--apply` it is a dry run.

Guidance:

- **Always review a dry run first** in scripted maintenance
  (`vacuum` then `vacuum --apply` on the same candidate list you inspected).
- **Grace period** (default 3600 s): objects younger than this are never
  touched. Set it comfortably above your *longest* ingest or plan-prepare
  duration — staged segments exist on disk before the commit that references
  them, and a grace period shorter than a slow bulk load can delete a
  commit-in-progress out from under it.
- **Cadence**: daily or weekly is plenty. Debris accrues only from crashed
  or conflicted writers and discarded/expired plans; a healthy append-only
  workload generates almost none.
- **Never run two `vacuum --apply` concurrently**, and don't run it during
  backups (above).

## Compaction

Frequent small appends produce many small segments; queries then pay
per-segment open/prune cost. `h5i-db compact <db> <table>` rewrites them
into target-sized segments as a new version (row count is verified to be
preserved; the commit aborts otherwise).

- Compact when a table accumulates hundreds of small segments, or when
  `versions` shows the segment count growing much faster than data volume.
- Compaction does **not** free disk: the pre-compaction segments remain
  pinned by historical versions (see disk math below). It is a *query
  performance* tool, not a space reclaimer.

## Mutation-plan hygiene

Plans (`--plan` on `delete-range` / `replace-range`) stage their segments at
plan time and protect them from vacuum until applied, discarded, or expired
(TTL: **7 days**, `PLAN_TTL_SECONDS`).

- List pending plans per table: `h5i-db plan list <db> <table>`.
- Discard plans you won't apply (`plan discard`) — an applied-or-discarded
  plan's staged segments become vacuum candidates immediately; an abandoned
  plan holds its staged bytes for the full 7 days.
- Applying a plan after the table head moved fails with a conflict (409 from
  the UI); re-plan instead of retrying.

## Disk-usage math

Nothing is ever deleted except by vacuum, and vacuum only deletes
*unreachable* objects — every committed version pins its segments forever
(version retention/GC is roadmap work). Practical consequences:

- **Append-only tables**: disk ≈ total data ever appended, plus one manifest
  per commit. The manifest lists every live segment, so manifest overhead is
  O(segments) per commit — another reason to compact and to batch appends.
- **`replace-range` / `delete-range` / `compact` / `write`**: each rewrites
  or re-references segments; the *old* segments stay pinned by history. A
  daily full `write` of a 1 GiB table costs ~365 GiB/year until retention
  exists.
- Quick audit: `du -sh <db>/tables/*/segments` vs.
  `h5i-db tables <db>` row counts shows how much is history vs. head.

## Filesystem caveats

- **Local ext4/xfs/apfs/NTFS**: the supported case. Durability relies on
  fsync-before-`HEAD`-swap plus atomic rename — standard semantics on all of
  these.
- **NFS and other network filesystems**: not recommended for multi-host
  access. Writer exclusion uses an OS-level `flock` on an open descriptor;
  its cross-host semantics depend on the NFS version and lock-daemon setup
  (NFSv3 needs a working `lockd`; some mounts silently downgrade locks to
  local-only). Close-to-open cache consistency can also delay another host's
  view of a renamed `HEAD`. Single-host access to an NFS mount works but
  still trusts the server's fsync honesty.
- **WSL2**: keep databases on the Linux filesystem (e.g. `~/data/…`,
  ext4). On `/mnt/c` (drvfs/9p) fsync and rename atomicity are not
  faithfully passed through to Windows, which voids the crash-safety
  guarantees. (This repository's own benchmarks are run from the ext4 side
  for the same reason.)
- **Containers**: overlayfs upper layers are fine; bind-mount the database
  directory to a real volume for anything you care about.

---

## Runbook: torn or corrupt HEAD

**Should not happen** on a supported filesystem — `HEAD` is replaced by
write-temp → fsync → rename → directory-fsync — so treat an occurrence as a
signal of filesystem misbehavior (see caveats above), not routine wear.

### Symptoms

- Any command fails with `Corruption { object: ".../HEAD", ... }`
  ("HEAD parse error"), or
- `HEAD` parses but `verify` reports `manifest missing` /
  `checksum mismatch` at the head sequence, or readers fail opening the
  manifest `HEAD` points at.

### Diagnosis

```bash
h5i-db verify <db> <table>        # walks the checksum chain from HEAD back
cat <db>/tables/<uuid>/HEAD       # {"format":1,"table_id":"…","sequence":N,
                                  #  "manifest_checksum":"<blake3-hex>"}
ls <db>/tables/<uuid>/manifests/  # zero-padded sequence-numbered JSON
```

Find the table's UUID via the catalog: `h5i-db tables <db>` then match, or
`grep -l '<name>' <db>/catalog/tables/*.json`.

### Recovery

1. **Stop writers** for the affected table.
2. **Find the newest intact manifest.** Starting from the highest file in
   `manifests/`, compute each candidate's checksum and walk its parent
   chain:

   ```bash
   b3sum <db>/tables/<uuid>/manifests/<seq>.json   # blake3 of file bytes
   ```

   A manifest is a good recovery point if it parses, its `parent_checksum`
   matches the blake3 of the parent file, and every segment `path` it lists
   exists with the recorded byte size. (This is exactly the check `verify`
   runs from `HEAD`; you are doing it from a candidate sequence instead.)
3. **Rewrite HEAD** to point at that manifest — `HEAD` is four fields of
   JSON; `manifest_checksum` must be the blake3 hex of the chosen manifest's
   exact bytes:

   ```bash
   SEQ=…; UUID=…; DB=…
   SUM=$(b3sum --no-names "$DB/tables/$UUID/manifests/$(printf %012d $SEQ).json")
   printf '{"format":1,"table_id":"%s","sequence":%d,"manifest_checksum":"%s"}' \
       "$UUID" "$SEQ" "$SUM" > /tmp/HEAD.new
   mv /tmp/HEAD.new "$DB/tables/$UUID/HEAD"
   ```

4. **Re-verify**: `h5i-db verify <db> <table> --deep` must be clean.
5. **Clean up**: manifests above the recovered sequence and any
   `HEAD.tmp.*` / `HEAD.lock` files are debris; a later
   `vacuum` (dry-run first) removes them. Do **not** delete them by hand
   before verify is clean.

If no manifest verifies, restore the table from backup (above). Rolling
`HEAD` back this way discards the commits after the recovery point — check
`committed_at_ns` / `note` in the recovered manifest to know exactly where
the table now stands.
