#!/usr/bin/env bash
# Fork-monitor demo: a swarm of simulated agents forking, writing, promoting,
# and pruning against one database, so the review UI's Forks tab has a live,
# evolving lineage tree to show.
#
#   terminal 1:  tools/fork_demo.sh /tmp/h5i-demo
#   terminal 2:  h5i-db ui /tmp/h5i-demo        # open the URL → Forks tab
#
# For a richer base than the built-in 400-row seed, seed first with
# tools/demo_data.py (synthetic multi-symbol ticks, or real Binance data)
# and point this script at the same path — it resumes on existing databases:
#
#   tools/demo_data.py synth --symbols AAPL,MSFT,NVDA --db /tmp/h5i-demo
#   tools/fork_demo.sh /tmp/h5i-demo
#
# The loop is weighted toward the shapes the monitor distinguishes: bursts of
# writes inside forks (working), quiet forks holding results (ahead), base
# commits that strand shadows (conflict), and the occasional promote or prune.
#
# Env:
#   H5I_DB_BIN  h5i-db binary   (default: target/release/h5i-db, then debug)
#   ROUNDS      actions to run  (default 200)
#   PACE        seconds between actions (default 1.2)
#   NEST_P      percent chance a new fork nests inside an existing one
#               (default: 25 while few forks exist, 50 once there are >3 —
#               nesting prefers parents that have already written, so the
#               parent-child edge is real dependency, not just a name)
set -euo pipefail

DB=${1:?usage: tools/fork_demo.sh <db-path>}
ROUNDS=${ROUNDS:-200}
PACE=${PACE:-1.2}
NEST_P=${NEST_P:-auto}

if [[ -z ${H5I_DB_BIN:-} ]]; then
  for cand in target/release/h5i-db target/debug/h5i-db; do
    [[ -x $cand ]] && H5I_DB_BIN=$cand && break
  done
fi
: "${H5I_DB_BIN:?no h5i-db binary found — build one (cargo build -p h5i-db-cli) or set H5I_DB_BIN}"
say() { printf '\033[2m%s\033[0m %s\n' "$(date +%H:%M:%S)" "$*"; }
db() { "$H5I_DB_BIN" "$@"; }

# ---------------------------------------------------------------- seed
# Event time is one strictly increasing clock (one second per row) shared by
# base and forks, so every append is a valid strict ordered append.
export TZ=UTC
EPOCH0=$(date -u +%s)
T=0
# rows runs on the left of a pipe (a subshell), so it cannot advance the
# clock itself — every `rows N … | db ingest …` is followed by `T=$((T+N))`.
rows() { # rows <count> <symbol> <base-price>
  local n=$1 sym=$2 base=$3 i
  echo "ts,symbol,price,size"
  for ((i = 1; i <= n; i++)); do
    printf '%(%Y-%m-%dT%H:%M:%SZ)T,%s,%d.%02d,%d\n' "$((EPOCH0 + T + i))" \
      "$sym" "$((base + RANDOM % 7))" "$((RANDOM % 100))" "$((RANDOM % 900 + 100))"
  done
}

if [[ ! -e $DB ]]; then
  say "creating $DB with table ticks"
  db init "$DB"
  db create-table "$DB" ticks --time-column ts --schema \
    '[{"name":"ts","type":"timestamp_ns","nullable":false},
      {"name":"symbol","type":"utf8","nullable":false},
      {"name":"price","type":"float64","nullable":false},
      {"name":"size","type":"int64","nullable":false}]'
  rows 400 BASE 100 | db ingest "$DB" ticks - --input-format csv --mode append
  T=$((T + 400))
else
  say "reusing existing $DB"
  # Resume the clock one second past the newest committed row.
  maxts=$(db query "$DB" "SELECT max(ts) AS m FROM ticks" --format json \
    | grep -o '"m": *"[^"]*"' | head -1 | sed 's/.*"m": *"//; s/"$//')
  EPOCH0=$(( $(date -u -d "${maxts%%[.+Z]*}" +%s 2>/dev/null || echo $((EPOCH0 + 864000))) + 1 ))
fi

# ---------------------------------------------------------------- swarm
OBJECTIVES=("vol regime scan" "spread backfill" "lr sweep" "outlier hunt" "asof repro" "signal decay probe")
PREFIXES=(scout tuner backfill prober)
FORKS=()
declare -A WROTE=()    # fork → 1 once it has committed something
declare -A PARENT=()   # fork → recorded parent fork
N=0

pick_fork() { echo "${FORKS[RANDOM % ${#FORKS[@]}]}"; }
# Nesting parent: strongly prefer forks that have written, so the child pins
# tables the parent owns and the lineage edge is real dependency rather than
# just a recorded name (a dependency-free child lets the parent be dropped
# out from under it, flattening the tree mid-demo).
pick_parent() {
  local writers=() f
  for f in "${FORKS[@]}"; do [[ -n ${WROTE[$f]:-} ]] && writers+=("$f"); done
  if ((${#writers[@]} > 0)) && ((RANDOM % 10 < 8)); then
    echo "${writers[RANDOM % ${#writers[@]}]}"
  else
    pick_fork
  fi
}
# Drop candidate: prefer leaves (forks no current fork records as parent) so
# lineage chains survive long enough to be seen.
pick_droppable() {
  local -A is_parent=()
  local f leaves=()
  for f in "${FORKS[@]}"; do
    [[ -n ${PARENT[$f]:-} ]] && is_parent[${PARENT[$f]}]=1
  done
  for f in "${FORKS[@]}"; do [[ -z ${is_parent[$f]:-} ]] && leaves+=("$f"); done
  if ((${#leaves[@]} > 0)); then echo "${leaves[RANDOM % ${#leaves[@]}]}"; else pick_fork; fi
}
nest_chance() {
  if [[ $NEST_P == auto ]]; then
    ((${#FORKS[@]} > 3)) && echo 50 || echo 25
  else
    echo "$NEST_P"
  fi
}
forget_fork() { # forget_fork <name> — also drops recorded children of it
  local gone=$1 keep=()
  for f in "${FORKS[@]}"; do [[ $f == "$gone" ]] || keep+=("$f"); done
  FORKS=("${keep[@]}")
  unset "WROTE[$gone]" "PARENT[$gone]" 2>/dev/null || true
}

for ((round = 0; round < ROUNDS; round++)); do
  r=$((RANDOM % 10))
  if ((${#FORKS[@]} == 0)) || ((r < 2 && ${#FORKS[@]} < 14)); then
    # New agent branch; sometimes a fork of a fork.
    name="${PREFIXES[RANDOM % ${#PREFIXES[@]}]}-$(printf '%02d' "$N")"; N=$((N + 1))
    obj="${OBJECTIVES[RANDOM % ${#OBJECTIVES[@]}]}"
    scope=()
    if ((${#FORKS[@]} > 0 && RANDOM % 100 < $(nest_chance))); then
      parent=$(pick_parent)
      scope=(--fork "$parent")
      PARENT[$name]=$parent
      say "fork $name (inside $parent) — $obj"
    else
      say "fork $name — $obj"
    fi
    db "${scope[@]}" fork create "$DB" "$name" --note "$obj" \
      --meta "{\"agent\":\"$name\",\"objective\":\"$obj\"}" >/dev/null
    FORKS+=("$name")
  elif ((r < 7)); then
    # The common shape: an agent writing inside its branch.
    f=$(pick_fork)
    n=$((RANDOM % 60 + 20))
    say "agent $f appends $n rows"
    rows "$n" "SIM" 100 | db --fork "$f" ingest "$DB" ticks - \
      --input-format csv --mode append >/dev/null
    T=$((T + n))
    WROTE[$f]=1
  elif ((r < 8)); then
    # Base moves on its own — forks that shadowed ticks go stale (conflict).
    say "base commit (strands shadows)"
    rows 30 BASE 100 | db ingest "$DB" ticks - --input-format csv --mode append >/dev/null
    T=$((T + 30))
  elif ((r < 9)); then
    # First promote wins; a CAS conflict is a legitimate demo beat, not a bug.
    f=$(pick_fork)
    if db fork promote "$DB" "$f" --table ticks >/dev/null 2>&1; then
      say "promote $f → base succeeded"
    else
      say "promote $f → base refused (base moved / nothing to promote)"
    fi
  else
    # Prune, preferring leaves so lineage chains stay visible for a while.
    # A refusal (children pin it) is also a demo beat.
    f=$(pick_droppable)
    if db fork drop "$DB" "$f" >/dev/null 2>&1; then
      say "drop $f"
      forget_fork "$f"
    else
      say "drop $f refused (nested forks pin it)"
    fi
  fi
  sleep "$PACE"
done

say "done — ${#FORKS[@]} forks left standing"
db fork list "$DB"
