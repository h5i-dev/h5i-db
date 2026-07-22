#!/usr/bin/env bash
# Regenerate the golden format-compatibility fixture (tests/fixtures/golden-v1).
#
# The fixture is a tiny database committed to git and opened READ-ONLY by
# tests/format_compat.rs. It gates on-disk format breaks: if current code
# cannot read a database produced by an earlier release, CI fails.
#
# Only regenerate it when the format version is deliberately bumped (and then
# keep the old directory as golden-v<N> so every supported reader version
# stays covered). Usage:
#
#   cargo build -p h5i-db-cli
#   crates/h5i-db-core/tests/fixtures/generate-golden.sh \
#       target/debug/h5i-db crates/h5i-db-core/tests/fixtures/golden-v1
set -euo pipefail

BIN=${1:?usage: generate-golden.sh <h5i-db binary> <output dir>}
OUT=${2:?usage: generate-golden.sh <h5i-db binary> <output dir>}

rm -rf "$OUT"
mkdir -p "$OUT"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

cat > "$tmp/trades1.csv" <<'EOF'
ts,symbol,price
2026-01-01T00:00:00Z,AAA,10.0
2026-01-01T00:01:00Z,BBB,20.0
2026-01-01T00:02:00Z,AAA,10.5
2026-01-01T00:03:00Z,BBB,20.5
EOF
cat > "$tmp/trades2.csv" <<'EOF'
ts,symbol,price
2026-01-01T00:04:00Z,AAA,11.0
2026-01-01T00:05:00Z,BBB,21.0
2026-01-01T00:06:00Z,AAA,11.5
2026-01-01T00:07:00Z,BBB,21.5
EOF
cat > "$tmp/quotes1.csv" <<'EOF'
ts,symbol,bid,ask
2026-01-01T00:00:30Z,AAA,9.9,10.1
2026-01-01T00:02:30Z,BBB,19.9,20.1
2026-01-01T00:04:30Z,AAA,10.9,11.1
EOF

TRADES_SCHEMA='[{"name":"ts","type":"timestamp_ns","nullable":false},{"name":"symbol","type":"utf8","nullable":false},{"name":"price","type":"float64","nullable":false}]'
QUOTES_SCHEMA='[{"name":"ts","type":"timestamp_ns","nullable":false},{"name":"symbol","type":"utf8","nullable":false},{"name":"bid","type":"float64","nullable":false},{"name":"ask","type":"float64","nullable":false}]'

"$BIN" init "$OUT"

# trades: v0 create, v1 append, v2 append, v3 delete-range (head = 3)
"$BIN" create-table "$OUT" trades --schema "$TRADES_SCHEMA" --time-column ts
"$BIN" ingest "$OUT" trades "$tmp/trades1.csv" --input-format csv
"$BIN" ingest "$OUT" trades "$tmp/trades2.csv" --input-format csv
"$BIN" delete-range "$OUT" trades \
    --start 2026-01-01T00:02:00Z --end 2026-01-01T00:04:00Z

# quotes: v0 create, v1 append (head = 1)
"$BIN" create-table "$OUT" quotes --schema "$QUOTES_SCHEMA" --time-column ts
"$BIN" ingest "$OUT" quotes "$tmp/quotes1.csv" --input-format csv

# a named snapshot pinning the final versions of both tables
"$BIN" snapshot create "$OUT" golden --note "format-compat fixture"

# one stored (never-applied) mutation plan; format_compat asserts it still
# parses and checksum-verifies. It will be long-expired when tests run —
# that is fine and deliberate: listing must not choke on expired plans.
"$BIN" delete-range "$OUT" trades --plan \
    --start 2026-01-01T00:06:00Z --end 2026-01-01T00:08:00Z

# strip transient debris; the fixture must contain immutable objects + HEADs
find "$OUT" -name '*.lock' -delete -o -name 'HEAD.tmp.*' -delete

echo "golden fixture written to $OUT"
