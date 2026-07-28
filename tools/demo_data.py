#!/usr/bin/env python3
"""Demo dataset generator for h5i-db. Stdlib only — no venv needed.

Produces tick-shaped CSV (`ts,symbol,price,size`) that matches the schema
`tools/fork_demo.sh` and `h5i-db demo` use, either synthesised or downloaded
from a real public source. With `--db` it seeds a database directly (init +
create-table + ingest through the h5i-db binary) so one command yields
something worth pointing the UI at.

Synthetic — GBM with jumps, bid-ask bounce, U-shaped intraday volume:

    tools/demo_data.py synth --symbols AAPL,MSFT,NVDA --hours 6 --db /tmp/demo-db
    tools/demo_data.py synth --symbols 8 --hours 1 --hz 5 > ticks.csv

Real crypto data from Binance's public archive (data.binance.vision, no API
key; 1s klines are ~2 MB/day/symbol). `price` is the bar close and `size`
the number of trades in the bar:

    tools/demo_data.py binance --symbols BTCUSDT,ETHUSDT --date 2026-07-25 --db /tmp/demo-db
    tools/demo_data.py binance --symbols BTCUSDT --kind trades --limit 100000 > btc.csv

Seed first, then point the fork swarm at the same database:

    tools/demo_data.py synth --symbols 6 --db /tmp/demo-db
    tools/fork_demo.sh /tmp/demo-db
"""

import argparse
import csv
import io
import math
import os
import random
import shutil
import subprocess
import sys
import urllib.request
import zipfile
from datetime import datetime, timedelta, timezone

NS = 1_000_000_000


def fmt_ts(ns: int) -> str:
    """RFC3339 with microseconds — the shape the CSV ingest parser accepts."""
    dt = datetime.fromtimestamp(ns / NS, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


# ---------------------------------------------------------------- synthetic


def u_shape(frac: float) -> float:
    """Intraday activity: busy open and close, quiet lunch (min 0.35x)."""
    return 0.35 + 0.65 * (2 * abs(frac - 0.5)) ** 2 + 0.65 * math.exp(-8 * frac)


def synth_rows(args, rng):
    symbols = symbol_list(args.symbols)
    start_ns = parse_start(args.start, hours=args.hours)
    total_s = int(args.hours * 3600)
    rows = []
    for sym in symbols:
        price = rng.uniform(20, 500)
        # Per-second vol consistent with ~25% annualised on a 6.5 h session.
        step_vol = 0.25 / math.sqrt(252 * 6.5 * 3600)
        spread = price * rng.uniform(0.0002, 0.001)
        t = 0.0
        while t < total_s:
            frac = t / total_s
            intensity = args.hz * u_shape(frac)
            t += rng.expovariate(intensity)
            if t >= total_s:
                break
            price *= math.exp(rng.gauss(0, step_vol))
            if rng.random() < 0.0004:  # news: a jump plus a volume burst
                price *= math.exp(rng.choice([-1, 1]) * rng.uniform(0.002, 0.01))
                burst = 8
            else:
                burst = 1
            # Trades print on either side of mid: bid-ask bounce.
            printed = price + rng.choice([-1, 1]) * spread / 2
            size = max(1, int(rng.lognormvariate(4.5, 1.0) * u_shape(frac) * burst))
            rows.append((start_ns + int(t * NS), sym, round(printed, 4), size))
    return rows


def symbol_list(spec: str):
    if spec.isdigit():
        return [f"SYM{i:02d}" for i in range(int(spec))]
    return [s.strip().upper() for s in spec.split(",") if s.strip()]


def parse_start(start, hours=0.0):
    if start:
        dt = datetime.fromisoformat(start.replace("Z", "+00:00"))
        return int(dt.timestamp() * NS)
    # Default: a window ending now, so a follow-up fork_demo.sh run (whose
    # clock starts at "now") can append behind it without a sort violation.
    return int((datetime.now(timezone.utc) - timedelta(hours=hours)).timestamp() * NS)


# ---------------------------------------------------------------- binance

BINANCE = "https://data.binance.vision/data/spot/daily"


def binance_fetch(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "h5i-db-demo"})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return r.read()
    except Exception as e:
        sys.exit(f"download failed: {url}\n  {e}\n  (offline? use `synth` instead)")


def binance_rows(args):
    date = args.date or (datetime.now(timezone.utc) - timedelta(days=2)).strftime("%Y-%m-%d")
    rows = []
    for sym in symbol_list(args.symbols):
        if args.kind == "klines":
            url = f"{BINANCE}/klines/{sym}/{args.interval}/{sym}-{args.interval}-{date}.zip"
        else:
            url = f"{BINANCE}/trades/{sym}/{sym}-trades-{date}.zip"
        print(f"downloading {url}", file=sys.stderr)
        blob = binance_fetch(url)
        with zipfile.ZipFile(io.BytesIO(blob)) as z:
            raw = io.TextIOWrapper(z.open(z.namelist()[0]), encoding="utf-8")
            n = 0
            for rec in csv.reader(raw):
                if not rec or not rec[0].isdigit():  # header line in newer files
                    continue
                # Archive timestamps flipped from ms to µs in 2025; normalise.
                t = int(rec[0] if args.kind == "klines" else rec[4])
                ts_ns = t * (1000 if t > 10**14 else 1_000_000)
                if args.kind == "klines":
                    # close price; trade count is the honest integer for size
                    rows.append((ts_ns, sym, float(rec[4]), int(rec[8])))
                else:
                    # price, quote quantity (notional, rounds to a sane int)
                    rows.append((ts_ns, sym, float(rec[1]), max(1, round(float(rec[3])))))
                n += 1
                if args.limit and n >= args.limit:
                    break
        print(f"  {n} rows for {sym}", file=sys.stderr)
    return rows


# ---------------------------------------------------------------- output


def emit(rows, out):
    # One global time order (ties broken by symbol) is what strict ordered
    # append expects; equal timestamps are fine, going backwards is not.
    rows.sort()
    w = csv.writer(out)
    w.writerow(["ts", "symbol", "price", "size"])
    for ts_ns, sym, price, size in rows:
        w.writerow([fmt_ts(ts_ns), sym, price, size])


SCHEMA = (
    '[{"name":"ts","type":"timestamp_ns","nullable":false},'
    '{"name":"symbol","type":"utf8","nullable":false},'
    '{"name":"price","type":"float64","nullable":false},'
    '{"name":"size","type":"int64","nullable":false}]'
)


def find_binary():
    env = os.environ.get("H5I_DB_BIN")
    if env:
        return env
    for cand in ("target/release/h5i-db", "target/debug/h5i-db"):
        if os.access(cand, os.X_OK):
            return cand
    return shutil.which("h5i-db") or sys.exit(
        "no h5i-db binary found — build one (cargo build -p h5i-db-cli) or set H5I_DB_BIN"
    )


def seed_db(db, table, rows):
    bin_ = find_binary()

    def run(*cmd, stdin=None, check=True):
        return subprocess.run([bin_, *cmd], input=stdin, capture_output=True, check=check)

    if not os.path.exists(db):
        run("init", db)
        print(f"created {db}", file=sys.stderr)
    # Idempotent create: "already exists" is fine, anything else is not.
    r = run("create-table", db, table, "--time-column", "ts", "--schema", SCHEMA, check=False)
    if r.returncode != 0 and b"exists" not in r.stderr + r.stdout:
        sys.exit(r.stderr.decode())
    buf = io.StringIO()
    emit(rows, buf)
    r = run("ingest", db, table, "-", "--input-format", "csv", "--mode", "append",
            stdin=buf.getvalue().encode(), check=False)
    if r.returncode != 0:
        sys.exit(r.stderr.decode())
    print(f"ingested {len(rows)} rows into {db}:{table}", file=sys.stderr)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="source", required=True)

    sy = sub.add_parser("synth", help="synthetic multi-symbol tick data")
    sy.add_argument("--symbols", default="6", help="count, or comma list of names (default: 6)")
    sy.add_argument("--hours", type=float, default=1.0, help="session length (default: 1)")
    sy.add_argument("--hz", type=float, default=2.0, help="mean ticks/sec/symbol at peak (default: 2)")
    sy.add_argument("--start", help="RFC3339 session start (default: ends now)")
    sy.add_argument("--seed", type=int, help="RNG seed for reproducible data")

    bn = sub.add_parser("binance", help="real data from data.binance.vision (no API key)")
    bn.add_argument("--symbols", default="BTCUSDT,ETHUSDT", help="comma list (default: BTCUSDT,ETHUSDT)")
    bn.add_argument("--kind", choices=["klines", "trades"], default="klines",
                    help="1s bars (~2 MB/day/symbol) or raw trades (large) (default: klines)")
    bn.add_argument("--interval", default="1s", help="kline interval (default: 1s)")
    bn.add_argument("--date", help="UTC day YYYY-MM-DD (default: two days ago)")
    bn.add_argument("--limit", type=int, default=200_000, help="max rows/symbol (default: 200000; 0 = all)")

    for p in (sy, bn):
        p.add_argument("--db", help="seed this database instead of writing CSV to stdout")
        p.add_argument("--table", default="ticks", help="table name with --db (default: ticks)")
        p.add_argument("--out", help="write CSV here instead of stdout")

    args = ap.parse_args()
    if args.source == "synth":
        rows = synth_rows(args, random.Random(args.seed))
    else:
        rows = binance_rows(args)
    if not rows:
        sys.exit("no rows produced")

    if args.db:
        seed_db(args.db, args.table, rows)
    elif args.out:
        with open(args.out, "w", newline="") as f:
            emit(rows, f)
        print(f"wrote {len(rows)} rows to {args.out}", file=sys.stderr)
    else:
        emit(rows, sys.stdout)


if __name__ == "__main__":
    main()
