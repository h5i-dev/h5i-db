#!/usr/bin/env python3
"""Generate the common quote stream and run it through a local LEAN build."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import time
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

RESULT_PATTERN = re.compile(
    r"H5I_BACKTEST_COMPARE events=(\d+) orders=(\d+) engine_ms=([0-9.]+)"
)


def mid_at(step: int) -> float:
    return 100.0 + ((step % 400) - 200) * 0.01


def generate_data(data_root: Path, workload: dict, lean_root: Path) -> None:
    marker = data_root / ".h5i-workload.json"
    expected = json.dumps(
        {"generator_version": 2, "workload": workload}, sort_keys=True
    )
    if marker.exists() and marker.read_text() == expected:
        return

    data_root.mkdir(parents=True, exist_ok=True)
    for name in ("market-hours", "symbol-properties"):
        link = data_root / name
        if not link.exists():
            link.symlink_to(
                (lean_root / "Data" / name).resolve(), target_is_directory=True
            )

    quote_dir = data_root / "forex" / "oanda" / "second" / "eurusd"
    quote_dir.mkdir(parents=True, exist_ok=True)
    for old in quote_dir.glob("*_quote.zip"):
        old.unlink()

    stamp = datetime.fromisoformat(workload["start_utc"].replace("Z", "+00:00"))
    exchange_timezone = ZoneInfo("America/New_York")
    by_day: dict[str, list[str]] = {}
    step = 0
    while step < int(workload["quote_events"]):
        local = stamp.astimezone(exchange_timezone)
        during_maintenance = (
            local.weekday() < 5
            and (local.hour, local.minute) >= (16, 58)
            and (local.hour, local.minute) < (17, 3)
        )
        if during_maintenance:
            stamp += timedelta(seconds=1)
            continue
        day = stamp.strftime("%Y%m%d")
        milliseconds = (
            stamp.hour * 3_600_000 + stamp.minute * 60_000 + stamp.second * 1_000
        )
        mid = mid_at(step)
        bid = mid - 0.01
        ask = mid + 0.01
        by_day.setdefault(day, []).append(
            f"{milliseconds},{bid:.5f},{bid:.5f},{bid:.5f},{bid:.5f},500,"
            f"{ask:.5f},{ask:.5f},{ask:.5f},{ask:.5f},500\n"
        )
        step += 1
        stamp += timedelta(seconds=1)

    for day, rows in by_day.items():
        archive = quote_dir / f"{day}_quote.zip"
        member = f"{day}_eurusd_second_quote.csv"
        with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as output:
            output.writestr(member, "".join(rows))
    marker.write_text(expected)


def run(
    workload_path: Path,
    lean_root: Path,
    dotnet: Path,
    cache_dir: Path,
) -> dict:
    workload_path = workload_path.resolve()
    lean_root = lean_root.resolve()
    dotnet = dotnet.resolve()
    cache_dir = cache_dir.resolve()
    workload = json.loads(workload_path.read_text())
    if workload.get("schema_version") != 1:
        raise ValueError("unsupported workload schema")

    data_root = cache_dir / "lean-data"
    generate_data(data_root, workload, lean_root)
    launcher_dir = lean_root / "Launcher" / "bin" / "Release"
    launcher = launcher_dir / "QuantConnect.Lean.Launcher.dll"
    algorithm = (
        Path(__file__).parent
        / "lean"
        / "bin"
        / "Release"
        / "net10.0"
        / "H5i.BacktestCompare.dll"
    ).resolve()
    if not launcher.exists() or not algorithm.exists():
        raise FileNotFoundError(
            "LEAN binaries are missing; run the build commands in README.md"
        )

    command = [
        str(dotnet),
        str(launcher),
        "--environment",
        "backtesting",
        "--algorithm-type-name",
        "H5i.BacktestCompare.H5iBacktestComparisonAlgorithm",
        "--algorithm-language",
        "CSharp",
        "--algorithm-location",
        str(algorithm),
        "--data-folder",
        str(data_root),
        "--parameters",
        (f"event-count:{workload['quote_events']},signal-count:{workload['signals']}"),
    ]
    started = time.perf_counter_ns()
    completed = subprocess.run(
        command,
        cwd=launcher_dir,
        text=True,
        capture_output=True,
        check=False,
        env={**os.environ, "DOTNET_CLI_TELEMETRY_OPTOUT": "1"},
    )
    wall_ms = (time.perf_counter_ns() - started) / 1_000_000
    combined = completed.stdout + "\n" + completed.stderr
    if completed.returncode != 0:
        raise RuntimeError(f"LEAN failed with exit {completed.returncode}:\n{combined}")
    match = RESULT_PATTERN.search(combined)
    if not match:
        raise RuntimeError(
            f"LEAN did not emit its benchmark marker:\n{combined[-4000:]}"
        )
    events, orders, engine_ms = match.groups()
    event_count = int(events)
    measured_ms = float(engine_ms)
    return {
        "schema_version": 1,
        "engine": "lean",
        "engine_version": (lean_root / ".git").exists()
        and subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=lean_root,
            text=True,
            capture_output=True,
            check=True,
        ).stdout.strip()
        or "unknown",
        "workload": workload["name"],
        "event_count": event_count,
        "signals_requested": int(workload["signals"]),
        "events_seen": event_count,
        "orders_submitted": int(orders),
        "timings_ms": {"engine": measured_ms, "process_wall": wall_ms},
        "throughput_events_per_sec": event_count / (measured_ms / 1000),
        "boundary": "first Slice callback -> OnEndOfAlgorithm; disk-fed",
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workload", type=Path, required=True)
    parser.add_argument("--lean-root", type=Path, required=True)
    parser.add_argument("--dotnet", type=Path, default=Path("dotnet"))
    parser.add_argument(
        "--cache-dir", type=Path, default=Path("/tmp/h5i-backtest-compare")
    )
    args = parser.parse_args()
    print(
        json.dumps(
            run(args.workload, args.lean_root, args.dotnet, args.cache_dir),
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
