#!/usr/bin/env python3
"""Run and summarize the common backtest workload across available engines."""

from __future__ import annotations

import argparse
import json
import platform
import statistics
import subprocess
from pathlib import Path


def command_json(command: list[str], cwd: Path) -> dict:
    completed = subprocess.run(
        command,
        cwd=cwd,
        text=True,
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"{' '.join(command)} failed with exit {completed.returncode}:\n"
            f"{completed.stdout}\n{completed.stderr}"
        )
    start = completed.stdout.find("{")
    if start < 0:
        raise RuntimeError(f"command emitted no JSON:\n{completed.stdout}")
    return json.loads(completed.stdout[start:])


def h5i_result(raw: dict, revision: str, workload_name: str) -> dict:
    config = raw["config"]
    timings = raw["derived"]
    event_count = int(config["book_events"]) + int(config["trades"])
    engine_ms = float(timings["kernel_ms"])
    return {
        "schema_version": 1,
        "engine": "h5i-db",
        "engine_version": revision,
        "workload": workload_name,
        "event_count": event_count,
        "signals_requested": int(config["signals"]),
        "events_seen": event_count,
        "orders_submitted": int(config["signals"]),
        "timings_ms": {
            "engine": engine_ms,
            "decode": float(timings["decode_ms"]),
            "persisted_run": float(timings["run_ms"]),
        },
        "throughput_events_per_sec": event_count / (engine_ms / 1000),
        "boundary": "decoded Records -> replay kernel; persisted_run is separate",
    }


def validate(result: dict, workload: dict) -> None:
    expected_events = int(workload["quote_events"])
    expected_signals = int(workload["signals"])
    if result["event_count"] != expected_events:
        raise ValueError(
            f"{result['engine']} processed {result['event_count']} events, "
            f"expected {expected_events}"
        )
    if result["events_seen"] != expected_events:
        raise ValueError(f"{result['engine']} did not observe every event")
    if result["orders_submitted"] != expected_signals:
        raise ValueError(f"{result['engine']} did not submit every signal")


def summarize(samples: list[dict]) -> dict:
    engine_ms = [float(sample["timings_ms"]["engine"]) for sample in samples]
    throughputs = [float(sample["throughput_events_per_sec"]) for sample in samples]
    return {
        "engine": samples[0]["engine"],
        "engine_version": samples[0]["engine_version"],
        "boundary": samples[0]["boundary"],
        "samples": len(samples),
        "median_engine_ms": statistics.median(engine_ms),
        "min_engine_ms": min(engine_ms),
        "max_engine_ms": max(engine_ms),
        "median_events_per_sec": statistics.median(throughputs),
    }


def main() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--workload", type=Path, default=Path(__file__).with_name("workload.json")
    )
    parser.add_argument(
        "--engines", default="h5i,nautilus,lean", help="comma-separated engine list"
    )
    parser.add_argument("--repetitions", type=int)
    parser.add_argument("--h5i-binary", type=Path)
    parser.add_argument(
        "--nautilus-python",
        type=Path,
        default=Path("/tmp/h5i-nautilus-wheel-venv/bin/python"),
    )
    parser.add_argument("--lean-root", type=Path, default=repo_root / "../../Ref/Lean")
    parser.add_argument("--dotnet", type=Path, default=Path("/tmp/h5i-dotnet/dotnet"))
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    workload_path = args.workload.resolve()
    workload = json.loads(workload_path.read_text())
    repetitions = args.repetitions or int(workload["repetitions"])
    engines = [engine.strip() for engine in args.engines.split(",") if engine.strip()]
    revision = subprocess.run(
        ["git", "rev-parse", "--short", "HEAD"],
        cwd=repo_root,
        text=True,
        capture_output=True,
        check=True,
    ).stdout.strip()
    if args.h5i_binary is None:
        candidates = sorted(
            (repo_root / "target/release/deps").glob("replay_path-*"),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        args.h5i_binary = next(
            (
                path
                for path in candidates
                if path.is_file() and path.stat().st_mode & 0o111
            ),
            None,
        )

    samples: dict[str, list[dict]] = {engine: [] for engine in engines}
    total_runs = repetitions + int(workload.get("warmups", 0))
    for engine in engines:
        for index in range(total_runs):
            if engine == "h5i":
                if args.h5i_binary is None:
                    raise FileNotFoundError("build the replay_path benchmark first")
                raw = command_json(
                    [
                        str(args.h5i_binary.resolve()),
                        "--book",
                        str(workload["quote_events"]),
                        "--trades",
                        "0",
                        "--instruments",
                        str(workload["instruments"]),
                        "--signals",
                        str(workload["signals"]),
                        "--trials",
                        "1",
                        "--common-quotes",
                    ],
                    repo_root,
                )
                result = h5i_result(raw, revision, workload["name"])
            elif engine == "nautilus":
                result = command_json(
                    [
                        str(args.nautilus_python.absolute()),
                        str(Path(__file__).with_name("nautilus_runner.py")),
                        "--workload",
                        str(workload_path),
                    ],
                    repo_root,
                )
            elif engine == "lean":
                result = command_json(
                    [
                        "python3",
                        str(Path(__file__).with_name("lean_runner.py")),
                        "--workload",
                        str(workload_path),
                        "--lean-root",
                        str(args.lean_root.resolve()),
                        "--dotnet",
                        str(args.dotnet.resolve()),
                    ],
                    repo_root,
                )
            else:
                raise ValueError(f"unknown engine {engine!r}")
            validate(result, workload)
            if index >= int(workload.get("warmups", 0)):
                samples[engine].append(result)

    report = {
        "schema_version": 1,
        "workload": workload,
        "machine": {
            "platform": platform.platform(),
            "processor": platform.machine(),
        },
        "summaries": [summarize(samples[engine]) for engine in engines],
        "samples": samples,
    }
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.write_text(rendered)
    print(rendered, end="")


if __name__ == "__main__":
    main()
