"""Command line surface for the quant layer.

    python -m h5i_db.quant factor --db research.db \\
        --factor signals --prices prices --periods 1,5,10 --out factor.html
    python -m h5i_db.quant tearsheet --db research.db --returns strategy_returns

Every verb takes the same read-point flags as the Python API and emits
either JSON (the default, for agents and pipelines) or a self-contained
HTML report.

This is the *only* command surface for the quant layer, deliberately. The
roadmap asks for one implementation reachable from two surfaces; a Rust
``h5i quant`` subcommand would have to rebuild the same SQL in a second
place, and two implementations of a statistic is exactly the failure the
rule exists to prevent. Moving the SQL builders into Rust and binding them
from Python would satisfy both, and is the way to add the native verb later.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Optional, Sequence

import h5i_db

from . import factor as factor_mod
from . import perf as perf_mod
from . import report as report_mod
# Imported by name: the package exports a `sweep` *function*, which shadows
# the module of the same name on the package.
from .sweep import VerificationError, verify


def _read_point(args) -> dict:
    version = args.version
    if version is not None and ":" in version:
        version = {
            part.split(":", 1)[0]: int(part.split(":", 1)[1])
            for part in version.split(",")
        }
    elif version is not None:
        version = int(version)
    return {
        "version": version,
        "as_of": args.as_of,
        "snapshot": args.snapshot,
        "event_time_cutoff": args.event_time_cutoff,
    }


def _periods(text: str) -> tuple:
    try:
        return tuple(int(p) for p in text.split(",") if p.strip())
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"--periods takes comma-separated bar counts, got {text!r}"
        )


def _emit(payload: dict, args) -> int:
    if args.out:
        report_mod.render(payload, path=args.out)
        if args.format == "json":
            print(json.dumps({"written": args.out, "digest": payload["provenance"]["digest"]}))
        else:
            print(f"wrote {args.out}")
        return 0
    if args.format == "html":
        sys.stdout.write(report_mod.render(payload))
        return 0
    json.dump(payload, sys.stdout, indent=2, default=str)
    sys.stdout.write("\n")
    return 0


def _cmd_factor(args) -> int:
    with h5i_db.Database(args.db) as db:
        panel = factor_mod.build_panel(
            db,
            args.factor,
            args.prices,
            periods=args.periods,
            quantiles=None if args.bins else args.quantiles,
            bins=args.bins,
            group=args.group,
            binning_by_group=args.binning_by_group,
            zero_aware=args.zero_aware,
            filter_zscore=args.filter_zscore,
            max_loss=args.max_loss,
            deterministic=not args.fast,
            **_read_point(args),
        )
        payload = report_mod.factor_payload(panel, title=args.title)
    return _emit(payload, args)


def _cmd_tearsheet(args) -> int:
    with h5i_db.Database(args.db) as db:
        series = perf_mod.returns(
            db,
            args.returns,
            annualization=args.annualization,
            deterministic=not args.fast,
            **_read_point(args),
        )
        benchmark = None
        if args.benchmark:
            benchmark = perf_mod.returns(
                db,
                args.benchmark,
                annualization=args.annualization,
                deterministic=not args.fast,
                **_read_point(args),
            )
        payload = report_mod.tearsheet_payload(
            series, benchmark=benchmark, title=args.title
        )
    return _emit(payload, args)


def _cmd_stats(args) -> int:
    with h5i_db.Database(args.db) as db:
        series = perf_mod.returns(
            db,
            args.returns,
            annualization=args.annualization,
            deterministic=not args.fast,
            **_read_point(args),
        )
        stats = series.stats()
        stats["_digest"] = series.provenance.digest
    if args.format == "table":
        width = max(len(k) for k in stats)
        for key, value in stats.items():
            print(f"{key:<{width}}  {value}")
    else:
        json.dump(stats, sys.stdout, indent=2, default=str)
        sys.stdout.write("\n")
    return 0


def _cmd_verify(args) -> int:
    with h5i_db.Database(args.db) as db:
        def build():
            return factor_mod.build_panel(
                db,
                args.factor,
                args.prices,
                periods=args.periods,
                quantiles=args.quantiles,
                filter_zscore=args.filter_zscore,
                max_loss=args.max_loss,
                **_read_point(args),
            )

        try:
            report = verify(build(), rerun=build, strict=not args.lenient)
        except VerificationError as exc:
            print(json.dumps({"verified": False, "reason": str(exc)}, indent=2))
            return 1
    if args.expect and report["digest"] != args.expect:
        print(
            json.dumps(
                {
                    "verified": False,
                    "reason": "digest does not match --expect",
                    "digest": report["digest"],
                    "expected": args.expect,
                },
                indent=2,
            )
        )
        return 1
    json.dump(report, sys.stdout, indent=2, default=str)
    sys.stdout.write("\n")
    return 0


def _add_common(parser) -> None:
    parser.add_argument("--db", required=True, help="path to the database")
    parser.add_argument(
        "--version",
        help="version pin: an integer, or table:version pairs "
        "(signals:2,prices:1)",
    )
    parser.add_argument("--as-of", dest="as_of", help="RFC3339 availability pin")
    parser.add_argument("--snapshot", help="snapshot name to read at")
    parser.add_argument(
        "--event-time-cutoff",
        dest="event_time_cutoff",
        help="decision-time embargo; no row stamped after this is read",
    )
    parser.add_argument(
        "--format",
        choices=("json", "html", "table"),
        default="json",
        help="output format (default: json)",
    )
    parser.add_argument("--out", help="write an HTML report to this path")
    parser.add_argument("--title", help="report title")
    parser.add_argument(
        "--fast",
        action="store_true",
        help="allow parallel execution; results stop being bit-reproducible",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m h5i_db.quant",
        description="Factor evaluation and performance reports on pinned data.",
    )
    subs = parser.add_subparsers(dest="command", required=True)

    factor = subs.add_parser("factor", help="evaluate a factor")
    _add_common(factor)
    factor.add_argument("--factor", required=True, help="factor table")
    factor.add_argument("--prices", required=True, help="price table")
    factor.add_argument("--periods", type=_periods, default=(1, 5, 10))
    factor.add_argument("--quantiles", type=int, default=5)
    factor.add_argument("--bins", type=int, help="equal-width bins instead")
    factor.add_argument("--group", help="group column on the factor table")
    factor.add_argument("--binning-by-group", action="store_true")
    factor.add_argument("--zero-aware", action="store_true")
    factor.add_argument("--filter-zscore", type=float, default=20.0)
    factor.add_argument("--max-loss", type=float, default=0.35)
    factor.set_defaults(func=_cmd_factor)

    tear = subs.add_parser("tearsheet", help="render a performance tearsheet")
    _add_common(tear)
    tear.add_argument("--returns", required=True, help="returns table")
    tear.add_argument("--benchmark", help="benchmark returns table")
    tear.add_argument("--annualization", type=float, default=perf_mod.DAILY)
    tear.set_defaults(func=_cmd_tearsheet)

    stats = subs.add_parser("stats", help="headline statistics only")
    _add_common(stats)
    stats.add_argument("--returns", required=True)
    stats.add_argument("--annualization", type=float, default=perf_mod.DAILY)
    stats.set_defaults(func=_cmd_stats)

    verify = subs.add_parser("verify", help="re-run a factor panel and check it")
    _add_common(verify)
    verify.add_argument("--factor", required=True)
    verify.add_argument("--prices", required=True)
    verify.add_argument("--periods", type=_periods, default=(1, 5, 10))
    verify.add_argument("--quantiles", type=int, default=5)
    verify.add_argument("--filter-zscore", type=float, default=20.0)
    verify.add_argument("--max-loss", type=float, default=0.35)
    verify.add_argument("--expect", help="digest the result must equal")
    verify.add_argument("--lenient", action="store_true")
    verify.set_defaults(func=_cmd_verify)

    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except h5i_db.H5iError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except (ValueError, TypeError, factor_mod.MaxLossExceededError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
