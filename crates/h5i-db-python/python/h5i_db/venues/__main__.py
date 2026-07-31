"""Command line for the vendor on-ramp.

Mirrors `python -m h5i_db.backtest`: a thin argument layer over the same typed
functions, so a shell pipeline and a notebook do the same thing. Market
definitions travel as a JSON file rather than as flags, because a market is a
dozen fields and a flag list would be unreadable and unversionable.

    python -m h5i_db.venues markets  market.db specs.json
    python -m h5i_db.venues ingest   market.db specs.json --root /mnt/pmxt
    python -m h5i_db.venues inspect  market.db
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import replace
from pathlib import Path
from typing import Any, Optional, Sequence

from ._archive import (
    KAGGLE_POLYMARKET_LAYOUT,
    KAGGLE_POLYMARKET_TRADES_LAYOUT,
    KALSHI_PMXT_LAYOUT,
    PMXT_LAYOUT,
    TELONEX_LAYOUT,
    discover,
    ingest_archive,
)
from ._bars import (
    BINANCE_KLINES_LAYOUT,
    GENERIC_OHLCV_LAYOUT,
    bars_from_trades,
    ingest_bars,
)
from ._markets import MarketSpec, polymarket_markets_from_json, write_markets

_LAYOUTS = {
    "pmxt": PMXT_LAYOUT,
    "telonex": TELONEX_LAYOUT,
    "kalshi-pmxt": KALSHI_PMXT_LAYOUT,
    "kaggle-polymarket": KAGGLE_POLYMARKET_LAYOUT,
    "kaggle-polymarket-trades": KAGGLE_POLYMARKET_TRADES_LAYOUT,
}

_BAR_LAYOUTS = {
    "ohlcv": GENERIC_OHLCV_LAYOUT,
    "binance-klines": BINANCE_KLINES_LAYOUT,
}


def _load_specs(path: Path) -> list[MarketSpec]:
    """Read market specs from JSON.

    Accepts either this module's own spec shape or a raw vendor market payload,
    distinguished by the presence of `outcome_labels`. Supporting both means a
    user can commit a hand-checked spec file *or* pipe the vendor's response
    through unchanged.
    """
    payload = json.loads(path.read_text(encoding="utf-8"))
    records = payload if isinstance(payload, list) else payload.get("markets", [])
    if not records:
        raise ValueError(f"{path}: no markets found")
    if all("outcome_labels" in record for record in records):
        return [
            MarketSpec(
                **{
                    key: (tuple(value) if isinstance(value, list) else value)
                    for key, value in record.items()
                }
            )
            for record in records
        ]
    return polymarket_markets_from_json(records)


def _emit(payload: Any) -> None:
    print(json.dumps(payload, indent=2, default=str))


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m h5i_db.venues")
    subparsers = parser.add_subparsers(dest="command", required=True)

    markets = subparsers.add_parser("markets", help="write instruments and resolutions")
    markets.add_argument("database")
    markets.add_argument("specs", help="JSON: spec objects or raw vendor payloads")

    ingest = subparsers.add_parser("ingest", help="normalise archive files into tables")
    ingest.add_argument("database")
    ingest.add_argument("specs")
    source = ingest.add_mutually_exclusive_group(required=True)
    source.add_argument("--root", help="directory to search for archive files")
    source.add_argument("--file", action="append", default=[], help="explicit file; repeatable")
    ingest.add_argument("--layout", choices=sorted(_LAYOUTS), default="pmxt")
    ingest.add_argument("--pattern", help="glob under --root; defaults to the layout's")
    ingest.add_argument("--start-ns", type=int, help="window start, epoch nanoseconds")
    ingest.add_argument("--end-ns", type=int, help="window end, exclusive")
    ingest.add_argument("--chunk-rows", type=int, default=250_000)
    ingest.add_argument("--note", help="commit note recorded with every append")
    ingest.add_argument(
        "--write-markets",
        action="store_true",
        help="also write instruments/resolutions from the same spec file",
    )
    ingest.add_argument(
        "--min-coverage",
        type=float,
        help="exit non-zero when the loaded window covers less than this fraction",
    )

    bars = subparsers.add_parser("bars", help="load OHLCV bars, or derive them from trades")
    bars.add_argument("database")
    bars_source = bars.add_mutually_exclusive_group(required=True)
    bars_source.add_argument("--root", help="directory to search for bar files")
    bars_source.add_argument("--file", action="append", default=[], help="explicit file; repeatable")
    bars_source.add_argument(
        "--from-trades",
        action="store_true",
        help="aggregate the stored trades table instead of reading files",
    )
    bars.add_argument("--layout", choices=sorted(_BAR_LAYOUTS), default="ohlcv")
    bars.add_argument("--pattern", help="glob under --root; defaults to the layout's")
    bars.add_argument(
        "--instrument",
        help="instrument id for the rows, when the files do not carry one",
    )
    bars.add_argument(
        "--interval",
        help="bar length such as 1m, 1h, 1d; required with --from-trades",
    )
    bars.add_argument("--outcome", type=int, default=0)
    bars.add_argument("--start-ns", type=int, help="window start, epoch nanoseconds")
    bars.add_argument("--end-ns", type=int, help="window end, exclusive")
    bars.add_argument("--chunk-rows", type=int, default=250_000)
    bars.add_argument("--note", help="commit note recorded with every append")

    inspect = subparsers.add_parser("inspect", help="report canonical tables in a database")
    inspect.add_argument("database")

    args = parser.parse_args(argv)
    from .. import Database
    from ._canonical import CANONICAL_SCHEMAS

    db = Database(args.database, create=args.command in ("markets", "ingest", "bars"))
    try:
        if args.command == "markets":
            report = write_markets(db, _load_specs(Path(args.specs)))
            _emit(report.to_dict())
            return 0

        if args.command == "ingest":
            layout = _LAYOUTS[args.layout]
            specs = _load_specs(Path(args.specs))
            if args.write_markets:
                _emit(write_markets(db, specs).to_dict())
            files = (
                [Path(item) for item in args.file]
                if args.file
                else discover(args.root, pattern=args.pattern, layout=layout)
            )
            if not files:
                parser.error("no archive files matched")
            if (args.start_ns is None) != (args.end_ns is None):
                parser.error("--start-ns and --end-ns must be given together")
            window = (
                (args.start_ns, args.end_ns) if args.start_ns is not None else None
            )
            report = ingest_archive(
                db,
                files=files,
                markets=specs,
                layout=layout,
                window=window,
                chunk_rows=args.chunk_rows,
                note=args.note,
            )
            _emit(report.to_dict())
            if args.min_coverage is not None:
                coverage = report.coverage
                if coverage is None:
                    print(
                        "coverage is undefined without a window; pass "
                        "--start-ns/--end-ns to gate on it",
                        file=sys.stderr,
                    )
                    return 2
                if coverage < args.min_coverage:
                    print(
                        f"coverage {coverage:.4f} is below the required "
                        f"{args.min_coverage:.4f}",
                        file=sys.stderr,
                    )
                    return 3
            return 0

        if args.command == "bars":
            if (args.start_ns is None) != (args.end_ns is None):
                parser.error("--start-ns and --end-ns must be given together")
            window = (args.start_ns, args.end_ns) if args.start_ns is not None else None
            if args.from_trades:
                if not args.interval:
                    parser.error("--from-trades needs --interval, the bar length to bucket into")
                try:
                    report = bars_from_trades(
                        db,
                        interval=args.interval,
                        chunk_rows=args.chunk_rows,
                        note=args.note,
                    )
                except ValueError as error:
                    # Asking to aggregate trades that are not there is a
                    # mistake in the command, not a defect, so it reads as one.
                    print(str(error), file=sys.stderr)
                    return 2
                _emit(report.to_dict())
                return 0
            layout = _BAR_LAYOUTS[args.layout]
            if args.interval:
                # An interval on the command line overrides the layout's, which
                # is what makes one layout serve every bar length a vendor ships.
                layout = replace(layout, interval=args.interval)
            files = (
                [Path(item) for item in args.file]
                if args.file
                else discover(args.root, pattern=args.pattern or layout.file_glob)
            )
            if not files:
                parser.error("no bar files matched")
            _emit(
                ingest_bars(
                    db,
                    files=files,
                    instrument_id=args.instrument,
                    layout=layout,
                    outcome=args.outcome,
                    window=window,
                    chunk_rows=args.chunk_rows,
                    note=args.note,
                ).to_dict()
            )
            return 0

        if args.command == "inspect":
            present = set(db.tables())
            summary = {}
            for name in CANONICAL_SCHEMAS:
                if name not in present:
                    continue
                rows = db.sql(f"SELECT count(*) AS n FROM {name}").to_pandas()["n"][0]
                versions = db.versions(name)
                summary[name] = {
                    "rows": int(rows),
                    "versions": len(versions),
                    "head": versions[-1]["sequence"] if versions else None,
                }
            _emit({"database": args.database, "tables": summary})
            return 0

        parser.error(f"unknown command {args.command!r}")
        return 2
    finally:
        db.close()


if __name__ == "__main__":  # pragma: no cover - CLI entry
    sys.exit(main())
