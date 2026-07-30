"""Factor evaluation: h5i-db against alphalens on identical data.

ROADMAP_QUANT.md §4.3 claims the engine path beats a pandas port end to end.
This is the script that decides whether that claim holds. It builds one
synthetic panel, runs the same six statistics through both stacks, and
prints wall-clock times plus a correctness check, because a speed number
from a wrong computation is worthless.

    python benchmarks/compare_alphalens.py --assets 500 --years 5

Needs alphalens-reloaded in the same environment. Sized to fit a small
machine by default; --assets 2500 --years 15 is the roadmap's figure and
wants real memory.
"""

from __future__ import annotations

import argparse
import datetime as dt
import tempfile
import time
from contextlib import contextmanager

import numpy as np
import pandas as pd
import pyarrow as pa

import h5i_db
from h5i_db import quant

PRICE_SCHEMA = pa.schema(
    [
        pa.field("ts", pa.timestamp("us"), nullable=False),
        pa.field("asset", pa.string()),
        pa.field("price", pa.float64()),
    ]
)
FACTOR_SCHEMA = pa.schema(
    [
        pa.field("ts", pa.timestamp("us"), nullable=False),
        pa.field("asset", pa.string()),
        pa.field("factor", pa.float64()),
    ]
)
PERIODS = (1, 5, 10)
QUANTILES = 5


@contextmanager
def timed(label, out):
    start = time.perf_counter()
    yield
    out[label] = time.perf_counter() - start


def build_data(n_assets, n_dates, seed=7):
    rng = np.random.default_rng(seed)
    assets = [f"A{i:05d}" for i in range(n_assets)]
    dates = [dt.datetime(2010, 1, 1) + dt.timedelta(days=d) for d in range(n_dates)]
    steps = rng.normal(0.0002, 0.013, size=(n_dates, n_assets)).astype(np.float64)
    levels = 100.0 * np.exp(np.cumsum(steps, axis=0))
    factors = rng.normal(size=(n_dates, n_assets)).astype(np.float64)
    return assets, dates, levels, factors


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--assets", type=int, default=500)
    ap.add_argument("--years", type=float, default=5.0)
    ap.add_argument("--skip-alphalens", action="store_true")
    ap.add_argument(
        "--fast",
        action="store_true",
        help="allow parallel execution (deterministic=False); results stop "
        "being bit-reproducible between runs",
    )
    args = ap.parse_args()

    n_dates = int(args.years * 252)
    assets, dates, levels, factors = build_data(args.assets, n_dates)
    rows = args.assets * n_dates
    print(f"panel: {args.assets} assets x {n_dates} dates = {rows:,} rows")

    times = {}
    with tempfile.TemporaryDirectory() as tmp:
        db = h5i_db.Database(f"{tmp}/bench.db", create=True)
        db.create_table("prices", PRICE_SCHEMA, time_column="ts")
        db.create_table("signals", FACTOR_SCHEMA, time_column="ts")
        flat_ts = np.repeat(np.array(dates, dtype="datetime64[us]"), args.assets)
        flat_asset = np.tile(np.array(assets, dtype=object), n_dates)
        with timed("h5i ingest", times):
            db.append(
                "prices",
                pa.table(
                    {"ts": flat_ts, "asset": flat_asset, "price": levels.reshape(-1)},
                    schema=PRICE_SCHEMA,
                ),
            )
            db.append(
                "signals",
                pa.table(
                    {"ts": flat_ts, "asset": flat_asset, "factor": factors.reshape(-1)},
                    schema=FACTOR_SCHEMA,
                ),
            )
        db.snapshot("bench")

        h5i_results = {}
        with timed("h5i total", times):
            with timed("h5i build_panel", times):
                panel = quant.build_panel(
                    db, "signals", "prices", periods=PERIODS, quantiles=QUANTILES,
                    filter_zscore=None, max_loss=1.0, snapshot="bench",
                    deterministic=not args.fast,
                )
            with timed("h5i ic", times):
                h5i_results["ic"] = panel.ic().to_pandas()
            with timed("h5i quantile_returns", times):
                h5i_results["qr"] = panel.quantile_returns().to_pandas()
            with timed("h5i turnover", times):
                h5i_results["to"] = panel.turnover().to_pandas()
            with timed("h5i autocorrelation", times):
                h5i_results["ac"] = panel.rank_autocorrelation().to_pandas()
            with timed("h5i factor_returns", times):
                h5i_results["fr"] = panel.returns().to_pandas()
        db.close()

    if not args.skip_alphalens:
        from alphalens import performance as al_perf
        from alphalens import utils as al_utils

        prices_df = pd.DataFrame(levels, index=pd.DatetimeIndex(dates), columns=assets)
        factor_df = pd.DataFrame(factors, index=pd.DatetimeIndex(dates), columns=assets)
        factor_series = factor_df.stack()
        factor_series.index = factor_series.index.set_names(["date", "asset"])

        al_results = {}
        with timed("alphalens total", times):
            with timed("alphalens get_clean_factor", times):
                data = al_utils.get_clean_factor_and_forward_returns(
                    factor=factor_series, prices=prices_df, periods=PERIODS,
                    quantiles=QUANTILES, filter_zscore=None, max_loss=1.0,
                )
            with timed("alphalens ic", times):
                al_results["ic"] = al_perf.factor_information_coefficient(data)
            with timed("alphalens quantile_returns", times):
                al_results["qr"] = al_perf.mean_return_by_quantile(data)
            with timed("alphalens turnover", times):
                al_results["to"] = {
                    q: al_perf.quantile_turnover(data["factor_quantile"], q, 1)
                    for q in range(1, QUANTILES + 1)
                }
            with timed("alphalens autocorrelation", times):
                al_results["ac"] = al_perf.factor_rank_autocorrelation(data, 1)
            with timed("alphalens factor_returns", times):
                al_results["fr"] = al_perf.factor_returns(data)

        first_col = list(
            al_utils.get_forward_returns_columns(al_results["ic"].columns)
        )[0]
        drift = np.abs(
            h5i_results["ic"]["ic_1"].to_numpy()
            - al_results["ic"][first_col].to_numpy()
        ).max()
        print(f"\nmax |IC difference| vs alphalens: {drift:.3e}")
        assert drift < 1e-9, "the two stacks disagree; the timings are meaningless"

    print()
    width = max(len(k) for k in times)
    for key in sorted(times):
        print(f"{key:<{width}}  {times[key] * 1000:9.1f} ms")
    if "alphalens total" in times:
        speedup = times["alphalens total"] / times["h5i total"]
        print(f"\nh5i-db is {speedup:.2f}x alphalens end to end "
              f"(excluding ingest, which alphalens has no equivalent of)")


if __name__ == "__main__":
    main()
