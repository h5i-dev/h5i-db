# Cross-engine backtest comparison

This benchmark uses one deterministic semantic workload in h5i-db,
NautilusTrader, and LEAN:

- one instrument;
- 200,000 ordered top-of-book updates;
- 200 alternating one-unit market orders; and
- a slowly oscillating mid price with a two-tick spread.

It is intentionally not presented as a single universal "winner" score.
NautilusTrader exposes an in-memory `BacktestEngine.run()` boundary, h5i-db
exposes both an in-memory decoded-record kernel and a persisted run, and LEAN's
public launcher streams one-second disk-backed subscription data. Results therefore report
the boundary next to every number.

## Prerequisites

The reference checkouts default to `../../Ref/nautilus_trader` and
`../../Ref/Lean` relative to the h5i-db repository.

NautilusTrader can be installed from a wheel without compiling its large Rust
workspace:

```bash
uv venv /tmp/h5i-nautilus-wheel-venv
uv pip install --python /tmp/h5i-nautilus-wheel-venv/bin/python \
    --only-binary=:all: nautilus_trader
```

The local Nautilus checkout currently identifies itself as 1.231.0, while the
available wheel is 1.230.0. The emitted result always records the version
actually executed.

Build LEAN and the comparison algorithm serially on memory-constrained hosts:

```bash
DOTNET_CLI_TELEMETRY_OPTOUT=1 /tmp/h5i-dotnet/dotnet build \
    ../../Ref/Lean/Launcher/QuantConnect.Lean.Launcher.csproj \
    --configuration Release --maxcpucount:1 -p:BuildInParallel=false
DOTNET_CLI_TELEMETRY_OPTOUT=1 /tmp/h5i-dotnet/dotnet build \
    benchmarks/backtest_compare/lean/H5i.BacktestCompare.csproj \
    --configuration Release --maxcpucount:1 -p:BuildInParallel=false
```

Run individual adapters:

```bash
/tmp/h5i-nautilus-wheel-venv/bin/python \
    benchmarks/backtest_compare/nautilus_runner.py \
    --workload benchmarks/backtest_compare/workload.json

python3 benchmarks/backtest_compare/lean_runner.py \
    --workload benchmarks/backtest_compare/workload.json \
    --lean-root ../../Ref/Lean --dotnet /tmp/h5i-dotnet/dotnet

cargo bench -p h5i-db-backtest --bench replay_path -- \
    --book 200000 --trades 0 --instruments 1 --signals 200 --trials 1 \
    --common-quotes
```

Data generation and compilation are outside all reported timing boundaries.
Run each adapter in a fresh process at least three times and compare medians.
The coordinator performs one warm-up plus the repetitions in `workload.json`,
validates event/order counts, and emits both raw samples and medians:

```bash
python3 benchmarks/backtest_compare/run.py \
    --output benchmarks/backtest_compare/results.json
```
