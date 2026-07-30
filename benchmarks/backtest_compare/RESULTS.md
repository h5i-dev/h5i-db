# Backtest engine comparison

Date: 2026-07-29 · Machine: WSL2 Linux, aarch64, 10 cores, 7.5 GiB RAM ·
Workload: `top_of_book_market_orders_v1`.

The workload sends one instrument 200,000 deterministic top-of-book updates
and submits 200 alternating one-unit market orders. Every adapter verifies that
the engine observed all 200,000 events and submitted all 200 orders. The
reported values are medians of three fresh-process measurements after one
warm-up.

| engine | measured boundary | median | throughput |
|---|---|---:|---:|
| h5i-db `7659713d` | decoded records through replay kernel | **65.7 ms** | **3.05 M events/s** |
| NautilusTrader 1.230.0 | in-memory model objects through `BacktestEngine.run()` | 767 ms | 261 k events/s |
| LEAN `11ba019f6` | first `Slice` callback through `OnEndOfAlgorithm`, disk-fed | 2,033 ms | 98.4 k events/s |

h5i-db's persisted run, which scans and decodes its database, creates a fork
and run tables, executes the kernel, and writes results, had a **331 ms**
median (605 k input events/s). Its median decode alone was 84.2 ms.

On this workload:

- h5i-db's decoded-record kernel is **11.7×** the NautilusTrader engine
  throughput and **31.0×** the LEAN disk-fed callback throughput;
- even h5i-db's persisted-run boundary is **2.3×** NautilusTrader's in-memory
  engine throughput and **6.1×** LEAN's measured callback throughput; and
- h5i-db storage orchestration, not event replay, is the larger optimization
  target: the persisted boundary takes about 5.0× the kernel time.

## Interpretation limits

This establishes a reproducible advantage for this narrow event-driven
workload, not a universal ranking of backtest systems.

- The h5i and Nautilus kernel boundaries begin with already-materialized
  objects. LEAN's public launcher reads compressed second-resolution quote
  data during the measured interval and performs subscription time-slicing.
- Nautilus invokes a Python strategy callback for every quote. h5i's signal
  stream and LEAN's strategy are native Rust and C#, respectively.
- The engines use their native instrument and execution models: an h5i
  perpetual, a Nautilus simulated FX instrument, and LEAN Oanda FX. The
  benchmark checks event and order counts, not PnL equivalence.
- h5i events are densely timestamped because its replay kernel is independent
  of wall-clock spacing. LEAN uses one-second quote bars and the generator
  skips Oanda's weekday maintenance closure. There are no timers or latency
  models in the workload.
- The local Nautilus checkout is 1.231.0, but building it exceeded this
  machine's memory twice. The benchmark therefore uses the nearest published
  aarch64 wheel, 1.230.0, and records that version in the artifact.

Raw samples, workload parameters, machine metadata, and exact revisions are in
[`results.json`](results.json). See [`README.md`](README.md) for reproduction
commands and the memory-safe serial LEAN build.
