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
median (605 k input events/s) at `7659713d`. Its median decode alone was
84.2 ms. That boundary has since moved; see [Persisted run,
re-measured](#persisted-run-re-measured-2026-07-31) below.

On this workload:

- h5i-db's decoded-record kernel is **11.7×** the NautilusTrader engine
  throughput and **31.0×** the LEAN disk-fed callback throughput;
- even h5i-db's persisted-run boundary is **2.3×** NautilusTrader's in-memory
  engine throughput and **6.1×** LEAN's measured callback throughput; and
- h5i-db storage orchestration, not event replay, is the larger optimization
  target: the persisted boundary takes about 5.0× the kernel time.

## Persisted run, re-measured 2026-07-31

The last bullet above was acted on at `d7f5bf0b`, which batches the metadata
commits a run pays for. Only the persisted boundary changes: the kernel is
untouched, and the three-engine table above stands as measured.

Re-measuring means re-measuring *both* revisions, because this machine drifts
between days by more than the change is worth. Both were built from the same
tree state, run in alternating fresh processes on 2026-07-31, and reported as
medians of five after one warm-up.

| boundary | `a3d5c2a4` (before) | `d7f5bf0b` (after) | |
|---|---:|---:|---:|
| persisted run, end to end | 386 ms | **280 ms** | -27 % |
| … as input throughput | 518 k events/s | **713 k events/s** | |
| decode (Arrow → `Record`) | 82.2 ms | 67.0 ms | -19 % |
| replay kernel | 69.9 ms | 65.7 ms | untouched |
| create run tables | 71.2 ms | 39.7 ms | -44 % |
| write run (one transaction) | 121.5 ms | 51.4 ms | -58 % |
| four-trial study | 1,371 ms | 1,070 ms | -22 % |

Two things to read off that table rather than assume:

- The kernel row is the control. Nothing in `d7f5bf0b` touches it, and the
  4.2 ms between the two arms is this machine's noise band on a phase that
  did not change. The 65.7 ms it lands on is also the figure the 2026-07-29
  table reports, which is why that table needs no revision.
- The `a3d5c2a4` arm measured 386 ms where 2026-07-29 measured 331 ms for the
  same code. The machine was slower on 2026-07-31, not faster. Comparing the
  280 ms against numbers collected on the faster day therefore understates
  the change rather than flattering it.

Against the 2026-07-29 competitor figures the persisted boundary is now
**2.7×** NautilusTrader's in-memory engine throughput and **7.3×** LEAN's, and
it takes **4.3×** the kernel time rather than 5.0×. Storage orchestration is
still the larger target: durability barriers, not scanning or replay, are what
is left in that 4.3×.

Reproduce either arm with:

```sh
cargo bench -p h5i-db-backtest --bench replay_path -- \
    --book 200000 --trades 0 --instruments 1 --signals 200 \
    --common-quotes --trials 4
```

## What the strategy boundary costs, 2026-07-31

The table at the top measures h5i through its declarative path: the strategy
is a `signals` table, so the replay never calls Python. Nautilus is measured
through a Python callback per quote. That is a real difference in what the two
were asked to do, and the ratio alone invites the reading that the kernel is
an order of magnitude faster at the same job. It is not.

h5i has the other path too. `backtest.EventStrategy` is public, and its
adapter reacquires the GIL for every callback deliberately. Arms run over one
seeded database, alternating within one process, so everything outside the
strategy boundary is identical and cancels in the differences.

Measuring it first showed most of the cost was not the call. It was building
argument dictionaries the callback never read, so the adapter was changed and
measured again. The `signals` arm is the control: nothing in the change
touches it, and it moved 394.6 → 395.9 ms, which is how much this machine
drifted between the two sessions.

| arm | strategy | before | after |
|---|---|---:|---:|
| `signals` | declarative; no Python during replay | 394.6 ms | 395.9 ms |
| `fills_only` | wants fills; `on_event` left as the base no-op | — | 416.0 ms |
| `noop` | `EventStrategy.on_event` returns `None` | 775.8 ms | 535.3 ms |
| `trading` | `EventStrategy` submits the same 200 orders | 897.4 ms | 608.3 ms |

| per event | before | after | |
|---|---:|---:|---:|
| crossing into Python and back (`noop` − `signals`) | 1.906 µs | 0.697 µs | -63 % |
| both, with a body (`trading` − `signals`) | 2.514 µs | 1.062 µs | -58 % |
| a callback the strategy never overrode | 1.906 µs | 0.101 µs | -95 % |
| a whole native kernel step, for comparison | 0.329 µs | 0.329 µs | untouched |

A callback costs **3.2× a complete native kernel step** now rather than 7.6×.
Book application, matching, mark bookkeeping and liquidation checks together
are still cheaper than handing the event to Python, which is the shape of the
problem and not something an adapter can fix.

The last row is a separate finding. `EventStrategy` defines all four callbacks
as no-ops, so a strategy that only wanted fills was indistinguishable from one
that wanted every event: the adapter crossed the boundary 200k times to be
handed `None`. Resolving the bound methods once, and skipping any the strategy
left at the base class's version, removes that entirely.

Nautilus was re-run the same day for a like-for-like comparison: 884 / 864 /
784 ms, median **864.3 ms** against the 766.6 ms recorded on 2026-07-29, which
is the same direction of drift the h5i arms show. Its benchmark strategy
(`nautilus_runner.py:40`) increments a counter and compares twice per quote,
which is the work the `trading` arm does.

| comparison | h5i | Nautilus | |
|---|---:|---:|---:|
| declarative path against callback path | 65.7 ms | 864.3 ms | 13.2× |
| callback against callback, before | 568.5 ms¹ | 864.3 ms | 1.52× |
| callback against callback, after | 278.1 ms¹ | 864.3 ms | **3.11×** |

¹ Derived, not measured: the native kernel (65.7 ms) plus the measured
  `trading` boundary cost. The Python API exposes only the persisted-run
  boundary, so the kernel-with-callback figure cannot be timed directly. The
  boundary term is measured; the kernel term is from the same day's
  `replay_path` run.

So the order-of-magnitude figure is mostly a difference in boundary, not in
engine. What survives as an architectural difference is narrower and worth
stating precisely: because the strategy can be a table, h5i can *choose* not
to have a boundary, and a system whose strategy is always an object receiving
callbacks has no equivalent choice. That choice is only available where the
decision is path-independent. React to your own fills and the run is on
`EventStrategy`, paying about a microsecond an event.

### What `context` costs, and what that implies

Callbacks may be declared without the `context` parameter, in which case it
is not built. Whether to build it is read once from the signature rather than
decided per event. A fifth arm prices it: `noop_lean` is `noop` with one
fewer parameter, same crossing and same body.

Pricing it needed the storage taken out of the way. On disk, eleven rounds
put `noop_lean` at 543.9 ms against `noop` at 518.6 -- the arm doing strictly
less work, slower. The medians were not separated, because each arm is a
whole persisted run of which the boundary is about a fifth, and the rest is
dominated by the fsyncs the persisted-run section above measures. Adding
rounds does not fix a signal buried under the wrong noise source; moving the
database to tmpfs does.

| arm | median (tmpfs) |
|---|---:|
| `signals` | 250.4 ms |
| `fills_only` | 278.0 ms |
| `noop_lean` | 376.3 ms |
| `noop` | 395.3 ms |
| `trading` | 405.1 ms |

The ladder is monotone, which is the check that it is working at all.

| per event | µs |
|---|---:|
| the whole crossing (`noop` − `signals`) | 0.724 |
| of which building `context` (`noop` − `noop_lean`) | **0.095** |
| what is left | 0.630 |

So `context` is 13% of the boundary. The estimate that motivated the change
was 0.25 µs, which was 2.6× too high: it came from counting objects rather
than what is in them, and `context` with no open positions is nearly empty.

The crossing itself measures 0.724 µs here against 0.697 µs on disk, which is
the cross-check worth having -- the boundary does not depend on where the
database lives, so the two environments agreeing is evidence neither number
is an artifact.

What the 0.630 µs remainder implies is worth stating as an inference rather
than a result. `context` is a pyclass, a `Vec`, and a position snapshot, and
it costs 0.095 µs. The event view is a pyclass and a `Vec` of thirteen, so it
should be the same order. That puts all object construction at roughly 0.2 µs
and leaves about 0.5 for the GIL acquisition and the call. If that holds,
eliding further objects is the wrong direction and the remaining cost is in
holding the GIL across a run -- which trades away other Python threads, so it
wants measuring before it is built, not after.

```sh
# tmpfs, for the reason above
TMPDIR=/dev/shm python3 benchmarks/backtest_compare/h5i_callback_boundary.py \
    --events 200000 --signals 200 --rounds 11 \
    --output benchmarks/backtest_compare/callback_boundary.json
```

Eleven rounds rather than five because the arms are close enough now that five
put `trading` below `noop`, which cannot be true: the medians were not
separated, and reporting them would have been reporting noise.

## What the kernel costs per instrument, 2026-07-31

Every number above was measured with one instrument, which turned out to be
the setting that hides the engine's worst scaling. Holding the event count at
200k and varying only how many instruments they are spread across:

| instruments | before | after |
|---:|---:|---:|
| 1 | 0.389 µs/event | 0.312 µs/event |
| 32 | 0.490 | 0.383 |
| 256 | 0.625 | 0.434 |
| 1024 | 0.712 | 0.522 |
| **growth, 1 → 1024** | **1.83×** | **1.67×** |

Before, the increments tracked `log2(N)` almost exactly — the shape of a tree
descent — and 45% of the per-event cost at 1024 instruments was growth in `N`
rather than anything the run had asked for. Six of the engine's maps were
keyed by `(InstrumentId, OutcomeId)`; the id is an `Arc<str>`, so every lookup
compared string contents and every insert cloned an `Arc`, six deep, on every
record. They are keyed by a dense `u32` slot now, computed once from the
instrument set.

**The growth figures are the result. The absolutes are not.** Each column was
measured within one session, alternating instrument counts, so the ratio down
each column is drift-free. The two columns are different sessions, so
comparing 0.389 against 0.312 carries a session of drift and comparing the
two ratios does not.

An alternating A/B of the two revisions makes the reason to distrust the
absolutes concrete. `decode` is the control: untouched by the change, and run
*before* the kernel in the same process, so nothing the engine does can reach
it.

| | before | after | |
|---|---:|---:|---:|
| kernel | 71.0 ms | 59.9 ms | -15.6 % |
| decode — unchanged code | 78.9 ms | 68.9 ms | -12.8 % |
| kernel ÷ decode | 0.918 | 0.882 | **-4.0 %** |

Decode cannot have got faster, so its -12.8% is common mode: two separately
linked binaries with different code layout, an effect worth 5-10% on its own.
Against that control the kernel's differential is about 4%, and the honest
statement is that this setup does not separate it from drift.

Which is why the table at the top of this file did not move. Replacing 65.7 ms
with 59.9 ms would publish drift as improvement, the same mistake the
persisted-run note above exists to avoid. The scaling result needs no such
claim: it is a shape, measured within a session, and it holds whatever the
machine was doing that day.

```sh
# vary --instruments over 1, 32, 256, 1024 with everything else fixed
cargo bench -p h5i-db-backtest --bench replay_path -- \
    --book 200000 --trades 0 --instruments 1024 --signals 200 \
    --common-quotes --trials 1
```

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
- Comparisons across sessions on this machine carry its drift, which the
  kernel-scaling section measures at 5-10% for a relinked binary alone. Only
  figures gathered by alternating arms within one session are quoted as
  before-and-after here; anything else is quoted as what it is.
- The 2.7× and 7.3× above put a 2026-07-31 h5i measurement beside 2026-07-29
  competitor measurements. Neither competitor's code changed, and the
  re-measured `a3d5c2a4` arm shows which direction the machine drifted, but
  they are not one session. A single-session three-engine table needs the
  coordinator re-run, which needs the LEAN launcher rebuilt.

Raw samples, workload parameters, machine metadata, and exact revisions for the
2026-07-29 run are in [`results.json`](results.json); the 2026-07-31
re-measurement is the `replay_path` command above rather than a coordinator
artifact. See [`README.md`](README.md) for reproduction
commands and the memory-safe serial LEAN build.
