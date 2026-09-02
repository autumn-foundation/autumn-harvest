# Harvest end-to-end benchmark results — v0.6.0

Reference numbers for release **0.6.0**, produced by
`autumn-harvest/benches/e2e_bench.rs`. This file is a snapshot: it is **not**
updated when a later release re-measures. See
[`../benchmarks.md`](../benchmarks.md) for the methodology, the honesty framing,
and how to reproduce any number here.

> **Reference-machine guidance, not an SLO.** Read
> [`../benchmarks.md`](../benchmarks.md) before designing against anything on
> this page.

## Reference environment

| | |
|:--|:--|
| CPU | 4 logical CPUs, x86_64 |
| OS | Linux (Ubuntu 24.04) |
| Postgres | 16.13 (Ubuntu 16.13-0ubuntu0.24.04.1), `fsync=off`, `synchronous_commit=off`, `max_connections=200` |
| topology | `independent-servers` — one native Postgres cluster per shard, ports 55432+, provisioned with the same settings as the services in `benchmarks/docker-compose.yml` |
| Profile | `bench` (release) |
| Workers | 1 per shard, 8 concurrent workflow tasks, 16 concurrent activity tasks |
| Poll interval | 25 ms, with LISTEN/NOTIFY wired per shard |
| Pool size | 32 connections per shard |
| Load | closed loop, 32 workflows in flight per shard (4x the workflow slots) |
| Harness | `autumn-harvest/tests/integration/e2e_bench_support.rs` |
| Measured | 2026-09-02 |

**Container caveat.** These figures come from native Postgres clusters. The
machine they were measured on had no Docker daemon, so the compose path — the
one `./benchmarks/run.sh` runs — was **not** measured here. The delta between
the two is unquantified and this page does not claim to know its sign.

## Headline numbers

| scenario | metric | 1 shard | 2 shards | 4 shards |
|:--|:--|--:|--:|--:|
| `throughput` | workflows/sec | **24.04** | **38.44** | **36.28** |
| `dispatch_latency` | p50 ms | **39.83** | **43.19** | **55.77** |
| `dispatch_latency` | p99 ms | **60.55** | **64.43** | **109.67** |
| `signal_roundtrip` | p50 ms | **54.69** | **60.51** | **46.43** |
| `signal_roundtrip` | p99 ms | **65.93** | **68.27** | **81.54** |
| `replay_throughput` | events/sec | **9 564 120.54** | **9 387 692.65** | **9 239 806.28** |

Every one of the twelve cells reported **sound**: none was thin, truncated,
feeder-bound, off-pace, missing a shard's contribution, or measured across a
clock offset worth more than 2% of the number it publishes.

## Was the box quiet?

Read this before the numbers above, because it bounds what they are worth.

Replay is in-memory and cannot legitimately move with shard count, so its spread
across the sweep is a direct reading of how much the reference machine's own
load moved while the other nine cells were measured. **This run: 3.4%**, inside
the 10% bar the harness applies.

For contrast, an earlier sweep of the same suite on the same box drifted **8.7%**
and produced throughput figures 5–6% *lower* across the board. Same code, same
machine, different amount of background noise. That is the honest size of the
run-to-run variation these numbers carry, and it is most of the reason the
reproduction tolerance is ±15% rather than something tighter.

## What the shard sweep showed

**Sharding bought 1.60x at two shards and then gave some back.** 24.04 → 38.44 →
36.28 workflows/sec. Four shards is slightly slower than two.

This is the "or honestly failing to demonstrate" case issue #941 asks to publish
either way. The reason is the machine, not the shard model: at four shards this
box runs four Postgres clusters, four workers and the harness on four cores.
Every shard drained its full share in every cell (`s0=1200 s1=1200 s2=1200
s3=1200`), so the multi-shard runtime is genuinely using every shard rather than
starving some — what this run cannot tell you is what four shards do on four
*machines*, and nothing here should be read as an answer to that.

**The tail is what degrades.** Dispatch p99 goes 60.55 → 64.43 → 109.67 ms while
p50 moves 39.83 → 55.77. Under a fixed core count, adding shards costs the tail
about three times what it costs the median — the same run-queue effect issue
#786 documented when it chose to gate p50 rather than p99.

## Reading the latency numbers

Both latency scenarios were paced at **8 starts/sec per shard**, roughly a third
of the saturated rate above, so these percentiles describe the dispatch and
signal paths rather than queue depth. The achieved pace matched the target in
every cell (8.00, 16.00, 32.04 starts/sec; 8.02, 16.01, 32.02 signals/sec).

**Dispatch latency spans two clocks** — `harvest_task_queue.created_at` is the
database's, the handler's first line is the host's — so the harness probes the
offset before and after each measured window and refuses to publish a cell where
it exceeds 2% of the p50, or drifts by more than that across the window. On this
single-host run the offset stayed in the tens of microseconds against a ~40 ms
measurement, and no negative sample was observed.

**Signal round-trip carries no skew term at all**: both ends are read from one
monotonic clock in one process. It does include a fresh loopback TCP connection
per sample, which is pessimistic against a keep-alive client.

**Throughput carries no cross-host skew either**, but for a weaker reason worth
stating: `completed_at` is written by the worker with `Utc::now()`, so it is a
host wall clock, not the database's. Every worker in this run is in the same
process, so all completions share one clock and the middle-half window is
self-consistent. A deployment with workers on several machines would not have
that property.

## Load level

The closed loop held **32 workflows in flight per shard** — four times the
worker's eight workflow slots — and the observed mean was 31.3, 31.4 and 31.7
against that target, so the engine and not the harness was the limiter in every
cell.

That depth is a deliberate ceiling, not a maximum found by search. Running the
same sweep at 128 in flight yields a higher number, because 128 is sixteen times
the worker's slots and holds ~120 workflows *pending* per shard — measuring
inside the claim-depth curve that `../performance.md` (issue #786) publishes,
under an end-to-end label. `HARVEST_BENCH_INFLIGHT=128` reproduces it for anyone
who wants to see the difference; the report prints the population actually held
next to the rate, so the two can never be confused.

## Results

| scenario | shards | metric | value | sound |
|:--|--:|:--|--:|:--|
| `throughput` | 1 | `workflows_per_sec` | 24.04 | yes |
| `throughput` | 1 | `measured_window_secs` | 24.96 | yes |
| `throughput` | 1 | `completions` | 1200.00 | yes |
| `dispatch_latency` | 1 | `p50_ms` | 39.83 | yes |
| `dispatch_latency` | 1 | `p99_ms` | 60.55 | yes |
| `dispatch_latency` | 1 | `samples` | 1080.00 | yes |
| `dispatch_latency` | 1 | `achieved_starts_per_sec` | 8.00 | yes |
| `signal_roundtrip` | 1 | `p50_ms` | 54.69 | yes |
| `signal_roundtrip` | 1 | `p99_ms` | 65.93 | yes |
| `signal_roundtrip` | 1 | `samples` | 400.00 | yes |
| `signal_roundtrip` | 1 | `achieved_signals_per_sec` | 8.02 | yes |
| `replay_throughput` | 1 | `events_per_sec` | 9564120.54 | yes |
| `replay_throughput` | 1 | `ms_per_history` | 1.05 | yes |
| `throughput` | 2 | `workflows_per_sec` | 38.44 | yes |
| `throughput` | 2 | `measured_window_secs` | 31.22 | yes |
| `throughput` | 2 | `completions` | 2400.00 | yes |
| `dispatch_latency` | 2 | `p50_ms` | 43.19 | yes |
| `dispatch_latency` | 2 | `p99_ms` | 64.43 | yes |
| `dispatch_latency` | 2 | `samples` | 2160.00 | yes |
| `dispatch_latency` | 2 | `achieved_starts_per_sec` | 16.00 | yes |
| `signal_roundtrip` | 2 | `p50_ms` | 60.51 | yes |
| `signal_roundtrip` | 2 | `p99_ms` | 68.27 | yes |
| `signal_roundtrip` | 2 | `samples` | 800.00 | yes |
| `signal_roundtrip` | 2 | `achieved_signals_per_sec` | 16.01 | yes |
| `replay_throughput` | 2 | `events_per_sec` | 9387692.65 | yes |
| `replay_throughput` | 2 | `ms_per_history` | 1.07 | yes |
| `throughput` | 4 | `workflows_per_sec` | 36.28 | yes |
| `throughput` | 4 | `measured_window_secs` | 66.16 | yes |
| `throughput` | 4 | `completions` | 4800.00 | yes |
| `dispatch_latency` | 4 | `p50_ms` | 55.77 | yes |
| `dispatch_latency` | 4 | `p99_ms` | 109.67 | yes |
| `dispatch_latency` | 4 | `samples` | 4320.00 | yes |
| `dispatch_latency` | 4 | `achieved_starts_per_sec` | 32.04 | yes |
| `signal_roundtrip` | 4 | `p50_ms` | 46.43 | yes |
| `signal_roundtrip` | 4 | `p99_ms` | 81.54 | yes |
| `signal_roundtrip` | 4 | `samples` | 1600.00 | yes |
| `signal_roundtrip` | 4 | `achieved_signals_per_sec` | 32.02 | yes |
| `replay_throughput` | 4 | `events_per_sec` | 9239806.28 | yes |
| `replay_throughput` | 4 | `ms_per_history` | 1.08 | yes |


## Notes

### `throughput` at 1 shard(s)

* per-shard completions: s0=1200
* topology: independent-servers
* closed loop: 32 workflows in flight per shard, 1200 measured completions, warmup population 240
* mean in-flight population: 31.3 per shard against a target of 32
* the headline is the middle-half rate: the ramp-up and the drain-down tails are excluded, so it is a sustained rate rather than an average over a changing queue depth
* wall clock: 60.3s
* sound: every published number on this row rests on a measured sample

### `dispatch_latency` at 1 shard(s)

* per-shard completions: s0=400
* topology: independent-servers
* target pace: 8.0 workflow starts/s
* host-to-database clock offset before the window: +0.030 ms (per shard, median of 7 probes)
* host-to-database clock offset after the window: +0.032 ms
* 0 re-dispatched task row(s); 0 dispatch(es) recorded no task id
* wall clock: 54.3s
* sound: every published number on this row rests on a measured sample

### `signal_roundtrip` at 1 shard(s)

* per-shard completions: s0=400
* topology: independent-servers
* target pace: 8.0 signals/s
* 400 measured signals after a discarded warmup cohort of 80
* wall clock: 68.7s
* sound: every published number on this row rests on a measured sample

### `replay_throughput` at 1 shard(s)

* 10001 events replayed (5000 activities), median of 20 iterations after 5 warmup iterations
* shard-invariant by construction: this row is the run's noise control, not a statement about sharding
* the same history `benches/replay_bench.rs` budgets at 200 ms (issue #135)
* sound: every published number on this row rests on a measured sample

### `throughput` at 2 shard(s)

* per-shard completions: s0=1200 s1=1200
* topology: independent-servers
* closed loop: 32 workflows in flight per shard, 2400 measured completions, warmup population 480
* mean in-flight population: 31.4 per shard against a target of 32
* the headline is the middle-half rate: the ramp-up and the drain-down tails are excluded, so it is a sustained rate rather than an average over a changing queue depth
* wall clock: 75.2s
* sound: every published number on this row rests on a measured sample

### `dispatch_latency` at 2 shard(s)

* per-shard completions: s0=400 s1=400
* topology: independent-servers
* target pace: 16.0 workflow starts/s
* host-to-database clock offset before the window: +0.045, +0.024 ms (per shard, median of 7 probes)
* host-to-database clock offset after the window: +0.024, +0.073 ms
* 0 re-dispatched task row(s); 0 dispatch(es) recorded no task id
* wall clock: 55.5s
* sound: every published number on this row rests on a measured sample

### `signal_roundtrip` at 2 shard(s)

* per-shard completions: s0=400 s1=400
* topology: independent-servers
* target pace: 16.0 signals/s
* 800 measured signals after a discarded warmup cohort of 160
* wall clock: 71.1s
* sound: every published number on this row rests on a measured sample

### `replay_throughput` at 2 shard(s)

* 10001 events replayed (5000 activities), median of 20 iterations after 5 warmup iterations
* shard-invariant by construction: this row is the run's noise control, not a statement about sharding
* the same history `benches/replay_bench.rs` budgets at 200 ms (issue #135)
* sound: every published number on this row rests on a measured sample

### `throughput` at 4 shard(s)

* per-shard completions: s0=1200 s1=1200 s2=1200 s3=1200
* topology: independent-servers
* closed loop: 32 workflows in flight per shard, 4800 measured completions, warmup population 960
* mean in-flight population: 31.7 per shard against a target of 32
* the headline is the middle-half rate: the ramp-up and the drain-down tails are excluded, so it is a sustained rate rather than an average over a changing queue depth
* wall clock: 159.6s
* sound: every published number on this row rests on a measured sample

### `dispatch_latency` at 4 shard(s)

* per-shard completions: s0=400 s1=400 s2=400 s3=400
* topology: independent-servers
* target pace: 32.0 workflow starts/s
* host-to-database clock offset before the window: +0.205, +0.031, +0.023, +0.023 ms (per shard, median of 7 probes)
* host-to-database clock offset after the window: +0.018, +0.031, +0.040, +0.066 ms
* 0 re-dispatched task row(s); 0 dispatch(es) recorded no task id
* wall clock: 62.3s
* sound: every published number on this row rests on a measured sample

### `signal_roundtrip` at 4 shard(s)

* per-shard completions: s0=400 s1=400 s2=400 s3=400
* topology: independent-servers
* target pace: 32.0 signals/s
* 1600 measured signals after a discarded warmup cohort of 320
* wall clock: 79.5s
* sound: every published number on this row rests on a measured sample

### `replay_throughput` at 4 shard(s)

* 10001 events replayed (5000 activities), median of 20 iterations after 5 warmup iterations
* shard-invariant by construction: this row is the run's noise control, not a statement about sharding
* the same history `benches/replay_bench.rs` budgets at 200 ms (issue #135)
* sound: every published number on this row rests on a measured sample


## Noise control

The replay scenario is in-memory and shard-invariant, so its spread across the sweep bounds how much the reference box's own load moved while the other cells were measured.

* replay spread across the sweep: **3.4%**
* within the 10% bar: the box stayed quiet enough for the other cells to be comparable with each other.


Every scenario reported sound.

