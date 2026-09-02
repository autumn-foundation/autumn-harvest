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
| topology | `independent-servers` — one native Postgres cluster per shard, ports 55432+, the same shape `benchmarks/docker-compose.yml` provisions as containers |
| Profile | `bench` (release) |
| Workers | 1 per shard, 8 concurrent workflow tasks, 16 concurrent activity tasks |
| Poll interval | 25 ms, with LISTEN/NOTIFY wired per shard |
| Pool size | 32 connections per shard |
| Load | closed loop, 128 workflows in flight per shard |
| Harness | `autumn-harvest/tests/integration/e2e_bench_support.rs` |
| Measured | 2026-09-01 |

**Container caveat.** These figures come from native Postgres clusters, not from
the compose topology a reader will run. Container networking and storage add
overhead that was not separately quantified, so a compose run is the more
pessimistic of the two.

## Headline numbers

| scenario | metric | 1 shard | 2 shards | 4 shards |
|:--|:--|--:|--:|--:|
| `throughput` | workflows/sec | **22.65** | **34.97** | **33.53** |
| `dispatch_latency` | p50 ms | **37.78** | **43.56** | **45.74** |
| `dispatch_latency` | p99 ms | **55.60** | **62.23** | **100.03** |
| `signal_roundtrip` | p50 ms | **55.79** | **59.66** | **45.60** |
| `signal_roundtrip` | p99 ms | **65.44** | **69.02** | **80.24** |
| `replay_throughput` | events/sec | **9 960 371.64** | **9 657 651.19** | **9 096 540.11** |

Every one of the twelve cells reported **sound**: no cell was thin, truncated,
feeder-bound, off-pace, or missing a shard's contribution.

## What the shard sweep showed

**Sharding bought 1.54x at two shards and then stopped.** 22.65 → 34.97 →
33.53 workflows/sec. Four shards is *slower* than two.

This is the "honestly failing to demonstrate" case issue #941 asks to publish
either way, and the reason is visible in the run's own control. The replay
scenario is in-memory and cannot legitimately move with shard count, yet it
drifts **-8.7%** across the sweep (9 960 371 → 9 096 540 events/sec). That drift
is the reference box getting more loaded as shards are added: at four shards
this machine is running four Postgres clusters, four workers and the harness on
four cores. The 4-shard column is therefore measured under roughly 9% more
background load than the 1-shard column, and the throughput plateau is a
statement about **a fixed four-core machine**, not about Harvest's shard model.

What this run does establish: every shard drained its full share in every cell
(`s0=1200 s1=1200 s2=1200 s3=1200`), so the multi-shard runtime is genuinely
using every shard rather than starving some. What it cannot establish is what
four shards do on four *machines*. Nothing here should be read as an answer to
that.

**The tail is the number that degrades.** Dispatch p99 goes 55.60 → 62.23 →
100.03 ms while p50 barely moves (37.78 → 45.74). Under a fixed core count,
adding shards costs the tail roughly twice what it costs the median — the same
run-queue effect issue #786 documented when it chose to gate p50 rather than
p99.

## Reading the latency numbers

Both latency scenarios were paced at **8 starts/sec per shard**, roughly 30% of
the saturated rate above, so these percentiles describe the dispatch and signal
paths rather than queue depth. The achieved pace matched the target in every
cell (8.01, 16.03, 32.06 starts/sec; 8.02, 16.02, 31.99 signals/sec).

**Dispatch latency spans two clocks** — `harvest_task_queue.created_at` is the
database's, the handler's first line is the host's — so the harness measured the
offset between them on every shard and publishes it:

| shards | measured host-to-database offset |
|--:|:--|
| 1 | +0.033 ms |
| 2 | +0.036, +0.063 ms |
| 4 | -0.001, +0.014, +0.036, +0.073 ms |

At tens of microseconds against a ~38 ms measurement, the error term is under
0.2% and no negative sample was observed. The offset is published rather than
assumed away.

**Signal round-trip carries no skew term at all**: both ends are read from one
monotonic clock in one process.

## Reproduction check

`HARVEST_BENCH_CHECK=1` re-ran the replay scenario against these published
baselines on the same machine, an hour after they were taken:

| scenario | shards | metric | published | measured | error | verdict |
|:--|--:|:--|--:|--:|--:|:--|
| `replay_throughput` | 1 | `events_per_sec` | 9960371.64 | 8693747.30 | -12.7% | reproduced |
| `replay_throughput` | 2 | `events_per_sec` | 9657651.19 | 9287919.95 | -3.8% | reproduced |
| `replay_throughput` | 4 | `events_per_sec` | 9096540.11 | 8796596.41 | -3.3% | reproduced |

All three inside the ±15% band, and the -12.7% on the first cell is worth
reading rather than skimming past: it is the *same* binary replaying the *same*
in-memory history on the *same* box, so it is a direct measurement of this
machine's run-to-run noise floor. A ±15% band is not generous here — it is
roughly what one reference machine costs you before any of your hardware
differs from it at all.

## Full report

Verbatim output of the run, rendered by the harness rather than typed by hand.

## Results

| scenario | shards | metric | value | sound |
|:--|--:|:--|--:|:--|
| `throughput` | 1 | `workflows_per_sec` | 22.65 | yes |
| `throughput` | 1 | `measured_window_secs` | 26.49 | yes |
| `throughput` | 1 | `completions` | 1200.00 | yes |
| `dispatch_latency` | 1 | `p50_ms` | 37.78 | yes |
| `dispatch_latency` | 1 | `p99_ms` | 55.60 | yes |
| `dispatch_latency` | 1 | `samples` | 1080.00 | yes |
| `dispatch_latency` | 1 | `achieved_starts_per_sec` | 8.01 | yes |
| `signal_roundtrip` | 1 | `p50_ms` | 55.79 | yes |
| `signal_roundtrip` | 1 | `p99_ms` | 65.44 | yes |
| `signal_roundtrip` | 1 | `samples` | 400.00 | yes |
| `signal_roundtrip` | 1 | `achieved_signals_per_sec` | 8.02 | yes |
| `replay_throughput` | 1 | `events_per_sec` | 9960371.64 | yes |
| `replay_throughput` | 1 | `ms_per_history` | 1.00 | yes |
| `throughput` | 2 | `workflows_per_sec` | 34.97 | yes |
| `throughput` | 2 | `measured_window_secs` | 34.31 | yes |
| `throughput` | 2 | `completions` | 2400.00 | yes |
| `dispatch_latency` | 2 | `p50_ms` | 43.56 | yes |
| `dispatch_latency` | 2 | `p99_ms` | 62.23 | yes |
| `dispatch_latency` | 2 | `samples` | 2160.00 | yes |
| `dispatch_latency` | 2 | `achieved_starts_per_sec` | 16.03 | yes |
| `signal_roundtrip` | 2 | `p50_ms` | 59.66 | yes |
| `signal_roundtrip` | 2 | `p99_ms` | 69.02 | yes |
| `signal_roundtrip` | 2 | `samples` | 800.00 | yes |
| `signal_roundtrip` | 2 | `achieved_signals_per_sec` | 16.02 | yes |
| `replay_throughput` | 2 | `events_per_sec` | 9657651.19 | yes |
| `replay_throughput` | 2 | `ms_per_history` | 1.04 | yes |
| `throughput` | 4 | `workflows_per_sec` | 33.53 | yes |
| `throughput` | 4 | `measured_window_secs` | 71.58 | yes |
| `throughput` | 4 | `completions` | 4800.00 | yes |
| `dispatch_latency` | 4 | `p50_ms` | 45.74 | yes |
| `dispatch_latency` | 4 | `p99_ms` | 100.03 | yes |
| `dispatch_latency` | 4 | `samples` | 4320.00 | yes |
| `dispatch_latency` | 4 | `achieved_starts_per_sec` | 32.06 | yes |
| `signal_roundtrip` | 4 | `p50_ms` | 45.60 | yes |
| `signal_roundtrip` | 4 | `p99_ms` | 80.24 | yes |
| `signal_roundtrip` | 4 | `samples` | 1600.00 | yes |
| `signal_roundtrip` | 4 | `achieved_signals_per_sec` | 31.99 | yes |
| `replay_throughput` | 4 | `events_per_sec` | 9096540.11 | yes |
| `replay_throughput` | 4 | `ms_per_history` | 1.10 | yes |


## Notes

### `throughput` at 1 shard(s)

* per-shard completions: s0=1200
* topology: independent-servers
* closed loop: 128 workflows in flight per shard, 1200 measured completions, warmup population 240
* mean in-flight population: 123.9 per shard against a target of 128
* the headline is the middle-half rate: the ramp-up and the drain-down tails are excluded, so it is a sustained rate rather than an average over a changing queue depth
* wall clock: 60.9s
* sound: every published number on this row rests on a measured sample

### `dispatch_latency` at 1 shard(s)

* per-shard completions: s0=400
* topology: independent-servers
* target pace: 8.0 workflow starts/s
* host-to-database clock offset: +0.033 ms (per shard, median of 7 probes)
* wall clock: 54.4s
* sound: every published number on this row rests on a measured sample

### `signal_roundtrip` at 1 shard(s)

* per-shard completions: s0=400
* topology: independent-servers
* target pace: 8.0 signals/s
* 400 measured signals after a discarded warmup cohort of 80
* wall clock: 68.8s
* sound: every published number on this row rests on a measured sample

### `replay_throughput` at 1 shard(s)

* 10001 events (5000 activities), median of 20 iterations after 5 warmup iterations
* shard-invariant by construction: this row is the run's noise control, not a statement about sharding
* the same history `benches/replay_bench.rs` budgets at 200 ms (issue #135)
* sound: every published number on this row rests on a measured sample

### `throughput` at 2 shard(s)

* per-shard completions: s0=1200 s1=1200
* topology: independent-servers
* closed loop: 128 workflows in flight per shard, 2400 measured completions, warmup population 480
* mean in-flight population: 124.0 per shard against a target of 128
* the headline is the middle-half rate: the ramp-up and the drain-down tails are excluded, so it is a sustained rate rather than an average over a changing queue depth
* wall clock: 80.1s
* sound: every published number on this row rests on a measured sample

### `dispatch_latency` at 2 shard(s)

* per-shard completions: s0=400 s1=400
* topology: independent-servers
* target pace: 16.0 workflow starts/s
* host-to-database clock offset: +0.036, +0.063 ms (per shard, median of 7 probes)
* wall clock: 55.9s
* sound: every published number on this row rests on a measured sample

### `signal_roundtrip` at 2 shard(s)

* per-shard completions: s0=400 s1=400
* topology: independent-servers
* target pace: 16.0 signals/s
* 800 measured signals after a discarded warmup cohort of 160
* wall clock: 71.6s
* sound: every published number on this row rests on a measured sample

### `replay_throughput` at 2 shard(s)

* 10001 events (5000 activities), median of 20 iterations after 5 warmup iterations
* shard-invariant by construction: this row is the run's noise control, not a statement about sharding
* the same history `benches/replay_bench.rs` budgets at 200 ms (issue #135)
* sound: every published number on this row rests on a measured sample

### `throughput` at 4 shard(s)

* per-shard completions: s0=1200 s1=1200 s2=1200 s3=1200
* topology: independent-servers
* closed loop: 128 workflows in flight per shard, 4800 measured completions, warmup population 960
* mean in-flight population: 124.0 per shard against a target of 128
* the headline is the middle-half rate: the ramp-up and the drain-down tails are excluded, so it is a sustained rate rather than an average over a changing queue depth
* wall clock: 166.5s
* sound: every published number on this row rests on a measured sample

### `dispatch_latency` at 4 shard(s)

* per-shard completions: s0=400 s1=400 s2=400 s3=400
* topology: independent-servers
* target pace: 32.0 workflow starts/s
* host-to-database clock offset: -0.001, +0.014, +0.036, +0.073 ms (per shard, median of 7 probes)
* wall clock: 62.8s
* sound: every published number on this row rests on a measured sample

### `signal_roundtrip` at 4 shard(s)

* per-shard completions: s0=400 s1=400 s2=400 s3=400
* topology: independent-servers
* target pace: 32.0 signals/s
* 1600 measured signals after a discarded warmup cohort of 320
* wall clock: 82.4s
* sound: every published number on this row rests on a measured sample

### `replay_throughput` at 4 shard(s)

* 10001 events (5000 activities), median of 20 iterations after 5 warmup iterations
* shard-invariant by construction: this row is the run's noise control, not a statement about sharding
* the same history `benches/replay_bench.rs` budgets at 200 ms (issue #135)
* sound: every published number on this row rests on a measured sample


Every scenario reported sound.

