# Harvest end-to-end benchmark results — v0.7.0

Reference numbers for release **0.7.0**, produced by
`autumn-harvest/benches/e2e_bench.rs` on `9f444b7` (`trunk-dev`). This file is a
snapshot. A later release adds its own file and leaves this one as it is. See
[`../benchmarks.md`](../benchmarks.md) for the method and how to reproduce any
number here.

> **Reference-machine guidance, not an SLO.** Read
> [`../benchmarks.md`](../benchmarks.md) before you design against any number
> on this page.

## Reference environment

| | |
|:--|:--|
| CPU | 4 logical CPUs, Intel Xeon @ 2.80 GHz, x86_64 |
| OS | Linux 6.18 (Ubuntu 24.04 userland) |
| Postgres | 16.15 (Ubuntu 16.15-0ubuntu0.24.04.1), `fsync=off`, `synchronous_commit=off`, `max_connections=200` |
| topology | `independent-servers`: one native Postgres cluster per shard, ports 55432 to 55435, set up like the services in `benchmarks/docker-compose.yml` |
| Profile | `bench` (release) |
| Workers | 1 per shard, 8 concurrent workflow tasks, 16 concurrent activity tasks |
| Poll interval | 25 ms, with LISTEN/NOTIFY wired per shard |
| Pool size | 32 connections per shard |
| Load | closed loop, 32 workflows in flight per shard (4x the workflow slots) |
| Harness | `autumn-harvest/tests/integration/e2e_bench_support.rs` |
| Measured | 2026-10-08, 16:57 to 17:14 UTC, on an otherwise idle box |

**This is not the 0.6.0 box.** The 0.6.0 numbers came from another 4-core
host. A change between the two files can come from the host or from the
release. The [same-box control](#same-box-control-060-on-this-host) below
separates the two.

## Headline numbers

| scenario | metric | 1 shard | 2 shards | 4 shards |
|:--|:--|--:|--:|--:|
| `throughput` | workflows/sec | **15.46** | **18.84** | **19.50** |
| `dispatch_latency` | p50 ms | **16.12** | **26.17** | **69.98** |
| `dispatch_latency` | p99 ms | **90.86** | **107.79** | **183.13** |
| `signal_roundtrip` | p50 ms | **47.53** | **51.10** | **62.27** |
| `signal_roundtrip` | p99 ms | **92.99** | **95.52** | **118.00** |
| `replay_throughput` | events/sec | **9 750 423.86** | **9 063 901.37** | **8 981 387.93** |

All twelve cells report **sound**. In each throughput cell, every shard held
31.5 to 31.8 of its 32 in-flight target. No cell lost its pace, its clock
offset or a shard.

## Was the box quiet? (Read this first)

The replay control spread **7.9%** across the sweep. That is inside the 10%
bar, so the cells compare with each other. It is ten times the 0.6.0 sweep's
0.8%, so treat this sweep as noisier. The 1-shard replay cell reads highest,
and the 2- and 4-shard cells sit within 1% of each other.

Nothing else ran during the sweep: no build, no git command, no agent. Only the
four shard clusters ran. The host is a cloud VM, so a neighbour on the same
hardware is a possible source of the spread. This page does not claim to know
the cause.

## What changed from 0.6.0

### Same-box control: 0.6.0 on this host

The 0.6.0 harness (`b407659`) ran on this host right after the 0.7.0 sweep,
against the same four clusters. Its verbatim report is
[`control-v0.6.0-on-v0.7.0-host.md`](control-v0.6.0-on-v0.7.0-host.md). It is
one post-hoc sweep, with a replay spread of 3.1%.

| scenario | metric | shards | 0.6.0, its own host | 0.6.0, this host | 0.7.0, this host |
|:--|:--|--:|--:|--:|--:|
| `throughput` | workflows/sec | 1 | 23.73 | 17.91 | 15.46 |
| `throughput` | workflows/sec | 2 | 35.70 | 21.23 | 18.84 |
| `throughput` | workflows/sec | 4 | 33.58 | 19.38 | 19.50 |
| `dispatch_latency` | p50 ms | 1 | 40.98 | 45.31 | 16.12 |
| `dispatch_latency` | p50 ms | 2 | 47.22 | 42.86 | 26.17 |
| `dispatch_latency` | p50 ms | 4 | 58.02 | 4 020.33 | 69.98 |
| `dispatch_latency` | p99 ms | 1 | 58.63 | 78.44 | 90.86 |
| `signal_roundtrip` | p50 ms | 1 | 53.59 | 45.01 | 47.53 |
| `signal_roundtrip` | p99 ms | 1 | 65.96 | 73.84 | 92.99 |

What it shows:

* **Most of the throughput drop is the host.** The same 0.6.0 code reads
  17.91 here against 23.73 on its own host, 25% lower.
* **0.7.0 is slower than 0.6.0 at one and two shards on this host.** It is
  14% lower at one shard and 11% lower at two. At four shards the two are
  level. Each figure is one sweep, and the 0.7.0 sweep's noise control read
  7.9%, so the size of the change is uncertain. The direction is the same
  at both shard counts.
* **0.7.0 dispatches faster at low shard counts.** The dispatch p50 is 16.12
  ms against 45.31 at one shard, and 26.17 against 42.86 at two.
* **0.6.0 broke down at four shards on this host.** Its dispatch p50 read
  4,020 ms. The harness marked the cell sound, because the pace and the clock
  held. 0.7.0 reads 69.98 ms in the same cell.
* **At one and two shards, 0.7.0's tails are wider.** Its dispatch p99 and
  signal p99 are higher than 0.6.0's on this host. At four shards 0.6.0
  broke down, so that cell does not compare.

This suite cannot attribute any of these changes. `docs/performance.md` and
the assay ledger are where a cause gets found.

## What the shard sweep showed

**Sharding bought 1.22x at two shards and 1.26x at four.** 15.46 → 18.84 →
19.50 workflows/sec. The four-core box runs every cluster, every worker and
the harness, so the sweep bounds software scale-out on fixed hardware. It
says nothing about four machines.

**Dispatch latency rises steeply with shard count.** The p50 goes 16.12 →
26.17 → 69.98 ms, and the p99 goes 90.86 → 183.13 ms. On four shared cores
the run queue grows with each shard, as on 0.6.0.

**The 1-shard dispatch tail is wide.** Its p99 is 5.6x its p50. On 0.6.0 the
ratio was 1.4x. This suite does not attribute it.

## Full report

Verbatim output of the run, rendered by the harness.

## Environment

| | |
|:--|:--|
| Logical CPUs | 4 |
| OS | linux / x86_64 |
| Profile | `bench` (release) |
| Harness | `autumn-harvest/tests/integration/e2e_bench_support.rs` |
| Workers | 1 per shard, 8 concurrent workflows, 16 concurrent activities |
| Poll interval | 25 ms (LISTEN/NOTIFY wired per shard) |
| Pool size | 32 per shard |
| Postgres | PostgreSQL 16.15 (Ubuntu 16.15-0ubuntu0.24.04.1) on x86_64-pc-linux-gnu, compiled by gcc (Ubuntu 13.3.0-6ubuntu2~24.04.1) 13.3.0, 64-bit |


## Results

| scenario | shards | metric | value | sound |
|:--|--:|:--|--:|:--|
| `throughput` | 1 | `workflows_per_sec` | 15.46 | yes |
| `throughput` | 1 | `measured_window_secs` | 38.82 | yes |
| `throughput` | 1 | `completions` | 1200.00 | yes |
| `dispatch_latency` | 1 | `p50_ms` | 16.12 | yes |
| `dispatch_latency` | 1 | `p99_ms` | 90.86 | yes |
| `dispatch_latency` | 1 | `samples` | 1080.00 | yes |
| `dispatch_latency` | 1 | `achieved_starts_per_sec` | 8.00 | yes |
| `signal_roundtrip` | 1 | `p50_ms` | 47.53 | yes |
| `signal_roundtrip` | 1 | `p99_ms` | 92.99 | yes |
| `signal_roundtrip` | 1 | `samples` | 400.00 | yes |
| `signal_roundtrip` | 1 | `achieved_signals_per_sec` | 7.99 | yes |
| `replay_throughput` | 1 | `events_per_sec` | 9750423.86 | yes |
| `replay_throughput` | 1 | `ms_per_history` | 1.03 | yes |
| `throughput` | 2 | `workflows_per_sec` | 18.84 | yes |
| `throughput` | 2 | `measured_window_secs` | 63.70 | yes |
| `throughput` | 2 | `completions` | 2400.00 | yes |
| `dispatch_latency` | 2 | `p50_ms` | 26.17 | yes |
| `dispatch_latency` | 2 | `p99_ms` | 107.79 | yes |
| `dispatch_latency` | 2 | `samples` | 2160.00 | yes |
| `dispatch_latency` | 2 | `achieved_starts_per_sec` | 16.00 | yes |
| `signal_roundtrip` | 2 | `p50_ms` | 51.10 | yes |
| `signal_roundtrip` | 2 | `p99_ms` | 95.52 | yes |
| `signal_roundtrip` | 2 | `samples` | 800.00 | yes |
| `signal_roundtrip` | 2 | `achieved_signals_per_sec` | 15.99 | yes |
| `replay_throughput` | 2 | `events_per_sec` | 9063901.37 | yes |
| `replay_throughput` | 2 | `ms_per_history` | 1.10 | yes |
| `throughput` | 4 | `workflows_per_sec` | 19.50 | yes |
| `throughput` | 4 | `measured_window_secs` | 123.10 | yes |
| `throughput` | 4 | `completions` | 4800.00 | yes |
| `dispatch_latency` | 4 | `p50_ms` | 69.98 | yes |
| `dispatch_latency` | 4 | `p99_ms` | 183.13 | yes |
| `dispatch_latency` | 4 | `samples` | 4320.00 | yes |
| `dispatch_latency` | 4 | `achieved_starts_per_sec` | 32.01 | yes |
| `signal_roundtrip` | 4 | `p50_ms` | 62.27 | yes |
| `signal_roundtrip` | 4 | `p99_ms` | 118.00 | yes |
| `signal_roundtrip` | 4 | `samples` | 1600.00 | yes |
| `signal_roundtrip` | 4 | `achieved_signals_per_sec` | 31.75 | yes |
| `replay_throughput` | 4 | `events_per_sec` | 8981387.93 | yes |
| `replay_throughput` | 4 | `ms_per_history` | 1.11 | yes |


## Notes

### `throughput` at 1 shard(s)

* per-shard completions: s0=1200
* topology: independent-servers
* closed loop: 32 workflows in flight per shard, 1200 measured completions, warmup population 240
* mean in-flight population per shard against a target of 32: 31.5
* the headline is the middle-half rate: the ramp-up and the drain-down tails are excluded, so it is a sustained rate rather than an average over a changing queue depth
* wall clock: 98.3s
* sound: every published number on this row rests on a measured sample

### `dispatch_latency` at 1 shard(s)

* per-shard completions: s0=400
* topology: independent-servers
* target pace: 8.0 workflow starts/s
* host-to-database clock offset before the window: +0.085 ms (per shard, median of 7 probes)
* host-to-database clock offset after the window: +0.047 ms
* 0 re-dispatched task row(s); 0 dispatch(es) recorded no task id
* wall clock: 56.6s
* sound: every published number on this row rests on a measured sample

### `signal_roundtrip` at 1 shard(s)

* per-shard completions: s0=400
* topology: independent-servers
* target pace: 8.0 signals/s per shard, one paced sender per shard running concurrently; achieved 8.0/s
* 400 measured signals after a discarded warmup cohort of 80
* wall clock: 67.4s
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
* mean in-flight population per shard against a target of 32: 31.7, 31.7
* the headline is the middle-half rate: the ramp-up and the drain-down tails are excluded, so it is a sustained rate rather than an average over a changing queue depth
* wall clock: 158.3s
* sound: every published number on this row rests on a measured sample

### `dispatch_latency` at 2 shard(s)

* per-shard completions: s0=400 s1=400
* topology: independent-servers
* target pace: 16.0 workflow starts/s
* host-to-database clock offset before the window: +0.065, +0.061 ms (per shard, median of 7 probes)
* host-to-database clock offset after the window: +0.037, +0.047 ms
* 0 re-dispatched task row(s); 0 dispatch(es) recorded no task id
* wall clock: 60.3s
* sound: every published number on this row rests on a measured sample

### `signal_roundtrip` at 2 shard(s)

* per-shard completions: s0=400 s1=400
* topology: independent-servers
* target pace: 8.0 signals/s per shard, one paced sender per shard running concurrently; achieved 8.0, 8.0/s
* 800 measured signals after a discarded warmup cohort of 160
* wall clock: 73.1s
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
* mean in-flight population per shard against a target of 32: 31.8, 31.8, 31.8, 31.8
* the headline is the middle-half rate: the ramp-up and the drain-down tails are excluded, so it is a sustained rate rather than an average over a changing queue depth
* wall clock: 294.2s
* sound: every published number on this row rests on a measured sample

### `dispatch_latency` at 4 shard(s)

* per-shard completions: s0=400 s1=400 s2=400 s3=400
* topology: independent-servers
* target pace: 32.0 workflow starts/s
* host-to-database clock offset before the window: +0.151, +0.109, +0.036, +0.152 ms (per shard, median of 7 probes)
* host-to-database clock offset after the window: +0.040, +0.060, +0.103, +0.055 ms
* 0 re-dispatched task row(s); 0 dispatch(es) recorded no task id
* wall clock: 97.3s
* sound: every published number on this row rests on a measured sample

### `signal_roundtrip` at 4 shard(s)

* per-shard completions: s0=400 s1=400 s2=400 s3=400
* topology: independent-servers
* target pace: 8.0 signals/s per shard, one paced sender per shard running concurrently; achieved 7.9, 7.9, 7.9, 7.9/s
* 1600 measured signals after a discarded warmup cohort of 320
* wall clock: 87.3s
* sound: every published number on this row rests on a measured sample

### `replay_throughput` at 4 shard(s)

* 10001 events replayed (5000 activities), median of 20 iterations after 5 warmup iterations
* shard-invariant by construction: this row is the run's noise control, not a statement about sharding
* the same history `benches/replay_bench.rs` budgets at 200 ms (issue #135)
* sound: every published number on this row rests on a measured sample


## Noise control

The replay scenario is in-memory and shard-invariant, so its spread across the sweep bounds how much the reference box's own load moved while the other cells were measured.

* replay spread across the sweep: **7.9%**
* within the 10% bar: the box stayed quiet enough for the other cells to be comparable with each other.


Every scenario reported sound.

