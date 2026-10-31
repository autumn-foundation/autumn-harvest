# Harvest end-to-end benchmark results — v0.8.0

Reference numbers for release **0.8.0**, produced by
`autumn-harvest/benches/e2e_bench.rs` on `33294ed` (the 0.8.0 release branch).
This file is a snapshot. A later release adds its own file and leaves this one
as it is. See [`../benchmarks.md`](../benchmarks.md) for the method and how to
reproduce any number here.

> **Reference-machine guidance, not an SLO.** Read
> [`../benchmarks.md`](../benchmarks.md) before you design against any number
> on this page.

## Reference environment

| | |
|:--|:--|
| CPU | 4 logical CPUs, Intel Xeon @ 2.10 GHz, x86_64 |
| OS | Linux 6.18 (Ubuntu 24.04 userland) |
| Postgres | 16.15 (Ubuntu 16.15-0ubuntu0.24.04.1), `fsync=off`, `synchronous_commit=off`, `max_connections=200` |
| topology | `independent-servers`: one native Postgres cluster per shard, ports 55432 to 55435, set up like the services in `benchmarks/docker-compose.yml` |
| Profile | `bench` (release) |
| Workers | 1 per shard, 8 concurrent workflow tasks, 16 concurrent activity tasks, the default 2 claim loops |
| Poll interval | 25 ms, with LISTEN/NOTIFY wired per shard |
| Pool size | 32 connections per shard |
| Load | closed loop, 32 workflows in flight per shard (4x the workflow slots) |
| Harness | `autumn-harvest/tests/integration/e2e_bench_support.rs` |
| Measured | 2026-10-29, 22:06 to 22:16 UTC, on an otherwise idle box |

**This is not the 0.7.0 box.** The 0.7.0 numbers came from another 4-core
host. A change between the two files can come from the host or from the
release. The [same-box control](#same-box-control-070-on-this-host) below
separates the two.

## Headline numbers

| scenario | metric | 1 shard | 2 shards | 4 shards |
|:--|:--|--:|--:|--:|
| `throughput` | workflows/sec | **52.91** | **48.14** | **50.90** |
| `dispatch_latency` | p50 ms | **6.89** | **10.68** | **25.35** |
| `dispatch_latency` | p99 ms | **12.11** | **22.93** | **49.73** |
| `signal_roundtrip` | p50 ms | **11.36** | **15.55** | **25.69** |
| `signal_roundtrip` | p99 ms | **80.38** | **81.96** | **85.03** |
| `replay_throughput` | events/sec | **9 571 608.09** | **9 591 638.46** | **9 058 597.83** |

All twelve cells report **sound**. In each throughput cell, every shard held
30.3 to 31.5 of its 32 in-flight target. No cell lost its pace, its clock
offset or a shard.

The 1-shard throughput cell rests on an 11.3 s measured window. That is the
middle half of 1,200 completions, so it is short. The 0.7.0 cell had 38.8 s.

## Was the box quiet? (Read this first)

The replay control spread **5.6%** across the sweep. That is inside the 10%
bar, so the cells compare with each other. The 4-shard replay cell reads
lowest, and the 1- and 2-shard cells sit within 1% of each other.

Nothing else ran during the sweep: no build, no git command, no agent. Only the
four shard clusters ran. This host is a noisy cloud VM. Earlier sweeps
exceeded the bar, ran beside a build or measured earlier code. None of them is
published. This page reports the first quiet sweep of the final code.

## What changed from 0.7.0

### Same-box control: 0.7.0 on this host

The 0.7.0 harness (tag `v0.7.0`, `cb0963e`) ran on this host three times,
against the same four clusters, before the 0.8.0 sweep. The third run's
verbatim report is
[`control-v0.7.0-on-v0.8.0-host.md`](control-v0.7.0-on-v0.8.0-host.md).

**No control run was quiet.** The replay spread read 31.8%, 36.7% and 15.7%.
All three are above the 10% bar. The database cells agree across the three
runs: the 1-shard throughput read 33.02, 32.95 and 33.61, and the 1-shard
dispatch p50 read 14.02, 12.99 and 12.60 ms. Treat the control as a rough
reading, not a baseline. The table uses the third run.

| scenario | metric | shards | 0.7.0, its own host | 0.7.0, this host | 0.8.0, this host |
|:--|:--|--:|--:|--:|--:|
| `throughput` | workflows/sec | 1 | 15.46 | 33.61 | 52.91 |
| `throughput` | workflows/sec | 2 | 18.84 | 40.12 | 48.14 |
| `throughput` | workflows/sec | 4 | 19.50 | 41.41 | 50.90 |
| `dispatch_latency` | p50 ms | 1 | 16.12 | 12.60 | 6.89 |
| `dispatch_latency` | p50 ms | 2 | 26.17 | 18.15 | 10.68 |
| `dispatch_latency` | p50 ms | 4 | 69.98 | 34.50 | 25.35 |
| `dispatch_latency` | p99 ms | 1 | 90.86 | 89.42 | 12.11 |
| `signal_roundtrip` | p50 ms | 1 | 47.53 | 43.04 | 11.36 |
| `signal_roundtrip` | p99 ms | 1 | 92.99 | 87.57 | 80.38 |

What it shows:

* **This host is faster than the 0.7.0 host.** The same 0.7.0 code reads
  33.61 workflows/sec here against 15.46 on its own host, despite a lower
  clock. Do not compare this page's numbers with the 0.7.0 page directly.
* **0.8.0 completes more workflows than 0.7.0 on this host.** It is 57%
  higher at one shard, 20% at two and 23% at four.
* **0.8.0 dispatches faster.** The 1-shard dispatch p50 is 6.89 ms against
  12.60, and the p99 is 12.11 ms against 89.42.
* **The signal p50 fell, the signal p99 did not.** The 1-shard p50 is 11.36
  ms against 43.04. The p99 is 80 to 85 ms at every shard count, close to
  0.7.0.

### Where the changes come from

Three changes to the worker's claim loop meet in this release. The
measurements behind each one are in
[`../performance.md`](../performance.md).

1. **Two claim loops by default (issue #2106).** A worker keeps about two
   claims in flight instead of one. On a deep backlog this raised throughput
   21% to 35%.
2. **A finished workflow task wakes the leader (issue #2418).** The first cut
   of two loops raised the 1-shard dispatch p50 from about 13 ms to 66 ms.
   Both loops drained the workflow backlog, then the leader slept the 50 to
   75 ms NOTIFY settle while the activities committed. The fix wakes the
   leader with no settle when a local workflow task ends.
3. **A NOTIFY polls before it settles (issue #2422).** After the #2418 fix,
   a signal to an idle worker waited the whole settle. The 1-shard signal p50
   read 68 ms. Now the worker polls first and settles only after an empty
   poll.

This suite cannot attribute each cell to one of the three. The signal p99
is not explained by this page either. A signal that arrives during a settle
still waits for it, which fits an 80 ms tail, but that reading is post hoc.

## What the shard sweep showed

**Sharding bought nothing on this box.** 52.91 → 48.14 → 50.90
workflows/sec. The four-core box runs every cluster, every worker and the
harness. With two claim loops, one shard already keeps the cores busy. The
sweep bounds software scale-out on fixed hardware. It says nothing about four
machines.

**Dispatch latency still rises with shard count.** The p50 goes 6.89 →
10.68 → 25.35 ms, and the p99 goes 12.11 → 22.93 → 49.73 ms. The run queue
grows with each shard, as on 0.7.0.

**The 1-shard dispatch tail is narrow.** Its p99 is 1.8x its p50. On 0.7.0
the ratio was 5.6x.

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
| `throughput` | 1 | `workflows_per_sec` | 52.91 | yes |
| `throughput` | 1 | `measured_window_secs` | 11.34 | yes |
| `throughput` | 1 | `completions` | 1200.00 | yes |
| `dispatch_latency` | 1 | `p50_ms` | 6.89 | yes |
| `dispatch_latency` | 1 | `p99_ms` | 12.11 | yes |
| `dispatch_latency` | 1 | `samples` | 1080.00 | yes |
| `dispatch_latency` | 1 | `achieved_starts_per_sec` | 8.00 | yes |
| `signal_roundtrip` | 1 | `p50_ms` | 11.36 | yes |
| `signal_roundtrip` | 1 | `p99_ms` | 80.38 | yes |
| `signal_roundtrip` | 1 | `samples` | 400.00 | yes |
| `signal_roundtrip` | 1 | `achieved_signals_per_sec` | 8.00 | yes |
| `replay_throughput` | 1 | `events_per_sec` | 9571608.09 | yes |
| `replay_throughput` | 1 | `ms_per_history` | 1.04 | yes |
| `throughput` | 2 | `workflows_per_sec` | 48.14 | yes |
| `throughput` | 2 | `measured_window_secs` | 24.93 | yes |
| `throughput` | 2 | `completions` | 2400.00 | yes |
| `dispatch_latency` | 2 | `p50_ms` | 10.68 | yes |
| `dispatch_latency` | 2 | `p99_ms` | 22.93 | yes |
| `dispatch_latency` | 2 | `samples` | 2160.00 | yes |
| `dispatch_latency` | 2 | `achieved_starts_per_sec` | 16.02 | yes |
| `signal_roundtrip` | 2 | `p50_ms` | 15.55 | yes |
| `signal_roundtrip` | 2 | `p99_ms` | 81.96 | yes |
| `signal_roundtrip` | 2 | `samples` | 800.00 | yes |
| `signal_roundtrip` | 2 | `achieved_signals_per_sec` | 15.98 | yes |
| `replay_throughput` | 2 | `events_per_sec` | 9591638.46 | yes |
| `replay_throughput` | 2 | `ms_per_history` | 1.04 | yes |
| `throughput` | 4 | `workflows_per_sec` | 50.90 | yes |
| `throughput` | 4 | `measured_window_secs` | 47.15 | yes |
| `throughput` | 4 | `completions` | 4800.00 | yes |
| `dispatch_latency` | 4 | `p50_ms` | 25.35 | yes |
| `dispatch_latency` | 4 | `p99_ms` | 49.73 | yes |
| `dispatch_latency` | 4 | `samples` | 4320.00 | yes |
| `dispatch_latency` | 4 | `achieved_starts_per_sec` | 32.04 | yes |
| `signal_roundtrip` | 4 | `p50_ms` | 25.69 | yes |
| `signal_roundtrip` | 4 | `p99_ms` | 85.03 | yes |
| `signal_roundtrip` | 4 | `samples` | 1600.00 | yes |
| `signal_roundtrip` | 4 | `achieved_signals_per_sec` | 32.00 | yes |
| `replay_throughput` | 4 | `events_per_sec` | 9058597.83 | yes |
| `replay_throughput` | 4 | `ms_per_history` | 1.10 | yes |


## Notes

### `throughput` at 1 shard(s)

* per-shard completions: s0=1200
* topology: independent-servers
* resident decisions (issue #2007, measured loop): 3600 hits of 4800, hit rate 75.0%; replays: cold 1200
* decisions (issue #2252, measured loop): 4800 for 1200 runs, 0 extra, 0 extra cold; continuation hit rate 100.0%
* owner mode: off
* closed loop: 32 workflows in flight per shard, 1200 measured completions, warmup population 240
* mean in-flight population per shard against a target of 32: 30.3
* the headline is the middle-half rate: the ramp-up and the drain-down tails are excluded, so it is a sustained rate rather than an average over a changing queue depth
* wall clock: 28.3s
* sound: every published number on this row rests on a measured sample

### `dispatch_latency` at 1 shard(s)

* per-shard completions: s0=400
* topology: independent-servers
* target pace: 8.0 workflow starts/s
* host-to-database clock offset before the window: +0.024 ms (per shard, median of 7 probes)
* host-to-database clock offset after the window: +0.018 ms
* 0 re-dispatched task row(s); 0 dispatch(es) recorded no task id
* wall clock: 52.6s
* sound: every published number on this row rests on a measured sample

### `signal_roundtrip` at 1 shard(s)

* per-shard completions: s0=400
* topology: independent-servers
* target pace: 8.0 signals/s per shard, one paced sender per shard running concurrently; achieved 8.0/s
* 400 measured signals after a discarded warmup cohort of 80
* wall clock: 64.6s
* sound: every published number on this row rests on a measured sample

### `replay_throughput` at 1 shard(s)

* 10001 events replayed (5000 activities), median of 20 iterations after 5 warmup iterations
* shard-invariant by construction: this row is the run's noise control, not a statement about sharding
* the same history `benches/replay_bench.rs` budgets at 200 ms (issue #135)
* sound: every published number on this row rests on a measured sample

### `throughput` at 2 shard(s)

* per-shard completions: s0=1200 s1=1200
* topology: independent-servers
* resident decisions (issue #2007, measured loop): 7200 hits of 9600, hit rate 75.0%; replays: cold 2400
* decisions (issue #2252, measured loop): 9600 for 2400 runs, 0 extra, 0 extra cold; continuation hit rate 100.0%
* owner mode: off
* closed loop: 32 workflows in flight per shard, 2400 measured completions, warmup population 480
* mean in-flight population per shard against a target of 32: 31.2, 31.1
* the headline is the middle-half rate: the ramp-up and the drain-down tails are excluded, so it is a sustained rate rather than an average over a changing queue depth
* wall clock: 60.9s
* sound: every published number on this row rests on a measured sample

### `dispatch_latency` at 2 shard(s)

* per-shard completions: s0=400 s1=400
* topology: independent-servers
* target pace: 16.0 workflow starts/s
* host-to-database clock offset before the window: +0.016, +0.033 ms (per shard, median of 7 probes)
* host-to-database clock offset after the window: +0.027, +0.022 ms
* 0 re-dispatched task row(s); 0 dispatch(es) recorded no task id
* wall clock: 54.8s
* sound: every published number on this row rests on a measured sample

### `signal_roundtrip` at 2 shard(s)

* per-shard completions: s0=400 s1=400
* topology: independent-servers
* target pace: 8.0 signals/s per shard, one paced sender per shard running concurrently; achieved 8.0, 8.0/s
* 800 measured signals after a discarded warmup cohort of 160
* wall clock: 67.1s
* sound: every published number on this row rests on a measured sample

### `replay_throughput` at 2 shard(s)

* 10001 events replayed (5000 activities), median of 20 iterations after 5 warmup iterations
* shard-invariant by construction: this row is the run's noise control, not a statement about sharding
* the same history `benches/replay_bench.rs` budgets at 200 ms (issue #135)
* sound: every published number on this row rests on a measured sample

### `throughput` at 4 shard(s)

* per-shard completions: s0=1200 s1=1200 s2=1200 s3=1200
* topology: independent-servers
* resident decisions (issue #2007, measured loop): 14400 hits of 19200, hit rate 75.0%; replays: cold 4800
* decisions (issue #2252, measured loop): 19200 for 4800 runs, 0 extra, 0 extra cold; continuation hit rate 100.0%
* owner mode: off
* closed loop: 32 workflows in flight per shard, 4800 measured completions, warmup population 960
* mean in-flight population per shard against a target of 32: 31.5, 31.5, 31.5, 31.5
* the headline is the middle-half rate: the ramp-up and the drain-down tails are excluded, so it is a sustained rate rather than an average over a changing queue depth
* wall clock: 115.5s
* sound: every published number on this row rests on a measured sample

### `dispatch_latency` at 4 shard(s)

* per-shard completions: s0=400 s1=400 s2=400 s3=400
* topology: independent-servers
* target pace: 32.0 workflow starts/s
* host-to-database clock offset before the window: +0.010, +0.028, +0.012, +0.018 ms (per shard, median of 7 probes)
* host-to-database clock offset after the window: +0.026, +0.018, +0.035, +0.034 ms
* 0 re-dispatched task row(s); 0 dispatch(es) recorded no task id
* wall clock: 59.3s
* sound: every published number on this row rests on a measured sample

### `signal_roundtrip` at 4 shard(s)

* per-shard completions: s0=400 s1=400 s2=400 s3=400
* topology: independent-servers
* target pace: 8.0 signals/s per shard, one paced sender per shard running concurrently; achieved 8.0, 8.0, 8.0, 8.0/s
* 1600 measured signals after a discarded warmup cohort of 320
* wall clock: 73.9s
* sound: every published number on this row rests on a measured sample

### `replay_throughput` at 4 shard(s)

* 10001 events replayed (5000 activities), median of 20 iterations after 5 warmup iterations
* shard-invariant by construction: this row is the run's noise control, not a statement about sharding
* the same history `benches/replay_bench.rs` budgets at 200 ms (issue #135)
* sound: every published number on this row rests on a measured sample


## Noise control

The replay scenario is in-memory and shard-invariant, so its spread across the sweep bounds how much the reference box's own load moved while the other cells were measured.

* replay spread across the sweep: **5.6%**
* within the 10% bar: the box stayed quiet enough for the other cells to be comparable with each other.


Every scenario reported sound.

