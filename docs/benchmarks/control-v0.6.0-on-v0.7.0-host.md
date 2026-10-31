# Same-box control: the 0.6.0 harness on the 0.7.0 host

The 0.6.0 end-to-end suite (`b407659`, the commit that published
[`results-v0.6.0.md`](results-v0.6.0.md)) ran on the host and clusters of
[`results-v0.7.0.md`](results-v0.7.0.md). It separates the host from the
release. It is a control, not a published baseline: one sweep, post hoc, not
pre-registered.

Measured 2026-10-08, 17:23 to 17:40 UTC, right after the 0.7.0 sweep, with the
same four native clusters and nothing else running. Replay spread 3.1%.

Verbatim output of the run, rendered by the 0.6.0 harness.

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
| `throughput` | 1 | `workflows_per_sec` | 17.91 | yes |
| `throughput` | 1 | `measured_window_secs` | 33.49 | yes |
| `throughput` | 1 | `completions` | 1200.00 | yes |
| `dispatch_latency` | 1 | `p50_ms` | 45.31 | yes |
| `dispatch_latency` | 1 | `p99_ms` | 78.44 | yes |
| `dispatch_latency` | 1 | `samples` | 1080.00 | yes |
| `dispatch_latency` | 1 | `achieved_starts_per_sec` | 8.00 | yes |
| `signal_roundtrip` | 1 | `p50_ms` | 45.01 | yes |
| `signal_roundtrip` | 1 | `p99_ms` | 73.84 | yes |
| `signal_roundtrip` | 1 | `samples` | 400.00 | yes |
| `signal_roundtrip` | 1 | `achieved_signals_per_sec` | 8.00 | yes |
| `replay_throughput` | 1 | `events_per_sec` | 8919819.73 | yes |
| `replay_throughput` | 1 | `ms_per_history` | 1.12 | yes |
| `throughput` | 2 | `workflows_per_sec` | 21.23 | yes |
| `throughput` | 2 | `measured_window_secs` | 56.54 | yes |
| `throughput` | 2 | `completions` | 2400.00 | yes |
| `dispatch_latency` | 2 | `p50_ms` | 42.86 | yes |
| `dispatch_latency` | 2 | `p99_ms` | 98.57 | yes |
| `dispatch_latency` | 2 | `samples` | 2160.00 | yes |
| `dispatch_latency` | 2 | `achieved_starts_per_sec` | 16.01 | yes |
| `signal_roundtrip` | 2 | `p50_ms` | 48.35 | yes |
| `signal_roundtrip` | 2 | `p99_ms` | 83.94 | yes |
| `signal_roundtrip` | 2 | `samples` | 800.00 | yes |
| `signal_roundtrip` | 2 | `achieved_signals_per_sec` | 16.00 | yes |
| `replay_throughput` | 2 | `events_per_sec` | 8891010.66 | yes |
| `replay_throughput` | 2 | `ms_per_history` | 1.12 | yes |
| `throughput` | 4 | `workflows_per_sec` | 19.38 | yes |
| `throughput` | 4 | `measured_window_secs` | 123.83 | yes |
| `throughput` | 4 | `completions` | 4800.00 | yes |
| `dispatch_latency` | 4 | `p50_ms` | 4020.33 | yes |
| `dispatch_latency` | 4 | `p99_ms` | 6235.49 | yes |
| `dispatch_latency` | 4 | `samples` | 4320.00 | yes |
| `dispatch_latency` | 4 | `achieved_starts_per_sec` | 32.01 | yes |
| `signal_roundtrip` | 4 | `p50_ms` | 69.66 | yes |
| `signal_roundtrip` | 4 | `p99_ms` | 119.51 | yes |
| `signal_roundtrip` | 4 | `samples` | 1600.00 | yes |
| `signal_roundtrip` | 4 | `achieved_signals_per_sec` | 31.99 | yes |
| `replay_throughput` | 4 | `events_per_sec` | 9175456.64 | yes |
| `replay_throughput` | 4 | `ms_per_history` | 1.09 | yes |


## Notes

### `throughput` at 1 shard(s)

* per-shard completions: s0=1200
* topology: independent-servers
* closed loop: 32 workflows in flight per shard, 1200 measured completions, warmup population 240
* mean in-flight population per shard against a target of 32: 31.5
* the headline is the middle-half rate: the ramp-up and the drain-down tails are excluded, so it is a sustained rate rather than an average over a changing queue depth
* wall clock: 80.4s
* sound: every published number on this row rests on a measured sample

### `dispatch_latency` at 1 shard(s)

* per-shard completions: s0=400
* topology: independent-servers
* target pace: 8.0 workflow starts/s
* host-to-database clock offset before the window: +0.052 ms (per shard, median of 7 probes)
* host-to-database clock offset after the window: +0.091 ms
* 0 re-dispatched task row(s); 0 dispatch(es) recorded no task id
* wall clock: 55.8s
* sound: every published number on this row rests on a measured sample

### `signal_roundtrip` at 1 shard(s)

* per-shard completions: s0=400
* topology: independent-servers
* target pace: 8.0 signals/s per shard, one paced sender per shard running concurrently; achieved 8.0/s
* 400 measured signals after a discarded warmup cohort of 80
* wall clock: 69.1s
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
* wall clock: 138.6s
* sound: every published number on this row rests on a measured sample

### `dispatch_latency` at 2 shard(s)

* per-shard completions: s0=400 s1=400
* topology: independent-servers
* target pace: 16.0 workflow starts/s
* host-to-database clock offset before the window: +0.072, +0.016 ms (per shard, median of 7 probes)
* host-to-database clock offset after the window: +0.046, +0.036 ms
* 0 re-dispatched task row(s); 0 dispatch(es) recorded no task id
* wall clock: 60.6s
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
* wall clock: 295.1s
* sound: every published number on this row rests on a measured sample

### `dispatch_latency` at 4 shard(s)

* per-shard completions: s0=400 s1=400 s2=400 s3=400
* topology: independent-servers
* target pace: 32.0 workflow starts/s
* host-to-database clock offset before the window: +0.031, +0.082, +0.067, +0.062 ms (per shard, median of 7 probes)
* host-to-database clock offset after the window: +0.027, +0.265, +0.033, +0.042 ms
* 0 re-dispatched task row(s); 0 dispatch(es) recorded no task id
* wall clock: 100.7s
* sound: every published number on this row rests on a measured sample

### `signal_roundtrip` at 4 shard(s)

* per-shard completions: s0=400 s1=400 s2=400 s3=400
* topology: independent-servers
* target pace: 8.0 signals/s per shard, one paced sender per shard running concurrently; achieved 8.0, 8.0, 8.0, 8.0/s
* 1600 measured signals after a discarded warmup cohort of 320
* wall clock: 91.2s
* sound: every published number on this row rests on a measured sample

### `replay_throughput` at 4 shard(s)

* 10001 events replayed (5000 activities), median of 20 iterations after 5 warmup iterations
* shard-invariant by construction: this row is the run's noise control, not a statement about sharding
* the same history `benches/replay_bench.rs` budgets at 200 ms (issue #135)
* sound: every published number on this row rests on a measured sample


## Noise control

The replay scenario is in-memory and shard-invariant, so its spread across the sweep bounds how much the reference box's own load moved while the other cells were measured.

* replay spread across the sweep: **3.1%**
* within the 10% bar: the box stayed quiet enough for the other cells to be comparable with each other.


Every scenario reported sound.

