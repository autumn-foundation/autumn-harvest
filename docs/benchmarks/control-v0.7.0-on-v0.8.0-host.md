# Same-box control: the 0.7.0 harness on the 0.8.0 host

The 0.7.0 end-to-end suite (tag `v0.7.0`, `cb0963e`) ran on the host and
clusters of [`results-v0.8.0.md`](results-v0.8.0.md). It separates the host
from the release. It is a control, not a published baseline: post hoc, not
pre-registered.

Measured 2026-10-29, 16:25 to 16:35 UTC, with the same four native clusters
and nothing else running. This is the third of three runs. The replay spread
read 31.8%, 36.7% and 15.7%, so **no run was within the 10% bar**. The
database cells agree across the three runs to within a few percent. Read the
numbers as a rough control.

Verbatim output of the run, rendered by the 0.7.0 harness.

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
| `throughput` | 1 | `workflows_per_sec` | 33.61 | yes |
| `throughput` | 1 | `measured_window_secs` | 17.85 | yes |
| `throughput` | 1 | `completions` | 1200.00 | yes |
| `dispatch_latency` | 1 | `p50_ms` | 12.60 | yes |
| `dispatch_latency` | 1 | `p99_ms` | 89.42 | yes |
| `dispatch_latency` | 1 | `samples` | 1080.00 | yes |
| `dispatch_latency` | 1 | `achieved_starts_per_sec` | 7.99 | yes |
| `signal_roundtrip` | 1 | `p50_ms` | 43.04 | yes |
| `signal_roundtrip` | 1 | `p99_ms` | 87.57 | yes |
| `signal_roundtrip` | 1 | `samples` | 400.00 | yes |
| `signal_roundtrip` | 1 | `achieved_signals_per_sec` | 8.00 | yes |
| `replay_throughput` | 1 | `events_per_sec` | 10126548.95 | yes |
| `replay_throughput` | 1 | `ms_per_history` | 0.99 | yes |
| `throughput` | 2 | `workflows_per_sec` | 40.12 | yes |
| `throughput` | 2 | `measured_window_secs` | 29.91 | yes |
| `throughput` | 2 | `completions` | 2400.00 | yes |
| `dispatch_latency` | 2 | `p50_ms` | 18.15 | yes |
| `dispatch_latency` | 2 | `p99_ms` | 94.40 | yes |
| `dispatch_latency` | 2 | `samples` | 2160.00 | yes |
| `dispatch_latency` | 2 | `achieved_starts_per_sec` | 16.00 | yes |
| `signal_roundtrip` | 2 | `p50_ms` | 43.78 | yes |
| `signal_roundtrip` | 2 | `p99_ms` | 83.82 | yes |
| `signal_roundtrip` | 2 | `samples` | 800.00 | yes |
| `signal_roundtrip` | 2 | `achieved_signals_per_sec` | 15.98 | yes |
| `replay_throughput` | 2 | `events_per_sec` | 8584637.78 | yes |
| `replay_throughput` | 2 | `ms_per_history` | 1.16 | yes |
| `throughput` | 4 | `workflows_per_sec` | 41.41 | yes |
| `throughput` | 4 | `measured_window_secs` | 57.96 | yes |
| `throughput` | 4 | `completions` | 4800.00 | yes |
| `dispatch_latency` | 4 | `p50_ms` | 34.50 | yes |
| `dispatch_latency` | 4 | `p99_ms` | 121.26 | yes |
| `dispatch_latency` | 4 | `samples` | 4320.00 | yes |
| `dispatch_latency` | 4 | `achieved_starts_per_sec` | 31.92 | yes |
| `signal_roundtrip` | 4 | `p50_ms` | 49.02 | yes |
| `signal_roundtrip` | 4 | `p99_ms` | 90.70 | yes |
| `signal_roundtrip` | 4 | `samples` | 1600.00 | yes |
| `signal_roundtrip` | 4 | `achieved_signals_per_sec` | 31.99 | yes |
| `replay_throughput` | 4 | `events_per_sec` | 10182326.88 | yes |
| `replay_throughput` | 4 | `ms_per_history` | 0.98 | yes |


## Notes

### `throughput` at 1 shard(s)

* per-shard completions: s0=1200
* topology: independent-servers
* closed loop: 32 workflows in flight per shard, 1200 measured completions, warmup population 240
* mean in-flight population per shard against a target of 32: 31.0
* the headline is the middle-half rate: the ramp-up and the drain-down tails are excluded, so it is a sustained rate rather than an average over a changing queue depth
* wall clock: 42.3s
* sound: every published number on this row rests on a measured sample

### `dispatch_latency` at 1 shard(s)

* per-shard completions: s0=400
* topology: independent-servers
* target pace: 8.0 workflow starts/s
* host-to-database clock offset before the window: +0.033 ms (per shard, median of 7 probes)
* host-to-database clock offset after the window: +0.038 ms
* 0 re-dispatched task row(s); 0 dispatch(es) recorded no task id
* wall clock: 53.3s
* sound: every published number on this row rests on a measured sample

### `signal_roundtrip` at 1 shard(s)

* per-shard completions: s0=400
* topology: independent-servers
* target pace: 8.0 signals/s per shard, one paced sender per shard running concurrently; achieved 8.0/s
* 400 measured signals after a discarded warmup cohort of 80
* wall clock: 64.8s
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
* mean in-flight population per shard against a target of 32: 31.3, 31.4
* the headline is the middle-half rate: the ramp-up and the drain-down tails are excluded, so it is a sustained rate rather than an average over a changing queue depth
* wall clock: 73.7s
* sound: every published number on this row rests on a measured sample

### `dispatch_latency` at 2 shard(s)

* per-shard completions: s0=400 s1=400
* topology: independent-servers
* target pace: 16.0 workflow starts/s
* host-to-database clock offset before the window: +0.022, +0.033 ms (per shard, median of 7 probes)
* host-to-database clock offset after the window: +0.022, +0.029 ms
* 0 re-dispatched task row(s); 0 dispatch(es) recorded no task id
* wall clock: 55.9s
* sound: every published number on this row rests on a measured sample

### `signal_roundtrip` at 2 shard(s)

* per-shard completions: s0=400 s1=400
* topology: independent-servers
* target pace: 8.0 signals/s per shard, one paced sender per shard running concurrently; achieved 8.0, 8.0/s
* 800 measured signals after a discarded warmup cohort of 160
* wall clock: 67.5s
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
* mean in-flight population per shard against a target of 32: 31.6, 31.6, 31.6, 31.6
* the headline is the middle-half rate: the ramp-up and the drain-down tails are excluded, so it is a sustained rate rather than an average over a changing queue depth
* wall clock: 143.1s
* sound: every published number on this row rests on a measured sample

### `dispatch_latency` at 4 shard(s)

* per-shard completions: s0=400 s1=400 s2=400 s3=400
* topology: independent-servers
* target pace: 32.0 workflow starts/s
* host-to-database clock offset before the window: +0.098, +0.017, +0.017, +0.037 ms (per shard, median of 7 probes)
* host-to-database clock offset after the window: +0.024, +0.019, +0.022, +0.032 ms
* 0 re-dispatched task row(s); 0 dispatch(es) recorded no task id
* wall clock: 62.1s
* sound: every published number on this row rests on a measured sample

### `signal_roundtrip` at 4 shard(s)

* per-shard completions: s0=400 s1=400 s2=400 s3=400
* topology: independent-servers
* target pace: 8.0 signals/s per shard, one paced sender per shard running concurrently; achieved 8.0, 8.0, 8.0, 8.0/s
* 1600 measured signals after a discarded warmup cohort of 320
* wall clock: 74.5s
* sound: every published number on this row rests on a measured sample

### `replay_throughput` at 4 shard(s)

* 10001 events replayed (5000 activities), median of 20 iterations after 5 warmup iterations
* shard-invariant by construction: this row is the run's noise control, not a statement about sharding
* the same history `benches/replay_bench.rs` budgets at 200 ms (issue #135)
* sound: every published number on this row rests on a measured sample


## Noise control

The replay scenario is in-memory and shard-invariant, so its spread across the sweep bounds how much the reference box's own load moved while the other cells were measured.

* replay spread across the sweep: **15.7%**
* **above the 10% bar**: the box was not quiet during this run. Treat every other number here as carrying at least this much noise, and re-run on an idle machine before publishing.


Every scenario reported sound.

