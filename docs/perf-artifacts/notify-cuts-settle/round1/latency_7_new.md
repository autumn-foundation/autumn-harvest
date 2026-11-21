# Harvest end-to-end benchmark run (issue #941)

## Environment

| | |
|:--|:--|
| Logical CPUs | 4 |
| OS | linux / x86_64 |
| Profile | `bench` (release) |
| Harness | `autumn-harvest/tests/integration/e2e_bench_support.rs` |
| Workers | 1 per shard in the matrix cells, 8 concurrent workflows, 16 concurrent activities |
| Poll interval | 25 ms (LISTEN/NOTIFY wired per shard) |
| Pool size | 32 per shard, and per worker process in a capacity cell |
| Postgres | PostgreSQL 16.15 (Ubuntu 16.15-0ubuntu0.24.04.1) on x86_64-pc-linux-gnu, compiled by gcc (Ubuntu 13.3.0-6ubuntu2~24.04.1) 13.3.0, 64-bit |


## Results

| scenario | shards | metric | value | sound |
|:--|--:|:--|--:|:--|
| `dispatch_latency` | 1 | `p50_ms` | 4.93 | yes |
| `dispatch_latency` | 1 | `p99_ms` | 9.04 | yes |
| `dispatch_latency` | 1 | `samples` | 1080.00 | yes |
| `dispatch_latency` | 1 | `achieved_starts_per_sec` | 8.00 | yes |
| `signal_roundtrip` | 1 | `p50_ms` | 6.76 | yes |
| `signal_roundtrip` | 1 | `p99_ms` | 22.75 | yes |
| `signal_roundtrip` | 1 | `samples` | 400.00 | yes |
| `signal_roundtrip` | 1 | `achieved_signals_per_sec` | 8.00 | yes |


## Notes

### `dispatch_latency` at 1 shard(s)

* per-shard completions: s0=400
* topology: independent-servers
* host layout: 1 shard(s) on 1 database host(s) (localhost); a database shares the bench host with the workers and the generator
* target pace: 8.0 workflow starts/s
* host-to-database clock offset before the window: +0.036 ms (per shard, median of 7 probes)
* host-to-database clock offset after the window: +0.026 ms
* 0 re-dispatched task row(s); 0 dispatch(es) recorded no task id
* wall clock: 51.9s
* sound: every published number on this row rests on a measured sample

### `signal_roundtrip` at 1 shard(s)

* per-shard completions: s0=400
* topology: independent-servers
* host layout: 1 shard(s) on 1 database host(s) (localhost); a database shares the bench host with the workers and the generator
* target pace: 8.0 signals/s per shard, one paced sender per shard running concurrently; achieved 8.0/s
* 400 measured signals after a discarded warmup cohort of 80
* wall clock: 64.1s
* sound: every published number on this row rests on a measured sample


Every scenario reported sound.

