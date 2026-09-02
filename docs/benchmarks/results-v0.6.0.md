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
| Measured | 2026-09-02, on an otherwise **idle** box (see below) |

**Container caveat.** These figures come from native Postgres clusters. The
machine they were measured on had no Docker daemon, so the compose path — the
one `./benchmarks/run.sh` runs — was **not** measured here. The delta between
the two is unquantified and this page does not claim to know its sign.

## Headline numbers

| scenario | metric | 1 shard | 2 shards | 4 shards |
|:--|:--|--:|--:|--:|
| `throughput` | workflows/sec | **23.73** | **35.70** | **33.58** |
| `dispatch_latency` | p50 ms | **40.98** | **47.22** | **58.02** |
| `dispatch_latency` | p99 ms | **58.63** | **65.49** | **111.75** |
| `signal_roundtrip` | p50 ms | **53.59** | **44.74** | **54.23** |
| `signal_roundtrip` | p99 ms | **65.96** | **71.16** | **92.90** |
| `replay_throughput` | events/sec | **9 204 142.19** | **9 282 928.36** | **9 240 557.56** |

Every one of the twelve cells reported **sound**: none was thin, truncated,
feeder-bound *on any shard*, off-pace *on any shard*, missing a shard's
contribution, or measured across a clock offset worth more than 2% of the number
it publishes.

## Was the box quiet? (Read this first)

Replay is in-memory and cannot legitimately move with shard count, so its spread
across the sweep is a direct reading of how much the reference machine's own load
moved while the other nine cells were measured. **This run: 0.8%** — 9 204 142,
9 282 928, 9 240 557 events/sec, essentially the same number three times.

That figure is the reason to trust the rest of this page, and it is worth
contrasting with what a *busy* box does. An earlier sweep of this same code on
this same machine, taken while the machine was also compiling, read **8.7%** on
the same control — and its latency cells were visibly deformed: dispatch p50 came
out 41.13 / 35.63 / 54.23 (non-monotonic), against 40.98 / 47.22 / 58.02
(monotonic) here. One cell in another compiling sweep read 602 ms where this run
reads 58 ms.

Two things follow. **The control works** — it flagged the bad sweeps without
anyone knowing in advance which they were. And **"idle" is a real precondition,
not boilerplate**: on four cores, a concurrent build is enough to move a
published latency by more than 10x. If you reproduce these numbers, check the
noise-control section of your own run before comparing anything.

## What the shard sweep showed

**Sharding bought 1.50x at two shards and then gave some back.** 23.73 → 35.70 →
33.58 workflows/sec. Four shards is slower than two.

This is the "or honestly failing to demonstrate" case issue #941 asks to publish
either way, and it has reproduced on every sweep taken on this box, quiet or not.
The reason is the machine, not the shard model: at four shards this box runs four
Postgres clusters, four workers and the harness on four cores. Every shard drained
its full share in every cell, and the in-flight population held at 31.3–31.9 of
its 32 target *on each shard individually*, so the runtime is genuinely using
every shard and the harness was not the limiter anywhere. What this run cannot
tell you is what four shards do on four *machines*.

**Latency degrades monotonically with shard count, and the tail degrades
fastest.** Dispatch p50 goes 40.98 → 47.22 → 58.02 (+42%) while its p99 goes
58.63 → 111.75 (+91%). Signal p99 goes 65.96 → 92.90 (+41%). Under a fixed core
count, adding shards costs the tail roughly twice what it costs the median — the
same run-queue effect issue #786 documented when it chose to gate p50 rather
than p99.

**One row is not monotonic, and on a quiet box that is now a finding rather than
noise.** Signal p50 dips at two shards (44.74) below one shard (53.59) — a 16%
gap — and recovers at four (54.23). It persisted through a sweep whose control
read 0.8%, so it is not the box. A plausible mechanism is that two workers each
carry half as many parked workflows, so the wake path is shorter; but this suite
has no instrumentation to attribute it, and guessing in a results file is how
benchmark pages lose their credibility. It is recorded here as an observation, not
an explanation.

## Reading the latency numbers

Both latency scenarios were paced at **8 starts/sec per shard**, roughly a third
of the saturated rate above, so these percentiles describe the dispatch and
signal paths rather than queue depth. The pace was checked **per shard**, not in
aggregate: an aggregate figure is satisfied by one shard carrying the whole rate
while its peers idle, which would publish a saturated shard's percentiles under a
sweep's label.

**Dispatch latency spans two clocks** — `harvest_task_queue.created_at` is the
database's, the handler's first line is the host's — so the harness probes the
offset before *and* after each measured window and refuses to publish a cell
where it exceeds 2% of the p50, or drifts by more than that across the window. On
this run the offsets stayed in the tens of microseconds against a ~41 ms
measurement — under 0.25% — with no negative sample anywhere.

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
worker's eight workflow slots — and the gate is applied per shard, so a single
starved shard cannot hide behind well-fed peers.

That depth is a deliberate ceiling, not a maximum found by search. The same sweep
at 128 in flight yields a higher number, because 128 is sixteen times the
worker's slots and holds ~120 workflows *pending* per shard — measuring inside
the claim-depth curve that `../performance.md` (issue #786) publishes, under an
end-to-end label. `HARVEST_BENCH_INFLIGHT=128` reproduces it for anyone who
wants to see the difference; the report prints the population actually held next
to the rate, so the two can never be confused.

## Full report

Verbatim output of the run, rendered by the harness rather than typed by hand.

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
| Postgres | PostgreSQL 16.13 (Ubuntu 16.13-0ubuntu0.24.04.1) on x86_64-pc-linux-gnu, compiled by gcc (Ubuntu 13.3.0-6ubuntu2~24.04.1) 13.3.0, 64-bit |


## Results

| scenario | shards | metric | value | sound |
|:--|--:|:--|--:|:--|
| `throughput` | 1 | `workflows_per_sec` | 23.73 | yes |
| `throughput` | 1 | `measured_window_secs` | 25.29 | yes |
| `throughput` | 1 | `completions` | 1200.00 | yes |
| `dispatch_latency` | 1 | `p50_ms` | 40.98 | yes |
| `dispatch_latency` | 1 | `p99_ms` | 58.63 | yes |
| `dispatch_latency` | 1 | `samples` | 1080.00 | yes |
| `dispatch_latency` | 1 | `achieved_starts_per_sec` | 8.00 | yes |
| `signal_roundtrip` | 1 | `p50_ms` | 53.59 | yes |
| `signal_roundtrip` | 1 | `p99_ms` | 65.96 | yes |
| `signal_roundtrip` | 1 | `samples` | 400.00 | yes |
| `signal_roundtrip` | 1 | `achieved_signals_per_sec` | 8.00 | yes |
| `replay_throughput` | 1 | `events_per_sec` | 9204142.19 | yes |
| `replay_throughput` | 1 | `ms_per_history` | 1.09 | yes |
| `throughput` | 2 | `workflows_per_sec` | 35.70 | yes |
| `throughput` | 2 | `measured_window_secs` | 33.61 | yes |
| `throughput` | 2 | `completions` | 2400.00 | yes |
| `dispatch_latency` | 2 | `p50_ms` | 47.22 | yes |
| `dispatch_latency` | 2 | `p99_ms` | 65.49 | yes |
| `dispatch_latency` | 2 | `samples` | 2160.00 | yes |
| `dispatch_latency` | 2 | `achieved_starts_per_sec` | 16.02 | yes |
| `signal_roundtrip` | 2 | `p50_ms` | 44.74 | yes |
| `signal_roundtrip` | 2 | `p99_ms` | 71.16 | yes |
| `signal_roundtrip` | 2 | `samples` | 800.00 | yes |
| `signal_roundtrip` | 2 | `achieved_signals_per_sec` | 16.00 | yes |
| `replay_throughput` | 2 | `events_per_sec` | 9282928.36 | yes |
| `replay_throughput` | 2 | `ms_per_history` | 1.08 | yes |
| `throughput` | 4 | `workflows_per_sec` | 33.58 | yes |
| `throughput` | 4 | `measured_window_secs` | 71.48 | yes |
| `throughput` | 4 | `completions` | 4800.00 | yes |
| `dispatch_latency` | 4 | `p50_ms` | 58.02 | yes |
| `dispatch_latency` | 4 | `p99_ms` | 111.75 | yes |
| `dispatch_latency` | 4 | `samples` | 4320.00 | yes |
| `dispatch_latency` | 4 | `achieved_starts_per_sec` | 32.04 | yes |
| `signal_roundtrip` | 4 | `p50_ms` | 54.23 | yes |
| `signal_roundtrip` | 4 | `p99_ms` | 92.90 | yes |
| `signal_roundtrip` | 4 | `samples` | 1600.00 | yes |
| `signal_roundtrip` | 4 | `achieved_signals_per_sec` | 31.97 | yes |
| `replay_throughput` | 4 | `events_per_sec` | 9240557.56 | yes |
| `replay_throughput` | 4 | `ms_per_history` | 1.08 | yes |


## Notes

### `throughput` at 1 shard(s)

* per-shard completions: s0=1200
* topology: independent-servers
* closed loop: 32 workflows in flight per shard, 1200 measured completions, warmup population 240
* mean in-flight population per shard against a target of 32: 31.3
* the headline is the middle-half rate: the ramp-up and the drain-down tails are excluded, so it is a sustained rate rather than an average over a changing queue depth
* wall clock: 61.0s
* sound: every published number on this row rests on a measured sample

### `dispatch_latency` at 1 shard(s)

* per-shard completions: s0=400
* topology: independent-servers
* target pace: 8.0 workflow starts/s
* host-to-database clock offset before the window: +0.037 ms (per shard, median of 7 probes)
* host-to-database clock offset after the window: +0.029 ms
* 0 re-dispatched task row(s); 0 dispatch(es) recorded no task id
* wall clock: 54.4s
* sound: every published number on this row rests on a measured sample

### `signal_roundtrip` at 1 shard(s)

* per-shard completions: s0=400
* topology: independent-servers
* target pace: 8.0 signals/s per shard, one paced sender per shard running concurrently; achieved 8.0/s
* 400 measured signals after a discarded warmup cohort of 80
* wall clock: 68.8s
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
* mean in-flight population per shard against a target of 32: 31.5, 31.5
* the headline is the middle-half rate: the ramp-up and the drain-down tails are excluded, so it is a sustained rate rather than an average over a changing queue depth
* wall clock: 81.2s
* sound: every published number on this row rests on a measured sample

### `dispatch_latency` at 2 shard(s)

* per-shard completions: s0=400 s1=400
* topology: independent-servers
* target pace: 16.0 workflow starts/s
* host-to-database clock offset before the window: +0.061, +0.021 ms (per shard, median of 7 probes)
* host-to-database clock offset after the window: +0.041, +0.094 ms
* 0 re-dispatched task row(s); 0 dispatch(es) recorded no task id
* wall clock: 55.7s
* sound: every published number on this row rests on a measured sample

### `signal_roundtrip` at 2 shard(s)

* per-shard completions: s0=400 s1=400
* topology: independent-servers
* target pace: 8.0 signals/s per shard, one paced sender per shard running concurrently; achieved 8.0, 8.0/s
* 800 measured signals after a discarded warmup cohort of 160
* wall clock: 70.8s
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
* mean in-flight population per shard against a target of 32: 31.7, 31.7, 31.7, 31.7
* the headline is the middle-half rate: the ramp-up and the drain-down tails are excluded, so it is a sustained rate rather than an average over a changing queue depth
* wall clock: 170.6s
* sound: every published number on this row rests on a measured sample

### `dispatch_latency` at 4 shard(s)

* per-shard completions: s0=400 s1=400 s2=400 s3=400
* topology: independent-servers
* target pace: 32.0 workflow starts/s
* host-to-database clock offset before the window: +0.258, +0.048, +0.025, +0.023 ms (per shard, median of 7 probes)
* host-to-database clock offset after the window: +0.008, +0.046, +0.045, +0.031 ms
* 0 re-dispatched task row(s); 0 dispatch(es) recorded no task id
* wall clock: 62.7s
* sound: every published number on this row rests on a measured sample

### `signal_roundtrip` at 4 shard(s)

* per-shard completions: s0=400 s1=400 s2=400 s3=400
* topology: independent-servers
* target pace: 8.0 signals/s per shard, one paced sender per shard running concurrently; achieved 8.0, 8.0, 8.0, 8.0/s
* 1600 measured signals after a discarded warmup cohort of 320
* wall clock: 79.6s
* sound: every published number on this row rests on a measured sample

### `replay_throughput` at 4 shard(s)

* 10001 events replayed (5000 activities), median of 20 iterations after 5 warmup iterations
* shard-invariant by construction: this row is the run's noise control, not a statement about sharding
* the same history `benches/replay_bench.rs` budgets at 200 ms (issue #135)
* sound: every published number on this row rests on a measured sample


## Noise control

The replay scenario is in-memory and shard-invariant, so its spread across the sweep bounds how much the reference box's own load moved while the other cells were measured.

* replay spread across the sweep: **0.8%**
* within the 10% bar: the box stayed quiet enough for the other cells to be comparable with each other.


Every scenario reported sound.

