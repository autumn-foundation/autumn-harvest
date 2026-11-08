# Harvest capacity results — open loop, 1 shard

Reference numbers for the open-loop capacity scenario, produced by
`autumn-harvest/benches/e2e_bench.rs` on `f96f11d` (`trunk-dev` plus this
scenario). This file is a snapshot. A later run adds its own file. See
[`../benchmarks.md`](../benchmarks.md#capacity-under-an-open-loop) for the
method and how to reproduce any number here.

> **Reference-machine guidance, not an SLO.** One shard, one small box, the
> database on the same host as the workers. Read the limits below before you
> compare this number with anything.

## Reference environment

| | |
|:--|:--|
| CPU | 4 logical CPUs, Intel Xeon @ 2.10 GHz, x86_64 |
| OS | Linux 6.18 (Ubuntu 24.04 userland) |
| Postgres | 16.15 (Ubuntu 16.15-0ubuntu0.24.04.1), `fsync=off`, `synchronous_commit=off`, `max_connections=200` |
| host layout | 1 shard on 1 database host (`localhost`). One native Postgres cluster on port 55432, set up like the services in `benchmarks/docker-compose.yml` |
| worker layout | 4 worker processes per shard, all on the bench host, each with 8 workflow slots, 16 activity slots and the default 2 claim loops |
| generator | a 16-connection pool per shard on the bench host, one task per arrival |
| Profile | `bench` (release) |
| Measured | 2026-11-08, 16:12 to 16:18 UTC, on an otherwise idle box |

This is the host class of the 0.8.0 results: the same CPU model, core count,
OS and Postgres build. The database, the four worker processes and the
generator all share its four logical CPUs.

## Headline numbers

| arm | workflow | sustainable rate, 1 shard | p99 at that rate | first failed step |
|:--|:--|--:|--:|--:|
| `capacity_noop` | zero activities | **190.7 workflows/s** | 139.2 ms | 238.4/s |
| `capacity_canonical` | 3 activities | **39.1 workflows/s** | 590.2 ms | 48.8/s |

Both cells report **sound**. Each ladder stopped on an engine failure, not
on the generator or the budget. The p99 runs from admission to completion.

* **`capacity_noop`.** Every step up to 190.7/s held a flat backlog. At
  238.4/s the backlog grew 8.2 runs/s and the median latency grew 33.9 ms/s.
  The generator wait p99 was 9.3 ms at the last sustainable step.
* **`capacity_canonical`.** Every step up to 39.1/s held a flat backlog. At
  48.8/s the backlog grew 3.3 runs/s and the median latency grew 70.8 ms/s.

The ladder steps by 1.25x. So the true limit of each arm lies between its
sustainable rate and its first failed step. The published number is the
lower end of that interval.

## The closed-loop control

The closed-loop `throughput` cell ran on the same commit and box right
after the capacity run, at 16:18 UTC. It read **41.28 workflows/s**, sound,
with 30.6 of its 32 workflows in flight. That is inside the canonical arm's
interval, 39.1 to 48.8. A saturated closed loop runs at the service rate. The
open loop reports the highest offered rate below it at which the backlog and
the latency stay flat.

The closed-loop cell read 52.91 on the 0.8.0 release. This file does not
explain that change. It ran one worker process per shard against four here,
and it measured different code.

## What this number is not

* **Not a scaling result.** One shard only. No multi-shard capacity cell
  here has its own database host or its own worker host.
* **Not a durable figure.** `fsync=off` and `synchronous_commit=off`, as
  for every number in [`../benchmarks.md`](../benchmarks.md).
* **Not a dedicated database.** The database shares four logical CPUs with
  the workers and the generator. A dedicated database host would move the
  number. This file does not say how far.

## Verbatim run output

The capacity run, with the replay cell as a box-speed control:

```text
# Harvest end-to-end benchmark run (issue #941)

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
| Capacity | 4 worker process(es) per shard, 16 generator connections per shard, 20s steps x1.25 |
| Postgres | PostgreSQL 16.15 (Ubuntu 16.15-0ubuntu0.24.04.1) on x86_64-pc-linux-gnu, compiled by gcc (Ubuntu 13.3.0-6ubuntu2~24.04.1) 13.3.0, 64-bit |


## Results

| scenario | shards | metric | value | sound |
|:--|--:|:--|--:|:--|
| `replay_throughput` | 1 | `events_per_sec` | 12194542.02 | yes |
| `replay_throughput` | 1 | `ms_per_history` | 0.82 | yes |
| `capacity_noop` | 1 | `sustainable_per_sec` | 190.70 | yes |
| `capacity_noop` | 1 | `sustainable_per_shard_per_sec` | 190.70 | yes |
| `capacity_noop` | 1 | `p99_ms_at_sustainable` | 139.17 | yes |
| `capacity_noop` | 1 | `steps` | 9.00 | yes |
| `capacity_noop` | 1 | `database_hosts` | 1.00 | yes |
| `capacity_noop` | 1 | `worker_hosts` | 1.00 | yes |
| `capacity_canonical` | 1 | `sustainable_per_sec` | 39.10 | yes |
| `capacity_canonical` | 1 | `sustainable_per_shard_per_sec` | 39.10 | yes |
| `capacity_canonical` | 1 | `p99_ms_at_sustainable` | 590.24 | yes |
| `capacity_canonical` | 1 | `steps` | 5.00 | yes |
| `capacity_canonical` | 1 | `database_hosts` | 1.00 | yes |
| `capacity_canonical` | 1 | `worker_hosts` | 1.00 | yes |


## Notes

### `replay_throughput` at 1 shard(s)

* 10001 events replayed (5000 activities), median of 20 iterations after 5 warmup iterations
* shard-invariant by construction: this row is the run's noise control, not a statement about sharding
* the same history `benches/replay_bench.rs` budgets at 200 ms (issue #135)
* sound: every published number on this row rests on a measured sample

### `capacity_noop` at 1 shard(s)

* topology: independent-servers
* host layout: 1 shard(s) on 1 database host(s) (localhost); a database shares the bench host with the workers and the generator
* worker layout: every worker process runs on the bench host
* open loop: 4 worker process(es) per shard, a 16-connection generator pool per shard, 20s steps from 40.0 starts/s per shard
* warmup (discarded): offered 40.0/s per shard; achieved 40.0; runs 800; backlog slope -0.0 runs/s; median growth 0.1 ms/s; judged-window p99 65.5/69.0/69.1 ms; step p99 68.8 ms; generator wait p99 2.5 ms; sustainable
* step 0: offered 40.0/s per shard; achieved 40.0; runs 800; backlog slope -0.0 runs/s; median growth -0.3 ms/s; judged-window p99 78.1/68.6/68.0 ms; step p99 69.1 ms; generator wait p99 2.6 ms; sustainable
* step 1: offered 50.0/s per shard; achieved 50.0; runs 1000; backlog slope -0.0 runs/s; median growth -0.0 ms/s; judged-window p99 66.9/62.8/62.0 ms; step p99 62.4 ms; generator wait p99 2.4 ms; sustainable
* step 2: offered 62.5/s per shard; achieved 62.5; runs 1250; backlog slope -0.0 runs/s; median growth 0.0 ms/s; judged-window p99 41.8/59.4/56.5 ms; step p99 55.0 ms; generator wait p99 2.5 ms; sustainable
* step 3: offered 78.1/s per shard; achieved 78.1; runs 1562; backlog slope 0.0 runs/s; median growth 0.1 ms/s; judged-window p99 42.3/28.9/44.9 ms; step p99 42.3 ms; generator wait p99 2.7 ms; sustainable
* step 4: offered 97.7/s per shard; achieved 97.7; runs 1954; backlog slope 0.0 runs/s; median growth -0.0 ms/s; judged-window p99 29.4/42.2/30.0 ms; step p99 34.0 ms; generator wait p99 3.2 ms; sustainable
* step 5: offered 122.1/s per shard; achieved 122.1; runs 2442; backlog slope 0.0 runs/s; median growth 0.3 ms/s; judged-window p99 35.4/37.3/46.5 ms; step p99 48.2 ms; generator wait p99 3.4 ms; sustainable
* step 6: offered 152.6/s per shard; achieved 152.5; runs 3052; backlog slope -0.1 runs/s; median growth -0.6 ms/s; judged-window p99 81.1/70.9/68.5 ms; step p99 247.4 ms; generator wait p99 4.8 ms; sustainable
* step 7: offered 190.7/s per shard; achieved 190.5; runs 3814; backlog slope -0.1 runs/s; median growth 0.1 ms/s; judged-window p99 158.6/121.1/141.2 ms; step p99 139.2 ms; generator wait p99 9.3 ms; sustainable
* step 8: offered 238.4/s per shard; achieved 238.0; runs 4768; backlog slope 8.2 runs/s; median growth 33.9 ms/s; judged-window p99 506.5/767.5/835.1 ms; step p99 821.9 ms; generator wait p99 85.1 ms; **not sustainable**: the median latency grew 33.9 ms/s, over the 5 ms/s limit
* ladder: 9 step(s), stopped EngineBound; sustainable 190.7 starts/s per shard
* engine latency runs from admission to `completed_at`, both on the bench host clock; the generator wait, from schedule to admission, is judged separately
* wall clock: 206.6s
* sound: every published number on this row rests on a measured sample

### `capacity_canonical` at 1 shard(s)

* topology: independent-servers
* host layout: 1 shard(s) on 1 database host(s) (localhost); a database shares the bench host with the workers and the generator
* worker layout: every worker process runs on the bench host
* open loop: 4 worker process(es) per shard, a 16-connection generator pool per shard, 20s steps from 20.0 starts/s per shard
* warmup (discarded): offered 20.0/s per shard; achieved 20.0; runs 400; backlog slope 0.0 runs/s; median growth 1.8 ms/s; judged-window p99 361.1/339.4/390.3 ms; step p99 361.1 ms; generator wait p99 3.2 ms; sustainable
* step 0: offered 20.0/s per shard; achieved 20.0; runs 400; backlog slope -0.0 runs/s; median growth -1.2 ms/s; judged-window p99 343.2/329.9/347.0 ms; step p99 354.6 ms; generator wait p99 3.7 ms; sustainable
* step 1: offered 25.0/s per shard; achieved 25.0; runs 500; backlog slope 0.1 runs/s; median growth 3.2 ms/s; judged-window p99 289.1/352.1/356.3 ms; step p99 353.5 ms; generator wait p99 3.9 ms; sustainable
* step 2: offered 31.3/s per shard; achieved 31.3; runs 626; backlog slope 0.1 runs/s; median growth 3.6 ms/s; judged-window p99 410.0/396.8/541.4 ms; step p99 483.0 ms; generator wait p99 6.0 ms; sustainable
* step 3: offered 39.1/s per shard; achieved 39.1; runs 782; backlog slope -0.2 runs/s; median growth -4.0 ms/s; judged-window p99 601.5/571.5/611.9 ms; step p99 590.2 ms; generator wait p99 9.4 ms; sustainable
* step 4: offered 48.8/s per shard; achieved 48.8; runs 976; backlog slope 3.3 runs/s; median growth 70.8 ms/s; judged-window p99 1258.6/1631.3/1782.1 ms; step p99 1755.4 ms; generator wait p99 11.5 ms; **not sustainable**: the backlog grew 3.31 runs/s, over the 2.44 runs/s limit; the median latency grew 70.8 ms/s, over the 5 ms/s limit
* ladder: 5 step(s), stopped EngineBound; sustainable 39.1 starts/s per shard
* engine latency runs from admission to `completed_at`, both on the bench host clock; the generator wait, from schedule to admission, is judged separately
* wall clock: 125.0s
* sound: every published number on this row rests on a measured sample


Every scenario reported sound.
```

The closed-loop control:

```text
# Harvest end-to-end benchmark run (issue #941)

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
| `throughput` | 1 | `workflows_per_sec` | 41.28 | yes |
| `throughput` | 1 | `measured_window_secs` | 14.54 | yes |
| `throughput` | 1 | `completions` | 1200.00 | yes |


## Notes

### `throughput` at 1 shard(s)

* per-shard completions: s0=1200
* topology: independent-servers
* host layout: 1 shard(s) on 1 database host(s) (localhost); a database shares the bench host with the workers and the generator
* resident decisions (issue #2007, measured loop): 3600 hits of 4800, hit rate 75.0%; replays: cold 1200
* decisions (issue #2252, measured loop): 4800 for 1200 runs, 0 extra, 0 extra cold; continuation hit rate 100.0%
* owner mode: off
* closed loop: 32 workflows in flight per shard, 1200 measured completions, warmup population 240
* mean in-flight population per shard against a target of 32: 30.6
* the headline is the middle-half rate: the ramp-up and the drain-down tails are excluded, so it is a sustained rate rather than an average over a changing queue depth
* a closed loop is latency-bound: this rate is the in-flight population divided by the workflow latency, not the shard's capacity. The capacity scenario measures capacity
* wall clock: 35.2s
* sound: every published number on this row rests on a measured sample


Every scenario reported sound.
```
