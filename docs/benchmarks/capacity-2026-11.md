# Harvest capacity results — open loop, 1 shard

Reference numbers for the open-loop capacity scenario, produced by
`autumn-harvest/benches/e2e_bench.rs` on `dad23f0` (`trunk-dev` plus this
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
| Measured | 2026-11-08, 17:05 to 17:11 UTC, on an otherwise idle box |

This is the host class of the 0.8.0 results: the same CPU model, core count,
OS and Postgres build. The database, the four worker processes and the
generator all share its four logical CPUs.

## Headline numbers

| arm | workflow | sustainable rate, 1 shard | p99 at that rate | first failed step |
|:--|:--|--:|--:|--:|
| `capacity_noop` | zero activities | **190.7 workflows/s** | 145.4 ms | 238.4/s |
| `capacity_canonical` | 3 activities | **48.8 workflows/s** | 548.2 ms | 61.0/s |

Both cells report **sound**. Each ladder stopped on an engine failure, not
on the generator or the budget. The p99 runs from admission to completion.

* **`capacity_noop`.** Every step up to 190.7/s held a flat backlog. At
  238.4/s the backlog grew 17.1 runs/s, over its 11.9 limit, and the median
  latency grew 81.0 ms/s. Step 5, at 152.6/s, failed once on the p99 rule
  alone. Its retry passed, with a p99 of 62.2 ms in the last window.
* **`capacity_canonical`.** Every step up to 48.8/s held a flat backlog. At
  61.0/s the backlog grew 9.0 runs/s, over its 3.05 limit, and the median
  latency grew 188.8 ms/s.

The ladder steps by 1.25x. So the true limit of each arm lies between its
sustainable rate and its first failed step. The published number is the
lower end of that interval.

## How stable is this?

An earlier sweep ran on `f96f11d`, 16:12 to 16:18 UTC, with the round-1
rule. The rule changes after it touch the retry and the start rates, not
the ladder rates. Its verbatim output is not kept here.

| arm | `f96f11d` | `dad23f0` (this file) |
|:--|--:|--:|
| `capacity_noop` | 190.7 | 190.7 |
| `capacity_canonical` | 39.1 | 48.8 |

The no-op arm reproduced to the step. The canonical arm moved one ladder
step. In the earlier sweep the 48.8 step grew its backlog 3.3 runs/s, over
the 2.44 limit. Here the same step held 0.1 runs/s. So the canonical limit
sits close to 48.8, and one step is the noise of this box. The box itself
moved between the sweeps: the replay control read 12.19 M events/s, then
10.23 M.

## The closed-loop control

The closed-loop `throughput` cell ran in the same sweep. It read **46.97
workflows/s**, sound, with 30.5 of its 32 workflows in flight. The earlier
sweep read 41.28. The 0.8.0 release read 52.91.

All three closed-loop numbers come from one in-process worker with 8
workflow slots. The capacity arms run 4 worker processes with 8 slots each.
So the closed-loop number is no check of the capacity number, and this file
does not explain the change since 0.8.0.

## What this number is not

* **Not a scaling result.** One shard only. No multi-shard capacity cell
  here has its own database host or its own worker host.
* **Not a durable figure.** `fsync=off` and `synchronous_commit=off`, as
  for every number in [`../benchmarks.md`](../benchmarks.md).
* **Not a dedicated database.** The database shares four logical CPUs with
  the workers and the generator. A dedicated database host would move the
  number. This file does not say how far.

## Verbatim run output

The sweep: the closed-loop control, the replay cell as a box-speed control,
and both capacity arms.

```text
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
| Capacity | 4 worker process(es) per shard, 16 generator connections per shard, 20s steps x1.25 |
| Postgres | PostgreSQL 16.15 (Ubuntu 16.15-0ubuntu0.24.04.1) on x86_64-pc-linux-gnu, compiled by gcc (Ubuntu 13.3.0-6ubuntu2~24.04.1) 13.3.0, 64-bit |


## Results

| scenario | shards | metric | value | sound |
|:--|--:|:--|--:|:--|
| `throughput` | 1 | `workflows_per_sec` | 46.97 | yes |
| `throughput` | 1 | `measured_window_secs` | 12.77 | yes |
| `throughput` | 1 | `completions` | 1200.00 | yes |
| `replay_throughput` | 1 | `events_per_sec` | 10229203.30 | yes |
| `replay_throughput` | 1 | `ms_per_history` | 0.98 | yes |
| `capacity_noop` | 1 | `sustainable_per_sec` | 190.70 | yes |
| `capacity_noop` | 1 | `sustainable_per_shard_per_sec` | 190.70 | yes |
| `capacity_noop` | 1 | `p99_ms_at_sustainable` | 145.38 | yes |
| `capacity_noop` | 1 | `steps` | 8.00 | yes |
| `capacity_noop` | 1 | `database_hosts` | 1.00 | yes |
| `capacity_noop` | 1 | `worker_hosts` | 1.00 | yes |
| `capacity_canonical` | 1 | `sustainable_per_sec` | 48.80 | yes |
| `capacity_canonical` | 1 | `sustainable_per_shard_per_sec` | 48.80 | yes |
| `capacity_canonical` | 1 | `p99_ms_at_sustainable` | 548.16 | yes |
| `capacity_canonical` | 1 | `steps` | 5.00 | yes |
| `capacity_canonical` | 1 | `database_hosts` | 1.00 | yes |
| `capacity_canonical` | 1 | `worker_hosts` | 1.00 | yes |


## Notes

### `throughput` at 1 shard(s)

* per-shard completions: s0=1200
* topology: independent-servers
* host layout: 1 shard(s) on 1 database host(s) (localhost); a database shares the bench host with the workers and the generator
* resident decisions (issue #2007, measured loop): 3600 hits of 4800, hit rate 75.0%; replays: cold 1200
* decisions (issue #2252, measured loop): 4800 for 1200 runs, 0 extra, 0 extra cold; continuation hit rate 100.0%
* owner mode: off
* closed loop: 32 workflows in flight per shard, 1200 measured completions, warmup population 240
* mean in-flight population per shard against a target of 32: 30.5
* the headline is the middle-half rate: the ramp-up and the drain-down tails are excluded, so it is a sustained rate rather than an average over a changing queue depth
* a closed loop is latency-bound: this rate is the in-flight population divided by the workflow latency, not the shard's capacity. The capacity scenario measures capacity
* wall clock: 31.9s
* sound: every published number on this row rests on a measured sample

### `replay_throughput` at 1 shard(s)

* 10001 events replayed (5000 activities), median of 20 iterations after 5 warmup iterations
* shard-invariant by construction: this row is the run's noise control, not a statement about sharding
* the same history `benches/replay_bench.rs` budgets at 200 ms (issue #135)
* sound: every published number on this row rests on a measured sample

### `capacity_noop` at 1 shard(s)

* topology: independent-servers
* host layout: 1 shard(s) on 1 database host(s) (localhost); a database shares the bench host with the workers and the generator
* worker layout: every worker process runs on the bench host
* open loop: 4 worker process(es) per shard, a 16-connection generator pool per shard, 20s steps from 50.0 starts/s per shard
* warmup (discarded): offered 50.0/s per shard; achieved 50.0; runs 1000; backlog slope 0.0 runs/s; median growth 0.1 ms/s; judged-window p99 70.4/62.7/62.2 ms; step p99 62.9 ms; generator wait p99 2.5 ms; sustainable
* step 0: offered 50.0/s per shard; achieved 50.0; runs 1000; backlog slope -0.0 runs/s; median growth -0.1 ms/s; judged-window p99 65.0/59.6/69.2 ms; step p99 65.0 ms; generator wait p99 2.4 ms; sustainable
* step 1: offered 62.5/s per shard; achieved 62.5; runs 1250; backlog slope -0.0 runs/s; median growth -0.1 ms/s; judged-window p99 51.3/48.8/55.0 ms; step p99 53.1 ms; generator wait p99 2.5 ms; sustainable
* step 2: offered 78.1/s per shard; achieved 78.1; runs 1562; backlog slope 0.0 runs/s; median growth -0.0 ms/s; judged-window p99 39.5/34.5/48.4 ms; step p99 42.7 ms; generator wait p99 2.8 ms; sustainable
* step 3: offered 97.7/s per shard; achieved 97.7; runs 1954; backlog slope -0.0 runs/s; median growth -0.1 ms/s; judged-window p99 28.9/24.7/27.6 ms; step p99 37.4 ms; generator wait p99 2.7 ms; sustainable
* step 4: offered 122.1/s per shard; achieved 122.1; runs 2442; backlog slope 0.0 runs/s; median growth 0.0 ms/s; judged-window p99 31.3/30.5/35.2 ms; step p99 33.0 ms; generator wait p99 3.2 ms; sustainable
* step 5 (noise only, retried): offered 152.6/s per shard; achieved 152.6; runs 3052; backlog slope 0.3 runs/s; median growth 0.3 ms/s; judged-window p99 64.7/90.6/230.4 ms; step p99 196.2 ms; generator wait p99 5.3 ms; **not sustainable**: the p99 grew from 64.7 ms to 230.4 ms, over the 229.3 ms limit
* step 5: offered 152.6/s per shard; achieved 152.6; runs 3052; backlog slope 0.0 runs/s; median growth 0.3 ms/s; judged-window p99 59.4/58.2/62.2 ms; step p99 58.4 ms; generator wait p99 4.0 ms; sustainable
* step 6: offered 190.7/s per shard; achieved 190.5; runs 3814; backlog slope 0.2 runs/s; median growth 1.2 ms/s; judged-window p99 104.2/111.5/119.5 ms; step p99 145.4 ms; generator wait p99 8.6 ms; sustainable
* step 7: offered 238.4/s per shard; achieved 238.0; runs 4768; backlog slope 17.1 runs/s; median growth 81.0 ms/s; judged-window p99 189.0/700.3/1048.2 ms; step p99 1030.0 ms; generator wait p99 42.7 ms; **not sustainable**: the backlog grew 17.12 runs/s, over the 11.92 runs/s limit; the median latency grew 81.0 ms/s, over the 5 ms/s limit; the p99 grew from 189.0 ms to 1048.2 ms, over the 478.0 ms limit
* ladder: 8 step(s), stopped EngineBound; sustainable 190.7 starts/s per shard
* engine latency runs from admission to `completed_at`, both on the bench host clock; the generator wait, from schedule to admission, is judged separately
* wall clock: 206.1s
* sound: every published number on this row rests on a measured sample

### `capacity_canonical` at 1 shard(s)

* topology: independent-servers
* host layout: 1 shard(s) on 1 database host(s) (localhost); a database shares the bench host with the workers and the generator
* worker layout: every worker process runs on the bench host
* open loop: 4 worker process(es) per shard, a 16-connection generator pool per shard, 20s steps from 25.0 starts/s per shard
* warmup (discarded): offered 25.0/s per shard; achieved 25.0; runs 500; backlog slope 0.0 runs/s; median growth 0.8 ms/s; judged-window p99 340.4/374.1/364.8 ms; step p99 364.8 ms; generator wait p99 3.7 ms; sustainable
* step 0: offered 25.0/s per shard; achieved 25.0; runs 500; backlog slope -0.0 runs/s; median growth -0.4 ms/s; judged-window p99 332.4/329.7/307.1 ms; step p99 326.0 ms; generator wait p99 2.7 ms; sustainable
* step 1: offered 31.3/s per shard; achieved 31.3; runs 626; backlog slope 0.1 runs/s; median growth 2.5 ms/s; judged-window p99 349.3/339.8/363.6 ms; step p99 352.9 ms; generator wait p99 4.6 ms; sustainable
* step 2: offered 39.1/s per shard; achieved 39.1; runs 782; backlog slope 0.1 runs/s; median growth 2.8 ms/s; judged-window p99 388.1/664.4/409.0 ms; step p99 613.3 ms; generator wait p99 9.1 ms; sustainable
* step 3: offered 48.8/s per shard; achieved 48.8; runs 976; backlog slope 0.1 runs/s; median growth 2.4 ms/s; judged-window p99 521.6/512.4/567.7 ms; step p99 548.2 ms; generator wait p99 9.6 ms; sustainable
* step 4: offered 61.0/s per shard; achieved 61.0; runs 1220; backlog slope 9.0 runs/s; median growth 188.8 ms/s; judged-window p99 2340.3/3164.7/4075.0 ms; step p99 3993.2 ms; generator wait p99 9.6 ms; **not sustainable**: the backlog grew 8.96 runs/s, over the 3.05 runs/s limit; the median latency grew 188.8 ms/s, over the 5 ms/s limit
* ladder: 5 step(s), stopped EngineBound; sustainable 48.8 starts/s per shard
* engine latency runs from admission to `completed_at`, both on the bench host clock; the generator wait, from schedule to admission, is judged separately
* wall clock: 127.9s
* sound: every published number on this row rests on a measured sample


Every scenario reported sound.
```
