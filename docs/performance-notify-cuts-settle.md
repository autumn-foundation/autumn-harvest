# A NOTIFY cuts the settle once (issue #2653)

A worker sleeps a 50 to 75 ms settle after an empty poll that a NOTIFY
caused. Before this change, a NOTIFY that arrived in that sleep waited for
its end. This page measures the change that lets a NOTIFY cut the settle
once. [`design/DESIGN-2653.md`](design/DESIGN-2653.md) holds the design.

> **This is a reference measurement, not an SLO.** It comes from one 4-core
> box with the database, the workers and the load generator on it.
> Reproduce it before you design against it.

## TL;DR

* **The signal round-trip p99 falls from 73.7 ms to 14.9 ms** at 1 shard.
  Each of five changed runs reads 14.3 to 15.7 ms. Each of five base runs
  reads 72.2 to 77.3 ms.
* **The dispatch p99 falls from 10.3 ms to 8.2 ms.** The p50 values of
  both scenarios fall by about 1 ms.
* **The capacity cells show no systematic loss.** Over three alternating
  pairs, the median paired difference is zero ladder steps for both arms.
  Each single difference is one step, and one step is the noise of this
  box. The no-op arm leans low and the canonical arm leans high.
* **Empty polls rise.** The median rate rises 21% in `capacity_noop`, 17% in
  `capacity_canonical` and 23% in the latency cells. A first version
  without the drain (below) raised them 36% and 27% in the capacity cells.

## The change

* A NOTIFY or an owner wake cuts a fresh settle. The leader polls at once.
  The rest of the settle stays owed, up to the original end, and no NOTIFY
  cuts the rest. So each settle has one NOTIFY cut at most, and the poll at
  the original end still comes for a row that a host clock stamped early.
* A settle that sleeps to its end takes the NOTIFYs queued then, and owes a
  fresh settle for them. The poll at the end covers each of them. Before,
  the next idle wait polled again at once. In a fleet, that poll was empty
  on each worker.
* `formal/verus/src/notify_settle.rs` proves the bound on the kernel. The
  unit tests in `worker.rs` check the glue.

## The method

* **Box.** 4 logical CPUs, Postgres 16.15 on the same host, `fsync=off`,
  `synchronous_commit=off`, `max_connections=200`. One native cluster on
  port 55432, set up like the services in
  [`benchmarks/docker-compose.yml`](../benchmarks/docker-compose.yml), with
  `pg_stat_statements` preloaded.
* **Builds.** `base` is `trunk-dev` at `83b0417`. `new` is the first
  version, with the cut and no drain. `v2` is the version that ships, with
  the cut and the drain. Each build is a release `e2e_bench` binary.
* **Runs.** Each run is one `e2e_bench` invocation at 1 shard. The builds
  alternate in the order base, changed, changed, base, so a drift of the
  box hits both. The latency runs hold `signal_roundtrip` and
  `dispatch_latency`. Each capacity run holds one arm, with the default 4
  worker processes.
* **Empty polls.** Before each run, `pg_stat_statements_reset()` clears the
  counters. After it, the claim statement gives its calls and its rows. A
  claim returns one row at most, so the empty polls are the calls less the
  rows. The rate divides them by the wall time of the run. A capacity run
  with a longer ladder runs longer at a high rate. So the page also gives
  the empty polls per claimed task.
* **Script.** [`run_cell.sh`](perf-artifacts/notify-cuts-settle/run_cell.sh)
  runs one cell and appends the census. [`summarize.py`](perf-artifacts/notify-cuts-settle/summarize.py)
  builds the tables below from the outputs.

## Latency at 1 shard

Five alternating pairs of the shipped version. The outputs are in
[`final/`](perf-artifacts/notify-cuts-settle/final/).

| build | signal p50 ms | signal p99 ms | dispatch p50 ms | dispatch p99 ms | empty polls/s | empty polls per claim |
|:--|--:|--:|--:|--:|--:|--:|
| base (median of 5) | 7.90 | 73.68 | 5.50 | 10.33 | 85.40 | 2.29 |
| v2 (median of 5) | 6.74 | 14.87 | 4.76 | 8.16 | 104.90 | 2.81 |

The signal p99 of the base build stays near the settle. A signal that
lands in a settle waits for the rest of it. That matches the 80 to 85 ms
p99 that [`benchmarks/results-v0.8.0.md`](benchmarks/results-v0.8.0.md)
published. The changed build polls at the NOTIFY instead.

## Capacity with 4 worker processes

Three alternating pairs of the shipped version, at 1 shard. The sustainable
rate is the highest ladder step that holds. The ladder steps by 1.25 times.

| arm | pair | base rate/s | v2 rate/s | base empty polls/s | v2 empty polls/s | base empty per claim | v2 empty per claim |
|:--|--:|--:|--:|--:|--:|--:|--:|
| `capacity_noop` | 1 | 372.5 | 372.5 | 281.1 | 314.7 | 1.64 | 1.81 |
| `capacity_noop` | 2 | 238.4 | 298.0 | 326.0 | 339.2 | 2.50 | 2.24 |
| `capacity_noop` | 3 | 372.5 | 298.0 | 274.0 | 364.9 | 1.57 | 2.32 |
| `capacity_canonical` | 1 | 76.3 | 76.3 | 237.2 | 281.5 | 0.67 | 0.86 |
| `capacity_canonical` | 2 | 61.0 | 61.0 | 269.6 | 348.5 | 0.91 | 1.19 |
| `capacity_canonical` | 3 | 61.0 | 76.3 | 329.1 | 315.9 | 1.12 | 0.97 |

Pairs 1 and 2 ran base first. Pair 3 ran v2 first.

* **The rate.** In `capacity_noop` the changed build is equal, one step
  higher and one step lower. In `capacity_canonical` it is equal, equal
  and one step higher. Over the five changed no-op runs of both versions,
  the arm held 372.5/s in two and 298.0/s in three. Base held 372.5/s in
  four of its five no-op runs. So a one-step loss on the no-op arm is
  possible. This box cannot separate it from noise, because base itself
  moved two steps between runs. Over the five changed canonical runs, the
  arm held 76.3/s in four. Base held it in two of five.
* **The p99 at the sustainable rate.** It moves with the rate and the run.
  In pair 1 of the canonical arm both builds held 76.3/s. There the p99
  read 812 ms for v2 and 483 ms for base. The first version read 985 and
  893 ms against 372 ms. A run at a lower rate is not comparable.
* **Empty polls.** The median rate rises from 281.1 to 339.2 per second in
  `capacity_noop` and from 269.6 to 315.9 in `capacity_canonical`.

## The first version, without the drain

The first version cut the settle and did nothing else. Two alternating
pairs per arm. The outputs are in
[`round1/`](perf-artifacts/notify-cuts-settle/round1/).

| arm | base rates/s | new rates/s | base empty polls/s | new empty polls/s |
|:--|:--|:--|:--|:--|
| `capacity_noop` | 372.5, 372.5 | 298.0, 372.5 | 305.2, 323.9 | 450.5, 402.4 |
| `capacity_canonical` | 76.3, 61.0 | 76.3, 76.3 | 296.1, 355.7 | 414.2, 411.9 |

Its latency gains matched the shipped version: a signal p99 of 15.4 ms
against 73.8 ms. But it raised the empty polls by up to 48% in a pair. The
leader polled at the cut, at the end, and again at once for each NOTIFY
that queued in the rest. The drain removes that last poll.

## What this page does not show

* **A dedicated database.** The database shares 4 CPUs with 4 worker
  processes and the generator. Each empty poll costs CPU that the claims
  need. A database on its own host would feel the extra polls less.
* **A larger fleet.** Each NOTIFY wakes every worker on the queue. With
  more workers the extra polls grow with the fleet. The bound of one cut
  per settle keeps each worker at two polls per settle under a dense
  NOTIFY stream.
* **More than one shard.** All cells ran at 1 shard.

## Reproduce

```bash
cargo bench -p autumn-harvest --features db,testing --bench e2e_bench --no-run
```

Then run [`run_cell.sh`](perf-artifacts/notify-cuts-settle/run_cell.sh)
with the binary that the build prints, once per cell, against a Postgres
with `pg_stat_statements` preloaded on port 55432.
