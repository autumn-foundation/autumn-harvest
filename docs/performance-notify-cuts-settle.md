# A NOTIFY cuts the settle once (issue #2653)

A worker sleeps a 50 to 75 ms settle after an empty poll that a NOTIFY
caused. Before this change, a NOTIFY that arrived in that sleep waited for
its end. This page measures the change that lets a NOTIFY cut the settle
once. [`design/DESIGN-2653.md`](design/DESIGN-2653.md) holds the design.

> **This is a reference measurement, not an SLO.** It comes from one 4-core
> box with the database, the workers and the load generator on it.
> Reproduce it before you design against it.

## TL;DR

* **The signal round-trip p99 falls from 74.8 ms to 15.9 ms** at 1 shard.
  Each of five changed runs reads 15.1 to 18.8 ms. Each of five base runs
  reads 72.1 to 75.4 ms.
* **The dispatch p99 falls from 12.6 ms to 10.7 ms.** The p50 values of
  both scenarios fall by about 1 ms.
* **The capacity cells show no systematic loss.** Over three alternating
  pairs per arm, no paired difference is more than one ladder step, and one
  step is the noise of this box. The no-op arm leans low and the canonical
  arm leans high, over all versions measured.
* **Empty polls rise a little.** The median rate rises 11% in
  `capacity_noop`, 11% in `capacity_canonical` and 6% in the latency cells.
  Two earlier versions, below, raised them more.

## The change

* A NOTIFY or an owner wake cuts a fresh settle. The leader polls at once.
  The rest of the settle stays owed, up to the original end, and no NOTIFY
  cuts the rest. So each settle has one NOTIFY cut at most, and the poll at
  the original end still comes for a row that a host clock stamped early.
* A rest always owes a fresh settle. So the NOTIFY that cut also gets a full
  margin of its own.
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
* **Builds.** `base` is `trunk-dev` at `83b0417`. Three changed builds ran
  in turn: `new` cut the settle and did nothing else, `v2` added the drain,
  and `v3` added the fresh settle after a rest. `v3` ships. Each build is a
  release `e2e_bench` binary.
* **Runs.** Each run is one `e2e_bench` invocation at 1 shard. The builds
  alternate, so a drift of the box hits both. The latency runs hold
  `signal_roundtrip` and `dispatch_latency`. Each capacity run holds one
  arm, with the default 4 worker processes.
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
| base (median of 5) | 9.12 | 74.79 | 6.65 | 12.55 | 83.40 | 2.25 |
| v3 (median of 5) | 7.93 | 15.87 | 5.93 | 10.68 | 88.50 | 2.38 |

The signal p99 of the base build stays near the settle. A signal that
lands in a settle waits for the rest of it. That matches the 80 to 85 ms
p99 that [`benchmarks/results-v0.8.0.md`](benchmarks/results-v0.8.0.md)
published. The changed build polls at the NOTIFY instead.

## Capacity with 4 worker processes

Three alternating pairs of the shipped version, at 1 shard. The sustainable
rate is the highest ladder step that holds. The ladder steps by 1.25 times.

| arm | pair | base rate/s | v3 rate/s | base empty polls/s | v3 empty polls/s | base empty per claim | v3 empty per claim |
|:--|--:|--:|--:|--:|--:|--:|--:|
| `capacity_noop` | 1 | 372.5 | not published | 281.2 | 345.1 | 1.65 | 2.64 |
| `capacity_noop` | 2 | 298.0 | 372.5 | 296.1 | 312.8 | 1.76 | 1.79 |
| `capacity_noop` | 3 | 372.5 | 298.0 | 276.7 | 290.0 | 1.60 | 1.92 |
| `capacity_canonical` | 1 | 76.3 | 76.3 | 272.0 | 301.5 | 0.82 | 0.91 |
| `capacity_canonical` | 2 | 61.0 | 61.0 | 263.1 | 297.3 | 0.92 | 0.89 |
| `capacity_canonical` | 3 | 61.0 | 76.3 | 268.1 | 245.7 | 0.93 | 0.70 |

Pairs 1 and 3 ran base first. Pair 2 ran v3 first.

* **Pair 1 of `capacity_noop` is not published.** At 298.0/s the generator
  wait p99 read 107.1 ms, over its 100 ms limit. The engine kept up: its
  backlog fell. The bench rule then calls the step generator-bound, so the
  cell has no engine rate. The step before it, 238.4/s, held.
* **The rate.** In `capacity_noop` the shipped build is one step higher in
  one pair and one step lower in another. In `capacity_canonical` it is
  equal, equal and one step higher.
* **All versions together.** The no-op arm held 372.5/s in three of the
  seven published changed runs, and base held it in six of eight. The
  canonical arm held 76.3/s in six of eight changed runs, and base held it
  in three of eight. So a one-step loss on the no-op arm and a one-step
  gain on the canonical arm are both possible. This box cannot separate
  either from noise, because base itself moved two steps between runs.
* **The p99 at the sustainable rate.** It moves with the rate and the run.
  In pair 1 of the canonical arm both builds held 76.3/s. There the p99 read
  1112 ms for v3 and 988 ms for base. A run at a lower rate is not
  comparable.
* **Empty polls.** The median rate rises from 281.2 to 312.8 per second in
  `capacity_noop` and from 268.1 to 297.3 in `capacity_canonical`.

## The earlier versions

The first version, `new`, cut the settle and did nothing else. Two
alternating pairs per arm, in
[`round1/`](perf-artifacts/notify-cuts-settle/round1/). The second, `v2`,
added the drain. Three pairs per arm and five latency pairs, in
[`round2/`](perf-artifacts/notify-cuts-settle/round2/).

| version | noop rates/s (base / changed) | canonical rates/s (base / changed) | median empty polls/s, noop (base / changed) | median empty polls/s, canonical (base / changed) | median empty polls/s, latency (base / changed) |
|:--|:--|:--|:--|:--|:--|
| `new` | 372.5/298.0, 372.5/372.5 | 76.3/76.3, 61.0/76.3 | 314.6 / 426.5 | 325.9 / 413.1 | 85.8 / 106.1 |
| `v2` | 372.5/372.5, 238.4/298.0, 372.5/298.0 | 76.3/76.3, 61.0/61.0, 61.0/76.3 | 281.1 / 339.2 | 269.6 / 315.9 | 85.4 / 104.9 |

* `new` polled at the cut, at the end, and again at once for each NOTIFY
  that queued in the rest. It raised the empty polls by up to 48% in a pair.
* `v2` removed that last poll. But a rest then owed nothing. So the NOTIFY
  that cut got only the poll at the cut and the poll at the old end, and
  both can come before a row that a host clock stamped early. A code review
  found this. `v3` makes a rest owe a fresh settle. That also took the
  empty polls down again.
* All three versions gave the same latency gain. The signal p99 read 15.4,
  14.9 and 15.9 ms against base runs at 73.8, 73.7 and 74.8 ms.

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
