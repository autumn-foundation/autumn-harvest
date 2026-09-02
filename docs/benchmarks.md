# Harvest end-to-end benchmarks

Harvest publishes two performance artifacts besides this page: the replay CPU
budget (a 10 001-event history replays in under 200 ms, issue #135) and the
task-claim microbenchmark with its CI-gated p50 budget (issue #786,
[`performance.md`](performance.md)). Both are *component* numbers. Neither
answers the question an evaluating architect asks first:

> How many workflows per second does this thing actually do, end to end, and at
> what latency?

This page answers it, and ships the harness so you can check the answer on your
own hardware in one command.

> **These are reference-machine numbers, not an SLO.** They were taken on one
> box, with one Postgres configuration, at one load level, all documented below.
> Your CPU count, your `shared_buffers`, your durability settings, your workflow
> shape and your worker concurrency all move them, and some of them move them by
> more than an order of magnitude. Reproduce them on your own hardware before
> designing against them — the whole suite is in the repo precisely so you can.

## What is measured

Four scenarios, each run at **1 shard**, **2 shards** and **4 shards**.

| id | headline | what it means |
|:--|:--|:--|
| `throughput` | sustained workflows completed/sec | a canonical **3-activity** workflow, run under a bounded closed loop |
| `dispatch_latency` | activity dispatch p50/p99 | `harvest_task_queue.created_at` (the activity was scheduled) → the activity handler's first line |
| `signal_roundtrip` | signal round-trip p50/p99 | an HTTP signal request leaving the client → the workflow's own code resuming past `wait_for_signal` |
| `replay_throughput` | replay events/sec | the issue #135 10 001-event history, in memory |

### Headline numbers, v0.6.0

Four logical CPUs, Postgres 16.13, durability off, one worker per shard. The
full environment, the per-cell notes and the verbatim run output are in
[`benchmarks/results-v0.6.0.md`](benchmarks/results-v0.6.0.md).

| scenario | metric | 1 shard | 2 shards | 4 shards |
|:--|:--|--:|--:|--:|
| `throughput` | workflows/sec | 22.65 | 34.97 | 33.53 |
| `dispatch_latency` | p50 ms | 37.78 | 43.56 | 45.74 |
| `dispatch_latency` | p99 ms | 55.60 | 62.23 | 100.03 |
| `signal_roundtrip` | p50 ms | 55.79 | 59.66 | 45.60 |
| `signal_roundtrip` | p99 ms | 65.44 | 69.02 | 80.24 |
| `replay_throughput` | events/sec | 9 960 371.64 | 9 657 651.19 | 9 096 540.11 |

Three things a reader should take from that table before anything else:

* **Sharding bought 1.54x at two shards and then stopped.** Four shards is
  slower than two *on this machine*. See
  [what the shard sweep can and cannot show](#what-the-shard-sweep-can-and-cannot-show)
  — the run's own replay control drifted -8.7% across the sweep, which is the
  box getting busier, not the engine getting worse.
* **The tail degrades faster than the median.** Dispatch p99 nearly doubles
  across the sweep while p50 moves 21%.
* **p99 is the least reproducible number here.** It is published and checked
  because #941's success metric names it, but a tail measured on a box that is
  also running the harness is partly a measurement of that box's run queue —
  the reason issue #786 gates p50 rather than p99. A p99 outside tolerance on a
  busy machine is expected, not a regression.

### Results by release

Each release's numbers are kept, not overwritten:

* **0.6.0** — [`benchmarks/results-v0.6.0.md`](benchmarks/results-v0.6.0.md)

## Reproducing

One command, against the committed four-shard Postgres topology in
[`benchmarks/docker-compose.yml`](../benchmarks/docker-compose.yml):

```bash
./benchmarks/run.sh
```

It brings the topology up, runs all twelve cells, writes the report to
`benchmarks/results/<timestamp>.md`, and tears the topology down again. Budget
roughly 20–40 minutes on a four-core machine.

To check a fresh run against the published numbers rather than just reading it:

```bash
HARVEST_BENCH_CHECK=1 ./benchmarks/run.sh
```

That prints a per-number verdict against the published baselines at the stated
tolerance (below). Nothing about it is a CI gate — see [scope](#scope).

### Narrowing a run

Reproducing one headline should not cost forty minutes:

```bash
HARVEST_BENCH_SCENARIOS=signal_roundtrip HARVEST_BENCH_SHARDS=1 ./benchmarks/run.sh
```

| variable | effect |
|:--|:--|
| `HARVEST_BENCH_SCENARIOS` | comma-separated scenario ids; unset runs all four |
| `HARVEST_BENCH_SHARDS` | comma-separated shard counts; unset runs 1, 2 and 4 |
| `HARVEST_BENCH_INFLIGHT` | closed-loop in-flight workflows per shard (the load level) |
| `HARVEST_BENCH_WORKFLOWS` | measured completions per shard |
| `HARVEST_BENCH_CHECK` | also print the reproduction verdict |
| `HARVEST_BENCH_KEEP` | leave the containers running afterwards |

### Without Docker

The harness resolves its shards in this order:

1. `HARVEST_BENCH_SHARD_URLS` — one **admin** Postgres URL per shard, comma
   separated. This is what `run.sh` sets, and the only mode that gives
   independent servers per shard.
2. `HARVEST_TEST_DATABASE_URL` — a single admin URL; one database per shard is
   created on it. Cheap, but every "shard" then shares one server's buffer
   cache, WAL writer and CPU, so a shard sweep taken this way is **not** a
   scale-out measurement. The report labels the topology it used.
3. A `postgres:16` testcontainer, same shape as (2).
4. None of those: the suite prints a skip notice and exits 0. `cargo bench` on a
   laptop with no Postgres is not a failure.

Every mode creates fresh, uniquely named databases per run and drops them
afterwards, so a benchmark can never leak a few thousand rows into a database
somebody else is using.

## Reproduction tolerance

A fresh clone on the documented hardware should land within **±15%** of the
published number. `HARVEST_BENCH_CHECK=1` applies exactly that band and prints
`reproduced` or `outside tolerance` per number.

Two honest caveats about that band:

* It is a band on *this* hardware class. On a different CPU count, or against a
  Postgres with durability enabled, expect to be outside it — that is the
  measurement working, not failing.
* The published numbers were taken on **native Postgres clusters**, not
  containers. The compose path adds container networking and storage overhead
  that was not separately quantified, so a compose run is the more pessimistic
  of the two.

## Methodology

**Warmup.** Every database scenario runs a discarded warmup population through
the identical path before the measured one, so connection pools are full, the
planner has real statistics, and no measured sample pays a first-time cost. The
dispatch scenario additionally discards the leading tenth of its samples; the
signal scenario signals a whole separate warmup cohort and keeps none of it.

**Sustained, not average.** The throughput headline is the completion rate over
the **middle half** of the measured population (the 25th→75th percentile
completion). A whole-run average would be dragged down by the ramp-up and the
drain-down tail, neither of which is a sustained rate.

**Bounded load, on purpose.** Throughput is measured under a *closed loop*: a
fixed number of workflows is held in flight and topped up as runs complete.
Pre-loading a deep backlog instead would measure the **claim-depth curve** —
claim cost grows superlinearly with pending backlog depth, which is issue #786's
published finding, not this suite's — and would produce a figure that depends on
where in the drain you looked. Because throughput under a closed loop is
`concurrency / latency`, the load level is a documented knob
(`HARVEST_BENCH_INFLIGHT`), and the report publishes the **mean in-flight
population actually held** next to the rate. If the harness could not hold its
target, the run says so and the number is not published.

**Latency is measured unsaturated.** `dispatch_latency` and `signal_roundtrip`
are paced deliberately below saturation. Under a saturated queue those
percentiles measure the backlog, not the dispatch or signal path. The report
prints the achieved rate against the target, and a run that could not hold its
pace is marked and not published.

**Clocks.** The signal round-trip is measured end to end on one monotonic clock
inside one process, so it carries no skew term at all. Activity dispatch spans a
database timestamp (`created_at`) and a host timestamp (the handler's first
line); the harness measures the host-to-database offset and publishes it beside
the number rather than assuming it away, and a negative sample — which only skew
can produce — is counted and reported, never clamped to zero.

**Nothing unsound is published.** Each cell reports `n/a` and a named reason,
rather than a confident-looking number, when it collected too few samples,
failed to drain what it started, left a shard idle, could not hold its pace or
its in-flight target, or saw a signal request fail.

**Replay is the noise control.** Replay is in-memory and cannot legitimately
move with shard count. It is run at all three counts anyway: drift across those
three runs bounds how loaded the box was while the other nine cells were
measured.

## What the shard sweep can and cannot show

The sweep holds **hardware fixed** and varies the number of shards on it. On the
reference box that means 1, 2 and 4 shards competing for the same four cores,
with one worker per shard. So the sweep bounds how well Harvest's *software*
scales out across shards on a fixed machine. It says nothing about what a
4-machine, 4-shard deployment does, and the numbers should not be read as if it
did. Issue #941 asks for the sweep to be published "or honestly failing to
demonstrate" scaling; the per-release results file reports what actually
happened.

## Known limitations

* **Durability is off.** Both the compose topology and the reference clusters run
  `fsync=off` and `synchronous_commit=off`. This is the single biggest thing
  separating these figures from a durable production deployment. It is
  deliberate — the suite measures the engine, not the reference machine's disk —
  and it means these numbers are an **upper bound** for a durably configured
  Postgres.
* **The signal endpoint is not the production router.** The HTTP hop is real: a
  loopback socket, a real request, a real response. But it is a minimal endpoint
  calling the same `signal::send_signal` entry point the plugin's
  `POST /workflows/{id}/signal/{signal_name}` route calls, without autumn-web's
  auth, tracing, rate-limiting or payload-schema middleware. Read the round-trip
  as the engine-side floor, not as what your gateway will deliver.
* **The workflow is deliberately trivial.** Three activities that return
  immediately. Real activities do work; this measures what Harvest costs around
  your work, not the sum.
* **One workflow shape.** No child workflows, no timers, no retries, no
  payload of consequence.
* **A single reference machine.** Four logical CPUs. Nothing here says how the
  engine behaves on 32 cores.

## Relationship to the other performance pages

* [`performance.md`](performance.md) (issue #786) is the **component-level
  complement** to this page: it measures `queue::claim_task` and the enqueue
  path in isolation, attributes cost to five accreted claim predicates, and
  carries the only performance budget CI actually gates. This suite deliberately
  contains **no claim or enqueue scenario** and adds **no CI gate** — it does not
  duplicate #786, it sits above it. When an end-to-end number here moves,
  `performance.md` is where you look to find out whether the claim path is why.
* `benches/replay_bench.rs` (issue #135) owns the replay CPU budget. The replay
  scenario here reports throughput over the *same* history, from the same
  builder — `build_history` lives in the shared harness and both benches call
  it — so the two can never drift into describing different workloads.

## Scope

Issue #941 measures and publishes; it does not tune, and it gates nothing.

* **No CI gate.** CI-gated end-to-end regression budgets are explicitly a
  follow-up, once these baselines have stabilised. No CI manifest row runs any
  scenario on this page, and
  `autumn-harvest/tests/integration/benchmarks_docs.rs` fails the build if one
  ever does.
* **No engine change.** This slice adds no `WorkflowEvent` variant, no
  migration, and no behaviour or public-API change. Every number is taken from
  columns and events that already existed.
* **No tuning.** Anything these numbers reveal is a separate issue.

## Where the code is

| | |
|:--|:--|
| Harness | `autumn-harvest/tests/integration/e2e_bench_support.rs` |
| Runner | `autumn-harvest/benches/e2e_bench.rs` |
| Topology | `benchmarks/docker-compose.yml` |
| One command | `benchmarks/run.sh` |
| Docs guard | `autumn-harvest/tests/integration/benchmarks_docs.rs` |
| Planning record | `docs/plans/2026-09-01-e2e-benchmark-suite.md` |
