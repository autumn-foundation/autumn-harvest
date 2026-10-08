# Harvest end-to-end benchmarks

Harvest publishes two performance artifacts besides this page: the replay CPU
budget (issue #135: a 10 000-event history replays in under 200 ms; the bench's
history is 10 001 events) and the
task-claim microbenchmark with its CI-gated p50 budget (issue #786,
[`performance.md`](performance.md)). Both are *component* numbers. Neither
answers the question an evaluating architect asks first:

> How many workflows per second does this thing actually do, end to end, and at
> what latency?

This page answers it, and ships the harness so you can check the answer on your
own hardware in one command.

For commit throughput with concurrent `pg_notify` writers (issue #1796), see
[`benchmarks/notify-commit.md`](benchmarks/notify-commit.md).

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
| `replay_throughput` | replay events/sec | the issue #135 history (10 001 events), in memory |

### Headline numbers, v0.7.0

Four logical CPUs, Postgres 16.15, durability off, one worker per shard. The
full environment, the per-cell notes and the verbatim run output are in
[`benchmarks/results-v0.7.0.md`](benchmarks/results-v0.7.0.md).

| scenario | metric | 1 shard | 2 shards | 4 shards |
|:--|:--|--:|--:|--:|
| `throughput` | workflows/sec | 15.46 | 18.84 | 19.50 |
| `dispatch_latency` | p50 ms | 16.12 | 26.17 | 69.98 |
| `dispatch_latency` | p99 ms | 90.86 | 107.79 | 183.13 |
| `signal_roundtrip` | p50 ms | 47.53 | 51.10 | 62.27 |
| `signal_roundtrip` | p99 ms | 92.99 | 95.52 | 118.00 |
| `replay_throughput` | events/sec | 9 750 423.86 | 9 063 901.37 | 8 981 387.93 |

Read these four points with the table:

* **This is a different host from 0.6.0, and most of the drop is the host.**
  A same-box control, linked from the results file, runs the 0.6.0 suite on
  this host. There,
  0.6.0 reads 17.91 workflows/sec at one shard, against 23.73 on its own
  host. On the same host, 0.7.0 is 14% below 0.6.0 at one shard, 11% below at
  two and level at four. It dispatches faster: a 1-shard p50 of 16.12 ms
  against 45.31.
* **The box was quiet, but less quiet than for 0.6.0.** The replay control
  spread **7.9%** against 0.8% for 0.6.0. That is inside the 10% bar. On four
  cores, a concurrent build once moved a published latency by more than 10x.
  Check your own run's noise-control section before you compare anything.
* **Sharding bought 1.22x at two shards and 1.26x at four.** Four shards
  share four cores with four Postgres clusters and the harness. See
  [what the shard sweep can and cannot show](#what-the-shard-sweep-can-and-cannot-show).
* **The dispatch tail is wide.** At one shard the dispatch p99 is 5.6x its
  p50. The p50 rises fastest with shard count: 16.12 to 69.98 ms.

### Reading this next to another engine

Before putting `15.46 workflows/sec` next to a number from another
durable-execution engine's own page, two things make that comparison
misleading on their own — independent of which engine comes out ahead.

**The unit is not the same.** This page's headline counts whole workflows.
Other engines often publish a per-transition, per-action, or per-step figure
instead. The canonical workflow this page measures is claimed off
`harvest_task_queue` **7** times per completed run: the workflow task is
claimed once to start the run, then once more each time an activity
completes and wakes it (three activities, so four workflow-task claims in
total), plus one claim per activity task (three). A claim is a dispatch —
a worker picking up and running a queued row — not a count of distinct rows;
the workflow task keeps one row for the whole run, reused through park and
wake updates, and gets claimed four times from it. So a number in this
page's unit is smaller than the same physical work counted the other way by
roughly that factor. Multiply a `workflows/sec` cell by **7** for a rough
per-dispatched-task rate in a shape closer to what a per-transition or
per-action page publishes. That reading is derived from the measurement
below, not a second measured result.

**The configuration is a floor, not a ceiling.** The three database-backed
scenarios — `throughput`, `dispatch_latency`, `signal_roundtrip` — are
latency-bound below, by choice: a 25 ms poll interval, one worker per shard,
and four logical CPUs shared with Postgres and the load generator — see
[the configuration these numbers were taken at](#the-configuration-these-numbers-were-taken-at).
`replay_throughput` is exempt: it runs entirely in memory as this page's
noise control, so none of that configuration bounds it. Little's law on the
`throughput` scenario's 1-shard cell — 32 workflows in flight ÷ 15.46/sec ≈
2.07 s
end to end per workflow — bounds how long a workflow spends in the system.
It says nothing about how that time splits between dispatch waiting,
database work, and worker execution; this suite does not instrument that
split for the throughput scenario itself. Adding workers, or giving Postgres
its own cores, moves the number, and neither is an architectural change. The
poll interval is not as direct a lever as it looks: with LISTEN/NOTIFY wired,
as it is here, a successful notification wakes a worker in 50 to 75 ms
regardless of the configured interval (a fixed 50 ms before issue #1796 added
the jitter; 0.7.0 has it), and a worker claiming tasks back to
back under load never waits at all. The interval mainly bounds a *missed*
notification, per
[the configuration these numbers were taken at](#the-configuration-these-numbers-were-taken-at).

Neither point says whether Harvest is faster or slower than anything else.
This suite still runs no competitor, and a competitor's own published figure
would carry accuracy and staleness this project cannot vouch for. A comparison
worth trusting re-runs both engines on the same hardware, which is why this
suite ships in the repo — see [Reproducing](#reproducing).

**Such comparisons have now been run on this page's own workflow, and
Harvest lost them.** They are reported in the assay ledger rather than here.
The engine is deliberately not named on this page: issue #1309's acceptance
criterion is that no competitor figure appears on it, and
`benchmarks_docs.rs` enforces that. See [the assay ledger](assays/README.md):
entries 11 and 14 are the cross-engine runs, and entry 10 compares harvest's
own modes. Entry 14, the 0.7.0 rerun, is the current one. The ledger links
its report without this page naming the engine. Each entry
carries its pre-registration, which fixed in advance what its numbers may not
be read to mean.

Two things from entry 14 bear on this page's own numbers:

* **Backlog depth decides the default mode's throughput on 0.7.0.** The
  Postgres claim path fell from 13.90 to 4.00 workflows/sec between a
  250-row and a 2,000-row backlog. The claim-path fix (#1971) holds it at
  21.1 to 22.1 at every depth. This page's closed loop keeps the backlog
  shallow on purpose, so it does not show the collapse.
* **The claim loop looks like the ceiling.** In every harvest run of entry
  14, the worker spent 91% to 99% of its wall time inside a claim. That fits
  one claim in flight at a time, with throughput set by the claim latency.
  This reading is post hoc, not a registered line.

### Results by release

Each release's numbers are kept, not overwritten:

* **0.6.0** — [`benchmarks/results-v0.6.0.md`](benchmarks/results-v0.6.0.md)
* **0.7.0** — [`benchmarks/results-v0.7.0.md`](benchmarks/results-v0.7.0.md)

## The configuration these numbers were taken at

Issue #941 asks for a documented worker/concurrency configuration, so it lives
here rather than only in the output of a run you have not done yet.
`benchmarks_docs.rs` pins these values to the harness constants.

| | | why |
|:--|--:|:--|
| workers per shard | 1 | adding a shard adds its worker — the deployment shape the shard sweep is a statement about |
| concurrent workflow tasks per worker | 8 | raising this to 24 on the four-core reference box *reduced* throughput; 72 concurrent tasks oversubscribe four cores |
| concurrent activity tasks per worker | 16 | two per workflow slot |
| connections per shard pool | 32 | at or above total worker concurrency, so no measurement includes pool-checkout queueing |
| workflows in flight per shard | 32 | four times the workflow slots — see [bounded load](#methodology) |
| paced start rate per shard | 8/sec | roughly 30% of the saturated rate, so the latency scenarios measure the path and not the queue |
| poll interval | 25 ms | with LISTEN/NOTIFY wired per shard, so this bounds a *missed* notification rather than every wake |
| durability | `fsync=off`, `synchronous_commit=off` | see [known limitations](#known-limitations) |

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
| `HARVEST_BENCH_INFLIGHT` | closed-loop in-flight workflows per shard — the load level. `throughput` only |
| `HARVEST_BENCH_WORKFLOWS` | measured completions per shard. `throughput` only |
| `HARVEST_BENCH_CHECK` | also print the reproduction verdict |
| `HARVEST_BENCH_KEEP` | leave the containers running afterwards |
| `HARVEST_BENCH_OUT` | write the report somewhere other than `benchmarks/results/` |

The two latency scenarios have no size knob; their populations are fixed so the
published percentiles always rest on the same sample count.

### Without docker compose

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
* The published numbers were taken on **native Postgres clusters**, provisioned
  with the same settings as the compose services but not through Docker. The
  0.6.0 box had no Docker daemon; the 0.7.0 sweep used native clusters too, so
  the two releases share one method. The compose path adds
  container networking and a container filesystem; **that delta has not been
  measured**, and this page does not claim to know its sign. Treat the ±15% band
  as applying to the native path, and treat a compose run's difference from it
  as unquantified rather than as a regression. Closing this needs one sweep on a
  Docker-capable machine of the same class.

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
rather than a confident-looking number, when it: collected too few samples;
failed to drain what it started; left a shard idle; could not hold its pace or
its in-flight target; saw a signal request fail; ran its warmup population into
the measured window instead of draining it first; produced far fewer dispatch
samples than dispatches that ran; re-dispatched a task row (which would make a
re-delivery delay indistinguishable from dispatch latency); saw a
host-to-database clock offset worth more than 2% of the p50 it would publish, or
one that drifted across the window; saw any workflow fail to observe its signal;
replayed a partial history; or ran out of its wall-clock budget. A negative
dispatch sample — which only clock skew can produce — voids the cell rather than
being clamped to zero.

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
* **The signal round-trip includes connection setup.** The stopwatch starts
  before `TcpStream::connect`, not after, so each sample carries a fresh
  loopback TCP handshake. That is pessimistic against a keep-alive client and it
  keeps the measured path identical for every sample — but it is not "the
  instant the request left the process".
* **The signal scenario has a residual pre-park window.** A workflow is
  considered ready when its handler reaches `wait_for_signal`; the suspension
  itself commits a moment later. The harness waits a further two seconds before
  the first signal, which is four orders of magnitude more than that window — but
  the window is not *closed*, and a signal that beat a suspension would be
  recorded as a shorter round trip than it was.
* **A crash's databases are swept on the next run, not the same one.** Databases
  are named uniquely per run and dropped on every ordinary exit, including the
  error paths. A panic or a Ctrl-C skips that: dropping a database is async, so
  the harness cannot run it from a `Drop`. Provisioning against an existing
  server (`HARVEST_TEST_DATABASE_URL` or `HARVEST_BENCH_SHARD_URLS`) sweeps for
  exactly this before creating its own databases: an advisory lock serializes
  the sweep against concurrent runs, only a name matching the full minted shape
  is ever a candidate, and a database still holding a live connection is left
  alone. A crash between runs is therefore self-healing; no manual cleanup is
  needed. The testcontainer path has nothing to sweep — the container itself is
  the whole server, and destroying it reclaims everything.
* **The throughput window does not verify every shard stayed loaded.** Each
  shard runs a fixed completion quota, and the sustained rate is taken over the
  middle half of all shards' completions pooled together. If one shard finishes
  its quota appreciably earlier than its peers, it can be idle for part of that
  window, and the published multi-shard rate is then partly a
  fewer-shards rate. Every current guard inspects a shard's *own* lifetime, so
  none of them catches it. On the reference runs the shards finish within a few
  seconds of each other, so the effect is small — but it is unmeasured, and
  closing it needs an aligned per-shard window. Tracked in
  [#1288](https://github.com/autumn-foundation/autumn-harvest/issues/1288).
* **A supplied URL must be able to `CREATE DATABASE`, and to `CONNECT` to the
  `postgres` database on that server.** `HARVEST_BENCH_SHARD_URLS` and
  `HARVEST_TEST_DATABASE_URL` are treated as **admin** URLs. The
  stale-database sweep and its advisory lock always connect through
  `postgres` specifically, regardless of which database the URL names, so
  that every client sweeping the same server agrees on where the lock lives.
  A role without either right produces a skip notice, not an error.

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

## Publishing a new release's numbers

The published baselines live as constants in the harness
(`PUBLISHED_BASELINES`), not only in Markdown, so the two cannot drift. Nothing
about a version bump forces a re-measurement: `PUBLISHED_RESULTS_VERSION` is
deliberately independent of the crate version, so a release that does not
re-measure keeps pointing at the last release that did.

To publish a fresh set:

1. Run the suite on an idle reference machine: `./benchmarks/run.sh`. It writes
   to `benchmarks/results/<timestamp>.md` (gitignored working output — not to be
   confused with `docs/benchmarks/`, which is the published, permanent copy).
2. Check the run's **noise control** section first. If the replay spread across
   the sweep is above 10%, the box was not quiet; re-run rather than publish.
3. Copy the report to `docs/benchmarks/results-v<version>.md`, adding the
   hardware block and the reading of the shard sweep.
4. Update `PUBLISHED_BASELINES` and `PUBLISHED_RESULTS_VERSION` in the harness,
   and the headline table above.
5. `cargo test -p autumn-harvest --test integration -- benchmarks_docs` will fail
   until all four agree.

## Where the code is

| | |
|:--|:--|
| Harness | `autumn-harvest/tests/integration/e2e_bench_support.rs` |
| Runner | `autumn-harvest/benches/e2e_bench.rs` |
| Topology | `benchmarks/docker-compose.yml` |
| One command | `benchmarks/run.sh` |
| Docs guard | `autumn-harvest/tests/integration/benchmarks_docs.rs` |
| Planning record | `docs/plans/2026-09-01-e2e-benchmark-suite.md` |
