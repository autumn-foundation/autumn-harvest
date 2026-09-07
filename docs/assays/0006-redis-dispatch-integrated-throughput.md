# ⛏️ Prospect: does integrated Redis dispatch clear the founding ">10,000 tasks/sec" line in a deployment-shaped run? (kill: 173.04 tasks/sec against a 10,000 line, ledger #6)

> Status: **measured.** The Pre-registration section below was committed
> (`46955f9`) before the apparatus was built or run; nothing in it has been
> edited since. The Apparatus, Assay, Verdict and Reproduce sections were
> appended afterward, in a follow-up commit, with the actual numbers.

## 🎯 Question

Ledger #1 and #2 measured `autumn-harvest-redis` standalone: no Postgres, no
worker, no completion write. Both reports named the same open pit: a
deployment-shaped assay "once the worker-integration refactor exists (real
worker pool, real transactional-boundary cost, real complete/ack in the
loop)". Issue #1312 builds that integration
(`docs/plans/2026-09-07-redis-dispatch-worker-integration.md`) and its third
acceptance criterion is this assay: it "gates whether this actually delivers
on the documented >10,000 tasks/sec claim once integration overhead is
included".

**Falsifiable question:** with the integrated path (a real `Worker` pool, real
Postgres claim-by-id and completion transactions, real Redis dispatch,
Postgres remaining the source of truth), does end-to-end task throughput
reach the founding line, and does the integrated path clear a decisive margin
over the same worker pool on the Postgres claim path?

**Decision this feeds:** whether `docs/autumn-workflow-architecture.md` §9.1
may present Redis dispatch as a working ">10,000 tasks/sec" escape hatch, or
must present it as a measured multiplier over the Postgres path with a lower
absolute ceiling on the reference machine. **Decider:** the owner of that
document and of `docs/plans/vantage-spec-redis-adapter.md`.

## ⚖️ Pre-registration

Copied verbatim from
[`docs/rnd/2026-09-07-redis-dispatch-integrated-throughput-preregistration.md`](../rnd/2026-09-07-redis-dispatch-integrated-throughput-preregistration.md)
(commit `46955f9`).

> One task is one `harvest_task_queue` row that reaches `COMPLETED`. A
> workflow with one activity produces three tasks: the workflow task that
> schedules the activity, the activity task, and the workflow task that
> completes the run. Throughput is completed tasks per second over the
> measured window, read from Postgres, never from the apparatus's own counters.
>
> - **L1 — the founding line.** Redis-dispatch arm, drain shape: **≥ 10,000
>   completed tasks/sec** over the window in which the seeded backlog drains
>   (window = first claim to last completion). Kill if below.
> - **L2 — the integration multiplier.** Redis-dispatch arm divided by the
>   Postgres-path control arm, same apparatus, same run, same seeded backlog:
>   **≥ 10x**. Kill if below.
> - **L3 — steady state does not collapse.** Paced-start shape at the rate the
>   drain arm sustained: dispatch latency p99 (row `created_at` to `started_at`)
>   **≤ 250 ms** for the Redis arm. Kill if above.
>
> **All three lines must clear for pursue; any one miss is kill.** The report
> records each line separately, so a kill on L1 with a pass on L2 is still
> informative for the decider: it says the escape hatch delivers a multiplier,
> not the founding absolute number, on this machine.
>
> **Correctness precondition, checked in both arms before any line counts:**
> every seeded workflow reaches `COMPLETED`, every activity side effect fires
> exactly once (a Postgres counter table, one row per activity run), and after
> the run the dispatch stream, its pending entries list and its marker keys are
> empty. A miss here voids the run.
>
> ## 🧪 Conditions
>
> - Reference machine: this session's 4 logical CPUs, 15 GB RAM. Postgres 16 on
>   loopback with `fsync=off`, `synchronous_commit=off` (matches
>   `docs/benchmarks.md`), Redis 7.0.15 on loopback with `save ""`,
>   `appendonly no`.
> - Pool shape, taken from the pinned deployment configuration in
>   `docs/benchmarks.md`: 4 in-process `Worker` instances, each with 8
>   workflow slots and 16 activity slots, one shared pool of 64 connections,
>   `poll_interval` 25 ms on the control arm, `dispatch.poll_interval` 20 ms
>   on the Redis arm.
> - Workload: one workflow type with one no-op activity and a ~40-byte input.
> - Drain shape: seed 10,000 workflows (30,000 tasks) with the workers
>   stopped, then start the pool and measure until the last completion.
> - Paced shape: start workflows at the rate the drain arm sustained, for 30 s,
>   and measure dispatch latency from `harvest_task_queue`.
> - Repetitions: 3 per arm per shape, alternating arms. The report shows every
>   run and grades the mean.
> - Control: the identical apparatus with no channel installed
>   (`dispatch::uninstall()`), so the workers use `claim_task_on_shard`.

## 🔍 Prior art

- [`docs/assays/0001-redis-adapter-throughput-ceiling.md`](0001-redis-adapter-throughput-ceiling.md)
  — the standalone ceiling; named this deployment-shaped follow-up as a shelf
  entry it could not build.
- [`docs/assays/0002-redis-matched-workload-vs-postgres.md`](0002-redis-matched-workload-vs-postgres.md)
  — the matched-workload multiplier (18,933 claims/s against 29/s), and the
  re-charter this assay discharges. Its apparatus conventions are reused here.
- [`docs/benchmarks.md`](../benchmarks.md) — the pool shape and the idle-box
  rule. Its published `throughput` headline is 23.73 workflows/sec for a
  three-activity workflow on one shard, which sets the scale this assay's
  numbers land in.
- [`docs/performance.md`](../performance.md) — the Postgres claim-path numbers
  that explain the size of the control arm's collapse.
- `autumn-harvest-redis/tests/worker_dispatch_e2e.rs` — read directly for the
  worker construction, the handler registry shape, the side-effect table and
  the Redis drain probe. The apparatus reuses all four.
- `autumn-harvest/benches/e2e_bench.rs` and
  `autumn-harvest/tests/integration/e2e_bench_support.rs` — read for the
  dispatch-latency definition and the paced-start discipline.

## 🧪 Apparatus

A standalone Rust binary
(`docs/assays/apparatus/0006-redis-dispatch-integrated/`, archived, **not a
workspace member, never build against it**) with path dependencies on the real
`autumn-harvest` (features `db`, `testing`) and `autumn-harvest-redis` crates.
It calls public APIs only. Zero lines of any workspace crate were touched.

Both arms are the same binary, the same pool shape and the same workload. The
only difference is the channel:

- **Redis arm.** `RedisDispatch::connect` under a per-run key prefix, installed
  through `autumn_harvest::dispatch::install` with
  `DispatchSettings { poll_interval: 20 ms, reconcile_interval: 1 s,
  ..Default::default() }`, before the workers start.
- **Control arm.** `autumn_harvest::dispatch::uninstall()`, so `Worker::run`
  takes the `poll_once` Postgres claim path.

Per run the binary drops and recreates its own database (`assay6`), applies
`autumn_harvest::test_init_sql()`, and creates an unlogged
`assay6_side_effects` table that takes one row per activity body. It then
builds one shared `DbPool` of 64 connections, starts four `Worker` instances
with distinct worker ids on one queue, each with
`max_concurrent_workflows = 8` and `max_concurrent_activities = 16`, and
`poll_interval = 25 ms`. The registry holds one workflow type with one
activity. The workflow input is exactly 40 bytes.

**Drain shape.** Seed 10,000 starts through the public
`autumn_harvest::start_or_load_workflow_execution` API on 16 parallel
connections, with the workers stopped. Start the pool. Poll Postgres until
every seeded execution is `COMPLETED`, or until the arm's cap. Throughput is
then read from Postgres alone:

```sql
SELECT count(*), EXTRACT(EPOCH FROM (max(completed_at) - min(started_at)))
FROM harvest_task_queue WHERE state = 'COMPLETED'
```

**Paced shape.** Start the pool first, then start workflows at the rate the
drain arm sustained for 30 s, on a 10 ms tick that carries the fractional
remainder forward. Wait for the tail to finish, then read the latency
population from Postgres.

**The latency definition, and why it differs from the benchmark's.** The line
is written as "row `created_at` to `started_at`". The apparatus measures
`started_at - GREATEST(COALESCE(created_at, scheduled_at), scheduled_at)`.
That expression is not an invention: it is the engine's own eligibility
floor, documented at `autumn-harvest/src/queue.rs:44` for the
schedule-to-start SLI, and `created_at` is nullable for pre-upgrade rows.
`benches/e2e_bench.rs` uses a different pair — the row's `created_at` against
an *in-process* observation of the activity handler's start — because it also
wants the handler dispatch cost. Both columns here come from Postgres, which
is what the pre-registration asks for. The percentiles are computed in
Postgres with `percentile_cont`. The report gives the activity-task
population, matching what the benchmark samples, and the whole-task
population beside it.

**Stubs list (what was faked or skipped, and why it matters for reading the
number):**

- Loopback only. One Postgres, one Redis, no replication, no cluster mode, no
  failover, no TLS, no auth. A real deployment's Redis is a separate host, and
  the network hop is latency this number never pays.
- Postgres durability relaxed (`fsync=off`, `synchronous_commit=off`), as the
  benchmark suite does. A durable deployment pays commit costs this number
  never pays.
- Redis persistence disabled (`save ""`, `appendonly no`).
- Four workers in one process, not four processes. They share one tokio
  runtime and one connection pool.
- One queue, one workflow type, one no-op activity, one fixed 40-byte input.
  No priority spread, no retries, no signals, no timers, no child workflows.
- LISTEN/NOTIFY is off on both arms (`notification_database_url` unset), so
  the control arm polls. This makes the two arms differ in exactly one thing,
  the channel, but it does deny the control arm a wake path a real deployment
  may configure.
- The activity body writes one row to a counter table through a second pool of
  16 connections. That write is the pre-registration's correctness counter. It
  is charged to both arms equally, so it cannot bias the comparison, but it is
  not free and a genuinely empty activity would run faster.
- The control arm's drain runs are capped and truncated (see the Assay).
- Single shard. Redis dispatch v1 rejects sharded runtimes, so no shard sweep
  is possible.
- No warmup trimming. Every sample of every window is reported.

**One apparatus defect found and fixed before the reported paced runs.** The
paced loop first computed its per-tick batch as `(rate / 100.0).max(1.0)`. The
floor made every rate below 100 workflows/s run at exactly 100 workflows/s, so
the first paced sweep held 97 workflows/s against an 86.52 target. The floor
was removed; the fractional remainder accumulator already guaranteed progress.
The paced numbers in the Assay are all from the corrected apparatus, which
held 84.44-85.05 workflows/s against the 86.52 target. The correction lowered
the Redis arm's p99 (from a 1,738-2,231 ms range to a 404-463 ms range) and did
not change the verdict on L3.

## 📊 Assay

The box was idle before each sweep. `uptime` reported a one-minute load
average of 2.25 before the drain sweep and 5.95 before the paced sweep, both
falling from the previous sweep's own load, on 4 logical CPUs. Postgres
reported `fsync = off` and `synchronous_commit = off`; Redis reported
7.0.15 with `save ""` and `appendonly no`. `nproc` reported 4 and `free`
reported 15 GB. Every binary was built with `--release`.

### Task count per workflow: two rows, not three

The pre-registration expects three completed `harvest_task_queue` rows per
workflow. The engine produces **two**. The workflow task row is parked and
re-pended for its second turn, so one row serves both the turn that schedules
the activity and the turn that completes the run. Verified directly on a
500-workflow run:

```
 task_type |   state   | count
-----------+-----------+-------
 activity  | COMPLETED |   500
 workflow  | COMPLETED |   500
```

The registered *definition* — "one task is one `harvest_task_queue` row that
reaches `COMPLETED`" — is unchanged and is what every number below counts. Only
the pre-registration's own arithmetic ("10,000 workflows (30,000 tasks)") was
wrong; the seeded backlog is 20,000 completable rows, not 30,000. Grading L1
against a 20,000-row backlog is *harder* than against a 30,000-row one, since
the same window must carry two thirds as many completions, so this correction
does not soften the kill.

### Drain shape, 10,000 workflows, registered configuration

Three repetitions per arm, alternating `redis, control, redis, control, ...`,
each on a freshly created database. The control arm is capped at 180 s per
run (see the note below the table).

| arm | rep | seeded | completed execs | completed task rows | side-effect rows | window s | **tasks/s** | workflows/s | correctness |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|:--|
| redis | 1 | 10,000 | 10,000 | 20,000 | 10,000 | 117.408 | **170.35** | 85.17 | pass |
| control | 1 | 10,000 | 0 | 0 | 0 | n/a | **0.00** | 0.00 | ⚠ truncated |
| redis | 2 | 10,000 | 10,000 | 20,000 | 10,000 | 115.986 | **172.43** | 86.22 | pass |
| control | 2 | 10,000 | 0 | 0 | 0 | n/a | **0.00** | 0.00 | ⚠ truncated |
| redis | 3 | 10,000 | 10,000 | 20,000 | 10,000 | 113.422 | **176.33** | 88.17 | pass |
| control | 3 | 10,000 | 0 | 0 | 0 | n/a | **0.00** | 0.00 | ⚠ truncated |

Redis arm mean **173.04 completed tasks/s** (range 170.35-176.33, spread 3.46%
of the mean) and **86.52 completed workflows/s**. Every Redis run passed the
correctness precondition in full: 10,000 of 10,000 executions `COMPLETED`,
10,000 side-effect rows for 10,000 activity runs, `XLEN` 0, `XPENDING` 0 and
zero `*:dispatch:marker:*` keys at the end, `dropped_hints` 0 and zero
side-effect write errors.

**The control arm completed nothing.** Not one `harvest_task_queue` row
reached `COMPLETED` in any of the three 180 s runs. This is not an artifact of
the cap. Two earlier runs of the same arm at the same configuration, kept here
because they bound the cap question, completed zero rows in **180 s** and zero
rows in **600 s**. In the 600 s run the arm did make progress on the backlog —
6,148 workflow task rows claimed and parked, 6,147 activity rows still
`PENDING`, zero activity rows ever `RUNNING` — so it was working the whole
time and finishing nothing.

### Why the control arm collapses: the metrics samplers, not the claim path

The collapse is larger than `docs/performance.md`'s claim-path numbers
predict, and the cause is a second one this assay found by inspection while it
ran. `pg_stat_activity` during a control run was dominated by a query no part
of this workload issues:

```sql
SELECT wf.workflow_name, COUNT(*)::bigint AS oversized_count
FROM harvest_workflow_executions wf
WHERE wf.state IN ('RUNNING', 'SUSPENDED')
  AND (SELECT COUNT(*) FROM harvest_events he WHERE he.workflow_exec_id = wf.id) > $1
GROUP BY wf.workflow_name
```

That is `sample_history_oversized_counts` in `autumn-harvest/src/worker.rs`.
`Worker::run` spawns it, and seven sibling samplers, on `self.config.poll_interval`
— 25 ms here, so 40 passes per second per worker, 160 across the pool. Each
pass runs a correlated subquery over every `RUNNING` execution, and at this
backlog depth that is 10,000 of them. Four samplers issue their SQL with **no
metrics-enabled guard at all**:

| sampler | guarded by `telemetry.metrics.is_enabled()` |
|:--|:--|
| `spawn_queue_depth_sampler` | yes, inside the task |
| `spawn_queue_pause_sampler` | yes, inside the task |
| `spawn_schedule_overdue_sampler` | yes, inside the task |
| `spawn_workflow_active_sampler` | yes, inside the task |
| `spawn_worker_slot_sampler` | yes, at the call site |
| `spawn_stranded_work_sampler` | yes, at the call site |
| `spawn_concurrency_sampler` | **no** |
| `spawn_rate_limit_sampler` | **no** |
| `spawn_dlq_depth_sampler` | **no** |
| `spawn_history_oversized_sampler` | **no** |

`spawn_queue_depth_sampler` carries the comment that names the rule the other
four break: "No recorder configured: never issue the sampler SQL. The
per-event `record_*` calls are zero-cost, but these gauge-feeding queries are
not." This apparatus registers `NoOpMetrics`, so every row those four queries
produce is discarded. The engine pays for them anyway, on both arms, forty
times a second per worker.

A diagnostic pair isolates the cost. It is **post hoc and not pre-registered**,
and it is reported only to separate the two effects. It raises
`WorkerRuntimeConfig.poll_interval` from 25 ms to 1,000 ms, which cuts the
sampler rate forty-fold and leaves the claim cadence almost untouched (under a
non-empty backlog the control arm's `poll_once` returns work on every
iteration and never reaches its idle sleep, and on the Redis arm the channel
read and the maintenance pass are driven by `DispatchSettings.poll_interval`,
not by this field). One run per arm, same 10,000-workflow backlog:

| arm | poll_interval | completed execs | completed task rows | window s | tasks/s |
|:--|--:|--:|--:|--:|--:|
| redis | 1,000 ms | 10,000 | 20,000 | 71.473 | **279.83** |
| control | 1,000 ms | 0 | 7,589 | 290.922 | **26.09** ⚠ truncated at 600 s |

With the samplers quieted the Redis arm goes from 173.04 to 279.83 tasks/s —
the samplers were costing it 38% — and the control arm goes from completing
nothing at all to 26.09 tasks/s. The control arm still completed zero
*executions* in 600 s: it drained 7,589 activity rows while their parent
workflow tasks, re-pended with later `scheduled_at` values, sat behind the
whole remaining backlog in `(priority DESC, scheduled_at ASC)` order.

### Paced shape, 86.52 workflows/s for 30 s

The pace is the Redis drain arm's own sustained rate, 86.52 workflows/s. The
control arm has no sustained rate of its own — its drain completed no task row
— so it holds the Redis arm's rate. Both arms therefore face the same offered
load, which is also what makes the two latency columns comparable.

| arm | rep | target wf/s | started | achieved wf/s | n | activity p50 ms | **activity p99 ms** | all-task p50 ms | all-task p99 ms | negative samples |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| redis | 1 | 86.52 | 2,595 | 84.56 | 2,595 | 6.868 | **462.542** | 6.396 | 463.168 | 0 |
| control | 1 | 86.52 | 2,595 | 84.90 | 2,595 | 9.262 | 190.962 | 9.723 | 191.812 | 169 |
| redis | 2 | 86.52 | 2,595 | 85.03 | 2,595 | 6.532 | **403.572** | 6.156 | 411.234 | 0 |
| control | 2 | 86.52 | 2,595 | 84.44 | 2,595 | 10.118 | 126.859 | 10.664 | 143.591 | 147 |
| redis | 3 | 86.52 | 2,595 | 85.05 | 2,595 | 6.257 | **414.751** | 5.845 | 421.680 | 0 |
| control | 3 | 86.52 | 2,595 | 84.60 | 2,595 | 10.171 | 120.443 | 11.028 | 123.110 | 125 |

Redis arm mean **p99 426.96 ms** on the activity population (432.03 ms on all
task rows) and mean p50 6.55 ms. Control arm mean p99 146.09 ms and mean p50
9.85 ms. Every paced run of both arms passed the correctness precondition:
2,595 of 2,595 executions `COMPLETED`, 2,595 side-effect rows, and for the
Redis arm an empty stream, an empty pending entries list and no marker keys.

Two soundness notes on that table. **The negative samples are a clock
artifact, and they are on the control arm only.** 125-169 of 2,595 control
samples (4.8-6.5%) have `started_at` earlier than the row's own eligibility
floor. `EnqueueParams::new` backdates an immediate row's `scheduled_at` with
the *host* clock while `started_at` is stamped with Postgres `NOW()`, and the
engine's own SLI clamps the difference to zero for exactly this reason. This
apparatus does not clamp, so the control arm's percentiles are, if anything,
slightly optimistic. The Redis arm has zero negative samples, which is what
the publish-after-commit rule predicts: a reference cannot be read before the
row that raised it is durable. **The Redis arm is slower here than the control
arm.** That is not a contradiction of the drain result. At 86.52 workflows/s
against an emptying queue the backlog never gets deep, so the Postgres claim
scan stays cheap and the channel is paying a Redis round trip the control arm
does not pay. It is the same shape ledger #2 predicted and
`docs/operations/redis-dispatch.md` already tells operators: leave the channel
off when the backlog is shallow.

## 🏁 Verdict

**KILL.** Two of the three registered lines miss.

**L1 — the founding line: KILL, by 57.8x.** The Redis-dispatch arm sustained a
mean **173.04 completed tasks/sec** across three drain runs (range
170.35-176.33) against a **≥ 10,000 tasks/sec** line. This is not a borderline
call decided by which run you pick: the *best* of the three runs (176.33)
misses the line by 56.7x, and the best number this assay produced under any
configuration, including the non-registered sampler-quieted diagnostic
(279.83), still misses it by 35.7x. The founding ">10,000 tasks/sec" figure is
not reachable by the integrated path on the reference machine, and the gap is
not the sort a tuning pass closes.

The number is not surprising in context, which is itself worth saying.
`docs/benchmarks.md` publishes 23.73 workflows/sec for a three-activity
workflow on one shard on the same class of machine. This assay's one-activity
workflow at 86.52 workflows/sec is faster per workflow and in the same order
of magnitude. Ledger #2's 18,933 claims/sec was a measurement of
`RedisTaskQueue::claim` in isolation with no Postgres write behind it. The two
orders of magnitude between that number and this one are what "integration
overhead" turned out to mean: a Postgres claim-by-id transaction, a completion
transaction, the history writes, the activity body, and — this assay's own
finding — a set of metrics samplers firing forty times a second per worker.

**L2 — the integration multiplier: PASS, but the honest statement of it is
awkward.** At the registered configuration the control arm completed **zero**
`harvest_task_queue` rows in 180 s, in three runs, and zero in a 600 s run and
a further 180 s run kept from the exploratory pass. The quotient the line asks
for has a zero denominator. Two bounded readings, both of which clear 10x:

- Read the zero as "fewer than one row in 180 s", the most generous possible
  reading of it, and the multiplier is greater than **31,000x**.
- Take the only finite pair this assay measured, the sampler-quieted
  diagnostic, and it is **279.83 / 26.09 = 10.72x** — clearing the line, but
  only just, and from one run per arm rather than three.

The line is graded **pass**, on the first reading, with the second recorded so
a reader can see how thin the margin becomes once the sampler defect stops
flattering it. The honest summary for the decider is that Redis dispatch is
the difference between draining a 10,000-workflow backlog in under two minutes
and not draining it at all, and that the size of that difference at the
registered configuration is set as much by the sampler defect as by the claim
path.

**L3 — steady state does not collapse: KILL, by 1.71x.** Paced at the drain
arm's own 86.52 workflows/s, the Redis arm's dispatch-latency p99 was
**426.96 ms** on the mean of three runs (462.542, 403.572, 414.751) against a
**≤ 250 ms** line. Every individual run misses. The p50 is 6.55 ms, so the
miss is entirely in the tail: the median reference is served in under 7 ms and
the worst hundredth is served two orders of magnitude later. The control arm,
which is not gated by this line, held 146.09 ms at the same pace, so the tail
is a property of the channel path under this load and not of the machine.

**The pre-registered rule is "all three lines must clear for pursue; any one
miss is kill."** Two miss. The verdict is kill.

**What this kill is, precisely, and what it is not.** It is a kill on the
*absolute claim* and on the *tail*, on this machine, at this pool shape. It is
not a finding that the integration does not work, and it is not a reason to
revert it. The correctness precondition passed in every single run of both
shapes and both arms: 10,000 of 10,000 executions completed, side-effect rows
exactly equal to the workflow count with no double fires and no misses, and an
empty stream, empty pending entries list and no leaked marker keys at the end
of every Redis run. The engineering is sound; the number the docs claimed for
it is not.

**For the named decision.** `docs/autumn-workflow-architecture.md` §9.1 may
**not** present Redis dispatch as a working ">10,000 tasks/sec" escape hatch.
It must say that the integrated path sustained 173 completed tasks/sec
draining a 10,000-workflow backlog on the four-core reference machine, where
the same worker pool on the Postgres claim path completed nothing in the same
window, and that the founding absolute figure is not reached.

**Re-charter, not close.** Four pits are named, none of them run:

1. **The unguarded samplers.** Four of ten metrics samplers issue SQL at
   `poll_interval` with no metrics-enabled guard, and the correlated-subquery
   scan in `sample_history_oversized_counts` costs the Redis arm 38% of its
   throughput at a 10,000-execution population — and costs the Postgres arm
   the entire run. Adding the guard their six siblings already have is a small
   change with a large measured payoff. It deserves its own issue and its own
   re-measurement, not a footnote here. This assay did not make the change:
   its charter is to measure, and the pre-registration pins the apparatus to
   public APIs and zero crate edits.
2. **The re-pended workflow task's place in the claim order.** The control
   arm drained 7,589 activity rows while completing zero executions, because a
   re-pended workflow task carries a later `scheduled_at` than every row still
   in the backlog and so sorts behind all of them. Whether a deep backlog
   should be able to starve in-flight runs to completion this way is a
   scheduling question this assay only stumbled over.
3. **The Redis arm's p99 tail.** The p50 is 6.55 ms and the p99 is 427 ms. The
   reconcile sweep, the release backoff and the visibility-timeout recovery are
   all plausible sources and none was instrumented here.
4. **A machine with more than four cores.** Every number here is CPU-bound on
   a four-core box with Postgres, Redis and four workers on it. Whether the
   multiplier or the absolute ceiling scales is untested.

## 🔬 Reproduce

```bash
# Postgres 16 with the benchmark suite's durability settings, and Redis 7 with
# no persistence, both on loopback.
psql -h 127.0.0.1 -U postgres -c "show fsync"              # off
psql -h 127.0.0.1 -U postgres -c "show synchronous_commit" # off
redis-server --daemonize yes --port 6379 --save "" --appendonly no
psql -h 127.0.0.1 -U postgres -c "create database assay6"

# Check the box is idle first, as docs/benchmarks.md requires.
uptime

# Build and run the archived apparatus. Never add it to the workspace
# Cargo.toml.
export CARGO_TARGET_DIR="$PWD/target"
cd docs/assays/apparatus/0006-redis-dispatch-integrated
cargo build --release

# The drain sweep: 3 repetitions per arm, alternating, 10,000 workflows each,
# the control arm capped at 180 s. ~22 minutes.
ASSAY6_SHAPES=drain ASSAY6_REPS=3 ASSAY6_WORKFLOWS=10000 \
ASSAY6_DRAIN_CAP_SECS=420 ASSAY6_CONTROL_CAP_SECS=180 \
  cargo run --release

# The paced sweep at the drain arm's measured rate (86.52 workflows/s, given
# in thousandths). ~3 minutes.
ASSAY6_SHAPES=paced ASSAY6_REPS=3 ASSAY6_PACED_SECS=30 \
ASSAY6_PACED_RATE_MILLI=86520 ASSAY6_CONTROL_CAP_SECS=180 \
  cargo run --release

# The post-hoc sampler diagnostic (not pre-registered): the same drain, with
# the worker poll interval raised from 25 ms to 1,000 ms.
ASSAY6_SHAPES=drain ASSAY6_REPS=1 ASSAY6_WORKER_POLL_MS=1000 \
ASSAY6_DRAIN_CAP_SECS=420 ASSAY6_CONTROL_CAP_SECS=600 \
  cargo run --release
```

Each run prints one `ASSAY6 ...` line with every field the tables above use,
then a summary table. The binary drops and recreates `assay6` before every
run, and deletes only the Redis keys under its own per-run prefix. It never
calls `FLUSHALL`.

See
[`docs/assays/apparatus/0006-redis-dispatch-integrated/README.md`](apparatus/0006-redis-dispatch-integrated/README.md)
for the full list of environment knobs.
