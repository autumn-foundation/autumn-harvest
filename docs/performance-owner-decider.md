# Owner decider: claims, throughput and fence cost

Issue #2255, stage 1b of epic #2247. In owner mode, an owner decider takes
the workflow tasks of its partitions by execution. The claim loops claim
activities only. [ADR 0009](adr/0009-decision-owner.md) decision 13 gives
the design. This page measures the four done-when items of the issue.

| Done-when item | Result |
|---|---|
| Claims per 3-activity workflow drop from 7 to 3 on the assay 0014 shape. | **Pass.** 3.00 task claims per workflow at 1, 2 and 4 claim loops. The 4 workflow tasks are owner takes. |
| Workflows/s is at least 1.5x at equal pool connections. | **Not met on this host.** 1.27x at 1 claim loop, 1.19x at 2 and 1.05x at 4. The host runs out of CPU first. See [the throughput section](#2-throughput). |
| A red test passes: a signal acknowledged just before an epoch bump is never lost. | **Pass.** Two cases, below. |
| The double-fence persist cost stays under 10%. | **Pass.** With DR on, an owner decision now persists 9.3% faster than a DR-only decision (median of six runs). Every run stays under +2.7%. |

## The workload and the host

The shape is the harvest `postgres` arm of assay #14, as on
[`performance-decision-owner-baseline.md`](performance-decision-owner-baseline.md).
One worker, 8 workflow slots, 16 activity slots, 32 pool connections and a
25 ms poll. The workflow runs 3 activities in sequence, so it makes 4
decisions. A backlog of 500, 1,000 or 2,000 workflows is seeded first, then
drained. The claim cap is 1, 2 or 4 loops. Legacy runs owner mode off.
Owner runs owner mode on, so the owner decider takes the workflow tasks.

[`apparatus.patch`](perf-artifacts/owner-decider/apparatus.patch) holds the
patch of issue #2248. It adds `ASSAY14_OWNER_MODE=1`, which turns owner mode
on after the schema, and it records each owner take by kind. The clock
starts when the worker starts, so owner mode pays for its acquisitions.

Host: 4 logical CPUs, 15 GB. PostgreSQL 16.15 on the same host with
`fsync=off`, `synchronous_commit=off` and `max_connections=400`. Three
rounds on 2026-10-23, 04:49 to 05:14 UTC. Each round runs both modes at each
cap, and odd rounds run owner mode first. All 54 runs are valid. Raw output:
[`results/raw/`](perf-artifacts/owner-decider/results/raw/). Graded:
[`results/graded.md`](perf-artifacts/owner-decider/results/graded.md).

Only ratios measured on one box compare. This box ran legacy faster than
the baseline page did, so compare each mode with the legacy runs here.

## 1. Claims per workflow

Per workflow, over all depths:

| mode | claim loops | workflow claims | activity claims | empty claims | owner takes | empty takes |
|:--|--:|--:|--:|--:|--:|--:|
| legacy | 1 | 4.00 | 3.00 | 0.00 | 0.00 | 0.00 |
| legacy | 2 | 4.00 | 3.00 | 0.01 | 0.00 | 0.00 |
| legacy | 4 | 4.00 | 3.00 | 0.55 | 0.00 | 0.00 |
| owner | 1 | 0.00 | 3.00 | 0.00 | 4.00 | 0.09 |
| owner | 2 | 0.00 | 3.00 | 0.01 | 4.00 | 0.05 |
| owner | 4 | 0.00 | 3.00 | 0.55 | 4.00 | 0.00 |

- **The claim count drops from 7 to 3.** The claim loops claim the 3
  activity tasks. The owner decider takes the 4 workflow tasks.
- **A take is not a head scan.** It is the owned claim keyed by the
  execution, so every gate still applies. It costs the same round trips as
  a claim, but no claim loop waits for it.
- **Empty takes are rare.** 0.05 to 0.09 per workflow. Most come from a wake
  that arrives while the decision of that run still holds its row.
- `an_owner_takes_workflow_tasks_off_the_claim_path` pins the count: 3n
  activity claims, 0 workflow claims and 4n takes for n runs.

## 2. Throughput

Completed workflows per second, the mean of three runs:

| depth | claim loops | legacy | owner | owner / legacy |
|--:|--:|--:|--:|--:|
| 500 | 1 | 35.64 | 45.55 | 1.28x |
| 1,000 | 1 | 34.38 | 43.59 | 1.27x |
| 2,000 | 1 | 32.65 | 41.20 | 1.26x |
| 500 | 2 | 46.55 | 55.25 | 1.19x |
| 1,000 | 2 | 45.44 | 54.84 | 1.21x |
| 2,000 | 2 | 43.84 | 51.75 | 1.18x |
| 500 | 4 | 49.61 | 52.89 | 1.07x |
| 1,000 | 4 | 50.52 | 53.79 | 1.06x |
| 2,000 | 4 | 50.04 | 51.20 | 1.02x |

Over all depths: 1.27x at 1 loop, 1.19x at 2 and 1.05x at 4.

**The 1.5x item is not met here.** The issue derived 1.5x from the
claim-loop ceiling, `1 / (claims × claim latency)`. On this host the claim
loop stops being the limit, and the CPU becomes the limit:

- The one-minute load reads 6 to 7 during the owner runs, on 4 CPUs.
  Postgres uses about 250% of the 400%.
- More decisions run at once, so every statement slows. At 2 loops, the
  activity claim rises from 5.96 ms to 11.41 ms. The persist rises from
  5.7 ms to 12.0 ms. Pool use rises from 6.5 to 14.5 connections.
- At 1 loop, the owner claim loop serves 3 activity claims of 7.2 ms each.
  That caps it at about 46 workflows/s, and it ran at 43.5.
- At 4 loops, both modes reach about 51 to 54 workflows/s. That is the CPU
  ceiling of this host for this shape.

`pg_stat_statements` for one run of each mode at 2 loops and depth 1,000
([legacy](perf-artifacts/owner-decider/results/pg-stat-statements-legacy-claims2-d1000.txt),
[owner](perf-artifacts/owner-decider/results/pg-stat-statements-owner-claims2-d1000.txt))
shows the same work in both modes:

- About 272,000 to 280,000 statements, seed included.
- Owner mode runs 22.6 s of server time against 18.7 s. The extra time is
  contention: the same statements run slower.
- A take costs 1.06 ms of server time per call, and a claim 1.05 ms. On a
  warm prepared plan, a take runs in about 0.5 ms.

What can close the gap, each as its own change:

- A host with the database apart from the worker, so the claim loop stays
  the limit. The formula then predicts more than 2x at 1 loop.
- A cheaper take. Each take still runs the full claim statement and its
  rechecks.
- Eager activity dispatch, B5 of `docs/design/DESIGN-claim-concurrency.md`. A decision
  then claims its own new activity rows, and the claim count falls below 3.
- Group commit across executions of one partition. Issue #2256 builds the
  lane. [`performance-group-commit.md`](performance-group-commit.md)
  measures it. Issue #2318 wires the lane into the worker.

## 3. The fence cost

[`benches/decision_fence_bench.rs`](../autumn-harvest/benches/decision_fence_bench.rs)
times a model decision persist, as on the baseline page. One transaction
holds the decision fence, two appends, one activity task enqueue and an
execution update. The modes that this page reads:

| mode | statements that lock a fence row |
|:--|:--|
| `generation` | DR only: the generation row at each append, in a savepoint |
| `owner` | owner mode only: the owner row, once, at the start |
| `generation_owner` | both, as issue #2255 runs them: one fused statement at the start, and no check at an append |
| `generation_owner_split` | both, as the code before issue #2255 ran them: two statements at the start, then the generation row at each append |

Six runs on 2026-10-23, one writer, each in another mode order. Raw output:
[`results/fence-bench/`](perf-artifacts/owner-decider/results/fence-bench/).
Graded:
[`results/fence-graded.md`](perf-artifacts/owner-decider/results/fence-graded.md).

| run | `generation` ms | `owner` ms | `generation_owner` ms | `generation_owner_split` ms | fused vs `generation` | fused vs split | fused vs `owner` |
|:--|--:|--:|--:|--:|--:|--:|--:|
| r0 | 3.99 | 3.63 | 3.78 | 5.53 | −5.3% | −31.7% | +4.1% |
| r1 | 4.54 | 3.61 | 3.86 | 5.66 | −14.9% | −31.7% | +7.0% |
| r2 | 3.92 | 4.08 | 3.86 | 5.73 | −1.7% | −32.7% | −5.3% |
| r3 | 4.26 | 3.67 | 3.69 | 5.48 | −13.3% | −32.7% | +0.7% |
| r4 | 4.11 | 3.45 | 3.51 | 5.27 | −14.6% | −33.5% | +1.6% |
| r5 | 3.87 | 4.17 | 3.97 | 6.29 | +2.7% | −36.9% | −4.7% |
| **median** | | | | | **−9.3%** | **−32.7%** | **+1.1%** |

**The pass condition holds.** The baseline page compared owner mode with DR
on against `generation`, and read +17.8%. The same comparison now reads
−9.3%, and no run reaches +10%:

- **The fused statement removes one round trip.** The generation row and the
  owner row lock in one call, in the order of ADR 0009 decision 6.
- **The appends check nothing again.** The start fence holds both rows until
  the commit, so each append skips its savepoint and its generation check.
  `generation` still pays those at each append, so owner mode now sends
  fewer statements than DR alone.
- **The second row itself costs little.** Against `owner`, which locks the
  owner row alone, the fused statement adds a median of 1.1%. The largest
  run adds 7.0%.
- The criterion interval of each case is within ±5% of its estimate. A mode
  moves by up to ±15% between runs, so read single runs with care.

`a_dr_fenced_owner_decision_sends_one_fence_statement` pins the statement
count: one fused call and one generation read per decision with two
appends.

## 4. Signals across an epoch bump

The decider holds execution ids only, as hints. A signal is a
`harvest_signals` row before `send_signal` returns. A new owner scans each
partition that it acquires. Two tests drive the case of restate#4566:

- `a_signal_acknowledged_just_before_an_epoch_bump_is_never_lost`: no owner
  listens when the signal commits. A new owner acquires at the next epoch
  and delivers it once. Before the decider, the run decided through a
  workflow claim, so the test failed.
- `a_signal_hinted_to_an_owner_that_loses_its_epoch_is_never_lost`: the live
  owner gets the wake, and its take misses. The owner row then moves to
  another holder at the next epoch. The next owner delivers the signal once.
- `a_due_timer_fires_through_the_owner_scan` and
  `the_owner_scan_reads_owned_and_unstamped_due_rows_only` cover the scan.

## What the measurement does not establish

- One box, one shape and one Postgres version. The worker and Postgres share
  4 CPUs, so the 2 and 4 loop cells are CPU-bound.
- The sweep runs one worker. With several owners, each scan reads only its
  own partitions. This page does not measure that.
- `fsync=off` hides the commit cost of a durable server.
- The clock includes the acquisition of 512 partitions at the start.
- The sweep predates issues #2296 and #2308. Issue #2296 takes each
  acquisition chunk in one transaction, not one transaction per partition.
  Issue #2308 adds a batched key lookup when free permits cannot cover
  every hint. A seeded backlog keeps the 8 workflow slots full, so this
  shape runs that lookup.

## Reproduce

See [`perf-artifacts/owner-decider/README.md`](perf-artifacts/owner-decider/README.md).
