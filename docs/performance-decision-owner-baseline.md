# Decision-owner baseline: claim split, persist phases and fence cost

Issue #2248, stage 1 of epic #2247. The case for a decision owner rests on
the formula `1 / (claims × claim latency)`. This page measures the three
inputs that the formula did not have:

1. **The claim split.** Workflow and activity tasks share one table and one
   claim statement. This page times each kind apart.
2. **The persist phases.** It splits the decision persist into its fence,
   event insert, task insert, notify, commit and the rest.
3. **The fence cost.** Owner mode adds a second locked row to each decision.
   A criterion bench times the persist with zero, one and two fence rows.

**Result in one line each:**

- A workflow claim costs about what an activity claim costs: 4.33 ms
  against 4.07 ms at one claim loop. Workflow claims take 59% of the claim
  time, because a workflow makes 4 of them and 3 activity claims.
- The decision persist takes 4.14 ms at one claim loop. Event inserts take
  14%, task inserts 14%, the commit 6%, the notify stage 0%. The other 67%
  is the rest of the transaction.
- **The second fence row fails the stage 1b pass condition.** Owner mode
  with DR on adds two statements to each decision: a second check of the
  generation row and the owner fence. Together they add 20% to the persist
  at one writer. The condition is under 10%.

## The workload and the host

The claim split and the phases come from the harvest `postgres` arm of
assay #14, the same shape as the
[claim-concurrency](performance.md#claim-concurrency) measurement. One
worker, 8 workflow slots, 16 activity slots, 32 connections, 25 ms poll.
The workflow runs 3 activities in sequence, so it makes 4 decisions. A
backlog of 250, 500, 1,000 or 2,000 workflows is seeded first, then
drained. The claim cap is 1, 2 or 4 loops.

[`apparatus.patch`](perf-artifacts/decision-owner-baseline/apparatus.patch)
adds three things to the assay #14 apparatus: the claim cap from
`ASSAY14_MAX_CONCURRENT_CLAIMS`, each claim by kind, and each persist phase.
The new `kind` label and the new phase metric of this issue feed them.

Host: 4 logical CPUs (Intel Xeon @ 2.10 GHz), 15 GB. PostgreSQL 16.15 with
`fsync=off`, `synchronous_commit=off` and `max_connections=300`. Three
rounds on 2026-10-22, 19:14 to 19:30 UTC. All 36 runs are valid. Raw
output: [`results/raw/`](perf-artifacts/decision-owner-baseline/results/raw/).
Graded: [`results/graded.md`](perf-artifacts/decision-owner-baseline/results/graded.md).

Only ratios measured on one box compare. Do not compare the absolute
numbers with another page.

## 1. The claim split

Completed workflows per second, the mean of three runs:

| depth | 1 loop | 2 loops | 4 loops |
|--:|--:|--:|--:|
| 250 | 31.40 | 40.72 | 43.56 |
| 500 | 31.27 | 41.65 | 44.27 |
| 1,000 | 32.26 | 40.33 | 45.99 |
| 2,000 | 31.29 | 41.21 | 47.44 |

Claims per workflow and the mean time of each kind, over all depths:

| claim loops | workflow claims | workflow ms | activity claims | activity ms | empty claims | empty ms | workflow share of claim time |
|:--|--:|--:|--:|--:|--:|--:|--:|
| 1 | 4.00 | 4.33 | 3.00 | 4.07 | 0.01 | 1.66 | 58.7% |
| 2 | 4.00 | 6.61 | 3.00 | 6.28 | 0.02 | 3.01 | 58.4% |
| 4 | 4.00 | 10.01 | 3.00 | 9.60 | 0.65 | 4.15 | 55.8% |

Each depth reads the same. The claims in flight, the claim count times the
mean claim time over the elapsed time, are 0.92, 1.84 and 3.24 at 1, 2 and 4
loops, as the claim-concurrency page found. `graded.md` gives them per
depth.

What the split shows:

- **The two kinds cost the same.** A workflow claim is 4% to 6% slower than
  an activity claim at every cap. The claim statement does not favour
  either kind.
- **The count drives the share.** A workflow makes 4 workflow claims and 3
  activity claims. So workflow claims take about 4/7 of the claim time.
- **Contention raises both kinds alike.** From 1 to 4 loops, both rise by
  about 2.3x.

### The formula with measured inputs

At one loop, the claim time per workflow is 4 × 4.33 + 3 × 4.07 = 29.5 ms.
That gives a ceiling of 33.9 workflows/sec. The measured rate is 31.6, so
the loop is busy 93% of the time.

If the 4 workflow claims leave the contended path, the claim time per
workflow falls to 12.2 ms. The claim-loop ceiling then rises to 81.9
workflows/sec, **2.42x**. The issue predicted about 2.3x. That is an upper
bound for the claim loop only:

- The decision persist stays. It takes 4 × 4.14 = 16.6 ms per workflow at
  one loop. Where an owner runs the decisions of its partitions in series,
  that time caps the gain at 1 / 16.6 ms, about 60 workflows/sec.
- The persist contends with the claims on the same database. From 1 to 4
  loops the persist rises from 4.14 ms to 10.27 ms.

## 2. The persist phases

The mean time of each phase per decision persist, over all depths:

| claim loops | persist ms | fence | events | tasks | notify | commit | other |
|:--|--:|--:|--:|--:|--:|--:|--:|
| 1 | 4.14 | 0.00 (0%) | 0.57 (14%) | 0.58 (14%) | 0.00 (0%) | 0.24 (6%) | 2.76 (67%) |
| 2 | 6.35 | 0.00 (0%) | 0.82 (13%) | 0.84 (13%) | 0.00 (0%) | 0.35 (5%) | 4.33 (68%) |
| 4 | 10.27 | 0.00 (0%) | 1.33 (13%) | 1.29 (13%) | 0.01 (0%) | 0.57 (6%) | 7.08 (69%) |

Each workflow makes 4.00 decision persists. The phases are:

- `fence`: the DR generation check and the owner fence. The assay pins no
  DR fence and runs no owner mode, so it is zero here. Section 3 measures
  it.
- `events` and `tasks`: the inserts into `harvest_events` and
  `harvest_task_queue`.
- `notify`: the stage of the post-commit wakes. Since issue #1796 the
  `NOTIFY` itself runs after the commit, on the sender's own connection.
- `commit`: the `COMMIT` statement.
- `other`: the rest of the transaction. That is `BEGIN`, the execution row
  lock, the pause check, the reads and the task and execution updates.

**The rest is most of the persist.** One diagnostic run at depth 1,000 and
one loop kept `pg_stat_statements`
([top 40 statements](perf-artifacts/decision-owner-baseline/results/pg-stat-statements-claims1-d1000.txt)).
The server runs each persist statement in 0.001 ms to 0.2 ms. In the same
run, the server ran the claim statement in 0.67 ms of a 4.33 ms claim. So
the time in `other` is consistent with round trips and client work, not
with slow statements. This run does not split `other` further, and CPU
contention on 4 cores can add to it.

## 3. The fence cost

[`benches/decision_fence_bench.rs`](../autumn-harvest/benches/decision_fence_bench.rs)
times a model decision persist made of public engine calls. One transaction
holds the decision fence, two appends, one activity task enqueue and an
execution update. The post-commit sender is registered. The bench uses the
real owner row and the real `harvest_decision_owner_fence` function of
issue #2251, so it needs no prototype row.

| mode | statements that lock a fence row |
|:--|:--|
| `none` | none |
| `generation` | the DR generation row `FOR SHARE`, once per append in a savepoint, as today |
| `owner` | the owner row `FOR KEY SHARE`, once, through the fence function |
| `generation_owner` | as owner mode with DR on runs them: the generation row and the owner fence at the start, then the generation row once per append |
| `generation_plain_owner_row` | `generation`, and one plain `FOR KEY SHARE` of the owner row with no function |
| `generation_owner_refence` | `generation_owner`, and the owner fence again after each append: the code before this issue |

So `generation_owner` adds two statements to `generation`, and
`generation_plain_owner_row` adds one.

**Since issue #2255 the labels measure new code.** `generation_owner` is now
the fused fence: one statement, and no generation check at an append. The
shape that this page measured is `generation_owner_split`.
`generation_owner_refence` still adds the owner fence at each append. See
[`performance-owner-decider.md`](performance-owner-decider.md). The owner row takes `FOR KEY SHARE`,
not the `FOR SHARE` that the issue names.

With 8 writers, `shared` makes every writer lock one owner row. `spread`
gives each writer its own owner row, as decisions of many partitions have.
Each case starts on empty event and task tables. Nine runs on 2026-10-22,
each in another mode order. Runs r0 to r2 time every case. Runs r3 to r8
time the three one-writer cases that the pass condition reads. Raw output:
[`results/fence-bench/`](perf-artifacts/decision-owner-baseline/results/fence-bench/).
Graded: [`results/fence-graded.md`](perf-artifacts/decision-owner-baseline/results/fence-graded.md).

The delta of each run at one writer, against `generation` in the same run:

| run | `generation` ms | `generation_owner` ms | `generation_plain_owner_row` ms | two statements | one statement |
|:--|--:|--:|--:|--:|--:|
| r0 | 4.12 | 5.18 | 5.08 | +25.9% | +23.3% |
| r1 | 4.33 | 5.44 | 4.64 | +25.6% | +7.1% |
| r2 | 4.34 | 4.81 | 4.80 | +10.8% | +10.5% |
| r3 | 4.28 | 5.08 | 5.10 | +18.6% | +19.1% |
| r4 | 4.65 | 5.01 | 4.63 | +7.7% | −0.5% |
| r5 | 3.69 | 5.53 | 4.82 | +49.9% | +30.7% |
| r6 | 4.51 | 5.26 | 4.67 | +16.6% | +3.4% |
| r7 | 4.11 | 4.84 | 4.85 | +17.8% | +18.0% |
| r8 | 4.47 | 5.22 | 4.46 | +16.6% | −0.4% |
| **median** | | | | **+17.8%** | **+10.5%** |

**The stage 1b pass condition fails.** It asks for a delta under 10%. Owner
mode with DR on adds +17.8% in the median run, and 10% or more in 8 of 9
runs. The pooled means give +20.4%. The one-statement case is not clear: its
median is +10.5%, and four runs fall under 10%.

The criterion interval of each case is within ±5% of its estimate. The same
mode moves by up to ±15% between runs, so read single runs with care.

The pooled means over all runs, ms:

| mode | 1 writer | 8 writers, shared | 8 writers, spread |
|:--|--:|--:|--:|
| `none` | 3.75 | 6.19 | 6.19 * |
| `generation` | 4.28 | 8.23 | 8.23 * |
| `owner` | 4.26 | 9.86 | 9.80 |
| `generation_owner` | 5.15 | 10.28 | 11.75 |
| `generation_plain_owner_row` | 4.78 | 9.02 | 9.65 |
| `generation_owner_refence` | 5.99 | 11.75 | 11.05 |

\* The mode locks no owner row, so the cell repeats the shared case.

**The 8-writer cells show nothing on this host.** Eight writers and the
server share 4 CPUs. A case moves by up to 2x between runs, and the
second-row delta changes sign between runs. They are kept as raw output
only.

What the bench shows at one writer:

- **Each added statement costs about 0.5 ms here.** The persist is mostly
  round trips, so each statement that it adds costs about as much as one.
  `generation` adds six statements to `none` for 0.53 ms.
- **The fix of this issue saves 14% at one writer.** Before it, each append
  also sent the owner fence again. At 8 writers the effect is lost in the
  noise. See the next section.

### The fence fix

With DR fencing on, `store::insert_event_rows` calls
`replication::assert_fence` for each append. That called
`decision_owner::fence_pinned`, which did not check the fenced scope. So an
owner-mode decision sent the owner fence once more for each append. That
broke the rule of issue #2252: a nested fence of the same connection sends
no query. `fence_pinned` now sends no query inside a fenced scope of its
connection. The start fence holds the owner row until the commit, so a
second fence cannot fail.
`cross_region_dr_tests::a_dr_fenced_owner_decision_sends_one_owner_fence`
proves it: a decision with two appends made 3 fence calls before, and makes
1 now.

## What stage 1b can do

These are proposals, not measured fixes. Stage 1b needs the first two
together to remove both added statements.

1. **Fence both rows in one statement.** One function can check the
   generation row and the owner row. That removes the second check of the
   generation row.
2. **Skip the per-append generation check inside a fenced scope** when the
   start fence took the same generation row. A colocated shard can use
   another row, so the skip must compare the rows. That removes two
   statements and a savepoint per append, more than the owner fence adds.
3. **Cut statements in the rest of the persist.** It is 67% of the persist.
   Each statement that the owner can skip, such as a read that its cache
   already holds, is worth about as much as a fence row.

## What the measurement does not establish

- One box, one shape and one Postgres version. The host has 4 CPUs, so the
  8-writer cases and the 4-loop cells are CPU-bound.
- The one-writer delta moves between runs, from +7.7% to +49.9%. Nine runs
  give a clear verdict for two statements, not a precise size.
- No case adds the owner row alone with DR on. The one-statement case has
  no function and no clear verdict.
- Each run times the modes in blocks, in a rotated order. Alternating modes
  inside one case would separate drift from mode better.
- The fence bench models the persist. It does not run the worker. Its base
  is 3.75 ms, near the assay persist of 4.14 ms, but it sends fewer
  statements.
- The assay runs no DR fence and no owner mode. Its `fence` phase is zero.
- `other` is not split into its statements.
- The cost of the phase capture is not measured. At 4 loops the sweep reads
  45.3 workflows/sec, against 48 to 51 on the claim-concurrency page. That
  page ran in another session, so the gap is not attributed.
- The `pg_stat_statements` run counts every statement of the run, the seed
  and the health checks included.
- `fsync=off` hides the commit cost of a durable server. A durable commit
  makes the persist longer and each fence row a smaller share.

## Reproduce

See [`perf-artifacts/decision-owner-baseline/README.md`](perf-artifacts/decision-owner-baseline/README.md).
