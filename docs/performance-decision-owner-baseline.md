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
  against 4.07 ms at one claim loop. Workflow claims take 58% of the claim
  time, because a workflow makes 4 of them and 3 activity claims.
- The decision persist takes 4.14 ms at one claim loop. Event inserts take
  14%, task inserts 14%, the commit 6%, the notify stage 0%. The other 67%
  is the rest of the transaction.
- **The second fence row fails the stage 1b pass condition.** It adds 21%
  to the persist at one writer. The condition is under 10%.

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

Each depth reads the same. The claims in flight are 0.92, 1.84 and 3.24 at
1, 2 and 4 loops, as the claim-concurrency page found.

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

**The rest is most of the persist, and it is round trips.** One
diagnostic run at depth 1,000 and one loop kept `pg_stat_statements`
([output](perf-artifacts/decision-owner-baseline/results/pg-stat-statements-claims1-d1000.txt)).
The server runs each persist statement in 0.001 ms to 0.2 ms. The run sent
about 294 statements per workflow, the seed included. So the persist time is mostly the
number of statements and their round trips, not their execution. The same
holds for the claim: the server runs the claim statement in 0.67 ms of its
4.33 ms.

## 3. The fence cost

[`benches/decision_fence_bench.rs`](../autumn-harvest/benches/decision_fence_bench.rs)
times a model decision persist made of public engine calls. One transaction
holds the decision fence, two appends, one activity task enqueue and an
execution update. The post-commit sender is registered. The bench uses the
real owner row and the real `harvest_decision_owner_fence` function of
issue #2251, so it needs no prototype row.

| mode | fence rows |
|:--|:--|
| `none` | none |
| `generation` | the DR generation row `FOR SHARE`, once per append, as today |
| `owner` | the owner row `FOR KEY SHARE`, once, through the fence function |
| `generation_owner` | both, as owner mode with DR on runs them |
| `generation_plain_owner_row` | the generation row, and a plain `FOR KEY SHARE` of the owner row with no function |
| `generation_owner_refence` | `generation_owner`, and the owner fence again after each append: the code before this issue |

With 8 writers, `shared` makes every writer lock one owner row. `spread`
gives each writer its own owner row, as decisions of many partitions have.
Each case starts on empty event and task tables. Three runs on 2026-10-22,
each in another mode order, on the same host and server. Raw output:
[`results/fence-bench/`](perf-artifacts/decision-owner-baseline/results/fence-bench/).
Graded: [`results/fence-graded.md`](perf-artifacts/decision-owner-baseline/results/fence-graded.md).

The mean persist latency, ms:

| mode | 1 writer | 8 writers, shared | 8 writers, spread |
|:--|--:|--:|--:|
| `none` | 3.75 | 6.19 | 6.19 |
| `generation` | 4.26 | 8.23 | 8.23 |
| `owner` | 4.26 | 9.86 | 9.80 |
| `generation_owner` | 5.14 | 10.28 | 11.75 |
| `generation_plain_owner_row` | 4.84 | 9.02 | 9.65 |
| `generation_owner_refence` | 5.99 | 11.75 | 11.05 |

The deltas:

| delta | 1 writer | 8 writers, shared | 8 writers, spread |
|:--|--:|--:|--:|
| **second row: `generation_owner` over `generation`** | **+20.7%** | +24.9% | +42.7% |
| plain row lock: `generation_plain_owner_row` over `generation` | +13.5% | +9.6% | +17.2% |
| first row: `generation` over `none` | +13.7% | +32.9% | +32.9% |
| the fix: `generation_owner` over `generation_owner_refence` | −14.2% | −12.5% | +6.3% |

**The stage 1b pass condition fails.** It asks for a delta under 10% for
the second row. At one writer the delta is +20.7%. No pairing of the three
runs falls under 10%: the lowest is +10.8%, the highest +32%. At one writer
each criterion interval is within ±5% of its mean.

The 8-writer cases put 8 writers and the server on 4 CPUs. Their runs vary
by up to ±20% between rounds, so read them as a direction only: contention
raises every fence cost.

What the bench shows:

- **One more statement costs about 0.6 ms.** The plain row lock adds one
  statement and +13.5%. A persist that is mostly round trips pays for each
  statement it adds.
- **Owner mode with DR on adds two statements.** The start fence takes the
  generation row once more, then calls the fence function. Each append
  still takes the generation row again.
- **The fix of this issue saves 14%.** Before it, each append also sent the
  owner fence again. See the next section.

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

These are proposals, not measured fixes.

1. **Fence both rows in one statement.** One function can check the
   generation row and the owner row. That removes one round trip.
2. **Skip the per-append generation check inside a fenced scope** when the
   start fence took the same generation row. A colocated shard can use
   another row, so the skip must compare the rows. That removes two
   statements and a savepoint per append.
3. **Cut statements in the rest of the persist.** It is 67% of the persist
   and it is round trips. Each statement that the owner can skip, such as a
   read that its cache already holds, is worth about as much as a fence row.

## What the measurement does not establish

- One box, one shape and one Postgres version. The host has 4 CPUs, so the
  8-writer cases and the 4-loop cells are CPU-bound.
- The fence bench models the persist. It does not run the worker. Its base
  is 3.75 ms, near the assay persist of 4.14 ms, but its statements are
  fewer.
- The assay runs no DR fence and no owner mode. Its `fence` phase is zero.
- `fsync=off` hides the commit cost of a durable server. A durable commit
  makes the persist longer and each fence row a smaller share.

## Reproduce

See [`perf-artifacts/decision-owner-baseline/README.md`](perf-artifacts/decision-owner-baseline/README.md).
