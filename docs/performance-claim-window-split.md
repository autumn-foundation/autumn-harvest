# Hot path audit: executor startup and the claim window split (issue #2680)

This page records a third audit of the hot path, after
[issue #2651](performance-hot-path-statement-cache.md) and
[issue #2654](performance-hot-path-round-trips.md). Those audits ranked
statements by `pg_stat_statements`. This audit adds a CPU profile of the
whole box, and it finds a cost that the census cannot see. It then removes
the largest part of that cost.

> **This is a reference measurement, not an SLO.** It comes from one 4-core
> box with the database, the worker and the load generator on it.
> Reproduce it before you design against it.

## TL;DR

* **The census does not count executor startup.** `pg_stat_statements`
  times `ExecutorRun` and `ExecutorFinish` only. A system-wide profile puts
  `standard_ExecutorStart` at 14.9% of all CPU samples, and
  `standard_ExecutorRun` at 25.7%.
* **The default claim is most of that startup.** Postgres initializes
  every CTE subplan at startup, also a subplan that never runs. The claim
  holds four candidate scans. Sampled plans give 204 plan nodes per claim,
  and 110 of them never run. The claim is 79.8% of all plan nodes that the
  hot path initializes.
* **The fix splits the claim.** A window form runs first. It holds only
  the window scan, and it reports when a deeper scan could find a row. The
  full statement, unchanged, runs only then. In the bench, 8 of 10,128
  claims ran it (0.08%).
* **The `throughput` cell moves from 92.2 to 104.3 workflows/s.** That is
  the median of six alternating pairs. The split build is faster in six of
  six, by 1.04 to 1.22 times (median 1.12). The median start-to-completion
  time falls from 315 ms to 274 ms.
* **`standard_ExecutorStart` falls from 15.1% to 12.2% of all samples**,
  in two back-to-back profiles of each build.
* **The claim of a row does not change.** `formal/verus/src/claim_window.rs`
  proves that the split claim takes the row of the full statement for
  every input. The claim suites pass unchanged.
* **Six findings are recorded, not fixed.** See
  [What this audit did not change](#what-this-audit-did-not-change).

## The method

The workload is the `throughput` cell of `e2e_bench` at one shard, as in
the [issue #2651 audit](performance-hot-path-statement-cache.md#the-method).
It runs one in-process worker with 8 workflow slots, 16 activity slots and
2 claim loops. 32 workflows are in flight.

Host: 4 logical CPUs, 15 GB. PostgreSQL 16.15 on the same host, with
`fsync = off`, `synchronous_commit = off` and `max_connections = 200`, as in
`benchmarks/docker-compose.yml`, and `pg_stat_statements` preloaded. The
base is `trunk-dev` at `21cb8e3`. Measured on 2026-11-14.

Five tools, each for one question:

| Question | Tool |
|:--|:--|
| Which processes and which Postgres phases use the CPU? | `perf record -a`, 299 Hz, frame-pointer call graphs |
| Which statements initialize the most plan nodes? | `auto_explain` with `log_analyze`, 3% sample, [`plan_nodes.py`](perf-artifacts/claim-window-split/plan_nodes.py) |
| What does one claim cost alone, with its startup? | `pgbench -M prepared`, one client, the claim plan settings, then a rollback |
| Which statements run, and how often? | `pg_stat_statements`, the census of the earlier audits and [`claim_forms.sql`](perf-artifacts/claim-window-split/claim_forms.sql) |
| Does the change move the cell? | alternating pairs, [`paired.sh`](perf-artifacts/claim-window-split/paired.sh) |

Raw output: [`perf-artifacts/claim-window-split/`](perf-artifacts/claim-window-split/).

**The base rate drifted during the session.** The base build measured
76.7 workflows/s in the first census and 92.2 in the median of the pairs.
So only a ratio from one pair compares. Do not compare the absolute numbers
with another page.

## 1. What the base spends

**Postgres uses three quarters of the CPU.** In one run, the box was busy
for 54.0 CPU seconds. The bench process used 13.5 of them. A system-wide
profile puts 68.9% of all samples in `postgres` and 22.5% in the worker
threads.

**Inside Postgres, executor startup is more than half as large as
execution.**

| Phase | Share of all samples |
|:--|--:|
| `standard_ExecutorRun` | 25.7% |
| `standard_ExecutorStart` | 14.9% |
| `ExecInitIndexScan`, inside startup | 4.2% |
| `ExecInsertIndexTuples` | 4.2% |
| `GetCachedPlan` | 5.5% |

**The census does not see startup.** `pg_stat_statements` starts its timer
in its `ExecutorRun` hook. A cost in `ExecutorStart` or `ExecutorEnd` is in
no row of the census. The earlier audits ranked by the census, so they
could not find this cost.

## 2. Where the startup goes

**One statement initializes most of the plan nodes.** `auto_explain`
sampled 6,229 plans in one run.
[`plan-nodes-base.txt`](perf-artifacts/claim-window-split/plan-nodes-base.txt)
ranks each statement by the plan nodes it initializes:

| Statement | Nodes per plan | Never run per plan |
|:--|--:|--:|
| the default claim | 204 | 110 |
| the timeout refill page | 88 | 5 |
| the by-id claim | 7 | 0 |
| every other hot statement | 10 or fewer | 4 or fewer |

The claim is 59,772 of 74,890 sampled nodes, or 79.8%.

**The claim holds four candidate scans, and one does the work.** The
claim text is 26.8 KB. Its candidate scans are `seek_candidate`,
`seek_held`, `seek_deep_candidate` and `legacy_candidate`
([`queue::claim_task_query`](../autumn-harvest/src/queue.rs)). Each scan
after the first runs only when the scans before it found no row. In 292 of
293 sampled claims, `seek_candidate` found the row.

Postgres initializes every CTE subplan of a statement at executor startup.
It does so whether the subplan runs or not. So each claim paid the startup
of all four scans, of the backlog-wide concurrency counts, and of the deep
window.

**Alone, the claim spends as much on startup as on execution.** A
`pgbench` loop of the claim statement on a probe database with 24 pending
rows gives:

| Form | Statement latency |
|:--|--:|
| the full claim | 0.94 ms |
| the window scans and `seek_held` only | 0.72 ms |
| the window scans only | 0.66 ms |

A profile of the full claim loop puts `standard_ExecutorStart` at 14.7% of
samples and `standard_ExecutorRun` at 15.0%
([`pgbench-results.txt`](perf-artifacts/claim-window-split/pgbench-results.txt)).

## 3. The change: the claim window split

A default claim now sends its **window form** first. The window form is the
full statement less the deep window, the full scan and the backlog-wide
concurrency counts. Its `candidate` is `seek_candidate`. It keeps
`seek_held` and `seek_contended`, so its decision is exact. It returns one
row: the claimed task, or no task, and a flag `claim_fallback`.

The **full statement, unchanged**, runs only when the flag is set. It runs
in the same claim transaction, after the window form. Both statements go
out from `queue::claim_task_one_statement`, the one function that sends a
default claim. Ageing claims and
fair claims skip the window, so they send the full statement alone, as
before.

### Why the split claims the same row

In the full statement, `candidate` is the union of the three scans. The
deep gate and the full-scan gate each start with
`NOT EXISTS (SELECT 1 FROM seek_candidate)`. So:

* **A row in `seek_candidate`.** The two deeper scans are empty. The
  window form claims the same row from the same snapshot.
* **No row, and the flag is clear.** The flag is
  `NOT EXISTS (seek_candidate) AND (EXISTS (seek_contended) OR EXISTS
  (seek_bounds) OR ageing)`. Those three tests are the gates of the deep
  scan and the full scan. With all three false, the full statement claims
  no row either.
* **The flag is set.** `seek_candidate` was empty. So the window form
  locked no row, debited no token and took no advisory lock. The full
  statement then reads a new snapshot under `READ COMMITTED`. That is a
  fresh claim attempt, which a later claim is always allowed to make.

The flag over-approximates in one case: a held head that the deep window
reads to its end. The full statement then finds no row. The claim pays one
more statement, and it never takes a different row.

`formal/verus/src/claim_window.rs` proves the three cases for every input
of a model of the full statement. `every_deeper_scan_of_the_full_form_waits_for_an_empty_window`
pins the gate text that the model assumes.

### How the text is built

[`queue::claim_window::window_claim_sql`](../autumn-harvest/src/queue.rs)
derives the window form from the final text of each full statement, after
every splice. So the DR fence, the kind predicate and the owned gate are
already in it. It parses the top-level CTE list, drops nine named CTEs and
rewrites `candidate`. It asserts that each dropped CTE exists once and that
no kept CTE names a dropped one. `window_claim_sql_holds_for_every_variant`
runs it for all twelve variants. Each variant is built once per process.

## 4. Results

### Paired runs

Six alternating pairs, `track_planning = off`
([`paired-runs.txt`](perf-artifacts/claim-window-split/paired-runs.txt)):

| Pair | Base (wf/s) | Split (wf/s) | Ratio |
|--:|--:|--:|--:|
| 1 | 103.6 | 107.9 | 1.04 |
| 2 | 96.5 | 109.9 | 1.14 |
| 3 | 92.5 | 104.4 | 1.13 |
| 4 | 92.0 | 102.6 | 1.11 |
| 5 | 85.7 | 104.2 | 1.22 |
| 6 | 90.2 | 100.5 | 1.11 |
| **median** | **92.2** | **104.3** | **1.12** |

The median start-to-completion time, by Little's law, falls from 315 ms to
274 ms.

**The in-flight gate withholds most split runs.** `e2e_bench` publishes a
closed-loop rate only when the generator holds 90% of its 32 workflows in
flight. In a closed loop, the mean in-flight count is the target less the
restart lag of the generator times the completion rate. Faster workflows
lower it. The split runs held 28.3 to 28.6. The base runs held 28.5 to
29.1, and base pair 1 failed the gate too. So the generator, not the
engine, bounds the split build at this target. A closed-loop rate is the
in-flight count over the latency, so a lower in-flight count understates
the split build. `paired.sh` computes the rate from the raw rows of the
cell: the middle half of the completions over the measured window. That is
the headline formula.

### Census

One census run of each build, back to back, with `track_planning = on`:

| | Base | Split |
|:--|--:|--:|
| statements | 307,241 | 304,145 |
| planner runs | 8,682 | 8,760 |
| execution time | 20,665 ms | 19,200 ms |
| claims, full form | 10,130 | 8 |
| claims, window form | 0 | 10,120 |
| census time per claim | 0.755 ms | 0.669 ms |

The census time per claim falls by 11% only. The rest of the gain is in
executor startup, which the census does not count.

### Profile

Two profiles of each build, back to back
([`perf-executor-start.txt`](perf-artifacts/claim-window-split/perf-executor-start.txt)):

| Symbol | Base | Split |
|:--|--:|--:|
| `standard_ExecutorStart` | 15.1%, 15.6% | 12.2%, 13.0% |
| `ExecInitIndexScan` | 4.5%, 4.6% | 3.6%, 3.4% |
| `standard_ExecutorRun` | 24.5%, 24.9% | 26.7%, 26.8% |

These are shares of a box that completes more workflows in the same time.
So the startup cost of one workflow falls by more than the shares show.

## What this audit did not change

Each item below is a measured cost that this change leaves in place.

**1. The window form still initializes `seek_held`.** Without `seek_held`
the claim loop costs 0.66 ms, not 0.72 ms. But the flag would then
over-approximate each time a window row fails a gate that is not
row-local, such as a concurrency cap. Each empty poll of such a queue would
send both statements. The exact flag is worth 5%.

**2. An activity start and an activity completion lock the task row
twice.** Each runs `lock_claim_for_update`, a `SELECT ... FOR UPDATE` with
the claim test, and then a fenced `UPDATE` of the same row. The fenced
`UPDATE` alone takes the lock and tests the claim. That is 6 statements
per workflow, each while the transaction holds the execution row lock.
`fail_claimed_task` reads the state of a lost claim for a warning, so a
merge needs one more read on that rare path.

**3. `harvest_task_queue` has 20 indexes and `harvest_workflow_executions`
has 39.** A task row moves between states on each claim and each
completion, so most of its updates are not HOT. `ExecInsertIndexTuples`
takes 4.2% of all samples, and `ExecOpenIndices` 2.3%. A drop needs the
read path of each index in production, not only in this bench.

**4. The worker keys the claim statement by its text.** `CachedClaimQuery`
has no static query id, so diesel builds, hashes and compares the full
text on each claim. In the base, `memmove` takes 4.3% of the worker
samples, and SipHash shows in the profile. The window form is 15.9 KB, not
26.8 KB, so this cost falls too. A static id needs one marker type per text.

**5. The timeout refill page plans on each poll tick.** It plans 425 times
per run at 1.9 ms each. [Issue #2654](performance-hot-path-round-trips.md#what-stays)
already records why its text stays unnamed.

**6. Other plans can hide startup cost too.** A profile and `auto_explain`
are the tools that find it. A future census should not rank by
`pg_stat_statements` alone.

## Reproducing

Run the pairs against a server with `pg_stat_statements` loaded:

```bash
bash docs/perf-artifacts/claim-window-split/paired.sh <base-bin> <new-bin> <outdir> 6
```

Run one census with
[`census.sh`](perf-artifacts/hot-path-round-trips/census.sh), then
[`claim_forms.sql`](perf-artifacts/claim-window-split/claim_forms.sql)
before the next reset. [`pgbench.sh`](perf-artifacts/claim-window-split/pgbench.sh)
runs one claim form alone, with the `pgbench-claim-*.sql` scripts.

The tests of the change:

| Test | Proves |
|:--|:--|
| `formal/verus/src/claim_window.rs` | the split claim takes the row of the full statement for every input |
| `queue::claim_window::tests::window_claim_sql_holds_for_every_variant` | each of the twelve window texts drops the deeper scans, keeps the window, binds the same parameters and returns its flag |
| `queue::claim_window::tests::every_deeper_scan_of_the_full_form_waits_for_an_empty_window` | the gate text that the proof assumes |
| `claim_seek_tests` | held heads, the deep window, full heads and ageing still claim the right row through `claim_task` |
| `decision_owner_claim_tests`, `owner_decider_tests`, `cross_region_dr_tests` | the owned and fenced variants |
