# Hot path audit: the lean claim tier (follows issue #2680)

This page follows the hot path audit of
[issue #2680](performance-claim-window-split.md). That audit found that
executor startup is a cost that `pg_stat_statements` cannot see, and it
split the claim. This audit measures what the window form still
initializes, and it puts a smaller statement in front of it.

> **This is a reference measurement, not an SLO.** It comes from one 4-core
> box with the database, the worker and the load generator on it.
> Reproduce it before you design against it.

## TL;DR

* **The window form still starts plan nodes that most claims never use.**
  One window claim initializes 131 plan nodes, and 56 of them never run.
  Most of them serve concurrency keys, rate limits, build pins and
  capability requirements.
* **The lean form drops them.** It is exact for a seek window of plain
  rows, and it initializes 69 plan nodes. In each other case it claims no
  row, changes no row and sets a flag. The unchanged window form then
  runs in the same transaction.
* **Alone, a lean claim costs 0.55 ms against 0.80 ms.** That is the
  median of twelve alternating `pgbench` rounds of each form, with
  executor startup. The lean form is faster in twelve of twelve.
* **The `throughput` cell moves from 75.4 to 88.0 workflows/s.** That is
  the median of six rounds of one binary with the lean form off and on.
  The lean form is faster in six of six, by 1.13 to 1.22 times (median
  1.17). The median start-to-completion time falls from 390 ms to 328 ms.
* **With the lean form off, the new binary matches the base.** The median
  ratio to the base binary is 1.00, so the switch restores the old cost.
* **`standard_ExecutorStart` falls from 12.8% and 13.1% to 9.9% and 10.1%
  of all samples**, in two back-to-back profiles of each arm.
* **The claim of a row does not change.** `formal/verus/src/claim_window.rs`
  proves that a claim takes the same row with and without the lean form.
  `lean_claim_db_tests` compares both orders over random queue states for
  all twelve statement variants.
* **A skip bounds the cost of a queue that cannot use the lean form.** A
  review of the first skip rule found a loop that it made slower than no
  lean form at all. The shipped rule keeps one skip for each claim kind
  and each weighted queue. That loop now runs faster than with the lean
  form off. See [The skip](#4-the-skip).
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
base is `trunk-dev` at `54a254d`. Measured on 2026-11-26.

Each tool answers one question:

| Question | Tool |
|:--|:--|
| What does one claim of each form cost alone, with its startup? | `pgbench -M prepared`, one client, the claim plan settings, then a rollback: [`pgbench.sh`](perf-artifacts/claim-lean-tier/pgbench.sh) |
| How many plan nodes does each form initialize? | `EXPLAIN (ANALYZE)` of a generic plan, at the end of `pgbench.sh` |
| Does the change move the cell? | three arms in a rotated order: [`paired3.sh`](perf-artifacts/claim-lean-tier/paired3.sh) |
| Which claim forms run, and how often? | `pg_stat_statements`: [`census.sh`](perf-artifacts/hot-path-round-trips/census.sh), then [`claim_forms.sql`](perf-artifacts/claim-window-split/claim_forms.sql) |
| Which Postgres phases use the CPU? | `perf record -a`, 999 Hz, frame-pointer call graphs: [`profile.sh`](perf-artifacts/claim-lean-tier/profile.sh) |
| What does a loop of mixed claim kinds send under each skip rule? | one `psql` session that replays the loop: [`mixed-kinds/run.sh`](perf-artifacts/claim-lean-tier/mixed-kinds/run.sh) |

Raw output: [`perf-artifacts/claim-lean-tier/`](perf-artifacts/claim-lean-tier/).

**The lean form has a switch, so one binary gives both arms.**
`HARVEST_BENCH_CLAIM_LEAN=0` runs the bench with the window form first.
The paired runs add a third arm, the base binary, to show that the switch
restores the base cost.

**Only a ratio from one round compares.** The base rate drifts between
sessions on this box. Do not compare the absolute numbers with another
page.

## 1. What the window form still spends

**A window claim initializes 131 plan nodes, and 56 never run.** The count
is from a generic plan of the default variant, on a probe database with 24
pending plain rows
([`plan-nodes.txt`](perf-artifacts/claim-lean-tier/plan-nodes.txt),
[`explain-any-window.txt`](perf-artifacts/claim-lean-tier/explain-any-window.txt)).

Postgres initializes every plan node of a statement at executor startup.
It does so whether the node runs or not. The window form keeps these
parts for each claim:

* the concurrency counts of the window, and the held-head scan
  `seek_held`;
* the rate-limit buckets and their debit;
* the build pin test and the capability test, with the worker labels;
* the pause arrays, joined to each scan.

A row is **plain** when its `concurrency_key`, `rate_limit_key`,
`required_build_id` and `required_capabilities` are all `NULL`. For a
window of plain rows, the first three parts decide nothing.

**A peer commit starts the subplans again.** Two claim loops read the same
window. When a peer commits a claim of a window row between the snapshot
and the row lock, Postgres rechecks the row in `EvalPlanQual`. That
recheck initializes each subplan of the statement again. So a larger
statement pays twice under contention.

## 2. The change: the lean form

A worker claim loop now sends the **lean form** first. Its text is the
window text of the same variant, less the parts that only a row that is
not plain needs. It returns one row: the claimed task, or no task, and a
flag `claim_fallback`.

* **It takes a row.** The claim ends, as a window claim ends.
* **Its window holds no row.** The claim ends with no row.
* **Its window holds a row and it locks none.** It sets its flag. The
  claim sends the **unchanged window form** in the same transaction, and
  goes on as before.

Both statements go out from `queue::claim_form_step`, which
`queue::claim_task_one_statement` calls. A caller with no loop state, such
as `queue::claim_task`, sends the window form first, as before. Ageing
claims, fair claims and batch claims send no lean form. The operator guide
is [`operations/claim-lean.md`](operations/claim-lean.md).

### Why the lean claim takes the same row

The lean candidate is the window candidate with one more test: the row is
plain. Each pin gate, deadline gate and owner gate keeps its text, and
each pause gate gives the same result.

* **The lean form takes a row.** The window scan orders its rows the same
  way in both forms. The lean form takes its first row only when no window
  row is other than plain. So the gates that it drops pass for each row
  of that window, and the window form takes the same row from the same
  snapshot.
* **No row, and the flag is clear.** The window holds no row, or the run
  of the locked row is past its deadline at the clock read. The window
  form answers the same from that snapshot. In neither case does it ask
  for the full statement.
* **The flag is set.** The lean form claimed no row and changed no row. It
  debited no token and took no advisory lock. The window form then reads a
  new snapshot under `READ COMMITTED`. That is a fresh claim attempt, which
  a later claim is always allowed to make.

The flag over-approximates. A parked row of a paused run sets it, and so
does a window whose rows peer claims hold. The claim then pays one more
statement, and it never takes a different row.

After a peer commit on a window row, a lean statement that sets its flag
keeps the lock of the row that it skipped, until the transaction ends. The
window form does the same.

`formal/verus/src/claim_window.rs` proves the three cases for every input
of its model. `lean_claim_sql_holds_for_every_variant` pins the text that
the model assumes.

### How the text is built

[`queue::claim_window::lean_claim_sql`](../autumn-harvest/src/queue.rs)
derives the lean form from the final window text of each variant. So the
DR fence, the kind predicate and the owned gate are already in it. Each
edit replaces an exact text and asserts its count. A changed gate in the
window text then fails at first use, and in the unit test of each variant.
No existing claim text changed.

A second step, `LEAN_PLAN_REWRITES`, changes how the lean text reads the
pauses. It changes the plan and not the result:

| Rewrite | Effect on the plan |
|:--|:--|
| scalar subqueries for the pause arrays | no join of the arrays to each scan |
| `ARRAY(SELECT ...)` for `array_agg` | no aggregate node |
| the saturated names of `$6` join the paused-activity array | one activity test less in each scan |

## 3. Results

### One claim alone

Twelve alternating rounds of 5 s for each form, one client, with a
rollback after each claim
([`pgbench-runs.txt`](perf-artifacts/claim-lean-tier/pgbench-runs.txt)):

| Form | Plan nodes | Never run | Statement latency, median | Range |
|:--|--:|--:|--:|--:|
| the window form | 131 | 56 | 0.798 ms | 0.767 to 0.869 ms |
| the lean form | 69 | 13 | 0.551 ms | 0.505 to 0.595 ms |

The lean form is faster in twelve of twelve rounds.

**The plan rewrites are 0.05 to 0.08 ms of that.** A second set of runs compares
the lean text before and after the rewrites
([`rewrites/results.txt`](perf-artifacts/claim-lean-tier/rewrites/results.txt)).
The `base` rows there are the lean text with no rewrite, and the `all` rows
are the shipped text:

| Run | Before the rewrites | Shipped | Paired difference | Faster in |
|:--|--:|--:|--:|--:|
| rotated rounds, plain fixture | 0.696 ms | 0.609 ms | -0.076 ms | 30 of 32 |
| interleaved statements, plain fixture | 0.781 ms | 0.703 ms | -0.076 ms | 15 of 15 |
| interleaved statements, live gates | 1.285 ms | 1.167 ms | -0.105 ms | 15 of 15 |

The absolute latency differs between the runs, as the `base` rows show.
The paired difference is the result. The file also holds three other runs
on the plain fixture. Their paired medians are -0.051, -0.056 and
-0.072 ms.

### Paired runs

Six rounds of three arms, `track_planning = off`
([`paired-runs.txt`](perf-artifacts/claim-lean-tier/paired-runs.txt)).
`Base` is the base binary. `Off` and `On` are the new binary with the lean
form off and on:

| Round | Base (wf/s) | Off (wf/s) | On (wf/s) | On / Off | On / Base |
|--:|--:|--:|--:|--:|--:|
| 1 | 76.8 | 75.3 | 87.0 | 1.16 | 1.13 |
| 2 | 67.6 | 74.6 | 89.0 | 1.19 | 1.32 |
| 3 | 70.5 | 76.3 | 86.2 | 1.13 | 1.22 |
| 4 | 72.5 | 70.7 | 84.5 | 1.20 | 1.17 |
| 5 | 76.3 | 75.5 | 92.5 | 1.22 | 1.21 |
| 6 | 78.1 | 78.3 | 89.3 | 1.14 | 1.14 |
| **median** | **74.4** | **75.4** | **88.0** | **1.17** | **1.19** |

The median of `Off / Base` is 1.00, with a range of 0.98 to 1.10.

The median start-to-completion time, by Little's law, falls from 390 ms to
328 ms.

**The in-flight gate withholds two lean runs.** `e2e_bench` publishes a
closed-loop rate only when the generator holds 90% of its 32 workflows in
flight. Faster workflows lower the in-flight count, as
[the issue #2680 page](performance-claim-window-split.md#paired-runs)
explains. The lean runs held 28.7 to 28.9, and rounds 5 and 6 fell below
the gate. The other arms held 29.3 to 29.6. So the generator bounds the
lean arm at this target, and the table understates it. `paired3.sh`
computes the rate from the raw rows of the cell, with the headline
formula.

### Census

One census run of each arm, back to back, with `track_planning = on`
([`on-claim-forms.txt`](perf-artifacts/claim-lean-tier/on-claim-forms.txt),
[`off-claim-forms.txt`](perf-artifacts/claim-lean-tier/off-claim-forms.txt)):

| | Lean off | Lean on |
|:--|--:|--:|
| statements | 320,740 | 316,914 |
| planner runs | 10,015 | 9,103 |
| execution time | 22,968 ms | 21,412 ms |
| claims, lean form | 0 | 10,117 |
| claims, window form | 10,125 | 33 |
| claims, full form | 6 | 3 |
| census time per call, lean form | | 0.669 ms |
| census time per call, window form | 0.769 ms | |

The lean form answers almost every claim of this cell. The 33 window
statements are the claims after a lean flag and the claims that each flag
skips.

The census time per claim falls by 13%. The rest of the gain is in
executor startup, which the census does not count.

### Profile

Two profiles of each arm, in turn
([`perf-phases.txt`](perf-artifacts/claim-lean-tier/perf-phases.txt)).
Each value is a share of all samples of the box:

| Symbol | Lean off | Lean on |
|:--|--:|--:|
| `standard_ExecutorStart` | 12.8%, 13.1% | 9.9%, 10.1% |
| `ExecInitNode` | 13.1%, 13.2% | 9.5%, 9.8% |
| `ExecInitNestLoop` | 5.1%, 4.9% | 2.2%, 2.3% |
| `ExecInitAgg` | 1.5%, 1.7% | 0.6%, 0.6% |
| `ExecSetParamPlan` | 8.1%, 7.4% | 6.1%, 6.3% |
| `standard_ExecutorEnd` | 2.5%, 2.5% | 2.0%, 2.0% |
| `EvalPlanQualBegin` | 1.1%, 0.9% | 0.4%, 0.4% |
| `standard_ExecutorRun` | 25.9%, 24.7% | 24.8%, 24.7% |
| `GetCachedPlan` | 4.7%, 5.9% | 7.8%, 7.8% |

These are shares of a box that completes more workflows in the same time.
So the startup cost of one workflow falls by more than the shares show.
`EvalPlanQualBegin` falls by more than half: a recheck after a peer commit
now starts the subplans of a smaller statement.

`GetCachedPlan` rises, and the planner under it (`pg_plan_queries`) holds
5.1% and 4.9% of the lean profiles against 2.1% and 3.3%. See
[item 6](#what-this-audit-did-not-change) below.

## 4. The skip

A lean statement that sets its flag is a cost with no gain: the window
form runs after it. A queue with a keyed row at its head would pay it on
each claim. So each claim loop keeps a **skip**. After a flag, the next
claims of the loop send the window form first.

**The first rule was wrong for a loop of mixed claims.** It kept one skip
for the loop. A flag doubled the skip, and a lean claim that took a row
ended it. A worker picks the claim kind for each poll: any kind when both
slot pools have room, and workflow rows only when the activity pool is
full. With one keyed activity row at the head of a queue, each lean claim
of any kind sets its flag, and each lean claim of workflow rows takes a
row. Each such claim ended the skip, so the skip never grew past 2
claims. A review of the change found this, and timed it.

**The shipped rule** keeps one skip for each claim kind, and for each
queue of a worker with queue weights. Each flag adds 4 claims to the
skip, up to 64. Each lean claim that takes a row takes 1 claim off the
next skip. So at least 4 window-first claims of that skip follow each
flag.

[`mixed-kinds/run.sh`](perf-artifacts/claim-lean-tier/mixed-kinds/run.sh)
replays a loop of 1,200 claims against the shipped texts, with a keyed
activity row at its cap at the head of the queue. One `psql` session
sends each statement as its own round trip. `N` is a claim of any kind and
`W` is a claim of workflow rows
([`mixed-kinds/counts.txt`](perf-artifacts/claim-lean-tier/mixed-kinds/counts.txt),
[`mixed-kinds/timing.txt`](perf-artifacts/claim-lean-tier/mixed-kinds/timing.txt)):

| Loop | Rule | Claim statements | Wall time, median of 6 | Against lean off |
|:--|:--|--:|--:|:--|
| `N W` in turn | lean off | 1,200 | 1,714 ms | |
| | the first rule | 1,600 | 2,029 ms | slower in 6 of 6 |
| | the shipped rule | 1,217 | 1,588 ms | faster in 5 of 6 |
| `N W W` in turn | lean off | 1,200 | 1,659 ms | |
| | the first rule | 1,600 | 1,773 ms | slower in 5 of 6 |
| | the shipped rule | 1,214 | 1,383 ms | faster in 6 of 6 |

The statement counts are exact, and the script checks the answer of each
statement. The times are from a shared box.

The kernel of the skip is three small functions in
`queue::claim_window`: `lean_first`, `lean_skip_span` and
`lean_skip_after`. `formal/verus/src/claim_window.rs` holds the proved
copy. It proves that the skip stays in its bound, that at least 4 claims
follow each flag, that the lean form returns within the cap, and that no
skip state changes the claimed row.

## What this audit did not change

Each item below is a cost or a limit that this change leaves in place. An
item with no table on this page is an estimate from reading and from small
probes, not a measurement of the cell.

**1. The lean flag over-approximates.** A parked row of a paused run and a
window that peer claims hold both set it. An exact flag needs the parts
that the lean form drops. The skip bounds the cost.

**2. The batch claim stays off by default.** One batch claim takes up to
8 rows in one statement. [Its page](performance-claim-batch.md) measures
the gain and records why the knob is off by default. A change of that
default is a product decision, not a tuning step.

**3. A continued workflow task still goes through the queue.** An activity
completion wakes the workflow task, and a claim loop then claims it. A
direct handoff to a free slot of the same worker would save one claim for
each continuation. It needs the gates of the claim in a second place, and
its gain falls with the number of workers of the fleet.

**4. The timeout refill page plans on each poll tick.**
[Issue #2654](performance-hot-path-round-trips.md#what-stays) records why
its text stays unnamed. It is a cost of the bench poll interval. At the
default interval it is small.

**5. The claim still sends its `BEGIN` batch as its own round trip.** A
pipelined claim would save one round trip for each claim. A claim that is
cancelled between the two messages would then need a new cleanup path.

**6. The planner holds a larger share of the lean profiles.** The census
of a whole run shows the opposite: 9,103 planner runs against 10,015, and
2,993 ms of planning against 3,328 ms. Each profile samples 10 s of a run,
so the window can fall on a different part of it. This audit did not find
the cause.

Four candidates did not survive a check and are not recorded as findings:
a pipeline of the activity start and completion writes, a `WHEN` clause on
the external outbox trigger, a named statement for the workflow start
insert, and a claim transaction of two round trips.

## Reproducing

Dump the claim texts of a build, then time one claim of each form:

```bash
HARVEST_CLAIM_TEXT_DIR=<texts> cargo test -p autumn-harvest --features db,testing \
  --test integration lean_claim_db_tests::write_every_windowed_claim_text -- --ignored --exact
```

```bash
bash docs/perf-artifacts/claim-lean-tier/pgbench.sh <texts> <outdir> 12 5
```

`pgbench.sh` and `mixed-kinds/run.sh` copy a migrated database. `TEMPLATE`
names it, and the default is `harvest_schema`.

Run the three arms against a server with `pg_stat_statements` loaded:

```bash
bash docs/perf-artifacts/claim-lean-tier/paired3.sh <base-bin> <new-bin> <outdir> 6
```

Run one census of each arm with
[`census.sh`](perf-artifacts/hot-path-round-trips/census.sh), with
`HARVEST_BENCH_CLAIM_LEAN=0` in the environment for the `off` arm. Run
[`claim_forms.sql`](perf-artifacts/claim-window-split/claim_forms.sql)
before the next reset.

Profile one run of each arm, and replay the loop of mixed claim kinds:

```bash
bash docs/perf-artifacts/claim-lean-tier/profile.sh off-1 <new-bin> <outdir> HARVEST_BENCH_CLAIM_LEAN=0
```

```bash
bash docs/perf-artifacts/claim-lean-tier/mixed-kinds/run.sh <texts> <outdir> 6 1200 NW NWW
```

The scripts of the rewrite runs are in
[`perf-artifacts/claim-lean-tier/rewrites/`](perf-artifacts/claim-lean-tier/rewrites/).
`mk.py` builds each text from `texts-before/`, and its `all` output is
byte-identical to the shipped lean text.

The tests of the change:

| Test | Proves |
|:--|:--|
| `formal/verus/src/claim_window.rs` | a lean claim takes the row of the window form for every input of the model, and the skip stays in its bound |
| `queue::claim_window::tests::lean_claim_sql_holds_for_every_variant` | each of the twelve lean texts keeps or replaces each window gate, reads no new bind and returns its flag |
| `lean_claim_db_tests::random_states_claim_the_same_row_through_either_first_form` | both orders take the same row over random queue states, for each variant, in owner mode and with a closed fence |
| `lean_claim_db_tests::random_drains_take_the_same_rows_with_and_without_the_lean_form` | a drain through the public claim takes the same rows in the same order |
| `lean_claim_db_tests::a_lean_claim_takes_the_next_row_after_a_peer_commits_the_head_row` | the recheck after a peer commit |
| `lean_claim_db_tests::a_loop_of_two_claim_kinds_keeps_one_skip_for_each_kind` | the statement counts of an `N W` loop of 600 claims, through `queue::claim_task_with_lean` |
| `lean_claim_db_tests::a_weighted_worker_keeps_the_lean_form_on_its_plain_queue` | one skip for each queue of a weighted worker |
| `queue::claim_window::tests::a_lean_fallback_is_one_claim_in_five_at_most` | the bound of the skip, over each sequence of answers |
| `lean_claim_db_tests::the_lean_form_keeps_its_plan_on_a_deep_backlog_of_one_queue` | a `Tid Scan` candidate and bounded head scans on a deep backlog |
| `lean_claim_db_tests::the_committed_census_scripts_name_the_lean_marker` | `claim_forms.sql` counts the lean form |
