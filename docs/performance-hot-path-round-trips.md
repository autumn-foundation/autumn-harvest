# Hot path round trips (issue #2654)

The [statement cache audit](performance-hot-path-statement-cache.md) left
six measured costs in place. This page fixes four of them and two in part.
It keeps the rest, each with its reason.
[`design/DESIGN-2654.md`](design/DESIGN-2654.md) holds the plan.

> **This is a reference measurement, not an SLO.** It comes from one 4-core
> box with the database, the worker and the load generator on it.
> Reproduce it before you design against it.

## TL;DR

* **The `throughput` cell moves from 71.8 to 78.5 workflows/s.** That is
  the median of six alternating pairs. The changed build is faster in six
  of six, by 1.01 to 1.13 times.
* **One run sends 6% fewer statements and plans 31% less often.** The
  census reads 328,863 statements and 13,564 planner runs for the base,
  and 307,677 and 9,387 for the changed build.
* **Only the claim pipeline moves the cell on its own** (median ratio
  1.05). Each other item alone stays inside the noise of the box. See
  [Each item alone](#each-item-alone).
* **A claim that lands on a row takes four round trips, not six.** The
  queue lock and the rechecks go in one pipeline. The server still runs
  them in order, each with its own snapshot.
* **Kept:** the execution and history reads of a decision, its second
  history load and its unlocked pause read, and four poll-tick scanners.
  See [What stays](#what-stays).

## The method

The workload is the `throughput` cell of `e2e_bench` at one shard, as in
the [audit](performance-hot-path-statement-cache.md#the-method). It runs
one in-process worker with 8 workflow slots, 16 activity slots and 2 claim
loops. 32 workflows are in flight.

Host: 4 logical CPUs, 15 GB. PostgreSQL 16 on the same host, with
`fsync = off`, `synchronous_commit = off` and `max_connections = 200`, as in
`benchmarks/docker-compose.yml`, and `pg_stat_statements` preloaded. The
base is `trunk-dev` at `a0045b6`. Measured on 2026-11-12.

* **Census.** One run each with `track_planning = on`. The query is
  [`census.sql`](perf-artifacts/hot-path-statement-cache/census.sql).
* **Paired runs.** Each pair runs both builds back to back, and the order
  swaps from pair to pair, so drift of the box hits both builds alike.
  `track_planning = off`, no commit probe.
* **Per item.** Each item alone on the base, as its own build, with its own
  paired runs. See [Each item alone](#each-item-alone).

Raw output: [`perf-artifacts/hot-path-round-trips/`](perf-artifacts/hot-path-round-trips/).
Only ratios measured on one box compare.

## The changes

### 1. The claim rechecks go in one pipeline

A claim sent the queue lock, the queue-pause recheck and the type recheck
one after another. Each waited for the reply of the one before. Now
`queue::apply_post_claim_rechecks` sends them as one pipeline through a
shared `&AsyncPgConnection`. A capped key adds its recheck to the same
pipeline.

**The order needs a proof, because the queue lock must run before the
snapshot of the pause recheck (issue #619).** Two facts of the driver give
it:

* The driver sends a statement on the first poll of its future, and
  `join4` polls its futures in argument order.
* A statement that is not prepared yet holds the statement cache lock while
  it prepares. That lock is fair. So the next statement waits, and it sends
  after the one before it.

`claim_recheck_pipeline_db_tests` proves the order on the server. A trigger
records whether the session holds the shared queue lock when the pause
recheck releases the row. It runs on a cold connection, a warm connection
and one that prepared the recheck before the lock. With the lock moved
after the recheck, three of its five tests fail
([`red-phase-tests.txt`](perf-artifacts/hot-path-round-trips/red-phase-tests.txt)).

**Each recheck now also runs when an earlier one gave the row back, or when
the lock failed.** A release requires `state = 'RUNNING'`, so a later
release changes nothing. A recheck can only give the row back, so one that
runs without the lock is safe. When the lock fails and no recheck released
the row, the claim gives it back itself, one round trip more on that rare
path. `formal/verus/src/claim_recheck.rs` proves that the pipeline keeps a
row exactly when the sequential rechecks keep it.

No statement text changes. The census shows the same statements at the
same counts.

### 2. The wake ingest reads timers and signals in one pipeline

`ingest_due_timers_and_signals` read the due timers, then the pending
signals. The two reads are independent, so they now go in one pipeline.
The server runs them in the same order, each with its own snapshot.

### 3. The suspension persist opens a savepoint only when it needs one

`persist_scheduled_activities` ran its body in a savepoint inside the
persist transaction: a `SAVEPOINT` and a `RELEASE` for each decision. The
savepoint matters for one path only. `handle_suspended_workflow` recovers
from a full tenant quota or an unavailable shard, then commits. The
savepoint discards the events that the body appended before such an error.

Within the body, two steps raise those errors: a detached child spawn, and
the cancel of a losing child of a race on a moving shard. So the savepoint
now opens only for a batch with one of those, or for a fork. A fork reads a
second history in the body. Any other error rolls back
the whole persist transaction, so it needs no savepoint.

One report changes. With no savepoint, a failed statement aborts the
persist transaction, and the failure write of `fail_execution_on_error`
then fails too. That function now reports the first error when the second
one is the aborted-transaction reply. The caller rolls back either way.

The census shows 1,440 savepoints per run, not 5,760. The one that stays
is the terminal persist, one for each workflow.

### 4. The notify sender reads and sends in one statement

Each tick of the sender read the commit state of its held notes, then sent
the ready wakes. Now one statement, `notify::read_gate`, does both when
every note is ready:

* Rust builds the wakes of all held notes before the read, with the same
  coalescing as before. Owner notes keep their own lookup.
* The statement sends them only when no status is `aborted` or
  `in progress`, and the server matches the server of every note. The send
  is a scalar subquery in a `CASE` branch, which Postgres runs only when
  the branch is taken.
* Otherwise the statement only reads, and the tick sends the ready wakes
  in a second statement, as before.
* When the combined statement fails, the tick reads again with no send,
  through `notify::read_gate_only`. A
  failed `pg_notify` thus still gives a good read, and the second send
  counts its wakes as lost, as before.

The census shows 14,215 combined statements and 1,611 separate sends per
run. The base sent 14,073 reads and 11,524 sends.

### 5. The low-risk poll-tick scanners stay prepared

The bench polls every 25 ms, so the scanners of the poll tick ran the
planner on every tick. These statements now carry the cache mark:

| Statement | Change |
|:--|:--|
| the three external outbox claims | an explicit column list replaces `e.*` |
| the broken-session scan | an explicit column list replaces `s.*` |
| the start-idempotency purge, the event-batch and debounce due scans, the scanner lease upsert, the throttle probe, the active codec key read and the completion outbox retry scan | the mark only |
| the table probes of the debounce, throttle, event-batch and start-idempotency scanners | the mark only |

Each has constant text. Each partial-index predicate that it needs is a
literal, so the generic plan still proves it. The census shows 76 to 90
planner runs per statement, five for each pooled connection, where each
ran the planner on every tick before.

### 6. The terminal mutex sweep reads its keys once

`mutex::sweep_terminal_holder_and_wake` sent a table probe. Then the lock
sweep sent a probe and a key read, and the waiter sweep did the same. Now it sends one probe and one key read, a `UNION ALL` of the held
keys and the waiter keys. The waiter part skips each key that the lock part
freed, because that free also deleted the waiter row. So the sweep does the
same work as before. `free_held_keys` and `drop_waiter_keys` hold the two
loops, which the old functions now share.

The issue suggested a gate on `mutex::history_records_a_grant`, as the lease
renewal uses. The merged read saves the same statement on every terminal
path, with no history. A cancel or a timeout has no history in scope.

The census shows two statements per workflow, not five.

## The result

### Statement census

| | Base | After |
|--:|--:|--:|
| statements | 328,863 | 307,677 |
| planner runs | 13,564 | 9,387 |
| planning time | 3.5 s | 2.8 s |
| execution time | 21.6 s | 21.6 s |

Each row of [`census-diff.txt`](perf-artifacts/hot-path-round-trips/census-diff.txt)
names one statement that moved. A run polls a few more times in one build
than in the other, so each poll-tick statement moves by about 10% in both
directions.

### Throughput

Six alternating pairs. Raw output:
[`paired-runs.txt`](perf-artifacts/hot-path-round-trips/paired-runs.txt).

| pair | base, workflows/s | changed, workflows/s | changed / base |
|--:|--:|--:|--:|
| 1 | 69.71 | 71.57 | 1.03 |
| 2 | 69.32 | 78.43 | 1.13 |
| 3 | 72.40 | 78.50 | 1.08 |
| 4 | 74.36 | 78.59 | 1.06 |
| 5 | 77.52 | 78.27 | 1.01 |
| 6 | 71.12 | 79.52 | 1.12 |
| **median** | **71.76** | **78.47** | **1.07** |

### Each item alone

Each item alone on the base, four alternating pairs each. Raw output:
[`per-item-paired-runs.txt`](perf-artifacts/hot-path-round-trips/per-item-paired-runs.txt).

| item | base median | item median | ratio per pair | median ratio |
|:--|--:|--:|:--|--:|
| 1. claim pipeline | 68.02 | 71.62 | 1.27, 1.06, 1.04, 0.99 | 1.05 |
| 2. ingest pipeline | 69.15 | 67.66 | 0.93, 1.02, 1.02, 0.96 | 0.99 |
| 3. savepoint gate | 67.05 | 66.53 | 0.95, 1.05, 0.98, 0.95 | 0.97 |
| 4. notify read and send | 69.22 | 66.11 | 0.80, 1.15, 1.07, 0.90 | 0.98 |
| 5. cached scanners | 70.85 | 71.49 | 1.04, 1.06, 0.97, 0.96 | 1.00 |
| 6. mutex sweep | 72.25 | 71.81 | 0.96, 0.98, 1.03, 1.01 | 1.00 |

**Only the claim pipeline moves the cell on its own.** The base itself
read 57.3 to 73.7 workflows/s over these runs, so one run of this box
moves by about 10%. Items 2 to 6 each remove one to six statements per
workflow, and alone they stay inside that noise. Together with item 1 they
give the six of six pairs above. The census shows each of their statement
counts fall, so each is kept for its cost, not for a throughput claim of
its own.

Item 3 ran here without the race-loser case of its savepoint gate. The
bench runs no race, so that case does not change this run.

## What stays

**The execution, history, timer and signal reads of a decision are not one
pipeline.** Only the timer and signal reads are (change 2). In owner mode
the execution row decides which history read runs. An offloaded history
runs more statements to inflate its payloads. And a failed read of each
kind takes its own path: a missing run fails its task, and an owner-mode
load failure re-pends it. A pipeline would have to apply those paths in
the same order, for a gain of one round trip on a cold decision.

**A cold decision still loads its history again after the wake ingest.**
That load also reads events that a concurrent writer appended during the
ingest, such as an activity completion. Without it, the decision persists
at an older event id more often and collides on the event key. The bench
measures one resident decision in four as cold, so the load costs one
statement per workflow there.

**The unlocked pause read before the persist stays.** It parks a paused
run before the non-determinism block, the deadlock requeue, the panic
retry, the cascade reads and the history-cap fail. Without it, each of
those acts on a paused run. A pause during a decision is rare, but each
of those outcomes differs.

**Four poll-tick scanners stay unnamed:**

* The SLA breach update binds `state <> 'PAUSED'` and `sla_breached = false`.
  The partial index `idx_harvest_executions_sla_deadline` needs both as
  literals.
* The two cross-shard child sweeps bind the status and `cancel_requested`
  that their partial indexes need as literals.
* The timeout refill page builds up to 30 texts with `format!`. Its
  predicates read `SELECT * FROM page`, and its `LIMIT` is a bind. A generic
  plan of it needs its own measurement.

The deadline scanner of executions loads `WorkflowExecution`, whose
unmapped columns are `sql::<T>()` fragments. The audit kept that load
unnamed on purpose.

## Reproducing

Run the cell against a server with `pg_stat_statements` loaded:

```bash
HARVEST_BENCH_SHARD_URLS=postgres://postgres:postgres@localhost:5432/postgres \
HARVEST_BENCH_SCENARIOS=throughput HARVEST_BENCH_SHARDS=1 \
cargo bench -p autumn-harvest --features db,testing --bench e2e_bench
```

[`census.sh`](perf-artifacts/hot-path-round-trips/census.sh) runs one cell
and writes the census. [`paired.sh`](perf-artifacts/hot-path-round-trips/paired.sh)
runs the alternating pairs. [`census_diff.py`](perf-artifacts/hot-path-round-trips/census_diff.py)
compares two censuses.

The tests of the change:

| Test | Proves |
|:--|:--|
| `claim_recheck_pipeline_db_tests` | the pause recheck runs under the queue lock in the pipeline, and a failed lock gives the row back |
| `formal/verus/src/claim_recheck.rs` | the pipeline keeps a row exactly when the sequential rechecks keep it |
| `worker::tests::a_plain_fan_out_needs_no_savepoint`, `a_detached_spawn_or_a_fork_keeps_the_savepoint` | the savepoint gate |
| `notify_post_commit_tests` | an aborted write sends no wake, and a failed send is counted |
| `mutex_terminal_sweep_db_tests` | the merged sweep costs two statements and frees the same rows |
| `cached_stmt_db_tests` | a marked statement is prepared once |
