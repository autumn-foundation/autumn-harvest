# Hot path audit: prepared statements and round trips (issue #2651)

This page records an audit of the whole hot path: start, claim, decide,
run an activity, complete. It measures the canonical 3-activity workflow of
[`benchmarks.md`](benchmarks.md) with a statement census, a CPU profile and
a statement trace. It then removes the largest cost that it finds.

> **This is a reference measurement, not an SLO.** It comes from one machine
> with one Postgres configuration. Reproduce it before you design against it.

## TL;DR

* **Postgres planned more than it executed.** In one run of the
  `throughput` cell, 172,247 of 368,753 statements ran the planner.
  Planning took 22.2 s and execution took 18.6 s. Tool:
  `pg_stat_statements` with `track_planning = on`.
* **The cause is the statement cache of the driver, not a slow query.**
  Diesel sends a statement unnamed when it cannot prove that its text is
  constant. That covers every `diesel::sql_query`, every DSL `UPDATE`,
  every batch insert, every insert with an `Option` value and every
  statement with a `sql::<T>()` fragment. An unnamed statement costs two
  round trips, and Postgres parses and plans it on every call.
* **The fix keeps 84 statement sites prepared.** One adapter,
  [`cached_stmt`](../autumn-harvest/src/cached_stmt.rs), marks a
  constant-text statement as safe to cache. Planner runs fall from 172,247
  to 11,795 per run, and planning time from 22.2 s to 2.9 s.
* **The `throughput` cell moves from 47.8 to 82.0 workflows/s.** That is
  the median of six alternating pairs on one box. The changed build is
  faster in six of six, by 1.67 to 1.79 times. The median
  start-to-completion time falls from 574 ms to 351 ms.
* **Each connection holds about 1.1 MB more.** The median backend grows
  from 6.8 MB to 7.9 MB of memory contexts, for its cached plans.
* **Three more changes remove round trips:** one batch for the claim
  `BEGIN` and its planner settings, no lease renewal for a run that never
  took a mutex, and a cached load of `WorkflowExecution`.
* **Four findings are recorded, not fixed.** See
  [What this audit did not change](#what-this-audit-did-not-change).

## The method

The workload is the `throughput` cell of `e2e_bench` at one shard: one
in-process worker with 8 workflow slots, 16 activity slots and 2 claim
loops, with 32 workflows in flight. Each run completes 1,440 workflows,
warmup included.

Host: 4 logical CPUs (Intel Xeon @ 2.10 GHz), 15 GB. PostgreSQL 16.15 on
the same host, with `fsync = off`, `synchronous_commit = off` and
`max_connections = 200`, as in `benchmarks/docker-compose.yml`. The base is
`trunk-dev` at `05e0013`. Measured on 2026-11-11.

Three tools, each for one question:

| Question | Tool |
|:--|:--|
| Which statements run, how often, and at what cost? | `pg_stat_statements`, [`census.sql`](perf-artifacts/hot-path-statement-cache/census.sql) |
| Where does the CPU go? | `perf record -a`, 299 Hz, DWARF call graphs |
| In what order does one workflow send its statements? | `log_min_duration_statement = 0`, one workflow in flight |

Raw output:
[`perf-artifacts/hot-path-statement-cache/`](perf-artifacts/hot-path-statement-cache/).

Only ratios measured on one box compare. Do not compare the absolute
numbers with another page.

## 1. What the base spends

**The box is busy, and Postgres is most of it.** `vmstat` reads 72% to 76%
busy over the measured loop, with a run queue of 4 to 14. A system-wide
profile puts 74% of its samples in `postgres` and 20% in the worker
threads.

**Inside Postgres, planning costs as much as running.** In the same
profile, `GetCachedPlan` and the planner below it take 22% of all samples.
`PortalRun` takes 22%. Parse analysis takes 5% more.

**One workflow sends 256 statements.** The census, per completed workflow:

| | Base | After |
|:--|--:|--:|
| statements | 256 | 243 |
| planner runs | 120 | 8 |
| planning time, ms | 15.4 | 2.0 |
| execution time, ms | 12.9 | 13.3 |

**A bare round trip is cheap. An unnamed statement is not.** `pgbench`
reads 47 µs for `SELECT 1` on this host. The same statement through the
engine pool reads 102 µs. The claim transaction had seven round trips and
took 5.4 ms, of which the server ran statements for 0.8 ms.

### Why a statement runs unnamed

`diesel-async` asks each statement whether its prepared form is safe to
cache. A statement that answers no goes through `prepare_typed`, then
`query_raw`, then a `Close`. That is a Parse and a Describe in one round
trip, and a Bind and an Execute in a second one. Postgres plans it with
the bind values each time.

Diesel answers no for five forms:

| Form | Why diesel refuses | Hot examples |
|:--|:--|:--|
| `diesel::sql_query` | the text can come from anywhere | the three claim rechecks, park, wake, re-pend |
| any DSL `UPDATE` | a changeset can skip a `None` field | task completion, the handler-start marker, the terminal execution update |
| a batch insert | the row count is part of the text | every `harvest_events` append |
| an insert with an `Option` value | `None` renders `DEFAULT` | every task enqueue, every workflow start |
| a `sql::<T>()` fragment | the text can come from anywhere | every load of `WorkflowExecution` |

The last row is a regression of the table split (issue #2492). The four
unmapped columns of `harvest_workflow_executions` became `sql::<T>()`
fragments in the select list of the model. Every load of an execution then
ran unnamed: 12 loads per workflow, 0.2 ms of planning each.

`queue::CachedClaimQuery` (issue #1971) fixed this for the claim statement
alone. 44 other statements of the measured path had `plans = calls`. The
full list is
[`baseline-replanned-every-call.txt`](perf-artifacts/hot-path-statement-cache/baseline-replanned-every-call.txt).

## 2. The changes

### Keep constant-text statements prepared

[`cached_stmt::CachePreparedDsl::cache_prepared`](../autumn-harvest/src/cached_stmt.rs)
wraps a statement and reports it as safe to cache. The cache key is the
SQL text and the bind types, so two texts never share a statement.

The module states three rules for a marked statement:

1. **The text comes from a small fixed set.** Nothing evicts a cached
   statement. A batch insert is cached up to 8 rows. A task insert is
   cached for a single row only, because each `None` changes its text.
2. **The result columns are named.** A cached statement that returns `*`
   fails with `cached plan must not change result type` after a migration
   adds a column. A debug build refuses such a statement, so the test
   suite finds one at its call site.
3. **The generic plan is acceptable.** Postgres plans the first five runs
   with the bind values. It then keeps the generic plan only when its
   estimate is lower. Most marked statements find one row by primary key.

[`cached_stmt::ConstSql`](../autumn-harvest/src/cached_stmt.rs) is a
`sql::<T>()` fragment with a type of its own. A statement that holds one
stays cacheable with no mark. It now carries the four unmapped execution
columns, the `RETURNING` stamp of each notifying write, `NOW()` in the
due-timer reads and the stored-bytes expressions.

No statement text changes. Each statement sends the same SQL and the same
binds as before.

### One batch for the claim `BEGIN` and its settings

A claim sent `BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED`, then its
planner settings as a second batch. `queue::CLAIM_BEGIN_WITH_SETTINGS_SQL`
sends both in one batch. The statements and their order are the same. Each
claim saves one round trip, and a workflow claims 7 times.

### No lease renewal for a run with no mutex

Each decision renewed the mutex leases of its run: a table probe and an
`UPDATE`. For a run that holds no mutex, the `UPDATE` changes no row.
`mutex::history_records_a_grant` now gates the renewal on a
`MutexGranted` event in the loaded history.

The gate is exact, not a heuristic. One statement inserts a lock row, one
function runs it, and its one caller appends `MutexGranted` in the same
transaction. Only the decision of a run can grant to that run. So a history
with no grant proves that the renewal would change no row. A unit test
guards that premise against a second writer of lock rows.

## 3. The result

### Statement census

One run each, `track_planning = on`:

| | Base | After |
|:--|--:|--:|
| statements | 368,753 | 349,602 |
| planner runs | 172,247 | 11,795 |
| planning time | 22.2 s | 2.9 s |
| execution time | 18.6 s | 19.1 s |

The planner runs that stay are the first five runs of each statement on
each connection, and the scanners of the poll tick, which this change
leaves unnamed.

### Throughput and latency

Six alternating pairs, `track_planning = off`, no commit probe. Each pair
runs both builds back to back, and the order swaps from pair to pair, so
drift of the box hits both builds alike. Raw output:
[`paired-runs.txt`](perf-artifacts/hot-path-statement-cache/paired-runs.txt).

| pair | base, workflows/s | changed, workflows/s | changed / base |
|--:|--:|--:|--:|
| 1 | 43.95 | 78.70 | 1.79 |
| 2 | 49.95 | 83.80 | 1.68 |
| 3 | 47.14 | 80.59 | 1.71 |
| 4 | 48.53 | 83.56 | 1.72 |
| 5 | 47.15 | 78.62 | 1.67 |
| 6 | 49.26 | 83.41 | 1.69 |
| **median** | **47.84** | **82.00** | **1.70** |

The replay cell of the bench is its noise control. It read 6.4 M to 10.4 M
events/s over the twelve runs, so the box was not quiet. Pair 3 ran in the
slowest window, and both of its builds read low there. The ratio of each
pair is steadier than either column.

The commit probe of the bench, one run each:

| | Base | After |
|:--|--:|--:|
| workflow claim, mean | 5.43 ms | 3.26 ms |
| decision persist, p50 | 4.13 ms | 2.67 ms |
| start to completion, p50 | 574 ms | 351 ms |
| start to completion, p99 | 694 ms | 416 ms |

The cell is a closed loop of 32 workflows, so its rate is that population
divided by the workflow latency. The claim latency sets most of it: two
claim loops, seven claims per workflow.

**A third claim loop does not help.** With three loops the rate stays at
77 workflows/s, and each claim slows from 3.7 ms to 5.2 ms. The box is the
limit, so the next gain is fewer statements, not more loops.

### The cost: memory for cached plans

Each cached statement keeps its plan in the backend of its connection.
`pg_log_backend_memory_contexts` on 12 worker backends, 13 s into each
paired run, reads a median of 6.8 MB for the base and 7.9 MB for the
changed build. That is about 1.1 MB for each pooled connection.

### No plan got worse

The mean execution time of each statement was compared before and after.
The writes that take a row lock read up to 0.1 ms slower. That is lock
wait at a higher rate, not a plan change: each one finds its row by
primary key. The claim statement did not change.

## What this audit did not change

Each item below is a measured cost that this change leaves in place.

**1. A NOTIFY does not cut a settle sleep.** After an empty poll that a
NOTIFY caused, a claim loop sleeps 50 to 75 ms (issue #2422). A NOTIFY
that arrives in that sleep waits for its end. In the statement trace, a
workflow that starts on an idle worker waits 65 ms for its first claim.
This is the open signal p99 of 80 to 85 ms in
[`benchmarks/results-v0.8.0.md`](benchmarks/results-v0.8.0.md). The sleep
also limits empty polls in a fleet, where each NOTIFY wakes every worker.
A change needs the capacity cell with 4 worker processes, so it is not
part of this page.

**2. A claim still sends six round trips.** After the `BEGIN` batch come
the claim, the queue lock, two rechecks and the `COMMIT`. The lock and the
rechecks could go in one pipeline. The queue lock must run before the
snapshot of the pause recheck (issue #619), so the order of a pipeline
needs its own proof.

**3. A decision reads four things in four round trips.** It loads the
execution, the history, the due timers and the pending signals one after
another. A cold decision loads the history twice. It also reads the state
of the run with no lock, as a fast path for a pause, just before the
persist transaction reads it again under a lock.

**4. Each commit that wakes a task costs two more statements.** The notify
sender reads the commit state of its held notes, then sends them. One
statement could do both. The sender runs 9 to 10 times per workflow.

**5. The scanners of the poll tick run unnamed.** They are most of the
planning time that stays, because the bench polls every 25 ms. A deployment with the
default poll interval runs them far less often. One of them returns `e.*`,
so it needs a column list before it can be cached.

## Reproducing

Run the cell against a server with `pg_stat_statements` loaded:

```bash
HARVEST_BENCH_SHARD_URLS=postgres://postgres:postgres@localhost:5432/postgres \
HARVEST_BENCH_SCENARIOS=throughput HARVEST_BENCH_SHARDS=1 \
HARVEST_BENCH_COMMIT_PROBE=1 \
cargo bench -p autumn-harvest --features db,testing --bench e2e_bench
```

Reset the statistics before the run, and read
[`census.sql`](perf-artifacts/hot-path-statement-cache/census.sql) after
it. The bench drops its database, and `pg_stat_statements` keeps the rows
of a dropped database.

The tests of the change:

| Test | Proves |
|:--|:--|
| `cached_stmt::tests` | the wrapper renders the same text and binds, and refuses `*` |
| `cached_stmt_db_tests` | a marked statement is prepared once, a load of `WorkflowExecution` is cached, and a cached statement survives an added column |
| `mutex::pure_tests::the_grant_has_one_caller_and_it_appends_the_event` | the premise of the lease renewal gate |
| `mutex_tests::a_decision_of_a_holder_renews_its_lease` | a holder still renews |
