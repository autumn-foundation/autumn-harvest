# Design — Issue #2677: the DLQ depth gauge reads every dead letter

`dlq::dead_letter_count` feeds the `harvest.dlq.entries` gauge. It was an
exact `SELECT COUNT(*) FROM harvest_dead_letters`. At 1,000,000 dead letters
one call read 744,158 buffers
(`docs/perf-artifacts/worker-sampler-cadence/baseline-statement-costs.md`) in
145 ms (`baseline-large-explain.txt`).
Each worker sends it once per gauge interval (15 s) for each assigned shard.

The fix is option A of the issue: a counter table that triggers keep exact.
The gauge read is then a sum over a few rows. Section 0.5 records how review
changed the first design.

**One migration. No new `WorkflowEvent` variant. No route change. No new
metric. No new grant.**

---

## 0. Planning record

### 0.1 Brainstorm — how can the gauge read stop scaling with the DLQ?

| # | Idea | Verdict |
|---|------|---------|
| B1 | Option A: one counter row per shard, kept by a trigger. | **Adopted, with B5 and B6.** The read is constant. |
| B2 | Option B: `pg_class.reltuples` above a threshold. | Rejected. An estimate can hide a burst of 6, and the alert reads a difference of 5. |
| B3 | Option C: one elected sampler per shard (#2679). | Rejected here. The cost of each call stays. It is independent of this change. |
| B4 | Keep the count in worker memory, from the DLQ write sites. | Rejected. Each worker sees only its own writes. Retention, the API and the CLI also write the table. |
| B5 | A statement trigger with a transition table, not a row trigger. | **Adopted.** A bulk delete of 1,000 rows writes the counter once, not 1,000 times. |
| B6 | Pick a counter row with `FOR UPDATE SKIP LOCKED`. Insert a row when every row is locked. | **Adopted.** A writer never waits for a counter row. So the counter adds no lock wait and no deadlock. |
| B7 | A delta log: each write inserts `+n` or `-n`, and a compactor folds the rows. | Rejected at first: the read scales with the rows since the last fold. **Adopted after review** (section 0.5), with folds by the writers and the sampler. |
| B8 | A fixed set of rows keyed by `pg_backend_pid() % N`. | Rejected. Two backends with the same key wait for each other. That wait can close a deadlock with the execution row locks of a failure transaction. |
| B9 | A SQL function that repairs the counter from an exact count. | **Adopted.** The migration seeds with it. An operator repairs a drift with it. |

### 0.2 Reverse brainstorm — how can this change do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | The counter row becomes a lock that every failure transaction waits for. A flood of failures then runs one at a time. | A writer locks no counter row (B7 after review). A test holds many open writers at once and needs each insert to end at once. |
| R2 | The counter wait closes a deadlock with the execution and parent row locks of a failure transaction. | A writer never waits for a counter row, so it cannot close a cycle. |
| R3 | The rows grow without limit. | The writers fold with a chance of 1 in 32, and the sampler folds each gauge interval. A test writes 2,000 times and checks the row count. The recount leaves one row. |
| R4 | A bulk delete or a retention pass writes the counter once per row. | B5. One delta row per statement. |
| R5 | `TRUNCATE` bypasses row and statement delete triggers. | An `AFTER TRUNCATE` trigger empties the counter. |
| R6 | A role without a grant on the new table writes a dead letter. The trigger then fails, and so does the failure transaction. | The functions are `SECURITY DEFINER` (V3). No role needs a grant on the counter. |
| R7 | The migration counts the table while writers add rows. The seed is then wrong. | The migration takes `SHARE ROW EXCLUSIVE` on `harvest_dead_letters` before it counts, so no write runs between the count and the triggers. `lock_timeout` is 5 s. |
| R8 | An operator disables the triggers, or a session sets `session_replication_role = replica`. The counter drifts. | `SELECT harvest_dead_letter_recount();` repairs it. The runbook says so. The sampler warns on a negative count. |
| R9 | A data-only restore with the triggers on counts each dead letter twice. A restore of `harvest_dead_letters` alone leaves the counter behind. | The backup runbook tells the operator to run the recount after such a restore. A full restore creates the triggers after it loads the data, so it agrees. |
| R10 | Logical replication (cross-region DR) applies rows with triggers off. | A `FOR ALL TABLES` publication carries the counter table too, and its primary key gives it a replica identity. `harvest dr promote` recounts (V6). |
| R11 | A writer in a `REPEATABLE READ` transaction updates a counter row that a later commit changed. Postgres then raises a serialization failure. | A writer only inserts (V2). |
| R12 | The count is an estimate, so the alert misses a burst. | The counter commits with its dead letters. A reader sees the exact committed count. A test drives the real sampler and checks the alert difference. |
| R13 | Each dead letter costs more. | The harness measures the insert and the delete before and after. See `docs/performance-dlq-depth-counter.md`. |

### 0.3 Six thinking hats

- **White (facts).** One read cost 744,158 buffers at 1M dead letters. Every
  worker sends it for each assigned shard every 15 s. Dead letters arrive
  only on a terminal failure. The worker, the poison-pill reclaimer and the
  callback scanner write them. The DLQ API, the CLI, a callback redrive and
  retention delete them.
- **Red (feelings).** A gauge that reads the whole table each 15 s feels
  wrong for a table that grows in an incident, when the database is already
  under load.
- **Black (risks).** R1 to R13. The largest risks are a new lock wait on the
  failure path and a grant that a least-privilege role lacks. Review added
  V1 to V9.
- **Yellow (benefits).** The read is constant. Every caller of
  `dead_letter_count` gets the same benefit: the preflight, the shard health
  probe and the status summary. The count stays exact, so the alert keeps its
  meaning.
- **Green (ideas).** The same pattern can serve another exact gauge over a
  large table. Option C (#2679) can still cut the call count.
- **Blue (process).** Capture the write cost before the change. Write the
  failing tests. Add the migration and the read. Capture the costs again.
  Update the guide, the runbook, the Ledger profile and the changelog.

### 0.4 Decision (first version)

Adopt B1 with B5, B6 and B9: one counter row per writer, picked with
`FOR UPDATE SKIP LOCKED`. Section 0.5 replaces B6 with B7.

### 0.5 Review record

Four reviewers read the first version: Postgres concurrency, Rust and tests,
documentation, and integration. They found these defects.

| # | Finding | Resolution |
|---|---------|------------|
| V1 | Each counter update leaves a dead row version. While a long transaction holds the xmin horizon, each writer scans them. A reviewer measured 589 pages and half the throughput after 90,000 writes. | B7: a writer only inserts a delta row. It reads no counter row, so a held snapshot does not slow it. The harness now measures that case. |
| V2 | At a `REPEATABLE READ` session default, the counter update raises 40001 when another writer committed first. The engine guards against that default in other places. | B7 again: an insert never conflicts. A writer at `SERIALIZABLE` does not fold. `writers_above_read_committed_never_conflict_on_the_counter` proves both levels. |
| V3 | The trigger ran as the writer. So each writing role needed a new grant, and an older binary failed each dead-letter write after the migration. | The trigger, the fold and the depth read are `SECURITY DEFINER`. `a_role_without_a_counter_grant_writes_and_reads_the_depth` proves it. |
| V4 | `SET search_path` without `pg_temp` lets a temporary table shadow the counter. | Each function lists `pg_temp` last. |
| V5 | At `REPEATABLE READ`, a seed or a recount reads a snapshot from before its lock, and misses rows that committed in the wait. | The migration runner pins `READ COMMITTED`. The recount raises at any higher level. |
| V6 | A logical standby applies rows with the triggers off, and its own migration seeds a row. So its counter is wrong after a promotion. | `harvest dr promote` runs the recount. |
| V7 | The preflight `dlq_read_access` check read the counter, not the DLQ. | It reads one row of `harvest_dead_letters` too. |
| V8 | The new suite had no row in `.github/ci/integration-suites.txt`, so CI never ran it. | Added. |
| V9 | The docs cited the wrong baseline file, an uncommitted second run and an unmeasured seed time. | The perf page cites only committed artifacts. |

### 0.6 Decision

Adopt B1 with B5, B7 and B9.

- `harvest_dead_letter_counts (slot UUID PRIMARY KEY, entries BIGINT)`. The
  sum of `entries` is the count of `harvest_dead_letters`.
- `harvest_dead_letter_count_apply()` runs after each `INSERT` and `DELETE`
  statement, with its transition table, and after `TRUNCATE`. It inserts one
  delta row. With a chance of 1 in 32 it folds, except at `SERIALIZABLE`.
- `harvest_dead_letter_fold()` folds the free rows into one. The DLQ sampler
  calls it each gauge interval.
- `harvest_dead_letter_recount()` locks the table, counts it and leaves one
  row. Only the owner runs it.
- `dead_letter_count` reads `dlq::DLQ_DEPTH_SQL`, which calls
  `harvest_dead_letter_depth()`.

## 1. Acceptance criteria

1. At 50,000 dead letters, a warm gauge statement reads no row of
   `harvest_dead_letters` and at most 16 buffers.
2. After each write path the gauge equals `COUNT(*)`. The paths are a
   single insert, a multi-row insert, a batch discard, an empty delete, an
   update, a rollback and a `TRUNCATE`.
3. Many open writers each insert a dead letter. No insert waits for
   another. After a mix of commits and rollbacks the gauge is exact.
4. After a drift, `harvest_dead_letter_recount()` makes the gauge exact and
   leaves one counter row. It refuses a level above `READ COMMITTED`.
5. Run on a table with rows, the migration seeds the gauge with the exact
   count. `down.sql` removes the counter.
6. The real DLQ sampler emits a series whose growth over the window is more
   than 5 after 6 new dead letters. That is the `harvest_dlq_growth` rule.
7. The harness records the read cost and the write cost before and after, at
   10,000, 100,000 and 1,000,000 dead letters.
8. Writers at `REPEATABLE READ` and `SERIALIZABLE` never conflict on the
   counter. A role with no counter grant writes, deletes and reads the depth.
9. After 2,000 writes the counter holds few rows, and a fold leaves one.
10. The upgrade guide, the runbooks, the telemetry table, the Ledger profile
    and a changelog fragment describe the change.
