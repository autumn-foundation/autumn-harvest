# Design — Issue #2677: the DLQ depth gauge reads every dead letter

`dlq::dead_letter_count` feeds the `harvest.dlq.entries` gauge. It was an
exact `SELECT COUNT(*) FROM harvest_dead_letters`. At 1,000,000 dead letters
one call read 744,158 buffers in 145 ms
(`docs/perf-artifacts/worker-sampler-cadence/baseline-statement-costs.md`).
Each worker sends it once per gauge interval (15 s) for each assigned shard.

The fix is option A of the issue: a counter table that triggers keep exact.
The gauge read is then a sum over a few rows.

**One migration. No new `WorkflowEvent` variant. No route change. No new
metric. One new grant.**

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
| B7 | A delta log: each write inserts `+n` or `-n`, and a compactor folds the rows. | Rejected. The read then scales with the write rate, and a reader must also write. |
| B8 | A fixed set of rows keyed by `pg_backend_pid() % N`. | Rejected. Two backends with the same key wait for each other. That wait can close a deadlock with the execution row locks of a failure transaction. |
| B9 | A SQL function that repairs the counter from an exact count. | **Adopted.** The migration seeds with it. An operator repairs a drift with it. |

### 0.2 Reverse brainstorm — how can this change do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | The counter row becomes a lock that every failure transaction waits for. A flood of failures then runs one at a time. | B6. A writer locks a free row with `SKIP LOCKED`, or inserts a new row. It never waits. A test holds many open writers at once and needs each insert to end at once. |
| R2 | The counter wait closes a deadlock with the execution and parent row locks of a failure transaction. | B6 again. A lock that nobody waits for cannot close a cycle. |
| R3 | The rows grow without limit. | A writer inserts a row only when all rows are locked by other open transactions. So the row count stays at or below the peak count of concurrent writers, which `max_connections` bounds. The repair function folds all rows into one. |
| R4 | A bulk delete or a retention pass writes the counter once per row. | B5. One update per statement. |
| R5 | `TRUNCATE` bypasses row and statement delete triggers. | An `AFTER TRUNCATE` trigger empties the counter. |
| R6 | A role without a grant on the new table writes a dead letter. The trigger then fails, and so does the failure transaction. | The trigger runs as the writer, as `harvest_entity_state_projection_trg` does. The startup preflight now checks `SELECT`, `INSERT` and `UPDATE` on `harvest_dead_letter_counts`. The upgrade guide names the grant. |
| R7 | The migration counts the table while writers add rows. The seed is then wrong. | The migration takes `SHARE ROW EXCLUSIVE` on `harvest_dead_letters` before it counts, so no write runs between the count and the triggers. `lock_timeout` is 5 s. |
| R8 | An operator disables the triggers, or a session sets `session_replication_role = replica`. The counter drifts. | `SELECT harvest_dead_letter_recount();` repairs it. The runbook says so. The alert reads a difference, so a constant drift does not change it. |
| R9 | A data-only restore with the triggers on counts each dead letter twice. A restore of `harvest_dead_letters` alone leaves the counter behind. | The backup runbook tells the operator to run the recount after such a restore. A full restore creates the triggers after it loads the data, so it agrees. |
| R10 | Logical replication (cross-region DR) applies rows with triggers off. | A `FOR ALL TABLES` publication carries the counter table too, and its primary key gives it a replica identity. The standby copies the counter of the primary. |
| R11 | A writer in a `REPEATABLE READ` transaction updates a counter row that a later commit changed. Postgres then raises a serialization failure. | Every engine write path uses `READ COMMITTED`. The module docs tell an embedder to retry, as for any `REPEATABLE READ` write. |
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
  failure path and a grant that a least-privilege role lacks.
- **Yellow (benefits).** The read is constant. Every caller of
  `dead_letter_count` gets the same benefit: the preflight, the shard health
  probe and the status summary. The count stays exact, so the alert keeps its
  meaning.
- **Green (ideas).** The same pattern can serve another exact gauge over a
  large table. Option C (#2679) can still cut the call count.
- **Blue (process).** Capture the write cost before the change. Write the
  failing tests. Add the migration and the read. Capture the costs again.
  Update the guide, the runbook, the Ledger profile and the changelog.

### 0.4 Decision

Adopt B1 with B5, B6 and B9.

- `harvest_dead_letter_counts (slot UUID PRIMARY KEY, entries BIGINT)`. The
  sum of `entries` is the count of `harvest_dead_letters`.
- `harvest_dead_letter_count_apply()` runs after each `INSERT` and `DELETE`
  statement, with its transition table, and after `TRUNCATE`.
- `harvest_dead_letter_recount()` locks the table, counts it and folds the
  counter into one row.
- `dead_letter_count` reads `dlq::DLQ_DEPTH_SQL`, a sum over the counter.

## 1. Acceptance criteria

1. At 50,000 dead letters, the gauge statement reads no row of
   `harvest_dead_letters` and at most 16 buffers.
2. After each write path the gauge equals `COUNT(*)`: a single insert, a
   multi-row insert, a batch discard, an empty delete, an update, a rollback
   and a `TRUNCATE`.
3. Many open writers each insert a dead letter. No insert waits for
   another. After a mix of commits and rollbacks the gauge is exact.
4. After a drift, `harvest_dead_letter_recount()` makes the gauge exact and
   leaves one counter row.
5. Run on a table with rows, the migration seeds the gauge with the exact
   count. `down.sql` removes the counter.
6. The real DLQ sampler emits a series whose growth over the window is more
   than 5 after 6 new dead letters. That is the `harvest_dlq_growth` rule.
7. The harness records the read cost and the write cost before and after, at
   10,000, 100,000 and 1,000,000 dead letters.
8. The preflight checks the grant. The upgrade guide, the runbook, the
   telemetry table, the Ledger profile and a changelog fragment describe the
   change.
