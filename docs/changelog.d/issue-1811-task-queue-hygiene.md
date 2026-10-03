## Engine — Task-queue table hygiene (issue #1811)

**Behavior change.** The retention janitor now deletes finished
`harvest_task_queue` rows. Before, only history retention removed them, and
history retention is off by default. So `COMPLETED`, `FAILED` and
`CANCELLED` rows stayed forever, and the table and its indexes grew without
limit.

**The janitor.** `queue::sweep_terminal_tasks` runs once per tick on each
shard. It is on by default and independent of history retention.

- It deletes a row only when the state is `COMPLETED`, `FAILED` or
  `CANCELLED` and `completed_at` is older than the window.
- The state list is positive. A future state is never deleted by default.
- It keeps a terminal workflow-task row while its execution is live. The
  concurrency supersede scan finds a live execution through that row.
- It reads `(completed_at, id)` in key order, with a keyset cursor.
- It deletes in `batch_size` batches with `FOR UPDATE SKIP LOCKED` and
  `DELETE ... USING`, at most 50 batches per shard per tick.
- Under `dry_run`, it runs a read-only preview with the same predicates.

**Config.** `RetentionConfig::terminal_task_retention_secs`, default 7 days,
floor 1 hour. `with_terminal_task_retention(Duration)` changes it, and
`without_terminal_task_gc()` turns it off. `RetentionConfig::enabled()` counts
it, so the runtime spawns for it alone.

**Observability.** `RetentionTickResult.terminal_task_gc` reports each shard's
rows deleted per state, `dry_run`, and any error, through
`GET /admin/retention`. The counter `harvest.retention.terminal_tasks_deleted`
counts real deletes per `state`. `GET /admin/preflight` now requires `DELETE`
on `harvest_task_queue`.

**Migration `20261003201739_harvest_task_queue_hygiene`.**

- Reloptions: `fillfactor = 80`, `autovacuum_vacuum_scale_factor = 0.02`,
  `autovacuum_analyze_scale_factor = 0.02`, `autovacuum_vacuum_cost_limit =
  2000`.
- New `idx_harvest_tq_terminal_completed_at` on `(completed_at, id)` for
  terminal rows. It serves the janitor.
- `idx_harvest_tq_running` keyed on `last_heartbeat_at`, so no heartbeat could
  be a HOT update. No query seeks on that column. It is now
  `idx_harvest_tq_running_started` on `(started_at)`, with the same predicate.
  A test proves that a heartbeat update is now HOT.
- Index audit: it drops `idx_harvest_task_queue_rate_limit_key`, because
  `idx_harvest_task_queue_rate_limit_key_live` has the same key and a wider
  predicate. It drops `harvest_task_queue_session_id_pending`, because no
  query implies its `state = 'PENDING'` predicate. The audit keeps the other
  indexes, because a query uses each one.
- Each statement fails after a 5 s lock wait. The migration holds the
  `CREATE INDEX CONCURRENTLY` recipe for a large live table.

No `WorkflowEvent` variant, no write to `harvest_events`, no replay impact.

**Docs.** `docs/autumn-workflow-architecture.md` said that the task queue is
list-partitioned. It is not. The doc now says so and describes the janitor.

**Benchmark.** `task_queue_hygiene_bench` measures claim latency after 1M
terminal rows, before and after the fix. Results are in
`docs/performance-task-queue-hygiene.md`.

**Also fixed.** `activity_default_timeout_tests.rs` did not compile on
`trunk-dev`. It builds a `WorkerRuntimeConfig` literal without the
`resident_workflows` field from #1798. The test now sets it.

**Tests.** 11 unit tests pin the config, the outcome and the SQL shape. 9 DB
tests in `terminal_task_gc_tests.rs` cover the AC: old terminal rows go,
`PENDING`/`RUNNING` rows stay at any age, one tick stops at the batch budget
and the next continues, a live execution keeps its workflow row, a locked row
is skipped, `dry_run` deletes nothing, the migration sets the reloptions and
indexes, and a heartbeat update is HOT.
