## Engine — Task-queue table hygiene (issue #1811)

**Behavior change.** The retention janitor now deletes finished
`harvest_task_queue` rows. Before, they went only when their execution was
deleted, and history retention is off by default. So `COMPLETED`, `FAILED` and
`CANCELLED` rows stayed forever, and the table and its indexes grew without
limit.

**The janitor.** `queue::sweep_terminal_tasks` runs once per tick on each
shard. It is on by default and independent of history retention.

- It deletes a row only when the state is `COMPLETED`, `FAILED` or
  `CANCELLED` and `completed_at` is older than the window.
- The state list is positive. A future state is never deleted by default.
- It keeps a terminal workflow-task row while its execution is live. The
  concurrency supersede scan finds a live execution through that row.
- It also keeps that row while a dead letter exists for the execution. A
  DLQ redrive can move a `FAILED` execution back to `RUNNING`.
- It reads `(completed_at, id)` in key order, with a keyset cursor.
- It deletes in `batch_size` batches, capped at 10,000 rows per statement,
  with `FOR UPDATE SKIP LOCKED` and `DELETE ... USING`. It runs at most 50
  batches per shard per tick.
- It sweeps each physical database once per tick. Shards aliased to one
  database share the pass and its budget, and each reports the outcome.
- A pass that fails after some batches committed still reports and meters
  those rows. Shutdown takes effect at the next database boundary, or
  during a connection checkout.
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
- New `idx_harvest_dl_workflow_exec_id` on `harvest_dead_letters
  (workflow_exec_id)`. It serves the dead-letter check and the retention
  purge of dead letters.
- Each new index is built only when missing. A pre-built index that is
  INVALID or has another definition stops the migration.
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
  `CONCURRENTLY` recipe for a large live table.

No `WorkflowEvent` variant, no write to `harvest_events`, no replay impact.

**Docs.** `docs/autumn-workflow-architecture.md` said that the task queue is
list-partitioned. It is not. The doc now says so and describes the janitor.

**Dashboard.** The starter pack gains a panel for the new counter.

**Benchmark.** `task_queue_hygiene_bench` measures claim latency after 1M
terminal rows, before and after the fix. Before vacuum, claim p50 doubles
(303 ms to 596 ms). After the sweep, it is back at the baseline. Results are
in `docs/performance-task-queue-hygiene.md`. CI compiles the bench.

**Also fixed.** `activity_default_timeout_tests.rs` did not compile on
`trunk-dev`. It builds a `WorkerRuntimeConfig` literal without the
`resident_workflows` field from #1798. The test now sets it.

**Tests.** 13 unit tests pin the config, the outcome and the SQL shape. 17
DB tests in `terminal_task_gc_tests.rs` cover the AC and the edges:

- old terminal rows go, and `PENDING`/`RUNNING` rows stay at any age;
- each statement deletes at most one batch, one tick runs at most 50
  statements, and the next tick continues;
- two shards aliased to one database share one pass per tick;
- no live row is locked, and a locked terminal row is skipped;
- a batch size above the cap is clamped;
- a NULL `completed_at` and a row at the cutoff stay;
- each of the 10 execution states keeps or releases its workflow row;
- a dead letter keeps its execution's workflow row;
- `dry_run` deletes nothing, and a disabled janitor reports `None`;
- a role without `DELETE` reports the error;
- the migration sets the reloptions and indexes, round-trips through
  `down.sql`, and makes a heartbeat update HOT.
