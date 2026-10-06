## Engine — Orphan reclaim skips a locked row (issue #1876)

`poison_pill::reclaim_orphaned_tasks` no longer stalls behind a task-row lock
that another session holds. A partitioned worker can keep its transaction, and
its row locks, open on the server.

- `requeue_orphan`, `quarantine_orphan` and `requeue_stuck_task` lock the task
  row with `FOR UPDATE SKIP LOCKED`. The pass skips a locked row. The next pass
  retries it.
- A quarantine also locks the owning execution and its open tasks. Those locks
  still wait, up to the session `lock_timeout`.
- A timeout or a deadlock on one row skips that row. Before, the error ended
  the pass. `SKIP LOCKED` lets two reclaimers deadlock on sibling orphans of
  one execution, so a deadlock skips the row too.
- Any other error still ends the pass.
- No migration. No new `WorkflowEvent` variant. No public API change.
- Docs: `docs/operations/postgres-timeouts.md`, `docs/architecture.md`,
  `docs/testing/chaos.md` and `docs/performance-poison-pill-orphan-recheck.md`.
- Tests: the integration tests below hold a row lock in a second session.
  Each test then releases the lock. The next pass must reclaim the row.
  - `locked_orphan_does_not_stall_the_requeue_of_other_orphans`
  - `locked_orphan_does_not_stall_the_quarantine_of_other_orphans`
  - `locked_stuck_task_does_not_stall_the_stuck_running_backstop`
  - `lock_timeout_on_one_orphan_does_not_end_the_pass`
  - `locked_sibling_skips_the_quarantine_of_its_execution`
- Unit tests in `poison_pill.rs` pin which errors skip a row and which end the
  pass.
