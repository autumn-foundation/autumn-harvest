## Engine — Orphan reclaim skips a locked row (issue #1876)

`poison_pill::reclaim_orphaned_tasks` no longer stalls behind a row lock that
another session holds. A partitioned worker can keep its transaction, and its
row locks, open on the server.

- `requeue_orphan`, `quarantine_orphan` and `requeue_stuck_task` lock the task
  row with `FOR UPDATE SKIP LOCKED`. A locked row is skipped. The next pass
  retries it.
- A session timeout on one row skips that row. Before, the error ended the
  pass. A quarantine also locks the owning execution row, so a `lock_timeout`
  there no longer stops the other orphans.
- Any other error still ends the pass.
- No migration. No new `WorkflowEvent` variant. No public API change.
- Tests in `tests/integration/poison_pill_tests.rs` hold a row lock in a second
  session: `locked_orphan_does_not_stall_the_requeue_of_other_orphans`,
  `locked_orphan_does_not_stall_the_quarantine_of_other_orphans`,
  `locked_stuck_task_does_not_stall_the_stuck_running_backstop` and
  `lock_timeout_on_one_orphan_does_not_end_the_pass`. Each test then releases
  the lock and checks that the next pass reclaims the row.
