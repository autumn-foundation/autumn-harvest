## Activity claim lock (PR #2683)

An activity start with a handler, an activity completion and an activity
failure no longer take the task row with `lock_claim_for_update` before
their fenced write. The fenced write runs first and is the lock: no row
means a lost claim. A start with no handler, the in-worker timeouts, the
workflow-task quarantine, the event-id re-drive and `run_transactional`
keep their lock.

The fenced write holds `FOR NO KEY UPDATE`, which `FOR KEY SHARE` does not
wait for. `read_lost_start` now takes `FOR SHARE`, so it still waits for a
start that is committing. `pg_timeouts_tests::a_lost_start_read_waits_for_a_start_in_progress`
fails with the merged start under `FOR KEY SHARE` and passes under
`FOR SHARE`.

Evidence: the e2e census drops 8,640 claim locks per run (6 per workflow),
2.9% of statements and 4.3% of execution time. Six alternating pairs show
no throughput change (median ratio 0.998). New tests in
`activity_claim_epoch_tests` assert that each merged path sends no separate
lock, and that a start with no handler keeps its lock and its lost-claim
fence. No migration and no new event.

The same PR cuts the claim window comments to what the code cannot say.
