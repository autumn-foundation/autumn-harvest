## Phase — Post-commit NOTIFY for appends and enqueues (issue #1796)

History appends and enqueues called `pg_notify` inside the write transaction.
Postgres takes a database-wide lock at commit for such a transaction, so these
commits ran one at a time. A full notification queue failed the commit, and a
failed `pg_notify` failed the append.

**Post-commit sender.** `notify_task_enqueued`, `notify_tasks_enqueued` and
`notify_workflow_events_appended` keep their signatures but never send inside
the write transaction. Each call stages a note with
`txid_current_if_assigned()` of the write. `notify::register_pool` starts one
sender for each pool. The sender reads `txid_status` on its own connection. It
sends committed notes, drops rolled-back notes and holds open ones. One
statement sends a batch, with one wake per queue channel and one per
execution. `Worker::run`, `Worker::run_with_listener` and
`WorkflowHandleClient::new` register their pools.

**Routing.** A note goes to the sender whose database fingerprint
(`current_database()` and `pg_postmaster_start_time()`) matches the write.
Other writes use the fallback. Outside a transaction the wake goes at once,
because the write already committed. Inside one it goes in a savepoint, so its
error cannot abort the write. Each call now returns `Ok(())`, and a lost wake
is counted. Polling stays the correctness floor.

**Settle delay.** The worker slept a fixed 50 ms after each wake. It now
sleeps a random 25 to 50 ms, so the workers that one wake reaches do not all
claim at once.

**Metrics.** `harvest.notify.send_failures` is the running total of lost
notifications. `harvest.notify.queue_usage` is the largest
`pg_notification_queue_usage()` a sender read. Both have dashboard panels,
alert rules and runbook sections.

**Invariants.** No new `WorkflowEvent` variant. No migration. The progress
channel (`notify_workflow_progress`, issue #791) keeps its deliver-on-commit
contract.

**Evidence.** `notify_post_commit_tests` shadows `pg_notify` through
`search_path` on one session. Appends, enqueues and wakes commit, inside and
outside a transaction. Before the fix, three of these tests failed. Further
tests show no wake before commit, a wake after commit within tolerance, no
wake after a rollback, one merged wake per queue, and a counted send failure.
`benchmarks/notify-commit.sh` measures commit throughput. At 16 to 64 writers,
the post-commit path commits 1.8 to 2.9 times as many transactions per second
as an in-transaction NOTIFY. See `docs/benchmarks/notify-commit.md`.
