## Phase — Post-commit NOTIFY for appends and enqueues (issue #1796)

History appends and enqueues called `pg_notify` inside the write transaction.
Postgres takes a database-wide lock at commit for such a transaction, so these
commits ran one at a time. A full notification queue failed the commit, and a
failed `pg_notify` failed the append.

**Post-commit sender.** `notify_task_enqueued`, `notify_tasks_enqueued` and
`notify_workflow_events_appended` keep their signatures but never send inside
the write transaction. Each call stages a note with the transaction id of the
write, from `txid_current()`. `notify::register_pool` starts one
sender for each pool. The sender reads `txid_status` on its own connection. It
sends committed notes, drops rolled-back notes and holds open ones. One
statement sends a batch, with one wake per queue channel and one per
execution. `Worker::run`, `Worker::run_with_listener`,
`WorkflowHandleClient::new`, `HarvestRunner::start` and
`SchedulerRuntime::spawn_sharded` register their pools.
A pool registered outside a Tokio runtime starts its sender at the first
notification a runtime stages. A stopping worker, runner or scheduler
flushes its senders, so the wakes of its last writes still go out.

**Routing.** A note goes to a healthy sender whose database fingerprint
(`current_database()` and `pg_postmaster_start_time()`) matches the write.
Each note keeps its fingerprint, so a note staged on another server is
dropped and counted, never gated on the wrong commit log. Other writes use the
fallback. Outside a transaction the wake goes at once, because the write
already committed. Inside one it goes in a savepoint, so its error cannot
abort the write. The fallback costs two more round trips than before, and it
keeps the NOTIFY commit lock. Polling stays the correctness floor.

**Errors.** A failed send never fails the write, and it is counted. A
`notify_*` call returns an error only when the write transaction has already
failed. Postgres would silently roll back that `COMMIT`, so the caller must
see the error.

**Behavior changes.** A queue wake can stand for several tasks and then
carries the nil task id. An event wake can stand for several appends and then
carries the summed count. A wake staged in a rolled-back savepoint still goes
out when the transaction commits. A queue channel longer than 63 bytes is now
cut on a character boundary, as `LISTEN` already cut it. Before, `pg_notify`
rejected the long name and failed the enqueue.

**Settle delay.** The worker slept a fixed 50 ms after each wake. It now
sleeps a random 50 to 75 ms. The 50 ms floor keeps the clock-skew margin, and
the jitter stops the workers that one wake reaches from all claiming at once.

**Metrics.** `harvest.notify.send_failures` is the running total of lost
notifications. `harvest.notify.queue_usage` is the largest
`pg_notification_queue_usage()` a sender read. Both have dashboard panels,
alert rules and runbook sections.

**Invariants.** No new `WorkflowEvent` variant. No migration. The progress
channel (`notify_workflow_progress`, issue #791) keeps its deliver-on-commit
contract.

**Evidence.** `notify_post_commit_tests` shadows `pg_notify` through
`search_path` on one session. Appends, enqueues and wakes commit, inside and
outside a transaction. On the code before the fix, the two append tests and
the in-transaction enqueue test failed when run. Further tests show no wake
before commit, and a median wake latency within 250 ms of an in-transaction
NOTIFY. They also show no wake after a rollback, one merged wake per queue, a
counted send failure, a woken listener on a long queue name, and registration
from a client and from outside a runtime.

`benchmarks/notify-commit.sh` measures commit throughput. At 16 to 64
writers, the post-commit path commits 1.4 to 2.3 times as many transactions
per second as an in-transaction NOTIFY, and loses no notification. See
`docs/benchmarks/notify-commit.md`.
