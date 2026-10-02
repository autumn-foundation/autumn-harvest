# Post-commit NOTIFY (issue #1796)

## Problem

History appends and enqueues call `pg_notify` inside the write transaction.
Postgres then takes a global lock at commit, so these commits run one at a
time. A full notification queue makes the commit fail. A failed `pg_notify`
also fails the append, because the error propagates.

## Facts (white hat)

- `pg_notify` only records the notification. Postgres queues it in
  `PreCommit_Notify`, under a database-wide lock, so a full queue fails the
  `COMMIT`.
- A notify-only transaction has no transaction id. Its commit writes no WAL
  record, so it holds the lock for a very short time.
- `store.rs` calls `notify_workflow_events_appended` in four append paths.
  `queue.rs`, `queue_pause.rs` and `activity_pause.rs` call
  `notify_task_enqueued` or `notify_tasks_enqueued` in about ten paths.
- Polling is the correctness floor. A lost wake costs latency, never work.
- The worker sleeps a fixed 50 ms after every wake, in two poll loops.
- CI runs Postgres 16, and some suites run Postgres 11.
  `txid_current_if_assigned()` and `txid_status()` exist from Postgres 10.

## Brainstorm

1. Send `pg_notify` in a savepoint and ignore its error.
2. Buffer notifications in a task-local scope. Each transaction owner flushes
   them after commit, as `dispatch::buffered` does for hints.
3. Stage each notification with the transaction id of the write. A background
   sender on its own connection sends it when `txid_status` reads `committed`.
4. Write an outbox row and drain it.
5. Remove NOTIFY and rely on polling.
6. Use a diesel `Instrumentation` as a commit hook.
7. Route by a global registry of database identities.

Option 3 with option 1 as the fallback is the choice. Option 2 needs a change
at every transaction owner, and a missed owner loses wakes. Option 4 adds a
write per notification. Option 5 adds a full poll interval of latency.
Option 6 fails because diesel emits `CommitTransaction` before `COMMIT` runs.

## Reverse brainstorm: how to make it worse

| Way to fail | Guard |
|---|---|
| Send before commit. A listener wakes, sees nothing and sleeps a full poll interval. | Gate on `txid_status`. |
| Send to the wrong database. | Match a database fingerprint before staging. Else use the fallback. |
| Grow memory without limit while the database is down. | Bound the queue. Drop and count the overflow. |
| Make the write path wait for the sender. | Push into a mutex-guarded queue. Never await the sender. |
| Force a transaction id on a read-only transaction. | Use `txid_current_if_assigned()`. |
| Keep an entry for a transaction that never ends. | Drop it after a maximum wait. |
| Fail one batch because one channel name is too long. | Filter invalid channels before the send. Count them. |
| Lose wakes with no signal. | Count send failures. Sample `pg_notification_queue_usage()`. |
| Wake all workers at the same time after one notify. | Jitter the settle delay. |
| Remove the settle delay and break the clock-skew margin. | Keep a jittered delay from 25 ms to 50 ms. |
| Keep a dead sender after its runtime stops. | Check `JoinHandle::is_finished` on lookup. Drop the sink. |

## Six hats

- **White:** see Facts.
- **Red:** operators fear lost wakes. Maintainers fear a large diff in
  `worker.rs`.
- **Black:** routing errors, runtime lifetimes in tests, the changed contract
  for embedders that LISTEN on `harvest_events`, the latency of one tick.
- **Yellow:** a notify failure never fails a write. Write commits no longer
  queue behind the NOTIFY lock. One tick sends one notification per queue.
- **Green:** gate on the transaction id. Shadow `pg_notify` through
  `search_path` to make it fail on one session only, for the RED tests.
- **Blue:** follow red, green, refactor. Keep the public `notify_*` signatures,
  so that call sites do not change.

## Design

`notify_workflow_events_appended`, `notify_task_enqueued` and
`notify_tasks_enqueued` keep their signatures. Each one stages a note and
returns `Ok(())`.

1. **Register.** `notify::register_pool(&pool)` starts one sender for each
   pool. `Worker::run`, `Worker::run_with_listener` and
   `WorkflowHandleClient::new` call it.
2. **Stage.** One statement on the write connection reads
   `txid_current_if_assigned()` and a fingerprint of the database. The
   fingerprint is `current_database()` and `pg_postmaster_start_time()`.
   A live sender with the same fingerprint gets the note.
3. **Send.** The sender reads `txid_status` for each staged id on its own
   connection. It sends committed notes, drops aborted notes and keeps notes
   still in progress. A note with no transaction id is sent on the next tick.
   One statement sends the batch in its own transaction.
4. **Coalesce.** One tick sends one queue notification for each channel. One
   task gives that task id. More tasks give the nil id, as
   `notify_tasks_enqueued` does. Event notes merge for each execution.
5. **Fall back.** With no matching sender, the note goes on the write
   connection. Outside a transaction it is sent at once. Inside one it is sent
   in a savepoint. Either way the error is counted and dropped.
6. **Measure.** `harvest.notify.send_failures` is the running total of lost
   notifications. `harvest.notify.queue_usage` is the largest
   `pg_notification_queue_usage()` that a sender read.
7. **Settle.** The worker sleeps a random 25 ms to 50 ms after a wake, not a
   fixed 50 ms.

The progress channel (`notify_workflow_progress`) is out of scope. Its
deliver-on-commit behavior is part of its contract (issue #791).

## Limits

The fallback still sends inside the transaction. A full queue can still fail
that commit. Only a write to a database that no runtime registered uses it.

## Tests

- RED: a shadowed `pg_notify` fails an append and an enqueue, inside and
  outside a transaction. Each must still commit.
- A registered sender does not use the write connection. A shadowed
  `pg_notify` on the write session does not stop the wake.
- No wake arrives before commit. The wake arrives within tolerance after
  commit. A rolled-back write sends nothing.
- A failing sender counts the failure, and the append commits.
- Unit tests cover coalescing, channel validation and the settle delay.
- `benchmarks/notify-commit.sh` measures commit throughput for N concurrent
  writers, with NOTIFY inside the transaction and after it.
