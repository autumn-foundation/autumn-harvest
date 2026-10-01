## Fix — worker heartbeat aborts on external cancellation (issue #1552)

`Worker::run_with_listener` and the multi-shard runner held each heartbeat
`JoinHandle` as a bare local. Cancelling the `Worker::run()` future from
outside, for example with `JoinHandle::abort()`, dropped the handle. Dropping a
`JoinHandle` detaches its task, so the heartbeat kept running and kept its pool
connection.

A private `AbortOnDrop` guard now owns each heartbeat handle. Its `Drop` calls
`abort()`. The graceful path calls `AbortOnDrop::join`, which waits for the
task to end on its own, so the `Stopped` write is unchanged. `join` keeps the
handle in the guard while it waits, so cancelling a caller inside `join` still
aborts the task. `Drop` cannot await, so the task stops at its next poll.

No migration and no new `WorkflowEvent` variant. Tests in `worker.rs` need no
database. Five unit tests cover the guard. Two wiring tests cancel
`run_with_listener` and the multi-shard `run` against unreachable pools. Each
checks that the heartbeat tasks release their shared state. All seven tests
fail when `Drop` stops aborting. The monitoring tasks use the same bare-handle
pattern and are out of scope here.
