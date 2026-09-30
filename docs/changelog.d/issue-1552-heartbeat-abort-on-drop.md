## Fix — worker heartbeat aborts on external cancellation (issue #1552)

`Worker::run_with_listener` and the multi-shard runner held each heartbeat
`JoinHandle` as a bare local. Cancelling the `Worker::run()` future from
outside, for example with `JoinHandle::abort()`, dropped the handle. Dropping a
`JoinHandle` detaches its task, so the heartbeat kept running and kept its pool
connection.

A private `AbortOnDrop` guard now owns each heartbeat handle. Its `Drop` calls
`abort()`. The graceful path calls `AbortOnDrop::join`, which disarms the guard
and waits for the task to end on its own, so the `Stopped` write is unchanged.
`Drop` cannot await, so the task stops at its next poll.

No migration and no new `WorkflowEvent` variant. Tests: four unit tests in
`worker.rs` cover drop, owner-future abort, graceful join and panic
propagation. The monitoring tasks use the same bare-handle pattern and are out
of scope here.
