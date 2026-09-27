## Phase 5.x — Redis dispatch: a worker keeps its channel, and a late install is revalidated (issue #1431)

Follow-up to issue #1312 and PR #1698. Both findings share one root cause: the
process-global dispatch slot can change after a worker starts.

**Binding.** PR #1698 already bound the worker's reads and reconcile sweep to
the channel `Worker::new` captured. Task hints still published through the
live slot. After a second runtime replaced the channel, the first worker's
hints went to the replacement. The replacement's worker probed its own
database, missed, and acked them as absent. Now `dispatch::with_bound_channel`
binds every publish in a scope, inline or background, to one channel. The
worker wraps each task body in that scope. A worker bound to no channel
publishes nothing.

**Late install.** A core caller can call `dispatch::install` after
`Worker::new`. The constructor saw no channel and checked nothing. The worker
now resolves its binding once, at the run boundary. It adopts a late channel
only when the span allows one channel and every queue name is valid.
Otherwise it logs one error and claims through Postgres. A worker that saw
any channel at construction keeps exactly what it captured.

Rejecting a replacing `install` was not chosen. The plugin runner depends on
replacement (`install_single`, `restore_single_if_current`).

No new `WorkflowEvent` variant. No migration. The plugin runner installs before
`Worker::new`, so its behavior does not change.

Tests: three `dispatch` unit tests cover inline, background, and empty
bindings. Seven `worker` unit tests cover late-install adoption and refusal,
the one-time binding, and the per-shard case. Two Postgres integration tests
in `dispatch_tests.rs` run a workflow end to end. One installs the channel
after `Worker::new`. One replaces the channel while the worker runs. Both fail
without the fix.
