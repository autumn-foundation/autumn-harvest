## Phase 5.x — Redis dispatch: a worker keeps its channel, and a late install is revalidated (issue #1431)

Follow-up to issue #1312 and PR #1698. Both findings share one root cause: the
process-global dispatch slot can change after a worker starts.

**Binding.** PR #1698 already bound the worker's reads and reconcile sweep to
the channel `Worker::new` captured. Hints still published through the live
slot. After a second runtime replaced the channel, the first worker's hints
went to the replacement. The replacement's worker probed its own database,
missed, and acked them as absent. Now `dispatch::with_bound_channel` binds
every publish in a scope, inline or background, to one channel. The worker
runs its poll loop and each task body in that scope. Its maintenance loops
(timeouts, poison-pill reclaim, session and quota reconcile, pause
auto-resume) start through `dispatch::spawn_bound`, so they keep the binding.
A worker bound to no channel publishes nothing. Hint writers check
`dispatch::hints_wanted`, so a bound worker still publishes after another
runtime clears the slot.

**Late install.** A core caller can call `dispatch::install` after
`Worker::new`. The constructor saw no channel and checked nothing. The worker
now resolves its binding once, at the run boundary. It adopts a late channel
only when the span allows one channel and every queue name is valid.
Otherwise it logs one error and claims through Postgres. A worker that saw
any channel at construction keeps exactly what it captured.

**Plugin runner.** With Redis off, the runner calls the new
`Worker::bind_dispatch` while it holds its start lock. Its worker then never
adopts a channel that another runtime installs later.

The fix keeps a replacing `install`, because the plugin runner depends on it
(`install_single`, `restore_single_if_current`). A replacing install still
stops the shared background publisher, so queued hints can drop. The
reconcile sweep republishes those rows.

No new `WorkflowEvent` variant. No migration.

Tests: six `dispatch` unit tests cover inline, background, empty and spawned
bindings, batch grouping, and the hint gate. Eight `worker` unit tests cover
late-install adoption and refusal, the one-time binding, `bind_dispatch`, and
the per-shard case. The `dispatch` and `worker` unit tests now share one lock
for the global slots. Two Postgres integration tests in `dispatch_tests.rs`
run a workflow end to end. One installs the channel after `Worker::new`. One
replaces the channel while the worker runs. Both fail without the fix.
