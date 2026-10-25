## Fix — The worker runs admitted declarative updates (issue #2035)

**Behaviour change.** The worker now runs each pending update and records its
result. Before this change the HTTP edge appended `UpdateAdmitted` and woke the
workflow task, but no engine code ran the handler. A caller with
`wait=completed` got `504`.

**Where an update runs.** An update runs after the workflow body, at the end of
the same decision cycle, in history order. Replay never runs the handler. It
returns the recorded result. Update events are transparent to command matching,
so the order cannot cause a divergence. `docs/design/DESIGN-2035.md` holds the planning
record.

**Mechanism.** `WorkflowContext::with_update_dispatch` marks a context for
dispatch. Only the worker cold path sets it, so a replay gate never runs a
handler. After `classify_cycle`, `update::dispatch_pending_updates` lists the
updates with no result. It runs each handler and adds a `RecordUpdateResult` to
the cycle commands. The worker already persists that command on the suspend
path and before a terminal event.

A suspended, completed, continued-as-new or errored-body cycle dispatches. A
divergent, panicked or deadlocked cycle does not, because the worker discards
its commands.

**Bounds.** Each handler runs under `UPDATE_HANDLER_TIMEOUT` (5 seconds). A
panic or a timeout records `UpdateFailed`. The panic text goes to the log, not
to history. A body that already ran the same update with
`execute_admitted_update` does not run it twice.

The cycle runs under the workflow-task budget. A handler starts only while a
full handler limit is left in half of the remaining budget. The first update of
a decision always runs. A re-drive of the same decision runs no update past the
window. Every handler stops at half of the budget that is left. A task with no
budget gets a fixed 30 second window. When updates remain,
`DriveResult::updates_deferred` is set, and the worker wakes the task so the
next cycle runs them. A cycle that defers an update is not kept resident, so
the next cycle replays cold and runs the dispatch. A burst of slow updates
therefore cannot cancel the cycle or cost poison-pill strikes.

**Limits.** Delivery is at least once. Any path that discards the cycle after
the handlers ran runs them again: a crash, a deadlock retry, an event-id
conflict, a pause, a lost claim and a capability-miss release. A handler has no
dedupe key. An admit does not wake a task that waits on a timer. A handler
cannot await an activity or a timer.

**No new `WorkflowEvent` variant. No migration.** This change adds no writer of
`harvest_events` rows. `harvest_events` stays append-only.

**Tests.** Unit tests in `update.rs` cover dispatch, replay, panics, timeouts,
ordering, terminal cycles, divergence, a panicking body and the dispatch window.
`declarative_update_tests` runs the worker on Postgres, including a local
activity and a closed window. The two `mcp_tools_integration` update tests lose
`#[ignore]`.
