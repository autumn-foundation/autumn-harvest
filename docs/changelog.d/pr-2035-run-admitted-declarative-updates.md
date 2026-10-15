## Fix — The worker runs admitted declarative updates (issue #2035)

**Behaviour change.** The worker now runs each admitted update and records its
result. Before this change the HTTP edge appended `UpdateAdmitted` and woke the
workflow task, but no engine code ran the handler. A caller with
`wait=completed` got `504`.

**Where an update runs.** After the workflow body, at the end of the same
decision cycle, in history order. Replay never runs the handler. It returns the
recorded result. Update events are transparent to command matching, so the
order cannot cause a divergence. `DESIGN-2035.md` holds the planning record.

**Mechanism.** `WorkflowContext::with_update_dispatch` marks a context for
dispatch. Only the worker cold path sets it, so a replay gate never runs a
handler. After `classify_cycle`, `update::dispatch_pending_updates` lists the
updates with no result. It runs each handler and adds a `RecordUpdateResult` to
the cycle commands. The worker already persists that command on the suspend
path and before a terminal event. A suspended, completed, continued-as-new or
failed cycle dispatches. A divergent, panicked or deadlocked cycle does not,
because the worker discards its commands.

**Bounds.** Each handler runs under `UPDATE_HANDLER_TIMEOUT` (5 seconds). A
panic or a timeout records `UpdateFailed`. A body that already ran the same
update with `execute_admitted_update` does not run it twice.

**Limits.** Delivery is at least once. A crash after the handler and before
the persist runs the handler again. An admit does not wake a task that waits on
a timer.

**No new `WorkflowEvent` variant. No migration.** `harvest_events` stays
append-only, and no code path rewrites a stored row.

**Tests.** 14 unit tests in `update.rs` cover dispatch, replay, panics,
timeouts, ordering, terminal cycles and the guards. `declarative_update_tests`
runs the worker on Postgres. The two `mcp_tools_integration` update tests lose
`#[ignore]`.
