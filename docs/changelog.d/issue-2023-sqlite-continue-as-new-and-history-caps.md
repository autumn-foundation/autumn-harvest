## SQLite: `continue_as_new` and the default history caps (issue #2023)

**`continue_as_new` works on SQLite.** Before, it failed the run with
`UnsupportedFeature`. Now one transaction appends `WorkflowContinuedAsNew`,
seals the run `CONTINUED_AS_NEW`, and starts a successor with the same
`workflow_name` and `workflow_id`. Undelivered signals move to the successor.
Consumed signals stay with the old run, as on Postgres. The hand-off deletes
the old run's pending tasks and unfired timers. The hand-off cycle keeps its
side effects and markers. It drops a dispatch and logs a warning, as the
Postgres worker does.

`run_until_blocked` returns the new `RunState::ContinuedAsNew(successor)`. A
later drive of the old run returns the same successor. **Breaking:** an
exhaustive `match` on `RunState` needs the new arm.
`examples/claude-agent-daemon` gets it: the old run retires and the successor
stays live.

The backend still rejects a continue-as-new to a different type
(`continue_as_new_as_type`) by name. Its token is
`ContinueAsNew.new_workflow_type`. The new `SqliteRuntime::successor(exec)`
returns the successor id without a drive.

**The #1804 history caps apply on SQLite.** The defaults match the Postgres
worker: 50,000 events and 50 MiB per run. `set_history_event_hard_cap` and
`set_history_byte_hard_cap` change them. `None` removes a cap. The event count
includes the decision's own appends. The byte count is the stored event JSON
length at decision start. A breaching decision rolls back. The run then fails
with `WorkflowFailed { error: <reason JSON> }`, the same event the Postgres
worker writes. The new `HistoryCapReason` parses the JSON. Its bytes match the
Postgres `DeadLetterReason`, and a test on each side pins them. A continuing
run is exempt. The seal removes the run's pending tasks, unfired timers and
staged signals.

No migration. No new `WorkflowEvent` variant. No change to the Postgres path.
`docs/audits/doc-claim-drift.py` pins the two retired claims.

Tests: `tests/integration/continue_as_new.rs` and
`tests/integration/history_caps.rs` in `autumn-harvest-sqlite`, the
`history_cap` unit tests, and
`history_cap_reasons_keep_the_bytes_the_sqlite_backend_mirrors` in
`autumn-harvest/src/dlq.rs`. Both histories of a continued run replay on the
core `WorkflowReplayer`. The plan is in `DESIGN-2023.md`.
