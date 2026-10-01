## Fix — the worker path ND-blocks a cycle that skips recorded commands (issue #1791)

Only the strict and canary executors checked for unconsumed recorded history
at the end of a cycle. The production worker path, `drive_workflow`, did not.
A deploy that removed a recorded command was not caught:

- If the new code returned early, the worker persisted `WorkflowCompleted`
  over unconsumed `ActivityScheduled` events.
- If the new code waited on a signal past a stray `TimerStarted`, the run
  parked forever.

`drive_workflow` now checks its completed and suspended outcomes with
`HistoryMatcher::first_unconsumed_command_event`. A hit returns `Failed` with
`non_deterministic_details`, so the #603 gate ND-blocks the run. The run stays
`RUNNING`, persists nothing from the cycle, and resumes after a rollback.

The check counts only events that a workflow command writes:
`ActivityScheduled`, `LocalActivityScheduled`, `TimerStarted`,
`TimerCancelled`, `ChildWorkflowStarted`, `ChildWorkflowSpawnedDetached`,
`MarkerRecorded`, `SideEffectRecorded` and the three `External*Requested`
events. A live history can hold a signal, a result or an update that the code
has not awaited yet. Those are not drift, so the check ignores them. The strict
check (`history_has_unconsumed_events`) counts them, so it is not reused.

Every case the new check flags, strict replay and the deploy canary already
report. The author `Err` arm is not checked, as on the strict path. A
fail-fast join can return `Err` without polling every branch.

The ND diagnostic follows the #603 runbook: `expected` is what the code did
and `actual` is the recorded event, for example
`expected: <workflow returned early>` and
`actual: ActivityScheduled(send_email)`.

No migration, no new `WorkflowEvent` variant, and no `harvest_events` write.
The strict and canary executors are unchanged.

Tests: four worker-path tests in `nd_block_tests.rs`. Three of them failed
before the fix: trailing activities, a stray timer before a signal wait, and
the same with the signal delivered. The fourth is a control: a pending signal
while parked is not drift. Unit tests in `replay.rs` and `executor.rs` cover
the predicate, both guarded arms and the author `Err` exemption.
