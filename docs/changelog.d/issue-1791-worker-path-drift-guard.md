## Fix — the worker path ND-blocks a cycle that skips recorded commands (issue #1791)

Only the replay paths (strict, canary, query and debugger) checked for
unconsumed recorded history at the end of a cycle. `drive_workflow`, the
production worker path, did not.
A deploy that removed a recorded command was not caught:

- If the new code returned early, the worker persisted `WorkflowCompleted`
  over unconsumed `ActivityScheduled` events.
- If the new code waited on a signal past a stray `TimerStarted`, the run
  parked forever.

`drive_workflow` now checks its completed and suspended outcomes with
`HistoryMatcher::first_unconsumed_command_event`. A hit returns `Failed` with
`non_deterministic_details`, so the #603 gate ND-blocks the run. The run stays
`RUNNING`, appends no event from the cycle, and resumes after a rollback.

The check counts only events that anchor a replayed workflow call:
`ActivityScheduled`, `LocalActivityScheduled`, `TimerStarted`,
`TimerCancelled`, `ChildWorkflowStarted`, `ChildWorkflowSpawnedDetached`,
`MarkerRecorded`, `SideEffectRecorded`, the three `External*Requested`
events, `ActivityAwaitingExternal` and `MutexGranted`. A live history can hold
a signal or a result, such as `ActivityCompleted`, that the code has not
awaited yet. Those events are not drift, so the check ignores them. The
strict check (`history_has_unconsumed_events`) counts them, so the worker
path does not reuse it.

Strict replay and the deploy canary already report every case that the new
check flags. The author `Err` arm is not checked, as on the strict path. A
fail-fast join can return `Err` without polling every branch.

`WorkflowTestEnv`, `WorkflowSimulator` and the SQLite runtime share
`drive_workflow`, so they also report this drift.

The ND diagnostic follows the #603 runbook: `expected` is what the code did
and `actual` is the recorded event, for example
`expected: <workflow returned early>` and
`actual: ActivityScheduled(send_email)`.

Upgrade impact. The engine upgrade alone can surface drift from an earlier
deploy. A run that skipped a recorded command used to complete or park. It
now blocks on its next wake. The diagnostic `build_id` is the current build,
so a rollback does not clear the block. Before the upgrade, run the deploy
canary or `replay-diagnosis` over in-flight runs. After it, reset a blocked
run to before the reported `event_index`, or terminate it. A stale
`patch:`/`version:` marker left by an early removal of `deprecate_patch` also
blocks now. The next `docs/upgrading/` guide must carry this note.

A workflow body that awaits non-durable work for more than 100 ms during
replay could fail terminally with zero commands. It now blocks with
`expected: <workflow suspended early>`, and stays recoverable. The ND-block
runbook and the alert runbook describe both cases.

No migration, no new `WorkflowEvent` variant, and no `harvest_events` write.
The strict and canary executors are unchanged.

Tests: four worker-path tests in `nd_block_tests.rs`. Three of them failed
before the fix: trailing activities, a stray timer before a signal wait, and
the same with the signal delivered. The fourth is a control: a pending signal
while parked is not drift. Unit tests in `replay.rs` and `executor.rs` cover
the predicate, both guarded arms and the author `Err` exemption.
