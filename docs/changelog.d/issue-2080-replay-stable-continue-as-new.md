## Phase X.Y — Replay-stable `should_continue_as_new()` (issue #2080)

`should_continue_as_new()` compared the loaded history size with the threshold.
The loaded size grows from task to task, so a cold replay of a loop could
answer `true` where the live run answered `false`. The replay then diverged.

The history-size trigger now reads the count only at the live frontier. It
records a `true` answer as a `SideEffectRecorded` event named
`__harvest_continue_as_new` (`CONTINUE_AS_NEW_SIDE_EFFECT_NAME`). A replay
reads that event back. Where history continues with no such event, the replay
answers `false`. A `false` answer records nothing.

Older histories have no record. A `WorkflowContinuedAsNew` event at the cursor
reads as `true`, also after a deadline probe clock read. Any other event reads
as `false`. No migration is needed, and no `WorkflowEvent` variant is new.

Known limits. An older run that answered `true` and then ran other commands
before `continue_as_new` replays as `false`. A `false` answer followed directly
by `continue_as_new` replays as `true`. Code that ignores a `true` answer
records one event per call.

Tests that built a history with no record for a `true` branch now include the
record. A recorded position no longer depends on the history policy.

The entity loop keeps its own checkpoint side effect. Its test
`the_checkpoint_decision_is_stable_when_history_grows` now asserts that a naive
loop replays clean.
