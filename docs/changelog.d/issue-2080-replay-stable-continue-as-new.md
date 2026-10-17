## Phase X.Y — Replay-stable `should_continue_as_new()` (issue #2080)

`should_continue_as_new()` compared the loaded history size with the threshold.
The loaded size grows from task to task, so a cold replay of a loop could
answer `true` where the live run answered `false`. The replay then diverged.

The history-size trigger now reads the count only at the live frontier. It
records each answer, `true` or `false`, as a `SideEffectRecorded` event named
`__harvest_continue_as_new` (`CONTINUE_AS_NEW_SIDE_EFFECT_NAME`). A replay
reads the event at the same position. The record keeps the positions of a
signal-driven loop apart, where no other command separates two calls.

The `__harvest_` prefix is reserved for engine side-effect ids. Author code must
not use `__harvest_continue_as_new`.

Older histories have no record. A `WorkflowContinuedAsNew` event at the cursor
reads as `true`, also after a deadline probe clock read. Any other event reads
as `false`. No migration is needed, and no `WorkflowEvent` variant is new.

The entity loop keeps its own checkpoint side effect. It calls an unrecorded
hint, so it adds no second event. Its test
`the_checkpoint_decision_is_stable_when_history_grows` now asserts that a naive
loop replays clean.

Known limits. An older run that answered `true` and then ran other commands
before `continue_as_new` replays as `false`. A call in an older history with
no record, followed directly by `continue_as_new`, replays as `true`. Each call
now records one small event.

Tests that built a history with no record for a `true` branch now include the
record. A recorded position no longer depends on the history policy.
