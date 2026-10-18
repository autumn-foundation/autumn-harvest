# Design — Issue #2193: release a Session when its owner drops it

A `Session` has no drop cleanup. A future that owns a session can drop before
`complete()` runs. The host slot then stays `ACTIVE` until the lease ends
(24 hours). A run that drops sessions in a loop can exhaust
`max_concurrent_sessions` for that time.

This change makes `Drop` issue a release that no body awaits.

**No new `WorkflowEvent` variant. No new `WorkflowCommand` variant. No schema
change.** One new reserved activity name carries the release.

---

## 0. Planning record

### 0.1 Facts found before the plan

- `Session` holds `&WorkflowContext`, an id, the host worker id and a queue.
  `complete(self)` dispatches `__harvest_session_release`, pinned to the host.
- `handle_session_release` is idempotent. The state update filters on
  `ACTIVE`, and freeing an absent slot is a no-op.
- `MutexGuard` already issues a command from `Drop`. It skips when the cycle
  is suspending, because the executor drops the handler future at each park.
- The matcher is positional. An `ActivityScheduled` that code does not
  re-issue at the same position is drift, unless the event is transparent.
- `HistoryMatcher::new` already marks classes of event transparent.
- A `ScheduleActivity` whose receiver is closed does not park the cycle. The
  worker still persists it. A `Completed` or `ContinuedAsNew` cycle discards
  it. Before this change, a `Failed` cycle recorded an abandoned pair for it.
- The broken-session scanner already reaps the sessions of a terminal run.
- The ids of an activity come from a counter that moves only on a live miss.
- A resident cycle with a closed-receiver `ScheduleActivity` is not captured.
  The next cycle replays cold.
- SQLite rejects every `ScheduleActivity` with a `session_id`. A `Session`
  cannot exist there.

### 0.2 Brainstorm — how can a drop release a slot?

| # | Idea | Verdict |
|---|------|---------|
| B1 | `Drop` blocks on the release. | Rejected. `Drop` is synchronous and a workflow must not block. |
| B2 | `Drop` spawns a task that releases the slot directly. | Rejected. The workflow runs on any worker, and only the host owns the slot. The write would also skip history. |
| B3 | `Drop` pushes a release command that no body awaits. | **Adopted.** The worker persists it like any activity. The host runs it. |
| B4 | Add a `ReleaseSession` command variant. | Rejected. About 25 exhaustive matches change, across the worker, SQLite, fork and resident code. |
| B5 | Reuse `ScheduleActivity` with a closed receiver. | **Adopted.** The worker already persists it. |
| B6 | Reuse the name `__harvest_session_release`. | Rejected. `complete()` matches that name by position. Replay could not tell the two apart. |
| B7 | Use a new reserved name `__harvest_session_drop_release`. | **Adopted.** Replay treats the name as transparent. |
| B8 | Match the release by position. | Rejected. The drop point shifts with each poll order. A transparent record needs no position. |
| B9 | Write a marker for the release. | Rejected. A marker is positional too. |
| B10 | Shorten the lease. | Rejected. It hides the leak and breaks long sessions. |
| B11 | Add a session cleanup to the scanner for dropped futures. | Rejected. The scanner cannot see a drop. |
| B12 | Skip the release when the cycle suspends. | **Adopted.** The executor drops the future at every park. |
| B13 | Skip the release when history already holds one for the session. | **Adopted.** It gives "recorded once". |
| B14 | Record the release for a finished run. | Rejected. The scanner reaps that session. |

### 0.3 Reverse brainstorm — how can this change do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | Every park releases a live session, because the executor drops the future. | `Drop` returns when the context is suspending. A test covers it. |
| R2 | `complete()` releases twice. | `complete()` sets a flag when its dispatch returns. A drop of `complete()` mid-await keeps the flag clear, so the drop releases. |
| R3 | A replay issues the release again. | `Drop` returns when the matcher holds a record for the session. The check runs before the id is minted. |
| R4 | The record shifts later commands in replay. | The matcher marks the record and its terminal events transparent. |
| R5 | A completed run replays with a drop but no record, and strict replay reports drift. | `is_replay_significant_command` ignores the new name. |
| R6 | A failing run records an abandoned pair for the release. | `abandoned_dispatch_events` returns nothing for the name. The counters skip it. |
| R7 | A completed run logs a warning for the discarded release. | `abandoned_dispatch_event_count` skips the name. |
| R8 | A release in a batch with a local activity, a mutex acquire or an external activity fails the run or panics. | The cycle discards the release before shape dispatch. The next replay drops the session again. A test covers it. |
| R9 | The new name gets the default retry and timeout floors. | The worker skips the floors for each name that `is_reserved_session_activity_name` accepts, and the name joins it. A test covers the function. |
| R10 | A deployment without sessions requires a worker for the new name. | The same function excludes it from the required queues. |
| R11 | The test harness fails with "no mock registered". | `resolve_activity` resolves the name to `Null`. |
| R12 | A release for a `COMPLETED` or `BROKEN` session fails the task. | The failure record is transparent by name. The workflow wakes once. |
| R13 | The release completion wakes the workflow and costs a cold replay. | Accepted. A drop is rare. |
| R14 | A panic unwinds through `Drop` and a poisoned lock aborts the process. | `Drop` returns while a panic unwinds. A test covers it. |
| R17 | A failing cleanup fails a healthy run. | The host handler fails the release task only. |
| R18 | A release for an inactive session writes a failure and wakes the run. | The broken-session check skips the name. |
| R19 | The test harness stops auto-firing a timer because of the release. | The harness predicate ignores the name. |
| R15 | An older worker cannot find the name. | Listed in section 2. |
| R16 | The fuzz mirror maps the record to an ordinary operation. | The mirror skips transparent records. |

### 0.4 Six thinking hats

- **White (facts).** Section 0.1. Replay already has transparency. The worker
  already persists a closed-receiver activity. The host release is idempotent.
- **Red (feelings).** A hidden command in `Drop` feels risky. `MutexGuard`
  ships the same pattern. Every other path stays unchanged.
- **Black (risks).** Section 0.3. The main risks are a double release, a
  replay drift and a batch conflict. Each has a test.
- **Yellow (benefits).** Scopes, race losers and `select!` all release their
  session with no change to their code. Future drop sites need no work.
- **Green (ideas).** Add an explicit, awaited `Session::release()`. Add a
  `ReleaseSession` command if the reuse becomes too fragile. Add a metric for
  drop releases. Add a version gate for a mixed fleet.
- **Blue (process).** Red tests, then green code, then refactor. Then a
  review from several angles.

---

## 1. Change

### 1.1 Mechanism

1. `SESSION_DROP_RELEASE_ACTIVITY_NAME` is `__harvest_session_drop_release`.
   It joins `is_reserved_session_activity_name`, the registry loop, the host
   handler dispatch and the test harness.
2. `Session` gets a `settled` flag. `complete()` sets it after its dispatch
   returns, with either result.
3. `Drop for Session` returns when `settled` is set, when the context is
   suspending, or when the matcher holds a record for the session. Otherwise
   it mints an activity id
   and pushes `ScheduleActivity` for the new name. The command carries the
   session id and the host worker id, and its receiver is closed.
4. `HistoryMatcher::new` marks each `ActivityScheduled` of the new name
   transparent. It marks every later event with the same activity id too. It
   keeps the session ids in a set for step 3.
5. The terminal path skips the name. The replay checks ignore it. The batch
   checks ignore it.

### 1.2 Why replay stays deterministic

- The drop point is a deterministic function of the code and the history.
- A replay never pushes the command, because the record is in history.
- A record of the name never shifts a positional match.
- A run that completed with a discarded release replays without drift.

---

## 2. Known limits

- A release runs only when a cycle suspends after the drop. A run that ends
  first keeps its slot until the scanner reaps it, as today.
- A cycle that drops a session never stays resident. The next cycle replays
  cold. The release completion wakes the workflow once more.
- A release for a session that is not `ACTIVE` records a transparent failure
  and wakes the workflow once.
- A worker of an older build cannot run the name. A session task has no other
  worker, so a host of an older build fails the run. Deploy the new build to
  every worker before a workflow drops a session.
- An older build does not treat the record as transparent. Replay of such a
  history on an older build reports non-determinism. No version gate exists.
- A cycle that holds an inline local activity, a mutex acquire or an external
  activity discards the release. The next replay drops the session again, and
  the next suspension issues the release.
- A `complete()` future that drops after its own release started also issues a
  drop release. The host handler is idempotent, so the cost is one activity.
- A permanent error in the host handler fails the release task only. The run
  lives, and the lease reaps the slot.
- The host handler ignores a release for a session that is not `ACTIVE`. The
  worker records no failure for it.
- The SQLite backend has no sessions, so `Drop` never issues a release there.

---

## 3. Tests and acceptance criteria

| AC | Test |
|----|------|
| A dropped session issues one pinned release that no body awaits. | `dropping_a_session_issues_one_pinned_release` |
| `complete()` does not release again. | `a_completed_session_issues_no_drop_release` |
| A dropped `complete()` future releases. | `a_dropped_complete_future_releases_the_session` |
| A suspending cycle does not release. | `a_suspending_cycle_issues_no_drop_release` |
| History with a record causes no second release. | `a_recorded_drop_release_is_not_issued_again` |
| The record never shifts a positional match. | `a_drop_release_record_is_transparent_to_the_matcher` |
| A completed run replays without drift. | `a_completed_run_with_a_dropped_session_replays_strictly` and `a_drop_release_is_not_a_replay_significant_command` |
| A failing run records no abandoned pair for the release. | `a_failed_cycle_records_nothing_for_a_drop_release` |
| A dedicated persist shape discards the release. | `a_dedicated_shape_discards_the_drop_release` |
| The test harness resolves the name. | `the_test_env_resolves_a_drop_release` |
| Two sessions get two releases. | `two_sessions_dropped_in_one_cycle_get_two_releases` |
| A park does not release a held session. | `a_session_held_across_a_park_is_not_released` and `a_session_dropped_before_a_park_is_released` |
| A panic does not release. | `a_panic_that_unwinds_a_session_issues_no_drop_release` |
| Every record shape stays transparent. | `every_record_shape_of_a_drop_release_is_transparent` |
| The name is reserved and registered. | `the_drop_release_is_a_reserved_name` and `handler_registry_always_registers_reserved_session_activities` |

The host handler is shared with `complete()`. A Postgres test for the new
name needs a container, so the CI session suite covers it.
