# DESIGN-2607 — External activities on the SQLite backend

Issue #2607. Parent epic #2019. Consumer: autumn-foundation/harvest-camel#9.

## 1. Problem

`ctx.execute_activity_external(...)` does not run on SQLite. The backend
rejects `ScheduleExternalActivity` and seals the run `FAILED` with
`UnsupportedFeature`. SQLite has no table of handoffs, no way to settle a
token, and no read of pending handoffs. An embedder on one SQLite file
cannot use request/reply through harvest-camel.

## 2. Facts

- The core context emits `ScheduleExternalActivity` with a fresh token. On
  each later replay that has no result, it emits the same command again
  with the same token.
- The core matcher accepts `ActivityCompletedExternally`,
  `ActivityFailedExternally` and `ActivityTimedOut` as the result. It skips
  `ActivityExternalDeadlineExtended`.
- Postgres writes `ActivityAwaitingExternal` and a `harvest_external_tasks`
  row in the decision transaction. It parks the workflow task.
- Postgres `complete_externally` and `fail_externally` return `false` for a
  token that is not `PENDING`, or for a run that has ended. An unknown
  token is `NotFound`. An output nested past the depth cap fails the
  activity instead.
- The Postgres scanner times out a `PENDING` row past its deadline. It
  writes `ActivityTimedOut { ScheduleToClose }` and sets `TIMED_OUT`.
- Postgres `list_external_handoffs` orders by deadline, then by update
  time, then by token.
- SQLite has one writer. Every public write takes `&mut self`, so no cycle
  runs while a settle writes.
- SQLite fires a durable timer from `harvest_timers` in the worker pass. A
  reserved timer id prefix already routes a deadline (`__signal_timeout:`,
  `__child_timeout:`).

## 3. Brainstorming

The chosen option is in bold.

| Question | Options |
|---|---|
| Where the handoff lives | events only · **a `harvest_external_tasks` table, as on Postgres** |
| What a row holds | identity only · **identity, input, queue, state, created and deadline times** |
| How the deadline fires | a scan of the new table in each pass · **a `harvest_timers` row with the reserved prefix `__external_deadline:`** |
| What the fire writes | `TimerFired` · **`ActivityTimedOut { ScheduleToClose }`, as Postgres** |
| How a settle writes | stage it, as a signal · **append it at once, as Postgres** |
| How a drive reports the park | `WaitingTimer` · **a new `RunState::WaitingExternal(token)`** |
| What a run end does to a `PENDING` row | leave it · **set it `CANCELLED` in each terminal setter** |
| A settle on an overdue run | append, and let the next drive seal · **seal `TIMED_OUT` first, then return `false`** |
| A deadline extension | out of scope · **`extend_external_deadline`, as Postgres `extend_deadline`** |

## 4. Reverse brainstorming

How can the port fail? Each row names the guard.

| Failure | Guard |
|---|---|
| The deadline fires after a settle and writes a second result. | A settle deletes the timer row. A fire also checks that the row is `PENDING`. |
| A re-park writes a second `ActivityAwaitingExternal`. | The arm skips a token that history already holds. The row insert ignores a known token. |
| A re-park moves the deadline. | The timer arms only with the first row. |
| A settle lands after the terminal event. | A settle refuses a run that is not `RUNNING`. |
| A dispatcher keeps a handoff of an ended run. | Each terminal setter sets `PENDING` rows `CANCELLED`. |
| A typo in a token passes silently. | An unknown token is `ExternalTaskNotFound`. |
| A deep output makes the history unreadable. | The settle fails the activity instead, as Postgres. |
| `run_until_idle` counts the park as progress and spins. | The fleet pass lists `WaitingExternal` as blocked. |
| The park reports `WaitingTimer`, because the deadline row exists. | The block check reads the external rows before the timers. |
| A purge leaves orphan rows. | Retention lists the new table. |
| An old file has no table. | `CREATE TABLE IF NOT EXISTS` adds it on open. |
| A `join!` puts another awaitable between an external record and its result, and replay waits forever. | An external activity must be the only awaitable in its batch, as on Postgres. |
| A reply after the deadline wins, because no drive fired the timer yet. | A settle or an extension times out a due token first. |
| A huge deadline has no `DateTime` form, so the read fails. | The deadline saturates at the last `DateTime`. |
| A side effect or marker lands after the external record, and the matcher stops at it. | A batch that would record one after a pending external activity is refused. |

## 5. Six hats

- **White.** The facts are in section 2. The conformance suite needs
  Docker or `HARVEST_TEST_DATABASE_URL`.
- **Red.** Many SQLite tests use the external activity as their stock
  unsupported command. They need a new one: the worker session acquire, or
  a cross-type continue-as-new.
- **Black.** A new `RunState` variant breaks an exhaustive match outside
  the crate. The crate is pre-1.0, and the changelog names the change. A
  timeout scenario depends on the wall clock, so it uses a one-second
  deadline and waits on events.
- **Yellow.** harvest-camel request/reply runs on one SQLite file. The
  history replays on the core `WorkflowReplayer` and matches Postgres.
- **Green.** The deadline reuses the timer path, so `_as_of` drives and
  a reopen need no new code.
- **Blue.** Red: integration tests and conformance scenarios first. Green:
  the smallest code that passes. Refactor: one module, `external_task.rs`.

## 6. Decision

- A new module `external_task.rs` owns the table, the settle writes, the
  read and the deadline fire.
- `apply_commands` records the handoff in the cycle transaction: the event,
  the row and the deadline timer.
- `fire_timer_within_tx` routes an `__external_deadline:` id to the timeout.
- `SqliteRuntime` gains `complete_external`, `fail_external`,
  `extend_external_deadline` and `list_external_handoffs`.
- The terminal drain records nothing for an unawaited external activity, as
  Postgres `NoRecord`.

## 7. Acceptance criteria

1. A run that awaits an external activity records `ActivityAwaitingExternal`
   with its token, input and deadline, and parks as `WaitingExternal`.
2. `complete_external` and `fail_external` settle a `PENDING` token and
   return `true`. A second settle returns `false`. An unknown token fails.
3. `list_external_handoffs` reads pending handoffs by activity name,
   ordered by deadline.
4. The deadline fires through the timer path and records
   `ActivityTimedOut { ScheduleToClose }`.
5. The conformance suite compares a completed, a failed and a timed-out
   external activity with Postgres.
