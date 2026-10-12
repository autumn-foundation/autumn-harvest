# Design — Issue #2020: SQLite ingests every staged signal

The SQLite backend appends a staged signal to history only when the workflow
reaches a blocking `wait_for_signal`. The non-blocking reads and the push
handlers never see a staged signal. A run can complete `Ok` and lose every
signal sent to it.

**No migration. No new `WorkflowEvent` variant. No public API change.**

---

## 0. Planning record

### 0.1 Facts found before the plan

- `apply_commands` ingests a signal only for a `WaitForSignal` command, one
  signal per command, after replay.
- `drain_signals_raw`, `try_receive_signal` and push handlers read only
  `SignalReceived` events that are already in history.
- `await_condition` emits no command. A run parked on it with no event to
  read is `SqliteError::Stuck`.
- Postgres ingests every pending signal at task preparation, before the
  handler runs (`ingest_due_timers_and_signals`). The core matcher buffers
  signals by name, so it accepts any arrival order.
- The #476 rule records a due `__signal_timeout:{seq}:{name}` deadline ahead
  of a later signal of that name. An unrelated timer must not go ahead of a
  signal (#1071).
- The non-determinism and panic gates promise that a discarded cycle leaves
  `harvest_events` unchanged.

### 0.2 Experiments run before the plan

Red tests in `tests/integration/signal_ingest.rs`, on trunk-dev `7a751d6`:

| Test | Result before the fix |
|------|-----------------------|
| `drain_loop_keeps_every_signal` | `Completed(0)`. Both signals lost. |
| `try_receive_signal_returns_a_staged_signal_without_blocking` | `Completed(null)`. |
| `signal_handler_and_await_condition_complete_with_the_payload` | `Err(Stuck)`. |
| `other_names_are_ingested_in_arrival_order` | The drain after the wait is empty. |
| Two race tests and the empty `try_receive_signal` | Pass. They are guards. |

### 0.3 Brainstorm — how can the run see its signals?

| # | Idea | Verdict |
|---|------|---------|
| B1 | Ingest every staged signal in its own transaction before replay. | Rejected. A discarded cycle (non-determinism, panic) then changes history. |
| B2 | Plan the ingest before replay. Replay sees it in memory. The cycle transaction that persists the outcome also persists the ingest. | **Adopted.** One transaction, and a discarded cycle writes nothing. |
| B3 | Keep the `WaitForSignal` ingest beside the new one. | Rejected. B2 already consumes every staged signal, so the old path is dead code. |
| B4 | Reject `drain_signals`, `try_receive_signal` and handlers as `UnsupportedFeature`. | Rejected. The issue names it only as a fallback. B2 is small. |
| B5 | Merge every due timer with the signals, as Postgres does. | Rejected. An unrelated `TimerFired` ahead of a signal wedges replay (#1071). Keep the #476 deadline-only rule. |
| B6 | Add a `WaitingCondition` state for a run parked on `await_condition`. | Deferred. It is a public API change. The run still resumes on the next drive after a signal. |

### 0.4 Reverse brainstorm — how can this change do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | A late signal wins a signal-or-deadline race. | The planner records the due deadline of that name first. `a_late_signal_loses_to_the_deadline_under_eager_ingest` and `signal_timeout.rs` pin it. |
| R2 | Ingest order follows insertion, not arrival. | The planner sorts by `received_at`, then `signal_seq`. `other_names_are_ingested_in_arrival_order` pins it. |
| R3 | Replay sees one history and storage holds another. | One list feeds both. The cycle appends it first, in list order. |
| R4 | A discarded cycle consumes signals. | Only the persisting transactions apply the ingest. The signals stay staged for the next drive. |
| R5 | The same deadline fires twice for two signals of one name. | The planner keeps a set of planned timers. |
| R6 | A history fails the core replayer. | Every new test replays on `WorkflowReplayer`. |
| R7 | Ingest makes no progress visible, so the loop stops early. | A non-empty ingest counts as progress. |

### 0.5 Six thinking hats

- **White (facts).** Sections 0.1 and 0.2.
- **Red (feelings).** Eager ingest changes when every signal reaches history.
  That feels risky for the race code. The race guards stay green.
- **Black (risks).** Section 0.4. A corrupt staged payload now fails each
  cycle, not only a wait for that name. Postgres does the same.
- **Yellow (benefits).** Drains, `try_receive_signal`, push handlers and
  `await_condition` work as on Postgres. A wait resolves one cycle earlier.
- **Green (ideas).** Section 0.3, B6, as a follow-up.
- **Blue (process).** Red tests, then the planner, then removal of the old
  path. Then the docs. Then a multi-angle review.

---

## 1. Change

1. `store::pending_signals` reads every undelivered signal of a run, in
   arrival order.
2. `worker::SignalIngest::plan` builds the events. For each signal, it first
   adds each due deadline timer of that name. Then it adds the signal.
3. `drive_one_cycle_inner` adds the planned events to the replay history.
4. The completed, failed and suspended transactions apply the plan first.
5. `ingest_awaited_signal` and `peek_pending_signal` go away. `WaitForSignal`
   writes nothing in `apply_commands`.

## 2. Out of scope

- A wait state for `await_condition` (B6).
- Merging unrelated due timers with signals (B5, #1071).
