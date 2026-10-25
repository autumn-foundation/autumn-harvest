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
| `a_parked_condition_resumes_on_the_drive_after_the_signal` | Added in green. It pins the `Stuck` drive and the resume. |

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
| R5 | A late signal wins the second of two races on one name. Review found it in a first-signal-only rule. | The planner checks deadlines before every signal and fires each one once. `a_late_signal_loses_the_second_of_two_same_name_races` and `ingest_fires_each_deadline_once_in_arrival_order` pin it. |
| R6 | A history fails the core replayer. | Every new test that writes history replays on `WorkflowReplayer`. |
| R7 | Ingest makes no progress visible, so the loop stops early. | A non-empty ingest counts as progress. |
| R8 | An unread signal now reaches history, and strict replay reports `EarlyCompletion`. | Same as Postgres. The race tests drain later signals. The changelog names the behaviour change. |
| R9 | A terminal run on an old file still holds an undelivered row. | The `reuse_policy` tests seed that legacy row. |
| R10 | A panic seal drops the signals that its replay saw. Codex found it. | The seal applies the plan before `WorkflowFailed`. `a_panic_seal_records_the_signal_that_replay_saw` pins it. |
| R11 | A corrupt row of any name blocks the run with an unclear error. | The `SqliteError::Corrupt` message names the row. `a_corrupt_staged_row_fails_the_drive_and_stays_staged` pins it. |
| R12 | An unsupported-feature seal drops the signals that its replay saw. Codex found it. | The seal plans again at the same `now` and applies the plan before `WorkflowFailed`. `an_unsupported_seal_records_the_signal_that_replay_saw` pins it. |
| R13 | The daemon restores a session that a corrupt row of another name blocks. Codex found it. | The restart checks every staged row as the backend reads it. `an_unreadable_staged_row_of_another_name_is_refused` pins it. |
| R14 | `continue_as_new` (#2023) moves replayed signals to the successor, so a drained signal runs twice. | The hand-off transaction delivers the plan to the old run first, as on Postgres. The user chose this over a move. `a_drained_signal_is_not_delivered_again_after_the_hand_off` pins it. |
| R15 | Planned signals miss the byte cap, so a run completes over it. Codex found it. | The cap counts the planned bytes as stored history. `ingested_signal_bytes_count_toward_the_byte_cap` pins it. |
| R16 | A signal flood is read whole into memory before any cap check. Codex found it. | The plan reads rows one at a time and stops at the room the caps leave. `a_signal_flood_is_planned_only_up_to_the_event_cap` and `a_plan_stops_at_its_budget` pin it. |
| R17 | A backlog past the caps meets a `continue_as_new`. The old run replays only a prefix. Codex found it. | The prefix stays with the old run, and the rest moves to the successor, so each signal arrives once. `a_backlog_past_the_caps_moves_on_at_the_hand_off_exactly_once` pins it. |
| R18 | One signal's group of due deadlines overshoots the budget. Codex found it. | The plan checks the room before each event. `a_plan_checks_its_budget_before_each_deadline_event` pins it. |

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
2. `worker::SignalIngest::plan` builds the events. Before each signal, it adds
   each race deadline of that name that falls at or before the signal arrival.
   Each deadline fires once. Then it adds the signal.
3. `drive_one_cycle_inner` adds the planned events to the replay history.
4. The completed, failed, suspended and panic-seal transactions apply the
   plan first. The unsupported-feature seal plans again and applies it.
5. `ingest_awaited_signal` and `peek_pending_signal` go away. `WaitForSignal`
   writes nothing in `apply_commands`.

## 2. Out of scope

- A wait state for `await_condition` (B6).
- Merging unrelated due timers with signals (B5, #1071).
