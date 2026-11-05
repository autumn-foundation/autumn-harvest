# DESIGN-2461 — A tool round over the cap resets the context

Issue #2461. Parent epic #2452. Follow-up to #2453 (PR #2456).

## 1. Problem

With `ContextReset` on, the loop checks the threshold only before a
request. A tool round measures the next request first, with `RoundBudget`.
When a result does not fit the cap, the round drops it and returns
`RoundEnd::Full`. The run then ends as `transcript_full`, also when resets
are left.

Example: the cap is 20 000 bytes and the threshold is 50 %. The request is
9 000 bytes, so no reset is due. A tool returns 12 000 bytes. The round
drops the result, and the run ends. A reset would have saved the run.

PR #2456 shipped in release 0.9.0. A run recorded under its rules must
replay as it ran.

## 2. Brainstorming

The chosen option is in bold.

| Question | Options |
|---|---|
| What the round does with the result | run it again after the reset · keep it in the reset window · **drop it, as today** |
| What the reset window keeps of the round | nothing · each call with a short fixed answer · **the assistant turn and its tool message, as recorded** |
| Where the kept round goes | after the notes · **after the bookings, before the notes** |
| Does the save turn run first | never · **when the notice and `max_notes_bytes` fit the cap** |
| Where the branch decides | before the next request · **in the `RoundEnd::Full` arm** |
| How replay of a #2456 run stays the same | a task field · **the patch marker `agent_reset_on_full_round`, asked at the branch** |
| The kernel | inline in the workflow · **`bounds::full_round_resets`, with a Verus proof** |

The kept round shows each call with the answer that it has today. A call
that ran shows its result or `DROPPED_FULL`. A call that did not run shows
`NOT_RUN_FULL`. So the model sees which calls ran. A short fixed answer
for every call would hide a denial among calls that ran, because the round
does not record which calls ran.

## 3. Reverse brainstorming

"How could this change break a run?" Each answer gives a rule.

| Failure | Rule |
|---|---|
| The model runs a call with an effect again after the reset. | The reset window keeps the round. Each call shows its answer, so the model sees that the call ran. |
| A run recorded under #2456 takes the new branch on replay. | The branch asks the patch marker. An old history holds no marker there, so the run ends as it ran. |
| A run with no `context_reset` asks the marker and changes its history. | Only a task with the setting and a reset left asks the marker. |
| The kept round is large, so the reset frees no room. | The reset window, with the kept round, must fall under the threshold. If not, the run ends as `transcript_full`, as today. |
| The kept round holds the `schedule_followup` booking. The window then holds that call twice. | The window shows the booking of the segment only when the kept round does not hold it. |
| A later reset of the segment keeps a stale round. | A reset clears the kept round. A new segment clears it too. |
| The text of the full turn becomes the answer. | The loop restores the text of the turn before, as today. |
| The save round of this reset does not fit the cap. | The save round resets, as today. It never asks for a second reset. |
| Each round over the cap resets, so the run never ends. | Each reset spends one of `max_resets`. With none left, the run ends. |
| A delta turn names the old window. | A reset clears `Progress::sent`, as today. |
| The window after the reset still exceeds the cap. | The kernel needs the window under the threshold, so under the cap. |

## 4. Six thinking hats

- **White (facts).** The round already answers each call. `DROPPED_FULL`
  and `NOT_RUN_FULL` are short. The round budget reserves a placeholder for
  each call, so the next request with the fixed answers fits the cap. The
  marker API is positional, so a marker asked at the branch matches only a
  history that took it.
- **Red (feeling).** A run that ends with resets left feels broken. A run
  that forgets which mail it sent is worse. The kept round answers both.
- **Black (risks).** A replay that diverges. A duplicate call id. A useless
  reset. A call that runs twice. Section 3 answers each one.
- **Yellow (value).** A tool with a large result no longer ends a long run.
  The model can read the result again in the room that the reset frees.
- **Green (alternatives).** A truncated result instead of a drop changes
  the round budget and every recorded run. A model-driven reset needs the
  RL training of CORAL. Neither fits a P3 change.
- **Blue (process).** Spec and proof of the kernel first. Then red tests,
  one per acceptance criterion. Then the code, the docs and the gates.

## 5. Decision

1. A round ends `Full` outside a save round. The task has `context_reset`.
   The loop builds the reset window with the round kept. The kernel
   `bounds::full_round_resets` gets the reset count, `max_resets`, the size
   of that window, the cap and `reset_at_percent`. It is true when a reset
   is left and the window falls under the threshold.
2. When the kernel is true and `ctx.patched("agent_reset_on_full_round")`
   is true, the reset is due. Else the run ends as `transcript_full`, with
   the report of today.
3. A due reset asks for a save turn first, when the notice and
   `max_notes_bytes` fit the cap. Else it resets at once. The save turn has
   the rules of #2453.
4. The reset window is the system prompt and the task. Then the bookings
   of #2453, the kept round, the notes and the steers follow.
5. The reset reads only recorded data: the round outcome, the window, the
   cap and the reset count. Replay takes the same branch.

`formal/verus/src/agent_reset.rs` proves that the kernel admits only a
window that fits the cap, that it never passes `max_resets`, and that a
zero cap never resets.

## 6. Acceptance criteria and their tests

All tests are in `autumn-harvest-agent/tests/context_reset.rs`.

| Criterion | Test |
|---|---|
| With a reset left, a round over the cap resets, and the run goes on | `a_round_over_the_cap_resets_and_the_run_goes_on` |
| The window after that reset shows which calls ran | `the_window_after_a_full_round_shows_which_calls_ran` |
| A call with an effect does not run twice after the reset | `a_call_with_an_effect_does_not_run_twice_after_the_reset` |
| With no reset left, the run ends as `transcript_full`, report as today | `with_no_reset_left_a_full_round_ends_as_before` |
| A full round after the last reset ends the run | `after_the_last_reset_a_full_round_ends_the_run` |
| A run recorded under the #2456 rules replays unchanged | `a_run_recorded_before_the_rule_replays_unchanged` |
| A crash right after that reset replays to the same window | `crash_after_a_full_round_reset_replays_the_same_window` |
| A kept round that frees no room ends the run as today | `a_kept_round_that_frees_no_room_ends_the_run` |
| A booking in the kept round shows once | `a_booking_in_the_full_round_shows_once` |
