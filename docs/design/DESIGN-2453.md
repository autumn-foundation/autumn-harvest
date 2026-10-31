# DESIGN-2453 — Progress notes and context reset in one agent run

Issue #2453. Parent epic #2452. After CORAL (arXiv 2610.40350).

## 1. Problem

A run whose transcript grows ends as `transcript_full` or
`tokens_exhausted`. Everything that the run confirmed goes with it. A durable
`Agent` trims its transcript with `trim_window`. The trim drops the oldest
turns whole, so a confirmed finding in an old turn goes too.

CORAL gives the agent a working-memory toolset. The agent saves its progress,
clears its context, and restarts from its notes only. This design takes the
tools and the reset. It does not take the RL training.

## 2. Brainstorming

Options for each open question. The chosen option is in bold.

| Question | Options |
|---|---|
| Where the setting lives | **`AgentTask::context_reset` and `AgentProfile::context_reset`, `None` by default** · a harness-wide setting · a `Mandate` field |
| What triggers a reset | **the size of the next request, against the request cap** · the token usage of the run · a model call to a `reset` tool |
| When the model saves | at any time, unasked · **also in a save turn: a fixed notice before the reset, where only `save_progress` runs** |
| What `save_progress` does | **replace the notes whole** · append to the notes · edit named sections |
| Where the notes go after a reset | a user message · the system prompt · **the model's own `save_progress` call and a fixed tool result** |
| What "the task" is | the segment input · **`AgentTask::input`** · the first user message of the history |
| Where a durable agent keeps the notes | in the transcript, kept by `trim_window` · **`AgentState::notes`, shown after the input of each op** |
| Does a delegated child inherit | no setting · **the setting, with no notes** · the setting and the notes |

## 3. Reverse brainstorming

"How could this feature break a run?" Each answer gives a rule.

| Failure | Rule |
|---|---|
| A transcript delta after a reset names the old prefix. The activity rebuilds the old window. | A reset clears `Progress::sent`. The next turn goes in full. |
| Replay takes another branch, so the history no longer matches. | The reset reads only recorded data: the request size, the cap and the reset count. |
| The notes come from a prompt injection and gain the authority of a person. | The notes never go into a user message. They stay in an assistant `save_progress` call, as in `followup.rs`. |
| A reset clears the taint of an untrusted read. | A reset changes only the transcript. `Tree::exercised` stays. Durable notes keep the labels of the run that saved them. |
| A reset drops a steer from a person. | A reset keeps the steer messages of the segment after the notes. |
| A reset drops a round that the model has not read. A call with an effect, such as a sent mail, is then forgotten and runs again. | A save turn comes first. The model reads the round, and only `save_progress` runs in the save turn. The reset runs before the next request. |
| A reset in a follow-up segment drops the `schedule_followup` call. The wake message then points at nothing. | The reset window keeps the last `schedule_followup` call with its result, and the wake message. |
| An unattended segment, such as a heartbeat tick or a peer message, plants notes that a later attended op reads. | A durable agent keeps the notes of a segment only when the segment may write memory and keeps its transcript. |
| The fixed result after the notes outranks a new message from a person. | The fixed result says that the user messages say what to do. |
| A model fills the notes until the request is full again. | `max_notes_bytes` bounds the notes. A larger call gets an error result and leaves the notes unchanged. |
| A window that is still over the threshold after a reset resets on every turn. | A reset runs only when its window falls under the threshold. `max_resets` bounds the run. |
| A durable agent shows two copies of its notes. | The agent strips the restored pair before `trim_window`. Each op adds one fresh pair. |
| The save round does not fit the cap, so the run ends with a reset left. | The save turn reserves `max_notes_bytes`. A save round that still does not fit resets with the earlier notes. |
| The model answers the notice in text, and that reply becomes the answer. | A text reply to the notice ends no segment. The reset runs next. |
| A read-only durable op resets, drops the history and keeps no summary. | Only a segment that may write memory gets the reset. |
| The reset window keeps a refused booking, not the one that woke the segment. | The segment records the booking that woke it when it opens. A refused call books nothing, so it never counts. |
| A recorded run replays under new code. | A task with no `context_reset` field takes no new path. No patch gate is needed. |
| A child reads the private notes of its parent. | A child starts with a new `Progress`, so it has no notes. |

## 4. Six thinking hats

- **White (facts).** `run_segment` already measures each request against the
  cap before it sends it. `schedule_followup` is a tool that the workflow
  handles. The flow labels live in `Tree`, not in the transcript. The
  response cache key hashes the messages and the tools.
- **Red (feeling).** An agent that forgets is a poor agent, but an agent that
  forgets in silence is worse. The model must know that a reset can come.
  The tool description says so.
- **Black (risks).** Model-written text in a user message. A stale delta
  prefix. A replay that diverges. Lost steers. A round that the model never
  reads. Notes that an unattended run plants. Section 3 answers each one.
- **Yellow (value).** A long run continues past the request cap. A durable
  agent keeps confirmed findings after its window drops their turns. The
  setting is off by default, so no run changes.
- **Green (alternatives).** A model-driven `reset` tool, as in CORAL, needs
  the RL training to be useful. A summariser activity costs a model call.
  The loop-driven reset at a threshold needs neither.
- **Blue (process).** Spec, proof, red tests, then code. The threshold kernel
  `bounds::reaches_percent` has a Verus proof. The tests map one to one to
  the acceptance criteria.

## 5. Decision

```rust
pub struct ContextReset {
    pub max_resets: u32,        // per run, follow-ups included
    pub reset_at_percent: u8,   // of AgentTask::request_cap
    pub max_notes_bytes: u32,
}
```

1. With the setting on, the model gets the `save_progress` tool. Its argument
   `notes` replaces the notes of the run. The workflow handles the call, so no
   activity runs. Replay reads the call from the recorded model turn.
2. Before each request, the loop measures it. When the request reaches
   `reset_at_percent` of the cap and resets are left, the reset is due. The
   reset runs only when its window falls under the threshold.
3. A due reset first asks for a save turn. The loop sends the window with a
   fixed notice. In that round only `save_progress` runs. The reset runs
   before the next request, also after a text reply or a round over the cap.
   When the notice and `max_notes_bytes` do not fit the cap, the reset runs
   at once.
4. The new window is the system prompt and the task. In a woken segment,
   the booking that woke it and the wake message follow. A booking of the
   segment itself follows them. Then the notes and the steers of the segment
   follow. The notes are a `save_progress` call with a fixed result.
5. With no resets left, the loop works as before. A request over the cap ends
   the run as `transcript_full`.
6. A durable agent keeps the notes in `AgentState::notes`, with their labels.
   Each op shows them after its input. The agent strips the restored pair
   before `trim_window`. Only a segment that may write memory gets the reset
   and the tool. Only such a segment that keeps its transcript writes the
   notes. `Control::ClearTranscript` clears them.
7. `AgentReport::resets` and `DelegationRecord::resets` count the resets.
   Neither is written when zero.
8. A delegated child inherits the setting and starts with no notes.

A zero cap is no cap, so it never resets. A `reset_at_percent` of 0 counts as
1, and a value over 100 counts as 100.

**Known limit.** A tool round that passes the cap in one step still ends as
`transcript_full`. The reset runs only before a request. Issue #2461 tracks
this limit.

## 6. Acceptance criteria and their tests

All tests are in `autumn-harvest-agent/tests/context_reset.rs`.

| Criterion | Test |
|---|---|
| Unset: an overflow ends as `transcript_full`, report as today | `with_no_setting_an_overflow_ends_as_before` |
| One reset: the next request is system, task and notes; `resets == 1` | `a_run_resets_once_and_restarts_from_its_notes` |
| A save turn runs only `save_progress` | `a_save_turn_runs_only_save_progress` |
| A reset that frees no room does not run | `a_reset_that_frees_no_room_does_not_run` |
| A follow-up reset keeps the booked call | `a_reset_in_a_followup_segment_keeps_the_followup_call` |
| A text reply to the notice still resets | `a_save_turn_that_answers_in_text_still_resets` |
| A save round over the cap still resets | `a_save_round_over_the_cap_still_resets` |
| No room for the notes resets at once | `with_no_room_for_the_notes_the_reset_runs_at_once` |
| A reset keeps the steers of the op | `a_reset_keeps_the_steers_of_the_op` |
| A read-only op keeps no notes | `a_read_only_op_keeps_no_notes` |
| Cap spent: the next overflow ends as `transcript_full` | `a_second_overflow_after_the_cap_is_spent_ends_the_run` |
| A crash after a reset replays to the same window and cache key | `crash_after_a_reset_replays_the_same_window` |
| Notes over `max_notes_bytes` get an error and leave the notes | `oversized_notes_are_refused_and_the_old_notes_stay` |
| Durable `window = 4`: an op 1 note is in the op 3 request | `a_durable_agent_keeps_its_notes_across_the_window` |
| An untrusted read before a reset still trips the flow rule | `a_reset_clears_no_taint` |
| A delegated child starts with no notes of its parent | `a_delegated_child_starts_with_no_notes` |

`formal/verus/src/agent_reset.rs` proves the threshold kernel. The unit
tests of `plan_reset` in `workflow.rs` cover the save turn and the reset at
once.
