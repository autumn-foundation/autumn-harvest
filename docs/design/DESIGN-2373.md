# DESIGN-2373: tool guidance and skills as overlay parts

Issue #2373. Parent: #2369. Needs: #2370.

## Problem

Harness² edits eight parts of an agent setup. Two parts have no home in
`AgentProfile`:

- **Tool guidance.** This text tells the model how to use a tool well.
- **Skills.** This is domain know-how. The agent loads it on demand. It
  does not carry it in every prompt.

Both parts are text, so an overlay can carry them.

## Decision

### Tool guidance

- `ProfileOverlay`, `AgentProfile` and `AgentTask` get
  `tool_guidance: BTreeMap<String, String>`. The key is a tool name.
- `apply` merges the overlay map over the profile map. An overlay key wins.
- Each key of the result must name a registered tool. Else `apply` fails
  with `OverlayError::UnknownTool`.
- A registered tool is an app tool of `AgentHarness::tool`. A built-in,
  such as `memory` or `delegate`, is not one.
- `ModelTurnRequest::tool_guidance` carries the map. The harness puts the
  text in place of `Tool::description()` in the `ToolDefinition`.
- The tool keeps its name, schema, `effect()`, labels and policy. The
  guidance changes only the text that the model reads.

### Skills

- `SkillText` holds a one-line `summary` and a `body`.
- `ProfileOverlay`, `AgentProfile` and `AgentTask` get
  `skills: BTreeMap<String, SkillText>`. `apply` merges them as it merges
  the guidance.
- `apply` refuses a skill with `OverlayError::InvalidSkill` when its name is
  not a valid name, or its summary is empty, has a line break or is too
  long.
- The workflow adds a `## Skills` section to the system prompt. It lists
  each name and summary. It never lists a body.
- With skills, the model gets the `load_skill` tool. Its effect is
  `ToolEffect::ReadOnly`.
- A `load_skill` call runs as an ordinary `agent_tool_call` activity. The
  workflow reads the body from the task and puts it in
  `ToolCallRequest::skill`. The harness returns it, cut like any tool
  result. History records the result, so replay never reads a body again.
- Without skills, the model gets no `load_skill` tool. A call to it is an
  unknown tool. The harness never shows an app tool with that name.

### The skill source

The task is the skill source. It is the recorded workflow input. The issue
#2372 records the overlay read, so a refined profile reaches the task in
the same way. A store of the harness would need the overlay version of
each run (#2371). So it waits for that issue.

The body is in the tool-call input, so the activity needs no lookup. The
workflow cuts the body to fit half of the request cap. The harness then cuts
it to the tool output limit.

## Brainstorm

| Question | Options | Choice |
|---|---|---|
| Where guidance goes | The system prompt. The tool definition. | The definition, as the issue says. A provider reads it next to the schema. |
| Which tools take guidance | Every tool. App tools only. | App tools only. A built-in is crate code, and its text is part of its contract. |
| Merge of two maps | Replace the map. Merge by key. | Merge by key, overlay wins. `apply` stays idempotent. |
| Where a body lives | A store on the harness. The task. | The task. It is recorded, so replay and probes see the same text. |
| How the activity gets a body | It reads the task. It reads a store. It reads its input. | Its input. An activity cannot read the task. |
| How the model learns the names | The tool schema enum. The system prompt. | The system prompt, as the issue says. The tool schema stays fixed. |
| A bad skill name | Escape it. Refuse it. | Refuse it. A name joins the prompt, so it holds no markup. |

## Reverse brainstorm

How can these parts break a run, or escape the operator? Each attack has a
guard.

| Attack | Guard |
|---|---|
| Guidance for a tool that the harness does not have. | `apply` refuses it with `UnknownTool`. |
| Guidance that claims a tool is safe, to change its effect. | Guidance changes only the description. The policy reads `effect()`. |
| Guidance for `delegate` or `memory`. | Only app tools are registered tools. |
| A body in the summary, to grow every prompt. | A summary is one line of at most 200 characters. |
| A skill name with a line break or a heading. | A name has only `a-z`, `0-9`, `_` and `-`. |
| A body larger than the request cap. | The workflow cuts it in the tool-call input. The harness cuts it in the result. |
| An app tool named `load_skill`, to take the place of the built-in. | The name is reserved. The harness never shows that app tool. |
| A `load_skill` call in a run with no skills. | The tool is not offered, so the call is an unknown tool. |
| A `load_skill` call in a read-only run. | It is `ReadOnly`, so it runs. It reads only operator text. |
| A call with no `name` or an unknown name. | The tool returns an error. The run goes on. |
| A replay that reads a new body. | The result is recorded. Replay reads it from history. |
| An old history with no skills. | Every new field is empty and skipped, so a recorded input keeps its bytes. |

## Six hats

- **White.** `AgentProfile` has no guidance and no skills. The tool
  definition comes from `Tool::definition`. A built-in tool such as
  `memory` already runs as an `agent_tool_call`. `eval` stubs each tool
  call that has no recorded outcome.
- **Red.** A model that rewrites tool text is a risk. It must never change
  what a tool may do. A refusal is better than a silent drop.
- **Black.** A large guidance map grows every model-turn request. No
  ceiling caps it yet. The request cap still ends the run as
  `TranscriptFull`. #2394 can add a ceiling. The body is recorded twice:
  in the tool-call input and in its result.
- **Yellow.** Domain know-how leaves the prompt. A run pays for a skill
  only when it loads it. A probe sees the same text as a live run.
- **Green.** A skill store keyed by overlay version can replace the body
  in the input after #2371. The tool and the listing stay.
- **Blue.** Spec and proof first. Then red tests, the code, and the
  gates: `cargo fmt`, `cargo clippy`, the Verus suite and
  `comment-hygiene.py`.

## Proof

`formal/verus/src/agent_overlay.rs` models the guidance keys as a set of
integers. `apply` refuses exactly when a key of the merged set is not a
registered tool. The theorems keep their form: the ceiling, idempotency
and empty-overlay properties hold with the guidance. A spec of the tool
definition proves that guidance keeps the name and the effect.

## Tests

| Criterion | Test |
|---|---|
| A scripted model sees the new description for `lookup` only. | `guidance_replaces_only_the_named_description` in `harness.rs` |
| Guidance for an unregistered tool fails `apply`. | `guidance_for_an_unregistered_tool_is_refused` in `overlay.rs` |
| `load_skill("missing")` returns an error. The run goes on. | `a_missing_skill_is_an_error_and_the_run_goes_on` in `workflow.rs` |
| Replay of a `load_skill` call makes no call to the source. | `replay_reads_a_loaded_skill_from_history` in `workflow.rs` |
| Two large skills fit when listed. A large body is cut. | `large_skills_fit_when_listed` and `a_long_body_is_cut_like_a_tool_result` in `workflow.rs` |
| `load_skill` is refused with no skills. | `load_skill_is_unknown_without_skills` in `harness.rs` |
| `evaluate` stubs a new `load_skill` call. | `a_new_skill_load_counts_as_a_stubbed_call` in `eval.rs` |
