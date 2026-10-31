## Agents — An evaluation candidate takes a whole overlay (issue #2375, epic #2369)

`eval::Candidate::overlay` runs a recorded history under a whole
`ProfileOverlay`. The harness reads the source profile from the recorded
task and applies the overlay with `overlay::apply` before the engine
starts. A refused overlay returns `EvalError::Overlay` and costs no model
call. The default ceilings are `Ceilings::from_profile` of the source, so
an overlay can only narrow it. `Candidate::ceilings` sets operator
ceilings. `Candidate::system_prompt` sets the overlay prompt.
`Candidate::model` sets the overlay model, admits it past the ceilings and
still asks it for every turn. `Candidate::agent_model` stays a route.
`Evaluation::overlay_digest` holds `ProfileOverlay::digest`, a SHA-256 of
the canonical JSON. A candidate report now shows the recorded run id, so
two evaluations of one candidate compare equal.

This change builds the tool guidance part of #2373. Skills stay in #2373.
`ProfileOverlay`, `AgentProfile`, `AgentTask` and `ModelTurnRequest` carry
`tool_guidance`, a map from a tool name to new text. The harness replaces
the description of each named app tool in the `ToolDefinition` that the
model sees. The effect, the policy and the eval matching rule do not
change. `apply` refuses a key that names no harness app tool with
`OverlayError::UnknownTool`, and a text over `MAX_TOOL_GUIDANCE_BYTES`
with `OverlayError::GuidanceTooLong`. A built-in tool name is not an app
tool. `AgentHarness::has_tool`, `BUILTIN_TOOLS` and
`ModelRegistry::has_tool` are new. The trait method has a default that
holds no tool. A delegated child takes no guidance. `compare` checks both
overlays before the first model call.

Behaviour change: `Candidate::model` now sets the task model and checks
it before any call. A model that the harness does not hold used to fail at
the first turn. A model that the source mandate forbids used to run
unchecked. Both now fail with `EvalError::Overlay`. A delegated child
with no peer model now inherits the model.

No event, migration or SQL changes. Serde skips an empty map, so a
recorded task and a recorded turn input replay as they ran. A struct
literal of `AgentTask`, `AgentProfile`, `ModelTurnRequest`,
`ProfileOverlay` or `Evaluation` needs the new field. The design is in
`docs/design/DESIGN-2375.md`.

Tests: `autumn-harvest-agent/tests/eval.rs` covers each criterion of the
issue and the early refusal of `compare`. `durable_agent.rs` shows that
profile guidance reaches the model and the history. Unit tests show that
every overlay part reaches the task, that a built-in tool keeps its text
and that a delegated child takes no guidance. `overlay.rs` adds
`UnknownTool`, `GuidanceTooLong`, merge and digest tests, and the oracle
proptest covers guidance. The Verus model in
`formal/verus/src/agent_overlay.rs` adds the guided tools, the long-text
flag and the tool registry. `theorem_apply_refuses_exactly` covers both
new refusals.
