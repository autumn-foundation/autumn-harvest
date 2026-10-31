## Agents — Tool guidance and skills as overlay parts (issue #2373, epic #2369)

`ProfileOverlay`, `AgentProfile` and `AgentTask` get `tool_guidance` and
`skills`. `skill` adds `SkillText`, `LOAD_SKILL_TOOL`, `SKILLS_HEADING`,
`MAX_SKILL_SUMMARY_CHARS` and `check`. `overlay` adds `ToolRegistry`, and
`OverlayError` adds `UnknownTool` and `InvalidSkill`.
`AgentHarness::has_tool` is new.

- **Tool guidance.** The text replaces the description of an app tool in
  the definition that the model sees. The name, the schema, the effect and
  the policy stay. `apply` refuses a key that names no app tool.
- **Skills.** The system prompt lists each name and summary, never a body.
  The model gets `load_skill`, with `ToolEffect::ReadOnly`. A call is an
  ordinary `agent_tool_call`. The workflow reads the body from the task, so
  history records it and replay reads it from there. The body is cut like
  any tool result. Without skills, the tool is not offered. The name
  `load_skill` is reserved.
- `apply` merges each map over the profile map, and an overlay entry wins.
  It checks the result, so a stale profile key fails too.

`ModelTurnRequest` gets `tool_guidance` and `skills`. `ToolCallRequest`
gets `skill`, a `SkillLookup`. Each new field is skipped when empty, so a
recorded history keeps its bytes. No event, migration or SQL changes. The
design is in `docs/design/DESIGN-2373.md`.

Tests: `formal/verus/src/agent_overlay.rs` models the guidance keys. It
proves that `apply` refuses exactly when a key names no registered tool,
and that guidance changes only the description. `tests/skills.rs` covers
each criterion of the issue on SQLite and in `eval::evaluate`. The overlay
proptests check the new rules against the oracle.
