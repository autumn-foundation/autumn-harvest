## Agents — Profile overlays under ceilings (issue #2370, epic #2369)

`agent::overlay` adds `ProfileOverlay`, `Ceilings`, `ModelRegistry`,
`OverlayError`, `GUARDRAILS_HEADING` and the pure kernel `apply`. The crate
root re-exports each of them except `apply`. A model writes an overlay, and
`apply` merges it into an `AgentProfile`. An overlay sets only `system`,
`guardrails`, `model`, the three bounds and `read_only`. It is
`deny_unknown_fields`, so a JSON overlay that names `tools`, `mandate` or
`tenant` fails to decode.

`apply` checks the result, not only the overlay. No result bound may be
above its ceiling. A `None` bound is above every set ceiling. A cleared
`read_only` needs `allow_clear_read_only`. The ceilings must hold the
result model. The profile mandate and the registry must hold an overlay
model. Each refusal is a named `OverlayError` variant. Under
`Ceilings::from_profile`, an overlay can only narrow the profile. The
guardrails go after the system prompt under `## Guardrails`, once.
`AgentHarness::has_model` is new.

No event, migration or SQL changes. The design is in
`docs/design/DESIGN-2370.md`.

Tests: `formal/verus/src/agent_overlay.rs` mirrors `merged_bound` and
`merged_read_only`. It proves the ceiling, read-only, model, unchanged-field
and idempotency properties of a spec of `apply`, and when `apply` refuses.
The unit tests in `agent/overlay.rs` cover each criterion of the issue. The
proptests `apply_matches_the_oracle`, `apply_is_idempotent`,
`a_result_stays_under_the_ceilings`, `an_empty_overlay_keeps_any_profile`
and `default_ceilings_only_narrow` check the engine `apply`.
