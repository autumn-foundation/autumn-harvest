## Phase X.Y — Profile overlays under ceilings (issue #2370)

`agent::overlay` adds `ProfileOverlay`, `Ceilings`, `ModelRegistry` and the
pure kernel `apply`. A model writes an overlay, and `apply` merges it into
an `AgentProfile`. An overlay sets only `system`, `guardrails`, `model`,
the three bounds and `read_only`. It is `deny_unknown_fields`, so a JSON
overlay that names `tools`, `mandate` or `tenant` fails to decode.

`apply` checks the result, not only the overlay. Each bound must be under
its ceiling, and `None` is no bound. A cleared `read_only` needs
`allow_clear_read_only`. The registry must hold an overlay model, and the
ceilings must hold the result model. Each refusal is a named
`OverlayError` variant. `Ceilings::from_profile` is the default, so an
overlay may only narrow. The guardrails go after the system prompt under
`## Guardrails`, once. `AgentHarness::has_model` is new.

No event, migration or SQL changes. The design is in
`docs/design/DESIGN-2370.md`.

Tests: `formal/verus/src/agent_overlay.rs` mirrors `merged_bound` and
`merged_read_only` and proves the ceiling, read-only, model, unchanged-field
and idempotency properties of a spec of `apply`. The unit tests in
`agent/overlay.rs` cover each criterion of the issue. The proptests
`apply_is_idempotent`, `a_result_stays_under_the_ceilings` and
`default_ceilings_only_narrow` check the engine `apply`.
