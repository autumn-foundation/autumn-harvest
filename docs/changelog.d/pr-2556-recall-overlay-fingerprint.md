## Phase 2452.4 — Recall sweep fingerprint reads the active overlay (issue #2556)

The `recall::sweep` fingerprint now hashes the op profile. With an overlay
store, that is the bare profile with the active overlay applied, through
`AgentHarness::overlaid_profile`. The model ids come from that profile too.
An activation therefore gives new agents, and a rerun on the same file calls
the model. A harness with no overlay store hashes the same bytes as before.
An overlay that does not apply fails its item, as it fails an op.

Tests: `a_rerun_after_an_activation_runs_again` and
`a_rerun_with_no_activation_calls_no_model` in
`autumn-harvest-agent/tests/recall.rs`.
