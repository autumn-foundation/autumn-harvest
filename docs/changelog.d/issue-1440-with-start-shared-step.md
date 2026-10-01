## Phase 6.1 — Share the with-start admission step (issue #1440)

Signal-with-start and update-with-start no longer duplicate their start path.

- `with_start_params!` builds `StartWorkflowParams` for both routes. The start
  provenance is a macro argument, so the `start_source_override` distinction
  stays explicit.
- `start_or_attach` runs the shared steps: policy resolution, start, input
  check, terminal-prior escalation, and the debounce gate. The live states and
  the input schema are data arguments, not a mode flag.
- `StartEffects` replaces the three deferred-work vectors and the post-commit
  tail.
- No public API change. No migration. No new `WorkflowEvent` variant. No change
  to `harvest_events` writers.

Tests: `with_start_shared_tests.rs` pins both routes before and after the
change. Unit tests cover the live-state predicate, the escalation rule, the
builder, and the effects collector.
