## Batch and webhook-delivery starts take the execution timeout (issue #2417)

A batch start resolved the execution timeout of its type, with any operator
override, but used it only to clamp the SLA. So a batch run had no
`deadline_at`, while a single API start of the same type had one.

What shipped:

- `POST /workflows/batch_start` passes the resolved timeout into the start.
  The immediate path sets `StartWorkflowParams.execution_timeout`. The
  throttled path stores `execution_timeout_secs` on the pending row, so the
  scanner fire gets the timeout too. Both paths already passed the fleet
  ceiling. The start caps the timeout at that ceiling and clamps the SLA to
  the result.
- The outbound webhook-delivery start had the same gap. It applied the SLA
  override and dropped the timeout. It now takes the declared timeout, the
  override and the fleet ceiling.
- The completion-trigger start sites already took these values (#2511). New
  DB tests read `deadline_at` and `sla_deadline_at` on all three sites: the
  same-shard start, the immediate relay (`DeferredTriggerStart::spawn`) and
  the outbox scanner.

**Behavior change.** A batch run of a type with a declared or overridden
`execution_timeout` now times out at that deadline. A throttled row stored
before the upgrade keeps no timeout.

No migration. No new `WorkflowEvent` variant. No new query.

Tests:

- `autumn-harvest-plugin/tests/run_defaults_integration.rs`:
  `a_batch_start_takes_the_timeout_override`,
  `a_throttled_batch_start_takes_the_timeout_override` and
  `the_fleet_ceiling_caps_a_batch_start_override`.
- `autumn-harvest/tests/integration/run_defaults_start_paths_tests.rs`:
  `each_target_site_sets_the_override_deadline_and_clamps_the_sla` and
  `each_target_site_caps_the_override_deadline_at_the_fleet_ceiling`.
- `autumn-harvest-plugin/src/plugin.rs`: `webhook_run_defaults_tests`.

Design: [`docs/design/DESIGN-2417.md`](../design/DESIGN-2417.md).
