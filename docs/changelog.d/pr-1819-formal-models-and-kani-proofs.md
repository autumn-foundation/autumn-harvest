## Testing — TLA+ models of the core protocols and Kani proofs (issue #1819)

**TLA+ models.** `formal/tla/` holds three models. TLC checks each one in
the new `formal-models` CI job, through `scripts/check-formal-models.sh`.
The runner reads `formal/tla/models.txt` and pins `tla2tools.jar` 1.7.4 by
SHA-256.

- `ActivityClaim`: claim, start, heartbeat, orphan reclaim, re-claim and
  complete, with the claim-epoch fence of #1789. The fixed config passes
  (3 workers, 5 claims). `ActivityClaimPreFix.cfg` turns the fence off and
  reproduces the #1789 bug: a stale owner completes after a later claim.
  `ActivityClaimReach.cfg` proves that the fixed model reaches that race.
- `WorkflowTaskClaim`: the workflow-task terminal guard. The pre-fix config
  reproduces the #1806 bug.
- `CodecRotation`: the re-encryption sweep against erasure. A blind write
  resurrects an erased payload. The compare-and-swap write does not.

**Kani proofs.** Seven `#[kani::proof]` harnesses run in the new `kani` CI
job. They cover the jitter bounds and `compute_retry_delay` (`policy.rs`),
the lifecycle transition table (`lifecycle.rs`) and the seeded chaos plan
(`chaos.rs`). Kani did not finish on the `failure_signature` string code,
so its fuzz target and proptests stay its only checks. `full_jitter` and
`equal_jitter` now call `full_jitter_nanos` and `equal_jitter_nanos`, so the
proofs cover the arithmetic directly. Behavior is unchanged.

**Guard.** `formal_models_coverage` (no DB) fails when a TLC config has no
manifest row, when CI does not run the runner or Kani, when the crate has
fewer than three proofs, or when `docs/testing/formal-methods.md` does not
name a model or proof.

No migration. No `WorkflowEvent` change. Model (d) and trace conformance
stay open; see `docs/testing/formal-methods.md`.
