## Testing — TLA+ models of the core protocols and Kani proofs (issue #1819)

**TLA+ models.** `formal/tla/` holds three models. The new `formal-models`
CI job checks each one through `scripts/check-formal-models.sh`. The runner
reads `formal/tla/models.txt` and pins `tla2tools.jar` 1.7.4 by SHA-256.

- `ActivityClaim`: claim, start, heartbeat, orphan reclaim, re-claim and
  complete, with the claim-epoch fence of #1789. The fixed config passes
  (3 workers, 5 claims). `ActivityClaimPreFix.cfg` turns the fence off and
  reproduces the #1789 bug: a stale owner completes after a later claim.
- `WorkflowTaskClaim`: the workflow-task terminal and suspension guards.
  The pre-fix config reproduces the #1806 bug.
- `CodecRotation`: the re-encryption sweep against erasure. A blind write
  resurrects an erased payload. The compare-and-swap write does not.

Each claim gets a ghost sequence number, so an invariant cannot restate the
guard that it checks. Each spec has a reachability witness, which proves
that the fixed model reaches the race that the fix closes.

**Open gap found.** The workflow capability-miss release after the handler
starts (`release_task_for_capability_miss_query`) has no `attempt` term.
`WorkflowTaskClaimCapMissGap.cfg` shows a stale cycle that re-pends a later
claim of the same worker. `WorkflowTaskClaimCapMissFix.cfg` shows that the
`attempt` term closes it. The code is not changed in this PR.

**Kani proofs.** Five `#[kani::proof]` harnesses run in the new `kani` CI
job, through `scripts/check-kani-proofs.sh`. They cover the jitter bounds
and `compute_retry_delay` (`policy.rs`) and the seeded chaos plan
(`chaos.rs`). The script fails when Kani verifies fewer proofs than the
source holds, or when a cover check fails.

Refactors with no behavior change:

- `full_jitter` and `equal_jitter` call `full_jitter_nanos` and
  `equal_jitter_nanos`, so the proofs cover the arithmetic directly.
- `pick_seeded_action` uses a fixed array, not a `Vec`. A new unit test
  pins the action order for every index.

The lifecycle table gets an exhaustive unit test, `migration_states_are_one_way`,
not a Kani proof: its domain is 100 state pairs. Kani did not finish on the
`failure_signature` string code. Unit tests, proptests and its fuzz target
still cover it.

**Guard.** `formal_models_coverage` (no DB) checks the wiring:

- Every TLC spec and config has a manifest row.
- Every invariant that a config names exists in its spec.
- CI runs both scripts on every ready PR with code changes.
- The crate holds at least three Kani proofs.
- `docs/testing/formal-methods.md` names every model and proof.

No migration. No `WorkflowEvent` change. Model (d) and trace conformance
stay open; see `docs/testing/formal-methods.md`.
