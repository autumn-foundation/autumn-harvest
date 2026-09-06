## Phase — Build-scope the diagnose rate-limit bypass per eligible worker (issue #1190)

Follow-up from issue #809 (PR #1188). `GET /workflows/{id}/diagnose`
(`build_diagnosis_report` in `autumn-harvest-plugin/src/api.rs`) decides
whether a pending activity's rate-limit bucket is even a claim-time
impediment by checking whether the activity is circuit-breaker-tracked in
**this API replica's own in-process** `CircuitBreakerRegistry` — a
compile-time `#[activity(circuit_breaker = ...)]` fact, because a
breaker-tracked activity is gated at dispatch, not at claim (#369). During a
rolling deploy the worker that will actually claim the row can run a
**different build**, whose handler may declare the breaker differently:

- Locally tracked, peer not: the old code always skipped the bucket, so a
  peer genuinely gated by the rate limit at claim read as `healthy_in_progress`
  — a false negative.
- Locally untracked, peer tracked: the old code always consulted the bucket,
  so a peer whose real gate is the breaker at dispatch could read
  `activity_rate_limited` / `activity_rate_limit_bucket_missing` — a false
  positive, reachable on a `stalled` verdict, the worse direction for a
  triage endpoint.

**The fix generalizes the round-13 machinery from #1188.** The tracked-set,
like the capability-registry fallback `registry_fallback_binds` already
scopes to a shared build, is a compile-time declaration and therefore
identical across workers on the SAME build. Two new pure functions:

- `every_eligible_worker_is_on_the_local_build` — true only when every worker
  that could claim the row (`eligible_worker_ids`, from #1188) shares this
  replica's own advertised build (`local_build_id_from_workers`, also #1188).
  Vacuously false for no eligible workers.
- `rate_limit_gate_applies = !locally_tracked && every_eligible_worker_is_on_the_local_build(...)`.

The rate-limit bucket lookup is no longer filtered out of the key set by
`cb_tracked` up front (it is looked up for every pending task's key
regardless — a per-task question, resolved once eligibility is known); both
`rate_limit_saturated` and `rate_limit_bucket_missing` are gated on
`rate_limit_gate_applies` instead of the old bare `!has_cb`. When build
agreement cannot be established, **neither** verdict is reported — an
unknown-build worker is assumed capable, never assumed blocked, the same
direction-of-safety rule `local_circuit_snapshot_is_authoritative` already
applies to the breaker phase itself.

**A residual, accepted false negative.** `rate_limit_gate_applies` returns
`false` whenever the activity is tracked locally, independent of build
agreement — there is no cross-process breaker registry to consult (the same
constraint #1188 documented), so a build mismatch on that side of the
disagreement cannot be turned into a positive detection, only prevented from
masking behind an incorrect build check. This mirrors the accepted
fleet-wide-outage false negative in `local_circuit_snapshot_is_authoritative`,
and is called out explicitly in the doc comments and tests rather than left
implicit.

**Documented consequence:** on an API-only replica in a build-routed
deployment the local build resolves to `""`, matching no advertised build, so
both rate-limit verdicts are suppressed there entirely — the same degradation
the circuit breaker's own multi-replica caveat already accepts. Both the
`activity_circuit_open` and the new `activity_rate_limited` /
`activity_rate_limit_bucket_missing` rows in
`docs/runbooks/triage-pending-tasks-idle-workers.md` now say so explicitly,
and `docs/api-contract.json`'s `/diagnose` description carries the same
caveat.

**No new `WorkflowEvent` variant, no migration, no route or response-shape
change** — only the internal derivation of two existing boolean fields
changed; the wire shape is identical.

Tests, red → green → refactor: 4 new pure-function unit tests in
`autumn-harvest-plugin/src/api.rs` (`every_eligible_worker_is_on_the_local_build`'s
unanimity requirement, `rate_limit_gate_applies` for both disagreement
directions, and the single-build-fleet regression pin), plus 6 new
integration tests in
`autumn-harvest-plugin/tests/stall_diagnosis_integration.rs` driving the real
`/diagnose` endpoint end to end: a breaker-tracked activity with its sole
eligible worker off-build (wire behaviour pinned unchanged, per the accepted
false-negative above), an untracked activity with its sole eligible worker
off-build (drained and missing-bucket variants, both now suppressed —
the genuine regression coverage for this fix), a mixed-build eligible set
(one worker on-build, one off — unanimity, not majority, still suppresses
it), and two same-build-fleet controls (tracked activity still bypassed,
untracked activity still reports a real rate-limit block) alongside the
pre-existing #1188 round-11/12/13 and rate-limit tests, all of which
continue to pass unchanged.
