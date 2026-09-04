## Phase — Distinguish a timed circuit-open from a forced one in stall diagnosis (issue #1193)

Follow-up from a Codex P2 on PR #1188 (issue #809, `GET /workflows/{id}/diagnose`).
`BlockedOn::health()` reported **every** non-half-open circuit-breaker verdict
as `ExecutionHealth::Stalled` ("needs a human: nothing will move this run
forward on its own"). That is correct for an operator-forced open, but an
organically-tripped open recovers on a cooldown timer with no human
involvement — `CircuitBreakerRegistry::on_dispatch` admits a probe as soon as
the cooldown elapses, and the two shapes are already distinguishable on the
verdict's own `cooldown_until` field (`Some` ⟺ timed, `None` ⟺ forced).

**Design decision (recorded in the `ExecutionHealth::Degraded` doc comment).**
Of the three options the issue raised, reporting the timed case `Healthy` was
rejected outright: dispatch of the blocked activity fast-fails
**non-retryably** on every attempt until the cooldown elapses (issue #369's
`ActivityFailure::circuit_open`), so the run is heading for a terminal
`ActivityFailed`, not progress — an operator must never read "healthy" about
that. Keeping `Stalled` for both shapes (lowest-risk, no label churn) was
rejected too: it would still tell an operator a genuinely self-healing
condition "needs a human", collapsing an actionable page with a non-actionable
one. Instead this adds a third value, `ExecutionHealth::Degraded` — "will move
forward without a human, but not toward success" — reserved for exactly this
case. The blast radius is contained to `stall_diagnosis.rs` and its two
consuming docs: `ExecutionHealth` is otherwise only ever passed through as
opaque data (the plugin's `api.rs` never matches on it; the CLI renders the
raw wire string), never exhaustively matched elsewhere in the workspace.

**What changed.** `BlockedOn::health()` now splits the `Open` phase: a
`cooldown_until: Some(..)` (organic) reports `Degraded`; a `cooldown_until:
None` (operator-forced) keeps reporting `Stalled` and still names
`force-close` as the remedy; a `half_open` breaker is unchanged (`Healthy`).
The organic-open summary text now states the automatic-recovery deadline and
explicitly names the non-retryable fast-fail interaction, so `degraded` is
never mistaken for a clean wait. `docs/runbooks/triage-pending-tasks-idle-workers.md`
and `docs/api-contract.json` both spell out the three-way split.

No new `WorkflowEvent` variant, no migration, no change to
`CircuitBreakerRegistry` — read-path label semantics only, exactly as scoped.

Tests: `stall_diagnosis.rs` pins the timed/forced/half-open split in one place
(`circuit_health_pins_the_timed_vs_forced_split_across_all_three_phases`),
flips the two pre-existing assertions that had encoded the old blanket
`Stalled` behavior for a timed open, adds wire-string coverage for
`ExecutionHealth::Degraded`, and pins the summary text (deadline stated,
fast-fail named, no `force-close` for the organic case; `force-close` kept for
the forced case). The end-to-end `ac5_organically_tripped_circuit_reports_a_derived_cooldown_until`
integration test now also asserts `health: "degraded"` against a real breaker
registry and database.
