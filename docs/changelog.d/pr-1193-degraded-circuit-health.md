## Phase — Distinguish a timed circuit-open from a forced one in stall diagnosis (issue #1193)

Follow-up from a Codex P2 on PR #1188 (issue #809, `GET /workflows/{id}/diagnose`).
`BlockedOn::health()` reported **every** non-half-open circuit-breaker verdict
as `ExecutionHealth::Stalled` ("needs a human: nothing will move this run
forward on its own"). That is correct for an operator-forced open, but an
organically-tripped open recovers on a cooldown timer with no human
involvement — `CircuitBreakerRegistry::on_dispatch` admits a probe as soon as
the cooldown elapses.

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

**What changed.** `BlockedOn::health()` now splits the `Open` phase: an
organic trip reports `Degraded`; an operator-forced open keeps reporting
`Stalled` and still names `force-close` as the remedy; a `half_open` breaker
is unchanged (`Healthy`). The organic-open summary text now states the
automatic-recovery deadline and explicitly names the non-retryable fast-fail
interaction, so `degraded` is never mistaken for a clean wait.
`docs/runbooks/triage-pending-tasks-idle-workers.md` and `docs/api-contract.json`
both spell out the three-way split.

**Codex round-1 findings on PR #1365, both confirmed and fixed.** The initial
implementation discriminated forced-vs-organic purely on whether
`cooldown_until` was present, which two reviews found unsound:

- **P2 — the discriminator was inferred, not authoritative.**
  `CircuitBreakerPolicy::new` accepts any `std::time::Duration` for its
  cooldown, and an organically-tripped breaker configured with one outside
  `chrono`'s representable range ALSO reports no `cooldown_until` (see
  `circuit_cooldown_until` in `api.rs`) — indistinguishable, by that field
  alone, from a genuinely forced-open breaker. Fixed by adding an
  authoritative `forced_open: bool` to both `PendingActivityFacts` and the
  `ActivityCircuitOpen` verdict itself, sourced directly from
  `CircuitSnapshot::forced_open` (which already existed in `circuit_breaker.rs`
  but was previously discarded at the `api.rs` mapping site) rather than
  inferred from the display deadline's computability. `health()`,
  `summarize()`, and the precedence functions below were all re-keyed on this
  field instead of `cooldown_until.is_none()`.
- **P1 — the tie in `activity_precedence` became consequential.** Both a
  forced-open and an organically-tripped breaker shared precedence 5. Before
  this PR that was harmless because both mapped to the same health
  (`Stalled`); once they diverge (`Stalled` vs `Degraded`), the
  keep-the-first-max fold in `classify_execution` could silently pick an
  organic-open row over a genuinely-stuck forced-open row elsewhere in the
  same fan-out, purely by which happened to come first in the list — masking
  the actionable stall behind a self-healing one. Fixed by giving a
  forced-open breaker its own precedence tier (6) strictly above the
  organic/half-open tier (5, unchanged), with every rank above it shifted up
  by one; `activity_precedence` and its allocation-free mirror
  `activity_precedence_for_facts` were both updated and re-verified against
  each other by the existing drift-detection property test.

No new `WorkflowEvent` variant, no migration, no change to
`CircuitBreakerRegistry`'s own logic (only what the plugin reads off its
existing `CircuitSnapshot`) — read-path label semantics only, exactly as
scoped.

Tests: `stall_diagnosis.rs` pins the timed/forced/half-open split in one place
(`circuit_health_pins_the_timed_vs_forced_split_across_all_three_phases`,
now including the organic-with-unrepresentable-cooldown case), adds
`organic_open_with_unrepresentable_cooldown_is_still_degraded` and
`half_open_forced_open_fact_is_ignored_for_half_open_phase` as direct P2
regression pins, adds `forced_open_circuit_outranks_an_organic_one_in_the_same_fan_out`
(asserted in both row orders) as the direct P1 regression pin, adds wire-string
coverage for `ExecutionHealth::Degraded`, and pins the summary text (deadline
stated, fast-fail named, no `force-close` for the organic case — including the
unrepresentable-cooldown shape — `force-close` kept for the forced case). The
end-to-end `ac5_organically_tripped_circuit_reports_a_derived_cooldown_until`
and `ac5_forced_open_circuit_reports_circuit_open_without_a_cooldown`
integration tests now also assert the wire-level `forced_open` field against a
real breaker registry and database.
