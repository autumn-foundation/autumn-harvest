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
  forced-open breaker its own precedence tier strictly above the
  organic/half-open tier, with every rank above it shifted up accordingly;
  `activity_precedence` and its allocation-free mirror
  `activity_precedence_for_facts` were both updated and re-verified against
  each other by the existing drift-detection property test.

**Codex round-2 finding on PR #1365, also confirmed and fixed.** The round-1
fix isolated forced-open but left organic-open and half-open sharing a tier —
the identical failure mode one level down:

- **P1 — organic-open and half-open still tied, and now differently
  healthed.** Both used to be considered together at the shared precedence
  tier; once `Degraded` (organic) diverged from `Healthy` (half-open), a
  half-open row ordered first in a fan-out could win the fold and report the
  whole execution `healthy` while a *different* activity's organically
  tripped breaker was heading toward a terminal, non-retryable failure —
  exactly the false-`healthy` scenario the whole feature exists to prevent.
  Fixed by giving organic-open its own tier strictly between forced-open and
  half-open (forced > organic > half-open), renumbering the ladder once more.

**Codex round-3 finding on PR #1365, also confirmed and fixed.** A different
class of masking, one level up the ladder:

- **P1 — a `degraded` activity verdict unconditionally suppressed a frozen
  workflow task.** `classify_execution`'s activity bucket returns
  unconditionally whenever any activity row exists, structurally never
  reaching `workflow_task_hard_impediment` (the run's own workflow-task-level
  `workflow_no_worker` / `workflow_queue_paused` checks) below it. That
  ordering is deliberate and pre-existing (pinned since before this issue by
  `wedged_activity_still_outranks_a_workflow_queue_impediment`) for every
  activity health it was ever exercised against, because those all happened
  to be at least as severe as anything the workflow task alone could report.
  `Degraded` breaks that assumption: it is milder than the `Stalled` a frozen
  workflow task (`workflow_no_worker`) represents, so an activity's
  organically-tripped breaker could report `degraded` — "no human needed" —
  while the execution's own decision cycle could never advance at all, unable
  to process even the completions of activities that DO finish. Fixed by
  checking `workflow_task_hard_impediment` whenever (and ONLY whenever) the
  winning activity verdict resolves to `Degraded`, preferring it when present.
  Every other activity health (`Stalled`, `BlockedExternal`, `Healthy`) is
  untouched — this does not touch the broader, unrelated question of whether
  a `Healthy` activity verdict should ever yield to a workflow-level stall
  (out of scope for this issue).

**Codex round-4 finding on PR #1365, also confirmed and fixed.** A timing
nuance in the same organic-open branch:

- **P2 — a not-yet-due row can be `degraded` when it will never fast-fail.**
  The circuit-open check outranks the not-due-yet check unconditionally
  (pre-existing, correct for `no_worker`/`rate_limit_bucket_missing`/forced-open,
  all permanent-until-manual-action). For an organic trip specifically, that
  is only right if this row would actually be *attempted* while the breaker
  is still open. `CircuitBreakerRegistry::on_dispatch` checks the elapsed
  cooldown at the actual dispatch instant, so a row whose own
  `scheduled_at` lands at or after `cooldown_until` is what the breaker
  would admit as its recovery probe (or dispatch normally once closed) —
  it would never fast-fail, so `degraded`'s "heading for a terminal
  failure" framing overclaims. Fixed by comparing the two timestamps and
  falling through to the ordinary `activity_retrying` / `activity_deferred`
  verdict when the cooldown will have already cleared by the time the row
  is due. Scoped to organic trips with a known `cooldown_until` only: a
  forced-open breaker has no cooldown to compare against (unconditionally
  `Stalled`, correctly, since only `force-close` clears it) and an organic
  trip with an unrepresentable cooldown has no deadline either, so both are
  unaffected. The allocation-free precedence mirror
  (`activity_precedence_for_facts`) was updated identically, and the
  property-test generator that guards the two functions from drifting apart
  was extended to actually vary `circuit_cooldown_until` (it previously
  hardcoded `None`, so it could never have caught a bug in this exact
  branch).

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
and `organic_circuit_outranks_a_half_open_one_in_the_same_fan_out` (each
asserted in both row orders) as the direct P1 regression pins for both
rounds, extends `activity_precedence_ladder_is_strictly_ordered` to cover all
three circuit shapes explicitly, adds wire-string coverage for
`ExecutionHealth::Degraded`, and pins the summary text (deadline stated,
fast-fail named, no `force-close` for the organic case — including the
unrepresentable-cooldown shape — `force-close` kept for the forced case). The
end-to-end `ac5_organically_tripped_circuit_reports_a_derived_cooldown_until`,
`ac5_forced_open_circuit_reports_circuit_open_without_a_cooldown`, and
`half_open_circuit_is_not_reported_as_operator_forced` integration tests now
also assert the wire-level `forced_open` field against a real breaker
registry and database. The round-3 fix is pinned by
`degraded_organic_circuit_yields_to_a_frozen_workflow_task_no_worker`,
`degraded_organic_circuit_yields_to_a_paused_workflow_queue`, and
`forced_open_circuit_still_outranks_a_frozen_workflow_task` (proving the
override is scoped to `Degraded` alone and does not touch the pre-existing
`Stalled` behavior). The round-4 fix is pinned by
`organic_open_falls_through_to_retrying_when_cooldown_clears_before_due`
(both with and without failure evidence),
`organic_cooldown_clearing_exactly_at_the_due_instant_still_falls_through`
(the `>=` boundary), `organic_open_still_wins_when_cooldown_has_not_cleared_by_due_time`
(the converse — still `degraded` when the fast-fail is real), and
`forced_open_circuit_wins_regardless_of_how_far_in_the_future_the_task_is`
(proving forced-open is untouched).
