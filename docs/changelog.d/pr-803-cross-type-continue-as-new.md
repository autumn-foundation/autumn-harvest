## Phase 3.53 — Cross-type continue-as-new for multi-phase entity workflows (issue #803)

`ctx.continue_as_new(input)` resets history but always resurrects the run as the **same** workflow type (`worker.rs` hardwired the successor's `workflow_name` to the predecessor's). An entity that does genuinely different work across lifecycle phases (`trial_subscription` → `paid_subscription` → `churned`) therefore had to either monolith every phase into one ever-branching handler — where every phase's code is loaded, versioned and replayed by every other phase, and `ctx.patched()` gates never retire — or terminate-and-restart, which breaks the stable `workflow_id` that `signal_with_start` (#244) / `update_with_start` (#479) address the entity by.

**Two new `WorkflowContext` methods** continue the same logical entity as a *different registered type*: the typed `continue_as_new_as::<I>(&paid_subscription_info(), input)` (resolves the name from the target's companion `WorkflowInfo`, no magic string) and the untyped `continue_as_new_as_type("paid_subscription", json!(input))` (for a dynamically-chosen target). The existing `continue_as_new(input)` is **unchanged**.

**No new `WorkflowEvent` variant, no migration.** The transition rides the **existing** `WorkflowContinuedAsNew` variant via a single additive optional field `new_workflow_type: Option<String>` (`#[serde(default, skip_serializing_if = "Option::is_none")]`), so a same-type continuation serializes byte-identically to a pre-#803 event and every pre-existing stored history deserializes to `None` and replays identically. `WorkflowCommand::ContinueAsNew` and `WorkflowOutcome::ContinuedAsNew` gain the same field.

**"Presence decides" is the load-bearing design rule.** `None` (a plain `continue_as_new`) takes the byte-identical legacy path — every lifecycle column carried verbatim from the predecessor's row, including a *per-start override the type itself never declared*. Only `Some(target)` re-resolves defaults from the target's `WorkflowInfo`. Resolving unconditionally would have silently broken same-type continue-as-new by discarding per-start overrides, so the branch is gated on presence, never on `Some(name) == predecessor_name` normalization.

**Successor lifecycle defaults (AC4).** For a cross-type continuation `execution_timeout` (#243), `sla` (#487), the concurrency key/limit (#247, re-resolved against the *new* input), `owner`/`runbook_url`/`severity` (#372) and the workflow-level retry policy (#523) all come from the **target type's** `WorkflowInfo`; per-run deadlines re-anchor to the successor's start, and `sla` is clamped to `execution_timeout` mirroring the start path (`handle.rs`). A target declaring no default **clears** the column rather than inheriting the predecessor's — otherwise the successor would run under a timeout its own type never declared. The cross-type arm uses `checked_add_signed` for deadline anchoring (a fresh `WorkflowInfo` duration is unvalidated and `DateTime + Duration` panics on overflow) while the verbatim arm keeps plain `+` to stay byte-identical to the pre-#803 path — a deliberate, documented asymmetry.

**The #617 chain lifetime cap is carried VERBATIM even cross-type.** Re-resolving `chain_execution_timeout`/`chain_deadline_at` from the new type would make a type change an escape hatch from the runaway-loop budget, so the predecessor's absolute chain deadline is copied unchanged — the whole continue-as-new chain shares one lifetime cap regardless of how many types it passes through.

**Terminal rejections (AC5).** `classify_continue_as_new_target` is a pure classifier (`Rejected` / `SameType` / `CrossType(&WorkflowInfo)`) and `classify_successor_slot` is its pure companion for the successor's uniqueness slot. Four cases fail the predecessor terminally via the existing `WorkflowFailed` path, each with an operator message naming the type and the remedy, and in every case **no** successor row, event or task is created — never a silent no-op, never an undispatchable execution: a **blank** target; an **unregistered** target ("register the handler across the fleet before continuing into it"); a target naming a registered **unified DAG** (a DAG successor would run the level walker while bypassing the `max_active_runs`/paused-schedule gates `trigger_unified_dag` enforces, so the message points at `POST /dags/{name}/trigger`) — the core `HandlerRegistry` gained a `dag_workflow_names` set threaded from `BuiltHarvest::dags` filtered to `workflow_handler.is_some()`, populated at both `into_worker_parts` sites; and a target whose `(type, workflow_id)` slot is held by a **live** run (harvest admits exactly one active run per pair, and this path never displaces a bystander — recovery is to resolve that run, then restart or reset (#148) the entity). A **terminal** prior run of the target phase is likewise rejected (see the post-review note below: an earlier cut released it by re-stating it `CONTINUED_AS_NEW`, which two Codex P1s showed is unsafe). Naming the run's **own** current type is explicitly not an error: it is a supported request for that type's declared defaults, and the occupant check excludes the predecessor because its own seal frees the slot. The root-only guard (`reject_child_continue_as_new`, AC8) runs first and is unchanged, so a child cannot cross-type continue either.

**Replay determinism (AC6).** `HistoryMatcher::match_continue_as_new(input, new_workflow_type)` compares the recorded target type **before** the input, so a code change that redirects an already-recorded transition surfaces as `HarvestError::NonDeterministic` / `NonDeterminismKind::ContinueAsNewMismatch` rather than silently retargeting a live entity. Both directions are covered: cross-type→different-type and cross-type→same-type.

**Addressing consequence (documented loudly, in the method docs, CLAUDE.md and the example).** Harvest's active-run identity is the **pair** `(workflow_name, workflow_id)`, not `workflow_id` alone (Temporal differs — it keys on `workflowId`). After a transition an external caller must name the **current phase type**; naming the old type does not error, it silently starts a *separate* run of that old type — the transition released its uniqueness slot — which coexists with the live successor. Pinned in both directions by DB tests.

**Rollout ordering.** The target must be registered on the worker running the transition, so the new phase's handler must be deployed fleet-wide **first**.

**Documented interactions (not defects, but load-bearing).** A cross-type successor is invisible to its schedule's overlap controls — `schedule_id`/`scheduled_for` are carried, but `max_active_runs` counting and `OverlapPolicy::CancelOther`/`TerminateOther` all select on the *schedule's* `workflow_name`. The target type's **admission** policies (`throttle` #607, `debounce` #499, `batch` #518) and `max_input_bytes` (#252) are **not** consulted: continue-as-new is in-flight continuation rather than a start, so the payload cap enforced is the predecessor's. And `GET /admin/workflow-types/reachability` (#520) reports `safe_to_remove` from non-terminal executions *of that type* only, so it cannot see that a live run of a **different** type is about to continue into it — `docs/runbooks/safe-handler-removal.md` gained a caveat for the delete direction, mirroring the rollout-ordering rule for the deploy direction. Out of scope per the issue: cross-shard relocation, child-workflow and DAG continue-as-new, re-validating the successor input against `input_schema` (#373), typed-stub (#341) codegen, and any change to #617 or #772.

**Docs / example.** CLAUDE.md gains a "Cross-type continue-as-new — multi-phase entities" usage subsection (carry/re-resolve matrix, the presence-decides rule, the addressing consequence, rollout ordering). `autumn-harvest/examples/entity_phase_transition.rs` is a three-phase subscription entity under one `workflow_id` demonstrating the typed form, the untyped form, and a same-type monthly-billing loop side by side.

**Tests (TDD red→green→refactor; every RED confirmed as a real compile error or failing assertion before implementation).** `event.rs`: 3 serde tests (same-type omits the field, cross-type round-trips, pre-#803 JSON deserializes to `None`). `replay.rs`: 5 matcher tests incl. both divergence directions. `context.rs`: 6 tests over the new methods and the shared impl (the divergence test is `tokio::time::timeout`-bounded on purpose — if the type guard regresses the call resolves `Matched` and parks forever, so an unbounded assertion would turn a regression into a CI wall-clock timeout instead of a red test). `worker.rs`: 11 pure tests (`can803_*`) covering same-type-verbatim, cross-type-from-target, target-without-defaults-clears, the SLA clamp, the #243 ceiling, non-positive-SLA-dropped, overflow-does-not-panic, the #617 chain cap carried verbatim even when the target declares its own, the target-classification matrix, the DAG rejection, and the successor-slot matrix (free / self / terminal-occupant / live-occupant). `tests/integration/replayer_tests.rs`: a "Cross-type continue-as-new" section whose success-metric sweep runs 100 iterations that **vary** the recorded target across three types and pair each positive with its two negatives — "identical successor type" is asserted by *exclusion* (exactly one of the three handlers replays clean; both others are rejected as non-determinism), because the resolved type is not readable off the report. Plus redirect-is-non-determinism, revert-to-same-type-is-non-determinism, JSON round-trip and a pre-#803 back-compat replay. `tests/integration/cross_type_continue_as_new_tests.rs`: 14 worker-driven DB tests (identity/shard/queue/**input** preservation, defaults-from-the-new-type with the #617 chain cap pinned verbatim, defaults-cleared, unregistered→terminal-with-no-successor, a terminal occupant sealed so the successor takes its slot, a live occupant blocking terminally with the bystander untouched, naming-your-own-type continuing normally, full AC7 carryover (`last_completion_result`, schedule lineage, memo, search attributes, context headers, completion callbacks, `assigned_build_id`, run-chain back-links), a mid-transition signal reassigning via the row-lock choreography, `signal_with_start` attaching to the live successor, the old-type-starts-a-separate-run consequence, concurrency re-resolution, the child guard, and same-type-unchanged). `examples/entity_phase_transition.rs`: 4 embedded `WorkflowTestEnv` tests, wired into CI with their own step (a `[[example]]` defaults to `test = false`, so without it they would never execute). Mutation-tested: dropping the SLA clamp, sourcing cross-type `owner` from the predecessor, accepting a blank target, disabling the matcher's type comparison, and forcing the successor slot always-`Free` were each applied and each caught — the last two were re-confirmed after the tests were strengthened, since the original success-metric loop survived the matcher mutant.

**Post-review hardening (Codex, PR #1159).** Two fixes.

**P1 — retry-ceiling bypass.** The cross-type arm resolved the target's
`retry_policy` from its `WorkflowInfo` and serialized it into the successor row
unchanged, bypassing the operator's fleet-wide `max_workflow_attempts_ceiling`
(#523). Authoritative rather than cosmetic: the retry consumer gates on
`attempt >= policy.max_attempts` read straight off the stored row and never
re-clamps. Now clamped at the write site, mirroring the two existing sites that
resolve a policy from a `WorkflowInfo` and so bypass `StartWorkflowParams` (the
normal start path in `execution.rs`, and the detached-child spawn). The
**same-type** arm is deliberately NOT clamped — it carries the predecessor's
already-clamped stored value, and re-clamping would silently shrink an
in-flight chain's budget if an operator lowered the ceiling mid-flight.

**P1 — cross-shard misrouting.** Rendezvous routing hashes the
`(workflow_name, workflow_id)` **pair**, so a type change re-routes the key —
measured at 143/200 ids (~75%) on a 4-shard router. The successor is
nonetheless inserted on the predecessor's shard (the seal and the insert are
one transaction; there is no cross-shard transaction to relocate it). Left
unguarded that silently (a) made the successor unreachable by
`workflow_id`-addressed signal/cancel/await (#751), which resolves its target by
hashing the new pair — precisely the addressing this feature exists to preserve —
and (b) hid a live run of the target type on the routed shard from
`resolve_successor_slot`, admitting two live runs under one key. Such a
transition is now **rejected terminally**, naming both shards, matching the
fail-closed posture already taken for blank / unregistered / DAG /
occupied-slot targets. Single-shard deployments are unaffected; `HarvestPlugin`
rejects multi-shard upstream, so the restriction binds only standalone-runner
embedders. Cross-type continue-as-new is therefore single-shard-only for now;
relocation or a routing directory would lift it.

Tests: `can803_cross_type_applies_the_workflow_attempts_ceiling`,
`can803_same_type_retry_policy_is_not_reclamped_by_the_ceiling`,
`can803_cross_shard_guard_matrix`, and
`can803_cross_type_key_routes_to_a_different_shard_for_most_ids` (pins the
pair-hash premise so the guard cannot silently become dead code). Both fixes
are mutation-verified.

**Codex P2 (PR #1159) — reject unrepresentable successor deadlines.** The cross-type
arm resolved the target's declared `execution_timeout` with `checked_add_signed`, so an
out-of-range duration stored `execution_timeout = Some(..)` alongside `deadline_at =
NULL`. `enforce_workflow_execution_timeouts` selects `deadline_at IS NOT NULL AND
deadline_at < now`, so that successor would have **claimed a hard runaway cap it did not
have**. Reachable: `task_duration` accepts e.g. `"1000000000d"`, which converts via
`chrono::Duration::from_std` but overflows `DateTime<Utc>`. This path is the first to
resolve the *target* type's declaration — the normal start path uses a plain `+`
(`execution.rs:592`), which panics rather than persisting such a run, so the type could
never have been started directly. New pure `classify_successor_deadline_representable`
now **rejects the transition** (terminal predecessor failure naming the type, the field,
and the three remedies), judging the *effective* (post-ceiling) timeout so a configured
`max_workflow_execution_timeout` rescues an otherwise-unrepresentable declaration. An
out-of-range **`sla`** is treated differently — it maps to "no SLA" (both fields
cleared), matching the documented #487 start-path rule: an observational counter may
degrade, a runaway cap may not. Mutation-verified. Tests:
`can803_unrepresentable_execution_timeout_is_rejected`,
`can803_ceiling_rescues_an_unrepresentable_declaration`,
`can803_unrepresentable_sla_maps_to_no_sla_not_a_lying_field`.
