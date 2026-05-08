## Problem (the "So What?")

When a `WorkflowSchedule` or DAG schedule fires and the previous run from the *same schedule* is still in flight, harvest's only behavior today is **skip**: once `running_count >= max_active_runs`, the scheduler increments `record_schedule_skipped(..., "max_active_runs_reached")` and moves on (`autumn-harvest/src/scheduler.rs:622` for DAG, `:829` for workflow schedules). That is one of six legitimate operator answers — and harvest hard-codes it without a knob.

Operators hit this within hours of running real schedules:

- **Daily billing report** runs long because of a slow downstream API. The 3am firing is mid-flight when the next day's 3am firing comes around. Today: that next firing silently drops, the operator gets a metric increment, and Tuesday's report never runs. They want **BufferOne** (queue exactly one and run it as soon as the current one finishes) so the schedule "catches up by exactly one slot."
- **Hourly sync workflow** wedges on a poison input. The operator wants **CancelOther** (cancel the wedged run, start the new one cleanly) — today they have to manually terminate the wedged execution before the schedule's next firing has any effect.
- **Backfill / replay tooling** that emits a burst of fire-times wants **BufferAll** to enqueue every missed slot. #177 (bounded backfills) is going to need this lever to be honest about what it actually does.
- **Long-running data pipeline** wants concurrent runs up to `max_active_runs > 1` (today's behavior) — but with the policy named explicitly, not as a side effect of a numeric cap.

The result of "skip is the only policy" is paged operators terminating workflows by hand and writing internal docs that say "don't use harvest schedules for anything that might run long." That's the wrong shape for a durable workflow engine.

### Verification done before filing this spec

- `autumn-harvest/src/policy.rs:280` defines `WorkflowSchedule` with `max_active_runs: u32`. No `overlap_policy` field. No `OverlapPolicy` enum anywhere in the workspace.
- `grep -ri "OverlapPolicy\|BufferOne\|CancelOther\|TerminateOther" autumn-harvest*/` returns zero hits.
- Scheduler decision sites that hard-code skip-on-collision: `autumn-harvest/src/scheduler.rs:622-639` (DAG path) and `autumn-harvest/src/scheduler.rs:829-836` (workflow-schedule path). Both call `record_schedule_skipped` with a fixed reason; neither has a branching policy lookup.
- `gh issue list --search "OverlapPolicy"` and `--search "buffer one cancel other"` both return zero results across open and closed issues.
- Adjacent shipped or in-flight schedule work: #91 (per-workflow cron, closed), #177 (bounded backfills, open), #229 (pause/resume, open), #240 (jitter, open). None of them spec what happens on a collision; jitter (#240) explicitly defers "per-tenant fairness / priority" but does not address overlap either. Issue #87 (`WorkflowIdReusePolicy`) is about duplicate *starts* against the same workflow id from outside callers — distinct from schedule firings colliding with their own previous run.
- `harvest_schedules` already has `max_active_runs INT NOT NULL DEFAULT 1` (`migrations/20260409000000_harvest_initial/up.sql:124`); a sibling `overlap_policy TEXT NOT NULL DEFAULT 'skip'` column is the obvious storage shape and preserves today's behavior without breaking existing rows.

## User Story

As an operator running cron-scheduled workflows, I want to declare what happens when a firing collides with a still-running previous run from the same schedule, so that long-running workflows, wedged executions, and backfills behave predictably without me having to write babysitter scripts on top of harvest.

## Acceptance Criteria

- [ ] **`OverlapPolicy` enum** is added to `autumn-harvest/src/policy.rs` with five variants:
  - `Skip` (today's behavior; default)
  - `BufferOne` — queue at most one pending firing; new firings while one is already buffered are dropped (and counted via the existing `schedule_skipped` metric with a new `reason = "buffered_slot_full"`)
  - `BufferAll` — queue every missed firing
  - `CancelOther` — cancel the in-flight run, then start the new one when cancellation is observed
  - `TerminateOther` — terminate the in-flight run immediately, then start the new one
- [ ] **`WorkflowSchedule::with_overlap_policy(OverlapPolicy)` builder + public `overlap_policy: OverlapPolicy` field.** Default `OverlapPolicy::Skip`, which preserves today's behavior bit-for-bit.
- [ ] **DAG schedules accept the same knob** with the same defaults and semantics (whatever the registration surface for `harvest_schedules` is on the DAG side).
- [ ] **`harvest_schedules` storage.** New column `overlap_policy TEXT NOT NULL DEFAULT 'skip'`. Existing rows read as `Skip` without a backfill. Engineering picks the exact column type and string spelling; the spec only requires the default preserves behavior and the column survives `diesel migration redo` cleanly.
- [ ] **Interaction with `max_active_runs`.** `max_active_runs` continues to mean "hard cap on concurrently running executions for this schedule." `OverlapPolicy` decides what happens when a *new firing* would push the count above the cap. `BufferOne` / `BufferAll` still respect `max_active_runs` when draining (a buffered slot dispatches only when a running slot frees up). Concurrent-runs behavior is `max_active_runs > 1` + `Skip` — we do **not** add a separate `AllowAll` variant; that's the existing knob composing correctly.
- [ ] **Cancellation / termination semantics** route through the same primitives #238 (saga + cancellation interaction) is defining. If #238 lands first, this spec inherits its cancellation/termination contract verbatim. If this lands first, `CancelOther` and `TerminateOther` are gated behind a feature flag or builder-side validation that returns a registration error until #238's contract is in place. Engineering picks the sequencing; the spec only requires we do not ship `CancelOther` / `TerminateOther` *without* a defined cancellation contract underneath them, because silent-failure-on-cancel is worse than no policy at all.
- [ ] **Buffered-slot durability.** Buffered firings under `BufferOne` / `BufferAll` survive scheduler restarts and leader handoffs. They are stored on the schedule row (or an adjacent table — engineering's call), not in scheduler memory. A scheduler crash mid-buffer must not silently drop pending firings.
- [ ] **Buffered-slot bound for `BufferAll`.** `BufferAll` accepts an optional cap (`u32`, with a safe default — engineering picks; Temporal uses a finite bound). Past the cap, additional firings drop and are recorded under `schedule_skipped` with `reason = "buffer_full"`. We are not building an unbounded queue; that's a memory-leak vector under a wedged downstream.
- [ ] **Observable in the management API.** `GET /schedules` (whatever the existing schedule listing endpoint is) returns the configured `overlap_policy` and, for `BufferOne` / `BufferAll`, the current buffered-slot count.
- [ ] **Metrics.** `record_schedule_skipped` gains the new reason strings (`"buffered_slot_full"`, `"buffer_full"`). The existing `"max_active_runs_reached"` reason is retained for back-compat and continues to be emitted for `Skip` behavior.
- [ ] **Zero event-schema impact for `Skip` / `BufferOne` / `BufferAll`.** No new `WorkflowEvent` variants. `CancelOther` and `TerminateOther` reuse whatever cancellation events #238 standardizes; they do not introduce schedule-specific event variants.
- [ ] **Replay-determinism preserved.** Workflows themselves don't observe the policy — it's a *scheduler-side* decision about whether to fire at all and which firing wins. No `WorkflowContext` API change, no replay contract change.
- [ ] **Pause/resume interaction (#229).** Pausing freezes both "firings to evaluate" and "buffered slots"; resume restores both. The buffered-slot count is preserved across pause/resume.
- [ ] **Backfill interaction (#177).** Backfill catchup respects the overlap policy. `BufferAll` is the natural pairing with catchup; `Skip` + catchup means "replay only the slots where nothing was running," which is what catchup already implies today.
- [ ] **Documentation.** Rustdoc on `OverlapPolicy` includes a decision matrix (one row per variant: when to use, what happens to the in-flight run, what happens to subsequent firings, durability guarantees). `CLAUDE.md` Phase 4 status reflects the addition.
- [ ] **Tests.** Integration tests covering each policy variant under a slow workflow handler that intentionally overlaps two firings, plus a scheduler-restart test for `BufferOne` / `BufferAll` durability. Pattern: extend `tests/integration_e2e.rs:workflow_schedule_max_active_runs_enforced` (which already exercises a 30s slow handler against `max_active_runs = 1`) to drive each policy.

## Success Metric

- **Operator MTTR on overlap incidents.** In a synthetic incident harness (slow handler + tight cron), the time-from-symptom-to-corrective-action drops measurably: today the operator must terminate the wedged run by hand, which takes minutes; under `CancelOther` / `TerminateOther`, the next firing self-heals on the next scheduler tick (target: < 30 s end-to-end). Engineering picks the harness; the PR description reports the before/after numbers.
- **Adoption signal.** At least one downstream embedder (vantage, kinetics, or similar) migrates a real schedule to a non-`Skip` policy within 60 days of merge. Counted via `gh code-search` for `with_overlap_policy(` across known consumer repos.
- **Zero replay regressions.** `cargo test -p autumn-harvest --features db` and the replayer harness suite stay green; no test in `replay_tests.rs` or `replayer_tests.rs` regresses.

## Out of Scope

- **Cross-schedule fairness / priority.** This spec is per-schedule overlap behavior only. Multi-tenant priority queues are a separate (much larger) discussion.
- **Per-firing `OverlapPolicy` overrides.** The policy is a property of the schedule, not of an individual firing. Ad-hoc one-off schedule runs use the schedule's configured policy; there's no per-call override.
- **Workflow-author visibility into overlap policy.** Workflows do not observe whether they are the "buffered" run or the "live" run. That distinction lives entirely in the scheduler; the workflow function signature and `WorkflowContext` are unchanged.
- **`AllowAll` as a separate variant.** `max_active_runs > 1` + `Skip` is already that behavior; adding a separate variant just creates two ways to express the same thing.
- **Policy migration tooling.** Operators change a schedule's policy by re-registering it (whatever the existing schedule re-registration path is). We are not adding a `PUT /schedules/{id}/overlap_policy` admin endpoint in this slice; that can be a follow-up under #175 (management API contract).
- **Auto-tuning the policy** based on observed run-time / overlap rate. Static-policy form is the well-understood shape.

## Gap Analysis

- **Temporal**: `ScheduleOverlapPolicy` is a first-class enum with `Skip`, `BufferOne`, `BufferAll`, `CancelOther`, `TerminateOther`, `AllowAll`. Documented and load-bearing for production schedules. This spec adopts the well-trodden shape minus `AllowAll` (we get that for free via `max_active_runs > 1` + `Skip`).
- **Cadence**: same lineage as Temporal; same enum.
- **DBOS**: `@DBOS.scheduled` with `OverlappingMode` enum (`Skip`, `Allow`); narrower than Temporal but the *concept* is first-class. Harvest doing better than DBOS here is a real DX wedge.
- **Hatchet / Inngest / Trigger.dev**: cron schedules with implicit "skip if running" or "allow concurrent." None expose `BufferOne` / `CancelOther` / `TerminateOther` as first-class knobs; users hand-roll deduplication inside workflow code or via external locks. Closing this gap is a competitive nudge.
- **Restate**: no scheduled-run primitive in the same shape; comparison doesn't apply directly.
- **Oban**: unique-job constraints (`Oban.Pro.Worker`) cover "don't enqueue if one is running" — a `Skip`-shape policy. No `BufferOne` / `CancelOther` semantics. Same gap.
- **Sidekiq / sidekiq-cron / Resque / Celery / apalis / sqlxmq**: cron expressions with implicit "fire and forget"; deduplication is the user's problem. Same gap.
- **Airflow**: `DAG.max_active_runs` + `catchup` together approximate `Skip` and `BufferAll`; no explicit `CancelOther` / `TerminateOther`. Operators write external scripts to terminate stuck DAG runs. Same gap.
- **Harvest-shaped answer**: a typed `OverlapPolicy` enum that composes cleanly with the existing `max_active_runs` numeric cap, the catchup mechanics #177 is defining, and the cancellation contract #238 is defining. Picks Temporal's well-trodden shape, lands `Skip`/`BufferOne`/`BufferAll` immediately (no event-schema impact), and gates `CancelOther`/`TerminateOther` behind #238 so we never ship a cancellation policy without a cancellation contract underneath it. Postgres-only storage makes the buffered-slot durability requirement trivial (one row, one column, zero distributed-locking concerns).

## Complexity Tier

**M** — new `OverlapPolicy` enum + builder + storage column on `harvest_schedules` (one defaulted migration), branching at two scheduler decision sites (`scheduler.rs:622` DAG, `:829` workflow), buffered-slot persistence for `BufferOne` / `BufferAll`, two new metric reason strings, integration tests under a slow-handler harness, and rustdoc + `CLAUDE.md` updates. **Zero new `WorkflowEvent` variants. Zero replay-contract impact. Zero shard-encoding impact.** The load-bearing risk is the cancellation/termination contract — sequencing with #238 is the explicit gate. Single migration, single new public type, no public API breakage (all additions are opt-in via a defaulted field). Touches `autumn-harvest` core only; `autumn-harvest-plugin` gains a one-line surface to expose the policy in `GET /schedules`.

---

*Phase 4 operability. Implementation should target `trunk-dev`, never `trunk` directly.*
