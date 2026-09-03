## Phase — Reject an unrepresentable workflow duration at `try_build()` (issue #1163)

Closes a silent-data-loss gap in `#[workflow(execution_timeout = "…")]` (and the
sibling `chain_execution_timeout` (#617) and `sla` (#487) fields): every start
path resolved a declared `std::time::Duration` via
`chrono::Duration::from_std(d).ok()`. Above `chrono::Duration`'s representable
ceiling (roughly `i64::MAX` milliseconds, ~292 million years) that conversion
returns `Err`, and `.ok()` turned it into `None` — indistinguishable from "no
timeout declared". The declared hard runaway cap (or chain cap, or SLA budget)
simply vanished, with no error and no log line, on every start path:
`worker.rs` (child spawn, chain cap, cross-type continue-as-new, #803),
`autumn-harvest-plugin/src/api.rs` (HTTP start, signal-with-start,
update-with-start, batch, manual trigger).

**Reachable through the macro.** `task_duration` accepts up to 20 digits with
checked `u64` arithmetic, so `#[workflow(execution_timeout =
"999999999999d")]` parses fine (`999999999999 * 86400` seconds is comfortably
under `u64::MAX`) while landing far past chrono's ceiling.

**Why not fix each call site, or clamp instead of rejecting.** Both
alternatives were raised and rejected on the parent issue (#803 / PR #1159).
Rejecting only on the cross-type continue-as-new path (as #1159 did for the
*different*, already-solved `DateTime` add-overflow tier) would make that path
reject a declaration every other start path accepts — a type startable
directly but not continuable-into. Clamping to the configured
`max_workflow_execution_timeout` ceiling before conversion silently
reinterprets the author's declaration, and does nothing when no ceiling is
configured, which is the case this bug is actually about.

**Fix.** New `validate_workflow_duration_fields` in
`autumn-harvest/src/builder.rs`, called once from `HarvestBuilder::try_build()`
alongside the existing `validate_workflow_concurrency_limits` /
`validate_workflow_throttle_policies` family. For every registered
`WorkflowInfo`, each of `execution_timeout`, `chain_execution_timeout`, and
`sla` that is `Some(d)` with `chrono::Duration::from_std(d)` erroring now fails
the build with `HarvestBuilderError::UnrepresentableWorkflowDuration`, naming
the workflow, the field, the declared value, and the representable ceiling
(`chrono::Duration::MAX.to_std()`). Runs against `self.workflows` *after*
`HarvestBuilder::dags()` has pushed the #743 DAG shadow `WorkflowInfo`
(`DagInfo::as_workflow_info()`) into it, so a DAG's own
`#[dag(execution_timeout = "…")]` gets the identical guarantee as a
`#[workflow]`-declared one — every start path benefits uniformly rather than
each silently accepting a declaration it can never honor.

**Behavior change.** An application that had (nonsensically) declared an
unconvertible `execution_timeout`/`chain_execution_timeout`/`sla` now fails
`try_build()` at startup instead of silently starting with the field dropped.
That is the point of this fix, not an accident.

**Scope.** No new `WorkflowEvent` variant, no migration — pure startup
validation, exactly as scoped in the issue. Does not touch the separate,
already-solved #803/#1159 `DateTime<Utc>` add-overflow tier
(`classify_successor_deadline_representable` in `worker.rs`, `can803_*`
tests), which guards a materially smaller range (`DateTime<Utc>`'s own bounds)
than `chrono::Duration`'s own ceiling this fix guards.

**Tests.** New tests in `builder::tests` (`autumn-harvest/src/builder.rs`):
per-field rejection at exactly one nanosecond past the ceiling for
`execution_timeout`, `chain_execution_timeout`, and `sla`; acceptance of
`None` and of the exact ceiling value; the issue's own
`999999999999d`-equivalent repro value; full `try_build()` wiring naming the
workflow/field/ceiling in the error message; and a DAG-shadow-`WorkflowInfo`
case proving the #743 propagation path is covered too.
