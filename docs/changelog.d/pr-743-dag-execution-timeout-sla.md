## Phase 3.x — DAG-level `execution_timeout`/`sla` attributes (issue #743)

**Implemented.** A `#[workflow]` has had a hard `execution_timeout` deadline
(issue #243, Phase 3.12) and a soft `sla` breach signal (issue #487, Phase
3.28) since long before unified DAG execution existed. A `#[dag]` had
neither: a scheduled nightly ETL DAG that hung on a stuck upstream had no
engine-level ceiling at all, and there was no way to alert an operator when a
DAG run was merely running slower than usual. This closes that gap with
**pure propagation, zero new machinery** — since Harvest 0.3
(`unified-dag-execution`, on by default) a `#[dag]` already executes as a
`#[workflow]` under the hood via a shadow `WorkflowInfo` (issue #256, Phase
3.9), so `execution_timeout`/`sla` declared on `#[dag]` are copied verbatim
onto that shadow `WorkflowInfo` and enforced by the **exact same scanners** a
plain workflow already uses: `timeout::enforce_workflow_execution_timeouts`
(#243) and `timeout::enforce_workflow_sla_breaches` (#487). **No new
`WorkflowEvent` variant, no new scanner, no migration.**

```rust
#[dag(schedule = "0 6 * * *", execution_timeout = "4h", sla = "3h")]
fn nightly_reconciliation(dag: &mut DagBuilder) {
    let extract = dag.activity(extract_ledger);
    let _reconcile = dag.activity(reconcile_accounts).upstream(&extract);
}
```

**Macro (`autumn-harvest-macros`).** `DagAttrs` gains `execution_timeout:
Option<String>` and `sla: Option<String>` (`dag.rs`), parsed at
macro-expansion time via the same `::autumn_harvest::task_duration(…)` call a
`#[workflow]` uses, and threaded into both the `DagInfo` and (when
`unified-dag-execution` is enabled on the proc-macro crate) the shadow
`WorkflowInfo` companion's `quote!` templates. The duration-string validator
`is_valid_task_duration` — previously private to `workflow.rs` — was hoisted
into a new shared `attr_util::is_valid_task_duration`, so `#[dag]` and
`#[workflow]` share one duration-parsing/validation implementation rather
than duplicating it. `emit_workflow_companion` was refactored from 9
positional arguments down to `(fn_name, fn_name_str, &DagAttrs)` to stay
under clippy's `too_many_arguments` threshold once the two new attributes
were added.

**Core (`autumn-harvest`).** `DagInfo` gains `execution_timeout:
Option<Duration>` and `sla: Option<Duration>` fields (`info.rs`).
`DagInfo::as_workflow_info()` — the function `HarvestBuilder::dags(...)` calls
to auto-register a unified DAG's shadow `WorkflowInfo` — copies both
**verbatim** onto the shadow `WorkflowInfo` it builds, alongside the
pre-existing `chain_execution_timeout: None` (DAGs carry no chain-scoped
lifetime cap, issue #617; unrelated to this feature). This is the
core-crate-level runtime propagation point; the macro-level `quote!` threading
into the DAG macro's OWN, separately-emitted shadow-`WorkflowInfo` companion
function (`emit_workflow_companion`, used for direct
`__autumn_workflow_info_{name}()` introspection/`workflows![]`-macro use, not
by `HarvestBuilder::dags(...)`) is covered in the "Macro" section above — both
sites are driven from the identical compile-time-parsed attribute string, so
they can never disagree for a given `#[dag(...)]` declaration, and both are
independently test-covered. Because the shadow
`WorkflowInfo.execution_timeout`/`.sla` are the *exact same fields* a plain
workflow populates, every downstream mechanism composes automatically with
no DAG-specific branch anywhere in the engine:

- **Hard deadline (AC1-AC2).** `deadline_at = started_at + execution_timeout`
  is set at start time (issue #322's existing per-run deadline machinery);
  the #243 scanner transitions an overrun DAG run to `TIMED_OUT` exactly like
  an overrun plain workflow, classified via the pre-existing
  `TimeoutType::WorkflowExecution` variant (no DAG-specific timeout kind was
  introduced).
- **Soft SLA (AC3-AC4).** `sla_deadline_at = started_at + sla` is set at
  start; the #487 scanner emits `harvest.workflow.sla_breached{workflow,
  queue}` **exactly once** when a DAG run passes it and keeps the run going
  — zero lifecycle effect, same as a plain workflow's soft SLA.
- **Clamp (AC5).** A DAG declaring `sla` larger than `execution_timeout` is
  clamped down to `execution_timeout` at start via the pre-existing
  `effective_sla = sla.min(hard)` arithmetic in `execution.rs` (the hard
  timeout would otherwise kill the run before the soft signal could ever
  fire) — the identical code path a plain workflow's clamp already runs
  through, not a DAG-specific reimplementation.
- **Fleet-wide ceiling (AC6).** `HarvestBuilder::max_workflow_execution_timeout(…)`
  caps a DAG's declared `execution_timeout` identically to a plain
  workflow's, since both flow through the same `WorkflowInfo.execution_timeout`
  field the ceiling clamps at start.
- **Zero regression (AC7).** A DAG declaring neither attribute gets `None`
  on both `DagInfo` fields; `deadline_at`/`sla_deadline_at` stay `NULL` at
  start and neither scanner ever inspects the run — byte-identical to a
  pre-#743 build.

**Compile-time validation (AC8, AC10).** `#[dag(execution_timeout = "4
hours")]` (a malformed duration string) is rejected at compile time with
`error: invalid execution_timeout duration; expected e.g. "30s", "5m", "4h",
"2d"`, mirroring the existing `#[workflow]` diagnostics rather than deferring
to a runtime `.expect(...)` panic. `#[dag(bogus_attr = "…")]`'s
"unsupported attribute" error now lists `execution_timeout` and `sla`
alongside every other recognized `#[dag]` attribute name.

**Management API (AC9, `autumn-harvest-plugin`).** `GET /admin/schedules`
(list) and `GET /admin/schedules/{id}` (single) surface each schedule's
*effective* `execution_timeout_secs`/`sla_secs` — resolved from the
registered workflow, or the DAG's shadow `WorkflowInfo` for `kind: "dag"`
schedules, and already clamped per AC5 — so an operator can see what a
scheduled run will get without cross-referencing source. New
`resolve_schedule_deadline_secs(registry, name)` (pure: looks up
`registry.workflows.get(name)`, applies the same `clamp_info_default_sla`
helper the start path uses) is resolved **once per request** (not once per
row) in `list_schedules`/`get_schedule`, and threaded through
`schedule_entry_from_row`'s new `registry: Option<&HandlerRegistry>`
parameter, `create_workflow_schedule`, and the PATCH `update_schedule_handler`
so a freshly created or edited schedule's response reflects it too. A
`None` registry (e.g. no live runtime installed) or an unregistered workflow
name resolves to `(None, None)` rather than panicking or guessing a stale
value.

**Test evidence.** TDD red-then-green throughout; all suites below were
actually executed (not merely compile-checked) against a locally provisioned
Postgres 16 in this sandbox, plus real `rustc`/`trybuild` compile-fail
subprocess runs — no Docker was needed since the plugin's own DB integration
test compiles clean and CI runs it Docker-backed.

- 3 new `trybuild` compile-fail fixtures (`autumn-harvest/tests/compile_fail/dag_invalid_execution_timeout.rs`,
  `dag_invalid_sla.rs`, `dag_unsupported_attribute.rs` + accepted `.stderr`
  snapshots), run via the existing `compile_fail_cases` harness in
  `macros_compile_fail.rs` — all pass, each producing exactly the diagnostic
  quoted above.
- 7 new pure macro-expansion unit tests in `tests/integration/macros_dag.rs`
  (attribute population, shadow-`WorkflowInfo` propagation, zero-regression
  defaults, `execution_timeout`-only leaves `sla` `None`) — no DB required,
  all pass.
- 3 new inline unit tests in `autumn-harvest/src/info.rs`
  (`dag_as_workflow_info_propagates_execution_timeout`,
  `..._propagates_sla`, `..._defaults_execution_timeout_and_sla_to_none`) —
  no DB required, all pass.
- 6 new DB integration tests in the new
  `autumn-harvest/tests/integration/dag_execution_timeout_tests.rs`
  (`dag_and_workflow_execution_timeout_ceilings_apply_identically`,
  `dag_declared_execution_timeout_and_sla_set_deadline_columns`,
  `dag_execution_timeout_scanner_transitions_expired_run_to_timed_out`,
  `dag_sla_breach_emits_metric_exactly_once_without_terminating`,
  `dag_sla_is_clamped_to_execution_timeout_at_start`,
  `dag_without_deadline_attributes_has_null_deadline_columns`) — driven end
  to end through a real worker + the real #243/#487 scanners against
  Postgres 16, all pass. Registered in `.github/ci/integration-suites.txt`
  for the Docker-backed Linux CI run.
- 6 new pure unit tests in `autumn-harvest-plugin/src/api.rs` — 5 for
  `resolve_schedule_deadline_secs` directly (no-registry, unregistered-name,
  verbatim propagation, the AC5 clamp, zero-regression defaults) plus 1 for
  `schedule_entry_from_row` end-to-end
  (`schedule_entry_from_row_resolves_execution_timeout_and_sla_for_a_dag_schedule`)
  proving the `kind: "dag"` path specifically: a `dag_name`-bearing row's
  `(kind, name)` derivation correctly feeds the DAG's own shadow
  `WorkflowInfo` lookup, not just a plain `workflow_name`-bearing row — no DB
  required, all pass.
- 1 new DB/HTTP integration test in `autumn-harvest-plugin/tests/api_scheduler_integration.rs`
  (`schedule_api_surfaces_effective_execution_timeout_and_sla`), exercising
  `GET /admin/schedules` and `GET /admin/schedules/{id}` end to end with
  three fixture workflows (undeclared / declared-unclamped / declared-clamped)
  — compile-checked in this sandbox (no Docker), run Docker-backed by CI.
- New example `autumn-harvest/examples/dag_execution_timeout.rs` (a nightly
  reconciliation DAG plus a zero-regression control DAG) with 5 embedded
  `#[test]`/`#[tokio::test]` self-checks, including a `WorkflowReplayer`
  determinism proof — all pass; `cargo run --example dag_execution_timeout`
  produces the expected demo output.
- Contract regression: `contract_response_fields_match_code_registry` and
  the other 30 tests in `contract_regression.rs` stay green unchanged — the
  hand-maintained per-route field lists in `management_api_response_fields()`
  document a curated subset of `ScheduleEntry`'s fields (many pre-#743
  fields, e.g. `remaining_runs`/`exhausted_at`/`calendar_name`, are likewise
  absent from those lists) and are checked only for code↔docs consistency,
  not code↔struct exhaustiveness, so `execution_timeout_secs`/`sla_secs`
  need no entry there to stay consistent with existing precedent.

**Docs.** `docs/getting-started/08-dags-and-schedules.md` gains
`execution_timeout`/`sla` rows in the `#[dag] attributes` table and a new
"Deadlines for scheduled DAG runs" subsection explaining the reuse-of-scanners
design, the clamp, the ceiling, and the `GET /admin/schedules` surfacing, with
a cross-link to the full workflow-level semantics in
`docs/getting-started/07-reliability-knobs.md` ("Soft SLA" / "SLA vs
`execution_timeout`"). Notes that classic (non-unified) DAGs are already
rejected at plugin startup and being retired, so `execution_timeout`/`sla`
are unified-DAG-only by construction (there is no shadow `WorkflowInfo` for a
classic DAG to carry them on).
