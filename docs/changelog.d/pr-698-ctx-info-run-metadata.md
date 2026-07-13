## Phase 3.52 — Workflow run metadata via `ctx.info()` (issue #698)

**Before:** a `#[workflow]` body could read its author-supplied business key
(`ctx.workflow_id()`) and type (`ctx.workflow_type()`), but **not** its
system-generated `execution_id` — the exact identifier every operator surface
keys on (the management-API path `/api/harvest/workflows/{exec_id}/...`, the DLQ,
reset/pause/cancel, and the ADR-0001 `execution.id` span attribute). A workflow
could log `workflow_id = "cart-42"` but an operator paging from that line had no
run UUID to open the execution or build a correlated trace query; authors also
couldn't mint run-scoped idempotency keys, and a child workflow couldn't identify
the parent that spawned it.

**After:** one read-only, replay-safe accessor `WorkflowContext::info() ->
WorkflowExecutionInfo` bundles the run's system metadata:
`execution_id`, `workflow_id`, `workflow_type`, `start_time`,
`history_event_count`, `is_replaying`, and `parent_execution_id`
(`None` for a top-level run, the spawning parent's id for a child). Two sibling
`const fn` accessors are added — `start_time()` (the **raw** frozen
`WorkflowStarted` timestamp, deliberately **not** the advancing virtual clock
that `now()` moves under the test harness) and `parent_execution_id()`.

**Design.** `info()` is the single source of truth built from the existing
accessors (`execution_id()`/`workflow_id()`/`workflow_type()`/`start_time`/
`history_event_count()`/`is_replaying()`/`parent_execution_id`), so it stays
consistent with each of them and adds no new logic. `WorkflowExecutionInfo` is a
`#[non_exhaustive]`, `Debug + Clone + PartialEq + Eq` struct co-located in
`context.rs` (leaving the `ActivityExecutionInfo` name free for the activity-side
slice, issue #783), re-exported from `lib.rs` and the prelude so it is usable from
any `#[workflow]` body **with no feature flag**.

**Parent threading.** `WorkflowExecuteSpanMeta` gains a
`parent_execution_id: Option<ExecutionId>` field; the worker populates it from the
execution row's `parent_id` column (`prepared.execution.parent_id.map(ExecutionId::from_uuid)`),
and the executor threads it into the live `WorkflowContext` alongside
`workflow_name`/`workflow_id`/`build_id`/`execution_timeout`/`deadline_at` in both
live construction chains (`run_workflow_with_state_history_policy_and_caps` and
`run_workflow_with_state_advancing_clock`). The query/update throwaway
(`new_for_handler`), replayer, and bare-test contexts default it to `None`.

**Invariants.** Reading `info()` appends **zero** `harvest_events` and emits
**zero** `WorkflowCommand` (same leave-no-trace property as a query handler), and
returns byte-identical values on every worker and every replay pass — all fields
derive from already-recorded state. **No new `WorkflowEvent` variant, no
migration, no shard-semantics impact.** Additive public API (minor bump). Out of
scope (per the issue): mutable search-attr reads, a workflow-level `attempt`
counter / continue-as-new run-count, nominal scheduled fire-time (#508), and the
`ActivityContext` equivalent (#783).

**Tests (TDD red→green).** Unit tests in `context.rs`: all-fields on `new_test`,
graceful `new_for_handler`, `start_time()` raw-vs-advancing-clock trap, configured
vs. default parent, zero command/event footprint, `execution_id` string
round-trip through `ExecutionId`'s `FromStr` (AC4 core), and the Success-Metric
replay-determinism property test (**N = 1,000** rebuilds from one fixture yield
byte-identical `info()`, 0 divergences). No-DB `WorkflowTestEnv` integration tests
(via the new `WorkflowTestEnv::with_parent_execution_id` builder): a
parent-reading workflow reports the configured parent and replays deterministically
(`ReplaySucceeded`), and a top-level run reports `null`. DB/worker integration test
`tests/integration/ctx_info_tests.rs` (testcontainers): a real spawned child
reports the parent's `execution_id` via `ctx.info().parent_execution_id` end to end
(worker → executor → context), while the top-level parent reports no parent.
Example `autumn-harvest/examples/ctx_info.rs` (logs `execution_id`, mints a
run-scoped `charge-{execution_id}` idempotency key, reads the parent) with embedded
`WorkflowTestEnv` tests.
