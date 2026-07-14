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

**Post-review hardening (Codex P2s).** (A) `info()` must be a genuinely
read-only snapshot: it previously read `history_event_count()`, which routes
through `match_history()` whose `pump_signal_handlers()` post-hook (issue #546)
dispatches push signal handlers and claims a reachable `SignalReceived` — so a
read-only `ctx.info()` would fire a registered signal handler, mutate
handler-captured state/control flow, and consume the signal, violating the AC5
zero-footprint contract. (The pre-existing `info_emits_no_commands_and_no_events`
test used a context with no handlers, so it passed vacuously.) `info()` now reads
`event_count`/`is_replaying` by locking the `HistoryMatcher` directly, bypassing
the post-hook; a doc note on the public `history_event_count()` accessor flags
that it still runs the pump. New regression test
`info_does_not_pump_signal_handlers_or_claim_signals` (registers a handler + a
reachable `SignalReceived`, asserts `info()` neither fires the handler nor
consumes the signal — a later flush still delivers it). (B) `parent_execution_id`
is now threaded through the DB `export_history` → `HistoryExportDocument` →
`replay_from_json` path (retention archives / offline replay), mirroring
`execution_timeout`/`deadline_at` exactly: `parent_id` is a row column absent from
every `WorkflowEvent`, so a parent-aware child exported through this path
previously deserialised parent = `None` and false-reported non-determinism. New
top-level `parent_execution_id: Option<ExecutionId>` field (additive
`#[serde(default, skip_serializing_if = "Option::is_none")]`, never redacted) on
both `HistoryExportRequest` and `HistoryExportDocument`; `export_history_decoded`
populates the document from the request, round-tripping into
`HistorySnapshot.parent_execution_id`. Threaded core-side through the retention
archive path (`CandidateExecution.parent_id` + both candidate SELECTs) and the
`handle.rs` query-hydration replay context (`replay_from_db`-style). Tests:
`HistoryExportDocument` round-trip + legacy-back-compat (no-parent field →
`None`) unit tests in `history_export.rs`, and end-to-end
`parent_aware_child_replays_clean_through_export_document_round_trip` (+ its
no-parent negative control) in `replayer_tests.rs` — a parent-aware child exported
via the document replays `ReplaySucceeded` through `replay_from_json` with **no**
manual `with_parent_execution_id` override. (C) **Plugin HTTP export handlers
threaded (completing the full #1040 mirror).** `parent_execution_id` is now
carried through **all three** export producers — single, batch, and retention —
exactly the way `execution_timeout`/`deadline_at` are, so `replay_from_json`
reconstructs the spawning parent for a child exported by **any** path. The two
`HistoryExportRequest` construction sites in the plugin's HTTP export route
(`autumn-harvest-plugin/src/api.rs`) set the field from the row/candidate
`parent_id` column: the single-execution handler (`export_history_for_execution`)
uses `execution.parent_id.map(ExecutionId::from_uuid)`, and the batch handler
(`export_history_for_candidate`) uses `candidate.parent_id.map(ExecutionId::from_uuid)`.
The plugin-local `HistoryExportCandidate` struct gained a `parent_id: Option<Uuid>`
field and the batch candidate SELECT (`HISTORY_EXPORT_CANDIDATES_SQL`) now selects
`w.parent_id` (added to the `GROUP BY` too), mirroring how it already SELECTs the
columns backing `execution_timeout`/`deadline_at`. With this, live HTTP single and
batch exports carry the parent (previously they round-tripped parent = `None`); the
core round-trip unit test and the `retention_archive_carries_parent_execution_id`
DB test cover the assertion end to end. (D) **`history_event_count` is the
replay-STABLE consumed position, not the total loaded snapshot length (Codex
P2).** `info()` previously reported `matcher.event_count()` — the total loaded
history snapshot, which **grows across workflow tasks** (a run replayed on task 2
loads a larger history than it did live on task 1), so the same `info()` call at
the same code position returned a different value live vs. replay, a false
non-determinism violating AC3. `info()` now reports the matcher's **consumed
cursor position** (`matcher.position()`, a pure non-pumping read consistent with
the existing non-pumping `info()` read path) — a pure function of how far the
workflow's own code has advanced through history at the call site, so it is
byte-identical across replay passes and workers. The public
`history_event_count()` accessor is **left as the total** because
`should_continue_as_new()` needs the whole-history size for its checkpoint
decision (the internal history-cap logic in `worker.rs` uses `events.len()`
directly); the `WorkflowExecutionInfo.history_event_count` field doc now states it
is the consumed position, replay-stable, and NOT suitable for continue-as-new
history-size decisions. New decisive regression test
`info_history_event_count_is_consumed_position_replay_stable` (two contexts for
the same execution driven to the SAME cursor, one loaded from a short history and
one from a longer one, assert EQUAL `history_event_count` — RED pre-fix: 3 vs 4).
(E) **`parent_execution_id` threaded through ALL remaining replay/handler-context
paths (closing the Codex parent-threading surface).** The field is now threaded at
**every** context-construction site that sources the sibling row-only columns
`execution_timeout`/`deadline_at` from the execution row — the exact same class,
since `parent_id` likewise lives on the row, absent from every `WorkflowEvent`:
worker dispatch (`WorkflowExecuteSpanMeta`), the executor strict/canary/live
construction chains, `WorkflowReplayer`/`replay_from_json` (strict + canary),
`WorkflowTestEnv`, the history-export document + retention archive + all three
plugin HTTP export handlers, the typed-client query-replay path (`handle.rs`), and
— **new this pass** — the plugin's `hydrate_ctx_for_query` query-replay context
(`autumn-harvest-plugin/src/api.rs`, `.with_parent_execution_id(execution.parent_id.map(ExecutionId::from_uuid))`)
and the declarative `#[update]` handler contexts (both the
`register_declarative_update_handler` registration wrapper and the `invoke_update`
declarative fallback in `context.rs`, which copy the parent's
`workflow_id`/headers/deadline into the throwaway `new_for_handler` context but
previously dropped `parent_execution_id`). Query handlers run against the main
`self` context (`h(self, args)`), so they already inherit the parent; signal
handlers likewise dispatch in the main context. New regression test
`update_handler_inherits_parent_execution_id` (a declarative update handler
invoked on a child run reads `ctx.info().parent_execution_id == Some(parent)` —
RED pre-fix: `None`). No CLI, scheduler, `ActivityContext`, event, schema, or
migration changes.
