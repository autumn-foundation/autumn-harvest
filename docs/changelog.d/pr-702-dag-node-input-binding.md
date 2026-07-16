## Phase 3.56 — DAG node input binding: pass data between graph nodes (issue #702)

A `#[dag]` node's activity is fed the DAG's *trigger input* (wrapped in a `{ "conf": …, "dag_task": "<name>" }` envelope), **not** the output of the upstream node it depends on — so a classic `extract → transform → load` pipeline, where each stage consumes the prior stage's output, forced authors to flatten the pipeline into one mega-activity or hand-thread outputs through shared state. **Node input binding** closes that gap: bind a node's activity input directly to one or more upstream node outputs, one builder call per data edge, zero hand-written output-threading.

**No new `WorkflowEvent` variant, no migration, core-only** — node inputs are computed deterministically in the workflow body from already-recorded `ActivityCompleted` outputs (frozen in `harvest_events` before the bound node is dispatched), so replay reconstructs byte-identical inputs on every worker and every pass. Bindings take effect on the unified workflow-execution path (`unified-dag-execution`).

**API — three fluent `DagTaskRef` methods** (all by-value `self -> Self`):

- `.input_from(&upstream)` — the bound activity receives that upstream's recorded output **verbatim** (`DagInputBinding::Single`). No `conf`/`dag_task` envelope (AC2).
- `.input_from_all(&[&a, &b])` — a JSON object merging every upstream's output, keyed by each upstream's **activity name** (`DagInputBinding::Merged`).
- `.input_from_aliased(&[("k", &up), …])` — the same merge, keyed by the **given alias**.

New public types re-exported from `autumn_harvest`: `DagInputBinding { Single(usize), Merged(Vec<DagMergeSource>) }` and `DagMergeSource { key: String, upstream_index: usize }`.

**Binding implies the dependency edge (AC4).** Each method auto-adds the upstream to `task.upstreams` (deduped), so no separate `.upstream(&up)` is required — a binding *is* a data edge, and the bound node runs only after its bound upstream(s) have recorded their outputs. An extra explicit `.upstream(&up)` is harmless.

**Raw output vs. the trigger-input `dag_task` wrapper (AC2/AC3).** At runtime the unified level walker (`run_unified_dag`) resolves a bound node's activity input via the pure `bind_activity_input(binding, &outputs)`: the raw upstream output for `Single`, or a keyed `serde_json::Map` for `Merged` — with **no** `dag_task` injection and **no** `conf` wrapping. The unbound branch (trigger-input + `dag_task` envelope) is left byte-identical, so an unbound DAG is unchanged (AC3, regression-guarded).

**Skipped/failed upstream → deterministic `Value::Null` (AC6).** The `outputs` vector is initialized to `Value::Null` per node, and a skipped-or-failed node never overwrites its slot, so a binding to such an upstream contributes `Value::Null` — never a missing key. A bound node whose upstream was skipped is itself skipped by default; give it `TriggerRule::AllDone` to run anyway and branch on null-vs-payload.

**Three build-time error guards** (all in `DagBuilder::build`, caught at compile-of-the-DAG time, never at run time):

- `DagBuildError::ConflictingInputBinding { task }` — a node declares both an `input_from` binding and a mapped upstream (`.map_activity(…).over(…)`); the two are contradictory input sources.
- `DagBuildError::DuplicateInputBindingKey { task, key }` — a `Merged` binding has a repeated key: two upstreams sharing an activity name via `input_from_all`, or a repeated alias via `input_from_aliased`.
- `DagBuildError::InputBindingNotAnUpstream { task }` — a bound source is not a declared upstream; unreachable via the public API (bindings auto-add the edge), validated defensively.

**Test evidence — 12 tests (6 unit + 6 integration), TDD-verified.** Unit tests in `dag.rs` cover single/merged binding storage + auto-added edge, activity-name vs. alias keying, and all three build-error guards. Integration tests in `tests/integration/dag_input_binding_tests.rs` (`unified-dag-execution` + `testing`, no DB) cover a bound node receiving the raw upstream output live (AC2), a merged binding delivering a keyed object, a bound-to-skipped-upstream yielding null (AC6), an unbound DAG staying byte-identical (AC3, regression guard), a bound ETL history replaying deterministically (AC5), and a **1000-replay sweep** with randomized within-level completion order asserting `ReplaySucceeded` on every iteration. Example `autumn-harvest/examples/dag_data_flow.rs` (a three-stage ETL plus a fan-in merge) carries three additional embedded `WorkflowTestEnv`/`WorkflowReplayer` self-check tests. Docs: "Passing data between nodes (node input binding)" in `docs/getting-started/08-dags-and-schedules.md`.
