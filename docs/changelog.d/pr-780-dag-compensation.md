## Phase 3.x — Declarative DAG node compensation (issue #780)

**Implemented.** A `#[dag]` pipeline that half-succeeds left its completed
nodes' side effects **dangling**: reserve inventory, charge the card, allocate
a shipment — then the label printer 500s, the DAG fails, and the inventory is
still held, the customer is still charged, the shipment slot is still
allocated. Harvest already had the right primitive for this
(`Saga`, LIFO compensation, `docs/saga.md`), but reaching it meant abandoning
the `#[dag]` surface entirely and hand-writing a `#[workflow]` wrapper with one
`saga.step(…)` per node — losing the graph the author actually wanted to
declare. This closes that gap **declaratively**: one builder call per node,
zero hand-written rollback code, and **no new engine machinery** — the unwind
runs on the existing `Saga`, rides the existing markers and counters, and adds
**no new `WorkflowEvent` variant and no migration**.

```rust
#[dag]
fn fulfillment(dag: &mut DagBuilder) {
    let reserve = dag.activity(reserve_inventory).compensate(release_inventory);
    let charge  = dag.activity(charge_payment).upstream(&reserve).compensate(refund_payment);
    let _label  = dag.activity(print_label).upstream(&charge).compensate(void_label);
}
```

**API (`autumn-harvest/src/dag.rs`).** `DagTask` gains an opt-in
`compensate: Option<String>`; `DagTaskRef::compensate(activity_fn)` derives the
name from the fn item exactly like `DagBuilder::activity` (so a typo is a
compile error, not a mid-unwind dispatch failure) and
`DagTaskRef::compensate_named("name")` is the string escape hatch for a
compensator whose fn item is not in scope. At most one per node; last call
wins. One compensator activity may be **shared** by several nodes — the
envelope's `dag_compensate` field disambiguates which node it is undoing.

**Unwind semantics.** The unwind fires on `run_unified_dag`'s existing terminal
failure check (the one producing `Err("one or more DAG tasks failed")`). A node
is compensated **iff** it BOTH reached `TaskStatus::Succeeded` AND declares a
compensator — a node that was skipped (trigger rule or `.condition(…)`), never
reached, or that **failed itself** is never compensated, since by the saga
contract only a successful forward step has an effect to undo. Compensations
are pushed in the DAG's own forward order (levels forward, ascending index
within a level) and popped LIFO by `Saga`, giving **reverse topological** order
deterministically by construction — a pure function of the level structure, not
of completion timing. A **successful** unwind still returns the original
`"one or more DAG tasks failed"` (compensation is cleanup, not an outcome
change); a **failing** compensator does not abort the unwind (continue-not-
abort, the same contract as `compensate_all`) and the run then surfaces a
stringified `HarvestError::SagaCompensationFailed` carrying both the original
DAG error and every compensation error.

**Envelope + queue.** Each compensator receives the fixed shape
`{"dag_compensate": <node's activity_name>, "input": <the node's resolved
forward input>, "output": <the node's recorded output>}` — so one generic
compensator can serve N nodes by branching on `dag_compensate`, and the
documented "compensate by ID" idempotency pattern reads that id out of
`output`. `input` is the node's *resolved* forward input: the raw upstream
output for an `.input_from(…)`-bound node (issue #702), the
`{ "conf": …, "dag_task": … }` wrapper for an unbound node, the whole mapped
array for a mapped node. Dispatch uses the ordinary DAG activity lowering
(`execute_activity_raw_with_opts`) on the **compensated node's own queue**
string — byte-identically resolved to the forward node's
(`tasks[i].queue.clone().unwrap_or_default()`), so `""` (unset) resolves at the
worker to the compensator activity's own `default_queue`, falling back to
`"default"`. The node's `retry`/`start_to_close` overrides are deliberately
**not** applied to the compensator: they describe the forward step's failure
budget, so the compensator activity's own `#[activity(…)]` attributes apply.

**Rides issue #801's saga rails verbatim.** The unwind is a plain `Saga`, so it
reuses that slice's machinery unchanged: the `saga_compensated:{seq}` /
`saga_compensation_failed:{seq}` `MarkerRecorded` dedup markers, the
`harvest.saga.compensated` / `harvest.saga.compensation_failed` counters, and
the starter alerts `harvest_saga_compensation_spike` /
`harvest_saga_compensation_failed`. **Operator-visible consequence:** those
counters now include DAG rollbacks as well as hand-written sagas, distinguished
by the `workflow` label (a unified DAG's shadow `WorkflowInfo` carries the DAG
name, issue #256). Compensation is recorded as ordinary
`ActivityScheduled`/`ActivityCompleted` events plus that single marker — pinned
by an allowed-event-type test that would fail on any new variant.

**Success path untouched.** The saga is constructed only on the failure branch,
so a DAG that succeeds dispatches zero compensators, records zero saga markers,
and emits zero saga metrics — byte-identical to a pre-#780 build.

**Cancellation skips the unwind — a deliberate decision, not an omission.**
A cancelled run returns the original DAG error and compensates nothing. This is
consistent with `docs/saga.md`'s "cancellation does NOT auto-compensate"
contract, and it is also load-bearing: a recorded `WorkflowCancelled` has no
workflow-command counterpart, so dispatching a compensator into it would
diverge (`expected ActivityScheduled(..), got WorkflowCancelled`) and
**nd-block (#603)** a run the operator had already cancelled — turning a clean
cancel into a wedged execution. **Accepted edge:** a cancel landing
*mid*-unwind terminates the run FAILED without running the remaining
compensators. That is the same class as the pre-existing
durable-compensation-after-cancel limitation already documented in
`docs/saga.md` (the engine will not schedule new durable work for a cancelled
execution); the unwind is guaranteed to terminate and to leave no deferred
non-determinism error behind, but the remaining state is genuinely dangling and
needs manual reconciliation (or reset, issue #148).

**Known limitation — compensation is node-granular.** Selective/partial
compensation is out of scope per the issue, with two concrete consequences for
mapped nodes: a **`CollectAll`** mapped node that reaches `Succeeded` *with
some failed cells* is compensated **once**, with the full cells array (failed
cells included) as `output` — the compensator decides per cell what to undo;
and a **`FailFast`** mapped node driven to `Failed` by a single cell is **not
compensated at all**, so the side effects of the cells that *did* succeed
before the failure are left **uncompensated**. Both are pinned by test, not
merely documented.

**Build-time rejections (fail before a single node runs, never mid-unwind when
the state is already dangling).** Three new `DagBuildError` variants —
`CompensateOnGate` (a signal gate dispatches no activity, so it has no side
effect to undo), `EmptyCompensator` (an empty/whitespace name would dispatch a
nameless activity at exactly the wrong moment), and
`CompensatorNameCollidesWithNode` (a compensator sharing a **forward node's**
identity — another node's name, the declaring node's own name, or a gate's
signal name — would be indistinguishable from that node in recorded history,
corrupting the name-keyed classification the DAG run graph (issue #690) and
retry-from-node (issue #366) depend on). Plus one new `HarvestBuilderError`
variant, `DagCompensationRequiresUnifiedExecution`: a **classic**
(non-unified) DAG has no unwind step, so its compensator would silently never
run — the worst possible failure mode for an undo (mirrors
`DagSignalGateRequiresUnifiedExecution`, issue #746). The pre-existing
`LocalActivityInDag` check was extended to compensators (a compensator goes
through the same activity-queue lowering as a forward node, so a `local = true`
activity is just as invalid there) and names the **compensator**, not the
forward node that declares it. Finally, plugin **preflight**
(`dag_unregistered_activity_failures`) now flags a compensator naming an
unregistered activity — `dag '…' references unregistered compensator '…' for
task '…'` — so a missing compensator is caught before rollout instead of
mid-unwind.

**Semver note.** `HarvestBuilderError` is `#[non_exhaustive]`, so its new
variant is fully additive. `DagBuildError` is **not** `#[non_exhaustive]`, and
`DagTask` gains a `pub compensate` field — so, exactly as issue #702's
`input_from` addition documented, both are minor additions that are breaking
only for a downstream *exhaustive* `match` on `DagBuildError` or a `DagTask`
struct literal / exhaustive destructure, consistent with this repo's existing
`WorkflowInfo`/`ActivityInfo` public-field-addition norm.

**Zero public `Saga` surface growth.** `Saga` gains two `pub(crate)` methods:
`push_compensation(closure)` registers a compensation **without** running a
forward step (the DAG unwind already knows from recorded history which nodes
succeeded, so it must not re-execute them — `step` couples the two), and
`compensate_all_after(original)` reports the caller's real error rather than
the fixed `"manual compensation requested"` string, so a DAG unwind's
`SagaCompensationFailed` carries the true `"one or more DAG tasks failed"`
cause. The public `compensate_all()` now delegates to
`compensate_all_after("manual compensation requested")` — unchanged behaviour
for every existing caller, and one shared unwind implementation rather than
two.

**Forward compatibility.** A #780 history — one that recorded a
`saga_compensated:{seq}` marker — does not replay under a pre-#780 build (the
marker is unknown to the old matcher); a rollback nd-blocks (#603,
non-terminal, recoverable) until rolled forward. The standard marker-feature
rule, identical to #801's. Pre-existing histories are untouched, since the saga
is only reached on the failure branch.

**CI.** One new manifest row in `.github/ci/integration-suites.txt`
(`linux  autumn-harvest  integration  testing  dag_compensation`) so the suite
actually executes rather than being only compile-checked, plus a
self-guarding test (`dag_compensation_suite_is_wired_into_ci`) that parses the
manifest and fails if the row is ever dropped. A new `ci.yml` step runs the
example's embedded tests, mirroring the existing `dag_data_flow` step.

**One RED-test correction during the cycle.** The success-metric test initially
asserted `ReplayStatus::ReplaySucceeded` for a compensated history. That status
is reserved for a handler returning `Ok`, and a failing DAG's handler returns
`Err("one or more DAG tasks failed")` — so *every* failing-DAG history
(compensating or not) replays to `ReplayStatus::WorkflowFailed`, which is
precisely "reproduced the same terminal error with **zero** divergence". The
assertion was corrected to pin `WorkflowFailed` **with the exact original
error**, which still rules out `NonDeterminismDetected`, and matches four
pre-existing precedents in the repo (`dag_unified_tests.rs`,
`dag_input_binding_tests.rs`, `dag_mapping_tests.rs`,
`dag_signal_gate_tests.rs`).

**Test evidence.** TDD red-then-green throughout; every suite below was
actually executed (not merely compile-checked) in this sandbox.

- **14 integration tests** in the new
  `autumn-harvest/tests/integration/dag_compensation_tests.rs` — all pass
  (`cargo test -p autumn-harvest --features testing --test integration --
  dag_compensation_tests --test-threads=1` → `14 passed`). Covers reverse-
  topological order over a diamond, the envelope for unbound and
  `.input_from`-bound nodes, skipped/never-reached/uncompensated/failed nodes
  invoking nothing, mapped-node granularity (both policies), cancellation
  skipping the unwind, a cancel landing mid-unwind terminating without an
  nd-block, `SagaCompensationFailed` with continue-not-abort plus its marker
  and page counter, the no-new-`WorkflowEvent`-variant guard, queue
  inheritance, and the untouched success path. The **success-metric** test
  (`fulfillment_dag_leaves_zero_uncompensated_side_effects_across_1000_runs`)
  drives 1000 deterministically-seeded runs across two topologies × every
  failure position, asserting a ledger that nets to **zero uncompensated side
  effects**, an exact reverse-of-the-compensable-succeeded-prefix dispatch
  order, and — folded into the same loop — that every produced history replays
  deterministically.
- **Pure unit tests**: 5 in `dag.rs` (typed/named/undecorated field
  population, the three build guards, and shared-compensator-is-not-a-collision),
  2 in `builder.rs` (local-activity and classic-DAG rejections, each asserting
  the error names the compensator), 2 in `saga.rs` (`push_compensation`
  registers without invoking; `compensate_all_after` carries the caller's
  original while `compensate_all` keeps its legacy string), 1 in the plugin's
  `preflight.rs` (an unregistered compensator is flagged, a registered one is
  not).
- **Whole-crate suites, all green**: `autumn-harvest --all-features --lib`
  → **2501 passed**; `autumn-harvest --no-default-features --features db --lib`
  (the classic, non-unified-DAG build) → **2412 passed**;
  `autumn-harvest --no-default-features --lib` → **1782 passed**;
  `autumn-harvest --no-default-features --features testing --test integration`
  → **1079 passed**; `autumn-harvest-plugin --lib` → **803 passed**.
- **Neighbour DAG/saga suites** (`-- dag_ saga_`) → **124 passed**. The 15
  `dag_execution_timeout_tests` cases in that filter are Docker/testcontainers-
  gated and cannot run in this sandbox (`SocketNotFoundError("/var/run/docker.sock")`);
  CI runs them Docker-backed.
- **New example** `autumn-harvest/examples/dag_compensation.rs` with 3 embedded
  `#[tokio::test]` self-checks — all pass
  (`cargo test -p autumn-harvest --no-default-features --features
  testing,unified-dag-execution --example dag_compensation` → `3 passed`):
  a successful run compensating nothing, a `print_label` failure unwinding in
  reverse order with a net-zero ledger and envelope assertions, and a
  `replay_check` determinism proof.
- `cargo fmt --all -- --check` and `cargo clippy … -D warnings` clean on both
  crates.

**Docs.** `docs/saga.md` gains a "Saga for DAGs — declarative node
compensation" section (the two builder methods, the compensated-iff table, the
reverse-topological order guarantee, the envelope, queue inheritance, failure
semantics, the shared #801 counters and their new DAG population, the
cancellation rule and its mid-unwind edge, the node-granularity limitation, the
build-time guard table, and forward compatibility) plus a second test-coverage
table for the new suite. `docs/getting-started/08-dags-and-schedules.md` gains
an author-facing "Automatic rollback — node compensation" section with the
five-node fulfillment example, a what-runs-and-what-doesn't table, a concrete
envelope sample, the errors an author can hit with their message gist, and
pointers to `docs/saga.md` for the full contract.
