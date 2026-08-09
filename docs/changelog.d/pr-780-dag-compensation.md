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

**Rolling the engine back mid-unwind truncates it silently** (corrected during
post-review — see the hardening section below; the original claim that a
rollback nd-blocks was empirically false). `saga_compensated:{seq}` is an issue
#801 marker the pre-#780 matcher already knows, AND pre-#780 `run_unified_dag`
returns `Err` at the terminal-failure check **before consuming anything** — so
an old build seals a mid-unwind run terminally FAILED with a partial unwind and
**no error signal at all**. Operators must drain in-flight *compensating* runs
before rolling back past #780, or roll forward. Runs that never unwind are
unaffected (the saga is only reached on the failure branch).

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

---

## Post-review hardening (four-angle adversarial review)

Four reviews — cross-crate, saga-contract, replay-determinism, and test-quality
— returned **two P1s and a batch of P2/P3s**, every one of them fixed below.
Both P1s were genuine engine bugs found (and reproduced) by review rather than
by the original test suite; both are fixed TDD-style with the RED failure
captured before the fix.

### P1-B (blocker) — `FailFast` mapped node, failing cell not last: unwind diverged into a permanent nd-block

**The bug.** `run_unified_dag`'s `FailFast` mapped-node arm abandoned its
`FuturesUnordered` instance stream (`drop(stream); break;`) on the first failing
cell. On a resume/replay cycle every instance resolves synchronously from
history, and `FuturesUnordered::poll_next` returns as soon as ONE future is
ready — so the instances not yet yielded were **never polled**, their recorded
`ActivityScheduled` events stayed unconsumed, and the replay cursor was left
parked on one of them.

That was harmless before #780 (the terminal check returned `Err` immediately and
nothing consumed history afterwards). #780 made the unwind the first thing to
consume history after the level loop, so it dispatched the first compensator
straight into that stale cursor: `match_activity` diverged, `Saga` swallowed the
divergence into `compensation_errors` — **but the divergence also set
`nd_details`**, so the executor's `Ok(Err(..))` arm handed the worker a
`WorkflowOutcome::Failed { non_deterministic_details: Some(..) }` and issue #603
**nd-blocked the run permanently**. Data-caused, so not self-healing: every
retry replays it identically. Net effect: the compensator silently never ran AND
the run wedged `RUNNING` forever — for **every** failing index except the last
one polled (9/10 of a 10-cell fan-out).

The original T13 fixture missed it because its `[1, -1]` array fails at the LAST
cell, the one position that happens to leave a clean cursor.

**The fix** (`autumn-harvest/src/dag.rs`): drain the instance stream instead of
abandoning it. The reviewer's alternative — gating the unwind on a cursor-clean
precondition and skipping it — was rejected: it preserves timing at the cost of
the compensation guarantee, which is the whole point of the feature. Draining is
also the only cursor-clean option, because polling a still-in-flight instance
once (the latency-preserving alternative) would push a `WaitForActivity` command
that the unwind's `ScheduleActivity` batch cannot legally share.

*Semantics preserved:* first failure wins (`status` only ever moves
`Succeeded -> Failed`, and `final_val` is built solely from the all-succeeded
case); a non-activity error (a genuine non-determinism divergence) still
propagates rather than being swallowed; `CollectAll` is untouched (`join_all`
already polls every instance); the LIVE pass is unchanged (instances suspend, so
the stream ends via suspension and the drain path is never reached).
*One behavioural change:* a `FailFast` mapped node now settles every instance
before the DAG terminates, instead of abandoning the in-flight siblings. The
`MapFailurePolicy::FailFast` doc comment claimed those siblings were "cancelled"
— they never were (the activities keep running on their workers; `drop` only
abandoned the local futures), so the doc was corrected rather than the
behaviour.

*RED evidence,* captured against the unmodified `dag.rs`:

```
assertion `left == right` failed: a FailFast mapped node failing mid-array must
still unwind the succeeded prefix (the failed mapped node itself is not compensated)
  left: []
 right: ["m_undo_root"]
```

*Pinned by* `failfast_mapped_node_failing_mid_array_compensates_and_never_nd_blocks`
(3-cell array failing in the middle; asserts the compensator dispatched, the
error is the ORIGINAL DAG error rather than a swallowed `SagaCompensationFailed`,
`nd_details` is `None`, and `replay_check` reports `WorkflowFailed` with that
exact error) and its randomized sibling
`failfast_mapped_node_compensates_for_every_failing_cell_position`, which sweeps
the failing cell across every position of a 5-cell node.

### P1-A — retrying a compensated DAG run double-spent the compensation

**The bug.** A terminal-FAILED DAG run that executed its unwind could still be
retried via `POST /dags/{name}/runs/{id}/retry` (issue #366). The resolver cuts
at the failed node's level and **carries over** the succeeded upstream nodes —
which is precisely the set the unwind just rolled back. The retried run then
proceeded as if those side effects still existed.

**The fix.** A guard in the pure `resolve_retry_plan`
(`autumn-harvest-plugin/src/dag_retry.rs`): a `MarkerRecorded` whose name
carries the `saga_compensat` prefix (covering both `saga_compensated:{seq}` and
`saga_compensation_failed:{seq}` — either proves an unwind ran) rejects the
request with the new `DagRetryResolveError::CompensatedRun`. Checked before any
node validation, so the operator gets the *state* answer rather than a
node-shaped one. HTTP mapping is a **`409 Conflict`** — the one resolver
rejection that is a state conflict about the run rather than a malformed node
request — alongside the existing COMPLETED/RUNNING conflicts; the message is
shared with the Vantage UI flash via one constant so the two surfaces cannot
drift. A run that failed **without** compensators (and every pre-#780 history)
records no such marker and stays fully retryable.

*RED evidence:* `error[E0599]: no variant or associated item named
`CompensatedRun` found for enum `dag_retry::DagRetryResolveError``.

*Pinned by* `resolve_rejects_a_run_that_already_compensated`,
`resolve_rejects_a_run_whose_compensation_failed`, and the regression guard
`resolve_allows_a_failed_run_without_a_compensation_marker` (which also asserts
an unrelated `dag_skip:` marker is not mistaken for a compensation).

### P2 / P3 batch

| Finding | Fix |
|---------|-----|
| **P2-B** — T15 asserted the wrong ND slot | `take_deferred_nd_error` is only written by the infallible issue #384 primitives; issue #603 gates the nd-block on `take_nd_details`. The old assertion would have passed while the run wedged forever. Switched to `nd_details`, and the same assertion is used by every new "must never nd-block" test. T15 also gained a zero-further-dispatch assertion (parity with T14) |
| **P2-C** — T13's mapped-envelope `input` assertion was tautological | `envelope.get("input").is_some()` also accepts `Null` — which is exactly what deleting the mapped-node input capture produces (verified: the mutant passed all 14 original tests). Replaced with `assert_eq!(…, Some(&json!([1, -1])))`; the same tautology in `examples/dag_compensation.rs` was replaced with an exact-value assertion too |
| **P2-D** — no fixture distinguished level order from index order | Every original fixture declared nodes in topological order, so a flat `for i in 0..tasks.len()` push passed all 14 (verified mutant). New `unwind_order_is_reverse_topological_not_reverse_declaration_index` declares a child BEFORE its parent, making declaration index the exact reverse of topological order |
| **P2-E** — AC2 "not recovered by retry" had zero evidence | No test applied `.retry(…)` to a DAG node. Added both halves: `a_node_recovered_by_retry_compensates_nothing` and `a_node_that_exhausts_its_retries_unwinds_the_prefix_but_not_itself` |
| **P2-F** — AC5 mid-unwind RESUME (crash recovery) untested | Added `an_unwind_resumed_mid_flight_dispatches_only_the_remaining_compensations`: a hand-built history carrying the forward pass, the `saga_compensated:1` marker, and the first compensator already scheduled AND completed. Asserts only the remainder is dispatched, no non-determinism, and no double-count of the counter. Drives through the new `drive_replay_tolerating_suspension` helper: a `for_replay` context has no driver, so the *remaining* compensation — genuinely new durable work — parks on its oneshot, which is the correct resume shape rather than a failure. The `ScheduleActivity` command is pushed before the await, so the assertion still observes it, and an empty command list would fail the `assert_eq!` (the test cannot pass vacuously) |
| **P2-2** — no positive metric assertion | `CompRecordingMetrics` now captures the `(workflow, queue)` labels. New `a_successful_unwind_fires_the_compensated_counter_once_with_labels` (exactly once per UNWIND, not per compensation, with the run's own labels); T16 gained label assertions plus a `failed ≤ compensated` check |
| **P3-B2** — no failure-path byte-identity test | New `failing_dag_without_compensators_emits_no_saga_commands_or_metrics`. This is the one that actually **reaches** `unwind_dag_compensations` and proves the empty-stack early return allocates no seq, records no marker, and emits no metric (T19 only covers the success path, which never constructs a saga at all) |
| **P3-6** — "last call wins" documented but untested | New `repeated_compensate_calls_are_last_wins` over all four typed/named orderings |
| **P3-7** — `compensate_named` did not trim while `EmptyCompensator` judged on the trimmed form | `compensate_named` now trims on insert. New `compensate_named_trims_surrounding_whitespace` also pins that a whitespace-only name is still a build error and that a padded name which trims onto a forward node still collides |
| **P3-T4** — cheap probes unpinned | Added `two_failing_compensators_collect_both_errors_and_page_once` (continue-not-abort with several failures; the page counter is per-unwind) and `a_compensator_inherits_no_retry_or_timeout_override_from_its_node` (pinned on the emitted `ScheduleActivity`, whose `retry_policy_override` / `start_to_close_override` are exactly where an inherited override would ride — `unwind_dag_compensations` passes `None, None`, so the compensator activity's own `#[activity(…)]` attributes apply) |
| **P3-2** — T24 could be credited by an unrelated short filter | Widened the tokenized match: the filter must both start with `dag_compensation` and be a prefix of the module name. The `linux` field check is deliberately **kept** — it is load-bearing, since an `allos` row runs `--no-default-features`, which `cfg`s the whole file away |
| **P3-3** — `#780` in a `ci.yml` step name | An unquoted `#` after whitespace starts a YAML comment, truncating the name at `(issue`. Quoted the step name |
| **P3-T2** — the example's forward mocks all returned the same blob | Each node now returns a distinct output, so an `output` assertion can only pass if the engine carried THAT node's recorded output |
| **P3-T3** — T19's "byte-for-byte" doc over-claimed | Reworded to what it asserts (dispatches / markers / metrics), pointing at the new failure-path test as its complement |

### Docs corrected

* **P2-1 (factually wrong).** The forward-compatibility paragraph claimed a
  rollback to a pre-#780 build mid-unwind nd-blocks. Empirically false:
  `saga_compensated:{seq}` is an issue #801 marker old builds already know, AND
  pre-#780 `run_unified_dag` returns `Err` at the terminal check *before
  consuming anything*. The real behaviour is worse and needed saying: the old
  build seals the run terminally FAILED with a **partial unwind and no error
  signal at all**. Rewritten in `docs/saga.md` (and the corresponding paragraph
  in this fragment) with the operator rule — drain in-flight compensating runs
  before rolling back, or roll forward.
* **P2-3.** `compensate_named` was described as supporting "remote/polyglot"
  compensators, but plugin preflight fails the boot for a compensator name not
  in the registry. Softened in both `docs/saga.md` and
  `docs/getting-started/08-dags-and-schedules.md`: it is name-based dispatch,
  and the name must still be registered.
* **P2-G.** Documented that a genuine code-drift divergence *inside* an unwind
  nd-blocks (issue #603) rather than sealing `SagaCompensationFailed`, even
  though `Saga` collects the error string — and that this is the correct
  outcome, since a divergence during an unwind is a deploy problem.
* **P3-4.** Compensations are ordinary activities and are **not** rendered as
  nodes in the DAG run-graph view (issue #690) or any definition-derived export.
* **P3-5.** The compensation envelope embeds the node's whole input *and*
  output, so the issue #252 activity-input cap applies to the **envelope** — for
  a mapped node over a large array it can be much larger than the node's own
  input was.
* **P3-B1.** An unsolicited signal to a DAG run leaves its unwind *uncounted*
  (inheriting #801's drained-signal-frontier conservatism): the compensations
  still run and still replay, only the observability is lost.
* **P3-T1.** The envelope's `input` documentation enumerated three node kinds
  but not the fourth: a **Merged** binding (`.input_from_all` /
  `.input_from_aliased`) yields the KEYED OBJECT, not a raw upstream output.
  Both docs now carry a four-row table.
* **#366 interaction.** `docs/saga.md` and
  `docs/runbooks/dag-retry-from-failed-node.md` document that a compensated run
  is not retryable, with the exact `409` body and the "start a fresh run"
  remediation.
* **P3-1** was checked and found already correct: both docs say the compensator
  dispatches on the *node's own* queue, which matches
  `tasks[i].queue.clone().unwrap_or_default()`. No change needed.

### Deliberately NOT changed

**P3-8 (T20's ~8 min runtime).** The test-quality review measured the cost as
100% `WorkflowTestEnv::run` (≈0.5 s/iteration = 5 levels × the 100 ms
`SUSPENSION_TIMEOUT`), **not** `replay_check` — so the originally-proposed fix
(sampling `replay_check` every Nth iteration) would save nothing. Reducing the
iteration count or DAG depth is also off the table: the issue's success metric
mandates 1,000 runs on the 5-node DAG. The cost is documented here instead: the
`dag_compensation` CI row takes roughly **8–10 minutes** of serial Linux CI time,
almost all of it in this one test.

## Post-PR review (Codex on PR #1153)

### P1 — a marker-less unwind escaped the retry guard

The `CompensatedRun` guard added above detected an unwind **only** by its
`saga_compensat*` marker. That marker is not guaranteed for a DAG: issue #801's
matcher deliberately leaves an unwind uncounted at a **drained signal frontier**,
so a DAG run that received an unsolicited signal dispatches its compensators and
rolls its side effects back while recording no marker at all (the known
limitation already documented in `docs/saga.md`). Such a run therefore stayed
retryable, and the level-granular cut carried over exactly the nodes the unwind
had just undone — the compensation double-spend the guard exists to prevent.

The observability gap and the retry guard were each reviewed and documented in
isolation; the defect was their intersection — the marker became load-bearing for
*correctness* while remaining best-effort for *observability*.

`ran_compensation_unwind` now takes the `DagDefinition` and accepts **either**
signal: the marker, **or** an `ActivityScheduled` naming one of that DAG's
declared compensators. The second signal is unambiguous by construction —
`DagBuildError::CompensatorNameCollidesWithNode` already rejects at build time
any compensator sharing a forward node's name, precisely so a compensation
dispatch stays distinguishable in recorded history for the name-keyed run graph
(#690) and this resolver (#366). A DAG with no compensators collects an empty
set, so it can never enter this branch and stays byte-identically retryable.

RED evidence (marker-less compensated history, before the fix):

```
left:  Ok(DagRetryPlan { reset_to_event_id: 2,
        nodes_to_re_execute: ["b", "c", "d"], nodes_carried_over: ["a"] })
right: Err(CompensatedRun)
```

`nodes_carried_over: ["a"]` is the double-spend: `undo_a` had already rolled `a`
back.

Pinned by `resolve_rejects_a_marker_less_compensated_run` (a history with an
unsolicited `SignalReceived`, a compensator dispatch, and **no** marker) and the
regression guard `resolve_allows_a_compensating_dag_whose_unwind_never_dispatched`
(a compensating DAG that failed before any compensator ran stays retryable).
Docs corrected in `docs/saga.md` (both the #366 section and the stray-signal
limitation, which is now explicitly scoped to observability only),
`docs/runbooks/dag-retry-from-failed-node.md`, and the `dag_retry` module doc.

### P1 — a mapped node's two non-dispatching outcomes broke the unwind in opposite directions

Two independent findings on the same code path, both reproduced RED before the
fix. A mapped (fan-out) node has two outcomes in which it settles **without
dispatching any forward work**, and the unwind got each one wrong:

**(a) An invalid mapped input skipped the unwind entirely.** A mapped node whose
upstream output is not a JSON array is a deterministic, pre-dispatch *input
shape* rejection. It returned `Err(...)`, which `activity_result?` propagated
straight out of the level loop — **past the terminal-failure check**, and
therefore past the unwind. A compensable upstream that had already succeeded was
left un-rolled-back, which is precisely the dangling-side-effect state this
feature exists to prevent.

**(b) An empty mapped fan-out was compensated for work that never ran.** A
mapped node over an *empty* upstream array dispatches zero instances and settles
`Succeeded` vacuously (the pre-existing `n_instances == 0` guard). The unwind's
only test was "did this node succeed?", so it dispatched that node's compensator
— undoing an effect that was never produced.

**The fix is one coherent change**, because the two outcomes are the same shape
(settled, no dispatch) read from opposite ends. Each level member's dispatch
future now returns a five-field `NodeRun`
(`(task_idx, status, output, dispatched_forward, shape_failure_reason)`):

- **`dispatched_forward: bool`** — `false` for both non-dispatching outcomes,
  `true` for every node that actually ran something. `unwind_dag_compensations`
  skips any node whose forward pass dispatched nothing, so a vacuous success has
  nothing to undo. The four per-node parallel arrays it consults are bundled into
  a small `NodeUnwindState<'_>` struct.
- **`shape_failure_reason: Option<String>`** — the invalid-shape branch now
  reports an ordinary node **failure** instead of returning `Err`, so it routes
  through the normal terminal path (and therefore the unwind), while the precise
  diagnostic is carried out-of-band and still surfaces as the caller-visible
  error. This deliberately preserves the exact `"not a JSON array"` message
  `dag_signal_gate_tests` pins, rather than collapsing it into the generic
  `"one or more DAG tasks failed"`.

Genuine replay / non-determinism errors are untouched: they still propagate
directly via `?` and never trigger an unwind. That distinction is load-bearing —
unwinding from a diverged cursor is exactly the P1-B nd-block failure fixed
above.

RED evidence (before the fix):

```
a_non_array_mapped_input_still_unwinds_the_succeeded_upstream
  left: []                                  right: ["m_undo_root"]

an_empty_mapped_fan_out_is_never_compensated
  left: ["m_undo_process", "m_undo_root"]   right: ["m_undo_root"]
```

Pinned by those two tests plus the `mapped_empty_comp_dag` /
`mapped_non_array_comp_dag` fixtures in `dag_compensation_tests.rs`.

### P1 — a deterministic pre-dispatch rejection escaped past the unwind

The third finding of the same class as the two above, and the one that forced
the ad-hoc handling to become a stated rule. `execute_activity_raw_with_opts`
rejects an oversized activity input with `HarvestError::PayloadTooLarge`
**before** it allocates an activity id, pushes a `ScheduleActivity` command, or
records any event. The node future preserved that as `Err`, so
`activity_result?` exited `run_unified_dag` before the terminal-failure check —
any compensable upstream (or a succeeded sibling in the same joined level) kept
its side effect.

Rather than special-case a third error, the classification is now an explicit,
documented predicate:

```rust
/// Is this error a **deterministic pre-dispatch rejection** — one the engine
/// raises *before* it allocates an activity id, pushes any `WorkflowCommand`,
/// or records any event?
const fn is_deterministic_dispatch_rejection(error: &HarvestError) -> bool {
    matches!(error, HarvestError::PayloadTooLarge { .. })
}
```

Such an error leaves **no history footprint and no side effect** and is a pure
function of already-recorded state plus stable configuration, so reporting it as
an ordinary node **failure** is both safe and necessary: the DAG reaches its
terminal check and unwinds. Applied at all three dispatch sites (plain,
mapped-`FailFast`, mapped-`CollectAll`), with the precise diagnostic carried
out-of-band so it stays operator-visible.

The predicate is deliberately **narrow**. Everything else keeps propagating
directly, because mis-classifying it would unwind a run that was not actually
terminal:

* `NonDeterministic` — unwinding from a diverged replay cursor is exactly the
  permanent nd-block (issue #603) fixed as P1-B above.
* `Cancelled` — `docs/saga.md`: cancellation does not auto-compensate.
* Transient engine/storage errors — the workflow task is retried.

**`CollectAll` deliberately does not fold this into a cell.** A pre-dispatch
rejection is not a per-cell business failure, so it fails the *node*. Folding it
into the cells array would let the DAG **complete successfully**, silently
converting a cap violation into a success; failing the node preserves today's
outcome (the DAG failed before this fix too) and merely adds the unwind.

RED evidence (before the fix):

```
an_oversized_activity_input_still_unwinds_the_succeeded_upstream
  left: []   right: ["pc_undo_root"]
```

**Sizing caveat surfaced while writing the test** (now documented in
`docs/saga.md`): the compensation envelope embeds the compensated node's whole
resolved input *and* its whole output, so it is necessarily larger than the
node's own input. The first draft of the fixture hung the oversized value off
the compensated node itself, and the *compensator* was then rejected by the same
cap — surfacing as `SagaCompensationFailed` rather than a dispatched rollback.
That is correct, honest behaviour (continue-not-abort), but it means a
compensable node running close to the activity-input cap can have its rollback
rejected. The fixture was restructured to hang the payload off a
non-compensated node.

### P1 — the marker-less unwind guard was blind to a renamed compensator

A follow-on to the marker-less fix above, and the sharper half of it. The
retry endpoint hands the resolver the **currently registered** `DagDefinition`,
not the one that produced the history. So the name-based signal — "an
`ActivityScheduled` naming one of this DAG's declared compensators" — silently
stops matching the moment a deployment renames or removes that compensator. A
run that unwound *without a marker* (the drained-signal-frontier case) and whose
compensator has since been renamed therefore became retryable again, carrying
over nodes whose side effects were already rolled back: the exact double-spend
the guard exists to prevent, re-opened by an ordinary refactor.

The fix is a signal that is **durable and definition-independent**: the
compensation **envelope**. `unwind_dag_compensations` dispatches every
compensator with the reserved shape

```json
{"dag_compensate": "<compensated node>", "input": …, "output": …}
```

and that envelope is recorded verbatim in the dispatch's own
`ActivityScheduled.input`. Reading it consults no registry, so it survives any
rename, removal, or topology change.

`is_compensation_envelope` matches **structurally**, not on mere key presence:
exactly three keys (`dag_compensate` / `input` / `output`) with a string
`dag_compensate`. A forward node's input is either the `{conf, dag_task}`
wrapper, a raw bound upstream output, or a mapped cell — none of that shape. The
failure direction is safe regardless: a false positive makes a retryable run
non-retryable (start a fresh run), whereas a false negative is the double-spend.

The name-based signal is **kept** as defence-in-depth: it is unambiguous by
construction (`CompensatorNameCollidesWithNode`) and would still fire for a
history predating the envelope shape.

RED evidence (before the fix — a marker-less unwind whose compensator was
renamed to `undo_a_v1_RENAMED`):

```
left:  Ok(DagRetryPlan { reset_to_event_id: 2,
        nodes_to_re_execute: ["b", "c", "d"], nodes_carried_over: ["a"] })
right: Err(CompensatedRun)
```

Pinned by `resolve_rejects_a_marker_less_unwind_whose_compensator_was_since_renamed`,
the near-miss suite `compensation_envelope_predicate_accepts_only_the_real_envelope`
(2-key subset, 4-key superset, non-string `dag_compensate`, the forward
`{conf, dag_task}` wrapper, a mapped-cell array, `null`), and the false-positive
guard `resolve_allows_a_run_whose_forward_inputs_are_plain_objects`. The
cross-crate coupling is guarded from the other side too: the core suite already
pins the envelope's exact shape with an `assert_eq!`, so changing it fails there
loudly rather than silently blinding this detector.

### P1 — the retry endpoint must inflate history before classifying an unwind

The compensation envelope lives in `ActivityScheduled.input`, and `input` is a
**payload-bearing field**. With payload offloading (issue #524) enabled, an
oversized envelope is replaced wholesale by an `_harvest_offload_envelope`
reference before it reaches `harvest_events`; a codec-encrypting deployment
(issue #608) wraps it likewise. The retry endpoint loaded history with plain
`store::load_history`, so the structural predicate never saw the three
compensation keys.

On its own that is only a missed signal, but combined with the other two failure
modes it reopens the exact hole the previous fix closed: an unwind at a drained
signal frontier records **no marker** (signal 1 blind), a since-renamed
compensator defeats the name check (signal 3 blind), and an offloaded envelope
defeats the shape check (signal 2 blind) — so a fully rolled-back run becomes
retryable and resumes onto rolled-back state. Unlike the codec case, which fails
CLOSED (`load_history` hard-errors `UnknownPayloadCodec`), the offload case fails
**open** and silently.

Fixed at the call site, which is where the information exists:
`retry_dag_run_inner` now loads via `store::load_history_inflated` with the
runtime's real codecs and payload offloader.

This is deliberately **not** fixed in `resolve_retry_plan`. An offloaded input is
indistinguishable from a large ordinary forward input, so treating one as a
compensation signal would reject every legitimate retry of a big-payload
compensating DAG. The resolver's contract is therefore "you must hand me
inflated history", pinned by
`an_offloaded_compensation_envelope_is_invisible_and_must_be_inflated_by_the_caller`,
which asserts both halves: fed the offloaded form the rolled-back run wrongly
resolves `Ok` (the fail-open the endpoint prevents), and fed the inflated form it
correctly resolves `Err(CompensatedRun)`.

With no `PayloadStore` registered `load_history_inflated` delegates verbatim to
the inline path, so the default deployment is byte-for-byte unchanged; only
oversized payloads cost a blob fetch, on a low-frequency operator endpoint.
Passing the runtime's real codecs (rather than the identity default
`load_history` uses) additionally makes the endpoint work at all on a
codec-encrypting deployment, where it previously returned an error. No payload is
surfaced to the caller — the plan carries only node names and an event id.

### P2 — the envelope signal must corroborate against the dispatch name

`is_compensation_envelope` matched on shape alone (exactly the three keys
`dag_compensate`/`input`/`output`, with a string `dag_compensate`). That shape is
reachable by accident: a mapped cell or an `input_from` binding hands a node the
**raw upstream output**, which is arbitrary user data. A forward activity whose
input happened to carry those keys was therefore classified as a compensation
dispatch — 409-ing a perfectly retryable run, and doing so even for a DAG that
declares **no compensators at all** and so cannot possibly have unwound.

RED evidence (a non-compensating `a → b → c → d` DAG whose `a` input is
`{"dag_compensate": "looks_like_a_node", "input": 1, "output": 2}`):

```
a_forward_dispatch_mimicking_the_envelope_is_not_an_unwind
  panicked: a non-compensating DAG must stay retryable: CompensatedRun
```

Signal 2 now corroborates against the dispatch's **name**: an envelope-shaped
input counts only when the dispatch is *not* named after a declared node. Every
forward dispatch is named after a node, and
`DagBuildError::CompensatorNameCollidesWithNode` guarantees at build time that no
compensator shares a node's name — so the corroboration rejects every forward
dispatch while costing nothing.

Crucially it preserves the rename-resilience signal 2 exists for: a compensator
that was renamed, or **removed outright**, is still absent from the node set and
still detected. Pinned by
`a_genuine_unwind_is_detected_even_after_all_compensators_were_removed` (the
definition declares zero compensators, yet the marker-less unwind is still
rejected) alongside the existing rename test.

The envelope's `dag_compensate` *value* is deliberately left unchecked. It names
a forward node, so validating it against the definition would reintroduce exactly
the registry dependence signal 2 was introduced to escape.

### P1 — the envelope corroboration must be historical, not definition-based

The previous round corroborated the envelope signal against the **current node
set**: an envelope-shaped input counted only when the dispatch was not named
after a declared node. That closed the user-payload false positive, but
introduced a new blindness — `CompensatorNameCollidesWithNode` is a
*per-definition-version* guarantee. It stops a compensator from sharing a node's
name in the build that declared it; nothing stops a **later** build from
introducing a forward node named after a compensator that has since been renamed
away. The current-definition check then suppressed a genuine envelope.

RED evidence (current definition `a → undo_a → c` with `undo_a` now a forward
node and no compensator declared; history carries a marker-less unwind that
dispatched `undo_a`):

```
a_genuine_unwind_is_detected_even_when_a_later_node_reuses_the_compensator_name
  left:  Ok(DagRetryPlan { reset_to_event_id: 2,
          nodes_to_re_execute: ["c", "undo_a"], nodes_carried_over: ["a"] })
  right: Err(CompensatedRun)
```

`nodes_carried_over: ["a"]` is the double-spend — `undo_a` had already rolled
`a` back.

The corroboration is now drawn from **history only**: the envelope's
`dag_compensate` value must name an activity that actually **succeeded in this
run** (correlated `ActivityScheduled` → `ActivityCompleted` by `activity_id`,
since the completion event carries no name). The unwind only ever compensates
succeeded nodes, so a genuine envelope always satisfies it.

Signal 2 therefore has **no registry dependence at all**, and survives every way
the definition can drift from the run that produced the history — compensator
renamed, removed outright, or its name later reused as a forward node. All three
are pinned:

- `resolve_rejects_a_marker_less_unwind_whose_compensator_was_since_renamed`
- `a_genuine_unwind_is_detected_even_after_all_compensators_were_removed`
- `a_genuine_unwind_is_detected_even_when_a_later_node_reuses_the_compensator_name`

The user-payload false positive stays closed
(`a_forward_dispatch_mimicking_the_envelope_is_not_an_unwind`: the mimic's
`dag_compensate` names nothing that succeeded).

Residual, documented and safe-direction: a forward dispatch whose input is
exactly the three envelope keys *and* whose `dag_compensate` happens to name a
node that succeeded in the same run is still a false positive — which marks a
retryable run non-retryable (start a fresh run), never the double-spend.

### P2 — the FailFast policy table described cancellation it never performed

`docs/getting-started/08-dags-and-schedules.md` promised `FailFast` would "stop
execution and cancel in-flight instances on first failure". The cancellation half
was **never** true: a mapped cell is a durable `harvest_task_queue` row, so
abandoning the workflow's future never cancelled it. The stop-execution half
became inaccurate with this PR's P1-B fix, which drains the instance stream so
the replay cursor stays clean for the unwind.

The table now describes what actually happens — the first cell failure decides
the node's outcome and stops **downstream** work, while already-dispatched cells
are drained to completion — with a paragraph stating that `FailFast` is an
outcome policy, not a cancellation policy, and is not a tool for bounded failure
latency.

### P1 — an old forward node named like a *current* compensator faked an unwind

The name-only signal 3 (a dispatch whose *name* is a currently-declared
compensator) was the mirror of the name-reuse hole fixed above, in the other
direction: a run produced by an **older** definition, where `undo_a` was an
ordinary forward node, is resolved against a **current** definition that has
since introduced `undo_a` as a compensator. Signal 3 then reported
`CompensatedRun` for a run that never unwound at all, blocking a legitimate
retry with a permanent `409`.

Root cause is the same per-definition-version scope of
`DagBuildError::CompensatorNameCollidesWithNode`: it guarantees no compensator
shadows a node *within one build*, and constrains nothing across versions. Two
consecutive review rounds hit that boundary from opposite sides, so rather than
add a third name-keyed patch, **signal 3 was removed entirely**. Detection is
now two signals, both purely historical:

1. a `saga_compensat*` marker, or
2. a compensation envelope whose `dag_compensate` names a node that **succeeded
   in the same run**.

Signal 3 was redundant with signal 2 by construction — every unwind this engine
produces writes the envelope, and only succeeded nodes are ever compensated — so
dropping it loses no detection while removing the only remaining cross-version
false-positive vector. No name-keyed check against the current definition
survives in either direction.

Pinned by `an_old_forward_node_named_like_a_current_compensator_is_not_an_unwind`
(RED before the change: `CompensatedRun`); the marker-less regression test was
also rewritten onto a realistic envelope-bearing fixture via a new
`compensator_dispatch` helper, since the old one asserted on a history the
engine cannot actually produce (a `Value::Null` input on a compensator dispatch).

### P1 — dispatch rejections have no history footprint (documented, not fixed)

A `PayloadTooLarge` rejection is routed to the unwind as an ordinary node
failure (that is the earlier P1-A fix, which stops it from `?`-escaping past the
terminal check and stranding a succeeded compensable upstream). It leaves **no**
history footprint by design — no activity id, no command, no event — which is
precisely what makes that routing safe, and is also its one limitation: the
decision is re-evaluated on every replay against **live** configuration.

Raise the activity-input cap while a compensating DAG is mid-unwind and the
previously-rejected node now dispatches, colliding with the recorded
compensator's `ScheduleActivity`.

Assessed and **documented rather than fixed**, for three reasons:

1. The consequence is a #603 **nd-block** — the run stays `RUNNING`, retries with
   backoff, and recovers when the cap is reverted. Never a silent partial
   rollback.
2. It needs a four-way conjunction: `PayloadTooLarge` + a compensating DAG + a
   cap change + that change landing inside the unwind's decision-cycle window.
3. It is the same class as the pre-existing engine-wide
   `known_limitation_early_config_dependent_failure_does_not_replay_cleanly`
   (issue #601), pinned there with a plain non-DAG `spawn_child_workflow_raw`
   call. Issue #780 **enlarges** the surface rather than creating it: before the
   unwind existed a rejection sealed the run `FAILED` with no compensation
   events, so nothing could collide.

The durable fix is out of scope by construction: it means giving the *engine's*
activity-dispatch path a history footprint for deterministic pre-dispatch
rejections, so replay reads the decision back instead of re-deciding it. It
cannot be done inside the DAG runtime — a level dispatches concurrently through
`join_all`, so a marker pushed from inside a task future has no deterministic
position, and one pushed after the join is read too late to gate the dispatch.
That is an engine-wide change affecting every workflow.

Pinned by `dag_compensation_tests::known_limitation_raising_the_cap_mid_unwind_diverges`
(asserts the divergence surfaces as nd details, i.e. a recoverable block) and
documented in `docs/saga.md` plus the `is_deterministic_dispatch_rejection` doc
comment, with the operational guidance: do not change payload caps while
compensating DAG runs are in flight.

### P2 — the DAG run-graph read compensation dispatches as forward nodes

Issue #780 introduced a **new class** of `ActivityScheduled` event — the
compensator dispatch — that every name-keyed history reader now sees for the
first time. The retry resolver was hardened against name reuse across
definition versions in the previous rounds; the run-graph view (#690) was not,
and it shares the same readers.

Within one definition version `CompensatorNameCollidesWithNode` keeps a
compensator distinguishable from a forward node. Across versions it does not: a
later definition may introduce a **forward node** named after an older
definition's compensator. `GET /dags/{name}/runs/{id}` would then report that
never-executed node as succeeded/failed, with the compensation's timestamps.

Both name-keyed readers now skip a compensation dispatch, using the same
history-only corroboration the retry guard uses (an envelope whose
`dag_compensate` names a node that succeeded in the same run), so neither can
drift across versions:

- `dag_retry::node_outcome` — node status (feeds the run-graph view too);
- `dag_graph::latest_scheduled` — node timing, attempts, and error.

Fixing **both** is load-bearing: they are separate readers of the same
authoritative attempt, so excluding in only one would leave a name-reused node
reporting `pending` alongside the old compensation's timestamps. The shared
predicate is exported as `dag_retry::is_compensation_dispatch`.

RED first, at both levels:

- `node_outcome_ignores_a_compensation_dispatch_named_like_a_forward_node`
- `a_compensation_dispatch_is_not_read_as_a_same_named_forward_node`
  (run-graph level; asserts `status`, `started_at`, and `attempts` together)

The user-payload false positive stays closed by the same corroboration
(`node_outcome_still_reads_a_forward_node_whose_input_mimics_the_envelope`: the
mimic's `dag_compensate` names nothing that succeeded, so a genuine forward
dispatch is never swallowed).

### P1 — a `CollectAll` node whose cells all failed was not detected as compensated

The corroboration for signal 2 (and for the run-graph exclusion above) required
the envelope's `dag_compensate` to name a node that **completed**. That was too
strict, on a case the DAG's own status model creates:

a `CollectAll` mapped node folds each cell failure into the cells array and
never flips the node's `TaskStatus` — only a deterministic dispatch rejection
does. So a node whose cells **all** failed still settles `Succeeded` at the DAG
level and is genuinely compensated, while recording **no** `ActivityCompleted`
under its name. Both guards missed it: retry-from-node stayed available on a
rolled-back run (the double-spend, when the marker was also absent), and the
run-graph exclusion failed to skip the compensation dispatch.

Fixed by moving the bar from **completion** to **dispatch** —
`dispatched_activity_names` / `compensates_a_dispatched_node`. That is precisely
the unwind's own guard (`dispatched_forward`): a node is compensated only if it
dispatched forward work, and dispatching records an `ActivityScheduled` under
the node's activity name. So the corroboration now matches the invariant it was
always meant to encode, rather than a proxy for it. An empty mapped fan-out is
excluded for free — it dispatches nothing and is never compensated.

Still purely historical, so every cross-version drift mode stays closed.

Pinned by `a_collect_all_node_whose_cells_all_failed_is_still_detected_as_compensated`
(RED before the change: `Ok(DagRetryPlan { .. })`). The false-positive guards
still hold — their mimic envelopes name activities that were never dispatched.

Accepted widening, documented: a forward dispatch whose user input is exactly
the three envelope keys *and* whose `dag_compensate` names a node that was
dispatched-but-failed is now a false positive where it previously was not. Same
safe direction as before — it marks a retryable run non-retryable, never the
double-spend.

### Review round 5 — the run-graph readers must decode before they filter, and no reader may hold a pool slot across blob I/O

Two independent P2s on the same root: the compensation filter reads
`ActivityScheduled.input`, a **payload-bearing** field, and the surfaces that
consume it were not set up to see through a stored envelope.

**(1) Codec/offload deployments silently lost the run-graph exclusion.** Both
run-graph readers — the #690 API handler and the Vantage DAG detail page — load
raw history (`store::load_history_with_timestamps` explicitly leaves payload
fields undecoded). On a codec-encrypting (#608) or payload-offloading (#524)
deployment the filter was handed an opaque envelope, returned `false`, and a
never-executed forward node was reported with an older definition's compensation
status, timings, and attempts — exactly the defect the round-4 exclusion exists
to close, reopened for those deployments.

Fixed with one shared pass (`api::decode_graph_history`) used by both readers, so
they can never disagree about whether the filter is looking at real payloads:

- **Gated on a cheap structural scan** (`graph_history_needs_inflation`) of only
  the two fields the derivation reads — `ActivityScheduled.input` and
  `MarkerRecorded.details`. The identity codec's `encode_payload` is a
  pass-through, so a default deployment stores no envelope, the scan answers
  `false`, and the pre-#780 read path runs byte-for-byte unchanged.
- **Degrades rather than errors**: the run graph is read-only observability, so
  an inflate/decode failure logs and renders from the raw history instead of
  failing the page. The retry endpoint keeps the opposite posture — fail closed,
  because there a hidden envelope permits the double-spend.
- Surfaces no payload (node names, statuses, timings, attempts, and a truncated
  `error`, which is not payload-bearing), so it needs no #608 read-decode opt-in
  or audit row — the same rationale the awaitables endpoint's replay drive uses.

New core predicate `payload_codec::is_codec_envelope`, delegating to the existing
authoritative `codec_envelope_parts` shape check so a caller's "is this opaque?"
question cannot drift from what the decoder recognises. Its offload sibling
(`payload_store::extract_offload_ref`) was already public.

**(2) The retry endpoint held a DB connection across sequential blob fetches.**
`store::load_history_inflated` loads the rows and then fetches every offloaded
blob while still holding the connection it loaded them with, so a slow or
unavailable payload store — or a few concurrent retries over large histories —
could pin pool slots and stall unrelated API and worker work. The endpoint now
loads raw, releases the pool slot, inflates/decodes in memory, and reacquires a
connection for the preview/reset — the discipline the awaitables endpoint
already established. It still fails closed on any inflate/decode error.

Pinned by `a_codec_encoded_compensation_dispatch_is_only_recognised_after_decoding`
(asserts the predicate is blind on the raw stored history and correct after the
pass), `graph_history_needs_inflation_detects_opaque_payload_fields`, and two
end-to-end run-graph tests — a codec-encoded compensator named like a current
forward node is excluded (status `pending`, no timing, zero attempts) while an
ordinary codec-encoded forward dispatch still reads normally.

### Post-review hardening (round 6) — a bare unwind marker is not proof of rollback

An automated review found a **P1** in the compensated-run retry guard: signal 1
accepted a `saga_compensat*` marker on its own, but the marker is recorded
*before* anything is rolled back.

`Saga::run_compensations` calls `observe_saga_unwind_start` — which records
`saga_compensated:{seq}` — and only then enters the compensation loop. A
compensator can be rejected *pre-dispatch*: `execute_activity_raw` returns
`PayloadTooLarge` before `push_command(ScheduleActivity)` when the
`{dag_compensate, input, output}` envelope exceeds the activity-input cap and no
payload offloading is configured. A run whose sole compensator is rejected that
way therefore ends as `[…, saga_compensated:1, saga_compensation_failed:1,
WorkflowFailed]` — a marker, zero `ActivityScheduled` events, and the upstream
side effect still live.

The guard read that marker as "already rolled back" and returned `409
CompensatedRun`, telling the operator to start a fresh DAG run — which **repeats**
the still-live upstream side effect. The rejection was in the dangerous direction:
it refused the safe recovery and recommended the unsafe one.

Signal 1 now requires a `saga_compensat*` marker **followed by at least one
activity dispatch**. The check is positional (any `ActivityScheduled` after the
marker's index), never name- or payload-keyed, which is why signal 1 is *refined*
rather than dropped: an issue #495 payload erasure tombstones the envelope's
contents so signal 2 goes blind, and only signal 1 keeps a genuinely rolled-back
run non-retryable. Because a DAG compensator is always dispatched as an activity,
the marker always precedes it, and no forward dispatch can follow it (the unwind
runs only after the level walk returns), the refinement introduces no false
negative.

Pinned by `a_marker_only_unwind_that_dispatched_nothing_stays_retryable` (a
marker pair with zero dispatches resolves `Ok`) and by
`resolve_rejects_a_run_whose_compensation_failed_after_dispatching`, which was
strengthened to dispatch its compensator so it proves the rejection path for a
real rollback rather than for a bare marker.

**Final detection contract** (supersedes the two-signal lists in the earlier
rounds above, which record what each round found rather than the end state). A
run is treated as compensated iff **either**:

1. a `saga_compensat*` marker is followed by at least one `ActivityScheduled`, or
2. an `ActivityScheduled` carries a `{dag_compensate, input, output}` envelope
   whose `dag_compensate` names an activity **dispatched** in the same run.

Both read only the run's own recorded history — never the registered definition.
A partial unwind still trips signal 1: if any compensator dispatched, the `409`
stands even when a later one is rejected. Only an unwind that dispatched
*nothing* stays retryable, which is precisely the case where nothing was rolled
back.

### Post-review hardening (round 7) — a PII-erased run is refused outright

An automated review found a **P1** at the intersection of two features neither
round had considered together: **issue #495 payload erasure** and the
**drained-signal-frontier** unwind (issue #801).

Erasure tombstones every payload field, so on an erased history
`ActivityScheduled.input` is `{"_harvest_erased": true}` and **signal 2 is
blind**. Signal 1 normally still holds — a marker's *name* is not a payload
field, and the round-6 positional "dispatch after the marker" check reads no
payload — but a run that unwound at a drained signal frontier recorded **no
marker at all**, leaving signal 1 nothing to anchor on. On that intersection a
fully rolled-back run looked retryable, and the retry would carry over upstream
nodes whose side effects had already been undone. Erasure is irreversible, so no
amount of inflating or decoding recovers the evidence.

The round-6 changelog and docs claimed the positional check "survives payload
erasure" without qualifying it — accurate only when a marker exists. Corrected in
`docs/saga.md`.

`retry_dag_run_inner` now refuses an erased source run before it resolves
anything, with a `409` naming erasure as the blocker. The check is
`erase::execution_input_is_erased(&execution.input)` — O(1), and the same guard
the issue #612 terminal-query path uses (erasure always tombstones the execution
row's own `input` column first).

**The refusal is correct independently of compensation.** The issue #148 fork
carries over the upstream events, whose `output` fields are now tombstones, so a
re-executing downstream node with an `input_from` binding (issue #702) would be
handed `{"_harvest_erased": true}` as its input. Retry-from-node on an erased run
produces garbage whether or not it ever compensated.

The cost is a safe-direction false positive: an erased run of a DAG that declares
no compensators is refused too. Deliberate — "the current definition declares no
compensators" is exactly the cross-version registry signal earlier rounds removed,
since the definition that produced the history may have declared them.

Pinned by `retry_erased_run_is_conflict` (erases a seeded FAILED run through the
real `erase_workflow_payloads`, asserts the `409` names erasure and a fresh run,
and asserts no fork was created) and by the characterization test
`an_erased_marker_less_unwind_is_invisible_and_must_be_refused_by_the_caller`,
which proves the resolver genuinely cannot see an erased unwind — so the guard
must live in the caller, which holds the execution row.
