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
