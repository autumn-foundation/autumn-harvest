# Workflows as analyzable artifacts — R&D spike report (issue #2010)

**Status: R&D spike, shipped in `autumn-harvest-verify`.** This report is the
"done when" of issue #2010. Design and planning record:
[`DESIGN-2010.md`](../../DESIGN-2010.md). User guide:
[`docs/harvest-verify.md`](../harvest-verify.md#saga-compensation-coverage).

**Verdict: go** for the step graph as data, and for model checks of user
workflows over it. The saga coverage check runs on the emitted JSON alone.
It found real gaps in two repository examples. The `unknown` rate of the
check is 4 of 71 example workflows (5.6 %). All four have no saga in sight
and a boundary that could hide one. On the 2 example workflows that use a
saga, the rate is 0. **No-go** for capability-typed determinism as an
allowlist guarantee in this form. §6 says why and gives the next step.

---

## 1. The question

Can `harvest-verify` emit each workflow's step, signal, update and
compensation graph as data? Can that graph support model checking of user
workflows?

The answer is yes, with explicit `unknown` boundaries. The spike shows it
in three parts:

1. The `--emit-structure` manifest of #1995 now has a **flow graph** per
   body and a **handler list** per workflow.
2. A **saga compensation coverage** check, `--check-structure FILE`, runs
   over the manifest. It reads no MIR.
3. This report measures both on every example in the repository.

## 2. What the graph holds

| Part | Source | Use |
|---|---|---|
| Bodies, call edges, digests | #1995 | Drift diff in the upgrade check |
| Step sites with kind and key | #1995 | Drift diff, history matching |
| Flow graph per body | #2010 | Event order: steps, calls, saga operations, exits |
| Handler list per workflow | #2010 | Signal, update and query entry points |
| Saga nodes: forward and compensation bodies | #2010 | Compensation coverage |

A flow node is an event. An edge joins two events when a control-flow path
joins them with no other event on it. The full node table is in the
[user guide](../harvest-verify.md#flow-graphs-and-handlers).

## 3. The property: saga compensation coverage

A forward step of a `Saga` that completed must be unwound on every failure
path. The check runs a forward fixpoint over each body that builds a saga.
The flag at a node says that a step may have completed and not been
unwound. An error exit of the workflow root with the flag set is a
**gap**.

- A tracked `saga.step(..).await?` labels its edges `ok` and `err`. The
  `ok` edge sets the flag. The `err` edge clears it, because a failed step
  unwinds every earlier step.
- An awaited `compensate_all` clears the flag.
- An exit whose value is not a literal `Ok(..)` counts as an error. A tail
  call result can be an error.
- A new saga over a pending step, or a helper that exits with a step
  pending, makes the verdict `unknown`.

## 4. Results on the examples

The spike ran over every example in the repository. That is the engine
examples (`-p autumn-harvest --all-examples`, feature `testing`) and the
six workspace example crates.

| Corpus | Workflows | With a boundary | Bodies | Step sites | Keyed steps with no key | Exits with `unknown` outcome | Handlers (named) | Saga steps (tracked) |
|---|---|---|---|---|---|---|---|---|
| Engine examples | 58 | 2 | 257 | 97 | 1 of 47 | 4 of 169 | 7 (7) | 0 |
| `standalone-runner` | 2 | 0 | 15 | 5 | 0 of 5 | 3 of 6 | 0 | 1 (1) |
| `billing-autumn-web` | 5 | 2 | 65 | 21 | 1 of 18 | 9 of 36 | 2 (2) | 5 (5) |
| `saga-choreography` | 2 | 0 | 4 | 4 | 0 of 3 | 0 of 7 | 0 | 0 |
| `quickstart` | 2 | 0 | 6 | 4 | 0 of 4 | 0 of 6 | 0 | 0 |
| `standalone-quickstart` | 1 | 0 | 1 | 1 | 0 of 1 | 0 of 2 | 0 | 0 |
| `claude-agent-daemon` | 1 | 1 | 20 | 2 | 0 of 2 | 2 of 16 | 0 | 0 |
| **Total** | **71** | **5** | **368** | **134** | **2 of 80** | **18 of 242** | **9 (9)** | **6 (6)** |

The check gives these verdicts:

| Corpus | `no-saga` | `covered` | `gap` | `unknown` |
|---|---|---|---|---|
| Engine examples | 56 | 0 | 0 | 2 |
| `standalone-runner` | 1 | 0 | 1 | 0 |
| `billing-autumn-web` | 3 | 0 | 1 | 1 |
| `saga-choreography` | 2 | 0 | 0 | 0 |
| `quickstart` | 2 | 0 | 0 | 0 |
| `standalone-quickstart` | 1 | 0 | 0 | 0 |
| `claude-agent-daemon` | 0 | 0 | 0 | 1 |
| **Total** | **65** | **0** | **2** | **4** |

### The `unknown` rate

| Measure | Value |
|---|---|
| Workflows whose graph has a boundary | 5 of 71 (7.0 %) |
| All workflows with an `unknown` coverage verdict | 4 of 71 (5.6 %) |
| Saga workflows with an `unknown` coverage verdict | 0 of 2 |
| Saga steps that are untracked | 0 of 6 |
| Keyed step sites with no key | 2 of 80 (2.5 %) |
| Exits with an `unknown` outcome | 18 of 242 (7.4 %) |

All four `unknown` coverage verdicts have the reason `boundary`. The
workflow has no saga in sight, and a boundary could hide one.

### The gaps are real

- `standalone_runner::workflows::standalone_order` reserves inventory in a
  saga step. Then `spawn_child_workflow(..).await?` can fail. That `?`
  returns with no unwind, so nothing releases the reservation.
- `billing_autumn_web::workflows::billing_checkout` has four such exits:
  the `payment_captured` signal wait, `record_payment_capture`, the
  `receipt-settlement-window` timer and `send_receipt`. Each `?` returns
  with the profile, the authorization, the subscription record and the
  invoice still in place.
- `billing_checkout` also has a `noop-compensation` note. The manual review
  step registers `|_| async { Ok(()) }`. That is a choice, not a defect,
  which is why it is a note.

The fixture `autumn-harvest-verify/tests/fixtures/saga_graph/` pins each
case: 24 workflows, with 7 `covered`, 5 `gap`, 9 `unknown` and 3 `no-saga`.

## 5. Where the analysis hits `unknown`, and why

| Cause | Where | Effect |
|---|---|---|
| `external-crate-body` | `futures_util` poll helpers in `collect_approvals`; a static and `to_vec` of another crate in `agent_session`; `panic_fmt` in `monthly_billing_cycle`; helpers in `claude-agent-daemon` | Boundary on the graph. No saga, so coverage is `unknown`. |
| `external-const` | `domain::OPS_QUEUE` in `billing_checkout`; module constants in `claude-agent-daemon` | Boundary for the drift diff. In `billing_checkout` the saga stays in view, so coverage is still decided. |
| Formatted step key | `ctx.timer(&format!(..))` | Key `null`. History matching cannot name the step. |
| Exit with a call result | `ctx.execute_activity(..).await` as the tail value | Outcome `unknown`, counted as an error. Conservative. |
| Saga passed to a helper or a capture | Fixture only | `saga-escapes`. |
| Step result not read by `?` | Fixture only (`match`, `map_err`, a prebuilt future) | `saga-result-untracked`. |
| A new saga over a pending step | Fixture only | `saga-recreated`. |
| A helper drops a pending saga | Fixture only | `saga-dropped-pending`. |

### Soundness across async state machines

Optimized MIR lowers each `async` body to a resume function. The graph
reads three facts from it:

- The resume function starts with a `switchInt` on its state. State `0` is
  the first entry. In the fixture, each other state jumps back into a poll
  loop that state `0` reaches. A test checks this for every fixture
  coroutine. No test proves it for every rustc.
- A suspend point returns `Poll::Pending`. That `return` is not an exit.
- `?` lowers to `Try::branch`, a `switchInt` with cases `0` and `1`, and
  `from_residual`. The walk after a saga call follows the value. The step
  result must reach `poll` through await plumbing, and `Try::branch` must
  read the `Ready` payload. Any other shape makes the step untracked.

A rustc change that removes the `Try::branch` shape moves a step to
untracked. The walk hard-codes the case values of `Poll` and
`ControlFlow`. A change to them is not detected. Run the fixture tests after
each toolchain bump.

### What the spike had to fix

The graph exposed four defects in the analyzer and the driver. Each defect
hid code or a whole target from the analysis:

1. A closure that returns an `async` block, given to a callee with no body
   (such as `Saga::step`), never had its `async` block analyzed. **Every
   step inside a saga closure was invisible**, to the graph and to the
   determinism verdict.
2. The type `{async fn body of Saga::step<.., {closure@..}>}` read as an
   unresolved callback. Each `saga.step(..).await` gave a false
   `unresolved-callback` boundary.
3. `switchInt` lost its case values in the parsed MIR, so no walk could
   tell `Ready` from `Pending`.
4. The driver could not find the MIR of a bin target. Cargo uplifts a bin
   from `deps/`, and the driver searched only the uplifted directory. So no
   workspace example crate could be analyzed.

### What the review found

Four review agents read the first version. They found real defects, and
each fix has a test:

- The first fix for item 1 indexed every `async` block body by its span. It
  turned three honest `unknown` verdicts into false `proven-deterministic`
  ones. The lookup now stays inside the closure that builds the block. A
  block that captures a `&mut` reference is a boundary.
- The `?` walk followed control flow only, so a `?` on another value could
  pass for a step failure. An unawaited `compensate_all()` cleared the
  flag. A new saga over a pending step read as `covered`. Each is closed.
- `Result<(), E>` exits read as `unknown`, which gave false gaps. That fix
  cut the `unknown` exits of the engine examples from 12 to 4.

### Generics and third-party crates

- The analysis reads a closure body once, with no substitution. It sees a
  saga inside a generic helper, but it does not specialize the helper per
  caller.
- The check finds the saga type by name: a type whose head is `Saga`. It
  does not see a saga in an `Option` or a struct field.
- A body in a crate outside the analysis is a boundary. A saga passed into
  it is a `saga-escapes`, because the call is not a `Saga` method.

## 6. The four uses

| Use | Status after the spike |
|---|---|
| Drift diff (#1995) | The same manifest. The flow graph adds event order, so a later diff can tell a reordered step from a changed one. |
| Model checking of user workflows | **Shown.** One property runs over the JSON alone. A signal race check can start from the handler list. It also needs the shared state that handlers write, which the graph does not hold yet. |
| Seeded fault injection for user workflows | Ready to start. Each `step` and `call` node is a fault point. A simulator can drive "fail at node k" paths through the graph and replay them. |
| Capability-typed determinism | **No-go in this form.** See below. |

**Capability-typed determinism.** The goal is an allowlist: the context is
the only source of time, randomness and I/O. The graph cannot prove that.
It holds commands, not every call. The MIR verifier is the right base, but
it trusts each `[[trusted]]` crate as a propagator. An allowlist mode would
turn that default around: each call outside an allowlist would be a
finding. The engine examples call into `std`, `serde_json`, `chrono` and
`tracing` on almost every path, so that list must be designed first. The
next step is that list, measured on the same examples.

## 7. Next steps

1. Fix the gaps in `standalone_order` and `billing_checkout`, or document
   them as deliberate.
2. Add `--check-structure` to the `harvest-verify` CI job for the examples.
3. A flow-graph diff in the upgrade check.
4. A signal race property over the handler list.
5. The allowlist design for capability-typed determinism.

## 8. Known limits

- The check covers the `Saga` builder only. DAG compensation runs in the
  engine, outside the graph.
- A panic is not an error exit. Unwind edges are not in the graph.
- A saga value passed straight from a coroutine state place as a call
  argument is not seen as an escape.
- The check trusts the `Saga` contract: a failed step unwinds every earlier
  step.
- The model version is now `2026.10.1`. Emit the two manifests of an
  upgrade check with the same `harvest-verify`.

## 9. Reproduce

```console
$ cargo build -p autumn-harvest-verify
$ B=target/debug/cargo-harvest-verify
$ $B harvest-verify -p autumn-harvest --all-examples --no-default-features \
    --features testing --emit-structure examples.json
$ $B harvest-verify -p standalone-runner --target-dir target/hv-runner \
    --emit-structure runner.json
$ $B harvest-verify -p billing-autumn-web --target-dir target/hv-billing \
    --emit-structure billing.json
$ $B --check-structure runner.json
$ $B --check-structure billing.json
```

Each example crate builds a debug binary of about 1 GB with MIR. Give each
crate its own `--target-dir`, and delete it after the run.

## 10. Test evidence

| Claim | Test |
|---|---|
| Each body has a flow graph | `saga_graph::every_body_has_a_flow_graph` |
| Steps inside saga closures are in the graph | `saga_graph::the_steps_inside_saga_closures_are_in_the_graph` |
| A saga future is not an unresolved callback | `saga_graph::a_saga_future_is_not_an_unresolved_callback` |
| Saga steps name their bodies and label their edges | `saga_graph::a_saga_step_names_its_forward_and_compensation_bodies`; `saga_graph::a_tracked_saga_step_labels_its_ok_and_err_edges` |
| Exits carry their outcome | `saga_graph::the_exits_carry_their_outcome` |
| Signal and update handlers are in the graph | `saga_graph::a_signal_handler_and_a_signal_wait_are_in_the_graph`; `saga_graph::an_update_handler_and_its_validator_are_in_the_graph` |
| Each resume target rejoins the state-0 graph | `saga_graph::every_resume_target_rejoins_code_that_state_zero_reaches` |
| Every fixture case gets its verdict | `saga_graph::every_fixture_workflow_gets_its_expected_verdict` |
| A back edge carries a pending step to an exit | `saga::tests::a_gap_reached_only_through_a_back_edge_is_found` |
| The `async` block follow never gives a false proof | `async_block_follow::a_case_the_analysis_cannot_follow_is_never_proven` |
| The check refuses an old manifest | `saga_graph::a_manifest_without_flow_graphs_is_refused`; `cli::check_structure_refuses_a_manifest_without_flow_graphs` |
| The CLI exit codes | `cli::check_structure_prints_a_verdict_per_workflow_and_fails_on_a_gap`; `cli::check_structure_strict_fails_on_unknown` |
| A bin target is analyzable | `driver::tests::an_uplifted_bin_is_resolved_through_its_hard_link_in_deps` |
