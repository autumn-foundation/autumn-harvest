# Design — Issue #2010: workflows as analyzable artifacts

Issue #2010 asks one research question. Can `harvest-verify` emit each
workflow's step, signal, update and compensation graph as data? Can a model
checker then prove a property of *user* workflows over that data?

The spike answers with three parts:

- The structure manifest of #1995 gains a **flow graph** per body. It also
  lists the signal, update and query handlers of each workflow.
- A **saga compensation coverage** check runs over the manifest alone. It
  reads no MIR.
- A write-up, [`docs/rnd/workflow-graph-spike.md`](docs/rnd/workflow-graph-spike.md),
  gives the verdict and the `unknown` rate on the examples.

**No engine change. No migration. No new `WorkflowEvent` variant.** The
work is in `autumn-harvest-verify` only.

---

## 0. Planning record

### 0.1 Brainstorm — where can the graph come from, and what can check it?

| # | Idea | Verdict |
|---|------|---------|
| B1 | Emit the graph from the `#[workflow]` macro with `syn`. | Rejected. The macro sees one function. It cannot follow helpers, closures or other crates. |
| B2 | Extend the #1995 structure manifest. | **Adopted.** One artifact then feeds the drift diff and every model check. |
| B3 | Write a second file with `--emit-graph`. | Rejected. Two artifacts from one build can drift apart. |
| B4 | Store a condensed control-flow graph per body. A node is an event: a step, a call, a saga operation, a handler or an exit. An edge is a path with no other event on it. | **Adopted.** A property needs events and their order, not every MIR block. |
| B5 | Store the full MIR CFG. | Rejected. It is large, and MIR text is not a stable API. |
| B6 | Export the graph to TLA+ or Alloy (`formal/`). | Deferred. The spike runs an in-tree fixpoint. An export is a later step. |
| B7 | Find saga calls by callee path: `Saga::new`, `Saga::step`, `Saga::compensate_all`. | **Adopted.** The API is one engine type. It needs no model row. |
| B8 | Add a `[[saga]]` table to the model. | Rejected for the spike. Nothing else uses the same shape yet. |
| B9 | Follow the `.await?` lowering after `Saga::step`. The `Continue` arm is the `ok` edge. The `Break` arm is the `err` edge. | **Adopted.** Any other shape is `unknown`. |
| B10 | Run the check on the JSON: `--check-structure FILE`. | **Adopted.** It proves the graph is enough without MIR. |
| B11 | Capability-typed determinism: the context is the only source of time, randomness and I/O. | Out of spike scope. The write-up assesses it. |
| B12 | A race check between signal handlers and the main body. | Out of spike scope. The graph records each handler, so a later check can use it. |

### 0.2 Reverse brainstorm — how can the check say "covered" for a gap?

| # | How to make it lie | Mitigation |
|---|--------------------|------------|
| R1 | Return a `Result` held in a variable. No `Err(..)` literal shows. | An exit with no literal `Ok` or `Err` has the outcome `unknown`. The check treats it as an error. |
| R2 | Put `map_err` between `.await` and `?`. `Break` then comes from another value. | The walk follows the value from the step call to `Try::branch`. Any other call gives `saga-result-untracked`. |
| R3 | Pass the saga to a helper that steps or unwinds. | A `Saga` value that reaches a call or a capture other than a `Saga` method gives `saga-escapes`. |
| R4 | Use two sagas. An unwind of one clears the other. | Two or more `saga-new` nodes in one body give `multiple-sagas`. |
| R5 | The coroutine dispatch adds false paths or hides real ones. | The entry is the target of state `0`. In the fixture, each resume target jumps back into a poll loop that state `0` reaches. |
| R6 | Read an old manifest with no flow graph. Every workflow then reads as "no saga". | The manifest names its flow format. The check refuses a manifest without it. |
| R7 | A boundary hides the body that owns the saga. | A boundary that runs code gives `unknown`, with or without a saga in sight. An `external-const` runs no code. |
| R8 | Ignore the result: `let _ = saga.step(..).await`. | No `Try::branch` reads it, so the result is untracked. |
| R9 | A step sits in a loop, and a later iteration fails outside the saga. | The check is a fixpoint over cycles. |
| R10 | A compensation emits no command. | It is a `noop-compensation` note. Some steps have nothing to undo. |
| R11 | Build `compensate_all()` and never await it. | Only an awaited unwind clears the flag. |
| R12 | Start a new saga while a step of the old one is pending. | `saga-recreated`. |
| R13 | A helper owns a saga and returns `Ok` with a step pending. | `saga-dropped-pending`. |

### 0.3 Six thinking hats

| Hat | Notes |
|-----|-------|
| White | #1995 records bodies, call edges and step sites. Optimized coroutine MIR returns `Pending` at each suspend point. `?` lowers to `Try::branch` and `from_residual`. The engine examples hold 58 workflows. Two have boundaries. Two workspace examples use `Saga`. |
| Red | A check that finds gaps in the repository examples has value. A false gap costs trust fast. |
| Black | MIR text is not stable. A new rustc can change the `?` lowering. DAG compensation runs inside the engine, so the graph cannot see it. Third-party crates stay boundaries. |
| Yellow | One artifact. No engine change. The check is a small fixpoint. It finds real defects. |
| Green | The fault-injection simulator can drive the event graph. The handler nodes enable a signal race check. A TLA+ export can reuse the same JSON. |
| Blue | Red phase: fixture and failing tests. Green phase: flow graph, handlers, check, CLI. Refactor phase: docs, examples run, review. |

### 0.4 Corrections after the review

Four review agents read the first version. Each fix has a test.

1. The first version indexed every `Pin<&mut {async block}>` body by its
   span. That let three former `unknown` verdicts become false
   `proven-deterministic` ones. The lookup now stays inside the closure that
   builds the block. A block that captures a `&mut` reference is a
   boundary. Tests: `tests/async_block_follow.rs`.
2. The `.await?` walk followed control flow only. A `?` on another value
   could pass for the step result. The walk now follows the value.
3. An unawaited `compensate_all()` cleared the flag. Now only an awaited
   one does.
4. A new saga over a pending step, and a helper that drops a pending saga,
   each read as `covered`. They now give `saga-recreated` and
   `saga-dropped-pending`.
5. `Result::<(), E>::Ok(..)` read as an `unknown` exit, which gave a false
   gap. Generics are now removed before the cut.
6. A move of the saga into a coroutine state place read as an escape. A
   capture of a state place was not seen. The type annotation of the place
   now decides both.
7. The CLI accepted build flags with `--check-structure` and ignored them.
   They are now a usage error. `--strict` fails a manifest with no
   workflow. The manifest format is checked before the full parse.
8. The model version is now `2026.10.1`. The same MIR gives another
   manifest than before, so the upgrade check must not compare the two.
9. A closure can build a future of an untrusted crate with no call, as a
   unit struct. Its `poll` has no body here, so it is now a boundary. A
   same-named local impl no longer stands in for it.
10. The check ignored a node that the entry cannot reach. A saga or a gap
    there was silent. Such a graph is now refused.
11. A local `Saga` method that took the engine saga, such as
    `other::Saga::compensate_all`, read as the engine unwind. A saga
    method now needs the engine path, `Saga` or `autumn_harvest::..`, and
    no body here. Any other such call is a `saga-escape`.
12. A boundary counted only when no saga was in sight. A body outside the
    analysis can hold a second saga with a gap. Now each boundary that runs
    code gives `unknown`. An `external-const` does not count.
13. A call to a body that the manifest omits was not checked. That body
    could hold the only saga. Such a manifest is now refused.

---

## 1. The flow graph

`BodyNode.flow` holds the condensed control-flow graph of one body. The
manifest field `flow` names its format, `harvest-flow/1`. The upgrade check
ignores both fields, so the manifest format stays `harvest-structure/1`.

| Node kind | Fields | Meaning |
|---|---|---|
| `entry` | | The first block. For a coroutine, the target of state `0`. |
| `step` | `step` | A sink call site. The value is its index in `steps`. |
| `call` | `callees` | A call that starts other bodies of the graph. |
| `handler` | `handler` | A handler registration. The value is its index in `handlers`. |
| `saga-new` | | `Saga::new`, or another call that returns a saga value. |
| `saga-step` | `forward`, `compensate`, `tracked` | `Saga::step`, with the two closure bodies. |
| `saga-compensate` | `tracked` | `Saga::compensate_all`. `tracked` is true when the body awaits it. |
| `saga-escape` | `to` | A saga value reaches a call or a capture that is not a `Saga` method. |
| `exit` | `outcome` | A write of the returned value: `ok`, `err` or `unknown`. |

An edge has an optional `label`. Only a tracked `saga-step` labels its
edges, `ok` or `err`, and it has at least one of each. A step whose arm
reaches no node, such as an arm that never returns, is untracked. An exit
node has no out-edge. The check refuses a graph that breaks these rules,
names a missing node, has other than one entry, or has a node that the entry
cannot reach. It also refuses a call, a saga step or a handler that names a
body the workflow does not hold.

`WorkflowStructure.handlers` lists each handler registration: its kind
(`signal`, `update`, `query` or `other`), its name when the MIR shows it,
and its bodies.

## 2. Saga compensation coverage

The check reads one manifest. For each body with a `saga-new` node, it runs
a forward fixpoint. The state at a node is one flag: a forward step may have
completed and not been unwound.

| Node | Out-state |
|---|---|
| tracked `saga-step` | `ok` edges set the flag. `err` edges clear it, because the saga unwound. |
| untracked `saga-step` | Every edge sets the flag. |
| tracked `saga-compensate` | Every edge clears the flag. |
| any other node | The in-state. |

In the workflow root, an exit with the outcome `err` or `unknown` and a set
flag is a **gap**. In any other body, an exit with a set flag gives
`saga-dropped-pending`. A `saga-new` with a set flag gives
`saga-recreated`.

| Verdict | When |
|---|---|
| `no-saga` | No body has a saga node, and no boundary runs code. |
| `covered` | No gap, and no reason for `unknown`. |
| `gap` | At least one gap, and no reason for `unknown`. |
| `unknown` | `saga-escapes`, `saga-result-untracked`, `multiple-sagas`, `saga-recreated`, `saga-dropped-pending`, `no-flow-graph`, or a boundary that runs code. The report lists each gap as possible. |

A `noop-compensation` note does not change the verdict.

## 3. TDD plan and acceptance criteria

| AC (issue #2010) | Red test first |
|---|---|
| Emit the graph for a representative set of example workflows | `every_body_has_a_flow_graph`; `a_saga_step_names_its_forward_and_compensation_bodies`; `a_signal_handler_and_a_signal_wait_are_in_the_graph`; `an_update_handler_and_its_validator_are_in_the_graph` |
| Run one property (saga compensation coverage) over it | `every_fixture_workflow_gets_its_expected_verdict`; `a_plain_step_after_the_saga_is_a_gap`; `two_saga_steps_are_covered`; `an_unwind_that_is_never_awaited_is_a_gap` |
| Report where the analysis hits `unknown` | `a_saga_passed_to_a_helper_is_unknown`; `a_step_result_that_is_not_the_question_mark_operand_is_untracked`; `a_new_saga_over_a_pending_step_is_unknown`; `a_case_the_analysis_cannot_follow_is_never_proven` |
| A spike write-up under `docs/rnd/` with a go / no-go verdict and the `unknown` rate | `docs/rnd/workflow-graph-spike.md`; `the_write_up_states_a_verdict_and_an_unknown_rate` |

## 4. Known limits

- The check covers the `Saga` builder only. DAG compensation runs in the
  engine, outside the graph.
- A panic is not an error exit. Unwind edges are not in the graph.
- A saga in an `Option` or a struct field is not seen as a saga.
- A saga value passed straight from a coroutine state place as a call
  argument is not seen as an escape.
- The check trusts the `Saga` contract: a failed step unwinds every earlier
  step.
- The determinism model does not track a write through `&self` of an
  interior-mutable local, such as `Cell::set` or a write through
  `Mutex::lock`. This gap predates #2010. A followed `async` block that
  captures such a value, owned or shared, adds a boundary.
- MIR text is not a stable API. A rustc change that removes the
  `Try::branch` shape moves a step to untracked. The walk hard-codes the
  case values of `Poll` and `ControlFlow`. A change to them is not
  detected, so run the fixture tests after each toolchain bump.
