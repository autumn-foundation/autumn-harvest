# Saga + Cancellation Semantics and Idempotency Contract

`Saga` (`autumn-harvest/src/saga.rs`) composes multi-step distributed
transactions with explicit LIFO compensation.  This document specifies the
interaction between `Saga` and the workflow cancellation primitive, and the
idempotency invariants that compensation activities must satisfy.

---

## Cancellation interaction

### Semantic chosen: cancellation does NOT auto-compensate

When an operator calls `cancel_workflow_execution`, a `WorkflowCancelled`
event is appended to the execution's event history.  On the next worker
pick-up, the executor replays the workflow function with a `WorkflowContext`
where `ctx.is_cancelled()` returns `true`.

**The `Saga` struct never observes this.**  It holds a plain `Vec` of pending
compensation closures and has no visibility into `WorkflowContext` state
during forward execution.  Cancellation therefore does **not** trigger
automatic compensation.

### Rationale

This matches Temporal's well-documented model and avoids two classes of
surprising behaviour in long-running sagas:

1. **Implicit partial-unwind surprise.** In a ten-step saga, automatic
   compensation after step 4 may be worse than no compensation: steps 5–10
   never ran, so there is nothing to undo for them.  Forcing the author to
   call `compensate_all()` explicitly makes the decision visible.
2. **Compensation-as-side-effect.** Compensation activities can be expensive
   (refund calls, seat releases, inventory restores).  Triggering them silently
   on every cancellation—including operator test cancels or retry-storm
   mitigations—creates unpredictable cost.

### Recommended pattern

Observe `ctx.is_cancelled()` in the workflow function and call
`saga.compensate_all()` explicitly:

```rust
#[workflow]
async fn checkout(ctx: &WorkflowContext, order: Order) -> Result<(), String> {
    let mut saga = Saga::new(ctx);

    let charge_id = saga
        .step(
            || ctx.execute_activity_raw("charge_payment", &order, "payments"),
            |charge_id| ctx.execute_activity_raw("refund_payment", &charge_id, "payments"),
        )
        .await
        .map_err(|e| e.to_string())?;

    let reservation_id = saga
        .step(
            || ctx.execute_activity_raw("reserve_inventory", &order, "inventory"),
            |rsv_id| ctx.execute_activity_raw("release_reservation", &rsv_id, "inventory"),
        )
        .await
        .map_err(|e| e.to_string())?;

    // ── Check for cancellation before committing ──────────────────────
    if ctx.is_cancelled() {
        saga.compensate_all()
            .await
            .map_err(|e| e.to_string())?;
        return Err(ctx.cancellation_reason().unwrap_or("cancelled").to_string());
    }
    // ─────────────────────────────────────────────────────────────────

    ctx.execute_activity_raw("confirm_order", &(charge_id, reservation_id), "default")
        .await
        .map_err(|e| e.to_string())?;

    Ok(())
}
```

`ctx.check_cancellation()?` is a shorthand that returns `Err(HarvestError::Cancelled(…))`
without running compensations.  Use it for fast-exit points where no
compensations are registered yet.

### Known limitation — durable compensations cannot run after cancellation

Once the `WorkflowCancelled` event is recorded in history, a compensation
that dispatches an activity (`ctx.execute_activity_raw(...)` inside the
compensation closure, as in the example above) **cannot actually run**: the
cancel event has no workflow-command counterpart, so the compensation's
dispatch diverges (`expected ActivityScheduled(..), got WorkflowCancelled`)
and the unwind fails with `SagaCompensationFailed` instead of scheduling the
activity.  This is a **pre-existing engine limitation** — verified empirically
against pre-#801 builds, where the identical flow fails with the identical
error — and is consistent with cancellation being a *terminal seal*: the
engine will not schedule new durable work for a CANCELLED execution.

Consequences for the cancel-and-compensate branch specifically:

- **In-memory compensation closures run fine** after cancellation (the
  canonical pattern locked in by `saga_compensate_all_on_cancel_pattern`).
- A **durable** compensation in the cancel branch fails the unwind; the
  workflow observes `SagaCompensationFailed`, and the
  `harvest.saga.compensation_failed` page fires — correctly, since the state
  is genuinely dangling.  Remediation is manual reconciliation (or reset,
  issue #148), the same as any durable-compensation failure.
- Durable compensations in the **step-failure rollback path**
  (`rollback_after`, triggered by a forward step's error while the run is
  still live) are unaffected — the limitation is specific to unwinding after
  a recorded `WorkflowCancelled`.

Pinned by
`known_limitation_durable_compensation_after_recorded_cancel_fails_and_is_counted`
in `tests/integration/saga_tests.rs`.

---

## Idempotency contract

### Why compensations re-run

Compensation closures are re-registered on **every** workflow replay.  The
`Saga` struct is rebuilt from scratch each time the workflow function executes;
it has no persistent identity in `harvest_events`.  If a worker crashes after
`compensate_all()` has started but before it finishes, the next worker will:

1. Replay all forward steps (returning their results from recorded history).
2. Re-register all compensation closures via the same `Saga::step()` calls.
3. Call `compensate_all()` again, re-invoking all compensation closures from the top of
   the LIFO stack — including closures that already ran before the crash.

**Consequence: compensation activities must be idempotent.**

### Good pattern — release by ID

```rust
// Safe to call twice: the second call is a no-op when the reservation is
// already released.
|rsv_id: String| ctx.execute_activity_raw(
    "release_reservation",
    &rsv_id,      // ← specific, stable identifier from forward step result
    "inventory",
)
```

The `rsv_id` is sourced from the `ActivityCompleted` event recorded when the
forward step ran, so it is the same value on every replay.

### Anti-pattern — release most-recent

```rust
// Dangerous: the second invocation releases whichever reservation was
// created most recently at that moment — which may belong to a different order.
|_| ctx.execute_activity_raw("release_last_reservation", &(), "inventory")
```

On a replay after a crash, `release_last_reservation` would release a
reservation that was never part of this saga.

---

## Replay-determinism contract

The `compensate` closure in `Saga::step` receives the forward step's `T`
result.  When the forward step calls `ctx.execute_activity_raw(...)`, that
result is sourced from the recorded `ActivityCompleted` event on replay rather
than re-executing the activity.  **Do not place non-deterministic or
side-effecting logic directly inside the compensation closure body**; invoke
an activity via `ctx.execute_activity_raw(...)` instead, so that the
compensation itself is durable and replay-safe.

---

## Saga for DAGs — declarative node compensation (issue #780)

A `#[dag]` node can declare the activity that **undoes** it. On terminal DAG
failure the engine builds a `Saga` over the nodes that completed successfully
and unwinds them for you — no hand-written `#[workflow]` wrapper, no manual
`saga.step(…)` per node. Everything specified above (idempotency, LIFO order,
replay determinism, the observability counters) applies unchanged; this
section documents only what is DAG-specific.

### Declaring a compensator

Two builder methods, opt-in per node, at most one per node (last call wins):

| Method | Compensator name |
|--------|------------------|
| `.compensate(undo_fn)` | Derived from the fn item, exactly like `DagBuilder::activity` — a typo is a compile error, not a mid-unwind dispatch failure |
| `.compensate_named("undo")` | The given string (trimmed) — the escape hatch for a compensator whose fn item is not in scope (an activity behind a feature flag, or a macro-computed name) |

**`compensate_named` is name-based dispatch, not remote dispatch.** The named
activity must still be **registered with the builder**: plugin preflight fails
the boot for a DAG compensator that resolves to no registered activity, exactly
as it does for a forward node (see [Build-time guards](#build-time-guards)).
It buys you a name computed at build time — not a reference to an activity that
only exists on a remote/polyglot worker.

```rust
#[dag]
fn fulfillment(dag: &mut DagBuilder) {
    let reserve = dag.activity(reserve_inventory).compensate(release_inventory);
    let charge  = dag.activity(charge_payment).upstream(&reserve).compensate(refund_payment);
    let _label  = dag.activity(print_label).upstream(&charge).compensate(void_label);
}
```

One compensator activity may be **shared by several nodes** — the envelope's
`dag_compensate` field says which node it is undoing.

### What triggers the unwind, and what it covers

The unwind runs on the DAG's terminal failure check — the same condition that
produces `Err("one or more DAG tasks failed")`. A node is compensated **iff**
it BOTH reached `TaskStatus::Succeeded` AND declares a compensator:

| Node state | Compensated? |
|------------|--------------|
| Succeeded, compensator declared | **yes** |
| Succeeded, no compensator | no — nothing declared |
| Skipped (trigger rule) or skipped (`.condition(…)` returned false) | no — it never ran, so there is no effect to undo |
| Never reached (an upstream failed/skipped) | no |
| Failed — **even when it declares a compensator** | no — by the saga contract only a *successful* forward step has an effect to undo |
| Succeeded **vacuously** — a mapped node over an *empty* upstream array | no — zero instances dispatched, so there is no effect to undo |

That last row is the one non-obvious case: a mapped
(`.map_activity(…).over(&up)`) node whose upstream produced `[]` settles
`Succeeded` without dispatching anything. Compensating it would undo work that
was never done, so the unwind skips any node whose forward pass dispatched
nothing.

More generally, a **deterministic pre-dispatch rejection** — an error the engine
raises *before* it allocates an activity id, pushes a command, or records an
event — is reported as an ordinary node `Failed` rather than escaping the run,
so the terminal failure is reached and **the unwind still runs**. Two cases
qualify today: a mapped node whose upstream output is **not a JSON array**, and
an activity input that exceeds the configured
[payload cap](getting-started/07-reliability-knobs.md). Both leave no history
footprint and no side effect, and the caller-visible error still names the
precise cause rather than the generic DAG failure.

Errors that are *not* in that class keep propagating directly and never trigger
an unwind: a replay divergence (unwinding from a diverged cursor would
[nd-block](runbooks/nondeterminism-block.md) the run), a cancellation (see
above), and transient engine/storage errors (the workflow task is retried, so
the run is not terminal).

> **Sizing note.** The compensation envelope embeds the compensated node's whole
> resolved input *and* its whole output, so it is necessarily larger than the
> node's own input. A node that runs close to the activity-input cap can
> therefore have its *compensator* rejected by that same cap — surfacing as
> `SagaCompensationFailed` rather than a dispatched rollback. Keep compensable
> nodes' payloads well clear of the cap, or offload large values (register a
> `PayloadStore`, issue #524) so the envelope carries a reference instead.

### Order — reverse topological (LIFO)

Compensations are pushed in the DAG's own forward order (levels forward,
ascending index within a level) and the `Saga` unwind pops them LIFO, so an
undo never runs before the undo of a node that depended on it. This is
deterministic by construction: the push order is a pure function of the DAG's
level structure, not of completion timing.

For the diamond `a → {b, c} → d` where `d` fails, the push order is `a, b, c`
and the dispatch order is `comp_c, comp_b, comp_a`.

### The compensator envelope

Every compensator receives one fixed shape:

```json
{
  "dag_compensate": "charge_payment",
  "input":  { "…": "the node's resolved forward input" },
  "output": { "charge_id": "ch_abc123" }
}
```

* `dag_compensate` — the compensated node's `activity_name`. One generic
  compensator can therefore serve N nodes by branching on this field.
* `input` — the node's **resolved forward input**, in one of four shapes:

  | Node kind | `input` shape |
  |-----------|---------------|
  | Unbound | the `{ "conf": …, "dag_task": … }` wrapper the forward dispatch used |
  | `.input_from(&up)` (issue #702) | the raw upstream output, verbatim |
  | `.input_from_all(…)` / `.input_from_aliased(…)` | the **keyed object** the binding produced (e.g. `{"extract": …, "enrich": …}`) — *not* any single upstream's raw output |
  | Mapped (`.map_activity(…).over(&up)`) | the **whole** mapped upstream array (node granularity — never one cell's item) |

* `output` — the node's recorded output, sourced from its `ActivityCompleted`
  event.

**Payload caps apply to the envelope.** The envelope embeds the node's whole
resolved input *and* its whole output, so for a mapped node over a large array
it can be substantially larger than either. It is dispatched through the
ordinary activity lowering, so the issue #252 activity-input cap applies to the
**envelope**, not to the node's original input — a node whose forward input sat
just under the cap can still produce an over-cap compensation envelope.

**Compensate by ID, read out of `output`.** The same idempotency contract as
the rest of this document applies verbatim: compensations re-run wholesale on
replay, so `release_inventory(rsv-9001)` (an id read from `output`) is safe
and `release_most_recent_reservation()` is not.

### Queue inheritance

A compensator is dispatched on the **compensated node's** queue string, so an
undo lands on the same worker pool that performed the forward step. A node
with no `.queue(…)` yields the empty-string queue, which the worker resolves
to the *compensator activity's own* `default_queue` (falling back to
`"default"`) — exactly as an unqueued forward node resolves.

The node's `.retry(…)` / `.start_to_close(…)` overrides are deliberately
**not** applied to the compensator: they describe the forward step's failure
budget, not the undo's. The compensator activity's own `#[activity(…)]`
defaults apply.

### Failure semantics

* A **successful** unwind still returns the original
  `Err("one or more DAG tasks failed")` — compensation is not an outcome
  change, it is a cleanup.
* A **failing** compensator does not abort the unwind: every remaining
  compensation is still attempted (continue-not-abort, the same contract as
  `compensate_all`), and the run then surfaces a stringified
  `HarvestError::SagaCompensationFailed` carrying both the original DAG error
  and every compensation error.

### Observability — DAG unwinds feed the same counters

A DAG unwind rides the **same** `saga_compensated:{seq}` /
`saga_compensation_failed:{seq}` dedup markers and the **same**
`harvest.saga.compensated` / `harvest.saga.compensation_failed` counters
described in the observability section below, including the starter alerts
`harvest_saga_compensation_spike` and `harvest_saga_compensation_failed`.

**Operator note:** those counters now include DAG rollbacks as well as
hand-written sagas. The two are distinguished by the `workflow` label — a
unified DAG's shadow `WorkflowInfo` carries the DAG's own name (issue #256),
so `harvest.saga.compensated{workflow="fulfillment"}` is a DAG unwind.

### Cancellation — a cancelled run does not unwind

Consistent with "cancellation does NOT auto-compensate" above, a **cancelled**
run skips the DAG unwind entirely and returns the original DAG error: zero
compensator dispatches, no `saga_compensated:` marker, no saga metric.

Beyond consistency, this is also load-bearing: a recorded `WorkflowCancelled`
has no workflow-command counterpart, so dispatching a compensator into it
would diverge (`expected ActivityScheduled(..), got WorkflowCancelled`) and
nd-block (#603) a run the operator has already cancelled.

**Accepted edge:** a cancel landing *mid-unwind* terminates the run FAILED
without running the remaining compensators. This is the same class as the
durable-compensation-after-cancel limitation documented above — the engine
will not schedule new durable work for a cancelled execution. The unwind is
guaranteed to terminate (never nd-block), but the remaining state is
genuinely dangling and needs manual reconciliation (or reset, issue #148).

### Known limitation — compensators are invisible to the DAG topology surfaces

A compensation is recorded as an **ordinary activity** (that is the whole point:
no new `WorkflowEvent` variant), and it is not a declared DAG node. So it is
visible in the raw event history and the execution timeline, but **not** as a
node in:

* the DAG run-graph view (`GET /dags/{name}/runs/{id}`, issue #690) — its
  topology comes from the registered `DagDefinition`, which carries forward
  nodes only;
* `dag_export` / any other definition-derived rendering.

To see whether a run unwound, read its history (or the `saga_compensated:{seq}`
marker) rather than its graph.

### Known limitation — a stray signal silences unwind observability

The `saga_compensated:{seq}` marker match is deliberately conservative about a
**drained signal frontier** (issue #801): when the recorded history ends in
un-awaited signals at the point the unwind begins, the unwind is left
*uncounted* — no marker, and neither saga counter fires.

A DAG consumes no signals of its own (a [signal gate](getting-started/08-dags-and-schedules.md#signal--approval-gates)
consumes exactly the gate's own signal), so **any unsolicited signal delivered
to a DAG run** can put it in that state. The compensations still run, and still
replay deterministically — only the observability is lost for that run. Do not
send ad-hoc signals to DAG executions.

This is an **observability** gap only. Anything that must be *correct* in the
presence of a marker-less unwind does not rely on the marker: the
[retry guard](#retrying-a-compensated-run-issue-366-interaction) also detects a
recorded compensator dispatch, so a marker-less rolled-back run is still
correctly refused a retry.

### Known limitation — compensation is node-granular

Compensation operates on **nodes**, never on individual mapped cells
(selective/partial compensation is out of scope for this slice). Two concrete
consequences for a [mapped node](getting-started/08-dags-and-schedules.md#dynamic-task-mapping-fan-out):

* A **`CollectAll`** mapped node that reaches `Succeeded` *with some failed
  cells* is compensated **once**, with the full cells array — failed cells
  included — as the envelope's `output`. The compensator must decide per cell
  what actually needs undoing.
* A **`FailFast`** mapped node that a single failed cell drove to `Failed` is
  **not compensated at all**, so the side effects of the cells that *did*
  succeed before the failure are left **uncompensated**. If a mapped node's
  cells commit real side effects, prefer `CollectAll` plus a
  cell-aware compensator, or make each cell self-compensating.

### Build-time guards

Every misuse below is rejected before a single node runs, rather than
surfacing mid-unwind when the state is already dangling:

| Error | Rejected because |
|-------|------------------|
| `DagBuildError::CompensateOnGate` | A [signal gate](getting-started/08-dags-and-schedules.md#signal--approval-gates) dispatches no activity, so it has no side effect to undo |
| `DagBuildError::EmptyCompensator` | An empty/whitespace name would dispatch a nameless activity at exactly the moment the state is dangling |
| `DagBuildError::CompensatorNameCollidesWithNode` | A compensator sharing a **forward node's** identity (another node's name, the declaring node's own name, or a gate's signal name) would be indistinguishable from that node in recorded history, corrupting the name-keyed classification the DAG run graph (issue #690) and retry-from-node (issue #366) depend on |
| `HarvestBuilderError::DagCompensationRequiresUnifiedExecution` | A **classic** (non-unified) DAG has no unwind step, so the compensator would silently never run — the worst possible failure mode for an undo |
| `HarvestBuilderError::LocalActivityInDag` | A compensator is dispatched through the ordinary DAG activity-queue lowering, so a `local = true` activity is just as invalid there as it is for a forward node (the error names the *compensator*) |

Plugin **preflight** additionally flags a compensator that names an
**unregistered** activity (`dag '…' references unregistered compensator '…'
for task '…'`), so a missing compensator is caught before rollout rather than
mid-unwind.

### Rolling back to a pre-#780 build — drain in-flight unwinds first

Rolling the *engine* back while a DAG run is **mid-unwind** silently truncates
that unwind. This is NOT the usual nd-block-and-roll-forward rule, so it is
worth stating precisely:

* A pre-#780 `run_unified_dag` returns `Err("one or more DAG tasks failed")` at
  the terminal-failure check **before consuming anything** — there is no unwind
  step to reach the recorded compensation events.
* `saga_compensated:{seq}` is an issue #801 marker that the old matcher already
  knows, so it is not an "unknown marker" that would trip replay either.

The result: the old build **seals the run terminally FAILED, with a partial
unwind and no error signal at all** — the compensations already dispatched
stay applied, the ones still pending never run, and nothing nd-blocks to tell
you. The dangling state must be reconciled manually (or via reset, issue #148).

**Operator rule:** before rolling back past #780, drain in-flight *compensating*
runs (a DAG run in a terminal-failure unwind is short-lived), or roll forward
instead. Alert on `harvest.saga.compensated` going quiet while
`harvest.workflow.terminal{outcome="failed"}` does not.

Runs that never unwind are unaffected: the saga is reached only on the failure
branch, so a DAG that succeeds is byte-identical to a pre-#780 build.

### Retrying a compensated run (issue #366 interaction)

A DAG run that executed its unwind is **not retryable from a failed node**.
`POST /dags/{name}/runs/{id}/retry` rejects it with `409 Conflict`:

> this run already executed its compensation unwind, so its succeeded nodes'
> side effects were rolled back; retrying would resume on rolled-back state —
> start a fresh DAG run instead

The reason is structural: retry-from-node deliberately **carries over** the
succeeded upstream nodes — which is exactly the set the unwind just undid. A
retry would therefore resume as if those side effects still existed and
double-spend the compensation.

Detection uses **two** independent signals — a `saga_compensat*` marker in the
run's history, **or** a recorded dispatch of one of that DAG's declared
compensator activities. The second is required, not redundant: an unwind at a
[drained signal frontier](#known-limitation--a-stray-signal-silences-unwind-observability)
records no marker at all, so a marker-only check would leave that fully
rolled-back run retryable. It is also unambiguous by construction —
[`CompensatorNameCollidesWithNode`](#build-time-guards) forbids a compensator
from sharing any forward node's name, so a compensator dispatch can never be
mistaken for a forward step.

A DAG run that failed **without** compensators (and every pre-#780 history)
triggers neither signal and stays fully retryable.

### Divergence *inside* an unwind still nd-blocks

`Saga` collects every compensation error — including a
`HarvestError::NonDeterministic` — into `compensation_errors`, so a genuine
code-drift divergence during an unwind surfaces as `SagaCompensationFailed`.
That is the *author-visible* result; the *engine* result is different and takes
precedence: the divergence also sets the run's non-determinism details, so the
worker nd-blocks it under issue #603 rather than sealing it FAILED.

That is the correct outcome — a divergence during an unwind is a deploy problem,
not a dangling-state problem, and #603's block is non-terminal and recoverable by
rolling the workflow code back. Do not read a `SagaCompensationFailed` in the
logs as "the unwind ran and failed" without checking whether the run is
nd-blocked (`GET /workflows?nd_blocked=true`).

---

## Observability (issue #801)

Compensation is a first-class, alertable signal. Two engine counters are
emitted from inside `Saga::run_compensations` (the single choke point both
`compensate_all()` and automatic step-failure rollback funnel through):

| Metric | Fires | Labels |
|--------|-------|--------|
| `harvest.saga.compensated` | Exactly once per **real compensation sequence** — a non-empty unwind actually running forward, counted at unwind start | `workflow`, `queue` |
| `harvest.saga.compensation_failed` | Exactly once per unwind that finishes with ≥1 compensation error (`HarvestError::SagaCompensationFailed` — the dangling-state case) | `workflow`, `queue` |

`execution.id` is never a label (ADR-0001 §7). A saga that never compensates
emits nothing; an empty `compensate_all()` is not a sequence.

**Exactly-once across replays.** Compensations re-run wholesale on every
replay (see the idempotency contract above), so the counters cannot simply
fire per call. Each unwind is keyed to durable `MarkerRecorded` dedup
markers reusing the existing event variant — `saga_compensated:{seq}`
recorded at unwind start (persisted in the same command batch as the first
compensation's own dispatch, so a crash at any point mid-unwind resumes
silent) and `saga_compensation_failed:{seq}` recorded at failed-unwind end.
The counter fires only when its marker is first recorded on the live
frontier; every replay observes the marker and stays silent. **No new
`WorkflowEvent` variant and no migration** — the markers are opaque names on
`MarkerRecorded`, exactly like `fan_out:{n}` / `race:{seq}` / `patch:{id}`.

**Backward compatibility.** A pre-#801 history (no saga markers) replays
untouched and uncounted: the marker matcher's `Absent` arm never moves the
cursor, so the recorded events still match the compensation's own commands —
never a divergence, never a retroactive count.

**In-Saga emission (not the worker terminal boundary).** The failure counter
fires even when the workflow author catches `SagaCompensationFailed` and the
run goes on to COMPLETE, and the compensated counter fires for
cancel-and-compensate unwinds in runs that never terminally fail — both
cases a terminal-boundary emission would structurally miss. It is therefore
fully separable from `harvest.workflow.terminal{outcome=failed}`.

**Per-unwind coherence (post-review hardening).** The unwind's disposition
(counted or not) is resolved **once**, at unwind start, and the failure
counter follows it — so the pair can never disagree about one logical
unwind, and the invariant **`failed ≤ compensated`** holds per unwind (ratio
dashboards never divide by zero or exceed 100%). Two concrete consequences:

- A *counted* unwind's failure is always counted, **including past a
  trailing un-awaited signal** at the failed-unwind end (e.g. a retried
  cancel webhook ingested at the final unwind cycle's wake) — the failure
  marker is recorded past the drained signal, replay-consistently.
- The **cancel-and-compensate pattern is counted**: a trailing
  `WorkflowCancelled` has no workflow-command counterpart and never leaves
  the replay cursor, so the marker matcher treats it as transparent — the
  unwind of a freshly-cancelled run is the live frontier. In a
  **`WorkflowReplayer` strict/canary probe** the byte-identical history
  shape (a pre-#801 marker-less *terminal* cancelled run) is a pure read,
  not a live unwind: the observe layer suppresses the frontier arms in probe
  contexts, so old cancelled histories are never counted retroactively and
  the strict replayer never sees a fresh marker command.
- **Cancel + durable compensation** (round-3 review): the marker
  transparency is saga-marker-local — `match_activity` still (and
  deliberately) diverges on the unconsumed `WorkflowCancelled`, so a
  cancel-branch unwind whose compensations are activity-backed fails at its
  first dispatch (the pre-existing limitation documented in the
  cancellation section above; baseline-identical on pre-#801 builds). Such
  an unwind counts **both** `compensated` (it began) and
  `compensation_failed` (it failed) — the truthful pair, and precisely the
  dangling-state page the failure counter exists for. The counted-but-
  cannot-proceed combination is therefore not a metric bug: the alert that
  fires is the correct one.

**Accepted edges** (documented deliberately):

- An unwind entered while unconsumed non-marker, non-cancellation events sit
  at the replay cursor is conservatively **uncounted as a whole** (both
  counters) — for a metrics-only feature, a silent under-count of a rare
  edge beats any chance of recording a marker at a wrong history position.
- **Signal-with-start caveat** (inherited from `ctx.patched()`): an unwind
  entered at a drained-signal frontier — canonically a signal-with-start run
  whose unwind begins before the staged signal is awaited — is
  conservatively **uncounted as a whole**, deterministically on every
  replay. The failure counter follows (never `failed=1` with
  `compensated=0`).
- **Crash window (at-least-once), both counters, durable and in-memory
  unwinds alike:** the sample is emitted in-process within the
  single-decision-cycle gap before its marker's batch commits (start marker:
  before the unwind's first dispatch batch persists; failure marker: before
  the post-unwind batch persists). A worker crash inside that gap re-emits
  on resume. A crash *mid-unwind* — between compensations, after the first
  batch committed — is exactly-once. A **pure in-memory** unwind (zero
  durable footprint) is the maximal case: a crash-resume anywhere within its
  single cycle can re-count — the metric mirror of the "compensations re-run
  wholesale" contract.
- **Pause race (at-least-once):** an operator pause (#383) committing while
  the unwind's first decision cycle is in flight discards the cycle's
  pending commands — including the freshly pushed marker — after the sample
  already fired; the re-derived cycle after resume emits again. Same class
  as the crash window.
- **Forward compatibility:** a history recorded by #801+ code does not
  replay under pre-#801 builds (the marker is unknown to the old matcher);
  a rollback nd-blocks (#603, non-terminal, recoverable) until rolled
  forward. Same rule as every marker feature.

Starter alerts `harvest_saga_compensation_spike` and
`harvest_saga_compensation_failed` ship in
`docs/alerts/starter-pack-v0.1.0.json` with runbook sections in
`docs/runbooks/harvest-alerts.md`.

---

## Test coverage

The integration tests in `autumn-harvest/tests/integration/saga_tests.rs`
lock in these semantics:

| Test | What it proves |
|------|----------------|
| `saga_cancellation_does_not_auto_compensate` | `Saga` leaves compensations pending when `ctx.is_cancelled()` is true; no automatic unwind occurs |
| `saga_compensate_all_on_cancel_pattern` | The recommended explicit cancel-and-compensate pattern works end-to-end; LIFO order is preserved — and (post-review) the unwind is counted, with its dedup marker recorded past the `WorkflowCancelled` event |
| `saga_compensation_idempotency_under_replay` | On a simulated second execution (replay after crash), all compensations re-run; by-ID compensations are safe, release-most-recent would produce double-effects |
| `saga_compensated_metric_emitted_once_across_crash_replay` | The `harvest.saga.compensated` counter reflects real compensation sequences, not the replay count — the durable marker suppresses re-emission on crash-resume (issue #801, AC3) |
| `saga_compensation_failed_metric_distinct_and_once` | The failure counter is a distinct signal, emitted exactly once and deduped across replays by its own marker |
| `saga_compensation_failed_emitted_even_when_author_catches_error` | An author-caught `SagaCompensationFailed` is still counted (in-Saga emission) |
| `saga_success_path_emits_no_saga_metrics_and_no_marker_commands` | A saga that never compensates emits nothing new (issue #801, AC6) |
| `pre_801_history_mid_unwind_replays_clean_and_uncounted` | A pre-#801 marker-less history — including one captured mid-unwind — replays without divergence and without emission |
| `trailing_unawaited_signal_does_not_suppress_the_failure_counter` | A counted unwind's failure fires exactly once even with a trailing un-awaited signal at the failed-unwind end (post-review P2-1) |
| `signal_with_start_shape_keeps_the_failed_counter_coupled_to_compensated` | An uncounted (drained-signal-frontier) unwind's failure stays uncounted — `failed ≤ compensated` holds per unwind (post-review P2-2) |
| `two_saga_unwinds_in_one_workflow_count_independently` | Two compensation sequences in one workflow allocate distinct seq markers and each count exactly once |
| `known_limitation_durable_compensation_after_recorded_cancel_fails_and_is_counted` | A durable (activity-backed) compensation after a recorded `WorkflowCancelled` fails at dispatch (pre-existing engine limitation, baseline-identical on pre-#801 builds) and the unwind truthfully counts both `compensated` and `compensation_failed` |

The declarative DAG unwind (issue #780) is locked in by
`autumn-harvest/tests/integration/dag_compensation_tests.rs`:

| Test | What it proves |
|------|----------------|
| `terminal_failure_compensates_succeeded_nodes_in_reverse_topological_order` | Over a diamond, compensators dispatch in exact reverse topological (LIFO) order, and a successful unwind returns the original DAG error unchanged |
| `compensator_receives_recorded_input_and_output` | The `{dag_compensate, input, output}` envelope carries the node identity, its resolved forward input, and its recorded output |
| `compensator_of_a_bound_node_receives_the_bound_input` | For an `.input_from(…)`-bound node (issue #702) the envelope's `input` is the raw upstream output, never the `conf`/`dag_task` wrapper |
| `skipped_and_never_reached_and_uncompensated_nodes_invoke_nothing` | Condition-skipped, trigger-rule-skipped, and compensator-less nodes dispatch nothing |
| `the_failed_node_itself_is_never_compensated` | A node that failed is never compensated, even when it declares a compensator |
| `mapped_node_compensation_is_node_granular_not_cell_granular` | A `CollectAll` mapped node is compensated once with the full cells array (failed cells included); a `FailFast` node driven to `Failed` is not compensated at all |
| `cancellation_does_not_auto_compensate` | A cancelled run dispatches zero compensators, records no `saga_compensated:` marker, emits no metric, and returns the original DAG error |
| `cancel_landing_mid_unwind_terminates_and_never_nd_blocks` | A cancel landing mid-unwind terminates with an error and leaves no deferred non-determinism error behind (which #603 would turn into a permanent nd-block) |
| `compensator_failure_surfaces_saga_compensation_failed` | A failing compensator does not abort the unwind; the run surfaces `SagaCompensationFailed` carrying both errors, records `saga_compensation_failed:1`, and fires the page counter exactly once |
| `compensation_is_recorded_as_ordinary_activity_events_only` | Compensation adds **no new `WorkflowEvent` variant** — ordinary `ActivityScheduled`/`ActivityCompleted` plus the single `saga_compensated:1` dedup marker |
| `compensator_dispatches_on_the_nodes_queue` | A compensator dispatches on the compensated node's queue; an unqueued node's compensator gets the empty-string queue, exactly like its forward dispatch |
| `success_path_emits_no_new_commands_and_no_saga_metrics` | A compensating DAG that succeeds constructs no saga at all: zero dispatches, zero markers, zero metrics |
| `fulfillment_dag_leaves_zero_uncompensated_side_effects_across_1000_runs` | **Success metric.** Across 1000 deterministically-seeded runs (two topologies × every failure position) a ledger nets to zero, the dispatch order is the exact reverse of the compensable succeeded prefix, and every produced history replays deterministically |
| `dag_compensation_suite_is_wired_into_ci` | The suite has a real CI run row in `.github/ci/integration-suites.txt`, so its guarantees actually execute |

Added by the post-review hardening pass:

| Test | What it proves |
|------|----------------|
| `failfast_mapped_node_failing_mid_array_compensates_and_never_nd_blocks` | **P1-B (blocker).** A `FailFast` mapped node whose failing cell is not the last one polled still unwinds cleanly: the compensator dispatches, the terminal error stays the original DAG error, `nd_details` is `None` (issue #603's nd-block gate), and the history replays deterministically |
| `failfast_mapped_node_compensates_for_every_failing_cell_position` | The randomized sibling: every failing-cell position over a 5-cell mapped node behaves identically (only the LAST position was covered before, and that is exactly the one that masked P1-B) |
| `unwind_order_is_reverse_topological_not_reverse_declaration_index` | The unwind follows EXECUTION LEVELS, not builder declaration order — pinned with a fixture that declares a child before its parent |
| `a_node_recovered_by_retry_compensates_nothing` | A node whose `.retry(…)` policy recovers it never reaches a terminal failure, so nothing is compensated |
| `a_node_that_exhausts_its_retries_unwinds_the_prefix_but_not_itself` | Retry exhaustion is a terminal node failure: the succeeded prefix unwinds and the exhausted node is not compensated |
| `a_compensator_inherits_no_retry_or_timeout_override_from_its_node` | The compensator command carries neither the node's `retry_policy_override` nor its `start_to_close_override` |
| `an_unwind_resumed_mid_flight_dispatches_only_the_remaining_compensations` | Crash recovery: a partially-recorded unwind resumes, replaying the already-dispatched compensator and issuing only the remainder, with no non-determinism and no double-count |
| `a_successful_unwind_fires_the_compensated_counter_once_with_labels` | `harvest.saga.compensated` fires exactly once per unwind (not per compensation) with the run's own `(workflow, queue)` labels |
| `two_failing_compensators_collect_both_errors_and_page_once` | Continue-not-abort with several failures: both errors are collected and the page counter still fires once |
| `failing_dag_without_compensators_emits_no_saga_commands_or_metrics` | The FAILURE path of an uncompensated DAG is unchanged: the empty unwind allocates no seq, records no marker, and emits no metric |

Build-time and preflight guards are covered by unit tests in
`autumn-harvest/src/dag.rs` (gate / empty / collision / shared-compensator /
last-wins / `compensate_named` trimming),
`autumn-harvest/src/builder.rs` (classic-DAG and local-activity rejections),
`autumn-harvest/src/saga.rs` (`push_compensation` registers without running;
`compensate_all_after` carries the caller's original error), and
`autumn-harvest-plugin/src/preflight.rs` (unregistered compensator). A worked
example with embedded self-checks lives in
`autumn-harvest/examples/dag_compensation.rs`.

---

## Out of scope

- `Saga::with_auto_compensate_on_cancel(true)` — separate issue if demand emerges.
- Cross-shard saga semantics — a `Saga` is scoped to a single workflow execution
  and therefore a single shard via `ExecutionId::shard()`.
- Saga + Update (#140) or saga + child-workflow interaction — separate specs.
