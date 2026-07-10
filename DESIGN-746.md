# Design — Issue #746: declarative DAG signal/timer gate nodes

Make a unified `#[dag]` able to **pause on a named signal** (with optional
timeout), lowering onto the existing #476 `receive_signal_timeout` /
`match_signal_or_timer` machinery. **No new `WorkflowEvent` variant, no
migration.**

## 1. Node model (core `autumn-harvest/src/dag.rs`)

A gate is a `DagTask` whose new `signal: Option<DagSignalGate>` field is `Some`.
A gate has **no activity dispatch**; its node identity (`activity_name`) is the
signal name, so it composes as an upstream / `.map_upstream` source and appears
in the graph by that name.

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GateTimeoutAction { FailRun, Continue }

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DagSignalGate {
    pub signal_name: String,
    pub timeout: Option<Duration>,
    pub on_timeout: GateTimeoutAction,  // ignored when timeout is None
}
```

`signal: Option<DagSignalGate>` is added to both `PendingDagTask` and the public
`DagTask` (+ `From` impl, + manual `Debug` for `PendingDagTask`). All other
per-task fields default (`AllSuccess` trigger, `queue = default_queue`, no
retry/condition/map).

### `DagBuilder` API — fluent, mirrors `.activity()` (DESIGN DEVIATION)

The handoff spec proposed `signal_gate(name, upstreams: &[usize]) -> usize`. The
real builder is fluent (`.activity(f).upstream(&ref)`), returning a
`DagTaskRef`. The prior worker's RED tests already assume the fluent shape, and
it is strictly more idiomatic and composable. **Adopted deviation:**

```rust
pub fn signal_gate(&mut self, signal_name: impl Into<String>) -> DagTaskRef
pub fn signal_gate_with_timeout(
    &mut self, signal_name: impl Into<String>,
    timeout: Duration, on_timeout: GateTimeoutAction,
) -> DagTaskRef
```

Returns a `DagTaskRef` usable as `.upstream(&other)`, as an upstream of a
downstream node, and as `.map_activity(f).over(&gate)`.

## 2. Level isolation (the highest-risk design point)

The worker's `should_requeue_signal_wait` requires **homogeneous** suspension
batches: a `WaitForSignal` command must not share a batch with a level's
`ScheduleActivity` dispatches. Therefore **a gate must occupy its own singleton
execution level.**

Implemented as a post-pass in `DagBuilder::build()` after Kahn levelling: only
when the DAG contains ≥1 gate, each Kahn level is split into
`[non-gate tasks (one level, if any)] ++ [each gate as its own singleton]`.
Independent same-level tasks re-sequence safely (they share dependency depth).
**When there are no gates the level vector is returned byte-for-byte unchanged**,
so every existing DAG test and replay is untouched.

## 3. Walker edit — SINGLE core helper (extraction chosen)

The macro emitted **two byte-approximate copies** of the level walker
(`workflow_handler_field` inlined into `DagInfo.workflow_handler`, and
`emit_workflow_companion`'s shadow `WorkflowInfo.handler`). They had **already
drifted**: the `workflow_handler_field` collect-all map branch swallowed *every*
error (incl. `NonDeterministic`) into a per-item `"failed"` cell, while
`emit_workflow_companion` propagated non-activity/non-timeout errors (matching
its own fail-fast branch). Extraction forces one behavior; I adopt the
**propagating** (correct, fail-fast-consistent) version.

New core `pub async fn dag::run_unified_dag(ctx, input, levels, tasks) ->
Result<Value, String>` holds the entire walk. Both macro copies shrink to:

```rust
handler: |ctx, _input| Box::pin(async move {
    let (levels, tasks) = { /* build DagBuilder in a scoped block, drop before await */ };
    ::autumn_harvest::dag::run_unified_dag(ctx, _input, levels, tasks).await
})
```

Kills the drift risk; the gate branch is written **once**.

### Gate branch semantics (inside the helper's level loop)

For a singleton gate level, before awaiting: still run
`dispatch_decision` (a gate downstream of a failed node is `Skipped`, never waits
forever). Then:

| `timeout` | signal arrives | deadline fires first |
|-----------|----------------|----------------------|
| `None`    | `Succeeded`, output = payload | (never) — `wait_for_signal` |
| `Some(t)` + `Continue` | `Succeeded`, output = payload | `Succeeded`, output = `Value::Null` |
| `Some(t)` + `FailRun`  | `Succeeded`, output = payload | `Failed` → run fails at the end |

**AC2 named-branch (Gap B / open question):** "continue to a named downstream
branch" is expressed **declaratively** via `on_timeout = Continue` (gate output
`Value::Null`) + a downstream `.condition(|ups| ups[0].is_null())` /
`.trigger_rule`. No bespoke branch-target mechanism is built this slice — this
is the documented interpretation; flag for owner confirmation.

**Signal payload as a `.map` collection (Gap B):** zero map-lowering change — the
gate stores its payload into `outputs[gate_idx]`; a downstream `.map_activity(f)
.over(&gate)` fans out over it (payload must be a JSON array).

## 4. Classic-DAG rejection (AC6) — `builder.rs`

New `validate_dags_have_no_signal_gates_on_classic_path()` in `try_build`,
mirroring `validate_dags_do_not_use_local_activities`. For each DAG with
`workflow_handler.is_none()` (the classic discriminator), `build_definition()`
and reject on the first gate task with new error
`HarvestBuilderError::DagSignalGateRequiresUnifiedExecution { dag, signal }`
(message names both the DAG and the signal). A gate in a unified DAG
(`workflow_handler.is_some()`) is allowed.

## 5. Graph-view (AC4) — plugin `dag_graph.rs`

Add `kind: DagNodeKind` (`Activity` | `Gate`) to `DagRunNode` and a `Waiting`
variant to `DagNodeStatus` (a gate whose `SignalReceived` has not yet arrived on
a live run). `build_run_graph` classifies a gate: `Waiting` while un-signalled on
a RUNNING run; `Succeeded` once its `SignalReceived` is in history; `TimedOut` if
its race timer fired (best-effort). `dag_retry::node_outcome` treats a gate as a
clean boundary (unchanged — gates record no activity events).

## 6. Non-goals / invariants
- No new `WorkflowEvent` variant (reuses `TimerStarted`/`TimerFired`/
  `SignalReceived` via #476). No migration. Append-only invariant intact.
- MCP restoration (Gap A: signal-capable DAG regains `signal_{dag}`) and the
  docs/example are a **later** slice — out of scope this run.

## Test matrix → AC

| Test | AC |
|------|----|
| `signal_gate_stores_gate_metadata_on_task` | AC1 surface |
| `signal_gate_with_timeout_stores_timeout_and_action` | AC2 surface |
| `signal_gate_is_isolated_into_its_own_singleton_level` | level isolation |
| `signal_gate_is_usable_as_upstream_and_map_source` | Gap B |
| `classic_dag_with_gate_is_rejected_at_build_time` | AC6 |
| `gate_suspends_with_only_a_wait_for_signal_command` | level isolation (worker shape) |
| `gate_signal_branch_unblocks_downstream_and_replays` | AC1 + AC5 signal branch (money test) |
| `gate_timeout_continue_branch_runs_downstream_and_replays` | AC2b + AC5 timer branch (money test) |
| `gate_timeout_failrun_branch_fails_the_dag` | AC2a |
| `gate_map_fans_out_over_signal_array_payload` | Gap B |
| `gate_node_reports_kind_gate_and_waiting_status` (plugin) | AC4 |
| `gate_node_reports_succeeded_once_signal_received` (plugin) | AC4 |

AC3 (durable, replay-safe) is proven by the two `WorkflowReplayer` money tests;
AC7 (no new event variant / no migration) is an invariant, not a test.
