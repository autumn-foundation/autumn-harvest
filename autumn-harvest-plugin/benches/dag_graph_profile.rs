//! Deterministic (non-criterion) instruction/allocation-count profiling
//! harness for `dag_graph::build_run_graph` — the pure projection behind
//! `GET /dag-run-graph` (issue #690) that reconstructs a unified DAG run's
//! node topology (status, timing, attempts, truncated error) purely from the
//! registered [`DagDefinition`] and the run's recorded `WorkflowEvent`
//! history.
//!
//! `harness = false` + its own `main()`, the same shape as
//! `autumn-harvest/benches/det_check_profile.rs` and
//! `autumn-harvest/benches/schema_validate_profile.rs`: the compiled
//! artifact is a plain executable meant to be pointed at
//! `valgrind --tool=callgrind` / `valgrind --tool=dhat` directly, not
//! measured with `cargo bench`'s wall-clock timing loop
//! (`build_run_graph` is a synchronous, in-memory, pure function — real
//! signal on this shared-vCPU machine comes only from deterministic
//! counters, never wall time).
//!
//! # Workload
//!
//! A realistic 93-task DAG built via `autumn_harvest::dag::DagBuilder`:
//! eight layers with genuine fan-out/fan-in (diamond) shapes — a root, an
//! 8-way fan-out, a 16-node 2-way-fan-in diamond layer, 4 signal-gate nodes
//! (a mix of plain `signal_gate` and `signal_gate_with_timeout`), a 16-node
//! layer fed by both gates and earlier activities, two more 20-node
//! 2-way-fan-in diamond layers, and an 8-node final aggregation layer with
//! 4-way fan-in — not a degenerate flat chain. `total tasks = 93` (89
//! activities + 4 gates), matching the "tens to ~100+ nodes" realism bar.
//!
//! The paired event history mixes every classification path `build_run_graph`
//! exercises: succeeded activities, failed activities, activities with a
//! genuine multi-attempt retry (repeated `ActivityStarted` before
//! `ActivityCompleted`), `dag_skip:{idx}` condition-skip markers (issue
//! #482), never-reached (pending) activities, gates resolved by
//! `SignalReceived`, gates resolved by a race-timer `TimerFired` (issue
//! #476/#746), and gates left unresolved (`waiting`, since the run's
//! `exec_state` is `RUNNING`, a live/non-terminal state). Every gate's
//! upstreams are forced to succeed ([`GATE_UPSTREAM_INDICES`]) so
//! `gate_status`'s reachability check (checked *before* resolution) actually
//! lets each gate reach its intended `SignalReceived`/`TimerFired`/unresolved
//! outcome, rather than every gate reporting `Pending` regardless of what is
//! recorded for it (issue #690 review, Codex). A handful of issue #780
//! **compensator** dispatches (the `dag_compensate` envelope) are also mixed
//! in — including one recorded under a *current node's own name*
//! (`t001`, compensating a different dispatched node), which is what actually
//! exercises `dispatched_activity_names`'s purpose: excluding a compensation
//! dispatch from that node's own `latest_scheduled`/`node_outcome` selection
//! (a compensator name that is not itself a current node name, like the
//! other two dispatches here, never reaches that exclusion at all — it fails
//! the leading `name == node` check first).
//!
//! # Running
//!
//! ```text
//! BIN=$(cargo bench -p autumn-harvest-plugin --no-default-features \
//!   --bench dag_graph_profile --no-run --message-format=json 2>/dev/null \
//!   | jq -r 'select(.reason=="compiler-artifact" and .target.name=="dag_graph_profile") | .executable')
//! valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
//! callgrind_annotate --threshold=98 cg.out
//! valgrind --tool=dhat --dhat-out-file=dhat.json "$BIN"
//! ```
//!
//! `DAG_GRAPH_PROFILE_REPS` (default 300) repeats the whole `build_run_graph`
//! call.

// Ninety-three trivial, distinct zero-sized activity-marker functions below
// (`fn t000() {}` ...) exist purely so `DagBuilder::activity` can derive a
// distinct node name per function -- there is no body to make `const`.
#![allow(clippy::missing_const_for_fn)]

use std::time::Duration;

use autumn_harvest::dag::{DagBuilder, DagDefinition, DagTaskRef, GateTimeoutAction};
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::types::{ActivityExecId, TimerId, WorkerId};
use autumn_harvest_plugin::dag_graph::build_run_graph;
use chrono::{DateTime, TimeZone, Utc};
use serde_json::Value;

// ── DAG activity functions ──────────────────────────────────────────────
// Each is a distinct zero-sized function item type, so `DagBuilder::activity`
// derives a distinct node name per function (the same idiom
// `dag_graph.rs`'s own `mod tests` uses — `fn a() {}`, `fn b() {}`, ...).
// Ninety-three of them here (89 activities + 4 signal gates below) gives a
// realistically sized DAG rather than a synthetic microbenchmark's toy graph.
fn t000() {}
fn t001() {}
fn t002() {}
fn t003() {}
fn t004() {}
fn t005() {}
fn t006() {}
fn t007() {}
fn t008() {}
fn t009() {}
fn t010() {}
fn t011() {}
fn t012() {}
fn t013() {}
fn t014() {}
fn t015() {}
fn t016() {}
fn t017() {}
fn t018() {}
fn t019() {}
fn t020() {}
fn t021() {}
fn t022() {}
fn t023() {}
fn t024() {}
fn t025() {}
fn t026() {}
fn t027() {}
fn t028() {}
fn t029() {}
fn t030() {}
fn t031() {}
fn t032() {}
fn t033() {}
fn t034() {}
fn t035() {}
fn t036() {}
fn t037() {}
fn t038() {}
fn t039() {}
fn t040() {}
fn t041() {}
fn t042() {}
fn t043() {}
fn t044() {}
fn t045() {}
fn t046() {}
fn t047() {}
fn t048() {}
fn t049() {}
fn t050() {}
fn t051() {}
fn t052() {}
fn t053() {}
fn t054() {}
fn t055() {}
fn t056() {}
fn t057() {}
fn t058() {}
fn t059() {}
fn t060() {}
fn t061() {}
fn t062() {}
fn t063() {}
fn t064() {}
fn t065() {}
fn t066() {}
fn t067() {}
fn t068() {}
fn t069() {}
fn t070() {}
fn t071() {}
fn t072() {}
fn t073() {}
fn t074() {}
fn t075() {}
fn t076() {}
fn t077() {}
fn t078() {}
fn t079() {}
fn t080() {}
fn t081() {}
fn t082() {}
fn t083() {}
fn t084() {}
fn t085() {}
fn t086() {}
fn t087() {}
fn t088() {}

// ── Event fixture helpers ───────────────────────────────────────────────
// Same idioms as `dag_graph.rs`'s own `mod tests` (`sched`, `completed`,
// `failed`, `activity_started`, `skip_marker_full`, `compensator_sched`).

fn ts(secs: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(1_700_000_000 + secs, 0).single().unwrap()
}

fn workflow_started() -> WorkflowEvent {
    WorkflowEvent::WorkflowStarted {
        input: Value::Null,
        timestamp: ts(0),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }
}

fn sched(name: &str, id: ActivityExecId) -> WorkflowEvent {
    WorkflowEvent::ActivityScheduled {
        activity_id: id,
        name: name.to_string(),
        input: Value::Null,
        queue: "default".to_string(),
    }
}

/// A compensator dispatch exactly as issue #780's `unwind_dag_compensations`
/// records it: the compensator's own activity name, plus the three-key
/// envelope naming the **forward node** being rolled back. Exercises the
/// exact exclusion `dispatched_activity_names` exists for.
fn compensator_sched(
    compensator_name: &str,
    compensates_node: &str,
    id: ActivityExecId,
) -> WorkflowEvent {
    WorkflowEvent::ActivityScheduled {
        activity_id: id,
        name: compensator_name.to_string(),
        input: serde_json::json!({
            "dag_compensate": compensates_node,
            "input": Value::Null,
            "output": Value::Null,
        }),
        queue: "default".to_string(),
    }
}

fn activity_started(id: ActivityExecId) -> WorkflowEvent {
    WorkflowEvent::ActivityStarted {
        activity_id: id,
        worker_id: WorkerId::new("worker-1"),
    }
}

fn completed(id: ActivityExecId) -> WorkflowEvent {
    WorkflowEvent::ActivityCompleted {
        activity_id: id,
        output: Value::Null,
    }
}

fn failed(id: ActivityExecId, error: &str) -> WorkflowEvent {
    WorkflowEvent::ActivityFailed {
        activity_id: id,
        error: error.to_string(),
        attempt: 1,
        error_type: "S3Error".to_string(),
        non_retryable: false,
        details: None,
    }
}

fn skip_marker_full(task_index: usize, task: &str, upstreams: &[usize]) -> WorkflowEvent {
    WorkflowEvent::MarkerRecorded {
        name: format!("dag_skip:{task_index}"),
        details: serde_json::json!({
            "task": task,
            "reason": "condition_false",
            "upstreams": upstreams,
        }),
    }
}

fn signal_received(signal_name: &str) -> WorkflowEvent {
    WorkflowEvent::SignalReceived {
        signal_name: signal_name.to_string(),
        payload: serde_json::json!({ "approved": true }),
    }
}

fn gate_timer_fired(seq: usize, signal_name: &str) -> WorkflowEvent {
    WorkflowEvent::TimerFired {
        timer_id: TimerId::new(format!("__signal_timeout:{seq}:{signal_name}")),
    }
}

// ── DAG construction ────────────────────────────────────────────────────
/// Build the realistic 93-task profiling DAG (89 activities + 4 signal
/// gates, diamond fan-out/fan-in across 8 layers — see the module doc for
/// the shape).
// Long by construction (93 explicit `builder.activity(...)`/`.upstream(...)`
// calls, one per node, so the wiring is inspectable rather than generated by
// a loop over an opaque index table) -- not a candidate for splitting up.
#[allow(clippy::too_many_lines)]
fn build_dag() -> DagDefinition {
    let mut builder = DagBuilder::new();

    let n_t000 = builder.activity(t000);
    let layer0: Vec<DagTaskRef> = vec![n_t000];
    let mut layer1: Vec<DagTaskRef> = Vec::new();
    let n_t001 = builder.activity(t001).upstream(&layer0[0]);
    layer1.push(n_t001);
    let n_t002 = builder.activity(t002).upstream(&layer0[0]);
    layer1.push(n_t002);
    let n_t003 = builder.activity(t003).upstream(&layer0[0]);
    layer1.push(n_t003);
    let n_t004 = builder.activity(t004).upstream(&layer0[0]);
    layer1.push(n_t004);
    let n_t005 = builder.activity(t005).upstream(&layer0[0]);
    layer1.push(n_t005);
    let n_t006 = builder.activity(t006).upstream(&layer0[0]);
    layer1.push(n_t006);
    let n_t007 = builder.activity(t007).upstream(&layer0[0]);
    layer1.push(n_t007);
    let n_t008 = builder.activity(t008).upstream(&layer0[0]);
    layer1.push(n_t008);
    let mut layer2: Vec<DagTaskRef> = Vec::new();
    let n_t009 = builder
        .activity(t009)
        .upstream(&layer1[0])
        .upstream(&layer1[3]);
    layer2.push(n_t009);
    let n_t010 = builder
        .activity(t010)
        .upstream(&layer1[1])
        .upstream(&layer1[4]);
    layer2.push(n_t010);
    let n_t011 = builder
        .activity(t011)
        .upstream(&layer1[2])
        .upstream(&layer1[5]);
    layer2.push(n_t011);
    let n_t012 = builder
        .activity(t012)
        .upstream(&layer1[3])
        .upstream(&layer1[6]);
    layer2.push(n_t012);
    let n_t013 = builder
        .activity(t013)
        .upstream(&layer1[4])
        .upstream(&layer1[7]);
    layer2.push(n_t013);
    let n_t014 = builder
        .activity(t014)
        .upstream(&layer1[5])
        .upstream(&layer1[0]);
    layer2.push(n_t014);
    let n_t015 = builder
        .activity(t015)
        .upstream(&layer1[6])
        .upstream(&layer1[1]);
    layer2.push(n_t015);
    let n_t016 = builder
        .activity(t016)
        .upstream(&layer1[7])
        .upstream(&layer1[2]);
    layer2.push(n_t016);
    let n_t017 = builder
        .activity(t017)
        .upstream(&layer1[0])
        .upstream(&layer1[3]);
    layer2.push(n_t017);
    let n_t018 = builder
        .activity(t018)
        .upstream(&layer1[1])
        .upstream(&layer1[4]);
    layer2.push(n_t018);
    let n_t019 = builder
        .activity(t019)
        .upstream(&layer1[2])
        .upstream(&layer1[5]);
    layer2.push(n_t019);
    let n_t020 = builder
        .activity(t020)
        .upstream(&layer1[3])
        .upstream(&layer1[6]);
    layer2.push(n_t020);
    let n_t021 = builder
        .activity(t021)
        .upstream(&layer1[4])
        .upstream(&layer1[7]);
    layer2.push(n_t021);
    let n_t022 = builder
        .activity(t022)
        .upstream(&layer1[5])
        .upstream(&layer1[0]);
    layer2.push(n_t022);
    let n_t023 = builder
        .activity(t023)
        .upstream(&layer1[6])
        .upstream(&layer1[1]);
    layer2.push(n_t023);
    let n_t024 = builder
        .activity(t024)
        .upstream(&layer1[7])
        .upstream(&layer1[2]);
    layer2.push(n_t024);
    let mut layer3: Vec<DagTaskRef> = Vec::new();
    let n_gate0 = builder
        .signal_gate("gate_0")
        .upstream(&layer2[0])
        .upstream(&layer2[5]);
    layer3.push(n_gate0);
    let n_gate1 = builder
        .signal_gate_with_timeout(
            "gate_1",
            Duration::from_secs(300),
            GateTimeoutAction::Continue,
        )
        .upstream(&layer2[3])
        .upstream(&layer2[8]);
    layer3.push(n_gate1);
    let n_gate2 = builder
        .signal_gate("gate_2")
        .upstream(&layer2[6])
        .upstream(&layer2[11]);
    layer3.push(n_gate2);
    let n_gate3 = builder
        .signal_gate_with_timeout(
            "gate_3",
            Duration::from_secs(300),
            GateTimeoutAction::Continue,
        )
        .upstream(&layer2[9])
        .upstream(&layer2[14]);
    layer3.push(n_gate3);
    let mut layer4: Vec<DagTaskRef> = Vec::new();
    let n_t025 = builder
        .activity(t025)
        .upstream(&layer3[0])
        .upstream(&layer2[0]);
    layer4.push(n_t025);
    let n_t026 = builder
        .activity(t026)
        .upstream(&layer3[1])
        .upstream(&layer2[5]);
    layer4.push(n_t026);
    let n_t027 = builder
        .activity(t027)
        .upstream(&layer3[2])
        .upstream(&layer2[10]);
    layer4.push(n_t027);
    let n_t028 = builder
        .activity(t028)
        .upstream(&layer3[3])
        .upstream(&layer2[15]);
    layer4.push(n_t028);
    let n_t029 = builder
        .activity(t029)
        .upstream(&layer3[0])
        .upstream(&layer2[4]);
    layer4.push(n_t029);
    let n_t030 = builder
        .activity(t030)
        .upstream(&layer3[1])
        .upstream(&layer2[9]);
    layer4.push(n_t030);
    let n_t031 = builder
        .activity(t031)
        .upstream(&layer3[2])
        .upstream(&layer2[14]);
    layer4.push(n_t031);
    let n_t032 = builder
        .activity(t032)
        .upstream(&layer3[3])
        .upstream(&layer2[3]);
    layer4.push(n_t032);
    let n_t033 = builder
        .activity(t033)
        .upstream(&layer3[0])
        .upstream(&layer2[8]);
    layer4.push(n_t033);
    let n_t034 = builder
        .activity(t034)
        .upstream(&layer3[1])
        .upstream(&layer2[13]);
    layer4.push(n_t034);
    let n_t035 = builder
        .activity(t035)
        .upstream(&layer3[2])
        .upstream(&layer2[2]);
    layer4.push(n_t035);
    let n_t036 = builder
        .activity(t036)
        .upstream(&layer3[3])
        .upstream(&layer2[7]);
    layer4.push(n_t036);
    let n_t037 = builder
        .activity(t037)
        .upstream(&layer3[0])
        .upstream(&layer2[12]);
    layer4.push(n_t037);
    let n_t038 = builder
        .activity(t038)
        .upstream(&layer3[1])
        .upstream(&layer2[1]);
    layer4.push(n_t038);
    let n_t039 = builder
        .activity(t039)
        .upstream(&layer3[2])
        .upstream(&layer2[6]);
    layer4.push(n_t039);
    let n_t040 = builder
        .activity(t040)
        .upstream(&layer3[3])
        .upstream(&layer2[11]);
    layer4.push(n_t040);
    let mut layer5: Vec<DagTaskRef> = Vec::new();
    let n_t041 = builder
        .activity(t041)
        .upstream(&layer4[0])
        .upstream(&layer4[7]);
    layer5.push(n_t041);
    let n_t042 = builder
        .activity(t042)
        .upstream(&layer4[1])
        .upstream(&layer4[8]);
    layer5.push(n_t042);
    let n_t043 = builder
        .activity(t043)
        .upstream(&layer4[2])
        .upstream(&layer4[9]);
    layer5.push(n_t043);
    let n_t044 = builder
        .activity(t044)
        .upstream(&layer4[3])
        .upstream(&layer4[10]);
    layer5.push(n_t044);
    let n_t045 = builder
        .activity(t045)
        .upstream(&layer4[4])
        .upstream(&layer4[11]);
    layer5.push(n_t045);
    let n_t046 = builder
        .activity(t046)
        .upstream(&layer4[5])
        .upstream(&layer4[12]);
    layer5.push(n_t046);
    let n_t047 = builder
        .activity(t047)
        .upstream(&layer4[6])
        .upstream(&layer4[13]);
    layer5.push(n_t047);
    let n_t048 = builder
        .activity(t048)
        .upstream(&layer4[7])
        .upstream(&layer4[14]);
    layer5.push(n_t048);
    let n_t049 = builder
        .activity(t049)
        .upstream(&layer4[8])
        .upstream(&layer4[15]);
    layer5.push(n_t049);
    let n_t050 = builder
        .activity(t050)
        .upstream(&layer4[9])
        .upstream(&layer4[0]);
    layer5.push(n_t050);
    let n_t051 = builder
        .activity(t051)
        .upstream(&layer4[10])
        .upstream(&layer4[1]);
    layer5.push(n_t051);
    let n_t052 = builder
        .activity(t052)
        .upstream(&layer4[11])
        .upstream(&layer4[2]);
    layer5.push(n_t052);
    let n_t053 = builder
        .activity(t053)
        .upstream(&layer4[12])
        .upstream(&layer4[3]);
    layer5.push(n_t053);
    let n_t054 = builder
        .activity(t054)
        .upstream(&layer4[13])
        .upstream(&layer4[4]);
    layer5.push(n_t054);
    let n_t055 = builder
        .activity(t055)
        .upstream(&layer4[14])
        .upstream(&layer4[5]);
    layer5.push(n_t055);
    let n_t056 = builder
        .activity(t056)
        .upstream(&layer4[15])
        .upstream(&layer4[6]);
    layer5.push(n_t056);
    let n_t057 = builder
        .activity(t057)
        .upstream(&layer4[0])
        .upstream(&layer4[7]);
    layer5.push(n_t057);
    let n_t058 = builder
        .activity(t058)
        .upstream(&layer4[1])
        .upstream(&layer4[8]);
    layer5.push(n_t058);
    let n_t059 = builder
        .activity(t059)
        .upstream(&layer4[2])
        .upstream(&layer4[9]);
    layer5.push(n_t059);
    let n_t060 = builder
        .activity(t060)
        .upstream(&layer4[3])
        .upstream(&layer4[10]);
    layer5.push(n_t060);
    let mut layer6: Vec<DagTaskRef> = Vec::new();
    let n_t061 = builder
        .activity(t061)
        .upstream(&layer5[0])
        .upstream(&layer5[9]);
    layer6.push(n_t061);
    let n_t062 = builder
        .activity(t062)
        .upstream(&layer5[1])
        .upstream(&layer5[10]);
    layer6.push(n_t062);
    let n_t063 = builder
        .activity(t063)
        .upstream(&layer5[2])
        .upstream(&layer5[11]);
    layer6.push(n_t063);
    let n_t064 = builder
        .activity(t064)
        .upstream(&layer5[3])
        .upstream(&layer5[12]);
    layer6.push(n_t064);
    let n_t065 = builder
        .activity(t065)
        .upstream(&layer5[4])
        .upstream(&layer5[13]);
    layer6.push(n_t065);
    let n_t066 = builder
        .activity(t066)
        .upstream(&layer5[5])
        .upstream(&layer5[14]);
    layer6.push(n_t066);
    let n_t067 = builder
        .activity(t067)
        .upstream(&layer5[6])
        .upstream(&layer5[15]);
    layer6.push(n_t067);
    let n_t068 = builder
        .activity(t068)
        .upstream(&layer5[7])
        .upstream(&layer5[16]);
    layer6.push(n_t068);
    let n_t069 = builder
        .activity(t069)
        .upstream(&layer5[8])
        .upstream(&layer5[17]);
    layer6.push(n_t069);
    let n_t070 = builder
        .activity(t070)
        .upstream(&layer5[9])
        .upstream(&layer5[18]);
    layer6.push(n_t070);
    let n_t071 = builder
        .activity(t071)
        .upstream(&layer5[10])
        .upstream(&layer5[19]);
    layer6.push(n_t071);
    let n_t072 = builder
        .activity(t072)
        .upstream(&layer5[11])
        .upstream(&layer5[0]);
    layer6.push(n_t072);
    let n_t073 = builder
        .activity(t073)
        .upstream(&layer5[12])
        .upstream(&layer5[1]);
    layer6.push(n_t073);
    let n_t074 = builder
        .activity(t074)
        .upstream(&layer5[13])
        .upstream(&layer5[2]);
    layer6.push(n_t074);
    let n_t075 = builder
        .activity(t075)
        .upstream(&layer5[14])
        .upstream(&layer5[3]);
    layer6.push(n_t075);
    let n_t076 = builder
        .activity(t076)
        .upstream(&layer5[15])
        .upstream(&layer5[4]);
    layer6.push(n_t076);
    let n_t077 = builder
        .activity(t077)
        .upstream(&layer5[16])
        .upstream(&layer5[5]);
    layer6.push(n_t077);
    let n_t078 = builder
        .activity(t078)
        .upstream(&layer5[17])
        .upstream(&layer5[6]);
    layer6.push(n_t078);
    let n_t079 = builder
        .activity(t079)
        .upstream(&layer5[18])
        .upstream(&layer5[7]);
    layer6.push(n_t079);
    let n_t080 = builder
        .activity(t080)
        .upstream(&layer5[19])
        .upstream(&layer5[8]);
    layer6.push(n_t080);
    // Final 4-way fan-in aggregation layer. Terminal nodes — nothing
    // downstream references them, so (unlike every earlier layer) their
    // `DagTaskRef`s are never read after construction and are intentionally
    // discarded rather than collected into an unused `Vec`.
    let _ = builder
        .activity(t081)
        .upstream(&layer6[0])
        .upstream(&layer6[1])
        .upstream(&layer6[2])
        .upstream(&layer6[3]);
    let _ = builder
        .activity(t082)
        .upstream(&layer6[4])
        .upstream(&layer6[5])
        .upstream(&layer6[6])
        .upstream(&layer6[7]);
    let _ = builder
        .activity(t083)
        .upstream(&layer6[8])
        .upstream(&layer6[9])
        .upstream(&layer6[10])
        .upstream(&layer6[11]);
    let _ = builder
        .activity(t084)
        .upstream(&layer6[12])
        .upstream(&layer6[13])
        .upstream(&layer6[14])
        .upstream(&layer6[15]);
    let _ = builder
        .activity(t085)
        .upstream(&layer6[16])
        .upstream(&layer6[17])
        .upstream(&layer6[18])
        .upstream(&layer6[19]);
    let _ = builder
        .activity(t086)
        .upstream(&layer6[0])
        .upstream(&layer6[1])
        .upstream(&layer6[2])
        .upstream(&layer6[3]);
    let _ = builder
        .activity(t087)
        .upstream(&layer6[4])
        .upstream(&layer6[5])
        .upstream(&layer6[6])
        .upstream(&layer6[7]);
    let _ = builder
        .activity(t088)
        .upstream(&layer6[8])
        .upstream(&layer6[9])
        .upstream(&layer6[10])
        .upstream(&layer6[11]);

    builder.build().expect("realistic profiling dag builds")
}

// ── Event history construction ──────────────────────────────────────────

/// Task indices that feed a signal-gate node's upstream (see `build_dag`'s
/// `n_gate0`..`n_gate3`). Gate reachability is checked BEFORE resolution
/// (`gate_status`/`task_reach`): an upstream that fails, is condition-skipped,
/// or is never reached makes the default `AllSuccess` trigger rule report
/// `SkippedByTrigger`/`NotReached`, so the gate itself is classified `Pending`
/// regardless of any `SignalReceived`/`TimerFired` recorded for it. These
/// indices are forced to a plain first-attempt success below (overriding the
/// generic `idx % 5` pattern) so every gate is actually *reached*, and the
/// signal/timer events pushed for it below are genuinely exercised rather
/// than being dead weight a broken upstream chain never lets `gate_status`
/// read (issue #690 review, Codex).
const GATE_UPSTREAM_INDICES: [usize; 8] = [9, 14, 12, 17, 15, 20, 18, 23];

/// Build a realistic recorded history for `def`: a mix of succeeded, failed,
/// multi-attempt-retried, condition-skipped (#482), and never-reached
/// (pending) activities; gates resolved by signal, resolved by race-timer
/// timeout (#476/#746), and left unresolved (`waiting`, since the harness
/// runs with a live `exec_state`); plus issue #780 compensator dispatches
/// against already-dispatched forward nodes -- including one that reuses a
/// *current* node's own activity name, the shape `is_compensation_dispatch`'s
/// exclusion exists for (a later DAG definition introducing a forward node
/// named after an older definition's compensator).
fn build_events(def: &DagDefinition) -> Vec<(DateTime<Utc>, WorkflowEvent)> {
    let tasks = def.tasks();
    let mut events: Vec<(DateTime<Utc>, WorkflowEvent)> = Vec::with_capacity(tasks.len() * 3 + 8);
    let mut t: i64 = 0;
    let push =
        |t: &mut i64, events: &mut Vec<(DateTime<Utc>, WorkflowEvent)>, ev: WorkflowEvent| {
            events.push((ts(*t), ev));
            *t += 1;
        };

    push(&mut t, &mut events, workflow_started());

    let mut gate_seen: usize = 0;
    for (idx, task) in tasks.iter().enumerate() {
        if let Some(gate) = &task.signal {
            match gate_seen % 4 {
                // gate_0 (plain signal_gate): resolved by a matching signal.
                0 => push(&mut t, &mut events, signal_received(&gate.signal_name)),
                // gate_1 (signal_gate_with_timeout): resolved by the race timer
                // firing before any signal arrives.
                1 => push(&mut t, &mut events, gate_timer_fired(1, &gate.signal_name)),
                // gate_2 (plain signal_gate) and gate_3 (signal_gate_with_timeout):
                // both left unresolved — `waiting` on a live run, the deadline
                // (where one exists) not yet fired either.
                _ => {}
            }
            gate_seen += 1;
            continue;
        }

        if GATE_UPSTREAM_INDICES.contains(&idx) {
            // Forced success so every downstream gate is actually reached
            // (see `GATE_UPSTREAM_INDICES`'s doc comment).
            let id = ActivityExecId::new();
            push(&mut t, &mut events, sched(&task.activity_name, id));
            push(&mut t, &mut events, activity_started(id));
            push(&mut t, &mut events, completed(id));
            continue;
        }

        match idx % 5 {
            // Never reached: no events at all.
            0 => {}
            // Succeeded on the first attempt.
            1 => {
                let id = ActivityExecId::new();
                push(&mut t, &mut events, sched(&task.activity_name, id));
                push(&mut t, &mut events, activity_started(id));
                push(&mut t, &mut events, completed(id));
            }
            // Failed (terminal, retry-exhausting failure).
            2 => {
                let id = ActivityExecId::new();
                push(&mut t, &mut events, sched(&task.activity_name, id));
                push(&mut t, &mut events, activity_started(id));
                push(
                    &mut t,
                    &mut events,
                    failed(id, "boom: transient dependency unavailable"),
                );
            }
            // Condition-skipped (#482): a `dag_skip` marker, no dispatch.
            3 => push(
                &mut t,
                &mut events,
                skip_marker_full(idx, &task.activity_name, &task.upstreams),
            ),
            // Succeeded after a genuine activity-level retry: two
            // `ActivityStarted` claims (the original + one requeue) before
            // the final `ActivityCompleted`, same `activity_id` throughout.
            4 => {
                let id = ActivityExecId::new();
                push(&mut t, &mut events, sched(&task.activity_name, id));
                push(&mut t, &mut events, activity_started(id));
                push(&mut t, &mut events, activity_started(id));
                push(&mut t, &mut events, completed(id));
            }
            _ => unreachable!(),
        }
    }

    // A couple of issue #780 compensator dispatches under their own
    // (non-reused) names, naming forward nodes that were actually dispatched
    // above (`t001` succeeded, `t002` failed) — these grow the
    // `dispatched_activity_names` set realistically but, since `undo_t001`/
    // `undo_t002` are not themselves node names in this DAG, `latest_scheduled`
    // short-circuits on `name == node` before ever reaching
    // `is_compensation_dispatch` for them.
    push(
        &mut t,
        &mut events,
        compensator_sched("undo_t001", "t001", ActivityExecId::new()),
    );
    push(
        &mut t,
        &mut events,
        compensator_sched("undo_t002", "t002", ActivityExecId::new()),
    );
    // The exclusion-exercising case (issue #690 review, Codex): a compensator
    // dispatch recorded under a *current node's own name* (`t001`), rolling
    // back a different, already-dispatched node (`t002`). Recorded after
    // `t001`'s genuine forward dispatch above, so `latest_scheduled(events,
    // "t001", ...)`'s reverse scan meets this envelope first, must recognize
    // it as a compensation dispatch via `is_compensation_dispatch`, skip it,
    // and keep walking back to the real forward `ActivityScheduled` for
    // `t001` — exactly the cross-definition node-name-reuse scenario the
    // exclusion exists for.
    push(
        &mut t,
        &mut events,
        compensator_sched("t001", "t002", ActivityExecId::new()),
    );

    events
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn main() {
    let def = build_dag();
    let timestamped_events = build_events(&def);
    let reps = env_usize("DAG_GRAPH_PROFILE_REPS", 300);
    let exec_state = "RUNNING";

    let mut total_nodes = 0usize;
    for _ in 0..reps {
        let graph = build_run_graph(&def, &timestamped_events, exec_state);
        total_nodes += graph.len();
        std::hint::black_box(&graph);
    }

    println!(
        "dag_graph_profile: tasks={} events={} reps={reps} total_nodes={total_nodes}",
        def.tasks().len(),
        timestamped_events.len()
    );
    assert_eq!(
        total_nodes,
        def.tasks().len() * reps,
        "fixture bug: build_run_graph did not return one node per task per rep"
    );
}
