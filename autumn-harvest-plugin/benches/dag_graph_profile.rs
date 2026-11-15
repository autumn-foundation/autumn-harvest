//! Deterministic (non-criterion) instruction/allocation-count profiling
//! harness for `dag_graph::build_run_graph`.
//!
//! That pure projection is behind `GET /dag-run-graph` (issue #690). It
//! rebuilds a unified DAG run's node topology (status, timing, attempts,
//! truncated error) from the registered [`DagDefinition`] and the run's
//! recorded `WorkflowEvent` history.
//!
//! `harness = false` + its own `main()`, the same shape as
//! `autumn-harvest/benches/det_check_profile.rs` and
//! `autumn-harvest/benches/schema_validate_profile.rs`. Point
//! `valgrind --tool=callgrind` / `valgrind --tool=dhat` at the executable.
//! Do not time it with `cargo bench`. `build_run_graph` is a synchronous,
//! in-memory, pure function. On this shared-vCPU machine only deterministic
//! counters give real signal, never wall time.
//!
//! # Workload
//!
//! A realistic 93-task DAG built via `autumn_harvest::dag::DagBuilder`. It has
//! eight layers with genuine fan-out/fan-in (diamond) shapes:
//!
//! - a root and an 8-way fan-out;
//! - a 16-node 2-way-fan-in diamond layer;
//! - 4 signal-gate nodes (plain `signal_gate` and `signal_gate_with_timeout`);
//! - a 16-node layer fed by both gates and earlier activities;
//! - two more 20-node 2-way-fan-in diamond layers;
//! - an 8-node final aggregation layer with 4-way fan-in.
//!
//! `total tasks = 93` (89 activities + 4 gates), matching the "tens to ~100+
//! nodes" realism bar.
//!
//! The paired event history is reachability-consistent by construction (see
//! [`build_events`]). A task receives outcome events only when all its
//! declared upstreams resolved successfully. The real unified-DAG walker
//! dispatches the same set. The history mixes every classification path
//! `build_run_graph` exercises for a non-gate node:
//!
//! - succeeded and failed activities;
//! - multi-attempt retries (repeated `ActivityStarted` before
//!   `ActivityCompleted`);
//! - `dag_skip:{idx}` condition-skip markers (issue #482);
//! - never-reached (pending) activities.
//!
//! All four gates resolve. Two resolve by `SignalReceived`. One resolves by a
//! race-timer `TimerStarted`/`TimerFired` pair (issue #476/#746). One resolves
//! by a signal that arrives before its own deadline. No gate stays `Waiting`.
//! The real walker processes execution levels strictly in sequence, and a gate
//! always occupies its own singleton level. A `Waiting` gate would stall the
//! walker there, and no later level would ever run. This fixture's later
//! layers would then collapse to a handful of `Pending` nodes with no events.
//!
//! This workload has no issue #780 compensator dispatches. Those require the
//! whole DAG run to fail terminally. That cannot coexist with the live,
//! still-progressing `RUNNING` history here (issue #690).
//! `dag_graph.rs`'s own unit tests cover the `is_compensation_dispatch`
//! exclusion (e.g.
//! `a_compensation_dispatch_is_not_read_as_a_same_named_forward_node`).
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

// The 93 activity-marker functions below (`fn t000() {}` ...) exist only so
// `DagBuilder::activity` derives a distinct node name per function. There is
// no body to make `const`.
#![allow(clippy::missing_const_for_fn)]

use std::time::Duration;

use autumn_harvest::dag::{
    DagBuilder, DagDefinition, DagSignalGate, DagTaskRef, GateTimeoutAction,
};
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::types::{ActivityExecId, TimerId, WorkerId};
use autumn_harvest_plugin::dag_graph::build_run_graph;
use chrono::{DateTime, TimeZone, Utc};
use serde_json::Value;

// ── DAG activity functions ──────────────────────────────────────────────
// Each is a distinct zero-sized function item type, so `DagBuilder::activity`
// derives a distinct node name per function. `dag_graph.rs`'s own `mod tests`
// uses the same idiom (`fn a() {}`, `fn b() {}`, ...). The 93 functions
// (89 activities + 4 signal gates) give a realistically sized DAG.
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
// `failed`, `activity_started`, `skip_marker_full`).

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

/// The `WorkflowCommand::StartTimer` that `context::wait_for_signal_timeout`
/// always records before the deadline can win a bounded gate's race.
///
/// The real walker never produces a `TimerFired` with no preceding
/// `TimerStarted` for the same `timer_id` (issue #690). Only a signal already
/// buffered at race start omits `TimerStarted`, and that path is `SignalWon`,
/// not `TimerWon`.
fn gate_timer_started(seq: usize, signal_name: &str, duration_secs: u64) -> WorkflowEvent {
    WorkflowEvent::TimerStarted {
        timer_id: TimerId::new(format!("__signal_timeout:{seq}:{signal_name}")),
        duration_secs,
    }
}

fn gate_timer_fired(seq: usize, signal_name: &str) -> WorkflowEvent {
    WorkflowEvent::TimerFired {
        timer_id: TimerId::new(format!("__signal_timeout:{seq}:{signal_name}")),
    }
}

// ── DAG construction ────────────────────────────────────────────────────
/// Build the 93-task profiling DAG (see the module doc for the shape).
// Long by construction: one explicit `builder.activity(...)`/`.upstream(...)`
// call per node keeps the wiring inspectable. Do not split it into a loop.
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
        .upstream(&layer2[0])
        .condition(|_ups| true);
    layer4.push(n_t025);
    let n_t026 = builder
        .activity(t026)
        .upstream(&layer3[1])
        .upstream(&layer2[5])
        .condition(|_ups| true);
    layer4.push(n_t026);
    let n_t027 = builder
        .activity(t027)
        .upstream(&layer3[2])
        .upstream(&layer2[10])
        .condition(|_ups| true);
    layer4.push(n_t027);
    let n_t028 = builder
        .activity(t028)
        .upstream(&layer3[3])
        .upstream(&layer2[15])
        .condition(|_ups| true);
    layer4.push(n_t028);
    let n_t029 = builder
        .activity(t029)
        .upstream(&layer3[0])
        .upstream(&layer2[4])
        .condition(|_ups| true);
    layer4.push(n_t029);
    let n_t030 = builder
        .activity(t030)
        .upstream(&layer3[1])
        .upstream(&layer2[9])
        .condition(|_ups| false);
    layer4.push(n_t030);
    let n_t031 = builder
        .activity(t031)
        .upstream(&layer3[2])
        .upstream(&layer2[14])
        .condition(|_ups| true);
    layer4.push(n_t031);
    let n_t032 = builder
        .activity(t032)
        .upstream(&layer3[3])
        .upstream(&layer2[3])
        .condition(|_ups| true);
    layer4.push(n_t032);
    let n_t033 = builder
        .activity(t033)
        .upstream(&layer3[0])
        .upstream(&layer2[8])
        .condition(|_ups| true);
    layer4.push(n_t033);
    let n_t034 = builder
        .activity(t034)
        .upstream(&layer3[1])
        .upstream(&layer2[13])
        .condition(|_ups| true);
    layer4.push(n_t034);
    let n_t035 = builder
        .activity(t035)
        .upstream(&layer3[2])
        .upstream(&layer2[2])
        .condition(|_ups| true);
    layer4.push(n_t035);
    let n_t036 = builder
        .activity(t036)
        .upstream(&layer3[3])
        .upstream(&layer2[7])
        .condition(|_ups| false);
    layer4.push(n_t036);
    let n_t037 = builder
        .activity(t037)
        .upstream(&layer3[0])
        .upstream(&layer2[12])
        .condition(|_ups| true);
    layer4.push(n_t037);
    let n_t038 = builder
        .activity(t038)
        .upstream(&layer3[1])
        .upstream(&layer2[1])
        .condition(|_ups| true);
    layer4.push(n_t038);
    let n_t039 = builder
        .activity(t039)
        .upstream(&layer3[2])
        .upstream(&layer2[6])
        .condition(|_ups| true);
    layer4.push(n_t039);
    let n_t040 = builder
        .activity(t040)
        .upstream(&layer3[3])
        .upstream(&layer2[11])
        .condition(|_ups| true);
    layer4.push(n_t040);
    let mut layer5: Vec<DagTaskRef> = Vec::new();
    let n_t041 = builder
        .activity(t041)
        .upstream(&layer4[0])
        .upstream(&layer4[7])
        .condition(|_ups| true);
    layer5.push(n_t041);
    let n_t042 = builder
        .activity(t042)
        .upstream(&layer4[1])
        .upstream(&layer4[8])
        .condition(|_ups| false);
    layer5.push(n_t042);
    let n_t043 = builder
        .activity(t043)
        .upstream(&layer4[2])
        .upstream(&layer4[9])
        .condition(|_ups| true);
    layer5.push(n_t043);
    let n_t044 = builder
        .activity(t044)
        .upstream(&layer4[3])
        .upstream(&layer4[10])
        .condition(|_ups| true);
    layer5.push(n_t044);
    let n_t045 = builder
        .activity(t045)
        .upstream(&layer4[4])
        .upstream(&layer4[11])
        .condition(|_ups| true);
    layer5.push(n_t045);
    let n_t046 = builder
        .activity(t046)
        .upstream(&layer4[5])
        .upstream(&layer4[12])
        .condition(|_ups| true);
    layer5.push(n_t046);
    let n_t047 = builder
        .activity(t047)
        .upstream(&layer4[6])
        .upstream(&layer4[13])
        .condition(|_ups| true);
    layer5.push(n_t047);
    let n_t048 = builder
        .activity(t048)
        .upstream(&layer4[7])
        .upstream(&layer4[14])
        .condition(|_ups| false);
    layer5.push(n_t048);
    let n_t049 = builder
        .activity(t049)
        .upstream(&layer4[8])
        .upstream(&layer4[15])
        .condition(|_ups| true);
    layer5.push(n_t049);
    let n_t050 = builder
        .activity(t050)
        .upstream(&layer4[9])
        .upstream(&layer4[0])
        .condition(|_ups| true);
    layer5.push(n_t050);
    let n_t051 = builder
        .activity(t051)
        .upstream(&layer4[10])
        .upstream(&layer4[1])
        .condition(|_ups| true);
    layer5.push(n_t051);
    let n_t052 = builder
        .activity(t052)
        .upstream(&layer4[11])
        .upstream(&layer4[2])
        .condition(|_ups| true);
    layer5.push(n_t052);
    let n_t053 = builder
        .activity(t053)
        .upstream(&layer4[12])
        .upstream(&layer4[3])
        .condition(|_ups| true);
    layer5.push(n_t053);
    let n_t054 = builder
        .activity(t054)
        .upstream(&layer4[13])
        .upstream(&layer4[4])
        .condition(|_ups| false);
    layer5.push(n_t054);
    let n_t055 = builder
        .activity(t055)
        .upstream(&layer4[14])
        .upstream(&layer4[5])
        .condition(|_ups| true);
    layer5.push(n_t055);
    let n_t056 = builder
        .activity(t056)
        .upstream(&layer4[15])
        .upstream(&layer4[6])
        .condition(|_ups| true);
    layer5.push(n_t056);
    let n_t057 = builder
        .activity(t057)
        .upstream(&layer4[0])
        .upstream(&layer4[7])
        .condition(|_ups| true);
    layer5.push(n_t057);
    let n_t058 = builder
        .activity(t058)
        .upstream(&layer4[1])
        .upstream(&layer4[8])
        .condition(|_ups| true);
    layer5.push(n_t058);
    let n_t059 = builder
        .activity(t059)
        .upstream(&layer4[2])
        .upstream(&layer4[9])
        .condition(|_ups| true);
    layer5.push(n_t059);
    let n_t060 = builder
        .activity(t060)
        .upstream(&layer4[3])
        .upstream(&layer4[10])
        .condition(|_ups| false);
    layer5.push(n_t060);
    let mut layer6: Vec<DagTaskRef> = Vec::new();
    let n_t061 = builder
        .activity(t061)
        .upstream(&layer5[0])
        .upstream(&layer5[9])
        .condition(|_ups| true);
    layer6.push(n_t061);
    let n_t062 = builder
        .activity(t062)
        .upstream(&layer5[1])
        .upstream(&layer5[10])
        .condition(|_ups| true);
    layer6.push(n_t062);
    let n_t063 = builder
        .activity(t063)
        .upstream(&layer5[2])
        .upstream(&layer5[11])
        .condition(|_ups| true);
    layer6.push(n_t063);
    let n_t064 = builder
        .activity(t064)
        .upstream(&layer5[3])
        .upstream(&layer5[12])
        .condition(|_ups| true);
    layer6.push(n_t064);
    let n_t065 = builder
        .activity(t065)
        .upstream(&layer5[4])
        .upstream(&layer5[13])
        .condition(|_ups| true);
    layer6.push(n_t065);
    let n_t066 = builder
        .activity(t066)
        .upstream(&layer5[5])
        .upstream(&layer5[14])
        .condition(|_ups| false);
    layer6.push(n_t066);
    let n_t067 = builder
        .activity(t067)
        .upstream(&layer5[6])
        .upstream(&layer5[15])
        .condition(|_ups| true);
    layer6.push(n_t067);
    let n_t068 = builder
        .activity(t068)
        .upstream(&layer5[7])
        .upstream(&layer5[16])
        .condition(|_ups| true);
    layer6.push(n_t068);
    let n_t069 = builder
        .activity(t069)
        .upstream(&layer5[8])
        .upstream(&layer5[17])
        .condition(|_ups| true);
    layer6.push(n_t069);
    let n_t070 = builder
        .activity(t070)
        .upstream(&layer5[9])
        .upstream(&layer5[18])
        .condition(|_ups| true);
    layer6.push(n_t070);
    let n_t071 = builder
        .activity(t071)
        .upstream(&layer5[10])
        .upstream(&layer5[19])
        .condition(|_ups| true);
    layer6.push(n_t071);
    let n_t072 = builder
        .activity(t072)
        .upstream(&layer5[11])
        .upstream(&layer5[0])
        .condition(|_ups| false);
    layer6.push(n_t072);
    let n_t073 = builder
        .activity(t073)
        .upstream(&layer5[12])
        .upstream(&layer5[1])
        .condition(|_ups| true);
    layer6.push(n_t073);
    let n_t074 = builder
        .activity(t074)
        .upstream(&layer5[13])
        .upstream(&layer5[2])
        .condition(|_ups| true);
    layer6.push(n_t074);
    let n_t075 = builder
        .activity(t075)
        .upstream(&layer5[14])
        .upstream(&layer5[3])
        .condition(|_ups| true);
    layer6.push(n_t075);
    let n_t076 = builder
        .activity(t076)
        .upstream(&layer5[15])
        .upstream(&layer5[4])
        .condition(|_ups| true);
    layer6.push(n_t076);
    let n_t077 = builder
        .activity(t077)
        .upstream(&layer5[16])
        .upstream(&layer5[5])
        .condition(|_ups| true);
    layer6.push(n_t077);
    let n_t078 = builder
        .activity(t078)
        .upstream(&layer5[17])
        .upstream(&layer5[6])
        .condition(|_ups| false);
    layer6.push(n_t078);
    let n_t079 = builder
        .activity(t079)
        .upstream(&layer5[18])
        .upstream(&layer5[7])
        .condition(|_ups| true);
    layer6.push(n_t079);
    let n_t080 = builder
        .activity(t080)
        .upstream(&layer5[19])
        .upstream(&layer5[8])
        .condition(|_ups| true);
    layer6.push(n_t080);
    // Final 4-way fan-in aggregation layer. Nothing downstream references
    // these terminal nodes, so their `DagTaskRef`s are discarded.
    let _ = builder
        .activity(t081)
        .upstream(&layer6[0])
        .upstream(&layer6[1])
        .upstream(&layer6[2])
        .upstream(&layer6[3])
        .condition(|_ups| true);
    let _ = builder
        .activity(t082)
        .upstream(&layer6[4])
        .upstream(&layer6[5])
        .upstream(&layer6[6])
        .upstream(&layer6[7])
        .condition(|_ups| true);
    let _ = builder
        .activity(t083)
        .upstream(&layer6[8])
        .upstream(&layer6[9])
        .upstream(&layer6[10])
        .upstream(&layer6[11])
        .condition(|_ups| true);
    let _ = builder
        .activity(t084)
        .upstream(&layer6[12])
        .upstream(&layer6[13])
        .upstream(&layer6[14])
        .upstream(&layer6[15])
        .condition(|_ups| false);
    let _ = builder
        .activity(t085)
        .upstream(&layer6[16])
        .upstream(&layer6[17])
        .upstream(&layer6[18])
        .upstream(&layer6[19])
        .condition(|_ups| true);
    let _ = builder
        .activity(t086)
        .upstream(&layer6[0])
        .upstream(&layer6[1])
        .upstream(&layer6[2])
        .upstream(&layer6[3])
        .condition(|_ups| true);
    let _ = builder
        .activity(t087)
        .upstream(&layer6[4])
        .upstream(&layer6[5])
        .upstream(&layer6[6])
        .upstream(&layer6[7])
        .condition(|_ups| true);
    let _ = builder
        .activity(t088)
        .upstream(&layer6[8])
        .upstream(&layer6[9])
        .upstream(&layer6[10])
        .upstream(&layer6[11])
        .condition(|_ups| true);

    builder.build().expect("realistic profiling dag builds")
}

// ── Event history construction ──────────────────────────────────────────

/// Last task index of the "backbone" -- root through layer2 -- that always
/// succeeds on the first attempt, unconditionally.
///
/// A DAG-blind per-index outcome pattern can produce a history the real
/// walker never could (issue #690). For example, the sole root is "never
/// reached", yet every node downstream of it still gets events.
/// [`build_events`] instead tracks, per task, whether all its upstreams are
/// `done`. It generates no event for an unreachable task.
///
/// The backbone gives that tracking a populated foundation. Every gate
/// (layer3) is reachable, so its scripted resolution really runs. A broken
/// upstream chain cannot short-circuit it to `Pending`. Layer4 onward then
/// has a real upstream population to test reachability against.
const BACKBONE_END: usize = 24;

/// Build a realistic recorded history for `def`.
///
/// It mixes succeeded, failed, multi-attempt-retried, condition-skipped
/// (#482), and never-reached (pending) activities. All four gates resolve:
/// two by signal, one by race-timer timeout (#476/#746), and one by a signal
/// that beats its own deadline.
///
/// Reachability-consistent by construction (issue #690). A task gets an
/// outcome only when all its `task.upstreams` are `done`. A task is `done`
/// when it is a succeeded activity (first attempt or after retry) or a gate.
/// Every gate resolves to `TaskStatus::Succeeded`, because
/// `push_gate_resolution` always returns `done`. An unreachable task gets no
/// events at all, as the real walker never dispatches it. A task is
/// unreachable when an upstream failed, was condition-skipped, or was itself
/// unreachable.
///
/// No gate stays `Waiting` (issue #690). The real unified-DAG walker
/// processes execution levels strictly in sequence. A signal gate always
/// occupies its own singleton level, so an unresolved gate stalls the walker
/// there. No later level then runs, whether or not it depends on the gate.
/// Layer4 onward, five sixths of the tasks, would collapse to `Pending` with
/// no events. That is correct, but far too sparse a workload to be useful.
///
/// This fixture also has no issue #780 compensator dispatches. Those require
/// the whole DAG run to fail terminally. That cannot coexist with the
/// still-progressing `RUNNING` history of a run whose gates all resolved.
/// `dag_graph.rs`'s own unit tests cover the `is_compensation_dispatch`
/// exclusion.
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

    // `done[idx]` -- whether task `idx`'s recorded outcome counts as "done"
    // for the default `AllSuccess` trigger rule every node in this fixture
    // uses. See the function doc comment.
    let mut done = vec![false; tasks.len()];
    let mut gate_seen: usize = 0;
    // Position among layer4-onward activities in creation order -- NOT gated
    // by reachability. `build_dag` sets each such task's `.condition(...)`
    // predicate by the same position (`pos % 6 != 5`). So `pos` advances for
    // every layer4-onward task, reachable or not. Otherwise the two disagree
    // on which task the real walker skips by condition (issue #690).
    let mut pos: usize = 0;

    for (idx, task) in tasks.iter().enumerate() {
        if let Some(gate) = &task.signal {
            let reachable = task.upstreams.iter().all(|&u| done[u]);
            if reachable {
                done[idx] = push_gate_resolution(&mut t, &mut events, gate, gate_seen % 4);
            }
            // The rotation advances regardless of reachability so `gate_seen`
            // always identifies the *intended* scenario (gate_0/1/2/3) by
            // position, independent of whether this particular gate turned
            // out reachable.
            gate_seen += 1;
            continue;
        }

        if idx <= BACKBONE_END {
            let id = ActivityExecId::new();
            push(&mut t, &mut events, sched(&task.activity_name, id));
            push(&mut t, &mut events, activity_started(id));
            push(&mut t, &mut events, completed(id));
            done[idx] = true;
            continue;
        }

        let this_pos = pos;
        pos += 1;

        let reachable = task.upstreams.iter().all(|&u| done[u]);
        if !reachable {
            // No real walker would ever have dispatched this node: no events.
            continue;
        }

        match this_pos % 6 {
            // Succeeded on the first attempt (half of reachable activities).
            0..=2 => {
                let id = ActivityExecId::new();
                push(&mut t, &mut events, sched(&task.activity_name, id));
                push(&mut t, &mut events, activity_started(id));
                push(&mut t, &mut events, completed(id));
                done[idx] = true;
            }
            // Succeeded after a genuine activity-level retry: two
            // `ActivityStarted` claims (the original + one requeue) before
            // the final `ActivityCompleted`, same `activity_id` throughout.
            3 => {
                let id = ActivityExecId::new();
                push(&mut t, &mut events, sched(&task.activity_name, id));
                push(&mut t, &mut events, activity_started(id));
                push(&mut t, &mut events, activity_started(id));
                push(&mut t, &mut events, completed(id));
                done[idx] = true;
            }
            // Failed (terminal, retry-exhausting failure). Not `done`.
            4 => {
                let id = ActivityExecId::new();
                push(&mut t, &mut events, sched(&task.activity_name, id));
                push(&mut t, &mut events, activity_started(id));
                push(
                    &mut t,
                    &mut events,
                    failed(id, "boom: transient dependency unavailable"),
                );
            }
            // Condition-skipped (#482): a `dag_skip` marker, no dispatch. The
            // real walker records this marker only for
            // `DagDispatchDecision::SkipByCondition`. That requires the task's
            // `.condition(...)` predicate to evaluate to `false`. `build_dag`
            // sets each layer4-onward task's condition to `this_pos % 6 != 5`,
            // with the same `this_pos` as the match above. So this arm runs
            // only for a task whose real predicate agrees (issue #690). Not
            // `done`.
            _ => push(
                &mut t,
                &mut events,
                skip_marker_full(idx, &task.activity_name, &task.upstreams),
            ),
        }
    }

    events
}

/// Resolve one already-*reached* signal gate per its rotating `scenario`
/// (`gate_seen % 4`, one of the four gates in `build_dag`), pushing whatever
/// events that resolution implies. Always returns `true` (`done`). Every gate
/// in this fixture resolves, and none stays `Waiting` (issue #690). See
/// [`build_events`] for why.
///
/// * `0 | 2 | 3` (`gate_0`, `gate_2` -- plain `signal_gate` -- and `gate_3`,
///   `signal_gate_with_timeout`, resolved by its signal arriving before the
///   deadline): resolved by a matching signal.
/// * `_` (`gate_1`, `signal_gate_with_timeout`,
///   `GateTimeoutAction::Continue`): resolved by the race timer firing before
///   any signal. The node status is `TimedOut`. Since `on_timeout` is
///   `Continue`, it still counts as `TaskStatus::Succeeded` for downstream
///   trigger rules. `TimerStarted` must precede `TimerFired` for the same
///   `timer_id`. `context::wait_for_signal_timeout` always arms the deadline
///   via `StartTimer` before a timeout can win (issue #690). Only a signal
///   already buffered at race start skips `TimerStarted`, and it resolves
///   `SignalWon`.
fn push_gate_resolution(
    t: &mut i64,
    events: &mut Vec<(DateTime<Utc>, WorkflowEvent)>,
    gate: &DagSignalGate,
    scenario: usize,
) -> bool {
    let push =
        |t: &mut i64, events: &mut Vec<(DateTime<Utc>, WorkflowEvent)>, ev: WorkflowEvent| {
            events.push((ts(*t), ev));
            *t += 1;
        };
    match scenario {
        // gate_0, gate_2 (plain `signal_gate`) and gate_3
        // (`signal_gate_with_timeout`, resolved by its signal arriving
        // before the deadline): all resolved by a matching signal.
        0 | 2 | 3 => {
            push(t, events, signal_received(&gate.signal_name));
        }
        // gate_1 (`signal_gate_with_timeout`): resolved by the race timer
        // firing before any signal arrives.
        _ => {
            push(t, events, gate_timer_started(1, &gate.signal_name, 300));
            // A durable timer cannot fire before its own deadline. Advance
            // the recorded clock by the full 300s the timer was armed for,
            // not the usual 1s between events (issue #690).
            *t += 300;
            push(t, events, gate_timer_fired(1, &gate.signal_name));
        }
    }
    true
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
