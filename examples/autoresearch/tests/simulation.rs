//! Seeded simulation of the two workflows (issue #2033).
//!
//! Each sweep runs every seed twice, checks the history is legal, replays it,
//! and runs the seed again with a crash at a seeded task. The mocks stand in
//! for git, the trainer and the agent, so the sweeps need no lab and no
//! database. The SQLite end-to-end tests run the real activities.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::float_cmp)]

use std::collections::BTreeSet;

use autoresearch::results::{ResultRow, short};
use autoresearch::rules::{Best, KeepPolicy, Status, Verdict};
use autoresearch::scripted::FIX_MODE;
use autoresearch::workflows::{
    Carry, Counts, ExperimentInput, ExperimentOutcome, MAX_FIXES, SessionConfig, SessionInput,
    SessionReport, experiment_info, research_session_info,
};
use autumn_harvest::dst::SeedPlan;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::testing::WorkflowTestEnv;
use autumn_harvest::testing::sim::{FaultPlan, SimOutcome, SimStatus};
use autumn_harvest::{always, sometimes};
use autumn_harvest_agent::AgentTask;
use autumn_harvest_agent::flow::labels::UNTRUSTED_INPUT;
use serde_json::{Value, json};

/// Enough seeds to reach each target, few enough for every CI run.
const SEEDS: SeedPlan = SeedPlan {
    first: 0,
    count: 64,
};

/// Commit ids are full ids, as git gives them. A row shows the short form.
const BASE: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const BASE_BPB: f64 = 0.9979;

/// The report of a finished agent run with `text` as its answer.
fn report(text: &str) -> Value {
    json!({
        "stop": "completed",
        "text": text,
        "steps_used": 3,
        "tool_calls": 4,
        "usage": {"input_tokens": 100, "output_tokens": 20},
        "messages": []
    })
}

fn summary_log(val_bpb: f64) -> String {
    format!("---\nval_bpb:          {val_bpb:.6}\npeak_vram_mb:     45060.2\n")
}

fn base() -> Best {
    Best {
        commit: BASE.into(),
        val_bpb: BASE_BPB,
        memory_gb: 44.0,
    }
}

fn experiment_input(baseline: bool) -> ExperimentInput {
    ExperimentInput {
        lab: "/lab".into(),
        tag: "t".into(),
        branch: "autoresearch/t".into(),
        session: "autoresearch/t".into(),
        gpu: "gpu0".into(),
        trainer: autoresearch::trainer::TrainerSpec::default(),
        number: u32::from(!baseline),
        baseline,
        best: (!baseline).then(base),
        policy: KeepPolicy::default(),
        hint: None,
        explore: false,
        recent: Vec::new(),
        program: "# program".into(),
        agent: autoresearch::workflows::AgentSettings::default(),
        flow_labels: BTreeSet::new(),
    }
}

/// An older kept commit that a rewind can move the base to.
const KEPT: &str = "k000001000000000000000000000000000000000";
/// A commit that `results.tsv` does not keep.
const LOST: &str = "x000001000000000000000000000000000000000";

fn kept() -> Best {
    Best {
        commit: KEPT.into(),
        val_bpb: 0.9985,
        memory_gb: 44.0,
    }
}

/// Git, the trainer and the agent as mocks. A seed picks each path:
///
/// - After the proposal, `HEAD` is the base, an older kept commit (an
///   approved rewind), or a commit that `results.tsv` does not keep.
/// - A proposal commit sometimes holds no change.
/// - A run crashes out of memory, runs past its time budget after its
///   summary, or lands near the base.
/// - The agent of experiment 3 fails.
fn experiment_env() -> WorkflowTestEnv {
    WorkflowTestEnv::new()
        .mock_child_workflow("agent_loop", |input| {
            let task: AgentTask = serde_json::from_value(input).map_err(|e| e.to_string())?;
            if task.input.contains("experiment 3 ") {
                return Err("the model is down".into());
            }
            Ok(if task.input.contains(FIX_MODE) {
                report("fix: DEVICE_BATCH_SIZE 128 -> 64")
            } else {
                report("raise MATRIX_LR: 0.04 -> 0.05")
            })
        })
        .mock_activity_seeded("head", |_, mock| {
            if mock.call() == 1 {
                return Ok(json!(BASE));
            }
            Ok(match mock.rng().below(100) {
                0..10 => json!(KEPT),
                10..14 => json!(LOST),
                _ => json!(BASE),
            })
        })
        .mock_activity("find_kept", |input| {
            Ok(if input["commit"] == KEPT {
                serde_json::to_value(kept()).unwrap()
            } else {
                Value::Null
            })
        })
        .mock_activity_seeded("commit", |input, mock| {
            let call = mock.call();
            let changed = !(call == 1 && mock.rng().below(100) < 10);
            Ok(json!({
                "commit": if changed { format!("c{call:06}{:0>33}", "") } else { input["parent"].as_str().unwrap_or(BASE).to_string() },
                "parent": input["parent"],
                "changed": changed,
                "lines_added": u32::from(changed),
                "lines_removed": u32::from(changed)
            }))
        })
        .mock_activity_seeded("train", |_, mock| {
            let roll = mock.rng().below(100);
            let bpb = 0.9900 + f64::from(u32::try_from(roll).unwrap()) / 10_000.0;
            Ok(match roll {
                0..25 => json!({"exit_code": 1, "timed_out": false,
                                "log_tail": "torch.OutOfMemoryError: CUDA out of memory.\n"}),
                // Killed at the budget after it printed a summary.
                25..32 => json!({"exit_code": null, "timed_out": true, "log_tail": summary_log(bpb)}),
                _ => json!({"exit_code": 0, "timed_out": false, "log_tail": summary_log(bpb)}),
            })
        })
        .mock_activity("parse_summary", |input| {
            Ok(json!(autoresearch::summary::parse(
                input.as_str().unwrap_or("")
            )))
        })
        .mock_activity("reset", |input| Ok(input["commit"].clone()))
        .mock_activity("record", |_| Ok(json!(true)))
}

fn scheduled<'a>(events: &'a [WorkflowEvent], name: &'a str) -> impl Iterator<Item = &'a Value> {
    events.iter().filter_map(move |event| match event {
        WorkflowEvent::ActivityScheduled { name: n, input, .. } if n == name => Some(input),
        _ => None,
    })
}

/// The outputs of each completed activity named `name`, in order.
fn completed<'a>(events: &'a [WorkflowEvent], name: &str) -> Vec<&'a Value> {
    let ids: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: n,
                ..
            } if n == name => Some(*activity_id),
            _ => None,
        })
        .collect();
    events
        .iter()
        .filter_map(|event| match event {
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output,
                ..
            } if ids.contains(activity_id) => Some(output),
            _ => None,
        })
        .collect()
}

fn check_experiment(run: &SimOutcome) {
    let events = run.events();
    let Ok(output) = run.result() else {
        // A step failed for good. The session resets and logs the crash.
        always!(
            completed(events, "record").is_empty(),
            "a failed experiment records no row"
        );
        sometimes!(true, "an experiment fails for good");
        return;
    };
    let out: ExperimentOutcome = serde_json::from_value(output.clone()).unwrap();
    let trains = scheduled(events, "train").count();
    let resets: Vec<&Value> = scheduled(events, "reset").collect();
    let grants = events
        .iter()
        .filter(|e| matches!(e, WorkflowEvent::MutexGranted { key, .. } if key == "gpu:gpu0"))
        .count();
    let base = if out.rewound { kept() } else { base() };

    always!(
        scheduled(events, "record").count() == 1,
        "one row per experiment"
    );
    always!(
        trains <= 1 + MAX_FIXES as usize,
        "a crash gets at most one fix"
    );
    always!(grants == trains, "each training takes the GPU mutex");
    always!(
        out.row.status == out.verdict.status,
        "the row has the verdict"
    );
    let timed_out = completed(events, "train")
        .last()
        .is_some_and(|run| run["timed_out"] == json!(true));
    if timed_out {
        always!(
            out.row.status == Status::Crash,
            "a run past its budget is a crash"
        );
        sometimes!(true, "a run passes its time budget");
    }
    if out.rewound {
        sometimes!(true, "an approved rewind moves the base");
    }
    if out.row.description.starts_with("no change") {
        always!(trains == 0, "no change trains nothing");
        always!(
            (out.row.val_bpb, out.row.memory_gb) == (base.val_bpb, base.memory_gb),
            "a no-change row holds the numbers of its base"
        );
        sometimes!(true, "an agent proposes no change");
    }
    match out.row.status {
        Status::Keep => {
            always!(resets.is_empty(), "a keep resets nothing");
            always!(
                out.best
                    .as_ref()
                    .is_some_and(|b| b.commit.len() == 40 && short(&b.commit) == out.row.commit),
                "a keep is the new base, by its full id"
            );
            always!(out.row.val_bpb < base.val_bpb, "a kept run beats the base");
            sometimes!(true, "a change is kept");
        }
        Status::Discard | Status::Crash => {
            always!(
                resets.last().and_then(|input| input["commit"].as_str())
                    == Some(base.commit.as_str()),
                "a discard or a crash resets to the base last"
            );
            always!(out.best == Some(base), "the base stays");
            if out.row.status == Status::Crash {
                always!(
                    out.row.val_bpb == 0.0 && out.row.memory_gb == 0.0,
                    "a crash row has zeros"
                );
                sometimes!(true, "a run crashes for good");
            } else {
                sometimes!(true, "a change is discarded");
            }
        }
    }
    if out.fixes == 1 && out.row.status != Status::Crash {
        sometimes!(true, "a fix recovers a crash");
    }
}

#[tokio::test]
async fn an_experiment_holds_its_invariants_on_every_seed() {
    let env = experiment_env();
    let report = env
        .simulation()
        .seeds(SEEDS)
        .faults(FaultPlan::new().transient("commit", 1, 2))
        .require_reached("a change is kept")
        .require_reached("a change is discarded")
        .require_reached("a run crashes for good")
        .require_reached("a fix recovers a crash")
        .require_reached("a run passes its time budget")
        .require_reached("an approved rewind moves the base")
        .require_reached("an agent proposes no change")
        .require_reached("an experiment fails for good")
        .sweep(
            experiment_info().handler,
            serde_json::to_value(experiment_input(false)).unwrap(),
            check_experiment,
        )
        .await
        .unwrap_or_else(|failure| panic!("{failure}"));
    assert!(report.seeds > 0);
}

#[tokio::test]
async fn an_experiment_whose_agent_fails_changes_nothing() {
    let input = ExperimentInput {
        number: 3,
        ..experiment_input(false)
    };
    experiment_env()
        .simulation()
        .seeds(SEEDS)
        .sweep(
            experiment_info().handler,
            serde_json::to_value(input).unwrap(),
            |run| {
                always!(run.result().is_err(), "the experiment fails");
                always!(
                    scheduled(run.events(), "commit").count() == 0
                        && scheduled(run.events(), "train").count() == 0,
                    "it commits and trains nothing"
                );
            },
        )
        .await
        .unwrap_or_else(|failure| panic!("{failure}"));
}

#[tokio::test]
async fn an_experiment_survives_transient_faults_on_every_activity() {
    let env = experiment_env();
    env.simulation()
        .seeds(SEEDS)
        .fault_percent(10)
        .require_reached("an experiment completes through a fault")
        .sweep(
            experiment_info().handler,
            serde_json::to_value(experiment_input(false)).unwrap(),
            |run| {
                check_experiment(run);
                if run.result().is_ok() && !run.faults().is_empty() {
                    sometimes!(true, "an experiment completes through a fault");
                }
            },
        )
        .await
        .unwrap_or_else(|failure| panic!("{failure}"));
}

#[tokio::test]
async fn a_baseline_trains_head_with_no_proposal() {
    let env = experiment_env();
    env.simulation()
        .seeds(SEEDS)
        .sweep(
            experiment_info().handler,
            serde_json::to_value(experiment_input(true)).unwrap(),
            |run| {
                always!(run.result().is_ok(), "a baseline completes");
                let children = run
                    .events()
                    .iter()
                    .filter(|e| matches!(e, WorkflowEvent::ChildWorkflowStarted { .. }))
                    .count();
                always!(children == 0, "a baseline asks no agent");
                always!(
                    scheduled(run.events(), "commit").count() == 0,
                    "a baseline commits nothing"
                );
                if let Ok(output) = run.result() {
                    let out: ExperimentOutcome = serde_json::from_value(output.clone()).unwrap();
                    always!(out.row.description == "baseline", "the row says baseline");
                    always!(out.row.commit == short(BASE), "the baseline is HEAD");
                }
            },
        )
        .await
        .unwrap_or_else(|failure| panic!("{failure}"));
}

/// A deterministic experiment: a crash every fourth, a keep every fourth,
/// and a discard otherwise. Experiment 2 fails. A hint shows in the
/// description. A `paper:` hint adds the untrusted-input label, as a real
/// paper read does.
fn mock_experiment(input: Value) -> Result<Value, String> {
    let input: ExperimentInput = serde_json::from_value(input).map_err(|e| e.to_string())?;
    let best = input.best.clone().unwrap_or_else(base);
    let n = input.number;
    if n % 5 == 2 {
        return Err("git broke".into());
    }
    let description = input
        .hint
        .as_ref()
        .map_or_else(|| format!("idea {n}"), |h| format!("hint: {}", h.text));
    let (status, val_bpb) = if input.baseline {
        (Status::Keep, BASE_BPB)
    } else {
        match n % 4 {
            0 => (Status::Crash, 0.0),
            1 => (Status::Keep, best.val_bpb - 0.001),
            _ => (Status::Discard, best.val_bpb + 0.001),
        }
    };
    let commit = if input.baseline {
        BASE.to_string()
    } else {
        format!("e{n:06}{:0>33}", "")
    };
    let row = ResultRow {
        commit: short(&commit),
        val_bpb,
        memory_gb: if status == Status::Crash { 0.0 } else { 44.0 },
        status,
        description: if input.baseline {
            "baseline".into()
        } else {
            description
        },
    };
    let new_best = if status == Status::Keep {
        Best {
            commit,
            val_bpb,
            memory_gb: 44.0,
        }
    } else {
        best
    };
    let mut flow_labels = input.flow_labels.clone();
    if input.hint.is_some_and(|h| h.text.starts_with("paper:")) {
        flow_labels.insert(UNTRUSTED_INPUT.to_string());
    }
    Ok(serde_json::to_value(ExperimentOutcome {
        row,
        best: Some(new_best),
        verdict: Verdict {
            status,
            score: 0.0,
            reason: "mock".into(),
        },
        rewound: false,
        fixes: 0,
        flow_labels,
    })
    .unwrap())
}

fn session_env() -> WorkflowTestEnv {
    WorkflowTestEnv::new()
        .mock_activity("setup", |_| {
            Ok(json!({"branch": "autoresearch/t", "head": BASE, "program": "# program"}))
        })
        .mock_child_workflow("experiment", mock_experiment)
        .mock_child_workflow("agent_loop", |_| Ok(report("Morning report.")))
        .mock_activity("reset", |input| Ok(input["commit"].clone()))
        .mock_activity("record", |_| Ok(json!(true)))
}

fn session_config() -> SessionConfig {
    let mut config = SessionConfig::new("/lab", "t");
    config.max_experiments = 5;
    config.experiments_per_run = 3;
    config.explore_percent = 50;
    config
}

fn experiment_inputs(events: &[WorkflowEvent]) -> Vec<ExperimentInput> {
    events
        .iter()
        .filter_map(|event| match event {
            WorkflowEvent::ChildWorkflowStarted {
                workflow_name,
                input,
                ..
            } if workflow_name == "experiment" => serde_json::from_value(input.clone()).ok(),
            _ => None,
        })
        .collect()
}

/// The outcome of each experiment child in order. `None` for a child that
/// failed. The session awaits each child, so the order is the start order.
fn experiment_outcomes(events: &[WorkflowEvent]) -> Vec<Option<ExperimentOutcome>> {
    events
        .iter()
        .filter_map(|event| match event {
            WorkflowEvent::ChildWorkflowCompleted { output, .. } => {
                serde_json::from_value::<ExperimentOutcome>(output.clone())
                    .ok()
                    .map(Some)
            }
            WorkflowEvent::ChildWorkflowFailed { .. } => Some(None),
            _ => None,
        })
        .collect()
}

/// The index of the first event that matches `pick`, or the end.
fn first(events: &[WorkflowEvent], pick: impl Fn(&WorkflowEvent) -> bool) -> usize {
    events.iter().position(pick).unwrap_or(events.len())
}

/// The index of the start event of each experiment child, in order.
fn experiment_starts(events: &[WorkflowEvent]) -> Vec<usize> {
    events
        .iter()
        .enumerate()
        .filter_map(|(k, event)| match event {
            WorkflowEvent::ChildWorkflowStarted { workflow_name, .. }
                if workflow_name == "experiment" =>
            {
                Some(k)
            }
            _ => None,
        })
        .collect()
}

/// The payload of the last signal named `name` before event `before`.
fn last_signal<'a>(events: &'a [WorkflowEvent], name: &str, before: usize) -> Option<&'a Value> {
    events[..before].iter().rev().find_map(|e| match e {
        WorkflowEvent::SignalReceived {
            signal_name,
            payload,
        } if signal_name == name => Some(payload),
        _ => None,
    })
}

/// The first experiment that a hint with labels reaches. The session takes
/// the labels when it takes the hint, before its next experiment.
fn labelled_from(events: &[WorkflowEvent]) -> Option<usize> {
    let signal = events.iter().position(|e| {
        matches!(e, WorkflowEvent::SignalReceived { signal_name, payload }
            if signal_name == "hint" && payload["labels"].as_array().is_some_and(|l| !l.is_empty()))
    })?;
    experiment_starts(events)
        .iter()
        .position(|&start| start > signal)
}

fn signals(events: &[WorkflowEvent], name: &str, before: usize) -> usize {
    events[..before]
        .iter()
        .filter(|e| matches!(e, WorkflowEvent::SignalReceived { signal_name, .. } if signal_name == name))
        .count()
}

/// Check a session run whose config hands off after `per_run` experiments.
fn check_session(run: &SimOutcome, per_run: u32) {
    let events = run.events();
    let inputs = experiment_inputs(events);
    let outcomes = experiment_outcomes(events);
    let (best, tainted) = check_experiments(&inputs, &outcomes, labelled_from(events));
    let failures = outcomes.iter().filter(|o| o.is_none()).count();
    always!(
        completed(events, "record").len() == failures,
        "the session records one crash row per failed experiment"
    );
    // A failed child can leave its commit at `HEAD`.
    let bases: Vec<&str> = inputs
        .iter()
        .zip(&outcomes)
        .filter(|(_, out)| out.is_none())
        .filter_map(|(input, _)| input.best.as_ref().map(|b| b.commit.as_str()))
        .collect();
    let resets: Vec<&str> = scheduled(events, "reset")
        .filter_map(|input| input["commit"].as_str())
        .collect();
    always!(
        resets == bases,
        "a failed experiment resets the branch to its base"
    );
    check_shared(events, &inputs);
    let used = inputs.iter().filter(|i| i.hint.is_some()).count();
    always!(
        used <= signals(events, "hint", events.len()),
        "an experiment uses only a hint that arrived"
    );
    if used > 0 {
        sometimes!(true, "a hint reaches an experiment");
    }
    match run.status() {
        SimStatus::Completed => {
            let report: SessionReport =
                serde_json::from_value(run.result().clone().unwrap()).unwrap();
            let Counts {
                kept,
                discarded,
                crashed,
            } = report.counts;
            always!(
                kept + discarded + crashed == report.experiments + 1,
                "every experiment and the baseline count once"
            );
            always!(report.stopped.is_some(), "only a stop ends the first run");
            always!(
                Some(&report.best) == best.as_ref(),
                "the report has the best"
            );
            // A stop that lands at the hand-off wins over `continue_as_new`.
            always!(
                report.experiments <= per_run,
                "a stop comes no later than the hand-off"
            );
            sometimes!(true, "a stop ends the night early");
            if report.experiments == per_run {
                sometimes!(true, "a stop at the hand-off ends the night");
            }
        }
        SimStatus::ContinuedAsNew => {
            check_hand_off(events, best.as_ref(), tainted, used, per_run);
        }
        other => {
            always!(
                other == SimStatus::Completed,
                "the session ended as {other}"
            );
        }
    }
}

/// Check the base, the policy and the labels of each experiment. Returns
/// the best result and the taint after the last one.
fn check_experiments(
    inputs: &[ExperimentInput],
    outcomes: &[Option<ExperimentOutcome>],
    labelled_from: Option<usize>,
) -> (Option<Best>, bool) {
    // The base of each experiment is the best after the one before it. A
    // crash or a failure never moves it.
    let mut best = inputs.first().and_then(|i| i.best.clone());
    let mut tainted = inputs.first().is_some_and(|i| !i.flow_labels.is_empty());
    let mut tighter_seen = false;
    for (i, input) in inputs.iter().enumerate() {
        if labelled_from == Some(i) {
            tainted = true;
            sometimes!(true, "the labels of a sibling reach a later experiment");
        }
        if i > 0 {
            always!(input.best == best, "each base is the best so far");
            always!(
                input.flow_labels.contains(UNTRUSTED_INPUT) == tainted,
                "an experiment carries a label after a paper or a failure, and only then"
            );
            if tainted {
                sometimes!(true, "a paper or a failure taints a later experiment");
            }
        }
        let tighter = input.policy != KeepPolicy::default();
        always!(!tighter_seen || tighter, "a new policy stays in force");
        tighter_seen |= tighter;
        if tighter {
            sometimes!(true, "a policy reaches an experiment");
        }
        match outcomes.get(i) {
            Some(Some(out)) => {
                if out.best.is_some() {
                    best.clone_from(&out.best);
                }
            }
            Some(None) => {
                tainted = true;
                sometimes!(true, "a failed experiment becomes a crash row");
            }
            None => {}
        }
        if input
            .hint
            .as_ref()
            .is_some_and(|h| h.text.starts_with("paper:"))
        {
            tainted = true;
        }
    }
    (best, tainted)
}

/// Check that each idea the session shares carries the labels of the
/// session. A sibling that takes the hint must also take its taint.
fn check_shared(events: &[WorkflowEvent], inputs: &[ExperimentInput]) {
    let starts = experiment_starts(events);
    for (k, event) in events.iter().enumerate() {
        let WorkflowEvent::ExternalSignalRequested {
            signal_name,
            payload,
            ..
        } = event
        else {
            continue;
        };
        if signal_name != "hint" {
            continue;
        }
        let labels: BTreeSet<String> =
            serde_json::from_value(payload["labels"].clone()).unwrap_or_default();
        let Some(i) = starts.iter().rposition(|&start| start < k) else {
            continue;
        };
        always!(
            inputs
                .get(i)
                .is_some_and(|input| input.flow_labels.is_subset(&labels)),
            "a shared idea carries the labels of its session"
        );
        if labels.contains(UNTRUSTED_INPUT) {
            sometimes!(true, "a tainted session shares its labels");
        }
    }
}

/// Check what the hand-off carries.
fn check_hand_off(
    events: &[WorkflowEvent],
    best: Option<&Best>,
    tainted: bool,
    used: usize,
    per_run: u32,
) {
    let hand_off = first(events, |e| {
        matches!(e, WorkflowEvent::WorkflowContinuedAsNew { .. })
    });
    let carry = events.iter().find_map(|event| match event {
        WorkflowEvent::WorkflowContinuedAsNew { input, .. } => {
            serde_json::from_value::<SessionInput>(input.clone()).ok()
        }
        _ => None,
    });
    let carry = carry.and_then(|input| input.carry);
    always!(
        carry.as_ref().is_some_and(|c| c.done == per_run),
        "the hand-off carries the count"
    );
    always!(
        carry.as_ref().map(|c| &c.best) == best,
        "the hand-off carries the best"
    );
    always!(
        signals(events, "stop", hand_off) == 0,
        "a stop before the hand-off ends the night"
    );
    if let Some(policy) = last_signal(events, "policy", hand_off) {
        let policy: KeepPolicy = serde_json::from_value(policy.clone()).unwrap();
        always!(
            carry.as_ref().is_some_and(|c| c.policy == policy),
            "the hand-off carries the policy"
        );
        if policy != KeepPolicy::default() {
            sometimes!(true, "a new policy crosses the hand-off");
        }
    }
    always!(
        carry
            .as_ref()
            .is_some_and(|c| used + c.hints.len() == signals(events, "hint", hand_off)),
        "each hint is used or carried"
    );
    if carry.as_ref().is_some_and(|c| !c.hints.is_empty()) {
        sometimes!(true, "a hint is carried across the hand-off");
    }
    always!(
        carry
            .as_ref()
            .is_some_and(|c| c.flow_labels.contains(UNTRUSTED_INPUT) == tainted),
        "the hand-off carries the labels"
    );
    sometimes!(true, "the session continues as new");
}

#[tokio::test]
async fn a_session_holds_its_invariants_under_signals_and_faults() {
    let env = session_env();
    let tighter = KeepPolicy {
        min_improvement: 0.0005,
        ..KeepPolicy::default()
    };
    env.simulation()
        .seeds(SEEDS)
        .signal("hint", json!({"text": "paper:warmdown", "from": "human"}))
        .signal("policy", serde_json::to_value(tighter).unwrap())
        .signal("stop", json!({"reason": "bed time"}))
        .faults(FaultPlan::new().transient("setup", 1, 2))
        .require_reached("a paper or a failure taints a later experiment")
        .require_reached("a policy reaches an experiment")
        .require_reached("a hint reaches an experiment")
        .require_reached("a stop ends the night early")
        .sweep(
            research_session_info().handler,
            serde_json::to_value(SessionInput {
                config: session_config(),
                carry: None,
            })
            .unwrap(),
            |run| check_session(run, 3),
        )
        .await
        .unwrap_or_else(|failure| panic!("{failure}"));
}

/// With one experiment per run, a stop that lands during that experiment
/// meets the hand-off. The stop must win.
///
/// A signal alone lands at the first boundary, where nothing else waits.
/// Signals arrive in call order, so four policy signals that change nothing
/// go first and spread the stop over later tasks.
#[tokio::test]
async fn a_stop_at_the_hand_off_ends_the_night() {
    let mut config = session_config();
    config.experiments_per_run = 1;
    let same = serde_json::to_value(KeepPolicy::default()).unwrap();
    session_env()
        .simulation()
        .seeds(SEEDS)
        .signal("policy", same.clone())
        .signal("policy", same.clone())
        .signal("policy", same.clone())
        .signal("policy", same)
        .signal("stop", json!({"reason": "bed time"}))
        .require_reached("a stop at the hand-off ends the night")
        .sweep(
            research_session_info().handler,
            serde_json::to_value(SessionInput {
                config,
                carry: None,
            })
            .unwrap(),
            |run| check_session(run, 1),
        )
        .await
        .unwrap_or_else(|failure| panic!("{failure}"));
}

/// With no stop, the first run reaches the hand-off. Experiment 2 fails on
/// the way, so the sweep checks the crash row, the taint and the carry. Four
/// hints for three experiments leave one to carry.
#[tokio::test]
async fn a_session_hands_off_its_carry_past_a_failed_experiment() {
    let tighter = KeepPolicy {
        min_improvement: 0.0005,
        ..KeepPolicy::default()
    };
    // A sibling shares a kept idea from a paper, so its hint has the taint.
    let sibling = json!({
        "text": "autoresearch/other kept a smaller batch",
        "from": "autoresearch/other",
        "labels": [UNTRUSTED_INPUT],
    });
    let mut config = session_config();
    config.siblings = vec!["autoresearch/other".into()];
    session_env()
        .simulation()
        .seeds(SEEDS)
        .signal("hint", sibling)
        .signal(
            "hint",
            json!({"text": "try a longer warmdown", "from": "human"}),
        )
        .signal(
            "hint",
            json!({"text": "try a deeper model", "from": "human"}),
        )
        .signal(
            "hint",
            json!({"text": "try a lower weight decay", "from": "human"}),
        )
        .signal("policy", serde_json::to_value(tighter).unwrap())
        .require_reached("a failed experiment becomes a crash row")
        .require_reached("a paper or a failure taints a later experiment")
        .require_reached("the labels of a sibling reach a later experiment")
        .require_reached("a tainted session shares its labels")
        .require_reached("a hint is carried across the hand-off")
        .require_reached("a new policy crosses the hand-off")
        .require_reached("the session continues as new")
        .sweep(
            research_session_info().handler,
            serde_json::to_value(SessionInput {
                config,
                carry: None,
            })
            .unwrap(),
            |run| {
                always!(
                    run.status() == SimStatus::ContinuedAsNew,
                    "with no stop, the first run hands off"
                );
                check_session(run, 3);
            },
        )
        .await
        .unwrap_or_else(|failure| panic!("{failure}"));
}

#[tokio::test]
async fn a_continued_session_finishes_from_its_carry() {
    let env = session_env();
    let carry = Carry {
        tag: "t".into(),
        branch: "autoresearch/t".into(),
        program: "# program".into(),
        best: base(),
        done: 3,
        recent: Vec::new(),
        hints: vec![autoresearch::workflows::Hint {
            text: "carried idea".into(),
            from: "human".into(),
            labels: BTreeSet::new(),
        }],
        policy: KeepPolicy::default(),
        counts: Counts {
            kept: 2,
            discarded: 1,
            crashed: 1,
        },
        runs: 1,
        flow_labels: BTreeSet::new(),
    };
    env.simulation()
        .seeds(SEEDS)
        .sweep(
            research_session_info().handler,
            serde_json::to_value(SessionInput {
                config: session_config(),
                carry: Some(carry),
            })
            .unwrap(),
            |run| {
                always!(
                    run.status() == SimStatus::Completed,
                    "the last run completes"
                );
                let report: SessionReport =
                    serde_json::from_value(run.result().clone().unwrap()).unwrap();
                always!(report.experiments == 5, "the count goes on from the carry");
                let inputs = experiment_inputs(run.events());
                always!(
                    inputs.iter().all(|i| !i.baseline),
                    "a continued run trains no baseline"
                );
                always!(
                    inputs
                        .first()
                        .and_then(|i| i.hint.as_ref())
                        .map(|h| h.text.as_str())
                        == Some("carried idea"),
                    "a carried hint goes to the next experiment"
                );
                always!(
                    scheduled(run.events(), "setup").count() == 0,
                    "a continued run does not set up again"
                );
            },
        )
        .await
        .unwrap_or_else(|failure| panic!("{failure}"));
}

#[tokio::test]
async fn a_crashed_baseline_fails_the_session() {
    let env = WorkflowTestEnv::new()
        .mock_activity("setup", |_| {
            Ok(json!({"branch": "autoresearch/t", "head": BASE, "program": "# program"}))
        })
        .mock_child_workflow("experiment", |input| {
            let mut out = mock_experiment(input)?;
            out["row"]["status"] = json!("crash");
            out["verdict"]["status"] = json!("crash");
            out["row"]["val_bpb"] = json!(0.0);
            Ok(out)
        });
    env.simulation()
        .seeds(SeedPlan {
            first: 0,
            count: 16,
        })
        .sweep(
            research_session_info().handler,
            serde_json::to_value(SessionInput {
                config: session_config(),
                carry: None,
            })
            .unwrap(),
            |run| {
                always!(
                    run.result().as_ref().is_err_and(|e| e.contains("baseline")),
                    "a crashed baseline fails the session and says why"
                );
            },
        )
        .await
        .unwrap_or_else(|failure| panic!("{failure}"));
}
