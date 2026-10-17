//! Seeded simulation of the two workflows (issue #2033).
//!
//! Each sweep runs every seed twice, checks the history is legal, replays it,
//! and runs the seed again with a crash at a seeded task. The mocks stand in
//! for git, the trainer and the agent, so the sweeps need no lab and no
//! database. The SQLite end-to-end tests run the real activities.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::float_cmp)]

use autoresearch::results::ResultRow;
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
use serde_json::{Value, json};

/// Enough seeds to reach each target, few enough for every CI run.
const SEEDS: SeedPlan = SeedPlan {
    first: 0,
    count: 64,
};

const BASE: &str = "aaaaaaa";
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
    }
}

/// Git, the trainer and the agent as mocks. A seed picks each run: about a
/// quarter crash out of memory, the rest land near the base.
fn experiment_env() -> WorkflowTestEnv {
    WorkflowTestEnv::new()
        .mock_child_workflow("agent_loop", |input| {
            let task: AgentTask = serde_json::from_value(input).map_err(|e| e.to_string())?;
            Ok(if task.input.contains(FIX_MODE) {
                report("fix: DEVICE_BATCH_SIZE 128 -> 64")
            } else {
                report("raise MATRIX_LR: 0.04 -> 0.05")
            })
        })
        .mock_activity("head", |_| Ok(json!(BASE)))
        .mock_activity_seeded("commit", |_, mock| {
            let call = mock.call();
            let parent = if call == 1 {
                BASE.to_string()
            } else {
                format!("c{:06}", call - 1)
            };
            Ok(json!({
                "commit": format!("c{call:06}"),
                "parent": parent,
                "changed": true,
                "lines_added": 1,
                "lines_removed": 1
            }))
        })
        .mock_activity_seeded("train", |_, mock| {
            let roll = mock.rng().below(100);
            let log = if roll < 25 {
                "torch.OutOfMemoryError: CUDA out of memory.\n".to_string()
            } else {
                summary_log(0.9900 + f64::from(u32::try_from(roll).unwrap()) / 10_000.0)
            };
            Ok(json!({"exit_code": 0, "timed_out": false, "log_tail": log}))
        })
        .mock_activity("parse_summary", |input| {
            Ok(json!(autoresearch::summary::parse(
                input.as_str().unwrap_or("")
            )))
        })
        .mock_activity("reset", |input| Ok(input["commit"].clone()))
        .mock_activity("record", |_| Ok(json!(true)))
        .mock_activity("find_kept", |_| Ok(Value::Null))
}

fn scheduled<'a>(events: &'a [WorkflowEvent], name: &'a str) -> impl Iterator<Item = &'a Value> {
    events.iter().filter_map(move |event| match event {
        WorkflowEvent::ActivityScheduled { name: n, input, .. } if n == name => Some(input),
        _ => None,
    })
}

fn check_experiment(run: &SimOutcome) {
    let Ok(output) = run.result() else {
        // An activity can exhaust its attempts under the fault lane. The
        // run then fails, and the sweep still checks replay and crashes.
        return;
    };
    let out: ExperimentOutcome = serde_json::from_value(output.clone()).unwrap();
    let events = run.events();
    let trains = scheduled(events, "train").count();
    let resets: Vec<&Value> = scheduled(events, "reset").collect();
    let grants = events
        .iter()
        .filter(|e| matches!(e, WorkflowEvent::MutexGranted { key, .. } if key == "gpu:gpu0"))
        .count();

    always!(
        scheduled(events, "record").count() == 1,
        "one row per experiment"
    );
    always!(
        trains <= 1 + MAX_FIXES as usize,
        "a crash gets at most one fix"
    );
    always!(grants >= trains, "each training holds the GPU mutex");
    always!(
        out.row.status == out.verdict.status,
        "the row has the verdict"
    );
    match out.row.status {
        Status::Keep => {
            always!(resets.is_empty(), "a keep resets nothing");
            always!(
                out.best.as_ref().map(|b| b.commit.as_str()) == Some(out.row.commit.as_str()),
                "a keep is the new base"
            );
            always!(out.row.val_bpb < BASE_BPB, "a kept run beats the base");
            sometimes!(true, "a change is kept");
        }
        Status::Discard | Status::Crash => {
            always!(
                resets.last().map(|input| input["commit"].as_str()) == Some(Some(BASE)),
                "a discard or a crash resets to the base last"
            );
            always!(out.best == Some(base()), "the base stays");
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
        .sweep(
            experiment_info().handler,
            serde_json::to_value(experiment_input(false)).unwrap(),
            check_experiment,
        )
        .await
        .unwrap_or_else(|failure| panic!("{failure}"));
    assert_eq!(report.seeds, SEEDS.count);
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
                    always!(out.row.commit == BASE, "the baseline is HEAD");
                }
            },
        )
        .await
        .unwrap_or_else(|failure| panic!("{failure}"));
}

/// A deterministic experiment: a crash every fourth, a keep every fourth,
/// and a discard otherwise. A hint shows in the description.
fn mock_experiment(input: Value) -> Result<Value, String> {
    let input: ExperimentInput = serde_json::from_value(input).map_err(|e| e.to_string())?;
    let best = input.best.clone().unwrap_or_else(base);
    let n = input.number;
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
        format!("e{n:06}")
    };
    let row = ResultRow {
        commit: commit.clone(),
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

fn check_session(run: &SimOutcome) {
    let inputs = experiment_inputs(run.events());
    let hinted = inputs.iter().filter(|i| i.hint.is_some()).count();
    always!(hinted <= 1, "a hint reaches at most one experiment");
    if hinted == 1 {
        sometimes!(true, "a hint reaches an experiment");
    }
    let mut last = f64::INFINITY;
    for input in &inputs {
        if let Some(best) = &input.best {
            always!(best.val_bpb <= last, "the base never gets worse");
            last = best.val_bpb;
        }
    }
    let tighter = KeepPolicy {
        min_improvement: 0.0005,
        ..KeepPolicy::default()
    };
    always!(
        inputs
            .iter()
            .all(|i| i.policy == KeepPolicy::default() || i.policy == tighter),
        "an experiment gets the old policy or the new one"
    );
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
            // A stop that lands at the hand-off wins over `continue_as_new`.
            always!(
                report.experiments <= 3,
                "a stop comes no later than the hand-off"
            );
            sometimes!(true, "a stop ends the night early");
        }
        SimStatus::ContinuedAsNew => {
            let carry = run.events().iter().find_map(|event| match event {
                WorkflowEvent::WorkflowContinuedAsNew { input, .. } => {
                    serde_json::from_value::<SessionInput>(input.clone()).ok()
                }
                _ => None,
            });
            let carry = carry.and_then(|input| input.carry);
            always!(
                carry.as_ref().is_some_and(|c| c.done == 3),
                "the hand-off carries the count"
            );
            always!(
                carry.as_ref().is_some_and(|c| c.best.val_bpb <= BASE_BPB),
                "the hand-off carries the best"
            );
            sometimes!(true, "the session continues as new");
        }
        other => {
            always!(
                other == SimStatus::Completed,
                "the session ended as {other}"
            );
        }
    }
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
        .signal(
            "hint",
            json!({"text": "try a longer warmdown", "from": "human"}),
        )
        .signal("policy", serde_json::to_value(tighter).unwrap())
        .signal("stop", json!({"reason": "bed time"}))
        .faults(FaultPlan::new().transient("setup", 1, 2))
        .sweep(
            research_session_info().handler,
            serde_json::to_value(SessionInput {
                config: session_config(),
                carry: None,
            })
            .unwrap(),
            check_session,
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
        }],
        policy: KeepPolicy::default(),
        counts: Counts {
            kept: 2,
            discarded: 1,
            crashed: 1,
        },
        runs: 1,
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
