//! End to end on the SQLite backend: a real git lab, the simulated trainer
//! and the scripted researcher. No GPU, no key, no network.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::float_cmp
)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use autoresearch::agent;
use autoresearch::inbox::{Command, Inbox};
use autoresearch::lab::RESULTS;
use autoresearch::rules::{KeepPolicy, Status};
use autoresearch::runtime::{self, Step, session_id};
use autoresearch::scripted::{PREFER_SIMPLER, ScriptedResearcher};
use autoresearch::trainer::TrainerSpec;
use autoresearch::workflows::{
    EXPERIMENT, ExperimentOutcome, SESSION, SessionReport, StatusView, with_program,
};
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest_agent::{AgentTask, MemoryScope, sqlite};
use autumn_harvest_sqlite::{ExecutionFilter, ExecutionOutcome, SqliteError};
use chrono::{TimeZone, Utc};
use common::{Fixture, activities_of, input_of, run_to_approval, run_to_end, runs_of, signals_of};
use serde_json::{Value, json};

fn model() -> Arc<ScriptedResearcher> {
    Arc::new(ScriptedResearcher::default())
}

fn hint(rt: &mut autumn_harvest_sqlite::SqliteRuntime, tag: &str, text: &str, key: &str) {
    runtime::apply(
        rt,
        key,
        &Command::Hint {
            session: session_id(tag),
            text: text.into(),
        },
    )
    .unwrap();
}

fn experiment_outputs(rt: &autumn_harvest_sqlite::SqliteRuntime) -> Vec<ExperimentOutcome> {
    runs_of(rt, EXPERIMENT)
        .into_iter()
        .filter_map(|exec| match rt.outcome(exec).unwrap() {
            ExecutionOutcome::Completed(value) => serde_json::from_value(value).ok(),
            _ => None,
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_night_of_research_keeps_only_improvements() {
    let fx = Fixture::new();
    let model = model();
    let mut rt = fx.open(&model);
    let exec = runtime::start(&mut rt, &fx.config("night", 6)).unwrap();
    let report = run_to_end(&mut rt, exec).await;

    // AC1: setup, a baseline and six experiments, one row each.
    assert_eq!(report.experiments, 6);
    assert_eq!(report.branch, "autoresearch/night");
    let rows = fx.lab().rows().unwrap();
    assert_eq!(rows.len(), 7);
    assert_eq!(rows[0].description, "baseline");
    assert_eq!(rows[0].status, Status::Keep);
    let text = fx.lab().read(RESULTS).unwrap();
    assert!(text.starts_with(autoresearch::results::HEADER));

    // AC3: each keep beats the best before it, under the line cost.
    let mut best = f64::INFINITY;
    for row in rows.iter().filter(|r| r.status == Status::Keep) {
        assert!(row.val_bpb < best + 0.001, "{row:?}");
        best = best.min(row.val_bpb);
    }
    let kept = rows.iter().filter(|r| r.status == Status::Keep).count();
    assert!(kept >= 3, "the scripted ideas mostly help: {rows:#?}");
    assert_eq!(report.counts.kept as usize, kept);

    // AC2: the branch holds the baseline and the kept commits only.
    assert_eq!(fx.lab().head().unwrap(), report.best.commit);
    assert_eq!(
        fx.lab().git(&["rev-list", "--count", "HEAD"]).unwrap(),
        kept.to_string()
    );
    assert_eq!(fx.lab().git(&["status", "--porcelain"]).unwrap(), "");

    // AC13: search attributes and a memo on the session.
    let page = rt
        .list_executions(
            &ExecutionFilter::new()
                .workflow_name(SESSION)
                .search_attribute("tag", json!("night")),
        )
        .unwrap();
    assert_eq!(page.executions.len(), 1);
    let summary = &page.executions[0];
    let attrs = summary.search_attributes.clone().into_value().unwrap();
    assert_eq!(attrs["phase"], "done");
    assert_eq!(attrs["experiments"], 6);
    assert_eq!(attrs["best_bpb"], json!(report.best.val_bpb));
    let memo = summary.memo.clone().into_value().unwrap();
    assert_eq!(memo["lab"], json!(fx.lab().root().display().to_string()));
    assert_eq!(memo["gpu"], "gpu0");

    // AC17: every history replays on the core replayer.
    let verified = runtime::verify(runtime::stored_runs(&rt).unwrap()).await;
    assert!(verified.failures.is_empty(), "{:#?}", verified.failures);
    assert!(verified.replayed >= 1 + 7 + 6, "{verified:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_crash_gets_one_fix_and_a_broken_idea_logs_a_crash_row() {
    let fx = Fixture::new();
    let model = model();
    let mut rt = fx.open(&model);
    let mut config = fx.config("bold", 2);
    config.explore_percent = 100;
    let exec = runtime::start(&mut rt, &config).unwrap();
    let base = fx.lab().head().unwrap();
    run_to_end(&mut rt, exec).await;

    let outputs = experiment_outputs(&rt);
    // Baseline, then the out-of-memory idea, then the diverging one.
    assert_eq!(outputs.len(), 3);
    let oom = &outputs[1];
    assert_eq!(oom.fixes, 1, "{oom:?}");
    assert_ne!(
        oom.row.status,
        Status::Crash,
        "the fix recovered it: {oom:?}"
    );
    let nan = &outputs[2];
    assert_eq!(nan.row.status, Status::Crash, "{nan:?}");
    assert_eq!((nan.row.val_bpb, nan.row.memory_gb), (0.0, 0.0));
    let rows = fx.lab().rows().unwrap();
    assert!(rows[2].to_line().contains("\t0.000000\t0.0\tcrash\t"));
    // A crash resets to the base.
    if oom.row.status == Status::Discard {
        assert_eq!(fx.lab().head().unwrap(), base);
    }
    assert!(
        !fx.lab()
            .read("train.py")
            .unwrap()
            .contains("MATRIX_LR = 0.4")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restart_resumes_without_paying_for_a_turn_or_a_run_twice() {
    // The control: the same night, with no restart.
    let control = Fixture::new();
    let control_model = model();
    let mut rt = control.open(&control_model);
    let exec = runtime::start(&mut rt, &control.config("r", 2)).unwrap();
    hint(&mut rt, "r", "paper:warmdown", "h1");
    let pending = run_to_approval(&mut rt, exec).await;
    runtime::apply(
        &mut rt,
        "a1",
        &Command::Approve {
            token: pending.token,
        },
    )
    .unwrap();
    run_to_end(&mut rt, exec).await;
    let total_calls = control_model.calls();
    let control_rows = control.lab().rows().unwrap();
    drop(rt);

    // The same night, with a process that dies at the approval wait.
    let fx = Fixture::new();
    let first = model();
    let (exec, token) = {
        let mut rt = fx.open(&first);
        let exec = runtime::start(&mut rt, &fx.config("r", 2)).unwrap();
        hint(&mut rt, "r", "paper:warmdown", "h1");
        let pending = run_to_approval(&mut rt, exec).await;
        (exec, pending.token)
    };
    let second = model();
    let mut rt = fx.open(&second);
    runtime::apply(&mut rt, "a1", &Command::Approve { token }).unwrap();
    run_to_end(&mut rt, exec).await;

    // AC16: no model turn is paid for twice.
    assert_eq!(first.calls() + second.calls(), total_calls);
    assert!(second.calls() < total_calls);
    // No run is trained twice, and the rows match the control.
    let rows = fx.lab().rows().unwrap();
    assert_eq!(rows.len(), control_rows.len());
    for (a, b) in rows.iter().zip(&control_rows) {
        assert_eq!(
            (a.status, a.val_bpb, &a.description),
            (b.status, b.val_bpb, &b.description)
        );
    }
    let trainings: usize = runs_of(&rt, EXPERIMENT)
        .into_iter()
        .map(|exec| {
            activities_of(&rt, exec)
                .iter()
                .filter(|a| *a == "train")
                .count()
        })
        .sum();
    assert_eq!(trainings, rows.len());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_edit_after_a_paper_waits_and_a_deny_changes_nothing() {
    let fx = Fixture::new();
    let mut rt = fx.open(&model());
    let exec = runtime::start(&mut rt, &fx.config("deny", 1)).unwrap();
    hint(&mut rt, "deny", "paper:muon", "h1");
    let before = fx.lab().read("train.py").unwrap();

    // AC6: the flow rule gates the edit that follows untrusted input.
    let pending = run_to_approval(&mut rt, exec).await;
    assert_eq!(pending.tool, agent::EDIT_TRAIN);
    assert!(pending.token.contains("tool_approval:"));
    runtime::apply(
        &mut rt,
        "d1",
        &Command::Deny {
            token: pending.token,
            reason: "papers are not trusted tonight".into(),
        },
    )
    .unwrap();
    run_to_end(&mut rt, exec).await;

    let rows = fx.lab().rows().unwrap();
    assert!(rows[1].description.starts_with("no change"), "{rows:?}");
    assert_eq!(fx.lab().read("train.py").unwrap(), before);
    // The injected order in the paper never reached `train.py`.
    assert!(!fx.lab().read("train.py").unwrap().contains("4096"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_approved_rewind_moves_the_base_to_an_older_kept_commit() {
    let fx = Fixture::new();
    let mut rt = fx.open(&model());
    let exec = runtime::start(&mut rt, &fx.config("rw", 2)).unwrap();
    let baseline = fx.lab().head().unwrap();
    hint(&mut rt, "rw", "paper:warmdown", "h1");
    hint(&mut rt, "rw", &format!("rewind:{baseline}"), "h2");

    let edit = run_to_approval(&mut rt, exec).await;
    assert_eq!(edit.tool, agent::EDIT_TRAIN);
    runtime::apply(&mut rt, "a1", &Command::Approve { token: edit.token }).unwrap();
    let rewind = run_to_approval(&mut rt, exec).await;
    assert_eq!(rewind.tool, agent::REWIND);
    assert_eq!(rewind.arguments["commit"], json!(baseline));
    runtime::apply(
        &mut rt,
        "a2",
        &Command::Approve {
            token: rewind.token,
        },
    )
    .unwrap();
    run_to_end(&mut rt, exec).await;

    let outputs = experiment_outputs(&rt);
    assert_eq!(outputs[1].row.status, Status::Keep, "{:?}", outputs[1]);
    let second = &outputs[2];
    assert!(second.rewound, "{second:?}");
    if second.row.status == Status::Keep {
        // The kept commit sits on the baseline, not on the first keep.
        let parent = fx
            .lab()
            .git(&["rev-parse", "--short=7", &format!("{}^", second.row.commit)])
            .unwrap();
        assert_eq!(parent, baseline);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_late_approval_never_releases_the_call() {
    let fx = Fixture::new();
    let mut rt = fx.open(&model());
    let mut config = fx.config("late", 1);
    config.agent.approval_timeout_secs = 1;
    let exec = runtime::start(&mut rt, &config).unwrap();
    hint(&mut rt, "late", "paper:muon", "h1");
    let before = fx.lab().read("train.py").unwrap();
    let pending = run_to_approval(&mut rt, exec).await;
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    run_to_end(&mut rt, exec).await;

    // The deadline passed, so the call did not run.
    assert_eq!(fx.lab().read("train.py").unwrap(), before);
    // A decision after the deadline is refused or stays unread.
    let late = runtime::apply(
        &mut rt,
        "a1",
        &Command::Approve {
            token: pending.token,
        },
    );
    assert!(late.is_err(), "{late:?}");
    assert_eq!(fx.lab().read("train.py").unwrap(), before);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn signals_a_query_and_an_update_steer_a_live_session() {
    let fx = Fixture::new();
    let mut rt = fx.open(&model());
    // Zero experiments: the session runs until a stop signal.
    let exec = runtime::start(&mut rt, &fx.config("live", 0)).unwrap();
    hint(&mut rt, "live", "paper:muon", "h1");
    let pending = run_to_approval(&mut rt, exec).await;

    // AC9: the query reads the live session.
    let status: StatusView =
        serde_json::from_value(rt.query(exec, "status", Value::Null).await.unwrap()).unwrap();
    assert_eq!(status.tag, "live");
    assert_eq!(status.phase, "experimenting");
    assert!(status.best.is_some());

    // The update answers a what-if, and its validator refuses nonsense.
    let keep = rt
        .update(exec, "whatif", json!({"val_bpb": 0.5}))
        .await
        .unwrap();
    assert_eq!(keep["status"], "keep");
    let worse = rt
        .update(exec, "whatif", json!({"val_bpb": 9.0}))
        .await
        .unwrap();
    assert_eq!(worse["status"], "discard");
    let refused = rt.update(exec, "whatif", json!({"val_bpb": -1.0})).await;
    assert!(
        matches!(refused, Err(SqliteError::UpdateRejected { .. })),
        "{refused:?}"
    );

    // Signals: a new policy, an idea, and a stop.
    let tighter = KeepPolicy {
        min_improvement: 0.0005,
        ..KeepPolicy::default()
    };
    let session = session_id("live");
    runtime::apply(
        &mut rt,
        "p1",
        &Command::Policy {
            session: session.clone(),
            policy: tighter,
        },
    )
    .unwrap();
    hint(&mut rt, "live", "try a longer warmdown", "h2");
    runtime::apply(
        &mut rt,
        "s1",
        &Command::Stop {
            session,
            reason: "morning".into(),
        },
    )
    .unwrap();
    runtime::apply(
        &mut rt,
        "a1",
        &Command::Approve {
            token: pending.token,
        },
    )
    .unwrap();
    let report = run_to_end(&mut rt, exec).await;

    assert_eq!(report.stopped.as_deref(), Some("morning"));
    assert_eq!(report.experiments, 1);
    let status: StatusView =
        serde_json::from_value(rt.query(exec, "status", Value::Null).await.unwrap()).unwrap();
    assert_eq!(status.policy, tighter);
    assert_eq!(status.phase, "done");
    assert_eq!(status.hints.len(), 1, "the unused idea stays visible");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_sessions_share_one_gpu_and_a_kept_idea() {
    let fx = Fixture::with_labs(2);
    let mut rt = fx.open(&model());
    let mut a = fx.config_for(0, "a", 2);
    a.siblings = vec![session_id("b"), session_id("ghost")];
    let mut b = fx.config_for(1, "b", 3);
    b.siblings = vec![session_id("a")];
    let exec_a = runtime::start(&mut rt, &a).unwrap();
    let exec_b = runtime::start(&mut rt, &b).unwrap();
    for _ in 0..200 {
        rt.run_until_idle().await.unwrap();
        let done = |exec| !matches!(rt.outcome(exec).unwrap(), ExecutionOutcome::Running);
        if done(exec_a) && done(exec_b) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    for exec in [exec_a, exec_b] {
        assert!(matches!(
            rt.outcome(exec).unwrap(),
            ExecutionOutcome::Completed(_)
        ));
    }

    // AC10: every training of both sessions took the one GPU key.
    let mut grants: Vec<i64> = Vec::new();
    for exec in runs_of(&rt, EXPERIMENT) {
        for event in rt.load_history(exec).unwrap() {
            if let WorkflowEvent::MutexGranted { key, lock_seq, .. } = event {
                assert_eq!(key, "gpu:gpu0");
                grants.push(lock_seq);
            }
        }
    }
    let trainings: usize = runs_of(&rt, EXPERIMENT)
        .into_iter()
        .map(|exec| {
            activities_of(&rt, exec)
                .iter()
                .filter(|a| *a == "train")
                .count()
        })
        .sum();
    assert_eq!(grants.len(), trainings);
    let mut unique = grants.clone();
    unique.sort_unstable();
    unique.dedup();
    assert_eq!(
        unique.len(),
        grants.len(),
        "each grant has its own fencing token"
    );

    // AC12: A tells both siblings about a keep. The ghost does not exist.
    let history = rt.load_history(exec_a).unwrap();
    let requested = history
        .iter()
        .filter(|e| matches!(e, WorkflowEvent::ExternalSignalRequested { .. }))
        .count();
    assert!(requested >= 2, "{history:#?}");
    assert!(
        history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ExternalSignalFailed { .. }))
    );
    let delivered = history
        .iter()
        .any(|e| matches!(e, WorkflowEvent::ExternalSignalDelivered { .. }));
    if delivered {
        let hints = signals_of(&rt, exec_b, "hint");
        assert!(
            hints.iter().any(|h| h["from"] == json!(session_id("a"))),
            "{hints:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_session_continues_as_new_and_carries_its_best() {
    let fx = Fixture::new();
    let mut rt = fx.open(&model());
    let mut config = fx.config("chain", 5);
    config.experiments_per_run = 2;
    let first = runtime::start(&mut rt, &config).unwrap();
    let report = run_to_end(&mut rt, first).await;

    // AC11: three runs carry one night.
    assert_eq!(report.experiments, 5);
    let second = rt.successor(first).unwrap().unwrap();
    let third = rt.successor(second).unwrap().unwrap();
    assert!(rt.successor(third).unwrap().is_none());
    let carried = |exec| input_of(&rt, exec)["carry"]["done"].as_u64().unwrap();
    assert_eq!((carried(second), carried(third)), (2, 4));
    assert_eq!(
        runtime::find_session(&rt, "chain").unwrap(),
        None,
        "the chain has ended"
    );
    assert_eq!(fx.lab().rows().unwrap().len(), 6);
    let verified = runtime::verify(runtime::stored_runs(&rt).unwrap()).await;
    assert_eq!(verified.failures, Vec::<String>::new());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_nightly_schedule_starts_a_session_and_the_heartbeat_speaks_up() {
    let fx = Fixture::new();
    let model = model();
    let mut rt = fx.open(&model);
    let clock = Arc::new(std::sync::Mutex::new(
        Utc.with_ymd_and_hms(2026, 10, 17, 22, 0, 0).unwrap(),
    ));
    let read = Arc::clone(&clock);
    rt.set_clock(move || *read.lock().unwrap());
    let mut config = fx.config("", 2);
    config.report = false;
    rt.upsert_schedule(&runtime::nightly(&config).unwrap())
        .unwrap();
    rt.upsert_schedule(&runtime::heartbeat(fx.lab().root()).unwrap())
        .unwrap();
    let at = |d, h, m| Utc.with_ymd_and_hms(2026, 10, d, h, m, 30).unwrap();

    // AC14: 23:00 starts the night under a tag from the slot time.
    *clock.lock().unwrap() = at(17, 23, 0);
    rt.poll_once_as_of(at(17, 23, 0)).await.unwrap();
    for _ in 0..50 {
        rt.run_until_idle().await.unwrap();
    }
    let sessions = runs_of(&rt, SESSION);
    assert_eq!(sessions.len(), 1);
    let ExecutionOutcome::Completed(output) = rt.outcome(sessions[0]).unwrap() else {
        panic!("the scheduled night did not complete");
    };
    let report: SessionReport = serde_json::from_value(output).unwrap();
    assert_eq!(report.tag, "oct17-2300");

    // A tick with nothing wrong stays silent. The interval counts from the
    // last fire, so the ticks land at 23:31 and at 00:02.
    let reports = autoresearch::stores::FileDelivery::dir(fx.lab()).unwrap();
    *clock.lock().unwrap() = at(17, 23, 31);
    rt.poll_once_as_of(at(17, 23, 31)).await.unwrap();
    rt.run_until_idle().await.unwrap();
    assert_eq!(std::fs::read_dir(&reports).unwrap().count(), 0);

    // Three crashes in a row: the next tick speaks.
    for commit in ["c000001", "c000002", "c000003"] {
        fx.lab()
            .record(&autoresearch::results::ResultRow {
                commit: commit.into(),
                val_bpb: 0.0,
                memory_gb: 0.0,
                status: Status::Crash,
                description: "broken".into(),
            })
            .unwrap();
    }
    *clock.lock().unwrap() = at(18, 0, 2);
    rt.poll_once_as_of(at(18, 0, 2)).await.unwrap();
    rt.run_until_idle().await.unwrap();
    let delivered: Vec<String> = std::fs::read_dir(&reports)
        .unwrap()
        .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
        .collect();
    assert_eq!(delivered.len(), 1);
    assert!(delivered[0].contains("ALERT"), "{delivered:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_finished_night_leaves_a_morning_report() {
    let fx = Fixture::new();
    let mut rt = fx.open(&model());
    let mut config = fx.config("report", 2);
    config.report = true;
    let exec = runtime::start(&mut rt, &config).unwrap();
    let report = run_to_end(&mut rt, exec).await;

    // AC15: the report agent is a detached child. It runs after the parent.
    let reporter = report.report_run.expect("a report agent started");
    rt.run_until_idle().await.unwrap();
    assert!(
        runs_of(&rt, "agent_loop")
            .iter()
            .any(|e| e.to_string() == reporter)
    );
    let reports = autoresearch::stores::FileDelivery::dir(fx.lab()).unwrap();
    let texts: Vec<String> = std::fs::read_dir(reports)
        .unwrap()
        .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
        .collect();
    assert_eq!(texts.len(), 1);
    assert!(texts[0].contains("Morning report"), "{texts:?}");
    assert!(texts[0].contains(&report.best.commit));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn eval_names_the_first_turn_where_a_new_program_differs() {
    let fx = Fixture::new();
    let model = model();
    let mut rt = fx.open(&model);
    let exec = runtime::start(&mut rt, &fx.config("eval", 1)).unwrap();
    run_to_end(&mut rt, exec).await;
    let proposal = runtime::proposals(&rt).unwrap()[0];
    let program = include_str!("../program.md");

    // AC19: the same program does not diverge.
    let history = rt.load_history(proposal).unwrap();
    let same = runtime::eval(&history, fx.harness(&model), program)
        .await
        .unwrap();
    assert!(!same.diverged(), "{same:#?}");
    // A program that prefers deletions picks another first edit.
    let candidate = format!("{program}\n{PREFER_SIMPLER}\n");
    let other = runtime::eval(&history, fx.harness(&model), &candidate)
        .await
        .unwrap();
    assert!(other.first_divergence.is_some(), "{other:#?}");
    assert_eq!(fx.lab().rows().unwrap().len(), 2, "an eval records no row");
}

#[test]
fn with_program_keeps_the_session_context() {
    let system = format!(
        "old program\n{}\nbest: 0.99",
        autoresearch::workflows::CONTEXT_MARKER
    );
    let swapped = with_program(&system, "new program");
    assert!(swapped.starts_with("new program"));
    assert!(swapped.ends_with("best: 0.99"));
    assert!(!swapped.contains("old program"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_response_cache_serves_an_identical_report_turn() {
    let fx = Fixture::new();
    let model = model();
    let mut rt = fx.open(&model);
    let exec = runtime::start(&mut rt, &fx.config("cache", 1)).unwrap();
    run_to_end(&mut rt, exec).await;

    // AC20: two report runs over the same results send the same requests.
    let task: AgentTask = agent::report_task(&fx.lab().root().display().to_string(), "cache");
    let first = sqlite::start(&mut rt, &task).unwrap();
    rt.run_until_blocked(first).await.unwrap();
    let calls = model.calls();
    let second = sqlite::start(&mut rt, &task).unwrap();
    rt.run_until_blocked(second).await.unwrap();
    assert_eq!(model.calls(), calls, "the second run asked no model");
    let hits = rt
        .load_history(second)
        .unwrap()
        .into_iter()
        .filter(|e| {
            matches!(e, WorkflowEvent::ActivityCompleted { output, .. }
            if output.get("cache_hit") == Some(&json!(true)))
        })
        .count();
    assert!(hits >= 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_notes_survive_a_restart() {
    let fx = Fixture::new();
    let model = model();
    {
        let mut rt = fx.open(&model);
        let exec = runtime::start(&mut rt, &fx.config("mem", 2)).unwrap();
        run_to_end(&mut rt, exec).await;
    }
    // AC8: a new process reads the notes of the tag.
    let harness = fx.harness(&model);
    let snapshot = harness
        .memory_snapshot(MemoryScope::new(session_id("mem")))
        .await
        .unwrap();
    assert!(snapshot.contains("tried raise MATRIX_LR"), "{snapshot}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_inbox_delivers_a_command_once() {
    let fx = Fixture::new();
    let mut rt = fx.open(&model());
    let exec = runtime::start(&mut rt, &fx.config("inbox", 1)).unwrap();
    let inbox = Inbox::new(&fx.state).unwrap();
    let id = inbox
        .post(&Command::Hint {
            session: session_id("inbox"),
            text: "try a longer warmdown".into(),
        })
        .unwrap();
    // AC21: a crash between the signal and the delete delivers it twice.
    for (id, command) in inbox.pending().unwrap() {
        runtime::apply(&mut rt, &id, &command).unwrap();
        runtime::apply(&mut rt, &id, &command).unwrap();
    }
    inbox.done(&id).unwrap();
    run_to_end(&mut rt, exec).await;
    assert_eq!(signals_of(&rt, exec, "hint").len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_command_trainer_runs_a_real_command_per_commit() {
    let fx = Fixture::new();
    let mut rt = fx.open(&model());
    let mut config = fx.config("cmd", 1);
    // AC22: a stand-in for `uv run train.py` that reads `train.py`.
    config.trainer = TrainerSpec::Command {
        program: "sh".into(),
        args: vec![
            "-c".into(),
            "if grep -q 'MATRIX_LR = 0.05' train.py; then v=0.990000; else v=1.000000; fi; \
             echo '---'; echo \"val_bpb:          $v\"; echo 'peak_vram_mb:     2048.0'"
                .into(),
        ],
        timeout_secs: 30,
    };
    let exec = runtime::start(&mut rt, &config).unwrap();
    run_to_end(&mut rt, exec).await;
    let rows = fx.lab().rows().unwrap();
    assert_eq!(rows[0].val_bpb, 1.0);
    assert_eq!(rows[1].val_bpb, 0.99);
    assert_eq!(rows[1].memory_gb, 2.0);
    assert_eq!(rows[1].status, Status::Keep);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn setup_refuses_missing_data_and_a_tag_in_use() {
    let fx = Fixture::new();
    let mut rt = fx.open(&model());
    let mut config = fx.config("nodata", 1);
    config.data_dir = Some(fx.dir.path().join("missing").display().to_string());
    let exec = runtime::start(&mut rt, &config).unwrap();
    match runtime::drive(&mut rt, exec).await.unwrap() {
        Step::Failed(error) => assert!(
            error.contains("DataMissing") || error.contains("data"),
            "{error}"
        ),
        other => panic!("expected a failed session, got {other:?}"),
    }
    // Upstream: the branch for a tag must not exist. A used tag is refused.
    assert!(runtime::start(&mut rt, &fx.config("nodata", 1)).is_err());
    assert!(runtime::start(&mut rt, &fx.config("", 1)).is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn purge_removes_old_finished_runs() {
    let fx = Fixture::new();
    let mut rt = fx.open(&model());
    let exec = runtime::start(&mut rt, &fx.config("old", 1)).unwrap();
    run_to_end(&mut rt, exec).await;
    // AC25: nothing is old enough yet.
    assert_eq!(
        rt.purge_terminal(Duration::from_secs(30 * 86_400)).unwrap(),
        0
    );
    let later = Utc::now() + chrono::Duration::days(31);
    let removed = rt
        .purge_terminal_as_of(later, Duration::from_secs(30 * 86_400))
        .unwrap();
    assert!(removed >= 3, "{removed}");
    assert_eq!(runs_of(&rt, SESSION), Vec::new());
}

#[test]
fn the_workflows_register_on_a_postgres_builder_with_an_llm_budget() {
    let fx = Fixture::new();
    // AC24: the same code builds the Postgres registration.
    let built = runtime::postgres(fx.harness(&model()), &fx.config("", 1)).unwrap();
    let names: Vec<&str> = built.workflow_infos().iter().map(|w| w.name).collect();
    for name in [SESSION, EXPERIMENT, "agent_loop", "agent_heartbeat"] {
        assert!(names.contains(&name), "{names:?}");
    }
    let agent_loop = built
        .workflow_infos()
        .iter()
        .find(|w| w.name == "agent_loop")
        .unwrap();
    assert!(
        agent_loop.quota.is_some(),
        "agent_loop carries the LLM budget"
    );
    assert!(
        built
            .state::<autumn_harvest_agent::AgentHarness>()
            .is_some()
    );
    assert_eq!(built.workflow_schedules().len(), 2);
}
