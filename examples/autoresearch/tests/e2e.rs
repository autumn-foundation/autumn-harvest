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
use autoresearch::results::short;
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
            tag: tag.into(),
            text: text.into(),
        },
    )
    .unwrap();
}

/// Drive `exec` to its end and approve each gated call on the way. Returns
/// the gated calls in order.
async fn run_approving(
    rt: &mut autumn_harvest_sqlite::SqliteRuntime,
    mut exec: autumn_harvest_sqlite::ExecutionId,
) -> Vec<runtime::Pending> {
    let mut asked = Vec::new();
    for _ in 0..10_000 {
        match runtime::drive(rt, exec).await.unwrap() {
            Step::Done(_) => return asked,
            Step::Continued(next) => exec = next,
            Step::Waiting | Step::Mutex(_) => tokio::time::sleep(Duration::from_millis(20)).await,
            Step::Approval(pending) => {
                let token = pending.token.clone();
                asked.push(pending);
                runtime::apply(
                    rt,
                    &format!("a{}", asked.len()),
                    &Command::Approve { token },
                )
                .unwrap();
            }
            Step::Failed(error) => panic!("the session failed: {error}"),
        }
    }
    panic!("the session did not end")
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

    // AC2: the branch holds the baseline and the kept commits only. The
    // baseline is the start commit of the session.
    assert_eq!(fx.lab().head().unwrap(), report.best.commit);
    assert_eq!(
        fx.lab()
            .git(&["rev-list", "--count", "master..HEAD"])
            .unwrap(),
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
    run_to_end(&mut rt, exec).await;

    let outputs = experiment_outputs(&rt);
    // Baseline, then the out-of-memory idea, then the diverging one.
    assert_eq!(outputs.len(), 3);
    let base = outputs[0].best.as_ref().unwrap().commit.clone();
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
    // The fixed run lands, and the scripted night discards it.
    assert_eq!(oom.row.status, Status::Discard, "{oom:?}");
    assert_eq!(fx.lab().head().unwrap(), base);
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
    let control_asked = run_approving(&mut rt, exec).await;
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
    runtime::apply(&mut rt, "first", &Command::Approve { token }).unwrap();
    let asked = run_approving(&mut rt, exec).await;
    assert_eq!(asked.len() + 1, control_asked.len());

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
    // A row with no run holds the numbers of its base, not zeros.
    assert_eq!(
        (rows[1].val_bpb, rows[1].memory_gb),
        (rows[0].val_bpb, rows[0].memory_gb)
    );
    assert_eq!(fx.lab().read("train.py").unwrap(), before);
    // The injected order in the paper never reached `train.py`.
    assert!(!fx.lab().read("train.py").unwrap().contains("4096"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_approved_rewind_moves_the_base_to_an_older_kept_commit() {
    let fx = Fixture::new();
    let mut rt = fx.open(&model());
    let exec = runtime::start(&mut rt, &fx.config("rw", 2)).unwrap();
    hint(&mut rt, "rw", "paper:warmdown", "h1");

    let edit = run_to_approval(&mut rt, exec).await;
    assert_eq!(edit.tool, agent::EDIT_TRAIN);
    // The edit waits, so `HEAD` is still the baseline: the start commit of
    // the session. A person copies its short form from results.tsv.
    let baseline = fx.lab().head().unwrap();
    hint(&mut rt, "rw", &format!("rewind:{}", short(&baseline)), "h2");
    runtime::apply(&mut rt, "a1", &Command::Approve { token: edit.token }).unwrap();
    let rewind = run_to_approval(&mut rt, exec).await;
    assert_eq!(rewind.tool, agent::REWIND);
    assert_eq!(rewind.arguments["commit"], json!(short(&baseline)));
    runtime::apply(
        &mut rt,
        "a2",
        &Command::Approve {
            token: rewind.token,
        },
    )
    .unwrap();
    // The paper taints the rest of the session, so each later edit asks.
    let later = run_approving(&mut rt, exec).await;
    assert!(
        !later.is_empty() && later.iter().all(|p| p.tool == agent::EDIT_TRAIN),
        "{later:?}"
    );

    let outputs = experiment_outputs(&rt);
    assert_eq!(outputs[1].row.status, Status::Keep, "{:?}", outputs[1]);
    let second = &outputs[2];
    assert!(second.rewound, "{second:?}");
    assert_eq!(second.row.status, Status::Keep, "{second:?}");
    {
        // The kept commit sits on the baseline, not on the first keep.
        let kept = &second.best.as_ref().unwrap().commit;
        assert_eq!(short(kept), second.row.commit);
        let parent = fx.lab().git(&["rev-parse", &format!("{kept}^")]).unwrap();
        assert_eq!(parent, baseline);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_late_approval_never_releases_the_call() {
    let fx = Fixture::new();
    let mut rt = fx.open(&model());
    let clock = Arc::new(std::sync::Mutex::new(Utc::now()));
    let read = Arc::clone(&clock);
    rt.set_clock(move || *read.lock().unwrap());
    let exec = runtime::start(&mut rt, &fx.config("late", 1)).unwrap();
    hint(&mut rt, "late", "paper:muon", "h1");
    let before = fx.lab().read("train.py").unwrap();
    let pending = run_to_approval(&mut rt, exec).await;
    // The deadline passes: 120 seconds in this fixture.
    *clock.lock().unwrap() += chrono::Duration::seconds(121);
    run_to_end(&mut rt, exec).await;

    // The call did not run, and the agent read that no approval came.
    let rows = fx.lab().rows().unwrap();
    assert!(
        rows[1].description.starts_with("no change") && rows[1].description.contains("no approval"),
        "{rows:?}"
    );
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
    runtime::apply(
        &mut rt,
        "p1",
        &Command::Policy {
            tag: "live".into(),
            policy: tighter,
        },
    )
    .unwrap();
    hint(&mut rt, "live", "try a longer warmdown", "h2");
    runtime::apply(
        &mut rt,
        "s1",
        &Command::Stop {
            tag: "live".into(),
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
    assert!(delivered, "the hint reached B");
    {
        let hints = signals_of(&rt, exec_b, "hint");
        assert!(
            hints.iter().any(|h| h["from"] == json!(session_id("a"))),
            "{hints:?}"
        );
    }
}

/// `continue_as_new` gives a session a chain of runs with one workflow id.
/// `status` shows each session once, by the newest run of its chain, so a
/// long session cannot crowd another one out.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn status_shows_each_session_once() {
    let fx = Fixture::new();
    let mut rt = fx.open(&model());
    let other = runtime::start(&mut rt, &fx.config("other", 1)).unwrap();
    run_to_end(&mut rt, other).await;
    let mut config = fx.config("chain", 5);
    config.experiments_per_run = 2;
    let first = runtime::start(&mut rt, &config).unwrap();
    run_to_end(&mut rt, first).await;
    let sessions = runtime::latest_sessions(|filter| rt.list_executions(filter), 2).unwrap();
    let shown: Vec<(String, String)> = sessions
        .iter()
        .map(|run| {
            (
                run.workflow_id.clone().into_value().unwrap_or_default(),
                run.state.clone().into_value().unwrap_or_default(),
            )
        })
        .collect();
    assert_eq!(
        shown,
        vec![
            ("autoresearch/chain".to_string(), "COMPLETED".to_string()),
            ("autoresearch/other".to_string(), "COMPLETED".to_string()),
        ]
    );
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
    // Each hand-off carries the best after its experiments: the last keep.
    let rows = fx.lab().rows().unwrap();
    let best_after = |done: usize| {
        rows[..=done]
            .iter()
            .rev()
            .find(|row| row.status == Status::Keep)
            .unwrap()
            .commit
            .clone()
    };
    let carried_best = |exec| {
        input_of(&rt, exec)["carry"]["best"]["commit"]
            .as_str()
            .unwrap()
            .to_string()
    };
    // The carry holds the full id. The rows hold its short form.
    assert_eq!(short(&carried_best(second)), best_after(2));
    assert_eq!(short(&carried_best(third)), best_after(4));
    assert_eq!(short(&report.best.commit), best_after(5));
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
    assert_eq!(report.tag, "20261017-2300");

    // A tick with nothing wrong stays silent. The interval counts from the
    // last fire, so the ticks land at 23:31 and at 00:02.
    let reports = autoresearch::stores::FileDelivery::dir(fx.lab()).unwrap();
    *clock.lock().unwrap() = at(17, 23, 31);
    rt.poll_once_as_of(at(17, 23, 31)).await.unwrap();
    rt.run_until_idle().await.unwrap();
    assert_eq!(std::fs::read_dir(&reports).unwrap().count(), 0);

    // Three crashes in a row: the next tick speaks.
    for commit in ["c000001", "c000002", "c000003"] {
        let row = autoresearch::results::ResultRow {
            commit: commit.into(),
            val_bpb: 0.0,
            memory_gb: 0.0,
            status: Status::Crash,
            description: "broken".into(),
        };
        fx.lab().record(&row, commit).unwrap();
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
    // Another session in the database, which no runner drives now.
    let other = runtime::start(&mut rt, &fx.config("other", 2)).unwrap();

    // AC15: the report agent is a detached child. It runs after the parent.
    let reporter = report.report_run.expect("a report agent started");
    runtime::drive_report(&mut rt, &reporter).await.unwrap();
    assert!(
        activities_of(&rt, other).is_empty(),
        "the report drives no other session"
    );
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
    assert!(texts[0].contains(&short(&report.best.commit)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn eval_names_the_first_turn_where_a_new_program_differs() {
    let fx = Fixture::new();
    let model = model();
    let mut rt = fx.open(&model);
    let exec = runtime::start(&mut rt, &fx.config("eval", 1)).unwrap();
    run_to_end(&mut rt, exec).await;
    let proposal = runtime::latest_proposal(&rt).unwrap().unwrap();
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
    let lab = fx.lab().root().display().to_string();
    let task: AgentTask = agent::report_task(&lab, "cache", "1 experiments, 1 kept");
    let calls = {
        let mut rt = fx.open(&model);
        let exec = runtime::start(&mut rt, &fx.config("cache", 1)).unwrap();
        run_to_end(&mut rt, exec).await;
        // AC20: two report runs over the same results send the same requests.
        let first = sqlite::start(&mut rt, &task).unwrap();
        rt.run_until_blocked(first).await.unwrap();
        model.calls()
    };
    // A new process: a turn whose result was lost in a crash asks again,
    // and the cache on disk answers it.
    let mut rt = fx.open(&model);
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
            tag: "inbox".into(),
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
        pass_env: Vec::new(),
    };
    let exec = runtime::start(&mut rt, &config).unwrap();
    run_to_end(&mut rt, exec).await;
    let rows = fx.lab().rows().unwrap();
    assert_eq!(rows[0].val_bpb, 1.0);
    assert_eq!(rows[1].val_bpb, 0.99);
    assert_eq!(rows[1].memory_gb, 2.0);
    assert_eq!(rows[1].status, Status::Keep);
}

/// A command run is model-written code. Whatever started the runner, the
/// run must not read the key from `/proc/<runner>/environ`. This binary
/// never runs the CLI, so only the trainer can hide the runner here.
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_command_run_hides_the_runner() {
    let fx = Fixture::new();
    let mut rt = fx.open(&model());
    let mut config = fx.config("hidden", 1);
    config.trainer = TrainerSpec::Command {
        program: "sh".into(),
        args: vec![
            "-c".into(),
            "echo '---'; echo 'val_bpb:          1.000000'; echo 'peak_vram_mb:     2048.0'".into(),
        ],
        timeout_secs: 30,
        pass_env: Vec::new(),
    };
    let exec = runtime::start(&mut rt, &config).unwrap();
    run_to_end(&mut rt, exec).await;
    assert_eq!(
        rustix::process::dumpable_behavior().unwrap(),
        rustix::process::DumpableBehavior::NotDumpable
    );
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

/// The Postgres registration builds its schedules from the config, not
/// through `start`. Each schedule must name the lab by its canonical path.
#[cfg(unix)]
#[test]
fn the_schedules_name_the_lab_by_its_canonical_path() {
    let fx = Fixture::new();
    let link = fx.lab().root().with_file_name("link");
    std::os::unix::fs::symlink(fx.lab().root(), &link).unwrap();
    let mut config = fx.config("", 1);
    config.lab = link.display().to_string();
    let real = fx.lab().root().display().to_string();
    let built = runtime::postgres(fx.harness(&model()), &config).unwrap();
    for schedule in built.workflow_schedules() {
        let input = schedule.input.to_string();
        assert!(input.contains(&real), "{input}");
        assert!(!input.contains(&config.lab), "{input}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_paper_taints_every_later_run_of_the_session() {
    let fx = Fixture::new();
    let mut rt = fx.open(&model());
    let exec = runtime::start(&mut rt, &fx.config("taint", 2)).unwrap();
    hint(&mut rt, "taint", "paper:muon", "h1");
    let before = fx.lab().read("train.py").unwrap();

    // AC6: a person denies the edit after the paper. The memory note of
    // that run still lands.
    let first = run_to_approval(&mut rt, exec).await;
    let deny = |token: String| Command::Deny {
        token,
        reason: "papers are not trusted tonight".into(),
    };
    runtime::apply(&mut rt, "d1", &deny(first.token)).unwrap();

    // The next experiment has no hint and reads no paper. It still asks,
    // because its memory and its prompt hold text from the paper.
    let second = run_to_approval(&mut rt, exec).await;
    assert_eq!(second.tool, agent::EDIT_TRAIN);
    runtime::apply(&mut rt, "d2", &deny(second.token)).unwrap();
    run_to_end(&mut rt, exec).await;

    assert_eq!(fx.lab().read("train.py").unwrap(), before);
    let outputs = experiment_outputs(&rt);
    let label = autumn_harvest_agent::flow::labels::UNTRUSTED_INPUT;
    assert!(
        outputs[0].flow_labels.is_empty(),
        "the baseline reads nothing"
    );
    assert!(outputs[1..].iter().all(|o| o.flow_labels.contains(label)));
    let agents = runs_of(&rt, "agent_loop");
    let labelled = agents
        .iter()
        .filter(|exec| {
            input_of(&rt, **exec)["flow_labels"]
                .as_array()
                .is_some_and(|labels| labels.contains(&json!(label)))
        })
        .count();
    assert!(labelled >= 1, "the second proposal starts with the label");
}

/// A nightly session that waits for its lab takes commands by its tag.
/// Session A holds the lab while its edit waits for a person, so the
/// nightly session waits for the lab before its setup.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_nightly_session_that_waits_for_its_lab_takes_commands_by_its_tag() {
    let fx = Fixture::new();
    let mut rt = fx.open(&model());
    let clock = Arc::new(std::sync::Mutex::new(
        Utc.with_ymd_and_hms(2026, 10, 17, 22, 59, 50).unwrap(),
    ));
    let read = Arc::clone(&clock);
    rt.set_clock(move || *read.lock().unwrap());
    rt.upsert_schedule(&runtime::nightly(&fx.config("", 0)).unwrap())
        .unwrap();
    let exec_a = runtime::start(&mut rt, &fx.config("a", 2)).unwrap();
    hint(&mut rt, "a", "paper:warmdown", "h1");
    run_to_approval(&mut rt, exec_a).await;
    // Forty seconds on: the edit of A still waits, and A holds the lab.
    let slot = Utc.with_ymd_and_hms(2026, 10, 17, 23, 0, 30).unwrap();
    *clock.lock().unwrap() = slot;
    rt.poll_once_as_of(slot).await.unwrap();
    rt.run_until_idle().await.unwrap();
    let night = runs_of(&rt, SESSION)
        .into_iter()
        .find(|exec| *exec != exec_a)
        .unwrap();
    assert!(
        activities_of(&rt, night).is_empty(),
        "the night waits for the lab before its setup"
    );
    assert_eq!(
        runtime::find_session(&rt, "20261017-2300")
            .unwrap()
            .map(|(exec, _)| exec),
        Some(night)
    );
    hint(&mut rt, "20261017-2300", "try a longer warmdown", "h2");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_sessions_in_one_lab_take_turns_on_their_own_branches() {
    let fx = Fixture::new();
    let mut rt = fx.open(&model());
    let exec_a = runtime::start(&mut rt, &fx.config("a", 2)).unwrap();
    let exec_b = runtime::start(&mut rt, &fx.config("b", 2)).unwrap();
    for _ in 0..400 {
        rt.run_until_idle().await.unwrap();
        let done = |exec| !matches!(rt.outcome(exec).unwrap(), ExecutionOutcome::Running);
        if done(exec_a) && done(exec_b) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    for exec in [exec_a, exec_b] {
        assert!(
            matches!(rt.outcome(exec).unwrap(), ExecutionOutcome::Completed(_)),
            "{:?}",
            rt.outcome(exec)
        );
    }
    // AC29: each experiment committed on its own branch, and the lab is
    // clean. A kept commit is on its branch. A discarded one is on none.
    assert_eq!(fx.lab().rows().unwrap().len(), 6);
    assert_eq!(fx.lab().git(&["status", "--porcelain"]).unwrap(), "");
    for exec in runs_of(&rt, EXPERIMENT) {
        let input = input_of(&rt, exec);
        let branch = input["branch"].as_str().unwrap().to_string();
        let ExecutionOutcome::Completed(output) = rt.outcome(exec).unwrap() else {
            panic!("an experiment did not complete");
        };
        let outcome: ExperimentOutcome = serde_json::from_value(output).unwrap();
        let on = |b: &str| {
            fx.lab()
                .git(&["merge-base", "--is-ancestor", &outcome.row.commit, b])
                .is_ok()
        };
        match outcome.row.status {
            Status::Keep => assert!(on(&branch), "{outcome:?} on {branch}"),
            _ if !input["baseline"].as_bool().unwrap() && outcome.fixes == 0 => {
                if outcome.row.description.starts_with("no change") {
                    continue;
                }
                assert!(
                    !on("autoresearch/a") && !on("autoresearch/b"),
                    "{outcome:?}"
                );
            }
            _ => {}
        }
    }
    // The sessions held the lab mutex for each experiment.
    let lab_key = format!("lab:{}", fx.lab().root().display());
    for exec in [exec_a, exec_b] {
        let grants = rt
            .load_history(exec)
            .unwrap()
            .into_iter()
            .filter(|e| matches!(e, WorkflowEvent::MutexGranted { key, .. } if *key == lab_key))
            .count();
        assert_eq!(grants, 3, "the baseline and two experiments");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_nightly_session_takes_commands_by_its_tag() {
    let fx = Fixture::new();
    let mut rt = fx.open(&model());
    let clock = Arc::new(std::sync::Mutex::new(
        Utc.with_ymd_and_hms(2026, 10, 17, 22, 0, 0).unwrap(),
    ));
    let read = Arc::clone(&clock);
    rt.set_clock(move || *read.lock().unwrap());
    // Zero experiments: the night runs until a stop.
    rt.upsert_schedule(&runtime::nightly(&fx.config("", 0)).unwrap())
        .unwrap();
    let slot = Utc.with_ymd_and_hms(2026, 10, 17, 23, 0, 30).unwrap();
    *clock.lock().unwrap() = slot;
    rt.poll_once_as_of(slot).await.unwrap();
    let first = runs_of(&rt, SESSION)[0];
    let night = drive_a_little(&mut rt, first).await;

    // AC30: the engine picked the workflow id, and the tag still reaches it.
    let (found, id) = runtime::find_session(&rt, "20261017-2300")
        .unwrap()
        .unwrap();
    assert_eq!(found, night);
    assert!(!id.starts_with("autoresearch/"), "{id}");
    let stop = Command::Stop {
        tag: "20261017-2300".into(),
        reason: "morning".into(),
    };
    runtime::apply(&mut rt, "s1", &stop).unwrap();
    let report = run_to_end(&mut rt, night).await;
    assert_eq!(report.stopped.as_deref(), Some("morning"));
    // A command for a tag that no session runs can never land.
    assert!(matches!(
        runtime::apply(&mut rt, "s2", &stop),
        Err(runtime::Refused::Permanent(_))
    ));
}

/// A keep policy that a signal could not carry cannot start a session.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_session_refuses_an_invalid_keep_policy() {
    let fx = Fixture::new();
    let mut rt = fx.open(&model());
    let mut config = fx.config("bad", 1);
    config.policy.line_cost = -1.0;
    assert!(runtime::start(&mut rt, &config).is_err());
    // A caller that starts the workflow itself meets the same check.
    let input = serde_json::to_value(autoresearch::workflows::SessionInput {
        config,
        carry: None,
    })
    .unwrap();
    let started = rt
        .start_workflow_with_options(SESSION, input, autumn_harvest_sqlite::StartOptions::new())
        .unwrap();
    let step = runtime::drive(&mut rt, started.exec_id).await.unwrap();
    assert!(matches!(step, Step::Failed(_)), "{step:?}");
    assert!(
        activities_of(&rt, started.exec_id).is_empty(),
        "no setup ran"
    );
}

/// A report run that fails is an error, so `run` does not print a report
/// that was never written.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_report_run_is_an_error() {
    let fx = Fixture::new();
    let mut rt = fx.open(&model());
    // An input that does not decode fails the run at once.
    let broken = rt
        .start_workflow_with_options(
            EXPERIMENT,
            json!({"not": "an experiment"}),
            autumn_harvest_sqlite::StartOptions::new(),
        )
        .unwrap();
    let failed = runtime::drive_report(&mut rt, &broken.exec_id.to_string()).await;
    assert!(failed.is_err(), "{failed:?}");
}

/// A run that the schedule started, and that has not run its first cycle,
/// has no tag yet. A process that dies there leaves it so. A command for
/// its tag waits, and lands once the session publishes the tag.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_command_waits_for_a_session_that_has_no_tag_yet() {
    let fx = Fixture::new();
    let mut rt = fx.open(&model());
    let slot = Utc.with_ymd_and_hms(2026, 10, 17, 23, 0, 30).unwrap();
    rt.set_clock(move || slot);
    let input = serde_json::to_value(autoresearch::workflows::SessionInput {
        config: fx.config("", 0),
        carry: None,
    })
    .unwrap();
    let started = rt
        .start_workflow_with_options(
            SESSION,
            input,
            autumn_harvest_sqlite::StartOptions::new().workflow_id("sched:nightly:1"),
        )
        .unwrap();
    let stop = Command::Stop {
        tag: "20261017-2300".into(),
        reason: "morning".into(),
    };
    assert!(matches!(
        runtime::apply(&mut rt, "s1", &stop),
        Err(runtime::Refused::Transient(_))
    ));
    let night = drive_a_little(&mut rt, started.exec_id).await;
    runtime::apply(&mut rt, "s1", &stop).unwrap();
    let report = run_to_end(&mut rt, night).await;
    assert_eq!(report.stopped.as_deref(), Some("morning"));
}

/// Drive a night that has no end for a few drives. Returns its run now.
async fn drive_a_little(
    rt: &mut autumn_harvest_sqlite::SqliteRuntime,
    mut exec: autumn_harvest_sqlite::ExecutionId,
) -> autumn_harvest_sqlite::ExecutionId {
    for _ in 0..5 {
        match runtime::drive(rt, exec).await.unwrap() {
            Step::Waiting | Step::Mutex(_) => tokio::time::sleep(Duration::from_millis(20)).await,
            Step::Continued(next) => exec = next,
            other => panic!("the night ended early: {other:?}"),
        }
    }
    exec
}

/// The session pauses on a durable timer before each experiment. The pause
/// ends the drive, so the runner delivers its inbox between two experiments
/// and not only when the session continues as new.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_command_lands_before_the_next_experiment() {
    let fx = Fixture::new();
    let mut rt = fx.open(&model());
    // Zero experiments: the night runs until a stop.
    let mut config = fx.config("paced", 0);
    config.experiments_per_run = 50;
    config.pace_secs = 1;
    let exec = runtime::start(&mut rt, &config).unwrap();
    let first = runtime::drive(&mut rt, exec).await.unwrap();
    assert_eq!(first, Step::Waiting, "the drive stops at the first pause");
    let status: StatusView =
        serde_json::from_value(rt.query(exec, "status", Value::Null).await.unwrap()).unwrap();
    assert_eq!(status.experiments, 0, "the baseline ran, no experiment yet");
    let stop = Command::Stop {
        tag: "paced".into(),
        reason: "morning".into(),
    };
    runtime::apply(&mut rt, "s1", &stop).unwrap();
    let report = run_to_end(&mut rt, exec).await;
    assert_eq!(report.stopped.as_deref(), Some("morning"));
    assert_eq!(report.experiments, 0);
}

/// `serve` names each session that ends, once. A night that fails is not
/// silent. A run that continues as new is not an end.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_ended_session_is_named_once() {
    let fx = Fixture::new();
    let mut rt = fx.open(&model());
    let mut seen = std::collections::HashSet::new();
    let mut config = fx.config("two", 2);
    config.experiments_per_run = 1;
    let exec = runtime::start(&mut rt, &config).unwrap();
    run_to_end(&mut rt, exec).await;
    let ended = runtime::newly_ended(&rt, &mut seen).unwrap();
    assert_eq!(ended.len(), 1, "{ended:?}");
    assert_eq!(ended[0].0, session_id("two"));
    assert!(
        matches!(&ended[0].1, runtime::Ended::Done(report) if report.experiments == 2),
        "{ended:?}"
    );
    assert_eq!(runtime::newly_ended(&rt, &mut seen).unwrap().len(), 0);

    let mut config = fx.config("nodata", 1);
    config.data_dir = Some(fx.dir.path().join("missing").display().to_string());
    let exec = runtime::start(&mut rt, &config).unwrap();
    let _ = runtime::drive(&mut rt, exec).await.unwrap();
    let ended = runtime::newly_ended(&rt, &mut seen).unwrap();
    assert!(
        matches!(ended.as_slice(), [(id, runtime::Ended::Failed(why))]
            if *id == session_id("nodata") && why.contains("data")),
        "{ended:?}"
    );
    assert_eq!(runtime::newly_ended(&rt, &mut seen).unwrap().len(), 0);
}

/// A session that waits for a mutex says so. Here another session holds
/// the lab while it waits for a person, and only its own runner can free it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_drive_names_the_mutex_that_a_session_waits_for() {
    let fx = Fixture::new();
    let mut rt = fx.open(&model());
    let holder = runtime::start(&mut rt, &fx.config("holder", 0)).unwrap();
    // A paper taints the run, so its next write waits for a person.
    hint(&mut rt, "holder", "paper:muon", "h1");
    run_to_approval(&mut rt, holder).await;
    let waiter = runtime::start(&mut rt, &fx.config("waiter", 1)).unwrap();
    let step = runtime::drive(&mut rt, waiter).await.unwrap();
    assert!(
        matches!(&step, Step::Mutex(key) if key.starts_with("lab:")),
        "{step:?}"
    );
}

/// History records the model of a session. A resume with another model is
/// refused, so a Claude night does not go on with the scripted researcher.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_session_resumes_only_with_its_own_model() {
    let fx = Fixture::new();
    let mut rt = fx.open(&model());
    let mut config = fx.config("gpu", 3);
    config.experiments_per_run = 1;
    config.model = "claude:claude-x".into();
    let exec = runtime::start(&mut rt, &config).unwrap();
    let next = match runtime::drive(&mut rt, exec).await.unwrap() {
        Step::Continued(next) => next,
        other => panic!("expected a hand-off, got {other:?}"),
    };
    for run in [exec, next] {
        runtime::check_model(&rt, run, "claude:claude-x").unwrap();
        let refused = runtime::check_model(&rt, run, "scripted").unwrap_err();
        assert!(refused.contains("claude:claude-x"), "{refused}");
    }
    // A session from before the model was recorded resumes with any model.
    let old = runtime::start(&mut rt, &fx.config("old", 1)).unwrap();
    runtime::check_model(&rt, old, "scripted").unwrap();
}

/// `serve` drives every unfinished run with its own model. A report agent
/// whose session ended is checked against the model of that session. A
/// heartbeat run is checked against the model of the `serve` that started
/// it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serve_checks_the_model_of_each_unfinished_agent_run() {
    let fx = Fixture::new();
    let mut rt = fx.open(&model());
    let mut config = fx.config("night", 1);
    config.report = true;
    config.model = "claude:claude-x".into();
    let exec = runtime::start(&mut rt, &config).unwrap();
    let report = run_to_end(&mut rt, exec).await;
    assert!(report.report_run.is_some(), "the report agent waits");
    let refused = runtime::check_agents(&rt, "scripted", None).unwrap_err();
    assert!(refused.contains("claude:claude-x"), "{refused}");
    runtime::check_agents(&rt, "claude:claude-x", None).unwrap();

    // A heartbeat run that an earlier `serve` started and did not finish.
    let fx = Fixture::new();
    let mut rt = fx.open(&model());
    let schedule = runtime::heartbeat(fx.labs[0].root()).unwrap();
    rt.start_workflow(&schedule.workflow_name, schedule.input.clone())
        .unwrap();
    let refused = runtime::check_agents(&rt, "claude:claude-x", Some("scripted")).unwrap_err();
    assert!(refused.contains("scripted"), "{refused}");
    runtime::check_agents(&rt, "scripted", Some("scripted")).unwrap();
    // A database from before the record: nothing to check against.
    runtime::check_agents(&rt, "claude:claude-x", None).unwrap();
}

/// `run` must not start a session while an unfinished run has another
/// model. Each would wait for a runner with its own model, and `serve`
/// could finish neither.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_refuses_while_a_report_of_another_model_is_unfinished() {
    let fx = Fixture::new();
    {
        let mut rt = fx.open(&model());
        let mut config = fx.config("night", 1);
        config.report = true;
        config.model = "claude:claude-x".into();
        let exec = runtime::start(&mut rt, &config).unwrap();
        let report = run_to_end(&mut rt, exec).await;
        assert!(report.report_run.is_some(), "the report agent waits");
    }
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_autoresearch"))
        .args(["run", "--lab", &fx.labs[0].root().display().to_string()])
        .args(["--db", &fx.db.display().to_string()])
        .args(["--tag", "next", "--experiments", "1", "--no-report"])
        .env_remove("ANTHROPIC_API_KEY")
        .env_remove("AUTORESEARCH_LAB")
        .env_remove("AUTORESEARCH_DB")
        .output()
        .unwrap();
    let why = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success(), "{why}");
    assert!(
        why.contains("claude:claude-x") && why.contains("serve"),
        "{why}"
    );
}

/// A caller can name the lab through a link. `start` makes the path
/// canonical, so the night is the night of the real path.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_session_started_through_a_link_to_its_lab_runs() {
    let plain = Fixture::new();
    let mut rt = plain.open(&model());
    let exec = runtime::start(&mut rt, &plain.config("night", 3)).unwrap();
    let want = run_to_end(&mut rt, exec).await;

    let fx = Fixture::new();
    let mut rt = fx.open(&model());
    let link = fx.lab().root().with_file_name("link");
    std::os::unix::fs::symlink(fx.lab().root(), &link).unwrap();
    let mut config = fx.config("night", 3);
    config.lab = link.display().to_string();
    let exec = runtime::start(&mut rt, &config).unwrap();
    let got = run_to_end(&mut rt, exec).await;
    assert_eq!(got.counts, want.counts);
    assert_eq!(got.experiments, want.experiments);
}

/// A purge would remove an ended session while its report agent runs on, and
/// with the session the model that the report needs. So `purge` refuses
/// until the report ends. A window that keeps the session purges as usual.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_purge_refuses_to_remove_a_session_whose_report_runs() {
    let fx = Fixture::new();
    let mut rt = fx.open(&model());
    let mut config = fx.config("night", 1);
    config.report = true;
    let exec = runtime::start(&mut rt, &config).unwrap();
    let reporter = run_to_end(&mut rt, exec)
        .await
        .report_run
        .expect("the report agent waits");
    let refusal = runtime::purge(&mut rt, Duration::ZERO).unwrap_err();
    assert!(refusal.contains("autoresearch serve"), "{refusal}");
    let day = Duration::from_secs(86_400);
    assert_eq!(runtime::purge(&mut rt, day).unwrap(), 0);
    runtime::drive_report(&mut rt, &reporter).await.unwrap();
    assert!(runtime::purge(&mut rt, Duration::ZERO).unwrap() > 0);
}

/// `purge` can remove a session while its detached report agent runs on.
/// The model of that report is then unknown, and the check must not fail
/// for good on the gone parent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_report_whose_session_was_purged_does_not_block_serve() {
    let fx = Fixture::new();
    let mut rt = fx.open(&model());
    let mut config = fx.config("night", 1);
    config.report = true;
    config.model = "claude:claude-x".into();
    let exec = runtime::start(&mut rt, &config).unwrap();
    let report = run_to_end(&mut rt, exec).await;
    let reporter = report.report_run.expect("the report agent waits");
    rt.purge_terminal(Duration::ZERO).unwrap();
    runtime::check_agents(&rt, "scripted", None).unwrap();
    runtime::drive_report(&mut rt, &reporter).await.unwrap();
    let reports = autoresearch::stores::FileDelivery::dir(fx.lab()).unwrap();
    assert_eq!(std::fs::read_dir(reports).unwrap().count(), 1);
}
