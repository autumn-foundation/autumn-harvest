//! The `SQLite` driver: one in-memory `SqliteRuntime` on the wall clock.
//!
//! Postgres fires timers on its own clock, so this driver uses the wall clock
//! too. It drives the run again until a step can go ahead or the run ends.

use std::collections::HashSet;
use std::time::{Duration, Instant};

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::types::ExecutionId;
use autumn_harvest_sqlite::{ExecutionOutcome, RunState, SqliteError, SqliteRuntime};

use crate::compare::{Execution, Outcome, Run, children, event_type};
use crate::scenario::{Scenario, Step};

/// The longest a script step or the final wait may take.
const WAIT: Duration = Duration::from_secs(30);

/// The pause between two drives of a run that waits on a timer.
const POLL: Duration = Duration::from_millis(20);

/// What one drive left behind.
enum Drive {
    /// The run can move on a later drive, for example after a timer.
    Again,
    /// The run cannot move until the script sends an input.
    Parked,
    /// The run ended. The id names its successor after a continue-as-new.
    Ended(Option<ExecutionId>),
    /// The drive failed with an error that the next drive repeats.
    Error(String),
}

/// Whether `events` hold a timer that has not fired and is not cancelled.
fn has_pending_timer(events: &[WorkflowEvent]) -> bool {
    let mut open = HashSet::new();
    for event in events {
        match event {
            WorkflowEvent::TimerStarted { timer_id, .. } => {
                open.insert(timer_id.clone());
            }
            WorkflowEvent::TimerFired { timer_id } | WorkflowEvent::TimerCancelled { timer_id, .. } => {
                open.remove(timer_id);
            }
            _ => {}
        }
    }
    !open.is_empty()
}

async fn drive(rt: &mut SqliteRuntime, exec: ExecutionId) -> Drive {
    let state = rt.run_until_blocked(exec).await;
    // Drive the runs that the root does not await, such as a detached child.
    let _ = rt.run_until_idle().await;
    match state {
        Ok(RunState::InProgress | RunState::WaitingTimer | RunState::WaitingWorkflow(_)) => {
            Drive::Again
        }
        // A signal wait can race a deadline timer. The wall clock fires it.
        Ok(RunState::WaitingSignal(_) | RunState::WaitingMutex(_)) => {
            let events = rt.load_history(exec).unwrap_or_default();
            if has_pending_timer(&events) {
                Drive::Again
            } else {
                Drive::Parked
            }
        }
        Ok(RunState::ContinuedAsNew(next)) => Drive::Ended(Some(next)),
        Ok(
            RunState::Completed(_)
            | RunState::Failed(_)
            | RunState::Cancelled(_)
            | RunState::Terminated(_)
            | RunState::TimedOut(_),
        ) => Drive::Ended(None),
        // The runtime sealed the run `FAILED` before it returned the error.
        Err(SqliteError::Unsupported(_)) => Drive::Ended(None),
        Err(other) => Drive::Error(other.to_string()),
    }
}

fn count(rt: &SqliteRuntime, exec: ExecutionId, event: &str) -> usize {
    rt.load_history(exec)
        .expect("read the history")
        .iter()
        .filter(|e| event_type(e) == event)
        .count()
}

/// Drive `exec` until `done` holds, the run cannot move, or [`WAIT`] passes.
async fn drive_until(
    rt: &mut SqliteRuntime,
    exec: ExecutionId,
    done: impl Fn(&SqliteRuntime) -> bool,
) -> Drive {
    let start = Instant::now();
    loop {
        let drive = drive(rt, exec).await;
        if done(rt) {
            return drive;
        }
        match drive {
            Drive::Again if start.elapsed() < WAIT => tokio::time::sleep(POLL).await,
            other => return other,
        }
    }
}

/// Run `scenario` on a fresh in-memory `SQLite` runtime.
pub async fn run(scenario: &Scenario) -> Run {
    let mut rt = SqliteRuntime::open_in_memory().expect("open SQLite");
    for info in (scenario.workflows)() {
        rt.register_workflow(&info);
    }
    for info in (scenario.activities)() {
        rt.register_activity(&info);
    }
    for info in (scenario.updates)() {
        rt.register_update(&info);
    }
    let root = rt
        .start_workflow(scenario.workflow, scenario.input.clone())
        .expect("start the root run");
    for (name, payload) in &scenario.initial_signals {
        rt.send_signal(root, name, payload.clone())
            .expect("send an initial signal");
    }

    let mut update_results = Vec::new();
    let mut error = None;
    for step in &scenario.steps {
        match step {
            Step::Signals(batch) => {
                for (name, payload) in batch {
                    rt.send_signal(root, name, payload.clone())
                        .expect("send a signal");
                }
            }
            Step::AwaitEvent { event, count: n } => {
                let drive = drive_until(&mut rt, root, |rt| count(rt, root, event) >= *n).await;
                if let Drive::Error(e) = drive {
                    error = Some(e);
                }
            }
            Step::Update { name, input } => {
                // The update replays the stored history, so the run must exist.
                let _ = drive(&mut rt, root).await;
                let result = match rt.update(root, name, input.clone()).await {
                    Ok(v) => v,
                    Err(SqliteError::UpdateFailed { error, .. }) => {
                        serde_json::json!({ "update_failed": error })
                    }
                    Err(other) => serde_json::json!({ "update_error": other.to_string() }),
                };
                update_results.push(result);
            }
            Step::Cancel(reason) => rt.cancel(root, reason).expect("cancel the run"),
            Step::Terminate(reason) => rt.terminate(root, reason).expect("terminate the run"),
        }
    }

    let mut chain = vec![root];
    loop {
        let current = chain[chain.len() - 1];
        match drive_until(&mut rt, current, |_| false).await {
            Drive::Ended(Some(next)) => chain.push(next),
            Drive::Ended(None) | Drive::Again | Drive::Parked => break,
            Drive::Error(e) => {
                error = Some(e);
                break;
            }
        }
    }

    let last = chain[chain.len() - 1];
    let outcome = match rt.outcome(last).expect("read the outcome") {
        ExecutionOutcome::Completed(v) => Outcome::Completed(v),
        ExecutionOutcome::Failed(e) => Outcome::Failed(e),
        ExecutionOutcome::Terminated(state) => Outcome::Ended(state),
        ExecutionOutcome::Running => {
            Outcome::NotEnded(error.unwrap_or_else(|| "parked".to_owned()))
        }
    };
    let mut executions: Vec<Execution> = chain
        .iter()
        .enumerate()
        .map(|(i, id)| Execution {
            role: if i == 0 {
                "root".to_owned()
            } else {
                format!("successor {i}")
            },
            workflow: scenario.workflow.to_owned(),
            events: rt.load_history(*id).expect("read the history"),
        })
        .collect();
    let mut next = 0;
    while next < executions.len() {
        for (child, workflow) in children(&executions[next].events) {
            let n = executions.iter().filter(|e| e.role.starts_with("child")).count() + 1;
            executions.push(Execution {
                role: format!("child {n}"),
                workflow,
                events: rt.load_history(child).expect("read a child history"),
            });
        }
        next += 1;
    }
    Run {
        outcome,
        executions,
        update_results,
    }
}
