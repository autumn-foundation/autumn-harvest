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

/// The pause between two drives of a run that waits on the clock.
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

/// Whether `events` hold work that the clock alone can finish.
///
/// That is a timer that has not fired or been cancelled, or an activity with
/// no result yet, such as one in retry backoff.
fn has_pending_work(events: &[WorkflowEvent]) -> bool {
    let mut timers = HashSet::new();
    let mut activities = HashSet::new();
    for event in events {
        match event {
            WorkflowEvent::TimerStarted { timer_id, .. } => {
                timers.insert(timer_id.clone());
            }
            WorkflowEvent::TimerFired { timer_id }
            | WorkflowEvent::TimerCancelled { timer_id, .. } => {
                timers.remove(timer_id);
            }
            WorkflowEvent::ActivityScheduled { activity_id, .. } => {
                activities.insert(*activity_id);
            }
            WorkflowEvent::ActivityCompleted { activity_id, .. }
            | WorkflowEvent::ActivityFailed { activity_id, .. }
            | WorkflowEvent::ActivityTimedOut { activity_id, .. } => {
                activities.remove(activity_id);
            }
            _ => {}
        }
    }
    !timers.is_empty() || !activities.is_empty()
}

/// One runtime and the errors it met on the way.
struct Driver {
    rt: SqliteRuntime,
    /// Errors that a later outcome can explain, such as a fleet drive error.
    notes: Vec<String>,
}

impl Driver {
    async fn drive(&mut self, exec: ExecutionId) -> Drive {
        let state = self.rt.run_until_blocked(exec).await;
        // Drive the runs that the root does not await, such as a detached child.
        if let Err(e) = self.rt.run_until_idle().await {
            self.notes.push(format!("run_until_idle: {e}"));
        }
        match state {
            Ok(RunState::InProgress | RunState::WaitingTimer | RunState::WaitingWorkflow(_)) => {
                Drive::Again
            }
            // A wait can race a deadline timer or a retry. The clock moves it.
            Ok(RunState::WaitingSignal(_) | RunState::WaitingMutex(_)) => {
                let events = self.rt.load_history(exec).unwrap_or_default();
                if has_pending_work(&events) {
                    Drive::Again
                } else {
                    Drive::Parked
                }
            }
            Ok(RunState::ContinuedAsNew(next)) => Drive::Ended(Some(next)),
            // On `Unsupported`, the runtime sealed the run `FAILED` before it
            // returned the error.
            Ok(
                RunState::Completed(_)
                | RunState::Failed(_)
                | RunState::Cancelled(_)
                | RunState::Terminated(_)
                | RunState::TimedOut(_),
            )
            | Err(SqliteError::Unsupported(_)) => Drive::Ended(None),
            Err(other) => Drive::Error(other.to_string()),
        }
    }

    fn count(&self, exec: ExecutionId, event: &str) -> usize {
        self.rt
            .load_history(exec)
            .expect("read the history")
            .iter()
            .filter(|e| event_type(e) == event)
            .count()
    }

    /// Drive `exec` until `done` holds, the run cannot move, or [`WAIT`]
    /// passes. A timeout goes into the notes with `what`.
    async fn drive_until(
        &mut self,
        exec: ExecutionId,
        what: &str,
        done: impl Fn(&Self) -> bool,
    ) -> Drive {
        let start = Instant::now();
        loop {
            let drive = self.drive(exec).await;
            if done(self) {
                return drive;
            }
            match drive {
                Drive::Again if start.elapsed() < WAIT => tokio::time::sleep(POLL).await,
                Drive::Again => {
                    self.notes.push(format!("timed out waiting for {what}"));
                    return Drive::Again;
                }
                other => return other,
            }
        }
    }

    /// Drive `exec`, and each successor after a continue-as-new, to the end.
    async fn drive_chain(&mut self, root: ExecutionId) -> Vec<ExecutionId> {
        let mut chain = vec![root];
        loop {
            let current = chain[chain.len() - 1];
            let what = format!("execution {current} to end");
            match self.drive_until(current, &what, |_| false).await {
                Drive::Ended(Some(next)) => chain.push(next),
                Drive::Ended(None) | Drive::Again | Drive::Parked => return chain,
                Drive::Error(e) => {
                    self.notes.push(e);
                    return chain;
                }
            }
        }
    }

    /// Every execution of the run: the chain, then each child in order of
    /// first mention. Drive each child to its end, as Postgres waits for it.
    async fn executions_of(
        &mut self,
        chain: &[ExecutionId],
        workflow: &str,
    ) -> Vec<(ExecutionId, String, String)> {
        let mut ids: Vec<(ExecutionId, String, String)> = chain
            .iter()
            .enumerate()
            .map(|(i, id)| {
                let role = if i == 0 {
                    "root".to_owned()
                } else {
                    format!("successor {i}")
                };
                (*id, role, workflow.to_owned())
            })
            .collect();
        let mut next = 0;
        while next < ids.len() {
            let events = self.rt.load_history(ids[next].0).expect("read a history");
            for (child, child_workflow) in children(&events) {
                self.drive_chain(child).await;
                let n = ids
                    .iter()
                    .filter(|(_, r, _)| r.starts_with("child"))
                    .count()
                    + 1;
                ids.push((child, format!("child {n}"), child_workflow));
            }
            next += 1;
        }
        ids
    }

    /// Run the script against `root`. Return the result of each update.
    async fn run_steps(&mut self, root: ExecutionId, steps: &[Step]) -> Vec<serde_json::Value> {
        let mut update_results = Vec::new();
        for step in steps {
            match step {
                Step::Signals(batch) => {
                    for (name, payload) in batch {
                        self.rt
                            .send_signal(root, name, payload.clone())
                            .expect("send a signal");
                    }
                }
                Step::AwaitEvent { event, count: n } => {
                    let what = format!("{n} {event} events");
                    let drive = self
                        .drive_until(root, &what, |d| d.count(root, event) >= *n)
                        .await;
                    if let Drive::Error(e) = drive {
                        self.notes.push(e);
                    }
                    if self.count(root, event) < *n {
                        self.notes.push(format!("the run never reached {what}"));
                    }
                }
                Step::Update { name, input } => {
                    // The update replays the stored history, so the run must exist.
                    let _ = self.drive(root).await;
                    let result = match self.rt.update(root, name, input.clone()).await {
                        Ok(v) => v,
                        Err(SqliteError::UpdateFailed { error, .. }) => {
                            serde_json::json!({ "update_failed": error })
                        }
                        Err(other) => serde_json::json!({ "update_error": other.to_string() }),
                    };
                    update_results.push(result);
                }
                Step::Cancel(reason) => self.rt.cancel(root, reason).expect("cancel the run"),
                Step::Terminate(reason) => {
                    self.rt.terminate(root, reason).expect("terminate the run");
                }
            }
        }
        update_results
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
    let mut d = Driver {
        rt,
        notes: Vec::new(),
    };

    let update_results = d.run_steps(root, &scenario.steps).await;

    let chain = d.drive_chain(root).await;
    let ids = d.executions_of(&chain, scenario.workflow).await;

    let last = chain[chain.len() - 1];
    let outcome = match d.rt.outcome(last).expect("read the outcome") {
        ExecutionOutcome::Completed(v) => Outcome::Completed(v),
        ExecutionOutcome::Failed(e) => Outcome::Failed(e),
        ExecutionOutcome::Terminated(state) => Outcome::Ended(state),
        ExecutionOutcome::Running => Outcome::NotEnded(if d.notes.is_empty() {
            "parked".to_owned()
        } else {
            d.notes.join("; ")
        }),
    };
    let executions = ids
        .into_iter()
        .map(|(id, role, workflow)| Execution {
            role,
            workflow,
            events: d.rt.load_history(id).expect("read a history"),
        })
        .collect();
    Run {
        outcome,
        executions,
        update_results,
    }
}
