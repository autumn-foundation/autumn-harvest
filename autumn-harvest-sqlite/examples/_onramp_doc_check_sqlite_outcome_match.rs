#![allow(clippy::all, clippy::pedantic, clippy::nursery, dead_code)]
use autumn_harvest::prelude::*;
use autumn_harvest_sqlite::SqliteRuntime;

#[workflow]
async fn onramp_check_workflow(_ctx: &WorkflowContext, _input: ()) -> Result<String, String> {
    Ok(String::new())
}

#[tokio::main]
async fn main() -> Result<(), autumn_harvest_sqlite::SqliteError> {
    let mut rt = SqliteRuntime::open_in_memory()?;
    rt.register_workflow(&onramp_check_workflow_info());
    let exec = rt.start_workflow("onramp_check_workflow", serde_json::json!(null))?;

use autumn_harvest_sqlite::{RunState, ExecutionOutcome};

// Drive a single run to its next stopping point:
match rt.run_until_blocked(exec).await? {
    RunState::Completed(output) => println!("done: {output}"),
    RunState::Failed(err)       => println!("failed: {err}"),
    RunState::WaitingSignal(name) => println!("blocked on signal `{name}`"),
    RunState::WaitingTimer      => println!("blocked on a durable timer"),
    RunState::WaitingWorkflow(other) => println!("blocked on run {other}"),
    RunState::WaitingMutex(key) => println!("blocked on the `{key}` mutex"),
    RunState::Cancelled(reason) => println!("cancelled: {reason}"),
    RunState::TimedOut(error)   => println!("timed out: {error}"),
    RunState::Terminated(reason) => println!("terminated: {reason}"),
    RunState::ContinuedAsNew(next) => println!("continued as new; drive {next}"),
    RunState::InProgress        => {} // not returned by run_until_blocked
}

// Or drive the whole fleet to quiescence, then read a stored outcome:
rt.run_until_idle().await?;
match rt.outcome(exec)? {
    ExecutionOutcome::Completed(output) => println!("{output}"),
    ExecutionOutcome::Failed(err)       => println!("{err}"),
    ExecutionOutcome::Running           => println!("still running"),
    ExecutionOutcome::Terminated(state) => println!("terminal ({state}) — superseded, cancelled, terminated or timed out"),
}

    Ok(())
}
