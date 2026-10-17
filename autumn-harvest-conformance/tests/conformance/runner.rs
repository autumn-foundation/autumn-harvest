//! Runs one scenario on both backends and checks the conformance rules.

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};

use crate::commands::{Support, support};
use crate::compare::{Execution, Outcome, Run, differences, event_type};
use crate::scenario::{Expect, Scenario};
use crate::{pg, sqlite};

/// Check `scenario`. Panic with every broken rule.
pub async fn check(scenario: Scenario) {
    match scenario.expect {
        Expect::Same => check_same(&scenario).await,
        Expect::Rejected { feature } => check_rejected(&scenario, feature).await,
    }
}

async fn check_same(scenario: &Scenario) {
    let pg = pg::run(scenario).await;
    let sqlite = sqlite::run(scenario).await;

    let mut problems = differences(&pg, &sqlite);
    problems.extend(replay_problems(scenario, &pg, &sqlite).await);
    for (backend, run) in [("postgres", &pg), ("sqlite", &sqlite)] {
        problems.extend(witness_problems(scenario, backend, run));
    }
    assert!(
        problems.is_empty(),
        "scenario `{}` does not conform:\n- {}",
        scenario.name,
        problems.join("\n- ")
    );
}

/// The replay verdict of one history on the core `WorkflowReplayer`.
///
/// The verdict leaves out the execution id and the event index text, so two
/// backends can compare it.
async fn verdict(scenario: &Scenario, execution: &Execution) -> String {
    let infos = (scenario.workflows)()
        .into_iter()
        .filter(|i| i.name == execution.workflow)
        .collect();
    let report = WorkflowReplayer::new()
        .register(infos)
        .updates((scenario.updates)())
        .replay_from_events(execution.events.clone())
        .await;
    match &report.status {
        ReplayStatus::ReplaySucceeded => "ReplaySucceeded".to_owned(),
        ReplayStatus::NonDeterminismDetected { kind, .. } => {
            format!("NonDeterminismDetected({kind}): {report}")
        }
        ReplayStatus::WorkflowFailed { .. } => format!("WorkflowFailed: {report}"),
    }
}

/// Whether a history ends with an external cancel or terminate.
///
/// The core replayer does not model either. It reports a mismatch at the
/// `WorkflowCancelled` event on both backends.
fn ends_by_operator(events: &[WorkflowEvent]) -> bool {
    matches!(events.last(), Some(WorkflowEvent::WorkflowCancelled { .. }))
}

/// Rule R1: each history replays with `ReplaySucceeded`. Rule R2: a history
/// that an operator ended must get the same verdict on both backends.
async fn replay_problems(scenario: &Scenario, pg: &Run, sqlite: &Run) -> Vec<String> {
    let mut out = Vec::new();
    for (p, s) in pg.executions.iter().zip(&sqlite.executions) {
        let (pv, sv) = (verdict(scenario, p).await, verdict(scenario, s).await);
        let kind = |v: &str| v.split(':').next().unwrap_or_default().to_owned();
        if ends_by_operator(&p.events) && ends_by_operator(&s.events) {
            if kind(&pv) != kind(&sv) {
                out.push(format!(
                    "{} replay verdicts differ: postgres {pv}, sqlite {sv}",
                    p.role
                ));
            }
            continue;
        }
        for (backend, v) in [("postgres", &pv), ("sqlite", &sv)] {
            if v != "ReplaySucceeded" {
                out.push(format!("{backend} {} history does not replay: {v}", p.role));
            }
        }
    }
    out
}

/// Each covered command with a witness event must leave that event.
fn witness_problems(scenario: &Scenario, backend: &str, run: &Run) -> Vec<String> {
    let types: Vec<String> = run
        .executions
        .iter()
        .flat_map(|e| e.events.iter().map(event_type))
        .collect();
    scenario
        .covers
        .iter()
        .filter_map(|kind| match support(*kind) {
            Support::Accepted {
                witness: Some(event),
            } if !types.iter().any(|t| t == event) => Some(format!(
                "{backend}: the scenario covers {kind:?}, but no {event} event is in its history"
            )),
            _ => None,
        })
        .collect()
}

async fn check_rejected(scenario: &Scenario, feature: &str) {
    let run = sqlite::run(scenario).await;
    let root = &run.executions[0].events;
    let failure = root.iter().rev().find_map(|e| match e {
        WorkflowEvent::WorkflowFailed {
            error_type,
            error,
            details,
            ..
        } => Some((error_type.clone(), error.clone(), details.clone())),
        _ => None,
    });
    let Some((error_type, error, details)) = failure else {
        panic!(
            "scenario `{}`: sqlite must reject `{feature}` loudly, but the run has no \
             WorkflowFailed event. Outcome: {:?}",
            scenario.name, run.outcome
        );
    };
    assert_eq!(
        error_type.as_deref(),
        Some("UnsupportedFeature"),
        "scenario `{}`: the failure must be typed UnsupportedFeature: {error}",
        scenario.name
    );
    let named = details
        .as_ref()
        .and_then(|d| d.get("feature"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    assert!(
        named.starts_with(feature),
        "scenario `{}`: the failure must name `{feature}`, but it names `{named}`: {error}",
        scenario.name
    );
    assert!(
        matches!(run.outcome, Outcome::Failed(_)),
        "scenario `{}`: a rejected run must end FAILED, not {:?}",
        scenario.name,
        run.outcome
    );
}
