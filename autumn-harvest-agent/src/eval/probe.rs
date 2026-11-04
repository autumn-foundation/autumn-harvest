//! A live probe on a fork (issue #2378).
//!
//! [`live_probe`] forks a recorded `agent_loop` run at its first event, with
//! live effects. The fork input is the source task under an overlay, with a
//! [`Probe`] mark. A worker then runs the fork.
//!
//! The mark limits the probe:
//!
//! - The tool-call activity runs a tool only when its effect is in the mark.
//!   Any other tool gets [`PROBE_EFFECT_REFUSED`](crate::PROBE_EFFECT_REFUSED)
//!   and does not run. No policy decision can change this.
//! - The run sends no report to the delivery.
//! - Each delegated child is a probe with the same effects. A delegation to a
//!   durable peer is refused.
//! - The run never moves the workspace head.
//!
//! The probe input has no follow-ups and no draw. A follow-up waits on a
//! durable timer, so the probe would not end. A draw root sets the run id,
//! and the probe needs its own run id for the cost ledger.
//!
//! Only Postgres has a fork. The SQLite host returns
//! [`EvalError::Unsupported`].

use std::collections::BTreeSet;

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::types::ExecutionId;
use serde_json::Value;

use super::{EvalError, source_profile, task_of, task_with_overlay};
use crate::agent::overlay::{Ceilings, ProfileOverlay};
use crate::harness::AgentHarness;
use crate::model::BoxFuture;
use crate::tool::ToolEffect;
use crate::types::{AgentTask, Probe};
use crate::workflow::WORKFLOW_NAME;

/// The audit reason of a probe fork.
#[cfg_attr(not(feature = "postgres"), allow(dead_code))]
pub const PROBE_FORK_REASON: &str = "live probe (issue #2378)";

/// The audit operator of a probe fork.
#[cfg_attr(not(feature = "postgres"), allow(dead_code))]
pub const PROBE_FORK_OPERATOR: &str = "autumn_harvest_agent::eval::live_probe";

/// The recorded source of a probe.
#[derive(Debug, Clone)]
pub struct SourceRun {
    /// The workflow type of the source.
    pub workflow_name: String,
    /// The decoded history of the source.
    pub events: Vec<WorkflowEvent>,
}

/// A committed probe fork.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProbeFork {
    /// The execution of the probe.
    pub exec_id: ExecutionId,
    /// The workflow id of the probe.
    pub workflow_id: String,
    /// The source execution. The fork did not change it.
    pub forked_from_exec_id: ExecutionId,
}

/// A backend that can fork a run for a live probe.
///
/// `PostgresAgents` forks with `ForkEffects::Live`. `SqliteRuntime` returns
/// [`EvalError::Unsupported`] from each method.
pub trait LiveProbeHost {
    /// Read the workflow type and the decoded history of `source`.
    ///
    /// # Errors
    ///
    /// Returns [`EvalError::Unsupported`] when the backend has no fork, and
    /// [`EvalError::Fork`] when the read fails.
    fn source_run(&self, source: ExecutionId) -> BoxFuture<'_, Result<SourceRun, EvalError>>;

    /// Fork `source` at event 0 with live effects and the start `input`.
    ///
    /// # Errors
    ///
    /// Returns [`EvalError::Unsupported`] when the backend has no fork, and
    /// [`EvalError::Fork`] when the fork is refused or fails.
    fn fork_live(
        &self,
        source: ExecutionId,
        input: Value,
    ) -> BoxFuture<'_, Result<ProbeFork, EvalError>>;
}

/// A started live probe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveProbe {
    /// The execution of the probe. Wait for it with `postgres::wait_run`.
    pub exec_id: ExecutionId,
    /// The workflow id of the probe.
    pub workflow_id: String,
    /// The source execution.
    pub forked_from_exec_id: ExecutionId,
    /// The tool effects that the probe may run.
    pub effects: BTreeSet<ToolEffect>,
    /// The digest of the overlay, as [`ProfileOverlay::digest`].
    pub overlay_digest: String,
}

/// Start a live probe of the `agent_loop` run `source`.
///
/// The probe runs the source task under `overlay`, with the source ceilings,
/// so the overlay can only narrow the source. It runs only the tools whose
/// effect is in `allowed`. Pass `RefinePolicy::probe_effects` as `allowed`.
/// Its default is `[ReadOnly]`. An empty `allowed` refuses each tool.
///
/// The function returns when the fork commits. A worker that holds
/// `harness` runs the probe. A gated call waits for a person, as in the
/// source. Bound the wait of the caller.
///
/// # Errors
///
/// - [`EvalError::Unsupported`]: the host has no fork, such as SQLite.
/// - [`EvalError::NotAnAgentRun`]: the source is not an `agent_loop` run.
/// - [`EvalError::ErasedSource`]: the source was erased.
/// - [`EvalError::Overlay`]: the overlay goes past the source ceilings, or
///   names a model or a tool that `harness` does not hold.
/// - [`EvalError::Fork`]: the read or the fork failed.
///
/// A refusal writes nothing.
// The future is `Send` when `H` is `Sync`, as `PostgresAgents` is.
// `SqliteRuntime` is not `Sync`, so the bound would refuse its host.
#[allow(clippy::future_not_send)]
pub async fn live_probe<H: LiveProbeHost + ?Sized>(
    host: &H,
    harness: &AgentHarness,
    source: ExecutionId,
    overlay: &ProfileOverlay,
    allowed: &[ToolEffect],
) -> Result<LiveProbe, EvalError> {
    let run = host.source_run(source).await?;
    if run.workflow_name != WORKFLOW_NAME {
        return Err(EvalError::NotAnAgentRun(format!(
            "the source is a {} run, not {WORKFLOW_NAME}",
            run.workflow_name
        )));
    }
    let effects: BTreeSet<ToolEffect> = allowed.iter().copied().collect();
    let task = probe_task(&task_of(&run.events)?, harness, overlay, &effects)?;
    let input = serde_json::to_value(&task).map_err(|e| EvalError::Undecodable(e.to_string()))?;
    let fork = host.fork_live(source, input).await?;
    Ok(LiveProbe {
        exec_id: fork.exec_id,
        workflow_id: fork.workflow_id,
        forked_from_exec_id: fork.forked_from_exec_id,
        effects,
        overlay_digest: overlay.digest(),
    })
}

/// The probe input: the source `task` under `overlay`, with the mark.
///
/// # Errors
///
/// Returns [`EvalError::Overlay`] for a refused overlay.
fn probe_task(
    task: &AgentTask,
    harness: &AgentHarness,
    overlay: &ProfileOverlay,
    effects: &BTreeSet<ToolEffect>,
) -> Result<AgentTask, EvalError> {
    let ceilings = Ceilings::from_profile(&source_profile(task));
    let mut task = task_with_overlay(task, overlay, &ceilings, harness)?;
    task.probe = Some(Probe::new(effects.iter().copied()));
    task.followups = None;
    task.draw = None;
    Ok(task)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use std::sync::{Arc, Mutex};

    use autumn_harvest::chrono::Utc;
    use serde_json::json;

    use super::*;
    use crate::draw::{Draw, DrawId};
    use crate::followup::Followups;
    use crate::model::{AgentModel, ChatRequest, ChatResponse};
    use crate::{AgentError, AgentTask};

    #[derive(Debug)]
    struct NoModel;

    impl AgentModel for NoModel {
        fn chat<'a>(
            &'a self,
            _request: &'a ChatRequest,
        ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
            Box::pin(async { panic!("a probe start asks no model") })
        }
    }

    /// A host that serves one history and records each fork input.
    struct FakeHost {
        name: String,
        events: Vec<WorkflowEvent>,
        forks: Mutex<Vec<Value>>,
    }

    impl FakeHost {
        fn new(input: Value) -> Self {
            Self {
                name: WORKFLOW_NAME.to_owned(),
                events: vec![WorkflowEvent::WorkflowStarted {
                    input,
                    timestamp: Utc::now(),
                    last_completion_result: None,
                    last_error: None,
                    scheduled_time: None,
                }],
                forks: Mutex::new(Vec::new()),
            }
        }

        fn forks(&self) -> Vec<Value> {
            self.forks.lock().unwrap().clone()
        }
    }

    impl LiveProbeHost for FakeHost {
        fn source_run(&self, _source: ExecutionId) -> BoxFuture<'_, Result<SourceRun, EvalError>> {
            let run = SourceRun {
                workflow_name: self.name.clone(),
                events: self.events.clone(),
            };
            Box::pin(async move { Ok(run) })
        }

        fn fork_live(
            &self,
            source: ExecutionId,
            input: Value,
        ) -> BoxFuture<'_, Result<ProbeFork, EvalError>> {
            self.forks.lock().unwrap().push(input);
            Box::pin(async move {
                Ok(ProbeFork {
                    exec_id: ExecutionId::new(),
                    workflow_id: "probe-1".to_owned(),
                    forked_from_exec_id: source,
                })
            })
        }
    }

    fn harness() -> AgentHarness {
        AgentHarness::new(Arc::new(NoModel))
    }

    fn system(text: &str) -> ProfileOverlay {
        ProfileOverlay {
            system: Some(text.to_owned()),
            ..ProfileOverlay::default()
        }
    }

    #[tokio::test]
    async fn the_probe_input_holds_the_overlay_and_the_mark() {
        let mut task = AgentTask::new("Check the account.").system("old");
        task.followups = Some(Followups::new(std::time::Duration::from_secs(60)));
        task.draw = Some(Draw::rooted("draws-1".to_owned(), DrawId::new(2)));
        let host = FakeHost::new(serde_json::to_value(&task).unwrap());
        let source = ExecutionId::new();

        let probe = live_probe(
            &host,
            &harness(),
            source,
            &system("new"),
            &[ToolEffect::Write, ToolEffect::ReadOnly, ToolEffect::Write],
        )
        .await
        .unwrap();

        let effects = BTreeSet::from([ToolEffect::ReadOnly, ToolEffect::Write]);
        assert_eq!(probe.forked_from_exec_id, source);
        assert_eq!(probe.effects, effects);
        assert_eq!(probe.overlay_digest, system("new").digest());
        let [input] = host.forks().try_into().unwrap();
        let forked: AgentTask = serde_json::from_value(input).unwrap();
        assert_eq!(forked.system.as_deref(), Some("new"));
        assert_eq!(forked.probe, Some(Probe::new(effects)));
        assert_eq!(forked.followups, None, "a probe books no follow-up");
        assert_eq!(forked.draw, None, "a probe has its own run id");
        assert_eq!(forked.input, task.input);
    }

    #[tokio::test]
    async fn an_empty_set_marks_a_probe_that_runs_no_tool() {
        let host = FakeHost::new(serde_json::to_value(AgentTask::new("hi")).unwrap());
        let probe = live_probe(
            &host,
            &harness(),
            ExecutionId::new(),
            &ProfileOverlay::default(),
            &[],
        )
        .await
        .unwrap();
        assert_eq!(probe.effects, BTreeSet::new());
        let [input] = host.forks().try_into().unwrap();
        assert_eq!(input["probe"]["effects"], json!([]));
    }

    #[tokio::test]
    async fn a_wider_overlay_is_refused_before_the_fork() {
        let task = AgentTask::new("hi").max_steps(4);
        let host = FakeHost::new(serde_json::to_value(&task).unwrap());
        let wider = ProfileOverlay {
            max_steps: Some(40),
            ..ProfileOverlay::default()
        };
        let refused = live_probe(
            &host,
            &harness(),
            ExecutionId::new(),
            &wider,
            &[ToolEffect::ReadOnly],
        )
        .await;
        assert!(matches!(refused, Err(EvalError::Overlay(_))), "{refused:?}");
        assert_eq!(host.forks(), Vec::<Value>::new(), "a refusal forks nothing");
    }

    #[tokio::test]
    async fn another_workflow_type_is_refused_before_the_fork() {
        let mut host = FakeHost::new(serde_json::to_value(AgentTask::new("hi")).unwrap());
        host.name = "agent".to_owned();
        let refused = live_probe(
            &host,
            &harness(),
            ExecutionId::new(),
            &ProfileOverlay::default(),
            &[ToolEffect::ReadOnly],
        )
        .await;
        assert!(
            matches!(refused, Err(EvalError::NotAnAgentRun(_))),
            "{refused:?}"
        );
        assert_eq!(host.forks(), Vec::<Value>::new());
    }

    #[tokio::test]
    async fn an_erased_source_is_refused_before_the_fork() {
        let mut host = FakeHost::new(serde_json::to_value(AgentTask::new("hi")).unwrap());
        host.events.push(WorkflowEvent::ActivityCompleted {
            activity_id: autumn_harvest::types::ActivityExecId::new(),
            output: json!({ "_harvest_erased": true }),
        });
        let refused = live_probe(
            &host,
            &harness(),
            ExecutionId::new(),
            &ProfileOverlay::default(),
            &[ToolEffect::ReadOnly],
        )
        .await;
        assert!(
            matches!(refused, Err(EvalError::ErasedSource)),
            "{refused:?}"
        );
        assert_eq!(host.forks(), Vec::<Value>::new());
    }

    #[test]
    fn the_mark_keeps_its_wire_shape() {
        for effects in [
            BTreeSet::new(),
            BTreeSet::from([ToolEffect::ReadOnly]),
            BTreeSet::from(ToolEffect::ALL),
        ] {
            let mark = Probe::new(effects.iter().copied());
            assert_eq!(mark.effects(), effects);
            let wire = serde_json::to_value(mark).unwrap();
            assert_eq!(wire, json!({ "effects": effects }));
            assert_eq!(serde_json::from_value::<Probe>(wire).unwrap(), mark);
            for effect in ToolEffect::ALL {
                assert_eq!(mark.allows(effect), effects.contains(&effect));
            }
        }
        // An unknown key fails, so no part of the mark drops in silence.
        let unknown = json!({ "effects": [], "source": "x" });
        assert!(serde_json::from_value::<Probe>(unknown).is_err());
    }

    #[test]
    fn the_refusal_names_its_code_and_effect() {
        let outcome = crate::ToolOutcome::probe_refused(ToolEffect::External);
        assert!(outcome.is_error);
        let value: Value = serde_json::from_str(&outcome.content).unwrap();
        assert_eq!(
            value,
            json!({"error": crate::PROBE_EFFECT_REFUSED, "effect": "external"})
        );
    }
}
