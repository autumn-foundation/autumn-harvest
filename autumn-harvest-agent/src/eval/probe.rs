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
//! The probe input also changes these fields of the source task:
//!
//! - No follow-ups. A follow-up waits on a durable timer, so the probe would
//!   not end.
//! - No draw. A draw root sets the run id, and the probe needs its own.
//! - `deliver` is off. An older worker ignores the mark, but not this flag.
//! - `parallel_children` is zero, so each delegation runs inline. A fan-out
//!   child is a separate run, and its end would notify outside systems.
//! - The approval wait is [`PROBE_APPROVAL_TIMEOUT_SECS`]. Nobody answers it.
//!
//! The fork uses `ForkEffects::LiveQuiet`, so its end sends no completion
//! callback and fires no completion trigger.
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

/// The approval wait of a probe, in seconds. A gated call times out to a
/// deny.
pub const PROBE_APPROVAL_TIMEOUT_SECS: u64 = 1;

/// The audit reason of a probe fork.
#[cfg_attr(not(feature = "postgres"), allow(dead_code))]
pub const PROBE_FORK_REASON: &str = "live probe (issue #2378)";

/// The audit operator of a probe fork.
#[cfg_attr(not(feature = "postgres"), allow(dead_code))]
pub const PROBE_FORK_OPERATOR: &str = "autumn_harvest_agent::eval::live_probe";

/// The start of the workflow id of a keyed probe fork. See
/// [`probe_fork_id`].
pub const PROBE_FORK_ID_PREFIX: &str = "agent-probe-";

/// The workflow id of the probe fork of `key`.
///
/// A caller passes a key that stays the same over its retries, such as the
/// idempotency key of its activity. Each retry then names the same fork.
#[must_use]
pub fn probe_fork_id(key: &str) -> String {
    format!("{PROBE_FORK_ID_PREFIX}{key}")
}

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
/// `PostgresAgents` forks with `ForkEffects::LiveQuiet`. `SqliteRuntime` returns
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

    /// The run that holds the workflow id `fork_id` on the shard of
    /// `source`, or `None`. A terminated run holds no id.
    ///
    /// The default has no fork.
    ///
    /// # Errors
    ///
    /// Returns [`EvalError::Unsupported`] when the backend has no fork, and
    /// [`EvalError::Fork`] when the read fails.
    fn find_fork(
        &self,
        source: ExecutionId,
        fork_id: &str,
    ) -> BoxFuture<'_, Result<Option<ProbeFork>, EvalError>> {
        let _ = (source, fork_id);
        Box::pin(std::future::ready(Err(EvalError::Unsupported("this host"))))
    }

    /// [`Self::fork_live`] under the workflow id `fork_id`.
    ///
    /// The default has no fork.
    ///
    /// # Errors
    ///
    /// The errors of [`Self::fork_live`], and [`EvalError::ForkIdInUse`]
    /// when another run holds `fork_id`.
    fn fork_live_as(
        &self,
        source: ExecutionId,
        input: Value,
        fork_id: &str,
    ) -> BoxFuture<'_, Result<ProbeFork, EvalError>> {
        let _ = (source, input, fork_id);
        Box::pin(std::future::ready(Err(EvalError::Unsupported("this host"))))
    }
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
    live_probe_with(host, harness, source, overlay, allowed, None).await
}

/// [`live_probe`] under `ceilings` in place of the source ceilings (issue
/// #2380).
///
/// A refine run passes the policy ceilings that its stages and replay
/// probes use. So an overlay that the policy allows to widen a bound or
/// switch a model gets its live re-probe too. `None` uses the source
/// ceilings, as [`live_probe`] does.
///
/// # Errors
///
/// The errors of [`live_probe`].
#[allow(clippy::future_not_send)]
pub async fn live_probe_with<H: LiveProbeHost + ?Sized>(
    host: &H,
    harness: &AgentHarness,
    source: ExecutionId,
    overlay: &ProfileOverlay,
    allowed: &[ToolEffect],
    ceilings: Option<&Ceilings>,
) -> Result<LiveProbe, EvalError> {
    let effects: BTreeSet<ToolEffect> = allowed.iter().copied().collect();
    let input = probe_input(host, harness, source, overlay, &effects, ceilings).await?;
    let fork = host.fork_live(source, input).await?;
    Ok(LiveProbe::of(fork, effects, overlay))
}

/// [`live_probe_with`] under the workflow id `fork_id`. A second call with
/// the same id finds the first fork, and forks nothing.
///
/// Pass [`probe_fork_id`] of a key that stays the same over the retries of
/// the caller. A crash during the wait for the fork then costs no second
/// fork. The id is held until the fork is terminated, so a fork that ended
/// is found too.
///
/// The function looks for the fork first. A found fork skips the source
/// read and the overlay checks, because the call that made it passed them.
/// A concurrent call that wins the id gives [`EvalError::ForkIdInUse`].
/// The function then reads the fork of the winner.
///
/// # Errors
///
/// The errors of [`live_probe`]. [`EvalError::Fork`] when a run of another
/// source holds `fork_id`.
#[allow(clippy::future_not_send)]
pub async fn live_probe_keyed<H: LiveProbeHost + ?Sized>(
    host: &H,
    harness: &AgentHarness,
    source: ExecutionId,
    overlay: &ProfileOverlay,
    allowed: &[ToolEffect],
    ceilings: Option<&Ceilings>,
    fork_id: &str,
) -> Result<LiveProbe, EvalError> {
    let effects: BTreeSet<ToolEffect> = allowed.iter().copied().collect();
    if let Some(fork) = host.find_fork(source, fork_id).await? {
        return of_source(fork, source).map(|fork| LiveProbe::of(fork, effects, overlay));
    }
    let input = probe_input(host, harness, source, overlay, &effects, ceilings).await?;
    let fork = match host.fork_live_as(source, input, fork_id).await {
        Err(EvalError::ForkIdInUse(_)) => {
            host.find_fork(source, fork_id).await?.ok_or_else(|| {
                EvalError::Fork(format!(
                    "the fork id {fork_id} is in use, but no run on the source shard holds it"
                ))
            })?
        }
        other => other?,
    };
    of_source(fork, source).map(|fork| LiveProbe::of(fork, effects, overlay))
}

/// `fork`, when `source` is its source.
///
/// # Errors
///
/// Returns [`EvalError::Fork`] when another source made `fork`.
fn of_source(fork: ProbeFork, source: ExecutionId) -> Result<ProbeFork, EvalError> {
    if fork.forked_from_exec_id == source {
        Ok(fork)
    } else {
        Err(EvalError::Fork(format!(
            "the fork id {} names a fork of {}, not of {source}",
            fork.workflow_id, fork.forked_from_exec_id
        )))
    }
}

impl LiveProbe {
    /// The started probe of `fork`.
    fn of(fork: ProbeFork, effects: BTreeSet<ToolEffect>, overlay: &ProfileOverlay) -> Self {
        Self {
            exec_id: fork.exec_id,
            workflow_id: fork.workflow_id,
            forked_from_exec_id: fork.forked_from_exec_id,
            effects,
            overlay_digest: overlay.digest(),
        }
    }
}

/// The fork input of a probe of `source`: its task under `overlay`, with
/// the mark.
///
/// # Errors
///
/// The refusals of [`live_probe`].
#[allow(clippy::future_not_send)]
async fn probe_input<H: LiveProbeHost + ?Sized>(
    host: &H,
    harness: &AgentHarness,
    source: ExecutionId,
    overlay: &ProfileOverlay,
    effects: &BTreeSet<ToolEffect>,
    ceilings: Option<&Ceilings>,
) -> Result<Value, EvalError> {
    let run = host.source_run(source).await?;
    if run.workflow_name != WORKFLOW_NAME {
        return Err(EvalError::NotAnAgentRun(format!(
            "the source is a {} run, not {WORKFLOW_NAME}",
            run.workflow_name
        )));
    }
    let task = probe_task(&task_of(&run.events)?, harness, overlay, effects, ceilings)?;
    serde_json::to_value(&task).map_err(|e| EvalError::Undecodable(e.to_string()))
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
    ceilings: Option<&Ceilings>,
) -> Result<AgentTask, EvalError> {
    let source = Ceilings::from_profile(&source_profile(task));
    let ceilings = ceilings.unwrap_or(&source);
    let mut task = task_with_overlay(task, overlay, ceilings, harness)?;
    task.probe = Some(Probe::new(effects.iter().copied()));
    task.followups = None;
    task.draw = None;
    // An older worker ignores the mark, but it reads this flag. So no report
    // leaves the probe on any worker.
    task.deliver = false;
    // A fan-out child is a separate run. Its end would send completion
    // callbacks and fire completion triggers. An inline child sends none.
    task.parallel_children = 0;
    // Nobody answers the approval of a probe, so a gated call fails fast.
    task.approval_timeout_secs = PROBE_APPROVAL_TIMEOUT_SECS;
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
        /// The fork that holds each workflow id.
        held: Mutex<Vec<(String, ProbeFork)>>,
        /// Hide a held id from the first lookup, as a concurrent fork does.
        race: Mutex<bool>,
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
                held: Mutex::new(Vec::new()),
                race: Mutex::new(false),
            }
        }

        /// Hold `fork_id` with a fork of `source`, as an earlier attempt did.
        fn hold(&self, fork_id: &str, source: ExecutionId) -> ExecutionId {
            let exec_id = ExecutionId::new();
            self.held.lock().unwrap().push((
                fork_id.to_owned(),
                ProbeFork {
                    exec_id,
                    workflow_id: fork_id.to_owned(),
                    forked_from_exec_id: source,
                },
            ));
            exec_id
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

        fn find_fork(
            &self,
            _source: ExecutionId,
            fork_id: &str,
        ) -> BoxFuture<'_, Result<Option<ProbeFork>, EvalError>> {
            let hidden = std::mem::take(&mut *self.race.lock().unwrap());
            let found = self
                .held
                .lock()
                .unwrap()
                .iter()
                .find(|(id, _)| id == fork_id && !hidden)
                .map(|(_, fork)| fork.clone());
            Box::pin(async move { Ok(found) })
        }

        fn fork_live_as(
            &self,
            source: ExecutionId,
            input: Value,
            fork_id: &str,
        ) -> BoxFuture<'_, Result<ProbeFork, EvalError>> {
            let held = self
                .held
                .lock()
                .unwrap()
                .iter()
                .any(|(id, _)| id == fork_id);
            let result = if held {
                Err(EvalError::ForkIdInUse(fork_id.to_owned()))
            } else {
                self.forks.lock().unwrap().push(input);
                let exec_id = self.hold(fork_id, source);
                Ok(ProbeFork {
                    exec_id,
                    workflow_id: fork_id.to_owned(),
                    forked_from_exec_id: source,
                })
            };
            Box::pin(async move { result })
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
        let mut task = AgentTask::new("Check the account.")
            .system("old")
            .parallel_children(4);
        task.deliver = true;
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
        assert!(!forked.deliver, "an older worker delivers nothing either");
        assert_eq!(forked.parallel_children, 0, "each child runs inline");
        assert_eq!(forked.approval_timeout_secs, PROBE_APPROVAL_TIMEOUT_SECS);
        assert_eq!(forked.input, task.input);
    }

    #[tokio::test]
    async fn given_ceilings_let_a_probe_switch_to_an_allowed_model() {
        let host = FakeHost::new(serde_json::to_value(AgentTask::new("hi")).unwrap());
        let harness = harness().model_named("other", Arc::new(NoModel));
        let overlay = ProfileOverlay {
            model: Some("other".to_owned()),
            ..ProfileOverlay::default()
        };
        // The source ceilings hold only the source model.
        let refused = live_probe(&host, &harness, ExecutionId::new(), &overlay, &[]).await;
        assert!(matches!(refused, Err(EvalError::Overlay(_))), "{refused:?}");
        // A refine run passes the policy ceilings, which allow the model.
        let mut ceilings = Ceilings::from_profile(&crate::agent::AgentProfile::new("k"));
        ceilings.models.insert("other".to_owned());
        live_probe_with(
            &host,
            &harness,
            ExecutionId::new(),
            &overlay,
            &[],
            Some(&ceilings),
        )
        .await
        .unwrap();
        let [input] = host.forks().try_into().unwrap();
        assert_eq!(input["model"], "other");
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

    /// A keyed probe of `source` with the fork id `fork_id`.
    async fn keyed(
        host: &FakeHost,
        source: ExecutionId,
        fork_id: &str,
    ) -> Result<LiveProbe, EvalError> {
        live_probe_keyed(
            host,
            &harness(),
            source,
            &system("new"),
            &[ToolEffect::ReadOnly],
            None,
            fork_id,
        )
        .await
    }

    #[tokio::test]
    async fn a_keyed_probe_forks_once_under_its_id() {
        let host = FakeHost::new(serde_json::to_value(AgentTask::new("hi")).unwrap());
        let source = ExecutionId::new();
        let first = keyed(&host, source, "probe-k").await.unwrap();
        assert_eq!(first.workflow_id, "probe-k");
        assert_eq!(first.forked_from_exec_id, source);
        // A retry with the same key finds the first fork.
        let again = keyed(&host, source, "probe-k").await.unwrap();
        assert_eq!(again, first);
        assert_eq!(host.forks().len(), 1, "a retry forks nothing");
        // Another key forks again.
        let other = keyed(&host, source, "probe-j").await.unwrap();
        assert_ne!(other.exec_id, first.exec_id);
        assert_eq!(host.forks().len(), 2);
    }

    #[tokio::test]
    async fn a_keyed_probe_that_loses_the_race_reads_the_winner() {
        let host = FakeHost::new(serde_json::to_value(AgentTask::new("hi")).unwrap());
        let source = ExecutionId::new();
        let winner = host.hold("probe-k", source);
        *host.race.lock().unwrap() = true;
        let probe = keyed(&host, source, "probe-k").await.unwrap();
        assert_eq!(probe.exec_id, winner);
        assert_eq!(host.forks(), Vec::<Value>::new(), "the loser forks nothing");
    }

    #[tokio::test]
    async fn a_fork_id_of_another_source_is_refused() {
        let host = FakeHost::new(serde_json::to_value(AgentTask::new("hi")).unwrap());
        host.hold("probe-k", ExecutionId::new());
        let refused = keyed(&host, ExecutionId::new(), "probe-k").await;
        assert!(matches!(refused, Err(EvalError::Fork(_))), "{refused:?}");
        assert_eq!(host.forks(), Vec::<Value>::new());
    }

    #[tokio::test]
    async fn a_keyed_probe_still_refuses_a_wider_overlay() {
        let task = AgentTask::new("hi").max_steps(4);
        let host = FakeHost::new(serde_json::to_value(&task).unwrap());
        let wider = ProfileOverlay {
            max_steps: Some(40),
            ..ProfileOverlay::default()
        };
        let refused = live_probe_keyed(
            &host,
            &harness(),
            ExecutionId::new(),
            &wider,
            &[],
            None,
            "probe-k",
        )
        .await;
        assert!(matches!(refused, Err(EvalError::Overlay(_))), "{refused:?}");
        assert_eq!(host.forks(), Vec::<Value>::new());
    }

    #[test]
    fn the_fork_id_is_a_pure_function_of_the_key() {
        assert_eq!(probe_fork_id("k-1"), probe_fork_id("k-1"));
        assert_ne!(probe_fork_id("k-1"), probe_fork_id("k-2"));
        assert!(probe_fork_id("k-1").starts_with(PROBE_FORK_ID_PREFIX));
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
