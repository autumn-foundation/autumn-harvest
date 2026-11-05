//! One consolidation schedule for many agents (issue #2540).
//!
//! The engine keys a schedule by its workflow name. So one schedule of
//! [`agent_consolidate`](crate::consolidate::agent_consolidate) serves one
//! agent only. The dispatcher [`agent_consolidate_all`] serves every agent
//! of every kind with the trigger `ConsolidationTrigger::Schedule`.
//!
//! One firing runs these steps:
//!
//! 1. The activity [`agent_consolidate_due`] lists the running agents of
//!    each scheduled kind with an [`AgentDirectory`].
//! 2. It loads the episode log of each agent. An agent with no pending
//!    episode is idle and gets no pass. An agent whose pass still runs gets
//!    no second pass.
//! 3. It sorts the other agents by their oldest pending episode, then by
//!    agent id. [`dispatch_passes`] gives how many passes start. The rest
//!    wait for the next firing.
//! 4. The workflow starts one child pass for each picked agent, and waits
//!    for each one.
//!
//! The design is in `docs/design/DESIGN-2540.md`.

use std::collections::BTreeSet;
use std::time::Duration;

use autumn_harvest::failure::{ActivityFailure, IntoActivityErrorString};
use autumn_harvest::policy::RetryPolicy;
use autumn_harvest::prelude::*;
use serde::{Deserialize, Serialize};

use crate::agent::{AgentId, AgentProfile};
use crate::bounds::dispatch_passes;
use crate::consolidate::{
    CONSOLIDATE_WORKFLOW_NAME, ConsolidateTask, ConsolidationReport, ConsolidationStop,
    episode_scope,
};
use crate::episode::ConsolidationTrigger;
use crate::error::AgentError;
use crate::harness::AgentHarness;
use crate::model::BoxFuture;
use crate::workflow::harness;

/// The registered workflow name of the dispatcher.
pub const CONSOLIDATE_ALL_WORKFLOW_NAME: &str = "agent_consolidate_all";

/// The default bound of one firing.
pub const DEFAULT_MAX_PASSES: u32 = 16;

/// The list of agents that the dispatcher reads.
///
/// `postgres::Directory` and `sqlite::Directory` read the engine. Install
/// one with `AgentHarness::directory`.
pub trait AgentDirectory: Send + Sync + std::fmt::Debug {
    /// The running agents of `kind`, by the search attribute `agent_kind`.
    ///
    /// # Errors
    ///
    /// Returns an error when the directory cannot read.
    fn agents<'a>(&'a self, kind: &'a str) -> BoxFuture<'a, Result<Vec<AgentId>, AgentError>>;

    /// Whether a pass of `agent` still runs, by the search attribute
    /// `agent_consolidation_of`.
    ///
    /// # Errors
    ///
    /// Returns an error when the directory cannot read.
    fn pass_runs<'a>(&'a self, agent: &'a AgentId) -> BoxFuture<'a, Result<bool, AgentError>>;
}

/// The input of [`agent_consolidate_all`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct ConsolidateAll {
    /// The most passes of one firing. 0 counts as 1. The cap is
    /// [`MAX_DISPATCH_PASSES`].
    pub max_passes: u32,
}

impl Default for ConsolidateAll {
    fn default() -> Self {
        Self {
            max_passes: DEFAULT_MAX_PASSES,
        }
    }
}

impl ConsolidateAll {
    /// A dispatcher with the default bound.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Start at most `n` passes in one firing.
    #[must_use]
    pub const fn max_passes(mut self, n: u32) -> Self {
        self.max_passes = n;
        self
    }
}

/// The recorded read of one firing.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct DueRead {
    /// The agents to start a pass for, in start order.
    pub agents: Vec<AgentId>,
    /// The running agents of the scheduled kinds.
    pub listed: u64,
    /// The agents with no pending episode.
    pub idle: u64,
    /// The agents whose pass still runs.
    pub running: u64,
    /// The agents with pending episodes and no running pass.
    pub due: u64,
}

/// The end of one pass of a firing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct DispatchedPass {
    /// The agent of the pass.
    pub agent: AgentId,
    /// How the pass ended. `None` when it failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stop: Option<ConsolidationStop>,
    /// The error of a failed pass.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// The output of [`agent_consolidate_all`].
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct DispatchReport {
    /// The running agents of the scheduled kinds.
    pub listed: u64,
    /// The agents with no pending episode. They cost no model call.
    pub idle: u64,
    /// The agents whose pass still runs. They get no second pass.
    pub running: u64,
    /// The agents with pending episodes and no running pass.
    pub due: u64,
    /// The passes that this firing started.
    pub started: u64,
    /// The due agents that wait for the next firing.
    pub waiting: u64,
    /// The passes that failed.
    pub failed: u64,
    /// One row for each started pass, in start order.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub passes: Vec<DispatchedPass>,
}

/// One firing of the consolidation schedule.
///
/// It starts one pass for each due agent, up to `max_passes`, and waits
/// for each one. A failed pass is one row of the report. It does not stop
/// the other passes. See the [module docs](self).
///
/// # Errors
///
/// Returns the error of the read, for example `AgentDirectoryMissing`.
#[workflow]
pub async fn agent_consolidate_all(
    ctx: &WorkflowContext,
    task: ConsolidateAll,
) -> Result<DispatchReport, String> {
    let read: DueRead = ctx
        .execute_activity(&agent_consolidate_due_info(), task.clone())
        .await
        .map_err(|err| format!("{err}"))?;
    let mut agents = read.agents;
    // The read applies the bound. The workflow applies it again, so an
    // activity bug cannot widen the fan-out.
    let bound = dispatch_passes(agents.len() as u64, task.max_passes);
    agents.truncate(usize::try_from(bound).unwrap_or(usize::MAX));
    let mut report = DispatchReport {
        listed: read.listed,
        idle: read.idle,
        running: read.running,
        due: read.due,
        started: agents.len() as u64,
        waiting: read.due.saturating_sub(agents.len() as u64),
        ..DispatchReport::default()
    };
    if agents.is_empty() {
        return Ok(report);
    }
    let mut inputs = Vec::with_capacity(agents.len());
    for agent in &agents {
        let input = serde_json::to_value(ConsolidateTask::dispatched(agent.clone()))
            .map_err(|err| err.to_string())?;
        inputs.push((CONSOLIDATE_WORKFLOW_NAME.to_owned(), input));
    }
    let ended = ctx
        .spawn_child_workflow_fan_out_collect_raw(inputs)
        .await
        .map_err(|err| err.to_string())?;
    for (agent, output) in agents.into_iter().zip(ended) {
        let pass = match output.and_then(|value| {
            serde_json::from_value::<ConsolidationReport>(value).map_err(|err| err.to_string())
        }) {
            Ok(pass) => DispatchedPass {
                agent,
                stop: Some(pass.stop),
                error: None,
            },
            Err(err) => {
                report.failed = report.failed.saturating_add(1);
                DispatchedPass {
                    agent,
                    stop: None,
                    error: Some(err),
                }
            }
        };
        report.passes.push(pass);
    }
    Ok(report)
}

/// The kinds that the dispatcher serves: each profile with `episodes` set
/// and the trigger `Schedule`. When two profiles share a kind, the later
/// one wins, as in `AgentHarness::agent_profile`.
fn scheduled_kinds(profiles: &[AgentProfile]) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut kinds = Vec::new();
    for profile in profiles.iter().rev() {
        if !seen.insert(profile.kind.as_str()) {
            continue;
        }
        let scheduled = profile
            .episodes
            .as_ref()
            .is_some_and(|settings| settings.trigger == ConsolidationTrigger::Schedule);
        if scheduled {
            kinds.push(profile.kind.clone());
        }
    }
    kinds.sort();
    kinds
}

/// The agents of one firing, oldest pending episode first.
///
/// `candidates` holds each due agent and the time of its oldest pending
/// episode. A tie goes to the lower agent id. The result holds at most
/// [`dispatch_passes`] agents.
#[must_use]
pub fn pick_due(mut candidates: Vec<(AgentId, i64)>, max_passes: u32) -> Vec<AgentId> {
    candidates.sort_by(|(a, at_a), (b, at_b)| at_a.cmp(at_b).then_with(|| a.cmp(b)));
    let bound = dispatch_passes(candidates.len() as u64, max_passes);
    candidates
        .into_iter()
        .take(usize::try_from(bound).unwrap_or(usize::MAX))
        .map(|(agent, _at)| agent)
        .collect()
}

/// Read the due agents of one firing.
///
/// A harness with no scheduled kind returns an empty read and needs no
/// directory. Each directory or store error fails the read, so the
/// activity retries.
pub(crate) async fn read_due(
    harness: &AgentHarness,
    task: &ConsolidateAll,
) -> Result<DueRead, String> {
    let kinds = scheduled_kinds(harness.agent_profiles());
    if kinds.is_empty() {
        return Ok(DueRead::default());
    }
    let directory = harness.agent_directory()?;
    let store = harness.episode_store()?;
    let mut read = DueRead::default();
    let mut candidates = Vec::new();
    for kind in &kinds {
        let mut agents = harness
            .hooked("the agent directory", directory.agents(kind))
            .await?;
        agents.sort();
        agents.dedup();
        // A directory may return an agent of another kind. Its own kind
        // decides its trigger, so skip it here.
        agents.retain(|agent| agent.kind() == kind);
        for agent in agents {
            read.listed = read.listed.saturating_add(1);
            let log = harness
                .hooked("the episode store", store.load(&episode_scope(&agent)))
                .await?;
            let Some(oldest) = log.pending(1).into_iter().next() else {
                read.idle = read.idle.saturating_add(1);
                continue;
            };
            let runs = harness
                .hooked("the agent directory", directory.pass_runs(&agent))
                .await?;
            if runs {
                read.running = read.running.saturating_add(1);
                continue;
            }
            candidates.push((agent, oldest.at_ms));
        }
    }
    read.due = candidates.len() as u64;
    read.agents = pick_due(candidates, task.max_passes);
    Ok(read)
}

/// The error of a harness with no agent directory.
pub(crate) fn directory_missing() -> String {
    ActivityFailure::non_retryable(
        "AgentDirectoryMissing",
        "a kind consolidates on a schedule, but no agent directory is installed: call AgentHarness::directory",
    )
    .into_error_payload()
}

/// The read of one firing, as an activity.
///
/// # Errors
///
/// Returns `AgentDirectoryMissing` or `EpisodeStoreMissing` when a scheduled
/// kind exists, or a directory or store error.
#[activity(
    start_to_close = "5m",
    retry = RetryPolicy::exponential(3, Duration::from_secs(1))
)]
pub async fn agent_consolidate_due(
    ctx: &ActivityContext,
    task: ConsolidateAll,
) -> Result<DueRead, String> {
    read_due(harness(ctx)?, &task).await
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use crate::agent::AgentMemory;
    use crate::bounds::MAX_DISPATCH_PASSES;
    use crate::episode::EpisodeSettings;

    fn id(name: &str) -> AgentId {
        AgentId::new("desk", name).unwrap()
    }

    #[test]
    fn a_missing_directory_is_a_named_error() {
        let payload = directory_missing();
        let failure = autumn_harvest::failure::parse_typed_payload(&payload).unwrap();
        assert_eq!(failure.error_type, "AgentDirectoryMissing");
        assert!(failure.non_retryable);
    }

    #[test]
    fn the_oldest_pending_episode_goes_first() {
        let picked = pick_due(vec![(id("b"), 5), (id("c"), 1), (id("a"), 5)], 16);
        assert_eq!(picked, [id("c"), id("a"), id("b")]);
    }

    #[test]
    fn a_pick_keeps_its_bound() {
        let many: Vec<(AgentId, i64)> = (0..300).map(|n| (id(&format!("a{n:03}")), n)).collect();
        assert_eq!(pick_due(many.clone(), 2).len(), 2);
        assert_eq!(pick_due(many.clone(), 0).len(), 1);
        assert_eq!(pick_due(many, u32::MAX).len(), MAX_DISPATCH_PASSES as usize);
        assert_eq!(pick_due(Vec::new(), 4), Vec::<AgentId>::new());
    }

    #[test]
    fn only_scheduled_kinds_are_served_and_the_later_profile_wins() {
        let stored = |kind: &str| AgentProfile::new(kind).memory(AgentMemory::Store);
        let profiles = [
            stored("b").episodes(EpisodeSettings::default()),
            stored("a").episodes(EpisodeSettings::default()),
            stored("every").episodes(EpisodeSettings::default().every(3)),
            stored("off"),
            // The later profile of `b` keeps no episodes.
            stored("b"),
        ];
        assert_eq!(scheduled_kinds(&profiles), ["a"]);
    }
}
