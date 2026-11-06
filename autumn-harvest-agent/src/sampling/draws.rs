//! The gate and the fan-out of a strategy run (issue #2439).
//!
//! [`agent_draws`] reads the profile of its kind and the policy of that
//! kind once each, in recorded activities. The profile read applies the
//! active overlay, as the op of a durable agent does (issue #2372). It admits the request with
//! [`admit`], then runs each draw as a child `agent_loop` run. Replay reads
//! the records, so a change during the run never reaches it.
//!
//! The kind must have a registered `AgentProfile`, and the profile builds
//! the task of each draw. The caller gives only the input text. So a caller
//! cannot borrow the policy of a kind and run a task of its own choice.
//!
//! The draws run in waves of at most the admitted concurrency. A wave
//! starts when the wave before it ends. Each draw runs with
//! `allowed_effects` set to the draw effects of the policy, met with the
//! effects of the profile. So a draw never gets more than a single run.
//!
//! #2440 adds draw identity. `agent_sample` (#2443) scores the draws and
//! selects one.

use std::collections::BTreeSet;
use std::num::NonZeroU32;
use std::time::Duration;

use autumn_harvest::policy::RetryPolicy;
use autumn_harvest::prelude::*;
use serde::{Deserialize, Serialize};

use super::admit::{Admission, Ask, Refusal, admit, wave_end};
use super::policy::{ScorerRef, Strategy};
use super::store::PolicyRead;
use crate::agent::overlay_store::OverlayVersion;
use crate::agent::profile_read::{OverlayRead, ProfileRead, overlaid};
use crate::agent::{AgentProfile, agent_profile_info, agent_profile_overlay_info};
use crate::draw::DrawId;
use crate::mesh::valid_name;
use crate::tool::ToolEffect;
use crate::types::{AgentReport, AgentTask, RunIdSource};
use crate::workflow::harness;

/// The workflow name of [`agent_draws`].
pub const DRAWS_WORKFLOW_NAME: &str = "agent_draws";

/// The input of one strategy run.
///
/// It denies unknown keys, so it never carries a policy field. It holds no
/// task: the profile of the kind builds the task of each draw.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DrawsInput {
    /// The agent kind. Its profile builds each draw, and its policy applies.
    pub kind: String,
    /// The user message of each draw.
    pub input: String,
    /// The strategy.
    pub strategy: Strategy,
    /// The draws of the whole run, across all rounds.
    pub draws: u32,
    /// The most draws that run at the same time. `None` takes the cap of
    /// the policy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub concurrency: Option<NonZeroU32>,
    /// The scorer, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scorer: Option<ScorerRef>,
}

impl DrawsInput {
    /// `draws` draws of `input` under `strategy`, for the kind `kind`.
    #[must_use]
    pub fn new(
        kind: impl Into<String>,
        input: impl Into<String>,
        strategy: Strategy,
        draws: u32,
    ) -> Self {
        Self {
            kind: kind.into(),
            input: input.into(),
            strategy,
            draws,
            concurrency: None,
            scorer: None,
        }
    }

    /// Run at most `n` draws at the same time. A value of 0 takes the cap
    /// of the policy.
    #[must_use]
    pub const fn concurrency(mut self, n: u32) -> Self {
        self.concurrency = NonZeroU32::new(n);
        self
    }

    /// Score the draws with `scorer`.
    #[must_use]
    pub fn scorer(mut self, scorer: ScorerRef) -> Self {
        self.scorer = Some(scorer);
        self
    }
}

/// The result of one strategy run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DrawsReport {
    /// The agent kind.
    pub kind: String,
    /// The strategy.
    pub strategy: Strategy,
    /// The revision of the policy that the run read. 0 for every default.
    pub revision: u64,
    /// The most draws that ran at the same time.
    pub concurrency: u32,
    /// The report of each draw, in draw order.
    pub draws: Vec<AgentReport>,
}

/// Run the draws of one strategy run.
///
/// # Errors
///
/// Returns an error that starts with
/// [`REFUSED_PREFIX`](super::REFUSED_PREFIX) when the policy refuses the run.
/// No model call runs before it. Returns an error when the policy read
/// fails for good, or when a draw run fails in the engine.
#[workflow]
pub async fn agent_draws(ctx: &WorkflowContext, input: DrawsInput) -> Result<DrawsReport, String> {
    if !valid_name(&input.kind) {
        return Err(Refusal::InvalidKind.error_text());
    }
    let Some(profile) = read_profile(ctx, &input.kind).await? else {
        return Err(Refusal::UnknownKind.error_text());
    };
    let read = read_policy(ctx, &input.kind).await?;
    let ask = Ask {
        strategy: input.strategy,
        draws: input.draws,
        concurrency: input.concurrency.map_or(0, NonZeroU32::get),
        scorer: input.scorer.as_ref(),
    };
    let admission = admit(&read.policy, &ask).map_err(|refusal| refusal.error_text())?;
    // A cascade asks a model per tier, one tier at a time. A plain fan-out
    // of the profile model would skip each tier rule.
    if input.strategy == Strategy::Cascade {
        let refusal = Refusal::SampleOnly {
            strategy: Strategy::Cascade,
        };
        return Err(refusal.error_text());
    }
    let task = profile_task(&profile, &input.input);
    let draws = run_admitted(ctx, &task, &admission).await?;
    Ok(DrawsReport {
        kind: input.kind,
        strategy: input.strategy,
        revision: read.revision,
        concurrency: admission.concurrency,
        draws,
    })
}

/// The profile of `kind`, as a durable agent reads it for one op (issue
/// #2372). With an overlay store, the active overlay applies. `None` means
/// that no profile has the kind.
pub(super) async fn read_profile(
    ctx: &WorkflowContext,
    kind: &str,
) -> Result<Option<AgentProfile>, String> {
    Ok(read_profile_versioned(ctx, kind)
        .await?
        .map(|(profile, _)| profile))
}

/// [`read_profile`], with the overlay version that the profile holds.
/// `None` is the bare profile.
pub(super) async fn read_profile_versioned(
    ctx: &WorkflowContext,
    kind: &str,
) -> Result<Option<(AgentProfile, Option<OverlayVersion>)>, String> {
    let read: Option<ProfileRead> = ctx
        .execute_activity(&agent_profile_info(), kind.to_owned())
        .await
        .map_err(|err| err.to_string())?;
    let Some(read) = read else {
        return Ok(None);
    };
    if !read.overlay_store {
        return Ok(Some((read.profile, None)));
    }
    let request = OverlayRead {
        kind: kind.to_owned(),
        profile: read.profile.clone(),
    };
    let op: ProfileRead = ctx
        .execute_activity(&agent_profile_overlay_info(), request)
        .await
        .map_err(|err| err.to_string())?;
    Ok(Some(match op.overlay_version {
        Some(version) => (overlaid(&read.profile, &op.profile), Some(version)),
        None => (read.profile, None),
    }))
}

/// Read the policy of `kind` once, through the recorded activity.
pub async fn read_policy(ctx: &WorkflowContext, kind: &str) -> Result<PolicyRead, String> {
    ctx.execute_activity(&agent_sampling_policy_info(), kind.to_owned())
        .await
        .map_err(|err| err.to_string())
}

/// Run the admitted draws of `task` in waves, and return their reports in
/// draw order.
///
/// Each wave is one [`run_draws`](crate::run_draws) batch, so each draw has
/// its own draw id, run id and cache key (issue #2440). Draw `n` has the id
/// `n` across all waves. A wave holds at most `MAX_PARALLEL_CHILDREN`
/// draws, the bound of one batch.
pub async fn run_admitted(
    ctx: &WorkflowContext,
    task: &AgentTask,
    admission: &Admission,
) -> Result<Vec<AgentReport>, String> {
    let draw = draw_task(task, &admission.draw_effects);
    let mut reports = Vec::new();
    let mut start = 0;
    while start < admission.draws {
        let end = wave_end(admission.draws, admission.concurrency, start);
        let ended = crate::draw::run_draws(ctx, &draw, (start..end).map(DrawId::new)).await?;
        for report in ended {
            reports.push(report?);
        }
        start = end;
    }
    Ok(reports)
}

/// The task that `profile` gives for `input`, as a durable agent builds
/// it. It has no memory, no workspace, no follow-ups and no delivery,
/// because a draw is not the answer that reaches a person.
pub(super) fn profile_task(profile: &AgentProfile, input: &str) -> AgentTask {
    let mandate_read_only = profile
        .mandate
        .as_ref()
        .is_some_and(|mandate| mandate.read_only);
    let mut task = AgentTask::new(input).max_steps(profile.max_steps);
    task.system.clone_from(&profile.system);
    task.max_total_tokens = profile.max_total_tokens;
    task.max_output_tokens = profile.max_output_tokens;
    task.approval_timeout_secs = profile.approval_timeout_secs;
    task.max_request_bytes = profile.max_request_bytes;
    task.loop_guard = profile.loop_guard;
    task.read_only = profile.read_only || mandate_read_only;
    task.tenant.clone_from(&profile.tenant);
    task.mandate.clone_from(&profile.mandate);
    task.model.clone_from(&profile.model);
    task.mesh.clone_from(&profile.mesh);
    task.stream = profile.stream;
    task.context_reset = profile.context_reset;
    task.tool_guidance.clone_from(&profile.tool_guidance);
    task.skills.clone_from(&profile.skills);
    task
}

/// The task of one draw.
///
/// - Its effects are the draw effects met with the effects of `task`.
/// - It is read-only unless its effects hold `write` or `external`. A
///   worker that predates `allowed_effects` then still refuses a write.
/// - It runs no delegation at the same time as another, so the open model
///   calls stay within the concurrency of the run.
/// - It saves no progress notes unless `internal` is allowed, so it has no
///   context reset then.
/// - It reports a failure, so the run keeps the usage of a failed draw.
/// - It delivers nothing and books no follow-up. Only the selected answer
///   may reach a person.
/// - It has no memory and no workspace, so N draws never make N writes to
///   one store (issue #2274).
/// - `run_draws` gives it a draw id, so its cache key differs from the key
///   of each other draw (issue #2440).
pub(super) fn draw_task(task: &AgentTask, draw_effects: &BTreeSet<ToolEffect>) -> AgentTask {
    let allowed_effects: BTreeSet<ToolEffect> = task.allowed_effects.as_ref().map_or_else(
        || draw_effects.clone(),
        |own| own.intersection(draw_effects).copied().collect(),
    );
    let writes = allowed_effects.contains(&ToolEffect::Write)
        || allowed_effects.contains(&ToolEffect::External);
    let context_reset = task
        .context_reset
        .filter(|_| allowed_effects.contains(&ToolEffect::Internal));
    AgentTask {
        read_only: task.read_only || !writes,
        allowed_effects: Some(allowed_effects),
        run_id_source: RunIdSource::ExecutionId,
        report_failure: true,
        deliver: false,
        followups: None,
        workspace: None,
        memory_scope: None,
        memory_in_state: false,
        parallel_children: 0,
        context_reset,
        ..task.clone()
    }
}

/// The sampling policy of one kind, as an activity.
///
/// A harness with no store returns every default.
///
/// # Errors
///
/// Returns a non-retryable error for a stored policy that does not decode,
/// and a retryable one when the store fails.
#[activity(
    start_to_close = "1m",
    retry = RetryPolicy::exponential(3, Duration::from_secs(1))
)]
pub async fn agent_sampling_policy(
    ctx: &ActivityContext,
    kind: String,
) -> Result<PolicyRead, String> {
    harness(ctx)?.sampling_policy(&kind).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_draw_meets_the_effects_of_its_task() {
        let draw_effects = BTreeSet::from([ToolEffect::ReadOnly, ToolEffect::Write]);
        let open = draw_task(&AgentTask::new("x"), &draw_effects);
        assert_eq!(open.allowed_effects, Some(draw_effects.clone()));
        let mut narrow = AgentTask::new("x");
        narrow.allowed_effects = Some(BTreeSet::from([ToolEffect::ReadOnly]));
        let draw = draw_task(&narrow, &draw_effects);
        assert_eq!(
            draw.allowed_effects,
            Some(BTreeSet::from([ToolEffect::ReadOnly]))
        );
        assert!(draw.report_failure);
        assert!(!draw.deliver);
        assert!(draw.read_only, "a draw with no write effect is read-only");
        assert_eq!(draw.parallel_children, 0);
        assert_eq!(draw.run_id_source, RunIdSource::ExecutionId);
        assert!(!open.read_only, "a draw with a write effect may write");
    }

    #[test]
    fn a_draw_builds_its_task_from_the_profile() {
        let mut profile = AgentProfile::new("researcher");
        profile.system = Some("Be brief.".to_owned());
        profile.max_steps = 3;
        profile.read_only = true;
        profile.parallel_children = 8;
        let task = profile_task(&profile, "Name a prime.");
        assert_eq!(task.input, "Name a prime.");
        assert_eq!(task.system.as_deref(), Some("Be brief."));
        assert_eq!(task.max_steps, 3);
        assert!(task.read_only);
        assert_eq!(task.memory_scope, None);
        assert!(!task.deliver);
    }

    #[test]
    fn a_draw_keeps_a_read_only_task_read_only() {
        let draw = draw_task(
            &AgentTask::new("x").read_only(),
            &BTreeSet::from([ToolEffect::Write]),
        );
        assert!(draw.read_only);
    }

    #[test]
    fn concurrency_zero_takes_the_cap() {
        let input = DrawsInput::new("k", "x", Strategy::Vote, 3).concurrency(0);
        assert_eq!(input.concurrency, None);
        let value = serde_json::to_value(&input).unwrap();
        assert!(value.get("concurrency").is_none());
        let zero = serde_json::json!({
            "kind": "k", "input": "x", "strategy": "vote", "draws": 3, "concurrency": 0,
        });
        assert!(serde_json::from_value::<DrawsInput>(zero).is_err());
    }
}
