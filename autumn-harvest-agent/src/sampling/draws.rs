//! The gate and the fan-out of a strategy run (issue #2439).
//!
//! [`agent_draws`] reads the policy of its kind once, in the recorded
//! activity [`agent_sampling_policy`]. It admits the request with
//! [`admit`], then runs each draw as a child `agent_loop` run. Replay reads
//! the recorded policy, so a change during the run never reaches it.
//!
//! The draws run in waves of at most the admitted concurrency. A wave
//! starts when the wave before it ends. Each draw runs with
//! `allowed_effects` set to the draw effects of the policy, met with the
//! effects of its task. So a draw never gets more than a single run.
//!
//! #2440 adds draw identity. #2441 and #2443 score the draws and select
//! one.

use std::collections::BTreeSet;
use std::num::NonZeroU32;
use std::time::Duration;

use autumn_harvest::policy::RetryPolicy;
use autumn_harvest::prelude::*;
use serde::{Deserialize, Serialize};

use super::admit::{Admission, Ask, Refusal, admit, wave_end};
use super::policy::{ScorerRef, Strategy};
use super::store::PolicyRead;
use crate::mesh::valid_name;
use crate::tool::ToolEffect;
use crate::types::{AgentReport, AgentTask, RunIdSource};
use crate::workflow::{WORKFLOW_NAME, harness};

/// The workflow name of [`agent_draws`].
pub const DRAWS_WORKFLOW_NAME: &str = "agent_draws";

/// The input of one strategy run.
///
/// It denies unknown keys, so it never carries a policy field.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DrawsInput {
    /// The agent kind whose policy applies.
    pub kind: String,
    /// The task of each draw.
    pub task: AgentTask,
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
    /// `draws` draws of `task` under `strategy`, for the kind `kind`.
    #[must_use]
    pub fn new(kind: impl Into<String>, task: AgentTask, strategy: Strategy, draws: u32) -> Self {
        Self {
            kind: kind.into(),
            task,
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
    let read = read_policy(ctx, &input.kind).await?;
    let ask = Ask {
        strategy: input.strategy,
        draws: input.draws,
        concurrency: input.concurrency.map_or(0, NonZeroU32::get),
        scorer: input.scorer.as_ref(),
    };
    let admission = admit(&read.policy, &ask).map_err(|refusal| refusal.error_text())?;
    let draws = run_draws(ctx, &input.task, &admission).await?;
    Ok(DrawsReport {
        kind: input.kind,
        strategy: input.strategy,
        revision: read.revision,
        concurrency: admission.concurrency,
        draws,
    })
}

/// Read the policy of `kind` once, through the recorded activity.
pub async fn read_policy(ctx: &WorkflowContext, kind: &str) -> Result<PolicyRead, String> {
    ctx.execute_activity(&agent_sampling_policy_info(), kind.to_owned())
        .await
        .map_err(|err| err.to_string())
}

/// Run the admitted draws of `task` in waves, and return their reports in
/// draw order.
pub async fn run_draws(
    ctx: &WorkflowContext,
    task: &AgentTask,
    admission: &Admission,
) -> Result<Vec<AgentReport>, String> {
    let draw = draw_task(task, &admission.draw_effects);
    let input = serde_json::to_value(&draw).map_err(|err| err.to_string())?;
    let mut reports = Vec::new();
    let mut start = 0;
    while start < admission.draws {
        let end = wave_end(admission.draws, admission.concurrency, start);
        let wave = (start..end)
            .map(|_| (WORKFLOW_NAME.to_owned(), input.clone()))
            .collect();
        let ended = ctx
            .spawn_child_workflow_fan_out_collect_raw(wave)
            .await
            .map_err(|err| err.to_string())?;
        for output in ended {
            let report = output.and_then(|value| {
                serde_json::from_value::<AgentReport>(value).map_err(|err| err.to_string())
            })?;
            reports.push(report);
        }
        start = end;
    }
    Ok(reports)
}

/// The task of one draw.
///
/// - Its effects are the draw effects met with the effects of `task`.
/// - It reports a failure, so the run keeps the usage of a failed draw.
/// - It delivers nothing and books no follow-up. Only the selected answer
///   may reach a person.
/// - It has no workspace, so the draws never race on one head (issue
///   #2274).
fn draw_task(task: &AgentTask, draw_effects: &BTreeSet<ToolEffect>) -> AgentTask {
    let allowed_effects = task.allowed_effects.as_ref().map_or_else(
        || draw_effects.clone(),
        |own| own.intersection(draw_effects).copied().collect(),
    );
    AgentTask {
        allowed_effects: Some(allowed_effects),
        run_id_source: RunIdSource::ExecutionId,
        report_failure: true,
        deliver: false,
        followups: None,
        workspace: None,
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
        assert_eq!(draw.run_id_source, RunIdSource::ExecutionId);
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
        let input = DrawsInput::new("k", AgentTask::new("x"), Strategy::Vote, 3).concurrency(0);
        assert_eq!(input.concurrency, None);
        let value = serde_json::to_value(&input).unwrap();
        assert!(value.get("concurrency").is_none());
        let zero = serde_json::json!({
            "kind": "k", "task": serde_json::to_value(AgentTask::new("x")).unwrap(),
            "strategy": "vote", "draws": 3, "concurrency": 0,
        });
        assert!(serde_json::from_value::<DrawsInput>(zero).is_err());
    }
}
