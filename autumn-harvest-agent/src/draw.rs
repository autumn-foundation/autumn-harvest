//! Draw identity (issue #2440).
//!
//! A draw is one sample of a run. Best-of-N, a vote and beam search run
//! several draws of one task. Each draw needs its own identity, so that:
//!
//! - the response cache keeps one entry per draw, not one per request;
//! - a model can pass the draw to its provider as a seed;
//! - tools get their own idempotency key per draw.
//!
//! [`run_draws`] runs each draw as a child `agent_loop` run with the run id
//! `{root}:s{n}`. An inline delegation uses `{root}:d{n}` in the same way.
//! A durable agent segment also ends in `:s{n}`, but its root is an
//! incarnation id. The two never share a root.

use std::collections::BTreeSet;

use autumn_harvest::prelude::WorkflowContext;
use serde::{Deserialize, Serialize};

use crate::types::{AgentReport, AgentTask, MAX_PARALLEL_CHILDREN};
use crate::workflow::WORKFLOW_NAME;

/// The id of one draw of a run.
///
/// It is a bare number on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DrawId(u32);

impl DrawId {
    /// The draw with the number `n`.
    #[must_use]
    pub const fn new(n: u32) -> Self {
        Self(n)
    }

    /// The number of the draw.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }

    /// The run id of this draw under `root`: `{root}:s{n}`.
    #[must_use]
    pub fn run_id(self, root: &str) -> String {
        format!("{root}:s{}", self.0)
    }
}

impl std::fmt::Display for DrawId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "s{}", self.0)
    }
}

/// The draw of an [`AgentTask`].
///
/// Each model request of the run carries `id`. With a root, the run is the
/// draw itself, and its run id is `{root}:s{id}`. A delegated child keeps
/// `id` and has no root. It then keeps its own run id.
///
/// Only [`run_draws`] sets a root. The loop refuses a root that is not a
/// UUID, so a workflow id never becomes a root. The guard stops mistakes.
/// It is not a security boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Draw {
    /// The id of the draw.
    pub id: DrawId,
    /// The root of the draw run id. It is a UUID that the parent recorded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    root: Option<String>,
}

impl Draw {
    /// The draw `id` of the batch `root`.
    pub(crate) const fn rooted(root: String, id: DrawId) -> Self {
        Self {
            id,
            root: Some(root),
        }
    }

    /// The draw that a delegated child keeps: the same id and no root.
    #[must_use]
    pub const fn inherited(&self) -> Self {
        Self {
            id: self.id,
            root: None,
        }
    }

    /// The root of the draw run id, if any.
    #[must_use]
    pub fn root(&self) -> Option<&str> {
        self.root.as_deref()
    }

    /// The run id of the draw run, or `None` when the draw has no root.
    ///
    /// # Errors
    ///
    /// Returns an error when the root is not a UUID. A root such as a
    /// workflow id can repeat across runs, and tools then share an
    /// idempotency key. The check reads the input only, so replay gives the
    /// same result as the live run.
    pub(crate) fn run_id(&self) -> Result<Option<String>, String> {
        let Some(root) = self.root.as_deref() else {
            return Ok(None);
        };
        if autumn_harvest::uuid::Uuid::parse_str(root).is_err() {
            return Err(format!(
                "the draw root {root:?} is not one that run_draws recorded"
            ));
        }
        Ok(Some(self.id.run_id(root)))
    }
}

/// Run one draw of `task` for each id in `ids`, as child `agent_loop` runs.
///
/// Each call records a new UUID as the root of its batch. Two batches of one
/// run never share a run id, and a reset or a fork replays the same root.
/// The draws may run at the same time. The engine records them in order, so
/// the result order never depends on completion order.
///
/// Each draw runs `task` with these changes:
///
/// - it has its own [`Draw`], so its requests and its cache keys differ;
/// - it does not deliver its answer, because the caller picks the answer;
/// - it reports a failure as [`AgentStop::Failed`](crate::AgentStop::Failed),
///   so the caller keeps its usage;
/// - it has no memory, no workspace and no follow-ups, as a delegated child.
///   A draw that the caller does not pick then leaves no durable state.
///
/// Each draw gets the whole token cap and the whole mandate of `task`. The
/// caller bounds the total, for example with a smaller cap per draw.
///
/// Returns one entry per id, in order. An entry is `Err` only when a draw
/// run ends with an error that it could not report.
///
/// # Errors
///
/// Returns an error, and starts no draw, when `ids` holds more than
/// [`MAX_PARALLEL_CHILDREN`] ids or one id twice. Returns an engine error,
/// such as a cancellation, as text.
pub async fn run_draws(
    ctx: &WorkflowContext,
    task: &AgentTask,
    ids: impl IntoIterator<Item = DrawId>,
) -> Result<Vec<Result<AgentReport, String>>, String> {
    let ids: Vec<DrawId> = ids.into_iter().collect();
    // Check the batch before the copies, so a bad batch copies no task.
    check_batch(&ids)?;
    let tasks = ids.into_iter().map(|id| (id, task.clone())).collect();
    run_draw_tasks(ctx, tasks).await
}

/// Refuse a batch over [`MAX_PARALLEL_CHILDREN`] ids, or with one id twice.
fn check_batch(ids: &[DrawId]) -> Result<(), String> {
    let bound = usize::try_from(MAX_PARALLEL_CHILDREN).unwrap_or(usize::MAX);
    if ids.len() > bound {
        return Err(format!(
            "a batch holds at most {MAX_PARALLEL_CHILDREN} draws, not {}",
            ids.len()
        ));
    }
    let mut unique = BTreeSet::new();
    if let Some(id) = ids.iter().find(|id| !unique.insert(**id)) {
        return Err(format!("the batch holds the draw {id} twice"));
    }
    Ok(())
}

/// [`run_draws`] with a task for each draw (issue #2443).
///
/// A sample run gives each draw of a wave its own token cap. The batch
/// shares one root, as in `run_draws`.
pub(crate) async fn run_draw_tasks(
    ctx: &WorkflowContext,
    tasks: Vec<(DrawId, AgentTask)>,
) -> Result<Vec<Result<AgentReport, String>>, String> {
    let ids: Vec<DrawId> = tasks.iter().map(|(id, _)| *id).collect();
    check_batch(&ids)?;
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    // History records the root, so replay, reset and fork read it back.
    let root = ctx.new_uuid().to_string();
    let mut inputs = Vec::with_capacity(ids.len());
    for (id, task) in tasks {
        // A send from a draw back up the chain is a deadlock, as in a fan-out.
        let mut waiting = task.waiting.clone();
        if crate::agent::AgentId::parse(ctx.workflow_id()).is_ok() {
            waiting.push(ctx.workflow_id().to_owned());
        }
        let draw = AgentTask {
            draw: Some(Draw::rooted(root.clone(), id)),
            deliver: false,
            report_failure: true,
            memory_scope: None,
            memory_in_state: false,
            unattended_memory_writes: false,
            workspace: None,
            followups: None,
            waiting,
            ..task
        };
        let input = serde_json::to_value(&draw).map_err(|err| err.to_string())?;
        inputs.push((WORKFLOW_NAME.to_owned(), input));
    }
    let ended = ctx
        .spawn_child_workflow_fan_out_collect_raw(inputs)
        .await
        .map_err(|err| err.to_string())?;
    Ok(ended
        .into_iter()
        .map(|output| {
            output.and_then(|value| {
                serde_json::from_value::<AgentReport>(value).map_err(|err| err.to_string())
            })
        })
        .collect())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn a_draw_id_is_a_bare_number_on_the_wire() {
        assert_eq!(serde_json::to_string(&DrawId::new(7)).unwrap(), "7");
        assert_eq!(serde_json::from_str::<DrawId>("7").unwrap(), DrawId::new(7));
    }

    const ROOT: &str = "01890a5d-ac96-774b-bcce-b302099a8057";

    #[test]
    fn a_draw_run_id_extends_its_root() {
        assert_eq!(DrawId::new(2).run_id("e1"), "e1:s2");
        let draw = Draw::rooted(ROOT.to_owned(), DrawId::new(2));
        assert_eq!(
            draw.run_id().unwrap().as_deref(),
            Some(format!("{ROOT}:s2").as_str())
        );
        let child = draw.inherited();
        assert_eq!(child.id, DrawId::new(2));
        assert_eq!(child.run_id().unwrap(), None);
        assert_eq!(child.run_id().unwrap(), None);
    }

    #[test]
    fn a_root_that_run_draws_did_not_record_is_refused() {
        let forged = Draw::rooted("daily".to_owned(), DrawId::new(0));
        assert!(forged.run_id().is_err());
        let forged = Draw::rooted(format!("{ROOT}:s1"), DrawId::new(0));
        assert!(forged.run_id().is_err());
    }

    #[test]
    fn a_batch_over_the_bound_or_with_a_repeat_is_refused() {
        let bound = MAX_PARALLEL_CHILDREN;
        let full: Vec<DrawId> = (0..bound).map(DrawId::new).collect();
        assert!(check_batch(&full).is_ok());
        let over: Vec<DrawId> = (0..=bound).map(DrawId::new).collect();
        assert!(check_batch(&over).unwrap_err().contains("at most"));
        let twice = [DrawId::new(1), DrawId::new(1)];
        assert!(check_batch(&twice).unwrap_err().contains("twice"));
    }

    #[test]
    fn an_inherited_draw_writes_no_root() {
        let child = Draw::rooted(ROOT.to_owned(), DrawId::new(4)).inherited();
        assert_eq!(
            serde_json::to_value(&child).unwrap(),
            serde_json::json!({"id": 4})
        );
        let back: Draw = serde_json::from_value(serde_json::json!({"id": 4})).unwrap();
        assert_eq!(back, child);
    }
}
