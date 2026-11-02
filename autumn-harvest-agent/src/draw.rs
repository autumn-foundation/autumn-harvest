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
//! `{root}:s{n}`. A delegation uses `{root}:d{n}` in the same way.

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
/// Each model request of the run carries `id`. With a `root`, the run is the
/// draw itself, and its run id is `{root}:s{id}`. A delegated child keeps
/// `id` and has no root. It then keeps its own run id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Draw {
    /// The id of the draw.
    pub id: DrawId,
    /// The run id that the draw run id extends. Use an id that no other run
    /// shares, such as an execution id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root: Option<String>,
}

impl Draw {
    /// The draw `id` of the run `root`.
    #[must_use]
    pub fn new(root: impl Into<String>, id: DrawId) -> Self {
        Self {
            id,
            root: Some(root.into()),
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

    /// The run id of the draw run, or `None` when the draw has no root.
    #[must_use]
    pub fn run_id(&self) -> Option<String> {
        self.root.as_deref().map(|root| self.id.run_id(root))
    }
}

/// Run one draw of `task` for each id in `ids`, as child `agent_loop` runs.
///
/// The root of each draw run id is the execution id of the calling
/// workflow, so no later run reuses it. The draws run at the same time. The
/// engine records them in order, so the result order never depends on
/// completion order.
///
/// Each draw runs `task` with these changes:
///
/// - it has its own [`Draw`], so its requests and its cache keys differ;
/// - it does not deliver its answer, because the caller picks the answer;
/// - it reports a failure as [`AgentStop::Failed`](crate::AgentStop::Failed),
///   so the caller keeps its usage.
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
    let root = ctx.execution_id().to_string();
    let mut inputs = Vec::with_capacity(ids.len());
    for id in ids {
        let draw = AgentTask {
            draw: Some(Draw::new(root.clone(), id)),
            deliver: false,
            report_failure: true,
            ..task.clone()
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

    #[test]
    fn a_draw_run_id_extends_its_root() {
        assert_eq!(DrawId::new(2).run_id("e1"), "e1:s2");
        assert_eq!(
            Draw::new("e1", DrawId::new(2)).run_id().as_deref(),
            Some("e1:s2")
        );
        let child = Draw::new("e1", DrawId::new(2)).inherited();
        assert_eq!(child.id, DrawId::new(2));
        assert_eq!(child.run_id(), None);
    }

    #[test]
    fn an_inherited_draw_writes_no_root() {
        let child = Draw::new("e1", DrawId::new(4)).inherited();
        assert_eq!(
            serde_json::to_value(&child).unwrap(),
            serde_json::json!({"id": 4})
        );
    }
}
