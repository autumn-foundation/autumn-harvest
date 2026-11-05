//! Where a refine run reads its source runs (issue #2380).
//!
//! Install a [`RefineSources`] with `AgentHarness::refine_sources`.
//! `sqlite::Sources` reads a SQLite file. `postgres::Sources` reads
//! Postgres, and it can also run a live re-probe on a fork (issue #2378).

use std::collections::BTreeSet;

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::types::ExecutionId;
use serde::{Deserialize, Serialize};

use crate::agent::overlay::ProfileOverlay;
use crate::error::AgentError;
use crate::harness::AgentHarness;
use crate::message::TokenUsage;
use crate::model::BoxFuture;
use crate::tool::ToolEffect;

/// One recorded source run.
#[derive(Debug, Clone)]
pub struct SourceRecord {
    /// The workflow type, such as `agent_loop`.
    pub workflow_name: String,
    /// The workflow id. A durable agent uses its agent id.
    pub workflow_id: String,
    /// `true` when the run is in a terminal state.
    pub ended: bool,
    /// The decoded history.
    pub events: Vec<WorkflowEvent>,
}

/// What a live re-probe runs.
#[derive(Debug, Clone, Copy)]
pub struct LiveRequest<'a> {
    /// The harness of the worker.
    pub harness: &'a AgentHarness,
    /// The source run to fork.
    pub source: ExecutionId,
    /// The overlay of the probe.
    pub overlay: &'a ProfileOverlay,
    /// The tool effects that the probe may run.
    pub effects: &'a BTreeSet<ToolEffect>,
}

/// How a live re-probe ended.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiveEnd {
    /// The execution of the fork.
    pub exec_id: String,
    /// `true` when the fork completed.
    pub completed: bool,
    /// The error of a fork that did not complete.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// The tokens of a completed fork, from its report. A fork that did not
    /// complete reports none.
    #[serde(default)]
    pub usage: TokenUsage,
    /// The cost of a completed fork, from its report.
    #[serde(default)]
    pub cost_usd_micros: u64,
}

/// Why a live re-probe did not run.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum LiveProbeError {
    /// The backend has no fork. The value names the backend.
    #[error("{0} has no fork")]
    Unsupported(&'static str),
    /// The fork or the wait failed.
    #[error("the live re-probe failed: {0}")]
    Failed(String),
}

/// A backend that serves the source runs of a refine run.
pub trait RefineSources: Send + Sync + std::fmt::Debug {
    /// The run `run`, or `None` when no run has this id.
    ///
    /// # Errors
    ///
    /// Returns an error when the read fails. The activity then retries.
    fn source(&self, run: ExecutionId) -> BoxFuture<'_, Result<Option<SourceRecord>, AgentError>>;

    /// Run a live re-probe of `request` on a fork, and wait for its end.
    ///
    /// The default has no fork.
    ///
    /// # Errors
    ///
    /// Returns [`LiveProbeError::Unsupported`] when the backend has no
    /// fork, and [`LiveProbeError::Failed`] when the fork fails.
    fn live_probe<'a>(
        &'a self,
        request: LiveRequest<'a>,
    ) -> BoxFuture<'a, Result<LiveEnd, LiveProbeError>> {
        let _ = request;
        Box::pin(std::future::ready(Err(LiveProbeError::Unsupported(
            "this backend",
        ))))
    }
}
