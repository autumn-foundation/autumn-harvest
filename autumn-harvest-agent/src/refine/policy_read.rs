//! The recorded refine policy read (issue #2394).
//!
//! A refine run calls [`read_refine_policy`] once at its start. The
//! activity [`agent_refine_policy`] reads the policy of the kind from the
//! overlay store. History records the result, so a change during the run
//! does not reach that run, and a replay never reads the store.

use std::time::Duration;

use autumn_harvest::failure::{ActivityFailure, IntoActivityErrorString};
use autumn_harvest::policy::RetryPolicy;
use autumn_harvest::prelude::*;

use super::policy::RefinePolicy;
use crate::agent::overlay_store::OverlayStoreError;
use crate::harness::{AgentHarness, truncate};
use crate::mesh::MAX_NAME_LEN;

impl AgentHarness {
    /// The refine policy of `kind`, from the installed overlay store.
    ///
    /// # Errors
    ///
    /// A worker with no store and a backend failure give a retryable error.
    /// Another worker can then take the retry. A corrupt row gives a
    /// non-retryable error.
    pub async fn refine_policy(&self, kind: &str) -> Result<RefinePolicy, String> {
        let kind_name = truncate(kind, MAX_NAME_LEN);
        let Some(store) = self.overlays() else {
            return Err(format!(
                "the refine run of the kind {kind_name:?} reads its policy, and this worker has no overlay store"
            ));
        };
        match store.policy(kind).await {
            Ok(state) => Ok(state.policy),
            Err(OverlayStoreError::Backend { message }) => {
                Err(format!("the overlay store failed: {message}"))
            }
            Err(err) => Err(ActivityFailure::non_retryable(
                "RefinePolicyReadFailed",
                format!("the refine policy of the kind {kind_name:?} does not read: {err}"),
            )
            .into_error_payload()),
        }
    }
}

/// The refine policy of one kind, as an activity. See
/// [`AgentHarness::refine_policy`].
///
/// # Errors
///
/// Returns a retryable error when no store is installed or the store
/// fails, and a non-retryable error for a corrupt row.
#[activity(
    start_to_close = "1m",
    retry = RetryPolicy::exponential(3, Duration::from_secs(1))
)]
pub async fn agent_refine_policy(
    ctx: &ActivityContext,
    kind: String,
) -> Result<RefinePolicy, String> {
    crate::workflow::harness(ctx)?.refine_policy(&kind).await
}

/// Read the refine policy of `kind` through the recorded activity
/// [`agent_refine_policy`].
///
/// Call it once, at the start of a refine run, and pass the result to
/// each step. A replay reads the recorded result.
///
/// # Errors
///
/// Returns the error of the activity.
pub async fn read_refine_policy(ctx: &WorkflowContext, kind: &str) -> HarvestResult<RefinePolicy> {
    ctx.execute_activity(&agent_refine_policy_info(), kind.to_owned())
        .await
}
