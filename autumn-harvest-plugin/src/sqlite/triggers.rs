//! Run starts for the trigger surfaces on SQLite (issue #2471).
//!
//! Webhook receivers and broker connectors start or signal runs through the
//! [`HarvestSqlite`] handle, not the Postgres start path. Each function
//! answers the Postgres response of the route it stands in for. So the
//! caller reads the answer with its own code, on both backends.
//!
//! Each caller gets the handle from `HarvestApiState::sqlite_handle`. That
//! is `None` on Postgres, and a `503` before startup on SQLite.

use autumn_harvest::WorkflowIdReusePolicy;
use autumn_harvest_sqlite::StartOptions;
use autumn_web::reexports::axum::response::Response;
use serde_json::Value;

use super::api::{signal_with_start_reply, start_reply};
use super::{HarvestSqlite, SignalWithStartParams};

/// Start `workflow_name` with an optional business id, under the Postgres
/// default reuse policy, `allow_duplicate`. It answers the Postgres `POST /workflows/{name}/start`
/// response: `201` for a new run, `200` for an attach.
pub async fn start(
    handle: &HarvestSqlite,
    workflow_name: &str,
    workflow_id: Option<String>,
    input: Value,
) -> Response {
    // The Postgres default of a request with no `reuse_policy`.
    let mut options = StartOptions::new().reuse_policy(WorkflowIdReusePolicy::AllowDuplicate);
    options.workflow_id = workflow_id;
    start_reply(handle, workflow_name, input, options).await
}

/// Signal the run of `(workflow_name, workflow_id)`, or start it first. It
/// answers the Postgres `POST /workflows/{name}/signal-with-start`
/// response.
///
/// `idempotency_key` dedupes the signal across the run chain. A connector
/// key keeps its reserved `conn:` prefix: only the HTTP route refuses one.
pub async fn signal_with_start(
    handle: &HarvestSqlite,
    workflow_name: &str,
    workflow_id: String,
    signal_name: String,
    payload: Value,
    idempotency_key: Option<String>,
) -> Response {
    let request = SignalWithStartParams {
        workflow_name: workflow_name.to_owned(),
        workflow_id,
        input: payload.clone(),
        reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
        signal_name,
        signal_payload: payload,
        idempotency_key,
    };
    signal_with_start_reply(handle, request).await
}
