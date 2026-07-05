//! Inbound HTTP webhook receiver route generation and dispatch (issue #344).
//!
//! Mirrors the `mcp_tools.rs` route-generation pattern (issue #597): app-level
//! [`autumn_web::Route`] values are generated at [`crate::plugin::HarvestPlugin::build`]
//! time from the descriptors [`HarvestPlugin::webhooks(...)`] registered, one
//! per `#[webhook]` binding.
//!
//! # Who verifies what
//!
//! Every generated route's handler takes
//! `Result<autumn_web::webhook::SignedWebhook, autumn_web::AutumnError>` as
//! its extractor argument -- signature, timestamp, and (if the endpoint
//! configures it) replay-duplicate verification have **already run**, inside
//! axum's extractor phase, before this module's handler code executes at
//! all. This module never parses a raw, unverified request body. See
//! `docs/getting-started/12-webhooks.md` for the `[security.webhooks]`
//! configuration side.
//!
//! # Dispatch
//!
//! After verification, parsing, and the mapping function run, dispatch
//! delegates to the same primitives the plain management API uses --
//! [`crate::api::start_workflow`] / [`crate::api::signal_with_start_workflow`]
//! -- so schema validation, the admission gate, debounce/batch/SLA/execution-
//! timeout resolution, and audit all apply identically to a webhook-triggered
//! start. This handler only reshapes the successful response into the
//! `{"status", "workflow_exec_id", "workflow_id"}` envelope the issue
//! specifies and classifies outcomes for `harvest.webhook.received`/`.rejected`.

use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::telemetry::{MetricsRecorder, WebhookOutcome};
use autumn_harvest::webhook_trigger::{
    WebhookCtx, WebhookHandlerError, WebhookTarget, WebhookTriggerInfo, validate_webhook_triggers,
};
use autumn_web::reexports::axum;
use autumn_web::webhook::SignedWebhook;
use axum::response::IntoResponse as _;
use axum::{Extension, Json};

use crate::api::{HarvestApiState, SignalWithStartRequest, StartWorkflowRequest};

/// Build the app-level routes for a set of registered webhook triggers.
///
/// # Panics
///
/// Panics when [`validate_webhook_triggers`] rejects the set (duplicate or
/// malformed binding paths) or when a trigger targets a workflow that is not
/// registered on the builder -- a mount conflict or dangling target must
/// fail at `HarvestPlugin::build` time, not on the first live request.
#[must_use]
pub fn build_webhook_routes(
    triggers: &[WebhookTriggerInfo],
    registered_workflows: &[WorkflowInfo],
    api_state: &HarvestApiState,
) -> Vec<autumn_web::Route> {
    if let Err(e) = validate_webhook_triggers(triggers) {
        panic!("HarvestPlugin::webhooks(...) failed validation: {e}");
    }
    for trigger in triggers {
        let workflow = trigger.target.workflow();
        assert!(
            registered_workflows.iter().any(|w| w.name == workflow),
            "webhook trigger '{}' (path '{}') targets workflow '{workflow}', which is not \
             registered on HarvestPlugin -- register it via .workflows(workflows![...]) before \
             .webhooks(webhooks![...])",
            trigger.name,
            trigger.path
        );
    }
    triggers
        .iter()
        .map(|trigger| build_webhook_route(trigger, api_state.clone()))
        .collect()
}

fn build_webhook_route(
    trigger: &WebhookTriggerInfo,
    api_state: HarvestApiState,
) -> autumn_web::Route {
    let path = trigger.path;
    let name = trigger.name;
    let target = trigger.target;
    let handler_fn = trigger.handler;
    let queue = trigger.queue;

    let handler = axum::routing::post(
        move |hook: Result<SignedWebhook, autumn_web::AutumnError>| {
            let api_state = api_state.clone();
            // Boxed: the delegated start/signal-with-start handler's future is
            // large (clippy::large_futures) and this is a cold edge path.
            async move {
                Box::pin(handle_webhook(
                    api_state, path, target, handler_fn, queue, hook,
                ))
                .await
            }
        },
    );

    autumn_web::Route {
        method: axum::http::Method::POST,
        path,
        handler,
        name,
        api_doc: autumn_web::openapi::ApiDoc {
            method: "POST",
            path,
            operation_id: name,
            description: Some(
                "Inbound webhook receiver (issue #344). Signature/timestamp/replay \
                 verification is configured under [security.webhooks], not here.",
            ),
            success_status: 202,
            // Never an MCP tool: an inbound webhook is reached by an external
            // sender's HMAC signature, not an authenticated MCP caller.
            mcp_tool: false,
            ..Default::default()
        },
        api_version: None,
        sunset_opt_out: false,
        repository: None,
        idempotency: autumn_web::RouteIdempotency::Direct,
    }
}

/// Record `harvest.webhook.received` unconditionally and
/// `harvest.webhook.rejected` for every outcome except [`WebhookOutcome::Accepted`]
/// and [`WebhookOutcome::IdempotentReplay`] (both are successful dispatch
/// outcomes, not rejections).
fn record_outcome(metrics: &dyn MetricsRecorder, path: &str, outcome: WebhookOutcome) {
    metrics.record_webhook_received(path, outcome);
    if !matches!(
        outcome,
        WebhookOutcome::Accepted | WebhookOutcome::IdempotentReplay
    ) {
        metrics.record_webhook_rejected(path, outcome);
    }
}

fn error_response(
    status: axum::http::StatusCode,
    error_code: &str,
    message: &str,
) -> axum::response::Response {
    (
        status,
        Json(serde_json::json!({ "error_code": error_code, "error": message })),
    )
        .into_response()
}

/// Classify a `SignedWebhook` extraction failure's HTTP status into a
/// [`WebhookOutcome`]. A `409 Conflict` (autumn-web's own replay-protection
/// layer rejecting a duplicate delivery ID) is a benign redelivery signal,
/// not a verification failure -- everything else genuinely failed
/// verification.
fn outcome_for_verify_status(status: axum::http::StatusCode) -> WebhookOutcome {
    if status == axum::http::StatusCode::CONFLICT {
        WebhookOutcome::IdempotentReplay
    } else {
        WebhookOutcome::VerifyFailed
    }
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn handle_webhook(
    api_state: HarvestApiState,
    path: &'static str,
    target: WebhookTarget,
    handler: autumn_harvest::webhook_trigger::WebhookHandlerFn,
    queue: Option<&'static str>,
    hook: Result<SignedWebhook, autumn_web::AutumnError>,
) -> axum::response::Response {
    // Check the verification result *before* the runtime-installed check: a
    // sender's bad signature is their problem, not ours, and must be
    // reported as such (401/400) even during the boot window before
    // `on_startup` installs the runtime -- masking it behind a generic
    // "runtime not started" would misdirect a legitimate integration
    // debugging a real signature/secret mismatch. Metrics are best-effort
    // here: if the runtime isn't up yet there is no `MetricsRecorder` to
    // record against (documented boot-window limitation, consistent with
    // every other management route).
    let hook = match hook {
        Ok(hook) => hook,
        Err(err) => {
            if let Ok(runtime) = api_state.runtime() {
                let metrics = runtime.registry().telemetry().metrics.clone();
                record_outcome(
                    metrics.as_ref(),
                    path,
                    outcome_for_verify_status(err.status()),
                );
            }
            return err.into_response();
        }
    };

    let runtime = match api_state.runtime() {
        Ok(r) => r,
        Err(e) => return crate::api::map_error(e).into_response(),
    };
    let metrics = runtime.registry().telemetry().metrics.clone();

    let payload: serde_json::Value = match serde_json::from_slice(hook.raw_body()) {
        Ok(v) => v,
        Err(e) => {
            record_outcome(metrics.as_ref(), path, WebhookOutcome::ParseFailed);
            return error_response(
                axum::http::StatusCode::BAD_REQUEST,
                "parse_failed",
                &format!("webhook body is not valid JSON: {e}"),
            );
        }
    };

    let ctx = WebhookCtx::new(
        path,
        hook.endpoint().to_string(),
        hook.provider().to_string(),
        hook.delivery_id().map(str::to_string),
        hook.event_type().map(str::to_string),
        hook.raw_body().to_vec(),
    );

    if matches!(target, WebhookTarget::SignalsWithStart { .. }) && ctx.delivery_id.is_none() {
        record_outcome(metrics.as_ref(), path, WebhookOutcome::MissingIdempotency);
        return error_response(
            axum::http::StatusCode::BAD_REQUEST,
            "missing_idempotency",
            &format!(
                "endpoint '{}' resolves no delivery id -- a SignalsWithStart webhook target \
                 requires one (configure delivery_id_header under [security.webhooks] or \
                 include a top-level \"id\" field in the payload)",
                ctx.endpoint
            ),
        );
    }

    let workflow_id = match handler(&ctx, payload.clone()) {
        Ok(id) => id,
        Err(WebhookHandlerError::Deserialize(msg)) => {
            record_outcome(metrics.as_ref(), path, WebhookOutcome::ParseFailed);
            return error_response(axum::http::StatusCode::BAD_REQUEST, "parse_failed", &msg);
        }
        Err(WebhookHandlerError::Rejected(msg)) => {
            record_outcome(metrics.as_ref(), path, WebhookOutcome::ParseFailed);
            return error_response(
                axum::http::StatusCode::BAD_REQUEST,
                "mapping_rejected",
                &msg,
            );
        }
    };

    let dispatch_response = match target {
        WebhookTarget::Starts { workflow } => {
            Box::pin(crate::api::start_workflow(
                Extension(api_state.clone()),
                axum::extract::Path(workflow.to_string()),
                None,
                axum::http::HeaderMap::new(),
                Json(StartWorkflowRequest::from_webhook(
                    workflow_id.as_str().to_string(),
                    payload,
                    queue.map(str::to_string),
                )),
            ))
            .await
        }
        WebhookTarget::SignalsWithStart {
            workflow,
            signal_name,
        } => {
            Box::pin(crate::api::signal_with_start_workflow(
                Extension(api_state.clone()),
                axum::extract::Path(workflow.to_string()),
                None,
                axum::http::HeaderMap::new(),
                Json(SignalWithStartRequest::from_webhook(
                    workflow_id.as_str().to_string(),
                    payload,
                    signal_name.to_string(),
                    ctx.delivery_id.clone(),
                    queue.map(str::to_string),
                )),
            ))
            .await
        }
    };

    reshape_dispatch_response(metrics.as_ref(), path, dispatch_response).await
}

/// A minimal shape shared by [`crate::api`]'s `StartWorkflowResponse` and
/// `SignalWithStartResponse` -- both carry `execution_id`/`workflow_id`,
/// which is all this module needs to build its own envelope. Reading only
/// these two fields (rather than the private response types) avoids
/// widening either type's visibility beyond what's needed.
#[derive(serde::Deserialize)]
struct DispatchIds {
    execution_id: String,
    workflow_id: String,
}

/// Rewrite a successful `start_workflow`/`signal_with_start_workflow` response
/// (`201`/`200`) into the issue's `{"status", "workflow_exec_id",
/// "workflow_id"}` envelope (`202`/`200`); pass any error response through
/// unchanged (it already carries a structured JSON body from the delegate).
async fn reshape_dispatch_response(
    metrics: &dyn MetricsRecorder,
    path: &str,
    response: axum::response::Response,
) -> axum::response::Response {
    let status = response.status();
    if status != axum::http::StatusCode::CREATED && status != axum::http::StatusCode::OK {
        record_outcome(metrics, path, WebhookOutcome::InternalError);
        return response;
    }

    let bytes = match axum::body::to_bytes(response.into_body(), 1024 * 1024).await {
        Ok(b) => b,
        Err(e) => {
            record_outcome(metrics, path, WebhookOutcome::InternalError);
            return error_response(
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                &format!("failed to read dispatch response: {e}"),
            );
        }
    };
    let parsed: DispatchIds = match serde_json::from_slice(&bytes) {
        Ok(p) => p,
        Err(e) => {
            record_outcome(metrics, path, WebhookOutcome::InternalError);
            return error_response(
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                &format!("failed to parse dispatch response: {e}"),
            );
        }
    };

    let outcome = if status == axum::http::StatusCode::CREATED {
        WebhookOutcome::Accepted
    } else {
        WebhookOutcome::IdempotentReplay
    };
    record_outcome(metrics, path, outcome);
    let http_status = if outcome == WebhookOutcome::Accepted {
        axum::http::StatusCode::ACCEPTED
    } else {
        axum::http::StatusCode::OK
    };
    (
        http_status,
        Json(serde_json::json!({
            "status": outcome.as_str(),
            "workflow_exec_id": parsed.execution_id,
            "workflow_id": parsed.workflow_id,
        })),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct RecordingMetrics {
        received: std::sync::Mutex<Vec<(String, WebhookOutcome)>>,
        rejected: std::sync::Mutex<Vec<(String, WebhookOutcome)>>,
    }

    impl MetricsRecorder for RecordingMetrics {
        fn record_webhook_received(&self, path: &str, outcome: WebhookOutcome) {
            self.received
                .lock()
                .unwrap()
                .push((path.to_string(), outcome));
        }
        fn record_webhook_rejected(&self, path: &str, outcome: WebhookOutcome) {
            self.rejected
                .lock()
                .unwrap()
                .push((path.to_string(), outcome));
        }
    }

    #[test]
    fn outcome_for_verify_status_maps_conflict_to_idempotent_replay() {
        assert_eq!(
            outcome_for_verify_status(axum::http::StatusCode::CONFLICT),
            WebhookOutcome::IdempotentReplay
        );
    }

    #[test]
    fn outcome_for_verify_status_maps_everything_else_to_verify_failed() {
        for status in [
            axum::http::StatusCode::BAD_REQUEST,
            axum::http::StatusCode::UNAUTHORIZED,
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        ] {
            assert_eq!(
                outcome_for_verify_status(status),
                WebhookOutcome::VerifyFailed
            );
        }
    }

    #[test]
    fn record_outcome_always_records_received() {
        let m = RecordingMetrics::default();
        for outcome in [
            WebhookOutcome::Accepted,
            WebhookOutcome::IdempotentReplay,
            WebhookOutcome::VerifyFailed,
            WebhookOutcome::ParseFailed,
            WebhookOutcome::MissingIdempotency,
            WebhookOutcome::InternalError,
        ] {
            record_outcome(&m, "/hooks/x", outcome);
        }
        assert_eq!(m.received.lock().unwrap().len(), 6);
    }

    #[test]
    fn record_outcome_only_rejects_failure_outcomes() {
        let m = RecordingMetrics::default();
        record_outcome(&m, "/hooks/x", WebhookOutcome::Accepted);
        record_outcome(&m, "/hooks/x", WebhookOutcome::IdempotentReplay);
        assert!(m.rejected.lock().unwrap().is_empty());

        record_outcome(&m, "/hooks/x", WebhookOutcome::VerifyFailed);
        record_outcome(&m, "/hooks/x", WebhookOutcome::ParseFailed);
        record_outcome(&m, "/hooks/x", WebhookOutcome::MissingIdempotency);
        record_outcome(&m, "/hooks/x", WebhookOutcome::InternalError);
        assert_eq!(m.rejected.lock().unwrap().len(), 4);
    }
}
