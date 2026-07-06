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
//! timeout resolution all apply identically to a webhook-triggered start.
//! [`reshape_dispatch_response`] then reshapes the delegate's own response
//! into the issue's `{"status", "workflow_exec_id", "workflow_id"}` envelope,
//! classifies the outcome for `harvest.webhook.received`/`.rejected`, and
//! writes this module's own `webhook.trigger` audit row (the delegate's
//! `workflow.start`/`workflow.signal_with_start` audit row is a separate,
//! additional record -- this one exists so an operator can find "which
//! webhook path triggered this" without cross-referencing route strings).
//!
//! Delegating to the plain management-API handlers (rather than calling
//! `execution.rs` primitives directly) avoids re-implementing admission-gate,
//! debounce, batch, and schema-validation logic a second time, but couples
//! [`reshape_dispatch_response`] to those handlers' HTTP response *shape*
//! (status code + JSON body) rather than their typed return values --
//! several of the outcome-classification bugs this module has had were
//! instances of that coupling (a `202` debounce admission or an ordinary
//! delegate-side `4xx` rejection being misread as an internal error). Status
//! codes are classified defensively (`is_client_error()` before falling
//! through to "anything else is internal") precisely because of that
//! coupling; a future response-shape change on the delegate side needs the
//! classification here re-checked by hand.

use autumn_harvest::audit::{
    self, OP_WEBHOOK_TRIGGER, STATUS_FAILED, STATUS_SUCCEEDED, TARGET_WORKFLOW,
};
use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::models::NewAuditRecord;
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
/// `registered_dags` (e.g. from `HarvestBuilder::dag_infos()`) is scanned so
/// a trigger targeting a registered DAG can be rejected with a specific,
/// actionable message instead of either silently passing (a unified DAG gets
/// an auto-registered shadow `WorkflowInfo`, so it would otherwise satisfy
/// the plain "is this a registered workflow" check below and only fail at
/// the first live request) or falling through to the generic "not
/// registered" message (a classic DAG, which has no shadow `WorkflowInfo` at
/// all). Only unified DAGs (`workflow_handler.is_some()`) are excluded here,
/// matching exactly which DAG names `HarvestApiState::is_registered_dag`
/// rejects at request time in `start_workflow`/`signal_with_start_workflow`
/// -- a classic DAG has no shadow `WorkflowInfo`, so it already fails the
/// plain registered-workflow check on its own.
///
/// # Panics
///
/// Panics when [`validate_webhook_triggers`] rejects the set (duplicate or
/// malformed binding paths), when a trigger targets a registered DAG (start
/// a DAG via `POST /dags/{name}/trigger` instead), or when a trigger targets
/// a workflow that is not registered on the builder -- a mount conflict or
/// dangling target must fail at `HarvestPlugin::build` time, not on the
/// first live request.
#[must_use]
pub fn build_webhook_routes(
    triggers: &[WebhookTriggerInfo],
    registered_workflows: &[WorkflowInfo],
    registered_dags: &[autumn_harvest::DagInfo],
    api_state: &HarvestApiState,
) -> Vec<autumn_web::Route> {
    if let Err(e) = validate_webhook_triggers(triggers) {
        panic!("HarvestPlugin::webhooks(...) failed validation: {e}");
    }
    let dag_names: std::collections::HashSet<&str> = registered_dags
        .iter()
        .filter(|d| d.workflow_handler.is_some())
        .map(|d| d.name)
        .collect();
    for trigger in triggers {
        let workflow = trigger.target.workflow();
        assert!(
            !dag_names.contains(workflow),
            "webhook trigger '{}' (path '{}') targets '{workflow}', which is a registered DAG \
             -- webhooks can only target plain #[workflow] handlers; DAGs are triggered via \
             POST /dags/{workflow}/trigger, not a #[webhook] binding",
            trigger.name,
            trigger.path
        );
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
        move |headers: axum::http::HeaderMap,
              hook: Result<SignedWebhook, autumn_web::AutumnError>| {
            let api_state = api_state.clone();
            // Boxed: the delegated start/signal-with-start handler's future is
            // large (clippy::large_futures) and this is a cold edge path.
            async move {
                Box::pin(handle_webhook(
                    api_state, path, target, handler_fn, queue, headers, hook,
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

/// Normalizes a resolved webhook delivery id: blank/whitespace-only is
/// treated as "no delivery id resolved", not a genuine (if empty) id.
///
/// autumn-web's own JSON-body `"id"` fallback (`resolve_delivery_id` /
/// `json_string_field`) has no non-empty guard the way its header-based
/// lookup does, so a naive/generic sender's `{"id": ""}` payload resolves
/// `Some("")` rather than `None`. Left unnormalized, every such delivery
/// would collide on the same namespaced idempotency key
/// (`{path}:{signal_name}:`), silently dropping all but the first unrelated
/// event for a `SignalsWithStart` target (Codex review, PR #918). A genuine,
/// non-blank id is returned untouched (not trimmed) -- only the
/// all-whitespace case is treated as missing.
fn normalize_delivery_id(raw: Option<&str>) -> Option<String> {
    raw.map(str::to_string).filter(|d| !d.trim().is_empty())
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn handle_webhook(
    api_state: HarvestApiState,
    path: &'static str,
    target: WebhookTarget,
    handler: autumn_harvest::webhook_trigger::WebhookHandlerFn,
    queue: Option<&'static str>,
    headers: axum::http::HeaderMap,
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

    // Deliberately NOT `map_error(e).into_response()` (which would map this
    // to a `400`): when `[security.webhooks].replay_protection` is enabled,
    // the `SignedWebhook` extractor above has already reserved this
    // delivery's replay key, and autumn-web's `webhook_replay_cleanup_middleware`
    // (wired automatically into every app's router) only releases a reserved
    // key when the response is a `5xx`. "The harvest runtime hasn't started
    // yet" is a transient boot-window condition, not a bad request -- a `400`
    // here would permanently consume the delivery id, so the provider's
    // automatic retry (arriving after the app finishes booting) would 409 as
    // a false duplicate forever instead of ever actually dispatching
    // (Codex review, PR #918).
    let runtime = match api_state.runtime() {
        Ok(r) => r,
        Err(e) => {
            return error_response(
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                "runtime_not_started",
                &e.to_string(),
            );
        }
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

    let delivery_id = normalize_delivery_id(hook.delivery_id());

    let ctx = WebhookCtx::new(
        path,
        hook.endpoint().to_string(),
        hook.provider().to_string(),
        delivery_id,
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

    let workflow_id = match handler(&ctx, &payload) {
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
                headers.clone(),
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
            // Namespace the idempotency key by this binding (path +
            // signal_name) rather than passing the raw provider delivery id
            // straight through: `signal_with_start`'s dedupe is scoped to
            // `(workflow_name, workflow_id, idempotency_key)` only -- it does
            // not know about webhook endpoints or signal names. Two
            // different `#[webhook(signals = ...)]` bindings that target the
            // same (workflow_name, workflow_id) pair (e.g. two providers
            // signalling one shared entity workflow) would otherwise collide
            // whenever their raw delivery ids happen to match (a real risk
            // for naive/generic senders using small sequential ids, or the
            // same event redelivered to two configured paths) -- the second
            // delivery would be misread as an idempotent replay of the
            // first and its signal silently dropped. `path` alone already
            // disambiguates every binding (`validate_webhook_triggers`
            // forbids duplicate paths); `signal_name` is included for
            // defense-in-depth. A same-binding redelivery of the same event
            // still maps to the same key, so exactly-once dedup for the
            // common case is unchanged.
            let namespaced_idempotency_key = ctx
                .delivery_id
                .as_deref()
                .map(|delivery_id| format!("{path}:{signal_name}:{delivery_id}"));
            Box::pin(crate::api::signal_with_start_workflow(
                Extension(api_state.clone()),
                axum::extract::Path(workflow.to_string()),
                None,
                headers.clone(),
                Json(SignalWithStartRequest::from_webhook(
                    workflow_id.as_str().to_string(),
                    payload,
                    signal_name.to_string(),
                    namespaced_idempotency_key,
                    queue.map(str::to_string),
                )),
            ))
            .await
        }
    };

    let route = format!("POST {path}");
    let audit_ctx = DispatchAuditCtx {
        api_state: &api_state,
        headers: &headers,
        route: &route,
        workflow_id: workflow_id.as_str(),
        delivery_id: ctx.delivery_id.as_deref(),
    };
    reshape_dispatch_response(metrics.as_ref(), path, dispatch_response, audit_ctx).await
}

/// Context needed to write the `webhook.trigger` audit row for one dispatch
/// attempt, threaded through from the verified request.
struct DispatchAuditCtx<'a> {
    api_state: &'a HarvestApiState,
    headers: &'a axum::http::HeaderMap,
    route: &'a str,
    /// The mapping function's deterministic `WorkflowId` -- used as a
    /// `target_id` fallback when the delegate never produced an execution
    /// (e.g. a debounce/batch admission, or a delegate-side rejection).
    workflow_id: &'a str,
    delivery_id: Option<&'a str>,
}

/// A minimal shape shared by [`crate::api`]'s `StartWorkflowResponse` and
/// `SignalWithStartResponse` -- both carry `execution_id`/`workflow_id`,
/// which is all this module needs to build its own envelope. Reading only
/// these two fields (rather than the private response types) avoids
/// widening either type's visibility beyond what's needed. `signal_delivered`
/// is present only on `SignalWithStartResponse`: a `SignalsWithStart` target
/// reports `200 OK` for both "attached to an existing execution and
/// delivered a genuinely new signal" and "deduped redelivery" -- the boolean
/// is the only way this module can tell them apart.
#[derive(serde::Deserialize)]
struct DispatchIds {
    execution_id: String,
    workflow_id: String,
    #[serde(default)]
    signal_delivered: Option<bool>,
}

/// Rewrite a successful `start_workflow`/`signal_with_start_workflow` response
/// into the issue's `{"status", "workflow_exec_id", "workflow_id"}` envelope;
/// pass a debounced/batched `202 Accepted` admission and any delegate error
/// response through unchanged (both already carry a structured JSON body).
/// Writes the `webhook.trigger` audit row for every dispatch attempt that
/// reaches this point -- i.e. passed signature verification, JSON parsing,
/// and the mapping function -- whether the delegate ultimately accepted or
/// rejected it.
async fn reshape_dispatch_response(
    metrics: &dyn MetricsRecorder,
    path: &str,
    response: axum::response::Response,
    audit_ctx: DispatchAuditCtx<'_>,
) -> axum::response::Response {
    let status = response.status();

    // A debounce/batch admission gate deferred the actual start -- there is
    // no execution yet to reshape the body against, but this is a genuine,
    // successful admission, not an internal error.
    if status == axum::http::StatusCode::ACCEPTED {
        record_outcome(metrics, path, WebhookOutcome::Accepted);
        write_dispatch_audit(&audit_ctx, STATUS_SUCCEEDED, None, None).await;
        return response;
    }

    // The delegate itself rejected the request (e.g. issue #373 schema
    // validation, a debounce/start_at conflict, a batching-max-size limit)
    // -- a caller/config problem, not an internal failure of this handler.
    // Pass the delegate's own structured error body through unchanged rather
    // than manufacturing a new one, and don't count it as an internal error.
    if status.is_client_error() {
        record_outcome(metrics, path, WebhookOutcome::ParseFailed);
        write_dispatch_audit(
            &audit_ctx,
            STATUS_FAILED,
            None,
            Some(&format!("dispatch rejected by delegate: {status}")),
        )
        .await;
        return response;
    }

    if status != axum::http::StatusCode::CREATED && status != axum::http::StatusCode::OK {
        record_outcome(metrics, path, WebhookOutcome::InternalError);
        write_dispatch_audit(
            &audit_ctx,
            STATUS_FAILED,
            None,
            Some(&format!("dispatch failed: {status}")),
        )
        .await;
        return response;
    }

    let bytes = match axum::body::to_bytes(response.into_body(), 1024 * 1024).await {
        Ok(b) => b,
        Err(e) => {
            record_outcome(metrics, path, WebhookOutcome::InternalError);
            write_dispatch_audit(
                &audit_ctx,
                STATUS_FAILED,
                None,
                Some(&format!("failed to read dispatch response: {e}")),
            )
            .await;
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
            write_dispatch_audit(
                &audit_ctx,
                STATUS_FAILED,
                None,
                Some(&format!("failed to parse dispatch response: {e}")),
            )
            .await;
            return error_response(
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                &format!("failed to parse dispatch response: {e}"),
            );
        }
    };

    // `201 Created` is unambiguous (`Accepted`). A `200 OK` is ambiguous:
    // `signal_delivered == Some(true)` means a live execution just received a
    // genuinely new signal (still a first-time `Accepted` from the sender's
    // point of view), vs. `Some(false)`/`None` (a `Starts` target, whose
    // `200 OK` is always a redelivery of an already-started deterministic
    // `WorkflowId`) meaning a deduped redelivery.
    let outcome =
        if status == axum::http::StatusCode::CREATED || parsed.signal_delivered == Some(true) {
            WebhookOutcome::Accepted
        } else {
            WebhookOutcome::IdempotentReplay
        };
    record_outcome(metrics, path, outcome);
    write_dispatch_audit(
        &audit_ctx,
        STATUS_SUCCEEDED,
        Some(parsed.execution_id.as_str()),
        None,
    )
    .await;
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

/// Write the `webhook.trigger` audit row for one dispatch attempt (issue
/// #344's audit AC). Best-effort: a failure to acquire a connection or
/// insert the row is logged and swallowed rather than turned into a second
/// error response layered on top of the dispatch outcome already decided --
/// the sender already has its real HTTP response, and a broken audit sink
/// must not un-deliver it.
///
/// When `exec_id` is known (any dispatch that produced or attached to an
/// execution), the row is written to that execution's own shard via
/// [`crate::api::db_conn_for_execution`], matching every other execution-
/// scoped audit write in this codebase. When there is no execution yet (a
/// debounced/batched admission, or a delegate-side rejection before an
/// execution was created), the row is written to the default shard,
/// mirroring the debounce/batch admission-audit writes in `api.rs`.
async fn write_dispatch_audit(
    audit_ctx: &DispatchAuditCtx<'_>,
    status: &'static str,
    exec_id: Option<&str>,
    error_summary: Option<&str>,
) {
    let (actor, source, request_id) =
        crate::api::audit_context(audit_ctx.headers, audit_ctx.api_state);

    let acquired = if let Some(raw_exec_id) = exec_id {
        match crate::api::parse_execution_id(raw_exec_id) {
            Ok(id) => match crate::api::db_conn_for_execution(audit_ctx.api_state, id).await {
                Ok(conn) => Some((conn, Some(id.shard().as_i32()))),
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        "audit conn acquisition failed for webhook.trigger"
                    );
                    None
                }
            },
            Err(e) => {
                tracing::error!(error = %e, "invalid execution id for webhook.trigger audit");
                None
            }
        }
    } else {
        match audit_ctx.api_state.storage_pool() {
            Ok(pool) => match crate::api::acquire_conn(pool.default_pool()).await {
                Ok(conn) => Some((conn, None)),
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        "audit conn acquisition failed for webhook.trigger"
                    );
                    None
                }
            },
            Err(e) => {
                tracing::error!(error = %e, "storage pool unavailable for webhook.trigger audit");
                None
            }
        }
    };

    let Some((mut conn, shard_id)) = acquired else {
        return;
    };

    let ar = NewAuditRecord {
        actor: &actor,
        operation: OP_WEBHOOK_TRIGGER,
        target_type: TARGET_WORKFLOW,
        target_id: exec_id.or(Some(audit_ctx.workflow_id)),
        route_or_command: audit_ctx.route,
        request_id: request_id.as_deref(),
        idempotency_key: audit_ctx.delivery_id,
        status,
        error_summary,
        shard_id,
        source: &source,
    };
    if let Err(e) = audit::insert_audit(&mut conn, &ar).await {
        tracing::error!(error = %e, "audit insert failed for webhook.trigger");
    }
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
    fn normalize_delivery_id_treats_none_as_none() {
        assert_eq!(normalize_delivery_id(None), None);
    }

    #[test]
    fn normalize_delivery_id_treats_blank_and_whitespace_as_none() {
        assert_eq!(normalize_delivery_id(Some("")), None);
        assert_eq!(normalize_delivery_id(Some("   ")), None);
        assert_eq!(normalize_delivery_id(Some("\t\n")), None);
    }

    #[test]
    fn normalize_delivery_id_preserves_a_genuine_id_untrimmed() {
        assert_eq!(
            normalize_delivery_id(Some("evt_123")),
            Some("evt_123".to_string())
        );
        // A real (non-blank) id with incidental surrounding whitespace is
        // passed through as-is -- only the all-whitespace case is missing.
        assert_eq!(
            normalize_delivery_id(Some(" evt_123 ")),
            Some(" evt_123 ".to_string())
        );
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

    fn no_db_audit_ctx<'a>(
        api_state: &'a HarvestApiState,
        headers: &'a axum::http::HeaderMap,
    ) -> DispatchAuditCtx<'a> {
        DispatchAuditCtx {
            api_state,
            headers,
            route: "POST /hooks/test",
            workflow_id: "wf-1",
            delivery_id: None,
        }
    }

    /// `write_dispatch_audit` is exercised transitively by every case below --
    /// with no storage pool installed (a fresh no-DB [`HarvestApiState`]),
    /// `storage_pool()`/`db_conn_for_execution` return `Err`, so the audit
    /// write is a logged no-op rather than a panic. This lets the pure
    /// outcome-classification logic in [`reshape_dispatch_response`] be
    /// tested here without Docker/testcontainers; the actual audit *row*
    /// content is covered by `webhook_receiver_integration.rs`.
    #[tokio::test]
    async fn reshape_passes_a_debounced_202_through_unchanged_as_accepted() {
        let m = RecordingMetrics::default();
        let api_state = HarvestApiState::new();
        let headers = axum::http::HeaderMap::new();
        let debounce_body = serde_json::json!({
            "debounced": true,
            "workflow_name": "wf",
            "workflow_id": "wf-1",
            "debounce_key": "k",
            "fire_at": "2026-01-01T00:00:00Z",
            "pending_count": 3,
        });
        let response = (
            axum::http::StatusCode::ACCEPTED,
            Json(debounce_body.clone()),
        )
            .into_response();

        let out = reshape_dispatch_response(
            &m,
            "/hooks/x",
            response,
            no_db_audit_ctx(&api_state, &headers),
        )
        .await;

        assert_eq!(out.status(), axum::http::StatusCode::ACCEPTED);
        assert_eq!(
            m.received.lock().unwrap().as_slice(),
            [("/hooks/x".to_string(), WebhookOutcome::Accepted)]
        );
        assert!(m.rejected.lock().unwrap().is_empty());
        let bytes = axum::body::to_bytes(out.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            parsed, debounce_body,
            "debounce body must pass through byte-for-byte"
        );
    }

    #[tokio::test]
    async fn reshape_classifies_delegate_client_error_as_parse_failed_not_internal() {
        let m = RecordingMetrics::default();
        let api_state = HarvestApiState::new();
        let headers = axum::http::HeaderMap::new();
        let response = (
            axum::http::StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "input validation failed"})),
        )
            .into_response();

        let out = reshape_dispatch_response(
            &m,
            "/hooks/x",
            response,
            no_db_audit_ctx(&api_state, &headers),
        )
        .await;

        assert_eq!(out.status(), axum::http::StatusCode::BAD_REQUEST);
        assert_eq!(
            m.received.lock().unwrap().as_slice(),
            [("/hooks/x".to_string(), WebhookOutcome::ParseFailed)]
        );
        assert_eq!(
            m.rejected.lock().unwrap().as_slice(),
            [("/hooks/x".to_string(), WebhookOutcome::ParseFailed)],
            "an ordinary delegate-side 4xx must not be counted as internal_error"
        );
    }

    #[tokio::test]
    async fn reshape_classifies_ok_with_signal_delivered_true_as_accepted() {
        let m = RecordingMetrics::default();
        let api_state = HarvestApiState::new();
        let headers = axum::http::HeaderMap::new();
        let response = (
            axum::http::StatusCode::OK,
            Json(serde_json::json!({
                "execution_id": "e1",
                "workflow_name": "wf",
                "workflow_id": "wf-1",
                "state": "RUNNING",
                "started_fresh": false,
                "signal_delivered": true,
            })),
        )
            .into_response();

        let out = reshape_dispatch_response(
            &m,
            "/hooks/x",
            response,
            no_db_audit_ctx(&api_state, &headers),
        )
        .await;

        assert_eq!(
            out.status(),
            axum::http::StatusCode::ACCEPTED,
            "a genuinely new signal delivered to an attached execution is `accepted`, not a replay"
        );
        assert_eq!(
            m.received.lock().unwrap().as_slice(),
            [("/hooks/x".to_string(), WebhookOutcome::Accepted)]
        );
        assert!(m.rejected.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn reshape_classifies_ok_with_signal_delivered_false_as_idempotent_replay() {
        let m = RecordingMetrics::default();
        let api_state = HarvestApiState::new();
        let headers = axum::http::HeaderMap::new();
        let response = (
            axum::http::StatusCode::OK,
            Json(serde_json::json!({
                "execution_id": "e1",
                "workflow_name": "wf",
                "workflow_id": "wf-1",
                "state": "RUNNING",
                "started_fresh": false,
                "signal_delivered": false,
            })),
        )
            .into_response();

        let out = reshape_dispatch_response(
            &m,
            "/hooks/x",
            response,
            no_db_audit_ctx(&api_state, &headers),
        )
        .await;

        assert_eq!(out.status(), axum::http::StatusCode::OK);
        assert_eq!(
            m.received.lock().unwrap().as_slice(),
            [("/hooks/x".to_string(), WebhookOutcome::IdempotentReplay)]
        );
        assert!(m.rejected.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn reshape_classifies_ok_with_no_signal_delivered_field_as_idempotent_replay() {
        // The `Starts` target's response shape has no `signal_delivered`
        // field at all -- its `200 OK` is always a redelivery.
        let m = RecordingMetrics::default();
        let api_state = HarvestApiState::new();
        let headers = axum::http::HeaderMap::new();
        let response = (
            axum::http::StatusCode::OK,
            Json(serde_json::json!({
                "execution_id": "e1",
                "workflow_name": "wf",
                "workflow_id": "wf-1",
                "state": "RUNNING",
            })),
        )
            .into_response();

        let out = reshape_dispatch_response(
            &m,
            "/hooks/x",
            response,
            no_db_audit_ctx(&api_state, &headers),
        )
        .await;

        assert_eq!(out.status(), axum::http::StatusCode::OK);
        assert_eq!(
            m.received.lock().unwrap().as_slice(),
            [("/hooks/x".to_string(), WebhookOutcome::IdempotentReplay)]
        );
    }

    #[tokio::test]
    async fn reshape_classifies_created_as_accepted() {
        let m = RecordingMetrics::default();
        let api_state = HarvestApiState::new();
        let headers = axum::http::HeaderMap::new();
        let response = (
            axum::http::StatusCode::CREATED,
            Json(serde_json::json!({
                "execution_id": "e1",
                "workflow_name": "wf",
                "workflow_id": "wf-1",
                "state": "RUNNING",
            })),
        )
            .into_response();

        let out = reshape_dispatch_response(
            &m,
            "/hooks/x",
            response,
            no_db_audit_ctx(&api_state, &headers),
        )
        .await;

        assert_eq!(out.status(), axum::http::StatusCode::ACCEPTED);
        assert_eq!(
            m.received.lock().unwrap().as_slice(),
            [("/hooks/x".to_string(), WebhookOutcome::Accepted)]
        );
    }

    #[tokio::test]
    async fn reshape_classifies_a_genuine_5xx_as_internal_error() {
        let m = RecordingMetrics::default();
        let api_state = HarvestApiState::new();
        let headers = axum::http::HeaderMap::new();
        let response = (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "storage pool exhausted"})),
        )
            .into_response();

        let out = reshape_dispatch_response(
            &m,
            "/hooks/x",
            response,
            no_db_audit_ctx(&api_state, &headers),
        )
        .await;

        assert_eq!(out.status(), axum::http::StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            m.received.lock().unwrap().as_slice(),
            [("/hooks/x".to_string(), WebhookOutcome::InternalError)]
        );
        assert_eq!(
            m.rejected.lock().unwrap().as_slice(),
            [("/hooks/x".to_string(), WebhookOutcome::InternalError)]
        );
    }
}
