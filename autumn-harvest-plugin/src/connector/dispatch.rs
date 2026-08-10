//! Dispatch a mapped broker message into harvest (issue #944).
//!
//! Like the inbound webhook receiver (issue #344), the connector delegates to
//! the plugin's own `start_workflow` / `signal_with_start_workflow` handlers
//! rather than reaching for the `execution.rs` primitives directly. That is
//! what buys the whole admission stack for free: published-schema validation
//! (#373/#610), the admission gate (#618), shard placement (#697), start
//! throttle (#607), debounce (#499), batch (#518), start idempotency (#808),
//! and the audit row.
//!
//! The cost of that choice — inherited verbatim from the webhook receiver — is
//! coupling to the delegate's *HTTP response shape*, so status classification
//! is defensive: a `202` deferred admission is a success, an ordinary `4xx` is
//! a deterministic refusal, and only genuinely unexpected statuses become
//! transient errors.

use autumn_harvest::WorkflowId;
use autumn_web::reexports::axum;
use axum::{Extension, Json};

use super::binding::{ConnectorTarget, IdempotencyMode};
use super::disposition::DispatchOutcome;
use crate::api::{HarvestApiState, SignalWithStartRequest, StartWorkflowRequest};

/// Everything the dispatcher needs for one message.
pub struct DispatchRequest<'a> {
    pub target: ConnectorTarget,
    pub workflow_id: WorkflowId,
    pub payload: serde_json::Value,
    pub queue: Option<&'static str>,
    pub idempotency_key: &'a str,
    pub idempotency_mode: IdempotencyMode,
    /// Rendered broker coordinates (`orders:3:91`), recorded as the start's
    /// provenance ref (issue #740) so an operator can trace an execution back
    /// to the exact message that produced it.
    pub coordinates: &'a str,
}

/// Minimal structural view of the delegate's success body.
///
/// Deliberately narrow (mirroring `webhook_receiver::DispatchIds`) so the
/// private response types' visibility never has to widen.
#[derive(serde::Deserialize)]
struct DispatchIds {
    execution_id: Option<String>,
    workflow_id: Option<String>,
    #[serde(default)]
    signal_delivered: Option<bool>,
    #[serde(default)]
    deduplicated: Option<bool>,
    #[serde(default)]
    started_fresh: Option<bool>,
}

/// Dispatch one mapped message, returning a classified outcome.
pub async fn dispatch(
    api_state: &HarvestApiState,
    request: DispatchRequest<'_>,
) -> DispatchOutcome {
    let workflow = request.target.workflow();
    let workflow_id = request.workflow_id.as_str().to_string();

    let response = match request.target {
        ConnectorTarget::Starts { .. } => {
            // Strip any inbound `Idempotency-Key`: the connector owns start
            // dedupe (issue #808 reads that header and it takes precedence
            // over the body field). Same reasoning as the webhook receiver —
            // an upstream producer's key must never hijack ours.
            let headers = axum::http::HeaderMap::new();
            let body = StartWorkflowRequest::from_broker(
                workflow_id.clone(),
                request.payload,
                request.queue.map(str::to_string),
                match request.idempotency_mode {
                    IdempotencyMode::BrokerCoordinates => Some(request.idempotency_key.to_string()),
                    // Dedupe rides the mapping function's deterministic
                    // workflow_id instead, so the start composes with a
                    // throttle/debounce/batch admission policy.
                    IdempotencyMode::WorkflowId => None,
                },
                Some(request.coordinates.to_string()),
            );
            Box::pin(crate::api::start_workflow(
                Extension(api_state.clone()),
                axum::extract::Path(workflow.to_string()),
                None,
                headers,
                Ok(Json(body)),
            ))
            .await
        }
        ConnectorTarget::SignalsWithStart { signal_name, .. } => {
            let body = SignalWithStartRequest::from_broker(
                workflow_id.clone(),
                request.payload,
                signal_name.to_string(),
                Some(request.idempotency_key.to_string()),
                request.queue.map(str::to_string),
            );
            Box::pin(crate::api::signal_with_start_workflow(
                Extension(api_state.clone()),
                axum::extract::Path(workflow.to_string()),
                None,
                axum::http::HeaderMap::new(),
                Json(body),
            ))
            .await
        }
    };

    classify_response(response, workflow_id).await
}

/// Classify the delegate's HTTP response into a [`DispatchOutcome`].
async fn classify_response(
    response: axum::response::Response,
    workflow_id: String,
) -> DispatchOutcome {
    let status = response.status();

    // A deferred admission (throttle/debounce/batch) is a SUCCESS: the start
    // is durably parked and its owning scanner will fire it. Busy-retrying it
    // would defeat the throttle (issue #607).
    if status == axum::http::StatusCode::ACCEPTED {
        return DispatchOutcome::Deferred { workflow_id };
    }

    let body = match axum::body::to_bytes(response.into_body(), 1024 * 1024).await {
        Ok(b) => b,
        Err(e) => {
            return DispatchOutcome::Transient(format!("could not read dispatch response: {e}"));
        }
    };

    if status.is_client_error() {
        // A deterministic refusal: schema validation, a mutually-exclusive
        // option, an already-exists conflict. Retrying cannot help.
        return DispatchOutcome::TargetRejected {
            status: status.as_u16(),
            detail: truncate(&String::from_utf8_lossy(&body), 1000),
        };
    }

    if !status.is_success() {
        return DispatchOutcome::Transient(format!(
            "dispatch failed with status {status}: {}",
            truncate(&String::from_utf8_lossy(&body), 500)
        ));
    }

    let Ok(ids) = serde_json::from_slice::<DispatchIds>(&body) else {
        return DispatchOutcome::Transient(
            "dispatch succeeded but the response body was unreadable".to_string(),
        );
    };

    classify_success(status.as_u16(), &ids, workflow_id)
}

/// Pure success classification, split out so it is unit-testable.
fn classify_success(status: u16, ids: &DispatchIds, workflow_id: String) -> DispatchOutcome {
    let workflow_id = ids.workflow_id.clone().unwrap_or(workflow_id);
    let execution_id = ids.execution_id.clone();

    // `201 Created` is always a fresh dispatch.
    if status == 201 {
        return DispatchOutcome::Dispatched {
            execution_id,
            workflow_id,
        };
    }

    // On `200 OK` the discriminators differ by path:
    //  * signal-with-start returns `signal_delivered`: true means a genuinely
    //    new signal landed on an existing execution.
    //  * a keyed start (#808) returns `deduplicated` / `started_fresh`.
    //  * a plain start attaching to an existing run returns neither, which is
    //    exactly the redelivery case.
    if ids.signal_delivered == Some(true) {
        return DispatchOutcome::Dispatched {
            execution_id,
            workflow_id,
        };
    }
    if ids.deduplicated == Some(false) && ids.started_fresh == Some(true) {
        return DispatchOutcome::Dispatched {
            execution_id,
            workflow_id,
        };
    }

    DispatchOutcome::IdempotentReplay {
        execution_id,
        workflow_id,
    }
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...", &s[..end])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids(
        execution_id: Option<&str>,
        signal_delivered: Option<bool>,
        deduplicated: Option<bool>,
        started_fresh: Option<bool>,
    ) -> DispatchIds {
        DispatchIds {
            execution_id: execution_id.map(str::to_string),
            workflow_id: Some("w-1".to_string()),
            signal_delivered,
            deduplicated,
            started_fresh,
        }
    }

    #[test]
    fn created_is_a_fresh_dispatch() {
        let out = classify_success(201, &ids(Some("e1"), None, None, None), "w-1".to_string());
        assert!(matches!(out, DispatchOutcome::Dispatched { .. }));
    }

    #[test]
    fn signal_delivered_true_is_a_fresh_dispatch() {
        let out = classify_success(
            200,
            &ids(Some("e1"), Some(true), None, None),
            "w".to_string(),
        );
        assert!(matches!(out, DispatchOutcome::Dispatched { .. }));
    }

    #[test]
    fn signal_delivered_false_is_an_idempotent_replay() {
        // The redelivery case: signal-with-start deduped on our derived key.
        let out = classify_success(
            200,
            &ids(Some("e1"), Some(false), None, None),
            "w".to_string(),
        );
        assert!(matches!(out, DispatchOutcome::IdempotentReplay { .. }));
    }

    #[test]
    fn keyed_start_deduplicated_is_an_idempotent_replay() {
        let out = classify_success(
            200,
            &ids(Some("e1"), None, Some(true), Some(false)),
            "w".to_string(),
        );
        assert!(matches!(out, DispatchOutcome::IdempotentReplay { .. }));
    }

    #[test]
    fn keyed_start_fresh_on_200_is_a_dispatch() {
        // #808 returns 200 + started_fresh=true when a keyed start attaches
        // to a genuinely new run created in the same call.
        let out = classify_success(
            200,
            &ids(Some("e1"), None, Some(false), Some(true)),
            "w".to_string(),
        );
        assert!(matches!(out, DispatchOutcome::Dispatched { .. }));
    }

    #[test]
    fn plain_start_attach_on_200_is_an_idempotent_replay() {
        // No discriminators at all: a plain start attaching to an existing
        // run, i.e. the WorkflowId-mode redelivery case.
        let out = classify_success(200, &ids(Some("e1"), None, None, None), "w".to_string());
        assert!(matches!(out, DispatchOutcome::IdempotentReplay { .. }));
    }

    #[test]
    fn missing_workflow_id_falls_back_to_the_mapped_one() {
        let mut i = ids(Some("e1"), None, None, None);
        i.workflow_id = None;
        let out = classify_success(200, &i, "mapped-1".to_string());
        match out {
            DispatchOutcome::IdempotentReplay { workflow_id, .. } => {
                assert_eq!(workflow_id, "mapped-1");
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[tokio::test]
    async fn accepted_is_deferred_not_an_error() {
        let resp = (
            axum::http::StatusCode::ACCEPTED,
            Json(serde_json::json!({"throttled": true})),
        );
        let out = classify_response(
            axum::response::IntoResponse::into_response(resp),
            "w-1".to_string(),
        )
        .await;
        assert_eq!(
            out,
            DispatchOutcome::Deferred {
                workflow_id: "w-1".to_string()
            }
        );
    }

    #[tokio::test]
    async fn client_error_is_a_deterministic_target_rejection() {
        let resp = (
            axum::http::StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "input validation failed"})),
        );
        let out = classify_response(
            axum::response::IntoResponse::into_response(resp),
            "w-1".to_string(),
        )
        .await;
        match out {
            DispatchOutcome::TargetRejected { status, detail } => {
                assert_eq!(status, 400);
                assert!(detail.contains("input validation failed"));
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[tokio::test]
    async fn server_error_is_transient_so_the_broker_redelivers() {
        let resp = (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "admission blocked"})),
        );
        let out = classify_response(
            axum::response::IntoResponse::into_response(resp),
            "w-1".to_string(),
        )
        .await;
        assert!(matches!(out, DispatchOutcome::Transient(_)), "{out:?}");
    }

    #[tokio::test]
    async fn unreadable_success_body_is_transient_not_a_silent_ack() {
        let resp = (
            axum::http::StatusCode::OK,
            axum::body::Body::from("not json"),
        );
        let out = classify_response(
            axum::response::IntoResponse::into_response(resp),
            "w-1".to_string(),
        )
        .await;
        assert!(matches!(out, DispatchOutcome::Transient(_)), "{out:?}");
    }

    #[test]
    fn truncate_respects_char_boundaries() {
        let s = "aaaé";
        // Cutting at 4 would split the 2-byte 'é'.
        let t = truncate(s, 4);
        assert!(t.starts_with("aaa"));
        assert!(t.ends_with("..."));
        assert_eq!(truncate("short", 100), "short");
    }
}
