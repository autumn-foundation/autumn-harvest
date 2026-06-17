#![cfg(feature = "webhooks")]

use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;

use autumn_harvest::prelude::*;
use autumn_web::AppState;
use autumn_web::webhook_outbound::{
    WebhookDeliveryLog, WebhookOutboundManager, WebhookSubscriptionStatus,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookDeliveryInput {
    pub subscription_id: String,
    pub topic: String,
    pub payload: String,
}

#[workflow]
#[allow(clippy::missing_errors_doc)]
pub async fn webhook_delivery(
    ctx: &WorkflowContext,
    input: WebhookDeliveryInput,
) -> HarvestResult<()> {
    ctx.execute_activity_raw(
        "deliver_webhook",
        serde_json::to_value(input).map_err(|e| HarvestError::WorkflowFailed {
            name: "webhook_delivery".to_owned(),
            reason: format!("failed to serialize activity input: {e}"),
        })?,
        "webhooks",
    )
    .await?;

    Ok(())
}

#[activity(
    start_to_close = "30s",
    queue = "webhooks",
    retry = RetryPolicy::exponential(5, Duration::from_secs(1)),
    max_concurrent = 20,
    concurrency_key = "webhooks"
)]
#[allow(clippy::missing_errors_doc, clippy::too_many_lines)]
pub async fn deliver_webhook(
    ctx: &ActivityContext,
    input: WebhookDeliveryInput,
) -> Result<Value, ActivityFailure> {
    let app_state = ctx.state::<AppState>().ok_or_else(|| {
        ActivityFailure::non_retryable(
            "ConfigError",
            "AppState not found in Harvest activity shared state",
        )
    })?;

    let manager = app_state
        .extension::<WebhookOutboundManager>()
        .ok_or_else(|| {
            ActivityFailure::non_retryable(
                "ConfigError",
                "WebhookOutboundManager not registered in AppState extensions",
            )
        })?;

    // Load active subscriptions for topic from the handler
    let subs = manager
        .store()
        .get_subscriptions(&input.topic)
        .await
        .map_err(|e| {
            ActivityFailure::non_retryable(
                "StorageError",
                format!("failed to fetch subscriptions: {e}"),
            )
        })?;

    let Some(sub) = subs.into_iter().find(|s| s.id == input.subscription_id) else {
        // If subscription is not active or deleted, fail fast and don't retry,
        // but write a failed delivery log for auditability before returning.
        let attempt = ctx.attempt();
        let log = WebhookDeliveryLog {
            id: uuid::Uuid::new_v4().to_string(),
            subscription_id: input.subscription_id.clone(),
            topic: input.topic.clone(),
            payload: input.payload.clone(),
            request_headers: HashMap::new(),
            response_status: None,
            response_body: None,
            elapsed_ms: 0,
            attempt,
            max_attempts: 5,
            is_dlq: false,
            last_error: Some(format!(
                "SubscriptionNotFound: active subscription {} not found",
                input.subscription_id
            )),
            timestamp: Utc::now(),
        };
        manager.store().log_delivery(log).await.ok();

        return Err(ActivityFailure::non_retryable(
            "SubscriptionNotFound",
            format!("active subscription {} not found", input.subscription_id),
        ));
    };

    if sub.status == WebhookSubscriptionStatus::Disabled {
        tracing::info!(subscription_id = %sub.id, "Webhook subscription is disabled; skipping delivery");

        let attempt = ctx.attempt();
        let log = WebhookDeliveryLog {
            id: uuid::Uuid::new_v4().to_string(),
            subscription_id: sub.id.clone(),
            topic: input.topic.clone(),
            payload: input.payload.clone(),
            request_headers: HashMap::new(),
            response_status: None,
            response_body: None,
            elapsed_ms: 0,
            attempt,
            max_attempts: 5,
            is_dlq: false,
            last_error: Some("SubscriptionDisabled: Webhook subscription is disabled".to_owned()),
            timestamp: Utc::now(),
        };
        manager.store().log_delivery(log).await.ok();

        return Ok(serde_json::json!({ "skipped": true }));
    }

    // Stripe-style payload signing: t=<timestamp>,v1=<signature>
    let timestamp = Utc::now().timestamp();
    let signing_payload = format!("{}.{}", timestamp, input.payload);
    let signature =
        autumn_web::security::hmac_sha256_hex(sub.secret.as_bytes(), signing_payload.as_bytes());
    let signature_header = format!("t={timestamp},v1={signature}");

    let mut request_headers = HashMap::new();
    request_headers.insert("Content-Type".to_owned(), "application/json".to_owned());
    request_headers.insert("Autumn-Signature".to_owned(), signature_header.clone());

    let attempt = ctx.attempt();
    let max_attempts = 5;

    let mut log = WebhookDeliveryLog {
        id: uuid::Uuid::new_v4().to_string(),
        subscription_id: sub.id.clone(),
        topic: input.topic.clone(),
        payload: input.payload.clone(),
        request_headers: request_headers.clone(),
        response_status: None,
        response_body: None,
        elapsed_ms: 0,
        attempt,
        max_attempts,
        is_dlq: false,
        last_error: None,
        timestamp: Utc::now(),
    };

    let start = std::time::Instant::now();
    // Retrieve client from manager to reuse configuration and mock registry
    let req = manager
        .client()
        .named(&sub.target_url)
        .post(&sub.target_url)
        .header("Content-Type", "application/json")
        .header("Autumn-Signature", signature_header)
        .text_body(input.payload.clone());

    let response = req.send().await;
    let elapsed = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);

    log.elapsed_ms = elapsed;
    log.timestamp = Utc::now();

    match response {
        Ok(res) => {
            let status = res.status();
            log.response_status = Some(status.as_u16());
            let is_success = res.is_success();
            let body_str = res.text();
            log.response_body = Some(body_str);

            if is_success {
                log.last_error = None;
                log.is_dlq = false;
                manager.store().log_delivery(log).await.ok();
                Ok(serde_json::json!({ "delivered": true }))
            } else {
                let status_err = format!("server returned status: {status}");
                log.last_error = Some(status_err.clone());
                let is_exhausted = attempt >= max_attempts;
                log.is_dlq = is_exhausted;
                manager.store().log_delivery(log).await.ok();

                // Return activity failure to trigger Harvest retry
                if is_exhausted {
                    Err(ActivityFailure::non_retryable(
                        "DeliveryFailedPermanent",
                        status_err,
                    ))
                } else {
                    Err(ActivityFailure::retryable("DeliveryFailed", status_err))
                }
            }
        }
        Err(e) => {
            let error_str = e.to_string();
            log.last_error = Some(error_str.clone());
            let is_exhausted = attempt >= max_attempts;
            log.is_dlq = is_exhausted;
            manager.store().log_delivery(log).await.ok();

            if is_exhausted {
                Err(ActivityFailure::non_retryable(
                    "DeliveryFailedPermanent",
                    error_str,
                ))
            } else {
                Err(ActivityFailure::retryable("DeliveryFailed", error_str))
            }
        }
    }
}
