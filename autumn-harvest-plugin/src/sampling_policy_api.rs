//! `GET`, `PUT` and `DELETE /admin/agents/{kind}/sampling-policy` (issue
//! #2439).
//!
//! The routes read, set and reset the sampling policy of one agent kind.
//! [`autumn_harvest_agent::sampling::PostgresSamplingPolicyStore`] on the
//! default shard holds the policy. The store writes each change and its
//! `harvest_agent_policy_audit` row with the old and new values in one
//! transaction.
//!
//! `PUT` and `DELETE` are `Mutating` routes. So a read-only principal gets
//! 403 before the handler runs, and nothing changes. Each `PUT` and `DELETE`
//! that reaches the handler also writes one `harvest_audit_log` row, a
//! refused request included.
//!
//! A strategy run reads the policy once at its start. So a change reaches
//! the next run of every process, with no restart.

use autumn_harvest::audit::{
    self, OP_AGENT_SAMPLING_POLICY_RESET, OP_AGENT_SAMPLING_POLICY_SET, STATUS_FAILED,
    STATUS_SUCCEEDED, TARGET_AGENT_KIND,
};
use autumn_harvest::models::NewAuditRecord;
use autumn_harvest_agent::sampling::{
    PolicyAudit, PolicyChange, PolicyStoreError, PostgresSamplingPolicyStore, SamplingPolicy,
    SamplingPolicyStore,
};
use autumn_web::AutumnError;
use autumn_web::reexports::axum;
use axum::extract::{Extension, Json, Path};
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};

use crate::api::{HarvestApiState, acquire_conn, audit_context, map_error};

const ROUTE_PUT: &str = "PUT /admin/agents/{kind}/sampling-policy";
const ROUTE_DELETE: &str = "DELETE /admin/agents/{kind}/sampling-policy";

/// The number of audit rows that `GET` returns, the newest first.
pub const SAMPLING_POLICY_AUDIT_ROWS: u32 = 20;

/// The body of `GET /admin/agents/{kind}/sampling-policy`.
#[derive(Debug, Clone, Serialize)]
pub struct SamplingPolicyView {
    /// The agent kind.
    pub kind: String,
    /// The revision of the stored row. 0 when no operator ever set or reset
    /// the policy.
    pub revision: u64,
    /// The policy, with each default filled in.
    pub policy: SamplingPolicy,
    /// The newest audit rows, the newest first.
    pub audit: Vec<PolicyAudit>,
}

/// The body of a `PUT` or a `DELETE`.
#[derive(Debug, Clone, Serialize)]
pub struct SamplingPolicyChangeView {
    /// The agent kind.
    pub kind: String,
    /// The revision after the change.
    pub revision: u64,
    /// The stored policy JSON before the change. `null` for every default.
    pub old: Option<serde_json::Value>,
    /// The stored policy JSON after the change. `null` after a reset.
    pub new: Option<serde_json::Value>,
}

impl SamplingPolicyChangeView {
    fn new(kind: &str, change: PolicyChange) -> Self {
        Self {
            kind: kind.to_owned(),
            revision: change.revision,
            old: change.old,
            new: change.new,
        }
    }
}

/// The body of `PUT /admin/agents/{kind}/sampling-policy`.
///
/// The policy replaces the stored policy as a whole. A key that the policy
/// leaves out takes its default. An unknown key fails the decode.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SamplingPolicyPutRequest {
    policy: SamplingPolicy,
}

/// The store on the default shard.
fn store(api_state: &HarvestApiState) -> Result<PostgresSamplingPolicyStore, AutumnError> {
    let pool = api_state.storage_pool().map_err(map_error)?;
    Ok(PostgresSamplingPolicyStore::new(
        pool.default_pool().clone(),
    ))
}

/// The HTTP error of a store error.
///
/// A bad kind, actor or policy is the fault of the caller. A backend error
/// is 503, as `map_error` gives for a database error. A corrupt row is 500.
fn store_error(error: &PolicyStoreError) -> AutumnError {
    match error {
        PolicyStoreError::InvalidKind { .. }
        | PolicyStoreError::InvalidActor { .. }
        | PolicyStoreError::Policy(_) => AutumnError::bad_request_msg(error.to_string()),
        PolicyStoreError::Full => {
            AutumnError::bad_request_msg(error.to_string()).with_status(StatusCode::CONFLICT)
        }
        PolicyStoreError::Backend { .. } => AutumnError::service_unavailable_msg(error.to_string()),
        _ => AutumnError::internal_server_error_msg(error.to_string()),
    }
}

/// `GET /admin/agents/{kind}/sampling-policy`.
pub async fn get_sampling_policy(
    Extension(api_state): Extension<HarvestApiState>,
    Path(kind): Path<String>,
) -> Result<Json<SamplingPolicyView>, AutumnError> {
    let store = store(&api_state)?;
    let read = store.read(&kind).await.map_err(|e| store_error(&e))?;
    let audit = store
        .audit(&kind, SAMPLING_POLICY_AUDIT_ROWS)
        .await
        .map_err(|e| store_error(&e))?;
    Ok(Json(SamplingPolicyView {
        kind,
        revision: read.revision,
        policy: read.policy,
        audit,
    }))
}

/// The audit fields of one request.
struct AuditCall<'a> {
    actor: &'a str,
    source: &'a str,
    request_id: Option<&'a str>,
    operation: &'static str,
    route: &'static str,
    kind: &'a str,
}

/// Insert one `harvest_audit_log` row.
///
/// The store holds no connection by then, so a pool of one is enough. A
/// failed request logs its audit error and keeps its own error. A succeeded
/// request returns the audit error, because an unaudited change must not
/// report success.
async fn audit_call(
    api_state: &HarvestApiState,
    call: &AuditCall<'_>,
    outcome: Result<&str, &str>,
) -> Result<(), AutumnError> {
    let (status, error_summary) = match outcome {
        Ok(summary) => (STATUS_SUCCEEDED, summary),
        Err(summary) => (STATUS_FAILED, summary),
    };
    let record = NewAuditRecord {
        actor: call.actor,
        operation: call.operation,
        target_type: TARGET_AGENT_KIND,
        target_id: Some(call.kind),
        route_or_command: call.route,
        request_id: call.request_id,
        idempotency_key: None,
        status,
        error_summary: Some(error_summary),
        shard_id: None,
        source: call.source,
    };
    let inserted = async {
        let pool = api_state.storage_pool().map_err(map_error)?;
        let mut conn = acquire_conn(pool.default_pool()).await?;
        audit::insert_audit(&mut conn, &record)
            .await
            .map_err(map_error)
    }
    .await;
    match inserted {
        Ok(_) => Ok(()),
        Err(error) if outcome.is_err() => {
            tracing::warn!(
                error = %error,
                kind = call.kind,
                "audit insert failed for a sampling policy change"
            );
            Ok(())
        }
        Err(error) => Err(error),
    }
}

/// Audit the outcome of a store write, and build the response.
async fn finish(
    api_state: &HarvestApiState,
    call: &AuditCall<'_>,
    result: Result<PolicyChange, PolicyStoreError>,
) -> Result<Json<SamplingPolicyChangeView>, AutumnError> {
    match result {
        Ok(change) => {
            let summary = format!("revision {}", change.revision);
            audit_call(api_state, call, Ok(&summary)).await?;
            Ok(Json(SamplingPolicyChangeView::new(call.kind, change)))
        }
        Err(error) => {
            audit_call(api_state, call, Err(&error.to_string())).await?;
            Err(store_error(&error))
        }
    }
}

/// `PUT /admin/agents/{kind}/sampling-policy`.
pub async fn put_sampling_policy(
    Extension(api_state): Extension<HarvestApiState>,
    Path(kind): Path<String>,
    headers: axum::http::HeaderMap,
    request: Result<Json<SamplingPolicyPutRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Json<SamplingPolicyChangeView>, AutumnError> {
    let store = store(&api_state)?;
    let (actor, source, request_id) = audit_context(&headers, &api_state);
    let call = AuditCall {
        actor: &actor,
        source: &source,
        request_id: request_id.as_deref(),
        operation: OP_AGENT_SAMPLING_POLICY_SET,
        route: ROUTE_PUT,
        kind: &kind,
    };
    // The handler takes the rejection itself. So a bad body is an audited
    // 400 of this route, not the plain-text 422 of axum.
    let request = match request {
        Ok(Json(request)) => request,
        Err(rejection) => {
            let summary = format!("invalid request body: {}", rejection.body_text());
            audit_call(&api_state, &call, Err(&summary)).await?;
            return Err(AutumnError::bad_request_msg(summary));
        }
    };
    let result = store.set(&kind, &request.policy, &actor).await;
    finish(&api_state, &call, result).await
}

/// `DELETE /admin/agents/{kind}/sampling-policy`.
pub async fn delete_sampling_policy(
    Extension(api_state): Extension<HarvestApiState>,
    Path(kind): Path<String>,
    headers: axum::http::HeaderMap,
) -> Result<Json<SamplingPolicyChangeView>, AutumnError> {
    let store = store(&api_state)?;
    let (actor, source, request_id) = audit_context(&headers, &api_state);
    let call = AuditCall {
        actor: &actor,
        source: &source,
        request_id: request_id.as_deref(),
        operation: OP_AGENT_SAMPLING_POLICY_RESET,
        route: ROUTE_DELETE,
        kind: &kind,
    };
    let result = store.reset(&kind, &actor).await;
    finish(&api_state, &call, result).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_put_body_refuses_an_unknown_key() {
        let error = serde_json::from_str::<SamplingPolicyPutRequest>(
            r#"{"policy": {"max_draws": 2}, "reason": "x"}"#,
        )
        .expect_err("an unknown top-level key fails");
        assert!(error.to_string().contains("reason"), "{error}");
        let error =
            serde_json::from_str::<SamplingPolicyPutRequest>(r#"{"policy": {"max_drawz": 2}}"#)
                .expect_err("an unknown policy key fails");
        assert!(error.to_string().contains("max_drawz"), "{error}");
    }

    #[test]
    fn a_put_body_fills_each_default() {
        let request: SamplingPolicyPutRequest =
            serde_json::from_str(r#"{"policy": {"max_draws": 4}}"#).expect("decodes");
        assert_eq!(request.policy.max_draws, 4);
        assert_eq!(
            request.policy.max_concurrent_draws,
            SamplingPolicy::default().max_concurrent_draws
        );
    }

    #[test]
    fn a_caller_fault_is_400_and_a_backend_fault_is_503() {
        let status = |error: PolicyStoreError| store_error(&error).status();
        assert_eq!(
            status(PolicyStoreError::InvalidKind {
                kind: "Bad Kind".to_owned()
            }),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            status(PolicyStoreError::InvalidActor { reason: "empty" }),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            status(PolicyStoreError::Backend {
                message: "down".to_owned()
            }),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            status(PolicyStoreError::Corrupt {
                reason: "bad json".to_owned()
            }),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }
}
