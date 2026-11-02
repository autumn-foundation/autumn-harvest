//! The agent profile overlay routes (issue #2374).
//!
//! The routes read and move the overlay versions of one agent kind, through
//! the [`ProfileOverlayStore`] that the embedder installs. They are not
//! under `/admin`, so the `mutate` scope of the `harvest-operator` role
//! reaches the two writes. A `read` scope reaches only the reads.
//!
//! Each activate and rollback request writes one audit row. A succeeded row
//! keeps `from v1 to v2` in `error_summary`. The design is in
//! `docs/design/DESIGN-2374.md`.

use std::sync::Arc;

use autumn_harvest::audit::{
    self, OP_AGENT_PROFILE_ACTIVATE, OP_AGENT_PROFILE_ROLLBACK, STATUS_FAILED, STATUS_SUCCEEDED,
    TARGET_AGENT_PROFILE,
};
use autumn_harvest::chrono::{DateTime, Utc};
use autumn_harvest::models::NewAuditRecord;
use autumn_harvest_agent::{
    OverlayChange, OverlayStoreError, OverlayVersion, ProfileOverlay, ProfileOverlayStore,
    Provenance, StoredOverlay, overlay_diff,
};
use autumn_web::AutumnError;
use autumn_web::reexports::axum;
use axum::extract::rejection::JsonRejection;
use axum::extract::{Extension, Json, Path, Query};
use axum::http::{HeaderMap, StatusCode};
use diesel_async::AsyncPgConnection;
use serde::{Deserialize, Serialize};

use crate::api::{HarvestApiState, acquire_conn, audit_context, deserialize_tristate, map_error};

const ACTIVATE_ROUTE: &str = "POST /agents/{kind}/profile/activate";
const ROLLBACK_ROUTE: &str = "POST /agents/{kind}/profile/rollback";

/// The message of every route when the embedder installed no store.
pub const NO_OVERLAY_STORE: &str =
    "no agent overlay store is installed: call HarvestPlugin::agent_overlay_store";

/// One version in a list, with no overlay body.
#[derive(Debug, Clone, Serialize)]
pub struct OverlayVersionSummary {
    /// The version number.
    pub version: OverlayVersion,
    /// Whether the version is the active one.
    pub active: bool,
    /// Who or what wrote the version.
    pub provenance: Provenance,
    /// The time of the put.
    pub created_at: DateTime<Utc>,
}

/// The body of `GET /agents/{kind}/profile/versions`.
#[derive(Debug, Clone, Serialize)]
pub struct OverlayVersionList {
    /// The agent kind.
    pub kind: String,
    /// The active version, or `null`.
    pub active: Option<OverlayVersion>,
    /// Every version, oldest first.
    pub versions: Vec<OverlayVersionSummary>,
}

/// One version with its overlay.
#[derive(Debug, Clone, Serialize)]
pub struct OverlayVersionView {
    /// The agent kind.
    pub kind: String,
    /// The version number.
    pub version: OverlayVersion,
    /// Whether the version is the active one.
    pub active: bool,
    /// Who or what wrote the version.
    pub provenance: Provenance,
    /// The time of the put.
    pub created_at: DateTime<Utc>,
    /// The `sha256:<hex>` digest of the overlay.
    pub digest: String,
    /// The overlay.
    pub overlay: ProfileOverlay,
}

/// The body of `GET /agents/{kind}/profile/diff`.
#[derive(Debug, Clone, Serialize)]
pub struct OverlayDiffView {
    /// The agent kind.
    pub kind: String,
    /// The first version.
    pub a: OverlayVersion,
    /// The second version.
    pub b: OverlayVersion,
    /// Each part that differs, in key order. Empty for equal overlays.
    pub changes: Vec<OverlayChange>,
}

/// The body of a succeeded activate or rollback.
#[derive(Debug, Clone, Serialize)]
pub struct OverlayMove {
    /// The agent kind.
    pub kind: String,
    /// The active version before the call, or `null`.
    pub from: Option<OverlayVersion>,
    /// The active version after the call.
    pub to: OverlayVersion,
    /// The operator id that the store recorded.
    pub actor: String,
}

/// The query of `GET /agents/{kind}/profile/diff`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiffQuery {
    a: String,
    b: String,
}

/// The body of `POST /agents/{kind}/profile/activate`.
///
/// `expected_active` is required, so a client always states the pointer
/// that it read. `null` means an unset pointer.
#[expect(
    clippy::option_option,
    reason = "serde tri-state: absent is an error, null is an unset pointer"
)]
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActivateRequest {
    version: u64,
    #[serde(default, deserialize_with = "deserialize_tristate")]
    expected_active: Option<Option<u64>>,
}

/// The body of `POST /agents/{kind}/profile/rollback`. It has no field.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RollbackRequest {}

/// The installed store, or the `404` of [`NO_OVERLAY_STORE`].
fn store(api_state: &HarvestApiState) -> Result<Arc<dyn ProfileOverlayStore>, AutumnError> {
    api_state
        .agent_overlay_store()
        .ok_or_else(|| AutumnError::not_found_msg(NO_OVERLAY_STORE))
}

/// A version from a path or query value: `2` or `v2`.
fn parse_version(text: &str) -> Result<OverlayVersion, AutumnError> {
    let digits = text.strip_prefix('v').unwrap_or(text);
    digits
        .parse()
        .ok()
        .and_then(OverlayVersion::new)
        .ok_or_else(|| {
            AutumnError::bad_request_msg(format!(
                "invalid overlay version {text:?}: use a number from 1, such as 2 or v2"
            ))
        })
}

/// The HTTP error of a store error.
fn store_error(error: &OverlayStoreError) -> AutumnError {
    let message = error.to_string();
    match error {
        OverlayStoreError::UnknownVersion { .. } | OverlayStoreError::UnknownKind { .. } => {
            AutumnError::not_found_msg(message)
        }
        OverlayStoreError::Conflict { .. } | OverlayStoreError::NoEarlierActivation => {
            AutumnError::bad_request_msg(message).with_status(StatusCode::CONFLICT)
        }
        OverlayStoreError::Overlay(_) => {
            AutumnError::bad_request_msg(message).with_status(StatusCode::UNPROCESSABLE_ENTITY)
        }
        OverlayStoreError::InvalidProvenance { .. } => AutumnError::bad_request_msg(message),
        _ => AutumnError::internal_server_error_msg(message),
    }
}

/// One stored version of `kind`, or `404`.
async fn stored(
    store: &dyn ProfileOverlayStore,
    kind: &str,
    version: OverlayVersion,
) -> Result<StoredOverlay, AutumnError> {
    store
        .get(kind, version)
        .await
        .map_err(|error| store_error(&error))?
        .ok_or_else(|| AutumnError::not_found_msg(format!("the kind has no overlay {version}")))
}

/// The active version of `kind`, or `None`.
async fn active_version(
    store: &dyn ProfileOverlayStore,
    kind: &str,
) -> Result<Option<OverlayVersion>, AutumnError> {
    Ok(store
        .active(kind)
        .await
        .map_err(|error| store_error(&error))?
        .map(|active| active.version()))
}

fn view(kind: &str, stored: &StoredOverlay, active: Option<OverlayVersion>) -> OverlayVersionView {
    OverlayVersionView {
        kind: kind.to_owned(),
        version: stored.version(),
        active: active == Some(stored.version()),
        provenance: stored.provenance().clone(),
        created_at: stored.created_at(),
        digest: stored.overlay().digest(),
        overlay: stored.overlay().clone(),
    }
}

/// `GET /agents/{kind}/profile/versions`.
pub async fn list_versions(
    Extension(api_state): Extension<HarvestApiState>,
    Path(kind): Path<String>,
) -> Result<Json<OverlayVersionList>, AutumnError> {
    let store = store(&api_state)?;
    let active = active_version(store.as_ref(), &kind).await?;
    let versions = store
        .list(&kind)
        .await
        .map_err(|error| store_error(&error))?
        .iter()
        .map(|stored| OverlayVersionSummary {
            version: stored.version(),
            active: active == Some(stored.version()),
            provenance: stored.provenance().clone(),
            created_at: stored.created_at(),
        })
        .collect();
    Ok(Json(OverlayVersionList {
        kind,
        active,
        versions,
    }))
}

/// `GET /agents/{kind}/profile`: the active version.
pub async fn get_active(
    Extension(api_state): Extension<HarvestApiState>,
    Path(kind): Path<String>,
) -> Result<Json<OverlayVersionView>, AutumnError> {
    let store = store(&api_state)?;
    let active = store
        .active(&kind)
        .await
        .map_err(|error| store_error(&error))?
        .ok_or_else(|| AutumnError::not_found_msg("the kind has no active overlay"))?;
    Ok(Json(view(&kind, &active, Some(active.version()))))
}

/// `GET /agents/{kind}/profile/versions/{version}`.
pub async fn get_version(
    Extension(api_state): Extension<HarvestApiState>,
    Path((kind, version)): Path<(String, String)>,
) -> Result<Json<OverlayVersionView>, AutumnError> {
    let version = parse_version(&version)?;
    let store = store(&api_state)?;
    let found = stored(store.as_ref(), &kind, version).await?;
    let active = active_version(store.as_ref(), &kind).await?;
    Ok(Json(view(&kind, &found, active)))
}

/// `GET /agents/{kind}/profile/diff?a=&b=`.
pub async fn diff_versions(
    Extension(api_state): Extension<HarvestApiState>,
    Path(kind): Path<String>,
    query: Result<Query<DiffQuery>, axum::extract::rejection::QueryRejection>,
) -> Result<Json<OverlayDiffView>, AutumnError> {
    let Query(query) = query.map_err(|rejection| {
        AutumnError::bad_request_msg(format!("invalid diff query: {}", rejection.body_text()))
    })?;
    let (a, b) = (parse_version(&query.a)?, parse_version(&query.b)?);
    let store = store(&api_state)?;
    let left = stored(store.as_ref(), &kind, a).await?;
    let right = stored(store.as_ref(), &kind, b).await?;
    Ok(Json(OverlayDiffView {
        kind,
        a,
        b,
        changes: overlay_diff(left.overlay(), right.overlay()),
    }))
}

/// The audit context of one write request.
struct AuditCtx<'a> {
    actor: &'a str,
    source: &'a str,
    request_id: Option<&'a str>,
    operation: &'static str,
    route: &'static str,
    kind: &'a str,
}

/// `from v1 to v2`, or `from unset to v1`.
fn move_summary(from: Option<OverlayVersion>, to: OverlayVersion) -> String {
    let from = from.map_or_else(|| "unset".to_owned(), |from| from.to_string());
    format!("from {from} to {to}")
}

/// Insert one audit row for a write request. `outcome` is the summary of a
/// succeeded move, or the error of a failed request.
///
/// A failed request logs its audit error and keeps its own error. A
/// succeeded request returns the audit error, so an unaudited move never
/// reports success.
async fn audit_write(
    conn: &mut AsyncPgConnection,
    ctx: &AuditCtx<'_>,
    outcome: Result<&str, &str>,
) -> Result<(), AutumnError> {
    let (status, summary) = match outcome {
        Ok(summary) => (STATUS_SUCCEEDED, summary),
        Err(error) => (STATUS_FAILED, error),
    };
    let record = NewAuditRecord {
        actor: ctx.actor,
        operation: ctx.operation,
        target_type: TARGET_AGENT_PROFILE,
        target_id: Some(ctx.kind),
        route_or_command: ctx.route,
        request_id: ctx.request_id,
        idempotency_key: None,
        status,
        error_summary: Some(summary),
        shard_id: None,
        source: ctx.source,
    };
    match audit::insert_audit(conn, &record).await {
        Ok(_) => Ok(()),
        Err(error) if outcome.is_err() => {
            tracing::warn!(error = %error, kind = ctx.kind, "audit insert failed for an overlay move");
            Ok(())
        }
        Err(error) => Err(map_error(error)),
    }
}

/// Audit the outcome of one write request, then answer it.
async fn answer(
    conn: &mut AsyncPgConnection,
    ctx: &AuditCtx<'_>,
    outcome: Result<(Option<OverlayVersion>, OverlayVersion), AutumnError>,
) -> Result<Json<OverlayMove>, AutumnError> {
    match outcome {
        Ok((from, to)) => {
            audit_write(conn, ctx, Ok(&move_summary(from, to))).await?;
            Ok(Json(OverlayMove {
                kind: ctx.kind.to_owned(),
                from,
                to,
                actor: ctx.actor.to_owned(),
            }))
        }
        Err(error) => {
            audit_write(conn, ctx, Err(&error.to_string())).await?;
            Err(error)
        }
    }
}

/// The error of a bad request body.
fn bad_body(rejection: &JsonRejection) -> AutumnError {
    AutumnError::bad_request_msg(format!("invalid request body: {}", rejection.body_text()))
}

/// The move of an activate request.
async fn activate_move(
    api_state: &HarvestApiState,
    kind: &str,
    request: Result<Json<ActivateRequest>, JsonRejection>,
    actor: &str,
) -> Result<(Option<OverlayVersion>, OverlayVersion), AutumnError> {
    let Json(request) = request.map_err(|rejection| bad_body(&rejection))?;
    let Some(expected) = request.expected_active else {
        return Err(AutumnError::bad_request_msg(
            "expected_active is required: send the active version that you read, or null",
        ));
    };
    let version = number(request.version)?;
    let expected = expected.map(number).transpose()?;
    store(api_state)?
        .activate(kind, version, expected, Provenance::operator(actor))
        .await
        .map_err(|error| store_error(&error))?;
    Ok((expected, version))
}

/// The move of a rollback request. An empty body is a valid request.
async fn rollback_move(
    api_state: &HarvestApiState,
    kind: &str,
    request: Result<Json<RollbackRequest>, JsonRejection>,
    actor: &str,
) -> Result<(Option<OverlayVersion>, OverlayVersion), AutumnError> {
    match request {
        Ok(Json(RollbackRequest {})) | Err(JsonRejection::MissingJsonContentType(_)) => {}
        Err(rejection) => return Err(bad_body(&rejection)),
    }
    let back = store(api_state)?
        .rollback(kind, Provenance::operator(actor))
        .await
        .map_err(|error| store_error(&error))?;
    Ok((Some(back.from), back.to))
}

/// A version from a JSON number.
fn number(value: u64) -> Result<OverlayVersion, AutumnError> {
    OverlayVersion::new(value).ok_or_else(|| {
        AutumnError::bad_request_msg("invalid overlay version 0: versions start at 1")
    })
}

/// `POST /agents/{kind}/profile/activate`.
pub async fn activate(
    Extension(api_state): Extension<HarvestApiState>,
    Path(kind): Path<String>,
    headers: HeaderMap,
    request: Result<Json<ActivateRequest>, JsonRejection>,
) -> Result<Json<OverlayMove>, AutumnError> {
    let pool = api_state.storage_pool().map_err(map_error)?;
    let mut conn = acquire_conn(pool.default_pool()).await?;
    let (actor, source, request_id) = audit_context(&headers, &api_state);
    let outcome = activate_move(&api_state, &kind, request, &actor).await;
    let ctx = AuditCtx {
        actor: &actor,
        source: &source,
        request_id: request_id.as_deref(),
        operation: OP_AGENT_PROFILE_ACTIVATE,
        route: ACTIVATE_ROUTE,
        kind: &kind,
    };
    answer(&mut conn, &ctx, outcome).await
}

/// `POST /agents/{kind}/profile/rollback`.
pub async fn rollback(
    Extension(api_state): Extension<HarvestApiState>,
    Path(kind): Path<String>,
    headers: HeaderMap,
    request: Result<Json<RollbackRequest>, JsonRejection>,
) -> Result<Json<OverlayMove>, AutumnError> {
    let pool = api_state.storage_pool().map_err(map_error)?;
    let mut conn = acquire_conn(pool.default_pool()).await?;
    let (actor, source, request_id) = audit_context(&headers, &api_state);
    let outcome = rollback_move(&api_state, &kind, request, &actor).await;
    let ctx = AuditCtx {
        actor: &actor,
        source: &source,
        request_id: request_id.as_deref(),
        operation: OP_AGENT_PROFILE_ROLLBACK,
        route: ROLLBACK_ROUTE,
        kind: &kind,
    };
    answer(&mut conn, &ctx, outcome).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_version_reads_with_or_without_its_prefix() {
        assert_eq!(parse_version("2").ok(), OverlayVersion::new(2));
        assert_eq!(parse_version("v2").ok(), OverlayVersion::new(2));
        for bad in ["0", "v0", "-1", "two", "", "vv2"] {
            assert!(parse_version(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn a_move_names_both_ends() {
        let (v1, v2) = (OverlayVersion::new(1), OverlayVersion::new(2));
        let v2 = v2.expect("v2");
        assert_eq!(move_summary(v1, v2), "from v1 to v2");
        assert_eq!(move_summary(None, v2), "from unset to v2");
    }

    #[test]
    fn each_store_error_has_its_status() {
        let v1 = OverlayVersion::new(1).expect("v1");
        for (error, status) in [
            (
                OverlayStoreError::UnknownVersion { version: v1 },
                StatusCode::NOT_FOUND,
            ),
            (
                OverlayStoreError::Conflict { actual: Some(v1) },
                StatusCode::CONFLICT,
            ),
            (OverlayStoreError::NoEarlierActivation, StatusCode::CONFLICT),
            (
                OverlayStoreError::InvalidProvenance { reason: "empty" },
                StatusCode::BAD_REQUEST,
            ),
            (OverlayStoreError::Full, StatusCode::INTERNAL_SERVER_ERROR),
        ] {
            assert_eq!(store_error(&error).status(), status, "{error}");
        }
    }
}
