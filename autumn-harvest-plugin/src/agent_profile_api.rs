//! The agent profile overlay routes (issue #2374).
//!
//! The routes read and move the overlay versions of one agent kind, through
//! the [`ProfileOverlayStore`] that the embedder installs. They are not in
//! `ADMIN_SCOPE_ROUTES`, so the `mutate` scope of the `harvest-operator`
//! role reaches the two writes. A `read` scope reaches only the reads.
//!
//! Each activate and rollback request writes one audit row. A succeeded row
//! keeps `from v1 to v2` in `error_summary`. The move and its audit run in a
//! task of their own, so a dropped client cannot stop the audit. The design is in
//! `docs/design/DESIGN-2374.md`.
//!
//! The refine policy routes (issue #2394) read, set and reset the policy of
//! one kind. A set and a reset are mutating, so only the operator role
//! reaches them. Each one writes one audit row that names each changed key
//! with its old and new value. The design is in
//! `docs/design/DESIGN-2394.md`.

use std::sync::Arc;

use autumn_harvest::audit::{
    self, OP_AGENT_PROFILE_ACTIVATE, OP_AGENT_PROFILE_POLICY_RESET, OP_AGENT_PROFILE_POLICY_SET,
    OP_AGENT_PROFILE_ROLLBACK, STATUS_FAILED, STATUS_SUCCEEDED, TARGET_AGENT_PROFILE,
};
use autumn_harvest::chrono::{DateTime, Utc};
use autumn_harvest::models::NewAuditRecord;
use autumn_harvest_agent::agent::MAX_AGENT_NAME_BYTES;
use autumn_harvest_agent::refine::policy::{PolicyChange, policy_changes};
use autumn_harvest_agent::{
    OverlayChange, OverlayStoreError, OverlayVersion, PolicyLogEntry, PolicyState, ProfileOverlay,
    ProfileOverlayStore, Provenance, RefinePolicy, StoredOverlay, overlay_diff,
};
use autumn_web::AutumnError;
use autumn_web::reexports::axum;
use axum::extract::rejection::JsonRejection;
use axum::extract::{Extension, Json, Path, Query};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::api::{HarvestApiState, acquire_conn, audit_context, deserialize_tristate, map_error};
use crate::authz::{MAX_AUDITED_SUMMARY_LEN, truncate};

const ACTIVATE_ROUTE: &str = "POST /agents/{kind}/profile/activate";
const ROLLBACK_ROUTE: &str = "POST /agents/{kind}/profile/rollback";
const POLICY_SET_ROUTE: &str = "PUT /agents/{kind}/profile/policy";
const POLICY_RESET_ROUTE: &str = "POST /agents/{kind}/profile/policy/reset";

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
        OverlayStoreError::Conflict { .. }
        | OverlayStoreError::NoEarlierActivation
        | OverlayStoreError::PolicyConflict { .. } => {
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

/// A change that the store committed: the answer, the keys that a failed
/// audit names, and the audit summary.
struct Committed {
    body: Value,
    ends: Value,
    summary: String,
}

/// One write request: its audit context and its route.
struct Write {
    api_state: HarvestApiState,
    kind: String,
    actor: String,
    source: String,
    request_id: Option<String>,
    operation: &'static str,
    route: &'static str,
}

/// `from v1 to v2`, `from unset to v1`, or `unchanged at v2` for an
/// activation of the active version.
fn move_summary(from: Option<OverlayVersion>, to: OverlayVersion) -> String {
    match from {
        Some(from) if from == to => format!("unchanged at {to}"),
        Some(from) => format!("from {from} to {to}"),
        None => format!("from unset to {to}"),
    }
}

impl Write {
    fn new(
        api_state: HarvestApiState,
        headers: &HeaderMap,
        kind: String,
        operation: &'static str,
        route: &'static str,
    ) -> Self {
        let (actor, source, request_id) = audit_context(headers, &api_state);
        Self {
            api_state,
            kind,
            actor,
            source,
            request_id,
            operation,
            route,
        }
    }

    /// Insert one audit row. `outcome` is the summary of a succeeded move,
    /// or the error of a failed request. Both texts are cut to
    /// [`MAX_AUDITED_SUMMARY_LEN`] bytes.
    ///
    /// It takes its connection after the move, so a store on the same pool
    /// never waits for it.
    async fn audit(&self, outcome: Result<&str, &str>) -> Result<(), AutumnError> {
        let (status, summary) = match outcome {
            Ok(summary) => (STATUS_SUCCEEDED, summary),
            Err(error) => (STATUS_FAILED, error),
        };
        let pool = self.api_state.storage_pool().map_err(map_error)?;
        let mut conn = acquire_conn(pool.default_pool()).await?;
        let record = NewAuditRecord {
            actor: &self.actor,
            operation: self.operation,
            target_type: TARGET_AGENT_PROFILE,
            target_id: Some(truncate(&self.kind, MAX_AUDITED_SUMMARY_LEN)),
            route_or_command: self.route,
            request_id: self.request_id.as_deref(),
            idempotency_key: None,
            status,
            error_summary: Some(truncate(summary, MAX_AUDITED_SUMMARY_LEN)),
            shard_id: None,
            source: &self.source,
        };
        audit::insert_audit(&mut conn, &record)
            .await
            .map(|_| ())
            .map_err(map_error)
    }

    /// Audit the outcome of the request, then answer it.
    ///
    /// A failed request logs its audit error and keeps its own error. A
    /// committed move whose audit row fails answers `500` with
    /// `committed: true` and both ends. The body is built here, so no
    /// redaction of a `5xx` message hides it. An operator then does not
    /// send the request again.
    async fn finish(
        self,
        outcome: Result<(Option<OverlayVersion>, OverlayVersion), AutumnError>,
    ) -> Response {
        let outcome = outcome.map(|(from, to)| {
            let summary = move_summary(from, to);
            Committed {
                body: serde_json::to_value(OverlayMove {
                    kind: self.kind.clone(),
                    from,
                    to,
                    actor: self.actor.clone(),
                })
                .unwrap_or_default(),
                ends: serde_json::json!({"from": from, "to": to}),
                summary,
            }
        });
        self.finish_change(outcome).await
    }

    /// Audit the outcome of a request that changes the store, then answer
    /// it.
    ///
    /// A failed request logs its audit error and keeps its own error. A
    /// committed change whose audit row fails answers `500` with
    /// `committed: true` and the keys of `ends`.
    async fn finish_change(self, outcome: Result<Committed, AutumnError>) -> Response {
        match outcome {
            Ok(committed) => {
                let summary = committed.summary;
                if let Err(error) = self.audit(Ok(&summary)).await {
                    tracing::error!(
                        error = %error,
                        kind = %truncate(&self.kind, MAX_AUDITED_SUMMARY_LEN),
                        summary,
                        "an agent profile change committed, but its audit row failed"
                    );
                    let mut body = serde_json::json!({
                        "error": format!(
                            "the change {summary} committed, but its audit row failed. \
                             Do not send the request again."
                        ),
                        "committed": true,
                        "kind": self.kind,
                    });
                    if let (Some(body), Value::Object(ends)) =
                        (body.as_object_mut(), committed.ends)
                    {
                        body.extend(ends);
                    }
                    return (StatusCode::INTERNAL_SERVER_ERROR, Json(body)).into_response();
                }
                Json(committed.body).into_response()
            }
            Err(error) => {
                if let Err(audit_error) = self.audit(Err(&error.to_string())).await {
                    tracing::warn!(
                        error = %audit_error,
                        kind = %truncate(&self.kind, MAX_AUDITED_SUMMARY_LEN),
                        "audit insert failed for a refused agent profile change"
                    );
                }
                error.into_response()
            }
        }
    }
}

/// Run `body` and its audit in a task of its own. A client that drops the
/// connection then cannot stop the audit after the move commits. The task
/// keeps the log context and the shard fence of the request.
async fn detached<F>(body: F) -> Response
where
    F: Future<Output = Response> + Send + 'static,
{
    let task = autumn_web::log::context::in_current_context(autumn_harvest::shard_fence::scoped(
        autumn_harvest::shard_fence::current(),
        body,
    ));
    match tokio::spawn(task).await {
        Ok(response) => response,
        Err(error) => {
            AutumnError::internal_server_error_msg(format!("the overlay move task failed: {error}"))
                .into_response()
        }
    }
}

/// Refuse a kind longer than an agent name can be, before any move. So
/// each audit row names the whole kind.
fn check_kind(kind: &str) -> Result<(), AutumnError> {
    if kind.len() > MAX_AGENT_NAME_BYTES {
        return Err(AutumnError::bad_request_msg(format!(
            "the kind is longer than {MAX_AGENT_NAME_BYTES} bytes"
        )));
    }
    Ok(())
}

/// The error of a bad request body.
fn bad_body(rejection: &JsonRejection) -> AutumnError {
    AutumnError::bad_request_msg(format!(
        "invalid request body: {}",
        truncate(&rejection.body_text(), MAX_AUDITED_SUMMARY_LEN)
    ))
}

/// The move of an activate request.
async fn activate_move(
    api_state: &HarvestApiState,
    kind: &str,
    request: Result<Json<ActivateRequest>, JsonRejection>,
    actor: &str,
) -> Result<(Option<OverlayVersion>, OverlayVersion), AutumnError> {
    check_kind(kind)?;
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
    check_kind(kind)?;
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
) -> Response {
    let write = Write::new(
        api_state,
        &headers,
        kind,
        OP_AGENT_PROFILE_ACTIVATE,
        ACTIVATE_ROUTE,
    );
    detached(async move {
        let outcome = activate_move(&write.api_state, &write.kind, request, &write.actor).await;
        write.finish(outcome).await
    })
    .await
}

/// `POST /agents/{kind}/profile/rollback`.
pub async fn rollback(
    Extension(api_state): Extension<HarvestApiState>,
    Path(kind): Path<String>,
    headers: HeaderMap,
    request: Result<Json<RollbackRequest>, JsonRejection>,
) -> Response {
    let write = Write::new(
        api_state,
        &headers,
        kind,
        OP_AGENT_PROFILE_ROLLBACK,
        ROLLBACK_ROUTE,
    );
    detached(async move {
        let outcome = rollback_move(&write.api_state, &write.kind, request, &write.actor).await;
        write.finish(outcome).await
    })
    .await
}

/// The body of `GET /agents/{kind}/profile/policy`, and of a set or a
/// reset.
#[derive(Debug, Clone, Serialize)]
pub struct PolicyView {
    /// The agent kind.
    pub kind: String,
    /// The revision of the last change, or `null` when the kind has none.
    pub revision: Option<u64>,
    /// Whether the last change stored a policy. A reset stores none.
    pub custom: bool,
    /// The policy. With no stored policy, every default.
    pub policy: RefinePolicy,
    /// The operator who made the change. Only a set or a reset has it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actor: Option<String>,
    /// Each setting that the change changed. Only a set or a reset has it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub changes: Option<Vec<PolicyChange>>,
}

/// One change in `GET /agents/{kind}/profile/policy/changes`.
#[derive(Debug, Clone, Serialize)]
pub struct PolicyChangeView {
    /// The number of the change, from 1.
    pub revision: u64,
    /// The operator id.
    pub actor: String,
    /// The time of the change.
    pub changed_at: DateTime<Utc>,
    /// The policy before the change, or `null` for every default.
    pub old: Option<RefinePolicy>,
    /// The policy after the change, or `null` for every default.
    pub new: Option<RefinePolicy>,
    /// Each setting that differs.
    pub changes: Vec<PolicyChange>,
}

/// The body of `GET /agents/{kind}/profile/policy/changes`.
#[derive(Debug, Clone, Serialize)]
pub struct PolicyChangeList {
    /// The agent kind.
    pub kind: String,
    /// Each change, oldest first.
    pub changes: Vec<PolicyChangeView>,
}

/// The body of `PUT /agents/{kind}/profile/policy`.
///
/// Both keys are required. `expected_revision` is the revision that the
/// client read, or `null` when the kind has no change.
#[expect(
    clippy::option_option,
    reason = "serde tri-state: absent is an error, null is no revision"
)]
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyRequest {
    policy: RefinePolicy,
    #[serde(default, deserialize_with = "deserialize_tristate")]
    expected_revision: Option<Option<u64>>,
}

/// The body of `POST /agents/{kind}/profile/policy/reset`. It has no field.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResetRequest {}

fn policy_view(kind: &str, state: PolicyState) -> PolicyView {
    PolicyView {
        kind: kind.to_owned(),
        revision: state.revision,
        custom: state.custom,
        policy: state.policy,
        actor: None,
        changes: None,
    }
}

/// The settings that `entry` changed.
fn entry_changes(entry: &PolicyLogEntry) -> Vec<PolicyChange> {
    let old = entry.before().cloned().unwrap_or_default();
    let new = entry.after().cloned().unwrap_or_default();
    policy_changes(&old, &new)
}

/// The audit rank of a policy path. The settings that widen the gate come
/// first, so a cut summary keeps them.
fn audit_rank(path: &str) -> u8 {
    match path {
        _ if path.starts_with("auto_adopt.") => 0,
        "probe_effects" => 1,
        _ if path.starts_with("ceilings.max_") => 2,
        "ceilings.models" => 3,
        "approval_timeout" => 4,
        _ => 5,
    }
}

/// One change as audit text. A list shows the names that it adds and
/// removes. Any other value shows `old -> new`.
fn audit_change(change: &PolicyChange) -> String {
    let names = |value: &Value| -> Option<Vec<String>> {
        match value {
            Value::Array(items) => Some(items.iter().map(Value::to_string).collect()),
            _ => None,
        }
    };
    match (names(&change.old), names(&change.new)) {
        (Some(old), Some(new)) => {
            let added: Vec<_> = new
                .iter()
                .filter(|name| !old.contains(name))
                .cloned()
                .collect();
            let removed: Vec<_> = old
                .iter()
                .filter(|name| !new.contains(name))
                .cloned()
                .collect();
            format!(
                "{} +[{}] -[{}]",
                change.path,
                added.join(", "),
                removed.join(", ")
            )
        }
        _ => format!("{} {} -> {}", change.path, change.old, change.new),
    }
}

/// `revision 2: auto_adopt.parts +["system"] -[]; ...`, or
/// `revision 2: no change`.
///
/// The settings that widen the gate come first. A summary above the audit
/// limit ends with a mark that names the revision. A reader then reads the
/// whole change from the policy log.
fn policy_summary(revision: u64, changes: &[PolicyChange]) -> String {
    if changes.is_empty() {
        return format!("revision {revision}: no change");
    }
    let mut ordered: Vec<&PolicyChange> = changes.iter().collect();
    ordered.sort_by_key(|change| audit_rank(&change.path));
    let parts: Vec<String> = ordered.into_iter().map(audit_change).collect();
    let summary = format!("revision {revision}: {}", parts.join("; "));
    if summary.len() <= MAX_AUDITED_SUMMARY_LEN {
        return summary;
    }
    let mark = format!(" ... (cut: read revision {revision} at GET .../policy/changes)");
    let mut end = MAX_AUDITED_SUMMARY_LEN.saturating_sub(mark.len());
    while !summary.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}{mark}", &summary[..end])
}

/// The committed answer of a set or a reset.
fn policy_committed(kind: &str, entry: &PolicyLogEntry, actor: &str) -> Committed {
    let changes = entry_changes(entry);
    let summary = policy_summary(entry.revision(), &changes);
    let body = PolicyView {
        kind: kind.to_owned(),
        revision: Some(entry.revision()),
        custom: entry.after().is_some(),
        policy: entry.after().cloned().unwrap_or_default(),
        actor: Some(actor.to_owned()),
        changes: Some(changes),
    };
    Committed {
        body: serde_json::to_value(body).unwrap_or_default(),
        ends: serde_json::json!({"revision": entry.revision()}),
        summary,
    }
}

/// `GET /agents/{kind}/profile/policy`.
pub async fn get_policy(
    Extension(api_state): Extension<HarvestApiState>,
    Path(kind): Path<String>,
) -> Result<Json<PolicyView>, AutumnError> {
    let state = store(&api_state)?
        .policy(&kind)
        .await
        .map_err(|error| store_error(&error))?;
    Ok(Json(policy_view(&kind, state)))
}

/// `GET /agents/{kind}/profile/policy/changes`.
pub async fn list_policy_changes(
    Extension(api_state): Extension<HarvestApiState>,
    Path(kind): Path<String>,
) -> Result<Json<PolicyChangeList>, AutumnError> {
    let log = store(&api_state)?
        .policy_log(&kind)
        .await
        .map_err(|error| store_error(&error))?;
    let changes = log
        .iter()
        .map(|entry| PolicyChangeView {
            revision: entry.revision(),
            actor: entry.actor().author(),
            changed_at: entry.changed_at(),
            old: entry.before().cloned(),
            new: entry.after().cloned(),
            changes: entry_changes(entry),
        })
        .collect();
    Ok(Json(PolicyChangeList { kind, changes }))
}

/// The change of a set request.
async fn set_policy_change(
    api_state: &HarvestApiState,
    kind: &str,
    request: Result<Json<PolicyRequest>, JsonRejection>,
    actor: &str,
) -> Result<Committed, AutumnError> {
    check_kind(kind)?;
    let Json(request) = request.map_err(|rejection| bad_body(&rejection))?;
    let Some(expected) = request.expected_revision else {
        return Err(AutumnError::bad_request_msg(
            "expected_revision is required: send the revision that you read, or null",
        ));
    };
    let entry = store(api_state)?
        .set_policy(kind, &request.policy, expected, Provenance::operator(actor))
        .await
        .map_err(|error| store_error(&error))?;
    Ok(policy_committed(kind, &entry, actor))
}

/// The change of a reset request. An empty body is a valid request.
async fn reset_policy_change(
    api_state: &HarvestApiState,
    kind: &str,
    request: Result<Json<ResetRequest>, JsonRejection>,
    actor: &str,
) -> Result<Committed, AutumnError> {
    check_kind(kind)?;
    match request {
        Ok(Json(ResetRequest {})) | Err(JsonRejection::MissingJsonContentType(_)) => {}
        Err(rejection) => return Err(bad_body(&rejection)),
    }
    let entry = store(api_state)?
        .reset_policy(kind, Provenance::operator(actor))
        .await
        .map_err(|error| store_error(&error))?;
    Ok(policy_committed(kind, &entry, actor))
}

/// `PUT /agents/{kind}/profile/policy`.
pub async fn set_policy(
    Extension(api_state): Extension<HarvestApiState>,
    Path(kind): Path<String>,
    headers: HeaderMap,
    request: Result<Json<PolicyRequest>, JsonRejection>,
) -> Response {
    let write = Write::new(
        api_state,
        &headers,
        kind,
        OP_AGENT_PROFILE_POLICY_SET,
        POLICY_SET_ROUTE,
    );
    detached(async move {
        let outcome = set_policy_change(&write.api_state, &write.kind, request, &write.actor).await;
        write.finish_change(outcome).await
    })
    .await
}

/// `POST /agents/{kind}/profile/policy/reset`.
pub async fn reset_policy(
    Extension(api_state): Extension<HarvestApiState>,
    Path(kind): Path<String>,
    headers: HeaderMap,
    request: Result<Json<ResetRequest>, JsonRejection>,
) -> Response {
    let write = Write::new(
        api_state,
        &headers,
        kind,
        OP_AGENT_PROFILE_POLICY_RESET,
        POLICY_RESET_ROUTE,
    );
    detached(async move {
        let outcome =
            reset_policy_change(&write.api_state, &write.kind, request, &write.actor).await;
        write.finish_change(outcome).await
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_version_reads_with_or_without_its_prefix() {
        assert_eq!(parse_version("2").ok(), OverlayVersion::new(2));
        assert_eq!(parse_version("v2").ok(), OverlayVersion::new(2));
        for bad in ["0", "v0", "-1", "two", "", "vv2"] {
            assert!(parse_version(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn a_policy_summary_puts_the_gate_first_and_marks_a_cut() {
        let change = |path: &str, old: Value, new: Value| PolicyChange {
            path: path.to_owned(),
            old,
            new,
        };
        let summary = policy_summary(
            3,
            &[
                change("approval_timeout", json!("7d"), json!("1h")),
                change("auto_adopt.parts", json!([]), json!(["system"])),
                change("probe_effects", json!(["read_only"]), json!(["write"])),
            ],
        );
        assert_eq!(
            summary,
            r#"revision 3: auto_adopt.parts +["system"] -[]; probe_effects +["write"] -["read_only"]; approval_timeout "7d" -> "1h""#
        );
        assert_eq!(policy_summary(4, &[]), "revision 4: no change");
        let many: Vec<Value> = (0..64).map(|n| json!(format!("model-{n:03}"))).collect();
        let long = policy_summary(
            5,
            &[
                change("ceilings.models", Value::Null, Value::Array(many)),
                change("probe_effects", json!(["read_only"]), json!(["external"])),
            ],
        );
        assert!(long.len() <= MAX_AUDITED_SUMMARY_LEN, "{}", long.len());
        assert!(
            long.starts_with(r#"revision 5: probe_effects +["external"]"#),
            "{long}"
        );
        assert!(
            long.ends_with("read revision 5 at GET .../policy/changes)"),
            "{long}"
        );
    }

    #[test]
    fn a_move_names_both_ends() {
        let (v1, v2) = (OverlayVersion::new(1), OverlayVersion::new(2));
        let v2 = v2.expect("v2");
        assert_eq!(move_summary(v1, v2), "from v1 to v2");
        assert_eq!(move_summary(None, v2), "from unset to v2");
        assert_eq!(move_summary(Some(v2), v2), "unchanged at v2");
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
