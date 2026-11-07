//! `GET` and `PATCH /admin/activity-types/{activity_name}/defaults`.
//!
//! The routes read and change the operator override of one activity type.
//! It holds three retry fields, four timeouts and the `max_concurrent` cap.
//! The override lives on the default shard. See
//! [`autumn_harvest::activity_defaults`] for the precedence and the scope.
//!
//! A change reaches new schedules in this process at once, because the route
//! writes it through to the cache. Other processes see it within their refresh period.
//! An activity that is already scheduled keeps its retry fields and its
//! timeouts.
//!
//! A change to the cap also applies to the tasks that are already
//! scheduled. Each task row stores its cap, and the claim reads it. So a
//! `PATCH` that names `max_concurrent` or `clear_override` sets the cap of
//! each live row of the activity on every shard. It runs that pass again at
//! each settle wait, for a row that a process with a stale cache stored. A
//! running task runs on.
//!
//! The route refuses a field that cannot apply:
//!
//! - a retry field for an activity with no code `retry_policy`;
//! - a `heartbeat_timeout_ms` for an activity with no code heartbeat
//!   timeout, because an activity that does not heartbeat would time out on
//!   each attempt;
//! - a heartbeat, `schedule_to_start` or `schedule_to_close` field for a
//!   local activity, because a local activity has none of them;
//! - a `max_concurrent` for a local activity, which has no task row;
//! - a `max_concurrent` for an activity with no code cap, whose rows have no
//!   concurrency key;
//! - a `max_concurrent` for an activity that shares its concurrency key with
//!   another activity, because one key would then hold two caps;
//! - any field for a reserved engine activity.

use std::time::Duration;

use autumn_harvest::activity_defaults::{
    ActivityDefaultChange, ActivityDefaultFields, ActivityDefaultOverride, ActivityDefaultPatch,
    ActivityDefaults, get_activity_default_override, key_sharer, update_activity_default_override,
};
use autumn_harvest::audit::{
    self, OP_ACTIVITY_TYPE_DEFAULTS, STATUS_FAILED, STATUS_SUCCEEDED, TARGET_ACTIVITY_TYPE,
};
use autumn_harvest::error::HarvestError;
use autumn_harvest::models::NewAuditRecord;
use autumn_harvest::workflow_retry::RetryLimitsPatch;
use autumn_web::AutumnError;
use autumn_web::reexports::axum;
use axum::extract::{Extension, Json, Path};
use diesel_async::AsyncPgConnection;
use serde::{Deserialize, Serialize};

use crate::api::{
    HarvestApiRuntime, HarvestApiState, acquire_conn, audit_context, deserialize_tristate,
    map_error,
};
use crate::run_defaults_api::{ShardJob, settle_on_every_shard};
use crate::shard_fanout::UnavailableShard;

const ROUTE: &str = "PATCH /admin/activity-types/{activity_name}/defaults";

/// A duration in whole milliseconds.
fn millis(value: Duration) -> u64 {
    u64::try_from(value.as_millis()).unwrap_or(u64::MAX)
}

/// The activity defaults, durations in milliseconds. `None` sets no value.
///
/// `max_concurrent` is a count of tasks, not a duration.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct ActivityDefaultMs {
    /// The retry `max_attempts`.
    pub retry_max_attempts: Option<u32>,
    /// The retry `initial_interval`.
    pub retry_initial_interval_ms: Option<u64>,
    /// The retry `max_interval`.
    pub retry_max_interval_ms: Option<u64>,
    /// The bound on one attempt.
    pub start_to_close_ms: Option<u64>,
    /// The bound between two heartbeats.
    pub heartbeat_timeout_ms: Option<u64>,
    /// The bound on the wait for a worker.
    pub schedule_to_start_ms: Option<u64>,
    /// The bound on every attempt and every backoff together.
    pub schedule_to_close_ms: Option<u64>,
    /// The cap on the running tasks of the concurrency key.
    pub max_concurrent: Option<u32>,
}

impl From<ActivityDefaultFields> for ActivityDefaultMs {
    fn from(fields: ActivityDefaultFields) -> Self {
        Self {
            retry_max_attempts: fields.retry.max_attempts,
            retry_initial_interval_ms: fields.retry.initial_interval.map(millis),
            retry_max_interval_ms: fields.retry.max_interval.map(millis),
            start_to_close_ms: fields.start_to_close.map(millis),
            heartbeat_timeout_ms: fields.heartbeat_timeout.map(millis),
            schedule_to_start_ms: fields.schedule_to_start.map(millis),
            schedule_to_close_ms: fields.schedule_to_close.map(millis),
            max_concurrent: fields.max_concurrent,
        }
    }
}

impl From<&ActivityDefaults> for ActivityDefaultMs {
    fn from(values: &ActivityDefaults) -> Self {
        let policy = values.retry_policy.as_ref();
        Self {
            retry_max_attempts: policy.map(|p| p.max_attempts),
            retry_initial_interval_ms: policy.map(|p| millis(p.initial_interval)),
            retry_max_interval_ms: policy.map(|p| millis(p.max_interval)),
            start_to_close_ms: values.start_to_close.map(millis),
            heartbeat_timeout_ms: values.heartbeat_timeout.map(millis),
            schedule_to_start_ms: values.schedule_to_start.map(millis),
            schedule_to_close_ms: values.schedule_to_close.map(millis),
            max_concurrent: values.max_concurrent,
        }
    }
}

/// The stored override, as the routes show it.
#[derive(Debug, Clone, Serialize)]
pub struct ActivityDefaultOverrideView {
    /// The fields that the override sets.
    #[serde(flatten)]
    pub fields: ActivityDefaultMs,
    /// The operator's stated reason.
    pub reason: Option<String>,
    /// The actor that last changed the override.
    pub updated_by: String,
    /// When the override last changed.
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

impl From<ActivityDefaultOverride> for ActivityDefaultOverrideView {
    fn from(stored: ActivityDefaultOverride) -> Self {
        Self {
            fields: stored.fields.into(),
            reason: stored.reason,
            updated_by: stored.updated_by,
            updated_at: stored.updated_at,
        }
    }
}

/// The defaults of one activity type.
#[derive(Debug, Clone, Serialize)]
pub struct ActivityDefaultsView {
    /// The activity name.
    pub activity_name: String,
    /// Whether the activity runs local, inline on the workflow task.
    pub local: bool,
    /// The values declared in code.
    pub code: ActivityDefaultMs,
    /// The stored override, or `null` when there is none.
    #[serde(rename = "override")]
    pub override_: Option<ActivityDefaultOverrideView>,
    /// The values that a new schedule takes, before a call-site option and
    /// the builder default.
    pub effective: ActivityDefaultMs,
    /// The rewrite of the stored caps. Present only on a `PATCH` that names
    /// `max_concurrent` or `clear_override`, for a remote activity with a
    /// code cap.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cap_rewrite: Option<CapRewriteReport>,
}

/// The first rewrite of the stored caps of the live task rows.
#[derive(Debug, Clone, Serialize)]
pub struct CapRewriteReport {
    /// The number of live task rows whose stored cap changed.
    pub rewritten_tasks: u64,
    /// The number of live task rows that still hold another cap, because
    /// another transaction held them. A settle pass sets them.
    pub unresolved_tasks: u64,
    /// The shards that the rewrite could not reach. Their rows keep the old
    /// cap until a settle pass reaches them. Send the request again to retry.
    pub unavailable_shards: Vec<UnavailableShard>,
    /// Whether a newer change to the cap stopped this rewrite.
    pub superseded: bool,
}

/// The body of `PATCH /admin/activity-types/{activity_name}/defaults`.
///
/// Each field is tri-state. An absent key keeps the stored override. A
/// `null` returns the field to the code value. A number sets the override.
#[expect(
    clippy::option_option,
    reason = "serde tri-state: absent keeps, null returns to code, a value sets"
)]
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActivityDefaultsPatchRequest {
    #[serde(default, deserialize_with = "deserialize_tristate")]
    retry_max_attempts: Option<Option<u32>>,
    #[serde(default, deserialize_with = "deserialize_tristate")]
    retry_initial_interval_ms: Option<Option<u64>>,
    #[serde(default, deserialize_with = "deserialize_tristate")]
    retry_max_interval_ms: Option<Option<u64>>,
    #[serde(default, deserialize_with = "deserialize_tristate")]
    start_to_close_ms: Option<Option<u64>>,
    #[serde(default, deserialize_with = "deserialize_tristate")]
    heartbeat_timeout_ms: Option<Option<u64>>,
    #[serde(default, deserialize_with = "deserialize_tristate")]
    schedule_to_start_ms: Option<Option<u64>>,
    #[serde(default, deserialize_with = "deserialize_tristate")]
    schedule_to_close_ms: Option<Option<u64>>,
    #[serde(default, deserialize_with = "deserialize_tristate")]
    max_concurrent: Option<Option<u32>>,
    #[serde(default)]
    clear_override: bool,
    reason: Option<String>,
}

/// Whether a tri-state field sets a value. A `null` sets none.
#[expect(
    clippy::option_option,
    reason = "serde tri-state: absent keeps, null returns to code, a value sets"
)]
const fn sets<T: Copy>(value: Option<Option<T>>) -> bool {
    matches!(value, Some(Some(_)))
}

impl ActivityDefaultsPatchRequest {
    fn patch(&self) -> ActivityDefaultPatch {
        let ms = |value: Option<Option<u64>>| value.map(|v| v.map(Duration::from_millis));
        ActivityDefaultPatch {
            retry: RetryLimitsPatch {
                max_attempts: self.retry_max_attempts,
                initial_interval: ms(self.retry_initial_interval_ms),
                max_interval: ms(self.retry_max_interval_ms),
            },
            start_to_close: ms(self.start_to_close_ms),
            heartbeat_timeout: ms(self.heartbeat_timeout_ms),
            schedule_to_start: ms(self.schedule_to_start_ms),
            schedule_to_close: ms(self.schedule_to_close_ms),
            max_concurrent: self.max_concurrent,
        }
    }

    /// Whether this request can change the effective cap.
    const fn touches_cap(&self) -> bool {
        self.max_concurrent.is_some() || self.clear_override
    }

    fn is_empty(&self) -> bool {
        self.patch().is_empty() && !self.clear_override
    }

    /// The reason to refuse this request for an activity, if any.
    ///
    /// `sharer` names another registered activity with the same concurrency
    /// key, if any.
    fn refusal(
        &self,
        code: &ActivityDefaults,
        local: bool,
        sharer: Option<&str>,
    ) -> Option<String> {
        if sets(self.max_concurrent) {
            if local {
                return Some(
                    "a local activity has no task row, so max_concurrent is refused".to_string(),
                );
            }
            if code.max_concurrent.is_none() {
                return Some(
                    "the activity declares no max_concurrent, so max_concurrent is refused: \
                     its tasks have no concurrency key"
                        .to_string(),
                );
            }
            if let Some(sharer) = sharer {
                return Some(format!(
                    "the activity shares its concurrency key with '{sharer}', so \
                     max_concurrent is refused: one key would hold two caps"
                ));
            }
        }
        if local
            && (sets(self.heartbeat_timeout_ms)
                || sets(self.schedule_to_start_ms)
                || sets(self.schedule_to_close_ms))
        {
            return Some(
                "a local activity has no heartbeat, schedule_to_start or schedule_to_close, \
                 so heartbeat_timeout_ms, schedule_to_start_ms and schedule_to_close_ms are \
                 refused"
                    .to_string(),
            );
        }
        if code.retry_policy.is_none() && self.patch().retry.sets_any() {
            return Some(
                "the activity declares no retry_policy, so retry_max_attempts, \
                 retry_initial_interval_ms and retry_max_interval_ms are refused"
                    .to_string(),
            );
        }
        if code.heartbeat_timeout.is_none() && sets(self.heartbeat_timeout_ms) {
            return Some(
                "the activity declares no heartbeat_timeout, so heartbeat_timeout_ms is \
                 refused: an activity that does not heartbeat would time out on each attempt"
                    .to_string(),
            );
        }
        None
    }
}

/// The code values of an activity and whether it is local, or `None` when
/// this process does not register it.
fn code_values(
    runtime: &HarvestApiRuntime,
    activity_name: &str,
) -> Option<(ActivityDefaults, bool)> {
    runtime
        .registry()
        .activities
        .get(activity_name)
        .map(|info| (ActivityDefaults::of(info), info.is_local))
}

fn view(
    activity_name: &str,
    code: &ActivityDefaults,
    local: bool,
    stored: Option<ActivityDefaultOverride>,
    cap_rewrite: Option<CapRewriteReport>,
) -> ActivityDefaultsView {
    let effective = stored
        .as_ref()
        .map_or_else(|| code.clone(), |stored| stored.fields.apply(code.clone()));
    ActivityDefaultsView {
        activity_name: activity_name.to_string(),
        local,
        code: code.into(),
        override_: stored.map(Into::into),
        effective: (&effective).into(),
        cap_rewrite,
    }
}

fn not_registered(activity_name: &str) -> AutumnError {
    AutumnError::not_found_msg(format!("activity type '{activity_name}' is not registered"))
}

/// `GET /admin/activity-types/{activity_name}/defaults`.
pub async fn get_activity_defaults(
    Extension(api_state): Extension<HarvestApiState>,
    Path(activity_name): Path<String>,
) -> Result<Json<ActivityDefaultsView>, AutumnError> {
    let runtime = api_state.runtime().map_err(map_error)?;
    let (code, local) =
        code_values(&runtime, &activity_name).ok_or_else(|| not_registered(&activity_name))?;
    let pool = api_state.storage_pool().map_err(map_error)?;
    let mut conn = acquire_conn(pool.default_pool()).await?;
    let stored = get_activity_default_override(&mut conn, &activity_name)
        .await
        .map_err(map_error)?;
    Ok(Json(view(&activity_name, &code, local, stored, None)))
}

/// One `activity_type.defaults` audit row. `error_summary` marks a failed
/// request.
const fn audit_record<'a>(
    ctx: (&'a str, &'a str, Option<&'a str>),
    activity_name: &'a str,
    error_summary: Option<&'a str>,
) -> NewAuditRecord<'a> {
    let (actor, source, request_id) = ctx;
    NewAuditRecord {
        actor,
        operation: OP_ACTIVITY_TYPE_DEFAULTS,
        target_type: TARGET_ACTIVITY_TYPE,
        target_id: Some(activity_name),
        route_or_command: ROUTE,
        request_id,
        idempotency_key: None,
        status: if error_summary.is_some() {
            STATUS_FAILED
        } else {
            STATUS_SUCCEEDED
        },
        error_summary,
        shard_id: None,
        source,
    }
}

/// Insert the audit row of a refused request.
///
/// A failed insert logs its error, and the request keeps its own error.
async fn audit_refusal(
    conn: &mut AsyncPgConnection,
    ctx: (&str, &str, Option<&str>),
    activity_name: &str,
    error_summary: &str,
) {
    let record = audit_record(ctx, activity_name, Some(error_summary));
    if let Err(error) = audit::insert_audit(conn, &record).await {
        tracing::warn!(error = %error, activity_name, "audit insert failed for activity defaults");
    }
}

/// Why a change did not commit.
enum ChangeFailure {
    /// The store refused or failed the write.
    Write(HarvestError),
    /// The audit insert failed, so the write rolled back.
    Audit(HarvestError),
}

impl From<diesel::result::Error> for ChangeFailure {
    fn from(error: diesel::result::Error) -> Self {
        Self::Write(error.into())
    }
}

/// Write the change and its audit row in one transaction.
///
/// So a change never applies without its audit row.
async fn commit_change(
    conn: &mut AsyncPgConnection,
    change: &ActivityDefaultChange<'_>,
    record: &NewAuditRecord<'_>,
) -> Result<Option<ActivityDefaultOverride>, ChangeFailure> {
    use diesel_async::AsyncConnection;

    Box::pin(conn.transaction(async |conn| {
        let stored = update_activity_default_override(conn, change)
            .await
            .map_err(ChangeFailure::Write)?;
        audit::insert_audit(conn, record)
            .await
            .map_err(ChangeFailure::Audit)?;
        Ok(stored)
    }))
    .await
}

/// `PATCH /admin/activity-types/{activity_name}/defaults`.
pub async fn patch_activity_defaults(
    Extension(api_state): Extension<HarvestApiState>,
    Path(activity_name): Path<String>,
    headers: axum::http::HeaderMap,
    request: Result<Json<ActivityDefaultsPatchRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Json<ActivityDefaultsView>, AutumnError> {
    let runtime = api_state.runtime().map_err(map_error)?;
    let pool = api_state.storage_pool().map_err(map_error)?;
    let mut conn = acquire_conn(pool.default_pool()).await?;
    let (actor, source, request_id) = audit_context(&headers, &api_state);
    let ctx = (actor.as_str(), source.as_str(), request_id.as_deref());

    // The handler unwraps the body, so a bad body gets this route's 400 with
    // an audit row, not the plain-text 422 of axum.
    let request = match request {
        Ok(Json(request)) if request.is_empty() => Err(String::from(
            "empty patch: set retry_max_attempts, retry_initial_interval_ms, \
             retry_max_interval_ms, start_to_close_ms, heartbeat_timeout_ms, \
             schedule_to_start_ms, schedule_to_close_ms, max_concurrent or clear_override",
        )),
        Ok(Json(request)) => Ok(request),
        Err(rejection) => Err(format!("invalid request body: {}", rejection.body_text())),
    };
    let request = match request {
        Ok(request) => request,
        Err(summary) => {
            audit_refusal(&mut conn, ctx, &activity_name, &summary).await;
            return Err(AutumnError::bad_request_msg(summary));
        }
    };
    if autumn_harvest::activity_defaults::is_reserved_activity(&activity_name) {
        let summary = "the activity is a reserved engine activity, so its defaults are refused";
        audit_refusal(&mut conn, ctx, &activity_name, summary).await;
        return Err(AutumnError::bad_request_msg(summary));
    }
    let Some((code, local)) = code_values(&runtime, &activity_name) else {
        audit_refusal(
            &mut conn,
            ctx,
            &activity_name,
            "activity type not registered",
        )
        .await;
        return Err(not_registered(&activity_name));
    };
    let sharer =
        key_sharer(runtime.registry().activities.values(), &activity_name).map(str::to_string);
    if let Some(summary) = request.refusal(&code, local, sharer.as_deref()) {
        audit_refusal(&mut conn, ctx, &activity_name, &summary).await;
        return Err(AutumnError::bad_request_msg(summary));
    }

    let change = ActivityDefaultChange {
        activity_name: &activity_name,
        patch: request.patch(),
        clear: request.clear_override,
        actor: &actor,
        reason: request.reason.as_deref(),
    };
    let record = audit_record(ctx, &activity_name, None);
    let stored = match commit_change(&mut conn, &change, &record).await {
        Ok(stored) => stored,
        Err(ChangeFailure::Write(error)) => {
            let summary = error.to_string();
            audit_refusal(&mut conn, ctx, &activity_name, &summary).await;
            return Err(match error {
                HarvestError::Config(message) => AutumnError::bad_request_msg(format!(
                    "invalid activity default override: {message}"
                )),
                other => map_error(other),
            });
        }
        Err(ChangeFailure::Audit(error)) => return Err(map_error(error)),
    };
    // Write through, so this process applies the change to its next schedule.
    let fields = stored.as_ref().map(|s| s.fields).unwrap_or_default();
    runtime
        .registry()
        .run_defaults()
        .write_through_activity(&activity_name, fields);
    // The pass takes its own connections, the default shard among them.
    // Holding this one would starve a small pool.
    drop(conn);
    // A stored row changes its cap too, so a pending task takes the new cap.
    let cap_rewrite = match code.max_concurrent {
        Some(code_cap) if request.touches_cap() && !local => {
            let cap = fields.max_concurrent.unwrap_or(code_cap);
            let job = ShardJob::ActivityCaps { code_cap, cap };
            let pass = settle_on_every_shard(&api_state, &pool, &activity_name, job).await?;
            Some(CapRewriteReport {
                rewritten_tasks: pass.changed,
                unresolved_tasks: pass.unresolved,
                unavailable_shards: pass.unavailable_shards,
                superseded: pass.superseded,
            })
        }
        _ => None,
    };
    Ok(Json(view(
        &activity_name,
        &code,
        local,
        stored,
        cap_rewrite,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use autumn_harvest::info::ActivityInfo;

    fn request(body: serde_json::Value) -> ActivityDefaultsPatchRequest {
        serde_json::from_value(body).expect("body")
    }

    fn full() -> ActivityDefaults {
        ActivityDefaults {
            retry_policy: Some(autumn_harvest::policy::RetryPolicy::default()),
            start_to_close: Some(Duration::from_secs(1)),
            heartbeat_timeout: Some(Duration::from_secs(1)),
            schedule_to_start: Some(Duration::from_secs(1)),
            schedule_to_close: Some(Duration::from_secs(1)),
            max_concurrent: Some(3),
        }
    }

    #[test]
    fn a_null_never_counts_as_a_set_field() {
        let bare = ActivityDefaults::default();
        let nulls = request(serde_json::json!({
            "retry_max_attempts": null,
            "heartbeat_timeout_ms": null,
            "schedule_to_close_ms": null,
            "max_concurrent": null,
        }));
        assert!(!nulls.is_empty());
        assert_eq!(nulls.refusal(&bare, true, None), None);
    }

    #[test]
    fn a_full_remote_activity_takes_every_field() {
        let all = request(serde_json::json!({
            "retry_max_attempts": 2,
            "retry_initial_interval_ms": 10,
            "retry_max_interval_ms": 20,
            "start_to_close_ms": 30,
            "heartbeat_timeout_ms": 40,
            "schedule_to_start_ms": 50,
            "schedule_to_close_ms": 60,
            "max_concurrent": 2,
        }));
        assert_eq!(all.refusal(&full(), false, None), None);
        let patch = all.patch();
        assert_eq!(patch.start_to_close, Some(Some(Duration::from_millis(30))));
        assert_eq!(
            patch.retry.max_interval,
            Some(Some(Duration::from_millis(20)))
        );
    }

    #[test]
    fn an_empty_body_is_empty_and_a_clear_is_not() {
        assert!(request(serde_json::json!({})).is_empty());
        assert!(request(serde_json::json!({ "reason": "x" })).is_empty());
        assert!(!request(serde_json::json!({ "clear_override": true })).is_empty());
    }

    #[test]
    fn a_cap_is_refused_where_it_cannot_apply() {
        let cap = request(serde_json::json!({ "max_concurrent": 1 }));
        assert!(cap.touches_cap());
        let local = cap.refusal(&full(), true, None).expect("local");
        assert!(local.contains("local"), "{local}");
        let uncapped = ActivityDefaults {
            max_concurrent: None,
            ..full()
        };
        let none = cap.refusal(&uncapped, false, None).expect("no code cap");
        assert!(none.contains("declares no max_concurrent"), "{none}");
        let shared = cap.refusal(&full(), false, Some("sync_b")).expect("shared");
        assert!(shared.contains("sync_b"), "{shared}");
        assert_eq!(cap.refusal(&full(), false, None), None);
        let clear = request(serde_json::json!({ "max_concurrent": null }));
        assert!(clear.touches_cap(), "a null returns to the code cap");
        assert_eq!(clear.refusal(&full(), false, Some("sync_b")), None);
        assert!(request(serde_json::json!({ "clear_override": true })).touches_cap());
        assert!(!request(serde_json::json!({ "start_to_close_ms": 1 })).touches_cap());
    }

    fn capped(name: &'static str, cap: Option<u32>, key: Option<&'static str>) -> ActivityInfo {
        ActivityInfo {
            name,
            module: "activity_defaults_api",
            default_retry_policy: None,
            default_start_to_close: None,
            default_heartbeat_timeout: None,
            default_schedule_to_start: None,
            default_schedule_to_close: None,
            default_queue: None,
            max_concurrent: cap,
            concurrency_key: key,
            is_local: false,
            max_input_bytes: None,
            max_result_bytes: None,
            rate_limit_rps: None,
            rate_limit_burst: None,
            rate_limit_key: None,
            rate_limit_key_expr: None,
            circuit_breaker: None,
            requires: None,
            handler: |_ctx, input| Box::pin(async move { Ok(input) }),
            input_schema: None,
            output_schema: None,
        }
    }

    #[test]
    fn a_sharer_has_the_same_key_and_a_cap() {
        let all = [
            capped("sync_c", Some(2), Some("partner")),
            capped("sync_a", Some(2), Some("partner")),
            capped("sync_b", Some(2), Some("partner")),
            capped("own", Some(2), None),
            capped("named_like_own", Some(2), Some("own")),
            capped("plain", None, None),
        ];
        assert_eq!(key_sharer(&all, "sync_c"), Some("sync_a"));
        assert_eq!(key_sharer(&all, "sync_a"), Some("sync_b"));
        assert_eq!(
            key_sharer(&all, "own"),
            Some("named_like_own"),
            "a default key is the activity name"
        );
        assert_eq!(key_sharer(&all, "plain"), None, "no cap, no key");
        let mut local = capped("inline", Some(2), Some("partner_local"));
        local.is_local = true;
        let remote = capped("remote", Some(2), Some("partner_local"));
        let pair = [local, remote];
        assert_eq!(
            key_sharer(&pair, "remote"),
            None,
            "a local activity has no task row, so it shares no key"
        );
        assert_eq!(key_sharer(&all, "unknown"), None);
        assert_eq!(key_sharer(&all[3..4], "own"), None);
    }
}
