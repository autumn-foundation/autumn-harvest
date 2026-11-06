//! Operator overrides for the defaults of an activity type.
//!
//! [`ActivityInfo`] declares five defaults in code: the retry policy,
//! `start_to_close`, `heartbeat_timeout`, `schedule_to_start` and
//! `schedule_to_close`. An override changes them at runtime, keyed by the
//! activity name. The precedence is fixed:
//!
//! > call site, then override, then code, then builder default.
//!
//! A field that the override does not set inherits the code value.
//!
//! ## Where an override applies
//!
//! The overrides live in `harvest_activity_default_overrides` on the default
//! shard. The [`RunDefaultsCache`](crate::run_defaults::RunDefaultsCache) of
//! each registry holds them, next to the workflow overrides. Each refresh
//! loop of that cache loads both tables. So a change reaches new schedules
//! within the TTL plus one loop period.
//!
//! The override applies at schedule time only:
//!
//! - A typed call in workflow code resolves its retry policy and its
//!   `start_to_close` through the context. See
//!   [`WorkflowContext::with_activity_overrides`](crate::context::WorkflowContext::with_activity_overrides).
//! - The worker resolves the other values when it stores the task row. It
//!   also resolves a value that the command does not carry.
//! - A local activity resolves in the context. The worker freezes the result
//!   in `LocalActivityScheduled`.
//!
//! ## Activities already scheduled
//!
//! The task row stores the resolved values, and a retry reuses the row. So
//! a scheduled activity keeps its values. `ActivityScheduled` stores none of
//! them, so replay never compares them. A recovered local activity reads the
//! values that its event froze.
//!
//! ## What an override cannot add
//!
//! A retry field applies only over a code retry policy. An activity whose
//! author declared no policy is not known to be safe to retry. A heartbeat
//! timeout applies only over a code heartbeat timeout. An activity that does
//! not heartbeat would time out on each attempt.

use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::info::ActivityInfo;
use crate::policy::RetryPolicy;
use crate::workflow_retry::{RetryLimits, RetryLimitsPatch};

/// The longest timeout that an override can set: 100 years.
///
/// A schedule adds `schedule_to_close` to its time. A sum past the range of
/// `chrono` panics, so the write refuses a value above this bound.
pub const MAX_ACTIVITY_TIMEOUT: Duration = crate::run_defaults::MAX_RUN_DEFAULT;

/// The five schedule-time values of one activity.
///
/// `None` sets no value.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ActivityDefaults {
    /// The retry policy.
    pub retry_policy: Option<RetryPolicy>,
    /// The bound on one attempt.
    pub start_to_close: Option<Duration>,
    /// The bound between two heartbeats.
    pub heartbeat_timeout: Option<Duration>,
    /// The bound on the wait for a worker.
    pub schedule_to_start: Option<Duration>,
    /// The bound on every attempt and every backoff together.
    pub schedule_to_close: Option<Duration>,
}

impl ActivityDefaults {
    /// The code values of `info`.
    #[must_use]
    pub fn of(info: &ActivityInfo) -> Self {
        Self {
            retry_policy: info.default_retry_policy.clone(),
            start_to_close: info.default_start_to_close,
            heartbeat_timeout: info.default_heartbeat_timeout,
            schedule_to_start: info.default_schedule_to_start,
            schedule_to_close: info.default_schedule_to_close,
        }
    }

    /// Whether these values bound a running attempt.
    ///
    /// A `start_to_close`, a `schedule_to_close` or a `heartbeat_timeout` is
    /// such a bound. The builder default `start_to_close` applies only when
    /// none is set.
    #[must_use]
    pub const fn declares_attempt_bound(&self) -> bool {
        self.start_to_close.is_some()
            || self.schedule_to_close.is_some()
            || self.heartbeat_timeout.is_some()
    }
}

/// The fields that an override can set.
///
/// `None` inherits the code value.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ActivityDefaultFields {
    /// The retry fields.
    pub retry: RetryLimits,
    /// The bound on one attempt.
    pub start_to_close: Option<Duration>,
    /// The bound between two heartbeats.
    pub heartbeat_timeout: Option<Duration>,
    /// The bound on the wait for a worker.
    pub schedule_to_start: Option<Duration>,
    /// The bound on every attempt and every backoff together.
    pub schedule_to_close: Option<Duration>,
}

impl ActivityDefaultFields {
    /// Whether the override sets no field.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.retry.is_empty()
            && self.start_to_close.is_none()
            && self.heartbeat_timeout.is_none()
            && self.schedule_to_start.is_none()
            && self.schedule_to_close.is_none()
    }

    /// Apply this override to the code values of one activity.
    ///
    /// A set field replaces the code value. A retry field applies only over
    /// a code policy. A heartbeat timeout applies only over a code heartbeat
    /// timeout.
    #[must_use]
    pub fn apply(&self, code: ActivityDefaults) -> ActivityDefaults {
        ActivityDefaults {
            retry_policy: code.retry_policy.map(|policy| self.retry.apply(policy)),
            start_to_close: self.start_to_close.or(code.start_to_close),
            heartbeat_timeout: code
                .heartbeat_timeout
                .map(|declared| self.heartbeat_timeout.unwrap_or(declared)),
            schedule_to_start: self.schedule_to_start.or(code.schedule_to_start),
            schedule_to_close: self.schedule_to_close.or(code.schedule_to_close),
        }
    }
}

/// A partial change to a stored override.
///
/// Each field is tri-state. The outer `None` keeps the stored value.
/// `Some(None)` returns the field to the code value. `Some(Some(v))` sets
/// `v`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ActivityDefaultPatch {
    /// The retry fields.
    pub retry: RetryLimitsPatch,
    /// The bound on one attempt.
    pub start_to_close: Option<Option<Duration>>,
    /// The bound between two heartbeats.
    pub heartbeat_timeout: Option<Option<Duration>>,
    /// The bound on the wait for a worker.
    pub schedule_to_start: Option<Option<Duration>>,
    /// The bound on every attempt and every backoff together.
    pub schedule_to_close: Option<Option<Duration>>,
}

impl ActivityDefaultPatch {
    /// Merge this patch over the stored fields.
    #[must_use]
    pub fn merge(&self, stored: ActivityDefaultFields) -> ActivityDefaultFields {
        ActivityDefaultFields {
            retry: self.retry.merge(stored.retry),
            start_to_close: self.start_to_close.unwrap_or(stored.start_to_close),
            heartbeat_timeout: self.heartbeat_timeout.unwrap_or(stored.heartbeat_timeout),
            schedule_to_start: self.schedule_to_start.unwrap_or(stored.schedule_to_start),
            schedule_to_close: self.schedule_to_close.unwrap_or(stored.schedule_to_close),
        }
    }

    /// Whether this patch names no field.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.retry.is_empty()
            && self.start_to_close.is_none()
            && self.heartbeat_timeout.is_none()
            && self.schedule_to_start.is_none()
            && self.schedule_to_close.is_none()
    }
}

/// One stored override.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivityDefaultOverride {
    /// The activity name.
    pub activity_name: String,
    /// The fields that the override sets.
    pub fields: ActivityDefaultFields,
    /// The operator's stated reason, if any.
    pub reason: Option<String>,
    /// The actor that last changed the override.
    pub updated_by: String,
    /// When the override was first stored.
    pub created_at: DateTime<Utc>,
    /// When the override last changed.
    pub updated_at: DateTime<Utc>,
}

#[cfg(feature = "db")]
pub use db::*;

#[cfg(feature = "db")]
mod db {
    use super::{
        ActivityDefaultFields, ActivityDefaultOverride, ActivityDefaultPatch, MAX_ACTIVITY_TIMEOUT,
    };
    use std::time::Duration;

    use chrono::{DateTime, Utc};
    use diesel::prelude::*;
    use diesel_async::{AsyncPgConnection, RunQueryDsl};

    use crate::error::{HarvestError, HarvestResult};
    use crate::run_defaults::{MAX_RETRY_INTERVAL, check_retry_limits, check_retry_patch};
    use crate::workflow_retry::RetryLimits;

    #[derive(Debug, Clone, Queryable, Selectable)]
    #[diesel(table_name = crate::schema::harvest_activity_default_overrides)]
    #[diesel(check_for_backend(diesel::pg::Pg))]
    struct OverrideRow {
        activity_name: String,
        retry_max_attempts: Option<i32>,
        retry_initial_interval_ms: Option<i64>,
        retry_max_interval_ms: Option<i64>,
        start_to_close_ms: Option<i64>,
        heartbeat_timeout_ms: Option<i64>,
        schedule_to_start_ms: Option<i64>,
        schedule_to_close_ms: Option<i64>,
        reason: Option<String>,
        updated_by: String,
        created_at: DateTime<Utc>,
        updated_at: DateTime<Utc>,
    }

    impl OverrideRow {
        /// Decode a stored row. A value out of its bounds is an error, not an
        /// inherit.
        fn decode(self) -> HarvestResult<ActivityDefaultOverride> {
            let unreadable = |field: &str, n: i64| {
                HarvestError::Config(format!(
                    "activity default override for '{}' holds an unreadable {field} ({n})",
                    self.activity_name
                ))
            };
            let millis = |field: &str, value: Option<i64>, max: Duration| {
                value
                    .map(|n| {
                        u64::try_from(n)
                            .ok()
                            .map(Duration::from_millis)
                            .filter(|d| !d.is_zero() && *d <= max)
                            .ok_or_else(|| unreadable(field, n))
                    })
                    .transpose()
            };
            let fields = ActivityDefaultFields {
                retry: RetryLimits {
                    max_attempts: self
                        .retry_max_attempts
                        .map(|n| {
                            u32::try_from(n)
                                .ok()
                                .filter(|n| *n > 0)
                                .ok_or_else(|| unreadable("retry_max_attempts", i64::from(n)))
                        })
                        .transpose()?,
                    initial_interval: millis(
                        "retry_initial_interval_ms",
                        self.retry_initial_interval_ms,
                        MAX_RETRY_INTERVAL,
                    )?,
                    max_interval: millis(
                        "retry_max_interval_ms",
                        self.retry_max_interval_ms,
                        MAX_RETRY_INTERVAL,
                    )?,
                },
                start_to_close: millis(
                    "start_to_close_ms",
                    self.start_to_close_ms,
                    MAX_ACTIVITY_TIMEOUT,
                )?,
                heartbeat_timeout: millis(
                    "heartbeat_timeout_ms",
                    self.heartbeat_timeout_ms,
                    MAX_ACTIVITY_TIMEOUT,
                )?,
                schedule_to_start: millis(
                    "schedule_to_start_ms",
                    self.schedule_to_start_ms,
                    MAX_ACTIVITY_TIMEOUT,
                )?,
                schedule_to_close: millis(
                    "schedule_to_close_ms",
                    self.schedule_to_close_ms,
                    MAX_ACTIVITY_TIMEOUT,
                )?,
            };
            Ok(ActivityDefaultOverride {
                activity_name: self.activity_name,
                fields,
                reason: self.reason,
                updated_by: self.updated_by,
                created_at: self.created_at,
                updated_at: self.updated_at,
            })
        }
    }

    fn to_millis(value: Duration) -> i64 {
        i64::try_from(value.as_millis()).unwrap_or(i64::MAX)
    }

    /// Load every activity override. Call it on the default shard.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Database`] when the read fails, and
    /// [`HarvestError::Config`] when a stored row does not decode.
    pub async fn load_activity_default_overrides(
        conn: &mut AsyncPgConnection,
    ) -> HarvestResult<Vec<ActivityDefaultOverride>> {
        use crate::schema::harvest_activity_default_overrides::dsl;

        let rows: Vec<OverrideRow> = dsl::harvest_activity_default_overrides
            .select(OverrideRow::as_select())
            .load(conn)
            .await
            .map_err(crate::error::database_error)?;
        rows.into_iter().map(OverrideRow::decode).collect()
    }

    /// Load the override of one activity, if any. Call it on the default
    /// shard.
    ///
    /// # Errors
    ///
    /// As [`load_activity_default_overrides`].
    pub async fn get_activity_default_override(
        conn: &mut AsyncPgConnection,
        activity_name: &str,
    ) -> HarvestResult<Option<ActivityDefaultOverride>> {
        select_override(conn, activity_name, false).await
    }

    async fn select_override(
        conn: &mut AsyncPgConnection,
        activity_name: &str,
        for_update: bool,
    ) -> HarvestResult<Option<ActivityDefaultOverride>> {
        use crate::schema::harvest_activity_default_overrides::dsl;

        let query = dsl::harvest_activity_default_overrides
            .find(activity_name)
            .select(OverrideRow::as_select());
        let row: Option<OverrideRow> = if for_update {
            query.for_update().first(conn).await
        } else {
            query.first(conn).await
        }
        .optional()
        .map_err(crate::error::database_error)?;
        row.map(OverrideRow::decode).transpose()
    }

    /// Take the transaction lock of one activity.
    ///
    /// A first override has no row for `FOR UPDATE` to lock. This lock
    /// serializes two patches of the same activity in that case too.
    async fn lock_activity(conn: &mut AsyncPgConnection, activity_name: &str) -> HarvestResult<()> {
        diesel::sql_query("SELECT pg_advisory_xact_lock(hashtext($1)::int8)")
            .bind::<diesel::sql_types::Text, _>(format!(
                "harvest:activity_defaults:{activity_name}"
            ))
            .execute(conn)
            .await
            .map_err(crate::error::database_error)?;
        Ok(())
    }

    /// One change to the override of an activity.
    #[derive(Debug, Clone, Copy)]
    pub struct ActivityDefaultChange<'a> {
        /// The activity name.
        pub activity_name: &'a str,
        /// The fields to change.
        pub patch: ActivityDefaultPatch,
        /// Delete the stored override before `patch` merges.
        pub clear: bool,
        /// The actor that makes the change.
        pub actor: &'a str,
        /// The operator's stated reason, if any.
        pub reason: Option<&'a str>,
    }

    /// Apply a change to the override of one activity. Call it on the
    /// default shard.
    ///
    /// The write refuses a value that no schedule can use. See
    /// [`check_patch`]. A merge that sets no field deletes the row. Returns
    /// the stored override after the change.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Config`] for a refused value, and
    /// [`HarvestError::Database`] when a read or a write fails.
    pub async fn update_activity_default_override(
        conn: &mut AsyncPgConnection,
        change: &ActivityDefaultChange<'_>,
    ) -> HarvestResult<Option<ActivityDefaultOverride>> {
        use crate::schema::harvest_activity_default_overrides::dsl;
        use diesel_async::AsyncConnection;

        check_patch(&change.patch)?;
        let change = *change;
        Box::pin(conn.transaction(async |conn| {
            lock_activity(conn, change.activity_name).await?;
            let stored = select_override(conn, change.activity_name, true).await?;
            let base = if change.clear {
                ActivityDefaultFields::default()
            } else {
                stored.map(|value| value.fields).unwrap_or_default()
            };
            let merged = change.patch.merge(base);
            if merged.is_empty() {
                diesel::delete(dsl::harvest_activity_default_overrides.find(change.activity_name))
                    .execute(conn)
                    .await
                    .map_err(crate::error::database_error)?;
                return Ok(None);
            }
            check_retry_limits(merged.retry)?;
            let now = Utc::now();
            let retry_max_attempts = merged
                .retry
                .max_attempts
                .map(|n| i32::try_from(n).unwrap_or(i32::MAX));
            let retry_initial_interval_ms = merged.retry.initial_interval.map(to_millis);
            let retry_max_interval_ms = merged.retry.max_interval.map(to_millis);
            let start_to_close_ms = merged.start_to_close.map(to_millis);
            let heartbeat_timeout_ms = merged.heartbeat_timeout.map(to_millis);
            let schedule_to_start_ms = merged.schedule_to_start.map(to_millis);
            let schedule_to_close_ms = merged.schedule_to_close.map(to_millis);
            diesel::insert_into(dsl::harvest_activity_default_overrides)
                .values((
                    dsl::activity_name.eq(change.activity_name),
                    dsl::retry_max_attempts.eq(retry_max_attempts),
                    dsl::retry_initial_interval_ms.eq(retry_initial_interval_ms),
                    dsl::retry_max_interval_ms.eq(retry_max_interval_ms),
                    dsl::start_to_close_ms.eq(start_to_close_ms),
                    dsl::heartbeat_timeout_ms.eq(heartbeat_timeout_ms),
                    dsl::schedule_to_start_ms.eq(schedule_to_start_ms),
                    dsl::schedule_to_close_ms.eq(schedule_to_close_ms),
                    dsl::reason.eq(change.reason),
                    dsl::updated_by.eq(change.actor),
                    dsl::created_at.eq(now),
                    dsl::updated_at.eq(now),
                ))
                .on_conflict(dsl::activity_name)
                .do_update()
                .set((
                    dsl::retry_max_attempts.eq(retry_max_attempts),
                    dsl::retry_initial_interval_ms.eq(retry_initial_interval_ms),
                    dsl::retry_max_interval_ms.eq(retry_max_interval_ms),
                    dsl::start_to_close_ms.eq(start_to_close_ms),
                    dsl::heartbeat_timeout_ms.eq(heartbeat_timeout_ms),
                    dsl::schedule_to_start_ms.eq(schedule_to_start_ms),
                    dsl::schedule_to_close_ms.eq(schedule_to_close_ms),
                    dsl::reason.eq(change.reason),
                    dsl::updated_by.eq(change.actor),
                    dsl::updated_at.eq(now),
                ))
                .execute(conn)
                .await
                .map_err(crate::error::database_error)?;
            select_override(conn, change.activity_name, false).await
        }))
        .await
    }

    /// Refuse a value that no schedule can use.
    ///
    /// The columns hold whole milliseconds. So the write refuses a timeout
    /// under 1 ms and one with a part of a millisecond. A zero timeout ends
    /// each attempt at once. A timeout over [`MAX_ACTIVITY_TIMEOUT`] can pass
    /// the range of `chrono` when a schedule adds it to its time. The retry
    /// fields follow the rules of the workflow retry override.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Config`] that names the refused field.
    pub fn check_patch(patch: &ActivityDefaultPatch) -> HarvestResult<()> {
        for (field, value) in [
            ("start_to_close_ms", patch.start_to_close),
            ("heartbeat_timeout_ms", patch.heartbeat_timeout),
            ("schedule_to_start_ms", patch.schedule_to_start),
            ("schedule_to_close_ms", patch.schedule_to_close),
        ] {
            let Some(value) = value.flatten() else {
                continue;
            };
            if value.as_millis() == 0 {
                return Err(HarvestError::Config(format!("{field} must be at least 1")));
            }
            if value.subsec_nanos() % 1_000_000 != 0 {
                return Err(HarvestError::Config(format!(
                    "{field} must be a whole number of milliseconds"
                )));
            }
            if value > MAX_ACTIVITY_TIMEOUT {
                return Err(HarvestError::Config(format!(
                    "{field} must be at most {} (100 years)",
                    MAX_ACTIVITY_TIMEOUT.as_millis()
                )));
            }
        }
        check_retry_patch(&patch.retry)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::JitterPolicy;

    const fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    fn code_policy() -> RetryPolicy {
        RetryPolicy {
            max_attempts: 5,
            initial_interval: Duration::from_millis(200),
            backoff_coefficient: 2.0,
            max_interval: secs(10),
            non_retryable_errors: vec!["Fatal".to_string()],
            jitter: JitterPolicy::None,
        }
    }

    fn code() -> ActivityDefaults {
        ActivityDefaults {
            retry_policy: Some(code_policy()),
            start_to_close: Some(secs(30)),
            heartbeat_timeout: Some(secs(5)),
            schedule_to_start: Some(secs(60)),
            schedule_to_close: Some(secs(600)),
        }
    }

    fn bare(name: &'static str) -> ActivityInfo {
        ActivityInfo {
            name,
            module: "billing",
            default_retry_policy: None,
            default_start_to_close: None,
            default_heartbeat_timeout: None,
            default_schedule_to_start: None,
            default_schedule_to_close: None,
            default_queue: None,
            max_concurrent: None,
            concurrency_key: None,
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

    fn info() -> ActivityInfo {
        let mut info = bare("charge");
        info.default_retry_policy = Some(code_policy());
        info.default_start_to_close = Some(secs(30));
        info.default_heartbeat_timeout = Some(secs(5));
        info.default_schedule_to_start = Some(secs(60));
        info.default_schedule_to_close = Some(secs(600));
        info
    }

    #[test]
    fn of_reads_the_five_code_values() {
        assert_eq!(ActivityDefaults::of(&info()), code());
    }

    #[test]
    fn an_empty_override_returns_the_code_values() {
        let fields = ActivityDefaultFields::default();
        assert!(fields.is_empty());
        assert_eq!(fields.apply(code()), code());
    }

    #[test]
    fn each_set_timeout_replaces_its_code_value() {
        let fields = ActivityDefaultFields {
            start_to_close: Some(secs(90)),
            heartbeat_timeout: Some(secs(15)),
            schedule_to_start: Some(secs(120)),
            schedule_to_close: Some(secs(1_800)),
            ..ActivityDefaultFields::default()
        };
        assert!(!fields.is_empty());
        let effective = fields.apply(code());
        assert_eq!(effective.start_to_close, Some(secs(90)));
        assert_eq!(effective.heartbeat_timeout, Some(secs(15)));
        assert_eq!(effective.schedule_to_start, Some(secs(120)));
        assert_eq!(effective.schedule_to_close, Some(secs(1_800)));
        assert_eq!(effective.retry_policy, Some(code_policy()));
    }

    #[test]
    fn an_unset_field_inherits_the_code_value() {
        let fields = ActivityDefaultFields {
            start_to_close: Some(secs(90)),
            ..ActivityDefaultFields::default()
        };
        let effective = fields.apply(code());
        assert_eq!(effective.start_to_close, Some(secs(90)));
        assert_eq!(effective.schedule_to_close, Some(secs(600)));
        assert_eq!(effective.heartbeat_timeout, Some(secs(5)));
    }

    #[test]
    fn a_retry_override_replaces_three_fields_of_the_code_policy() {
        let fields = ActivityDefaultFields {
            retry: RetryLimits {
                max_attempts: Some(1),
                initial_interval: Some(secs(2)),
                max_interval: None,
            },
            ..ActivityDefaultFields::default()
        };
        let policy = fields.apply(code()).retry_policy.expect("policy");
        assert_eq!(policy.max_attempts, 1);
        assert_eq!(policy.initial_interval, secs(2));
        assert_eq!(policy.max_interval, secs(10));
        assert!(
            (policy.backoff_coefficient - 2.0).abs() < f64::EPSILON,
            "the coefficient stays as declared"
        );
        assert_eq!(policy.non_retryable_errors, vec!["Fatal".to_string()]);
    }

    #[test]
    fn a_timeout_override_can_set_a_value_that_code_leaves_unset() {
        let fields = ActivityDefaultFields {
            start_to_close: Some(secs(90)),
            schedule_to_start: Some(secs(120)),
            schedule_to_close: Some(secs(1_800)),
            ..ActivityDefaultFields::default()
        };
        let effective = fields.apply(ActivityDefaults::default());
        assert_eq!(effective.start_to_close, Some(secs(90)));
        assert_eq!(effective.schedule_to_start, Some(secs(120)));
        assert_eq!(effective.schedule_to_close, Some(secs(1_800)));
    }

    /// An activity that does not heartbeat would time out on each attempt.
    #[test]
    fn a_heartbeat_override_never_adds_a_heartbeat_timeout() {
        let fields = ActivityDefaultFields {
            heartbeat_timeout: Some(secs(15)),
            ..ActivityDefaultFields::default()
        };
        assert_eq!(
            fields.apply(ActivityDefaults::default()).heartbeat_timeout,
            None
        );
    }

    /// An activity with no code policy is not known to be safe to retry.
    #[test]
    fn a_retry_override_never_adds_a_retry_policy() {
        let fields = ActivityDefaultFields {
            retry: RetryLimits {
                max_attempts: Some(7),
                ..RetryLimits::default()
            },
            ..ActivityDefaultFields::default()
        };
        assert_eq!(fields.apply(ActivityDefaults::default()).retry_policy, None);
    }

    #[test]
    fn an_override_bound_counts_as_an_attempt_bound() {
        assert!(!ActivityDefaults::default().declares_attempt_bound());
        let fields = ActivityDefaultFields {
            schedule_to_close: Some(secs(60)),
            ..ActivityDefaultFields::default()
        };
        assert!(
            fields
                .apply(ActivityDefaults::default())
                .declares_attempt_bound()
        );
        let with_stc = ActivityDefaults {
            start_to_close: Some(secs(1)),
            ..ActivityDefaults::default()
        };
        assert!(with_stc.declares_attempt_bound());
        let with_heartbeat = ActivityDefaults {
            heartbeat_timeout: Some(secs(1)),
            ..ActivityDefaults::default()
        };
        assert!(with_heartbeat.declares_attempt_bound());
    }

    #[test]
    fn of_agrees_with_the_info_attempt_bound() {
        let bare = bare("bare");
        assert_eq!(
            ActivityDefaults::of(&bare).declares_attempt_bound(),
            bare.declares_attempt_bound()
        );
        let full = info();
        assert_eq!(
            ActivityDefaults::of(&full).declares_attempt_bound(),
            full.declares_attempt_bound()
        );
    }

    #[test]
    fn patch_merge_keeps_the_fields_it_does_not_name() {
        let stored = ActivityDefaultFields {
            start_to_close: Some(secs(90)),
            schedule_to_close: Some(secs(900)),
            retry: RetryLimits {
                max_attempts: Some(2),
                ..RetryLimits::default()
            },
            ..ActivityDefaultFields::default()
        };
        let clear_stc = ActivityDefaultPatch {
            start_to_close: Some(None),
            ..ActivityDefaultPatch::default()
        };
        assert!(!clear_stc.is_empty());
        assert_eq!(
            clear_stc.merge(stored),
            ActivityDefaultFields {
                start_to_close: None,
                ..stored
            }
        );
        let set_heartbeat = ActivityDefaultPatch {
            heartbeat_timeout: Some(Some(secs(3))),
            schedule_to_start: Some(Some(secs(4))),
            retry: RetryLimitsPatch {
                max_attempts: Some(None),
                ..RetryLimitsPatch::default()
            },
            ..ActivityDefaultPatch::default()
        };
        assert_eq!(
            set_heartbeat.merge(stored),
            ActivityDefaultFields {
                heartbeat_timeout: Some(secs(3)),
                schedule_to_start: Some(secs(4)),
                retry: RetryLimits::default(),
                ..stored
            }
        );
        assert!(ActivityDefaultPatch::default().is_empty());
    }

    #[test]
    fn each_single_field_override_is_not_empty() {
        let one = |fields: ActivityDefaultFields| assert!(!fields.is_empty(), "{fields:?}");
        one(ActivityDefaultFields {
            heartbeat_timeout: Some(secs(1)),
            ..ActivityDefaultFields::default()
        });
        one(ActivityDefaultFields {
            schedule_to_start: Some(secs(1)),
            ..ActivityDefaultFields::default()
        });
        one(ActivityDefaultFields {
            schedule_to_close: Some(secs(1)),
            ..ActivityDefaultFields::default()
        });
        one(ActivityDefaultFields {
            retry: RetryLimits {
                max_interval: Some(secs(1)),
                ..RetryLimits::default()
            },
            ..ActivityDefaultFields::default()
        });
    }
}
