//! Operator overrides for per-type run defaults (issues #2387, #2406, #2407
//! and #2408).
//!
//! [`WorkflowInfo::sla`](crate::info::WorkflowInfo::sla),
//! [`WorkflowInfo::execution_timeout`](crate::info::WorkflowInfo::execution_timeout),
//! [`WorkflowInfo::chain_execution_timeout`](crate::info::WorkflowInfo::chain_execution_timeout)
//! the `limit` of
//! [`WorkflowInfo::concurrency`](crate::info::WorkflowInfo::concurrency) and
//! the admission caps of [`WorkflowInfo::quota`](crate::info::WorkflowInfo::quota)
//! are declared in code. A DAG lowers to a shadow workflow type of the same
//! name, so its `DagInfo` values arrive here too. Each start path resolves the
//! deadlines of a new run from them, so a change needed a restart.
//!
//! An override is a persisted operator value for one or more fields, keyed by
//! the workflow type name. The precedence is fixed:
//!
//! > override, then code.
//!
//! A field that the override does not set inherits the code value.
//!
//! ## Where an override applies
//!
//! The overrides live in `harvest_run_default_overrides` on the default
//! shard, as admission gates do. Each [`HandlerRegistry`](crate::worker::HandlerRegistry)
//! holds a [`RunDefaultsCache`]. A start path reads the cache and never sends
//! a query for it. A refresh loop reloads the cache when it is older than
//! [`RUN_DEFAULTS_TTL`]. The scheduler tick and the plugin's gate refresh
//! loop both run it. So a change reaches new starts within the TTL plus one
//! loop period.
//!
//! The existing clamps still apply after the override. An `sla` above the
//! effective `execution_timeout` is clamped to it, and the fleet-wide ceiling
//! still caps `execution_timeout`. The fleet chain ceiling
//! `max_workflow_chain_timeout` still caps `chain_execution_timeout`.
//!
//! ## Runs already in flight
//!
//! A change applies to new starts only. A continue-as-new successor of the
//! same type keeps the values of its run, as before. Each successor copies
//! the stored `chain_deadline_at` of its chain, for a cross-type successor
//! too. So a chain in flight keeps its chain deadline. The opt-in
//! [`recompute_in_flight_sla`] moves the SLA deadline of the non-terminal
//! runs of a type that have not breached yet. It never changes a hard
//! `deadline_at`, because a shorter one could end a run at once.
//!
//! ## Concurrency limit
//!
//! An override of the concurrency limit keeps the declared key and
//! `on_conflict`. Each start stores its limit on its task row, and the claim
//! compares the running count of the key to that stored cap. So a lower limit
//! applies to the next start, and a run in flight keeps its cap.
//! [`raise_waiting_concurrency_caps`] lifts the stored cap of the live rows
//! after a raise, so the next claim admits the waiting starts.
//!
//! A latest-wins policy (`cancel_running`) keeps its code limit. Its start
//! cancels runs down to the limit, so any later drop of an override would
//! cancel runs in flight.
//!
//! ## Quota limits
//!
//! An override of the quota caps (issue #2408) keeps the declared key and the
//! LLM caps. No row stores a quota cap. Each admission reads the caps again
//! through [`current_quota_policy`], inside `enforce_quota_admission`. So a
//! lower cap refuses the next admission over it, and a higher cap admits the
//! next one. Only admission reads a cap, so a cap never ends a run in flight.
//! A type with no admission cap keeps its code policy. See
//! [`QuotaLimits::apply`].
//!
//! ## Failure mode
//!
//! A refresh that fails keeps the last snapshot, and the next tick retries.
//! A process that never loaded the table uses the code values. This is the
//! same posture as a missed SLA: an SLA only reports a breach, and it never
//! ends a run.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};

use crate::concurrency::ConcurrencyPolicy;
use crate::quota::{QuotaLimits, QuotaLimitsPatch, QuotaPolicy};

/// The maximum age of a loaded snapshot before a refresh loop reloads it.
pub const RUN_DEFAULTS_TTL: Duration = Duration::from_secs(5);

/// The longest duration that an override can set: 100 years.
///
/// A start adds the duration to its start time. A sum past the range of
/// `chrono` panics, and a larger value fails its conversion and drops the
/// deadline. So the write refuses a value above this bound.
pub const MAX_RUN_DEFAULT: Duration = Duration::from_hours(876_000);

/// The largest concurrency limit that an override can set.
///
/// A task row stores its cap in an `INTEGER` column, so a larger value does
/// not fit.
pub const MAX_CONCURRENCY_LIMIT: u32 = i32::MAX.unsigned_abs();

/// The largest `max_history_bytes` that an override can set (issue #2408).
///
/// The override column is a `BIGINT`, so a larger value does not fit.
pub const MAX_QUOTA_HISTORY_BYTES: u64 = i64::MAX.unsigned_abs();

/// The run-default fields that an override can set.
///
/// `None` inherits the code value.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RunDefaultFields {
    /// The hard execution timeout of a new run.
    pub execution_timeout: Option<Duration>,
    /// The soft SLA of a new run.
    pub sla: Option<Duration>,
    /// The cap of a new continue-as-new chain (issue #2406).
    pub chain_execution_timeout: Option<Duration>,
    /// The concurrency limit of a new start (issue #2407).
    pub concurrency_limit: Option<u32>,
    /// The quota admission caps of a new admission (issue #2408).
    pub quota: QuotaLimits,
}

impl RunDefaultFields {
    /// Whether the override sets no field.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.execution_timeout.is_none()
            && self.sla.is_none()
            && self.chain_execution_timeout.is_none()
            && self.concurrency_limit.is_none()
            && self.quota.is_empty()
    }

    /// Apply this override to the code quota policy of one type (issue
    /// #2408).
    ///
    /// A type with no code policy gets none. See [`QuotaLimits::apply`].
    #[must_use]
    pub const fn apply_quota(&self, policy: Option<QuotaPolicy>) -> Option<QuotaPolicy> {
        match policy {
            Some(policy) => Some(self.quota.apply(policy)),
            None => None,
        }
    }

    /// Apply this override to the code concurrency policy of one type
    /// (issue #2407).
    ///
    /// The result keeps the code key and `on_conflict`. A type with no code
    /// policy gets none. A latest-wins policy keeps its code limit, because a
    /// later drop of the override would cancel runs in flight.
    #[must_use]
    pub fn apply_concurrency(
        &self,
        policy: Option<ConcurrencyPolicy>,
    ) -> Option<ConcurrencyPolicy> {
        let mut policy = policy?;
        if let Some(limit) = self.concurrency_limit
            && !policy.on_conflict.is_cancel_running()
        {
            policy.limit = limit;
        }
        Some(policy)
    }

    /// Apply this override to the code chain cap of one type.
    #[must_use]
    pub fn apply_chain(&self, chain_execution_timeout: Option<Duration>) -> Option<Duration> {
        self.chain_execution_timeout.or(chain_execution_timeout)
    }

    /// Apply this override to the code values of one type.
    ///
    /// Returns `(execution_timeout, sla)`.
    #[must_use]
    pub fn apply(
        &self,
        execution_timeout: Option<Duration>,
        sla: Option<Duration>,
    ) -> (Option<Duration>, Option<Duration>) {
        (
            self.execution_timeout.or(execution_timeout),
            self.sla.or(sla),
        )
    }
}

/// A partial change to a stored override.
///
/// Each field is tri-state. The outer `None` keeps the stored value.
/// `Some(None)` returns the field to the code value. `Some(Some(v))` sets
/// `v`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RunDefaultPatch {
    /// The hard execution timeout.
    pub execution_timeout: Option<Option<Duration>>,
    /// The soft SLA.
    pub sla: Option<Option<Duration>>,
    /// The chain cap.
    pub chain_execution_timeout: Option<Option<Duration>>,
    /// The concurrency limit (issue #2407).
    pub concurrency_limit: Option<Option<u32>>,
    /// The quota admission caps (issue #2408).
    pub quota: QuotaLimitsPatch,
}

impl RunDefaultPatch {
    /// Merge this patch over the stored fields.
    #[must_use]
    pub fn merge(&self, stored: RunDefaultFields) -> RunDefaultFields {
        RunDefaultFields {
            execution_timeout: self.execution_timeout.unwrap_or(stored.execution_timeout),
            sla: self.sla.unwrap_or(stored.sla),
            chain_execution_timeout: self
                .chain_execution_timeout
                .unwrap_or(stored.chain_execution_timeout),
            concurrency_limit: self.concurrency_limit.unwrap_or(stored.concurrency_limit),
            quota: self.quota.merge(stored.quota),
        }
    }
}

/// One stored override.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunDefaultOverride {
    /// The workflow type name. A DAG uses its own name.
    pub workflow_name: String,
    /// The fields that the override sets.
    pub fields: RunDefaultFields,
    /// The operator's stated reason, if any.
    pub reason: Option<String>,
    /// The actor that last changed the override.
    pub updated_by: String,
    /// When the override was first stored.
    pub created_at: DateTime<Utc>,
    /// When the override last changed.
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Default)]
struct Snapshot {
    loaded_at: Option<Instant>,
    overrides: Arc<HashMap<String, RunDefaultFields>>,
}

/// The in-process copy of the overrides.
///
/// A start path reads it with [`Self::apply`], which takes a read lock and
/// sends no query. A refresh loop replaces the whole map.
#[derive(Debug, Default)]
pub struct RunDefaultsCache {
    snapshot: RwLock<Snapshot>,
}

impl RunDefaultsCache {
    /// An empty cache that has never loaded. Every type uses its code values.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The effective `(execution_timeout, sla)` of one type.
    ///
    /// `execution_timeout` and `sla` are the code values. A poisoned lock
    /// returns them unchanged.
    #[must_use]
    pub fn apply(
        &self,
        workflow_name: &str,
        execution_timeout: Option<Duration>,
        sla: Option<Duration>,
    ) -> (Option<Duration>, Option<Duration>) {
        let Ok(snapshot) = self.snapshot.read() else {
            return (execution_timeout, sla);
        };
        snapshot
            .overrides
            .get(workflow_name)
            .map_or((execution_timeout, sla), |fields| {
                fields.apply(execution_timeout, sla)
            })
    }

    /// The effective chain cap of one type (issue #2406).
    ///
    /// `chain_execution_timeout` is the code value. A poisoned lock returns
    /// it unchanged. The start applies the fleet chain ceiling after this.
    #[must_use]
    pub fn apply_chain(
        &self,
        workflow_name: &str,
        chain_execution_timeout: Option<Duration>,
    ) -> Option<Duration> {
        let Ok(snapshot) = self.snapshot.read() else {
            return chain_execution_timeout;
        };
        snapshot
            .overrides
            .get(workflow_name)
            .map_or(chain_execution_timeout, |fields| {
                fields.apply_chain(chain_execution_timeout)
            })
    }

    /// The effective concurrency policy of one type (issue #2407).
    ///
    /// `policy` is the code policy. A poisoned lock returns it unchanged. See
    /// [`RunDefaultFields::apply_concurrency`].
    #[must_use]
    pub fn apply_concurrency(
        &self,
        workflow_name: &str,
        policy: Option<ConcurrencyPolicy>,
    ) -> Option<ConcurrencyPolicy> {
        let Ok(snapshot) = self.snapshot.read() else {
            return policy;
        };
        snapshot
            .overrides
            .get(workflow_name)
            .map_or(policy, |fields| fields.apply_concurrency(policy))
    }

    /// The effective quota policy of one type (issue #2408).
    ///
    /// `policy` is the code policy. A poisoned lock returns it unchanged. See
    /// [`RunDefaultFields::apply_quota`].
    #[must_use]
    pub fn apply_quota(
        &self,
        workflow_name: &str,
        policy: Option<QuotaPolicy>,
    ) -> Option<QuotaPolicy> {
        let Ok(snapshot) = self.snapshot.read() else {
            return policy;
        };
        snapshot
            .overrides
            .get(workflow_name)
            .map_or(policy, |fields| fields.apply_quota(policy))
    }

    /// The override of one type in the current snapshot, if any.
    #[must_use]
    pub fn get(&self, workflow_name: &str) -> Option<RunDefaultFields> {
        self.snapshot
            .read()
            .ok()
            .and_then(|snapshot| snapshot.overrides.get(workflow_name).copied())
    }

    /// Replace the snapshot with `overrides`, loaded now.
    pub fn replace(&self, overrides: impl IntoIterator<Item = RunDefaultOverride>) {
        let map = overrides
            .into_iter()
            .map(|value| (value.workflow_name, value.fields))
            .collect();
        if let Ok(mut snapshot) = self.snapshot.write() {
            *snapshot = Snapshot {
                loaded_at: Some(Instant::now()),
                overrides: Arc::new(map),
            };
        }
    }

    /// Write one change through to the snapshot.
    ///
    /// The management API calls this after its write commits. So the process
    /// that served the change applies it at once, and the others within the
    /// TTL. Empty `fields` remove the entry.
    pub fn write_through(&self, workflow_name: &str, fields: RunDefaultFields) {
        if let Ok(mut snapshot) = self.snapshot.write() {
            let mut map = (*snapshot.overrides).clone();
            if fields.is_empty() {
                map.remove(workflow_name);
            } else {
                map.insert(workflow_name.to_string(), fields);
            }
            snapshot.overrides = Arc::new(map);
        }
    }

    /// Whether the snapshot is older than `ttl`, or was never loaded.
    #[must_use]
    pub fn is_stale(&self, ttl: Duration) -> bool {
        self.snapshot
            .read()
            .ok()
            .and_then(|snapshot| snapshot.loaded_at)
            .is_none_or(|loaded_at| loaded_at.elapsed() >= ttl)
    }
}

#[cfg(feature = "db")]
pub use db::*;

#[cfg(feature = "db")]
mod db {
    use super::{
        ConcurrencyPolicy, MAX_CONCURRENCY_LIMIT, MAX_QUOTA_HISTORY_BYTES, MAX_RUN_DEFAULT,
        QuotaLimits, QuotaLimitsPatch, QuotaPolicy, RUN_DEFAULTS_TTL, RunDefaultFields,
        RunDefaultOverride, RunDefaultPatch, RunDefaultsCache,
    };
    use std::time::Duration;

    use chrono::{DateTime, Utc};
    use diesel::prelude::*;
    use diesel_async::{AsyncPgConnection, RunQueryDsl};

    use crate::error::{HarvestError, HarvestResult};

    /// The number of runs that one recompute statement moves.
    ///
    /// A batch bounds the row locks that one statement holds on the hot
    /// executions table.
    pub const RECOMPUTE_BATCH: i64 = 500;

    #[derive(Debug, Clone, Queryable, Selectable)]
    #[diesel(table_name = crate::schema::harvest_run_default_overrides)]
    #[diesel(check_for_backend(diesel::pg::Pg))]
    struct OverrideRow {
        workflow_name: String,
        sla_secs: Option<i64>,
        execution_timeout_secs: Option<i64>,
        chain_execution_timeout_secs: Option<i64>,
        concurrency_limit: Option<i32>,
        quota_max_active_executions: Option<i64>,
        quota_max_history_bytes: Option<i64>,
        quota_max_dead_letters: Option<i64>,
        reason: Option<String>,
        updated_by: String,
        created_at: DateTime<Utc>,
        updated_at: DateTime<Utc>,
    }

    impl OverrideRow {
        /// Decode a stored row. A negative value is an error, not an inherit.
        fn decode(self) -> HarvestResult<RunDefaultOverride> {
            let secs = |field: &str, value: Option<i64>| -> HarvestResult<Option<Duration>> {
                value
                    .map(|n| {
                        u64::try_from(n).map(Duration::from_secs).map_err(|_| {
                            HarvestError::Config(format!(
                                "run-default override for '{}' holds an unreadable {field} ({n})",
                                self.workflow_name
                            ))
                        })
                    })
                    .transpose()
            };
            let unreadable = |field: &str, n: i64| {
                HarvestError::Config(format!(
                    "run-default override for '{}' holds an unreadable {field} ({n})",
                    self.workflow_name
                ))
            };
            let count = |field: &str, value: Option<i64>| -> HarvestResult<Option<u32>> {
                value
                    .map(|n| {
                        u32::try_from(n)
                            .ok()
                            .filter(|n| *n > 0)
                            .ok_or_else(|| unreadable(field, n))
                    })
                    .transpose()
            };
            let quota = QuotaLimits {
                max_active_executions: count(
                    "quota_max_active_executions",
                    self.quota_max_active_executions,
                )?,
                max_history_bytes: self
                    .quota_max_history_bytes
                    .map(|n| {
                        u64::try_from(n)
                            .ok()
                            .filter(|n| *n > 0)
                            .ok_or_else(|| unreadable("quota_max_history_bytes", n))
                    })
                    .transpose()?,
                max_dead_letters: count("quota_max_dead_letters", self.quota_max_dead_letters)?,
            };
            let fields = RunDefaultFields {
                execution_timeout: secs("execution_timeout_secs", self.execution_timeout_secs)?,
                sla: secs("sla_secs", self.sla_secs)?,
                chain_execution_timeout: secs(
                    "chain_execution_timeout_secs",
                    self.chain_execution_timeout_secs,
                )?,
                concurrency_limit: self
                    .concurrency_limit
                    .map(|n| {
                        u32::try_from(n).ok().filter(|n| *n > 0).ok_or_else(|| {
                            HarvestError::Config(format!(
                                "run-default override for '{}' holds an unreadable \
                                 concurrency_limit ({n})",
                                self.workflow_name
                            ))
                        })
                    })
                    .transpose()?,
                quota,
            };
            Ok(RunDefaultOverride {
                workflow_name: self.workflow_name,
                fields,
                reason: self.reason,
                updated_by: self.updated_by,
                created_at: self.created_at,
                updated_at: self.updated_at,
            })
        }
    }

    fn to_secs(value: Duration) -> i64 {
        i64::try_from(value.as_secs()).unwrap_or(i64::MAX)
    }

    /// Load every override. Call it on the default shard.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Database`] when the read fails, and
    /// [`HarvestError::Config`] when a stored row does not decode.
    pub async fn load_run_default_overrides(
        conn: &mut AsyncPgConnection,
    ) -> HarvestResult<Vec<RunDefaultOverride>> {
        use crate::schema::harvest_run_default_overrides::dsl;

        let rows: Vec<OverrideRow> = dsl::harvest_run_default_overrides
            .select(OverrideRow::as_select())
            .load(conn)
            .await
            .map_err(crate::error::database_error)?;
        rows.into_iter().map(OverrideRow::decode).collect()
    }

    /// Load the override of one type, if any. Call it on the default shard.
    ///
    /// # Errors
    ///
    /// As [`load_run_default_overrides`].
    pub async fn get_run_default_override(
        conn: &mut AsyncPgConnection,
        workflow_name: &str,
    ) -> HarvestResult<Option<RunDefaultOverride>> {
        select_override(conn, workflow_name, false).await
    }

    async fn select_override(
        conn: &mut AsyncPgConnection,
        workflow_name: &str,
        for_update: bool,
    ) -> HarvestResult<Option<RunDefaultOverride>> {
        use crate::schema::harvest_run_default_overrides::dsl;

        let query = dsl::harvest_run_default_overrides
            .find(workflow_name)
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

    /// One change to the override of a type.
    #[derive(Debug, Clone, Copy)]
    pub struct RunDefaultChange<'a> {
        /// The workflow type name.
        pub workflow_name: &'a str,
        /// The fields to change.
        pub patch: RunDefaultPatch,
        /// Delete the stored override before `patch` merges.
        pub clear: bool,
        /// The actor that makes the change.
        pub actor: &'a str,
        /// The operator's stated reason, if any.
        pub reason: Option<&'a str>,
    }

    /// Apply a change to the override of one type. Call it on the default
    /// shard.
    ///
    /// A zero duration is rejected, because a zero SLA breaches at once and a
    /// zero timeout ends the run or the chain at once. The write also refuses
    /// a zero concurrency limit or quota cap, because no start can then run.
    /// A merge that sets no field deletes the row. Returns the stored override
    /// after the change.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Config`] for a zero or out-of-range value, and
    /// [`HarvestError::Database`] when a read or write fails.
    pub async fn update_run_default_override(
        conn: &mut AsyncPgConnection,
        change: &RunDefaultChange<'_>,
    ) -> HarvestResult<Option<RunDefaultOverride>> {
        use crate::schema::harvest_run_default_overrides::dsl;
        use diesel_async::AsyncConnection;

        check_patch(&change.patch)?;
        let change = *change;
        Box::pin(conn.transaction(async |conn| {
            // A first override has no row for FOR UPDATE to lock. This lock
            // serializes two patches of the same type in that case too, so
            // neither one merges over a read that the other replaces.
            lock_type(conn, change.workflow_name).await?;
            let stored = select_override(conn, change.workflow_name, true).await?;
            let base = if change.clear {
                RunDefaultFields::default()
            } else {
                stored.map(|value| value.fields).unwrap_or_default()
            };
            let merged = change.patch.merge(base);
            if merged.is_empty() {
                diesel::delete(dsl::harvest_run_default_overrides.find(change.workflow_name))
                    .execute(conn)
                    .await
                    .map_err(crate::error::database_error)?;
                return Ok(None);
            }
            let now = Utc::now();
            let sla_secs = merged.sla.map(to_secs);
            let execution_timeout_secs = merged.execution_timeout.map(to_secs);
            let chain_execution_timeout_secs = merged.chain_execution_timeout.map(to_secs);
            let concurrency_limit = merged
                .concurrency_limit
                .map(|n| i32::try_from(n).unwrap_or(i32::MAX));
            let quota_max_active_executions = merged.quota.max_active_executions.map(i64::from);
            let quota_max_history_bytes = merged
                .quota
                .max_history_bytes
                .map(|n| i64::try_from(n).unwrap_or(i64::MAX));
            let quota_max_dead_letters = merged.quota.max_dead_letters.map(i64::from);
            diesel::insert_into(dsl::harvest_run_default_overrides)
                .values((
                    dsl::workflow_name.eq(change.workflow_name),
                    dsl::sla_secs.eq(sla_secs),
                    dsl::execution_timeout_secs.eq(execution_timeout_secs),
                    dsl::chain_execution_timeout_secs.eq(chain_execution_timeout_secs),
                    dsl::concurrency_limit.eq(concurrency_limit),
                    dsl::quota_max_active_executions.eq(quota_max_active_executions),
                    dsl::quota_max_history_bytes.eq(quota_max_history_bytes),
                    dsl::quota_max_dead_letters.eq(quota_max_dead_letters),
                    dsl::reason.eq(change.reason),
                    dsl::updated_by.eq(change.actor),
                    dsl::created_at.eq(now),
                    dsl::updated_at.eq(now),
                ))
                .on_conflict(dsl::workflow_name)
                .do_update()
                .set((
                    dsl::sla_secs.eq(sla_secs),
                    dsl::execution_timeout_secs.eq(execution_timeout_secs),
                    dsl::chain_execution_timeout_secs.eq(chain_execution_timeout_secs),
                    dsl::concurrency_limit.eq(concurrency_limit),
                    dsl::quota_max_active_executions.eq(quota_max_active_executions),
                    dsl::quota_max_history_bytes.eq(quota_max_history_bytes),
                    dsl::quota_max_dead_letters.eq(quota_max_dead_letters),
                    dsl::reason.eq(change.reason),
                    dsl::updated_by.eq(change.actor),
                    dsl::updated_at.eq(now),
                ))
                .execute(conn)
                .await
                .map_err(crate::error::database_error)?;
            select_override(conn, change.workflow_name, false).await
        }))
        .await
    }

    /// Refuse a value that no start can use.
    ///
    /// A zero duration is refused, because a zero SLA breaches at once and a
    /// zero timeout ends the run or the chain at once. A zero concurrency
    /// limit or quota cap is refused, because no start can then run.
    fn check_patch(patch: &RunDefaultPatch) -> HarvestResult<()> {
        for (field, value) in [
            ("execution_timeout", patch.execution_timeout),
            ("sla", patch.sla),
            ("chain_execution_timeout", patch.chain_execution_timeout),
        ] {
            if value.flatten().is_some_and(|d| d.as_secs() == 0) {
                return Err(HarvestError::Config(format!(
                    "{field} must be at least one second"
                )));
            }
            if value.flatten().is_some_and(|d| d > MAX_RUN_DEFAULT) {
                return Err(HarvestError::Config(format!(
                    "{field} must be at most {} seconds (100 years)",
                    MAX_RUN_DEFAULT.as_secs()
                )));
            }
        }
        match patch.concurrency_limit.flatten() {
            Some(0) => {
                return Err(HarvestError::Config(
                    "concurrency_limit must be at least 1".to_string(),
                ));
            }
            Some(limit) if limit > MAX_CONCURRENCY_LIMIT => {
                return Err(HarvestError::Config(format!(
                    "concurrency_limit must be at most {MAX_CONCURRENCY_LIMIT}"
                )));
            }
            _ => {}
        }
        check_quota_patch(&patch.quota)
    }

    /// Refuse a quota cap of zero, or one past its column (issue #2408).
    ///
    /// A zero cap refuses every admission of the type. A pause does that
    /// already, so the write refuses it, as for the concurrency limit.
    fn check_quota_patch(quota: &QuotaLimitsPatch) -> HarvestResult<()> {
        for (field, value) in [
            (
                "quota_max_active_executions",
                quota.max_active_executions.flatten().map(u64::from),
            ),
            ("quota_max_history_bytes", quota.max_history_bytes.flatten()),
            (
                "quota_max_dead_letters",
                quota.max_dead_letters.flatten().map(u64::from),
            ),
        ] {
            if value == Some(0) {
                return Err(HarvestError::Config(format!("{field} must be at least 1")));
            }
        }
        if quota
            .max_history_bytes
            .flatten()
            .is_some_and(|n| n > MAX_QUOTA_HISTORY_BYTES)
        {
            return Err(HarvestError::Config(format!(
                "quota_max_history_bytes must be at most {MAX_QUOTA_HISTORY_BYTES}"
            )));
        }
        Ok(())
    }

    /// The effective quota policy of one type, from the process cache (issue
    /// #2408).
    ///
    /// `policy` is the code policy, or the caps that a cross-shard child spec
    /// carries. The cache is [`GLOBAL_RUN_DEFAULTS`](crate::completion_trigger::GLOBAL_RUN_DEFAULTS),
    /// because admission code has no registry. With no cache, `policy` comes
    /// back unchanged. Each admission calls this, so a change reaches the next
    /// admission within the cache TTL.
    #[must_use]
    pub fn current_quota_policy(workflow_name: &str, policy: QuotaPolicy) -> QuotaPolicy {
        let cache = crate::completion_trigger::GLOBAL_RUN_DEFAULTS
            .read()
            .ok()
            .and_then(|guard| guard.clone());
        cache
            .and_then(|cache| cache.apply_quota(workflow_name, Some(policy)))
            .unwrap_or(policy)
    }

    impl RunDefaultsCache {
        /// Reload the snapshot from the default shard.
        ///
        /// A failed load keeps the last snapshot.
        ///
        /// # Errors
        ///
        /// As [`load_run_default_overrides`].
        pub async fn refresh(&self, conn: &mut AsyncPgConnection) -> HarvestResult<()> {
            let overrides = load_run_default_overrides(conn).await?;
            self.replace(overrides);
            Ok(())
        }

        /// Reload the snapshot when it is older than [`RUN_DEFAULTS_TTL`].
        ///
        /// Returns whether it sent a query. A fresh snapshot sends none.
        ///
        /// # Errors
        ///
        /// As [`load_run_default_overrides`].
        pub async fn refresh_if_stale(&self, conn: &mut AsyncPgConnection) -> HarvestResult<bool> {
            if !self.is_stale(RUN_DEFAULTS_TTL) {
                return Ok(false);
            }
            self.refresh(conn).await?;
            Ok(true)
        }
    }

    /// Take the transaction lock of one workflow type.
    ///
    /// An override change takes it on the default shard. A recompute batch
    /// takes it on the shard that it updates.
    async fn lock_type(conn: &mut AsyncPgConnection, workflow_name: &str) -> HarvestResult<()> {
        diesel::sql_query("SELECT pg_advisory_xact_lock(hashtext($1)::int8)")
            .bind::<diesel::sql_types::Text, _>(format!("harvest:run_defaults:{workflow_name}"))
            .execute(conn)
            .await
            .map_err(crate::error::database_error)?;
        Ok(())
    }

    /// The result of [`recompute_in_flight_sla`].
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct Recompute {
        /// The number of runs that moved.
        pub moved: u64,
        /// Whether a newer change replaced `sla`, so the recompute stopped.
        pub superseded: bool,
    }

    /// Move the SLA of the in-flight runs of one type to `sla`. Call it on
    /// every shard.
    ///
    /// Only a RUNNING, SUSPENDED or PAUSED run that has not breached moves.
    /// The run keeps its SLA anchor: the new deadline is the old deadline
    /// minus the old SLA, plus the new one. A run with no SLA so far anchors
    /// at `started_at`. The new SLA is clamped to the run's own
    /// `execution_timeout`, as at start. `None` removes the SLA. The hard
    /// `deadline_at` never changes.
    ///
    /// The update runs in batches of [`RECOMPUTE_BATCH`] rows. Each batch is
    /// one transaction under the lock of the type on this shard. Under that
    /// lock it reads the stored override from `store`, the default shard.
    /// `None` means that `conn` is the default shard. If the effective SLA is
    /// no longer `sla`, a newer change replaced it, and the recompute stops.
    /// `code_sla` is the code value, which applies when no override is
    /// stored.
    ///
    /// So two recomputes of one type cannot undo each other. A batch of the
    /// older one either runs before the newer change commits, or it sees that
    /// change and stops. A newer batch waits on the lock for an older one, so
    /// it always runs after it.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Database`] when a read or an update fails, and
    /// [`HarvestError::Config`] when the stored override does not decode.
    pub async fn recompute_in_flight_sla(
        conn: &mut AsyncPgConnection,
        mut store: Option<&mut AsyncPgConnection>,
        workflow_name: &str,
        code_sla: Option<Duration>,
        sla: Option<Duration>,
    ) -> HarvestResult<Recompute> {
        use diesel::sql_types::{BigInt, Nullable, Text};
        use diesel_async::AsyncConnection;

        // `target` is the clamped new SLA of each row. The `IS DISTINCT FROM`
        // filter makes each batch skip the rows that already moved, so the
        // loop ends.
        const SQL: &str = "\
            WITH batch AS ( \
                SELECT id, \
                    CASE WHEN $2::bigint IS NULL THEN NULL::interval \
                         ELSE LEAST($2::bigint * INTERVAL '1 second', \
                                    COALESCE(execution_timeout, $2::bigint * INTERVAL '1 second')) \
                    END AS target \
                FROM harvest_workflow_executions \
                WHERE workflow_name = $1 \
                  AND state IN ('RUNNING', 'SUSPENDED', 'PAUSED') \
                  AND sla_breached = false \
                  AND sla IS DISTINCT FROM ( \
                    CASE WHEN $2::bigint IS NULL THEN NULL::interval \
                         ELSE LEAST($2::bigint * INTERVAL '1 second', \
                                    COALESCE(execution_timeout, $2::bigint * INTERVAL '1 second')) \
                    END) \
                ORDER BY id \
                LIMIT $3 \
                FOR UPDATE \
            ) \
            UPDATE harvest_workflow_executions AS e \
            SET sla = batch.target, \
                sla_deadline_at = CASE WHEN batch.target IS NULL THEN NULL \
                    ELSE COALESCE(e.sla_deadline_at - e.sla, e.started_at) + batch.target END \
            FROM batch \
            WHERE e.id = batch.id";

        let secs = sla.map(|d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX));
        let mut moved = 0_u64;
        loop {
            let store = store.as_deref_mut();
            let batch = Box::pin(conn.transaction(async |conn| {
                lock_type(conn, workflow_name).await?;
                let stored = match store {
                    Some(store) => get_run_default_override(store, workflow_name).await?,
                    None => get_run_default_override(conn, workflow_name).await?,
                };
                let current = stored.map_or(code_sla, |stored| stored.fields.sla.or(code_sla));
                if current != sla {
                    return Ok(None);
                }
                diesel::sql_query(SQL)
                    .bind::<Text, _>(workflow_name)
                    .bind::<Nullable<BigInt>, _>(secs)
                    .bind::<BigInt, _>(RECOMPUTE_BATCH)
                    .execute(conn)
                    .await
                    .map(Some)
                    .map_err(crate::error::database_error)
            }))
            .await?;
            let Some(updated) = batch else {
                return Ok(Recompute {
                    moved,
                    superseded: true,
                });
            };
            moved += u64::try_from(updated).unwrap_or(0);
            if i64::try_from(updated).unwrap_or(i64::MAX) < RECOMPUTE_BATCH {
                return Ok(Recompute {
                    moved,
                    superseded: false,
                });
            }
        }
    }
    /// The result of [`raise_waiting_concurrency_caps`].
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct CapRaise {
        /// The number of task rows whose cap went up.
        pub raised: u64,
        /// Whether a newer change replaced the limit, so the pass stopped.
        pub superseded: bool,
    }

    /// Raise the stored cap of the live workflow task rows of one type to
    /// `limit` (issue #2407). Call it on every shard.
    ///
    /// The claim compares the running count of a key to the cap stored on
    /// each task row. A waiting start keeps the cap of the start that wrote
    /// it. A run in flight keeps one task row for its life. The row is parked
    /// between decisions, and a wake sets it back to PENDING with its cap. So
    /// this pass lifts each PENDING or RUNNING workflow task row of the type
    /// whose cap is below `limit`. The next claim then admits it.
    ///
    /// The pass only raises. It never lowers a cap. So a lower limit applies
    /// to the next start only.
    ///
    /// The update runs in batches of [`RECOMPUTE_BATCH`] rows, and it skips a
    /// row that another transaction holds. Each batch takes the lock of the
    /// type on the default shard, then reads the stored override there. If
    /// the effective limit of `code` is no longer `limit`, a newer change
    /// replaced it, and the pass stops.
    ///
    /// `store` is the default shard. `None` means that `conn` is the default
    /// shard, and one transaction holds the lock and the update. Otherwise a
    /// transaction on `store` holds the lock until the batch on `conn`
    /// commits. A change of the override takes the same lock. So a newer,
    /// lower limit cannot commit between the check and the update.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Database`] when a read or an update fails, and
    /// [`HarvestError::Config`] when the stored override does not decode.
    pub async fn raise_waiting_concurrency_caps(
        conn: &mut AsyncPgConnection,
        mut store: Option<&mut AsyncPgConnection>,
        workflow_name: &str,
        code: ConcurrencyPolicy,
        limit: u32,
    ) -> HarvestResult<CapRaise> {
        use diesel::sql_types::{BigInt, Integer, Text};
        use diesel_async::AsyncConnection;

        // A run in a terminal state has no live task row, so the filter skips
        // it.
        const RAISE_SQL: &str = "\
            WITH batch AS ( \
                SELECT t.id \
                FROM harvest_workflow_executions e \
                JOIN harvest_task_queue t ON t.workflow_exec_id = e.id \
                WHERE e.workflow_name = $1 \
                  AND e.state NOT IN ('COMPLETED', 'FAILED', 'CANCELLED', 'TIMED_OUT', \
                                      'CONTINUED_AS_NEW', 'TERMINATED') \
                  AND t.task_type = 'workflow' \
                  AND t.state IN ('PENDING', 'RUNNING') \
                  AND t.concurrency_key IS NOT NULL \
                  AND t.concurrency_cap < $2 \
                ORDER BY t.id \
                LIMIT $3 \
                FOR UPDATE OF t SKIP LOCKED \
            ) \
            UPDATE harvest_task_queue AS t \
            SET concurrency_cap = $2 \
            FROM batch \
            WHERE t.id = batch.id";

        let cap = i32::try_from(limit).unwrap_or(i32::MAX);
        let effective = |stored: Option<RunDefaultOverride>| {
            stored
                .map_or(Some(code), |stored| {
                    stored.fields.apply_concurrency(Some(code))
                })
                .map(|policy| policy.limit)
        };
        let update = async |conn: &mut AsyncPgConnection| {
            diesel::sql_query(RAISE_SQL)
                .bind::<Text, _>(workflow_name)
                .bind::<Integer, _>(cap)
                .bind::<BigInt, _>(RECOMPUTE_BATCH)
                .execute(conn)
                .await
                .map_err(crate::error::database_error)
        };
        let mut raised = 0_u64;
        loop {
            let batch = match store.as_deref_mut() {
                None => {
                    Box::pin(conn.transaction(async |conn| {
                        lock_type(conn, workflow_name).await?;
                        let stored = get_run_default_override(conn, workflow_name).await?;
                        if effective(stored) != Some(limit) {
                            return Ok(None);
                        }
                        update(conn).await.map(Some)
                    }))
                    .await?
                }
                Some(store) => {
                    let conn = &mut *conn;
                    Box::pin(store.transaction(async |store| {
                        lock_type(store, workflow_name).await?;
                        let stored = get_run_default_override(store, workflow_name).await?;
                        if effective(stored) != Some(limit) {
                            return Ok(None);
                        }
                        Box::pin(conn.transaction(async |conn| update(conn).await))
                            .await
                            .map(Some)
                    }))
                    .await?
                }
            };
            let Some(updated) = batch else {
                return Ok(CapRaise {
                    raised,
                    superseded: true,
                });
            };
            raised += u64::try_from(updated).unwrap_or(0);
            if i64::try_from(updated).unwrap_or(i64::MAX) < RECOMPUTE_BATCH {
                return Ok(CapRaise {
                    raised,
                    superseded: false,
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stored(name: &str, fields: RunDefaultFields) -> RunDefaultOverride {
        RunDefaultOverride {
            workflow_name: name.to_string(),
            fields,
            reason: None,
            updated_by: "ops".to_string(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    const fn mins(n: u64) -> Duration {
        Duration::from_secs(n * 60)
    }

    #[test]
    fn an_override_wins_and_an_unset_field_inherits() {
        let cache = RunDefaultsCache::new();
        cache.replace([stored(
            "etl",
            RunDefaultFields {
                sla: Some(mins(5)),
                ..RunDefaultFields::default()
            },
        )]);
        assert_eq!(
            cache.apply("etl", Some(mins(60)), Some(mins(180))),
            (Some(mins(60)), Some(mins(5)))
        );
        assert_eq!(
            cache.apply("other", Some(mins(60)), Some(mins(180))),
            (Some(mins(60)), Some(mins(180))),
            "another type keeps its code values"
        );
    }

    #[test]
    fn an_override_can_set_a_value_that_code_leaves_unset() {
        let cache = RunDefaultsCache::new();
        cache.replace([stored(
            "etl",
            RunDefaultFields {
                execution_timeout: Some(mins(30)),
                sla: Some(mins(10)),
                ..RunDefaultFields::default()
            },
        )]);
        assert_eq!(
            cache.apply("etl", None, None),
            (Some(mins(30)), Some(mins(10)))
        );
    }

    #[test]
    fn a_never_loaded_cache_is_stale_and_inert() {
        let cache = RunDefaultsCache::new();
        assert!(cache.is_stale(RUN_DEFAULTS_TTL));
        assert_eq!(
            cache.apply("etl", None, Some(mins(3))),
            (None, Some(mins(3)))
        );
        cache.replace([]);
        assert!(!cache.is_stale(RUN_DEFAULTS_TTL));
        assert!(cache.is_stale(Duration::ZERO));
    }

    #[test]
    fn write_through_sets_and_removes_one_entry() {
        let cache = RunDefaultsCache::new();
        let fields = RunDefaultFields {
            sla: Some(mins(5)),
            ..RunDefaultFields::default()
        };
        cache.write_through("etl", fields);
        assert_eq!(cache.get("etl"), Some(fields));
        cache.write_through("etl", RunDefaultFields::default());
        assert_eq!(cache.get("etl"), None);
    }

    #[test]
    fn a_chain_override_wins_and_an_unset_chain_inherits() {
        let cache = RunDefaultsCache::new();
        cache.replace([stored(
            "etl",
            RunDefaultFields {
                chain_execution_timeout: Some(mins(120)),
                ..RunDefaultFields::default()
            },
        )]);
        assert_eq!(cache.apply_chain("etl", Some(mins(60))), Some(mins(120)));
        assert_eq!(cache.apply_chain("etl", None), Some(mins(120)));
        assert_eq!(
            cache.apply_chain("other", Some(mins(60))),
            Some(mins(60)),
            "another type keeps its code value"
        );
        assert_eq!(
            cache.apply("etl", Some(mins(30)), Some(mins(10))),
            (Some(mins(30)), Some(mins(10))),
            "a chain override leaves the per-run fields alone"
        );
    }

    #[test]
    fn a_never_loaded_cache_returns_the_code_chain_value() {
        let cache = RunDefaultsCache::new();
        assert_eq!(cache.apply_chain("etl", Some(mins(60))), Some(mins(60)));
        assert_eq!(cache.apply_chain("etl", None), None);
    }

    #[test]
    fn a_chain_only_override_is_not_empty() {
        let fields = RunDefaultFields {
            chain_execution_timeout: Some(mins(5)),
            ..RunDefaultFields::default()
        };
        assert!(!fields.is_empty());
        let cache = RunDefaultsCache::new();
        cache.write_through("etl", fields);
        assert_eq!(cache.get("etl"), Some(fields));
    }

    #[test]
    fn patch_merge_keeps_the_chain_field_it_does_not_name() {
        let base = RunDefaultFields {
            execution_timeout: Some(mins(60)),
            sla: Some(mins(5)),
            chain_execution_timeout: Some(mins(600)),
            concurrency_limit: None,
            quota: QuotaLimits::default(),
        };
        let clear_sla = RunDefaultPatch {
            sla: Some(None),
            ..RunDefaultPatch::default()
        };
        assert_eq!(
            clear_sla.merge(base).chain_execution_timeout,
            Some(mins(600))
        );
        let clear_chain = RunDefaultPatch {
            chain_execution_timeout: Some(None),
            ..RunDefaultPatch::default()
        };
        assert_eq!(
            clear_chain.merge(base),
            RunDefaultFields {
                chain_execution_timeout: None,
                ..base
            }
        );
    }

    fn limit_only(limit: u32) -> RunDefaultFields {
        RunDefaultFields {
            concurrency_limit: Some(limit),
            ..RunDefaultFields::default()
        }
    }

    const DEFER: ConcurrencyPolicy = ConcurrencyPolicy::new("input.tenant", 5);
    const LATEST_WINS: ConcurrencyPolicy = ConcurrencyPolicy::new("input.tenant", 5)
        .with_on_conflict(crate::concurrency::ConcurrencyOnConflict::CancelRunning);

    #[test]
    fn a_concurrency_override_replaces_the_code_limit() {
        let cache = RunDefaultsCache::new();
        cache.replace([stored("etl", limit_only(2))]);
        let lower = cache.apply_concurrency("etl", Some(DEFER)).expect("policy");
        assert_eq!(lower.limit, 2, "a lower limit applies");
        assert_eq!(lower.key_expr, DEFER.key_expr, "the key stays the code key");
        assert_eq!(lower.on_conflict, DEFER.on_conflict);
        cache.write_through("etl", limit_only(9));
        assert_eq!(
            cache.apply_concurrency("etl", Some(DEFER)).map(|p| p.limit),
            Some(9),
            "a higher limit applies"
        );
        assert_eq!(
            cache.apply_concurrency("other", Some(DEFER)),
            Some(DEFER),
            "another type keeps its code limit"
        );
    }

    /// A latest-wins start cancels runs down to its limit. Any later drop of
    /// an override, a clear included, would cancel runs in flight. So a
    /// latest-wins policy always keeps its code limit.
    #[test]
    fn a_latest_wins_policy_keeps_its_code_limit() {
        let cache = RunDefaultsCache::new();
        for limit in [1, 5, 8] {
            cache.write_through("etl", limit_only(limit));
            assert_eq!(
                cache.apply_concurrency("etl", Some(LATEST_WINS)),
                Some(LATEST_WINS),
                "an override of {limit} is ignored"
            );
        }
    }

    #[test]
    fn a_concurrency_override_needs_a_code_policy() {
        let cache = RunDefaultsCache::new();
        cache.replace([stored("etl", limit_only(3))]);
        assert_eq!(cache.apply_concurrency("etl", None), None);
    }

    #[test]
    fn a_never_loaded_cache_returns_the_code_policy() {
        let cache = RunDefaultsCache::new();
        assert_eq!(cache.apply_concurrency("etl", Some(DEFER)), Some(DEFER));
    }

    #[test]
    fn a_concurrency_only_override_is_not_empty_and_leaves_the_durations() {
        let fields = limit_only(4);
        assert!(!fields.is_empty());
        let cache = RunDefaultsCache::new();
        cache.write_through("etl", fields);
        assert_eq!(cache.get("etl"), Some(fields));
        assert_eq!(
            cache.apply("etl", Some(mins(30)), Some(mins(10))),
            (Some(mins(30)), Some(mins(10)))
        );
        assert_eq!(cache.apply_chain("etl", Some(mins(60))), Some(mins(60)));
    }

    #[test]
    fn patch_merge_keeps_the_concurrency_field_it_does_not_name() {
        let base = RunDefaultFields {
            sla: Some(mins(5)),
            concurrency_limit: Some(7),
            ..RunDefaultFields::default()
        };
        let clear_sla = RunDefaultPatch {
            sla: Some(None),
            ..RunDefaultPatch::default()
        };
        assert_eq!(clear_sla.merge(base).concurrency_limit, Some(7));
        let clear_limit = RunDefaultPatch {
            concurrency_limit: Some(None),
            ..RunDefaultPatch::default()
        };
        assert_eq!(
            clear_limit.merge(base),
            RunDefaultFields {
                concurrency_limit: None,
                ..base
            }
        );
    }

    const QUOTA: QuotaPolicy = QuotaPolicy::new("input.tenant")
        .with_max_active_executions(10)
        .with_max_history_bytes(1_000);

    fn quota_only(max_active_executions: u32) -> RunDefaultFields {
        RunDefaultFields {
            quota: QuotaLimits {
                max_active_executions: Some(max_active_executions),
                ..QuotaLimits::default()
            },
            ..RunDefaultFields::default()
        }
    }

    #[test]
    fn a_quota_override_replaces_the_code_caps() {
        let cache = RunDefaultsCache::new();
        cache.replace([stored("etl", quota_only(2))]);
        let effective = cache.apply_quota("etl", Some(QUOTA)).expect("policy");
        assert_eq!(effective.max_active_executions, Some(2));
        assert_eq!(effective.max_history_bytes, Some(1_000), "unset inherits");
        assert_eq!(effective.key_expr, QUOTA.key_expr);
        assert_eq!(
            cache.apply_quota("other", Some(QUOTA)),
            Some(QUOTA),
            "another type keeps its code caps"
        );
        assert_eq!(cache.apply_quota("etl", None), None, "no code policy");
    }

    #[test]
    fn a_never_loaded_cache_returns_the_code_quota() {
        let cache = RunDefaultsCache::new();
        assert_eq!(cache.apply_quota("etl", Some(QUOTA)), Some(QUOTA));
    }

    #[test]
    fn a_quota_only_override_is_not_empty_and_leaves_the_other_fields() {
        let fields = quota_only(3);
        assert!(!fields.is_empty());
        let cache = RunDefaultsCache::new();
        cache.write_through("etl", fields);
        assert_eq!(cache.get("etl"), Some(fields));
        assert_eq!(
            cache.apply("etl", Some(mins(30)), Some(mins(10))),
            (Some(mins(30)), Some(mins(10)))
        );
        assert_eq!(cache.apply_concurrency("etl", Some(DEFER)), Some(DEFER));
    }

    #[test]
    fn patch_merge_keeps_the_quota_caps_it_does_not_name() {
        let base = RunDefaultFields {
            sla: Some(mins(5)),
            ..quota_only(7)
        };
        let clear_sla = RunDefaultPatch {
            sla: Some(None),
            ..RunDefaultPatch::default()
        };
        assert_eq!(clear_sla.merge(base).quota, base.quota);
        let clear_quota = RunDefaultPatch {
            quota: QuotaLimitsPatch {
                max_active_executions: Some(None),
                ..QuotaLimitsPatch::default()
            },
            ..RunDefaultPatch::default()
        };
        assert_eq!(
            clear_quota.merge(base),
            RunDefaultFields {
                quota: QuotaLimits::default(),
                ..base
            }
        );
    }

    #[test]
    fn the_history_bytes_bound_fits_the_bigint_column() {
        assert_eq!(i64::try_from(MAX_QUOTA_HISTORY_BYTES).ok(), Some(i64::MAX));
    }

    #[test]
    fn the_limit_bound_fits_the_cap_column() {
        assert_eq!(i64::from(MAX_CONCURRENCY_LIMIT), i64::from(i32::MAX));
    }

    #[test]
    fn patch_merge_sets_clears_and_keeps() {
        let base = RunDefaultFields {
            execution_timeout: Some(mins(60)),
            sla: Some(mins(5)),
            ..RunDefaultFields::default()
        };
        let patch = RunDefaultPatch {
            sla: Some(None),
            ..RunDefaultPatch::default()
        };
        assert_eq!(
            patch.merge(base),
            RunDefaultFields {
                execution_timeout: Some(mins(60)),
                sla: None,
                ..RunDefaultFields::default()
            }
        );
        assert!(
            RunDefaultPatch {
                execution_timeout: Some(None),
                sla: Some(None),
                ..RunDefaultPatch::default()
            }
            .merge(base)
            .is_empty()
        );
    }
}
