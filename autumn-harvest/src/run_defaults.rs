//! Operator overrides for per-type run defaults (issue #2387).
//!
//! [`WorkflowInfo::sla`](crate::info::WorkflowInfo::sla) and
//! [`WorkflowInfo::execution_timeout`](crate::info::WorkflowInfo::execution_timeout)
//! are declared in code. A DAG lowers to a shadow workflow type of the same
//! name, so its `DagInfo` values arrive here too. Each start path resolves the
//! deadlines of a new run from them, so a change needed a restart.
//!
//! An override is a persisted operator value for one or both fields, keyed by
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
//! still caps `execution_timeout`.
//!
//! ## Runs already in flight
//!
//! A change applies to new starts only. A continue-as-new successor of the
//! same type keeps the values of its run, as before. The opt-in
//! [`recompute_in_flight_sla`] moves the SLA deadline of the non-terminal
//! runs of a type that have not breached yet. It never changes a hard
//! `deadline_at`, because a shorter one could end a run at once.
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

/// The maximum age of a loaded snapshot before a refresh loop reloads it.
pub const RUN_DEFAULTS_TTL: Duration = Duration::from_secs(5);

/// The longest duration that an override can set: 100 years.
///
/// A start adds the duration to its start time. A sum past the range of
/// `chrono` panics, and a larger value fails its conversion and drops the
/// deadline. So the write refuses a value above this bound.
pub const MAX_RUN_DEFAULT: Duration = Duration::from_secs(100 * 365 * 86_400);

/// The run-default fields that an override can set.
///
/// `None` inherits the code value.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RunDefaultFields {
    /// The hard execution timeout of a new run.
    pub execution_timeout: Option<Duration>,
    /// The soft SLA of a new run.
    pub sla: Option<Duration>,
}

impl RunDefaultFields {
    /// Whether the override sets no field.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.execution_timeout.is_none() && self.sla.is_none()
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
}

impl RunDefaultPatch {
    /// Merge this patch over the stored fields.
    #[must_use]
    pub fn merge(&self, stored: RunDefaultFields) -> RunDefaultFields {
        RunDefaultFields {
            execution_timeout: self.execution_timeout.unwrap_or(stored.execution_timeout),
            sla: self.sla.unwrap_or(stored.sla),
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
        MAX_RUN_DEFAULT, RUN_DEFAULTS_TTL, RunDefaultFields, RunDefaultOverride, RunDefaultPatch,
        RunDefaultsCache,
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
            let fields = RunDefaultFields {
                execution_timeout: secs("execution_timeout_secs", self.execution_timeout_secs)?,
                sla: secs("sla_secs", self.sla_secs)?,
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
    /// zero timeout ends the run at once. A merge that sets no field deletes
    /// the row. Returns the stored override after the change.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Config`] for a zero duration, and
    /// [`HarvestError::Database`] when a read or write fails.
    pub async fn update_run_default_override(
        conn: &mut AsyncPgConnection,
        change: &RunDefaultChange<'_>,
    ) -> HarvestResult<Option<RunDefaultOverride>> {
        use crate::schema::harvest_run_default_overrides::dsl;
        use diesel_async::AsyncConnection;

        for (field, value) in [
            ("execution_timeout", change.patch.execution_timeout),
            ("sla", change.patch.sla),
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
        let change = *change;
        Box::pin(conn.transaction(async |conn| {
            // A first override has no row for FOR UPDATE to lock. This lock
            // serializes two patches of the same type in that case too, so
            // neither one merges over a read that the other replaces.
            diesel::sql_query("SELECT pg_advisory_xact_lock(hashtext($1)::int8)")
                .bind::<diesel::sql_types::Text, _>(format!(
                    "harvest:run_defaults:{}",
                    change.workflow_name
                ))
                .execute(conn)
                .await
                .map_err(crate::error::database_error)?;
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
            diesel::insert_into(dsl::harvest_run_default_overrides)
                .values((
                    dsl::workflow_name.eq(change.workflow_name),
                    dsl::sla_secs.eq(sla_secs),
                    dsl::execution_timeout_secs.eq(execution_timeout_secs),
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

    /// Move the SLA of the in-flight runs of one type to `sla`. Call it on
    /// every shard.
    ///
    /// Only a RUNNING, SUSPENDED or PAUSED run that has not breached moves. The run keeps
    /// its SLA anchor: the new deadline is the old deadline minus the old SLA,
    /// plus the new one. A run with no SLA so far anchors at `started_at`. The
    /// new SLA is clamped to the run's own `execution_timeout`, as at start.
    /// `None` removes the SLA. The hard `deadline_at` never changes.
    ///
    /// The update runs in batches of [`RECOMPUTE_BATCH`] rows. Returns the
    /// number of runs that moved.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Database`] when an update fails.
    pub async fn recompute_in_flight_sla(
        conn: &mut AsyncPgConnection,
        workflow_name: &str,
        sla: Option<Duration>,
    ) -> HarvestResult<u64> {
        use diesel::sql_types::{BigInt, Nullable, Text};

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
            let updated = diesel::sql_query(SQL)
                .bind::<Text, _>(workflow_name)
                .bind::<Nullable<BigInt>, _>(secs)
                .bind::<BigInt, _>(RECOMPUTE_BATCH)
                .execute(conn)
                .await
                .map_err(crate::error::database_error)?;
            moved += u64::try_from(updated).unwrap_or(0);
            if i64::try_from(updated).unwrap_or(i64::MAX) < RECOMPUTE_BATCH {
                return Ok(moved);
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
    fn patch_merge_sets_clears_and_keeps() {
        let base = RunDefaultFields {
            execution_timeout: Some(mins(60)),
            sla: Some(mins(5)),
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
            }
        );
        assert!(
            RunDefaultPatch {
                execution_timeout: Some(None),
                sla: Some(None),
            }
            .merge(base)
            .is_empty()
        );
    }
}
