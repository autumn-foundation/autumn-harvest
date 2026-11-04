//! The operator policies of agent kinds (issue #2439).
//!
//! Each agent kind has one row per policy name, and an audit row for each
//! change. The typed store is `SamplingPolicyStore` in
//! `autumn-harvest-agent`. This module holds the row types, the revision
//! kernel and the Postgres queries. `autumn-harvest-sqlite` holds the
//! `SQLite` queries.
//!
//! The rules:
//!
//! - A set writes a body. A reset writes no body, so every setting reads
//!   its default. Both keep the row.
//! - Each set and reset adds 1 to the revision. See [`next_policy_revision`].
//!   The first revision is 1. The revision never goes back.
//! - Each set and reset writes an audit row with the old and the new body,
//!   in the transaction of the change.
//!
//! The design is in `docs/design/DESIGN-2439.md`.

use chrono::{DateTime, Utc};

/// The policy name of the sampling policy (issue #2439).
pub const POLICY_SAMPLING: &str = "sampling";

/// The audit action of a set.
pub const ACTION_SET: &str = "set";

/// The audit action of a reset.
pub const ACTION_RESET: &str = "reset";

/// The revision after `current`. A row that does not exist has revision 0.
/// `None` means that the counter is full.
///
/// `formal/verus/src/agent_sampling.rs` proves it.
#[must_use]
pub const fn next_policy_revision(current: u64) -> Option<u64> {
    if current < u64::MAX {
        Some(current + 1)
    } else {
        None
    }
}

/// One stored policy row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyRow {
    /// The JSON of the policy. `None` after a reset.
    pub body: Option<String>,
    /// The revision, from 1.
    pub revision: u64,
    /// The actor of the last change.
    pub updated_by: String,
    /// The time of the last change.
    pub updated_at: DateTime<Utc>,
}

/// The result of one set or reset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyChangeRow {
    /// The body before the change.
    pub old: Option<String>,
    /// The body after the change.
    pub new: Option<String>,
    /// The revision after the change.
    pub revision: u64,
}

/// One audit row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyAuditRow {
    /// The revision that the change wrote.
    pub revision: u64,
    /// [`ACTION_SET`] or [`ACTION_RESET`].
    pub action: String,
    /// The actor of the change.
    pub actor: String,
    /// The body before the change.
    pub old: Option<String>,
    /// The body after the change.
    pub new: Option<String>,
    /// The time of the change.
    pub at: DateTime<Utc>,
}

#[cfg(feature = "db")]
#[doc(hidden)]
pub use pg::{list_agent_policy_audit, read_agent_policy, write_agent_policy};

/// The Postgres queries. They do not validate a body. Call them through
/// `SamplingPolicyStore`, which does.
#[cfg(feature = "db")]
mod pg {
    use chrono::{DateTime, Utc};
    use diesel::QueryableByName;
    use diesel::sql_types::{BigInt, Nullable, Text, Timestamptz};
    use diesel_async::{AsyncPgConnection, RunQueryDsl};

    use super::{
        ACTION_RESET, ACTION_SET, PolicyAuditRow, PolicyChangeRow, PolicyRow, next_policy_revision,
    };
    use crate::error::{HarvestError, HarvestResult, database_error};

    #[derive(QueryableByName)]
    struct Row {
        #[diesel(sql_type = Nullable<Text>)]
        body: Option<String>,
        #[diesel(sql_type = BigInt)]
        revision: i64,
        #[diesel(sql_type = Text)]
        updated_by: String,
        #[diesel(sql_type = Timestamptz)]
        updated_at: DateTime<Utc>,
    }

    #[derive(QueryableByName)]
    struct AuditRow {
        #[diesel(sql_type = BigInt)]
        revision: i64,
        #[diesel(sql_type = Text)]
        action: String,
        #[diesel(sql_type = Text)]
        actor: String,
        #[diesel(sql_type = Nullable<Text>)]
        old_body: Option<String>,
        #[diesel(sql_type = Nullable<Text>)]
        new_body: Option<String>,
        #[diesel(sql_type = Timestamptz)]
        at: DateTime<Utc>,
    }

    /// A stored revision as `u64`. The table check keeps it at 1 or more.
    fn to_revision(stored: i64) -> HarvestResult<u64> {
        u64::try_from(stored)
            .map_err(|_| HarvestError::Database(format!("policy revision {stored} is negative")))
    }

    /// Read the row of `kind` and `policy`. `None` means that no operator
    /// ever set or reset it.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Database`] when the statement fails.
    pub async fn read_agent_policy(
        conn: &mut AsyncPgConnection,
        kind: &str,
        policy: &str,
    ) -> HarvestResult<Option<PolicyRow>> {
        select_policy(conn, kind, policy, false).await
    }

    /// The row of `kind` and `policy`, locked when `for_update` is set.
    async fn select_policy(
        conn: &mut AsyncPgConnection,
        kind: &str,
        policy: &str,
        for_update: bool,
    ) -> HarvestResult<Option<PolicyRow>> {
        let lock = if for_update { " FOR UPDATE" } else { "" };
        let rows: Vec<Row> = diesel::sql_query(format!(
            "SELECT body::text AS body, revision, updated_by, updated_at
             FROM harvest_agent_policies
             WHERE kind = $1 AND policy = $2{lock}"
        ))
        .bind::<Text, _>(kind)
        .bind::<Text, _>(policy)
        .load(conn)
        .await
        .map_err(database_error)?;
        rows.into_iter()
            .next()
            .map(|row| {
                Ok(PolicyRow {
                    body: row.body,
                    revision: to_revision(row.revision)?,
                    updated_by: row.updated_by,
                    updated_at: row.updated_at,
                })
            })
            .transpose()
    }

    /// Set the body of `kind` and `policy` to `body`, or reset it when
    /// `body` is `None`. Writes the audit row in the same transaction.
    ///
    /// An advisory lock serializes two writes of one row, also when no row
    /// exists yet. So each audit row names the body that its change
    /// replaced. The caller validates `body` first.
    ///
    /// Returns `None` when the revision counter is full. Nothing changes
    /// then.
    ///
    /// The transaction is `READ COMMITTED` whatever the session default.
    /// So the read after the lock sees the write that the lock waited for.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Database`] when a statement fails.
    pub async fn write_agent_policy(
        conn: &mut AsyncPgConnection,
        kind: &str,
        policy: &str,
        body: Option<&str>,
        actor: &str,
    ) -> HarvestResult<Option<PolicyChangeRow>> {
        let action = if body.is_some() {
            ACTION_SET
        } else {
            ACTION_RESET
        };
        Box::pin(conn.build_transaction().read_committed().run(async |conn| {
            lock_policy(conn, kind, policy).await?;
            let stored = select_policy(conn, kind, policy, true).await?;
            let current = stored.as_ref().map_or(0, |row| row.revision);
            let Some(revision) =
                next_policy_revision(current).and_then(|next| i64::try_from(next).ok())
            else {
                return Ok(None);
            };
            let old = stored.and_then(|row| row.body);
            upsert_policy(conn, kind, policy, body, revision, actor).await?;
            insert_policy_audit(
                conn,
                &AuditInsert {
                    kind,
                    policy,
                    revision,
                    action,
                    actor,
                    old: old.as_deref(),
                    new: body,
                },
            )
            .await?;
            Ok(Some(PolicyChangeRow {
                old,
                new: body.map(str::to_owned),
                revision: to_revision(revision)?,
            }))
        }))
        .await
    }

    /// Take the transaction lock of one policy row.
    async fn lock_policy(
        conn: &mut AsyncPgConnection,
        kind: &str,
        policy: &str,
    ) -> HarvestResult<()> {
        diesel::sql_query("SELECT pg_advisory_xact_lock(hashtext($1)::int8)")
            .bind::<Text, _>(format!("harvest:agent_policy:{policy}:{kind}"))
            .execute(conn)
            .await
            .map_err(database_error)?;
        Ok(())
    }

    /// Write the row of a set or a reset.
    async fn upsert_policy(
        conn: &mut AsyncPgConnection,
        kind: &str,
        policy: &str,
        body: Option<&str>,
        revision: i64,
        actor: &str,
    ) -> HarvestResult<()> {
        diesel::sql_query(
            "INSERT INTO harvest_agent_policies
                 (kind, policy, body, revision, updated_by, updated_at)
             VALUES ($1, $2, CAST($3 AS jsonb), $4, $5, clock_timestamp())
             ON CONFLICT (kind, policy) DO UPDATE
                 SET body = EXCLUDED.body,
                     revision = EXCLUDED.revision,
                     updated_by = EXCLUDED.updated_by,
                     updated_at = clock_timestamp()",
        )
        .bind::<Text, _>(kind)
        .bind::<Text, _>(policy)
        .bind::<Nullable<Text>, _>(body)
        .bind::<BigInt, _>(revision)
        .bind::<Text, _>(actor)
        .execute(conn)
        .await
        .map_err(database_error)?;
        Ok(())
    }

    /// The fields of one audit row.
    struct AuditInsert<'a> {
        kind: &'a str,
        policy: &'a str,
        revision: i64,
        action: &'a str,
        actor: &'a str,
        old: Option<&'a str>,
        new: Option<&'a str>,
    }

    /// Write the audit row of a change.
    async fn insert_policy_audit(
        conn: &mut AsyncPgConnection,
        row: &AuditInsert<'_>,
    ) -> HarvestResult<()> {
        diesel::sql_query(
            "INSERT INTO harvest_agent_policy_audit
                 (kind, policy, revision, action, actor, old_body, new_body, at)
             VALUES ($1, $2, $3, $4, $5, CAST($6 AS jsonb), CAST($7 AS jsonb),
                     clock_timestamp())",
        )
        .bind::<Text, _>(row.kind)
        .bind::<Text, _>(row.policy)
        .bind::<BigInt, _>(row.revision)
        .bind::<Text, _>(row.action)
        .bind::<Text, _>(row.actor)
        .bind::<Nullable<Text>, _>(row.old)
        .bind::<Nullable<Text>, _>(row.new)
        .execute(conn)
        .await
        .map_err(database_error)?;
        Ok(())
    }

    /// The newest `limit` audit rows of `kind` and `policy`, the newest
    /// first.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Database`] when the statement fails.
    pub async fn list_agent_policy_audit(
        conn: &mut AsyncPgConnection,
        kind: &str,
        policy: &str,
        limit: u32,
    ) -> HarvestResult<Vec<PolicyAuditRow>> {
        let rows: Vec<AuditRow> = diesel::sql_query(
            "SELECT revision, action, actor, old_body::text AS old_body,
                    new_body::text AS new_body, at
             FROM harvest_agent_policy_audit
             WHERE kind = $1 AND policy = $2
             ORDER BY id DESC
             LIMIT $3",
        )
        .bind::<Text, _>(kind)
        .bind::<Text, _>(policy)
        .bind::<BigInt, _>(i64::from(limit))
        .load(conn)
        .await
        .map_err(database_error)?;
        rows.into_iter()
            .map(|row| {
                Ok(PolicyAuditRow {
                    revision: to_revision(row.revision)?,
                    action: row.action,
                    actor: row.actor,
                    old: row.old_body,
                    new: row.new_body,
                    at: row.at,
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::next_policy_revision;

    #[test]
    fn the_revision_counts_from_one_and_stops_at_the_top() {
        assert_eq!(next_policy_revision(0), Some(1));
        assert_eq!(next_policy_revision(41), Some(42));
        assert_eq!(next_policy_revision(u64::MAX), None);
    }
}
