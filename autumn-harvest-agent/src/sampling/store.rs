//! The store of sampling policies (issue #2439).
//!
//! Each agent kind has at most one policy. A set validates the policy,
//! writes it and writes an audit row with the old and new values, in one
//! transaction. A reset restores every default and is audited too. A read
//! of a kind with no policy returns [`SamplingPolicy::default`].
//!
//! A store does its work when the future is polled. The `SQLite` store runs
//! its statements on the polling thread.

use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};

use autumn_harvest::agent_policy_log::{
    ACTION_RESET, ACTION_SET, PolicyAuditRow, PolicyChangeRow, PolicyRow, next_policy_revision,
};
use autumn_harvest::chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::policy::{PolicyError, SamplingPolicy};
use crate::agent::overlay_store::check_operator_id;
use crate::harness::truncate;
use crate::mesh::{MAX_NAME_LEN, valid_name};
use crate::model::BoxFuture;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
use autumn_harvest::agent_policy_log::POLICY_SAMPLING;

/// The most audit rows that one [`SamplingPolicyStore::audit`] call returns.
pub const MAX_AUDIT_ROWS: u32 = 1_000;

/// The policy that a read returns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyRead {
    /// The policy. Every default when no operator set one.
    pub policy: SamplingPolicy,
    /// The revision of the row. 0 when no operator ever set or reset it.
    pub revision: u64,
}

/// The result of one set or reset.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyChange {
    /// The stored policy before the change. `None` for every default.
    pub old: Option<SamplingPolicy>,
    /// The stored policy after the change. `None` after a reset.
    pub new: Option<SamplingPolicy>,
    /// The revision after the change.
    pub revision: u64,
}

/// What an audit row records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicyAction {
    /// An operator set the policy.
    Set,
    /// An operator restored every default.
    Reset,
}

/// One audited change.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyAudit {
    /// The revision that the change wrote.
    pub revision: u64,
    /// A set or a reset.
    pub action: PolicyAction,
    /// The operator id.
    pub actor: String,
    /// The stored policy before the change.
    pub old: Option<SamplingPolicy>,
    /// The stored policy after the change.
    pub new: Option<SamplingPolicy>,
    /// The time of the change.
    pub at: DateTime<Utc>,
}

/// Why a store refuses a call.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum PolicyStoreError {
    /// The kind is not a valid agent kind.
    #[error("{kind:?} is not a valid agent kind")]
    InvalidKind {
        /// The kind, cut to [`MAX_NAME_LEN`] characters.
        kind: String,
    },
    /// The operator id is not valid.
    #[error("the actor is not valid: {reason}")]
    InvalidActor {
        /// Why.
        reason: &'static str,
    },
    /// The policy is not valid.
    #[error(transparent)]
    Policy(#[from] PolicyError),
    /// The revision counter of the kind is full.
    #[error("the policy revision counter is full")]
    Full,
    /// A stored row does not decode. A run fails closed on it.
    #[error("a stored sampling policy is corrupt: {reason}")]
    Corrupt {
        /// Why.
        reason: String,
    },
    /// The backend failed.
    #[error("the sampling policy store failed: {message}")]
    Backend {
        /// The backend error.
        message: String,
    },
}

fn backend(error: impl std::fmt::Display) -> PolicyStoreError {
    PolicyStoreError::Backend {
        message: error.to_string(),
    }
}

/// Storage for the sampling policy of each agent kind.
///
/// Each set and reset writes its audit row in the same transaction. A read
/// of a kind with no policy returns every default.
pub trait SamplingPolicyStore: Send + Sync + std::fmt::Debug {
    /// The policy of `kind`.
    ///
    /// # Errors
    ///
    /// [`PolicyStoreError::InvalidKind`], [`PolicyStoreError::Corrupt`] and
    /// [`PolicyStoreError::Backend`].
    fn read<'a>(&'a self, kind: &'a str) -> BoxFuture<'a, Result<PolicyRead, PolicyStoreError>>;

    /// Validate `policy` and store it as the policy of `kind`.
    ///
    /// # Errors
    ///
    /// [`PolicyStoreError::InvalidKind`], [`PolicyStoreError::InvalidActor`],
    /// [`PolicyStoreError::Policy`], [`PolicyStoreError::Full`] and
    /// [`PolicyStoreError::Backend`]. Nothing changes on an error.
    fn set<'a>(
        &'a self,
        kind: &'a str,
        policy: &'a SamplingPolicy,
        actor: &'a str,
    ) -> BoxFuture<'a, Result<PolicyChange, PolicyStoreError>>;

    /// Restore every default for `kind`.
    ///
    /// # Errors
    ///
    /// As [`set`](Self::set), with no policy to validate.
    fn reset<'a>(
        &'a self,
        kind: &'a str,
        actor: &'a str,
    ) -> BoxFuture<'a, Result<PolicyChange, PolicyStoreError>>;

    /// The newest `limit` audit rows of `kind`, the newest first. A limit
    /// above [`MAX_AUDIT_ROWS`] counts as that limit.
    ///
    /// # Errors
    ///
    /// [`PolicyStoreError::InvalidKind`], [`PolicyStoreError::Corrupt`] and
    /// [`PolicyStoreError::Backend`].
    fn audit<'a>(
        &'a self,
        kind: &'a str,
        limit: u32,
    ) -> BoxFuture<'a, Result<Vec<PolicyAudit>, PolicyStoreError>>;
}

/// Refuse a kind that is not a valid agent kind.
fn check_kind(kind: &str) -> Result<(), PolicyStoreError> {
    if valid_name(kind) {
        Ok(())
    } else {
        Err(PolicyStoreError::InvalidKind {
            kind: truncate(kind, MAX_NAME_LEN),
        })
    }
}

/// Check a write, and encode its policy. Every store calls it first.
fn check_write(
    kind: &str,
    policy: Option<&SamplingPolicy>,
    actor: &str,
) -> Result<Option<String>, PolicyStoreError> {
    check_kind(kind)?;
    check_operator_id(actor).map_err(|reason| PolicyStoreError::InvalidActor { reason })?;
    policy
        .map(|policy| {
            policy.validate()?;
            serde_json::to_string(policy).map_err(backend)
        })
        .transpose()
}

/// Decode a stored body. A body that does not decode is corrupt, so a run
/// never falls back to a wider value.
fn decode_body(body: Option<&str>) -> Result<Option<SamplingPolicy>, PolicyStoreError> {
    body.map(|body| {
        let policy: SamplingPolicy =
            serde_json::from_str(body).map_err(|err| PolicyStoreError::Corrupt {
                reason: err.to_string(),
            })?;
        policy.validate().map_err(|err| PolicyStoreError::Corrupt {
            reason: err.to_string(),
        })?;
        Ok(policy)
    })
    .transpose()
}

fn decode_read(row: Option<PolicyRow>) -> Result<PolicyRead, PolicyStoreError> {
    let Some(row) = row else {
        return Ok(PolicyRead {
            policy: SamplingPolicy::default(),
            revision: 0,
        });
    };
    Ok(PolicyRead {
        policy: decode_body(row.body.as_deref())?.unwrap_or_default(),
        revision: row.revision,
    })
}

fn decode_change(row: &PolicyChangeRow) -> Result<PolicyChange, PolicyStoreError> {
    Ok(PolicyChange {
        old: decode_body(row.old.as_deref())?,
        new: decode_body(row.new.as_deref())?,
        revision: row.revision,
    })
}

fn decode_audit(rows: Vec<PolicyAuditRow>) -> Result<Vec<PolicyAudit>, PolicyStoreError> {
    rows.into_iter()
        .map(|row| {
            let action = match row.action.as_str() {
                ACTION_SET => PolicyAction::Set,
                ACTION_RESET => PolicyAction::Reset,
                _ => {
                    return Err(PolicyStoreError::Corrupt {
                        reason: "an audit action is unknown".to_owned(),
                    });
                }
            };
            Ok(PolicyAudit {
                revision: row.revision,
                action,
                actor: row.actor,
                old: decode_body(row.old.as_deref())?,
                new: decode_body(row.new.as_deref())?,
                at: row.at,
            })
        })
        .collect()
}

/// A [`SamplingPolicyStore`] in memory, for tests and one-process apps.
#[derive(Debug, Default)]
pub struct InMemorySamplingPolicyStore {
    kinds: Mutex<HashMap<String, MemoryKind>>,
}

#[derive(Debug, Default)]
struct MemoryKind {
    row: Option<PolicyRow>,
    audit: Vec<PolicyAuditRow>,
}

impl InMemorySamplingPolicyStore {
    /// An empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn write(
        &self,
        kind: &str,
        body: Option<String>,
        actor: &str,
    ) -> Result<PolicyChangeRow, PolicyStoreError> {
        let mut kinds = self.kinds.lock().unwrap_or_else(PoisonError::into_inner);
        let entry = kinds.entry(kind.to_owned()).or_default();
        let current = entry.row.as_ref().map_or(0, |row| row.revision);
        let revision = next_policy_revision(current).ok_or(PolicyStoreError::Full)?;
        let old = entry.row.take().and_then(|row| row.body);
        let at = Utc::now();
        let action = if body.is_some() {
            ACTION_SET
        } else {
            ACTION_RESET
        };
        entry.row = Some(PolicyRow {
            body: body.clone(),
            revision,
            updated_by: actor.to_owned(),
            updated_at: at,
        });
        entry.audit.push(PolicyAuditRow {
            revision,
            action: action.to_owned(),
            actor: actor.to_owned(),
            old: old.clone(),
            new: body.clone(),
            at,
        });
        drop(kinds);
        Ok(PolicyChangeRow {
            old,
            new: body,
            revision,
        })
    }
}

impl SamplingPolicyStore for InMemorySamplingPolicyStore {
    fn read<'a>(&'a self, kind: &'a str) -> BoxFuture<'a, Result<PolicyRead, PolicyStoreError>> {
        Box::pin(async move {
            check_kind(kind)?;
            let row = self
                .kinds
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .get(kind)
                .and_then(|entry| entry.row.clone());
            decode_read(row)
        })
    }

    fn set<'a>(
        &'a self,
        kind: &'a str,
        policy: &'a SamplingPolicy,
        actor: &'a str,
    ) -> BoxFuture<'a, Result<PolicyChange, PolicyStoreError>> {
        Box::pin(async move {
            let body = check_write(kind, Some(policy), actor)?;
            decode_change(&self.write(kind, body, actor)?)
        })
    }

    fn reset<'a>(
        &'a self,
        kind: &'a str,
        actor: &'a str,
    ) -> BoxFuture<'a, Result<PolicyChange, PolicyStoreError>> {
        Box::pin(async move {
            check_write(kind, None, actor)?;
            decode_change(&self.write(kind, None, actor)?)
        })
    }

    fn audit<'a>(
        &'a self,
        kind: &'a str,
        limit: u32,
    ) -> BoxFuture<'a, Result<Vec<PolicyAudit>, PolicyStoreError>> {
        Box::pin(async move {
            check_kind(kind)?;
            let limit = usize::try_from(limit.min(MAX_AUDIT_ROWS)).unwrap_or(usize::MAX);
            let rows: Vec<PolicyAuditRow> = self
                .kinds
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .get(kind)
                .map(|entry| entry.audit.iter().rev().take(limit).cloned().collect())
                .unwrap_or_default();
            decode_audit(rows)
        })
    }
}

/// A [`SamplingPolicyStore`] in a `SQLite` file.
///
/// It opens its own connection. It can share the file of a
/// `SqliteRuntime`, so a second process on the file sees each change.
#[cfg(feature = "sqlite")]
pub struct SqliteSamplingPolicyStore {
    log: autumn_harvest_sqlite::SqlitePolicyLog,
}

#[cfg(feature = "sqlite")]
impl std::fmt::Debug for SqliteSamplingPolicyStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteSamplingPolicyStore")
            .finish_non_exhaustive()
    }
}

#[cfg(feature = "sqlite")]
impl SqliteSamplingPolicyStore {
    /// Open the store in the database at `path`.
    ///
    /// # Errors
    ///
    /// Returns [`PolicyStoreError::Backend`] when the file does not open.
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self, PolicyStoreError> {
        let log = autumn_harvest_sqlite::SqlitePolicyLog::open(path).map_err(backend)?;
        Ok(Self { log })
    }

    /// Open a store in a new, private in-memory database.
    ///
    /// # Errors
    ///
    /// Returns [`PolicyStoreError::Backend`] when the database does not
    /// open.
    pub fn open_in_memory() -> Result<Self, PolicyStoreError> {
        let log = autumn_harvest_sqlite::SqlitePolicyLog::open_in_memory().map_err(backend)?;
        Ok(Self { log })
    }

    fn write(
        &self,
        kind: &str,
        body: Option<&str>,
        actor: &str,
    ) -> Result<PolicyChange, PolicyStoreError> {
        let row = self
            .log
            .write(kind, POLICY_SAMPLING, body, actor)
            .map_err(backend)?;
        decode_change(&row)
    }
}

#[cfg(feature = "sqlite")]
impl SamplingPolicyStore for SqliteSamplingPolicyStore {
    fn read<'a>(&'a self, kind: &'a str) -> BoxFuture<'a, Result<PolicyRead, PolicyStoreError>> {
        Box::pin(async move {
            check_kind(kind)?;
            decode_read(self.log.read(kind, POLICY_SAMPLING).map_err(backend)?)
        })
    }

    fn set<'a>(
        &'a self,
        kind: &'a str,
        policy: &'a SamplingPolicy,
        actor: &'a str,
    ) -> BoxFuture<'a, Result<PolicyChange, PolicyStoreError>> {
        Box::pin(async move {
            let body = check_write(kind, Some(policy), actor)?;
            self.write(kind, body.as_deref(), actor)
        })
    }

    fn reset<'a>(
        &'a self,
        kind: &'a str,
        actor: &'a str,
    ) -> BoxFuture<'a, Result<PolicyChange, PolicyStoreError>> {
        Box::pin(async move {
            check_write(kind, None, actor)?;
            self.write(kind, None, actor)
        })
    }

    fn audit<'a>(
        &'a self,
        kind: &'a str,
        limit: u32,
    ) -> BoxFuture<'a, Result<Vec<PolicyAudit>, PolicyStoreError>> {
        Box::pin(async move {
            check_kind(kind)?;
            let rows = self
                .log
                .audit(kind, POLICY_SAMPLING, limit.min(MAX_AUDIT_ROWS))
                .map_err(backend)?;
            decode_audit(rows)
        })
    }
}

/// A [`SamplingPolicyStore`] on Postgres.
///
/// The queries are in `autumn_harvest::agent_policy_log`. Each read sends
/// one query, so a change in one process reaches the next strategy run of
/// every other process. In a sharded deployment, give the store the pool
/// of shard 0.
#[cfg(feature = "postgres")]
pub struct PostgresSamplingPolicyStore {
    pool: autumn_harvest::worker::DbPool,
}

#[cfg(feature = "postgres")]
impl std::fmt::Debug for PostgresSamplingPolicyStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresSamplingPolicyStore")
            .finish_non_exhaustive()
    }
}

#[cfg(feature = "postgres")]
impl PostgresSamplingPolicyStore {
    /// A store on `pool`.
    #[must_use]
    pub const fn new(pool: autumn_harvest::worker::DbPool) -> Self {
        Self { pool }
    }

    async fn write(
        &self,
        kind: &str,
        body: Option<&str>,
        actor: &str,
    ) -> Result<PolicyChange, PolicyStoreError> {
        let mut conn = self.pool.get().await.map_err(backend)?;
        let row = autumn_harvest::agent_policy_log::write_agent_policy(
            &mut conn,
            kind,
            POLICY_SAMPLING,
            body,
            actor,
        )
        .await
        .map_err(backend)?;
        decode_change(&row)
    }
}

#[cfg(feature = "postgres")]
impl SamplingPolicyStore for PostgresSamplingPolicyStore {
    fn read<'a>(&'a self, kind: &'a str) -> BoxFuture<'a, Result<PolicyRead, PolicyStoreError>> {
        Box::pin(async move {
            check_kind(kind)?;
            let mut conn = self.pool.get().await.map_err(backend)?;
            let row = autumn_harvest::agent_policy_log::read_agent_policy(
                &mut conn,
                kind,
                POLICY_SAMPLING,
            )
            .await
            .map_err(backend)?;
            decode_read(row)
        })
    }

    fn set<'a>(
        &'a self,
        kind: &'a str,
        policy: &'a SamplingPolicy,
        actor: &'a str,
    ) -> BoxFuture<'a, Result<PolicyChange, PolicyStoreError>> {
        Box::pin(async move {
            let body = check_write(kind, Some(policy), actor)?;
            self.write(kind, body.as_deref(), actor).await
        })
    }

    fn reset<'a>(
        &'a self,
        kind: &'a str,
        actor: &'a str,
    ) -> BoxFuture<'a, Result<PolicyChange, PolicyStoreError>> {
        Box::pin(async move {
            check_write(kind, None, actor)?;
            self.write(kind, None, actor).await
        })
    }

    fn audit<'a>(
        &'a self,
        kind: &'a str,
        limit: u32,
    ) -> BoxFuture<'a, Result<Vec<PolicyAudit>, PolicyStoreError>> {
        Box::pin(async move {
            check_kind(kind)?;
            let mut conn = self.pool.get().await.map_err(backend)?;
            let rows = autumn_harvest::agent_policy_log::list_agent_policy_audit(
                &mut conn,
                kind,
                POLICY_SAMPLING,
                limit.min(MAX_AUDIT_ROWS),
            )
            .await
            .map_err(backend)?;
            decode_audit(rows)
        })
    }
}
