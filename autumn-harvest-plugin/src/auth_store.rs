//! The store of API tokens and audit rows, on either backend (issue #2469).
//!
//! The token layer, the token routes, the audit read and each deny audit
//! reach storage through [`AuthStore`]. On Postgres it is the Harvest pool.
//! On SQLite it is the [`SqliteAuthStore`] of the Harvest file. One layer
//! stack thus serves both backends, and no layer has a second copy.
//!
//! A caller opens an [`AuthSession`] for one request. On Postgres it holds
//! one connection, so a lookup or a change and its audit row share it.
//!
//! A SQLite call blocks, so it runs on the blocking pool.
//!
//! [`SqliteAuthStore`]: autumn_harvest_sqlite::SqliteAuthStore

use chrono::{DateTime, Utc};
use uuid::Uuid;

use autumn_harvest::HarvestError;
use autumn_harvest::audit::{self, AuditFilters};
use autumn_harvest::models::{ApiToken, AuditRecord, NewAuditRecord};
use autumn_web::error::AutumnError;

use crate::HarvestDbPool;
use crate::api::{PoolConn, acquire_conn};
use crate::api_token::{MintResult, TokenScope};

/// A failed store call.
#[derive(Debug)]
pub enum StoreError {
    /// No connection: the pool is down or exhausted. It maps to `503`.
    Unavailable(AutumnError),
    /// The call reached the store and failed.
    Failed(HarvestError),
}

impl From<HarvestError> for StoreError {
    fn from(error: HarvestError) -> Self {
        Self::Failed(error)
    }
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unavailable(error) => write!(f, "the auth store is unavailable: {error}"),
            Self::Failed(error) => write!(f, "the auth store failed: {error}"),
        }
    }
}

/// The result of a store call.
pub type StoreResult<T> = Result<T, StoreError>;

/// Where tokens and audit rows live.
#[derive(Clone)]
pub enum AuthStore {
    /// The default shard of the Harvest Postgres database.
    Postgres(HarvestDbPool),
    /// The Harvest file of a SQLite app.
    #[cfg(feature = "sqlite")]
    Sqlite(std::sync::Arc<autumn_harvest_sqlite::SqliteAuthStore>),
}

impl AuthStore {
    /// Open one session. On Postgres it holds one connection, so a lookup
    /// and its audit row share it. Drop it before a handler runs (issue
    /// #1827).
    pub async fn session(&self) -> StoreResult<AuthSession> {
        match self {
            Self::Postgres(pool) => Ok(AuthSession::Postgres(Box::new(
                acquire_conn(pool.default_pool())
                    .await
                    .map_err(StoreError::Unavailable)?,
            ))),
            #[cfg(feature = "sqlite")]
            Self::Sqlite(store) => Ok(AuthSession::Sqlite(std::sync::Arc::clone(store))),
        }
    }

    /// Record a use of the token `id`. Best effort, so the caller spawns it.
    pub async fn touch_token(&self, id: Uuid) -> StoreResult<()> {
        match self {
            Self::Postgres(pool) => {
                let mut conn = pool.default_pool().get().await.map_err(|e| {
                    StoreError::Unavailable(AutumnError::service_unavailable_msg(e.to_string()))
                })?;
                Ok(crate::api_token::touch_last_used(&mut conn, id).await?)
            }
            #[cfg(feature = "sqlite")]
            Self::Sqlite(store) => blocking(store, move |s| s.touch_token(id, Utc::now())).await,
        }
    }

    /// The audit rows that match `filters`, newest first, from every shard.
    pub async fn list_audit(&self, filters: &AuditFilters) -> StoreResult<Vec<AuditRecord>> {
        match self {
            Self::Postgres(pool) => {
                let mut records = Vec::new();
                for (_shard, shard_pool) in pool.iter_shards() {
                    let mut conn = acquire_conn(shard_pool)
                        .await
                        .map_err(StoreError::Unavailable)?;
                    records.append(&mut audit::list_audit(&mut conn, filters).await?);
                }
                // Merge the shards: newest first, then by id.
                records.sort_by(|a, b| {
                    b.occurred_at
                        .cmp(&a.occurred_at)
                        .then_with(|| b.id.cmp(&a.id))
                });
                records.truncate(usize::try_from(filters.limit).unwrap_or(usize::MAX));
                Ok(records)
            }
            #[cfg(feature = "sqlite")]
            Self::Sqlite(store) => {
                let filter = sqlite::audit_filter(filters);
                let rows = blocking(store, move |s| s.list_audit(&filter)).await?;
                Ok(rows.into_iter().map(sqlite::to_audit_record).collect())
            }
        }
    }
}

/// One use of the store. See [`AuthStore::session`].
pub enum AuthSession {
    /// One connection to the default shard. It is boxed, as a pool
    /// connection is large.
    Postgres(Box<PoolConn>),
    /// The store of the Harvest file. Each call runs on the blocking pool.
    #[cfg(feature = "sqlite")]
    Sqlite(std::sync::Arc<autumn_harvest_sqlite::SqliteAuthStore>),
}

impl AuthSession {
    /// The token whose stored hash matches `secret`, or `None`.
    pub async fn token_by_secret(&mut self, secret: &str) -> StoreResult<Option<ApiToken>> {
        match self {
            Self::Postgres(conn) => Ok(crate::api_token::lookup_by_secret(conn, secret).await?),
            #[cfg(feature = "sqlite")]
            Self::Sqlite(store) => {
                let hash = crate::api_token::hash_secret(secret);
                let found = blocking(store, move |s| s.token_by_hash(&hash)).await?;
                Ok(found.map(sqlite::to_api_token))
            }
        }
    }

    /// Mint and store a new token.
    pub async fn create_token(
        &mut self,
        name: &str,
        scope: TokenScope,
        expires_at: Option<DateTime<Utc>>,
        created_by: &str,
        tenant: Option<&str>,
    ) -> StoreResult<MintResult> {
        match self {
            Self::Postgres(conn) => Ok(crate::api_token::create_token(
                conn, name, scope, expires_at, created_by, tenant,
            )
            .await?),
            #[cfg(feature = "sqlite")]
            Self::Sqlite(store) => {
                let secret = crate::api_token::mint_secret();
                let hash = crate::api_token::hash_secret(&secret);
                let (name, created_by) = (name.to_owned(), created_by.to_owned());
                let tenant = tenant.map(str::to_owned);
                let row = blocking(store, move |s| {
                    s.insert_token(&autumn_harvest_sqlite::NewToken {
                        id: Uuid::new_v4(),
                        name: &name,
                        token_hash: &hash,
                        scope: scope.as_str(),
                        created_at: Utc::now(),
                        expires_at,
                        created_by: &created_by,
                        tenant: tenant.as_deref(),
                    })
                })
                .await?;
                Ok(MintResult::new(&sqlite::to_api_token(row), secret))
            }
        }
    }

    /// Every token, newest first, as metadata only.
    pub async fn list_tokens(&mut self) -> StoreResult<Vec<crate::api_token::TokenView>> {
        match self {
            Self::Postgres(conn) => Ok(crate::api_token::list_tokens(conn).await?),
            #[cfg(feature = "sqlite")]
            Self::Sqlite(store) => {
                let rows =
                    blocking(store, autumn_harvest_sqlite::SqliteAuthStore::list_tokens).await?;
                Ok(rows
                    .into_iter()
                    .map(|row| crate::api_token::TokenView::from_row(&sqlite::to_api_token(row)))
                    .collect())
            }
        }
    }

    /// Revoke the token `id`. Returns `false` when it is unknown or already
    /// revoked.
    pub async fn revoke_token(&mut self, id: Uuid) -> StoreResult<bool> {
        match self {
            Self::Postgres(conn) => Ok(crate::api_token::revoke_token(conn, id).await?),
            #[cfg(feature = "sqlite")]
            Self::Sqlite(store) => blocking(store, move |s| s.revoke_token(id, Utc::now())).await,
        }
    }

    /// Write one audit row to the control store.
    pub async fn insert_audit(&mut self, record: &NewAuditRecord<'_>) -> StoreResult<()> {
        match self {
            Self::Postgres(conn) => {
                audit::insert_audit(conn, record).await?;
                Ok(())
            }
            #[cfg(feature = "sqlite")]
            Self::Sqlite(store) => {
                let owned = sqlite::OwnedAudit::of(record);
                blocking(store, move |s| s.insert_audit(&owned.row()).map(|_| ())).await
            }
        }
    }
}

/// Run `f` on `store` on the blocking pool.
#[cfg(feature = "sqlite")]
async fn blocking<T: Send + 'static>(
    store: &std::sync::Arc<autumn_harvest_sqlite::SqliteAuthStore>,
    f: impl FnOnce(&autumn_harvest_sqlite::SqliteAuthStore) -> autumn_harvest_sqlite::SqliteResult<T>
    + Send
    + 'static,
) -> StoreResult<T> {
    let store = std::sync::Arc::clone(store);
    tokio::task::spawn_blocking(move || f(&store))
        .await
        .map_err(|e| StoreError::Failed(HarvestError::Config(format!("a store task: {e}"))))?
        .map_err(|e| StoreError::Failed(HarvestError::Config(e.to_string())))
}

/// The conversions between the SQLite rows and the Postgres models.
#[cfg(feature = "sqlite")]
mod sqlite {
    use autumn_harvest::audit::AuditFilters;
    use autumn_harvest::models::{ApiToken, AuditRecord, NewAuditRecord};
    use autumn_harvest_sqlite::{AuditFilter, AuditRow, NewAuditRow, StoredToken};
    use chrono::{DateTime, Utc};

    pub(super) fn to_api_token(row: StoredToken) -> ApiToken {
        let StoredToken {
            id,
            name,
            token_hash,
            scope,
            created_at,
            expires_at,
            last_used_at,
            revoked_at,
            created_by,
            tenant,
        } = row;
        ApiToken {
            id,
            name,
            token_hash,
            scope,
            created_at,
            expires_at,
            last_used_at,
            revoked_at,
            created_by,
            tenant,
        }
    }

    pub(super) fn to_audit_record(row: AuditRow) -> AuditRecord {
        let AuditRow {
            id,
            occurred_at,
            actor,
            operation,
            target_type,
            target_id,
            route_or_command,
            request_id,
            idempotency_key,
            status,
            error_summary,
            shard_id,
            source,
        } = row;
        AuditRecord {
            id,
            occurred_at,
            actor,
            operation,
            target_type,
            target_id,
            route_or_command,
            request_id,
            idempotency_key,
            status,
            error_summary,
            shard_id,
            source,
        }
    }

    pub(super) fn audit_filter(filters: &AuditFilters) -> AuditFilter {
        AuditFilter {
            actor: filters.actor.clone(),
            operation: filters.operation.clone(),
            target_type: filters.target_type.clone(),
            target_id: filters.target_id.clone(),
            status: filters.status.clone(),
            since: filters.since,
            before: filters.before,
            before_id: filters.before_id,
            limit: filters.limit,
        }
    }

    /// An audit row that owns its fields, so it can cross to the blocking
    /// pool.
    pub(super) struct OwnedAudit {
        occurred_at: DateTime<Utc>,
        actor: String,
        operation: String,
        target_type: String,
        target_id: Option<String>,
        route_or_command: String,
        request_id: Option<String>,
        idempotency_key: Option<String>,
        status: String,
        error_summary: Option<String>,
        shard_id: Option<i32>,
        source: String,
    }

    impl OwnedAudit {
        /// Copy `record`. The time is now, as the Postgres column default.
        pub(super) fn of(record: &NewAuditRecord<'_>) -> Self {
            Self {
                occurred_at: Utc::now(),
                actor: record.actor.to_owned(),
                operation: record.operation.to_owned(),
                target_type: record.target_type.to_owned(),
                target_id: record.target_id.map(str::to_owned),
                route_or_command: record.route_or_command.to_owned(),
                request_id: record.request_id.map(str::to_owned),
                idempotency_key: record.idempotency_key.map(str::to_owned),
                status: record.status.to_owned(),
                error_summary: record.error_summary.map(str::to_owned),
                shard_id: record.shard_id,
                source: record.source.to_owned(),
            }
        }

        pub(super) fn row(&self) -> NewAuditRow<'_> {
            NewAuditRow {
                occurred_at: self.occurred_at,
                actor: &self.actor,
                operation: &self.operation,
                target_type: &self.target_type,
                target_id: self.target_id.as_deref(),
                route_or_command: &self.route_or_command,
                request_id: self.request_id.as_deref(),
                idempotency_key: self.idempotency_key.as_deref(),
                status: &self.status,
                error_summary: self.error_summary.as_deref(),
                shard_id: self.shard_id,
                source: &self.source,
            }
        }
    }
}
