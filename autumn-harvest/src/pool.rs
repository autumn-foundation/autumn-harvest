//! Database pool configuration with separate pools and shared ceiling.
//!
//! Design Decision DD-2: Two `deadpool` instances — one for the web server,
//! one for Harvest workers — share a `max_total_connections` ceiling so the
//! combined pool sizes never exceed Postgres `max_connections`.
//!
//! If the sum of requested sizes exceeds the ceiling, both are scaled down
//! proportionally, with any remainder awarded to the web pool (HTTP latency
//! is more user-visible than worker throughput).
//!
//! ## Sharded deployments
//!
//! `worker_pool_size` is applied *per shard*. With `N` shards the process
//! holds `worker_pool_size * N` total worker connections distributed across
//! `N` independent Postgres databases. Operators sizing a multi-shard
//! deployment should account for that multiplication when setting Postgres
//! `max_connections` on each shard host and the process-wide ceiling.

use crate::error::{HarvestError, HarvestResult};

/// Configuration for the Harvest worker database pool.
///
/// The web pool size is configured separately (via `autumn-web`); this struct
/// controls the worker side and the shared ceiling that constrains both.
#[derive(Debug, Clone)]
pub struct HarvestPoolConfig {
    /// Number of connections reserved for Harvest workers.
    pub worker_pool_size: usize,
    /// Maximum combined connections across both pools.
    ///
    /// Default: 95 (i.e. `pg max_connections` 100 minus 5 for superuser/admin).
    pub max_total_connections: usize,
}

impl Default for HarvestPoolConfig {
    fn default() -> Self {
        Self {
            worker_pool_size: 10,
            max_total_connections: 95,
        }
    }
}

impl HarvestPoolConfig {
    /// Validate the pool configuration against a given web pool size.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Config`] if:
    /// - `worker_pool_size` is zero
    /// - `web_pool_size` is zero
    ///
    /// Logs a warning (via `tracing`) if the combined sizes exceed the ceiling
    /// but does not reject — [`compute_pool_sizes`] will scale them down.
    pub fn validate(&self, web_pool_size: usize) -> HarvestResult<()> {
        if self.worker_pool_size == 0 {
            return Err(HarvestError::Config(
                "worker_pool_size must be at least 1".into(),
            ));
        }
        if web_pool_size == 0 {
            return Err(HarvestError::Config(
                "web_pool_size must be at least 1".into(),
            ));
        }

        let combined = web_pool_size + self.worker_pool_size;
        if combined > self.max_total_connections {
            tracing::warn!(
                web_pool_size,
                worker_pool_size = self.worker_pool_size,
                max_total_connections = self.max_total_connections,
                "combined pool sizes ({combined}) exceed ceiling; pools will be scaled down"
            );
        }

        Ok(())
    }
}

/// Scale two pool sizes so their sum does not exceed `ceiling`.
///
/// If `requested_web + requested_worker <= ceiling`, both values are returned
/// unchanged. Otherwise they are scaled down proportionally, with any integer
/// remainder awarded to the web pool (prioritising HTTP latency).
///
/// Both returned values are guaranteed to be at least 1.
#[must_use]
pub fn compute_pool_sizes(
    requested_web: usize,
    requested_worker: usize,
    ceiling: usize,
) -> (usize, usize) {
    // Guarantee at least 1 each — clamp inputs.
    let requested_web = requested_web.max(1);
    let requested_worker = requested_worker.max(1);
    let ceiling = ceiling.max(2); // need room for at least 1 + 1

    let combined = requested_web + requested_worker;
    if combined <= ceiling {
        return (requested_web, requested_worker);
    }

    // Scale proportionally using integer arithmetic to avoid cast warnings.
    // worker gets floor(ceiling * requested_worker / combined), minimum 1.
    let mut scaled_worker = (ceiling * requested_worker / combined).max(1);
    // web gets the rest, minimum 1.
    let mut scaled_web = ceiling.saturating_sub(scaled_worker).max(1);

    // If rounding pushed us over ceiling, trim worker (prioritise web).
    if scaled_web + scaled_worker > ceiling {
        scaled_worker = ceiling.saturating_sub(scaled_web).max(1);
        scaled_web = ceiling.saturating_sub(scaled_worker);
    }

    (scaled_web, scaled_worker)
}

// ---------------------------------------------------------------------------
// Engine connection timeouts (issue #1788)
// ---------------------------------------------------------------------------

/// The fallback acquire bound for a pool with no deadpool `wait` timeout.
pub const DEFAULT_ACQUIRE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// The largest session timeout Postgres accepts: `i32::MAX` milliseconds.
pub const MAX_SESSION_TIMEOUT: std::time::Duration =
    std::time::Duration::from_millis(i32::MAX as u64);

/// The work an engine pool serves. It selects the session timeouts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DbRole {
    /// Claim and persist.
    Hot,
    /// Background scanners.
    Scanner,
    /// Maintenance and operator tooling.
    Maintenance,
}

/// Postgres session timeouts set on each new engine connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionTimeouts {
    /// `statement_timeout`.
    pub statement: std::time::Duration,
    /// `lock_timeout`.
    pub lock: std::time::Duration,
    /// `idle_in_transaction_session_timeout`.
    pub idle_in_transaction: std::time::Duration,
    /// `transaction_timeout`. Needs Postgres 17 or later.
    pub transaction: std::time::Duration,
}

impl SessionTimeouts {
    /// The default timeouts for `role`.
    ///
    /// `transaction` is zero for every role, because Postgres 16 and earlier
    /// reject `transaction_timeout`.
    #[must_use]
    pub const fn for_role(role: DbRole) -> Self {
        let (statement, lock, idle_in_transaction) = match role {
            DbRole::Hot => (30, 5, 300),
            DbRole::Scanner => (300, 30, 300),
            DbRole::Maintenance => (1_800, 60, 600),
        };
        Self {
            statement: std::time::Duration::from_secs(statement),
            lock: std::time::Duration::from_secs(lock),
            idle_in_transaction: std::time::Duration::from_secs(idle_in_transaction),
            transaction: std::time::Duration::ZERO,
        }
    }

    /// The `SET` statements that apply these timeouts, joined by `; `.
    ///
    /// A zero timeout sends no `SET`, so the server or role default stays.
    /// A part of a millisecond rounds up. A `'0ms'` value would switch the
    /// limit off.
    #[must_use]
    pub fn setup_sql(&self) -> String {
        [
            ("statement_timeout", self.statement),
            ("lock_timeout", self.lock),
            (
                "idle_in_transaction_session_timeout",
                self.idle_in_transaction,
            ),
            ("transaction_timeout", self.transaction),
        ]
        .into_iter()
        .filter(|(_, value)| !value.is_zero())
        .map(|(name, value)| {
            let ms = value.as_millis() + u128::from(value.subsec_nanos() % 1_000_000 != 0);
            format!("SET {name} = '{ms}ms'")
        })
        .collect::<Vec<_>>()
        .join("; ")
    }
}

/// deadpool timeouts for an engine pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolTimeouts {
    /// Wait for a free slot.
    pub wait: std::time::Duration,
    /// Open a new connection.
    pub create: std::time::Duration,
    /// Check an idle connection before reuse.
    pub recycle: std::time::Duration,
}

impl Default for PoolTimeouts {
    fn default() -> Self {
        Self {
            wait: DEFAULT_ACQUIRE_TIMEOUT,
            create: std::time::Duration::from_secs(10),
            recycle: std::time::Duration::from_secs(5),
        }
    }
}

/// Pool and session timeouts for every engine role.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EngineDbTimeouts {
    /// deadpool timeouts.
    pub pool: PoolTimeouts,
    /// Session timeouts for [`DbRole::Hot`].
    pub hot: SessionTimeouts,
    /// Session timeouts for [`DbRole::Scanner`].
    pub scanner: SessionTimeouts,
    /// Session timeouts for [`DbRole::Maintenance`].
    pub maintenance: SessionTimeouts,
}

impl Default for EngineDbTimeouts {
    fn default() -> Self {
        Self {
            pool: PoolTimeouts::default(),
            hot: SessionTimeouts::for_role(DbRole::Hot),
            scanner: SessionTimeouts::for_role(DbRole::Scanner),
            maintenance: SessionTimeouts::for_role(DbRole::Maintenance),
        }
    }
}

impl EngineDbTimeouts {
    /// The session timeouts for `role`.
    #[must_use]
    pub const fn session(&self, role: DbRole) -> SessionTimeouts {
        match role {
            DbRole::Hot => self.hot,
            DbRole::Scanner => self.scanner,
            DbRole::Maintenance => self.maintenance,
        }
    }

    /// Reject a zero pool timeout and a session timeout above
    /// [`MAX_SESSION_TIMEOUT`].
    ///
    /// # Errors
    ///
    /// [`HarvestError::Config`] when `wait`, `create` or `recycle` is zero, or
    /// when a session timeout is too large for Postgres.
    pub fn validate(&self) -> HarvestResult<()> {
        for (name, value) in [
            ("wait", self.pool.wait),
            ("create", self.pool.create),
            ("recycle", self.pool.recycle),
        ] {
            if value.is_zero() {
                return Err(HarvestError::Config(format!(
                    "pool {name} timeout must be greater than zero"
                )));
            }
        }
        for (role, session) in [
            (DbRole::Hot, self.hot),
            (DbRole::Scanner, self.scanner),
            (DbRole::Maintenance, self.maintenance),
        ] {
            for (name, value) in [
                ("statement", session.statement),
                ("lock", session.lock),
                ("idle_in_transaction", session.idle_in_transaction),
                ("transaction", session.transaction),
            ] {
                if value > MAX_SESSION_TIMEOUT {
                    return Err(HarvestError::Config(format!(
                        "{role:?} {name} timeout must be at most {MAX_SESSION_TIMEOUT:?}"
                    )));
                }
            }
        }
        Ok(())
    }
}

/// Whether `error` is a `statement_timeout` or `lock_timeout` cancel.
///
/// Postgres rolls back the failed transaction and keeps the session, so the
/// same connection can run the write again.
///
/// The check reads the English message text. Diesel keeps no SQLSTATE in its
/// error, so a server with a non-English `lc_messages` is not detected. The
/// `partition` timeout checks have the same limit.
#[must_use]
pub fn is_session_timeout(error: &HarvestError) -> bool {
    match error {
        HarvestError::Database(msg) => {
            msg.contains("57014")
                || msg.contains("statement timeout")
                || msg.contains("55P03")
                || msg.contains("lock timeout")
        }
        _ => false,
    }
}

/// A pooled engine connection.
#[cfg(feature = "db")]
pub type PooledConn = deadpool::managed::Object<
    diesel_async::pooled_connection::AsyncDieselConnectionManager<diesel_async::AsyncPgConnection>,
>;

/// A builder for an engine pool.
#[cfg(feature = "db")]
pub type DbPoolBuilder = deadpool::managed::PoolBuilder<
    diesel_async::pooled_connection::AsyncDieselConnectionManager<diesel_async::AsyncPgConnection>,
>;

/// Add the engine timeouts for `role` to `builder`.
///
/// Sets the deadpool timeouts and the Tokio runtime that they need. Adds a
/// `post_create` hook that runs [`SessionTimeouts::setup_sql`] once on each new
/// connection.
///
/// # Errors
///
/// [`HarvestError::Config`] when [`EngineDbTimeouts::validate`] rejects
/// `timeouts`. deadpool treats a zero `wait` as "do not wait".
#[cfg(feature = "db")]
pub fn with_engine_timeouts(
    builder: DbPoolBuilder,
    role: DbRole,
    timeouts: &EngineDbTimeouts,
) -> HarvestResult<DbPoolBuilder> {
    timeouts.validate()?;
    let builder = builder
        .runtime(deadpool::Runtime::Tokio1)
        .wait_timeout(Some(timeouts.pool.wait))
        .create_timeout(Some(timeouts.pool.create))
        .recycle_timeout(Some(timeouts.pool.recycle));
    let sql = timeouts.session(role).setup_sql();
    if sql.is_empty() {
        return Ok(builder);
    }
    let sql: std::sync::Arc<str> = sql.into();
    Ok(builder.post_create(deadpool::managed::Hook::async_fn(
        move |conn: &mut diesel_async::AsyncPgConnection, _: &deadpool::managed::Metrics| {
            let sql = std::sync::Arc::clone(&sql);
            Box::pin(async move {
                diesel_async::SimpleAsyncConnection::batch_execute(conn, &sql)
                    .await
                    .map_err(|e| {
                        deadpool::managed::HookError::Message(
                            format!("could not set session timeouts: {e}").into(),
                        )
                    })
            })
        },
    )))
}

/// Build an engine pool for `role`.
///
/// # Errors
///
/// [`HarvestError::Config`] when `timeouts` is not valid or the pool does not
/// build.
#[cfg(feature = "db")]
pub fn engine_pool(
    dsn: impl Into<String>,
    max_size: usize,
    role: DbRole,
    timeouts: &EngineDbTimeouts,
) -> HarvestResult<crate::worker::DbPool> {
    let manager = diesel_async::pooled_connection::AsyncDieselConnectionManager::<
        diesel_async::AsyncPgConnection,
    >::new(dsn);
    with_engine_timeouts(
        deadpool::managed::Pool::builder(manager).max_size(max_size.max(1)),
        role,
        timeouts,
    )?
    .build()
    .map_err(|e| HarvestError::Config(format!("could not build a connection pool: {e}")))
}

/// The acquire bound for `pool`.
#[cfg(feature = "db")]
#[must_use]
pub fn acquire_bound(pool: &crate::worker::DbPool) -> std::time::Duration {
    pool.timeouts()
        .wait
        .filter(|wait| !wait.is_zero())
        .unwrap_or(DEFAULT_ACQUIRE_TIMEOUT)
}

/// Get a connection from `pool` within [`acquire_bound`].
///
/// # Errors
///
/// See [`acquire`].
#[cfg(feature = "db")]
pub async fn acquire_within_pool_bound(pool: &crate::worker::DbPool) -> HarvestResult<PooledConn> {
    acquire(pool, acquire_bound(pool)).await
}

/// Get a connection for a write that must not be lost, such as an executed
/// activity result.
///
/// Makes up to `attempts` tries, each within [`acquire_bound`]. A try that
/// fails early, for example on a refused connect, waits out the rest of its
/// bound before the next try. The tries then always span about `attempts`
/// times the bound. A short pool incident or outage delays the write but does
/// not drop it.
///
/// # Errors
///
/// The error of the last attempt, see [`acquire`].
#[cfg(feature = "db")]
pub async fn acquire_with_retries(
    pool: &crate::worker::DbPool,
    attempts: u32,
) -> HarvestResult<PooledConn> {
    let attempts = attempts.max(1);
    let bound = acquire_bound(pool);
    let mut attempt = 1;
    loop {
        let started = tokio::time::Instant::now();
        match acquire(pool, bound).await {
            Ok(conn) => return Ok(conn),
            Err(error) if attempt < attempts => {
                tracing::warn!(attempt, attempts, error = %error, "pool acquire failed; trying again");
                tokio::time::sleep_until(started + bound).await;
                attempt += 1;
            }
            Err(error) => return Err(error),
        }
    }
}

/// Get a connection from `pool` within `bound`.
///
/// The bound applies to every pool, also to a pool with no deadpool timeouts.
/// A deadpool timeout returns the same typed error.
///
/// # Errors
///
/// [`HarvestError::PoolAcquireTimeout`] when the bound elapses.
/// [`HarvestError::PoolAcquireFailed`] for any other pool error.
#[cfg(feature = "db")]
pub async fn acquire(
    pool: &crate::worker::DbPool,
    bound: std::time::Duration,
) -> HarvestResult<PooledConn> {
    let started = std::time::Instant::now();
    match tokio::time::timeout(bound, pool.get()).await {
        Ok(Ok(conn)) => Ok(conn),
        Ok(Err(deadpool::managed::PoolError::Timeout(_))) => {
            Err(HarvestError::PoolAcquireTimeout {
                waited: started.elapsed(),
            })
        }
        Ok(Err(e)) => Err(HarvestError::PoolAcquireFailed {
            reason: e.to_string(),
        }),
        Err(_elapsed) => Err(HarvestError::PoolAcquireTimeout { waited: bound }),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_config_default_values() {
        let cfg = HarvestPoolConfig::default();
        assert_eq!(cfg.worker_pool_size, 10);
        assert_eq!(cfg.max_total_connections, 95);
    }

    #[test]
    fn pool_config_validates_ceiling() {
        let cfg = HarvestPoolConfig {
            worker_pool_size: 10,
            max_total_connections: 95,
        };
        // combined = 20 + 10 = 30 < 95 => ok
        assert!(cfg.validate(20).is_ok());
    }

    #[test]
    fn pool_config_rejects_zero_worker_pool() {
        let cfg = HarvestPoolConfig {
            worker_pool_size: 0,
            max_total_connections: 95,
        };
        let err = cfg.validate(20).unwrap_err();
        assert!(err.to_string().contains("worker_pool_size"));
    }

    #[test]
    fn pool_config_rejects_zero_web_pool() {
        let cfg = HarvestPoolConfig::default();
        let err = cfg.validate(0).unwrap_err();
        assert!(err.to_string().contains("web_pool_size"));
    }

    #[test]
    fn pool_sizes_respect_ceiling() {
        // 60 + 60 = 120 > 100 ceiling => must scale down
        let (web, worker) = compute_pool_sizes(60, 60, 100);
        assert!(
            web + worker <= 100,
            "combined {web} + {worker} = {} exceeds ceiling 100",
            web + worker
        );
        assert!(web >= 1);
        assert!(worker >= 1);
    }

    #[test]
    fn pool_sizes_unchanged_under_ceiling() {
        let (web, worker) = compute_pool_sizes(20, 10, 95);
        assert_eq!(web, 20);
        assert_eq!(worker, 10);
    }

    #[test]
    fn pool_sizes_remainder_goes_to_web() {
        // 70 + 30 = 100, ceiling = 50 => scale down
        // ratio = 0.5 => web ~35, worker ~15, but floor could leave remainder
        let (web, worker) = compute_pool_sizes(70, 30, 50);
        assert!(web + worker <= 50);
        assert!(web >= 1);
        assert!(worker >= 1);
        // web should get at least as much proportion as worker
        assert!(web >= worker, "web ({web}) should be >= worker ({worker})");
    }

    #[test]
    fn pool_sizes_extreme_imbalance() {
        // 1 web + 99 worker, ceiling 10
        let (web, worker) = compute_pool_sizes(1, 99, 10);
        assert!(web + worker <= 10);
        assert!(web >= 1);
        assert!(worker >= 1);
    }

    #[test]
    fn pool_sizes_exact_fit() {
        let (web, worker) = compute_pool_sizes(50, 50, 100);
        assert_eq!(web, 50);
        assert_eq!(worker, 50);
    }

    #[test]
    fn pool_sizes_minimum_guarantee() {
        // Both request 0 => clamped to 1 each, ceiling 2
        let (web, worker) = compute_pool_sizes(0, 0, 2);
        assert!(web >= 1);
        assert!(worker >= 1);
    }

    // -- Issue #1788: engine connection timeouts --------------------------

    use std::time::Duration;
    #[cfg(feature = "db")]
    use std::time::Instant;

    fn session(statement_ms: u64, lock_ms: u64, idle_ms: u64, tx_ms: u64) -> SessionTimeouts {
        SessionTimeouts {
            statement: Duration::from_millis(statement_ms),
            lock: Duration::from_millis(lock_ms),
            idle_in_transaction: Duration::from_millis(idle_ms),
            transaction: Duration::from_millis(tx_ms),
        }
    }

    #[test]
    fn setup_sql_sets_each_timeout_in_milliseconds() {
        let sql = session(1_500, 250, 60_000, 0).setup_sql();
        assert!(sql.contains("SET statement_timeout = '1500ms'"), "{sql}");
        assert!(sql.contains("SET lock_timeout = '250ms'"), "{sql}");
        assert!(
            sql.contains("SET idle_in_transaction_session_timeout = '60000ms'"),
            "{sql}"
        );
    }

    /// Zero keeps the server or role default. An explicit `0` would switch off
    /// a limit an operator set with `ALTER ROLE`.
    #[test]
    fn setup_sql_skips_a_zero_timeout() {
        let sql = session(0, 250, 0, 0).setup_sql();
        assert!(!sql.contains("statement_timeout"), "{sql}");
        assert!(
            !sql.contains("idle_in_transaction_session_timeout"),
            "{sql}"
        );
        assert!(!sql.contains("transaction_timeout ="), "{sql}");
        assert!(sql.contains("SET lock_timeout = '250ms'"), "{sql}");
    }

    #[test]
    fn setup_sql_sets_transaction_timeout_only_when_asked() {
        let sql = session(0, 0, 0, 90_000).setup_sql();
        assert_eq!(sql, "SET transaction_timeout = '90000ms'");
    }

    /// `'0ms'` would switch the limit off, so a part of a millisecond rounds
    /// up.
    #[test]
    fn setup_sql_rounds_a_part_of_a_millisecond_up() {
        let tiny = SessionTimeouts {
            statement: Duration::from_micros(500),
            lock: Duration::from_micros(1_500),
            ..session(0, 0, 0, 0)
        };
        let sql = tiny.setup_sql();
        assert!(sql.contains("SET statement_timeout = '1ms'"), "{sql}");
        assert!(sql.contains("SET lock_timeout = '2ms'"), "{sql}");
    }

    #[test]
    fn validate_rejects_a_session_timeout_postgres_cannot_hold() {
        let too_long = MAX_SESSION_TIMEOUT + Duration::from_millis(1);
        let cfg = EngineDbTimeouts {
            scanner: SessionTimeouts {
                idle_in_transaction: too_long,
                ..SessionTimeouts::for_role(DbRole::Scanner)
            },
            ..EngineDbTimeouts::default()
        };
        let err = cfg
            .validate()
            .expect_err("Postgres rejects a value this large");
        assert!(matches!(err, HarvestError::Config(_)), "{err}");

        let at_limit = EngineDbTimeouts {
            hot: SessionTimeouts {
                statement: MAX_SESSION_TIMEOUT,
                ..SessionTimeouts::for_role(DbRole::Hot)
            },
            ..EngineDbTimeouts::default()
        };
        assert!(at_limit.validate().is_ok());
    }

    #[test]
    fn is_session_timeout_matches_statement_and_lock_timeouts_only() {
        let statement =
            HarvestError::Database("canceling statement due to statement timeout".into());
        let lock = HarvestError::Database("canceling statement due to lock timeout".into());
        let other = HarvestError::Database("duplicate key value violates unique constraint".into());
        assert!(is_session_timeout(&statement));
        assert!(is_session_timeout(&lock));
        assert!(!is_session_timeout(&other));
        assert!(!is_session_timeout(&HarvestError::NotFound(
            "lock timeout".into()
        )));
    }

    #[test]
    fn role_defaults_grow_from_hot_to_maintenance() {
        let hot = SessionTimeouts::for_role(DbRole::Hot);
        let scanner = SessionTimeouts::for_role(DbRole::Scanner);
        let maintenance = SessionTimeouts::for_role(DbRole::Maintenance);

        assert!(Duration::ZERO < hot.statement);
        assert!(hot.statement < scanner.statement);
        assert!(scanner.statement < maintenance.statement);
        assert!(Duration::ZERO < hot.lock);
        assert!(hot.lock < scanner.lock);
        assert!(scanner.lock < maintenance.lock);
        for role in [hot, scanner, maintenance] {
            assert!(role.idle_in_transaction > Duration::ZERO);
            // PostgreSQL 16 does not know `transaction_timeout`.
            assert_eq!(role.transaction, Duration::ZERO);
        }
    }

    #[test]
    fn validate_rejects_a_zero_pool_timeout() {
        assert!(EngineDbTimeouts::default().validate().is_ok());
        for zeroed in [
            PoolTimeouts {
                wait: Duration::ZERO,
                ..PoolTimeouts::default()
            },
            PoolTimeouts {
                create: Duration::ZERO,
                ..PoolTimeouts::default()
            },
            PoolTimeouts {
                recycle: Duration::ZERO,
                ..PoolTimeouts::default()
            },
        ] {
            let cfg = EngineDbTimeouts {
                pool: zeroed,
                ..EngineDbTimeouts::default()
            };
            let err = cfg
                .validate()
                .expect_err("a zero pool timeout is not valid");
            assert!(matches!(err, HarvestError::Config(_)), "{err}");
        }
    }

    /// A loopback listener that accepts TCP and never answers. A connect to it
    /// hangs in the Postgres handshake, so a pool slot never frees.
    #[cfg(feature = "db")]
    async fn silent_listener() -> (tokio::net::TcpListener, String) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind an ephemeral loopback port");
        let addr = listener.local_addr().expect("listener has a local address");
        (listener, format!("postgres://silent@{addr}/silent"))
    }

    #[cfg(feature = "db")]
    fn timeouts_with_pool(wait_ms: u64) -> EngineDbTimeouts {
        let bound = Duration::from_millis(wait_ms);
        EngineDbTimeouts {
            pool: PoolTimeouts {
                wait: bound,
                create: bound,
                recycle: bound,
            },
            ..EngineDbTimeouts::default()
        }
    }

    #[cfg(feature = "db")]
    #[tokio::test]
    async fn acquire_bound_reads_the_pool_wait_timeout() {
        let (_listener, dsn) = silent_listener().await;
        let pool = engine_pool(dsn.clone(), 1, DbRole::Hot, &timeouts_with_pool(750))
            .expect("pool builds without connecting");
        assert_eq!(acquire_bound(&pool), Duration::from_millis(750));

        let manager = diesel_async::pooled_connection::AsyncDieselConnectionManager::<
            diesel_async::AsyncPgConnection,
        >::new(dsn);
        let plain = deadpool::managed::Pool::builder(manager)
            .max_size(1)
            .build()
            .expect("pool builds without connecting");
        assert_eq!(acquire_bound(&plain), DEFAULT_ACQUIRE_TIMEOUT);
    }

    /// A pool with no deadpool timeouts still returns within the bound.
    #[cfg(feature = "db")]
    #[tokio::test]
    async fn acquire_returns_a_typed_timeout_within_the_bound() {
        let (_listener, dsn) = silent_listener().await;
        let manager = diesel_async::pooled_connection::AsyncDieselConnectionManager::<
            diesel_async::AsyncPgConnection,
        >::new(dsn);
        let pool = deadpool::managed::Pool::builder(manager)
            .max_size(1)
            .build()
            .expect("pool builds without connecting");

        let bound = Duration::from_millis(200);
        let started = Instant::now();
        let outcome = tokio::time::timeout(Duration::from_secs(5), acquire(&pool, bound))
            .await
            .expect("acquire must not hang past its bound");
        let err = outcome
            .err()
            .expect("a silent database yields no connection");
        assert!(err.is_pool_acquire_timeout(), "{err}");
        assert!(matches!(err, HarvestError::PoolAcquireTimeout { waited } if waited == bound));
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "{:?}",
            started.elapsed()
        );
    }

    /// A connect that fails at once is a typed acquire failure, not a plain
    /// database error. The worker then releases the activity claim.
    ///
    /// The listener accepts and closes each connection, so the handshake
    /// fails at once on every OS. A closed port does not: Windows retries a
    /// refused connect for about two seconds.
    #[cfg(feature = "db")]
    #[tokio::test]
    async fn acquire_types_an_immediate_connect_failure() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind an ephemeral loopback port");
        let addr = listener.local_addr().expect("listener has a local address");
        let closer = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                drop(stream);
            }
        });
        let pool = engine_pool(
            format!("postgres://closed@{addr}/closed"),
            1,
            DbRole::Hot,
            &timeouts_with_pool(10_000),
        )
        .expect("pool builds without connecting");
        let err = acquire(&pool, Duration::from_secs(10))
            .await
            .err()
            .expect("the listener closes every connection");
        closer.abort();
        assert!(
            matches!(err, HarvestError::PoolAcquireFailed { .. }),
            "{err}"
        );
        assert!(err.is_pool_acquire_failure(), "{err}");
        assert!(
            HarvestError::PoolAcquireTimeout {
                waited: Duration::from_secs(1)
            }
            .is_pool_acquire_failure()
        );
    }

    /// A deadpool timeout maps to the same typed error.
    #[cfg(feature = "db")]
    #[tokio::test]
    async fn acquire_maps_a_deadpool_timeout_to_the_typed_error() {
        let (_listener, dsn) = silent_listener().await;
        let pool = engine_pool(dsn, 1, DbRole::Hot, &timeouts_with_pool(150))
            .expect("pool builds without connecting");

        let started = Instant::now();
        let outcome = tokio::time::timeout(
            Duration::from_secs(5),
            acquire(&pool, Duration::from_secs(60)),
        )
        .await
        .expect("the pool's own timeout must fire first");
        let err = outcome
            .err()
            .expect("a silent database yields no connection");
        assert!(err.is_pool_acquire_timeout(), "{err}");
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "{:?}",
            started.elapsed()
        );
    }

    /// Each attempt is bounded, and the last error is the typed timeout.
    #[cfg(feature = "db")]
    #[tokio::test]
    async fn acquire_with_retries_makes_each_attempt_then_gives_up() {
        let (_listener, dsn) = silent_listener().await;
        let pool = engine_pool(dsn, 1, DbRole::Hot, &timeouts_with_pool(100))
            .expect("pool builds without connecting");

        let started = Instant::now();
        let outcome = tokio::time::timeout(Duration::from_secs(10), acquire_with_retries(&pool, 3))
            .await
            .expect("three bounded attempts must end");
        let err = outcome
            .err()
            .expect("a silent database yields no connection");
        assert!(err.is_pool_acquire_timeout(), "{err}");
        assert!(
            started.elapsed() >= Duration::from_millis(300),
            "{:?}",
            started.elapsed()
        );
    }

    /// A refused connect fails at once. The retries still spread over the
    /// bound, so they can ride out a short outage.
    #[cfg(feature = "db")]
    #[tokio::test]
    async fn acquire_with_retries_waits_out_an_immediate_failure() {
        let pool = engine_pool(
            "postgres://refused@127.0.0.1:1/refused",
            1,
            DbRole::Hot,
            &timeouts_with_pool(100),
        )
        .expect("pool builds without connecting");

        let started = Instant::now();
        let outcome = tokio::time::timeout(Duration::from_secs(10), acquire_with_retries(&pool, 3))
            .await
            .expect("three bounded attempts must end");
        assert!(outcome.is_err(), "nothing listens on port 1");
        assert!(
            started.elapsed() >= Duration::from_millis(200),
            "{:?}",
            started.elapsed()
        );
    }

    #[cfg(feature = "db")]
    #[test]
    fn with_engine_timeouts_rejects_invalid_timeouts() {
        let manager = diesel_async::pooled_connection::AsyncDieselConnectionManager::<
            diesel_async::AsyncPgConnection,
        >::new("postgres://unused@127.0.0.1:1/none");
        let outcome = with_engine_timeouts(
            deadpool::managed::Pool::builder(manager),
            DbRole::Hot,
            &timeouts_with_pool(0),
        );
        assert!(matches!(outcome, Err(HarvestError::Config(_))));
    }

    #[cfg(feature = "db")]
    #[test]
    fn engine_pool_rejects_invalid_timeouts() {
        let err = engine_pool(
            "postgres://unused@127.0.0.1:1/none",
            1,
            DbRole::Hot,
            &timeouts_with_pool(0),
        )
        .err()
        .expect("a zero wait is not valid");
        assert!(matches!(err, HarvestError::Config(_)), "{err}");
    }
}
