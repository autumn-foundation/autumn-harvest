//! Wake a closed fleet tenant when its Harvest has due work.
//!
//! The fleet closes an idle tenant, and a closed tenant runs no driver. So
//! each tenant driver reports `SqliteRuntime::next_wake_at` to a book. The
//! sweep writes the book to the control database, and opens each owned,
//! closed tenant whose time has come.
//!
//! ```text
//! tenant driver: idle, value changed ──► WakeBook (memory)
//! tenant driver: stop ─────────────────► WakeBook, then flush
//! sweep, each tick:
//!   flush the book ──► harvest_tenant_wakes (control database)
//!   read the due rows ──► owned, closed, not in flight
//!   open_background ──► Settle (the driver reaches its idle point)
//!   drop the lease ──► close_if_unused
//! ```
//!
//! A host wakes only the tenants of the slot ranges that it owns. The
//! Harvest file advances a schedule slot in its fire transaction. So one
//! wake fires a slot once, and no second host fires it again.
//!
//! The design record is `docs/design/DESIGN-2650.md`.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use autumn_harvest_sqlite::SqliteRuntime;
use autumn_web::db::RuntimeConnection;
use autumn_web::db::fleet::FleetError;
use autumn_web::fleet_layout::FleetDbKey;
use diesel::sql_types::{BigInt, Nullable, Text};
use diesel_async::pooled_connection::deadpool::Pool;
use diesel_async::{RunQueryDsl as _, SimpleAsyncConnection as _};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::fleet::HarvestFleet;

/// DDL of the wake table on the control database. Times are epoch
/// milliseconds. A `NULL` time means no deadline.
pub const SCHEMA_SQL: &str = "\
CREATE TABLE IF NOT EXISTS harvest_tenant_wakes (
    tenant_key   TEXT PRIMARY KEY,
    next_wake_at INTEGER
);
CREATE INDEX IF NOT EXISTS idx_harvest_tenant_wakes_due
    ON harvest_tenant_wakes (next_wake_at)
    WHERE next_wake_at IS NOT NULL;";

/// Set the wake time of one tenant.
const UPSERT_SQL: &str = "INSERT INTO harvest_tenant_wakes (tenant_key, next_wake_at) \
     VALUES (?, ?) ON CONFLICT (tenant_key) DO UPDATE SET next_wake_at = excluded.next_wake_at";

/// Remove the row of a deleted tenant.
const DELETE_SQL: &str = "DELETE FROM harvest_tenant_wakes WHERE tenant_key = ?";

/// The due rows, earliest first. The partial index answers it.
const DUE_SQL: &str = "SELECT tenant_key, next_wake_at FROM harvest_tenant_wakes \
     WHERE next_wake_at IS NOT NULL AND next_wake_at <= ? ORDER BY next_wake_at LIMIT ?";

/// The most due rows that one sweep reads.
const DUE_LIMIT: i64 = 64;

/// The most tenants that one host wakes at the same time. A background
/// open never goes past `max_open` too.
const MAX_IN_FLIGHT: usize = 8;

/// How long a wake keeps its tenant open while the driver works. A
/// workflow that never blocks then lets the tenant close.
const WAKE_BUDGET: Duration = Duration::from_secs(300);

/// When a driver tries again a deadline that it could not clear.
pub const STUCK_RETRY_MS: i64 = 60_000;

/// How long a stopping driver waits for its flush.
const EXIT_FLUSH_BUDGET: Duration = Duration::from_secs(5);

/// One change that waits for a flush.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Pending {
    /// Set the wake time. `None` is no deadline.
    Set(Option<i64>),
    /// Remove the row.
    Forget,
}

/// The wake times that wait for the control database.
///
/// A driver never waits for the control database while it runs. It writes
/// here, and the sweep flushes. A stopping driver flushes once, so the
/// close leaves its final value on disk.
pub struct WakeBook {
    pool: Pool<RuntimeConnection>,
    pending: Mutex<HashMap<String, Pending>>,
    /// One flush at a time. A flush takes the book under this lock, so an
    /// older value never lands after a newer one.
    flush_turn: tokio::sync::Mutex<()>,
}

impl std::fmt::Debug for WakeBook {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WakeBook").finish_non_exhaustive()
    }
}

impl WakeBook {
    /// Create the wake table on the control database, and return its book.
    ///
    /// # Errors
    ///
    /// Returns a message when no connection is available or the DDL fails.
    pub async fn open(pool: Pool<RuntimeConnection>) -> Result<Self, String> {
        let mut conn = pool
            .get()
            .await
            .map_err(|error| format!("wake table: no connection: {error}"))?;
        conn.batch_execute(SCHEMA_SQL)
            .await
            .map_err(|error| format!("wake table: {error}"))?;
        drop(conn);
        Ok(Self {
            pool,
            pending: Mutex::new(HashMap::new()),
            flush_turn: tokio::sync::Mutex::new(()),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Pending>> {
        self.pending.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Record the wake time of `tenant`. `None` is no deadline.
    pub fn record(&self, tenant: &FleetDbKey, at: Option<i64>) {
        self.lock().insert(tenant.name(), Pending::Set(at));
    }

    /// Remove the row of `tenant`.
    pub fn forget(&self, tenant: &FleetDbKey) {
        self.lock().insert(tenant.name(), Pending::Forget);
    }

    /// Write each pending change to the control database.
    ///
    /// A failed write puts back each change that no newer change replaced,
    /// so the next flush tries it again.
    ///
    /// # Errors
    ///
    /// Returns a message when the write fails.
    pub async fn flush(&self) -> Result<(), String> {
        let _turn = self.flush_turn.lock().await;
        let batch = std::mem::take(&mut *self.lock());
        if batch.is_empty() {
            return Ok(());
        }
        let written = write(&self.pool, &batch).await;
        if written.is_err() {
            let mut pending = self.lock();
            for (key, change) in batch {
                pending.entry(key).or_insert(change);
            }
        }
        written
    }

    /// The due rows at `now`, earliest first.
    pub(crate) async fn due_names(&self, now: i64) -> Result<Vec<String>, String> {
        #[derive(diesel::QueryableByName)]
        struct Row {
            #[diesel(sql_type = Text)]
            tenant_key: String,
        }
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|error| format!("wake read: no connection: {error}"))?;
        let rows: Vec<Row> = diesel::sql_query(DUE_SQL)
            .bind::<BigInt, _>(now)
            .bind::<BigInt, _>(DUE_LIMIT)
            .load(&mut *conn)
            .await
            .map_err(|error| format!("wake read: {error}"))?;
        Ok(rows.into_iter().map(|row| row.tenant_key).collect())
    }
}

/// Write `batch` in one transaction, so a flush costs one commit.
async fn write(
    pool: &Pool<RuntimeConnection>,
    batch: &HashMap<String, Pending>,
) -> Result<(), String> {
    let mut conn = pool
        .get()
        .await
        .map_err(|error| format!("wake write: no connection: {error}"))?;
    conn.batch_execute("BEGIN IMMEDIATE")
        .await
        .map_err(|error| format!("wake write: {error}"))?;
    let mut written = Ok(());
    for (key, change) in batch {
        let query = match change {
            Pending::Set(at) => {
                diesel::sql_query(UPSERT_SQL)
                    .bind::<Text, _>(key)
                    .bind::<Nullable<BigInt>, _>(*at)
                    .execute(&mut *conn)
                    .await
            }
            Pending::Forget => {
                diesel::sql_query(DELETE_SQL)
                    .bind::<Text, _>(key)
                    .execute(&mut *conn)
                    .await
            }
        };
        if let Err(error) = query {
            written = Err(format!("wake write: {error}"));
            break;
        }
    }
    let end = if written.is_ok() {
        "COMMIT"
    } else {
        "ROLLBACK"
    };
    let ended = conn
        .batch_execute(end)
        .await
        .map_err(|error| format!("wake write: {error}"));
    written.and(ended)
}

/// The wake time to report at an idle point.
///
/// `raw` is `next_wake_at` after a pass that made no progress. That pass
/// started at `pass_started`. A value due at that start is stuck: the pass
/// could not clear it. An example is a timer of a run that fails on each
/// drive. A report of it as is would make the sweep open the tenant on each
/// tick. So it is reported as `now + STUCK_RETRY_MS`.
#[must_use]
pub const fn idle_value(raw: Option<i64>, pass_started: i64, now: i64) -> Option<i64> {
    match raw {
        Some(at) if at <= pass_started => Some(now.saturating_add(STUCK_RETRY_MS)),
        other => other,
    }
}

/// The wake reports of one tenant driver.
pub struct WakeReport {
    book: Arc<WakeBook>,
    tenant: FleetDbKey,
    /// The last raw value and the value reported for it.
    last: Option<(Option<i64>, Option<i64>)>,
    /// A command ran or a pass made progress since the last idle point.
    dirty: bool,
}

impl WakeReport {
    pub const fn new(book: Arc<WakeBook>, tenant: FleetDbKey) -> Self {
        Self {
            book,
            tenant,
            last: None,
            // The first idle point always reports.
            dirty: true,
        }
    }

    /// The driver ran a command or a pass that made progress.
    pub const fn touch(&mut self) {
        self.dirty = true;
    }

    /// The driver is idle after a pass that started at `pass_started`.
    /// Record the value when it changed.
    pub fn idle(&mut self, runtime: &SqliteRuntime, pass_started: i64) {
        let raw = match runtime.next_wake_at() {
            Ok(raw) => raw,
            Err(error) => {
                tracing::warn!(tenant = %self.tenant, %error, "cannot read the next wake time");
                return;
            }
        };
        self.dirty = false;
        let stuck = raw.is_some_and(|at| at <= pass_started);
        // A stuck value reports once. A value that turns stuck reports again.
        if let Some((last_raw, last_value)) = self.last
            && last_raw == raw
            && (!stuck || last_value != raw)
        {
            return;
        }
        let value = idle_value(raw, pass_started, now_ms());
        self.last = Some((raw, value));
        self.book.record(&self.tenant, value);
    }

    /// The driver stops. Record the final value, then flush the book.
    ///
    /// `raw` is `next_wake_at` at the stop. A driver that stops with work in
    /// flight reports now, so the next sweep opens the tenant and finishes
    /// it. Otherwise the last idle value stands.
    pub async fn exit(&self, raw: Option<i64>) {
        if self.dirty {
            self.book.record(&self.tenant, Some(now_ms()));
        } else if self.last.is_none_or(|(last_raw, _)| last_raw != raw) {
            self.book.record(&self.tenant, raw);
        }
        match tokio::time::timeout(EXIT_FLUSH_BUDGET, self.book.flush()).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                tracing::warn!(tenant = %self.tenant, %error, "cannot write the wake time");
            }
            Err(_) => tracing::warn!(
                tenant = %self.tenant,
                "the wake time write timed out: the sweep writes it later"
            ),
        }
    }
}

/// The wall clock in epoch milliseconds.
fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// The sweep that wakes closed tenants.
pub struct WakeSweep {
    fleet: HarvestFleet,
    book: Arc<WakeBook>,
    in_flight: Arc<Mutex<HashSet<FleetDbKey>>>,
}

impl WakeSweep {
    pub fn new(fleet: HarvestFleet, book: Arc<WakeBook>) -> Self {
        Self {
            fleet,
            book,
            in_flight: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    /// Run a sweep each `every`, until `stop`. A last flush runs at the
    /// stop.
    pub fn spawn(self, every: Duration, stop: CancellationToken) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                self.sweep_once().await;
                tokio::select! {
                    () = stop.cancelled() => break,
                    () = tokio::time::sleep(every) => {}
                }
            }
            if let Err(error) = self.book.flush().await {
                tracing::warn!(%error, "cannot write the last tenant wake times");
            }
        })
    }

    /// Flush the book, then wake each due tenant that this host owns.
    /// Returns how many wakes began.
    pub async fn sweep_once(&self) -> usize {
        if let Err(error) = self.book.flush().await {
            tracing::warn!(%error, "cannot write the tenant wake times");
        }
        let due = match self.book.due_names(now_ms()).await {
            Ok(due) => due,
            Err(error) => {
                tracing::warn!(%error, "cannot read the tenant wake times");
                return 0;
            }
        };
        let mut began = 0;
        for name in due {
            let fleet = self.fleet.database_fleet();
            let key = match fleet.key_for_name(&name) {
                Ok(key) => key,
                Err(error) => {
                    // A row of another fleet mode can never wake.
                    tracing::debug!(tenant = %name, %error, "a wake row names no tenant");
                    continue;
                }
            };
            // Another host serves this key. Its row stays for that host.
            if !fleet.owns(&key) || self.fleet.has_driver(&key) {
                continue;
            }
            let Some(guard) = InFlight::claim(&self.in_flight, &key) else {
                continue;
            };
            began += 1;
            let fleet = self.fleet.clone();
            let book = Arc::clone(&self.book);
            tokio::spawn(async move {
                wake(&fleet, &book, &key).await;
                drop(guard);
            });
        }
        began
    }
}

/// One wake that runs. It leaves the set when it drops.
struct InFlight {
    set: Arc<Mutex<HashSet<FleetDbKey>>>,
    key: FleetDbKey,
}

impl InFlight {
    /// Claim `key`, or `None` when it is in flight or the set is full.
    fn claim(set: &Arc<Mutex<HashSet<FleetDbKey>>>, key: &FleetDbKey) -> Option<Self> {
        let claimed = {
            let mut keys = set.lock().unwrap_or_else(PoisonError::into_inner);
            keys.len() < MAX_IN_FLIGHT && keys.insert(key.clone())
        };
        claimed.then(|| Self {
            set: Arc::clone(set),
            key: key.clone(),
        })
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        self.set
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&self.key);
    }
}

/// Open `key` in the background, run its driver until it is idle, then let
/// the fleet close it.
async fn wake(fleet: &HarvestFleet, book: &WakeBook, key: &FleetDbKey) {
    let background = match fleet.database_fleet().open_background(key).await {
        Ok(Some(background)) => background,
        // No room now. The next sweep tries again.
        Ok(None) => return,
        Err(FleetError::NotFound { .. }) => {
            // This host owns the key, and the tenant has no file.
            book.forget(key);
            return;
        }
        Err(error) => {
            tracing::debug!(tenant = %key, %error, "cannot wake the tenant now");
            return;
        }
    };
    let opened = background.opened;
    match fleet.handle_of(background.db).await {
        Ok(handle) => {
            if tokio::time::timeout(WAKE_BUDGET, handle.settle())
                .await
                .is_err()
            {
                tracing::info!(tenant = %key, "the woken tenant is still busy: the fleet closes it");
            }
            drop(handle);
        }
        Err(error) => tracing::warn!(tenant = %key, %error, "cannot start the woken tenant"),
    }
    if opened {
        let _ = fleet.database_fleet().close_if_unused(key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_future_value_is_reported_as_is() {
        assert_eq!(idle_value(Some(2_000), 1_000, 1_500), Some(2_000));
        assert_eq!(idle_value(None, 1_000, 1_500), None);
    }

    /// A value due before the idle pass started is stuck. It is tried
    /// again later, so the sweep does not open the tenant on each tick.
    #[test]
    fn a_stuck_value_is_tried_again_later() {
        assert_eq!(
            idle_value(Some(900), 1_000, 1_500),
            Some(1_500 + STUCK_RETRY_MS)
        );
        assert_eq!(
            idle_value(Some(1_000), 1_000, 1_500),
            Some(1_500 + STUCK_RETRY_MS)
        );
    }

    /// A value that came due during the pass is not stuck.
    #[test]
    fn a_value_due_after_the_pass_start_is_not_stuck() {
        assert_eq!(idle_value(Some(1_001), 1_000, 1_500), Some(1_001));
    }

    fn control(dir: &std::path::Path) -> Pool<RuntimeConnection> {
        autumn_web::db::create_pool(&autumn_web::config::DatabaseConfig {
            url: Some(format!("sqlite://{}", dir.join("control.db").display())),
            ..autumn_web::config::DatabaseConfig::default()
        })
        .expect("pool")
        .expect("a url gives a pool")
    }

    fn tenant(name: &str) -> FleetDbKey {
        FleetDbKey::Tenant {
            id: autumn_web::fleet_layout::TenantDbId::parse(name).expect("id"),
            slot: autumn_web::sharding::slot_for_key(name.into()).0,
        }
    }

    /// A flush writes each change. A due read returns the due rows only,
    /// earliest first. A forget removes the row.
    #[tokio::test]
    async fn the_book_writes_and_reads_the_due_rows() {
        let dir = tempfile::tempdir().unwrap();
        let book = WakeBook::open(control(dir.path())).await.expect("open");
        let (acme, bee, cat) = (tenant("acme"), tenant("bee"), tenant("cat"));
        book.record(&acme, Some(2_000));
        book.record(&bee, Some(1_000));
        book.record(&cat, None);
        assert!(
            book.due_names(5_000).await.unwrap().is_empty(),
            "not flushed"
        );

        book.flush().await.expect("flush");
        assert_eq!(
            book.due_names(5_000).await.unwrap(),
            vec![bee.name(), acme.name()]
        );
        assert_eq!(book.due_names(1_500).await.unwrap(), vec![bee.name()]);

        // The newest change of a key wins.
        book.record(&bee, Some(9_000));
        book.forget(&acme);
        book.flush().await.expect("flush");
        assert!(book.due_names(5_000).await.unwrap().is_empty());
        assert_eq!(book.due_names(9_000).await.unwrap(), vec![bee.name()]);
    }

    /// A driver that stops with work in flight reports now. A clean stop
    /// reports the raw value when it changed since the last idle point.
    #[tokio::test]
    async fn a_dirty_stop_reports_now() {
        let dir = tempfile::tempdir().unwrap();
        let book = Arc::new(WakeBook::open(control(dir.path())).await.expect("open"));
        let acme = tenant("acme");
        let before = now_ms();
        let report = WakeReport::new(Arc::clone(&book), acme.clone());
        report.exit(Some(before + 600_000)).await;
        let due = book.due_names(now_ms()).await.unwrap();
        assert_eq!(due, vec![acme.name()], "a dirty stop is due now");

        let mut report = WakeReport::new(Arc::clone(&book), acme.clone());
        report.dirty = false;
        report.exit(Some(before + 600_000)).await;
        assert!(book.due_names(now_ms()).await.unwrap().is_empty());
        assert_eq!(
            book.due_names(before + 600_000).await.unwrap(),
            vec![acme.name()]
        );
    }

    /// The DDL runs twice with no error, so each boot can run it.
    #[tokio::test]
    async fn the_schema_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        WakeBook::open(control(dir.path())).await.expect("first");
        WakeBook::open(control(dir.path())).await.expect("second");
    }

    /// A full set or a key in flight refuses a claim. A drop frees it.
    #[test]
    fn a_key_is_in_flight_once() {
        let set = Arc::new(Mutex::new(HashSet::new()));
        let acme = tenant("acme");
        let first = InFlight::claim(&set, &acme).expect("first");
        assert!(InFlight::claim(&set, &acme).is_none());
        drop(first);
        assert!(InFlight::claim(&set, &acme).is_some());

        let set = Arc::new(Mutex::new(HashSet::new()));
        let held: Vec<_> = (0..MAX_IN_FLIGHT)
            .map(|n| InFlight::claim(&set, &tenant(&format!("t{n}"))).expect("room"))
            .collect();
        assert!(InFlight::claim(&set, &tenant("late")).is_none());
        drop(held);
    }
}
