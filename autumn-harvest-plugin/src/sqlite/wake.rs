//! Wake a closed fleet tenant when its Harvest has due work.
//!
//! The fleet closes an idle tenant, and a closed tenant runs no driver. So
//! each tenant driver records `SqliteRuntime::next_wake_at` in a book. The
//! sweep writes the book to the control database, and opens each owned,
//! closed tenant whose time has come.
//!
//! ```text
//! tenant driver: work starts ──────────► WakeBook: now
//! tenant driver: idle, value changed ──► WakeBook: next_wake_at
//! tenant driver: stop ─────────────────► WakeBook: final value
//! sweep, each tick:
//!   flush the book ──► harvest_tenant_wakes (control database)
//!   read the due rows of the owned slot ranges ──► closed, not in flight
//!   open_background ──► Settle (the driver reaches its idle point)
//!   drop the lease ──► close_if_unused
//! ```
//!
//! A driver never waits for the control database. A crash loses at most one
//! tick of records. A driver records now when work starts, so a crash during
//! work leaves a due row.
//!
//! A host wakes only the tenants of the slot ranges that it owns. The
//! Harvest file advances a schedule slot in its fire transaction. So one
//! wake fires a slot once, and no second host fires it again.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use autumn_harvest_sqlite::SqliteRuntime;
use autumn_web::db::RuntimeConnection;
use autumn_web::db::fleet::{DatabaseFleet, FleetError};
use autumn_web::fleet_layout::FleetDbKey;
use diesel::sql_types::{BigInt, Nullable, Text};
use diesel_async::pooled_connection::deadpool::Pool;
use diesel_async::{RunQueryDsl as _, SimpleAsyncConnection as _};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::fleet::HarvestFleet;

/// DDL of the wake table on the control database.
///
/// Times are epoch milliseconds. A `NULL` time means no deadline.
/// `routing_slot` lets a host read the rows of its own slot ranges only.
/// `epoch` is the ownership epoch of the last write, or 0 in a fleet with no
/// ownership.
pub const SCHEMA_SQL: &str = "\
CREATE TABLE IF NOT EXISTS harvest_tenant_wakes (
    tenant_key   TEXT PRIMARY KEY,
    routing_slot INTEGER NOT NULL,
    next_wake_at INTEGER,
    epoch        INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_harvest_tenant_wakes_due
    ON harvest_tenant_wakes (next_wake_at, tenant_key)
    WHERE next_wake_at IS NOT NULL;";

/// Set the wake time of one tenant.
///
/// An ownership epoch only grows. A write under an older epoch than the row
/// changes nothing. So a host that lost its range during a flush never
/// overwrites the row of the new owner.
const UPSERT_SQL: &str = "INSERT INTO harvest_tenant_wakes \
     (tenant_key, routing_slot, next_wake_at, epoch) VALUES (?, ?, ?, ?) \
     ON CONFLICT (tenant_key) DO UPDATE \
     SET next_wake_at = excluded.next_wake_at, epoch = excluded.epoch \
     WHERE excluded.epoch >= harvest_tenant_wakes.epoch";

/// Remove the row of a deleted tenant, under the same epoch rule.
const DELETE_SQL: &str = "DELETE FROM harvest_tenant_wakes WHERE tenant_key = ? AND epoch <= ?";

/// The due rows after a cursor, earliest first. The partial index answers
/// it.
///
/// The read pages with the cursor `(next_wake_at, tenant_key)`. So a row
/// that this host skips never hides a later row. With `ranges` slot ranges,
/// the read keeps the rows of those ranges only. The rows of another host
/// then never fill a page.
fn due_sql(ranges: usize) -> String {
    let mut sql = String::from(
        "SELECT tenant_key, next_wake_at FROM harvest_tenant_wakes \
         WHERE next_wake_at IS NOT NULL AND next_wake_at <= ? \
         AND (next_wake_at, tenant_key) > (?, ?)",
    );
    if ranges > 0 {
        let any = vec!["routing_slot BETWEEN ? AND ?"; ranges].join(" OR ");
        sql.push_str(" AND (");
        sql.push_str(&any);
        sql.push(')');
    }
    sql.push_str(" ORDER BY next_wake_at, tenant_key LIMIT ?");
    sql
}

/// The due rows of one page.
const DUE_PAGE: i64 = 64;

/// The most pages that one sweep reads. A page holds only the rows of this
/// host. The sweep skips a row only while its tenant is open or in flight,
/// so `max_open` bounds the skipped rows.
const DUE_PAGES: usize = 64;

/// The most tenants that one host wakes at the same time. A background
/// open never goes past `max_open` too.
const MAX_IN_FLIGHT: usize = 8;

/// How long a wake keeps its lease while the driver works. The sweep then
/// asks the fleet to close the tenant. A tenant that a request uses stays
/// open until the fleet closes it as idle.
const WAKE_BUDGET: Duration = Duration::from_secs(300);

/// When a driver tries again a deadline that it could not clear, and when
/// the sweep tries again a wake that failed.
pub const STUCK_RETRY_MS: i64 = 60_000;

/// One change that waits for a flush.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Pending {
    /// Set the wake time of a tenant of this routing slot. `None` is no
    /// deadline.
    Set { slot: u16, at: Option<i64> },
    /// Remove the row.
    Forget,
}

/// A due row: its wake time and its tenant key.
pub type DueRow = (i64, String);

/// The wake times that wait for the control database.
///
/// A driver never waits for the control database. It writes here, and the
/// sweep flushes on each tick and once at its stop.
pub struct WakeBook {
    pool: Pool<RuntimeConnection>,
    /// The fleet of this host. A flush writes only the keys that this host
    /// owns, so a host that lost a range never overwrites the new owner.
    fleet: Option<DatabaseFleet>,
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
    /// `fleet` limits each flush to the keys that this host owns. `None`
    /// writes every key.
    ///
    /// # Errors
    ///
    /// Returns a message when no connection is available or the DDL fails.
    pub async fn open(
        pool: Pool<RuntimeConnection>,
        fleet: Option<DatabaseFleet>,
    ) -> Result<Self, String> {
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
            fleet,
            pending: Mutex::new(HashMap::new()),
            flush_turn: tokio::sync::Mutex::new(()),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Pending>> {
        self.pending.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Record the wake time of `tenant`. `None` is no deadline.
    pub fn record(&self, tenant: &FleetDbKey, at: Option<i64>) {
        let slot = tenant.routing_slot();
        self.lock().insert(tenant.name(), Pending::Set { slot, at });
    }

    /// Ask the next sweep to try `tenant` again after the retry delay.
    pub fn defer(&self, tenant: &FleetDbKey) {
        self.record(tenant, Some(now_ms().saturating_add(STUCK_RETRY_MS)));
    }

    /// Remove the row of `tenant`.
    pub fn forget(&self, tenant: &FleetDbKey) {
        self.lock().insert(tenant.name(), Pending::Forget);
    }

    /// The ownership epoch under which this host writes the row of `name`.
    /// `None` when this host does not own the key. A fleet with no
    /// ownership writes under epoch 0.
    fn epoch_of(&self, name: &str) -> Option<i64> {
        let Some(fleet) = &self.fleet else {
            return Some(0);
        };
        let key = fleet.key_for_name(name).ok()?;
        fleet.ownership().map_or(Some(0), |ownership| {
            ownership
                .epoch_for(key.routing_slot())
                .map(|epoch| i64::try_from(epoch.get()).unwrap_or(i64::MAX))
        })
    }

    /// Write each pending change to the control database.
    ///
    /// A failed or a dropped write puts back each change that no newer
    /// change replaced, so the next flush tries it again. A change of a key
    /// that this host no longer owns is dropped.
    ///
    /// # Errors
    ///
    /// Returns a message when the write fails.
    pub async fn flush(&self) -> Result<(), String> {
        let _turn = self.flush_turn.lock().await;
        let mut batch = std::mem::take(&mut *self.lock());
        let mut rows = Vec::with_capacity(batch.len());
        batch.retain(|name, change| {
            let epoch = self.epoch_of(name);
            if let Some(epoch) = epoch {
                rows.push((name.clone(), *change, epoch));
            }
            epoch.is_some()
        });
        let mut taken = Taken {
            book: self,
            batch,
            written: false,
        };
        if rows.is_empty() {
            return Ok(());
        }
        let written = write(&self.pool, rows).await;
        taken.written = written.is_ok();
        written
    }

    /// One page of the due rows at `now`, after `cursor`, earliest first.
    /// `ranges` keeps the rows of those inclusive slot ranges only. `None`
    /// keeps every row.
    pub async fn due_page(
        &self,
        now: i64,
        cursor: &DueRow,
        ranges: Option<&[(u16, u16)]>,
    ) -> Result<Vec<DueRow>, String> {
        #[derive(diesel::QueryableByName)]
        struct Row {
            #[diesel(sql_type = Text)]
            tenant_key: String,
            #[diesel(sql_type = BigInt)]
            next_wake_at: i64,
        }
        let ranges = ranges.unwrap_or_default();
        let mut query = diesel::sql_query(due_sql(ranges.len()))
            .into_boxed::<autumn_web::db::RuntimeBackend>()
            .bind::<BigInt, _>(now)
            .bind::<BigInt, _>(cursor.0)
            .bind::<Text, _>(cursor.1.clone());
        for (start, end) in ranges {
            query = query
                .bind::<BigInt, _>(i64::from(*start))
                .bind::<BigInt, _>(i64::from(*end));
        }
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|error| format!("wake read: no connection: {error}"))?;
        let rows: Vec<Row> = query
            .bind::<BigInt, _>(DUE_PAGE)
            .load(&mut *conn)
            .await
            .map_err(|error| format!("wake read: {error}"))?;
        Ok(rows
            .into_iter()
            .map(|row| (row.next_wake_at, row.tenant_key))
            .collect())
    }

    /// The names of the due rows at `now`, for a test: one page, every
    /// range.
    #[cfg(test)]
    pub async fn due_names(&self, now: i64) -> Result<Vec<String>, String> {
        let page = self.due_page(now, &(i64::MIN, String::new()), None).await?;
        Ok(page.into_iter().map(|(_, name)| name).collect())
    }

    /// The pending change of `tenant`, for a test.
    #[cfg(test)]
    fn pending_at(&self, tenant: &FleetDbKey) -> Option<Option<i64>> {
        match self.lock().get(&tenant.name()) {
            Some(Pending::Set { at, .. }) => Some(*at),
            _ => None,
        }
    }
}

/// A batch that a flush took from the book. A drop before the write puts
/// it back, so a cancelled flush loses no change.
struct Taken<'a> {
    book: &'a WakeBook,
    batch: HashMap<String, Pending>,
    written: bool,
}

impl Drop for Taken<'_> {
    fn drop(&mut self) {
        if self.written || self.batch.is_empty() {
            return;
        }
        let mut pending = self.book.lock();
        for (key, change) in self.batch.drain() {
            // A newer change of the key wins.
            pending.entry(key).or_insert(change);
        }
    }
}

/// One change to write: the key, the change and the ownership epoch.
type Row = (String, Pending, i64);

/// Write `rows` in one immediate transaction, so a flush costs one commit.
///
/// The transaction runs as one blocking task. A dropped caller does not
/// stop it, so the pooled connection never stays inside a transaction. The
/// epoch rule runs inside the transaction, so it holds at the commit.
async fn write(pool: &Pool<RuntimeConnection>, rows: Vec<Row>) -> Result<(), String> {
    let mut conn = pool
        .get()
        .await
        .map_err(|error| format!("wake write: no connection: {error}"))?;
    conn.spawn_blocking(move |conn| {
        conn.immediate_transaction(|conn| {
            for (key, change, epoch) in &rows {
                match change {
                    Pending::Set { slot, at } => diesel::RunQueryDsl::execute(
                        diesel::sql_query(UPSERT_SQL)
                            .bind::<Text, _>(key)
                            .bind::<BigInt, _>(i64::from(*slot))
                            .bind::<Nullable<BigInt>, _>(*at)
                            .bind::<BigInt, _>(*epoch),
                        conn,
                    )?,
                    Pending::Forget => diesel::RunQueryDsl::execute(
                        diesel::sql_query(DELETE_SQL)
                            .bind::<Text, _>(key)
                            .bind::<BigInt, _>(*epoch),
                        conn,
                    )?,
                };
            }
            Ok(())
        })
    })
    .await
    .map_err(|error| format!("wake write: {error}"))
}

/// The wake time to report for `raw`.
///
/// `stuck` says that a pass ran after `raw` came due and could not clear
/// it. An example is a timer of a run that fails on each drive. A report of
/// it as is would make the sweep open the tenant on each tick. So the
/// driver reports the earlier of the next healthy deadline, `later`, and
/// `now + STUCK_RETRY_MS`.
#[must_use]
pub fn report_value(
    raw: Option<i64>,
    stuck: bool,
    later: impl FnOnce() -> Option<i64>,
    now: i64,
) -> Option<i64> {
    if !stuck {
        return raw;
    }
    let retry = now.saturating_add(STUCK_RETRY_MS);
    Some(later().map_or(retry, |at| at.min(retry)))
}

/// What the last idle point read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Last {
    raw: Option<i64>,
    stuck: bool,
}

/// Whether an idle point can skip its read. Without work, the value changes
/// only when a known deadline passes.
fn quiet(dirty: bool, last: Option<Last>, now: i64) -> bool {
    !dirty && last.is_some_and(|last| !last.stuck && last.raw.is_none_or(|at| at > now))
}

/// The wake reports of one tenant driver.
pub struct WakeReport {
    book: Arc<WakeBook>,
    tenant: FleetDbKey,
    last: Option<Last>,
    /// The start of the last pass that made no progress.
    last_pass: i64,
    /// Work ran since the last idle point.
    dirty: bool,
}

impl WakeReport {
    /// The report of a driver that starts now. A crash before its first
    /// idle point leaves a due row, so the next sweep finishes the work.
    pub fn new(book: Arc<WakeBook>, tenant: FleetDbKey) -> Self {
        book.record(&tenant, Some(now_ms()));
        Self {
            book,
            tenant,
            last: None,
            last_pass: i64::MIN,
            dirty: true,
        }
    }

    /// The driver runs a command or a pass that made progress.
    ///
    /// The first work after an idle point records now. A crash before the
    /// next idle point then leaves a due row. The sweep skips a tenant that
    /// has a driver, so the row costs nothing while the tenant is open.
    pub fn touch(&mut self) {
        if !self.dirty {
            self.dirty = true;
            self.last = None;
            self.book.record(&self.tenant, Some(now_ms()));
        }
    }

    /// The driver is idle after a pass that started at `pass_started`.
    /// Record the value when it changed.
    pub fn idle(&mut self, runtime: &SqliteRuntime, pass_started: i64) {
        let now = now_ms();
        self.last_pass = pass_started;
        if quiet(self.dirty, self.last, now) {
            return;
        }
        // A backoff that ended during the pass still counts.
        let raw = match runtime.next_wake_since(pass_started) {
            Ok(raw) => raw,
            Err(error) => {
                tracing::warn!(tenant = %self.tenant, %error, "cannot read the next wake time");
                return;
            }
        };
        self.dirty = false;
        let last = Last {
            raw,
            stuck: raw.is_some_and(|at| at <= pass_started),
        };
        if self.last == Some(last) {
            return;
        }
        self.last = Some(last);
        let later = || runtime.next_wake_after(pass_started).ok().flatten();
        let value = report_value(raw, last.stuck, later, now);
        self.book.record(&self.tenant, value);
    }

    /// The driver stops. Record the final value. The sweep writes it.
    ///
    /// A driver that stops with work in flight records now, so the next
    /// sweep opens the tenant and finishes it. A value that came due after
    /// the last idle pass is not stuck: the next sweep fires it.
    pub fn exit(&self, runtime: &SqliteRuntime) {
        let now = now_ms();
        if self.dirty {
            self.book.record(&self.tenant, Some(now));
            return;
        }
        // A backoff that ended after the last pass still counts.
        let raw = runtime.next_wake_since(self.last_pass).ok().flatten();
        let stuck = raw.is_some_and(|at| at <= self.last_pass);
        let later = || runtime.next_wake_after(self.last_pass).ok().flatten();
        self.book
            .record(&self.tenant, report_value(raw, stuck, later, now));
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
    /// stop. The app closes its fleet before the plugin stops, so this
    /// flush writes the final value of each tenant.
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
    ///
    /// The read pages past each row that this host cannot wake now. It
    /// stops when the in-flight set is full or no due row is left.
    pub async fn sweep_once(&self) -> usize {
        if let Err(error) = self.book.flush().await {
            tracing::warn!(%error, "cannot write the tenant wake times");
        }
        let ranges = self.owned_ranges();
        if ranges.as_ref().is_some_and(Vec::is_empty) {
            return 0;
        }
        let now = now_ms();
        let mut cursor: DueRow = (i64::MIN, String::new());
        let mut began = 0;
        for _ in 0..DUE_PAGES {
            let page = match self.book.due_page(now, &cursor, ranges.as_deref()).await {
                Ok(page) => page,
                Err(error) => {
                    tracing::warn!(%error, "cannot read the tenant wake times");
                    return began;
                }
            };
            let last = page.len() < usize::try_from(DUE_PAGE).unwrap_or(usize::MAX);
            for row in page {
                if self.in_flight_full() {
                    return began;
                }
                if self.try_wake(&row.1) {
                    began += 1;
                }
                cursor = row;
            }
            if last {
                break;
            }
        }
        began
    }

    /// The inclusive slot ranges that this host holds, or `None` when the
    /// fleet has no ownership: the host then serves every slot.
    fn owned_ranges(&self) -> Option<Vec<(u16, u16)>> {
        let ownership = self.fleet.database_fleet().ownership()?;
        Some(
            ownership
                .held()
                .into_iter()
                .map(|(range, _)| (range.start(), range.end()))
                .collect(),
        )
    }

    fn in_flight_full(&self) -> bool {
        self.in_flight
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
            >= MAX_IN_FLIGHT
    }

    /// Begin a wake of the tenant `name`, when this host can wake it now.
    fn try_wake(&self, name: &str) -> bool {
        let fleet = self.fleet.database_fleet();
        let key = match fleet.key_for_name(name) {
            Ok(key) => key,
            Err(error) => {
                // A row of another fleet mode can never wake.
                tracing::debug!(tenant = %name, %error, "a wake row names no tenant");
                return false;
            }
        };
        // Skip a key that another host owns or that a driver runs now. A held
        // range can lapse, and the row then stays for the next owner.
        if !fleet.owns(&key) || self.fleet.has_driver(&key) {
            return false;
        }
        let Some(guard) = InFlight::claim(&self.in_flight, &key) else {
            return false;
        };
        let fleet = self.fleet.clone();
        let book = Arc::clone(&self.book);
        tokio::spawn(async move {
            wake(&fleet, &book, &key).await;
            drop(guard);
        });
        true
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
///
/// A failed wake tries again after the retry delay, so a broken tenant does
/// not reopen on each tick. Only a fleet delete removes a row. A missing
/// file can be a takeover that has not restored the tenant yet.
async fn wake(fleet: &HarvestFleet, book: &WakeBook, key: &FleetDbKey) {
    let background = match fleet.database_fleet().open_background(key).await {
        Ok(Some(background)) => background,
        // No room now, a delete that removes the row, or another host that
        // serves the key now. The row stays as it is.
        Ok(None) | Err(FleetError::Deleting { .. } | FleetError::NotOwner { .. }) => return,
        Err(error) => {
            tracing::warn!(tenant = %key, %error, "cannot wake the tenant: it waits a minute");
            book.defer(key);
            return;
        }
    };
    let opened = background.opened;
    match fleet.handle_of(background.db).await {
        Ok(handle) => {
            match tokio::time::timeout(WAKE_BUDGET, handle.settle()).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    tracing::info!(tenant = %key, %error, "the woken tenant stopped before it was idle");
                }
                Err(_) => {
                    tracing::info!(tenant = %key, "the woken tenant is still busy: the fleet closes it");
                }
            }
            drop(handle);
        }
        Err(error) => {
            tracing::warn!(tenant = %key, %error, "cannot start the woken tenant: it waits a minute");
            book.defer(key);
        }
    }
    if opened {
        let _ = fleet.database_fleet().close_if_unused(key);
    }
}

#[cfg(test)]
mod tests {
    // The `#[workflow]` macro needs an `async fn`.
    #![allow(clippy::unused_async)]

    use super::*;
    use autumn_harvest::prelude::*;

    /// The driver reports a value that is not stuck as it is.
    #[test]
    fn a_healthy_value_is_reported_as_is() {
        assert_eq!(
            report_value(Some(2_000), false, || None, 1_500),
            Some(2_000)
        );
        assert_eq!(report_value(None, false, || None, 1_500), None);
    }

    /// The driver tries a stuck value again later, so the sweep does not
    /// open the tenant on each tick.
    #[test]
    fn a_stuck_value_is_tried_again_later() {
        assert_eq!(
            report_value(Some(900), true, || None, 1_500),
            Some(1_500 + STUCK_RETRY_MS)
        );
    }

    /// A stuck value never hides a healthy deadline before the retry.
    #[test]
    fn a_stuck_value_keeps_an_earlier_healthy_deadline() {
        assert_eq!(
            report_value(Some(900), true, || Some(5_000), 1_500),
            Some(5_000)
        );
        assert_eq!(
            report_value(Some(900), true, || Some(1_500 + 2 * STUCK_RETRY_MS), 1_500),
            Some(1_500 + STUCK_RETRY_MS)
        );
    }

    /// An idle point skips its read only with no work, no stuck value and
    /// a known deadline still in the future.
    #[test]
    fn a_quiet_idle_point_skips_its_read() {
        let future = Last {
            raw: Some(2_000),
            stuck: false,
        };
        assert!(quiet(false, Some(future), 1_000));
        assert!(!quiet(true, Some(future), 1_000), "work ran");
        assert!(!quiet(false, Some(future), 2_000), "the deadline passed");
        assert!(!quiet(false, None, 1_000), "no read yet");
        let stuck = Last {
            raw: Some(500),
            stuck: true,
        };
        assert!(!quiet(false, Some(stuck), 1_000));
        let none = Last {
            raw: None,
            stuck: false,
        };
        assert!(quiet(false, Some(none), 1_000));
    }

    fn control(dir: &std::path::Path) -> Pool<RuntimeConnection> {
        autumn_web::db::create_pool(&autumn_web::config::DatabaseConfig {
            url: Some(format!("sqlite://{}", dir.join("control.db").display())),
            ..autumn_web::config::DatabaseConfig::default()
        })
        .expect("pool")
        .expect("a url gives a pool")
    }

    async fn book(dir: &std::path::Path) -> Arc<WakeBook> {
        Arc::new(WakeBook::open(control(dir), None).await.expect("open"))
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
        let book = book(dir.path()).await;
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

    /// The cursor pages past each earlier row. The ranges keep the rows of
    /// their slots only.
    #[tokio::test]
    async fn the_due_read_pages_and_keeps_the_owned_slots() {
        let dir = tempfile::tempdir().unwrap();
        let book = book(dir.path()).await;
        let names: Vec<String> = (0..70).map(|n| format!("t{n:02}")).collect();
        for (n, name) in names.iter().enumerate() {
            book.record(&tenant(name), Some(1_000 + i64::try_from(n).unwrap()));
        }
        book.flush().await.expect("flush");

        let first = book
            .due_page(5_000, &(i64::MIN, String::new()), None)
            .await
            .unwrap();
        assert_eq!(first.len(), 64);
        let rest = book
            .due_page(5_000, first.last().unwrap(), None)
            .await
            .unwrap();
        assert_eq!(rest.len(), 6, "the next page starts after the cursor");
        assert_eq!(rest.last().unwrap().1, tenant(&names[69]).name());

        let slot = tenant("t07").routing_slot();
        let owned = book
            .due_page(5_000, &(i64::MIN, String::new()), Some(&[(slot, slot)]))
            .await
            .unwrap();
        assert!(owned.iter().any(|(_, name)| name == "tenant:t07"));
        assert!(
            owned
                .iter()
                .all(|(_, name)| tenant(&name["tenant:".len()..]).routing_slot() == slot),
            "{owned:?}"
        );
    }

    /// A flush that the caller drops puts its batch back.
    #[tokio::test]
    async fn a_dropped_flush_keeps_its_batch() {
        let dir = tempfile::tempdir().unwrap();
        let book = book(dir.path()).await;
        let acme = tenant("acme");
        book.record(&acme, Some(1_000));
        let batch = std::mem::take(&mut *book.lock());
        drop(Taken {
            book: &book,
            batch,
            written: false,
        });
        assert_eq!(book.pending_at(&acme), Some(Some(1_000)));

        // A newer change of the key wins over the batch.
        book.record(&acme, Some(2_000));
        let mut batch = HashMap::new();
        batch.insert(
            acme.name(),
            Pending::Set {
                slot: acme.routing_slot(),
                at: Some(1_000),
            },
        );
        drop(Taken {
            book: &book,
            batch,
            written: false,
        });
        assert_eq!(book.pending_at(&acme), Some(Some(2_000)));
    }

    /// A write under an older epoch than the row changes nothing. A write
    /// under the same or a newer epoch wins.
    #[tokio::test]
    async fn an_older_epoch_never_overwrites_a_newer_one() {
        let dir = tempfile::tempdir().unwrap();
        let book = book(dir.path()).await;
        let acme = tenant("acme");
        let set = |at: i64| Pending::Set {
            slot: acme.routing_slot(),
            at: Some(at),
        };
        write(&book.pool, vec![(acme.name(), set(9_000), 5)])
            .await
            .unwrap();
        // The old owner writes late, under epoch 3.
        write(&book.pool, vec![(acme.name(), set(1_000), 3)])
            .await
            .unwrap();
        write(&book.pool, vec![(acme.name(), Pending::Forget, 3)])
            .await
            .unwrap();
        assert_eq!(book.due_names(9_000).await.unwrap(), vec![acme.name()]);
        assert!(
            book.due_names(8_999).await.unwrap().is_empty(),
            "the stale value lost"
        );

        write(&book.pool, vec![(acme.name(), set(2_000), 5)])
            .await
            .unwrap();
        assert_eq!(book.due_names(2_000).await.unwrap(), vec![acme.name()]);
        write(&book.pool, vec![(acme.name(), Pending::Forget, 6)])
            .await
            .unwrap();
        assert!(
            book.due_names(9_000).await.unwrap().is_empty(),
            "a newer delete wins"
        );
    }

    /// The DDL runs twice with no error, so each boot can run it.
    #[tokio::test]
    async fn the_schema_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        WakeBook::open(control(dir.path()), None)
            .await
            .expect("first");
        WakeBook::open(control(dir.path()), None)
            .await
            .expect("second");
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

    /// Sleeps on a durable timer of `secs` seconds.
    #[workflow]
    async fn dozing(ctx: &WorkflowContext, secs: u64) -> Result<(), String> {
        ctx.timer("nap", secs)
            .await
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    /// A runtime at a fixed clock with two timers: 5 s and 90 s out.
    async fn two_timers(dir: &std::path::Path, t0: i64) -> SqliteRuntime {
        let mut runtime = SqliteRuntime::open(&dir.join("t.harvest.db")).expect("open");
        runtime.set_clock(move || chrono::DateTime::from_timestamp_millis(t0).expect("time"));
        runtime.register_workflow(&dozing_info());
        runtime
            .start_workflow("dozing", serde_json::json!(5))
            .unwrap();
        runtime
            .start_workflow("dozing", serde_json::json!(90))
            .unwrap();
        runtime.poll_once().await.unwrap();
        runtime
    }

    /// A new report records now. A crash before the first idle point then
    /// leaves a due row.
    #[tokio::test]
    async fn a_new_report_records_now() {
        let dir = tempfile::tempdir().unwrap();
        let book = book(dir.path()).await;
        let acme = tenant("acme");
        let before = now_ms();
        let _report = WakeReport::new(Arc::clone(&book), acme.clone());
        let at = book.pending_at(&acme).flatten().expect("a value");
        assert!(at >= before && at <= now_ms());
    }

    /// An idle point records the next deadline once. A stuck deadline
    /// reports the next healthy one. Work then records now.
    #[tokio::test]
    async fn an_idle_point_records_each_change_once() {
        let dir = tempfile::tempdir().unwrap();
        let book = book(dir.path()).await;
        let acme = tenant("acme");
        let t0 = 1_767_225_600_000;
        let runtime = two_timers(dir.path(), t0).await;
        let mut report = WakeReport::new(Arc::clone(&book), acme.clone());

        report.idle(&runtime, t0);
        assert_eq!(book.pending_at(&acme), Some(Some(t0 + 5_000)));
        // The same read records nothing new.
        book.lock().clear();
        report.idle(&runtime, t0);
        assert_eq!(book.pending_at(&acme), None);

        // The 5 s timer was due before a pass that could not fire it. The
        // next healthy deadline wins over the retry.
        report.touch();
        assert!(
            book.pending_at(&acme).flatten().is_some(),
            "work records now"
        );
        report.idle(&runtime, t0 + 10_000);
        assert_eq!(book.pending_at(&acme), Some(Some(t0 + 90_000)));
        book.lock().clear();
        report.idle(&runtime, t0 + 10_000);
        assert_eq!(book.pending_at(&acme), None, "a stuck value reports once");
    }

    /// A clean stop records the final value. A dirty stop records now.
    #[tokio::test]
    async fn a_stop_records_the_final_value() {
        let dir = tempfile::tempdir().unwrap();
        let book = book(dir.path()).await;
        let acme = tenant("acme");
        let t0 = 1_767_225_600_000;
        let runtime = two_timers(dir.path(), t0).await;

        let mut report = WakeReport::new(Arc::clone(&book), acme.clone());
        report.idle(&runtime, t0);
        book.lock().clear();
        report.exit(&runtime);
        assert_eq!(book.pending_at(&acme), Some(Some(t0 + 5_000)));

        let before = now_ms();
        report.touch();
        report.exit(&runtime);
        let at = book.pending_at(&acme).flatten().expect("a value");
        assert!(at >= before, "a dirty stop records now");
    }
}
