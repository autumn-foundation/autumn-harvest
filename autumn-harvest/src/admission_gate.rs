//! Admission gate primitive for incident-response operators (issue #377).
//!
//! An admission gate halts new workflow starts fleet-wide or for a scoped
//! subset of work (by workflow name, queue, shard, or owner) while letting
//! in-flight executions drain naturally. Gates persist in Postgres and survive
//! plugin restart; the plugin loads active gates before its worker pool starts
//! so there is no admission window between boot and re-apply.
//!
//! ## Known gaps — ungated workflow producers
//!
//! The following code paths call `start_or_load_workflow_execution` without
//! consulting the admission gate cache. Each is a follow-up candidate.
//!
//! * **Completion triggers** (`completion_trigger.rs`): runs in a background
//!   task; the `AdmissionGateCache` is not threaded through to it. Pass the
//!   cache into the completion trigger runner so fleet/name/queue/owner gates
//!   are honoured for trigger-initiated starts during an incident.
//!
//! * **Outbox relay** (`outbox.rs` — `spawn_workflow_start_outbox_relay`):
//!   replays workflow-start events that were durably written to the outbox
//!   before the gate was raised. Gating outbox relay is semantically
//!   questionable (the commit already happened) but can be useful for
//!   rate-limiting recovery after an incident.
//!
//! * **Webhook delegate** (`plugin.rs` — `WebhookDelegate`): the Autumn
//!   webhook integration starts a workflow inline in the HTTP handler path.
//!   The delegate does not have access to the gate cache today; thread it in
//!   via `AppState` extension so webhook-triggered starts can be gated.
//!
//! ## Standalone router note
//!
//! [`AdmissionGateCache::new`] initialises the cache as **open** (no gates).
//! Standalone integrations that mount `harvest_api_router` without the plugin
//! boot loader (i.e. without calling `HarvestPlugin::on_startup`) must
//! explicitly call `load_active_gates` from the DB and pass the result to
//! [`AdmissionGateCache::refresh`] on startup to pick up any gates that were
//! persisted before the process restarted. Without this step, gates created in
//! a previous process lifetime are invisible until a local create/lift happens
//! on the same replica.
//!
//! ## Scope semantics
//!
//! | Scope | Blocks |
//! |-------|--------|
//! | `Fleet` | every new start |
//! | `WorkflowName(n)` | starts for workflow named `n` |
//! | `Queue(q)` | starts routed to queue `q` |
//! | `ShardId(s)` | starts landing on shard `s` |
//! | `Owner(o)` | starts whose `WorkflowInfo.owner` equals `o` |
//!
//! Multiple active gates are evaluated as OR: any match → blocked.
//! Expired gates are never matched, regardless of scope.
//!
//! ## Upper bound on simultaneous gates
//!
//! [`MAX_ACTIVE_GATES`] documents the supported maximum. Exceeding it returns
//! [`TooManyGates`](GateCreateError::TooManyGates) rather than silently
//! accepting the gate.

use chrono::{DateTime, Utc};
use std::fmt;
use uuid::Uuid;

// ── Constants ─────────────────────────────────────────────────────────────────

/// Maximum number of simultaneously active gates.
///
/// Reaching this limit returns [`GateCreateError::TooManyGates`] rather than
/// silently accepting more. This bounds the scan cost of the admission check
/// and prevents unbounded metric cardinality growth from the `gate_id` label.
///
/// 100 is intentionally generous for incident-response use; normal production
/// usage is 1–5 gates at a time.
pub const MAX_ACTIVE_GATES: usize = 100;

// ── GateScope ─────────────────────────────────────────────────────────────────

/// The scope over which an admission gate blocks new workflow starts.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum GateScope {
    /// Block all new workflow starts on this plugin deployment.
    Fleet,
    /// Block new starts for the workflow named `value`.
    WorkflowName(String),
    /// Block new starts routed to the named task queue.
    Queue(String),
    /// Block new starts landing on the given Postgres shard (0-indexed).
    ShardId(i32),
    /// Block new starts whose `WorkflowInfo.owner` equals `value`.
    Owner(String),
}

impl GateScope {
    /// The discriminator string persisted in `scope_kind`.
    #[must_use]
    pub const fn kind_str(&self) -> &'static str {
        match self {
            Self::Fleet => "fleet",
            Self::WorkflowName(_) => "workflow_name",
            Self::Queue(_) => "queue",
            Self::ShardId(_) => "shard_id",
            Self::Owner(_) => "owner",
        }
    }

    /// The value persisted in `scope_value` (None for Fleet).
    #[must_use]
    pub fn value_str(&self) -> Option<String> {
        match self {
            Self::Fleet => None,
            Self::WorkflowName(v) | Self::Queue(v) | Self::Owner(v) => Some(v.clone()),
            Self::ShardId(n) => Some(n.to_string()),
        }
    }

    /// Reconstruct a `GateScope` from the `scope_kind` / `scope_value` pair
    /// stored in the database.
    ///
    /// Returns `None` when `kind` is unrecognised (forward-compat with future
    /// variants added after this deployment).
    #[must_use]
    pub fn from_db(kind: &str, value: Option<&str>) -> Option<Self> {
        match kind {
            "fleet" => Some(Self::Fleet),
            "workflow_name" => Some(Self::WorkflowName(value?.to_string())),
            "queue" => Some(Self::Queue(value?.to_string())),
            "shard_id" => {
                let n: i32 = value?.parse().ok()?;
                Some(Self::ShardId(n))
            }
            "owner" => Some(Self::Owner(value?.to_string())),
            _ => None,
        }
    }
}

impl fmt::Display for GateScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Fleet => write!(f, "fleet"),
            Self::WorkflowName(v) => write!(f, "workflow_name:{v}"),
            Self::Queue(v) => write!(f, "queue:{v}"),
            Self::ShardId(n) => write!(f, "shard_id:{n}"),
            Self::Owner(v) => write!(f, "owner:{v}"),
        }
    }
}

// ── AdmissionGateId ───────────────────────────────────────────────────────────

/// Newtype wrapper for the admission gate UUID primary key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct AdmissionGateId(pub Uuid);

impl fmt::Display for AdmissionGateId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<Uuid> for AdmissionGateId {
    fn from(u: Uuid) -> Self {
        Self(u)
    }
}

// ── AdmissionGate ─────────────────────────────────────────────────────────────

/// An in-memory representation of a persisted admission gate.
///
/// Constructed from [`crate::models::AdmissionGateRow`] after loading from the
/// database. The [`AdmissionGateCache`] holds a `Vec<AdmissionGate>` refreshed
/// every second; the admission check reads from this snapshot without hitting
/// the database.
#[derive(Debug, Clone)]
pub struct AdmissionGate {
    /// Unique gate identifier.
    pub id: AdmissionGateId,
    /// What work this gate blocks.
    pub scope: GateScope,
    /// Human-readable reason; surfaced in the blocked-caller error.
    /// Reason
    pub reason: String,
    /// Optional extended message displayed in the Vantage UI.
    pub message: Option<String>,
    /// Identity of the operator who created the gate.
    pub created_by: String,
    /// Wall-clock time the gate was created.
    pub created_at: DateTime<Utc>,
    /// Optional expiry; `None` = no expiry.
    pub expires_at: Option<DateTime<Utc>>,
}

impl AdmissionGate {
    /// Returns `true` if the gate is still active at `now`.
    ///
    /// A gate is inactive when `expires_at` is `Some` and in the past.
    #[must_use]
    pub fn is_active_at(&self, now: DateTime<Utc>) -> bool {
        self.expires_at.is_none_or(|exp| exp > now)
    }

    /// Returns `true` if this gate matches the given admission parameters.
    ///
    /// The gate must also be active (see [`is_active_at`](Self::is_active_at))
    /// for the combined result to constitute a block.
    #[must_use]
    pub fn matches(
        &self,
        workflow_name: &str,
        queue_name: &str,
        shard_id: i32,
        owner: Option<&str>,
    ) -> bool {
        match &self.scope {
            GateScope::Fleet => true,
            GateScope::WorkflowName(n) => n == workflow_name,
            GateScope::Queue(q) => q == queue_name,
            GateScope::ShardId(s) => *s == shard_id,
            GateScope::Owner(o) => owner.is_some_and(|ow| ow == o),
        }
    }
}

// ── check_admission ───────────────────────────────────────────────────────────

/// Returns the first active gate that matches the given admission parameters,
/// or `None` if admission is allowed.
///
/// The caller should supply a snapshot of the currently active gates (e.g.
/// from [`AdmissionGateCache`]). This function is pure and does not touch the
/// database.
///
/// # Arguments
///
/// * `gates` – slice of active gates (may include expired ones; they are
///   filtered out internally against `Utc::now()`).
/// * `workflow_name` – the workflow type name being started.
/// * `queue_name` – the task queue the execution would be routed to.
/// * `shard_id` – the Postgres shard the execution would land on.
/// * `owner` – the `WorkflowInfo.owner` value, if any.
#[must_use]
pub fn check_admission<'a>(
    gates: &'a [AdmissionGate],
    workflow_name: &str,
    queue_name: &str,
    shard_id: i32,
    owner: Option<&str>,
) -> Option<&'a AdmissionGate> {
    let now = Utc::now();
    gates
        .iter()
        .find(|g| g.is_active_at(now) && g.matches(workflow_name, queue_name, shard_id, owner))
}

// ── GateCreateError ───────────────────────────────────────────────────────────

/// Error returned when a gate cannot be created.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GateCreateError {
    /// The active gate count reached [`MAX_ACTIVE_GATES`].
    #[allow(missing_docs)]
    TooManyGates { limit: usize },
    /// The reason string was empty.
    EmptyReason,
}

impl fmt::Display for GateCreateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooManyGates { limit } => write!(
                f,
                "cannot create gate: active gate limit of {limit} reached"
            ),
            Self::EmptyReason => write!(f, "cannot create gate: reason must not be empty"),
        }
    }
}

impl std::error::Error for GateCreateError {}

// ── AdmissionGateCache ────────────────────────────────────────────────────────

#[derive(Debug)]
/// In-process cache for active admission gates.
///
/// Populated at plugin boot and refreshed every ≤1 second by a background
/// task. `check()` **fails closed** when the cache has not yet been
/// successfully populated — i.e. it returns a synthetic "blocked" result —
/// so a DB error during startup cannot create an admission window for
/// persisted gates.
pub struct AdmissionGateCache {
    gates: std::sync::RwLock<Vec<AdmissionGate>>,
    /// Set to `true` after the first successful `refresh()`.
    initialized: std::sync::atomic::AtomicBool,
}

impl Default for AdmissionGateCache {
    fn default() -> Self {
        Self {
            gates: std::sync::RwLock::new(Vec::new()),
            initialized: std::sync::atomic::AtomicBool::new(false),
        }
    }
}

impl AdmissionGateCache {
    /// Create an initialized, empty cache (fail-open).
    ///
    /// Suitable for standalone API routers and test setups that do not load
    /// gates from a database. The plugin uses this path too — it immediately
    /// calls [`refresh`](Self::refresh) with gates loaded from Postgres before
    /// workers start accepting work, so the brief open window is harmless.
    #[must_use]
    pub fn new() -> Self {
        let cache = Self::default();
        // Mark as initialized so `check()` treats an empty list as "no gates
        // active" rather than triggering the fail-closed sentinel block.
        cache
            .initialized
            .store(true, std::sync::atomic::Ordering::Release);
        cache
    }

    /// Create an uninitialized (fail-closed) cache.
    ///
    /// `check()` returns a synthetic block until the first successful
    /// [`refresh`](Self::refresh), preventing new workflow starts from slipping
    /// through a startup DB error when persisted gates are present.
    ///
    /// Used internally by the plugin boot path, which calls `refresh()` before
    /// the worker pool starts accepting work.
    #[must_use]
    pub fn new_fail_closed() -> Self {
        Self::default()
    }

    /// Revert the cache to fail-closed (uninitialized) mode.
    ///
    /// After this call `check()` returns a synthetic block until the next
    /// `refresh()`. Used by the plugin to arm the fail-closed semantic after
    /// construction so the window between HTTP server bind and the boot-time
    /// gate load is safe.
    pub fn set_fail_closed(&self) {
        self.initialized
            .store(false, std::sync::atomic::Ordering::Release);
    }

    /// Replace the cached gate list with a freshly loaded snapshot and mark
    /// the cache as initialized.
    pub fn refresh(&self, gates: Vec<AdmissionGate>) {
        if let Ok(mut guard) = self.gates.write() {
            *guard = gates;
            self.initialized
                .store(true, std::sync::atomic::Ordering::Release);
        }
    }

    /// Acquire a read lock and call `check_admission` against the snapshot.
    ///
    /// Returns `(gate_id, reason, scope_kind)` when a gate matches, where
    /// `scope_kind` is the static discriminator string (e.g. `"fleet"`,
    /// `"workflow_name"`) suitable for use as a low-cardinality metric label.
    ///
    /// **Fail-closed**: returns a synthetic fleet-scope block when the cache
    /// has not yet been successfully populated from the database.
    #[must_use]
    pub fn check(
        &self,
        workflow_name: &str,
        queue_name: &str,
        shard_id: i32,
        owner: Option<&str>,
    ) -> Option<(Uuid, String, &'static str)> {
        if !self.initialized.load(std::sync::atomic::Ordering::Acquire) {
            // Cache not yet populated from DB — fail closed so that a transient
            // startup DB error cannot bypass persisted incident gates.
            return Some((
                Uuid::nil(),
                "admission gate cache not yet initialized (fail-closed)".to_string(),
                "fleet",
            ));
        }
        let guard = self.gates.read().ok()?;
        check_admission(&guard, workflow_name, queue_name, shard_id, owner)
            .map(|g| (g.id.0, g.reason.clone(), g.scope.kind_str()))
    }

    /// Return the number of currently cached active gates.
    #[must_use]
    pub fn active_count(&self) -> usize {
        self.gates.read().map_or(0, |g| g.len())
    }
}

// ── DB layer (requires `db` feature) ─────────────────────────────────────────

#[cfg(feature = "db")]
/// Generic field documentation.
pub mod db {
    use super::{AdmissionGate, AdmissionGateId, DateTime, GateScope, MAX_ACTIVE_GATES, Utc, Uuid};
    use crate::error::{HarvestResult, database_error};
    use crate::models::{AdmissionGateRow, NewAdmissionGateRow};
    use crate::schema::harvest_admission_gates;
    use diesel::{BoolExpressionMethods, ExpressionMethods, NullableExpressionMethods, QueryDsl};
    use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
    use scoped_futures::ScopedFutureExt;

    /// Convert a DB row into an in-memory [`AdmissionGate`].
    ///
    /// Rows whose `scope_kind` / `scope_value` cannot be parsed into a known
    /// [`GateScope`] are silently dropped: forward-compat with variants added
    /// in future versions.
    #[must_use]
    pub fn row_to_gate(row: AdmissionGateRow) -> Option<AdmissionGate> {
        let scope = GateScope::from_db(&row.scope_kind, row.scope_value.as_deref())?;
        Some(AdmissionGate {
            id: AdmissionGateId(row.id),
            scope,
            reason: row.reason,
            message: row.message,
            created_by: row.created_by,
            created_at: row.created_at,
            expires_at: row.expires_at,
        })
    }

    /// Load all currently active (not lifted, not expired) gates.
    ///
    /// Called by the plugin's background refresh task and at startup.
    ///
    /// # Errors
    ///
    /// Returns `HarvestError::Database` if the query fails.
    pub async fn load_active_gates(
        conn: &mut AsyncPgConnection,
    ) -> HarvestResult<Vec<AdmissionGate>> {
        use harvest_admission_gates::dsl as g;
        let now = Utc::now();

        let rows = g::harvest_admission_gates
            .filter(g::lifted_at.is_null())
            .filter(
                g::expires_at
                    .is_null()
                    .or(g::expires_at.assume_not_null().gt(now)),
            )
            .order_by(g::created_at.asc())
            .load::<AdmissionGateRow>(conn)
            .await
            .map_err(database_error)?;

        Ok(rows.into_iter().filter_map(row_to_gate).collect())
    }

    /// Create a new gate and return it.
    ///
    /// Validates that:
    /// - `reason` is not empty.
    /// - The number of currently active (not lifted, not expired) gates is
    ///   below [`MAX_ACTIVE_GATES`].
    ///
    /// # Errors
    ///
    /// - `HarvestError::Config` when `reason` is empty.
    /// - `HarvestError::Config` when the active gate count would exceed
    ///   [`MAX_ACTIVE_GATES`].
    /// - `HarvestError::Database` if the insert or count query fails.
    pub async fn create_gate(
        conn: &mut AsyncPgConnection,
        scope: &GateScope,
        reason: &str,
        message: Option<&str>,
        actor: &str,
        expires_at: Option<DateTime<Utc>>,
    ) -> HarvestResult<AdmissionGate> {
        use crate::error::HarvestError;

        if reason.trim().is_empty() {
            return Err(HarvestError::Config(
                "admission gate reason must not be empty".to_string(),
            ));
        }

        // Capture owned copies so they can be moved into the transaction closure.
        let scope_kind = scope.kind_str();
        let scope_value_owned = scope.value_str();
        let reason_owned = reason.to_owned();
        let message_owned = message.map(str::to_owned);
        let actor_owned = actor.to_owned();

        conn.transaction::<AdmissionGate, HarvestError, _>(|conn| {
            async move {
                use harvest_admission_gates::dsl as g;

                // Serialise concurrent gate creates so the count-check + insert is
                // atomic and the MAX_ACTIVE_GATES cap cannot be exceeded under
                // concurrent POST /admin/gates requests.  Advisory lock key 377 =
                // issue number; xact-scoped so it auto-releases on commit/rollback.
                diesel::sql_query("SELECT pg_advisory_xact_lock(377)")
                    .execute(conn)
                    .await
                    .map_err(database_error)?;

                let now = Utc::now();
                let active_count: i64 = g::harvest_admission_gates
                    .filter(g::lifted_at.is_null())
                    .filter(
                        g::expires_at
                            .is_null()
                            .or(g::expires_at.assume_not_null().gt(now)),
                    )
                    .count()
                    .get_result::<i64>(conn)
                    .await
                    .map_err(database_error)?;

                #[allow(clippy::cast_possible_wrap)]
                if active_count >= MAX_ACTIVE_GATES as i64 {
                    return Err(HarvestError::Config(format!(
                        "cannot create admission gate: active gate limit of {MAX_ACTIVE_GATES} reached",
                    )));
                }

                let scope_value_ref = scope_value_owned.as_deref();
                let new_gate = NewAdmissionGateRow {
                    id: Uuid::new_v4(),
                    scope_kind,
                    scope_value: scope_value_ref,
                    reason: &reason_owned,
                    message: message_owned.as_deref(),
                    created_by: &actor_owned,
                    expires_at,
                };

                let row: AdmissionGateRow = diesel::insert_into(g::harvest_admission_gates)
                    .values(&new_gate)
                    .get_result(conn)
                    .await
                    .map_err(database_error)?;

                row_to_gate(row).ok_or_else(|| {
                    HarvestError::Config("created gate row could not be decoded".to_string())
                })
            }
            .scope_boxed()
        })
        .await
    }

    /// Soft-delete a gate by setting `lifted_at`.
    ///
    /// Returns the lifted gate record, or `None` if the gate was not found or
    /// was already lifted.
    ///
    /// # Errors
    ///
    /// Returns `HarvestError::Database` if the update query fails.
    pub async fn lift_gate(
        conn: &mut AsyncPgConnection,
        gate_id: Uuid,
        actor: &str,
    ) -> HarvestResult<Option<AdmissionGate>> {
        use diesel::OptionalExtension;
        use harvest_admission_gates::dsl as g;

        let now = Utc::now();

        let row: Option<AdmissionGateRow> = diesel::update(
            g::harvest_admission_gates
                .find(gate_id)
                .filter(g::lifted_at.is_null()),
        )
        .set((g::lifted_at.eq(Some(now)), g::lifted_by.eq(Some(actor))))
        .get_result(conn)
        .await
        .optional()
        .map_err(database_error)?;

        Ok(row.and_then(row_to_gate))
    }

    /// List all non-lifted gates (includes expired ones for UI display).
    ///
    /// # Errors
    ///
    /// Returns `HarvestError::Database` if the query fails.
    pub async fn list_gates(conn: &mut AsyncPgConnection) -> HarvestResult<Vec<AdmissionGateRow>> {
        use harvest_admission_gates::dsl as g;

        g::harvest_admission_gates
            .filter(g::lifted_at.is_null())
            .order_by(g::created_at.asc())
            .load(conn)
            .await
            .map_err(database_error)
    }
}

// ── Serialisable view (for API responses) ────────────────────────────────────

/// JSON-serialisable view of an admission gate for API responses.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AdmissionGateView {
    /// Generic field documentation.
    pub id: Uuid,
    /// Generic field documentation.
    pub scope_kind: String,
    /// Generic field documentation.
    pub scope_value: Option<String>,
    /// Reason
    pub reason: String,
    /// Generic field documentation.
    pub message: Option<String>,
    /// Generic field documentation.
    pub created_by: String,
    /// Generic field documentation.
    pub created_at: DateTime<Utc>,
    /// Generic field documentation.
    pub expires_at: Option<DateTime<Utc>>,
    /// `true` when the gate is currently blocking (active + not expired).
    pub is_active: bool,
}

impl From<AdmissionGate> for AdmissionGateView {
    fn from(g: AdmissionGate) -> Self {
        let is_active = g.is_active_at(Utc::now());
        Self {
            id: g.id.0,
            scope_kind: g.scope.kind_str().to_string(),
            scope_value: g.scope.value_str(),
            reason: g.reason,
            message: g.message,
            created_by: g.created_by,
            created_at: g.created_at,
            expires_at: g.expires_at,
            is_active,
        }
    }
}

#[cfg(feature = "db")]
impl From<&crate::models::AdmissionGateRow> for AdmissionGateView {
    fn from(row: &crate::models::AdmissionGateRow) -> Self {
        let now = Utc::now();
        let is_active = row.lifted_at.is_none() && row.expires_at.is_none_or(|exp| exp > now);
        Self {
            id: row.id,
            scope_kind: row.scope_kind.clone(),
            scope_value: row.scope_value.clone(),
            reason: row.reason.clone(),
            message: row.message.clone(),
            created_by: row.created_by.clone(),
            created_at: row.created_at,
            expires_at: row.expires_at,
            is_active,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fleet_gate(reason: &str) -> AdmissionGate {
        AdmissionGate {
            id: AdmissionGateId(Uuid::new_v4()),
            scope: GateScope::Fleet,
            reason: reason.to_string(),
            message: None,
            created_by: "test".to_string(),
            created_at: Utc::now(),
            expires_at: None,
        }
    }

    #[test]
    fn gate_scope_kind_str_round_trips() {
        assert_eq!(GateScope::Fleet.kind_str(), "fleet");
        assert_eq!(
            GateScope::WorkflowName("x".into()).kind_str(),
            "workflow_name"
        );
        assert_eq!(GateScope::Queue("q".into()).kind_str(), "queue");
        assert_eq!(GateScope::ShardId(0).kind_str(), "shard_id");
        assert_eq!(GateScope::Owner("o".into()).kind_str(), "owner");
    }

    #[test]
    fn gate_scope_from_db_round_trips() {
        let cases = [
            ("fleet", None, GateScope::Fleet),
            (
                "workflow_name",
                Some("foo"),
                GateScope::WorkflowName("foo".into()),
            ),
            ("queue", Some("q1"), GateScope::Queue("q1".into())),
            ("shard_id", Some("3"), GateScope::ShardId(3)),
            ("owner", Some("team"), GateScope::Owner("team".into())),
        ];
        for (kind, val, expected) in cases {
            let got = GateScope::from_db(kind, val).expect("should parse");
            assert_eq!(got, expected);
        }
    }

    #[test]
    fn unknown_scope_kind_returns_none() {
        assert!(GateScope::from_db("region", Some("us-east-1")).is_none());
    }

    #[test]
    fn active_gate_with_no_expiry_matches() {
        let g = fleet_gate("test");
        let now = Utc::now();
        assert!(g.is_active_at(now));
    }

    #[test]
    fn gate_with_future_expiry_is_active() {
        let mut g = fleet_gate("test");
        g.expires_at = Some(Utc::now() + chrono::Duration::hours(1));
        assert!(g.is_active_at(Utc::now()));
    }

    #[test]
    fn gate_with_past_expiry_is_inactive() {
        let mut g = fleet_gate("test");
        g.expires_at = Some(Utc::now() - chrono::Duration::seconds(1));
        assert!(!g.is_active_at(Utc::now()));
    }

    #[test]
    fn check_admission_empty_gates_allows() {
        assert!(check_admission(&[], "wf", "q", 0, None).is_none());
    }

    #[test]
    fn cache_new_is_initialized_and_open() {
        // new() starts initialized-empty so standalone routers pass admission
        // checks without requiring a DB load.
        let cache = AdmissionGateCache::new();
        assert!(
            cache.check("wf", "q", 0, None).is_none(),
            "new() cache must be open (no active gates)"
        );
    }

    #[test]
    fn cache_new_fail_closed_blocks_until_refreshed() {
        // new_fail_closed() starts uninitialized so a transient boot DB error
        // cannot create an admission window for persisted gates.
        let cache = AdmissionGateCache::new_fail_closed();
        assert!(
            cache.check("wf", "q", 0, None).is_some(),
            "new_fail_closed() cache must fail closed until refresh()"
        );
        cache.refresh(vec![]);
        assert!(
            cache.check("wf", "q", 0, None).is_none(),
            "after refresh with empty list, cache must be open"
        );
    }

    #[test]
    fn cache_check_returns_reason_after_refresh() {
        let cache = AdmissionGateCache::new();
        cache.refresh(vec![fleet_gate("incident-42")]);
        let result = cache.check("wf", "q", 0, None);
        assert!(result.is_some());
        let (_, reason, scope_kind) = result.unwrap();
        assert_eq!(reason, "incident-42");
        assert_eq!(scope_kind, "fleet");
    }
}
