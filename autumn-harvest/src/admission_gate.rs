//! Admission gate primitive for incident-response operators (issue #377).
//!
//! An admission gate halts new workflow starts fleet-wide or for a scoped
//! subset of work (by workflow name, queue, shard, or owner) while letting
//! in-flight executions drain naturally. Gates persist in Postgres and survive
//! plugin restart; the plugin loads active gates before its worker pool starts
//! so there is no admission window between boot and re-apply.
//!
//! ## Start-producer contract (issue #618)
//!
//! The admission gate is **authoritative** across every in-process workflow
//! start producer: each producer either consults the gate before starting, or
//! is explicitly **exempt-by-design** and increments the observable
//! `harvest.admission.bypassed{producer}` counter so the exemption is never
//! silent. The full contract is enumerated by [`producer_contract`] and
//! surfaced at `GET /admin/gates` (`producers` block) so operators can discover
//! it without reading source.
//!
//! | Producer | Status | How |
//! |----------|--------|-----|
//! | [`Api`](StartProducer::Api) | gated | HTTP start route checks the gate before any DB write |
//! | [`BatchStart`](StartProducer::BatchStart) | gated | each item checks the gate with its resolved target shard |
//! | [`CompletionTrigger`](StartProducer::CompletionTrigger) | gated | checks the gate inside the source terminal commit before starting the target; fail-closed sentinel proceeds |
//! | [`WebhookDelegate`](StartProducer::WebhookDelegate) | gated | inbound receiver delegates to the gated HTTP start route; outbound webhook-delivery producer consults the cached gate and blocks on a match |
//! | [`Scheduler`](StartProducer::Scheduler) | gated | each due schedule slot checks the gate before firing |
//! | [`Debounce`](StartProducer::Debounce) | gated-at-admission | gated at HTTP admission; deferred scanner fire is exempt-with-bypass-counter |
//! | [`Throttle`](StartProducer::Throttle) | gated-at-admission | gated at HTTP admission; deferred scanner fire is exempt-with-bypass-counter |
//! | [`EventBatch`](StartProducer::EventBatch) | gated-at-admission | gated at HTTP admission; deferred scanner fire is exempt-with-bypass-counter |
//! | [`CompletionTriggerOutbox`](StartProducer::CompletionTriggerOutbox) | gated-at-relay | cross-shard relay, gated authoritatively at relay time on the target shard's real queue: blocks a matching gate (drops the row + counts blocked), else starts + counts the bypass |
//! | [`Outbox`](StartProducer::Outbox) | exempt-by-design | relays already-committed in-flight work; increments the bypass counter |
//!
//! **Out of scope — in-flight continuation, not new admission.** The gate governs
//! *new* workflow starts. It intentionally does NOT block continuation of work
//! already admitted: workflow-level retry (issue #523), continue-as-new, child /
//! detached-child spawn, reset forks, and the in-process typed-client handle all
//! start executions without a gate check because they extend an already-accepted
//! run rather than admit a new one. See `docs/operations/admission-gate-producers.md`.
//!
//! The core completion-trigger path reaches the shared [`AdmissionGateCache`]
//! through the process-global [`GLOBAL_ADMISSION_GATE_CACHE`] static (mirroring
//! `GLOBAL_WORKFLOW_METADATA` / `GLOBAL_CALLBACK_CONFIG`), populated by the
//! plugin at boot with the same `Arc` the management API uses. When the static
//! is unset (standalone integrations, or the plugin's brief boot window) the
//! core gate check is skipped — byte-identical to pre-#618 behaviour.
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
use std::sync::Arc;
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

    /// Write-through: insert (replacing any entry with the same id) `gate` into the
    /// cached snapshot under the write lock, independent of any background refresh
    /// (issue #618, F-round16).
    ///
    /// The management create handler persists the gate to the DB **first**, then
    /// calls this so the in-memory snapshot reflects the just-persisted gate even if
    /// the follow-up full [`load_active_gates`](db::load_active_gates) refresh fails.
    /// Without this, a failed post-create refresh left the snapshot stale-open and a
    /// completion-trigger start (which consults [`check_cached`](Self::check_cached)
    /// and does **not** retry after a synthetic fail-closed block) would durably
    /// MISS the new gate.
    ///
    /// Replacing by id is idempotent against a concurrent background refresh that may
    /// have already picked the new gate up (the create persists to the DB before this
    /// call, so the refresh cannot lose it). Does **not** touch the `initialized`
    /// flag: applying a *known* delta means the snapshot is not blind, so the handler
    /// no longer arms fail-closed on a post-mutation refresh failure.
    pub fn apply_created(&self, gate: AdmissionGate) {
        if let Ok(mut guard) = self.gates.write() {
            let id = gate.id.0;
            guard.retain(|g| g.id.0 != id);
            guard.push(gate);
        }
    }

    /// Write-through: remove any cached gate whose id equals `gate_id` from the
    /// snapshot under the write lock, independent of any background refresh (issue
    /// #618, F-round16).
    ///
    /// The management lift handler soft-deletes the gate in the DB **first**, then
    /// calls this so a just-lifted gate stops matching on this replica even if the
    /// follow-up full [`load_active_gates`](db::load_active_gates) refresh fails.
    /// Without this, a failed post-lift refresh left the lifted gate in the snapshot
    /// and a completion-trigger start would keep dropping targets as
    /// `admission_blocked` against a gate the operator already lifted. No-op when the
    /// id is absent (idempotent). Does **not** touch the `initialized` flag.
    pub fn apply_lifted(&self, gate_id: Uuid) {
        if let Ok(mut guard) = self.gates.write() {
            guard.retain(|g| g.id.0 != gate_id);
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
                sentinel_gate_id(),
                "admission gate cache not yet initialized (fail-closed)".to_string(),
                "fleet",
            ));
        }
        let guard = self.gates.read().ok()?;
        check_admission(&guard, workflow_name, queue_name, shard_id, owner)
            .map(|g| (g.id.0, g.reason.clone(), g.scope.kind_str()))
    }

    /// Match against the **last-known cached gates snapshot**, ignoring the
    /// fail-closed / initialized flag (issue #618, Codex P1).
    ///
    /// Unlike [`check`](Self::check) — which returns the synthetic
    /// [`sentinel_gate_id`] block for every start while the cache is
    /// fail-closed/uninitialized — this returns only a **real** matching gate
    /// (never the sentinel), or `None` when no cached gate matches (including an
    /// empty snapshot during the boot window).
    ///
    /// `set_fail_closed()` retains the gates snapshot, so a gate loaded before a
    /// transient gate-DB read error is still honoured here. Used by the
    /// completion-trigger path: a completion-trigger start is in-flight
    /// continuation of already-committed work and cannot retry, so it honours
    /// only the gates the cache last knew about (block+count on a match, proceed
    /// otherwise) rather than the API path's block-everything-under-fail-closed.
    #[must_use]
    pub fn check_cached(
        &self,
        workflow_name: &str,
        queue_name: &str,
        shard_id: i32,
        owner: Option<&str>,
    ) -> Option<(Uuid, String, &'static str)> {
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

/// The synthetic gate id [`AdmissionGateCache::check`] returns when the cache is
/// fail-closed.
///
/// The cache is fail-closed while uninitialized, or after `set_fail_closed()` on
/// a transient gate-DB read error. Callers can distinguish this sentinel from a
/// real operator gate (issue #618, F3): a completion-trigger start blocked by
/// the sentinel is in-flight continuation and should PROCEED rather than be
/// dropped, whereas a real gate (`id != sentinel_gate_id()`) blocks + drops +
/// counts.
#[must_use]
pub const fn sentinel_gate_id() -> Uuid {
    Uuid::nil()
}

// ── StartProducer & producer contract (issue #618) ────────────────────────────

/// How a start path consults the admission gate when enforcement is moved into
/// the core start primitive (issue #618, PR #1014).
///
/// The gate is evaluated inside `start_or_load_workflow_execution_collect` at the
/// point — under a `FOR UPDATE` lock on any prior run — where it decides it will
/// CREATE a new execution (a fresh admission). The mode selects which cache read
/// the primitive performs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateMode {
    /// Fresh admissions (API start, batch, keyed-#808): fail-closed, retryable.
    ///
    /// Consults [`AdmissionGateCache::check`], which returns the synthetic
    /// sentinel block for every start while the cache is uninitialized /
    /// fail-closed — so a transient startup / gate-DB read error cannot bypass a
    /// persisted incident gate. A blocked caller retries.
    Check,
    /// In-flight continuation / delivery (webhook delegate, completion-trigger
    /// relay): consults [`AdmissionGateCache::check_cached`].
    ///
    /// Honours only the last-known cached gates snapshot — blocks a REAL matching
    /// gate but NEVER the fail-closed sentinel — so already-committed
    /// continuation work is not permanently dropped by a boot/DB blip it cannot
    /// retry past.
    CheckCached,
}

/// A bounded enumeration of the in-process code paths that can start a workflow.
///
/// Used as the low-cardinality `producer` label on
/// [`crate::telemetry::METRIC_ADMISSION_BYPASSED`] and to render the discoverable
/// producer contract ([`producer_contract`]) at `GET /admin/gates`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StartProducer {
    /// The HTTP `POST /workflows/{name}/start` route.
    Api,
    /// The batch-start API (`POST /workflows/batch-start`).
    BatchStart,
    /// A declarative completion trigger firing a target workflow (issue #517).
    CompletionTrigger,
    /// An inbound webhook that delegates to the gated start / signal-with-start
    /// route (issue #344).
    WebhookDelegate,
    /// A due schedule slot firing on the scheduler tick.
    Scheduler,
    /// A debounced start firing from the debounce scanner (issue #499).
    Debounce,
    /// A throttled start firing from the throttle scanner (issue #607).
    Throttle,
    /// An event-batch start firing from the event-batch scanner (issue #357).
    EventBatch,
    /// The completion-trigger cross-shard outbox relay
    /// ([`enforce_completion_triggers_outbox`](crate::completion_trigger::enforce_completion_triggers_outbox)).
    CompletionTriggerOutbox,
    /// The transactional workflow-start outbox relay.
    Outbox,
}

impl StartProducer {
    /// Every producer, for exhaustive iteration in the contract and tests.
    pub const ALL: [Self; 10] = [
        Self::Api,
        Self::BatchStart,
        Self::CompletionTrigger,
        Self::WebhookDelegate,
        Self::Scheduler,
        Self::Debounce,
        Self::Throttle,
        Self::EventBatch,
        Self::CompletionTriggerOutbox,
        Self::Outbox,
    ];

    /// The stable, low-cardinality label string for metrics and the contract.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Api => "api",
            Self::BatchStart => "batch_start",
            Self::CompletionTrigger => "completion_trigger",
            Self::WebhookDelegate => "webhook_delegate",
            Self::Scheduler => "scheduler",
            Self::Debounce => "debounce",
            Self::Throttle => "throttle",
            Self::EventBatch => "event_batch",
            Self::CompletionTriggerOutbox => "completion_trigger_outbox",
            Self::Outbox => "outbox",
        }
    }
}

impl fmt::Display for StartProducer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Whether a start producer consults the admission gate before starting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProducerGateStatus {
    /// The producer checks an active gate synchronously and blocks a matching
    /// start.
    Gated,
    /// A split producer: **gated at HTTP admission time** (the initial request
    /// checks the gate before the start is durably deferred), and its later
    /// **scanner fire is exempt-with-bypass-counter** — the deferred fire relays
    /// a start already admitted through the gate, so gating it again would drop
    /// already-accepted work. Each scanner fire increments
    /// [`crate::telemetry::METRIC_ADMISSION_BYPASSED`] so the exemption is
    /// observable.
    GatedAtAdmission,
    /// The producer relays already-accepted work and is exempt by design; each
    /// relayed start increments [`crate::telemetry::METRIC_ADMISSION_BYPASSED`]
    /// so the exemption is observable.
    ExemptByDesign,
    /// **Gated authoritatively at relay/fire time** on the target shard, where
    /// the real target queue is always known. The producer re-checks the gate on
    /// the resolved queue immediately before starting a deferred/relayed start
    /// (`check_cached`): a matching gate blocks + increments
    /// [`crate::telemetry::METRIC_ADMISSION_BLOCKED`], and the producer then
    /// disposes of the blocked start **per its own contract** — the
    /// completion-trigger cross-shard outbox **drops** the row (its source
    /// terminal decision has already fired, so a permanently-gated relay must not
    /// accumulate), while the throttle scanner **re-defers** the row (its `202`
    /// promise is "nothing lost", so the held start fires once the gate opens or
    /// its `schedule_to_start` deadline passes; issue #1053). When no gate matches
    /// the relay/fire starts and increments
    /// [`crate::telemetry::METRIC_ADMISSION_BYPASSED`] (relay of pre-committed /
    /// already-accepted work). The source-side/HTTP inline check is only a
    /// best-effort fast path — the relay/fire-time check is authoritative
    /// (halt-beats-pace). See each producer's `rationale` for its disposition.
    GatedAtRelay,
}

/// One discoverable entry in the start-producer contract (issue #618, AC5).
///
/// Serialised into the `producers` block of `GET /admin/gates`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ProducerContractEntry {
    /// The producer's stable label (matches [`StartProducer::as_str`]).
    pub producer: &'static str,
    /// Whether this producer is gated or exempt-by-design.
    pub status: ProducerGateStatus,
    /// One-line human-readable rationale, especially for exempt producers.
    pub rationale: &'static str,
}

/// The full, discoverable start-producer contract (issue #618).
///
/// Every [`StartProducer`] variant appears exactly once. Surfaced at
/// `GET /admin/gates` so operators can see which producers honour a gate and
/// which are exempt-by-design without reading source.
#[must_use]
pub fn producer_contract() -> Vec<ProducerContractEntry> {
    use ProducerGateStatus::{ExemptByDesign, Gated, GatedAtAdmission, GatedAtRelay};
    vec![
        ProducerContractEntry {
            producer: StartProducer::Api.as_str(),
            status: Gated,
            rationale: "Checks the gate at the HTTP start route before any DB write.",
        },
        ProducerContractEntry {
            producer: StartProducer::BatchStart.as_str(),
            status: Gated,
            rationale: "Each item checks the gate with its resolved target shard.",
        },
        ProducerContractEntry {
            producer: StartProducer::CompletionTrigger.as_str(),
            status: Gated,
            rationale: "Checks the gate inside the source terminal commit before \
                        starting the target; a blocked start is dropped (not deferred), \
                        recorded exactly-once and counted as an admission block. A \
                        transient gate-DB blip (fail-closed sentinel) proceeds rather \
                        than dropping already-committed in-flight continuation.",
        },
        ProducerContractEntry {
            producer: StartProducer::WebhookDelegate.as_str(),
            status: Gated,
            rationale: "Two webhook producers, both gated: the INBOUND webhook receiver \
                        delegates to the gated HTTP start / signal-with-start route; the \
                        OUTBOUND webhook-delivery producer consults the cached gate \
                        snapshot before starting `webhook_delivery` and blocks (recording \
                        an admission block) when a gate matches.",
        },
        ProducerContractEntry {
            producer: StartProducer::Scheduler.as_str(),
            status: Gated,
            rationale: "Each due schedule slot checks the gate before firing.",
        },
        ProducerContractEntry {
            producer: StartProducer::Debounce.as_str(),
            status: GatedAtAdmission,
            rationale: "Gated at HTTP admission; the deferred scanner fire relays a \
                        start already admitted through the gate and is exempt-with-\
                        bypass-counter (harvest.admission.bypassed{producer=\"debounce\"}).",
        },
        ProducerContractEntry {
            producer: StartProducer::Throttle.as_str(),
            status: GatedAtRelay,
            rationale: "Gated AUTHORITATIVELY at fire time (issue #1053). The throttle \
                        scanner re-checks the gate on the workflow's real queue immediately \
                        before firing a deferred start (check_cached): a matching gate BLOCKS \
                        + counts harvest.admission.blocked and RE-DEFERS the row (nothing \
                        dropped — the held start fires once the gate opens or its \
                        schedule_to_start deadline passes); when no gate matches the start \
                        fires and counts harvest.admission.bypassed{producer=\"throttle\"}. \
                        Unlike the completion-trigger outbox relay (also GatedAtRelay, which \
                        DROPS on block), the throttle contract is \"nothing lost\". The HTTP \
                        admission pre-check is only a best-effort fast path — the fire-time \
                        check is authoritative (halt-beats-pace).",
        },
        ProducerContractEntry {
            producer: StartProducer::EventBatch.as_str(),
            status: GatedAtAdmission,
            rationale: "Gated at HTTP admission; the deferred scanner fire relays a \
                        start already admitted through the gate and is exempt-with-\
                        bypass-counter (harvest.admission.bypassed{producer=\"event_batch\"}).",
        },
        ProducerContractEntry {
            producer: StartProducer::CompletionTriggerOutbox.as_str(),
            status: GatedAtRelay,
            rationale: "Cross-shard completion-trigger relay, gated AUTHORITATIVELY at \
                        relay time on the target shard (both the immediate spawn relay and \
                        the scanner). The real target queue is resolved on the target shard \
                        and re-checked against the gate (check_cached) immediately before \
                        starting: a matching gate BLOCKS + drops the outbox row + counts \
                        harvest.admission.blocked; when no gate matches the relay starts and \
                        counts harvest.admission.bypassed{producer=\"completion_trigger_outbox\"}. \
                        The source-side inline check is only a best-effort fast path.",
        },
        ProducerContractEntry {
            producer: StartProducer::Outbox.as_str(),
            status: ExemptByDesign,
            rationale: "Relays workflow-start requests durably committed before the gate \
                        was raised; gating would drop already-accepted in-flight work. \
                        Each relayed start increments harvest.admission.bypassed{producer=\"outbox\"}.",
        },
    ]
}

// ── Global gate cache accessor (issue #618) ───────────────────────────────────

/// Process-global handle to the shared [`AdmissionGateCache`].
///
/// Populated by the plugin at boot with the same `Arc` the management API and
/// the background refresh loop use, so the core completion-trigger start path
/// observes gate creates/lifts without threading the cache through every
/// call site. Mirrors `completion_trigger::GLOBAL_WORKFLOW_METADATA` and
/// `completion_callback::GLOBAL_CALLBACK_CONFIG`.
///
/// `None` (the default, and standalone integrations that never set it) means
/// the core gate check is skipped — byte-identical to pre-#618 behaviour.
pub static GLOBAL_ADMISSION_GATE_CACHE: std::sync::RwLock<Option<Arc<AdmissionGateCache>>> =
    std::sync::RwLock::new(None);

/// Install (or clear, with `None`) the process-global admission gate cache.
///
/// Called by the plugin at boot with the shared cache `Arc`, and cleared with
/// `None` in `stop_harvest_runtime` so a stopped plugin's cache (whose refresh
/// loop is frozen) never leaks into a sibling plugin's completion triggers.
///
/// If a *different* non-`None` cache is already installed, a `tracing::warn!` is
/// emitted before replacing it — two live `HarvestPlugin` builds in one process
/// (parallel tests, multi-tenant) would otherwise silently share one plugin's
/// gate state. Mirrors `completion_callback`'s `GLOBAL_CALLBACK_CONFIG` guard.
/// Recovers a poisoned lock (`into_inner`) rather than silently no-op'ing, so a
/// prior panic while holding the write lock cannot wedge the global.
pub fn set_global_admission_gate_cache(cache: Option<Arc<AdmissionGateCache>>) {
    let mut guard = GLOBAL_ADMISSION_GATE_CACHE
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let (Some(existing), Some(incoming)) = (guard.as_ref(), cache.as_ref())
        && !Arc::ptr_eq(existing, incoming)
    {
        tracing::warn!(
            "set_global_admission_gate_cache: replacing an already-installed, \
             divergent admission gate cache. Two live HarvestPlugin builds in one \
             process will not share gate state coherently."
        );
    }
    *guard = cache;
}

/// Read the process-global admission gate cache handle, if installed.
///
/// Recovers a poisoned lock (`into_inner`) rather than failing closed to `None`
/// — a `None` here would silently skip the completion-trigger gate check.
#[must_use]
pub fn global_admission_gate_cache() -> Option<Arc<AdmissionGateCache>> {
    GLOBAL_ADMISSION_GATE_CACHE
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
}

/// Process-global metrics recorder for admission-gate block counting (issue #618).
///
/// Published by the plugin at boot alongside [`GLOBAL_ADMISSION_GATE_CACHE`]
/// (the same `Arc<dyn MetricsRecorder>` the workers/scanners use). The
/// completion-trigger block path (`evaluate_triggers_for_execution`) falls back
/// to this recorder when its caller passes `metrics: None` — which the
/// cancel / terminate / parent-close-cascade paths in `execution.rs` all do — so
/// a completion-trigger start blocked by an active gate is counted on
/// `harvest.admission.blocked` regardless of how the source reached terminal.
/// `None` (standalone integrations, or before boot) means the fallback is
/// skipped and behaviour is byte-identical to a caller-supplied `None`.
pub static GLOBAL_ADMISSION_METRICS: std::sync::RwLock<
    Option<Arc<dyn crate::telemetry::MetricsRecorder>>,
> = std::sync::RwLock::new(None);

/// Install (or clear, with `None`) the process-global admission metrics recorder.
///
/// Called by the plugin at boot with the shared recorder `Arc`, and cleared with
/// `None` in `stop_harvest_runtime`. Recovers a poisoned lock (`into_inner`)
/// rather than silently no-op'ing. Mirrors [`set_global_admission_gate_cache`].
pub fn set_global_admission_metrics(recorder: Option<Arc<dyn crate::telemetry::MetricsRecorder>>) {
    let mut guard = GLOBAL_ADMISSION_METRICS
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    *guard = recorder;
}

/// Read the process-global admission metrics recorder, if installed.
///
/// Recovers a poisoned lock (`into_inner`) rather than failing to `None`.
#[must_use]
pub fn global_admission_metrics() -> Option<Arc<dyn crate::telemetry::MetricsRecorder>> {
    GLOBAL_ADMISSION_METRICS
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
}

// ── DB layer (requires `db` feature) ─────────────────────────────────────────

#[cfg(feature = "db")]
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
    pub id: Uuid,
    pub scope_kind: String,
    pub scope_value: Option<String>,
    pub reason: String,
    pub message: Option<String>,
    pub created_by: String,
    pub created_at: DateTime<Utc>,
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

    #[test]
    fn check_cached_honours_snapshot_under_fail_closed() {
        // issue #618 (Codex P1): a real gate loaded into the snapshot then
        // fail-closed must still MATCH via check_cached (never the sentinel),
        // while `check()` returns the block-everything sentinel.
        let cache = AdmissionGateCache::new();
        cache.refresh(vec![fleet_gate("cached-incident")]);
        cache.set_fail_closed();

        // check() fails closed → synthetic sentinel block.
        let (sentinel_id, _r, _s) = cache.check("wf", "q", 0, None).expect("fail-closed blocks");
        assert_eq!(sentinel_id, sentinel_gate_id());

        // check_cached() returns the REAL matched gate (not the sentinel).
        let (real_id, reason, scope_kind) = cache
            .check_cached("wf", "q", 0, None)
            .expect("cached gate matches under fail-closed");
        assert_ne!(real_id, sentinel_gate_id());
        assert_eq!(reason, "cached-incident");
        assert_eq!(scope_kind, "fleet");
    }

    #[test]
    fn check_cached_returns_none_on_empty_snapshot() {
        // An empty snapshot (boot window / never-loaded) matches nothing, so a
        // completion-trigger start PROCEEDS rather than being dropped.
        let cache = AdmissionGateCache::new_fail_closed();
        assert!(cache.check_cached("wf", "q", 0, None).is_none());
        // And an initialized-but-empty cache also matches nothing.
        let open = AdmissionGateCache::new();
        assert!(open.check_cached("wf", "q", 0, None).is_none());
    }

    #[test]
    fn apply_created_makes_a_new_gate_matchable_even_under_fail_closed() {
        // issue #618 (F-round16): write-through the just-persisted gate so a
        // completion-trigger start (which consults check_cached and does NOT retry
        // after a synthetic fail-closed block) never MISSES a just-created gate,
        // even if the follow-up full refresh fails and the cache is fail-closed.
        let cache = AdmissionGateCache::new();
        // Simulate the pre-write-through state: an open/empty snapshot that a failed
        // post-create refresh would leave stale.
        cache.set_fail_closed();
        // BEFORE the delta: the new gate is invisible — this is the bug the
        // write-through closes (a stale-open snapshot misses the new gate).
        assert!(
            cache.check_cached("wf", "q", 0, None).is_none(),
            "precondition: an un-written-through snapshot misses the new gate"
        );

        cache.apply_created(fleet_gate("new-incident"));

        // AFTER the delta: check_cached matches the REAL gate (never the sentinel),
        // regardless of the fail-closed flag.
        let (real_id, reason, scope_kind) = cache
            .check_cached("wf", "q", 0, None)
            .expect("write-through gate matches under fail-closed");
        assert_ne!(real_id, sentinel_gate_id());
        assert_eq!(reason, "new-incident");
        assert_eq!(scope_kind, "fleet");
    }

    #[test]
    fn apply_lifted_stops_a_gate_matching_even_under_fail_closed() {
        // issue #618 (F-round16): write-through the lift so a just-lifted gate stops
        // blocking on this replica even if the follow-up refresh fails (fail-closed).
        let g = fleet_gate("lift-me");
        let gid = g.id.0;
        let cache = AdmissionGateCache::new();
        cache.refresh(vec![g]);
        cache.set_fail_closed();
        // BEFORE the delta: the stale snapshot still contains the lifted gate, so
        // check_cached keeps matching it (the durable false-block bug).
        assert!(
            cache.check_cached("wf", "q", 0, None).is_some(),
            "precondition: an un-written-through snapshot still honours the lifted gate"
        );

        cache.apply_lifted(gid);

        assert!(
            cache.check_cached("wf", "q", 0, None).is_none(),
            "a lifted gate no longer matches via check_cached, even under fail-closed"
        );
    }

    #[test]
    fn apply_lifted_without_fail_closed_stops_check_blocking_the_lifted_target() {
        // issue #618 (F-round16): the new handler applies the lift delta and does NOT
        // arm fail-closed on a refresh failure. So check() (the API path) reads the
        // delta-applied, still-initialized snapshot and does NOT synthetically block
        // the target the operator just unblocked. (Contrast the old behavior: a
        // set_fail_closed() here would make check() return the block-everything
        // sentinel, re-blocking the just-lifted target until the next refresh.)
        let g = fleet_gate("lift-me");
        let gid = g.id.0;
        let cache = AdmissionGateCache::new();
        cache.refresh(vec![g]); // initialized, gate present
        assert!(
            cache.check("wf", "q", 0, None).is_some(),
            "precondition: the gate blocks via check() before the lift"
        );

        // Handler's post-lift, refresh-failed path: apply the delta, DON'T fail-closed.
        cache.apply_lifted(gid);

        assert!(
            cache.check("wf", "q", 0, None).is_none(),
            "check() must not block a just-lifted target when the delta is written \
             through and fail-closed is not armed"
        );
    }

    #[test]
    fn apply_created_is_idempotent_by_id() {
        // A concurrent background refresh may already have picked up the persisted
        // gate; applying the same gate again must not duplicate it.
        let g = fleet_gate("dup");
        let cache = AdmissionGateCache::new();
        cache.apply_created(g.clone());
        cache.apply_created(g);
        assert_eq!(cache.active_count(), 1, "apply_created dedupes by id");
    }

    #[test]
    fn apply_lifted_absent_id_is_a_noop() {
        let cache = AdmissionGateCache::new();
        cache.refresh(vec![fleet_gate("keep")]);
        cache.apply_lifted(Uuid::new_v4()); // unknown id
        assert_eq!(
            cache.active_count(),
            1,
            "lifting an absent id leaves the snapshot unchanged"
        );
    }

    // ── issue #618: StartProducer + producer contract + global cache ──────────

    #[test]
    fn start_producer_as_str_is_bounded_and_snake_case() {
        // Every variant maps to a distinct, stable, low-cardinality label.
        let labels: Vec<&str> = StartProducer::ALL.iter().map(|p| p.as_str()).collect();
        assert!(labels.contains(&"api"));
        assert!(labels.contains(&"batch_start"));
        assert!(labels.contains(&"completion_trigger"));
        assert!(labels.contains(&"webhook_delegate"));
        assert!(labels.contains(&"debounce"));
        assert!(labels.contains(&"throttle"));
        assert!(labels.contains(&"event_batch"));
        assert!(labels.contains(&"completion_trigger_outbox"));
        assert!(labels.contains(&"outbox"));
        // No duplicates (each label is a distinct metric series).
        let mut sorted = labels.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), labels.len(), "producer labels must be unique");
    }

    #[test]
    fn producer_contract_covers_every_producer_and_marks_outbox_exempt() {
        let contract = producer_contract();
        // Every StartProducer variant appears exactly once.
        for p in StartProducer::ALL {
            let count = contract.iter().filter(|e| e.producer == p.as_str()).count();
            assert_eq!(
                count,
                1,
                "producer {} must appear exactly once in the contract",
                p.as_str()
            );
        }
        // The API producer is gated; the outbox producer is exempt-by-design.
        let api = contract
            .iter()
            .find(|e| e.producer == "api")
            .expect("api entry");
        assert_eq!(api.status, ProducerGateStatus::Gated);
        let outbox = contract
            .iter()
            .find(|e| e.producer == "outbox")
            .expect("outbox entry");
        assert_eq!(outbox.status, ProducerGateStatus::ExemptByDesign);
        assert!(
            !outbox.rationale.is_empty(),
            "exempt producer must state a rationale (AC3/AC5)"
        );
        // Completion triggers are now gated (AC1).
        let ct = contract
            .iter()
            .find(|e| e.producer == "completion_trigger")
            .expect("completion_trigger entry");
        assert_eq!(ct.status, ProducerGateStatus::Gated);
        // Deferred-fire producers gated at HTTP admission, scanner fire
        // exempt-with-bypass-counter (issue #618, F2). Throttle is NO LONGER in
        // this set — it flipped to GatedAtRelay (fire-time gate, re-defer) in
        // issue #1053; debounce + event_batch remain GatedAtAdmission (#1053
        // scopes only throttle).
        for split in ["debounce", "event_batch"] {
            let e = contract
                .iter()
                .find(|e| e.producer == split)
                .unwrap_or_else(|| panic!("{split} entry"));
            assert_eq!(
                e.status,
                ProducerGateStatus::GatedAtAdmission,
                "{split} must be gated-at-admission"
            );
            assert!(!e.rationale.is_empty());
        }
        // The cross-shard completion-trigger relay is gated authoritatively at
        // relay time (issue #618, F-round7).
        let cto = contract
            .iter()
            .find(|e| e.producer == "completion_trigger_outbox")
            .expect("completion_trigger_outbox entry");
        assert_eq!(cto.status, ProducerGateStatus::GatedAtRelay);
        assert!(!cto.rationale.is_empty());
        // Throttle is gated authoritatively at fire time (issue #1053): a closed
        // gate blocks the deferred fire and RE-DEFERS the row (nothing dropped).
        let throttle = contract
            .iter()
            .find(|e| e.producer == "throttle")
            .expect("throttle entry");
        assert_eq!(throttle.status, ProducerGateStatus::GatedAtRelay);
        assert!(!throttle.rationale.is_empty());
    }

    #[test]
    fn sentinel_gate_id_matches_fail_closed_check() {
        // The fail-closed sentinel a fresh cache returns must equal the documented
        // sentinel (issue #618, F3) so callers can distinguish it from a real gate.
        let cache = AdmissionGateCache::new_fail_closed();
        let (gate_id, _reason, _scope) = cache
            .check("wf", "q", 0, None)
            .expect("fail-closed cache blocks");
        assert_eq!(
            gate_id,
            sentinel_gate_id(),
            "fail-closed check() must return the sentinel gate id"
        );
        assert_eq!(sentinel_gate_id(), Uuid::nil());
    }

    // Serialises the two tests that mutate the process-global gate cache so they
    // don't race under the default parallel test runner (issue #618 review).
    static GLOBAL_CACHE_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn global_gate_cache_round_trips_and_shares_state() {
        let _g = GLOBAL_CACHE_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Default: unset → None (backward-compat: core producers skip the check).
        set_global_admission_gate_cache(None);
        assert!(global_admission_gate_cache().is_none());

        let cache = std::sync::Arc::new(AdmissionGateCache::new());
        set_global_admission_gate_cache(Some(std::sync::Arc::clone(&cache)));
        let fetched = global_admission_gate_cache().expect("cache installed");
        // Same underlying instance: a refresh on one Arc is visible via the other.
        fetched.refresh(vec![fleet_gate("global-incident")]);
        let hit = cache.check("wf", "q", 0, None);
        assert!(hit.is_some(), "shared Arc must observe the refresh");
        assert_eq!(hit.unwrap().1, "global-incident");

        // Reset so serial tests are unaffected by process-global state.
        set_global_admission_gate_cache(None);
    }

    #[test]
    fn global_gate_cache_divergent_replace_and_clear() {
        let _g = GLOBAL_CACHE_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // issue #618 (F5): a divergent re-publish replaces (last-write-wins; the
        // warn is a side effect), and clearing with None fully removes the handle
        // (teardown semantics mirrored by stop_harvest_runtime).
        let a = std::sync::Arc::new(AdmissionGateCache::new());
        let b = std::sync::Arc::new(AdmissionGateCache::new());
        set_global_admission_gate_cache(Some(std::sync::Arc::clone(&a)));
        // Re-publishing the SAME Arc is a benign no-op (ptr-eq, no divergence).
        set_global_admission_gate_cache(Some(std::sync::Arc::clone(&a)));
        assert!(std::sync::Arc::ptr_eq(
            &global_admission_gate_cache().expect("installed"),
            &a
        ));
        // A divergent re-publish replaces it.
        set_global_admission_gate_cache(Some(std::sync::Arc::clone(&b)));
        assert!(std::sync::Arc::ptr_eq(
            &global_admission_gate_cache().expect("installed"),
            &b
        ));
        // Clearing removes it entirely.
        set_global_admission_gate_cache(None);
        assert!(global_admission_gate_cache().is_none());
    }
}
