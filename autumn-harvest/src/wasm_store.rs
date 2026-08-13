//! Postgres content-hash storage and worker-dispatch resolution for sandboxed
//! WebAssembly activities (issue #965, milestone 2).
//!
//! This module is the storage half of issue #965. It sits on top of the pure
//! runtime in [`crate::wasm_activities`]: modules are published to and resolved
//! from `harvest_wasm_modules`, and [`resolve_wasm_dispatch`] turns a resolved
//! module into a ready-to-invoke [`PreparedWasmActivity`] for the worker
//! dispatch seam.
//!
//! Everything here is gated behind the `wasm-activities` Cargo feature (which
//! implies `db`), so a default build pulls in neither `wasmtime` nor this table
//! access and is byte-for-byte unchanged.
//!
//! # Content addressing and the single-active invariant
//!
//! A module version is identified by the lowercase-hex SHA-256 of its bytes
//! ([`WasmModuleStore::compute_hash`]). The `harvest_wasm_modules` table has a
//! composite primary key `(hash, activity_name)` so identical bytes can bind to
//! two different activity names independently. A partial unique index
//! (`WHERE active`) guarantees at most one **active** version per activity name,
//! so a hot-swap (publish v2 → deactivate v1 → activate v2) can never leave two
//! active versions racing.
//!
//! [`publish_wasm_module`] serialises concurrent publishes for the same activity
//! name with a transaction-scoped advisory lock, so two workers publishing
//! different versions of the same activity converge on exactly one active row.

use std::sync::Arc;
use std::time::{Duration, Instant};

use diesel::{ExpressionMethods, QueryDsl};
use tokio_util::sync::CancellationToken;
use wasmtime::Module;

use crate::error::{HarvestResult, database_error};
use crate::failure::{ActivityFailure, IntoActivityErrorString};
use crate::policy::RetryPolicy;
use crate::wasm_activities::{WasmCapabilities, WasmLimits, WasmModuleStore};

/// Hard ceiling on a single published WASM module's byte length: 32 MiB.
///
/// Enforced by [`publish_wasm_module`] **before** any hashing or database work,
/// so an oversized blob is rejected without touching the connection.
pub const MAX_WASM_MODULE_BYTES: usize = 32 * 1024 * 1024;

/// The host capabilities and resource limits applied to one WASM activity's
/// guest invocations.
///
/// Carried on the [`crate::worker::HandlerRegistry`] per registered WASM
/// activity so the worker dispatch seam can resolve the sandbox policy without a
/// second lookup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WasmBinding {
    /// Host capabilities granted to the guest (deny-all by default).
    pub capabilities: WasmCapabilities,
    /// Per-invocation resource budget (fuel, memory, wall clock).
    pub limits: WasmLimits,
}

/// A single WASM activity registration supplied to the builder (issue #965).
///
/// Bundles the activity's name, its module bytes, the scheduling knobs a native
/// `#[activity]` exposes (queue, retry policy, start-to-close, schedule-to-close)
/// and the sandbox [`WasmCapabilities`]/[`WasmLimits`]. Construct with
/// [`WasmActivityRegistration::new`] and refine with the fluent `with_*` setters.
///
/// # Knobs a native `#[activity]` has that this deliberately omits
///
/// These are absent by design in the spike, not oversights (issue #965 review):
///
/// * `heartbeat_timeout` — a guest cannot heartbeat. Delivering heartbeats into
///   the guest is the main GA gap; see `docs/rnd/wasm-activities-spike.md` §4.
/// * `circuit_breaker` and `rate_limit_*` — both exist to protect a *downstream
///   dependency*. Under deny-all capabilities a guest performs no I/O, so there
///   is no downstream to protect. Revisit if network capabilities are ever
///   granted.
/// * `max_input_bytes` / `max_result_bytes` — guest output is already bounded by
///   [`crate::wasm_activities::WASM_MAX_OUTPUT_BYTES`], checked before the host
///   parses it.
/// * `schedule_to_start` — no WASM-specific queue-wait semantics exist.
///
/// Because they are hardcoded `None` on the generated [`crate::info::ActivityInfo`],
/// setting them elsewhere would silently do nothing; they are listed here so that
/// is a documented decision rather than a surprise.
#[derive(Debug, Clone)]
pub struct WasmActivityRegistration {
    /// Snake-case activity name — the key both the task queue and the module
    /// store use.
    pub name: String,
    /// The compiled `.wasm` module bytes to publish for this activity.
    pub wasm_bytes: Vec<u8>,
    /// Task queue override. `None` = the `"default"` queue.
    pub queue: Option<String>,
    /// Retry policy applied to failed attempts.
    pub retry: RetryPolicy,
    /// Start-to-close timeout for a single attempt. `None` = unbounded;
    /// default `Some(30s)`.
    pub start_to_close: Option<Duration>,
    /// Cross-retry schedule-to-close deadline (issue #378). `None` (the default)
    /// = unbounded, matching a native `#[activity]` that does not set one.
    ///
    /// This is the only bound that stops a deterministically-failing guest from
    /// walking its entire retry curve: `start_to_close` bounds one attempt,
    /// `schedule_to_close` bounds the whole sequence.
    pub schedule_to_close: Option<Duration>,
    /// Host capabilities granted to the guest (deny-all by default).
    pub capabilities: WasmCapabilities,
    /// Per-invocation resource budget.
    pub limits: WasmLimits,
}

impl WasmActivityRegistration {
    /// Register `name` backed by `wasm_bytes` with sane defaults: no queue
    /// override, the default [`RetryPolicy`], a 30s start-to-close, deny-all
    /// capabilities, and the default [`WasmLimits`].
    #[must_use]
    pub fn new(name: impl Into<String>, wasm_bytes: impl Into<Vec<u8>>) -> Self {
        Self {
            name: name.into(),
            wasm_bytes: wasm_bytes.into(),
            queue: None,
            retry: RetryPolicy::default(),
            start_to_close: Some(Duration::from_secs(30)),
            schedule_to_close: None,
            capabilities: WasmCapabilities::default(),
            limits: WasmLimits::default(),
        }
    }

    /// Route this activity's tasks to `queue`.
    #[must_use]
    pub fn with_queue(mut self, queue: impl Into<String>) -> Self {
        self.queue = Some(queue.into());
        self
    }

    /// Override the retry policy.
    #[must_use]
    pub fn with_retry(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    /// Override the start-to-close timeout (`None` = unbounded).
    #[must_use]
    pub const fn with_start_to_close(mut self, start_to_close: Option<Duration>) -> Self {
        self.start_to_close = start_to_close;
        self
    }

    /// Bound the activity's whole retry sequence with a schedule-to-close
    /// deadline (issue #378). `None` leaves it unbounded.
    #[must_use]
    pub const fn with_schedule_to_close(mut self, schedule_to_close: Option<Duration>) -> Self {
        self.schedule_to_close = schedule_to_close;
        self
    }

    /// Grant host capabilities to the guest.
    #[must_use]
    pub fn with_capabilities(mut self, capabilities: WasmCapabilities) -> Self {
        self.capabilities = capabilities;
        self
    }

    /// Override the per-invocation resource limits.
    #[must_use]
    pub const fn with_limits(mut self, limits: WasmLimits) -> Self {
        self.limits = limits;
        self
    }

    /// Project the sandbox policy carried on the worker registry.
    #[must_use]
    pub fn binding(&self) -> WasmBinding {
        WasmBinding {
            capabilities: self.capabilities.clone(),
            limits: self.limits,
        }
    }
}

/// A metadata-only view of a stored module version (no bytes) for the admin
/// listing surface.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WasmModuleRow {
    /// Content hash (lowercase-hex SHA-256).
    pub hash: String,
    /// Activity name this version is bound to.
    pub activity_name: String,
    /// Whether this is the active version for its activity name.
    pub active: bool,
    /// When this version was published (or last (re)activated).
    pub published_at: chrono::DateTime<chrono::Utc>,
}

/// Where a prepared activity's compiled module comes from.
///
/// A cache hit at resolve time carries the already-compiled module
/// ([`Self::Compiled`]); a cache miss defers the CPU-bound wasmtime compile to
/// [`PreparedWasmActivity::invoke`] (which the worker runs on the blocking
/// pool), carrying only the raw bytes ([`Self::Deferred`]) so no compilation
/// ever runs on the async dispatch path (issue #965 review).
enum WasmModuleSource {
    /// Cache hit: the module was already compiled and is served directly.
    Compiled(Arc<Module>),
    /// Cache miss: compile-then-cache these bytes on the blocking pool inside
    /// `invoke`. The content-integrity check (hash vs bytes) runs there too.
    Deferred { hash: String, bytes: Vec<u8> },
}

/// A resolved WASM activity ready to invoke against a JSON input.
///
/// Holds the shared engine store plus the caller's capability/limit/deadline
/// policy. The compiled module is either already cached (a resolve-time hit) or
/// compiled lazily on first [`invoke`](Self::invoke) (a resolve-time miss), so
/// the async dispatch path never runs wasmtime compilation.
pub struct PreparedWasmActivity {
    /// The shared engine + compiled-module cache (one per worker).
    pub store: Arc<WasmModuleStore>,
    /// The compiled module, or the bytes to compile on the blocking pool.
    source: WasmModuleSource,
    /// Host capabilities granted to the guest.
    pub caps: WasmCapabilities,
    /// Per-invocation resource budget.
    pub limits: WasmLimits,
    /// Effective wall-clock deadline for the invocation (typically the
    /// activity's start-to-close), clamped by `limits.max_wall_clock`.
    pub deadline: Option<Duration>,
    /// Optional cancellation token: when set, a cancelled guest is
    /// cooperatively interrupted within ~1 epoch tick instead of running to the
    /// wall-clock ceiling (issue #965 review).
    pub cancel: Option<CancellationToken>,
    /// The instant the activity's start-to-close clock began — the worker
    /// captures this as `ActivityStarted` is recorded, *before* dispatch
    /// resolution (issue #965 review round 7). By the time the guest is about to
    /// run, this clock has ticked through active-hash resolution, cold-cache
    /// byte fetch from Postgres, the async→blocking handoff, and (on a cache
    /// miss) module compilation. Measuring `dispatch_start.elapsed()` just
    /// before the guest invoke charges that entire pre-guest interval against
    /// the guest's epoch deadline in one measurement — subsuming the earlier
    /// compile-only subtraction. `None` charges nothing (a non-worker
    /// constructor that has no dispatch-start reference).
    pub dispatch_start: Option<Instant>,
}

impl PreparedWasmActivity {
    /// Run the guest against `input`, returning its JSON output or a typed
    /// [`ActivityFailure`].
    ///
    /// This is CPU-bound and blocking (it drives the guest to completion on the
    /// calling thread), so the worker calls it inside `spawn_blocking`. On a
    /// resolve-time cache miss it also compiles the module here (on that same
    /// blocking thread), verifying content integrity before compiling — so
    /// wasmtime compilation never occupies an async worker thread.
    ///
    /// # Errors
    ///
    /// Returns the typed [`ActivityFailure`] classifying any sandbox denial,
    /// resource exhaustion, guest trap, ABI violation, cooperative
    /// cancellation, contained host-glue panic, or (on a deferred-compile miss)
    /// a `WasmModuleInvalid` integrity/compile failure.
    pub fn invoke(&self, input: &serde_json::Value) -> Result<serde_json::Value, ActivityFailure> {
        // Resolve the module (compiling on a Deferred cache miss), then hand the
        // raw start-to-close deadline plus the `dispatch_start` instant to the
        // invoke path. ALL pre-guest overhead accounting happens there, at the
        // last moment before the guest's epoch deadline is armed (issue #965
        // review rounds 7 & 9): the whole pre-guest interval — active-hash
        // resolution, cold-cache byte fetch from Postgres, the async→blocking
        // handoff, module compilation (this function), and input serialization —
        // is charged against the guest's deadline in ONE measurement, and an
        // attempt whose overhead already spent the budget fails fast as a
        // retryable `ResourceExhausted` *without invoking the guest*. Compile
        // stays here because it happens on this blocking thread and needs the
        // cancel-before-compile skip below.
        let module = match &self.source {
            WasmModuleSource::Compiled(module) => Arc::clone(module),
            WasmModuleSource::Deferred { hash, bytes } => {
                // Skip a cold compile entirely if the attempt is already
                // cancelled (issue #965 review). The cancellation-aware epoch
                // deadline is only armed inside the invoke *after* this compile,
                // so without this check a cancel arriving during a large cold
                // compile drops the join handle while the blocking thread keeps
                // compiling to completion. NB: a `Module::new` compile ALREADY in
                // flight cannot be interrupted (wasmtime compilation is not
                // cancellable); this only avoids *starting* one. The blast radius
                // is bounded by the 32 MiB module-size cap, and the GA mitigation
                // is to precompile at publish/seed time so no compile ever runs
                // on the dispatch path.
                if self
                    .cancel
                    .as_ref()
                    .is_some_and(CancellationToken::is_cancelled)
                {
                    return Err(ActivityFailure::resource_exhausted(
                        "wasm activity cancelled before compilation",
                    ));
                }
                // Compile (and integrity-check) here, on the blocking pool. A
                // concurrent dispatch may have compiled the same hash already;
                // `get_or_compile` serves the cached `Arc` if so, otherwise
                // compiles and caches it.
                self.store.get_or_compile(hash, bytes).map_err(|e| {
                    ActivityFailure::wasm_module_invalid(format!(
                        "wasm module {hash} is invalid: {e}"
                    ))
                })?
            }
        };
        crate::wasm_activities::invoke_wasm_activity_cancellable(
            &self.store,
            &module,
            input,
            &self.caps,
            &self.limits,
            self.deadline,
            self.dispatch_start,
            self.cancel.as_ref(),
        )
    }
}

/// The guest's remaining wall-clock budget after charging pre-guest overhead
/// against the configured start-to-close deadline (issue #965 review).
///
/// Distinguishes the three outcomes the caller must handle differently: fail
/// fast without invoking, invoke with a reduced deadline, or invoke unbounded.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum InvokeBudget {
    /// A start-to-close deadline was configured but pre-guest overhead already
    /// met or exceeded it before the guest could start. The attempt has spent
    /// its whole budget resolving/fetching/compiling, so it must fail fast as a
    /// retryable `ResourceExhausted` **without invoking the guest** — a fast
    /// guest handed a clamped 1-tick deadline could otherwise race to
    /// completion and be recorded successful past its own deadline.
    Exhausted,
    /// A strictly-positive remaining budget; invoke the guest with this deadline.
    Remaining(Duration),
    /// No start-to-close deadline configured; invoke the guest unbounded (the
    /// mandatory `limits.max_wall_clock` ceiling still applies inside the invoke).
    Unbounded,
}

/// Charge `pre_guest_elapsed` — the total time from when the activity's
/// start-to-close clock began until just before the guest is invoked — against
/// the guest's wall-clock `deadline` (issue #965 review rounds 7 & 9).
///
/// This interval accrues against `start_to_close` from the moment the worker
/// records `ActivityStarted`, and covers dispatch resolution (active-hash
/// lookup), cold-cache byte fetch from Postgres, the async→blocking handoff,
/// and (on a cache miss) module compilation — every millisecond of pre-guest
/// overhead, not just compile. Resolving it here classifies the guest's real
/// remaining budget:
///
/// * `None` (no deadline configured) → [`InvokeBudget::Unbounded`] — the
///   mandatory `limits.max_wall_clock` ceiling still applies inside the invoke.
/// * Pre-guest overhead that meets or exceeds the deadline
///   (`pre_guest_elapsed >= d`) → [`InvokeBudget::Exhausted`], so the attempt
///   fails fast **before** the guest is invoked rather than running under a
///   zero-that-clamps-to-one-tick deadline it may win a race against.
/// * A strictly-positive remainder → [`InvokeBudget::Remaining`], tightening the
///   guest's epoch deadline toward the real remaining budget. On a cache hit
///   with negligible resolution overhead this is effectively the full deadline.
#[must_use]
pub(crate) fn effective_invoke_deadline(
    deadline: Option<Duration>,
    pre_guest_elapsed: Duration,
) -> InvokeBudget {
    let Some(d) = deadline else {
        return InvokeBudget::Unbounded;
    };
    match d.checked_sub(pre_guest_elapsed) {
        Some(remaining) if remaining > Duration::ZERO => InvokeBudget::Remaining(remaining),
        // `None` (over budget) or `Some(ZERO)` (exactly at budget): the guest
        // never gets a positive budget, so fail fast without invoking.
        _ => InvokeBudget::Exhausted,
    }
}

/// The outcome of resolving a WASM activity for dispatch.
///
/// Either the activity is ready to run ([`WasmDispatch::Invoke`]) or resolution
/// failed and the worker should record the carried typed-failure payload as an
/// ordinary `ActivityFailed` ([`WasmDispatch::Fail`]).
pub enum WasmDispatch {
    /// Resolution succeeded; run the guest.
    Invoke(PreparedWasmActivity),
    /// Resolution failed; the string is a typed error payload produced by
    /// [`IntoActivityErrorString::into_error_payload`], carrying the
    /// retryable/non-retryable classification the worker honours.
    Fail(String),
}

/// Publish `bytes` as the active WASM module for `activity_name` (issue #965).
///
/// Idempotent and hot-swap safe. Rejects an oversized blob
/// (> [`MAX_WASM_MODULE_BYTES`]) **before** any hashing or database work.
/// Otherwise, in ONE transaction that first serialises concurrent publishes for
/// the same name with a transaction-scoped advisory lock:
///
/// 1. deactivate every existing active row for `activity_name`, then
/// 2. upsert `(hash, activity_name)` with `active = true`.
///
/// Republishing identical bytes re-activates the same row (a no-op beyond
/// refreshing `published_at`). Returns the content hash.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Config`] for an oversized blob, or
/// `HarvestError::Database` on any transaction failure.
pub async fn publish_wasm_module(
    conn: &mut diesel_async::AsyncPgConnection,
    activity_name: &str,
    bytes: &[u8],
) -> HarvestResult<String> {
    use diesel_async::{AsyncConnection, RunQueryDsl};

    if bytes.len() > MAX_WASM_MODULE_BYTES {
        return Err(crate::error::HarvestError::Config(format!(
            "wasm module for activity '{activity_name}' is {} bytes, exceeding the maximum \
             of {MAX_WASM_MODULE_BYTES} bytes",
            bytes.len()
        )));
    }

    let hash = WasmModuleStore::compute_hash(bytes);
    let hash_for_txn = hash.clone();
    let name = activity_name.to_owned();

    Box::pin(
        conn.transaction::<(), crate::error::HarvestError, _>(async |conn| {
            use crate::schema::harvest_wasm_modules::dsl as m;

            // Serialise concurrent publishes for the SAME activity name so the
            // deactivate + upsert pair is atomic and the partial unique index
            // (`WHERE active`) can never see two active rows mid-swap. The lock
            // is transaction-scoped (auto-released on commit/rollback), and
            // keyed on the activity name so distinct names never contend.
            diesel::sql_query("SELECT pg_advisory_xact_lock(hashtext($1)::bigint)")
                .bind::<diesel::sql_types::Text, _>(&name)
                .execute(conn)
                .await
                .map_err(database_error)?;

            // 1. Deactivate every currently-active version for this name.
            diesel::update(m::harvest_wasm_modules.filter(m::activity_name.eq(&name)))
                .filter(m::active.eq(true))
                .set(m::active.eq(false))
                .execute(conn)
                .await
                .map_err(database_error)?;

            // 2. Upsert the requested version as active.
            let new_row = crate::models::NewHarvestWasmModule {
                hash: &hash_for_txn,
                activity_name: &name,
                wasm_bytes: bytes,
                active: true,
            };
            diesel::insert_into(m::harvest_wasm_modules)
                .values(&new_row)
                .on_conflict((m::hash, m::activity_name))
                .do_update()
                .set((m::active.eq(true), m::published_at.eq(diesel::dsl::now)))
                .execute(conn)
                .await
                .map_err(database_error)?;

            Ok(())
        }),
    )
    .await?;

    Ok(hash)
}

/// Seed `bytes` as a WASM module version for `activity_name` at worker startup
/// (issue #965).
///
/// **Seed, not publish.** Unlike [`publish_wasm_module`] — which always
/// activates the given bytes (operator hot-swap intent) — seeding is
/// *activate-only-if-absent*: it ensures the `(hash, activity_name)` row exists
/// (so the bytes are fetchable by hash for in-flight pinned attempts and for
/// this worker to run its own embedded version), but it activates the embedded
/// version **only when no active version already exists** for the name.
///
/// This is the fix for a rolling-deploy hazard: if the database has already been
/// hot-swapped to v2 and an *older* worker embedding v1 restarts, a blind
/// publish would flip the shard back to v1 (running stale code until another
/// publisher wins). Seeding leaves the existing active v2 untouched while still
/// making v1's bytes available.
///
/// Idempotent, and serialised against concurrent publishes/seeds for the same
/// name with the same transaction-scoped advisory lock `publish_wasm_module`
/// uses. Rejects an oversized blob (> [`MAX_WASM_MODULE_BYTES`]) **before** any
/// hashing or database work. Returns the content hash.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Config`] for an oversized blob, or
/// `HarvestError::Database` on any transaction failure.
pub async fn seed_wasm_module(
    conn: &mut diesel_async::AsyncPgConnection,
    activity_name: &str,
    bytes: &[u8],
) -> HarvestResult<String> {
    use diesel_async::{AsyncConnection, RunQueryDsl};

    if bytes.len() > MAX_WASM_MODULE_BYTES {
        return Err(crate::error::HarvestError::Config(format!(
            "wasm module for activity '{activity_name}' is {} bytes, exceeding the maximum \
             of {MAX_WASM_MODULE_BYTES} bytes",
            bytes.len()
        )));
    }

    let hash = WasmModuleStore::compute_hash(bytes);
    let hash_for_txn = hash.clone();
    let name = activity_name.to_owned();

    Box::pin(
        conn.transaction::<(), crate::error::HarvestError, _>(async |conn| {
            use crate::schema::harvest_wasm_modules::dsl as m;

            // Serialise against a concurrent publish/seed for the SAME name so
            // the "is there an active version?" check and the conditional
            // activate are atomic. Same lock key as `publish_wasm_module`.
            diesel::sql_query("SELECT pg_advisory_xact_lock(hashtext($1)::bigint)")
                .bind::<diesel::sql_types::Text, _>(&name)
                .execute(conn)
                .await
                .map_err(database_error)?;

            // 1. Ensure the row exists (inactive if new, unchanged if already
            //    present) so its bytes are always fetchable by hash. DO NOTHING
            //    on conflict so an already-active row is never demoted.
            let new_row = crate::models::NewHarvestWasmModule {
                hash: &hash_for_txn,
                activity_name: &name,
                wasm_bytes: bytes,
                active: false,
            };
            diesel::insert_into(m::harvest_wasm_modules)
                .values(&new_row)
                .on_conflict((m::hash, m::activity_name))
                .do_nothing()
                .execute(conn)
                .await
                .map_err(database_error)?;

            // 2. Activate the seeded version ONLY when no active version exists
            //    for this name. Under the advisory lock the `NOT EXISTS` guard
            //    cannot race a concurrent activation, so the partial unique
            //    index (`WHERE active`) can never see two active rows. $2 is
            //    referenced twice; Postgres binds it once.
            diesel::sql_query(
                "UPDATE harvest_wasm_modules SET active = true, published_at = now() \
             WHERE hash = $1 AND activity_name = $2 \
             AND NOT EXISTS ( \
                 SELECT 1 FROM harvest_wasm_modules \
                 WHERE activity_name = $2 AND active \
             )",
            )
            .bind::<diesel::sql_types::Text, _>(&hash_for_txn)
            .bind::<diesel::sql_types::Text, _>(&name)
            .execute(conn)
            .await
            .map_err(database_error)?;

            Ok(())
        }),
    )
    .await?;

    Ok(hash)
}

/// Seed a batch of `(activity_name, bytes)` module registrations (issue #965).
///
/// Used at worker startup to make builder-registered WASM activities available
/// on the worker's shard without clobbering a version the shard was already
/// hot-swapped to. See [`seed_wasm_module`] for the activate-only-if-absent
/// semantics and why a blind publish at startup would be wrong.
///
/// # Errors
///
/// Returns the first seed failure (oversized blob or database error).
pub async fn seed_registered_wasm_modules(
    conn: &mut diesel_async::AsyncPgConnection,
    registrations: &[(String, Vec<u8>)],
) -> HarvestResult<()> {
    for (name, bytes) in registrations {
        seed_wasm_module(conn, name, bytes).await?;
    }
    Ok(())
}

/// Resolve the active module **hash** for `activity_name`, if any (issue #965).
///
/// Cheap: selects only the hash column, never the bytes. Returns `None` when no
/// active version is published.
///
/// # Errors
///
/// Returns `HarvestError::Database` on failure.
pub async fn resolve_active_wasm_hash(
    conn: &mut diesel_async::AsyncPgConnection,
    activity_name: &str,
) -> HarvestResult<Option<String>> {
    use diesel::OptionalExtension as _;
    use diesel_async::RunQueryDsl;

    use crate::schema::harvest_wasm_modules::dsl as m;
    m::harvest_wasm_modules
        .filter(m::activity_name.eq(activity_name))
        .filter(m::active.eq(true))
        .order(m::published_at.desc())
        .select(m::hash)
        .first::<String>(conn)
        .await
        .optional()
        .map_err(database_error)
}

/// Resolve the active module hash **and bytes** for `activity_name`, if any
/// (issue #965).
///
/// Prefer [`resolve_active_wasm_hash`] on the hot path (a cache hit needs no
/// bytes); this variant is for tests and callers that always need the bytes.
///
/// # Errors
///
/// Returns `HarvestError::Database` on failure.
pub async fn resolve_active_wasm_module(
    conn: &mut diesel_async::AsyncPgConnection,
    activity_name: &str,
) -> HarvestResult<Option<(String, Vec<u8>)>> {
    use diesel::OptionalExtension as _;
    use diesel_async::RunQueryDsl;

    use crate::schema::harvest_wasm_modules::dsl as m;
    m::harvest_wasm_modules
        .filter(m::activity_name.eq(activity_name))
        .filter(m::active.eq(true))
        .order(m::published_at.desc())
        .select((m::hash, m::wasm_bytes))
        .first::<(String, Vec<u8>)>(conn)
        .await
        .optional()
        .map_err(database_error)
}

/// Fetch a module's bytes by content `hash`, regardless of active state
/// (issue #965).
///
/// Any row for the hash suffices — content addressing guarantees identical
/// bytes across every `(hash, activity_name)` row sharing a hash.
///
/// # Errors
///
/// Returns `HarvestError::Database` on failure.
pub async fn fetch_wasm_module_bytes(
    conn: &mut diesel_async::AsyncPgConnection,
    hash: &str,
) -> HarvestResult<Option<Vec<u8>>> {
    use diesel::OptionalExtension as _;
    use diesel_async::RunQueryDsl;

    use crate::schema::harvest_wasm_modules::dsl as m;
    m::harvest_wasm_modules
        .filter(m::hash.eq(hash))
        .select(m::wasm_bytes)
        .first::<Vec<u8>>(conn)
        .await
        .optional()
        .map_err(database_error)
}

/// List every stored module version (metadata only, no bytes) newest-first
/// (issue #965).
///
/// # Errors
///
/// Returns `HarvestError::Database` on failure.
pub async fn list_wasm_modules(
    conn: &mut diesel_async::AsyncPgConnection,
) -> HarvestResult<Vec<WasmModuleRow>> {
    use diesel_async::RunQueryDsl;

    use crate::schema::harvest_wasm_modules::dsl as m;
    let rows: Vec<(String, String, bool, chrono::DateTime<chrono::Utc>)> = m::harvest_wasm_modules
        .select((m::hash, m::activity_name, m::active, m::published_at))
        .order(m::published_at.desc())
        .load(conn)
        .await
        .map_err(database_error)?;
    Ok(rows
        .into_iter()
        .map(
            |(hash, activity_name, active, published_at)| WasmModuleRow {
                hash,
                activity_name,
                active,
                published_at,
            },
        )
        .collect())
}

/// Resolve a WASM activity into a dispatchable form for the worker seam
/// (issue #965).
///
/// **Resolve-hash-first, compile-off-the-async-path**: cheaply resolve the
/// active hash, then serve a compiled module from the store's in-process cache
/// on a hit (no bytes fetch). On a miss, load the bytes but **defer** the
/// CPU-bound wasmtime compile to [`PreparedWasmActivity::invoke`] (which the
/// worker runs on the blocking pool), so the async dispatch thread never runs
/// compilation, which could otherwise stall unrelated polling/heartbeats/
/// cancellation (issue #965 review). So the async path only ever does the hash
/// resolve, the cache probe, and (on a miss) the byte fetch.
///
/// `cancel`, when set, is carried onto the prepared activity so a cancelled
/// guest is cooperatively interrupted rather than running to its wall-clock
/// ceiling.
///
/// Failure classification (via typed [`ActivityFailure`] → [`WasmDispatch::Fail`]):
/// - no active module → non-retryable `WasmModuleUnavailable`
/// - DB error resolving the hash / fetching bytes → retryable `WasmModuleLookupFailed`
///
/// An integrity or compile failure on a deferred miss surfaces later, from
/// `invoke`, as a non-retryable `WasmModuleInvalid`.
///
/// Never returns an `Err`: every resolution failure is folded into a typed
/// `Fail` payload the worker records as an ordinary `ActivityFailed`.
pub async fn resolve_wasm_dispatch(
    conn: &mut diesel_async::AsyncPgConnection,
    store: &Arc<WasmModuleStore>,
    binding: &WasmBinding,
    name: &str,
    deadline: Option<Duration>,
    cancel: Option<CancellationToken>,
    // The instant the activity's start-to-close clock began (issue #965 review
    // round 7). Threaded into the resolved `PreparedWasmActivity` so `invoke`
    // charges the whole resolution + fetch + compile interval against the
    // guest's deadline, not just compile.
    dispatch_start: Instant,
) -> WasmDispatch {
    let hash = match resolve_active_wasm_hash(conn, name).await {
        Ok(Some(hash)) => hash,
        Ok(None) => {
            return WasmDispatch::Fail(
                ActivityFailure::wasm_module_unavailable(format!(
                    "no active wasm module is published for activity '{name}'"
                ))
                .into_error_payload(),
            );
        }
        Err(e) => {
            return WasmDispatch::Fail(
                ActivityFailure::wasm_module_lookup_failed(format!(
                    "failed to resolve the active wasm module for activity '{name}': {e}"
                ))
                .into_error_payload(),
            );
        }
    };

    // Cache hit: serve the compiled module without touching the bytes. Cache
    // miss: fetch the bytes (async) but defer the compile to `invoke` on the
    // blocking pool.
    let source = if let Some(module) = store.cached(&hash) {
        WasmModuleSource::Compiled(module)
    } else {
        let bytes = match fetch_wasm_module_bytes(conn, &hash).await {
            Ok(Some(bytes)) => bytes,
            Ok(None) => {
                // The hash resolved active a moment ago but its row is gone —
                // a transient race (deactivated/deleted concurrently). Retry
                // re-resolves to a valid version or to Unavailable.
                return WasmDispatch::Fail(
                    ActivityFailure::wasm_module_lookup_failed(format!(
                        "wasm module {hash} for activity '{name}' vanished before its bytes \
                         could be fetched"
                    ))
                    .into_error_payload(),
                );
            }
            Err(e) => {
                return WasmDispatch::Fail(
                    ActivityFailure::wasm_module_lookup_failed(format!(
                        "failed to fetch bytes for wasm module {hash} (activity '{name}'): {e}"
                    ))
                    .into_error_payload(),
                );
            }
        };
        WasmModuleSource::Deferred { hash, bytes }
    };

    WasmDispatch::Invoke(PreparedWasmActivity {
        store: Arc::clone(store),
        source,
        caps: binding.capabilities.clone(),
        limits: binding.limits,
        deadline,
        cancel,
        dispatch_start: Some(dispatch_start),
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn registration_defaults_are_sane() {
        let reg = WasmActivityRegistration::new("checksum", vec![1, 2, 3]);
        assert_eq!(reg.name, "checksum");
        assert_eq!(reg.wasm_bytes, vec![1, 2, 3]);
        assert!(reg.queue.is_none());
        assert_eq!(reg.start_to_close, Some(Duration::from_secs(30)));
        assert_eq!(reg.capabilities, WasmCapabilities::default());
        assert_eq!(reg.limits, WasmLimits::default());
    }

    #[test]
    fn registration_carries_schedule_to_close_into_activity_info() {
        // Issue #965 review: `schedule_to_close` (#378) is the CROSS-RETRY wall
        // clock — the only bound that stops a deterministically-failing guest
        // (say, an infinite loop that burns its start-to-close every attempt)
        // from walking the whole retry curve. A native `#[activity]` can set it;
        // a WASM activity had no way to, and `ActivityInfo::wasm` hardcoded
        // `None`, so the bound was structurally unreachable.
        let reg = WasmActivityRegistration::new("wasm_act", b"\0asm".to_vec())
            .with_schedule_to_close(Some(Duration::from_secs(300)));
        assert_eq!(reg.schedule_to_close, Some(Duration::from_secs(300)));

        let info = crate::info::ActivityInfo::wasm(
            "wasm_act",
            None,
            None,
            reg.start_to_close,
            reg.schedule_to_close,
        );
        assert_eq!(
            info.default_schedule_to_close,
            Some(Duration::from_secs(300)),
            "the registration's schedule_to_close must reach ActivityInfo, otherwise the \
             enqueue path never sets schedule_to_close_at and the cross-retry deadline \
             silently does not exist"
        );
    }

    #[test]
    fn registration_schedule_to_close_defaults_to_none() {
        // Back-compat: unset means "no cross-retry deadline", exactly as before.
        let reg = WasmActivityRegistration::new("wasm_act", b"\0asm".to_vec());
        assert_eq!(reg.schedule_to_close, None);
    }

    #[test]
    fn registration_fluent_setters_apply() {
        let caps = WasmCapabilities {
            allow_clock: true,
            ..Default::default()
        };
        let limits = WasmLimits {
            fuel: 42,
            ..Default::default()
        };
        let reg = WasmActivityRegistration::new("x", vec![])
            .with_queue("gpu")
            .with_retry(RetryPolicy::fixed(5, Duration::from_millis(10)))
            .with_start_to_close(None)
            .with_capabilities(caps.clone())
            .with_limits(limits);
        assert_eq!(reg.queue.as_deref(), Some("gpu"));
        assert_eq!(reg.retry.max_attempts, 5);
        assert_eq!(reg.start_to_close, None);
        assert_eq!(reg.capabilities, caps);
        assert_eq!(reg.limits.fuel, 42);
    }

    #[test]
    fn binding_projects_capabilities_and_limits() {
        let caps = WasmCapabilities {
            allow_random: true,
            ..Default::default()
        };
        let reg = WasmActivityRegistration::new("x", vec![]).with_capabilities(caps.clone());
        let binding = reg.binding();
        assert_eq!(binding.capabilities, caps);
        assert_eq!(binding.limits, WasmLimits::default());
    }

    #[test]
    fn max_wasm_module_bytes_is_32_mib() {
        assert_eq!(MAX_WASM_MODULE_BYTES, 32 * 1024 * 1024);
    }

    #[test]
    fn effective_deadline_subtracts_total_pre_guest_elapsed() {
        // Total pre-guest overhead (resolution + fetch + compile) that ate part
        // of the budget tightens the remaining deadline (issue #965 round 7).
        assert_eq!(
            effective_invoke_deadline(Some(Duration::from_secs(10)), Duration::from_secs(3)),
            InvokeBudget::Remaining(Duration::from_secs(7))
        );
    }

    #[test]
    fn effective_deadline_charges_resolution_plus_compile_not_just_compile() {
        // The whole class this fix closes: a slow pool checkout + DB byte fetch
        // (say 4s) plus a compile (say 2s) — 6s of pre-guest overhead — must be
        // charged, so a 10s start-to-close leaves the guest 4s, not 8s.
        let resolution = Duration::from_secs(4);
        let compile = Duration::from_secs(2);
        let total_pre_guest = resolution + compile;
        assert_eq!(
            effective_invoke_deadline(Some(Duration::from_secs(10)), total_pre_guest),
            InvokeBudget::Remaining(Duration::from_secs(4)),
            "resolution+fetch time must be charged, not just compile"
        );
    }

    #[test]
    fn effective_deadline_none_is_unbounded() {
        // No configured deadline: the `max_wall_clock` ceiling still applies
        // inside the invoke, so the guest runs unbounded here.
        assert_eq!(
            effective_invoke_deadline(None, Duration::from_secs(3)),
            InvokeBudget::Unbounded
        );
    }

    #[test]
    fn effective_deadline_over_budget_is_exhausted() {
        // Pre-guest overhead that EXCEEDED the deadline must fail fast without
        // invoking the guest (issue #965 round 9) — not hand it a zero deadline
        // that clamps to one epoch tick a fast guest could win a race against.
        assert_eq!(
            effective_invoke_deadline(Some(Duration::from_secs(2)), Duration::from_secs(5)),
            InvokeBudget::Exhausted
        );
    }

    #[test]
    fn effective_deadline_exactly_at_budget_is_exhausted() {
        // Boundary: pre-guest overhead EXACTLY equal to the deadline leaves zero
        // remaining, which is still Exhausted (`pre_guest_elapsed >= d`).
        assert_eq!(
            effective_invoke_deadline(Some(Duration::from_secs(4)), Duration::from_secs(4)),
            InvokeBudget::Exhausted
        );
    }

    #[test]
    fn effective_deadline_cache_hit_tiny_elapsed_is_unchanged() {
        // A cache hit fetches/compiles nothing; the resolution overhead is
        // negligible. Modelled as Duration::ZERO, the deadline is returned
        // verbatim — effectively unchanged behaviour on the hot path.
        assert_eq!(
            effective_invoke_deadline(Some(Duration::from_secs(4)), Duration::ZERO),
            InvokeBudget::Remaining(Duration::from_secs(4))
        );
    }

    #[test]
    fn cancelled_deferred_invoke_skips_compile() {
        // An already-cancelled token on a cache-miss (Deferred) invoke must skip
        // the cold compile and return promptly (issue #965 review). The bytes are
        // deliberately invalid wasm: had the compile run, `get_or_compile` would
        // return `WasmModuleInvalid`; instead we get a cancelled
        // `ResourceExhausted`, proving the compile was never started.
        let store = Arc::new(WasmModuleStore::new());
        let invalid = vec![0u8, 1, 2, 3];
        let hash = WasmModuleStore::compute_hash(&invalid);
        let cancel = CancellationToken::new();
        cancel.cancel();
        let prepared = PreparedWasmActivity {
            store,
            source: WasmModuleSource::Deferred {
                hash,
                bytes: invalid,
            },
            caps: WasmCapabilities::default(),
            limits: WasmLimits::default(),
            deadline: None,
            cancel: Some(cancel),
            dispatch_start: Some(Instant::now()),
        };
        let err = prepared
            .invoke(&serde_json::Value::Null)
            .expect_err("cancelled invoke must fail");
        assert_eq!(
            err.error_type,
            crate::failure::ERROR_TYPE_RESOURCE_EXHAUSTED,
            "a skipped compile yields cancelled ResourceExhausted, not WasmModuleInvalid"
        );
    }

    #[test]
    fn over_budget_invoke_fails_fast_without_running_the_guest() {
        // An attempt whose pre-guest overhead already spent the whole
        // start-to-close budget must fail fast as ResourceExhausted BEFORE the
        // guest is invoked (issue #965 review round 9). The module is a valid,
        // trivially-fast ECHO guest: had it run under the old zero→1-tick
        // deadline it would have completed and returned Ok(input). Getting the
        // specific "before guest start" ResourceExhausted instead proves the
        // guest never ran and the deadline race can't be lost.
        const ECHO_WAT: &str = r#"
            (module
              (memory (export "memory") 1)
              (global $bump (mut i32) (i32.const 1024))
              (func (export "alloc") (param $len i32) (result i32)
                (local $ptr i32)
                (local.set $ptr (global.get $bump))
                (global.set $bump (i32.add (global.get $bump) (local.get $len)))
                (local.get $ptr))
              (func (export "run") (param $in_ptr i32) (param $in_len i32) (result i64)
                (i64.or
                  (i64.shl (i64.extend_i32_u (local.get $in_ptr)) (i64.const 32))
                  (i64.extend_i32_u (local.get $in_len)))))
        "#;
        let store = Arc::new(WasmModuleStore::new());
        let bytes = wat::parse_str(ECHO_WAT).expect("wat must assemble");
        let hash = WasmModuleStore::compute_hash(&bytes);
        let module = store
            .get_or_compile(&hash, &bytes)
            .expect("echo module must compile");
        let prepared = PreparedWasmActivity {
            store,
            source: WasmModuleSource::Compiled(module),
            caps: WasmCapabilities::default(),
            limits: WasmLimits::default(),
            deadline: Some(Duration::from_millis(50)),
            cancel: None,
            // Start-to-close clock began well before now — the whole 50ms budget
            // was already spent before the guest could start.
            dispatch_start: Some(Instant::now().checked_sub(Duration::from_secs(5)).unwrap()),
        };
        let input = serde_json::json!({ "sentinel": "must-not-echo" });
        let err = prepared
            .invoke(&input)
            .expect_err("an over-budget invoke must fail, not echo the input");
        assert_eq!(
            err.error_type,
            crate::failure::ERROR_TYPE_RESOURCE_EXHAUSTED,
            "an exhausted start-to-close budget is retryable ResourceExhausted"
        );
        assert!(
            err.message.contains("budget exhausted before guest start"),
            "expected the fail-fast message, got: {}",
            err.message
        );
    }

    #[test]
    fn in_budget_invoke_runs_the_guest() {
        // The complement of the fail-fast case: with pre-guest overhead well
        // under the deadline (round-7 behaviour preserved), the guest IS invoked
        // and the ECHO module returns Ok(input).
        const ECHO_WAT: &str = r#"
            (module
              (memory (export "memory") 1)
              (global $bump (mut i32) (i32.const 1024))
              (func (export "alloc") (param $len i32) (result i32)
                (local $ptr i32)
                (local.set $ptr (global.get $bump))
                (global.set $bump (i32.add (global.get $bump) (local.get $len)))
                (local.get $ptr))
              (func (export "run") (param $in_ptr i32) (param $in_len i32) (result i64)
                (i64.or
                  (i64.shl (i64.extend_i32_u (local.get $in_ptr)) (i64.const 32))
                  (i64.extend_i32_u (local.get $in_len)))))
        "#;
        let store = Arc::new(WasmModuleStore::new());
        let bytes = wat::parse_str(ECHO_WAT).expect("wat must assemble");
        let hash = WasmModuleStore::compute_hash(&bytes);
        let module = store
            .get_or_compile(&hash, &bytes)
            .expect("echo module must compile");
        let prepared = PreparedWasmActivity {
            store,
            source: WasmModuleSource::Compiled(module),
            caps: WasmCapabilities::default(),
            limits: WasmLimits::default(),
            deadline: Some(Duration::from_secs(10)),
            cancel: None,
            // Negligible pre-guest overhead — the guest keeps essentially the
            // full 10s budget and runs to completion.
            dispatch_start: Some(Instant::now()),
        };
        let input = serde_json::json!({ "echo": "me" });
        let out = prepared
            .invoke(&input)
            .expect("an in-budget invoke must run the guest and echo the input");
        assert_eq!(out, input, "the ECHO guest returns its input verbatim");
    }
}
