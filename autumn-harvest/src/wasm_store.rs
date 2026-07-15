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
use std::time::Duration;

use diesel::{ExpressionMethods, QueryDsl};
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
/// Bundles the activity's name, its module bytes, and every knob a native
/// `#[activity]` exposes (queue, retry policy, start-to-close) plus the sandbox
/// [`WasmCapabilities`]/[`WasmLimits`]. Construct with [`WasmActivityRegistration::new`]
/// and refine with the fluent `with_*` setters.
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
    /// Start-to-close timeout. `None` = unbounded; default `Some(30s)`.
    pub start_to_close: Option<Duration>,
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

/// A resolved, compiled WASM activity ready to invoke against a JSON input.
///
/// Holds shared handles (the engine store and the compiled module) plus the
/// caller's capability/limit/deadline policy, so [`invoke`](Self::invoke) is a
/// thin wrapper over [`crate::wasm_activities::invoke_wasm_activity`].
pub struct PreparedWasmActivity {
    /// The shared engine + compiled-module cache (one per worker).
    pub store: Arc<WasmModuleStore>,
    /// The compiled module for this activity's active version.
    pub module: Arc<Module>,
    /// Host capabilities granted to the guest.
    pub caps: WasmCapabilities,
    /// Per-invocation resource budget.
    pub limits: WasmLimits,
    /// Effective wall-clock deadline for the invocation (typically the
    /// activity's start-to-close), clamped by `limits.max_wall_clock`.
    pub deadline: Option<Duration>,
}

impl PreparedWasmActivity {
    /// Run the guest against `input`, returning its JSON output or a typed
    /// [`ActivityFailure`].
    ///
    /// This is CPU-bound and blocking (it drives the guest to completion on the
    /// calling thread), so the worker calls it inside `spawn_blocking`.
    ///
    /// # Errors
    ///
    /// Returns the typed [`ActivityFailure`] classifying any sandbox denial,
    /// resource exhaustion, guest trap, ABI violation, or contained host-glue
    /// panic — see [`crate::wasm_activities::invoke_wasm_activity`].
    pub fn invoke(&self, input: &serde_json::Value) -> Result<serde_json::Value, ActivityFailure> {
        crate::wasm_activities::invoke_wasm_activity(
            &self.store,
            &self.module,
            input,
            &self.caps,
            &self.limits,
            self.deadline,
        )
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
    use scoped_futures::ScopedFutureExt as _;

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

    conn.transaction::<(), crate::error::HarvestError, _>(|conn| {
        async move {
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
        }
        .scope_boxed()
    })
    .await?;

    Ok(hash)
}

/// Publish a batch of `(activity_name, bytes)` module registrations, idempotently
/// (issue #965). Used at worker startup to auto-publish builder-registered WASM
/// activities to the worker's shard database.
///
/// # Errors
///
/// Returns the first publish failure (oversized blob or database error).
pub async fn publish_registered_wasm_modules(
    conn: &mut diesel_async::AsyncPgConnection,
    registrations: &[(String, Vec<u8>)],
) -> HarvestResult<()> {
    for (name, bytes) in registrations {
        publish_wasm_module(conn, name, bytes).await?;
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
/// **Resolve-hash-first**: cheaply resolve the active hash, then serve a
/// compiled module from the store's in-process cache without ever fetching
/// bytes on a cache hit. Only a cache miss loads the bytes and compiles.
///
/// Failure classification (via typed [`ActivityFailure`] → [`WasmDispatch::Fail`]):
/// - no active module → non-retryable `WasmModuleUnavailable`
/// - DB error resolving the hash / fetching bytes → retryable `WasmModuleLookupFailed`
/// - integrity or compile failure → non-retryable `WasmModuleInvalid`
///
/// Never returns an `Err`: every failure is folded into a typed `Fail` payload
/// the worker records as an ordinary `ActivityFailed`.
pub async fn resolve_wasm_dispatch(
    conn: &mut diesel_async::AsyncPgConnection,
    store: &Arc<WasmModuleStore>,
    binding: &WasmBinding,
    name: &str,
    deadline: Option<Duration>,
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

    // Cache hit: serve the compiled module without touching the bytes.
    let module = if let Some(module) = store.cached(&hash) {
        module
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
        match store.get_or_compile(&hash, &bytes) {
            Ok(module) => module,
            Err(e) => {
                return WasmDispatch::Fail(
                    ActivityFailure::wasm_module_invalid(format!(
                        "wasm module {hash} for activity '{name}' is invalid: {e}"
                    ))
                    .into_error_payload(),
                );
            }
        }
    };

    WasmDispatch::Invoke(PreparedWasmActivity {
        store: Arc::clone(store),
        module,
        caps: binding.capabilities.clone(),
        limits: binding.limits,
        deadline,
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
}
