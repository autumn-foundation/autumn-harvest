#![cfg(feature = "wasm-activities")]
//! Postgres content-hash storage + worker dispatch seam for sandboxed WASM
//! activities — issue #965, milestone 2.
//!
//! Storage layer (this milestone's core): publish/resolve/fetch/list round-trips,
//! the hot-swap + single-active invariant, composite-PK independence, the
//! concurrent-publish race, the oversized-module reject, and startup-publish.
//!
//! Worker seam (AC1/AC2/AC4/AC5): a published WASM activity dispatched through the
//! real worker loop reaches COMPLETED with only ordinary
//! `ActivityScheduled`/`ActivityCompleted` events; a sandbox denial fails the
//! workflow terminally as an ordinary `ActivityFailed` with `error_type ==
//! SandboxDenied`; an in-flight attempt is pinned to the loaded module version
//! across a mid-flight republish; and a fuel-exhausting guest is retried.
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run
//! against it directly (single-threaded; each test scrubs first); otherwise a
//! fresh testcontainers Postgres is booted with the full migration bundle
//! (`autumn_harvest::full_migrations_sql()`).

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::failure::{
    ERROR_TYPE_RESOURCE_EXHAUSTED, ERROR_TYPE_SANDBOX_DENIED, ERROR_TYPE_WASM_MODULE_INVALID,
    ERROR_TYPE_WASM_MODULE_LOOKUP_FAILED, ERROR_TYPE_WASM_MODULE_UNAVAILABLE, ERROR_TYPE_WASM_TRAP,
};
use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::models::{NewWorkflowExecution, WorkflowExecution};
use autumn_harvest::policy::RetryPolicy;
use autumn_harvest::queue::{self, EnqueueParams, TaskType};
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::telemetry::{MetricsRecorder, TelemetryConfig};
use autumn_harvest::types::{ExecutionId, ShardId};
use autumn_harvest::wasm_activities::{
    WasmCapabilities, WasmLimits, WasmModuleStore, invoke_wasm_activity,
};
use autumn_harvest::wasm_store::{
    MAX_WASM_MODULE_BYTES, WasmActivityRegistration, WasmBinding, WasmDispatch,
    fetch_wasm_module_bytes, list_wasm_modules, publish_registered_wasm_modules,
    publish_wasm_module, resolve_active_wasm_hash, resolve_active_wasm_module, resolve_wasm_dispatch,
    seed_registered_wasm_modules, seed_wasm_module,
};
use autumn_harvest::HarvestBuilder;
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use autumn_harvest::{WorkflowContext, store};
use chrono::Utc;
use diesel::prelude::*;
use diesel::sql_types::BigInt;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

fn init_sql() -> Vec<u8> {
    autumn_harvest::full_migrations_sql().as_bytes().to_vec()
}

async fn setup_db() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let container = Postgres::default()
        .with_init_sql(init_sql())
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, Some(container))
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool build failed")
}

/// Scrub the module table so a shared migrated DB stays isolated per test.
async fn scrub(conn: &mut AsyncPgConnection) {
    diesel::sql_query("DELETE FROM harvest_wasm_modules")
        .execute(conn)
        .await
        .expect("scrub harvest_wasm_modules");
}

/// A correct bump-allocator echo guest (mirrors the M1 `ECHO_WAT`): `run`
/// returns `packed(in_ptr, in_len)`, so the host reads back the exact input.
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

/// A second, byte-distinct echo guest (extra scratch global) so its content
/// hash differs from `ECHO_WAT` while behaving identically.
const ECHO_WAT_V2: &str = r#"
    (module
      (memory (export "memory") 1)
      (global $bump (mut i32) (i32.const 1024))
      (global $unused (mut i32) (i32.const 7))
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

fn echo_bytes() -> Vec<u8> {
    wat::parse_str(ECHO_WAT).expect("echo wat assembles")
}

fn echo_bytes_v2() -> Vec<u8> {
    wat::parse_str(ECHO_WAT_V2).expect("echo v2 wat assembles")
}

async fn conn_and_pool() -> (DbPool, Option<ContainerAsync<Postgres>>) {
    let (url, container) = setup_db().await;
    (build_pool(&url), container)
}

// ── pure: error-type constants ──────────────────────────────────────────────

#[test]
fn wasm_error_type_constants_are_stable() {
    assert_eq!(ERROR_TYPE_SANDBOX_DENIED, "SandboxDenied");
    assert_eq!(ERROR_TYPE_RESOURCE_EXHAUSTED, "ResourceExhausted");
    assert_eq!(ERROR_TYPE_WASM_TRAP, "WasmTrap");
    assert_eq!(ERROR_TYPE_WASM_MODULE_UNAVAILABLE, "WasmModuleUnavailable");
    assert_eq!(ERROR_TYPE_WASM_MODULE_INVALID, "WasmModuleInvalid");
    assert_eq!(
        ERROR_TYPE_WASM_MODULE_LOOKUP_FAILED,
        "WasmModuleLookupFailed"
    );
}

// ── storage: publish/resolve/fetch round-trip ───────────────────────────────

#[tokio::test]
async fn publish_resolves_and_fetches_round_trip() {
    let (pool, _c) = conn_and_pool().await;
    let mut conn = pool.get().await.expect("conn");
    scrub(&mut conn).await;

    let bytes = echo_bytes();
    let expected = WasmModuleStore::compute_hash(&bytes);
    let hash = publish_wasm_module(&mut conn, "echo", &bytes)
        .await
        .expect("publish");
    assert_eq!(hash, expected);

    // hash-only resolve
    let active = resolve_active_wasm_hash(&mut conn, "echo")
        .await
        .expect("resolve hash");
    assert_eq!(active.as_deref(), Some(expected.as_str()));

    // hash+bytes resolve
    let (rhash, rbytes) = resolve_active_wasm_module(&mut conn, "echo")
        .await
        .expect("resolve module")
        .expect("some");
    assert_eq!(rhash, expected);
    assert_eq!(rbytes, bytes);

    // fetch by hash
    let fetched = fetch_wasm_module_bytes(&mut conn, &expected)
        .await
        .expect("fetch")
        .expect("bytes present");
    assert_eq!(fetched, bytes);

    // unknown activity resolves to None
    assert!(
        resolve_active_wasm_hash(&mut conn, "nope")
            .await
            .expect("resolve")
            .is_none()
    );
}

// ── storage: hot-swap flips the active version ──────────────────────────────

#[tokio::test]
async fn hot_swap_flips_active_and_keeps_old_version_fetchable() {
    let (pool, _c) = conn_and_pool().await;
    let mut conn = pool.get().await.expect("conn");
    scrub(&mut conn).await;

    let v1 = echo_bytes();
    let v2 = echo_bytes_v2();
    let h1 = WasmModuleStore::compute_hash(&v1);
    let h2 = WasmModuleStore::compute_hash(&v2);
    assert_ne!(h1, h2, "the two versions must have distinct hashes");

    publish_wasm_module(&mut conn, "echo", &v1)
        .await
        .expect("publish v1");
    publish_wasm_module(&mut conn, "echo", &v2)
        .await
        .expect("publish v2");

    // active flips to v2
    assert_eq!(
        resolve_active_wasm_hash(&mut conn, "echo")
            .await
            .expect("resolve")
            .as_deref(),
        Some(h2.as_str())
    );
    // v1 bytes are still fetchable by hash (deactivated, not deleted)
    assert_eq!(
        fetch_wasm_module_bytes(&mut conn, &h1)
            .await
            .expect("fetch v1")
            .as_deref(),
        Some(v1.as_slice())
    );
    // exactly one active row for the name
    assert_eq!(active_count(&mut conn, "echo").await, 1);
}

// ── storage: republish identical bytes is idempotent ────────────────────────

#[tokio::test]
async fn republish_identical_bytes_is_idempotent() {
    let (pool, _c) = conn_and_pool().await;
    let mut conn = pool.get().await.expect("conn");
    scrub(&mut conn).await;

    let bytes = echo_bytes();
    let h1 = publish_wasm_module(&mut conn, "echo", &bytes)
        .await
        .expect("publish 1");
    let h2 = publish_wasm_module(&mut conn, "echo", &bytes)
        .await
        .expect("publish 2");
    assert_eq!(h1, h2);
    assert_eq!(total_rows(&mut conn, "echo").await, 1);
    assert_eq!(active_count(&mut conn, "echo").await, 1);
}

// ── storage: composite PK — same bytes, two names, independent active ───────

#[tokio::test]
async fn identical_bytes_bound_to_two_names_resolve_independently() {
    let (pool, _c) = conn_and_pool().await;
    let mut conn = pool.get().await.expect("conn");
    scrub(&mut conn).await;

    let bytes = echo_bytes();
    let hash = WasmModuleStore::compute_hash(&bytes);
    publish_wasm_module(&mut conn, "alpha", &bytes)
        .await
        .expect("publish alpha");
    publish_wasm_module(&mut conn, "beta", &bytes)
        .await
        .expect("publish beta");

    // Both resolve their own active module (same hash, distinct rows).
    assert_eq!(
        resolve_active_wasm_hash(&mut conn, "alpha")
            .await
            .expect("alpha")
            .as_deref(),
        Some(hash.as_str())
    );
    assert_eq!(
        resolve_active_wasm_hash(&mut conn, "beta")
            .await
            .expect("beta")
            .as_deref(),
        Some(hash.as_str())
    );
    assert_eq!(active_count(&mut conn, "alpha").await, 1);
    assert_eq!(active_count(&mut conn, "beta").await, 1);
    // Two rows total (one per (hash, name) pair).
    assert_eq!(total_rows_all(&mut conn).await, 2);
}

// ── storage: concurrent different-hash publishes → exactly one active ───────

#[tokio::test]
async fn concurrent_publishes_leave_exactly_one_active_row() {
    let (pool, _c) = conn_and_pool().await;
    {
        let mut conn = pool.get().await.expect("conn");
        scrub(&mut conn).await;
    }

    let v1 = echo_bytes();
    let v2 = echo_bytes_v2();

    let mut c1 = pool.get().await.expect("conn1");
    let mut c2 = pool.get().await.expect("conn2");
    let (r1, r2) = tokio::join!(
        publish_wasm_module(&mut c1, "echo", &v1),
        publish_wasm_module(&mut c2, "echo", &v2),
    );
    r1.expect("publish v1");
    r2.expect("publish v2");

    let mut conn = pool.get().await.expect("conn");
    // The single-active invariant holds under concurrency: exactly one active
    // row (whichever publish committed second wins the active flag).
    assert_eq!(active_count(&mut conn, "echo").await, 1);
    assert_eq!(total_rows(&mut conn, "echo").await, 2);
}

// ── storage: oversized module rejected before insert ────────────────────────

#[tokio::test]
async fn oversized_module_is_rejected_before_insert() {
    let (pool, _c) = conn_and_pool().await;
    let mut conn = pool.get().await.expect("conn");
    scrub(&mut conn).await;

    let too_big = vec![0u8; MAX_WASM_MODULE_BYTES + 1];
    let err = publish_wasm_module(&mut conn, "echo", &too_big)
        .await
        .expect_err("oversized module must be rejected");
    assert!(
        matches!(err, autumn_harvest::error::HarvestError::Config(_)),
        "got {err:?}"
    );
    // Nothing was inserted.
    assert_eq!(total_rows_all(&mut conn).await, 0);
}

// ── storage: startup-publish helper resolves ────────────────────────────────

#[tokio::test]
async fn publish_registered_modules_are_resolvable() {
    let (pool, _c) = conn_and_pool().await;
    let mut conn = pool.get().await.expect("conn");
    scrub(&mut conn).await;

    let regs = vec![
        ("echo".to_string(), echo_bytes()),
        ("beta".to_string(), echo_bytes_v2()),
    ];
    publish_registered_wasm_modules(&mut conn, &regs)
        .await
        .expect("batch publish");
    // idempotent re-run
    publish_registered_wasm_modules(&mut conn, &regs)
        .await
        .expect("batch publish again");

    assert!(
        resolve_active_wasm_hash(&mut conn, "echo")
            .await
            .expect("echo")
            .is_some()
    );
    assert!(
        resolve_active_wasm_hash(&mut conn, "beta")
            .await
            .expect("beta")
            .is_some()
    );
    assert_eq!(active_count(&mut conn, "echo").await, 1);
    assert_eq!(active_count(&mut conn, "beta").await, 1);

    let listed = list_wasm_modules(&mut conn).await.expect("list");
    assert_eq!(listed.len(), 2);
    assert!(listed.iter().all(|r| r.active));
}

// ── resolve_wasm_dispatch: unavailable / invalid / invoke ───────────────────

#[tokio::test]
async fn resolve_dispatch_unavailable_when_nothing_published() {
    let (pool, _c) = conn_and_pool().await;
    let mut conn = pool.get().await.expect("conn");
    scrub(&mut conn).await;

    let store = Arc::new(WasmModuleStore::new());
    let binding = WasmBinding {
        capabilities: WasmCapabilities::default(),
        limits: WasmLimits::default(),
    };
    let dispatch = resolve_wasm_dispatch(&mut conn, &store, &binding, "echo", None, None).await;
    match dispatch {
        WasmDispatch::Fail(payload) => {
            assert!(
                payload.contains(ERROR_TYPE_WASM_MODULE_UNAVAILABLE),
                "expected unavailable, got: {payload}"
            );
        }
        WasmDispatch::Invoke(_) => panic!("must be unavailable"),
    }
}

#[tokio::test]
async fn resolve_dispatch_invokes_a_published_echo() {
    let (pool, _c) = conn_and_pool().await;
    let mut conn = pool.get().await.expect("conn");
    scrub(&mut conn).await;

    publish_wasm_module(&mut conn, "echo", &echo_bytes())
        .await
        .expect("publish");

    let store = Arc::new(WasmModuleStore::new());
    let binding = WasmBinding {
        capabilities: WasmCapabilities::default(),
        limits: WasmLimits::default(),
    };
    let dispatch = resolve_wasm_dispatch(
        &mut conn,
        &store,
        &binding,
        "echo",
        Some(Duration::from_secs(5)),
        None,
    )
    .await;
    let prepared = match dispatch {
        WasmDispatch::Invoke(prepared) => prepared,
        WasmDispatch::Fail(payload) => panic!("expected invoke, got: {payload}"),
    };
    let input = serde_json::json!({"hello": "world", "n": 42});
    let out = prepared.invoke(&input).expect("echo runs");
    assert_eq!(out, input);

    // Second resolution serves the cached compiled module (no bytes fetch).
    assert!(
        store
            .cached(&WasmModuleStore::compute_hash(&echo_bytes()))
            .is_some()
    );
}

#[tokio::test]
async fn resolve_dispatch_invalid_surfaces_at_invoke_not_resolve() {
    // Finding 6 (issue #965 review): compilation is deferred off the async
    // resolve path, so garbage bytes resolve to `Invoke` and the integrity/
    // compile failure surfaces (still non-retryable `WasmModuleInvalid`) only
    // from `invoke`, which the worker runs on the blocking pool.
    let (pool, _c) = conn_and_pool().await;
    let mut conn = pool.get().await.expect("conn");
    scrub(&mut conn).await;

    publish_wasm_module(&mut conn, "bad", b"not a wasm module")
        .await
        .expect("publish garbage");

    let store = Arc::new(WasmModuleStore::new());
    let binding = WasmBinding {
        capabilities: WasmCapabilities::default(),
        limits: WasmLimits::default(),
    };
    // Resolve does NOT compile — garbage still yields a dispatchable Invoke.
    let dispatch = resolve_wasm_dispatch(&mut conn, &store, &binding, "bad", None, None).await;
    let prepared = match dispatch {
        WasmDispatch::Invoke(prepared) => prepared,
        WasmDispatch::Fail(payload) => {
            panic!("resolve must defer the compile, got Fail: {payload}")
        }
    };
    // The invalid-bytes failure surfaces from the (blocking-pool) invoke.
    let failure = prepared
        .invoke(&serde_json::json!(null))
        .expect_err("garbage bytes must fail to compile at invoke");
    assert_eq!(failure.error_type, ERROR_TYPE_WASM_MODULE_INVALID);
    assert!(
        failure.non_retryable,
        "a compile/integrity failure is non-retryable"
    );
}

// ── Finding 6: cache-miss compilation is deferred off the async path ─────────

#[tokio::test]
async fn resolve_dispatch_defers_compile_to_invoke_on_cache_miss() {
    // The async resolve must NOT call `get_or_compile` on a miss: it returns an
    // Invoke carrying the raw bytes, and compilation happens inside `invoke`
    // (which the worker runs on the blocking pool). Observable via the store
    // cache: empty right after resolve, populated only after invoke.
    let (pool, _c) = conn_and_pool().await;
    let mut conn = pool.get().await.expect("conn");
    scrub(&mut conn).await;

    let bytes = echo_bytes();
    let hash = WasmModuleStore::compute_hash(&bytes);
    publish_wasm_module(&mut conn, "echo", &bytes)
        .await
        .expect("publish");

    let store = Arc::new(WasmModuleStore::new());
    let binding = WasmBinding {
        capabilities: WasmCapabilities::default(),
        limits: WasmLimits::default(),
    };
    let prepared =
        match resolve_wasm_dispatch(&mut conn, &store, &binding, "echo", None, None).await {
            WasmDispatch::Invoke(prepared) => prepared,
            WasmDispatch::Fail(payload) => panic!("expected invoke, got: {payload}"),
        };
    // A cache miss must not have compiled during the async resolve.
    assert!(
        store.cached(&hash).is_none(),
        "resolve must defer compilation off the async path (cache still empty)"
    );
    // The (blocking-pool) invoke compiles + caches, and runs correctly.
    let input = serde_json::json!({"deferred": true});
    let out = prepared
        .invoke(&input)
        .expect("deferred-compile invoke runs");
    assert_eq!(out, input);
    assert!(
        store.cached(&hash).is_some(),
        "invoke compiled and cached the module"
    );
}

// ── Finding 5: startup seed activates only when no active version exists ──────

#[tokio::test]
async fn seed_activates_only_when_no_active_version_exists() {
    // (a) Empty table: seeding activates the seeded module.
    let (pool, _c) = conn_and_pool().await;
    let mut conn = pool.get().await.expect("conn");
    scrub(&mut conn).await;

    let v1 = echo_bytes();
    let h1 = WasmModuleStore::compute_hash(&v1);
    seed_wasm_module(&mut conn, "echo", &v1)
        .await
        .expect("seed v1 into empty table");
    assert_eq!(
        resolve_active_wasm_hash(&mut conn, "echo")
            .await
            .expect("resolve")
            .as_deref(),
        Some(h1.as_str()),
        "seeding into an empty table activates the seeded version"
    );
    assert_eq!(active_count(&mut conn, "echo").await, 1);
}

#[tokio::test]
async fn seed_does_not_clobber_an_existing_active_version() {
    // (b) The rolling-deploy hazard: the DB is already on v2 (active); a
    // restarted OLDER worker that embeds v1 seeds it. v2 must STAY active, and
    // v1 must be present + fetchable-by-hash (so in-flight pinned v1 attempts
    // and this worker can still run v1) but NOT activated — no flip-back.
    let (pool, _c) = conn_and_pool().await;
    let mut conn = pool.get().await.expect("conn");
    scrub(&mut conn).await;

    let v1 = echo_bytes();
    let v2 = echo_bytes_v2();
    let h1 = WasmModuleStore::compute_hash(&v1);
    let h2 = WasmModuleStore::compute_hash(&v2);

    // DB is hot-swapped to v2 (an operator publish).
    publish_wasm_module(&mut conn, "echo", &v2)
        .await
        .expect("publish v2");
    // Old worker restarts and seeds its embedded v1.
    seed_wasm_module(&mut conn, "echo", &v1)
        .await
        .expect("seed v1");

    // v2 stays the active version — the shard is NOT flipped back to v1.
    assert_eq!(
        resolve_active_wasm_hash(&mut conn, "echo")
            .await
            .expect("resolve")
            .as_deref(),
        Some(h2.as_str()),
        "seeding must not clobber the already-active v2"
    );
    assert_eq!(active_count(&mut conn, "echo").await, 1);
    // v1 row is present + fetchable by hash, just inactive.
    assert_eq!(
        fetch_wasm_module_bytes(&mut conn, &h1)
            .await
            .expect("fetch v1")
            .as_deref(),
        Some(v1.as_slice()),
        "the seeded v1 bytes are still fetchable by hash"
    );
    assert_eq!(total_rows(&mut conn, "echo").await, 2);
}

#[tokio::test]
async fn duplicate_registration_seeds_the_later_bytes() {
    // Finding 12 (issue #965 review): registering the same activity name twice
    // with DIFFERENT bytes must keep only the LATER blob in the builder's
    // registration vector, so seeding a clean shard activates the module that
    // matches the last binding/retry/queue metadata — not the stale first blob.
    let v1 = echo_bytes();
    let v2 = echo_bytes_v2();
    assert_ne!(v1, v2, "the two versions must differ for this test");
    let h2 = WasmModuleStore::compute_hash(&v2);

    let built = HarvestBuilder::new()
        .wasm_activity(WasmActivityRegistration::new("echo", v1).with_queue("first"))
        .wasm_activity(WasmActivityRegistration::new("echo", v2).with_queue("second"))
        .build();

    // Exactly one registration entry survives, carrying the later bytes.
    let regs: Vec<&(String, Vec<u8>)> = built
        .wasm_module_registrations()
        .iter()
        .filter(|(n, _)| n == "echo")
        .collect();
    assert_eq!(regs.len(), 1, "duplicate name must not keep both blobs");
    assert_eq!(regs[0].1, echo_bytes_v2(), "must retain the later bytes");

    // Seeding those registrations into a clean shard activates the later module.
    let (pool, _c) = conn_and_pool().await;
    let mut conn = pool.get().await.expect("conn");
    scrub(&mut conn).await;

    seed_registered_wasm_modules(&mut conn, built.wasm_module_registrations())
        .await
        .expect("seed registered modules");

    assert_eq!(
        resolve_active_wasm_hash(&mut conn, "echo")
            .await
            .expect("resolve")
            .as_deref(),
        Some(h2.as_str()),
        "the ACTIVE module must be the later (second) registration's bytes"
    );
    assert_eq!(active_count(&mut conn, "echo").await, 1);
    assert_eq!(
        total_rows(&mut conn, "echo").await,
        1,
        "only one row (the later bytes) is seeded, not the stale first blob"
    );
}

// ── small query helpers ─────────────────────────────────────────────────────

#[derive(diesel::QueryableByName)]
struct CountRow {
    #[diesel(sql_type = BigInt)]
    n: i64,
}

async fn active_count(conn: &mut AsyncPgConnection, name: &str) -> i64 {
    diesel::sql_query(
        "SELECT COUNT(*) AS n FROM harvest_wasm_modules WHERE activity_name = $1 AND active",
    )
    .bind::<diesel::sql_types::Text, _>(name)
    .get_result::<CountRow>(conn)
    .await
    .expect("count active")
    .n
}

async fn total_rows(conn: &mut AsyncPgConnection, name: &str) -> i64 {
    diesel::sql_query("SELECT COUNT(*) AS n FROM harvest_wasm_modules WHERE activity_name = $1")
        .bind::<diesel::sql_types::Text, _>(name)
        .get_result::<CountRow>(conn)
        .await
        .expect("count rows")
        .n
}

async fn total_rows_all(conn: &mut AsyncPgConnection) -> i64 {
    diesel::sql_query("SELECT COUNT(*) AS n FROM harvest_wasm_modules")
        .get_result::<CountRow>(conn)
        .await
        .expect("count all")
        .n
}

// ═══════════════════════════════════════════════════════════════════════════
// Worker dispatch seam (AC1/AC2/AC4/AC5): a published WASM activity dispatched
// through the real worker poll loop. Harness modelled on
// activity_interceptor_tests.rs. Each test uses a unique task queue so a shared
// HARVEST_TEST_DATABASE_URL run cannot cross-claim tasks.
// ═══════════════════════════════════════════════════════════════════════════

async fn connect(url: &str) -> AsyncPgConnection {
    <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("connect")
}

/// Guest returning a fixed JSON literal `{"version":1}` (13 bytes), so callers
/// can tell which module version actually ran (AC4 pinning).
const VERSION1_WAT: &str = r#"
    (module
      (memory (export "memory") 1)
      (data (i32.const 100) "{\"version\":1}")
      (func (export "alloc") (param i32) (result i32) (i32.const 4096))
      (func (export "run") (param i32 i32) (result i64)
        (i64.or (i64.shl (i64.const 100) (i64.const 32)) (i64.const 13))))
"#;

const VERSION2_WAT: &str = r#"
    (module
      (memory (export "memory") 1)
      (data (i32.const 100) "{\"version\":2}")
      (func (export "alloc") (param i32) (result i32) (i32.const 4096))
      (func (export "run") (param i32 i32) (result i64)
        (i64.or (i64.shl (i64.const 100) (i64.const 32)) (i64.const 13))))
"#;

/// Guest that imports an ungranted host function (`fs_read`) → denied at
/// instantiation → non-retryable `SandboxDenied`.
const DENY_WAT: &str = r#"
    (module
      (import "env" "fs_read" (func $imported (param i32) (result i32)))
      (memory (export "memory") 1)
      (func (export "alloc") (param i32) (result i32) (i32.const 1024))
      (func (export "run") (param i32 i32) (result i64) (i64.const 0)))
"#;

/// Guest that spins forever → exhausts CPU fuel → retryable `ResourceExhausted`.
const FUEL_WAT: &str = r#"
    (module
      (memory (export "memory") 1)
      (func (export "alloc") (param i32) (result i32) (i32.const 1024))
      (func (export "run") (param i32 i32) (result i64)
        (loop $l (br $l))
        (i64.const 0)))
"#;

fn assemble(wat: &str) -> Vec<u8> {
    wat::parse_str(wat).expect("wat assembles")
}

/// Capturing metrics recorder for the retry assertion (AC5).
#[derive(Default)]
struct RecordingMetrics {
    retried: Mutex<Vec<String>>,
    failed: Mutex<Vec<String>>,
}
impl MetricsRecorder for RecordingMetrics {
    fn record_activity_retried(&self, activity_name: &str, _queue: &str) {
        self.retried.lock().unwrap().push(activity_name.to_string());
    }
    fn record_activity_failed(
        &self,
        activity_name: &str,
        _workflow_type: &str,
        _error_type: &str,
        _non_retryable: bool,
    ) {
        self.failed.lock().unwrap().push(activity_name.to_string());
    }
}

/// One WASM activity to register on a worker registry.
struct WasmActivitySpec {
    name: &'static str,
    bytes: Vec<u8>,
    caps: WasmCapabilities,
    limits: WasmLimits,
    retry: Option<RetryPolicy>,
}

/// Build a `HandlerRegistry` wired with WASM activities (bindings, shared store,
/// and startup-publish registrations), so `worker.run()` publishes the modules
/// to the DB before polling.
fn build_wasm_registry(
    workflows: Vec<WorkflowInfo>,
    wasm: Vec<WasmActivitySpec>,
    metrics: Arc<dyn MetricsRecorder>,
) -> Arc<HandlerRegistry> {
    let mut activities = Vec::new();
    let mut bindings = HashMap::new();
    let mut registrations = Vec::new();
    for spec in wasm {
        activities.push(ActivityInfo::wasm(
            spec.name,
            None,
            spec.retry.clone(),
            Some(Duration::from_secs(5)),
        ));
        bindings.insert(
            spec.name.to_string(),
            WasmBinding {
                capabilities: spec.caps,
                limits: spec.limits,
            },
        );
        registrations.push((spec.name.to_string(), spec.bytes));
    }
    let telemetry = Arc::new(TelemetryConfig::builder().metrics(metrics).build());
    let store = Arc::new(WasmModuleStore::new());
    Arc::new(
        HandlerRegistry::with_state_and_telemetry(
            workflows,
            activities,
            autumn_harvest::context::empty_shared_state(),
            telemetry,
        )
        .with_wasm_activities(store, bindings, registrations),
    )
}

fn build_worker(worker_id: &str, queue: &str, registry: Arc<HandlerRegistry>) -> Arc<Worker> {
    Arc::new(
        Worker::new(
            WorkerRuntimeConfig {
                worker_id: worker_id.to_string(),
                queues: vec![queue.to_string()],
                notification_database_url: None,
                max_concurrent_workflows: 1,
                max_concurrent_activities: 1,
                poll_interval: Duration::from_millis(25),
                shutdown_timeout: Duration::from_secs(1),
                cancellation_grace_period: Duration::from_secs(1),
                sticky_timeout: Duration::from_secs(5),
                max_local_activity_start_to_close: Duration::from_secs(60),
                shard_assignments: vec![ShardId::new(0)],
                worker_heartbeat_interval: Duration::from_secs(5),
                build_id: String::new(),
                deployment_name: None,
                workflow_cache_size: 1000,
                priority_aging_secs: None,
                unknown_target_grace_window: Duration::from_secs(5),
                poison_pill_threshold: 3,
                workflow_task_timeout: Duration::from_secs(30),
                workflow_panic_max_attempts: 3,
                labels: HashMap::new(),
                queue_weights: HashMap::new(),
                max_workflow_pause_duration: Duration::from_secs(24 * 3600),
                max_workflow_history_events: None,
                shard_notification_database_urls: Vec::new(),
                sharded_pool: None,
                slot_tuner: None,
                max_concurrent_sessions: 0,
            },
            registry,
        )
        .expect("worker builds"),
    )
}

async fn seed_workflow(
    conn: &mut AsyncPgConnection,
    workflow_name: &'static str,
    input: serde_json::Value,
    queue: &str,
) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let row = NewWorkflowExecution {
        id: exec_id.as_uuid(),
        workflow_name,
        workflow_id: &format!("wf-{}", exec_id.as_uuid()),
        run_id: Uuid::new_v4(),
        shard_id: 0,
        input: input.clone(),
        parent_id: None,
        queue_name: queue,
        execution_timeout: None,
        deadline_at: None,
        memo: None,
        search_attrs: None,
        assigned_build_id: None,
        parent_close_policy: None,
        owner: None,
        runbook_url: None,
        severity: None,
        context_headers: None,
        sla: None,
        sla_deadline_at: None,
        schedule_id: None,
        scheduled_for: None,
        workflow_attempt: 1,
        workflow_retry_policy: None,
        retry_of_exec_id: None,
        origin: None,
        completion_callbacks: None,
        continued_from_exec_id: None,
        first_exec_id: None,
    };
    diesel::insert_into(harvest_workflow_executions::table)
        .values(&row)
        .execute(conn)
        .await
        .expect("insert execution");
    store::append_events(
        conn,
        exec_id,
        &[WorkflowEvent::workflow_started(input.clone(), Utc::now())],
        0,
    )
    .await
    .expect("append WorkflowStarted");
    let mut params = EnqueueParams::new(queue, TaskType::Workflow, input);
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(5);
    queue::enqueue(conn, &params)
        .await
        .expect("enqueue workflow");
    exec_id
}

async fn load_execution(url: &str, exec_id: ExecutionId) -> WorkflowExecution {
    let mut conn = connect(url).await;
    harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .select(WorkflowExecution::as_select())
        .first(&mut conn)
        .await
        .expect("reload execution")
}

async fn load_history(url: &str, exec_id: ExecutionId) -> Vec<WorkflowEvent> {
    let mut conn = connect(url).await;
    store::load_history(&mut conn, exec_id)
        .await
        .expect("load_history")
        .events
}

async fn wait_for_state(
    url: &str,
    exec_id: ExecutionId,
    want: &str,
    timeout: Duration,
) -> WorkflowExecution {
    tokio::time::timeout(timeout, async {
        loop {
            let e = load_execution(url, exec_id).await;
            if e.state == want {
                break e;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("execution did not reach {want} within {timeout:?}"))
}

async fn run_to_state(
    url: &str,
    pool: &DbPool,
    worker: Arc<Worker>,
    exec_id: ExecutionId,
    want: &str,
    timeout: Duration,
) -> WorkflowExecution {
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move { runner.run(&pool_for_run).await });
    let execution = wait_for_state(url, exec_id, want, timeout).await;
    worker.shutdown();
    handle.await.expect("worker joins cleanly");
    execution
}

fn wf_info(name: &'static str, handler: autumn_harvest::info::WorkflowHandlerFn) -> WorkflowInfo {
    WorkflowInfo {
        mcp: false,
        name,
        module: "wasm_activities_tests",
        handler,
        execution_timeout: None,
        sla: None,
        concurrency: None,
        debounce: None,
        batch: None,
        throttle: None,
        max_input_bytes: None,
        owner: None,
        runbook_url: None,
        severity: None,
        description: None,
        input_schema: None,
        output_schema: None,
        error_schema: None,
        retry_policy: None,
    }
}

type BoxFut<'a> = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>,
>;

/// Workflow that dispatches the single WASM activity `echo_wasm` and returns it.
fn wf_run_wasm(ctx: &WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        let queue = ctx.queue_name().to_string();
        ctx.execute_activity_raw("echo_wasm", input, &queue)
            .await
            .map_err(|e| e.to_string())
    })
}

fn find_activity_completed(history: &[WorkflowEvent]) -> Option<serde_json::Value> {
    history.iter().find_map(|e| match e {
        WorkflowEvent::ActivityCompleted { output, .. } => Some(output.clone()),
        _ => None,
    })
}

fn find_activity_failed(history: &[WorkflowEvent]) -> Option<(String, String, bool)> {
    history.iter().find_map(|e| match e {
        WorkflowEvent::ActivityFailed {
            error,
            error_type,
            non_retryable,
            ..
        } => Some((error.clone(), error_type.clone(), *non_retryable)),
        _ => None,
    })
}

// ── AC1/AC6: worker-e2e echo → COMPLETED with only ordinary events ──────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_runs_wasm_echo_to_completion_with_ordinary_events() {
    let (url, _c) = setup_db().await;
    let queue = "q-wasm-echo";
    let mut conn = connect(&url).await;
    // Isolate the module table: startup-SEED (activate-only-if-absent) will not
    // swap in this test's bytes if a prior test's `echo_wasm` is still active on
    // a shared HARVEST_TEST_DATABASE_URL run (a no-op on CI's per-test DB).
    scrub(&mut conn).await;
    let input = serde_json::json!({"hello": "wasm", "n": 7});
    let exec_id = seed_workflow(&mut conn, "wf_run_wasm", input.clone(), queue).await;

    // Register a WASM echo activity (deny-all caps). Startup-publish seeds it.
    let echo = assemble(ECHO_WAT);
    let registry = build_wasm_registry(
        vec![wf_info("wf_run_wasm", wf_run_wasm)],
        vec![WasmActivitySpec {
            name: "echo_wasm",
            bytes: echo,
            caps: WasmCapabilities::default(),
            limits: WasmLimits::default(),
            retry: None,
        }],
        Arc::new(RecordingMetrics::default()),
    );
    let worker = build_worker("w-wasm-echo", queue, Arc::clone(&registry));
    let pool = build_pool(&url);
    run_to_state(
        &url,
        &pool,
        worker,
        exec_id,
        "COMPLETED",
        Duration::from_secs(30),
    )
    .await;

    let history = load_history(&url, exec_id).await;
    // Echoed output matches the input.
    assert_eq!(find_activity_completed(&history), Some(input.clone()));
    // History carries ordinary ActivityScheduled + ActivityCompleted; NO
    // wasm-specific event type exists.
    let types: Vec<&str> = history.iter().map(WorkflowEvent::type_name).collect();
    assert!(types.contains(&"ActivityScheduled"), "types: {types:?}");
    assert!(types.contains(&"ActivityCompleted"), "types: {types:?}");
    assert!(
        !types.iter().any(|t| t.to_lowercase().contains("wasm")),
        "no wasm-specific event variant may appear: {types:?}"
    );
}

// ── AC2: sandbox denial in the worker → SandboxDenied terminal FAILED ───────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_wasm_sandbox_denial_fails_workflow_terminally() {
    let (url, _c) = setup_db().await;
    let queue = "q-wasm-deny";
    let mut conn = connect(&url).await;
    // Isolate the module table under startup-SEED semantics (see the echo test).
    scrub(&mut conn).await;
    let exec_id = seed_workflow(&mut conn, "wf_run_wasm", serde_json::json!(null), queue).await;

    let deny = assemble(DENY_WAT);
    let metrics = Arc::new(RecordingMetrics::default());
    let registry = build_wasm_registry(
        vec![wf_info("wf_run_wasm", wf_run_wasm)],
        vec![WasmActivitySpec {
            name: "echo_wasm",
            bytes: deny,
            caps: WasmCapabilities::default(), // deny-all: fs_read is never granted
            limits: WasmLimits::default(),
            retry: Some(RetryPolicy::fixed(3, Duration::from_millis(50))),
        }],
        metrics.clone(),
    );
    let worker = build_worker("w-wasm-deny", queue, Arc::clone(&registry));
    let pool = build_pool(&url);
    // Non-retryable → terminal FAILED without exhausting the retry curve.
    run_to_state(
        &url,
        &pool,
        worker,
        exec_id,
        "FAILED",
        Duration::from_secs(30),
    )
    .await;

    let history = load_history(&url, exec_id).await;
    let (error, error_type, non_retryable) =
        find_activity_failed(&history).expect("an ActivityFailed event must exist");
    assert_eq!(error_type, ERROR_TYPE_SANDBOX_DENIED, "error was: {error}");
    assert!(non_retryable, "sandbox denial is non-retryable");
    // Non-retryable → not retried.
    assert!(
        metrics.retried.lock().unwrap().is_empty(),
        "a non-retryable sandbox denial must not be retried"
    );
}

// ── AC4: an in-flight attempt is pinned to the loaded module version ────────
//
// Deterministic (no wall-clock race): the worker resolves+compiles a module
// ONCE per attempt at dispatch and holds the resulting `Arc<Module>` for the
// attempt's whole lifetime. This test proves that invariant at the dispatch
// primitive: a `PreparedWasmActivity` resolved for v1 keeps running v1 even
// after v2 is published as the new active version; a fresh resolution sees v2.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_flight_dispatch_is_pinned_across_a_mid_flight_republish() {
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.expect("conn");
    scrub(&mut conn).await;

    publish_wasm_module(&mut conn, "pin", &assemble(VERSION1_WAT))
        .await
        .expect("publish v1");

    let store = Arc::new(WasmModuleStore::new());
    let binding = WasmBinding {
        capabilities: WasmCapabilities::default(),
        limits: WasmLimits::default(),
    };
    // Resolve while v1 is active → prepared is pinned to v1's compiled module.
    let prepared = match resolve_wasm_dispatch(&mut conn, &store, &binding, "pin", None, None).await
    {
        WasmDispatch::Invoke(prepared) => prepared,
        WasmDispatch::Fail(p) => panic!("expected invoke: {p}"),
    };

    // A concurrent operator publishes v2 as the new active version.
    publish_wasm_module(&mut conn, "pin", &assemble(VERSION2_WAT))
        .await
        .expect("publish v2");

    // The already-resolved (in-flight) dispatch still runs v1.
    let out = prepared.invoke(&serde_json::json!(null)).expect("invoke");
    assert_eq!(
        out,
        serde_json::json!({"version": 1}),
        "in-flight is pinned to v1"
    );

    // A FRESH resolution now sees v2.
    let fresh = match resolve_wasm_dispatch(&mut conn, &store, &binding, "pin", None, None).await {
        WasmDispatch::Invoke(prepared) => prepared,
        WasmDispatch::Fail(p) => panic!("expected invoke: {p}"),
    };
    let out2 = fresh.invoke(&serde_json::json!(null)).expect("invoke v2");
    assert_eq!(
        out2,
        serde_json::json!({"version": 2}),
        "a fresh dispatch sees v2"
    );
}

// ── AC5: a retryable resource exhaustion is retried, then terminal ──────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_wasm_fuel_exhaustion_is_retried_then_terminal() {
    let (url, _c) = setup_db().await;
    let queue = "q-wasm-fuel";
    let mut conn = connect(&url).await;
    // Isolate the module table under startup-SEED semantics (see the echo test).
    scrub(&mut conn).await;
    let exec_id = seed_workflow(&mut conn, "wf_run_wasm", serde_json::json!(null), queue).await;

    // Small fuel budget → the spin loop always exhausts it → ResourceExhausted
    // (retryable). Retry policy allows one retry (max_attempts = 2).
    let limits = WasmLimits {
        fuel: 10_000,
        ..WasmLimits::default()
    };
    let metrics = Arc::new(RecordingMetrics::default());
    let registry = build_wasm_registry(
        vec![wf_info("wf_run_wasm", wf_run_wasm)],
        vec![WasmActivitySpec {
            name: "echo_wasm",
            bytes: assemble(FUEL_WAT),
            caps: WasmCapabilities::default(),
            limits,
            retry: Some(RetryPolicy::fixed(2, Duration::from_millis(50))),
        }],
        metrics.clone(),
    );
    let worker = build_worker("w-wasm-fuel", queue, Arc::clone(&registry));
    let pool = build_pool(&url);
    // Retries exhaust → the workflow fails terminally.
    run_to_state(
        &url,
        &pool,
        worker,
        exec_id,
        "FAILED",
        Duration::from_secs(30),
    )
    .await;

    let history = load_history(&url, exec_id).await;
    let (error, error_type, non_retryable) =
        find_activity_failed(&history).expect("an ActivityFailed event must exist");
    assert_eq!(
        error_type, ERROR_TYPE_RESOURCE_EXHAUSTED,
        "error was: {error}"
    );
    assert!(!non_retryable, "resource exhaustion is retryable");
    // The activity was retried at least once before exhausting the budget.
    assert!(
        !metrics.retried.lock().unwrap().is_empty(),
        "a retryable resource exhaustion must be retried"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// SUCCESS METRIC (AC1 headline): a single workflow runs 1 NATIVE + 1 WASM
// activity to completion, through the standard dispatch path, and the recorded
// history is indistinguishable from an all-native run (AC6: ordinary
// ActivityScheduled/ActivityCompleted events only, no wasm-specific variant).
// ═══════════════════════════════════════════════════════════════════════════

type ActFut<'a> = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>,
>;

/// Native `#[activity]`-shaped handler: doubles the input's `"n"` field. Proves
/// a plain Rust activity and a WASM activity coexist on the same worker/queue.
fn native_double(
    _ctx: &autumn_harvest::context::ActivityContext,
    input: serde_json::Value,
) -> ActFut<'_> {
    Box::pin(async move {
        let n = input
            .get("n")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0);
        Ok(serde_json::json!({ "doubled": n * 2 }))
    })
}

/// Workflow: run the native activity `native_double` AND the WASM activity
/// `echo_wasm`, then return both outputs combined.
fn wf_native_and_wasm(ctx: &WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        let queue = ctx.queue_name().to_string();
        let native = ctx
            .execute_activity_raw("native_double", input.clone(), &queue)
            .await
            .map_err(|e| e.to_string())?;
        let wasm = ctx
            .execute_activity_raw("echo_wasm", input, &queue)
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({ "native": native, "wasm": wasm }))
    })
}

/// A native `ActivityInfo` literal (mirrors the `act_info` helper in
/// `activity_interceptor_tests.rs`) so a real Rust handler is registered.
fn native_act_info(
    name: &'static str,
    handler: autumn_harvest::info::ActivityHandlerFn,
) -> ActivityInfo {
    ActivityInfo {
        name,
        module: "wasm_activities_tests",
        default_retry_policy: None,
        default_start_to_close: Some(Duration::from_secs(5)),
        default_heartbeat_timeout: None,
        default_schedule_to_start: None,
        default_schedule_to_close: None,
        default_queue: None,
        max_concurrent: None,
        concurrency_key: None,
        rate_limit_rps: None,
        rate_limit_burst: None,
        rate_limit_key: None,
        circuit_breaker: None,
        is_local: false,
        max_input_bytes: None,
        max_result_bytes: None,
        requires: None,
        handler,
    }
}

/// Build a registry wiring BOTH native `ActivityInfo`s (with real handlers) and
/// WASM activities (bindings + shared store + startup-publish registrations),
/// so `worker.run()` publishes the WASM modules before polling and dispatches
/// native and WASM activities side by side.
fn build_mixed_registry(
    workflows: Vec<WorkflowInfo>,
    native: Vec<ActivityInfo>,
    wasm: Vec<WasmActivitySpec>,
    metrics: Arc<dyn MetricsRecorder>,
) -> Arc<HandlerRegistry> {
    let mut activities = native;
    let mut bindings = HashMap::new();
    let mut registrations = Vec::new();
    for spec in wasm {
        activities.push(ActivityInfo::wasm(
            spec.name,
            None,
            spec.retry.clone(),
            Some(Duration::from_secs(5)),
        ));
        bindings.insert(
            spec.name.to_string(),
            WasmBinding {
                capabilities: spec.caps,
                limits: spec.limits,
            },
        );
        registrations.push((spec.name.to_string(), spec.bytes));
    }
    let telemetry = Arc::new(TelemetryConfig::builder().metrics(metrics).build());
    let store = Arc::new(WasmModuleStore::new());
    Arc::new(
        HandlerRegistry::with_state_and_telemetry(
            workflows,
            activities,
            autumn_harvest::context::empty_shared_state(),
            telemetry,
        )
        .with_wasm_activities(store, bindings, registrations),
    )
}

/// Correlate `ActivityScheduled { name }` → its `ActivityCompleted { output }`
/// via the shared `activity_id`, so a caller can read a named activity's output.
fn completed_output_for(
    history: &[WorkflowEvent],
    activity_name: &str,
) -> Option<serde_json::Value> {
    let id = history.iter().find_map(|e| match e {
        WorkflowEvent::ActivityScheduled {
            activity_id, name, ..
        } if name == activity_name => Some(*activity_id),
        _ => None,
    })?;
    history.iter().find_map(|e| match e {
        WorkflowEvent::ActivityCompleted {
            activity_id,
            output,
        } if *activity_id == id => Some(output.clone()),
        _ => None,
    })
}

fn workflow_completed_output(history: &[WorkflowEvent]) -> Option<serde_json::Value> {
    history.iter().find_map(|e| match e {
        WorkflowEvent::WorkflowCompleted { output, .. } => Some(output.clone()),
        _ => None,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runs_one_native_and_one_wasm_activity_to_completion() {
    let (url, _c) = setup_db().await;
    let queue = "q-native-and-wasm";
    let mut conn = connect(&url).await;
    // Isolate the module table under startup-SEED semantics (see the echo test).
    scrub(&mut conn).await;
    let input = serde_json::json!({ "n": 21 });
    let exec_id = seed_workflow(&mut conn, "wf_native_and_wasm", input.clone(), queue).await;

    // One native activity (real Rust handler) + one WASM activity (deny-all
    // caps, startup-published). Both bound to the same worker/queue.
    let registry = build_mixed_registry(
        vec![wf_info("wf_native_and_wasm", wf_native_and_wasm)],
        vec![native_act_info("native_double", native_double)],
        vec![WasmActivitySpec {
            name: "echo_wasm",
            bytes: assemble(ECHO_WAT),
            caps: WasmCapabilities::default(),
            limits: WasmLimits::default(),
            retry: None,
        }],
        Arc::new(RecordingMetrics::default()),
    );
    let worker = build_worker("w-native-and-wasm", queue, Arc::clone(&registry));
    let pool = build_pool(&url);
    run_to_state(
        &url,
        &pool,
        worker,
        exec_id,
        "COMPLETED",
        Duration::from_secs(30),
    )
    .await;

    let history = load_history(&url, exec_id).await;

    // BOTH activities produced the correct output.
    assert_eq!(
        completed_output_for(&history, "native_double"),
        Some(serde_json::json!({ "doubled": 42 })),
        "the native activity must double n=21 to 42"
    );
    assert_eq!(
        completed_output_for(&history, "echo_wasm"),
        Some(input.clone()),
        "the WASM activity must echo its input"
    );
    // The workflow's terminal output combines both — proving both ran to
    // completion through the standard dispatch path.
    assert_eq!(
        workflow_completed_output(&history),
        Some(serde_json::json!({
            "native": { "doubled": 42 },
            "wasm": input,
        })),
        "the workflow output combines the native and WASM results"
    );

    // AC6: the WASM activity is INDISTINGUISHABLE from the native one in
    // history — exactly two ordinary ActivityScheduled + two ActivityCompleted,
    // and NO wasm-specific event variant of any kind.
    let types: Vec<&str> = history.iter().map(WorkflowEvent::type_name).collect();
    assert_eq!(
        types.iter().filter(|t| **t == "ActivityScheduled").count(),
        2,
        "exactly two ActivityScheduled (one native, one WASM): {types:?}"
    );
    assert_eq!(
        types.iter().filter(|t| **t == "ActivityCompleted").count(),
        2,
        "exactly two ActivityCompleted (one native, one WASM): {types:?}"
    );
    assert!(
        !types.iter().any(|t| t.to_lowercase().contains("wasm")),
        "no wasm-specific event variant may appear in history: {types:?}"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// Dispatch-overhead measurement (Success Metric: "< 10 ms p99 vs native").
//
// #[ignore]d — a percentile-timing microbenchmark is inherently flaky under CI
// scheduling and is NOT a gate. Run explicitly with:
//   cargo test -p autumn-harvest --features wasm-activities --test integration \
//     -- --ignored dispatch_overhead --nocapture
//
// Measures the PER-INVOCATION overhead of a trivial cached WASM echo
// (instantiate + call — the module is compiled/cached ONCE up front, so
// compilation is excluded) against a native Rust closure baseline. This is
// deliberately the pure invocation primitive (`invoke_wasm_activity`), not the
// full worker round-trip, so the number isolates WASM instantiation cost from
// task-queue and DB latency (which native and WASM activities share equally).
// ═══════════════════════════════════════════════════════════════════════════

#[test]
#[ignore = "percentile microbenchmark; run with --ignored --nocapture, not a CI gate"]
fn dispatch_overhead_wasm_echo_vs_native() {
    use std::time::Instant;

    const N: usize = 500;
    const WARMUP: usize = 50;

    let store = WasmModuleStore::new();
    let bytes = echo_bytes();
    let hash = WasmModuleStore::compute_hash(&bytes);
    // Compile + cache ONCE up front so the measured loop excludes compilation.
    let module = store.get_or_compile(&hash, &bytes).expect("compile echo");
    let caps = WasmCapabilities::default();
    let limits = WasmLimits::default();
    let input = serde_json::json!({ "hello": "world", "n": 42 });

    // Native baseline: a trivial echo closure returning the same JSON.
    let native = |v: &serde_json::Value| -> serde_json::Value { v.clone() };

    // Nearest-rank percentile via integer math (no float casts): for a sorted
    // slice of length L, idx = round(p * (L-1) / 100).
    let pct = |sorted: &[Duration], p: usize| -> Duration {
        if sorted.is_empty() {
            return Duration::ZERO;
        }
        let idx = (p * (sorted.len() - 1) + 50) / 100;
        sorted[idx.min(sorted.len() - 1)]
    };

    // Warm up both paths (JIT/page faults/allocator).
    for _ in 0..WARMUP {
        let _ = invoke_wasm_activity(&store, &module, &input, &caps, &limits, None).unwrap();
        std::hint::black_box(native(&input));
    }

    let mut wasm_samples = Vec::with_capacity(N);
    let mut native_samples = Vec::with_capacity(N);
    for _ in 0..N {
        let t0 = Instant::now();
        let out = invoke_wasm_activity(&store, &module, &input, &caps, &limits, None).unwrap();
        wasm_samples.push(t0.elapsed());
        assert_eq!(out, input);

        let t1 = Instant::now();
        std::hint::black_box(native(&input));
        native_samples.push(t1.elapsed());
    }

    wasm_samples.sort_unstable();
    native_samples.sort_unstable();

    let w50 = pct(&wasm_samples, 50);
    let w99 = pct(&wasm_samples, 99);
    let n50 = pct(&native_samples, 50);
    let n99 = pct(&native_samples, 99);
    let d50 = w50.saturating_sub(n50);
    let d99 = w99.saturating_sub(n99);

    println!("\n=== WASM cached-echo dispatch overhead (N={N}) ===");
    println!("  wasm   p50={w50:?}  p99={w99:?}");
    println!("  native p50={n50:?}  p99={n99:?}");
    println!("  overhead (wasm - native)  p50={d50:?}  p99={d99:?}");
    println!(
        "  success-metric target: p99 overhead < 10ms  →  {}",
        if d99 < Duration::from_millis(10) {
            "MET"
        } else {
            "NOT MET (per-invocation instantiation cost; mitigation: instance pooling)"
        }
    );
}
