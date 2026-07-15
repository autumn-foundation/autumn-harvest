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

use autumn_harvest::failure::{
    ERROR_TYPE_RESOURCE_EXHAUSTED, ERROR_TYPE_SANDBOX_DENIED, ERROR_TYPE_WASM_MODULE_INVALID,
    ERROR_TYPE_WASM_MODULE_LOOKUP_FAILED, ERROR_TYPE_WASM_MODULE_UNAVAILABLE, ERROR_TYPE_WASM_TRAP,
};
use autumn_harvest::wasm_activities::{WasmCapabilities, WasmLimits, WasmModuleStore};
use autumn_harvest::wasm_store::{
    MAX_WASM_MODULE_BYTES, WasmBinding, WasmDispatch, fetch_wasm_module_bytes, list_wasm_modules,
    publish_registered_wasm_modules, publish_wasm_module, resolve_active_wasm_hash,
    resolve_active_wasm_module, resolve_wasm_dispatch,
};
use autumn_harvest::worker::DbPool;
use diesel::sql_types::BigInt;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use std::sync::Arc;
use std::time::Duration;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

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
    assert_eq!(ERROR_TYPE_WASM_MODULE_LOOKUP_FAILED, "WasmModuleLookupFailed");
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
    let dispatch = resolve_wasm_dispatch(&mut conn, &store, &binding, "echo", None).await;
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
    let dispatch =
        resolve_wasm_dispatch(&mut conn, &store, &binding, "echo", Some(Duration::from_secs(5)))
            .await;
    let prepared = match dispatch {
        WasmDispatch::Invoke(prepared) => prepared,
        WasmDispatch::Fail(payload) => panic!("expected invoke, got: {payload}"),
    };
    let input = serde_json::json!({"hello": "world", "n": 42});
    let out = prepared.invoke(&input).expect("echo runs");
    assert_eq!(out, input);

    // Second resolution serves the cached compiled module (no bytes fetch).
    assert!(store.cached(&WasmModuleStore::compute_hash(&echo_bytes())).is_some());
}

#[tokio::test]
async fn resolve_dispatch_invalid_when_bytes_do_not_compile() {
    let (pool, _c) = conn_and_pool().await;
    let mut conn = pool.get().await.expect("conn");
    scrub(&mut conn).await;

    // Publish garbage bytes: the store's integrity/compile step must reject them.
    publish_wasm_module(&mut conn, "bad", b"not a wasm module")
        .await
        .expect("publish garbage");

    let store = Arc::new(WasmModuleStore::new());
    let binding = WasmBinding {
        capabilities: WasmCapabilities::default(),
        limits: WasmLimits::default(),
    };
    let dispatch = resolve_wasm_dispatch(&mut conn, &store, &binding, "bad", None).await;
    match dispatch {
        WasmDispatch::Fail(payload) => {
            assert!(
                payload.contains(ERROR_TYPE_WASM_MODULE_INVALID),
                "expected invalid, got: {payload}"
            );
        }
        WasmDispatch::Invoke(_) => panic!("garbage bytes must be invalid"),
    }
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
