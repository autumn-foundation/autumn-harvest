#![allow(clippy::all, clippy::pedantic, clippy::nursery)]
//! WASM-sandboxed polyglot activities — issue #965 (R&D spike, `wasm-activities`
//! feature).
//!
//! ## The problem
//!
//! Every Harvest activity is a Rust `#[activity]` compiled into the worker. A
//! team whose business logic lives in another language must rewrite it in Rust or
//! stand up a second service. This spike runs an activity implemented as a
//! **WebAssembly module** inside the existing worker — sandboxed, resource-metered,
//! and content-hash hot-swappable — dispatched through the *standard* task-queue
//! path (JSON in, JSON out, honouring queue/retry/start-to-close like a native
//! activity).
//!
//! ## The embedder API — one builder call
//!
//! ```no_run
//! use autumn_harvest::prelude::*;
//! use autumn_harvest::wasm_activities::{WasmCapabilities, WasmLimits};
//! use autumn_harvest::wasm_store::WasmActivityRegistration;
//!
//! # fn wasm_bytes() -> Vec<u8> { Vec::new() }
//! let builder = HarvestBuilder::new().wasm_activity(
//!     WasmActivityRegistration::new("checksum", wasm_bytes())
//!         .with_capabilities(WasmCapabilities { allow_clock: true, ..Default::default() })
//!         // Any `WasmLimits { .. }` literal MUST set every field — spread the
//!         // default so `max_wall_clock` (the mandatory hard ceiling) is present.
//!         .with_limits(WasmLimits { fuel: 50_000_000, ..WasmLimits::default() }),
//! );
//! ```
//!
//! The module bytes are auto-published to each worker's shard database at startup
//! and the guest is run through the worker's WASM dispatch seam. Capabilities are
//! **deny-all by default**; grant them (and override limits/queue/retry) via the
//! fluent setters.
//!
//! ## What this example demonstrates (runs standalone, no database)
//!
//! Using the public runtime primitive (`invoke_wasm_activity` — the same call the
//! worker seam makes), against three tiny WAT guests:
//!
//! 1. **JSON round-trip** through an echo guest (deny-all caps).
//! 2. **A capability GRANT** — a guest importing `env::now_millis` runs only when
//!    `allow_clock` is granted.
//! 3. **A sandbox DENIAL** — a guest importing `env::fs_read` (filesystem access is
//!    never grantable in this spike) is denied at instantiation as a non-retryable
//!    `SandboxDenied`, without touching the host.
//!
//! Run with: `cargo run -p autumn-harvest --features wasm-activities --example wasm_activity`

use autumn_harvest::HarvestBuilder;
use autumn_harvest::failure::ERROR_TYPE_SANDBOX_DENIED;
use autumn_harvest::wasm_activities::{
    WasmCapabilities, WasmLimits, WasmModuleStore, invoke_wasm_activity,
};
use autumn_harvest::wasm_store::WasmActivityRegistration;

/// Echo guest (no imports), loaded from the shared guests directory so this
/// example runs the SAME bytes the integration tests and `wasm-guests/README.md`
/// point at (issue #965 review — each site previously carried its own inline
/// copy, leaving the documented guest unreferenced and free to drift).
///
/// `run` returns `packed(in_ptr, in_len)`, so the host reads back the exact JSON
/// bytes it wrote. A bump allocator serves `alloc`.
const ECHO_WAT: &str = include_str!("wasm-guests/echo.wat");

/// Clock guest: imports `env::now_millis` (a GRANTABLE capability), calls it, and
/// returns the JSON literal `true`. Runs only when `allow_clock` is granted.
const CLOCK_WAT: &str = r#"
    (module
      (import "env" "now_millis" (func $now (result i64)))
      (memory (export "memory") 1)
      (data (i32.const 2048) "true")
      (func (export "alloc") (param i32) (result i32) (i32.const 4096))
      (func (export "run") (param i32 i32) (result i64)
        (drop (call $now))
        (i64.or (i64.shl (i64.const 2048) (i64.const 32)) (i64.const 4))))
"#;

/// Filesystem guest: imports `env::fs_read`, which is NEVER grantable (no host
/// function backs it). Denied at instantiation under any capability set.
const FS_WAT: &str = r#"
    (module
      (import "env" "fs_read" (func $fs (param i32) (result i32)))
      (memory (export "memory") 1)
      (func (export "alloc") (param i32) (result i32) (i32.const 1024))
      (func (export "run") (param i32 i32) (result i64) (i64.const 0)))
"#;

fn assemble(wat: &str) -> Vec<u8> {
    wat::parse_str(wat).expect("wat assembles")
}

fn main() {
    // ── The embedder API: register two WASM activities on a builder. ─────────
    //
    // This is all an application does — the module bytes are startup-published
    // and dispatched through the worker's WASM seam. We inspect the wired
    // registrations to show they landed; a real app would go on to
    // `.workflows(...)`, `.worker(...)`, etc.
    let built = HarvestBuilder::new()
        .wasm_activity(
            WasmActivityRegistration::new("echo_wasm", assemble(ECHO_WAT))
                // Deny-all: this guest needs no host capabilities.
                .with_limits(WasmLimits::default()),
        )
        .wasm_activity(
            WasmActivityRegistration::new("clock_wasm", assemble(CLOCK_WAT))
                // GRANT the clock capability to this activity only.
                .with_capabilities(WasmCapabilities {
                    allow_clock: true,
                    ..Default::default()
                })
                .with_limits(WasmLimits {
                    fuel: 50_000_000,
                    ..WasmLimits::default()
                }),
        )
        .build();
    assert_eq!(
        built.wasm_module_registrations().len(),
        2,
        "both WASM activities are wired for startup-publish"
    );
    println!(
        "registered {} WASM activities (startup-published to each worker)",
        built.wasm_module_registrations().len()
    );

    // ── The runtime behaviour, demonstrated directly (no DB / worker needed). ─
    let store = WasmModuleStore::new();
    let compile = |wat: &str| {
        let bytes = assemble(wat);
        let hash = WasmModuleStore::compute_hash(&bytes);
        store.get_or_compile(&hash, &bytes).expect("compile")
    };

    // 1. JSON round-trip through the echo guest.
    let echo = compile(ECHO_WAT);
    let input = serde_json::json!({ "order_id": 42, "items": ["a", "b"] });
    let out = invoke_wasm_activity(
        &store,
        &echo,
        &input,
        &WasmCapabilities::default(),
        &WasmLimits::default(),
        None,
    )
    .expect("echo runs under deny-all caps");
    assert_eq!(out, input);
    println!("1. echo round-trip: {out}");

    // 2. Capability GRANT: the clock guest runs only when `allow_clock` is set.
    let clock = compile(CLOCK_WAT);
    let granted = WasmCapabilities {
        allow_clock: true,
        ..Default::default()
    };
    let out = invoke_wasm_activity(
        &store,
        &clock,
        &serde_json::json!(null),
        &granted,
        &WasmLimits::default(),
        None,
    )
    .expect("clock guest runs when the clock capability is granted");
    assert_eq!(out, serde_json::json!(true));
    println!("2. capability GRANT (allow_clock): clock guest ran → {out}");

    //    ...and the SAME guest is DENIED under deny-all caps (import unsatisfied).
    let denied = invoke_wasm_activity(
        &store,
        &clock,
        &serde_json::json!(null),
        &WasmCapabilities::default(),
        &WasmLimits::default(),
        None,
    )
    .expect_err("clock guest is denied when the capability is NOT granted");
    assert_eq!(denied.error_type, ERROR_TYPE_SANDBOX_DENIED);
    assert!(denied.non_retryable, "a sandbox denial is non-retryable");
    println!(
        "   same guest under deny-all caps: DENIED ({}) — non-retryable",
        denied.error_type
    );

    // 3. Sandbox DENIAL: filesystem access is never grantable.
    let fs = compile(FS_WAT);
    let denied = invoke_wasm_activity(
        &store,
        &fs,
        &serde_json::json!(null),
        // Grant EVERYTHING representable — fs_read still has no host function.
        &WasmCapabilities {
            allow_clock: true,
            allow_random: true,
            allow_env: vec!["ANY".to_string()],
        },
        &WasmLimits::default(),
        None,
    )
    .expect_err("filesystem access is never grantable");
    assert_eq!(denied.error_type, ERROR_TYPE_SANDBOX_DENIED);
    println!(
        "3. sandbox DENIAL (env::fs_read, ungrantable): {} — the worker records this as an \
         ordinary non-retryable ActivityFailed and is never compromised",
        denied.error_type
    );

    println!("\nAll WASM activity demonstrations passed.");
}
