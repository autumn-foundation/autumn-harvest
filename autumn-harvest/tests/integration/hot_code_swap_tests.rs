//! Hot code swap for workflow definitions via runtime modules (issue #967).
//!
//! These are the executable half of the R&D spike whose written half is
//! `docs/rnd/hot-code-swap.md`. Everything here is gated behind the
//! `hot-code-swap` Cargo feature, so a default build compiles none of it.
//!
//! The suite is organised by the deliverable it evidences:
//!
//! * **ABI + verification** — the decide-loop wire format, content addressing,
//!   and the checksum/signature gate a worker applies before it will load
//!   bytes a registry handed it (AC2's "verify" step).
//! * **Registry** — load / get / unload, the duplicate-registration rejection
//!   that keeps two modules from claiming one workflow name outside build-id
//!   governance (AC5), and the unload-with-live-holder hazard (AC5).
//! * **Trampoline** — a WASM-hosted workflow driven to completion through the
//!   ordinary `WorkflowHandlerFn` seam, including the routing rule that a
//!   module is chosen by the *execution's* assigned build, never by the
//!   worker's own `ctx.build_id()`.
//! * **Replay fidelity** — the AC4 proof: a module-hosted history is
//!   byte-identical to the statically-linked one, and each replays clean under
//!   the other's hosting.
//! * **Safety bounds** — step cap, fuel/epoch bound, trap containment.
//! * **Registry storage + hot swap** (DB) — publish/fetch/verify/sync against
//!   Postgres, then the AC3 end-to-end demonstration: v2 published under a new
//!   build id with no restart, `set_build_ramp` moving new starts onto it while
//!   a v1-assigned in-flight execution stays on v1, and `clear_build_ramp`
//!   rolling back.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use autumn_harvest::build_routing::{
    BuildCompatibilitySet, BuildPolicy, clear_build_ramp, declare_compat, set_build_policy,
    set_build_ramp,
};
use autumn_harvest::context::WorkflowContext;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::hot_swap::{
    DecideOutcome, DecideRequest, DecideResponse, DecisionCache, HotSwapError,
    MAX_CACHED_DECISION_BYTES, MAX_CACHED_DECISIONS, MAX_CACHED_RESPONSE_BYTES, ModuleHost,
    ModuleRegistry, ModuleVerification, compute_module_hash, encode_decide_request,
    module_workflow_handler, sign_module_binding, verify_module_bytes, with_module_host,
};
use autumn_harvest::hot_swap_store::{
    fetch_workflow_module, list_workflow_modules_for_build, publish_workflow_module,
    retire_build_modules, sync_build_into_registry,
};
use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer, WorkflowTestEnv};
use autumn_harvest::types::ExecutionId;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

type BoxFut<'a> = Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>>;

// ── guests ────────────────────────────────────────────────────────────────────

const PIPELINE_V1_WAT: &str = include_str!("../../examples/workflow-modules/pipeline_v1.wat");
const PIPELINE_V2_WAT: &str = include_str!("../../examples/workflow-modules/pipeline_v2.wat");

/// A guest whose `run` traps immediately — the panic/trap containment probe.
const TRAP_WAT: &str = r#"
    (module
      (memory (export "memory") 1)
      (global $bump (mut i32) (i32.const 1024))
      (func (export "alloc") (param $len i32) (result i32)
        (local $ptr i32)
        (local.set $ptr (global.get $bump))
        (global.set $bump (i32.add (global.get $bump) (local.get $len)))
        (local.get $ptr))
      (func (export "run") (param i32) (param i32) (result i64)
        (unreachable)))
"#;

/// A guest that spins forever — the fuel / epoch bound probe.
const SPIN_WAT: &str = r#"
    (module
      (memory (export "memory") 1)
      (global $bump (mut i32) (i32.const 1024))
      (func (export "alloc") (param $len i32) (result i32)
        (local $ptr i32)
        (local.set $ptr (global.get $bump))
        (global.set $bump (i32.add (global.get $bump) (local.get $len)))
        (local.get $ptr))
      (func (export "run") (param i32) (param i32) (result i64)
        (loop $forever (br $forever))
        (i64.const 0)))
"#;

/// A guest that always asks for one more activity — the decide-step cap probe.
const NEVER_TERMINATES_WAT: &str = r#"
    (module
      (memory (export "memory") 1)
      (data (i32.const 1024) "{\"kind\":\"await\",\"activity\":\"charge\",\"input\":{\"amount\":100}}")
      (global $bump (mut i32) (i32.const 4096))
      (func (export "alloc") (param $len i32) (result i32)
        (local $ptr i32)
        (local.set $ptr (global.get $bump))
        (global.set $bump (i32.add (global.get $bump) (local.get $len)))
        (local.get $ptr))
      (func (export "run") (param i32) (param i32) (result i64)
        (i64.or
          (i64.shl (i64.extend_i32_u (i32.const 1024)) (i64.const 32))
          (i64.extend_i32_u (i32.const 59)))))
"#;

/// A guest that asks for a queue override — the lateral-movement probe.
const QUEUE_HOPPER_WAT: &str = r#"
    (module
      (memory (export "memory") 1)
      (data (i32.const 1024) "{\"kind\":\"await\",\"activity\":\"charge\",\"input\":{\"amount\":100},\"queue\":\"other-queue\"}")
      (data (i32.const 1280) "{\"kind\":\"complete\",\"output\":\"v1-done\"}")
      (global $bump (mut i32) (i32.const 4096))
      (func (export "alloc") (param $len i32) (result i32)
        (local $ptr i32)
        (local.set $ptr (global.get $bump))
        (global.set $bump (i32.add (global.get $bump) (local.get $len)))
        (local.get $ptr))
      (func $pack (param $ptr i32) (param $len i32) (result i64)
        (i64.or
          (i64.shl (i64.extend_i32_u (local.get $ptr)) (i64.const 32))
          (i64.extend_i32_u (local.get $len))))
      (func (export "run") (param $in_ptr i32) (param $in_len i32) (result i64)
        (if (result i64)
          (i32.eqz
            (i32.sub
              (i32.load8_u (i32.add (local.get $in_ptr) (i32.const 8)))
              (i32.const 48)))
          (then (call $pack (i32.const 1024) (i32.const 81)))
          (else (call $pack (i32.const 1280) (i32.const 38))))))
"#;

fn pipeline_v1_bytes() -> Vec<u8> {
    wat::parse_str(PIPELINE_V1_WAT).expect("pipeline_v1.wat assembles")
}

fn pipeline_v2_bytes() -> Vec<u8> {
    wat::parse_str(PIPELINE_V2_WAT).expect("pipeline_v2.wat assembles")
}

// ── statically-linked equivalents ─────────────────────────────────────────────

/// The statically-linked twin of `pipeline_v1.wat`.
///
/// Same command stream, same queue, same terminal output. The AC4 proof rests
/// on this being *logically* the same workflow hosted a different way — so the
/// histories must be byte-identical and each must replay clean under the
/// other's hosting.
fn native_pipeline_v1(ctx: &WorkflowContext, _input: Value) -> BoxFut<'_> {
    Box::pin(async move {
        let queue = ctx.queue_name().to_string();
        ctx.execute_activity_raw("charge", json!({"amount": 100}), &queue)
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!("v1-done"))
    })
}

/// The statically-linked twin of `pipeline_v2.wat`.
fn native_pipeline_v2(ctx: &WorkflowContext, _input: Value) -> BoxFut<'_> {
    Box::pin(async move {
        let queue = ctx.queue_name().to_string();
        ctx.execute_activity_raw("charge", json!({"amount": 100}), &queue)
            .await
            .map_err(|e| e.to_string())?;
        ctx.execute_activity_raw("notify", json!({"channel": "email"}), &queue)
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!("v2-done"))
    })
}

/// A frozen clock anchor, so two independent runs of the same logic produce the
/// same `WorkflowStarted` timestamp and the byte-identity comparison below is
/// comparing the code's behaviour rather than the wall clock.
fn anchor() -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::parse_from_rfc3339("2026-07-08T00:00:00Z")
        .expect("valid anchor")
        .with_timezone(&chrono::Utc)
}

fn env() -> WorkflowTestEnv {
    WorkflowTestEnv::new()
        .with_workflow_name("pipeline")
        // A real queue name, because the trampoline refuses to schedule onto
        // the empty one: no worker polls it, so the activity would sit until
        // schedule-to-start expired instead of failing fast. The harness
        // defaults `ctx.queue_name()` to `""`, which is only ever a test
        // artefact.
        .with_queue_name("default")
        .with_frozen_anchor(anchor())
        .mock_activity("charge", |_| Ok(json!({"ok": true})))
        .mock_activity("notify", |_| Ok(json!({"sent": true})))
}

fn registry_with(entries: &[(&str, &str, Vec<u8>)]) -> Arc<ModuleRegistry> {
    let registry = Arc::new(ModuleRegistry::new());
    for (build_id, workflow, bytes) in entries {
        registry
            .load_module(build_id, workflow, bytes, &ModuleVerification::none())
            .unwrap_or_else(|e| panic!("load {build_id}/{workflow}: {e}"));
    }
    registry
}

fn host(registry: &Arc<ModuleRegistry>, build_id: &str) -> ModuleHost {
    ModuleHost::new(Arc::clone(registry)).with_build_id(build_id)
}

// ══ ABI + verification ════════════════════════════════════════════════════════

#[test]
fn decide_request_serialises_step_first_so_a_wat_guest_can_read_it() {
    // The WAT guests in `examples/workflow-modules/` read their step index with
    // a single `i32.load8_u` at offset 8. That is only sound while `step` is
    // the FIRST serialised field: `{`,`"`,`s`,`t`,`e`,`p`,`"`,`:`,<digit>.
    // Reordering the struct would silently make every guest decide step 0
    // forever, so the offset is pinned here rather than left to convention.
    let request = DecideRequest {
        step: 2,
        workflow: "pipeline".to_string(),
        input: json!({"a": 1}),
        resolved: vec![DecideOutcome::Ok {
            output: json!("first"),
        }],
    };
    // Deliberately the HOST's own encoder, not a local `serde_json::to_vec`.
    // An earlier cut of this guard serialised the struct directly while the host
    // handed the guest a `serde_json::Value` round trip — whose object is a
    // `BTreeMap`, so the real bytes reached the guest alphabetically ordered
    // (`abi_version` first) and every guest read a step of `'r' - '0'`. The
    // guard passed and every guest silently completed at step 0. Asserting on
    // `encode_decide_request` is what makes this test non-vacuous.
    let bytes = encode_decide_request(&request).expect("serialise");
    assert_eq!(
        &bytes[..9],
        br#"{"step":2"#,
        "step must serialise first, at a fixed offset, or the WAT guests break"
    );
    assert_eq!(bytes[8], b'2');
}

#[test]
fn the_hosts_encoder_never_reorders_keys_the_way_a_json_value_would() {
    // The specific regression above, pinned as its own claim: a `Value` round
    // trip sorts keys, the host's encoder must not.
    let request = DecideRequest {
        step: 0,
        workflow: "pipeline".to_string(),
        input: json!({}),
        resolved: Vec::new(),
    };
    let host_bytes = encode_decide_request(&request).expect("serialise");
    let via_value = serde_json::to_vec(&serde_json::to_value(&request).expect("to_value"))
        .expect("serialise value");
    assert!(host_bytes.starts_with(br#"{"step":"#));
    assert!(
        !via_value.starts_with(br#"{"step":"#),
        "a `Value` round trip sorts the keys, so `step` must NOT come first \
         through it. If serde_json ever stops sorting, this guard is obsolete — \
         but the host must not depend on either behaviour, which is why it \
         serialises the struct directly. Got: {}",
        String::from_utf8_lossy(&via_value)
    );
    assert_ne!(
        host_bytes, via_value,
        "the two encodings must stay distinguishable, or this guard proves nothing"
    );
}

#[test]
fn decide_response_variants_round_trip() {
    for (wire, expected) in [
        (
            json!({"kind": "await", "activity": "charge", "input": {"amount": 100}}),
            DecideResponse::Await {
                activity: "charge".to_string(),
                input: json!({"amount": 100}),
                queue: None,
            },
        ),
        (
            json!({"kind": "complete", "output": "v1-done"}),
            DecideResponse::Complete {
                output: json!("v1-done"),
            },
        ),
        (
            json!({"kind": "fail", "error": "boom"}),
            DecideResponse::Fail {
                error: "boom".to_string(),
            },
        ),
    ] {
        let parsed: DecideResponse =
            serde_json::from_value(wire.clone()).unwrap_or_else(|e| panic!("parse {wire}: {e}"));
        assert_eq!(parsed, expected);
    }
}

#[test]
fn decide_outcome_round_trips() {
    let ok: DecideOutcome =
        serde_json::from_value(json!({"kind": "ok", "output": 7})).expect("parse ok outcome");
    assert_eq!(ok, DecideOutcome::Ok { output: json!(7) });
    let err: DecideOutcome =
        serde_json::from_value(json!({"kind": "err", "error_type": "Error", "error": "nope"}))
            .expect("parse err outcome");
    assert_eq!(
        err,
        DecideOutcome::Err {
            error_type: "Error".to_string(),
            details: None,
            error: "nope".to_string(),
        }
    );
}

#[test]
fn module_hash_is_content_addressed() {
    let v1 = pipeline_v1_bytes();
    let v2 = pipeline_v2_bytes();
    let h1 = compute_module_hash(&v1);
    assert_eq!(h1, compute_module_hash(&v1), "hashing is stable");
    assert_ne!(
        h1,
        compute_module_hash(&v2),
        "different bytes, different id"
    );
    assert_eq!(h1.len(), 64, "lowercase hex sha-256");
    assert!(
        h1.chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
    );
}

const OPERATOR_KEY: &[u8] = b"a-sufficiently-long-operator-key";
const ATTACKER_KEY: &[u8] = b"a-sufficiently-long-attacker-key";

fn sign(build_id: &str, workflow: &str, bytes: &[u8]) -> String {
    sign_module_binding(
        OPERATOR_KEY,
        build_id,
        workflow,
        &compute_module_hash(bytes),
    )
    .expect("sign")
}

#[test]
fn verification_rejects_a_hash_mismatch() {
    let bytes = pipeline_v1_bytes();
    let wrong = compute_module_hash(&pipeline_v2_bytes());
    let err = verify_module_bytes("wf-v1", "pipeline", &bytes, Some(&wrong), None, None)
        .expect_err("bytes that do not hash to the expected id must be refused");
    assert!(
        matches!(err, HotSwapError::HashMismatch { .. }),
        "expected HashMismatch, got {err:?}"
    );
}

#[test]
fn verification_accepts_matching_bytes() {
    let bytes = pipeline_v1_bytes();
    let hash = compute_module_hash(&bytes);
    verify_module_bytes("wf-v1", "pipeline", &bytes, Some(&hash), None, None)
        .expect("matching bytes verify");
}

#[test]
fn verification_rejects_a_missing_signature_when_a_key_is_configured() {
    let bytes = pipeline_v1_bytes();
    let hash = compute_module_hash(&bytes);
    let err = verify_module_bytes(
        "wf-v1",
        "pipeline",
        &bytes,
        Some(&hash),
        None,
        Some(OPERATOR_KEY),
    )
    .expect_err("an unsigned module must not load into a signing deployment");
    assert!(
        matches!(err, HotSwapError::MissingSignature),
        "expected MissingSignature, got {err:?}"
    );
}

#[test]
fn verification_rejects_a_forged_signature() {
    let bytes = pipeline_v1_bytes();
    let hash = compute_module_hash(&bytes);
    let forged =
        sign_module_binding(ATTACKER_KEY, "wf-v1", "pipeline", &hash).expect("attacker signs");
    let err = verify_module_bytes(
        "wf-v1",
        "pipeline",
        &bytes,
        Some(&hash),
        Some(&forged),
        Some(OPERATOR_KEY),
    )
    .expect_err("a signature under the wrong key must be refused");
    assert!(
        matches!(err, HotSwapError::BadSignature { .. }),
        "expected BadSignature, got {err:?}"
    );
}

#[test]
fn verification_accepts_a_good_signature() {
    let bytes = pipeline_v1_bytes();
    let hash = compute_module_hash(&bytes);
    let signature = sign("wf-v1", "pipeline", &bytes);
    verify_module_bytes(
        "wf-v1",
        "pipeline",
        &bytes,
        Some(&hash),
        Some(&signature),
        Some(OPERATOR_KEY),
    )
    .expect("a correctly signed module verifies");
}

#[test]
fn a_signed_module_cannot_be_rebound_under_another_build_id() {
    // The downgrade attack the binding signature exists to stop. An attacker
    // with INSERT on the registry — but no key — copies a legitimately signed
    // row and re-publishes those exact bytes, with that exact signature, under
    // whichever build id the ramp currently points at. Signing the content hash
    // alone would let this through: the bytes really were approved, just not
    // *as this build*.
    let bytes = pipeline_v1_bytes();
    let hash = compute_module_hash(&bytes);
    let legitimate = sign("wf-v1", "pipeline", &bytes);

    let err = verify_module_bytes(
        "wf-v9-current-ramp-target",
        "pipeline",
        &bytes,
        Some(&hash),
        Some(&legitimate),
        Some(OPERATOR_KEY),
    )
    .expect_err("a signature must not travel to another build id");
    assert!(matches!(err, HotSwapError::BadSignature { .. }), "{err:?}");
}

#[test]
fn a_signed_module_cannot_be_rebound_under_another_workflow_name() {
    // The substitution attack: run workflow A's approved module as workflow B,
    // so B's inputs are fed to A's logic.
    let bytes = pipeline_v1_bytes();
    let hash = compute_module_hash(&bytes);
    let legitimate = sign("wf-v1", "pipeline", &bytes);

    let err = verify_module_bytes(
        "wf-v1",
        "payment_capture",
        &bytes,
        Some(&hash),
        Some(&legitimate),
        Some(OPERATOR_KEY),
    )
    .expect_err("a signature must not travel to another workflow name");
    assert!(matches!(err, HotSwapError::BadSignature { .. }), "{err:?}");
}

#[test]
fn a_signature_is_ignored_when_no_key_is_configured() {
    // A deployment that has not configured a signing key is not made *less*
    // safe by a module carrying a signature it cannot check: content addressing
    // still applies, the signature is simply not load-bearing.
    let bytes = pipeline_v1_bytes();
    let hash = compute_module_hash(&bytes);
    let signature = sign("wf-v1", "pipeline", &bytes);
    verify_module_bytes(
        "wf-v1",
        "pipeline",
        &bytes,
        Some(&hash),
        Some(&signature),
        None,
    )
    .expect("unchecked signature is not an error");
}

#[test]
fn an_uppercase_hex_signature_is_the_same_signature() {
    // Operational foot-gun, not a vulnerability: hex case carries no meaning,
    // so a tag that round-tripped through something that upcased it must still
    // verify rather than reading as a forgery.
    let bytes = pipeline_v1_bytes();
    let hash = compute_module_hash(&bytes);
    let signature = sign("wf-v1", "pipeline", &bytes).to_uppercase();
    verify_module_bytes(
        "wf-v1",
        "pipeline",
        &bytes,
        Some(&hash),
        Some(&signature),
        Some(OPERATOR_KEY),
    )
    .expect("hex case is not part of the tag");
}

#[test]
fn a_too_short_signing_key_is_refused_rather_than_silently_weak() {
    let bytes = pipeline_v1_bytes();
    let hash = compute_module_hash(&bytes);
    // An unset environment variable is the realistic way this happens.
    let err = verify_module_bytes(
        "wf-v1",
        "pipeline",
        &bytes,
        Some(&hash),
        Some("00"),
        Some(b""),
    )
    .expect_err("an empty key must not yield a publicly-computable signature");
    assert!(
        matches!(err, HotSwapError::SigningKeyTooShort { .. }),
        "{err:?}"
    );
}

#[test]
fn verification_rejects_empty_bytes() {
    let err = verify_module_bytes("wf-v1", "pipeline", &[], None, None, None)
        .expect_err("empty module is not a module");
    assert!(matches!(err, HotSwapError::EmptyModule), "got {err:?}");
}

// ══ registry ══════════════════════════════════════════════════════════════════

#[test]
fn a_loaded_module_is_retrievable_by_build_and_workflow() {
    let registry = registry_with(&[("wf-v1", "pipeline", pipeline_v1_bytes())]);
    let loaded = registry
        .get("wf-v1", "pipeline")
        .expect("the module just loaded must be retrievable");
    assert_eq!(loaded.descriptor().build_id, "wf-v1");
    assert_eq!(loaded.descriptor().workflow_name, "pipeline");
    assert_eq!(
        loaded.descriptor().module_hash,
        compute_module_hash(&pipeline_v1_bytes())
    );
    assert!(registry.get("wf-v2", "pipeline").is_none());
    assert!(registry.get("wf-v1", "other").is_none());
}

#[test]
fn one_workflow_name_may_be_hosted_under_many_build_ids() {
    // This is the whole point: v1 and v2 of the SAME workflow coexist, kept
    // apart by build id rather than by name.
    let registry = registry_with(&[
        ("wf-v1", "pipeline", pipeline_v1_bytes()),
        ("wf-v2", "pipeline", pipeline_v2_bytes()),
    ]);
    assert_eq!(registry.len(), 2);
    assert_ne!(
        registry
            .get("wf-v1", "pipeline")
            .unwrap()
            .descriptor()
            .module_hash,
        registry
            .get("wf-v2", "pipeline")
            .unwrap()
            .descriptor()
            .module_hash,
    );
}

#[test]
fn two_modules_may_not_claim_one_workflow_name_under_one_build_id() {
    // AC5's determinism hazard: a second module registering the same workflow
    // name *outside* build-id governance must be rejected, mirroring the
    // duplicate-registration hardening from #597.
    let registry = registry_with(&[("wf-v1", "pipeline", pipeline_v1_bytes())]);
    let err = registry
        .load_module(
            "wf-v1",
            "pipeline",
            &pipeline_v2_bytes(),
            &ModuleVerification::none(),
        )
        .expect_err("rebinding a (build_id, workflow) to different bytes must be refused");
    assert!(
        matches!(err, HotSwapError::DuplicateRegistration { .. }),
        "expected DuplicateRegistration, got {err:?}"
    );
    // ... and the original binding is untouched.
    assert_eq!(
        registry
            .get("wf-v1", "pipeline")
            .unwrap()
            .descriptor()
            .module_hash,
        compute_module_hash(&pipeline_v1_bytes())
    );
}

#[test]
fn republishing_identical_bytes_under_one_build_id_is_idempotent() {
    let registry = registry_with(&[("wf-v1", "pipeline", pipeline_v1_bytes())]);
    registry
        .load_module(
            "wf-v1",
            "pipeline",
            &pipeline_v1_bytes(),
            &ModuleVerification::none(),
        )
        .expect("re-loading the same bytes is a no-op, not a conflict");
    assert_eq!(registry.len(), 1);
}

#[test]
fn unloading_a_build_drops_its_modules_but_not_a_live_holder() {
    // AC5's unload hazard. In the WASM hosting the answer is structural: a
    // caller that already resolved the module holds an `Arc`, so unload
    // removes the *binding* while the code stays alive for the in-flight
    // invocation. (The dylib answer is a use-after-free; see the report.)
    let registry = registry_with(&[
        ("wf-v1", "pipeline", pipeline_v1_bytes()),
        ("wf-v2", "pipeline", pipeline_v2_bytes()),
    ]);
    let in_flight = registry
        .get("wf-v1", "pipeline")
        .expect("resolve before unload");

    assert_eq!(registry.unload_build("wf-v1"), 1);
    assert!(registry.get("wf-v1", "pipeline").is_none(), "binding gone");
    assert!(
        registry.get("wf-v2", "pipeline").is_some(),
        "other build untouched"
    );

    // The holder is still usable — this is the safety property, not a leak.
    assert_eq!(in_flight.descriptor().build_id, "wf-v1");
    assert_eq!(
        in_flight.descriptor().module_hash,
        compute_module_hash(&pipeline_v1_bytes())
    );
}

#[test]
fn unloading_an_unknown_build_is_a_no_op() {
    let registry = registry_with(&[("wf-v1", "pipeline", pipeline_v1_bytes())]);
    assert_eq!(registry.unload_build("wf-does-not-exist"), 0);
    assert_eq!(registry.len(), 1);
}

#[test]
fn the_registry_refuses_bytes_that_fail_verification() {
    let registry = Arc::new(ModuleRegistry::new());
    let bytes = pipeline_v1_bytes();
    let wrong = compute_module_hash(&pipeline_v2_bytes());
    let err = registry
        .load_module(
            "wf-v1",
            "pipeline",
            &bytes,
            &ModuleVerification::none().with_expected_hash(&wrong),
        )
        .expect_err("verification runs before compilation");
    assert!(
        matches!(err, HotSwapError::HashMismatch { .. }),
        "got {err:?}"
    );
    assert!(
        registry.is_empty(),
        "a refused module must leave no binding"
    );
}

#[test]
fn the_registry_refuses_bytes_that_are_not_wasm() {
    let registry = Arc::new(ModuleRegistry::new());
    let err = registry
        .load_module(
            "wf-v1",
            "pipeline",
            b"not wasm at all",
            &ModuleVerification::none(),
        )
        .expect_err("garbage must not load");
    assert!(matches!(err, HotSwapError::Compile { .. }), "got {err:?}");
    assert!(registry.is_empty());
}

#[test]
fn the_registry_refuses_an_oversized_module() {
    let registry = Arc::new(ModuleRegistry::new());
    let huge = vec![0u8; autumn_harvest::hot_swap::MAX_WORKFLOW_MODULE_BYTES + 1];
    let err = registry
        .load_module("wf-v1", "pipeline", &huge, &ModuleVerification::none())
        .expect_err("the size ceiling is checked before any hashing or compilation");
    assert!(matches!(err, HotSwapError::TooLarge { .. }), "got {err:?}");
}

// ══ trampoline ════════════════════════════════════════════════════════════════

#[tokio::test]
async fn a_wasm_hosted_workflow_runs_to_completion_through_the_ordinary_handler_seam() {
    let registry = registry_with(&[("wf-v1", "pipeline", pipeline_v1_bytes())]);
    let outcome = with_module_host(
        host(&registry, "wf-v1"),
        env().run(module_workflow_handler, json!({"order": 1})),
    )
    .await;

    assert_eq!(outcome.result, Ok(json!("v1-done")));
    let scheduled: Vec<&str> = outcome
        .events()
        .iter()
        .filter_map(|e| match e {
            WorkflowEvent::ActivityScheduled { name, .. } => Some(name.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(scheduled, ["charge"]);
}

#[tokio::test]
async fn the_v2_module_runs_the_extra_step() {
    let registry = registry_with(&[("wf-v2", "pipeline", pipeline_v2_bytes())]);
    let outcome = with_module_host(
        host(&registry, "wf-v2"),
        env().run(module_workflow_handler, json!({"order": 1})),
    )
    .await;

    assert_eq!(outcome.result, Ok(json!("v2-done")));
    let scheduled: Vec<&str> = outcome
        .events()
        .iter()
        .filter_map(|e| match e {
            WorkflowEvent::ActivityScheduled { name, .. } => Some(name.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(scheduled, ["charge", "notify"]);
}

#[tokio::test]
async fn the_module_is_chosen_by_the_executions_build_not_by_the_workers_build_id() {
    // The routing rule that makes hot swap safe. `ctx.build_id()` is the
    // WORKER's configured build (issue #798 fixed that semantics deliberately),
    // so routing modules on it would hand a v1-assigned in-flight execution to
    // v2 code the moment the worker was relabelled. The host binds the
    // EXECUTION's assigned build instead.
    let registry = registry_with(&[
        ("wf-v1", "pipeline", pipeline_v1_bytes()),
        ("wf-v2", "pipeline", pipeline_v2_bytes()),
    ]);

    // One worker process, advertising a single host build id, hosting both.
    let worker_env = env().with_build_id("host-1");

    let v1 = with_module_host(
        host(&registry, "wf-v1"),
        worker_env.run(module_workflow_handler, json!({})),
    )
    .await;
    assert_eq!(
        v1.result,
        Ok(json!("v1-done")),
        "v1-assigned run stays on v1"
    );

    let v2 = with_module_host(
        host(&registry, "wf-v2"),
        env()
            .with_build_id("host-1")
            .run(module_workflow_handler, json!({})),
    )
    .await;
    assert_eq!(v2.result, Ok(json!("v2-done")), "v2-assigned run uses v2");
}

#[tokio::test]
async fn a_missing_module_fails_the_execution_loudly() {
    let registry = registry_with(&[("wf-v1", "pipeline", pipeline_v1_bytes())]);
    let outcome = with_module_host(
        host(&registry, "wf-v9"),
        env().run(module_workflow_handler, json!({})),
    )
    .await;

    let err = outcome
        .result
        .expect_err("an unresolvable module must not silently succeed");
    assert!(
        err.contains("wf-v9") && err.contains("pipeline"),
        "the error must name the build id and workflow it could not resolve: {err}"
    );
}

#[tokio::test]
async fn running_the_trampoline_without_a_host_bound_fails_loudly() {
    let outcome = env().run(module_workflow_handler, json!({})).await;
    let err = outcome
        .result
        .expect_err("no module host bound must be an error, not a silent no-op");
    assert!(
        err.to_lowercase().contains("module host"),
        "error should name the missing binding: {err}"
    );
}

#[tokio::test]
async fn an_activity_failure_is_handed_to_the_guest_rather_than_failing_the_run() {
    // The one place a hosted workflow is NOT interchangeable with a naive
    // statically-linked twin, pinned here so it is specified rather than
    // discovered.
    //
    // `native_pipeline_v1` propagates an activity failure with `?`, so the run
    // fails. The trampoline instead appends the failure to `resolved` and
    // re-enters the guest, because the guest — not the host — owns the decision
    // about what a failed step means. `pipeline_v1.wat` does not inspect
    // `resolved`, so it completes anyway.
    //
    // This is deliberate: a host that failed the run on the guest's behalf would
    // make saga/compensation logic inexpressible in a module. A guest that wants
    // propagation returns `DecideResponse::Fail`.
    let registry = registry_with(&[("wf-v1", "pipeline", pipeline_v1_bytes())]);
    let failing = WorkflowTestEnv::new()
        .with_workflow_name("pipeline")
        .with_queue_name("default")
        .with_frozen_anchor(anchor())
        .mock_activity("charge", |_| Err("card declined".to_string()));

    let hosted = with_module_host(
        host(&registry, "wf-v1"),
        failing.run(module_workflow_handler, json!({})),
    )
    .await;
    assert_eq!(
        hosted.result,
        Ok(json!("v1-done")),
        "the guest saw the failure and chose to continue; the host must not          override that"
    );

    // The statically-linked twin, on the same history, propagates instead —
    // which is why the byte-identity claim in this file is scoped to the
    // success path the guests and their twins actually share.
    let native = WorkflowTestEnv::new()
        .with_workflow_name("pipeline")
        .with_queue_name("default")
        .with_frozen_anchor(anchor())
        .mock_activity("charge", |_| Err("card declined".to_string()))
        .run(native_pipeline_v1, json!({}))
        .await;
    assert!(
        native.result.is_err(),
        "the native twin propagates the failure: {:?}",
        native.result
    );
}

// ══ replay fidelity — the AC4 proof ═══════════════════════════════════════════

/// Serialise a history, blanking only the one field that cannot be equal
/// between two independent runs of *any* workflow.
///
/// `WorkflowEvent` is adjacently tagged (`{"type": ..., "data": {...}}`), so
/// everything interesting lives under `data`. The single normalisation is
/// `activity_id`: `WorkflowContext::next_activity_id` mints a fresh UUID per
/// live dispatch, so it differs between two runs of the *same statically-linked
/// handler* just as much as between a hosted and a native one. Every other field
/// — the event type, its order, the activity name, its input, its queue, the
/// result payloads, the timestamps (pinned by `with_frozen_anchor`) — is
/// compared verbatim, which is what makes this a byte-identity claim rather
/// than a shape check.
fn normalise(events: &[WorkflowEvent]) -> Vec<Value> {
    events
        .iter()
        .map(|e| {
            let mut v = serde_json::to_value(e).expect("event serialises");
            if let Some(data) = v.get_mut("data").and_then(Value::as_object_mut)
                && data.contains_key("activity_id")
            {
                data.insert("activity_id".to_string(), json!("<normalised>"));
            }
            v
        })
        .collect()
}

#[tokio::test]
async fn a_module_hosted_history_is_byte_identical_to_the_statically_linked_one() {
    let registry = registry_with(&[("wf-v1", "pipeline", pipeline_v1_bytes())]);

    let hosted = with_module_host(
        host(&registry, "wf-v1"),
        env().run(module_workflow_handler, json!({"order": 7})),
    )
    .await;
    let native = env().run(native_pipeline_v1, json!({"order": 7})).await;

    assert_eq!(hosted.result, native.result, "same terminal result");
    assert_eq!(
        normalise(hosted.events()),
        normalise(native.events()),
        "hot swap changes where code comes from, never what the engine records"
    );
}

#[tokio::test]
async fn a_module_hosted_history_replays_clean_under_statically_linked_code() {
    // The cross-hosting replay the AC names: history produced by module-hosted
    // code, replayed by the ordinary `WorkflowReplayer` against the
    // statically-linked handler.
    let registry = registry_with(&[("wf-v1", "pipeline", pipeline_v1_bytes())]);
    let hosted = with_module_host(
        host(&registry, "wf-v1"),
        env().run(module_workflow_handler, json!({"order": 7})),
    )
    .await;

    let report = hosted.replay_check(native_pipeline_v1).await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "module-hosted history must replay clean under static code: {report:?}"
    );
}

#[tokio::test]
async fn a_statically_linked_history_replays_clean_under_module_hosting() {
    // The reverse direction, which is the one an operator actually depends on
    // during a swap: histories recorded by the old *binary* must replay under
    // the new *module*.
    let registry = registry_with(&[("wf-v1", "pipeline", pipeline_v1_bytes())]);
    let native = env().run(native_pipeline_v1, json!({"order": 7})).await;

    // Boxed: the replay future is large enough to trip `clippy::large_futures`
    // when combined with the host scope.
    let report = Box::pin(with_module_host(
        host(&registry, "wf-v1"),
        native.replay_check(module_workflow_handler),
    ))
    .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "static history must replay clean under module hosting: {report:?}"
    );
}

#[tokio::test]
async fn the_replayer_routes_a_module_hosted_history_by_name() {
    // The same proof through the public `WorkflowReplayer::register_fn` seam
    // rather than the harness's own `replay_check` shortcut.
    let registry = registry_with(&[("wf-v1", "pipeline", pipeline_v1_bytes())]);
    let hosted = with_module_host(
        host(&registry, "wf-v1"),
        env().run(module_workflow_handler, json!({"order": 7})),
    )
    .await;

    let replayer = WorkflowReplayer::new().register_fn("pipeline", native_pipeline_v1);
    let report = replayer.replay_from_events(hosted.events().to_vec()).await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "{report:?}"
    );
}

#[tokio::test]
async fn hosting_never_introduces_a_new_event_variant() {
    // AC4, stated as a machine-checkable claim: the set of event *kinds* a
    // module-hosted run produces is a subset of what the statically-linked run
    // produces. Nothing about module loading reaches history.
    let registry = registry_with(&[("wf-v2", "pipeline", pipeline_v2_bytes())]);
    let hosted = with_module_host(
        host(&registry, "wf-v2"),
        env().run(module_workflow_handler, json!({})),
    )
    .await;
    let native = env().run(native_pipeline_v2, json!({})).await;

    // `WorkflowEvent` is adjacently tagged, so the variant name is the `type`
    // field.
    let kinds = |events: &[WorkflowEvent]| -> Vec<String> {
        events
            .iter()
            .map(|e| {
                serde_json::to_value(e)
                    .expect("serialise")
                    .get("type")
                    .and_then(|t| t.as_str().map(str::to_string))
                    .unwrap_or_default()
            })
            .collect()
    };
    assert_eq!(kinds(hosted.events()), kinds(native.events()));
}

// ══ safety bounds ═════════════════════════════════════════════════════════════

#[tokio::test]
async fn a_trapping_guest_is_contained_as_a_workflow_error() {
    let bytes = wat::parse_str(TRAP_WAT).expect("trap wat assembles");
    let registry = registry_with(&[("wf-trap", "pipeline", bytes)]);
    let outcome = with_module_host(
        host(&registry, "wf-trap"),
        env().run(module_workflow_handler, json!({})),
    )
    .await;
    assert!(
        outcome.result.is_err(),
        "a guest trap must surface as an error, never unwind the worker"
    );
}

#[tokio::test]
async fn a_spinning_guest_is_bounded_by_fuel_and_the_epoch_deadline() {
    let bytes = wat::parse_str(SPIN_WAT).expect("spin wat assembles");
    let registry = registry_with(&[("wf-spin", "pipeline", bytes)]);
    let started = Instant::now();
    let outcome = with_module_host(
        host(&registry, "wf-spin"),
        env().run(module_workflow_handler, json!({})),
    )
    .await;
    assert!(
        outcome.result.is_err(),
        "an unbounded guest must be stopped"
    );
    assert!(
        started.elapsed() < Duration::from_secs(30),
        "the decide budget must bound a spinning guest, took {:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn a_guest_that_never_completes_is_stopped_by_the_decide_step_cap() {
    let bytes = wat::parse_str(NEVER_TERMINATES_WAT).expect("wat assembles");
    let registry = registry_with(&[("wf-loop", "pipeline", bytes)]);
    let outcome = with_module_host(
        host(&registry, "wf-loop"),
        env().run(module_workflow_handler, json!({})),
    )
    .await;
    let scheduled = outcome
        .events()
        .iter()
        .filter(|e| matches!(e, WorkflowEvent::ActivityScheduled { .. }))
        .count();
    let err = outcome
        .result
        .expect_err("a guest that asks for activities forever must be stopped");
    assert!(
        err.contains("ceiling") || err.contains("decide steps"),
        "the error must name the decide-step ceiling: {err}"
    );

    // ... and it must stop BEFORE running the activity it could not consume.
    // A cap that let the last permitted step schedule a real activity would
    // move money, send mail or ship a package and *then* fail the run for not
    // terminating, having paid for a side effect it can never use.
    assert!(
        scheduled < autumn_harvest::hot_swap::MAX_DECIDE_STEPS,
        "the ceiling must be checked before scheduling, got {scheduled} activities against a \
         {}-decision ceiling",
        autumn_harvest::hot_swap::MAX_DECIDE_STEPS
    );
}

#[tokio::test]
async fn a_guest_may_not_schedule_an_activity_the_host_did_not_allow() {
    // `Await` is the guest's one host-side capability, and deny-all
    // `WasmCapabilities` does not constrain it: the sandbox governs what the
    // guest may *import*, not what the host will do on its behalf. Without an
    // allowlist a module can schedule any activity the worker knows, read its
    // output, and hand it back out — exfiltration through the front door.
    let registry = registry_with(&[("wf-v1", "pipeline", pipeline_v1_bytes())]);
    let outcome = with_module_host(
        host(&registry, "wf-v1").allowing_activities(["notify"]),
        env().run(module_workflow_handler, json!({})),
    )
    .await;
    let scheduled_any = outcome
        .events()
        .iter()
        .any(|e| matches!(e, WorkflowEvent::ActivityScheduled { .. }));
    let err = outcome
        .result
        .expect_err("`charge` is not on the allowlist");
    assert!(
        err.contains("charge") && err.contains("does not allow"),
        "{err}"
    );
    assert!(
        !scheduled_any,
        "a disallowed activity must not be scheduled"
    );
}

#[tokio::test]
async fn a_guest_may_not_pick_the_queue_unless_the_host_allows_it() {
    // Letting a guest name any queue in the shard is lateral movement, not
    // configuration.
    let bytes = wat::parse_str(QUEUE_HOPPER_WAT).expect("wat assembles");
    let registry = registry_with(&[("wf-hop", "pipeline", bytes)]);

    let refused = with_module_host(
        host(&registry, "wf-hop"),
        env().run(module_workflow_handler, json!({})),
    )
    .await;
    let err = refused
        .result
        .expect_err("queue override is off by default");
    assert!(err.contains("override"), "{err}");

    // With the opt-in it is honoured, so the control is a policy rather than a
    // missing feature.
    let allowed = with_module_host(
        host(&registry, "wf-hop").allowing_queue_override(),
        env().run(module_workflow_handler, json!({})),
    )
    .await;
    assert_eq!(allowed.result, Ok(json!("v1-done")));
    let queues: Vec<&str> = allowed
        .events()
        .iter()
        .filter_map(|e| match e {
            WorkflowEvent::ActivityScheduled { queue, .. } => Some(queue.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(queues, ["other-queue"]);
}

#[test]
fn a_host_cannot_lift_the_resource_ceilings_only_tighten_them() {
    let registry = registry_with(&[("wf-v1", "pipeline", pipeline_v1_bytes())]);
    let lifted =
        host(&registry, "wf-v1").with_limits(autumn_harvest::wasm_activities::WasmLimits {
            memory_bytes: usize::MAX,
            fuel: u64::MAX,
            max_wall_clock: Duration::from_secs(3600),
        });
    assert_eq!(
        lifted.limits,
        autumn_harvest::hot_swap::default_decide_limits(),
        "an override must not raise a bound the safety analysis relies on"
    );
}

// ══ registry storage + hot swap (Postgres) ════════════════════════════════════

fn init_sql() -> Vec<u8> {
    autumn_harvest::test_init_sql().as_bytes().to_vec()
}

async fn setup_db() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(admin_url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        // Each test gets a throwaway database: the swap demonstration drives
        // queue-wide build policy, which is shard-wide state.
        let db_name = format!("harvest_hot_swap_{}", Uuid::new_v4().simple());
        let mut admin = <AsyncPgConnection as AsyncConnection>::establish(&admin_url)
            .await
            .expect("HARVEST_TEST_DATABASE_URL must be reachable");
        admin
            .batch_execute(&format!("CREATE DATABASE \"{db_name}\""))
            .await
            .expect("create throwaway database");
        let url = swap_database(&admin_url, &db_name);
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&url)
            .await
            .expect("connect to throwaway database");
        conn.batch_execute(autumn_harvest::full_migrations_sql())
            .await
            .expect("apply migrations");
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

/// Rewrite a Postgres URL's database path, dropping any query string.
///
/// Mirrors `codec_rotation_db_tests::swap_database` rather than pulling in a URL
/// parser the integration target does not otherwise need.
fn swap_database(url: &str, db_name: &str) -> String {
    let (base, _) = url.split_once('?').unwrap_or((url, ""));
    let cut = base.rfind('/').expect("a postgres URL has a database path");
    format!("{}/{db_name}", &base[..cut])
}

async fn db() -> (AsyncPgConnection, Option<ContainerAsync<Postgres>>) {
    let (url, container) = setup_db().await;
    let conn = <AsyncPgConnection as AsyncConnection>::establish(&url)
        .await
        .expect("connect");
    (conn, container)
}

#[tokio::test]
async fn publishing_a_module_round_trips_through_postgres() {
    let (mut conn, _c) = db().await;
    let bytes = pipeline_v1_bytes();
    let descriptor = publish_workflow_module(&mut conn, "wf-v1", "pipeline", &bytes, None, None)
        .await
        .expect("publish");
    assert_eq!(descriptor.module_hash, compute_module_hash(&bytes));

    let row = fetch_workflow_module(&mut conn, "wf-v1", "pipeline")
        .await
        .expect("fetch")
        .expect("row present");
    assert_eq!(row.module_bytes, bytes);
    assert_eq!(row.module_hash, descriptor.module_hash);
    assert!(row.signature.is_none());
}

#[tokio::test]
async fn a_build_ids_module_binding_is_immutable() {
    // The registry mirrors the start-time-immutable `assigned_build_id`
    // invariant: a build id names one exact module for a workflow, forever.
    // Rebinding it would silently change what already-assigned in-flight
    // executions run.
    let (mut conn, _c) = db().await;
    publish_workflow_module(
        &mut conn,
        "wf-v1",
        "pipeline",
        &pipeline_v1_bytes(),
        None,
        None,
    )
    .await
    .expect("publish v1");

    let err = publish_workflow_module(
        &mut conn,
        "wf-v1",
        "pipeline",
        &pipeline_v2_bytes(),
        None,
        None,
    )
    .await
    .expect_err("rebinding a published build id must be refused");
    assert!(
        err.to_string().contains("immutable") || err.to_string().contains("already"),
        "error should explain the immutability rule: {err}"
    );

    // Idempotent republish of identical bytes is fine.
    publish_workflow_module(
        &mut conn,
        "wf-v1",
        "pipeline",
        &pipeline_v1_bytes(),
        None,
        None,
    )
    .await
    .expect("republishing identical bytes is idempotent");
}

#[tokio::test]
async fn publishing_rejects_an_oversized_module_before_touching_the_database() {
    let (mut conn, _c) = db().await;
    let huge = vec![0u8; autumn_harvest::hot_swap::MAX_WORKFLOW_MODULE_BYTES + 1];
    publish_workflow_module(&mut conn, "wf-big", "pipeline", &huge, None, None)
        .await
        .expect_err("oversized publish must be refused");
    assert!(
        list_workflow_modules_for_build(&mut conn, "wf-big")
            .await
            .expect("list")
            .is_empty(),
        "a refused publish must leave no row"
    );
}

#[tokio::test]
async fn syncing_a_build_discovers_verifies_and_loads_every_module() {
    let (mut conn, _c) = db().await;
    let bytes = pipeline_v1_bytes();
    let signature = sign("wf-v1", "pipeline", &bytes);
    publish_workflow_module(
        &mut conn,
        "wf-v1",
        "pipeline",
        &bytes,
        Some(&signature),
        Some(OPERATOR_KEY),
    )
    .await
    .expect("publish");

    let registry = Arc::new(ModuleRegistry::new());
    let loaded = sync_build_into_registry(&mut conn, &registry, "wf-v1", Some(OPERATOR_KEY))
        .await
        .expect("sync");
    assert_eq!(loaded.len(), 1);
    assert!(registry.get("wf-v1", "pipeline").is_some());
}

#[tokio::test]
async fn syncing_refuses_a_module_whose_stored_bytes_were_tampered_with() {
    let (mut conn, _c) = db().await;
    let bytes = pipeline_v1_bytes();
    publish_workflow_module(&mut conn, "wf-v1", "pipeline", &bytes, None, None)
        .await
        .expect("publish");

    // Someone with write access to the table swaps the payload but leaves the
    // recorded content hash alone.
    diesel::sql_query(
        "UPDATE harvest_workflow_modules SET module_bytes = $1 WHERE build_id = 'wf-v1'",
    )
    .bind::<diesel::sql_types::Binary, _>(pipeline_v2_bytes())
    .execute(&mut conn)
    .await
    .expect("tamper");

    let registry = Arc::new(ModuleRegistry::new());
    sync_build_into_registry(&mut conn, &registry, "wf-v1", None)
        .await
        .expect_err("content verification must fail closed on tampered bytes");
    assert!(
        registry.is_empty(),
        "nothing may load from a failed verification"
    );
}

#[tokio::test]
async fn syncing_refuses_an_unsigned_module_when_a_signing_key_is_configured() {
    let (mut conn, _c) = db().await;
    publish_workflow_module(
        &mut conn,
        "wf-v1",
        "pipeline",
        &pipeline_v1_bytes(),
        None,
        None,
    )
    .await
    .expect("publish");

    let registry = Arc::new(ModuleRegistry::new());
    sync_build_into_registry(&mut conn, &registry, "wf-v1", Some(OPERATOR_KEY))
        .await
        .expect_err("an unsigned module must not load into a signing deployment");
    assert!(registry.is_empty());
}

#[tokio::test]
async fn retiring_a_build_hides_its_modules_from_every_read_path() {
    let (mut conn, _c) = db().await;
    publish_workflow_module(
        &mut conn,
        "wf-v1",
        "pipeline",
        &pipeline_v1_bytes(),
        None,
        None,
    )
    .await
    .expect("publish");
    assert_eq!(
        retire_build_modules(&mut conn, "wf-v1")
            .await
            .expect("retire"),
        1
    );
    assert!(
        fetch_workflow_module(&mut conn, "wf-v1", "pipeline")
            .await
            .expect("fetch")
            .is_none(),
        "a retired module must not be loadable"
    );
    assert!(
        list_workflow_modules_for_build(&mut conn, "wf-v1")
            .await
            .expect("list")
            .is_empty()
    );
}

#[tokio::test]
async fn a_retired_build_id_still_cannot_be_repointed_at_different_code() {
    // Retirement is soft precisely so it cannot launder the immutability
    // guarantee. A hard DELETE would free the primary key and let `wf-v1` be
    // republished with new bytes — and an execution still parked on a long
    // timer under `wf-v1` would resume on logic it never started under.
    let (mut conn, _c) = db().await;
    publish_workflow_module(
        &mut conn,
        "wf-v1",
        "pipeline",
        &pipeline_v1_bytes(),
        None,
        None,
    )
    .await
    .expect("publish v1");
    retire_build_modules(&mut conn, "wf-v1")
        .await
        .expect("retire");

    let err = publish_workflow_module(
        &mut conn,
        "wf-v1",
        "pipeline",
        &pipeline_v2_bytes(),
        None,
        None,
    )
    .await
    .expect_err("a retired build id must not be re-pointed at different bytes");
    assert!(
        err.to_string().contains("immutable"),
        "the error should explain the immutability rule: {err}"
    );
}

#[tokio::test]
async fn syncing_a_build_with_no_modules_is_an_error_not_a_silent_success() {
    // A typo'd build id, a not-yet-published build, and — in a sharded
    // deployment — a publish that landed on another shard's database all look
    // identical here. A silent `Ok(vec![])` defers the symptom to executions
    // failing one by one for want of a module, long after the cause is visible.
    let (mut conn, _c) = db().await;
    let registry = Arc::new(ModuleRegistry::new());
    let err = sync_build_into_registry(&mut conn, &registry, "wf-never-published", None)
        .await
        .expect_err("an empty build must not report success");
    assert!(
        err.to_string().contains("registers no workflow modules"),
        "{err}"
    );
}

#[tokio::test]
async fn a_failing_module_leaves_the_whole_build_unbound() {
    // The fail-closed property that matters, and the one a per-module loop
    // cannot give: a build whose *second* module fails verification must not
    // leave the first bound. A worker advertising a build it can only
    // half-serve claims executions for the workflow it has and destroys every
    // execution for the one it does not.
    let (mut conn, _c) = db().await;
    publish_workflow_module(
        &mut conn,
        "wf-v1",
        "alpha",
        &pipeline_v1_bytes(),
        None,
        None,
    )
    .await
    .expect("publish alpha");
    publish_workflow_module(&mut conn, "wf-v1", "zeta", &pipeline_v2_bytes(), None, None)
        .await
        .expect("publish zeta");

    // Tamper with the second module only. `alpha` sorts first, so a naive loop
    // would have bound it before reaching `zeta`.
    diesel::sql_query(
        "UPDATE harvest_workflow_modules SET module_bytes = $1 \
         WHERE build_id = 'wf-v1' AND workflow_name = 'zeta'",
    )
    .bind::<diesel::sql_types::Binary, _>(pipeline_v1_bytes())
    .execute(&mut conn)
    .await
    .expect("tamper");

    let registry = Arc::new(ModuleRegistry::new());
    sync_build_into_registry(&mut conn, &registry, "wf-v1", None)
        .await
        .expect_err("the tampered module must fail the sync");
    assert!(
        registry.is_empty(),
        "no module from a failed build may be bound, including the ones that verified: {:?}",
        registry.descriptors()
    );
}

#[tokio::test]
async fn publishing_verifies_the_signature_it_is_asked_to_store() {
    // A bad signature stored unchecked is a fleet-wide outage deferred until
    // the next sync: every worker refuses the build, and the publisher is long
    // gone. Verify at the point the mistake is made.
    let (mut conn, _c) = db().await;
    let bytes = pipeline_v1_bytes();
    let wrong = sign_module_binding(
        ATTACKER_KEY,
        "wf-v1",
        "pipeline",
        &compute_module_hash(&bytes),
    )
    .expect("sign");
    publish_workflow_module(
        &mut conn,
        "wf-v1",
        "pipeline",
        &bytes,
        Some(&wrong),
        Some(OPERATOR_KEY),
    )
    .await
    .expect_err("a signature that does not verify must be refused at publish time");
    assert!(
        list_workflow_modules_for_build(&mut conn, "wf-v1")
            .await
            .expect("list")
            .is_empty(),
        "a refused publish must leave no row"
    );
}

#[tokio::test]
async fn signing_can_be_introduced_and_rotated_on_an_existing_build() {
    // Codex review round 4. The identical-bytes republish path cleared
    // `retired_at` and left the row's OLD (or NULL) signature in place. So a
    // republish carrying a signature valid under a NEW key returned success and
    // then made the next sync with that key reject the very row it had just
    // accepted — introducing or rotating signing would have required minting a
    // new build id, i.e. a deploy, which is exactly the coupling this design
    // exists to remove.
    let (mut conn, _c) = db().await;
    let bytes = pipeline_v1_bytes();
    let hash = compute_module_hash(&bytes);

    // Published unsigned first: the pre-signing state of an existing build.
    publish_workflow_module(&mut conn, "wf-v1", "pipeline", &bytes, None, None)
        .await
        .expect("unsigned publish");
    let registry = Arc::new(ModuleRegistry::new());
    sync_build_into_registry(&mut conn, &registry, "wf-v1", Some(OPERATOR_KEY))
        .await
        .expect_err("an unsigned row must not satisfy a key-configured sync");

    // Introduce signing by republishing the SAME bytes with a signature.
    let signature = sign_module_binding(OPERATOR_KEY, "wf-v1", "pipeline", &hash).expect("sign");
    publish_workflow_module(
        &mut conn,
        "wf-v1",
        "pipeline",
        &bytes,
        Some(&signature),
        Some(OPERATOR_KEY),
    )
    .await
    .expect("republishing identical bytes with a signature is the rotation path");

    let registry = Arc::new(ModuleRegistry::new());
    sync_build_into_registry(&mut conn, &registry, "wf-v1", Some(OPERATOR_KEY))
        .await
        .expect("the stored signature must now satisfy the sync");
    assert!(registry.get("wf-v1", "pipeline").is_some());

    // And rotate to a second key the same way.
    let rotated = sign_module_binding(ATTACKER_KEY, "wf-v1", "pipeline", &hash).expect("sign");
    publish_workflow_module(
        &mut conn,
        "wf-v1",
        "pipeline",
        &bytes,
        Some(&rotated),
        Some(ATTACKER_KEY),
    )
    .await
    .expect("rotation republish");

    let registry = Arc::new(ModuleRegistry::new());
    sync_build_into_registry(&mut conn, &registry, "wf-v1", Some(ATTACKER_KEY))
        .await
        .expect("the rotated signature must satisfy a sync under the new key");
    let registry = Arc::new(ModuleRegistry::new());
    sync_build_into_registry(&mut conn, &registry, "wf-v1", Some(OPERATOR_KEY))
        .await
        .expect_err("the superseded key must no longer verify");
}

#[tokio::test]
async fn an_unsigned_republish_does_not_erase_an_existing_signature() {
    // Codex review round 5, correcting round 4. Writing the signature
    // unconditionally meant a publisher supplying none would NULL a valid one —
    // so mid-rollout, an older or unsigned publisher re-seeding the same build
    // would turn a harmless duplicate publish into a fleet-wide refusal, every
    // worker syncing with the key rejecting a row that was correctly signed a
    // moment earlier. An explicit (verified) signature replaces; `None` leaves
    // the stored attestation alone.
    let (mut conn, _c) = db().await;
    let bytes = pipeline_v1_bytes();
    let signature = sign_module_binding(
        OPERATOR_KEY,
        "wf-v1",
        "pipeline",
        &compute_module_hash(&bytes),
    )
    .expect("sign");

    publish_workflow_module(
        &mut conn,
        "wf-v1",
        "pipeline",
        &bytes,
        Some(&signature),
        Some(OPERATOR_KEY),
    )
    .await
    .expect("signed publish");

    // The unsigned re-seed: identical bytes, no signature, no key.
    publish_workflow_module(&mut conn, "wf-v1", "pipeline", &bytes, None, None)
        .await
        .expect("an idempotent re-seed must still succeed");

    assert_eq!(
        fetch_workflow_module(&mut conn, "wf-v1", "pipeline")
            .await
            .expect("fetch")
            .expect("row")
            .signature
            .as_deref(),
        Some(signature.as_str()),
        "the stored signature must survive a republish that supplies none"
    );

    let registry = Arc::new(ModuleRegistry::new());
    sync_build_into_registry(&mut conn, &registry, "wf-v1", Some(OPERATOR_KEY))
        .await
        .expect("a key-configured sync must still accept the build");
}

// ── the AC3 end-to-end demonstration ──────────────────────────────────────────

/// Publish v2 under a new build id while the process is running, ramp new
/// starts onto it with the **shipped** `set_build_ramp` API, prove a
/// v1-assigned in-flight execution still runs v1 code, then roll back by
/// repointing the policy — all without restarting the host.
#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "one end-to-end narrative — load v1, hot-load v2, ramp, check the \
              in-flight run, roll back. Splitting it would hide that all of it \
              happens without a restart, which is the acceptance criterion."
)]
async fn hot_swap_ramp_and_rollback_without_a_restart() {
    let (mut conn, _c) = db().await;
    let registry = Arc::new(ModuleRegistry::new());

    // ── (a) the process loads its v1 module at startup ───────────────────────
    publish_workflow_module(
        &mut conn,
        "wf-v1",
        "pipeline",
        &pipeline_v1_bytes(),
        None,
        None,
    )
    .await
    .expect("publish v1");
    sync_build_into_registry(&mut conn, &registry, "wf-v1", None)
        .await
        .expect("initial load");
    set_build_policy(&mut conn, "default", "wf-v1", Some("hot-swap-demo"))
        .await
        .expect("policy");

    // A v1-assigned execution is started and is now "in flight".
    let in_flight = ExecutionId::new();
    let policy_v1 = autumn_harvest::build_routing::get_build_policy(&mut conn, "default")
        .await
        .expect("get policy")
        .expect("policy exists");
    let in_flight_build = policy_v1.resolve_assigned_build(in_flight);
    assert_eq!(in_flight_build, "wf-v1");

    // ── (b) hot-load v2 under a new build id, no restart ─────────────────────
    let swap_started = Instant::now();
    publish_workflow_module(
        &mut conn,
        "wf-v2",
        "pipeline",
        &pipeline_v2_bytes(),
        None,
        None,
    )
    .await
    .expect("publish v2");
    sync_build_into_registry(&mut conn, &registry, "wf-v2", None)
        .await
        .expect("hot load v2");
    // One worker process, one host build id, now hosting both module versions.
    let mut compat = BuildCompatibilitySet::new();
    declare_compat(&mut conn, "host-1", "wf-v1")
        .await
        .expect("compat v1");
    declare_compat(&mut conn, "host-1", "wf-v2")
        .await
        .expect("compat v2");
    compat.add_declaration("host-1", "wf-v1");
    compat.add_declaration("host-1", "wf-v2");
    assert!(compat.is_eligible("host-1", Some("wf-v1")));
    assert!(compat.is_eligible("host-1", Some("wf-v2")));

    // ── (c) ramp new starts to v2 with the shipped API ───────────────────────
    set_build_ramp(&mut conn, "default", "wf-v2", 100)
        .await
        .expect("ramp to v2");
    let ramped = autumn_harvest::build_routing::get_build_policy(&mut conn, "default")
        .await
        .expect("get policy")
        .expect("policy exists");
    let fresh = ExecutionId::new();
    assert_eq!(ramped.resolve_assigned_build(fresh), "wf-v2");

    // The new start really executes the v2 module...
    let v2_run = with_module_host(
        host(&registry, &ramped.resolve_assigned_build(fresh)),
        env()
            .with_build_id("host-1")
            .run(module_workflow_handler, json!({})),
    )
    .await;
    assert_eq!(v2_run.result, Ok(json!("v2-done")));
    let swap_latency = swap_started.elapsed();

    // ... while the v1-assigned in-flight execution still runs v1 code.
    let v1_run = with_module_host(
        host(&registry, &in_flight_build),
        env()
            .with_build_id("host-1")
            .run(module_workflow_handler, json!({})),
    )
    .await;
    assert_eq!(
        v1_run.result,
        Ok(json!("v1-done")),
        "an in-flight v1 execution must not be dragged onto v2 code"
    );
    // ... with zero replay divergence against the v1 logic it started under.
    let v1_replay = v1_run.replay_check(native_pipeline_v1).await;
    assert!(
        matches!(v1_replay.status, ReplayStatus::ReplaySucceeded),
        "{v1_replay:?}"
    );

    // ── (d) rollback by repointing the registry ──────────────────────────────
    let rollback_started = Instant::now();
    clear_build_ramp(&mut conn, "default")
        .await
        .expect("rollback");
    let rolled_back = autumn_harvest::build_routing::get_build_policy(&mut conn, "default")
        .await
        .expect("get policy")
        .expect("policy exists");
    let after_rollback = ExecutionId::new();
    assert_eq!(
        rolled_back.resolve_assigned_build(after_rollback),
        "wf-v1",
        "clearing the ramp sends new starts back to v1"
    );
    let rollback_latency = rollback_started.elapsed();

    // ── success metric ───────────────────────────────────────────────────────
    assert!(
        swap_latency < Duration::from_secs(10),
        "publish -> first v2-assigned execution running must be under 10s, was {swap_latency:?}"
    );
    assert!(
        rollback_latency < Duration::from_secs(5),
        "rollback must take effect for new starts under 5s, was {rollback_latency:?}"
    );
}

#[tokio::test]
async fn a_partial_ramp_splits_new_starts_across_both_hosted_modules() {
    // The ramp is a pure function of the execution id, so a 50% ramp must send
    // roughly half of new starts to each module — and each must run the code
    // its build id names.
    let (mut conn, _c) = db().await;
    let registry = Arc::new(ModuleRegistry::new());
    publish_workflow_module(
        &mut conn,
        "wf-v1",
        "pipeline",
        &pipeline_v1_bytes(),
        None,
        None,
    )
    .await
    .expect("publish v1");
    publish_workflow_module(
        &mut conn,
        "wf-v2",
        "pipeline",
        &pipeline_v2_bytes(),
        None,
        None,
    )
    .await
    .expect("publish v2");
    sync_build_into_registry(&mut conn, &registry, "wf-v1", None)
        .await
        .expect("load v1");
    sync_build_into_registry(&mut conn, &registry, "wf-v2", None)
        .await
        .expect("load v2");
    set_build_policy(&mut conn, "default", "wf-v1", None)
        .await
        .expect("policy");
    set_build_ramp(&mut conn, "default", "wf-v2", 50)
        .await
        .expect("ramp 50%");

    let policy: BuildPolicy = autumn_harvest::build_routing::get_build_policy(&mut conn, "default")
        .await
        .expect("get policy")
        .expect("policy exists");

    let mut v1 = 0usize;
    let mut v2 = 0usize;
    for _ in 0..200 {
        match policy.resolve_assigned_build(ExecutionId::new()).as_str() {
            "wf-v1" => v1 += 1,
            "wf-v2" => v2 += 1,
            other => panic!("unexpected build {other}"),
        }
    }
    assert!(
        v1 > 40 && v2 > 40,
        "a 50% ramp should split traffic: {v1}/{v2}"
    );

    // Both halves resolve to a loaded module.
    assert!(registry.get("wf-v1", "pipeline").is_some());
    assert!(registry.get("wf-v2", "pipeline").is_some());
}

// ── the AC3 demonstration, inside one real worker process ─────────────────────

/// Everything the AC3 demonstration above proves, but driven through a **real
/// `Worker`** claiming real tasks off the real queue — so "one worker process"
/// is literal rather than a figure of speech.
///
/// The worker is started once, at the top, and never restarted or reconfigured.
/// Every subsequent step (publishing v2, syncing it into the live registry,
/// ramping, rolling back) happens while it is polling.
mod one_worker_process {
    use super::{
        Arc, Duration, Instant, Postgres, Uuid, anchor, pipeline_v1_bytes, pipeline_v2_bytes,
    };
    use autumn_harvest::build_routing::{
        clear_build_ramp, declare_compat, get_build_policy, set_build_policy, set_build_ramp,
    };
    use autumn_harvest::context::ActivityContext;
    use autumn_harvest::context::empty_shared_state;
    use autumn_harvest::event::WorkflowEvent;
    use autumn_harvest::hot_swap::{ModuleRegistry, module_workflow_handler};
    use autumn_harvest::hot_swap_store::{publish_workflow_module, sync_build_into_registry};
    use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
    use autumn_harvest::models::{NewWorkflowExecution, WorkflowExecution};
    use autumn_harvest::queue::{self, EnqueueParams, TaskType};
    use autumn_harvest::schema::harvest_workflow_executions;
    use autumn_harvest::store;
    use autumn_harvest::telemetry::TelemetryConfig;
    use autumn_harvest::types::{ExecutionId, ShardId};
    use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
    use diesel::prelude::*;
    use diesel_async::pooled_connection::AsyncDieselConnectionManager;
    use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
    use serde_json::{Value, json};
    use std::collections::HashMap;
    use testcontainers::ContainerAsync;

    type ActFut<'a> =
        std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>>;

    fn charge(_ctx: &ActivityContext, _input: Value) -> ActFut<'_> {
        Box::pin(async move { Ok(json!({"ok": true})) })
    }

    fn notify(_ctx: &ActivityContext, _input: Value) -> ActFut<'_> {
        Box::pin(async move { Ok(json!({"sent": true})) })
    }

    fn act(name: &'static str, handler: autumn_harvest::info::ActivityHandlerFn) -> ActivityInfo {
        ActivityInfo {
            name,
            module: "hot_code_swap_tests",
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
            rate_limit_key_expr: None,
            circuit_breaker: None,
            is_local: false,
            max_input_bytes: None,
            max_result_bytes: None,
            requires: None,
            handler,
        }
    }

    /// A `WorkflowInfo` whose body comes from a runtime module: its handler is
    /// the statically-linked trampoline, and nothing else about the registration
    /// differs from a `#[workflow]`-generated one.
    fn module_hosted_workflow(name: &'static str) -> WorkflowInfo {
        WorkflowInfo {
            quota: None,
            declared_activities: None,
            declared_children: None,
            mcp: false,
            name,
            module: "hot_code_swap_tests",
            handler: module_workflow_handler,
            execution_timeout: None,
            chain_execution_timeout: None,
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

    fn build_pool(url: &str) -> DbPool {
        let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
        deadpool::managed::Pool::builder(manager)
            .max_size(8)
            .build()
            .expect("pool build failed")
    }

    async fn connect(url: &str) -> AsyncPgConnection {
        <AsyncPgConnection as AsyncConnection>::establish(url)
            .await
            .expect("connect")
    }

    fn build_worker(queue: &str, registry: Arc<HandlerRegistry>) -> Arc<Worker> {
        Arc::new(
            Worker::new(
                WorkerRuntimeConfig {
                    codec_rotation_batch_size: 0,
                    dr_fencing: false,
                    worker_id: "hot-swap-host-1".to_string(),
                    queues: vec![queue.to_string()],
                    notification_database_url: None,
                    max_concurrent_workflows: 2,
                    max_concurrent_activities: 2,
                    poll_interval: Duration::from_millis(25),
                    shutdown_timeout: Duration::from_secs(2),
                    cancellation_grace_period: Duration::from_secs(1),
                    sticky_timeout: Duration::from_secs(5),
                    max_local_activity_start_to_close: Duration::from_secs(60),
                    shard_assignments: vec![ShardId::new(0)],
                    worker_heartbeat_interval: Duration::from_secs(5),
                    // The worker advertises ONE build identity and hosts both
                    // module versions; compatibility declarations (below) are
                    // what let it claim tasks for either.
                    build_id: "host-1".to_string(),
                    deployment_name: None,
                    workflow_cache_size: 1000,
                    priority_aging_secs: None,
                    unknown_target_grace_window: Duration::from_secs(5),
                    poison_pill_threshold: 3,
                    capability_miss_max_redeliveries: 5,
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

    /// Seed an execution already assigned to `build_id` and enqueue its first
    /// workflow task with the matching `required_build_id`, exactly as
    /// `start_workflow` would after `BuildPolicy::resolve_assigned_build`.
    async fn seed(
        conn: &mut AsyncPgConnection,
        queue: &str,
        build_id: &str,
        input: Value,
    ) -> ExecutionId {
        let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
        let row = NewWorkflowExecution {
            quota_key: None,
            id: exec_id.as_uuid(),
            workflow_name: "pipeline",
            workflow_id: &format!("wf-{}", exec_id.as_uuid()),
            run_id: Uuid::new_v4(),
            shard_id: 0,
            input: input.clone(),
            parent_id: None,
            queue_name: queue,
            execution_timeout: None,
            deadline_at: None,
            chain_execution_timeout: None,
            chain_deadline_at: None,
            memo: None,
            search_attrs: None,
            assigned_build_id: Some(build_id.to_string()),
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
            start_source: None,
            start_source_ref: None,
            started_by: None,
        };
        diesel::insert_into(harvest_workflow_executions::table)
            .values(&row)
            .execute(conn)
            .await
            .expect("insert execution");
        store::append_events(
            conn,
            exec_id,
            &[WorkflowEvent::workflow_started(input.clone(), anchor())],
            0,
        )
        .await
        .expect("append WorkflowStarted");
        let mut params = EnqueueParams::new(queue, TaskType::Workflow, input);
        params.workflow_exec_id = Some(exec_id.as_uuid());
        params.required_build_id = Some(build_id.to_string());
        params.scheduled_at = chrono::Utc::now() - chrono::Duration::seconds(5);
        queue::enqueue(conn, &params).await.expect("enqueue");
        exec_id
    }

    async fn wait_completed(
        url: &str,
        exec_id: ExecutionId,
        budget: Duration,
    ) -> WorkflowExecution {
        tokio::time::timeout(budget, async {
            loop {
                let mut conn = connect(url).await;
                let e: WorkflowExecution = harvest_workflow_executions::table
                    .find(exec_id.as_uuid())
                    .select(WorkflowExecution::as_select())
                    .first(&mut conn)
                    .await
                    .expect("reload execution");
                if e.state == "COMPLETED" || e.state == "FAILED" {
                    break e;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("execution {exec_id:?} did not settle within {budget:?}"))
    }

    async fn setup_db() -> (String, Option<ContainerAsync<Postgres>>) {
        super::setup_db().await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[allow(
        clippy::too_many_lines,
        reason = "one end-to-end narrative: start a \
        worker, hot-load v2, ramp, check the in-flight straggler, roll back. \
        Splitting it would hide the fact that ONE worker process does all of it, \
        which is the acceptance criterion."
    )]
    async fn a_running_worker_adopts_v2_under_a_new_build_id_without_restarting() {
        let (url, _container) = setup_db().await;
        let queue = "q-hot-swap";
        let pool = build_pool(&url);
        let mut conn = connect(&url).await;

        // ── (a) the process loads its v1 module at startup ───────────────────
        let modules = Arc::new(ModuleRegistry::new());
        publish_workflow_module(
            &mut conn,
            "wf-v1",
            "pipeline",
            &pipeline_v1_bytes(),
            None,
            None,
        )
        .await
        .expect("publish v1");
        sync_build_into_registry(&mut conn, &modules, "wf-v1", None)
            .await
            .expect("startup load");
        set_build_policy(&mut conn, queue, "wf-v1", Some("hot-swap-demo"))
            .await
            .expect("policy");
        // One worker identity, hosting whichever module versions it has loaded.
        declare_compat(&mut conn, "host-1", "wf-v1")
            .await
            .expect("compat v1");

        let registry = Arc::new(
            HandlerRegistry::with_state_and_telemetry(
                vec![module_hosted_workflow("pipeline")],
                vec![act("charge", charge), act("notify", notify)],
                empty_shared_state(),
                Arc::new(TelemetryConfig::builder().build()),
            )
            .with_module_registry(Arc::clone(&modules)),
        );
        let worker = build_worker(queue, registry);

        // The worker starts ONCE here and is never restarted below.
        let runner = Arc::clone(&worker);
        let pool_for_run = pool.clone();
        let handle = tokio::spawn(async move { runner.run(&pool_for_run).await });

        let v1_exec = seed(&mut conn, queue, "wf-v1", json!({"order": 1})).await;
        let v1 = wait_completed(&url, v1_exec, Duration::from_secs(30)).await;
        assert_eq!(v1.state, "COMPLETED", "v1 run must complete: {v1:?}");
        assert_eq!(v1.output, Some(json!("v1-done")));

        // ── (b) hot-load v2 under a new build id, with the worker running ────
        let swap_started = Instant::now();
        publish_workflow_module(
            &mut conn,
            "wf-v2",
            "pipeline",
            &pipeline_v2_bytes(),
            None,
            None,
        )
        .await
        .expect("publish v2");
        sync_build_into_registry(&mut conn, &modules, "wf-v2", None)
            .await
            .expect("hot load v2");
        declare_compat(&mut conn, "host-1", "wf-v2")
            .await
            .expect("compat v2");

        // ── (c) ramp new starts to v2 with the shipped API ───────────────────
        set_build_ramp(&mut conn, queue, "wf-v2", 100)
            .await
            .expect("ramp to v2");
        let policy = get_build_policy(&mut conn, queue)
            .await
            .expect("get policy")
            .expect("policy exists");
        let v2_exec_id = ExecutionId::new_for_shard(ShardId::new(0));
        assert_eq!(
            policy.resolve_assigned_build(v2_exec_id),
            "wf-v2",
            "a full ramp must send new starts to the swapped build"
        );

        let v2_exec = seed(&mut conn, queue, "wf-v2", json!({"order": 2})).await;
        let v2 = wait_completed(&url, v2_exec, Duration::from_secs(30)).await;
        let swap_latency = swap_started.elapsed();
        assert_eq!(v2.state, "COMPLETED", "v2 run must complete: {v2:?}");
        assert_eq!(
            v2.output,
            Some(json!("v2-done")),
            "the SAME worker process, not restarted, must now run v2 code"
        );

        // The v2 history carries the extra activity v2 added...
        let mut hist_conn = connect(&url).await;
        let v2_history = store::load_history(&mut hist_conn, v2_exec)
            .await
            .expect("load v2 history")
            .events;
        let v2_scheduled: Vec<&str> = v2_history
            .iter()
            .filter_map(|e| match e {
                WorkflowEvent::ActivityScheduled { name, .. } => Some(name.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(v2_scheduled, ["charge", "notify"]);

        // ── (c') a v1-assigned execution started AFTER the swap still runs v1 ─
        // This is the in-flight guarantee: the build id is fixed at start time,
        // so the code it denotes must be too.
        let straggler = seed(&mut conn, queue, "wf-v1", json!({"order": 3})).await;
        let straggler = wait_completed(&url, straggler, Duration::from_secs(30)).await;
        assert_eq!(
            straggler.output,
            Some(json!("v1-done")),
            "a v1-assigned execution must keep running v1 code after the swap"
        );

        // ── (d) rollback by repointing the ramp ──────────────────────────────
        let rollback_started = Instant::now();
        clear_build_ramp(&mut conn, queue).await.expect("rollback");
        let rolled_back = get_build_policy(&mut conn, queue)
            .await
            .expect("get policy")
            .expect("policy exists");
        assert_eq!(
            rolled_back.resolve_assigned_build(ExecutionId::new_for_shard(ShardId::new(0))),
            "wf-v1",
            "clearing the ramp sends new starts back to v1"
        );
        let rollback_latency = rollback_started.elapsed();

        let after_rollback = seed(&mut conn, queue, "wf-v1", json!({"order": 4})).await;
        let after_rollback = wait_completed(&url, after_rollback, Duration::from_secs(30)).await;
        assert_eq!(after_rollback.output, Some(json!("v1-done")));

        worker.shutdown();
        handle.await.expect("worker joins cleanly");

        // ── the issue's success metric ───────────────────────────────────────
        assert!(
            swap_latency < Duration::from_secs(10),
            "publish -> first v2-assigned execution running must be under 10s, was \
             {swap_latency:?}"
        );
        assert!(
            rollback_latency < Duration::from_secs(5),
            "rollback must take effect for new starts under 5s, was {rollback_latency:?}"
        );
    }
}

// ══ Codex review round 1 regressions ══════════════════════════════════════════
//
// One test per finding, each written to fail against the code as reviewed.

#[tokio::test]
async fn the_activity_allowlist_survives_the_worker_dispatch_seam() {
    // P1. `HandlerRegistry` stored only the `ModuleRegistry`, so the dispatch
    // seam built a fresh `ModuleHost::new` per task — whose `allowed_activities`
    // is `None`. Every restriction an operator configured through
    // `allowing_activities` was therefore discarded on the one path that
    // matters, and production guests could schedule any activity the worker
    // knew. The registry now stores the whole policy; this pins that the
    // allowlist is what dispatch reads back.
    let registry = registry_with(&[("wf-v1", "pipeline", pipeline_v1_bytes())]);
    let configured = ModuleHost::new(Arc::clone(&registry))
        .allowing_activities(["refund"])
        .allowing_queue_override();

    let handlers =
        autumn_harvest::worker::HandlerRegistry::new(vec![], vec![]).with_module_host(configured);

    let policy = handlers
        .module_host()
        .expect("module hosting is configured on the registry");
    assert_eq!(
        policy
            .allowed_activities
            .as_ref()
            .expect("the allowlist reaches dispatch")
            .iter()
            .collect::<Vec<_>>(),
        vec!["refund"],
        "dispatch must read the operator's allowlist, not a default one"
    );
    assert!(
        policy.allow_queue_override,
        "dispatch must read the operator's queue-override switch"
    );
    assert!(
        handlers.module_registry().is_some(),
        "the registry is still reachable through the stored policy"
    );

    // `with_module_registry` remains the unrestricted shorthand, and says so.
    let defaulted = autumn_harvest::worker::HandlerRegistry::new(vec![], vec![])
        .with_module_registry(Arc::clone(&registry));
    let defaulted = defaulted.module_host().expect("configured");
    assert!(defaulted.allowed_activities.is_none());
    assert!(!defaulted.allow_queue_override);
}

#[tokio::test]
async fn a_pinned_module_survives_an_unload_that_races_the_dispatch() {
    // P1. The worker seam resolves the module before dispatch so a miss is a
    // typed capability miss rather than a terminal failure. It then dropped the
    // `Arc` and let the trampoline look the binding up a *second* time — so an
    // `unload_build` landing between the two turned a passed capability check
    // into `Err(String)`, i.e. exactly the terminal `WorkflowFailed` the
    // pre-check exists to avoid, and contradicted the safety analysis's claim
    // that unloading is safe for in-flight invocations.
    let registry = registry_with(&[("wf-v1", "pipeline", pipeline_v1_bytes())]);
    let pinned = registry
        .get("wf-v1", "pipeline")
        .expect("dispatch resolves the module");

    // The race: the binding is gone before the handler ever runs.
    assert_eq!(registry.unload_build("wf-v1"), 1);
    assert!(registry.get("wf-v1", "pipeline").is_none());

    let outcome = with_module_host(
        host(&registry, "wf-v1").with_pinned_module(pinned),
        env().run(module_workflow_handler, json!({"order": 1})),
    )
    .await;

    assert_eq!(
        outcome.result.expect("the pinned module still runs"),
        json!("v1-done"),
        "an unload must not destroy an execution that already resolved its module"
    );
}

#[tokio::test]
async fn an_unpinned_host_still_reports_a_missing_module() {
    // The fallback path is for direct/embedder use and must keep failing loudly
    // rather than silently running nothing.
    let registry = registry_with(&[("wf-v1", "pipeline", pipeline_v1_bytes())]);
    assert_eq!(registry.unload_build("wf-v1"), 1);

    let outcome = with_module_host(
        host(&registry, "wf-v1"),
        env().run(module_workflow_handler, json!({})),
    )
    .await;
    let err = outcome.result.expect_err("no module is loaded");
    assert!(
        err.contains("no workflow module is loaded"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn the_trampoline_never_yields_between_decisions() {
    // P1, and the subtlest of the three. `executor::run_workflow_handler_cycle`
    // drives the handler inside `tokio::time::timeout(SUSPENSION_TIMEOUT)` —
    // 100ms — and a workflow that returns `Poll::Pending` is BY DEFINITION
    // treated as suspended. A `yield_now()` between guest decisions was
    // therefore the one point at which that timer could fire while the
    // trampoline had not yet recorded a command, yielding a zero-command
    // suspension: a workflow parked on nothing, which the worker fails
    // terminally.
    //
    // The invariant that replaces it: the trampoline's ONLY await is
    // `execute_activity_raw`, which pushes its `ScheduleActivity` command before
    // parking. Every suspension a hosted workflow can produce therefore carries
    // a command, exactly as a statically-linked one's does. This test pins the
    // source, because the failure it guards against is a timing race that a
    // functional test reproduces only flakily.
    let src = include_str!("../../src/hot_swap.rs");
    let handler_start = src
        .find("pub fn module_workflow_handler(")
        .expect("the trampoline is where the guard expects it");
    let body = &src[handler_start..];
    let live: String = body
        .lines()
        .filter(|line| !line.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        !live.contains("yield_now"),
        "the trampoline must not yield between decisions: a `Poll::Pending` \
         without a recorded command is read as a zero-command suspension and \
         fails the workflow terminally"
    );

    // And the guest still completes when its decisions are the slow part.
    let outcome = with_module_host(
        host(
            &registry_with(&[("wf-v1", "pipeline", pipeline_v1_bytes())]),
            "wf-v1",
        ),
        env().run(module_workflow_handler, json!({"order": 1})),
    )
    .await;
    assert_eq!(outcome.result.expect("completes"), json!("v1-done"));
}

#[tokio::test]
async fn decisions_are_reused_across_separate_workflow_tasks() {
    // Codex review round 2 corrected my round-1 fix, and the reason my own test
    // missed it is worth stating: it asserted the cache had ENTRIES, not that it
    // ever produced a HIT. Insertions prove nothing — the broken version
    // inserted on every step too.
    //
    // The substance: an ordinary activity ENDS the workflow task. Only local
    // activities re-drive in place, so a hosted workflow resumes in a NEW
    // `process_workflow_task` call, and a cache installed per task is reset on
    // every durable cycle — leaving the O(n^2) exactly where it was. The cache
    // therefore lives on the registry, for the worker process.
    let registry = registry_with(&[("wf-v2", "pipeline", pipeline_v2_bytes())]);

    // Two independent drives, as two separate tasks would be.
    for _ in 0..2 {
        let outcome = with_module_host(
            ModuleHost::new(Arc::clone(&registry)).with_build_id("wf-v2"),
            env().run(module_workflow_handler, json!({"order": 1})),
        )
        .await;
        assert_eq!(outcome.result.expect("completes"), json!("v2-done"));
    }

    let (hits, misses) = registry.decisions().expect("cache is not poisoned").stats();
    assert!(
        hits > 0,
        "the second drive must reuse the first's decisions; got {hits} hits / {misses} misses"
    );
    assert!(
        misses > 0,
        "the first drive must have populated the cache ({misses} misses expected)"
    );
}

#[test]
fn the_run_budget_is_charged_and_checked_once_for_both_cache_paths() {
    // Codex review round 5, correcting round 4. Charging cache hits was the
    // right fix; keeping a *separate* check on each path was not. The miss path
    // checked before computing and never re-checked after adding the cost, so a
    // fresh decision that pushed the run over budget was accepted while the same
    // total served from cache was rejected — the residency dependence surviving
    // inside its own fix.
    //
    // Guarded structurally rather than functionally: reproducing it needs a
    // guest slow enough to exhaust a ten-second budget, which is not a test
    // anyone should wait for. The invariant is that the cost is charged and the
    // budget checked in exactly ONE place, on the path both branches join, and
    // *before* the response is acted on — an over-budget `Await` acted on
    // optimistically schedules a real activity the run then fails immediately
    // after.
    let src = include_str!("../../src/hot_swap.rs");
    let start = src
        .find("pub fn module_workflow_handler(")
        .expect("the trampoline is where the guard expects it");
    let body = &src[start..];
    let live: Vec<&str> = body
        .lines()
        .filter(|line| !line.trim_start().starts_with("//"))
        .collect();

    let checks: Vec<usize> = live
        .iter()
        .enumerate()
        .filter(|(_, line)| line.contains("run_budget_exceeded"))
        .map(|(i, _)| i)
        .collect();
    assert_eq!(
        checks.len(),
        1,
        "the run budget must be checked in exactly one place, or the cached and          recomputed paths can disagree; found {} checks",
        checks.len()
    );

    let charges: Vec<usize> = live
        .iter()
        .enumerate()
        .filter(|(_, line)| line.contains("guest_time = guest_time.saturating_add"))
        .map(|(i, _)| i)
        .collect();
    assert_eq!(
        charges.len(),
        1,
        "the budget must be charged in exactly one place, so a hit and a miss          cost the same accounting; found {} charge sites",
        charges.len()
    );

    let acted_on = live
        .iter()
        .position(|line| line.trim_start().starts_with("match response"))
        .expect("the trampoline acts on the response with `match response`");
    assert!(
        charges[0] < checks[0] && checks[0] < acted_on,
        "order must be charge -> check -> act (charge at {}, check at {}, act at          {acted_on}); checking after acting lets an over-budget `Await` schedule          a real activity the run then fails",
        charges[0],
        checks[0]
    );
}

#[tokio::test]
async fn the_decision_cache_never_serves_one_builds_answer_to_another() {
    // A `DecideRequest` carries no build id — the guest has no business knowing
    // its own version — so v1 and v2 of one workflow see byte-identical input at
    // step 0. Keying the cache on the request alone would let v1's decision be
    // served to v2, silently defeating the swap the spike exists to
    // demonstrate. The key digests the request TOGETHER WITH
    // `(build_id, module_hash)`.
    //
    // Sharper now than in round 1: the cache is process-wide, so both builds
    // genuinely share one map rather than merely sharing a host.
    let registry = registry_with(&[
        ("wf-v1", "pipeline", pipeline_v1_bytes()),
        ("wf-v2", "pipeline", pipeline_v2_bytes()),
    ]);

    let v1 = with_module_host(
        ModuleHost::new(Arc::clone(&registry)).with_build_id("wf-v1"),
        env().run(module_workflow_handler, json!({})),
    )
    .await;
    let v2 = with_module_host(
        ModuleHost::new(Arc::clone(&registry)).with_build_id("wf-v2"),
        env().run(module_workflow_handler, json!({})),
    )
    .await;

    assert_eq!(v1.result.expect("v1 completes"), json!("v1-done"));
    assert_eq!(
        v2.result.expect("v2 completes"),
        json!("v2-done"),
        "v2 must not be served v1's cached decision"
    );
}

#[test]
fn the_decision_cache_is_bounded_in_bytes_not_just_entries() {
    // Codex review round 3, and the finding is a regression my own round-2 fix
    // introduced: a count is NOT a memory bound. The key is a 32-byte digest but
    // the VALUE is a whole `DecideResponse`, which a guest may return at up to
    // `WASM_MAX_OUTPUT_BYTES` (4 MiB) — so 4096 entries could retain ~16 GiB and
    // OOM the worker, walking straight past the per-invocation memory ceiling
    // the sandbox enforces. I had written "entries are small" in the doc comment
    // and never checked what made them small.
    let mut cache = DecisionCache::new();

    // A response over the per-entry ceiling is not cached at all, so one fat
    // response cannot evict the whole cache to make room for itself.
    let fat = "x".repeat(MAX_CACHED_RESPONSE_BYTES + 1024);
    assert!(
        !cache.insert(
            DecisionCache::key("wf-v1", "hash", b"fat"),
            DecideResponse::Complete { output: json!(fat) },
            Duration::ZERO,
        ),
        "a response over the per-entry ceiling must be refused, not cached"
    );
    assert_eq!(cache.len(), 0);
    assert_eq!(cache.retained_bytes(), 0);

    // Many merely-large responses stay under the total budget by eviction.
    let chunky = "y".repeat(MAX_CACHED_RESPONSE_BYTES / 2);
    for i in 0..512_usize {
        cache.insert(
            DecisionCache::key("wf-v1", "hash", &i.to_be_bytes()),
            DecideResponse::Complete {
                output: json!(format!("{chunky}{i}")),
            },
            Duration::ZERO,
        );
        assert!(
            cache.retained_bytes() <= MAX_CACHED_DECISION_BYTES,
            "the cache must never exceed its byte budget; at {i} it held {}",
            cache.retained_bytes()
        );
    }
    assert!(
        cache.retained_bytes() > 0,
        "the cache should still be doing its job"
    );
}

#[test]
fn a_cache_hit_is_charged_to_the_run_budget_like_a_recomputation() {
    // Codex review round 4. A hit used to be free, which made the run's TERMINAL
    // OUTCOME depend on cache residency: the same workflow with the same history
    // would complete when its earlier decisions were still resident and fail
    // when unrelated executions had evicted them, or when one response exceeded
    // `MAX_CACHED_RESPONSE_BYTES` and was never cached. That is the class of
    // defect `MAX_DECIDE_STEPS` is a compile-time constant to avoid, arriving
    // through the optimisation instead. The cache may make a run cheaper; it may
    // not change what the run decides.
    let mut cache = DecisionCache::new();
    let key = DecisionCache::key("wf-v1", "hash", b"step-0");
    let spent = Duration::from_millis(250);

    assert!(cache.insert(
        key,
        DecideResponse::Complete {
            output: json!("done")
        },
        spent,
    ));

    let (_, recorded) = cache.get(&key).expect("the decision is cached");
    assert_eq!(
        recorded, spent,
        "a hit must report what the decision originally cost, so the budget is          charged identically whether the step was recomputed or served"
    );
}

#[test]
fn the_decision_cache_evicts_oldest_first() {
    let mut cache = DecisionCache::new();
    let key_for = |i: usize| DecisionCache::key("wf-v1", "hash", &i.to_be_bytes());

    let first = key_for(0);
    for i in 0..(MAX_CACHED_DECISIONS + 8) {
        cache.insert(
            key_for(i),
            DecideResponse::Complete {
                output: json!(i as u64),
            },
            Duration::ZERO,
        );
    }

    assert_eq!(
        cache.len(),
        MAX_CACHED_DECISIONS,
        "the cache must not grow past its ceiling"
    );
    assert!(
        cache.get(&first).is_none(),
        "the oldest entry must have been evicted"
    );
    let newest = key_for(MAX_CACHED_DECISIONS + 7);
    assert!(
        cache.get(&newest).is_some(),
        "the newest entry must survive"
    );
}

#[test]
fn the_decision_cache_key_separates_build_workflow_and_request() {
    // Length-prefixed, so no two distinct triples share a preimage — otherwise
    // `("ab", "c", ..)` and `("a", "bc", ..)` would collide and one build could
    // be served another's answer through the back door.
    let a = DecisionCache::key("ab", "c", b"x");
    let b = DecisionCache::key("a", "bc", b"x");
    assert_ne!(a, b, "the key must not be a naive concatenation");

    assert_eq!(
        DecisionCache::key("wf", "h", b"r"),
        DecisionCache::key("wf", "h", b"r")
    );
    assert_ne!(
        DecisionCache::key("wf1", "h", b"r"),
        DecisionCache::key("wf2", "h", b"r")
    );
    assert_ne!(
        DecisionCache::key("wf", "h1", b"r"),
        DecisionCache::key("wf", "h2", b"r")
    );
    assert_ne!(
        DecisionCache::key("wf", "h", b"r1"),
        DecisionCache::key("wf", "h", b"r2")
    );
}

#[test]
fn a_load_batch_may_not_bind_two_modules_to_one_key() {
    // P2. Both `prepare` calls succeed and the commit-time duplicate check
    // compared each entry only against the ALREADY-BOUND map — which knows
    // nothing about the batch — so with nothing bound beforehand the insert loop
    // let the last entry silently overwrite the first, bypassing the immutable-
    // binding guarantee `load_modules` exists to enforce.
    let registry = Arc::new(ModuleRegistry::new());
    let v1 = pipeline_v1_bytes();
    let v2 = pipeline_v2_bytes();
    let none = ModuleVerification::none();

    let err = registry
        .load_modules(&[
            ("wf-v1", "pipeline", v1.as_slice(), none),
            ("wf-v1", "pipeline", v2.as_slice(), none),
        ])
        .expect_err("two modules may not claim one (build_id, workflow_name)");
    assert!(
        matches!(err, HotSwapError::DuplicateRegistration { .. }),
        "unexpected error: {err}"
    );
    assert_eq!(
        registry.len(),
        0,
        "a refused batch must bind nothing at all"
    );

    // The same key twice with the SAME bytes is still idempotent, not an error.
    registry
        .load_modules(&[
            ("wf-v1", "pipeline", v1.as_slice(), none),
            ("wf-v1", "pipeline", v1.as_slice(), none),
        ])
        .expect("identical bytes under one key stay idempotent");
    assert_eq!(registry.len(), 1);
}

#[test]
fn the_example_readme_documents_the_wire_format_the_host_actually_sends() {
    // P2. The README's `DecideRequest` example carried an `abi_version` field
    // the host has never transmitted (the ABI version is a host-side constant,
    // deliberately not on the wire), so a guest written from the example could
    // require or branch on a field it would never receive.
    let readme = include_str!("../../examples/workflow-modules/README.md");
    let example = readme
        .lines()
        .find(|line| line.starts_with(r#"{"step":0"#))
        .expect("the README shows a DecideRequest example");

    let encoded = encode_decide_request(&DecideRequest {
        step: 0,
        workflow: "checkout".to_string(),
        input: json!({}),
        resolved: vec![],
    })
    .expect("encodes");
    let encoded = String::from_utf8(encoded).expect("utf-8");

    let field_names = |s: &str| -> Vec<String> {
        let mut out = Vec::new();
        let bytes: Vec<char> = s.chars().collect();
        let mut depth = 0usize;
        let mut i = 0usize;
        while i < bytes.len() {
            match bytes[i] {
                '{' | '[' => depth += 1,
                '}' | ']' => depth -= 1,
                '"' if depth == 1 => {
                    let start = i + 1;
                    let mut j = start;
                    while j < bytes.len() && bytes[j] != '"' {
                        j += 1;
                    }
                    if j + 1 < bytes.len() && bytes[j + 1] == ':' {
                        out.push(bytes[start..j].iter().collect());
                    }
                    i = j;
                }
                _ => {}
            }
            i += 1;
        }
        out
    };

    assert_eq!(
        field_names(example),
        field_names(&encoded),
        "the README's wire example must name exactly the fields, in the order, \
         that `encode_decide_request` emits"
    );
    assert!(
        !example.contains("abi_version"),
        "the ABI version is a host-side constant and is never sent"
    );

    // Codex review round 4: the failure outcome's shape must be documented as
    // the host actually sends it. An example showing only `kind` and `error`
    // would make a guest with a strict schema reject every failed-activity
    // request, and would push a guest that does parse it onto `error` — a
    // diagnostic string that differs between the inline and replayed delivery
    // paths, so branching on it behaves differently on replay than it did live.
    let err_example = readme
        .lines()
        .find(|line| line.starts_with(r#"{"kind":"err""#))
        .expect("the README shows a failed DecideOutcome");
    let encoded_err = serde_json::to_string(&DecideOutcome::Err {
        error_type: "CircuitOpen".to_string(),
        details: Some(json!({"retry_after_secs": 30})),
        error: "...".to_string(),
    })
    .expect("encodes");
    assert_eq!(
        field_names(err_example),
        field_names(&encoded_err),
        "the README's failure example must name exactly the fields, in the \
         order, that `DecideOutcome::Err` serialises"
    );
    assert!(
        readme.contains("Branch on `error_type`, never on `error`"),
        "the README must say which field is the stable contract"
    );
}
