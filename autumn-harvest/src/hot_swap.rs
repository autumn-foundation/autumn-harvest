//! Hot code swap for workflow definitions via runtime modules (issue #967).
//!
//! **R&D spike, behind the `hot-code-swap` Cargo feature.** Not a committed GA
//! feature. The written half of the spike — the constraint inventory, the
//! dylib-vs-WASM evaluation, the safety analysis and the go/no-go — is
//! `docs/rnd/hot-code-swap.md`. This module is the executable half.
//!
//! # What it does
//!
//! A `#[workflow]` body normally reaches a worker as a `fn` pointer compiled
//! into the binary, so shipping new workflow code means shipping a new binary
//! and bouncing the fleet. This module lets a *running* worker acquire workflow
//! logic as a WebAssembly module fetched from a Postgres registry, registered
//! under an explicit **`BuildId`**, so a deploy is "publish a module" and a
//! rollback is "repoint the ramp".
//!
//! Everything that decides *which executions see which code* is already shipped
//! and is consumed unchanged: percent ramp (issue #604) governs which new starts
//! land on the swapped code, [`BuildCompatibilitySet`] + claim-time filtering
//! (issue #171) governs which in-flight executions a module may process, and
//! build reachability (issues #520/#535) answers when the old module is
//! retirable. This module adds **no** safety machinery, and — see
//! [`module_workflow_handler`] — **no replay surface**: no new
//! [`WorkflowEvent`](crate::event::WorkflowEvent) variant, no change to event
//! JSON, no change to `HistoryMatcher` or executor semantics.
//!
//! [`BuildCompatibilitySet`]: crate::build_routing::BuildCompatibilitySet
//!
//! # The hosting boundary
//!
//! [`WorkflowHandlerFn`](crate::info::WorkflowHandlerFn) is a bare `fn` pointer
//! (design decision #6 — it keeps [`WorkflowInfo`](crate::info::WorkflowInfo)
//! `Sync` without an `Arc`), and no runtime-loaded module can mint one. The
//! boundary this spike adopts sidesteps that entirely:
//!
//! * [`module_workflow_handler`] is **one statically-linked `fn`** — a
//!   trampoline. `WorkflowInfo` keeps its `fn` pointer and DD-6 is untouched.
//! * The trampoline resolves *which* module to run from a task-scoped
//!   [`ModuleHost`] binding carrying the **execution's assigned build id**.
//! * The module itself is a **pure decision function**, re-invoked once per
//!   await: `run(DecideRequest) -> DecideResponse`. It never re-enters host
//!   async code, which is what makes it hostable in WASM at all.
//!
//! ## Why the routing key is not `ctx.build_id()`
//!
//! [`WorkflowContext::build_id`](crate::context::WorkflowContext::build_id)
//! reports the **worker's own configured build** — deliberately, since issue
//! #798, so a pre-promotion replay gate can ask "what will the candidate do with
//! these histories?". It is *not* the execution's `assigned_build_id`. Routing
//! modules on it would hand a v1-assigned in-flight execution to v2 code the
//! moment an operator relabelled the worker, which is precisely the divergence
//! build routing exists to prevent. The binding therefore carries the
//! execution's build, threaded by the worker seam from the execution row's
//! **`assigned_build_id`** column — not the task row's denormalised
//! `required_build_id` copy. They agree, but the execution row is the authority
//! and is the value fixed at start time.
//!
//! ## Why the guest runs inline and synchronously
//!
//! The executor classifies a `Poll::Pending` with a zero replay-significant
//! command delta as a workflow *suspension* (see `executor::poll_query_step`).
//! A host-side `.await` inside a handler — e.g. awaiting a
//! `tokio::task::spawn_blocking` join handle — is therefore indistinguishable
//! from a park, and would be misread. The guest is consequently invoked
//! synchronously on the decision-cycle thread.
//!
//! Two consequences follow, and both are engineered for rather than tolerated:
//!
//! * **Fuel, not the clock, is the operative budget.** A wall-clock ceiling that
//!   could plausibly fire before fuel exhausts would make a run's terminal
//!   outcome depend on host load — two workers replaying one history could
//!   disagree about whether it failed. [`DECIDE_MAX_WALL_CLOCK`] is therefore a
//!   generous *backstop* for the one class fuel cannot bound (bulk-memory
//!   instructions cost one fuel unit regardless of bytes moved), while
//!   [`DECIDE_FUEL`] is the bound a well-behaved guest actually meets.
//! * **A decision cycle memoises.** Under the oneshot suspension model (DD-1)
//!   the workflow restarts at `step = 0` every cycle, so a naive loop would
//!   re-invoke the guest once per already-resolved await — `O(n²)` guest calls
//!   per run, all inside one `poll` with no yield point. The trampoline instead
//!   caches `step -> DecideResponse` for the lifetime of one handler invocation,
//!   which is sound *because* the guest is a pure function of
//!   `(input, resolved)`, and yields between fresh decisions so the runtime can
//!   preempt.
//!
//! # What is deliberately not here
//!
//! No dylib hosting. Rust has no stable ABI, `WorkflowContext` is not
//! `#[repr(C)]`, and a `dlclose` while a suspended execution still holds module
//! code is a use-after-free rather than a refcount decrement. See
//! `docs/rnd/hot-code-swap.md` §3.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, PoisonError, RwLock};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::context::WorkflowContext;
use crate::error::HarvestError;
use crate::wasm_activities::{WasmCapabilities, WasmLimits, WasmModuleStore};

/// Wire-format version of the decide-loop ABI.
///
/// Deliberately **not** carried in [`DecideRequest`]. A per-decision version
/// field invites a guest to branch on it, which would let a host upgrade change
/// a module's command sequence while its build id — the value that is supposed
/// to pin what the code means — stayed the same. That is an escape hatch
/// straight out of build-id governance. The version is instead a property of the
/// host binary: bumping it means republishing modules under a new build id.
pub const DECIDE_ABI_VERSION: u32 = 1;

/// Hard ceiling on how many decisions one execution may take.
///
/// A guest that answers [`DecideResponse::Await`] forever would otherwise append
/// activity events without bound — a durable, replayable denial of service
/// rather than a transient one.
///
/// **A compile-time constant, deliberately not configurable.** An earlier cut
/// exposed a per-[`ModuleHost`] override; that made an execution's terminal
/// outcome a function of *which worker* claimed it, since a worker with a
/// tighter cap fails a run another worker completes. A cap that can decide a
/// terminal outcome must be uniform across the fleet, and the only way to
/// guarantee that is to not let it vary.
pub const MAX_DECIDE_STEPS: usize = 64;

/// Hard ceiling on a published workflow module's byte length: 32 MiB.
///
/// Enforced by [`verify_module_bytes`] before hashing or compilation, by
/// [`publish_workflow_module`](crate::hot_swap_store::publish_workflow_module)
/// before any database work, and by a `CHECK` constraint on the table itself so
/// a direct `INSERT` cannot plant an oversized row for a worker to materialise.
pub const MAX_WORKFLOW_MODULE_BYTES: usize = 32 * 1024 * 1024;

/// Wall-clock **backstop** for a single guest decision.
///
/// Not the operative budget — see [`DECIDE_FUEL`]. It exists for the one class
/// fuel provably cannot bound: bulk-memory instructions (`memory.fill`,
/// `memory.copy`) cost one fuel unit regardless of how many bytes they move, so
/// a guest can burn real time inside a small fuel budget.
///
/// Set generously *above* the time a fuel-bounded decision can take, so the two
/// bounds do not race. A tight ceiling would make the terminal outcome of a run
/// depend on host load: the same history would fail on a busy worker and succeed
/// on an idle one, which is precisely the non-determinism this spike exists to
/// avoid.
pub const DECIDE_MAX_WALL_CLOCK: Duration = Duration::from_secs(5);

/// Cumulative guest wall-clock budget for one handler invocation.
///
/// [`DECIDE_MAX_WALL_CLOCK`] bounds a single decision; this bounds the whole
/// decision cycle, so a guest cannot compose per-decision budgets into an
/// unbounded occupancy of a runtime worker thread. With memoisation a cycle
/// performs one *new* decision, so a well-behaved guest never approaches it.
pub const DECIDE_RUN_WALL_CLOCK: Duration = Duration::from_secs(10);

/// CPU fuel budget for a single guest decision — the operative bound.
///
/// Deterministic: the same guest on the same input consumes the same fuel on
/// every host, so a fuel-exhaustion failure is reproducible rather than
/// load-dependent.
pub const DECIDE_FUEL: u64 = 10_000_000;

/// Linear-memory ceiling for a guest decision: 4 MiB.
pub const DECIDE_MEMORY_BYTES: usize = 4 * 1024 * 1024;

/// Ceiling on the serialized [`DecideRequest`] handed to a guest: 1 MiB.
///
/// The request carries every previously-resolved activity output, so it grows
/// with the run. Without a bound the host would allocate and serialize an
/// ever-larger buffer and only discover the problem when the guest's `alloc`
/// trapped against [`DECIDE_MEMORY_BYTES`] — after paying for all of it, and
/// after the run had already performed real side effects.
pub const MAX_DECIDE_REQUEST_BYTES: usize = 1024 * 1024;

/// Ceiling on guest-authored text the host will copy into an error message or a
/// durable event: 2 KiB.
///
/// A guest's output can be megabytes. Interpolating it into an `Err(String)`
/// puts guest-controlled bytes into a terminal `WorkflowFailed` event, which is
/// then durable and re-read on every history load. Truncation keeps the
/// diagnostic while bounding the blast radius.
pub const MAX_GUEST_TEXT_BYTES: usize = 2048;

/// Minimum module-signing key length, in bytes.
///
/// HMAC accepts a key of any length, including empty — so a misconfigured or
/// unset environment variable would otherwise yield a "signature" anyone can
/// compute, silently, with no error anywhere.
pub const MIN_SIGNING_KEY_BYTES: usize = 16;

/// The per-decision resource budget: [`DECIDE_MEMORY_BYTES`], [`DECIDE_FUEL`],
/// [`DECIDE_MAX_WALL_CLOCK`].
#[must_use]
pub const fn default_decide_limits() -> WasmLimits {
    WasmLimits {
        memory_bytes: DECIDE_MEMORY_BYTES,
        fuel: DECIDE_FUEL,
        max_wall_clock: DECIDE_MAX_WALL_CLOCK,
    }
}

/// Truncate guest-authored text to [`MAX_GUEST_TEXT_BYTES`] on a char boundary,
/// marking the elision so a reader knows the value is partial.
fn bound_guest_text(text: &str) -> String {
    if text.len() <= MAX_GUEST_TEXT_BYTES {
        return text.to_string();
    }
    let mut end = MAX_GUEST_TEXT_BYTES;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}… [{} bytes elided]", &text[..end], text.len() - end)
}

// ── the decide-loop ABI ───────────────────────────────────────────────────────

/// What the host asks the module on each step of a run.
///
/// # Field order is load-bearing
///
/// `step` serialises **first**, at a fixed byte offset, so a guest can read its
/// step index without a JSON parser — the hand-written WAT guests in
/// `autumn-harvest/examples/workflow-modules/` do exactly that. That only holds
/// while the bytes carry this struct's declaration order, which is why
/// [`encode_decide_request`] exists and why nothing here may round-trip through
/// [`serde_json::Value`] (whose object is a `BTreeMap`, and would sort the keys).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecideRequest {
    /// Zero-based index of this decision within the run. Always equals
    /// `resolved.len()`; supplied explicitly so a guest need not count.
    pub step: u32,
    /// The workflow type name being hosted.
    pub workflow: String,
    /// The workflow's own input, verbatim from `WorkflowStarted`.
    pub input: Value,
    /// Outcomes of the activities the guest asked for at steps `0..step`, in
    /// order. Every value here came back through a `WorkflowContext` await, so
    /// it is history-backed — which is what makes a hosted run replayable.
    pub resolved: Vec<DecideOutcome>,
}

/// The outcome of one previously-awaited activity, as the guest sees it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DecideOutcome {
    /// The activity completed; `output` is its result payload.
    Ok {
        /// The activity's JSON result.
        output: Value,
    },
    /// The activity failed after exhausting its retry policy.
    ///
    /// Carries the engine's **stable** classification alongside the message: a
    /// guest that branches on failure should match `error_type` (issue #227's
    /// low-cardinality class) or read `details`, never parse `error`. The
    /// message is a `Display` whose format is not a compatibility surface, and
    /// it embeds the attempt number, which differs between the inline and
    /// replayed delivery paths — so a guest that branched on it would replay
    /// differently than it ran.
    Err {
        /// The engine's stable failure class, e.g. `"CircuitOpen"` or the
        /// `"Error"` fallback for an untyped `Err(String)` failure.
        error_type: String,
        /// Structured detail carried by a typed failure, when there was one.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        details: Option<Value>,
        /// Human-readable detail. Diagnostic only — do not branch on it.
        error: String,
    },
}

/// What the module answers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DecideResponse {
    /// Schedule `activity` and re-enter the module at `step + 1` with the
    /// result appended to [`DecideRequest::resolved`].
    Await {
        /// Registered activity name.
        activity: String,
        /// JSON input for the activity.
        input: Value,
        /// Queue override. Rejected unless the host opts in via
        /// [`ModuleHost::allowing_queue_override`]: letting a guest name any
        /// queue in the shard is lateral movement, not configuration. `None`
        /// (the normal case) uses the execution's own queue, which is what a
        /// statically-linked handler calling
        /// [`execute_activity_raw`](crate::context::WorkflowContext::execute_activity_raw)
        /// with `ctx.queue_name()` produces — so a hosted run's
        /// `ActivityScheduled` events are byte-identical to the native ones.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        queue: Option<String>,
    },
    /// Terminal success.
    Complete {
        /// The workflow's JSON result.
        output: Value,
    },
    /// Terminal failure.
    Fail {
        /// The error string the workflow returns. Bounded by
        /// [`MAX_GUEST_TEXT_BYTES`] before it reaches history.
        error: String,
    },
}

// ── errors ────────────────────────────────────────────────────────────────────

/// Every way loading a workflow module can be refused.
///
/// Kept separate from [`HarvestError`] so the spike's failure modes are
/// enumerable in one place for the safety analysis; `From<HotSwapError>` lowers
/// into `HarvestError::Config` at the DB seam.
#[derive(Debug, thiserror::Error)]
pub enum HotSwapError {
    /// The bytes are empty.
    #[error("workflow module is empty")]
    EmptyModule,
    /// The bytes exceed [`MAX_WORKFLOW_MODULE_BYTES`].
    #[error("workflow module is {actual} bytes, over the {limit}-byte ceiling")]
    TooLarge {
        /// Actual byte length.
        actual: usize,
        /// The enforced ceiling.
        limit: usize,
    },
    /// The bytes do not hash to the expected content id.
    #[error("workflow module content integrity check failed: expected {expected}, got {actual}")]
    HashMismatch {
        /// The hash the registry claimed.
        expected: String,
        /// The hash the bytes actually have.
        actual: String,
    },
    /// A signing key is configured but the module carries no signature.
    #[error("workflow module is unsigned but this deployment requires a signature")]
    MissingSignature,
    /// The signature does not verify for this module under this identity.
    #[error(
        "workflow module signature does not verify for `{workflow_name}` under build \
         `{build_id}`"
    )]
    BadSignature {
        /// The build id the signature was checked against.
        build_id: String,
        /// The workflow name the signature was checked against.
        workflow_name: String,
    },
    /// The configured signing key is shorter than [`MIN_SIGNING_KEY_BYTES`].
    #[error(
        "module signing key is {actual} bytes; at least {MIN_SIGNING_KEY_BYTES} are required \
         (an empty or truncated key yields a signature anyone can compute)"
    )]
    SigningKeyTooShort {
        /// The configured key's length.
        actual: usize,
    },
    /// wasmtime refused the bytes.
    #[error("failed to compile workflow module: {message}")]
    Compile {
        /// The compiler's diagnostic.
        message: String,
    },
    /// A different module is already bound to this `(build_id, workflow_name)`.
    ///
    /// The determinism hazard the safety analysis names: two modules claiming
    /// one workflow name *outside* build-id governance. A build id names one
    /// exact module for a workflow, immutably — mirroring the start-time
    /// immutability of `assigned_build_id`.
    #[error(
        "workflow `{workflow_name}` is already bound to module {existing} under build id \
         `{build_id}`; a build id's module binding is immutable (attempted: {attempted})"
    )]
    DuplicateRegistration {
        /// The build id being rebound.
        build_id: String,
        /// The workflow name being rebound.
        workflow_name: String,
        /// Content hash already bound.
        existing: String,
        /// Content hash that was refused.
        attempted: String,
    },
    /// The build was unloaded while this load was compiling.
    ///
    /// Without this check a retirement that raced a concurrent sync would be
    /// silently undone: `unload_build` would report success while the in-flight
    /// load re-inserted the binding afterwards, leaving a worker serving code
    /// its operator believes is gone.
    #[error(
        "build `{build_id}` was unloaded while its module for `{workflow_name}` was compiling; \
         re-sync if the build is still wanted"
    )]
    UnloadedDuringLoad {
        /// The build id that was unloaded.
        build_id: String,
        /// The workflow whose load lost the race.
        workflow_name: String,
    },
}

impl From<HotSwapError> for HarvestError {
    fn from(err: HotSwapError) -> Self {
        Self::Config(err.to_string())
    }
}

// ── content addressing and verification ───────────────────────────────────────

/// Lowercase-hex SHA-256 of module bytes — the module's content id.
#[must_use]
pub fn compute_module_hash(bytes: &[u8]) -> String {
    WasmModuleStore::compute_hash(bytes)
}

/// Domain separator for the module-binding MAC, so a tag minted here can never
/// be replayed as any other HMAC in the system.
const MODULE_SIGNATURE_DOMAIN: &[u8] = b"harvest-workflow-module-binding-v1";

/// Detached signature over a module's **binding** — `(build_id, workflow_name,
/// module_hash)` — as lowercase-hex HMAC-SHA256.
///
/// # Why the whole binding, not just the hash
///
/// An earlier cut signed the content hash alone. That is sound as far as "these
/// bytes were approved" goes, but it says nothing about *what those bytes were
/// approved to be*. An attacker with `INSERT` on the registry could copy any
/// existing row's `(hash, signature)` pair and re-bind it under a different
/// build id or a different workflow name, and verification would pass — giving
/// them a **downgrade** (resurrect a superseded but signed module under the
/// build the ramp currently points at) and a **substitution** (run workflow A's
/// module as workflow B, feeding it B's inputs). Signing the identity closes
/// both, and costs nothing: a registry listing already carries all three fields.
///
/// Each field is length-prefixed before hashing, so `("a", "bc")` and
/// `("ab", "c")` cannot produce the same message.
///
/// # Errors
///
/// [`HotSwapError::SigningKeyTooShort`] if `key` is shorter than
/// [`MIN_SIGNING_KEY_BYTES`].
///
/// # Panics
///
/// Never in practice: the only fallible step is HMAC key ingestion, and
/// HMAC-SHA256 accepts a key of any length, so the `expect` is unreachable once
/// the length check above has passed.
pub fn sign_module_binding(
    key: &[u8],
    build_id: &str,
    workflow_name: &str,
    module_hash: &str,
) -> Result<String, HotSwapError> {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    use std::fmt::Write as _;

    if key.len() < MIN_SIGNING_KEY_BYTES {
        return Err(HotSwapError::SigningKeyTooShort { actual: key.len() });
    }
    let mut mac =
        <Hmac<Sha256> as Mac>::new_from_slice(key).expect("HMAC-SHA256 takes any key length");
    mac.update(MODULE_SIGNATURE_DOMAIN);
    for field in [build_id, workflow_name, module_hash] {
        // Length-prefix every field so concatenation is unambiguous.
        mac.update(&(field.len() as u64).to_be_bytes());
        mac.update(field.as_bytes());
    }
    let tag = mac.finalize().into_bytes();
    let mut out = String::with_capacity(tag.len() * 2);
    for b in tag {
        let _ = write!(out, "{b:02x}");
    }
    Ok(out)
}

/// Decode lowercase- or uppercase-hex into bytes; `None` on any non-hex input.
fn decode_hex(s: &str) -> Option<Vec<u8>> {
    if s.len() % 2 != 0 {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok())
        .collect()
}

/// Verify module bytes and their binding before a worker will load them.
///
/// Runs the checks in escalating order so the cheapest refusal happens first:
/// size, then content hash, then signature.
///
/// * `build_id` / `workflow_name` — the identity these bytes are being bound to.
///   Part of the signed message; see [`sign_module_binding`].
/// * `expected_hash` — the content id the registry claims. `None` skips the
///   comparison (the caller is publishing bytes it produced itself).
/// * `signature` — the detached signature the registry stored, if any.
/// * `signing_key` — the operator-configured key. `None` means this deployment
///   does not require signatures; a signature that happens to be present is then
///   simply not load-bearing, and content addressing still applies.
///
/// # Errors
///
/// [`HotSwapError::EmptyModule`], [`HotSwapError::TooLarge`],
/// [`HotSwapError::HashMismatch`], [`HotSwapError::MissingSignature`],
/// [`HotSwapError::BadSignature`] or [`HotSwapError::SigningKeyTooShort`].
pub fn verify_module_bytes(
    build_id: &str,
    workflow_name: &str,
    bytes: &[u8],
    expected_hash: Option<&str>,
    signature: Option<&str>,
    signing_key: Option<&[u8]>,
) -> Result<(), HotSwapError> {
    if bytes.is_empty() {
        return Err(HotSwapError::EmptyModule);
    }
    if bytes.len() > MAX_WORKFLOW_MODULE_BYTES {
        return Err(HotSwapError::TooLarge {
            actual: bytes.len(),
            limit: MAX_WORKFLOW_MODULE_BYTES,
        });
    }

    let actual = compute_module_hash(bytes);
    if let Some(expected) = expected_hash
        && expected != actual
    {
        return Err(HotSwapError::HashMismatch {
            expected: expected.to_string(),
            actual,
        });
    }

    if let Some(key) = signing_key {
        let Some(signature) = signature else {
            return Err(HotSwapError::MissingSignature);
        };
        let bad = || HotSwapError::BadSignature {
            build_id: build_id.to_string(),
            workflow_name: workflow_name.to_string(),
        };
        // Compare decoded bytes, not hex text: it makes the check
        // case-insensitive (an uppercase-hex tag is the same tag) and rejects
        // malformed input as a bad signature rather than a length mismatch.
        let expected_tag = sign_module_binding(key, build_id, workflow_name, &actual)?;
        let (Some(expected_tag), Some(offered)) =
            (decode_hex(&expected_tag), decode_hex(signature))
        else {
            return Err(bad());
        };
        if !constant_time_eq(&expected_tag, &offered) {
            return Err(bad());
        }
    }

    Ok(())
}

/// Length-independent, early-exit-free byte comparison.
///
/// Compares the two slices without branching on content. A length difference is
/// still observable — it is not secret; the tag length is fixed and public.
///
/// `black_box` on the accumulator stops the optimiser turning the loop back into
/// an early-exit comparison, which it is otherwise entitled to do.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    std::hint::black_box(diff) == 0
}

/// The verification policy applied to one [`ModuleRegistry::load_module`] call.
///
/// Borrows rather than owns so a caller can point at a registry row's fields
/// without cloning.
#[derive(Debug, Clone, Copy, Default)]
pub struct ModuleVerification<'a> {
    /// The content id the source claims these bytes have.
    pub expected_hash: Option<&'a str>,
    /// The detached signature the source stored alongside them.
    pub signature: Option<&'a str>,
    /// The operator-configured signing key, when the deployment requires
    /// signatures.
    pub signing_key: Option<&'a [u8]>,
}

impl<'a> ModuleVerification<'a> {
    /// Verify nothing beyond the intrinsic size/emptiness checks — the shape
    /// used when the caller is handing over bytes it produced itself.
    #[must_use]
    pub const fn none() -> Self {
        Self {
            expected_hash: None,
            signature: None,
            signing_key: None,
        }
    }

    /// Require the bytes to hash to `hash`.
    #[must_use]
    pub const fn with_expected_hash(mut self, hash: &'a str) -> Self {
        self.expected_hash = Some(hash);
        self
    }

    /// Attach the detached signature to check.
    #[must_use]
    pub const fn with_signature(mut self, signature: &'a str) -> Self {
        self.signature = Some(signature);
        self
    }

    /// Require a valid signature under `key`.
    #[must_use]
    pub const fn with_signing_key(mut self, key: &'a [u8]) -> Self {
        self.signing_key = Some(key);
        self
    }
}

// ── the in-process registry ───────────────────────────────────────────────────

/// Identity of one loaded module: which build id it serves, which workflow it
/// implements, and the content id of the exact bytes behind it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModuleDescriptor {
    /// The `BuildId` this module is registered under. Every shipped routing
    /// rule keys off this value and nothing else.
    pub build_id: String,
    /// The workflow type name this module implements.
    pub workflow_name: String,
    /// Lowercase-hex SHA-256 of the module bytes.
    pub module_hash: String,
}

/// A compiled module plus its identity.
///
/// Handed out behind an [`Arc`], which is the whole answer to the unload
/// hazard: [`ModuleRegistry::unload_build`] removes the *binding*, while an
/// invocation that already resolved the module keeps the code alive until it
/// finishes. In dylib hosting the same sequence is a use-after-free.
#[derive(Debug)]
pub struct LoadedWorkflowModule {
    descriptor: ModuleDescriptor,
    module: Arc<wasmtime::Module>,
}

impl LoadedWorkflowModule {
    /// The module's identity.
    #[must_use]
    pub const fn descriptor(&self) -> &ModuleDescriptor {
        &self.descriptor
    }

    /// The compiled wasmtime module.
    #[must_use]
    pub fn module(&self) -> &wasmtime::Module {
        &self.module
    }
}

/// A worker's in-process table of loaded workflow modules, keyed by
/// `(build_id, workflow_name)`.
///
/// Compilation and the compiled-code cache are delegated to the
/// [`WasmModuleStore`] issue #965 already vetted, so two builds publishing
/// identical bytes compile once.
///
/// Note that a *binding* holds its own `Arc<wasmtime::Module>`, so it pins that
/// compiled code resident regardless of the store's LRU. The LRU bounds
/// *unbound* compiled code; bound code is bounded by
/// [`unload_build`](Self::unload_build), which reachability tells an operator
/// when to call.
pub struct ModuleRegistry {
    store: Arc<WasmModuleStore>,
    /// `BTreeMap` rather than `HashMap` so [`Self::descriptors`] has a stable,
    /// reproducible order for diagnostics and tests.
    bindings: RwLock<BTreeMap<(String, String), Arc<LoadedWorkflowModule>>>,
    /// Bumped by every [`Self::unload_build`]. A load captures it before
    /// compiling and refuses to insert if it moved, so a retirement cannot be
    /// silently undone by a sync that was already in flight.
    generation: AtomicU64,
}

impl std::fmt::Debug for ModuleRegistry {
    /// Hand-written because [`WasmModuleStore`] holds a wasmtime `Engine` and a
    /// ticker `JoinHandle`, neither of which is `Debug`. Prints what is
    /// diagnostically useful — the bindings and the compiled-module cache
    /// occupancy — rather than the engine's innards.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ModuleRegistry")
            .field("bindings", &self.descriptors())
            .field("compiled_modules_cached", &self.store.cache_len())
            .finish()
    }
}

impl Default for ModuleRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// One entry of a [`ModuleRegistry::load_modules`] batch: an identity and the
/// compiled module to bind to it.
struct PreparedBinding {
    key: (String, String),
    loaded: Arc<LoadedWorkflowModule>,
}

impl ModuleRegistry {
    /// Build a registry owning a fresh [`WasmModuleStore`].
    ///
    /// # Panics
    ///
    /// Panics under the same fixed-config / thread-spawn faults as
    /// [`WasmModuleStore::new`].
    #[must_use]
    pub fn new() -> Self {
        Self::with_store(Arc::new(WasmModuleStore::new()))
    }

    /// Build a registry sharing an existing [`WasmModuleStore`] — e.g. the one
    /// the worker already runs WASM *activities* on, so a process hosting both
    /// has one engine and one epoch ticker rather than two.
    #[must_use]
    pub fn with_store(store: Arc<WasmModuleStore>) -> Self {
        Self {
            store,
            bindings: RwLock::new(BTreeMap::new()),
            generation: AtomicU64::new(0),
        }
    }

    /// The underlying module store.
    #[must_use]
    pub const fn store(&self) -> &Arc<WasmModuleStore> {
        &self.store
    }

    fn binding(&self, key: &(String, String)) -> Option<Arc<LoadedWorkflowModule>> {
        self.bindings
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(key)
            .map(Arc::clone)
    }

    /// Verify and compile `bytes` without binding them.
    ///
    /// Split out so [`Self::load_modules`] can prepare a whole build before
    /// committing any of it.
    fn prepare(
        &self,
        build_id: &str,
        workflow_name: &str,
        bytes: &[u8],
        verification: &ModuleVerification<'_>,
    ) -> Result<PreparedBinding, HotSwapError> {
        verify_module_bytes(
            build_id,
            workflow_name,
            bytes,
            verification.expected_hash,
            verification.signature,
            verification.signing_key,
        )?;
        let module_hash = compute_module_hash(bytes);
        let key = (build_id.to_string(), workflow_name.to_string());

        // Check the existing binding before compiling: a conflicting rebind
        // should not pay for a compile it will not use. The decision is
        // re-taken under the write lock in `commit`, which is what actually
        // makes it race-free.
        if let Some(existing) = self.binding(&key) {
            return if existing.descriptor.module_hash == module_hash {
                Ok(PreparedBinding {
                    key,
                    loaded: existing,
                })
            } else {
                Err(HotSwapError::DuplicateRegistration {
                    build_id: build_id.to_string(),
                    workflow_name: workflow_name.to_string(),
                    existing: existing.descriptor.module_hash.clone(),
                    attempted: module_hash,
                })
            };
        }

        let module = self
            .store
            .get_or_compile(&module_hash, bytes)
            .map_err(|e| HotSwapError::Compile {
                message: e.to_string(),
            })?;

        Ok(PreparedBinding {
            key,
            loaded: Arc::new(LoadedWorkflowModule {
                descriptor: ModuleDescriptor {
                    build_id: build_id.to_string(),
                    workflow_name: workflow_name.to_string(),
                    module_hash,
                },
                module,
            }),
        })
    }

    /// Commit prepared bindings under one write lock, all or nothing.
    ///
    /// `generation_at_start` is the value the generation counter held before the
    /// (slow, unlocked) compile step; a change means an `unload_build` ran in
    /// the meantime and this load must not resurrect it.
    fn commit(
        &self,
        prepared: Vec<PreparedBinding>,
        generation_at_start: u64,
    ) -> Result<Vec<ModuleDescriptor>, HotSwapError> {
        let mut bindings = self
            .bindings
            .write()
            .unwrap_or_else(PoisonError::into_inner);

        if self.generation.load(Ordering::Acquire) != generation_at_start
            && let Some(first) = prepared.first()
        {
            return Err(HotSwapError::UnloadedDuringLoad {
                build_id: first.key.0.clone(),
                workflow_name: first.key.1.clone(),
            });
        }

        // Re-take the duplicate decision for every entry before mutating
        // anything, so a batch that would conflict leaves the registry
        // untouched rather than half-applied.
        for entry in &prepared {
            if let Some(existing) = bindings.get(&entry.key)
                && existing.descriptor.module_hash != entry.loaded.descriptor.module_hash
            {
                return Err(HotSwapError::DuplicateRegistration {
                    build_id: entry.key.0.clone(),
                    workflow_name: entry.key.1.clone(),
                    existing: existing.descriptor.module_hash.clone(),
                    attempted: entry.loaded.descriptor.module_hash.clone(),
                });
            }
        }

        let mut out = Vec::with_capacity(prepared.len());
        for entry in prepared {
            out.push(entry.loaded.descriptor.clone());
            bindings.insert(entry.key, entry.loaded);
        }
        Ok(out)
    }

    /// Verify, compile and bind `bytes` as `workflow_name` under `build_id`.
    ///
    /// Re-loading the *same* bytes under an existing binding is idempotent (a
    /// worker re-syncing a build it already holds is the normal case).
    /// Rebinding to *different* bytes is refused — see
    /// [`HotSwapError::DuplicateRegistration`].
    ///
    /// Named `load_module` rather than `load` deliberately: `diesel_async`'s
    /// `RunQueryDsl` has a blanket `impl<T> RunQueryDsl for T` supplying a
    /// `load` method, and a caller holding an `Arc<ModuleRegistry>` with that
    /// trait in scope would resolve `registry.load(..)` to *diesel's* method
    /// before auto-deref reached ours — producing a wall of unrelated
    /// `QueryFragment` errors. Every DB-side caller here has `RunQueryDsl` in
    /// scope, so the distinct name is load-bearing, not stylistic.
    ///
    /// # Errors
    ///
    /// Any [`HotSwapError`] from [`verify_module_bytes`], plus
    /// [`HotSwapError::Compile`], [`HotSwapError::DuplicateRegistration`] and
    /// [`HotSwapError::UnloadedDuringLoad`].
    pub fn load_module(
        &self,
        build_id: &str,
        workflow_name: &str,
        bytes: &[u8],
        verification: &ModuleVerification<'_>,
    ) -> Result<ModuleDescriptor, HotSwapError> {
        let generation = self.generation.load(Ordering::Acquire);
        let prepared = self.prepare(build_id, workflow_name, bytes, verification)?;
        let mut committed = self.commit(vec![prepared], generation)?;
        Ok(committed.remove(0))
    }

    /// Verify and compile every module in `batch`, then bind them **all at
    /// once** — or bind none of them.
    ///
    /// This is the property a whole-build sync needs and a per-module loop
    /// cannot give: a build whose third module fails verification must not leave
    /// the first two bound, because a worker advertising a build it can only
    /// half-serve claims executions it must then destroy.
    ///
    /// `batch` entries are `(build_id, workflow_name, bytes, verification)`.
    ///
    /// # Errors
    ///
    /// The first failure encountered while preparing, or a commit-time
    /// [`HotSwapError::DuplicateRegistration`] /
    /// [`HotSwapError::UnloadedDuringLoad`].
    pub fn load_modules(
        &self,
        batch: &[(&str, &str, &[u8], ModuleVerification<'_>)],
    ) -> Result<Vec<ModuleDescriptor>, HotSwapError> {
        let generation = self.generation.load(Ordering::Acquire);
        let mut prepared = Vec::with_capacity(batch.len());
        for (build_id, workflow_name, bytes, verification) in batch {
            prepared.push(self.prepare(build_id, workflow_name, bytes, verification)?);
        }
        self.commit(prepared, generation)
    }

    /// Resolve the module bound to `(build_id, workflow_name)`, if any.
    #[must_use]
    pub fn get(&self, build_id: &str, workflow_name: &str) -> Option<Arc<LoadedWorkflowModule>> {
        self.binding(&(build_id.to_string(), workflow_name.to_string()))
    }

    /// Drop every binding for `build_id`, returning how many were removed.
    ///
    /// Safe to call the moment
    /// [`build_reachability`](crate::build_routing::build_reachability) reports
    /// `safe_to_retire`; safe to call *before* that too, because an in-flight
    /// invocation holds its own `Arc`.
    ///
    /// Bumps the registry generation, so a load that is mid-compile for this
    /// build fails with [`HotSwapError::UnloadedDuringLoad`] instead of
    /// re-inserting the binding behind the operator's back.
    pub fn unload_build(&self, build_id: &str) -> usize {
        let mut bindings = self
            .bindings
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        let before = bindings.len();
        bindings.retain(|(bid, _), _| bid != build_id);
        // Bumped under the write lock so a concurrent `commit` either sees the
        // old generation and this removal, or the new generation and refuses.
        self.generation.fetch_add(1, Ordering::AcqRel);
        before - bindings.len()
    }

    /// Every currently-bound module, in `(build_id, workflow_name)` order.
    #[must_use]
    pub fn descriptors(&self) -> Vec<ModuleDescriptor> {
        self.bindings
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .map(|m| m.descriptor.clone())
            .collect()
    }

    /// Number of bound modules.
    #[must_use]
    pub fn len(&self) -> usize {
        self.bindings
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }

    /// Whether no module is bound.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// ── the task-scoped host binding ──────────────────────────────────────────────

/// The module-hosting context for one workflow task: which registry to resolve
/// from, which **execution** build id to resolve for, and the policy the guest
/// runs under.
#[derive(Debug, Clone)]
pub struct ModuleHost {
    /// The worker's loaded-module table.
    pub registry: Arc<ModuleRegistry>,
    /// The **execution's** assigned build id, threaded by the worker seam from
    /// the execution row's `assigned_build_id`. `None` means the execution
    /// predates any build policy, in which case no module can be resolved for it
    /// and the trampoline fails loudly rather than guessing a version.
    pub build_id: Option<String>,
    /// Host capabilities granted to the guest. Deny-all by default: a decider
    /// that could read a clock or draw randomness would be non-deterministic by
    /// construction, and no amount of build routing repairs that.
    pub capabilities: WasmCapabilities,
    /// Per-decision resource budget. Clamped to [`default_decide_limits`] on the
    /// way in — see [`with_limits`](Self::with_limits).
    pub limits: WasmLimits,
    /// Activity names the guest may schedule. `None` (the default) allows any
    /// registered activity.
    ///
    /// `Await` is the guest's one host-side capability, and deny-all
    /// [`WasmCapabilities`] does not constrain it at all: the sandbox governs
    /// what the guest may *import*, not what the host will do on its behalf. A
    /// module can otherwise schedule any activity the worker registry knows,
    /// read its output, and return it to the caller. An allowlist is the control
    /// for that.
    pub allowed_activities: Option<BTreeSet<String>>,
    /// Whether a guest may name the queue an activity is scheduled onto.
    ///
    /// `false` by default. Letting a guest pick any queue name in the shard is
    /// lateral movement, not configuration.
    pub allow_queue_override: bool,
}

impl ModuleHost {
    /// A host over `registry` with no build bound, deny-all capabilities, the
    /// default decide budget, no activity allowlist and no queue override.
    #[must_use]
    pub fn new(registry: Arc<ModuleRegistry>) -> Self {
        Self {
            registry,
            build_id: None,
            capabilities: WasmCapabilities::default(),
            limits: default_decide_limits(),
            allowed_activities: None,
            allow_queue_override: false,
        }
    }

    /// Bind the execution's assigned build id.
    #[must_use]
    pub fn with_build_id(mut self, build_id: impl Into<String>) -> Self {
        self.build_id = Some(build_id.into());
        self
    }

    /// Bind an optional build id — the shape the worker seam has, where a
    /// legacy execution carries no `assigned_build_id`.
    #[must_use]
    pub fn with_optional_build_id(mut self, build_id: Option<String>) -> Self {
        self.build_id = build_id;
        self
    }

    /// Tighten the per-decision resource budget.
    ///
    /// Every field is clamped **down** against [`default_decide_limits`]: these
    /// are safety ceilings, and an override exists to make a deployment stricter,
    /// never to lift a bound the safety analysis relies on. (The fields are
    /// `pub` for inspection; assigning them directly bypasses this clamp, which
    /// is why [`module_workflow_handler`] re-clamps at dispatch.)
    #[must_use]
    pub fn with_limits(mut self, limits: WasmLimits) -> Self {
        self.limits = clamp_decide_limits(limits);
        self
    }

    /// Restrict the activities the guest may schedule.
    #[must_use]
    pub fn allowing_activities<I, S>(mut self, names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.allowed_activities = Some(names.into_iter().map(Into::into).collect());
        self
    }

    /// Permit the guest to name the queue an activity is scheduled onto.
    #[must_use]
    pub const fn allowing_queue_override(mut self) -> Self {
        self.allow_queue_override = true;
        self
    }

    /// Override the guest capability grant.
    ///
    /// Granting a clock or randomness makes the hosted workflow
    /// non-deterministic and it will fail replay. Exposed for the safety
    /// analysis's benefit, not as a recommendation.
    #[must_use]
    pub fn with_capabilities(mut self, capabilities: WasmCapabilities) -> Self {
        self.capabilities = capabilities;
        self
    }
}

/// Clamp every field of `limits` down against [`default_decide_limits`].
#[must_use]
pub fn clamp_decide_limits(limits: WasmLimits) -> WasmLimits {
    let ceiling = default_decide_limits();
    WasmLimits {
        memory_bytes: limits.memory_bytes.min(ceiling.memory_bytes),
        fuel: limits.fuel.min(ceiling.fuel),
        max_wall_clock: limits.max_wall_clock.min(ceiling.max_wall_clock),
    }
}

tokio::task_local! {
    /// The [`ModuleHost`] in scope for the current workflow task.
    ///
    /// A task-local (not a thread-local) because a workflow future is polled
    /// across `.await` points and may resume on a different runtime thread; and
    /// not a process global because two builds' tasks run concurrently in one
    /// process and each must see its own binding.
    static MODULE_HOST: ModuleHost;
}

/// Run `fut` with `host` bound as the current module host.
///
/// Wrap the workflow-handler drive with this — in the worker seam, in
/// `WorkflowTestEnv::run`, and around `WorkflowReplayer` replays of hosted
/// histories.
pub async fn with_module_host<F: Future>(host: ModuleHost, fut: F) -> F::Output {
    MODULE_HOST.scope(host, fut).await
}

/// The [`ModuleHost`] bound to the current task, if any.
#[must_use]
pub fn current_module_host() -> Option<ModuleHost> {
    MODULE_HOST.try_with(Clone::clone).ok()
}

/// Whether `handler` is the module-hosting trampoline.
///
/// Lets the worker seam tell a module-hosted `WorkflowInfo` from a
/// statically-linked one without a second registration table, mirroring
/// `ActivityInfo::is_wasm_stub`. Used to resolve the module *before* the handler
/// runs, so a worker that cannot serve a build releases the task instead of
/// destroying the execution.
#[must_use]
pub fn is_module_hosted(handler: crate::info::WorkflowHandlerFn) -> bool {
    std::ptr::fn_addr_eq(
        handler,
        module_workflow_handler as crate::info::WorkflowHandlerFn,
    )
}

// ── the trampoline ────────────────────────────────────────────────────────────

/// Serialize a [`DecideRequest`] into exactly the bytes the guest receives.
///
/// Deliberately `serde_json::to_vec` on the **struct**, never a round trip
/// through [`serde_json::Value`]: a `Value`'s object is a `BTreeMap`, so going
/// through one would reorder the keys alphabetically and move `step` off the
/// fixed offset the WAT guests read it from. This is the one function whose
/// output the guests' fixed-offset assumption depends on, so it is public and
/// pinned by `decide_request_serialises_step_first_so_a_wat_guest_can_read_it`.
///
/// # Errors
///
/// The serializer's message if the request cannot be represented as JSON, or a
/// bounds message if the encoded request exceeds [`MAX_DECIDE_REQUEST_BYTES`].
pub fn encode_decide_request(request: &DecideRequest) -> Result<Vec<u8>, String> {
    let bytes = serde_json::to_vec(request)
        .map_err(|e| format!("failed to serialize decide request: {e}"))?;
    if bytes.len() > MAX_DECIDE_REQUEST_BYTES {
        return Err(format!(
            "decide request for step {} is {} bytes, over the {MAX_DECIDE_REQUEST_BYTES}-byte \
             ceiling; the accumulated activity results are too large to hand back to the guest",
            request.step,
            bytes.len()
        ));
    }
    Ok(bytes)
}

/// Invoke a guest for one decision.
///
/// Reuses the issue-#965 sandbox: same engine, same fresh per-invocation store,
/// same fuel/epoch/memory bounding, same bounds-checked linear-memory ABI, same
/// host-glue panic containment. The spike adds no second sandbox and no new
/// `unsafe`.
fn decide(
    host: &ModuleHost,
    limits: &WasmLimits,
    module: &LoadedWorkflowModule,
    request: &DecideRequest,
) -> Result<DecideResponse, String> {
    let request_bytes = encode_decide_request(request)?;

    let raw = crate::wasm_activities::invoke_wasm_guest_bytes(
        host.registry.store(),
        module.module(),
        &request_bytes,
        &host.capabilities,
        limits,
        Some(limits.max_wall_clock),
    )
    .map_err(|failure| {
        format!(
            "workflow module {} (build `{}`) failed at step {}: {}",
            module.descriptor().module_hash,
            module.descriptor().build_id,
            request.step,
            bound_guest_text(&failure.to_string()),
        )
    })?;

    serde_json::from_value(raw.clone()).map_err(|e| {
        format!(
            "workflow module {} returned a response the host cannot parse ({e}): {}",
            module.descriptor().module_hash,
            bound_guest_text(&raw.to_string()),
        )
    })
}

/// Classify an error returned by a `WorkflowContext` await.
///
/// Only a genuine activity outcome is the guest's business. A replay divergence
/// or a cancellation is the **engine** telling the host that this run cannot
/// continue, and handing it to the guest as ordinary data would let the guest
/// answer `Complete` over a history it demonstrably diverged from — sealing the
/// execution COMPLETED with a wrong result, and disabling the issue-#603
/// ND-blocking net that every other part of this design leans on.
///
/// `None` means "propagate this, it is not a step outcome".
fn outcome_for_guest(err: &HarvestError) -> Option<DecideOutcome> {
    match err {
        HarvestError::ActivityFailed {
            error_type,
            details,
            ..
        } => Some(DecideOutcome::Err {
            error_type: error_type.clone(),
            details: details.clone(),
            error: bound_guest_text(&err.to_string()),
        }),
        // NonDeterministic, Cancelled, PayloadTooLarge, Config, Database, ...
        // all mean "the engine cannot carry this run forward", not "your step
        // failed". They propagate.
        _ => None,
    }
}

/// The statically-linked [`WorkflowHandlerFn`](crate::info::WorkflowHandlerFn)
/// that hosts a runtime-loaded module.
///
/// Register this as the `handler` of any [`WorkflowInfo`](crate::info::WorkflowInfo)
/// whose body should come from a module instead of the binary. It is an ordinary
/// `fn` pointer, so nothing about `WorkflowInfo`, the worker dispatch path, the
/// executor or the replayer changes.
///
/// # What it does
///
/// Resolves the [`ModuleHost`] bound to the current task, resolves the module
/// for `(host.build_id, ctx.workflow_type())`, then runs the decide loop:
/// call the guest, await whatever activity it asks for through the ordinary
/// [`WorkflowContext`] surface, feed the outcome back, repeat until the guest
/// completes or fails.
///
/// # Determinism
///
/// The guest sees only its input and the outcomes of awaits it itself requested
/// — every one of which is replayed from history on a replay. It is granted no
/// clock and no randomness. So a hosted run emits exactly the command sequence
/// its statically-linked twin does, and the recorded history is byte-identical.
///
/// "Its statically-linked twin" means one that **catches** activity failures: a
/// failed activity is delivered to the guest as a [`DecideOutcome::Err`] and the
/// guest decides what it means, rather than the host propagating it. A native
/// handler using `?` is a *different* workflow, and its history differs on the
/// failure path. A guest that wants propagation returns [`DecideResponse::Fail`].
///
/// An engine-level error — a replay divergence, a cancellation — is **not** an
/// activity outcome and never reaches the guest; see [`outcome_for_guest`].
///
/// # Failure modes
///
/// All are returned as `Err(String)`, which the engine treats as an ordinary
/// workflow failure: no module host bound, no module for this build, a guest
/// trap or resource exhaustion, an unparseable or oversized response, a
/// disallowed activity or queue, or more than [`MAX_DECIDE_STEPS`] decisions.
///
/// The *resolution* failures (no host, no build, no module) are pre-empted by
/// the worker seam, which resolves the module before dispatch and releases the
/// task as a capability miss instead — so in a worker they are defence in depth
/// rather than the live path. They remain errors here for the test harness and
/// the replayer, which have no seam.
#[must_use]
pub fn module_workflow_handler(
    ctx: &WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + '_>> {
    Box::pin(async move {
        let host = current_module_host().ok_or_else(|| {
            "no module host bound for this task: wrap the workflow drive in \
             `hot_swap::with_module_host(...)` before dispatching a module-hosted workflow"
                .to_string()
        })?;
        let workflow_name = ctx.workflow_type().to_string();
        let build_id = host.build_id.clone().ok_or_else(|| {
            format!(
                "workflow `{workflow_name}` is module-hosted but this execution carries no \
                 assigned build id, so no module version can be resolved for it"
            )
        })?;
        let module = host
            .registry
            .get(&build_id, &workflow_name)
            .ok_or_else(|| {
                format!(
                    "no workflow module is loaded for build `{build_id}` and workflow \
                 `{workflow_name}`"
                )
            })?;
        // Re-clamp: `limits` is a `pub` field, so a caller can assign past
        // `with_limits`. These ceilings are what the safety analysis reasons
        // about, so the dispatch path must not trust the struct.
        let limits = clamp_decide_limits(host.limits);

        // Under DD-1 the workflow restarts at step 0 on every decision cycle.
        // The guest is a pure function of `(input, resolved)`, so a decision
        // taken earlier in *this* invocation can be reused rather than
        // recomputed — turning an O(n^2) cycle into O(1) new guest calls and
        // removing the unbounded per-poll occupancy that came with it.
        let mut memo: BTreeMap<u32, DecideResponse> = BTreeMap::new();
        let mut guest_time = Duration::ZERO;
        let mut resolved: Vec<DecideOutcome> = Vec::new();

        for step in 0..MAX_DECIDE_STEPS {
            let step_index = u32::try_from(step)
                .expect("step is bounded by MAX_DECIDE_STEPS, far below u32::MAX");
            let response = if let Some(cached) = memo.get(&step_index) {
                cached.clone()
            } else {
                if guest_time >= DECIDE_RUN_WALL_CLOCK {
                    return Err(format!(
                        "workflow module for build `{build_id}` exceeded the \
                         {DECIDE_RUN_WALL_CLOCK:?} cumulative guest budget for one decision cycle"
                    ));
                }
                let request = DecideRequest {
                    step: step_index,
                    workflow: workflow_name.clone(),
                    input: input.clone(),
                    resolved: resolved.clone(),
                };
                let started = Instant::now();
                let response = decide(&host, &limits, &module, &request)?;
                guest_time = guest_time.saturating_add(started.elapsed());
                memo.insert(step_index, response.clone());
                // Give the runtime a chance to preempt between fresh decisions.
                // The executor reads a `Poll::Pending` with a zero command delta
                // and a self-wake as a spin to keep driving (see `DriveStep`),
                // so this is safe here in a way a real host await is not.
                tokio::task::yield_now().await;
                response
            };

            match response {
                DecideResponse::Complete { output } => return Ok(output),
                DecideResponse::Fail { error } => return Err(bound_guest_text(&error)),
                DecideResponse::Await {
                    activity,
                    input: activity_input,
                    queue,
                } => {
                    // Refuse the await the cap could not consume, BEFORE
                    // scheduling it. Otherwise the last permitted step runs a
                    // real activity — money moved, mail sent — and the run is
                    // then failed for not terminating, having already paid for
                    // a side effect it can never use.
                    if step + 1 >= MAX_DECIDE_STEPS {
                        return Err(format!(
                            "workflow module for build `{build_id}` asked for another activity at \
                             step {step}, which would exceed the {MAX_DECIDE_STEPS}-decision \
                             ceiling; refusing to schedule it"
                        ));
                    }
                    if let Some(allowed) = &host.allowed_activities
                        && !allowed.contains(&activity)
                    {
                        return Err(format!(
                            "workflow module for build `{build_id}` asked to schedule activity \
                             `{}`, which this host does not allow",
                            bound_guest_text(&activity)
                        ));
                    }
                    let queue = match queue {
                        Some(_) if !host.allow_queue_override => {
                            return Err(format!(
                                "workflow module for build `{build_id}` asked to override the \
                                 activity queue, which this host does not allow"
                            ));
                        }
                        Some(queue) => queue,
                        None => ctx.queue_name().to_string(),
                    };
                    if queue.is_empty() {
                        return Err(format!(
                            "workflow module for build `{build_id}` resolved an empty activity \
                             queue for `{}`; no worker polls the empty queue, so the activity \
                             would never be picked up",
                            bound_guest_text(&activity)
                        ));
                    }
                    let outcome = ctx
                        .execute_activity_raw(&activity, activity_input, &queue)
                        .await;
                    resolved.push(match outcome {
                        Ok(output) => DecideOutcome::Ok { output },
                        Err(err) => outcome_for_guest(&err).ok_or_else(|| err.to_string())?,
                    });
                }
            }
        }
        Err(format!(
            "workflow module for build `{build_id}` did not terminate within \
             {MAX_DECIDE_STEPS} decide steps"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: &[u8] = b"a-sufficiently-long-operator-key";

    #[test]
    fn constant_time_eq_matches_plain_equality() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"abcd"));
        assert!(constant_time_eq(b"", b""));
    }

    #[test]
    fn decode_hex_round_trips_and_rejects_junk() {
        assert_eq!(decode_hex("00ff10"), Some(vec![0x00, 0xff, 0x10]));
        assert_eq!(decode_hex("00FF10"), Some(vec![0x00, 0xff, 0x10]));
        assert_eq!(decode_hex("0"), None, "odd length");
        assert_eq!(decode_hex("zz"), None, "non-hex");
    }

    #[test]
    fn signing_is_deterministic_and_binds_the_whole_identity() {
        let hash = compute_module_hash(b"some module bytes");
        let base = sign_module_binding(KEY, "wf-v1", "pipeline", &hash).expect("sign");
        assert_eq!(
            base,
            sign_module_binding(KEY, "wf-v1", "pipeline", &hash).expect("sign")
        );
        // Every component of the identity changes the tag, so a signed module
        // cannot be re-bound under a different build id or workflow name.
        assert_ne!(
            base,
            sign_module_binding(KEY, "wf-v2", "pipeline", &hash).expect("sign")
        );
        assert_ne!(
            base,
            sign_module_binding(KEY, "wf-v1", "payments", &hash).expect("sign")
        );
        assert_ne!(
            base,
            sign_module_binding(KEY, "wf-v1", "pipeline", &"0".repeat(64)).expect("sign")
        );
        assert_eq!(base.len(), 64);
    }

    #[test]
    fn length_prefixing_stops_field_boundary_collisions() {
        let hash = compute_module_hash(b"bytes");
        assert_ne!(
            sign_module_binding(KEY, "a", "bc", &hash).expect("sign"),
            sign_module_binding(KEY, "ab", "c", &hash).expect("sign"),
        );
    }

    #[test]
    fn a_short_signing_key_is_refused_rather_than_silently_accepted() {
        let hash = compute_module_hash(b"bytes");
        assert!(matches!(
            sign_module_binding(b"", "wf-v1", "pipeline", &hash),
            Err(HotSwapError::SigningKeyTooShort { actual: 0 })
        ));
        assert!(matches!(
            sign_module_binding(b"short", "wf-v1", "pipeline", &hash),
            Err(HotSwapError::SigningKeyTooShort { actual: 5 })
        ));
    }

    #[test]
    fn limits_clamp_down_and_never_up() {
        let ceiling = default_decide_limits();
        let lifted = clamp_decide_limits(WasmLimits {
            memory_bytes: usize::MAX,
            fuel: u64::MAX,
            max_wall_clock: Duration::from_secs(3600),
        });
        assert_eq!(
            lifted, ceiling,
            "an override must not lift a safety ceiling"
        );

        let tightened = clamp_decide_limits(WasmLimits {
            memory_bytes: 1024,
            fuel: 1,
            max_wall_clock: Duration::from_millis(1),
        });
        assert_eq!(tightened.memory_bytes, 1024);
        assert_eq!(tightened.fuel, 1);
        assert_eq!(tightened.max_wall_clock, Duration::from_millis(1));
    }

    #[test]
    fn the_wall_clock_backstop_sits_well_above_any_fuel_bounded_decision() {
        // If these could plausibly race, a run's terminal outcome would depend
        // on host load: the same history would fail on a busy worker and succeed
        // on an idle one. The backstop must be comfortably the looser bound.
        let limits = default_decide_limits();
        assert!(limits.max_wall_clock >= Duration::from_secs(1));
        assert!(DECIDE_RUN_WALL_CLOCK > limits.max_wall_clock);
    }

    #[test]
    fn guest_text_is_bounded_before_it_can_reach_history() {
        let huge = "x".repeat(MAX_GUEST_TEXT_BYTES * 4);
        let bounded = bound_guest_text(&huge);
        assert!(bounded.len() < huge.len());
        assert!(bounded.contains("bytes elided"));
        assert_eq!(bound_guest_text("short"), "short");
        // Multi-byte characters must not be split mid-character.
        let multibyte = "é".repeat(MAX_GUEST_TEXT_BYTES);
        let bounded = bound_guest_text(&multibyte);
        assert!(bounded.contains("bytes elided"));
    }

    #[test]
    fn only_activity_outcomes_are_handed_to_the_guest() {
        assert!(
            outcome_for_guest(&HarvestError::NonDeterministic {
                reason: "activity mismatch".to_string(),
                details: Box::new(crate::error::NonDeterministicDetails {
                    event_index: Some(3),
                    expected: Some("charge".to_string()),
                    actual: Some("refund".to_string()),
                    workflow_type: None,
                    build_id: None,
                }),
            })
            .is_none(),
            "a replay divergence must never be delivered to the guest as data: the guest \
             could answer Complete over a history it diverged from"
        );
        assert!(
            outcome_for_guest(&HarvestError::Cancelled("gone".to_string())).is_none(),
            "a cancellation must propagate, not become a guest-visible step failure"
        );
        assert!(
            outcome_for_guest(&HarvestError::Config("bad".to_string())).is_none(),
            "an engine config error must propagate"
        );

        let activity = HarvestError::ActivityFailed {
            name: "charge".to_string(),
            attempt: 3,
            error_type: "CircuitOpen".to_string(),
            details: Some(serde_json::json!({"retry_after_secs": 30})),
            source: Box::new(HarvestError::Config("declined".to_string())),
        };
        match outcome_for_guest(&activity) {
            Some(DecideOutcome::Err {
                error_type,
                details,
                ..
            }) => {
                assert_eq!(error_type, "CircuitOpen", "the stable class is carried");
                assert!(details.is_some(), "structured detail is carried");
            }
            other => panic!("an activity failure IS the guest's business, got {other:?}"),
        }
    }

    #[test]
    fn the_trampoline_is_recognisable_by_the_worker_seam() {
        assert!(is_module_hosted(module_workflow_handler));
        fn other(
            _ctx: &WorkflowContext,
            _input: Value,
        ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + '_>> {
            Box::pin(async move { Ok(Value::Null) })
        }
        assert!(!is_module_hosted(other));
    }

    #[test]
    fn an_oversized_decide_request_is_refused_before_the_guest_sees_it() {
        let request = DecideRequest {
            step: 0,
            workflow: "pipeline".to_string(),
            input: Value::String("x".repeat(MAX_DECIDE_REQUEST_BYTES + 1)),
            resolved: Vec::new(),
        };
        let err = encode_decide_request(&request).expect_err("over the ceiling");
        assert!(err.contains("ceiling"), "{err}");
    }
}
