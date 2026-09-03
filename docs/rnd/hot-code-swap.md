# Hot code swap for workflow definitions — R&D spike report (issue #967)

**Status: R&D spike, behind the `hot-code-swap` Cargo feature.** This is not a
committed GA feature. This document is the written deliverable issue #967 asks
for, backed by a working prototype: `autumn-harvest/src/hot_swap.rs` (host,
registry, trampoline), `autumn-harvest/src/hot_swap_store.rs` (Postgres
registry), the guests in `autumn-harvest/examples/workflow-modules/`, the worker
dispatch seam in `src/worker.rs`, and the suite in
`autumn-harvest/tests/integration/hot_code_swap_tests.rs`.

It is written to be **readable without the spike code**: a reader who only wants
the decision can read §1, §5 and §9.

---

## 1. The hosting question

Harvest's *routing* side of deploys is finished. Build-ID routing (#171),
compatibility sets, percent ramps (#604), reachability checks (#520/#535) and
version gates (`ctx.version`) already implement a world where two versions of the
same workflow are live at once and the engine knows exactly which executions may
see which. What is not finished is the *delivery* side: new workflow code still
arrives as a new binary, so shipping a one-line workflow fix means bouncing the
fleet — draining sessions (#606), cold workflow caches (#235), poll-loop gaps,
and, for embedders whose worker *is* their web app via the Autumn plugin, a full
application restart.

The R&D question: **can a live worker load workflow and activity definitions as
runtime modules**, so that a deploy becomes "publish a module, workers pick it up
under a new build id" and a rollback becomes "repoint the ramp" — with **zero
change to the replay surface**, because all the safety machinery that decides
which executions see which code already shipped?

The short answer, developed below: **as a matter of feasibility, yes** for a
restricted, honestly-bounded shape of workflow, via WebAssembly; and **no for
general Rust workflow bodies**, via any mechanism, for reasons that are
properties of Rust and of harvest's context design rather than of effort
available.

Feasible is not the same as advisable, and §9 explains why the verdict is still
*not a go* for workflow hosting: hosted workflows are a **second authoring
surface**, and that is a product commitment a spike cannot justify. The spike's
most durable output is §2 — the constraint inventory — and §5, the boundary that
satisfies it.

### What the competition does

Nobody in this cohort has restart-free code delivery.

| Engine | Delivery model | Restart-free? |
|--------|----------------|---------------|
| **Temporal** | New code ships as new workers; Worker Versioning (build IDs) governs routing — the model harvest matched with #171/#604 | No: fleet redeploy |
| **DBOS** | Library model; code changes are app redeploys. Versioned workflow recovery pins in-flight runs to the old app version (the same start-time-immutable idea) | No |
| **Inngest** | Functions are served by the app's own HTTP endpoint, so a deploy is the app platform's deploy | Only as fast as the platform redeploys |
| **Hatchet** | Worker redeploys | No |
| **Restate** | Closest in spirit: service deployments register with the runtime at versioned endpoints, and in-flight invocations pin to the deployment that started them — but the code lives in separately deployed processes | No: out-of-process |
| **Airflow** | DAG files re-parsed from disk — genuinely hot-ish | Yes, and it is the cautionary tale: import-time side effects and version skew are why harvest must gate swapped code behind build ids rather than silent re-parse |

Harvest is unusually well prepared on the governance side and unusually badly
placed on the mechanism side. That asymmetry is the whole finding.

---

## 2. Hard constraints the host boundary must satisfy

These are the constraints any hosting option must satisfy, derived from the live
tree. The docs-guard suite
(`autumn-harvest/tests/integration/hot_code_swap_docs.rs`) re-derives the
falsifiable ones at test time, so this inventory cannot quietly rot.

### C1 — Handlers are bare `fn` pointers

```rust
pub type WorkflowHandlerFn =
    fn(&WorkflowContext, serde_json::Value)
        -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + '_>>;
```

`WorkflowInfo::handler` and `ActivityInfo::handler` are `fn`, not
`Box<dyn Fn>` — design decision #6 in `docs/architecture.md`, taken so
`WorkflowInfo` is `Sync` without an `Arc`. A `fn` pointer is a *static* address.
**No runtime-loaded module can mint one**, and widening the type to
`Arc<dyn Fn>` to make room for one would touch every registration site, the
macros, and the `Sync` argument DD-6 rests on.

*Consequence:* the module cannot **be** the handler. Something statically linked
has to be, and it has to find the module at call time.

### C2 — Macro-generated code assumes `::autumn_harvest::` paths and in-process types

`#[workflow]` expands to code that names `::autumn_harvest::context::WorkflowContext`,
`::autumn_harvest::error::HarvestError`, and the crate's serde glue. A module
compiled separately would have to link against the *same* monomorphised instances
of those types — not merely types with the same name. For a dylib that is the
stable-ABI problem (C4); for WASM it is impossible outright, because the guest
has no access to host types at all.

*Consequence:* a hosted workflow cannot be "the same `#[workflow]` body,
elsewhere". It is a different authoring surface, and the report must say so
rather than imply a lift-and-shift.

### C3 — `WorkflowContext` is re-entrant, async, and suspension-driven

Under DD-1 the coroutine stays in memory and durability comes from the event
history: `ctx.execute_activity_raw(...)` suspends the workflow future via a
oneshot channel, and the executor re-invokes the workflow from the top on each
replay cycle. A hosted body must therefore either (a) be able to call back into
host async code and be suspended mid-call, or (b) never be suspended at all.

WASM core modules cannot do (a): a guest call is a synchronous host stack frame,
and there is no mechanism to unwind and resume it across a durable suspension.

*Consequence:* the module interface must be **re-entrant by re-invocation** — a
pure decision function called once per await — not a continuation.

### C4 — Rust has no stable ABI

`WorkflowContext`, `WorkflowInfo`, `HarvestError`, `serde_json::Value` are all
`repr(Rust)`. Field order, niche layout and enum discriminants are unspecified and
may differ between two compilations of *the same source* under different compiler
versions, feature flags, or optimisation settings. Passing any of them across a
`dlopen` boundary is undefined behaviour unless host and module are built as one
compilation unit — which is exactly the thing a hot swap is trying to avoid.

### C5 — The executor cannot distinguish a host await from a workflow suspension

`executor::poll_query_step` classifies a `Poll::Pending` with a **zero**
replay-significant command delta as a cold park. A handler that awaits something
host-side — a `tokio::task::spawn_blocking` join handle, an HTTP call to a
sidecar, a channel — is therefore read by the engine as *the workflow suspended*.

*Consequence:* whatever the host does to obtain a decision from a module must be
**synchronous** with respect to the workflow future, and must therefore be
bounded tightly enough that blocking a runtime worker for that long is acceptable.
This constraint also rules out the naive "sidecar over HTTP" hosting shape without
executor changes, which are out of scope.

### C6 — Panic containment must not regress

Local activity dispatch already wraps construction and poll in
`catch_construct` + `catch_unwind` (issue #782) so a handler panic becomes a
typed retryable error rather than unwinding the worker. Any hosting option must
be at least as contained.

### C7 — The replay surface must not move

`harvest_events` is append-only, with exactly **two** sanctioned in-place
writers (`erase.rs`, `codec_rotation.rs`; see `CLAUDE.md`). Hosting must add no
third writer, no new `WorkflowEvent` variant, no change to event JSON, and no
change to `HistoryMatcher` or executor semantics. This is not a nice-to-have: a
history recorded by module-hosted code must replay under statically-linked code
of the same logic, or an operator can never roll a swap back into a binary.

### C8 — Module identity must be governable by the shipped machinery

The premise of the issue is that hot swap needs **no new safety machinery**. So
whatever identifies a module version must be exactly a `BuildId`, and nothing
else — otherwise ramp, compatibility and reachability all need parallel
implementations for modules.

---

## 3. Option A — dylib hosting (`libloading`)

**Shape.** Compile each workflow crate as a `cdylib` exporting a C-ABI
registration symbol; the worker `dlopen`s it and obtains handler entry points.

**What works.** Native speed, no sandbox tax, no new authoring surface in the
happy case, and — uniquely — the module *could* in principle hand back a real
`fn` pointer, satisfying C1 directly.

**Why it is refused.**

1. **C4 is fatal, not merely risky.** The registration symbol would have to hand
   over `WorkflowInfo` (containing `&'static str`, `Option<Duration>`,
   `Option<ConcurrencyPolicy>`, …) and the handler would receive
   `&WorkflowContext` and `serde_json::Value`. All are `repr(Rust)`. There is no
   supported way to make this sound across independent compilations; `abi_stable`
   or `stabby` can make a *bespoke* C-ABI-safe façade sound, but the façade would
   have to re-express `WorkflowContext`'s entire ~160-method surface, which is
   both an enormous ongoing maintenance surface and, by C2, still not the
   `#[workflow]` authoring experience.
2. **Unload is a use-after-free, not a refcount decrement.** A suspended
   execution's in-memory coroutine (DD-1) holds code *and* data belonging to the
   module. `dlclose` while any future still references it unmaps live code. The
   only sound policies are "never unload" (memory grows without bound over
   repeated swaps — see §8) or "unload only after reachability says the build has
   no open executions *and* no cached coroutine anywhere in the fleet", which is
   an unbounded distributed wait.
3. **Panic across the FFI boundary is UB.** Unwinding out of an `extern "C"`
   function aborts the process (or worse, on older toolchains). Satisfying C6
   requires every module entry point to `catch_unwind` internally — enforceable
   only by convention, i.e. not enforceable.
4. **Allocator and global-state mismatch.** A `Vec` allocated in the module and
   freed in the host is UB unless both use the same allocator instance; `tracing`
   subscribers, `once_cell` statics and thread-locals are all duplicated per
   module.
5. **Blast radius is the whole process.** A module bug is host memory corruption,
   with no containment boundary at all — in an engine whose entire value
   proposition is durable correctness.

**Verdict on Option A: no-go, at any tier.** Not "expensive"; unsound. The one
scenario that would revive it is a first-party-only deployment where host and
modules are built from one workspace by one toolchain invocation and are
*guaranteed* version-locked — at which point the modules could simply have been
linked in, and the exercise has no purpose.

---

## 4. Option B — WebAssembly hosting

**Shape.** The module is a WebAssembly module executed by the wasmtime engine
harvest already embeds for WASM activities (issue #965), under deny-all
capabilities and per-invocation fuel / epoch / memory bounds.

**How it lands against the constraints.**

| Constraint | WASM |
|---|---|
| C1 `fn` pointers | Satisfied indirectly: one statically-linked trampoline `fn` is the handler; the module is data it resolves (see §5) |
| C2 macro-generated paths | Not satisfied and not attempted — a guest is authored against the decide ABI, not `#[workflow]`. This is the honest cost |
| C3 suspension | Satisfied by re-invocation: the guest is a pure decision function, never suspended mid-call |
| C4 stable ABI | **Not applicable** — WASM's own ABI is specified; the payload is JSON over linear memory |
| C5 synchronous host | Satisfied: the guest call is a synchronous frame, bounded by a tight decide budget |
| C6 panic containment | Satisfied and strengthened: a guest trap is a typed error, and host glue is itself `catch_unwind`-wrapped |
| C7 replay surface | Satisfied by construction (§7) |
| C8 build-id identity | Satisfied: the registry's primary key *is* `(build_id, workflow_name)` (§6) |

### Core module vs the component model

The spike uses the **core-WASM** module model with the JSON-over-linear-memory
`memory` / `alloc` / `run` ABI issue #965 already established, for the same
reasons that spike gave: every language that targets `wasm32-unknown` can export
three functions, no WIT toolchain is required, and the point of a spike is to
prove the dispatch/sandbox/routing shape rather than front-load IDL cost.

**For GA the component model + a WIT world is the right target**, again matching
#965's recommendation: typed, versioned interfaces instead of a
"both-sides-agree-on-JSON" convention, and a canonical ABI that removes the
hand-rolled pointer packing. The decide-loop *shape* below is orthogonal to that
choice and survives the migration unchanged.

### Cost of the sandbox

The guest pays a compile on first load (cached thereafter by the shared
`WasmModuleStore` LRU), plus a JSON serialise/parse per decision. For a decider —
which is by construction a small pure function — that is not the bottleneck; the
activity it schedules dominates by orders of magnitude. What the sandbox *does*
cost is expressiveness, and §5 is explicit about how much.

---

## 5. The recommended host boundary

**The module is a pure decision function, re-invoked once per await, reached
through one statically-linked trampoline.**

```text
run(DecideRequest) -> DecideResponse

DecideRequest  { step, abi_version, workflow, input, resolved: [DecideOutcome] }
DecideResponse = Await { activity, input, queue? }
               | Complete { output }
               | Fail { error }
```

The host loop, in `hot_swap::module_workflow_handler`:

1. Resolve the `ModuleHost` bound to the current task; resolve the module for
   `(execution's assigned build id, ctx.workflow_type())`.
2. Call the guest with `step = 0` and an empty `resolved`.
3. On `Await`, schedule the activity through the ordinary
   `ctx.execute_activity_raw(...)` surface, append the outcome to `resolved`,
   and call the guest again at `step + 1`.
4. On `Complete` / `Fail`, return.

### Why this boundary and not the alternatives

* **Not "re-link native handler fns"** — C1 and C4 make it unsound (§3).
* **Not "the guest calls host functions to schedule activities"** — that is the
  continuation shape C3 forbids: the guest's stack frame cannot survive a durable
  suspension.
* **A serialized-command interface** is what this is, in the issue's vocabulary —
  but expressed as a *loop over single commands* rather than a batch. A batch
  interface ("here is the history, emit all commands") would be closer to
  Temporal's decider model and would allow concurrency (`join!`); it is strictly
  more powerful and strictly harder, because the host must then translate a
  command batch back into `ctx` awaits and reconcile partial completion. §9 tiers
  it.

### The determinism implication

The guest sees **only** its own input and the outcomes of awaits it itself
requested. Every one of those outcomes is history-backed: on replay the engine
returns the recorded result rather than re-running the activity. The guest is
granted no clock and no randomness (`WasmCapabilities` is deny-all, and a hosted
decider that were granted either would fail replay — the module host exposes
`with_capabilities` for the safety analysis's benefit, not as a recommendation).

Therefore a hosted run emits exactly the command sequence its statically-linked
twin emits, and this is *proved*, not asserted — see §7.

### The routing rule, and the trap next to it

The trampoline resolves the module by the **execution's** assigned build id,
threaded in the worker seam from `prepared.execution.assigned_build_id`.

It must **not** use `ctx.build_id()`. That reports the *worker's own configured*
`WorkerConfig::build_id`, deliberately so since issue #798, because a
pre-promotion replay gate needs to ask "what will the candidate build do with
these in-flight histories?". It is not the execution's `assigned_build_id`.
Routing modules on it would hand a v1-assigned in-flight execution to v2 code the
moment an operator relabelled a worker — precisely the divergence build routing
exists to prevent. `the_module_is_chosen_by_the_executions_build_not_by_the_workers_build_id`
pins the distinction.

### The cost this boundary carries: quadratic guest invocations

Under DD-1 the executor re-invokes the workflow from the top on every decision
cycle, replaying recorded events until it reaches the suspension point. The
trampoline's loop therefore restarts at `step = 0` each cycle, and a run with `n`
awaits performs `0 + 1 + … + n = O(n²)` guest invocations in total — against
`O(n)` for a statically-linked handler, whose replayed awaits are near-free.

For the workflows this boundary can express (short, sequential), the constant is
small: a decision is a fresh `Store`, a JSON round trip and a handful of guest
instructions, and the activity it schedules dominates by orders of magnitude. But
it is a real asymptotic cost of *re-invocation* as the re-entrancy strategy, and
it should be named rather than discovered later:

* it is why `MAX_DECIDE_STEPS` (512) is a ceiling rather than a target, and why
  `ModuleHost::with_max_decide_steps` can only tighten it;
* the obvious GA mitigation is a per-cycle memo of `step -> DecideResponse`,
  which is sound precisely because the guest is a pure function of
  `(input, resolved)` — the same property §7 rests on. It collapses the cycle's
  cost to `O(1)` new decisions and the run's to `O(n)`.

The other cost is C5's: a decision runs inline on the decision-cycle thread, so
`DECIDE_MAX_WALL_CLOCK` (500 ms) is also the worst case for how long one workflow
task can occupy a runtime worker. That is a policy number, not a law, and a
deployment with many hosted workflows should size its worker pool knowing it.

### What a hosted workflow can and cannot express

**Can:** sequential activities with arbitrary guest-side logic between them,
including branching on activity results; terminal success and typed-string
failure; per-call queue overrides.

**Cannot, in this spike:** concurrent waits (`join!`, `select!`), timers,
signals, queries, updates, child workflows, sagas, local activities,
continue-as-new, `ctx.version` / `ctx.patched`, or any other `WorkflowContext`
surface. Each is a further command kind in the ABI, and the concurrent ones need
the batch interface above.

Two consequences are worth stating rather than leaving to be discovered:

* **Queries against a hosted workflow do not work.** `WorkflowHandle::query`
  re-drives the handler to reach the query point, and it does so from a *client*,
  which holds no `ModuleRegistry` — that is worker-side state. The trampoline
  therefore reports "no module host bound" on the first poll. The same applies to
  the `run_workflow_canary` / `run_workflow_strict` pre-promotion replay gate,
  which is awkward given §5 cites that gate's semantics as a design motivation:
  the gate cannot currently verify a hosted build unless the caller wraps it in
  `with_module_host` itself. Threading an optional host through those entry
  points is T2 work.
* **The step ceiling is a fleet-wide constant, not configuration.** `MAX_DECIDE_STEPS`
  decides a terminal outcome, so a per-worker override would make an execution's
  fate depend on which worker claimed it. An earlier cut had one; it was removed
  rather than documented.

This is the honest ceiling of the prototype, and §9 prices raising it.

---

## 6. Module registry design

### Storage

One table, in the database the engine already owns. **The Postgres-only core
invariant holds; no new infrastructure.**

```sql
CREATE TABLE harvest_workflow_modules (
    build_id      TEXT NOT NULL,
    workflow_name TEXT NOT NULL,
    module_hash   TEXT NOT NULL,   -- lowercase-hex SHA-256 of module_bytes
    module_bytes  BYTEA NOT NULL,
    signature     TEXT,            -- hex HMAC-SHA256 over module_hash; NULL = unsigned
    published_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (build_id, workflow_name)
);
CREATE INDEX idx_harvest_workflow_modules_hash
    ON harvest_workflow_modules (module_hash);
```

**The primary key is the design.** `(build_id, workflow_name)` says two things at
once:

1. **A build id names exactly one module for a workflow, immutably.** That mirrors
   the start-time immutability of `assigned_build_id`: an execution's build is
   fixed when it starts, so the code that build denotes must be fixed too, or the
   execution's meaning changes underneath it. `publish_workflow_module` refuses a
   rebind (`a_build_ids_module_binding_is_immutable`); republishing identical
   bytes is idempotent.
2. **Two modules can never claim one workflow name outside build-id governance** —
   the determinism hazard the AC names, mirroring #597's duplicate-registration
   hardening. They can coexist only under *different* build ids, which is exactly
   the case ramp / compatibility / reachability were built for.

There is deliberately **no `active` flag**. Which module a new execution lands on
is decided by `harvest_build_policies` and its percent ramp; which module an
in-flight execution keeps is decided by its recorded `assigned_build_id`. A second
switch here would be a second source of truth for the same question, and the two
would eventually disagree. This is the one place the design diverges from the
`harvest_wasm_modules` precedent (#965), which does carry an `active` flag —
because an *activity* module has no build-routed identity to defer to.

### Lifecycle

| Step | API | Enforcement |
|------|-----|-------------|
| **discover** | `list_workflow_modules_for_build` | — |
| **fetch** | `fetch_workflow_module` | — |
| **verify** | `sync_build_into_registry` | SHA-256 content check first, then HMAC signature |
| **load** | `ModuleRegistry::load_module` | compiles via the shared `WasmModuleStore`, binds `(build_id, workflow_name)` |
| **unload** | `ModuleRegistry::unload_build` | drops the binding; `Arc` keeps in-flight invocations valid |
| **retire** | `retire_build_modules` | call once `build_reachability(...).safe_to_retire` |

`sync_build_into_registry` is the whole worker-side lifecycle in one call — run at
startup for the worker's own build, and run again, **with no restart**, when an
operator publishes a build the worker must begin serving.

### Verification

Two independent gates, cheapest first:

* **Content addressing.** The bytes are re-hashed and compared to the stored
  `module_hash` on *every* load, so a row whose payload was altered without
  updating the hash fails closed rather than executing unreviewed code
  (`syncing_refuses_a_module_whose_stored_bytes_were_tampered_with`).

  **What this is not.** `module_hash` is an ordinary column in the same row as
  `module_bytes`, so content addressing is a *consistency* check between two
  columns a writer controls together — it detects corruption and partial writes,
  not an authorized-looking write. An attacker who updates both columns coherently
  passes it. Write access to this table **is** code execution unless signatures
  are configured; §8.6 says so, and this sentence exists because an earlier draft
  of it claimed the opposite.

* **Signatures.** A detached lowercase-hex HMAC-SHA256 over the whole **binding** —
  the length-prefixed, domain-separated tuple `(build_id, workflow_name,
  module_hash)` — under an operator-configured key, compared in constant time on
  decoded bytes.

  Signing the *binding* rather than the bytes alone is load-bearing. A signature
  over the content hash alone says "these bytes were approved" but not "approved
  *as this*", so a writer could copy any existing row's `(hash, signature)` pair
  and re-bind it under a different build id — a **downgrade**, resurrecting a
  superseded but signed module under whichever build the ramp points at — or under
  a different workflow name — a **substitution**, running workflow A's logic on
  workflow B's inputs. Both are pinned:
  `a_signed_module_cannot_be_rebound_under_another_build_id` and
  `..._under_another_workflow_name`. The "verifiable from a bytes-free listing"
  property survives, because a listing already carries all three fields.

  A worker configured with a key refuses an unsigned module; a worker with no key
  does not treat a present signature as load-bearing. Keys shorter than
  `MIN_SIGNING_KEY_BYTES` are refused outright, because an unset environment
  variable would otherwise yield a signature anyone can compute, silently.

An asymmetric scheme (Ed25519) is the GA answer, and not only for publisher
convenience: **HMAC means every verifier is a forger.** The key must be present on
every worker in order to verify, so any worker compromise, config leak or core
dump yields the ability to mint valid signatures — against an attacker who, by
the threat model in §8.6, already has or can get the database write. The spike's
signature detects an outsider tampering with the table; it is not a code-provenance
control and does not survive a worker compromise. HMAC keeps the spike
dependency-free (`hmac`/`sha2` are already direct dependencies); GA should not
keep it.

**Fail-closed, and genuinely whole-build.** Every module in a build is fetched,
verified and compiled *first*; only then are they all bound, under one lock, by
`ModuleRegistry::load_modules`. A build whose third module fails verification
therefore leaves the first two **unbound**.

The ordering is the whole point, and the first cut of this spike got it wrong: a
per-module loop bound each module as it verified, so a failing row left the worker
advertising a build it could only half-serve — claiming executions for the
workflows it had, and destroying every execution for the one it did not. Both
fail-closed tests published a single module, so `registry.is_empty()` passed
trivially and never exercised the claim.
`a_failing_module_leaves_the_whole_build_unbound` publishes two and tampers with
the one that sorts *second*.

Payloads are also fetched **one at a time**, so peak host residency is one module
rather than a whole build, and compilation runs on `spawn_blocking` — Cranelift is
neither fuel- nor epoch-bounded, and the rule that it must not occupy an async
worker thread is the one `wasm_store` established for guest invocation.

### How the shipped machinery governs a swap, unchanged

| Question | Answered by | New code required |
|----------|-------------|-------------------|
| Which *new* executions land on the swapped code? | `BuildPolicy::resolve_assigned_build` + `set_build_ramp` (#604) | none |
| Which *in-flight* executions may a module process? | `BuildCompatibilitySet::is_eligible` + claim-time filtering (#171) | none |
| When is the old module retirable? | `build_reachability(...).safe_to_retire` (#520/#535) | none |
| How do I roll back? | `clear_build_ramp` (or `set_build_ramp(..., 0)`) | none |

A worker hosting v1 and v2 simultaneously keeps its own single
`WorkerConfig::build_id` (say `host-1`) and declares compatibility
`host-1 → wf-v1` and `host-1 → wf-v2`, so claim-time filtering admits both. No
"multi-build worker" concept is introduced.

**Rollback is repointing the ramp, not touching the registry.** Clearing the ramp
stops new starts reaching the build immediately, while executions already assigned
to it finish on the code they started with — the start-time-immutable
`assigned_build_id` invariant, doing exactly its job.

---

## 7. Zero replay-surface change

**The claim.** Hot swap changes *where code comes from*, never *what the engine
records*. Specifically:

* no new `WorkflowEvent` variant;
* no change to any event's JSON;
* no change to `HistoryMatcher` or executor semantics;
* no third in-place writer of `harvest_events.event_data` (the two sanctioned
  ones, `erase.rs` and `codec_rotation.rs`, are untouched);
* no read or write of a module hash, build id, or anything else about hosting into
  history.

**Why it holds by construction.** The trampoline is an ordinary
`WorkflowHandlerFn`. Everything downstream of it — command emission, event
recording, matching, suspension — is the code path a statically-linked handler
takes, byte for byte. The only thing that differs is *which logic decides what to
emit*, and that logic is a pure function of history-backed values.

**Why that is proved rather than asserted.** Four tests, all in
`hot_code_swap_tests.rs`:

| Test | What it proves |
|------|----------------|
| `a_module_hosted_history_is_byte_identical_to_the_statically_linked_one` | The recorded event stream of a module-hosted run equals its statically-linked twin's, field for field, with one normalisation: `activity_id` is a per-dispatch UUID that differs between two runs of the *same* native handler, so it is renamed positionally rather than compared |
| `a_module_hosted_history_replays_clean_under_statically_linked_code` | The cross-hosting replay the AC names: a module-produced history replays `ReplaySucceeded` against native code |
| `a_statically_linked_history_replays_clean_under_module_hosting` | The reverse — the direction an operator actually depends on when swapping a running system |
| `hosting_never_introduces_a_new_event_variant` | The event *kind* sequence is identical, so no hosting-specific variant can creep in unnoticed |

Two further guards sit under the same claim: `only_activity_outcomes_are_handed_to_the_guest`
(an engine error never becomes guest data — see below) and
`an_activity_failure_is_handed_to_the_guest_rather_than_failing_the_run` (the
failure-path semantics, stated rather than implied).

The migration this spike adds (`20260903161045_harvest_workflow_modules`) touches
no existing table and no event.

### What "the statically-linked twin" means, precisely

A hosted workflow's twin is a native handler that **catches** activity failures,
not one that uses `?`. The trampoline delivers a failed activity to the guest as a
`DecideOutcome::Err` and lets the guest decide what it means — because a host that
failed the run on the guest's behalf would make saga and compensation logic
inexpressible in a module. A native `?` handler is therefore a *different*
workflow, and the two histories differ on the failure path: one records
`WorkflowFailed`, the other `WorkflowCompleted`. That is pinned, not glossed, by
`an_activity_failure_is_handed_to_the_guest_rather_than_failing_the_run`, which
asserts both halves. A guest that wants propagation returns `DecideResponse::Fail`.

### The divergence detector must not be handed to the guest

An **engine** error is not a step outcome, and this is the sharpest edge in the
whole design. `execute_activity_raw` returns `HarvestError::NonDeterministic` when
replay diverges. An earlier cut of the trampoline converted *every* error into a
`DecideOutcome::Err`, so a divergence arrived at the guest as ordinary data — and
a guest that answered `Complete` would seal the execution COMPLETED over a history
it demonstrably diverged from, with the replay gate reporting `ReplaySucceeded`,
because `nd_details` alone does not fail the executor's success arm. That is the
#603 ND-blocking net switched off for every hosted workflow, by the one component
whose whole justification is that it does not need new safety machinery.

`outcome_for_guest` now hands the guest **only** `HarvestError::ActivityFailed`.
Divergences, cancellations, payload-limit and config errors propagate as the
engine intended. `only_activity_outcomes_are_handed_to_the_guest` pins the
classification.

---

## 8. Safety analysis

### 8.1 Panic / UB blast radius

| Failure | dylib hosting | WASM hosting (adopted) |
|---|---|---|
| Module bug corrupts memory | Host address space; arbitrary corruption of unrelated executions | Confined to the guest's linear memory; host memory unreachable |
| Module panics / traps | Unwinding across `extern "C"` aborts the process | Typed error → ordinary workflow failure (`a_trapping_guest_is_contained_as_a_workflow_error`) |
| Host glue panics | n/a | `catch_unwind` in the guest-invocation wrapper converts it to a typed failure |
| Module reads host state it was not given | Trivially (shared address space) | No *linked host function* exists — capabilities are deny-all. But see §8.7: `Await` is a host capability by another name, and deny-all does not constrain it |
| Malicious module bytes | Arbitrary code execution as the worker | Bounded by the sandbox; and content+signature verification gates loading in the first place |

The spike introduces **no new `unsafe`** and no second sandbox: guest invocation
enters `wasm_activities::invoke_wasm_guest_bytes`, a sibling entry point that
shares `invoke_wasm_activity_inner` — the same engine, the same fresh
per-invocation `Store`, the same fuel / epoch / memory bounding, the same
bounds-checked linear-memory ABI, the same output ceiling and the same
`catch_unwind` host-glue containment as the activity path.

The one difference is where serialization happens, and it is load-bearing. The
activity path takes a `serde_json::Value`; a `Value`'s object is a `BTreeMap`, so
its keys reach the guest in **alphabetical** order. That is invisible to an
activity guest, which parses JSON — but §5's ABI pins `step` to a fixed byte
offset so a hand-written WAT guest can read it without a parser, and that only
holds if the bytes carry the *struct's* declaration order. The byte entry point
hands the caller's exact bytes to the guest, unmediated by `Value`.

This was found the hard way: the first cut routed the request through
`serde_json::to_value`, every guest read its step as `'r' - '0'` (the `a` of
`abi_version` having sorted to the front), and every hosted workflow silently
completed at step 0 without scheduling anything. Nine tests caught it — but the
ABI guard test did *not*, because it asserted on a local `serde_json::to_vec` of
the struct rather than on the bytes the host actually sends. `encode_decide_request`
now exists precisely so there is one encoder, and
`decide_request_serialises_step_first_so_a_wat_guest_can_read_it` and
`the_hosts_encoder_never_reorders_keys_the_way_a_json_value_would` both assert
against it. A guard that tests a path production does not take is worse than no
guard, and this one had to be rewritten to earn its place.

### 8.2 Module unload hazards

The hazard is unloading a module while an in-flight task still holds its code.

* **dylib:** use-after-free. A suspended execution's in-memory coroutine (DD-1)
  holds module code and data; `dlclose` unmaps it. There is no sound local policy
  — see §3.
* **WASM (adopted):** structurally safe. `ModuleRegistry::get` hands out an `Arc`,
  so `unload_build` removes the *binding* while an invocation that already
  resolved the module keeps the code alive until it finishes
  (`unloading_a_build_drops_its_modules_but_not_a_live_holder`). Unload is
  therefore safe to call at any time; reachability decides when it is *useful*,
  not when it is *legal*.

### 8.3 Memory growth under repeated swaps

Three bounds, none of them "hope":

1. **Unbound compiled code** lives in the shared `WasmModuleStore`'s LRU
   (`WASM_MODULE_CACHE_CAP`, 64 by default), keyed by content hash. Two builds
   publishing identical bytes share one compiled module.
2. **Bound compiled code is *not* bounded by that LRU**, and it is worth being
   precise about this rather than counting it twice. A binding holds its own
   `Arc<wasmtime::Module>`, so it pins that compiled code resident for as long as
   the binding exists — eviction frees nothing while a binding lives. A fleet
   that swapped a thousand times and never retired would hold a thousand
   *resident compiled modules*, not a thousand pointers. The real bound is
   therefore `unload_build` + `retire_build_modules`, gated on
   `build_reachability(...).safe_to_retire` — i.e. operator discipline, which
   §8.3's last paragraph already names as the residual risk.
3. **Per-invocation memory** is bounded by `DECIDE_MEMORY_BYTES` (4 MiB) against a
   fresh `Store` per decision, so a leaky guest leaks nothing across decisions.

The residual risk is operator discipline: nothing *forces* retirement. A GA
version should reap automatically on the reachability signal.

### 8.4 Two modules registering the same workflow name

The determinism hazard the AC names explicitly. Rejected at two layers:

* **In-process:** `ModuleRegistry::load_module` refuses to rebind a
  `(build_id, workflow_name)` to different bytes
  (`two_modules_may_not_claim_one_workflow_name_under_one_build_id`), re-checked
  under the write lock so two concurrent syncs cannot both win.
* **In the registry:** the primary key makes the row physically unique, and
  `publish_workflow_module` uses `ON CONFLICT DO NOTHING` and then adjudicates —
  identical bytes are idempotent, different bytes are refused
  (`a_build_ids_module_binding_is_immutable`).
* **Across retirement:** retirement is a `retired_at` **tombstone**, not a
  `DELETE`, because the row *is* the guarantee. Deleting it would free the primary
  key and let `wf-v1` be republished with new code — and an execution still parked
  on a long timer under `wf-v1` would resume on logic it never started under.
  `a_retired_build_id_still_cannot_be_repointed_at_different_code` pins it.

Two versions of a workflow coexisting is not the hazard; it is the *feature*. The
hazard is two versions coexisting **outside build-id governance**, and that is
what is impossible here.

### 8.5 The guest's real capability is `Await`, and deny-all does not touch it

The sandbox governs what a guest may **import**. It says nothing about what the
host will do on the guest's behalf — and the host will schedule an activity,
hand back its output, and do it again, once per decision. That is a host
capability, and for most of this spike's life it was unrestricted along three
axes:

* **which activity** — any name the worker's registry knows. A module could ask
  for `export_customer_pii`, read the result out of `resolved`, and return it
  through `Fail { error: <the data> }`, where it lands in a durable, readable
  failure message. That is "reading host state it was not given", through the
  front door.
* **which queue** — any queue name in the shard, i.e. lateral movement.
* **how many** — bounded only by the step ceiling.

`ModuleHost` now carries `allowed_activities` (an optional allowlist) and
`allow_queue_override` (**off** by default), pinned by
`a_guest_may_not_schedule_an_activity_the_host_did_not_allow` and
`a_guest_may_not_pick_the_queue_unless_the_host_allows_it`. A GA default should
be narrower still: the activities the workflow's own registration declares.

This section exists because the first version of this analysis reasoned carefully
about the channel that is closed and never mentioned the one that is open by
design — which is a worse failure in a safety analysis than an unmitigated risk
honestly named.

### 8.6 Resource exhaustion by a guest

* **CPU:** wasmtime fuel (`DECIDE_FUEL`, a tenth of the activity default).
* **Wall clock:** an epoch deadline (`DECIDE_MAX_WALL_CLOCK`, 5 s) as a
  **backstop**, not as the operative budget. It exists for the one class fuel
  cannot bound — bulk-memory instructions cost one fuel unit regardless of bytes
  moved — and is set generously *above* any fuel-bounded decision precisely so
  the two do not race. A tight wall-clock ceiling would make a run's terminal
  outcome depend on host load: the same history failing on a busy worker and
  succeeding on an idle one is exactly the non-determinism this spike exists to
  avoid. `a_spinning_guest_is_bounded_by_fuel_and_the_epoch_deadline` asserts
  only that an unbounded guest is stopped inside the budget; it does not (and
  cannot, with a `br`-looping guest that does consume fuel) distinguish which of
  the two bounds fired.
* **Cumulative:** `DECIDE_RUN_WALL_CLOCK` bounds the guest time of a whole
  decision cycle, so per-decision budgets cannot be composed into unbounded
  occupancy of a runtime worker thread.
* **Decision count:** `MAX_DECIDE_STEPS` (512). Without it a guest answering
  `Await` forever would append activity events without bound — a durable,
  replayable denial of service rather than a transient one
  (`a_guest_that_never_completes_is_stopped_by_the_decide_step_cap`).

By C5 a decision runs **inline on the decision-cycle thread**, so guest time is
runtime-worker time. It is tempting to say the per-decision ceiling is therefore
the worst case for one workflow task — and that would be wrong, which is worth
recording because the first draft said it.

On a replay cycle the trampoline restarts at step 0 and re-resolves every prior
await; a replayed `execute_activity_raw` returns from the matcher **without
yielding**, so a naive loop performs *n+1* synchronous guest invocations
back-to-back inside a single `poll`. The real bound was
`MAX_DECIDE_STEPS × DECIDE_MAX_WALL_CLOCK` per poll, uninterruptible — and
`tokio::time::timeout` cannot help, because it only cancels at an await point and
there was none. With `max_concurrent_workflows` defaulting to 20, a handful of
such executions wedge every runtime thread in the process.

Three changes close it, and they compose:

1. **Per-cycle memoisation** of `step -> DecideResponse`, sound because the guest
   is a pure function of `(input, resolved)` — the same property §7 rests on. A
   cycle now performs **one** new decision, not *n+1*.
2. **`yield_now` between fresh decisions**, so the scheduler and the workflow-body
   timeout can preempt. (Safe here specifically: the executor reads a
   `Poll::Pending` with a zero command delta and a self-wake as a spin to keep
   driving, not as a park.)
3. **`DECIDE_RUN_WALL_CLOCK`**, a cumulative guest budget for the whole cycle, so
   per-decision budgets cannot be composed into unbounded occupancy even if the
   memo is defeated.

Cancellation is still **not** threaded into a running decision, so worker
shutdown cannot interrupt one mid-flight; it waits out the per-decision backstop.
That is a real residual, listed in §9's open questions rather than papered over.

### 8.7 Supply chain

Publishing a module is code deployment, and the registry is a Postgres table.
Anyone with write access to `harvest_workflow_modules` can therefore deploy code —
which is why signature verification exists and why the GA answer is asymmetric
signing with the private key held by CI, not by the database. Content addressing
alone means a *tamper* fails closed, but a *legitimate-looking publish* does not.

---

## 9. Go / no-go

### The do-nothing baseline

Blue/green worker fleets plus the shipped build routing already deliver **safe**
deploys: ramps, compatibility gating, reachability-driven retirement, and
ND-blocking (#603) as the last-line net. What they do not deliver is
**restart-free** deploys. The delta hot swap buys is: no drained sessions, no cold
workflow caches, no poll-loop gap, and — for plugin embedders — no application
restart to change one workflow.

That delta is real but narrow, and it is bought at the price of a **second
authoring surface** (§5's "cannot" list). Any recommendation that ignores that
trade is not honest.

### Costed tiers

| Tier | Scope | Rough cost | Value | Recommendation |
|---|---|---|---|---|
| **T0 — do nothing** | Blue/green + build routing as today | 0 | Safe deploys, restart cost stays | The bar everything else must beat |
| **T1 — WASM activities only** | Already shipped as #965's spike; productize it: heartbeats into the guest, component-model ABI, packaging | ~1 quarter, 1 engineer | Restart-free delivery for the code that changes most often and carries the least determinism risk | **Go** — the plausible first productizable slice |
| **T2 — WASM workflows, sequential shape** | This spike, hardened: component-model/WIT ABI, timers + signals + child workflows in the ABI, automatic retirement, asymmetric signing, a packaging CLI | ~2 quarters on top of T1 | Restart-free workflow delivery for workflows that fit the sequential shape | **Conditional go**, and only after T1 lands and is used |
| **T3 — WASM workflows, full `WorkflowContext`** | Batch/decider ABI with concurrency, the full context surface, `ctx.version`/`ctx.patched` projection | Multi-quarter program | Parity with native workflows | **No-go for now.** The batch ABI is a second executor; building one is a bigger commitment than the delivery win justifies |
| **T4 — dylib hosting** | Any tier | — | — | **No-go, permanently** (§3): unsound, not merely expensive |

### **Verdict:** conditional go, activities first — and *not* a go for workflow hosting yet.

The spike proves the hard half of the question and answers it in the affirmative:
**harvest's governance machinery needs zero changes to make a hot swap safe**, and
a module-hosted workflow's history is provably indistinguishable from a
statically-linked one. That is the finding worth having, and it is the reason a
future decision to productize can be taken quickly.

But the mechanism half lands on a genuine wall (§2 C1–C4): general Rust workflow
bodies cannot be runtime-loaded soundly, so hosted workflows are a *different
authoring surface*, not the same one delivered faster. Introducing a second way to
write a workflow — with a subset of `WorkflowContext` — is a large product
commitment to buy a delivery-latency win, and it should not be made on the
strength of a spike.

The recommendation is therefore:

1. **Productize T1 (WASM activities).** It carries most of the restart-free value,
   because activity code changes far more often than workflow structure, and it
   introduces no second workflow-authoring surface at all.
2. **Keep T2 warm, behind this feature flag.** Revisit once T1 has real users and
   the demand for restart-free *workflow* changes is evidenced rather than
   assumed.
3. **Close the door on dylibs explicitly**, so the question is not re-litigated
   from first principles every time someone notices that `libloading` exists.

### Success metric, measured

Issue #967's metric — v2 published → first v2-assigned execution running in
< 10 s, v1 in-flight executions completing on v1 code with **0** replay
divergences, and rollback taking effect for new starts in < 5 s — is asserted by
`hot_swap_ramp_and_rollback_without_a_restart`, which performs the publish, the
ramp, the in-flight check and the rollback against a real Postgres and fails if
either latency budget is exceeded or the v1 replay reports anything but
`ReplaySucceeded`.

### Known limitations of the spike, carried forward

Named here rather than left to be discovered, because a spike that hides its
residuals is worse than one that has more of them:

1. **Cancellation is not threaded into a running decision.** Worker shutdown
   cannot interrupt a guest mid-decision; it waits out the per-decision backstop.
   The activity path threads a `CancellationToken` for exactly this reason, and
   the decide path should too.
2. **Queries and the pre-promotion replay gate cannot reach a hosted workflow**
   (§5). Both re-drive the handler from a context that holds no registry.
3. **Publishing is single-shard.** `publish_workflow_module` takes one
   connection, so in a sharded deployment a publish lands on one shard's
   database. A sync on another shard now *errors* rather than silently reporting
   success, so the failure is visible — but a fan-out publish helper is missing.
4. **Retirement is not automated.** Nothing reaps unreachable builds; a binding
   pins its compiled module resident until an operator calls `unload_build`.
5. **The activity allowlist is opt-in, not derived.** A GA default should be the
   activities the workflow's own registration declares, not "any registered
   activity unless configured otherwise".
6. **HMAC is the wrong primitive for the stated threat model** (§6): every
   verifier can forge. Ed25519 with CI-held private keys is the answer.

### Open questions for a T2 decision

1. **Authoring.** What does a guest developer actually write? A Rust crate
   compiled to `wasm32-unknown-unknown` against a `harvest-guest` SDK is the
   obvious answer, and is a whole workstream.
2. **Concurrency.** Does the batch/decider ABI (§5) land inside T2 or define T3?
   The answer decides whether `join!` is expressible.
3. **Guest determinism.** The host cannot *prove* a guest is deterministic. Can
   `harvest-verify` (#962) be pointed at guest source, or is ND-blocking (#603)
   the only net?
4. **Retirement automation.** Should a worker reap unreachable builds on its own,
   and what does it do about a build it is still the only holder of?
5. **Signing.** Ed25519 with CI-held keys, and where the public keys live.

---

## 10. What the prototype demonstrates

Everything below is behind the `hot-code-swap` Cargo feature, which is **not** in
the default feature set, enables **no new dependency** (it builds on the
`wasm-activities` embedding), and leaves the default build byte-for-byte
unaffected — asserted by `the_feature_exists_and_is_not_in_the_default_set` and
`the_feature_adds_no_new_dependency_to_the_workspace`.

### The pieces

| Piece | Where |
|---|---|
| Decide ABI, verification, in-process registry, trampoline | `autumn-harvest/src/hot_swap.rs` |
| Postgres registry (publish / fetch / list / retire / sync) | `autumn-harvest/src/hot_swap_store.rs` |
| Registry table | `migrations/*_harvest_workflow_modules/up.sql` |
| Worker dispatch seam (binds the execution's build) | `src/worker.rs`, at the `run_workflow_with_state_history_policy_and_caps` call |
| Guests CI actually executes | `autumn-harvest/examples/workflow-modules/` |
| Suite | `autumn-harvest/tests/integration/hot_code_swap_tests.rs` |
| Report guards | `autumn-harvest/tests/integration/hot_code_swap_docs.rs` |

### The AC3 demonstration, step by step

`hot_swap_ramp_and_rollback_without_a_restart` runs, against a real Postgres and
without restarting anything:

1. **(a) Loads a workflow module at startup.** `pipeline_v1.wat` is published
   under build `wf-v1` and synced into the registry; a build policy points the
   `default` queue at `wf-v1`; and `BuildPolicy::resolve_assigned_build` confirms
   a fresh execution id resolves to `wf-v1`. (This test drives the routing
   decision and the module dispatch; the *real worker* variant below is the one
   that starts and completes executions off the queue.)
2. **(b) Hot-loads v2 under a new build id.** `pipeline_v2.wat` — same workflow
   name, an extra activity — is published under `wf-v2` and synced into the *same
   live registry*. Compatibility is declared `host-1 → wf-v1` and
   `host-1 → wf-v2`, and `BuildCompatibilitySet::is_eligible` confirms the one
   worker may claim both.
3. **(c) Ramps new starts to v2 with the shipped API.** `set_build_ramp(…, "wf-v2", 100)`;
   a fresh execution resolves to `wf-v2` and running it produces `"v2-done"`,
   while a `wf-v1`-assigned run still produces `"v1-done"` and replays clean
   against the v1 logic it started under. (The *scheduling* evidence — that v2
   records `charge` **and** `notify` while v1 records only `charge` — is asserted
   by `the_v2_module_runs_the_extra_step` and by the real-worker test below, not
   by this one, which compares terminal outputs.)
4. **(d) Rolls back by repointing the ramp.** `clear_build_ramp` sends new
   starts back to `wf-v1` immediately; already-assigned v2 executions are
   untouched, per the start-time-immutable `assigned_build_id` invariant.

Both latency budgets are asserted in the test rather than described here.

### The same demonstration, inside one real worker process

`a_running_worker_adopts_v2_under_a_new_build_id_without_restarting` repeats it
through a real `Worker` claiming real tasks off the real queue, so "one worker
process" is literal. The worker is started once and never restarted or
reconfigured; everything after that — publishing v2, syncing it into the *live*
registry, declaring compatibility, ramping, rolling back — happens while it is
polling. It asserts:

* a `wf-v1`-assigned execution completes with `"v1-done"`;
* after the hot load, a `wf-v2`-assigned execution completes with `"v2-done"` and
  its recorded history carries both `charge` and `notify` — the same process, new
  code;
* a `wf-v1`-assigned execution seeded **after** the swap still completes with
  `"v1-done"`, which is the in-flight guarantee: the build id is fixed at start
  time, so the code it denotes must be too;
* after `clear_build_ramp`, the policy resolves new starts back to `wf-v1` and a
  fresh run completes on v1 code.

The worker advertises a single `WorkerConfig::build_id` (`host-1`) and claims
tasks for both builds through ordinary compatibility declarations — no
"multi-build worker" concept exists.

### What the demonstration does *not* claim

One worker, one shard, one queue. It is not a fleet soak: rolling adoption across
many workers, sticky-routing interaction with a mid-swap cache, and reachability-
driven retirement under load are all productization work, not feasibility
questions.
