## Phase 5.x — hot code swap for workflow definitions via runtime modules, R&D (issue #967)

Harvest's *routing* side of deploys has been finished for a while — build-ID
routing (#171), compatibility sets, percent ramps (#604), reachability
(#520/#535) — but the *delivery* side is still "ship a new binary and bounce the
fleet". This R&D issue asks whether a live worker can load workflow definitions
as runtime modules, so a deploy becomes "publish a module, workers pick it up
under a new build id" and a rollback becomes "repoint the ramp", **with zero
change to the replay surface**.

- **`docs/rnd/hot-code-swap.md` — the AC1 feasibility report**, and the
  deliverable the issue exists for. It inventories the eight hard constraints a
  host boundary must satisfy (bare `fn`-pointer handlers per DD-6; macro-generated
  `::autumn_harvest::` paths; the oneshot suspension model, DD-1; Rust's lack of a
  stable ABI; the executor's inability to tell a host `await` from a workflow
  suspension; panic containment; the append-only replay surface; build-id-only
  identity), evaluates dylib hosting against them and refuses it as **unsound
  rather than merely expensive**, adopts WebAssembly, names the recommended host
  boundary, and reaches an explicit verdict: **conditional go, activities first**
  (T1 productize #965's WASM activities; T2 WASM workflows kept warm behind the
  flag; T3 full `WorkflowContext` parity a no-go for now; T4 dylibs a permanent
  no-go), explicitly priced against the do-nothing baseline of blue/green fleets
  plus build routing.
- **The host boundary (`src/hot_swap.rs`)** — the design's one real idea. A
  runtime module cannot mint a `WorkflowHandlerFn`, so it does not try to: one
  statically-linked trampoline `fn` is the handler, and the module is a **pure
  decision function re-invoked once per await**
  (`run(DecideRequest) -> Await | Complete | Fail`). That sidesteps DD-6 entirely,
  needs no continuation across a durable suspension, and — because the guest sees
  only its input and the outcomes of awaits it itself requested, all history-backed,
  with deny-all capabilities — makes replay determinism a property of the
  construction rather than a convention. Guest invocation enters
  `wasm_activities::invoke_wasm_guest_bytes`, a sibling entry point sharing the
  activity path's inner implementation: same engine, same fuel/epoch/memory
  bounding, same bounds-checked ABI, same host-glue panic containment. (It exists
  because the activity path serialises a `serde_json::Value`, whose object is a
  `BTreeMap` — the keys would reach the guest alphabetically, moving `step` off
  the fixed byte offset the WAT guests read it from.) **No new `unsafe`, no second
  sandbox, no new dependency.**
- **The routing rule, and the trap next to it.** The trampoline resolves its module
  by the **execution's** `assigned_build_id`, threaded in the worker seam from the
  execution row — deliberately *not* `ctx.build_id()`, which reports the worker's
  own configured build (issue #798 made that so on purpose, for pre-promotion
  replay gates). Routing on the worker's build would drag a v1-assigned in-flight
  execution onto v2 code the moment an operator relabelled a worker, which is
  precisely the divergence build routing exists to prevent.
- **The registry (`src/hot_swap_store.rs`, migration
  `20260903161045_harvest_workflow_modules`)** — Postgres-only, no new infrastructure. The
  primary key `(build_id, workflow_name)` *is* the design: a build id names one
  module for a workflow immutably (mirroring start-time-immutable
  `assigned_build_id`), and two modules can therefore never claim one workflow name
  outside build-id governance. There is deliberately **no `active` flag** — the
  ramp is the switch, so rollback is `clear_build_ramp` and needs no registry
  write. Verification is content addressing (re-hashed on
  every load, so a *tamper* — one column changed without the other — fails closed;
  a coherent write does not, and the report says so) plus an optional detached
  HMAC-SHA256 signature over the whole **binding**, the length-prefixed
  domain-separated `(build_id, workflow_name, module_hash)` tuple. Signing the
  identity rather than the bytes alone is what stops a table writer copying an
  existing row's `(hash, signature)` pair and re-binding it under a different build
  id (a **downgrade**, onto whatever build the ramp points at) or a different
  workflow name (a **substitution**). A sync verifies and compiles the whole build
  *before* binding any of it, so a build whose third module fails leaves the first
  two unbound; and retirement is a `retired_at` **tombstone**, not a `DELETE`,
  because the row *is* the immutability guarantee — deleting it would free the
  primary key and let a build id be re-pointed at new code under an execution
  still parked on a long timer.
- **An engine error is never handed to the guest.** The sharpest edge in the
  design, and the one review round-tripped: `execute_activity_raw` returns
  `HarvestError::NonDeterministic` on a replay divergence, and an earlier cut of
  the trampoline converted *every* error into a guest-visible `DecideOutcome::Err`.
  A guest that then answered `Complete` would seal the execution COMPLETED over a
  history it demonstrably diverged from — with the replay gate reporting
  `ReplaySucceeded`, since `nd_details` alone does not fail the executor's success
  arm. That is #603's ND-blocking net switched off for every hosted workflow, by
  the one component whose justification is that it needs no new safety machinery.
  `outcome_for_guest` now hands the guest **only** `ActivityFailed` (with its
  stable `error_type` and `details`, so a guest branches on the class rather than
  parsing a `Display` string that differs between the inline and replayed delivery
  paths); divergences, cancellations and config errors propagate.
- **A missing module releases the task, it does not destroy the execution.** The
  worker seam resolves the module *before* dispatch and, on a miss, raises the
  same typed `HandlerNotRegistered` capability miss the unknown-workflow-type
  check uses (#804) — so a worker that has not yet synced a build, or retired it
  early, leaves the work for a capable peer. Resolving inside the handler would
  have made it an `Err(String)`, i.e. a terminal `WorkflowFailed`. The resolved
  `Arc` is then **held** for the invocation and handed to the trampoline: a
  second lookup could miss where the first hit — an `unload_build` landing
  between them — turning a passed capability check into exactly the terminal
  failure the check exists to prevent, and falsifying the safety analysis's claim
  that unloading is safe for in-flight work.
- **The guest's policy is the operator's, not a default.** The worker stores a
  whole `ModuleHost` prototype rather than just the registry, and dispatch clones
  it per task (`for_task`) before stamping on the execution's build id and
  module. Storing only the registry meant the dispatch seam built a fresh
  `ModuleHost::new` per task — discarding the activity allowlist and the
  queue-override switch on the one path that matters, so a production guest could
  schedule any activity the worker knew while the tests, which bind a host
  directly, showed the restriction working.
- **Zero replay-surface change, proved not asserted.** No new `WorkflowEvent`
  variant, no event-JSON change, no `HistoryMatcher`/executor change, no third
  in-place writer of `harvest_events.event_data`. Four tests carry it:
  `a_module_hosted_history_is_byte_identical_to_the_statically_linked_one`,
  `a_module_hosted_history_replays_clean_under_statically_linked_code`, the
  reverse direction `a_statically_linked_history_replays_clean_under_module_hosting`
  (the one an operator actually depends on mid-swap), and
  `hosting_never_introduces_a_new_event_variant`.
- **Safety analysis with tests attached** — panic/UB blast radius per option,
  the unload hazard (`Arc` refcounting makes it structurally safe here; `dlclose`
  makes it a use-after-free there —
  `unloading_a_build_drops_its_modules_but_not_a_live_holder`), memory growth under
  repeated swaps (a binding pins its compiled module, so the LRU bounds only
  *unbound* code and reachability-gated retirement is the real bound), the
  duplicate-registration hazard rejected at three layers, and — added because the
  first draft of the analysis reasoned about the channel that is *closed* and never
  mentioned the one open by design — the fact that **`Await` is a host capability
  and deny-all `WasmCapabilities` does not touch it**: a module could otherwise
  schedule any activity the worker knows, read its output, and return it through a
  failure message. `ModuleHost` now carries an activity allowlist and keeps queue
  override off by default. Resource exhaustion is bounded by fuel as the
  *operative*, deterministic budget with the wall clock as a generous backstop
  (a tight ceiling would make a run's terminal outcome depend on host load), a
  cumulative per-cycle guest budget, and a `MAX_DECIDE_STEPS` ceiling checked
  *before* scheduling so the last permitted step cannot run a real activity and
  then fail the run for not terminating.
- **C9: the host may not introduce an await that records no command.** The
  boundary's least obvious constraint, and the one that cost a line of code that
  looked like good manners. `executor::run_workflow_handler_cycle` drives the
  handler inside `tokio::time::timeout(SUSPENSION_TIMEOUT)` — 100 ms — and a
  workflow returning `Poll::Pending` is *by definition* how a suspension is
  detected. A `yield_now()` between guest decisions was therefore the one point
  at which that timer could fire before the trampoline had scheduled anything,
  producing a zero-command suspension: a workflow parked on nothing, which the
  worker fails terminally. It is gone, and what replaces it is an invariant
  rather than a knob — the trampoline's *only* await is `execute_activity_raw`,
  which records its `ScheduleActivity` before parking, so every suspension a
  hosted workflow can produce carries a command exactly as a statically-linked
  one's does. The general form is now C9 in the report, because it constrains
  anyone who extends this boundary, and it is C5 restated: the oneshot suspension
  model gives the executor no way to tell "the host is thinking" from "the
  workflow is suspended".
- **The quadratic is actually removed now, and the lifetime took two goes.**
  Under DD-1 the trampoline restarts at step 0 every cycle, so a run with `n`
  awaits asks the guest `O(n²)` times. The memo meant to fix that was created
  *inside* the handler future, where a single monotonically increasing pass
  writes every entry and reads none — it could never hit. Moving it to
  `ModuleHost`, per workflow task, was still wrong: an ordinary activity **ends
  the task** (only local activities re-drive in place), so the workflow resumes
  in a new `process_workflow_task` call and a per-task cache is reset on every
  durable cycle. It lives on the `ModuleRegistry` now, for the worker process,
  bounded and oldest-first. Sharing it across executions is sound for the same
  reason the whole design is: the guest is a pure function of its request.
  Its key is a digest of **`(build_id, module_hash, request bytes)`**,
  length-prefixed: a `DecideRequest` deliberately carries no build id, so v1 and
  v2 of one workflow see byte-identical input at step 0, and keying on the
  request alone would serve v1's decision to v2 — silently defeating the swap the
  spike exists to demonstrate
  (`the_decision_cache_never_serves_one_builds_answer_to_another`).
- **A cache bounded in entries is not bounded in memory.** The decision cache's
  ceiling was a count, on the reasoning that "entries are small" — which was
  never checked. The key is a 32-byte digest, but the *value* is a whole
  `DecideResponse`, and a guest may return up to `WASM_MAX_OUTPUT_BYTES` (4 MiB)
  per decision: `MAX_CACHED_DECISIONS` of those is ~16 GiB, reached by a guest
  that simply returns a distinct large response each time. That is the
  per-invocation memory ceiling the sandbox enforces, defeated through the one
  structure deliberately built to outlive an invocation. The operative bound is
  now `MAX_CACHED_DECISION_BYTES` (8 MiB retained), with
  `MAX_CACHED_RESPONSE_BYTES` (64 KiB) refusing any single response large enough
  to evict the cache to make room for itself — a refusal costs only the
  optimisation for that step, since the guest is simply re-asked. The general
  form, recorded in the report because it will outlive this cache: **a bound
  stated in entries is a bound on the wrong thing whenever the guest chooses the
  entry size.**
- **The cache may make a run cheaper; it may not change what the run decides.**
  A cache hit was free while a recomputation was charged to the run's cumulative
  guest budget, so the same workflow on the same history would complete while
  its earlier decisions were still resident and fail once unrelated executions
  had evicted them — or when one response exceeded the per-entry ceiling and was
  never cached at all. A terminal outcome turning on cache residency is exactly
  the defect `MAX_DECIDE_STEPS` is a compile-time constant to avoid, arriving
  through the optimisation instead. Each entry now records what its decision
  cost, a hit charges that, and both paths report through one error constructor,
  since even a differing message is a differing observable outcome.
  Charging the hit was necessary but not sufficient: the first cut left the
  *check* on each branch separately, so a fresh decision that pushed the run
  over budget was accepted while the same total served from cache was rejected —
  the residency dependence surviving inside its own fix. Cost is charged and the
  budget checked in exactly one place, on the path both branches join, and
  before the response is acted on, since an over-budget `Await` acted on
  optimistically schedules a real activity the run then fails immediately after.
- **Signing can be introduced or rotated on a build that already exists.** The
  identical-bytes republish path cleared `retired_at` and left the row's old (or
  NULL) signature, so a republish carrying a signature valid under a new key
  returned success and then made the next sync with that key reject the row it
  had just accepted — rotation would have required minting a new build id, i.e.
  a deploy, the exact coupling this design exists to remove. The signature is
  written too; safe because it is verified against the caller's key before any
  write, and the bytes are unchanged, so it rebinds the same content to a fresh
  attestation of the same tuple rather than smuggling in different code. A
  republish supplying *no* signature leaves the stored one alone (`COALESCE`)
  rather than erasing it: mid-rollout, an older or unsigned publisher re-seeding
  the same build would otherwise NULL a valid attestation and make every worker
  syncing with the key reject a row that was correctly signed a moment earlier.
  Withdrawing a signature is deliberately not expressible — it would be
  indistinguishable from that accident, and retirement already exists for
  withdrawing a module.
- **The guest-facing failure shape is documented as the host actually sends
  it.** The example showed `{"kind":"err","error":"..."}`; the real outcome
  always carries `error_type` and may carry `details`. A guest with a strict
  schema would have rejected every failed-activity request, and one branching on
  the advertised field would have parsed `error` — a diagnostic string that
  differs between the inline and replayed delivery paths, so branching on it
  behaves differently on replay than it did live. The guard now diffs the
  failure example against a real `DecideOutcome::Err` too.
- **A sync now refuses a build whose module set moved under it.** Publishing a
  *new* `(build_id, workflow_name)` under an existing build is allowed by design
  — the primary key only makes an existing name's bytes immutable — so a module
  published between the manifest listing and the commit was silently missed, and
  the sync reported success while the worker lacked a module for a workflow its
  build compatibility admits: every task for it becomes a capability-miss
  redelivery until someone syncs again. The manifest is re-read before
  committing and a change fails the sync with a retry instruction, because "this
  build is loaded" is the claim the worker acts on when deciding which
  executions it can serve, and a worker confidently half-serving a build is the
  failure §8 argues is worse than not serving it at all. This **narrows** the
  window — from the whole fetch-and-compile pass to one `spawn_blocking`
  dispatch — rather than closing it: a build's membership is open-ended by
  design, so there is no moment at which "this build is complete" is knowable
  and no snapshot can make one. Closing it needs build-level sealing, carried
  forward as limitation 7 with the reason it is not paid for here.
- **An activity timeout is a step outcome, and now reaches the guest.** The
  mirror image of the ND bug above. `execute_activity_raw` builds
  `HarvestError::Timeout` from `HistoryMatch::TimedOut`, so it is history-backed
  and replay-deterministic on the same footing as `ActivityFailed` — and the
  engine pairs the two everywhere it classifies a step result. Withholding it
  gave a hosted workflow strictly *less* than its statically-linked twin: a
  timeout a native handler catches and compensates for killed a hosted run
  outright. Where over-delivering an error lets a guest swallow a divergence,
  under-delivering one silently removes recovery the platform otherwise
  guarantees. Only the four **activity-scoped** types cross the boundary;
  `WorkflowExecution` and `WorkflowChain` are the run's own deadline and still
  propagate, because a guest that could see those could answer `Complete` past a
  deadline the engine had just enforced.
- **A build's modules are no longer all resident at once during sync.** Sync read
  every payload into a `Vec` and only then compiled, so the whole build's source
  bytes were resident together — the table's `CHECK` caps a row at 32 MiB but
  nothing caps how many workflow names a build has, so a large enough build could
  OOM a worker mid-sync. Each payload is now fetched, compiled on
  `spawn_blocking`, and dropped before the next is fetched; atomic binding is
  preserved by splitting `prepare_module` (verify + compile) from
  `commit_prepared` (bind all or none). The doc comment claiming one-module peak
  residency is now true rather than aspirational, and says plainly that the
  *compiled* artifacts are still held for the whole build, which atomicity makes
  irreducible.
- **A load batch may not bind two modules to one key.** `load_modules` compared
  each entry only against what was *already* bound, which knows nothing about the
  batch, so two different modules for one `(build_id, workflow_name)` with
  nothing bound beforehand let the last silently overwrite the first — through
  the very API whose purpose is the immutable binding.
- **The AC3 demonstration, twice.** `hot_swap_ramp_and_rollback_without_a_restart`
  drives the whole swap against a real Postgres and the real shipped routing APIs:
  v1 published and loaded, v2 published and hot-loaded under a new build id with no
  restart, `set_build_ramp` moving new starts onto v2 while the v1-assigned
  in-flight execution keeps running v1 code and replays clean, then
  `clear_build_ramp` rolling back — asserting the issue's <10s swap and <5s
  rollback budgets. `a_running_worker_adopts_v2_under_a_new_build_id_without_restarting`
  then repeats it through a **real `Worker`** claiming real tasks off the real
  queue: the worker starts once and is never restarted, publishes/syncs/ramps/rolls
  back while polling, and the v2 run's recorded history carries the extra activity
  v2 added while a `wf-v1`-assigned execution seeded *after* the swap still
  completes on v1 code. The worker advertises a single `WorkerConfig::build_id`
  and claims both builds through ordinary compatibility declarations — no
  "multi-build worker" concept is introduced.
- **Invisible by default.** Everything is behind the `hot-code-swap` Cargo
  feature, which is not in `default` and enables **no new dependency** (it implies
  `wasm-activities`, reusing the wasmtime embedding #965 already vetted). The
  effective MSRV of the feature is inherited (1.94); the crate's core MSRV stays
  1.88. `hot_code_swap_docs.rs` — which compiles in a *default* build — guards the
  report against the code it audits: the `fn`-pointer constraint, the
  `ctx.build_id()` semantics, the shipped routing symbols, `CLAUDE.md`'s
  append-only exception count, the feature's absence from `default`, and the
  existence of every test the report cites.
