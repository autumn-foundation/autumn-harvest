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
  have made it an `Err(String)`, i.e. a terminal `WorkflowFailed`.
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
