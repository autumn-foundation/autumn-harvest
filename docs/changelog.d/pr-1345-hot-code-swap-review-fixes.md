## Phase 5.x — hot code swap review-round 6/7 fixes (issue #1345)

Seven correctness findings against the `hot-code-swap` R&D spike (#967),
found past that PR's review budget and recorded on this issue instead. All
behind the `hot-code-swap` Cargo feature, so none affects a default build.

- **`unload_build`'s safety doc overclaimed.** It said early unload is safe
  for in-flight invocations because a resolved caller holds an `Arc` — true
  against use-after-free, but read as covering a *suspended* execution too.
  A suspended execution holds no `Arc`; its next `process_workflow_task` does
  a fresh lookup, which misses after an early unload — the typed capability
  miss (#804), not a crash, but a redelivery cost the doc must not paper
  over. The doc comment and the report now say plainly: legal to call at any
  time, free of that cost only once `build_reachability` reports
  `safe_to_retire`.
- **The decision cache is unsound for a capability-enabled host.** Its
  soundness rests on the guest being a pure function of its request, which
  holds only under deny-all `WasmCapabilities`. `with_capabilities` can grant
  a clock or randomness, and a guest granted either is not pure — the cache
  could serve a stale time- or random-dependent answer without invoking the
  guest at all. The host now skips the cache entirely whenever capabilities
  are not deny-all, rather than trying to key it by capability grant.
- **A guest-chosen activity queue name was unbounded.** `resolve_activity_queue`
  only checked for an empty string. `harvest_task_queue.queue_name` sits in a
  B-tree poll index, and a long enough guest-chosen name would make every
  future insert for that queue fail at the index. A new
  `MAX_QUEUE_NAME_BYTES` (200) ceiling refuses an oversized name outright —
  truncating it would silently route work to a queue nobody polls.
- **The example README documented a wire shape the host never sends.**
  `DecideOutcome::Err::details` is `skip_serializing_if = "Option::is_none"`,
  so a failure without structured detail omits the key entirely rather than
  serializing `"details":null`. The README's timeout example showed the
  `null` form; the guard test now checks both failure examples against a
  real serialization instead of only the first one it finds.
- **The cached decision cost was itself non-deterministic.** An earlier round
  made a cache hit charge the run's cumulative budget, so residency would not
  change the run's outcome. What it charged was the wall-clock `Duration`
  measured when the decision was first computed — which varies with host
  load, so a slow-under-load decision charged that same slow cost to every
  later hit, while a fast recomputation after eviction could complete a run
  the cached hit would have failed. The cache now stores and charges **fuel
  consumed** instead: deterministic for a given guest and request. This
  needed `invoke_wasm_guest_bytes` (and the shared `invoke_wasm_activity_inner`
  it sits alongside) to report the fuel a call actually consumed, and the
  cumulative budget is now `DECIDE_RUN_FUEL_BUDGET` (twice `DECIDE_FUEL`)
  rather than a ten-second wall clock. A bot review of this exact fix caught
  that fuel alone does not bound wall-clock *occupancy*: a capability-enabled
  host or a cache miss recomputes every step fresh, so a guest cheap in fuel
  but slow in real time (bulk-memory instructions) could occupy a runtime
  worker for minutes while staying under the fuel budget. A new
  `DECIDE_RUN_WALL_CLOCK_BACKSTOP` (10 s) closes that: a live `Instant::now()`
  check, re-read every step and never charged from a cached value, so it adds
  a real-time ceiling without reintroducing the residency-dependent bug the
  fuel budget itself fixed.
- **Compiled modules could accumulate unbounded during one sync.**
  `sync_build_into_registry` fetches source bytes one payload at a time,
  bounding *source* residency to one module — but atomic binding needs every
  module compiled before any of them is bound, so the *compiled* artifacts
  stay resident for the whole batch regardless, and nothing bounded how many
  workflow names a build could register. A new `MAX_WORKFLOW_NAMES_PER_BUILD`
  (256) ceiling refuses the sync outright, before any module is fetched or
  compiled.
- **Unload generations were registry-wide, not per build.** `commit` refused
  a load whose registry generation had moved since it started, via one
  counter bumped by every `unload_build` call for every build. Retiring
  `wf-a` therefore also failed an unrelated `wf-b` sync that happened to be
  mid-compile at the same time, reporting that `wf-b` was unloaded when it
  never was. The registry now also records the generation at which each
  build id was last unloaded, and `commit` checks only the builds its own
  batch actually binds. That record itself grows without eviction, one entry
  per distinct build id ever unloaded — a known, slow residual paced by
  operator-driven retirements rather than by execution volume, not a new
  hazard; documented in the report rather than patched with an eviction
  policy that could re-open the resurrection bug this map exists to close.

No new `WorkflowEvent` variant, no migration, no event-contract or
replay-determinism change. New and updated tests in
`tests/integration/hot_code_swap_tests.rs` and
`tests/integration/hot_code_swap_docs.rs` pin each fix; `docs/rnd/hot-code-swap.md`
is updated to match.
