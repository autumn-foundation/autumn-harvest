# `dag_graph::build_run_graph`: hoist the per-node `dispatched_activity_names` rebuild

This note documents a profiling pass over
`autumn_harvest_plugin::dag_graph::build_run_graph` -- the pure projection
behind `GET /dag-run-graph` (issue #690) that reconstructs a unified DAG
run's node topology (status, timing, attempts, truncated error) purely from
the registered `DagDefinition` and the run's recorded `WorkflowEvent`
history. Wall-clock timing is not admissible evidence on this (shared-vCPU)
machine -- every number below is a deterministic instruction count
(`valgrind --tool=callgrind`) or allocation count/bytes
(`valgrind --tool=dhat`), both reproducible bit-for-bit on any machine.

## Workload

`benches/dag_graph_profile.rs` calls `build_run_graph` 300 times
(`DAG_GRAPH_PROFILE_REPS`) against a realistic 93-task DAG built via
`autumn_harvest::dag::DagBuilder`: eight layers with genuine diamond
fan-out/fan-in shapes (a root, an 8-way fan-out, a 16-node 2-way-fan-in
layer, 4 signal-gate nodes, a 16-node layer fed by both gates and earlier
activities, two more 20-node 2-way-fan-in layers, and an 8-node 4-way-fan-in
aggregation layer) -- not a degenerate flat chain. The paired 137-event
history mixes succeeded, failed, multi-attempt-retried, condition-skipped
(`dag_skip:{idx}`, issue #482), and never-reached (unreachable) activities;
two gates resolved by signal, one resolved by a `TimerStarted`/`TimerFired`
race-timer timeout pair (issue #476/#746), and exactly one -- the last in
execution-level order -- left unresolved; and issue #780 compensator
dispatches against already-dispatched forward nodes -- including one
recorded under a *current node's own name*, so the exclusion
`dispatched_activity_names` exists for is genuinely exercised. This is the
real public entry point (`build_run_graph`), not a synthetic microbenchmark
of a pre-selected function.

### Harness correction (post-review)

A GitHub Codex review of the PR that introduced this harness caught four
realism gaps in the fixture across several review rounds, all now fixed and
reflected in every number on this page:

1. **Every gate reported `Pending` regardless of the signal/timer events
   recorded for it.** `gate_status` checks upstream reachability *before*
   resolution: an upstream that failed, was condition-skipped, or was never
   reached makes the default `AllSuccess` trigger rule report
   `SkippedByTrigger`/`NotReached`, and the gate is classified `Pending`
   without ever looking at `gate_resolution`. The fixture's original `idx % 5`
   outcome pattern happened to assign a failed, skipped, or never-reached
   outcome to 3 of the 4 gates' upstream tasks, so the `SignalReceived` /
   `TimerFired` / left-unresolved events pushed for those gates were dead
   weight -- present in the history, never actually read.
2. **Neither compensator dispatch exercised `is_compensation_dispatch`.**
   `latest_scheduled` checks `name == node` before checking whether an event
   is a compensation dispatch; the two original compensator events
   (`undo_t001`, `undo_t002`) are not themselves node names in this DAG, so
   that check short-circuits before the exclusion predicate ever runs for
   them. Fixed by adding a third compensator dispatch recorded under a
   *current* node's own name (`t001`, compensating the separately-dispatched
   `t002`) after `t001`'s genuine forward dispatch, so `latest_scheduled`'s
   reverse scan must recognize and skip it to find the real one.
3. **The recorded history was not producible by any real walker.** The
   original fixture picked each task's outcome from its raw DAG-definition
   index (`idx % 5`/`idx % 4`) with no awareness of the DAG's actual
   dependency edges -- so, for example, the sole root task could be marked
   "never reached" while every one of the 92 other tasks (all transitively
   downstream of it) still received scheduled/completed/failed events
   regardless. No real walker could ever produce that history: it never
   dispatches a node whose upstreams have not all resolved successfully.
   Fixed by rewriting `build_events` to track per-task reachability directly:
   a "backbone" of root-through-layer2 (`BACKBONE_END`, task indices 0..=24)
   always succeeds, guaranteeing every gate is reachable and its scripted
   resolution below is genuinely exercised; every task from layer4 onward
   only receives an outcome when `task.upstreams` are *all* already `done`,
   and is left with no events at all otherwise -- exactly what a real walker
   would (not) have dispatched. The two gates left deliberately unresolved
   (originally `gate_2`/`gate_3`, `Waiting`) correctly make their own
   downstream layer4 nodes unreachable too, cascading further into later
   layers: a real DAG paused on an unanswered approval gate has exactly this
   shape.
4. **Two further gaps the reachability fix above didn't catch, found in the
   next review round.** First, a bounded gate resolved by its race timer
   (`gate_1`, `TimedOut`) recorded only a `TimerFired` event, no preceding
   `TimerStarted` -- but `context::wait_for_signal_timeout` always arms the
   deadline via `StartTimer` before it can be won by a timeout (the only path
   that skips `TimerStarted` is a signal already buffered when the race
   starts, which resolves `SignalWon`, not `TimerWon`), so a `TimerFired`
   with no matching `TimerStarted` is a history the real walker can never
   produce either. Fixed by recording `TimerStarted` immediately before
   `TimerFired` for the same `timer_id`. Second, fix 3's reachability model
   checked only *declared upstream edges*, not *execution-level order* --
   but `run_unified_dag` processes levels strictly sequentially and a signal
   gate always occupies its own singleton level (guaranteed by
   `DagBuilder::build`, enforced by a `debug_assert_eq!` in the walker), so
   **at most one gate can ever be genuinely mid-`.await` in one recorded
   run**, whichever is earliest in level order among the unresolved ones.
   Leaving two gates (`gate_2` and `gate_3`) simultaneously `Waiting` was
   still a history no real walker could produce, even after fix 3, because
   reaching `gate_3`'s level requires `gate_2`'s `.await` to have already
   resolved one way or another. Fixed by resolving `gate_2` (by signal, like
   `gate_0`) and leaving only `gate_3` -- the last gate in level order --
   genuinely unresolved.

Fixes 1 and 3 interact: the reachability tracking that fix 3 introduces is
also what fix 1's gate resolutions now rely on (a gate is reachable only
when its own upstreams are `done`), so they landed as one change to
`build_events`. Verified directly (temporary debug print, since removed):
`gate_0: Succeeded`, `gate_1: TimedOut`, `gate_2: Succeeded`,
`gate_3: Waiting`, `t001: Succeeded` (its genuine dispatch, correctly winning
over the later compensator envelope sharing its name) -- and a full-graph
status-mix dump showing every node's status is one a real walker could
produce: 40 succeeded, 3 failed, 3 skipped, 1 gate timed-out (counts as
succeeded downstream), 1 gate waiting, 45 legitimately pending (some never
reached, most cascaded from the one waiting gate or an upstream
failure/skip further back).

None of these four gaps invalidated the fix itself (all four bugs are in
the harness, not in code paths this change touches), but they did mean the
workload's realism claims overstated what the fixture actually exercised.
The numbers below are from the fully corrected harness; event count moved
from the original 202 (211 after fix 2, 111 after fix 3, 137 after fix 4),
since a reachability- and level-order-consistent history dispatches a
different subset of the 93 tasks than any of the earlier, flawed attempts.

## Profile

```bash
BIN=$(cargo bench -p autumn-harvest-plugin --no-default-features \
  --bench dag_graph_profile --no-run --message-format=json 2>/dev/null \
  | jq -r 'select(.reason=="compiler-artifact" and .target.name=="dag_graph_profile") | .executable')
valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
callgrind_annotate --threshold=98 cg.out
```

Baseline (unmodified `HEAD`, fully corrected harness):

```
853,441,425 (100.0%)  PROGRAM TOTALS

200,057,753 (23.44%)  __memcmp_avx2_movbe [libc]
132,636,600 (15.54%)  core::slice::sort::stable::drift::sort
 98,208,000 (11.51%)  alloc::collections::btree::map::BTreeMap<K,V,A>::bulk_build_from_sorted_iter
 71,766,300 ( 8.41%)  <alloc::vec::Vec<T> as SpecFromIter<T,I>>::from_iter
 44,468,142 ( 5.21%)  <alloc::collections::btree::map::BTreeMap<K,V,A> as Drop>::drop
 43,521,600 ( 5.10%)  <core::iter::adapters::map::Map<I,F> as Iterator>::fold
 34,615,649 ( 4.06%)  malloc.c:_int_free
 28,918,500 ( 3.39%)  autumn_harvest_plugin::dag_retry::node_outcome
 11,757,300 ( 1.38%)  autumn_harvest_plugin::dag_graph::has_skip_marker
```

None of these lines is named `dispatched_activity_names` or
`latest_scheduled` -- both are aggressively inlined into their call sites, so
their cost surfaces under the generic `BTreeSet<&str>: FromIterator`
machinery (`from_iter`, `bulk_build_from_sorted_iter`, the sort it drives,
the `Drop` glue, and the `memcmp`-based `&str` ordering comparisons) rather
than under their own names: `__memcmp_avx2_movbe` +
`core::slice::sort::stable::drift::sort` +
`BTreeMap::bulk_build_from_sorted_iter` +
`<Vec<T> as SpecFromIter<T,I>>::from_iter` +
`<BTreeMap<K,V,A> as Drop>::drop` + `<Map<I,F> as Iterator>::fold` alone sum
to **69.21%** of total instructions (590,658,395 / 853,441,425) -- comfortably
clearing the >=5%-of-workload floor -- with `node_outcome`'s and
`has_skip_marker`'s own (untouched-by-this-fix) self-costs shown separately
above for the before/after comparison below.

## Hypothesis

`dag_retry::dispatched_activity_names(events)` scans the whole event history
and allocates a fresh `BTreeSet<&str>`, purely to give `latest_scheduled`
(called once per non-gate node from `classify`, itself called once per node
from `build_run_graph`'s loop) a way to exclude issue #780 compensator
dispatches when picking a node's latest `ActivityScheduled`. Its result
depends only on `events` -- identical for every node in one `build_run_graph`
call -- so rebuilding it inside `latest_scheduled` on every node is an
entirely redundant N-fold recomputation for an N-node DAG (the same class of
fix as commit 1c9493c, "drop per-call HashSet rebuild in schema
validation"). Hoisting the computation to run once in `build_run_graph` and
threading it down as a parameter should remove `latest_scheduled`'s share of
the BTreeSet-construction cost while leaving `node_outcome`'s own separate,
out-of-scope call to `dispatched_activity_names` -- and its self-cost,
28,918,500 Ir above -- untouched.

## Change

`autumn-harvest-plugin/src/dag_graph.rs`:

* `latest_scheduled` gains a third parameter, `dispatched: &BTreeSet<&str>`,
  and no longer computes it internally:

  ```rust
  fn latest_scheduled(
      events: &[WorkflowEvent],
      node: &str,
      dispatched: &BTreeSet<&str>,
  ) -> Option<(usize, ActivityExecId)> {
      events.iter().enumerate().rev().find_map(|(idx, event)| {
          if let WorkflowEvent::ActivityScheduled { activity_id, name, input, .. } = event {
              (name == node && !crate::dag_retry::is_compensation_dispatch(input, dispatched))
                  .then_some((idx, *activity_id))
          } else {
              None
          }
      })
  }
  ```

* `build_run_graph` computes `dispatched` once, right after building the
  `events` clone, and threads it through `classify` (which gained a matching
  `dispatched: &BTreeSet<&str>` parameter) down to `latest_scheduled`'s one
  call site -- but only when the DAG actually has an activity node to need
  it. Gate nodes take an early-return branch above this and never reach
  `classify`/`latest_scheduled`, so a DAG built entirely from signal gates
  (a supported shape) paid nothing for `dispatched_activity_names` before
  this change and must not start paying an unconditional O(events) scan for
  it now (issue #690 review, Codex):

  ```rust
  let dispatched = if tasks.iter().any(|t| t.signal.is_none()) {
      crate::dag_retry::dispatched_activity_names(&events)
  } else {
      BTreeSet::new()
  };
  ```

  The guard is `O(tasks)`, not `O(events)`, so it stays cheap even when it
  does fire. A new unit test, `gate_only_dag_classifies_without_an_activity_node`,
  pins the gate-only case correctly reaching `Waiting` on a live run. On this
  page's own fixture (which has 89 activity nodes) the guard always takes the
  `dispatched_activity_names` branch, so it does not change any number
  measured below -- confirmed by re-running the identical harness before and
  after adding it (789,692,008 -> 789,695,411 Ir, +0.0004%, noise, on the
  fixture composition at that point; the harness's event count changed
  again afterward for unrelated reasons, see the "Harness correction"
  section above).

`latest_scheduled` is called from exactly one place (`classify`, confirmed
by grep before making the change), so no other call site needed updating.
`dag_retry.rs` -- including `node_outcome`'s own separate
`dispatched_activity_names` call and `is_compensation_dispatch`'s public
signature -- is **untouched**: this is deliberately the smallest fix that
moves the counter, not a broader refactor of every redundant call site in
the module.

Behavior is unchanged: `latest_scheduled` receives the exact same
`BTreeSet<&str>` value it used to compute internally (same `events` slice,
same construction), just computed once instead of once per node.

## Measurement

Both binaries built from the identical harness/`Cargo.toml` bench
declaration, differing only by the `dag_graph.rs` diff above, same
`valgrind --tool=callgrind --branch-sim=no --cache-sim=no` and
`valgrind --tool=dhat` invocations, same session.

### Instructions (Ir)

| | Instructions (Ir) |
|---|---|
| Before | 853,441,425 |
| After  | 557,244,524 |
| **Reduction** | **296,196,901 (34.71%)** |

Clears the >=5% floor by ~7x.

The after-trace's flat profile shows `node_outcome`'s self-cost unchanged at
**28,918,500** Ir and `has_skip_marker`'s at **11,757,300** Ir -- both
identical to the baseline down to the last instruction, independent
confirmation that nothing in `dag_retry.rs` (untouched by this change) or in
the parts of `dag_graph.rs` this change doesn't touch shifted:

```
557,244,524 (100.0%)  PROGRAM TOTALS

125,513,119 (22.52%)  __memcmp_avx2_movbe [libc]
 69,883,800 (12.54%)  core::slice::sort::stable::drift::sort
 51,744,000 ( 9.29%)  BTreeMap::bulk_build_from_sorted_iter
 46,520,700 ( 8.35%)  <Map<I,F> as Iterator>::fold
 38,053,500 ( 6.83%)  <Vec<T> as SpecFromIter<T,I>>::from_iter
 28,918,500 ( 5.19%)  autumn_harvest_plugin::dag_retry::node_outcome   <- unchanged
 27,255,747 ( 4.89%)  malloc.c:_int_malloc
 11,757,300 ( 2.11%)  autumn_harvest_plugin::dag_graph::has_skip_marker   <- unchanged
```

The `BTreeSet`-construction family (`memcmp` + `sort` + `bulk_build_from_
sorted_iter` + `Vec::from_iter` + `Map::fold`, the same lines the baseline
profile above attributes to the two `dispatched_activity_names` call sites)
drops from 69.21% of a larger total to a smaller share of a smaller total --
exactly the shape of "one of two redundant call sites removed, the other
(`node_outcome`'s) left at its original, now proportionally larger, cost".

### Allocations (`valgrind --tool=dhat`)

| dhat | Before | After | Reduction |
|---|---|---|---|
| Total blocks | 749,237 | 485,237 | 264,000 (**35.24%**) |
| Total bytes  | 185,642,993 | 105,386,993 | 80,256,000 (**43.23%**) |

Both independently clear the alternate >=10%-allocation floor as well as the
primary Ir floor.

### Correctness

* `cargo fmt -p autumn-harvest-plugin -- --check` -- clean.
* `cargo clippy -p autumn-harvest-plugin --lib --benches --features "webhooks,mcp,metrics,connectors,unified-dag-execution" -- -D warnings` -- clean.
  (`--all-features` also enables `kafka`, whose `rdkafka-sys` build requires
  system `libcurl` headers unavailable in this sandbox -- an environment
  limitation unrelated to this change, not a code issue; every feature this
  crate can build in-sandbox was exercised, including `unified-dag-execution`.)
* `cargo clippy -p autumn-harvest --all-features -- -D warnings` -- clean
  (`dag_retry.rs` is untouched by this change; this just confirms nothing
  else in the workspace regressed).
* `cargo test -p autumn-harvest-plugin --lib --features "webhooks,mcp,metrics,connectors,unified-dag-execution"` --
  **1,234 passed, 0 failed**, including all **40** `dag_graph::tests::*`
  unit tests (39 unchanged + 1 new, `gate_only_dag_classifies_without_an_activity_node`).

No test's expected value needed to change: `latest_scheduled` receives the
identical `BTreeSet<&str>` value it used to build internally, just built
once per `build_run_graph` call instead of once per node.

## Reproduce

```bash
BIN=$(cargo bench -p autumn-harvest-plugin --no-default-features \
  --bench dag_graph_profile --no-run --message-format=json 2>/dev/null \
  | jq -r 'select(.reason=="compiler-artifact" and .target.name=="dag_graph_profile") | .executable')

# Instruction count:
valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
callgrind_annotate --threshold=98 cg.out | head -30

# Allocation counts/bytes:
valgrind --tool=dhat --dhat-out-file=dhat.json "$BIN"
```

`DAG_GRAPH_PROFILE_REPS` (default 300) repeats the whole `build_run_graph`
call if more valgrind wall-time headroom is needed.

## Scope note: a second, untouched redundant call site

`dag_retry::node_outcome` (called once per non-gate node directly from
`build_run_graph`, plus recursively from `resolved_upstream_status` during
gate-reachability checks) makes its **own** independent
`dispatched_activity_names(events)` call -- self-cost 28,918,500 Ir,
confirmed byte-identical before and after this change. It is a real,
measurable redundancy of the same shape as the one this change fixes, but
it is deliberately **out of scope** here: the assigned fix is the smallest
change that moves the counter for the `latest_scheduled` call site
specifically, not a broader refactor of every redundant call to
`dispatched_activity_names` in the module. A follow-up change threading
`dispatched` through `node_outcome` (and, more invasively, through
`task_reach`/`resolved_upstream_status`/`gate_status` for the recursive gate-
reachability call sites) would be a reasonable next Bolt pass, profiled and
evidenced independently.
