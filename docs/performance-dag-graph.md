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
aggregation layer) -- not a degenerate flat chain. The paired 111-event
history mixes succeeded, failed, multi-attempt-retried, condition-skipped
(`dag_skip:{idx}`, issue #482), and never-reached (unreachable) activities;
gates resolved by signal, resolved by race-timer timeout (issue #476/#746),
and left unresolved; and issue #780 compensator dispatches against
already-dispatched forward nodes -- including one recorded under a *current
node's own name*, so the exclusion `dispatched_activity_names` exists for is
genuinely exercised. This is the real public entry point (`build_run_graph`),
not a synthetic microbenchmark of a pre-selected function.

### Harness correction (post-review)

A GitHub Codex review of the PR that introduced this harness caught three
realism gaps in the fixture, all now fixed and reflected in every number on
this page:

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
   (`gate_2`/`gate_3`, `Waiting`) correctly make their own downstream layer4
   nodes unreachable too, cascading further into later layers: a real DAG
   paused on two unanswered approval gates has exactly this shape.

Fixes 1 and 3 interact: the reachability tracking that fix 3 introduces is
also what fix 1's gate resolutions now rely on (a gate is reachable only
when its own upstreams are `done`), so they landed as one change to
`build_events`. Verified directly (temporary debug print, since removed):
`gate_0: Succeeded`, `gate_1: TimedOut`, `gate_2: Waiting`, `gate_3: Waiting`,
`t001: Succeeded` (its genuine dispatch, correctly winning over the later
compensator envelope sharing its name) -- and a full-graph status-mix dump
showing every node's status is one a real walker could produce: 34 succeeded,
1 failed, 1 skipped, 1 gate timed-out (counts as succeeded downstream), 2
gates waiting, 54 legitimately pending (some never reached, most cascaded
from the two waiting gates or an upstream failure/skip further back).

None of these three gaps invalidated the fix itself (all three bugs are in
the harness, not in code paths this change touches), but they did mean the
workload's realism claims overstated what the fixture actually exercised.
The numbers below are from the fully corrected harness; event count dropped
from the original 202 (then 211, after fix 2) to 111, since a
reachability-consistent history necessarily has far fewer of the 93 tasks
actually dispatched.

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
749,286,635 (100.0%)  PROGRAM TOTALS

165,660,649 (22.11%)  __memcmp_avx2_movbe [libc]
113,106,600 (15.10%)  core::slice::sort::stable::drift::sort
 86,769,000 (11.58%)  alloc::collections::btree::map::BTreeMap<K,V,A>::bulk_build_from_sorted_iter
 59,731,500 ( 7.97%)  <alloc::vec::Vec<T> as SpecFromIter<T,I>>::from_iter
 38,589,867 ( 5.15%)  <alloc::collections::btree::map::BTreeMap<K,V,A> as Drop>::drop
 34,360,800 ( 4.59%)  <core::iter::adapters::map::Map<I,F> as Iterator>::fold
 33,747,955 ( 4.50%)  malloc.c:_int_free
 23,135,400 ( 3.09%)  autumn_harvest_plugin::dag_retry::node_outcome
 10,822,800 ( 1.44%)  autumn_harvest_plugin::dag_graph::has_skip_marker
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
to **66.61%** of total instructions (499,208,566 / 749,286,635) -- comfortably
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
23,135,400 Ir above -- untouched.

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
  fixture composition at that point; the harness's event count changed again
  afterward for an unrelated reason, see the next "Harness correction" round
  above).

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
| Before | 749,286,635 |
| After  | 487,802,199 |
| **Reduction** | **261,484,436 (34.90%)** |

Clears the >=5% floor by ~7x.

The after-trace's flat profile shows `node_outcome`'s self-cost unchanged at
**23,135,400** Ir and `has_skip_marker`'s at **10,822,800** Ir -- both
identical to the baseline down to the last instruction, independent
confirmation that nothing in `dag_retry.rs` (untouched by this change) or in
the parts of `dag_graph.rs` this change doesn't touch shifted:

```
487,802,199 (100.0%)  PROGRAM TOTALS

104,696,370 (21.46%)  __memcmp_avx2_movbe [libc]
 59,593,800 (12.22%)  core::slice::sort::stable::drift::sort
 45,717,000 ( 9.37%)  BTreeMap::bulk_build_from_sorted_iter
 36,843,600 ( 7.55%)  <Map<I,F> as Iterator>::fold
 31,668,300 ( 6.49%)  <Vec<T> as SpecFromIter<T,I>>::from_iter
 25,652,048 ( 5.26%)  malloc.c:_int_malloc
 23,742,646 ( 4.87%)  malloc.c:_int_free
 23,135,400 ( 4.74%)  autumn_harvest_plugin::dag_retry::node_outcome   <- unchanged
 10,822,800 ( 2.22%)  autumn_harvest_plugin::dag_graph::has_skip_marker   <- unchanged
```

The `BTreeSet`-construction family (`memcmp` + `sort` + `bulk_build_from_
sorted_iter` + `Vec::from_iter` + `Map::fold`, the same lines the baseline
profile above attributes to the two `dispatched_activity_names` call sites)
drops from 66.61% of a larger total to a smaller share of a smaller total --
exactly the shape of "one of two redundant call sites removed, the other
(`node_outcome`'s) left at its original, now proportionally larger, cost".

### Allocations (`valgrind --tool=dhat`)

| dhat | Before | After | Reduction |
|---|---|---|---|
| Total blocks | 734,491 | 470,491 | 264,000 (**35.94%**) |
| Total bytes  | 183,584,976 | 103,328,976 | 80,256,000 (**43.72%**) |

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
`dispatched_activity_names(events)` call -- self-cost 23,135,400 Ir,
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
