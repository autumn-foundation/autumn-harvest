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
aggregation layer) -- not a degenerate flat chain. The paired 211-event
history mixes succeeded, failed, multi-attempt-retried, condition-skipped
(`dag_skip:{idx}`, issue #482), and never-reached activities; gates resolved
by signal, resolved by race-timer timeout (issue #476/#746), and left
unresolved; and issue #780 compensator dispatches against already-dispatched
forward nodes -- including one recorded under a *current node's own name*,
so the exclusion `dispatched_activity_names` exists for is genuinely
exercised. This is the real public entry point (`build_run_graph`), not a
synthetic microbenchmark of a pre-selected function.

### Harness correction (post-review)

A GitHub Codex review of the PR that introduced this harness caught two
realism gaps in the fixture, both now fixed and reflected in every number on
this page:

1. **Every gate reported `Pending` regardless of the signal/timer events
   recorded for it.** `gate_status` checks upstream reachability *before*
   resolution: an upstream that failed, was condition-skipped, or was never
   reached makes the default `AllSuccess` trigger rule report
   `SkippedByTrigger`/`NotReached`, and the gate is classified `Pending`
   without ever looking at `gate_resolution`. The fixture's `idx % 5` outcome
   pattern happened to assign a failed, skipped, or never-reached outcome to
   3 of the 4 gates' upstream tasks, so the `SignalReceived` /
   `TimerFired` / left-unresolved events pushed for those gates were dead
   weight -- present in the history, never actually read. Fixed by forcing
   every gate-upstream task index (`GATE_UPSTREAM_INDICES`) to a plain
   first-attempt success, so all four gates are genuinely *reached* and each
   resolves the way its pushed events say it should (confirmed directly:
   `gate_0: Succeeded`, `gate_1: TimedOut`, `gate_2: Waiting`,
   `gate_3: Waiting`).
2. **Neither compensator dispatch exercised `is_compensation_dispatch`.**
   `latest_scheduled` checks `name == node` before checking whether an event
   is a compensation dispatch; the two original compensator events
   (`undo_t001`, `undo_t002`) are not themselves node names in this DAG, so
   that check short-circuits before the exclusion predicate ever runs for
   them. Fixed by adding a third compensator dispatch recorded under a
   *current* node's own name (`t001`, compensating the separately-dispatched
   `t002`) after `t001`'s genuine forward dispatch, so `latest_scheduled`'s
   reverse scan must recognize and skip it to find the real one (confirmed
   directly: `t001: Succeeded`, matching its genuine dispatch, not the
   compensator envelope).

Neither gap invalidated the fix itself (both bugs are in code paths this
change does not touch), but they did mean the workload's realism claims
overstated what the fixture actually exercised. The numbers below are from
the corrected harness.

## Profile

```bash
BIN=$(cargo bench -p autumn-harvest-plugin --no-default-features \
  --bench dag_graph_profile --no-run --message-format=json 2>/dev/null \
  | jq -r 'select(.reason=="compiler-artifact" and .target.name=="dag_graph_profile") | .executable')
valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
callgrind_annotate --threshold=98 cg.out
```

Baseline (unmodified `HEAD`, corrected harness):

```
1,191,754,669 (100.0%)  PROGRAM TOTALS

285,761,963 (23.98%)  __memcmp_avx2_movbe [libc]
181,852,200 (15.26%)  core::slice::sort::stable::drift::sort
140,113,800 (11.76%)  alloc::collections::btree::map::BTreeMap<K,V,A>::bulk_build_from_sorted_iter
104,061,900 ( 8.73%)  <alloc::vec::Vec<T> as SpecFromIter<T,I>>::from_iter
 81,004,800 ( 6.80%)  <core::iter::adapters::map::Map<I,F> as Iterator>::fold
 61,781,451 ( 5.18%)  <alloc::collections::btree::map::BTreeMap<K,V,A> as Drop>::drop
 49,817,700 ( 4.18%)  autumn_harvest_plugin::dag_retry::node_outcome
 44,951,941 ( 3.77%)  malloc.c:_int_free
 39,840,885 ( 3.34%)  malloc.c:_int_malloc
 30,842,156 ( 2.59%)  malloc.c:malloc
```

None of these lines is named `dispatched_activity_names` or
`latest_scheduled` -- both are aggressively inlined into their call sites, so
their cost surfaces under the generic `BTreeSet<&str>: FromIterator`
machinery (`from_iter`, `bulk_build_from_sorted_iter`, the sort it drives,
the `Drop` glue, and the `memcmp`-based `&str` ordering comparisons) rather
than under their own names. Cross-referencing the raw callgrind call graph
(`cfn=`/`calls=` arcs, grouped by caller) isolates the two contributing call
sites precisely:

```
fn=(968) autumn_harvest_plugin::dag_retry::node_outcome
  cfn=(970) <BTreeSet<T> as FromIterator<T>>::from_iter
    calls=29,100   Ir=400,536,277   (33.61% of total)

fn=(966) <Map<I,F> as Iterator>::fold   [build_run_graph's per-task closure,
                                          i.e. classify -> latest_scheduled]
  cfn=(970) <BTreeSet<T> as FromIterator<T>>::from_iter   (two call-site arcs)
    calls=9,300 + 17,400 = 26,700   Ir=127,957,430 + 239,442,093 = 367,399,523
    (30.83% of total)
```

`node_outcome`'s 29,100 calls = `89 non-gate tasks x 300 reps` (26,700,
called directly from `build_run_graph`) `+ 2 upstreams x 4 gates x 300 reps`
(2,400, called recursively from `resolved_upstream_status` while checking
gate reachability -- only exercised now that the harness correction above
makes gates genuinely reachable). `latest_scheduled`'s 26,700 = exactly one
redundant `dispatched_activity_names` rebuild per non-gate node per
`build_run_graph` call, all computing the identical result for the same
`events` slice. Combined, the two call sites account for **64.45%** of total
program instructions (767,935,800 / 1,191,754,669); the
`latest_scheduled`-attributable share alone (30.83%) clears the
>=5%-of-workload floor more than 6x on its own.

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
threading it down as a parameter should remove the `latest_scheduled`-share
of the BTreeSet-construction cost (the 30.83%/367,399,523 Ir measured above)
while leaving `node_outcome`'s own separate, out-of-scope call to
`dispatched_activity_names` (33.61%/400,536,277 Ir) untouched.

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
  after adding it: 789,692,008 -> 789,695,411 Ir (+3,403, +0.0004%, noise).

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
| Before | 1,191,754,669 |
| After  | 789,692,008 |
| **Reduction** | **402,062,661 (33.74%)** |

Clears the >=5% floor by ~6.7x.

The after-trace's call graph confirms the mechanism directly: `node_outcome`
still calls `<BTreeSet<T> as FromIterator<T>>::from_iter` exactly **29,100**
times -- byte-for-byte identical to the baseline's 29,100, confirming the
untouched call site truly is untouched -- while the *only* other caller of
that same function is now `build_run_graph` itself, called **300** times
(once per `build_run_graph` invocation, i.e. once per rep, down from the
26,700 per-node calls the old `latest_scheduled` made -- that call site is
gone entirely from the after trace). The after-trace's flat profile shows
`node_outcome`'s self-cost unchanged at **49,817,700** Ir -- identical to the
baseline down to the last instruction -- independent confirmation that
nothing else in the measured workload shifted.

```
789,692,008 (100.0%)  PROGRAM TOTALS

174,475,395 (22.09%)  __memcmp_avx2_movbe [libc]
 95,814,600 (12.13%)  core::slice::sort::stable::drift::sort
 87,049,800 (11.02%)  <Map<I,F> as Iterator>::fold
 73,823,400 ( 9.35%)  BTreeMap::bulk_build_from_sorted_iter
 55,195,500 ( 6.99%)  <Vec<T> as SpecFromIter<T,I>>::from_iter
 49,817,700 ( 6.31%)  autumn_harvest_plugin::dag_retry::node_outcome   <- unchanged
 38,176,901 ( 4.83%)  malloc.c:_int_malloc
 32,609,451 ( 4.13%)  <BTreeMap<K,V,A> as Drop>::drop
 32,450,291 ( 4.11%)  malloc.c:_int_free
```

### Allocations (`valgrind --tool=dhat`)

| dhat | Before | After | Reduction |
|---|---|---|---|
| Total blocks | 929,718 | 612,918 | 316,800 (**34.07%**) |
| Total bytes  | 213,916,454 | 123,522,854 | 90,393,600 (**42.26%**) |

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
`dispatched_activity_names(events)` call -- the 33.61%/400,536,277 Ir share
measured above, confirmed unchanged after this change. It is a real,
measurable redundancy of the same shape as the one this change fixes, but
it is deliberately **out of scope** here: the assigned fix is the smallest
change that moves the counter for the `latest_scheduled` call site
specifically, not a broader refactor of every redundant call to
`dispatched_activity_names` in the module. A follow-up change threading
`dispatched` through `node_outcome` (and, more invasively, through
`task_reach`/`resolved_upstream_status`/`gate_status` for the recursive gate-
reachability call sites) would be a reasonable next Bolt pass, profiled and
evidenced independently.
