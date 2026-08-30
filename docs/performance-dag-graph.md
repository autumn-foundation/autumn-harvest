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
aggregation layer) -- not a degenerate flat chain. The paired 202-event
history mixes succeeded, failed, multi-attempt-retried, condition-skipped
(`dag_skip:{idx}`, issue #482), and never-reached activities; gates resolved
by signal, resolved by race-timer timeout (issue #476/#746), and left
unresolved; and a couple of issue #780 compensator dispatches against
already-dispatched forward nodes, so the exclusion `dispatched_activity_names`
exists for is genuinely exercised. This is the real public entry point
(`build_run_graph`), not a synthetic microbenchmark of a pre-selected
function.

## Profile

```bash
BIN=$(cargo bench -p autumn-harvest-plugin --no-default-features \
  --bench dag_graph_profile --no-run --message-format=json 2>/dev/null \
  | jq -r 'select(.reason=="compiler-artifact" and .target.name=="dag_graph_profile") | .executable')
valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
callgrind_annotate --threshold=98 cg.out
```

Baseline (unmodified `HEAD`):

```
955,049,746 (100.0%)  PROGRAM TOTALS

193,500,241 (20.26%)  __memcmp_avx2_movbe [libc]
128,535,000 (13.46%)  alloc::collections::btree::map::BTreeMap<K,V,A>::bulk_build_from_sorted_iter
100,265,700 (10.50%)  <alloc::vec::Vec<T> as SpecFromIter<T,I>>::from_iter
 78,262,500 ( 8.19%)  <core::iter::adapters::map::Map<I,F> as Iterator>::fold
 63,897,000 ( 6.69%)  core::slice::sort::stable::drift::sort
 58,154,947 ( 6.09%)  <alloc::collections::btree::map::BTreeMap<K,V,A> as Drop>::drop
 51,412,500 ( 5.38%)  autumn_harvest_plugin::dag_retry::node_outcome
 42,829,965 ( 4.48%)  malloc.c:_int_free
 40,695,502 ( 4.26%)  malloc.c:_int_malloc
 28,995,512 ( 3.04%)  malloc.c:malloc
 12,046,800 ( 1.26%)  autumn_harvest_plugin::dag_graph::has_skip_marker
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
    calls=30300   Ir=286,103,468   (29.96% of total)

fn=(966) <Map<I,F> as Iterator>::fold   [build_run_graph's per-task closure,
                                          i.e. classify -> latest_scheduled]
  cfn=(970) <BTreeSet<T> as FromIterator<T>>::from_iter   (two call-site arcs)
    calls=10,500 + 16,200 = 26,700   Ir=99,102,512 + 152,889,390 = 251,991,902
    (26.38% of total)
```

`26,700 = 89 non-gate tasks x 300 reps` -- exactly one redundant
`dispatched_activity_names` rebuild per node per `build_run_graph` call, all
computing the identical result for the same `events` slice. Combined, the
two call sites account for **56.34%** of total program instructions
(538,095,370 / 955,049,746); the `latest_scheduled`-attributable share alone
(26.38%) clears the >=5%-of-workload floor more than 5x on its own.

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
of the BTreeSet-construction cost (the 26.38%/251,991,902 Ir measured above)
while leaving `node_outcome`'s own separate, out-of-scope call to
`dispatched_activity_names` (29.96%/286,103,468 Ir) untouched.

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
  call site:

  ```rust
  let dispatched = crate::dag_retry::dispatched_activity_names(&events);
  ```

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
| Before | 955,049,746 |
| After  | 674,789,613 |
| **Reduction** | **280,260,133 (29.35%)** |

Clears the >=5% floor by ~5.9x.

The after-trace's call graph confirms the mechanism directly: `node_outcome`
(978 in the after trace) still calls `<BTreeSet<T> as FromIterator<T>>::from_iter`
exactly **30,300** times -- byte-for-byte identical to the baseline's 30,300,
confirming the untouched call site truly is untouched -- while the *only*
other caller of that same function is now `build_run_graph` itself, called
**300** times (once per `build_run_graph` invocation, i.e. once per rep,
down from the 26,700 per-node calls the old `latest_scheduled` made). The
after-trace's flat profile shows `node_outcome`'s self-cost unchanged at
**51,412,500** Ir (identical to the baseline) and `has_skip_marker`'s
self-cost unchanged at **12,046,800** Ir -- both untouched code paths
reporting byte-identical costs is independent confirmation that nothing
else in the measured workload shifted.

```
674,789,613 (100.0%)  PROGRAM TOTALS

128,642,695 (19.06%)  __memcmp_avx2_movbe [libc]
 84,672,900 (12.55%)  <Map<I,F> as Iterator>::fold
 69,003,000 (10.23%)  BTreeMap::bulk_build_from_sorted_iter
 54,171,300 ( 8.03%)  <Vec<T> as SpecFromIter<T,I>>::from_iter
 51,412,500 ( 7.62%)  autumn_harvest_plugin::dag_retry::node_outcome   <- unchanged
 39,401,283 ( 5.84%)  malloc.c:_int_malloc
 34,302,600 ( 5.08%)  core::slice::sort::stable::drift::sort
 31,796,744 ( 4.71%)  malloc.c:_int_free
 31,279,747 ( 4.64%)  <BTreeMap<K,V,A> as Drop>::drop
```

### Allocations (`valgrind --tool=dhat`)

| dhat | Before | After | Reduction |
|---|---|---|---|
| Total blocks | 894,621 | 604,221 | 290,400 (**32.46%**) |
| Total bytes  | 207,011,916 | 121,687,116 | 85,324,800 (**41.22%**) |

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
  **1,233 passed, 0 failed**, including all **39** `dag_graph::tests::*`
  unit tests, unchanged.

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
`dispatched_activity_names(events)` call -- the 29.96%/286,103,468 Ir share
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
