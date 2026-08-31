# `dag_graph::build_run_graph`: hoist the per-node `dispatched_activity_names` rebuild

This note documents a profiling pass over
`autumn_harvest_plugin::dag_graph::build_run_graph` -- the pure projection
behind `GET /dag-run-graph` (issue #690) that reconstructs a unified DAG
run's node topology (status, timing, attempts, truncated error) purely from
the registered `DagDefinition` and the run's recorded `WorkflowEvent`
history. Wall-clock timing is not admissible evidence on this (shared-vCPU)
machine -- every number below is a deterministic instruction count
(`valgrind --tool=callgrind`) or allocation count/bytes
(`valgrind --tool=dhat`), both reproducible bit-for-bit on the *same* binary
and environment (unlike wall time, they don't vary run to run here). They
are not a portable cross-machine guarantee: a different libc's dynamically
selected implementation (`__memcmp_avx2_movbe` below is one such
CPU-feature-dispatched routine) or a different toolchain can generate
different instruction counts for the same source, so the specific figures
on this page are this session's, stable within measurement noise -- the
percentage reductions and the mechanism they demonstrate are the portable
claim.

## Workload

`benches/dag_graph_profile.rs` calls `build_run_graph` 300 times
(`DAG_GRAPH_PROFILE_REPS`) against a realistic 93-task DAG built via
`autumn_harvest::dag::DagBuilder`: eight layers with genuine diamond
fan-out/fan-in shapes (a root, an 8-way fan-out, a 16-node 2-way-fan-in
layer, 4 signal-gate nodes, a 16-node layer fed by both gates and earlier
activities, two more 20-node 2-way-fan-in layers, and an 8-node 4-way-fan-in
aggregation layer) -- not a degenerate flat chain. The paired 164-event
history mixes succeeded, failed, multi-attempt-retried, condition-skipped
(`dag_skip:{idx}`, issue #482), and never-reached (unreachable) activities,
plus all four gates resolved (two by signal, one by a
`TimerStarted`/`TimerFired` race-timer timeout pair (issue #476/#746), one by
a signal beating its own deadline). This is the real public entry point
(`build_run_graph`), not a synthetic microbenchmark of a pre-selected
function.

### Harness correction (post-review)

A GitHub Codex review of the PR that introduced this harness caught seven
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
5. **Fix 4's own "last gate waiting" model was still incomplete, found in the
   next review round.** The real unified-DAG walker's sequential level loop
   means an unresolved gate stalls the walker at *that specific level* --
   which blocks every *later* level from ever running at all, not merely the
   nodes that happen to declare that gate as a direct upstream. Layer4
   activities downstream of the three gates that *did* resolve were still
   getting events, which is a history the real walker cannot produce either:
   reaching any level after `gate_3`'s requires `gate_3`'s own `.await` to
   have resolved, full stop, regardless of which specific upstream edges a
   later node declares. Compounding this, a genuinely `Waiting` gate also
   cannot coexist with the issue #780 compensator dispatches fix 2 added:
   compensation requires the whole DAG run to have failed *terminally*, which
   is inconsistent with a live, still-suspended-mid-gate `RUNNING` history.
   Modeling both correctly in one history (a `Waiting` gate that legitimately
   blocks its own and every later level, *and* a separate terminally-failed
   history for compensation) was judged not worth the added complexity for a
   workload whose value is realistic *volume and diversity*, not gate-state
   feature completeness. Fixed by having **all four gates resolve** instead
   (two by signal, one by timer, one by signal beating its own deadline) --
   restoring full diversity across every layer with a single, unambiguously
   producible `RUNNING` history -- and removing the compensator dispatches
   entirely; `is_compensation_dispatch`'s exclusion correctness is covered by
   `dag_graph.rs`'s own unit tests instead
   (`a_compensation_dispatch_is_not_read_as_a_same_named_forward_node` et
   al.), which don't need this fixture's history to be internally consistent
   with a *different* run's terminal-failure state.
6. **The condition-skip markers had no configured condition to justify them,
   found in the next review round.** The real walker only records a
   `dag_skip:{idx}` marker for `DagDispatchDecision::SkipByCondition`, which
   requires the task to actually carry a `.condition(...)` predicate --
   `build_dag` never called `.condition(...)` on any task, so the five
   `dag_skip` markers the fixture recorded for its condition-skipped
   activities were events the real walker could never have produced for
   those specific (condition-less) tasks either. Fixed (at the time) by
   giving every layer4-onward activity task a `.condition(|_ups| true)`
   predicate in `build_dag`, on the reasoning that only the predicate's
   *presence* mattered since `dag_graph.rs`'s classification logic reads
   recorded history events, not `DagTask::condition`. Fix 7 below found that
   reasoning incomplete.
7. **The condition predicate's own return value still didn't agree with which
   tasks the fixture marked condition-skipped, found in the next review
   round.** Fix 6 gave every layer4-onward task a `.condition(...)` predicate,
   but every one of them unconditionally returned `true` -- so
   `DagTask::dispatch_decision` would run every one of those tasks for real,
   never `SkipByCondition`, regardless of which tasks `build_events`
   separately chose (via its own `activity_seq % 6 == 5` rotation) to record
   a `dag_skip` marker for. The marker events fix 6 called "structurally
   legitimate" were still unproducible by the real walker: the one thing that
   actually decides `SkipByCondition` -- the predicate evaluating `false` --
   never happened for any task. Fixed by making the two sides agree: each
   layer4-onward task's position among its 64 peers (`this_pos`, assigned in
   creation order, unconditionally -- not gated by reachability, so
   `build_dag`'s predicate and `build_events`'s outcome rotation always mean
   the same task by the same number) sets both its `.condition(...)`
   predicate (`this_pos % 6 != 5`) and the `match this_pos % 6` arm
   `build_events` uses to decide its outcome, so a task only ever gets a
   `dag_skip` marker when its own predicate would actually have evaluated
   `false`. Switching the outcome rotation from a reachability-gated counter
   (which only advanced for tasks that turned out reachable, so `build_dag`
   -- built before any reachability is known -- could never have matched it)
   to this unconditional per-task position is what makes the two sides
   possible to keep in agreement at all.

Fixes 1 and 3 interact: the reachability tracking that fix 3 introduces is
also what fix 1's gate resolutions now rely on (a gate is reachable only
when its own upstreams are `done`), so they landed as one change to
`build_events`. Verified directly (temporary debug print, since removed):
`gate_0: Succeeded`, `gate_1: TimedOut`, `gate_2: Succeeded`,
`gate_3: Succeeded` -- and a full-graph status-mix dump showing every node's
status is one a real walker could produce.

None of these seven gaps invalidated the fix itself (all seven bugs are in
the harness, not in code paths this change touches), but they did mean the
workload's realism claims overstated what the fixture actually exercised.
The numbers below are from the fully corrected harness; event count moved
from the original 202 (211 after fix 2, 111 after fix 3, 137 after fix 4,
175 after fix 5, unchanged through fix 6, 164 after fix 7), since each
round's reachability/outcome model dispatches a different subset of the 93
tasks than the one before it.

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
826,812,687 (100.0%)  PROGRAM TOTALS

170,887,673 (20.67%)  __memcmp_avx2_movbe [libc]
116,901,000 (14.14%)  alloc::collections::btree::map::BTreeMap<K,V,A>::bulk_build_from_sorted_iter
 83,469,900 (10.10%)  <alloc::vec::Vec<T> as SpecFromIter<T,I>>::from_iter
 56,525,400 ( 6.84%)  core::slice::sort::stable::drift::sort
 55,119,300 ( 6.67%)  <core::iter::adapters::map::Map<I,F> as Iterator>::fold
 51,826,500 ( 6.27%)  <alloc::collections::btree::map::BTreeMap<K,V,A> as Drop>::drop
 39,502,978 ( 4.78%)  malloc.c:_int_free
 35,843,400 ( 4.34%)  autumn_harvest_plugin::dag_retry::node_outcome
 11,408,100 ( 1.38%)  autumn_harvest_plugin::dag_graph::has_skip_marker
```

None of these lines is named `dispatched_activity_names` or
`latest_scheduled` -- both are aggressively inlined into their call sites, so
their cost surfaces under the generic `BTreeSet<&str>: FromIterator`
machinery (`from_iter`, `bulk_build_from_sorted_iter`, the sort it drives,
the `Drop` glue, and the `memcmp`-based `&str` ordering comparisons) rather
than under their own names: `__memcmp_avx2_movbe` +
`BTreeMap::bulk_build_from_sorted_iter` +
`<Vec<T> as SpecFromIter<T,I>>::from_iter` +
`<Map<I,F> as Iterator>::fold` + `core::slice::sort::stable::drift::sort` +
`<BTreeMap<K,V,A> as Drop>::drop` alone sum to **64.67%** of total
instructions (534,729,773 / 826,812,687) -- comfortably clearing the
>=5%-of-workload floor -- with `node_outcome`'s and `has_skip_marker`'s own
(untouched-by-this-fix) self-costs shown separately above for the
before/after comparison below.

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
35,843,400 Ir above -- untouched.

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
  `dispatched_activity_names` branch, so it does not change the mechanism
  measured below; the harness's event count and exact Ir figures have since
  moved for the unrelated reasons in the "Harness correction" section above.

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
| Before | 826,812,687 |
| After  | 558,799,610 |
| **Reduction** | **268,013,077 (32.42%)** |

Clears the >=5% floor by ~6.5x.

The after-trace's flat profile shows `node_outcome`'s self-cost unchanged at
**35,843,400** Ir and `has_skip_marker`'s at **11,408,100** Ir -- both
identical to the baseline down to the last instruction, independent
confirmation that nothing in `dag_retry.rs` (untouched by this change) or in
the parts of `dag_graph.rs` this change doesn't touch shifted:

```
558,799,610 (100.0%)  PROGRAM TOTALS

111,581,538 (19.97%)  __memcmp_avx2_movbe [libc]
 61,593,000 (11.02%)  BTreeMap::bulk_build_from_sorted_iter
 58,615,500 (10.49%)  <Map<I,F> as Iterator>::fold
 44,265,900 ( 7.92%)  <Vec<T> as SpecFromIter<T,I>>::from_iter
 35,843,400 ( 6.41%)  autumn_harvest_plugin::dag_retry::node_outcome   <- unchanged
 34,482,621 ( 6.17%)  malloc.c:_int_malloc
 29,782,200 ( 5.33%)  core::slice::sort::stable::drift::sort
 11,408,100 ( 2.04%)  autumn_harvest_plugin::dag_graph::has_skip_marker   <- unchanged
```

The `BTreeSet`-construction family (`memcmp` + `bulk_build_from_sorted_iter`
+ `Vec::from_iter` + `Map::fold` + `sort` + `Drop`, the same lines the
baseline profile above attributes to the two `dispatched_activity_names`
call sites) drops from 64.67% of a larger total to a smaller share of a
smaller total -- exactly the shape of "one of two redundant call sites
removed, the other (`node_outcome`'s) left at its original, now
proportionally larger, cost".

### Allocations (`valgrind --tool=dhat`)

| dhat | Before | After | Reduction |
|---|---|---|---|
| Total blocks | 813,525 | 523,125 | 290,400 (**35.70%**) |
| Total bytes  | 197,652,103 | 112,327,303 | 85,324,800 (**43.17%**) |

Both independently clear the alternate >=10%-allocation floor as well as the
primary Ir floor. The blocks/bytes reduction is identical to every prior
harness correction: it is purely `(non-gate node count) x (reps - 1)` fewer
`BTreeSet<&str>` allocations at `latest_scheduled`'s call site, a quantity
that does not depend on the fixture's event content or ordering.

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
`dispatched_activity_names(events)` call -- self-cost 37,469,100 Ir,
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
