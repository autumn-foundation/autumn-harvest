# `ReplayDebugger::trace_snapshot`: quantifying the documented O(N²) prefix-replay cost, and why the obvious local fix is unsafe

This note profiles `autumn_harvest::debugger::ReplayDebugger::trace_snapshot`
(issue #949's **library** replay-debugger arm — see "Scope" below, not the
packaged `harvest debug` CLI subcommand) against a realistic workload, turns
its already-documented `O(N²)` cost into precise, reproducible numbers, and
investigates whether a local (non-architectural) constant-factor reduction is
available. It is not: the one plausible local candidate is ruled out with
concrete source evidence for a real correctness hazard, not merely an
unmeasured or below-floor gain.
Wall-clock timing is not admissible evidence on this (shared-vCPU) machine —
every number below is a deterministic instruction or allocation count from
`valgrind --tool=callgrind` / `valgrind --tool=dhat`, reproducible bit-for-bit
on any machine.

**This is a findings-only document.** No production code changed as part of
this investigation — see "What changed in this PR" below.

## Context: an already-documented cost, not a discovered bug

`debugger.rs`'s own module doc comment states the cost class up front:

> Prefix replay is O(N²) in history length. [`ReplayDebugger::max_steps`]
> caps it (default [`DEFAULT_MAX_STEPS`]) and the resulting trace sets
> [`ReplayTrace::truncated`].

and `docs/replay-debugger.md`'s "Cost" section frames it qualitatively:

> A step is a full prefix replay, so building a complete trace of an
> `N`-event history performs `N` replays and is **O(N²)** in total work.
> That is fine for the interactive histories this tool is for (tens to low
> hundreds of events) and deliberately not how the CI gates work — they
> replay each history once.

So the complexity class was already known and already has a sanctioned
mitigation (`DEFAULT_MAX_STEPS = 500`, or `.max_steps(...)` for a library
caller — see "Scope" immediately below for why this is a library-side knob,
not a CLI flag). Neither existing doc gives concrete numbers, mechanism
attribution, or answers whether the constant factor behind that class is
addressable without raising or lowering the cap. That is what this note
adds: an empirical confirmation of the scaling curve across four input
sizes, a precise allocation-site attribution of what dominates the constant
factor, and — because the allocation-site data makes an "obvious" local fix
look tempting — a source-confirmed explanation of exactly why that fix would
silently corrupt results, closing off a plausible-sounding but unsound
future change before anyone attempts it.

## Scope: the library API, not the `harvest debug` CLI

`ReplayDebugger::trace_snapshot` is one of **two arms** `docs/replay-debugger.md`
documents, and this note profiles only the one that requires an embedder to
register their own compiled `#[workflow]` handler
(`ReplayDebugger::register_fn`/`.trace_json`/`.trace_snapshot`). **The shipped
`harvest debug replay` CLI subcommand never reaches this code path at all** —
confirmed by reading its entry point
(`autumn-harvest-cli/src/debug.rs::run_replay`), which unconditionally builds
its trace via:

```rust
let trace = ReplayTrace::from_history_capped(
    snapshot.workflow_name.clone(),
    snapshot.execution_id,
    &snapshot.events,
    max_steps.unwrap_or(usize::MAX),
);
```

— the **handler-free** projection `debugger.rs`'s own `# Cost` module-doc
section states is "always O(N) and needs no registered code at all". The
packaged `harvest` binary is statically linked and cannot register arbitrary
embedder workflow functions (`docs/replay-debugger.md`'s "The two arms"
section states this constraint directly), so it structurally cannot invoke
the prefix-replay path profiled below; grepping the whole workspace confirms
`trace_snapshot`/`trace_json` are called only from library consumers —
tests, examples, and this benchmark — never from `autumn-harvest-cli`'s
production `run_replay`/`debug_tui` code. `harvest debug replay --max-steps N`
is a real CLI flag, but it caps the CLI's own (cheap, O(N)) handler-free
walk, not the O(N²) cost this note measures.

So every workload-configuration claim below (`ReplayDebugger::new()` at
"library defaults", `DEFAULT_MAX_STEPS`) describes **an embedder calling
`ReplayDebugger` directly from their own Rust program**, not a default or
flag-reachable behavior of the packaged `harvest debug` command. An earlier
draft of this note conflated the two and claimed this workload models "a
real `harvest debug` invocation" — that framing was wrong and is corrected
here; the underlying instruction/allocation numbers are unaffected by the
correction (they were always measuring the library API), only their scope
was mis-stated.

## Workload

The harness is `autumn-harvest/benches/debugger_trace_profile.rs`, a
`harness = false` binary with its own `main()` — no criterion wall-clock
loop, so a profiler can be pointed at it directly with nothing but the
target workload running.

It reuses `benches/replay_profile_support.rs`'s `sequential_workflow` /
`build_history` — the exact same issue #135 realistic-payload workload
`replay_profile.rs` (and its own findings document,
`docs/performance-replay.md`) already use — so this harness measures
debugger tracing over the *same* documented shape, not a bespoke one
invented to flatter a particular change. `n` activities produce `2n + 1`
events (`WorkflowStarted` + `ActivityScheduled` + `ActivityCompleted` per
activity), each carrying a ~230-byte realistic JSON payload (an order line
item with a nested `items` array — `serde_json::Value::Object` is
`BTreeMap`-backed, since this workspace does not enable serde_json's
`preserve_order` feature, so cloning any of these payloads recurses through
`BTreeMap`'s clone machinery).

`ReplayDebugger::new()` is driven with **library defaults** — no
`.max_steps()` override, so `DEFAULT_MAX_STEPS = 500` applies: exactly what
an embedder gets from `ReplayDebugger::new()` without an explicit
`.max_steps(...)` override (see "Scope" above — the packaged `harvest debug`
CLI never reaches this code path, so "default" here means the library's
default, not the CLI's). Every swept `n`
(`20, 40, 80, 160` → `41, 81, 161, 321` events) stays under that cap, so no
run in the sweep is truncated; the harness asserts `!trace.truncated` and
`trace.steps.len() == total_events` to guarantee it is measuring the real
uncapped cost, not an artifact of hitting the ceiling. It also asserts every
step's `divergence.is_none()`, so a run that silently regressed into
reporting spurious non-determinism would fail the harness rather than
quietly profiling something other than the intended clean-replay path.

## Profile

### Instruction-count scaling (the asymptotic argument)

```
BIN=$(cargo bench -p autumn-harvest --no-default-features --features debugger \
  --bench debugger_trace_profile --no-run --message-format=json \
  | jq -r 'select(.reason == "compiler-artifact" and .target.name == "debugger_trace_profile") | .executable')

for n in 20 40 80 160; do
  DEBUGGER_TRACE_PROFILE_N=$n valgrind --tool=callgrind --branch-sim=no \
    --cache-sim=no --callgrind-out-file=callgrind_n$n.out "$BIN"
done
```

| `n` | events (`2n+1`) | Total Ir | Ratio vs. previous |
|----:|-----------------:|---------:|--------------------:|
| 20  | 41  |    18,842,911 | — |
| 40  | 81  |    67,428,049 | 3.5784x |
| 80  | 161 |   257,018,223 | 3.8117x |
| 160 | 321 |   998,527,995 | 3.8850x |

Each doubling of `n` should scale total work by a factor approaching **4.0x**
for a genuine `O(n²)` process (vs. `2.0x` for `O(n)`). The observed ratio
climbs monotonically toward 4.0x as `n` grows — 3.578x → 3.812x → 3.885x —
which is exactly the signature of a quadratic dominant term plus a shrinking
lower-order additive term (one-time harness/process setup, and the `O(n)`
`workflow_input(&snapshot.events).clone()` per step), confirming the
documented `O(N²)` class empirically rather than by reading the doc comment
alone.

### Instruction-count flat profile at n=80 (mechanism attribution)

```
callgrind_annotate --threshold=90 callgrind_n80.out
```

```
51,960,705 (20.22%)  _int_malloc
32,686,362 (12.72%)  _int_free
20,253,371 ( 7.88%)  malloc
13,945,104 ( 5.43%)  alloc::collections::btree::map::IntoIter<K,V,A>::dying_next
13,779,518 ( 5.36%)  malloc_consolidate
13,053,740 ( 5.08%)  free
 8,202,431 ( 3.19%)  alloc::collections::btree::map::BTreeMap<K,V,A>::insert
 7,846,146 ( 3.05%)  <alloc::string::String as core::clone::Clone>::clone
 7,301,637 ( 2.84%)  core::ptr::drop_in_place<serde_json::value::Value>
 6,696,423 ( 2.61%)  unlink_chunk
 5,940,591 ( 2.31%)  __memcpy_avx_unaligned_erms
 5,319,918 ( 2.07%)  __memcmp_avx2_movbe
 4,195,796 ( 1.63%)  __rustc::__rdl_alloc
 4,192,446 ( 1.63%)  btree::node::Handle<..>::insert_recursing
 4,092,000 ( 1.59%)  <BTreeMap<K,V,A> as Clone>::clone::clone_subtree'2
 3,603,600 ( 1.40%)  <BTreeMap<K,V,A> as Clone>::clone::clone_subtree
 3,602,880 ( 1.40%)  <BTreeMap<K,V,A> as PartialEq>::eq'2
 3,559,822 ( 1.39%)  drop_in_place<btree::map::IntoIter<String, Value>>
 3,524,808 ( 1.37%)  btree::map::entry::VacantEntry<K,V,A>::insert_entry
 3,402,000 ( 1.32%)  <BTreeMap<K,V,A> as PartialEq>::eq
 3,233,680 ( 1.26%)  debugger_trace_profile::support::activity_payload
 3,102,010 ( 1.21%)  drop_in_place<btree::map::IntoIter<String, Value>>
 2,686,436 ( 1.05%)  _int_free_merge_chunk
 2,621,280 ( 1.02%)  alloc::fmt::format::format_inner
 2,450,560 ( 0.95%)  ReplayDebugger::trace_snapshot::{{closure}}
 2,140,594 ( 0.83%)  core::fmt::write
```

The glibc allocator family alone (`_int_malloc` + `_int_free` + `malloc` +
`malloc_consolidate` + `free` + `unlink_chunk` + `_int_free_merge_chunk`)
accounts for **54.92%** of total instructions, and the `BTreeMap`/`Value`
clone-and-drop machinery (`dying_next`, `BTreeMap::insert`,
`String::clone`, `drop_in_place<Value>`, `clone_subtree`×2,
`BTreeMap::eq`×2, `VacantEntry::insert_entry`, the two `IntoIter` drop
paths) accounts for another **~22.4%**. Neither `trace_snapshot::{{closure}}`
(0.95%) nor `activity_payload` (1.26%) — the two functions a naive read might
expect to dominate — carry much *self* cost individually; almost the entire
budget is spent one level down, in allocating, cloning, comparing, and
freeing `serde_json::Value` trees on their behalf. This corroborates, via a
completely independent tool (callgrind vs. DHAT), the same conclusion the
allocation-site categorization below reaches directly: this is an
allocator-and-clone-bound workload, not a CPU-bound one.

### Allocation-count/byte scaling (`dhat`)

```
for n in 20 40 80 160; do
  DEBUGGER_TRACE_PROFILE_N=$n valgrind --tool=dhat --num-callers=30 \
    --dhat-out-file=dhat_n$n.json "$BIN"
done
```

`--num-callers=30` is required to reproduce the categorized breakdown below
from these captures: `serde_json::Value`'s recursive-descent clone/compare
machinery and `BTreeMap`'s recursive node-clone routines both nest well past
DHAT's default 12-frame capture depth for this workload's nested-object
payload shape (confirmed: the deepest resolved stack in the n=80 capture is
21 frames). The four total-byte/total-block figures below are unaffected by
capture depth — DHAT's grand totals are invariant to how allocations are
grouped by stack; only the *categorized* breakdown that follows depends on it.

| `n` | Total bytes | Ratio | Total blocks | Ratio |
|----:|-------------:|------:|--------------:|------:|
| 20  |     3,336,748 | — |     34,300 | — |
| 40  |    11,991,028 | 3.5936x |    124,540 | 3.6309x |
| 80  |    45,300,388 | 3.7779x |    473,020 | 3.7981x |
| 160 |   175,952,053 | 3.8841x |  1,841,980 | 3.8941x |

Both allocation-count measures independently climb toward the same ~4.0x
per-doubling signature the instruction-count scaling shows — three
independent measures (Ir, allocation bytes, allocation blocks) agreeing on
the same asymptotic trend.

### Allocation-site attribution at n=80 (mechanism breakdown)

`autumn-harvest/scripts/classify_dhat_allocations.py` resolves each of DHAT's
"program points" (one per unique call stack) to a fixed, precedence-ordered,
mutually-exclusive category and asserts the categories sum to the capture's
exact total bytes/blocks — see the script's own docstring for the full
precedence rationale (the same methodology
`autumn-harvest-sqlite/scripts/classify_dhat_allocations.py` established for
the equivalent SQLite-backend investigation).

```
$ python3 autumn-harvest/scripts/classify_dhat_allocations.py dhat_n80.json
Total: 45,300,388 bytes in 473,020 blocks

category                                  bytes       %       blocks       %
harness_fixture_setup                   366,142   0.81%        3,841   0.81%
trace_snapshot_prefix_clone          27,425,216  60.54%      288,962  61.09%
replay_prefix_or_context_build           12,880   0.03%          322   0.07%
history_matcher_internals                     0   0.00%            0   0.00%
workflow_reexecution                 14,437,593  31.87%      171,441  36.24%
fmt_format_machinery                     11,706   0.03%          413   0.09%
tokio_runtime                         3,042,973   6.72%        8,026   1.70%
other                                     3,878   0.01%           15   0.00%
```

`other` (0.01% of bytes) is entirely fixed, ~3.9 KB of process-startup /
`std::rt` / thread-attribute noise, present identically across every `n`
swept — confirmed nothing meaningful escaped categorization.

Re-running the same script at `n = 20, 40, 160` shows the categories moving
exactly as the O(N²) hypothesis predicts:

| category | n=20 | n=40 | n=80 | n=160 |
|---|---:|---:|---:|---:|
| `harness_fixture_setup` | 2.75% | 1.53% | 0.81% | 0.42% |
| `trace_snapshot_prefix_clone` | 55.16% | 58.58% | **60.54%** | 61.59% |
| `workflow_reexecution` | 30.83% | 31.50% | 31.87% | 32.06% |
| `tokio_runtime` | 10.95% | 8.25% | 6.72% | 5.90% |

`harness_fixture_setup` is a one-time `O(n)` cost (building the seed
history once, before `trace_snapshot` is even called), so it shrinks as a
share of the `O(n²)` total as `n` grows — exactly the behavior expected of
an additive lower-order term. `tokio_runtime` (async task/waker machinery
`drive_query_replay_async` runs on, once per step — `O(n)` total) shrinks
the same way, for the same reason. `trace_snapshot_prefix_clone` and
`workflow_reexecution` are both genuinely `O(n²)` (see "Hypothesis" below),
so their combined share (86.0% → 90.1% → 92.4% → 93.6% across the sweep)
converges toward the whole budget as `n → ∞`, and their *relative* split
between the two stays roughly stable (~2:1 in favor of the prefix clone)
because both scale at the same asymptotic order.

`history_matcher_internals` (`HistoryMatcher`/`match_history`/
`drive_query_replay`'s own bookkeeping — walking an already-owned prefix and
deciding what command comes next) is **exactly 0 bytes at every measured
size** — but this is a limit of what the categorizer can observe, not
evidence that the underlying work is cheap. Confirmed empirically, not
assumed: (1) grepping the raw DHAT frame table (`ftbl`) for
`HistoryMatcher`, `match_history`, `drive_query_replay`, `replay_prefix`,
`match_activity`, and `scan_activity_terminal` returns **zero matches** at
every swept size — none of these functions appears as its own frame in any
resolved call stack; (2) swapping this bucket's precedence to check *before*
`replay_prefix_or_context_build` (on the theory that a stack carrying both
markers was being misattributed to the wrong, less-specific bucket) changes
**nothing** — re-running the categorizer against the same captures with the
swapped order produces byte-for-byte identical output, because there is no
stack in the data carrying either marker for either ordering to disambiguate
between; (3) rebuilding the harness with full DWARF debug info
(`RUSTFLAGS="-C debuginfo=2"`) and re-capturing at `n=80` reproduces the
identical total (45,300,388 bytes) and the identical zero-match result for
the same six function names. Together these three independent checks
confirm the true cause: in this workspace's default `cargo bench` release
profile, every one of these functions is **fully inlined away** and its call
frame is physically absent from the runtime stack DHAT captures — no
debug-info flag can recover a frame that was never emitted, and only a
DWARF-inline-aware tool (e.g. `addr2line -i`) can synthesize the logical
inline chain after the fact, which this categorizer does not attempt. So a
`0` here means "unobservable by this methodology," not "free": any
allocation this code performs is silently folded into whichever caller
frame survived inlining — most plausibly `trace_snapshot::{{closure}}`,
landing in the `trace_snapshot_prefix_clone` bucket, or the
workflow-driving frames feeding `workflow_reexecution`. See
`autumn-harvest/scripts/classify_dhat_allocations.py`'s own docstring for
the same finding recorded alongside the categorizer's precedence rationale.

## Hypothesis (source-confirmed, not inferred from the number alone)

`ReplayDebugger::trace_snapshot` (`autumn-harvest/src/debugger.rs`):

```rust
let mut trace = ReplayTrace::from_history_capped(/* ... */); // O(N), one pass
let input = workflow_input(&snapshot.events);
for step in &mut trace.steps {
    let prefix = snapshot.events[..=step.index].to_vec();       // <-- O(k) clone, every step
    let replayed = self
        .replay_prefix(&snapshot, handler, input.clone(), prefix)  // fresh replay from the top
        .await;
    // ...
}
```

For an `n`-event history, `ReplayTrace::from_history_capped` builds `N = n`
steps (one per event index `k = 0..n`, "one step per consumed event"), and
for **every** step it clones the prefix `events[0..=k]` — `k + 1` events,
many carrying `serde_json::Value` payloads — into a fresh owned `Vec` before
handing it to `replay_prefix`, which builds a brand-new
`WorkflowContext::for_replay_canary_with_state(...)` and drives the
workflow handler through `drive_query_replay_async` against *just that
prefix*, from event `0`. So total work across the whole trace is
`Σ(k=0..n-1) O(k+1) = O(n²)` — matching the module doc comment's stated
class, and matching the empirical Ir/byte/block scaling above.

This is a **direct, deliberate consequence of the debugger's design**, not
an accident: the module doc comment states "stepping" *is* prefix replay
precisely because "replay is deterministic, so 'backward' is re-run-forward"
(issue #949's own specified mechanism) — every step's `.await` on
`replay_prefix` genuinely re-executes the workflow closure
(`support::sequential_workflow`, rebuilding every not-yet-replayed
activity's payload via `activity_payload`) from `WorkflowStarted` onward,
which is exactly what `workflow_reexecution`'s 31.87%-and-rising share
above measures.

## Why no local fix clears the floor

The allocation-site data makes an "obvious" local fix look tempting: since
`events[0..=k]` for step `k` is a strict superset of step `k-1`'s prefix,
one might try to avoid the per-step `O(k)` re-clone by **growing one
persistent `Vec<WorkflowEvent>` across steps** (appending only the single
newly-in-scope event at each step, instead of cloning the whole growing
range from scratch every time) — turning `Σ(k=0..n-1) O(k+1)` clone-copies
into a single `O(n)` sequence of one-element appends, addressing the
dominant 60.5%-and-rising `trace_snapshot_prefix_clone` bucket directly.

**This is unsafe**, confirmed by direct source inspection, not merely
inconvenient or unmeasured. `docs/performance-replay.md` documents a prior,
already-shipped optimization in `HistoryMatcher::scan_activity_terminal`
(`autumn-harvest/src/replay.rs:1130`):

```rust
let WorkflowEvent::ActivityCompleted { output, .. } = &mut self.events[scan_cursor] else {
    unreachable!("just matched ActivityCompleted above")
};
let result = HistoryMatch::Matched {
    output: std::mem::take(output),   // <-- destructively zeroes the matched event's output
};
```

That fix's own safety argument (quoted from `docs/performance-replay.md`) is:

> the taken event is never read again by any production code path, and
> `HistoryMatcher` owns its `events: Vec<WorkflowEvent>` exclusively (no
> shared/external aliasing), confirmed by auditing every construction site.

That invariant holds for `HistoryMatcher`'s **original** usage pattern:
one matcher, constructed fresh, driven through exactly one replay, then
discarded. It does **not** hold if the *same* underlying event buffer were
threaded across `trace_snapshot`'s per-step loop instead of being freshly
cloned each time: every step replays *from event 0*, so **every** step
`k ≥ i` re-matches activity `i`'s already-recorded `ActivityCompleted` — and
the first such step to do so would `mem::take` its `output` field, zeroing
it in the shared buffer. The very next step (`k+1`, still `≥ i`) would then
replay against a buffer where activity `i`'s recorded output is already
`Value::Null` instead of the real payload — not a performance regression,
silent data corruption in the debugger's own output (an incorrect
`resolved_payload`/`step.commands` for every later step, with no error
raised). Concretely: in this workload's shape (every activity's `Scheduled`
immediately followed by its `Completed`), a shared buffer would corrupt the
very first activity it re-touches — i.e. as early as step 2.

So a persistent, incrementally-grown prefix buffer is only safe to
introduce alongside a change to how `HistoryMatcher` treats an already-
matched event — either:

1. **Revert the `mem::take` optimization** back to `output.clone()` in
   `scan_activity_terminal`, restoring non-destructive reads. This would
   reintroduce, on the *production* replay hot path used by every worker
   task pickup and every deploy-time replay-canary sample, exactly the
   per-activity `Value` deep-clone `docs/performance-replay.md` measured and
   eliminated for issue #135's 10k-event budget — a clear regression to a
   different, already-optimized, higher-traffic path, in exchange for a
   speedup on an interactive debugging tool explicitly scoped to "tens to
   low hundreds of events." Net negative, and out of this agent's mandate
   regardless (this file is itself the artifact of a prior floor-clearing
   fix; reverting it here is not "a change," it's undoing one).
2. **Make the shared prefix cheap to reuse without destructive mutation** —
   e.g. wrapping event payloads (or whole `WorkflowEvent`s) in `Arc` so
   growing/cloning the prefix becomes `O(k)` reference-count bumps instead
   of `O(k)` deep JSON clones, while the underlying data stays immutably
   shared and `mem::take`-style destructive reads on it are no longer even
   expressible. This changes the ownership shape of `WorkflowEvent`/
   `HistoryMatcher`/`HistorySnapshot` — types shared by both the debugger
   and the production Postgres and SQLite replay engines.

Both are squarely "architectural changes: ... data structure ownership
model" — this agent's mandate is to ask before making that class of change,
not decide it unilaterally. No candidate that stays local to
`debugger.rs`/`trace_snapshot` exists: the fix has to live in
`replay.rs`/`event.rs`, shared production-critical code, or not at all.

## What isn't the finding

`workflow_reexecution` (31.87% of allocation bytes at n=80, and the second
half of the `O(n²)` total alongside the prefix clone) is **not** part of
this recommendation. It is the workflow author's own code —
`sequential_workflow`'s loop and its `activity_payload` helper — rebuilding
every not-yet-matched activity's payload from scratch on every step's
fresh-from-the-top replay. This is the deterministic-replay execution
model's documented, load-bearing property (the same one
`docs/performance-sqlite-runtime-drive.md` calls out for its own,
architecturally distinct, `SqliteRuntime` finding): a workflow author's
surrounding computation is expected to re-run on replay; only already-
recorded dispatch is skipped. Here it is doubly inherent — issue #949's
design is *specifically* "step k is a fresh replay of `events[0..=k]`" — so
addressing it would mean changing what the debugger's stepping model
guarantees (an honest "what does the code do at step k" answer), not a
`trace_snapshot` implementation detail. It is called out here only so the
categorized breakdown adds up to a legible whole, not as something this
finding proposes changing.

## Recommendation (requires a human decision — not implemented here)

Two candidates were identified above; neither is implemented in this pass
because both are architectural (data-structure ownership / public API
shape), and — per this agent's mandate — that class of change needs a
maintainer decision, not a unilateral commit:

1. **Leave it as-is.** `DEFAULT_MAX_STEPS = 500` is already the sanctioned
   mitigation, and both existing docs already scope this tool to
   "interactive histories (tens to low hundreds of events)," explicitly
   distinct from the CI-gate replay paths (`WorkflowReplayer`, single-pass
   `O(N)`) this tool complements rather than replaces. At the documented
   scope, the absolute cost is small (998M Ir / 176MB allocated at the
   largest swept size, `n=160` → 321 events, still well under the 500-step
   cap) and a fix's value would be speeding up an already-usable
   interactive tool, not unblocking anything currently broken.
2. **`Arc`-wrap event payloads** (or whole `WorkflowEvent`s) so a prefix
   "clone" becomes cheap reference-count bumps rather than deep `Value`
   clones — directly addressing the dominant 60.5%-and-rising
   `trace_snapshot_prefix_clone` share. **This shrinks the constant factor,
   not the complexity class**: there are still `n` steps each doing `O(k)`
   work (now cheap refcount bumps instead of deep clones, but still linear
   in `k` per step), so the total remains `Σ(k=0..n-1) O(k+1) = O(n²)` —
   raising the *practical* `n` this tool stays comfortable at, not removing
   the need for `--max-steps` on a pathologically long history. It also
   touches `WorkflowEvent`/`HistoryMatcher`, shared by both production
   replay engines — a change with blast radius well beyond this one
   debugging tool, needing its own correctness review (in particular:
   whether `Arc`'s reference-counting itself introduces new allocator
   traffic proportional to how many `Arc<WorkflowEvent>` clones exist
   simultaneously across in-flight steps, which would need measuring before
   claiming a net win rather than assumed).

A maintainer should weigh option 2 against the tool's stated interactive
scope (option 1) before deciding whether the added ownership-model
complexity is warranted, and if so, whether it should extend to the
production replay engines too or stay debugger-local via a parallel,
cheaper-to-clone representation used only here.

## What changed in this PR

Nothing in production code. Only:

- `autumn-harvest/benches/debugger_trace_profile.rs` (new) — the profiling
  harness described above. Its own module doc comment was tightened in
  response to review — it originally described the workload's
  `DEFAULT_MAX_STEPS` default as "what a real `harvest debug` invocation
  gets," the same inaccuracy corrected in this document's "Scope" section;
  it now states explicitly, with a pointer to the CLI's actual entry point,
  that the packaged CLI never calls `trace_snapshot` at all.
- `autumn-harvest/Cargo.toml` — registers the new `[[bench]]` target
  (`harness = false`, `required-features = ["debugger"]`, the same
  convention as every other deterministic profiling harness in this repo).
- `autumn-harvest/scripts/classify_dhat_allocations.py` (new) — the
  allocation-site categorizer used above, mirroring
  `autumn-harvest-sqlite/scripts/classify_dhat_allocations.py`'s
  methodology (precedence-ordered, mutually-exclusive, exhaustive-partition-
  asserted). Its `history_matcher_internals`/`replay_prefix_or_context_build`
  precedence check was checked ahead of the latter (matcher frames are
  always nested inside `replay_prefix`'s own drive call), and its docstring
  now records — with the empirical evidence above — that this precedence
  ordering makes no observable difference in practice, because every
  function either bucket names is inlined away in this build profile.
- `docs/replay-debugger.md` — cross-linked to this note from its existing
  "Cost" section, and that section's own wording was tightened to state
  explicitly that it describes the library `trace_snapshot` arm, not the
  packaged CLI's default (handler-free, O(N)) behavior.
- This document.

`cargo fmt --all -- --check`, `cargo check -p autumn-harvest
--no-default-features --features debugger --bench debugger_trace_profile`,
and the full existing `autumn-harvest --no-default-features --features
testing --lib` suite (2,148 tests) all pass unchanged. `cargo clippy -p
autumn-harvest --no-default-features --features debugger -- -D warnings`
could not be run to completion in this sandbox: it fails while compiling
the unmodified `autumn-harvest` library on an unrelated, pre-existing
`#[cfg_attr(.., allow(clippy::unused_async_trait_impl))]` in
`autumn-harvest/src/context.rs:13950` that names a lint clippy only
recognizes from a newer version than this sandbox's installed toolchain —
confirmed identical and reproducible with this PR's changes fully reverted
(`git stash`), i.e. the exact same pre-existing, unrelated sandbox-toolchain
mismatch already documented in `docs/performance-sqlite-runtime-drive.md`.

## Reproduce

```bash
# Locate the compiled binary (cargo bench builds in release, doesn't run it):
BIN=$(cargo bench -p autumn-harvest --no-default-features --features debugger \
  --bench debugger_trace_profile --no-run --message-format=json \
  | jq -r 'select(.reason == "compiler-artifact" and .target.name == "debugger_trace_profile") | .executable')

export DEBUGGER_TRACE_PROFILE_N=80

# Sanity: prints a summary line, runs natively (no valgrind overhead):
"$BIN"

# Instruction count -- valgrind inherits the exported env var, so it must be
# set (exported) in the shell BEFORE the valgrind invocation, not passed as
# an argument to it:
valgrind --tool=callgrind --branch-sim=no --cache-sim=no \
  --callgrind-out-file=callgrind.out "$BIN"
callgrind_annotate --threshold=90 callgrind.out | head -40

# Allocation counts/bytes -- `--num-callers=30` is REQUIRED to reproduce the
# allocation-site attribution table above (see "Methodology note" under
# "Allocation-count/byte scaling" above):
valgrind --tool=dhat --dhat-out-file=dhat.json --num-callers=30 "$BIN"

# Reproduce the "Allocation-site attribution" table above from that capture:
python3 autumn-harvest/scripts/classify_dhat_allocations.py dhat.json
```

`DEBUGGER_TRACE_PROFILE_N` (default `80`) and `DEBUGGER_TRACE_PROFILE_REPS`
(default `1`) are read from the environment; the four-point scaling tables
above were produced by re-running the same binary at `N=20,40,80,160` with
`REPS=1`.

## See also

- `docs/performance-replay.md` — the prior, already-shipped
  `HistoryMatcher::scan_activity_terminal` optimization whose safety
  invariant this note's "Why no local fix clears the floor" section relies
  on, and the workload (`replay_profile_support.rs`) this harness reuses.
- `docs/performance-sqlite-runtime-drive.md` — the equivalent
  "confirm-the-O(n²)-then-rule-out-local-fixes" investigation on the
  SQLite backend's decision-cycle drive, whose allocation-site-categorizer
  methodology and document structure this note mirrors.
- `docs/replay-debugger.md` — the user-facing tool documentation, including
  the pre-existing qualitative "Cost" section this note quantifies.
- Issue #949 — the time-travel replay debugger this profiles.
- Issue #135 — the replay-throughput budget the reused workload originates
  from.
