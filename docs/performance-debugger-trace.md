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
across repeated runs of the same compiled binary. That determinism is scoped
to **a fixed profiling environment, not "any machine"**: Callgrind counts the
instructions the executable actually executes, and a different rustc/cargo
version, valgrind version, or libc build can change codegen or which
CPU-dispatched libc routine gets selected — changing the exact counts without
changing the O(N²) conclusion they support. Every number in this document was
captured with `rustc 1.94.1 (e408947bf 2026-03-25)` / `cargo 1.94.1
(29ea6fb6a 2026-03-24)`, `valgrind-3.22.0`, on `x86_64-unknown-linux-gnu` with
Ubuntu `glibc 2.39`; reproducing the absolute figures exactly requires
matching that environment. Reproducing the *scaling conclusion* — the
monotonic climb toward a 4.0x ratio per doubling of `n` (O(N²), not the exact
Ir count at each point) — does not; see "Instruction-count scaling (the
asymptotic argument)" below, which is the load-bearing evidence, not the
absolute counts in isolation.

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
uncapped cost, not an artifact of hitting the ceiling.

The one deliberate deviation from library defaults is the **per-step wall-clock
budget**: the harness calls `.step_timeout(Duration::from_secs(60))` rather
than leaving `DEFAULT_STEP_TIMEOUT = 5s` in effect. That budget is checked
against `std::time::Instant` — a real OS clock, unaffected by CPU-bound
instrumentation slowdown — so under `valgrind --tool=callgrind`/`--tool=dhat`
(which can run a single step's `O(k)` prefix replay tens to hundreds of times
slower in wall-clock terms than an uninstrumented build) the production-sized
5s budget risked tripping on a step that was making real progress, not
spinning. Raising it is an instrumentation-headroom knob orthogonal to the
`max_steps` framing above — it changes how long the harness is willing to
*wait* for a step, never what work that step does, and a step that still
exceeds even the raised budget is still a genuine, harness-failing signal
(see below). Every capture cited in this document was re-verified against
this fixed harness: total allocation bytes/blocks are byte-for-byte identical
to captures taken before the budget was raised (see "What changed in this
PR"), confirming the 5s default was never actually exceeded in the
originally-published data — but the risk was real and the harness now closes
it rather than relying on that having been true by luck.

The harness asserts, for every step, both `step.outcome.replay_succeeded()`
**and** `step.divergence.is_none()`. Checking `divergence.is_none()` alone is
not sufficient: `StepOutcome::TimedOut` and `StepOutcome::Panicked` both leave
`divergence: None` (per `StepOutcome::replay_succeeded`'s own doc comment,
the drive never reached a conclusion to compare against the recording), so a
run that silently timed out or panicked mid-step would report *no*
divergence and pass a divergence-only check while actually profiling an
incomplete, truncated replay rather than the intended clean-replay path.
`replay_succeeded()` (`matches!(self, Suspended | ReachedTerminal)`) is the
load-bearing check that rejects that failure mode; `divergence.is_none()` is
kept alongside it to additionally reject a step that reached a conclusion but
disagreed with the recorded history.

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
| 20  | 41  |    18,843,848 | — |
| 40  | 81  |    67,429,086 | 3.5783x |
| 80  | 161 |   257,019,327 | 3.8117x |
| 160 | 321 |   998,529,380 | 3.8850x |

Each doubling of `n` should scale total work by a factor approaching **4.0x**
for a genuine `O(n²)` process (vs. `2.0x` for `O(n)`). The observed ratio
climbs monotonically toward 4.0x as `n` grows — 3.578x → 3.812x → 3.885x —
which is exactly the signature of a quadratic dominant term plus a shrinking
lower-order additive term (one-time harness/process setup, and the `O(n)`
`workflow_input(&snapshot.events).clone()` per step), confirming the
documented `O(N²)` class empirically rather than by reading the doc comment
alone.

These figures were re-captured against the harness described above (with the
raised `step_timeout` and the strengthened `replay_succeeded()` assertion in
place) and differ from an earlier capture by 937–1,385 instructions out of
18.8M–998.5M total (0.0001%–0.005%) — fully attributable to the harness's own
added code (one extra per-step `matches!` check across up to 321 steps, plus
a one-time env-var read and `Duration` construction), not to any change in
the profiled `trace_snapshot` workload itself. The ratios are unaffected to
four significant figures (only the `n=40` ratio's fourth decimal digit moves,
3.5784x → 3.5783x); the O(N²) scaling conclusion is identical either way.

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

Re-verified byte-for-byte against fresh DHAT captures taken with the fixed
harness (raised `step_timeout`, strengthened `replay_succeeded()` assertion,
see "Workload" above): total bytes and total blocks at every `n` are
identical to the figures published here — no measurable heap-allocation
effect from the harness fix, unlike the (tiny, fully-explained) Ir delta
above. This makes sense: neither reading an unset environment variable
(`std::env::var` returns without allocating when the key is absent) nor
constructing a `Duration`/calling the `.step_timeout(...)` builder allocates
on the heap, so the fix changes instruction count slightly (extra
comparisons/branches) but not the allocation-count evidence at all.

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
history_projection_one_time_build        952,740   2.10%        8,973   1.90%
trace_snapshot_prefix_clone          29,110,827  64.26%      283,924  60.02%
replay_prefix_or_context_build          409,224   0.90%        4,652   0.98%
history_matcher_internals                     0   0.00%            0   0.00%
workflow_reexecution                 14,437,593  31.87%      171,441  36.24%
fmt_format_machinery                          0   0.00%            0   0.00%
tokio_runtime                             8,384   0.02%           12   0.00%
other                                    15,478   0.03%          177   0.04%
```

This table supersedes an earlier version of this note (see "What changed in
this PR" for the review that prompted the correction): the categorizer's
`tokio_runtime` and `trace_snapshot_prefix_clone` checks were both refined
for precision, and a new `history_projection_one_time_build` bucket was
split out. The grand totals (45,300,388 bytes / 473,020 blocks) are
unchanged — only the internal categorization moved, exactly as expected
from DHAT's own invariant that its totals don't depend on how allocations
are grouped by stack. **Updated a further time** in response to a later
review comment: `trace_snapshot_prefix_clone`'s whole-stack fallback
condition was being checked too early — ahead of buckets 4 and 5 instead
of after them — so it was absorbing a slice of their allocations; the
table above already reflects that fix. See the note following the
`tokio_runtime` deep-dive below for the exact evidence.

`other` (0.03% of bytes) is no longer a single fixed figure at every `n` —
confirmed by directly decomposing its top stacks, not assumed. Two
components: a genuinely fixed **~3.96 KB / 17 blocks**, present
byte-for-byte identically at every swept `n` (process-startup / `std::rt` /
`pthread_getattr_np` / thread-attribute noise — the same noise this section
previously reported as the whole of `other`), plus a small, genuinely `O(n)`
residual that **used to be hidden inside the old `tokio_runtime` bucket**
and only became visible once that bucket's check was depth-limited (see
below): a `144-bytes-per-step`, exactly-linear-in-`n` allocation whose
innermost surviving frame is `<Vec<T> as
alloc::vec::spec_from_iter::SpecFromIter<T,I>>::from_iter`, one frame above
`trace_snapshot::{{closure}}` rather than at it — a further LLVM/Rust
release-profile inlining variant of the same per-step
`snapshot.events[..=step.index].to_vec()` call that neither of
`trace_snapshot_prefix_clone`'s two conditions (a proximate-frame match, or
a whole-stack `to_vec`/`Clone`/`clone` substring) catches, since this stack
contains none of those substrings anywhere in its 9 frames. This is
genuinely immaterial to every conclusion in this document (0.03% of total
bytes at n=80, 0.013% at the largest swept n=160) and this categorizer is
not chasing it further — recorded here in the same spirit that motivated
this precision pass in the first place: confirming nothing meaningful is
silently swept under an unaudited catch-all, not claiming the categorizer
is now perfectly exhaustive.

Re-running the same script at `n = 20, 40, 160` shows the categories moving
exactly as the O(N²) hypothesis predicts:

| category | n=20 | n=40 | n=80 | n=160 |
|---|---:|---:|---:|---:|
| `harness_fixture_setup` | 2.75% | 1.53% | 0.81% | 0.42% |
| `history_projection_one_time_build` | 7.18% | 3.98% | 2.10% | 1.08% |
| `trace_snapshot_prefix_clone` | 55.71% | 61.13% | **64.26%** | 65.95% |
| `workflow_reexecution` | 30.83% | 31.50% | 31.87% | 32.06% |
| `tokio_runtime` | 0.25% | 0.07% | 0.02% | 0.00% |

`harness_fixture_setup` is a one-time `O(n)` cost (building the seed
history once, before `trace_snapshot` is even called), so it shrinks as a
share of the `O(n²)` total as `n` grows — exactly the behavior expected of
an additive lower-order term. `history_projection_one_time_build` (the
single, up-front `ReplayTrace::from_history_capped` pass over the whole
history, before the per-step loop begins — see the bucket 2 rationale in
`classify_dhat_allocations.py`'s docstring) shrinks the same way, for the
same reason, and its own byte counts confirm the claim directly: they
double almost exactly (239,580 → 477,300 → 952,740 → 1,904,308, each ratio
1.99x–2.00x) as `n` doubles, the clean signature of a genuine `O(n)` pass
rather than an approximation.

`tokio_runtime` is now **essentially flat** across the whole sweep —
8,384 bytes / 12 blocks at every single measured `n`, byte-for-byte and
block-for-block identical — confirming it really is a small, fixed,
one-time process-startup cost (async runtime `Builder::build`,
`BlockingPool::new`, `Wheel::new`, and similar) entirely unrelated to
workload size. **This corrects a materially wrong prior claim in this same
document.** An earlier version of this table reported `tokio_runtime` at
10.95% → 8.25% → 6.72% → 5.90% and described it as "async task/waker
machinery `drive_query_replay_async` runs on, once per step". Both the
numbers and the causal claim were wrong: a Codex review comment on this
PR flagged that the bucket's check (`"tokio::" in joined`, a whole-stack
substring search) was matching a distant *ancestor* frame —
`tokio::runtime::scheduler::current_thread::CurrentThread::block_on`,
which legitimately sits somewhere above literally every allocation in this
`rt.block_on(debugger.trace_snapshot(snapshot))`-wrapped program — rather
than the allocation's true, proximate cause.

Investigating turned up a larger problem than the review comment itself
implied. Measured directly against the n=80 capture, **99.72% of the old
bucket's 3,042,973 bytes (3,034,589 bytes) was misattributed**, resolving
to three genuine causes once the check is corrected: 72.0%
(2,190,888 bytes, one single program point) was `trace_snapshot::{{closure}}`
itself, its allocation's immediate caller; 27.4% (832,181 bytes) was
`BTreeMap`/`serde_json::Value` serialization frames reached through
`normalized_event_facts`/`command_payload`/`WorkflowEvent::serialize`; and
0.4% (11,520 bytes) was the small `from_iter`-mediated per-step residual
described in the `other` explanation above (its stack also happens to pass
through `block_on`, so the old whole-stack check caught it too). In every
one of these cases the `tokio::` marker appeared only 5–15 frames up the
ancestor chain, never as the true cause. Every *genuinely* tokio-caused
allocation in the same capture, by contrast, has its `tokio::` frame
immediately above the allocator entry point, with zero exceptions — a
clean, decisive, bimodal split that is itself the evidence the corrected
check relies on. The categorizer's `tokio_runtime` check is now
depth-limited to the allocation's own near-immediate caller (`fs[1]`/`fs[2]`,
never a distant ancestor reached only through a 5-to-15-frame `block_on`
chain); the misattributed 72.0% now lands in `trace_snapshot_prefix_clone`,
the 27.4% splits across `history_projection_one_time_build` and
`trace_snapshot_prefix_clone` depending on which pass it belongs to, and
the 0.4% now lands, correctly, in `other`.

`trace_snapshot_prefix_clone`'s own check was **further refined** after
this same round of review, for the identical class of bug bucket 8's fix
above addresses: the whole-stack `"trace_snapshot" in joined` fallback
(condition (b)) cannot distinguish "`trace_snapshot` is a distant
ancestor" from "`trace_snapshot::{{closure}}` is the true proximate
cause" — a further Codex review comment on this PR flagged that this lets
`render_command`/`command_payload` allocations (bucket 5's own named
descendants, which run nested inside `replay_prefix`, itself called from
`trace_snapshot`'s per-step loop, so `trace_snapshot` genuinely IS an
ancestor of their allocations too, just not the most specific one) get
swept into this bucket instead of `replay_prefix_or_context_build`.
Measured directly at n=80 against the categorizer's precedence ordering
before this fix: **166,467 bytes (0.37% of the 45,300,388-byte capture, 7
program points, every one resolving to `render_command`/`command_payload`
further up the stack) were misattributed this way** — small relative to
the bucket's ~64% share, but real and previously undetected. The
reviewer's broader question — whether this bucket's ~64% share as a whole
could be trusted at all without excluding named descendants — does not
hold up beyond that narrow slice: the remaining 29,110,827 bytes (99.63%
of this bucket's total after the fix) are dominated (90.73% of the
fallback-matched subset) by `BTreeMap::clone::clone_subtree` sitting
directly at the allocation's proximate frame — a deeper LLVM inlining
variant of the same per-step `to_vec()`/`clone()` mechanism condition (a)
already catches at a shallower inlining depth, not a different mechanism.
Fixed by moving buckets 4 and 5's checks ahead of condition (b) in the
code (condition (a) — the direct proximate-frame match — stays checked
first and unconditionally, since a stack's own immediate caller can only
ever be one function and therefore cannot collide with a descendant
marker). The misattributed volume now lands, correctly, in
`replay_prefix_or_context_build`; measured across the full sweep it grows
with `n` at roughly the same rate as the rest of the bucket's `O(n²)`
mechanism (41,607 → 83,227 → 166,467 → 333,074 bytes at n=20/40/80/160,
each ratio ≈2.00x per doubling — consistent with
`render_command`/`command_payload` running once per drained command per
step, the same fresh-replay-per-step design, just attributed to a more
specific function). See `classify_dhat_allocations.py`'s own docstring
(the "KNOWN FIX" note under bucket 3) for the full precedence rationale.

`trace_snapshot_prefix_clone` and `workflow_reexecution` are both genuinely
`O(n²)` (see "Hypothesis" below), so their combined share (86.5% → 92.6% →
96.1% → 98.0% across the sweep) converges toward the whole budget as
`n → ∞` — a tighter, cleaner convergence than the pre-correction figures
(86.0% → 90.1% → 92.4% → 93.6%) showed, since the fix stops diluting both
numerators with allocations that were never theirs. (This combined-share
figure moved slightly again, from an intermediate 87.8% → 93.3% → 96.5% →
98.2%, after the `render_command`/`command_payload` fix immediately
above: that fix moves bytes out of `trace_snapshot_prefix_clone` into a
*third* bucket, `replay_prefix_or_context_build`, which this combined
figure never counted, so removing them from the numerator here — without
adding them anywhere this figure sums — pulls the total down slightly.)
Their *relative* split between the two grows modestly across the sweep
(1.81:1 → 1.94:1 → 2.02:1 → 2.06:1 in favor of the prefix clone) — both
scale at the same asymptotic order, so the ratio was already
approximately stable before this correction and remains so after it;
this document does not claim it is perfectly constant.

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
dominant 64.3%-and-rising `trace_snapshot_prefix_clone` bucket directly.

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
   clones — directly addressing the dominant 64.3%-and-rising
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
  asserted). Its `history_matcher_internals` bucket is checked ahead of
  `replay_prefix_or_context_build` (matcher frames are always nested inside
  `replay_prefix`'s own drive call), and its docstring records — with the
  empirical evidence above — that this precedence ordering makes no
  observable difference in practice, because every function either bucket
  names is inlined away in this build profile.

  **Revised twice in response to Codex review comments on this PR**, both
  addressed together since the investigation showed them to be causally
  linked (fixing one required correctly separating what was flowing into
  the other): (1) the `tokio_runtime` check was a whole-stack `"tokio::" in
  joined` substring search, which matched a distant `CurrentThread::block_on`
  *ancestor* frame present above every allocation in this
  `rt.block_on(...)`-wrapped program — measured to be misattributing 99.7%
  of that bucket's bytes at n=80 (see the corrected per-n table above for
  the full before/after). It is now depth-limited to the allocation's own
  near-immediate caller. (2) The `trace_snapshot_prefix_clone` bucket's
  whole-stack `"trace_snapshot" in joined` + `to_vec`/`Clone`/`clone`
  substring check was conflating the genuine per-step `O(k)` prefix
  `to_vec()` clone with the one-time, `O(n)` `ReplayTrace::from_history_capped`
  decode pass that runs once, before the per-step loop even begins (both
  share `trace_snapshot::{{closure}}` as a common ancestor frame, so a
  whole-stack search cannot tell them apart) — confirmed by directly reading
  `trace_snapshot`'s body, not inferred from the allocation data alone. A
  new `history_projection_one_time_build` bucket, checked ahead of
  `trace_snapshot_prefix_clone`, now separates that one-time decode cost;
  `trace_snapshot_prefix_clone` itself gained a stronger, first-hand
  proximate-frame check (`fs[1]` resolving directly to
  `trace_snapshot::{{closure}}`) alongside the retained whole-stack fallback,
  since LLVM's release-profile inlining is not uniform across call sites of
  the same generic function and some of this loop's `to_vec()` calls leave
  no `to_vec`-named frame anywhere on the stack at all. See the script's own
  docstring for the full precedence rationale and the exact byte-level
  evidence for both corrections.
- `docs/replay-debugger.md` — cross-linked to this note from its existing
  "Cost" section, and that section's own wording was tightened to state
  explicitly that it describes the library `trace_snapshot` arm, not the
  packaged CLI's default (handler-free, O(N)) behavior.

  **Revised a third time in response to a further Codex review comment**:
  the harness's own assertion loop checked only `step.divergence.is_none()`,
  which cannot detect a step that timed out or panicked mid-replay — both
  leave `divergence: None` (nothing was ever compared against the
  recording), so a run silently corrupted by, e.g., valgrind's real
  wall-clock slowdown tripping the library's 5s `DEFAULT_STEP_TIMEOUT`
  would have passed the old check while actually profiling an incomplete
  trace. Fixed by also asserting `step.outcome.replay_succeeded()` (which
  rejects `TimedOut`/`Panicked`) and by raising the harness's own
  `step_timeout` to 60s — a real risk under callgrind/DHAT instrumentation,
  investigated and closed rather than assumed away. Re-ran all four capture
  sizes under both `--tool=dhat` and `--tool=callgrind` with the
  strengthened assertion in place: every run passed (exit 0), directly
  confirming that no step in the previously-published captures had actually
  timed out or panicked. The re-captured DHAT totals are byte-for-byte
  identical to what was already published; the re-captured Ir totals differ
  by 0.0001%–0.005% (fully attributable to the harness's own added
  instructions, not the profiled workload) and are updated above with an
  explanation of the delta rather than silently left stale.

  **Revised a fourth time in response to a further Codex review comment**:
  the opening paragraph claimed every number below is "reproducible
  bit-for-bit on any machine" — overreaching, since Callgrind counts the
  instructions the compiled executable actually executes, and a different
  rustc/cargo version, valgrind version, or libc build can change codegen
  or which CPU-dispatched libc routine gets selected, changing the exact
  counts on a different machine without changing the O(N²) conclusion they
  support. Fixed by scoping the determinism claim to a fixed profiling
  environment, recording the exact versions used to capture every number in
  this document (`rustc`/`cargo` 1.94.1, `valgrind-3.22.0`,
  `x86_64-unknown-linux-gnu`, Ubuntu `glibc 2.39`), and pointing at the
  "Instruction-count scaling" section's ratio-based argument as the
  load-bearing, environment-independent evidence for the scaling
  conclusion — distinct from the absolute Ir counts, which do require a
  matching environment to reproduce exactly.

  **Revised a fifth time in response to a further Codex review comment**:
  `trace_snapshot_prefix_clone`'s whole-stack fallback condition
  (condition (b)) was checked ahead of buckets 4
  (`history_matcher_internals`) and 5 (`replay_prefix_or_context_build`)
  in the categorizer, so it could not distinguish "`trace_snapshot` is a
  distant ancestor of this allocation" from "`trace_snapshot::{{closure}}`
  is the true proximate cause" — the identical bug class already fixed for
  `tokio_runtime` earlier in this same document, just narrower. Measured
  directly at n=80: 166,467 bytes (0.37% of the 45,300,388-byte capture, 7
  program points, all `render_command`/`command_payload`) were
  misattributed away from bucket 5 into this bucket. The reviewer's
  broader implication — that this bucket's ~64% share as a whole could not
  be trusted without excluding named descendants — does not hold beyond
  that narrow slice: 99.63% of the bucket's total is confirmed, by
  proximate-frame grouping, to genuinely be the prefix-clone mechanism
  (dominated by `BTreeMap::clone::clone_subtree` at the proximate frame, a
  deeper LLVM inlining variant of the same mechanism condition (a) already
  catches shallower). Fixed by moving buckets 4 and 5's checks ahead of
  condition (b) in the code, while condition (a) (the direct
  proximate-frame match) stays checked first and unconditionally, since it
  structurally cannot collide with either bucket's descendant markers. All
  four capture sizes were re-classified with the fixed script; the
  "Allocation-site attribution" section above reflects the corrected
  `trace_snapshot_prefix_clone` / `replay_prefix_or_context_build` figures
  and the recomputed combined-share/ratio figures that depend on them. The
  underlying DHAT captures are unchanged — this is a categorization-only
  fix, verified by re-running `classify_dhat_allocations.py`'s own
  exhaustive-partition assertion against all four captures (it still
  passes: category totals still sum to each capture's exact grand total).
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
