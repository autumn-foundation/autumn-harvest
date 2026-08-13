# Workflow replay: instruction-count profiling and the `mem::take` fix

This note documents a profiling pass over `WorkflowReplayer`'s in-memory
replay path against issue #135's published CPU-path budget ("a 10,000-event
history replays in under 200ms") and the resulting fix in
`HistoryMatcher::scan_activity_terminal`. Wall-clock timing is not admissible
evidence on this (shared-vCPU) machine — every number below is a deterministic
instruction count from `valgrind --tool=callgrind`, reproducible bit-for-bit
on any machine.

## Workload

The harness is `benches/replay_profile.rs` (+ `benches/replay_profile_support.rs`),
a `harness = false` binary with its own `main()` — no criterion wall-clock
loop, so a profiler can be pointed at it directly with nothing but the target
workload running.

It replays the exact workload issue #135 budgets: a `sequential_workflow`
that calls `ctx.execute_activity_raw` `n = 5_000` times (the same `n`
`replay_bench.rs`'s `bench_replay_10k` uses), producing a 10,001-event
history (`WorkflowStarted` + 5,000 × `ActivityScheduled`/`ActivityCompleted`
pairs), driven through `WorkflowReplayer::replay_from_events` in strict
replay mode — the code path that runs on every worker task pickup and on
every deploy-time replay-canary sample.

Unlike `replay_bench.rs`'s `Value::Null` payloads, each activity here carries
a realistic ~230-byte JSON record (`activity_payload`, an order line item
with a nested `items` array) as both its scheduled input and its completed
output, so the measured cost includes real per-activity `serde_json::Value`
clone/compare/drop traffic instead of only the `Value::Null` fast path.

## Profile

```
valgrind --tool=callgrind --branch-sim=no --cache-sim=no \
  --callgrind-out-file=callgrind.out <replay_profile binary>
callgrind_annotate --threshold=100 callgrind.out
```

The flat (self-cost) profile of the *entire process* — history construction
plus the actual `replay_from_events` call — is dominated by allocator and
`BTreeMap` traffic (`serde_json::Value::Object` is `BTreeMap`-backed, since
this crate does not enable serde_json's `preserve_order` feature):

```
34,844,717 (15.70%)  _int_malloc
31,972,248 (14.41%)  _int_free
20,191,304 ( 9.10%)  malloc
13,800,026 ( 6.22%)  BTreeMap::IntoIter::dying_next
13,020,876 ( 5.87%)  free
11,500,000 ( 5.18%)  BTreeMap::insert
 7,200,000 ( 3.24%)  drop_in_place<serde_json::value::Value>
 6,060,000 ( 2.73%)  BTreeMap Handle::insert_recursing
 ...
 3,080,000 ( 1.39%)  BTreeMap Clone::clone::clone_subtree'2
 2,715,000 ( 1.22%)  BTreeMap Clone::clone::clone_subtree
```

Tracing the call tree, exactly one `Value` deep-clone happens per activity in
`build_history` (setup, `payload.clone()` for `ActivityScheduled.input` vs.
`ActivityCompleted.output` — legitimate, not part of the code under test) and
a *second* deep-clone happens per activity inside the replayer itself, in
`HistoryMatcher::scan_activity_terminal`'s `ActivityCompleted` match arm,
which returned `HistoryMatch::Matched { output: output.clone() }` for every
one of the 5,000 already-recorded, already-matched activity completions.

That second clone is pure waste: `settle_terminal` (called immediately after,
with the same `scan_cursor`) only ever re-reads `activity_id` from this same
event, never `output`. Once an `ActivityCompleted` event is matched, its
recorded payload is never observed again anywhere in `HistoryMatcher` — this
was confirmed by grepping every `ActivityCompleted` pattern match in
`replay.rs` (a 12k-line file) outside `#[cfg(test)]`.

## Hypothesis

Replacing the clone with `std::mem::take(output)` — extracting the value in
place and leaving `Value::Null` behind in the (already fully-consumed) event
— removes exactly one `Value::clone()` per matched activity from the replay
path, at zero cost to correctness: the taken event is never read again by any
production code path, and `HistoryMatcher` owns its `events: Vec<WorkflowEvent>`
exclusively (no shared/external aliasing), confirmed by auditing every
construction site.

Given the flat profile already attributes the `clone_subtree`/malloc/free
family to a mix of setup-side and replay-side cloning, removing one of the
two clones per activity should roughly halve the `clone_subtree` cost and
meaningfully reduce total allocator traffic — the primary drag on this
workload.

## Change

`autumn-harvest/src/replay.rs`, `HistoryMatcher::scan_activity_terminal`'s
`ActivityCompleted` match arm: replaced `output.clone()` with a narrow
mutable re-borrow of the same slot (`&mut self.events[scan_cursor]`) and
`std::mem::take(output)`. The match's scrutinee stays a shared reference
(`&self.events[scan_cursor]`) for every other arm — only the target arm's
pattern was narrowed to bind `activity_id: id` and the outer shared borrow is
allowed to end at the guard check before the arm body re-borrows mutably.
This was chosen over converting the whole match to `&mut` because that forces
every other arm (several of which call `&mut self` methods with arguments
derived from the scrutinee, e.g. `stash_external_signal_request`) to satisfy
the borrow checker for no benefit — a strictly larger, riskier diff for the
same win.

No behavior change: the recorded event's `activity_id` (the only field any
other code path reads from a matched `ActivityCompleted`) is untouched;
`output` is left as `Value::Null`, but nothing reads it again.

## Measurement

Both runs: identical release-profile binary (`cargo bench --no-run`), same
`valgrind --tool=callgrind --branch-sim=no --cache-sim=no` invocation, same
defaults (`REPLAY_PROFILE_N=5000`, `REPLAY_PROFILE_REPS=1`).

| | Instructions (Ir) |
|---|---|
| Before | 221,886,322 |
| After  | 189,162,617 |
| **Reduction** | **32,723,705 (14.75%)** |

This benchmark *is* the target workload (issue #135's own documented replay
budget), not a synthetic microbenchmark of an isolated function, so the
"represents ≥5% of workload cost" qualifier is satisfied trivially; the
14.75% total-process instruction reduction clears the ≥5% floor by close to
3×.

The mechanism is directly traceable in the flat profile, not just inferred
from the total: `clone_subtree`/`clone_subtree'2` (the `BTreeMap` clone the
target `.clone()` call bottoms out into) dropped from
`3,080,000 + 2,715,000 = 5,795,000` to `1,540,000 + 1,355,000 = 2,895,000`
instructions — a reduction to essentially half, matching the expected "one of
two per-activity clones removed" mechanism exactly. `_int_malloc`/`_int_free`/
`malloc`/`free` (the allocator traffic those `BTreeMap` clones drive, for both
the flat map itself and its nested `items` array) each dropped by
1.9M–6.9M instructions in absolute terms.

Correctness: `cargo test -p autumn-harvest --no-default-features --features
testing --lib` (1,850 tests, includes `replay.rs`'s own `#[cfg(test)] mod
tests`) and `cargo test -p autumn-harvest --no-default-features --features
testing --test integration` (1,176 tests, including the
`macros_compile_fail` trybuild suite) both pass unchanged, 0 failures.
`cargo build -p autumn-harvest --all-features`, `cargo clippy -p
autumn-harvest --lib --benches --all-features -- -D warnings`, and `cargo fmt
--check -p autumn-harvest` are all clean.

## Reproduce

```bash
# Locate the compiled binary (cargo bench builds in release, doesn't run it):
BIN=$(cargo bench -p autumn-harvest --no-default-features --features testing \
  --bench replay_profile --no-run --message-format=json 2>/dev/null \
  | jq -r 'select(.reason == "compiler-artifact" and .target.name == "replay_profile") | .executable')

# Instruction count:
valgrind --tool=callgrind --branch-sim=no --cache-sim=no \
  --callgrind-out-file=callgrind.out "$BIN"
callgrind_annotate --threshold=100 callgrind.out | head -40

# Allocation counts/bytes (Valgrind's built-in dhat tool):
valgrind --tool=dhat --dhat-out-file=dhat.json "$BIN"
```

`REPLAY_PROFILE_N` (default `5_000`) and `REPLAY_PROFILE_REPS` (default `1`,
each rep rebuilds its own history — never reuses/clones a prior one, so no
extra `Clone` cost leaks into the measured call) are read from the
environment if a different history size or more valgrind wall-time headroom
is needed.
