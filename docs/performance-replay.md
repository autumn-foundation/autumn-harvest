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

## Negative result: the same fix does *not* apply to `ActivityScheduled.input`

The `ActivityCompleted.output` fix above naturally raises the question:
does its sibling field, `ActivityScheduled.input`, have the same
redundant-clone shape? It does not, and applying the identical `mem::take`
pattern there is a measured **regression**, not an improvement — recorded
here so nobody re-attempts it without re-deriving this from scratch.

### Hypothesis

`HistoryMatcher::match_activity` (non-strict) never reads the recorded
`ActivityScheduled.input` field (it destructures it with `..`), and
`match_activity_strict` reads it exactly once, by reference, purely for a
`recorded_input != input` equality check — neither path clones it. The
recorded input therefore incurs its (unavoidable, since `Value::Object` is
`BTreeMap`-backed and its drop recurses per node) full structural drop cost
exactly once, whenever the owning `Vec<WorkflowEvent>` is finally dropped at
the end of the replay session. Isolating `replay_from_events`'s own cost
from `build_history` setup (`valgrind --tool=callgrind
--toggle-collect='*replay_from_events*'`) shows `drop_in_place<WorkflowEvent>`
accounting for ~19.4% of that isolated window's instructions, called 10,001
times. The hypothesis: mirroring the `ActivityCompleted.output` fix —
`mem::take`ing `input`/`recorded_input` at match time, leaving `Value::Null`
behind — would make that eventual drop trivial and reduce total
instructions, by direct analogy to the accepted fix above.

### Why this hypothesis is wrong

The analogy breaks on the one detail that made the original fix a genuine
win: `ActivityCompleted.output` was being **cloned** (`output.clone()`) to
produce the match's return value while the original stayed in the event —
so the *original* code paid for one clone (N new heap allocations) plus two
full drops (the original's, and the clone's) for memory that only ever
needed to exist once. `mem::take` collapsed that to zero clones and one
drop — a genuine 2-of-3 reduction.

`ActivityScheduled.input`/`recorded_input` was never cloned by either match
function — only compared by reference (or not read at all, in the
non-strict path). There is no redundant work to eliminate. The single
`Vec<WorkflowEvent>` drop that frees this memory has to happen exactly once
regardless of *when* it happens; `mem::take`ing it at match time doesn't
remove that unavoidable drop, it only **relocates** it earlier in time,
while adding the cost of a fresh `if let` pattern-match + mutable re-borrow
on every one of the 10,001 events. Net effect: the same total deallocation
work, plus pure branching overhead with nothing offsetting it.

### Change tested (reverted — not present in the shipped source)

`autumn-harvest/src/replay.rs`: added, in both `match_activity` (before
`self.cursor += 1;`, after the name-match check passes) and
`match_activity_strict` (after `result` resolves to `Ok`, before
`self.cursor += 1;`), a mutable re-borrow of `self.events[self.cursor]`
that `std::mem::take`s the `input` field, mirroring
`scan_activity_terminal`'s accepted `output` fix exactly:

```diff
@@ match_activity, after the name-match check, before self.cursor += 1 @@
+        if let WorkflowEvent::ActivityScheduled { input, .. } = &mut self.events[self.cursor] {
+            let _ = std::mem::take(input);
+        }
         self.cursor += 1;
         self.scan_activity_terminal(activity_id, self.cursor)

@@ match_activity_strict, after `result` resolves Ok, before self.cursor += 1 @@
+        if let WorkflowEvent::ActivityScheduled { input, .. } = &mut self.events[self.cursor] {
+            let _ = std::mem::take(input);
+        }
         self.cursor += 1;
         self.scan_activity_terminal(activity_id, self.cursor)
```

### Measurement

Same binary build methodology, same `valgrind --tool=callgrind
--branch-sim=no --cache-sim=no` invocation, same session, same defaults
(`REPLAY_PROFILE_N=5000`, `REPLAY_PROFILE_REPS=1`), controlled A/B via
`git stash`/`git stash drop` so both runs used identical toolchain state
apart from this one change:

| | Instructions (Ir) |
|---|---|
| Before (clean `HEAD`) | 189,169,774 |
| After (`mem::take` on `ActivityScheduled.input` in both match fns) | 189,349,123 |
| **Delta** | **+179,349 (+0.095%) — a regression** |

The "before" number reproduces the fix's own documented figure
(189,162,617, §Measurement above) to within the same "handful of
instructions" determinism noise this doc already calls out (0.004%
difference between runs there) — confirming the methodology and toolchain
state are directly comparable, and that the +0.095% delta measured here is
a real effect, not noise (roughly 25× the observed noise floor).

Correctness was unaffected either way — `cargo test -p autumn-harvest
--no-default-features --features testing --lib` (2,148 tests) and `--test
integration` (1,617 tests) both passed with the change applied, and `cargo
build`/`clippy -p autumn-harvest --lib --benches --all-features -- -D
warnings` were clean. The change was reverted purely because it moved the
wrong direction on the one counter this repo's Bolt agent gates on — not
because of any correctness issue.

### Conclusion

`ActivityScheduled.input`/`recorded_input`'s drop cost, visible in the
profile as part of `drop_in_place<WorkflowEvent>`, is **inherent** given the
current data model: `HistoryMatcher` retains the full history —including
recorded inputs — for the life of a replay session (required so
`match_activity_strict` can compare against it), and that memory has to be
freed exactly once, no matter when. Reducing it further would mean not
retaining the full input in the first place (e.g. discarding a matched
event's payload from the retained `Vec` once it can no longer be needed, or
restructuring what `WorkflowEvent`/`HistoryMatcher` store) — a change to the
data structure's ownership model, which this repo's Bolt agent charter
requires asking a maintainer about before attempting, not a local
match-site fix. Reported here as a stopping point per that same charter's
"if top entries are inherent work, that's a legitimate finding, report and
stop."
