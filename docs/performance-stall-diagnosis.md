# Stall diagnosis: allocation-free ranking pass over pending activities

This note documents a profiling pass over
`stall_diagnosis::classify_execution` — the pure root-cause classifier
behind `GET /api/harvest/workflows/{id}/diagnose` (issue #809) — and a
"rank-then-construct" split in its activity fold that eliminates the
per-candidate `BlockedOn` allocation the original fold paid for every
pending-activity row, not just the eventual winner. Wall-clock timing is
not admissible evidence on this (shared-vCPU) machine — every number below
is a deterministic instruction count (`valgrind --tool=callgrind`) or
allocation count (`valgrind --tool=dhat`), both reproducible bit-for-bit on
any machine (the module has no `HashMap`/`RandomState` in its hot path, so
this is not subject to the run-to-run seed variance documented in
`docs/performance-dlq-aggregate.md`).

## Workload

The harness is `benches/stall_diagnosis_profile.rs`, a `harness = false`
binary with its own `main()` — no criterion wall-clock loop.

The plugin handler for `GET /workflows/{id}/diagnose`
(`autumn-harvest-plugin/src/api.rs`) calls `classify_execution` **twice**
per HTTP request: once to compute `db_verdict` (deciding whether an
additional replay pass could change the answer) and once more, after
populating `inputs.awaited_signals`/`inputs.replay_waits`, to compute the
actual `blocked_on` response field. For the workload this harness
exercises (a wide *pending-activity* fan-out with a genuine worst-cause row
somewhere in it), `db_verdict` already resolves to an activity verdict, so
`replay_could_win` is `false` and the second call runs unconditionally
regardless. This harness reproduces that exact double-call shape.

The module's own docs name the wide fan-out as its **headline** use case:
"one wedged slot among nineteen healthy ones" — the whole reason
`classify_execution` folds over every pending activity row and reports the
*worst* verdict, rather than the first. The harness builds exactly that
shape: 500 pending-activity rows, ~70% in retry backoff with a recorded
downstream error (the single most common shape in a real stalled fan-out),
~20% healthy, ~9% with an open circuit breaker, and exactly one genuine
`activity_no_worker` row (the true, permanent root cause) placed two-thirds
of the way through the collection — away from either end, so a correctness
fix cannot get away with only checking the first or last row. This is
deliberately **not** the all-healthy case: the endpoint exists to answer
"why is this stalled?", so the realistic worst-case invocation this harness
measures is exactly the one operators trigger it for. 2,000 repetitions of
this fixed fan-out (4,000 total `classify_execution` calls) is the measured
workload.

## Profile

```bash
BIN=$(cargo bench -p autumn-harvest --no-default-features \
  --bench stall_diagnosis_profile --no-run --message-format=json 2>/dev/null \
  | jq -r 'select(.reason=="compiler-artifact" and .target.name=="stall_diagnosis_profile") | .executable')
valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
callgrind_annotate --threshold=98 cg.out
```

```
909,070,414 (100.0%)  PROGRAM TOTALS

162,183,344 (17.84%)  malloc.c:_int_free
155,322,821 (17.09%)  autumn_harvest::stall_diagnosis::classify_pending_activity
135,105,074 (14.86%)  malloc.c:malloc
119,841,953 (13.18%)  autumn_harvest::stall_diagnosis::classify_execution
 99,024,750 (10.89%)  <alloc::string::String as core::clone::Clone>::clone
 84,074,480 ( 9.25%)  malloc.c:free
 60,829,198 ( 6.69%)  core::ptr::drop_in_place<BlockedOn>
 42,298,176 ( 4.65%)  memmove-vec-unaligned-erms.S:__memcpy_avx_unaligned_erms
 27,023,895 ( 2.97%)  __rustc::__rdl_alloc
  9,007,980 ( 0.99%)  malloc/arena.c:free
```

`classify_pending_activity` (17.09%) plus `classify_execution` (13.18%) —
the two functions this change touches — already account for **30.27%**
(275,164,774 / 909,070,414) of total program instructions before any
change, comfortably clearing the ≥5%-of-workload gate by 6×. The
remaining five malloc/free lines (`_int_free`, `malloc`, `free`,
`__rdl_alloc`, `arena.c:free`) sum to 417,394,773 — **45.91%** of the
total — plus `String::clone` (10.89%), `drop_in_place<BlockedOn>`
(6.69%), and the `memcpy` glibc routine libcore's `Vec`/`String` cloning
lowers to (4.65%). All of it is downstream of the same two call sites:
every allocation on this profile originates from a `BlockedOn`
construction inside `classify_pending_activity`, called from
`classify_execution`'s fold.

## Hypothesis

`classify_execution`'s activity fold constructs a full `BlockedOn` — via
`classify_pending_activity`, which clones `activity_name`/`queue`/
`last_error`/etc. on most of its ten branches — for **every** candidate
row, then immediately compares its precedence against the running winner
and drops every value except the eventual maximum. For a fold over `N`
rows this pays `N` allocation-bearing constructions to produce exactly `1`
retained result — `N − 1` of them are pure waste, and on this fixture's
realistic mostly-degraded shape almost every row takes an
allocation-bearing branch (`ActivityRetrying` clones two `String`s;
`ActivityCircuitOpen` clones one plus a `DateTime` copy).

Splitting the fold into two passes — a cheap, allocation-free precedence
comparison over every row, followed by exactly one `BlockedOn`
construction for the winning row only — should remove essentially all of
the malloc/clone/drop/memcpy family from the profile, since the fold no
longer allocates 499 times more than it needs to on this fixture.

## Change

`autumn-harvest/src/stall_diagnosis.rs` gains a new private function,
`activity_precedence_for_facts(facts: &PendingActivityFacts, now:
DateTime<Utc>) -> u8` — a byte-for-byte structural mirror of
`classify_pending_activity`'s branch order and guard conditions (including
the `&& let Some(...)` "flag set but its companion field is absent →
fall through to the next branch" semantics the five guarded branches rely
on), but returning only the bare `u8` precedence integer instead of
constructing a `BlockedOn`. No `String` clone, no heap allocation, no
`BlockedOn` value anywhere in its body.

`classify_execution`'s activity fold now finds the winning **index** using
only `activity_precedence_for_facts` (`inputs.activities.iter().enumerate()
.fold(None::<(usize, u8)>, ...)`, preserving the original fold's documented
"keep the first row of an equal-ranked tie" semantics exactly — `>=`
retains the current winner, so a strictly greater precedence is required to
displace it), then materializes the real `BlockedOn` via
`classify_pending_activity` exactly once, for `inputs.activities[idx]`
only:

```rust
let worst_activity_index = inputs.activities.iter().enumerate().fold(
    None::<(usize, u8)>,
    |worst: Option<(usize, u8)>, (idx, facts)| {
        let candidate_precedence = activity_precedence_for_facts(facts, now);
        match worst {
            Some((_, current_precedence)) if current_precedence >= candidate_precedence => {
                worst
            }
            _ => Some((idx, candidate_precedence)),
        }
    },
);
if let Some((idx, _)) = worst_activity_index {
    return Some(classify_pending_activity(&inputs.activities[idx], now));
}
```

Behavior is unchanged: the reported verdict, its precedence-based ranking,
and the tie-break rule are byte-identical to before — only *how many
times* `BlockedOn` is constructed changes (from `N` to at most `1`), never
*what* is constructed or *which* row wins. A new property test,
`activity_precedence_for_facts_props::matches_classify_then_precedence`
(512 randomized `proptest` cases by default, `PROPTEST_CASES`-overridable),
sweeps every branch-relevant field of `PendingActivityFacts` — including
every "flag true but its companion `Option` field is `None`" fallthrough
shape — and asserts `activity_precedence_for_facts(facts, now) ==
activity_precedence(&classify_pending_activity(facts, now))` on every
generated input, so a future edit to either function's branch order that
drifts the two apart fails loudly here rather than silently mis-ranking a
live fan-out.

## Measurement

Both binaries built from the identical harness/`Cargo.toml` bench
declaration, differing only by this one-file diff, same
`valgrind --tool=callgrind --branch-sim=no --cache-sim=no` and
`valgrind --tool=dhat` invocations, same session.

| | Instructions (Ir) |
|---|---|
| Before | 909,070,414 |
| After  | 164,943,286 |
| **Reduction** | **744,127,128 (81.86%)** |

The reduction clears the ≥5% floor by more than 16×.

| dhat | Before | After | Reduction |
|---|---|---|---|
| Blocks | 3,002,662 | 9,914 | 2,992,748 (**99.67%**) |
| Bytes  | 102,482,479 | 200,915 | 102,281,564 (**99.80%**) |

The remaining 9,914 blocks are the expected residual: 500 rows of one-time
fixture setup (`build_inputs`, each row cloning up to three `String`
fields, called once outside the measured loop) plus exactly one `BlockedOn`
materialization per `classify_execution` call (4,000 calls × up to 2 clones
for the winning `ActivityNoWorker` row) — not per-candidate cost.

The flat profile after the change confirms the mechanism, not just the
total — the entire malloc/clone/drop/memcpy family drops out of the
`--threshold=98` view, leaving `classify_execution` itself (now inlining
the cheap comparison fold) as almost the whole cost:

```
164,943,286 (100.0%)  PROGRAM TOTALS

160,356,079 (97.22%)  autumn_harvest::stall_diagnosis::classify_execution
    574,868 ( 0.35%)  malloc.c:_int_free
    468,000 ( 0.28%)  core::hash::sip::Hasher<S>::write
    431,414 ( 0.26%)  malloc.c:malloc
```

`malloc`/`_int_free` combined are now 0.61% of the (much smaller) total,
down from ~46% of the original — consistent with the fold now allocating
`O(1)` `BlockedOn` values per `classify_execution` call instead of `O(N)`.

**Correctness**: all 116 pre-existing `stall_diagnosis::` unit tests pass
unchanged, plus the new 512-case property test — before, during, and after
this change — 0 failures. `cargo fmt --check` and `cargo clippy -p
autumn-harvest --all-features --tests -- -D warnings` are both clean.

## Reproduce

```bash
BIN=$(cargo bench -p autumn-harvest --no-default-features \
  --bench stall_diagnosis_profile --no-run --message-format=json 2>/dev/null \
  | jq -r 'select(.reason=="compiler-artifact" and .target.name=="stall_diagnosis_profile") | .executable')

# Instruction count:
valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
callgrind_annotate --threshold=98 cg.out | head -30

# Allocation counts/bytes:
valgrind --tool=dhat --dhat-out-file=dhat.json "$BIN"
```

`STALL_DIAGNOSIS_PROFILE_N` (default `500`) and `STALL_DIAGNOSIS_PROFILE_REPS`
(default `2_000`) control the fan-out width and repetition count if more
valgrind wall-time headroom is needed.
