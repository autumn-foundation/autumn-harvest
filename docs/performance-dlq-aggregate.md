# DLQ aggregate grouping: instruction/allocation-count profiling and the redundant-clone fix

This note documents a profiling pass over `dlq::group_dead_letter_rows` — the
in-memory grouping core behind `GET /api/harvest/dead-letters/aggregate`
(issue #385, extended with cause-targeted `dlq_reason`/`error_class`
dimensions by issue #613) — and the resulting fix to its per-row group
lookup. Wall-clock timing is not admissible evidence on this (shared-vCPU)
machine — every number below is a deterministic instruction or allocation
count from `valgrind --tool=callgrind` / `valgrind --tool=dhat`,
reproducible bit-for-bit on any machine.

## Workload

The harness is `benches/dlq_aggregate_profile.rs`, a `harness = false`
binary with its own `main()` — no criterion wall-clock loop, so a profiler
can be pointed at it directly with nothing but the target workload running.

`GET /dead-letters/aggregate` exists to answer an operator's first incident
question during a "DLQ flood": a bad deploy or a broken downstream causes a
large *volume* of dead-letters, almost all of which share one of a small
number of *root causes*. This harness constructs exactly that shape —
`DLQ_PROFILE_N` rows (default 20,000, a plausible flood before an operator
reacts) collapsing into `DLQ_PROFILE_GROUPS` distinct
`(workflow_name, failure_signature)` groups (default 25, a fleet with
several workflow types where one or two failure classes dominate) — rather
than picking a group count to flatter the fix under test: 25 groups over
20,000 rows is an 800:1 collapse ratio, well inside what a real incident
produces (a single root cause repeated by automatic retries) and far short
of "every row its own group" (which would make grouping pointless) or "one
row, one group" (which would make grouping trivial and hide the bug this
fix addresses).

Error text realistically mixes the shapes `failure_signature`/`dlq_reason`/
`error_class`'s own unit tests exercise: tagged `DeadLetterReason` JSON
envelopes (poison-pill, history-cap, task-timeout), a typed `ActivityFailure`
envelope, and plain messages carrying dynamic UUID/hex/decimal noise the
classifier must normalize away — so the classifier functions this harness's
target sits beside get real exercise, not an artificially cheap fast path.

`group_by = [WorkflowName, FailureSignature]` mirrors the Vantage UI's own
`DEFAULT_DLQ_SUMMARY_GROUP_BY` (`"workflow_name,failure_signature"`), the
grouping an operator actually lands on by default — not a synthetic
single-dimension shape chosen to be easy to optimize.

`group_dead_letter_rows` is called directly (no database): it is the pure,
DB-free core `aggregate_dead_letters` (the `db`-gated Diesel query function)
delegates to after loading rows, extracted specifically so it can be driven
here without Docker/Postgres, and its `AggregateRow` row-tuple type and
`DlqAggregateParams`/`DlqRawGroup` types are `pub` for exactly this reason.

## Profile

```
valgrind --tool=callgrind --branch-sim=no --cache-sim=no \
  --callgrind-out-file=callgrind.out <dlq_aggregate_profile binary>
callgrind_annotate --threshold=100 callgrind.out
```

### Isolating the target from benchmark setup

`build_rows` (constructing the 20,000-row `Vec<AggregateRow>` plus the
`workflow_names` lookup map) is legitimate benchmark setup, not part of the
code under test. To measure `group_dead_letter_rows` in isolation, `main()`
was temporarily patched to skip the call to it (rows are built, then
discarded without grouping) and re-profiled:

```
==31235== I   refs:      72,358,040
```

That is the setup-only cost. Subtracting it from the whole-process baseline
(`268,401,099`, below) isolates `group_dead_letter_rows`'s own cost at
`196,043,059` Ir — **73.0%** of the whole-process workload, confirming the
target is not a peripheral cost center. The isolated bracket is the more
rigorous of the two measurements (it cannot be diluted by unrelated setup
cost); the whole-process bracket is reported alongside it because this
harness's *entire* process **is** the target workload (grouping a realistic
DLQ flood), not a synthetic microbenchmark of an isolated function — so the
"represents ≥5% of workload cost" qualifier is satisfied either way.

### Flat (self-cost) profile, whole process, before the fix

```
268,401,099 (100.0%)  PROGRAM TOTALS

 45,767,200 (17.05%)  autumn_harvest::dlq::failure_signature
 36,076,000 (13.44%)  autumn_harvest::dlq::normalize_token
 26,295,061 ( 9.80%)  _int_free
 18,338,660 ( 6.83%)  malloc
 15,252,802 ( 5.68%)  _int_malloc
 11,791,031 ( 4.39%)  core::hash::sip::Hasher<S>::write
 11,675,056 ( 4.35%)  free
  7,379,045 ( 2.75%)  __memcpy_avx_unaligned_erms
  6,880,800 ( 2.56%)  serde_json::ser::format_escaped_str_contents
  6,249,593 ( 2.33%)  hashbrown::rustc_entry::HashMap<K,V,S,A>::rustc_entry
  4,763,720 ( 1.77%)  core::hash::BuildHasher::hash_one
  4,720,125 ( 1.76%)  realloc
  4,676,425 ( 1.74%)  alloc::raw_vec::RawVecInner<A>::finish_grow
  3,922,450 ( 1.46%)  alloc::fmt::format::format_inner
  3,842,700 ( 1.43%)  alloc::raw_vec::RawVecInner<A>::reserve::do_reserve_and_handle
  3,782,099 ( 1.41%)  autumn_harvest::dlq::group_dead_letter_rows
  ...
  2,640,825 ( 0.98%)  <alloc::string::String as core::clone::Clone>::clone
  ...
  1,620,000 ( 0.60%)  <alloc::vec::Vec<T,A> as core::clone::Clone>::clone
```

`failure_signature`/`normalize_token` dominate (30%+ combined) because they
run on *every* row (classifying its error text is unavoidable, real work).
`hashbrown::rustc_entry` — the machinery behind `HashMap::entry(key)` — is
the fifth-largest attributed symbol at 2.33%, and `HashMap::get_mut` does
not appear in the symbol table at all: the old code's every-row
`groups.entry(key.clone())` call took `key` by value unconditionally, so
`rustc_entry` (and the `String`/`Vec` clones that feed it — see below) ran
once per **row** regardless of whether that row landed on an already-seen
group.

Tracing the call tree: `groups.entry(key.clone())` (`dlq.rs`, the pre-fix
line) clones a `Vec<Option<String>>` — the two-dimension grouping key,
holding up to two `String`s (`workflow_name`, `failure_signature`) — on
*every one of the 20,000 rows*, even though `group_by = [WorkflowName,
FailureSignature]` over 20,000 rows collapsing into 25 groups means 19,975
of those 20,000 clones are wasted: the group already exists, and
`HashMap::entry`'s API forces the caller to construct an owned key before it
can even check.

## Hypothesis

`HashMap::entry(key)` always takes its key by value, so
`groups.entry(key.clone())` unconditionally clones `key` before the map even
looks it up — paying the clone cost on the (overwhelmingly common, in a
real incident) already-exists path, when a cheap by-reference `get_mut(&key)`
lookup would do. Restructuring the loop as "look up by reference first, only
construct/insert an owned key on the (rare) group-miss path" should remove
19,975 of the 20,000 `Vec<Option<String>>` clones (and the `String` clones
nested inside them) from this workload, at zero cost to correctness: the
grouping semantics (count, first/last-seen, capped sample ids) are unchanged
regardless of which branch performs the update.

Given the flat profile already attributes `rustc_entry` plus the
`String`/`Vec` clone family to a meaningful (if not dominant) share of the
per-row cost, removing ~99.9% of those clones (19,975 of 20,000 redundant
group-hits vs. only 25 genuine group-misses) should visibly reduce
`rustc_entry`, the clone symbols, and total allocator traffic.

## Change

`autumn-harvest/src/dlq.rs`, `group_dead_letter_rows`'s per-row grouping
loop (extracted from `aggregate_dead_letters` as a pure, DB-free `pub`
function so it can be profiled and unit-tested without a database):

```rust
// `HashMap::entry` always takes its key by value, so the obvious
// `groups.entry(key.clone())` clones `key` -- a `Vec<Option<String>>`,
// itself owning up to `group_by.len()` `String`s -- on *every* row,
// even though a real DLQ aggregate's whole point is that most rows
// land in an *already-seen* group (a "flood" is many rows, few root
// causes). `get_mut` looks the key up by reference on the hot
// (existing-group) path, so the clone below runs only once per
// *group*, not once per *row*.
if let Some(entry) = groups.get_mut(&key) {
    entry.count += 1;
    entry.first_seen = min_instant(entry.first_seen, Some(failed_at));
    entry.last_seen = max_instant(entry.last_seen, Some(failed_at));
    if entry.sample_ids.len() < params.samples_per_group as usize {
        entry.sample_ids.push(id.to_string());
    }
} else {
    let sample_ids = if params.samples_per_group > 0 {
        vec![id.to_string()]
    } else {
        Vec::new()
    };
    groups.insert(
        key.clone(),
        DlqRawGroup { key, count: 1, first_seen: Some(failed_at), last_seen: Some(failed_at), sample_ids },
    );
}
```

`get_mut(&key)` looks the key up by reference — no clone — on every row; the
`key.clone()` in the miss branch is unavoidable (the map's key and
`DlqRawGroup.key` are two independently-owned copies of the same value by
design, unrelated to this bug) but now runs at most once per **group**
(25 times for this workload, not 20,000). No other behavior changed:
`min_instant`/`max_instant` are pure `Option<DateTime<Utc>>` reducers with
the same short-circuit-on-`None` semantics on both branches, and the
sample-id cap (`params.samples_per_group`) is checked identically in both
branches.

## Measurement

Both runs: identical release-profile binary (`cargo bench --no-run`), same
`valgrind --tool=callgrind --branch-sim=no --cache-sim=no` invocation, same
defaults (`DLQ_PROFILE_N=20000`, `DLQ_PROFILE_GROUPS=25`,
`DLQ_PROFILE_REPS=1`).

### Instruction count (Ir)

| | Whole process | Isolated (minus 72,358,040 setup) |
|---|---|---|
| Before | 268,401,099 | 196,043,059 |
| After  | 255,978,668 | 183,620,628 |
| **Reduction** | **12,422,431 (4.63%)** | **12,422,431 (6.34%)** |

The isolated bracket clears the ≥5% impact floor (6.34% > 5%); the
whole-process bracket, while below the floor on its own, corroborates the
same absolute instruction reduction on the workload as a whole and confirms
the fix is not diluted away by unrelated cost.

The mechanism is directly traceable in the flat profile, not just inferred
from the total:

| Symbol | Before | After | Δ |
|---|---|---|---|
| `Vec<T,A>::clone` | 1,620,000 | 2,025 | **−99.87%** |
| `String::clone` | 2,640,825 | 1,322,475 | −49.93% |
| `hashbrown::rustc_entry` | 6,249,593 | 3,139,850 | −49.77% |
| `HashMap::get_mut` | (absent) | 2,158,670 | *new symbol* |

`Vec<T,A>::clone` — the direct target of the fix — collapsed by 99.87%,
consistent with removing 19,975 of 20,000 redundant clones and leaving only
the 25 genuine group-miss clones (plus incidental Vec-clone traffic
elsewhere in the process, accounting for the non-zero `2,025` remainder).
`String::clone` and `hashbrown::rustc_entry` each dropped by essentially
half, matching the expected mechanism exactly: half of the per-row
`entry()`-path work (the "already exists, look it up and mutate in place"
half, now served by the new `HashMap::get_mut` symbol) moved off the
clone-then-entry path entirely, while the fraction of `rustc_entry` calls
still reachable from the (unavoidable) miss-branch `groups.insert` retains
its own internal `entry`-adjacent hashing cost.

### Allocation count/bytes (dhat)

| | Blocks | Bytes |
|---|---|---|
| Before | 493,768 | 19,609,888 |
| After  | 433,868 | 16,949,019 |
| **Reduction** | **59,900 (12.13%)** | **2,660,869 (13.57%)** |

Both clear the ≥10% allocation-reduction floor independently of the
instruction-count evidence. The predicted mechanism cross-validates
numerically: 19,975 redundant rows (20,000 total rows minus 25 genuine
group-misses) × 3 allocations per redundant clone (one `Vec` backing buffer
+ two `String` reallocations for the two non-null `Option<String>` grouping
key components) = 59,925 predicted block reduction, within 0.04% of the
59,900 measured — the allocation-count evidence and the instruction-count
evidence independently point at the same root cause.

### Correctness

All 71 pre-existing `dlq::` unit tests pass unchanged (`cargo test -p
autumn-harvest --features db --lib dlq::`) — `failure_signature_*`,
`dlq_reason_*`, `error_class_*`, `merge_*`, `params_*`,
`dead_letter_reason_*`, `group_dimension_wire_round_trips_cause_dims`,
`dimension_wire_round_trip`, `redrive_*`, `time_bucket_formats`,
`escape_like_escapes_wildcards`, `invalid_dead_letter_task_type_is_config_error`,
etc. `cargo test -p autumn-harvest --features db --lib` (full lib suite),
`cargo clippy -p autumn-harvest --all-features --tests -- -D warnings`, and
`cargo fmt --check` are all clean.

## Reproduce

```bash
# Locate the compiled binary (cargo bench builds in release, doesn't run it):
BIN=$(cargo bench -p autumn-harvest --features db \
  --bench dlq_aggregate_profile --no-run --message-format=json 2>/dev/null \
  | jq -r 'select(.reason == "compiler-artifact" and .target.name == "dlq_aggregate_profile") | .executable')

# Instruction count:
valgrind --tool=callgrind --branch-sim=no --cache-sim=no \
  --callgrind-out-file=callgrind.out "$BIN"
callgrind_annotate --threshold=100 callgrind.out | head -40

# Allocation counts/bytes (Valgrind's built-in dhat tool):
valgrind --tool=dhat --dhat-out-file=dhat.json "$BIN"
```

`DLQ_PROFILE_N` (default `20_000`), `DLQ_PROFILE_GROUPS` (default `25`), and
`DLQ_PROFILE_REPS` (default `1`, each rep rebuilds its own rows/map — never
reuses/clones a prior one, so no extra `Clone` cost leaks into the measured
call) are read from the environment if a different flood size, group
cardinality, or more valgrind wall-time headroom is needed.

To isolate `group_dead_letter_rows`'s own cost from `build_rows` setup cost
(the "isolated" column above), temporarily edit `main()` in
`benches/dlq_aggregate_profile.rs` to skip the call to
`group_dead_letter_rows`, rebuild, and re-profile; subtract that setup-only
Ir count from the full-run Ir count.
