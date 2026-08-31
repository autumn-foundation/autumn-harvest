# DLQ aggregate merge: redundant key clones in the cross-shard fold

This note documents a profiling pass over `dlq::merge_dlq_aggregates` — the
**cross-shard merge** stage behind `GET /api/harvest/dead-letters/aggregate`
(issue #385/#613), invoked once per request after every shard has already
grouped its own rows with `dlq::group_dead_letter_rows`. That per-shard
grouping stage was already profiled and hardened in
`docs/performance-dlq-aggregate.md`; this is a **different function with a
different loop** — merging already-grouped partials across shards, not
grouping raw rows within one shard — and it had never been profiled.

Wall-clock timing is not admissible evidence on this (shared-vCPU) machine:
every number below is a deterministic instruction count
(`valgrind --tool=callgrind`) or allocation count (`valgrind --tool=dhat`).
Like `group_dead_letter_rows`, `merge_dlq_aggregates` builds a
`std::collections::HashMap` with the default randomly-seeded `RandomState`,
so instruction counts carry the same small (≲0.1%) run-to-run variance
documented in `docs/performance-dlq-aggregate.md`'s "Post-revert harness
hardening" section — far below every delta reported here.

## Workload

The harness is `benches/dlq_merge_profile.rs`, a `harness = false` binary
with its own `main()` — no criterion wall-clock loop.

`GET /dead-letters/aggregate` fans out to every shard first
(`aggregate_dead_letters` → per-shard `group_dead_letter_rows`), then merges
the resulting `DlqAggregatePartial`s with `merge_dlq_aggregates`. A real
incident's failure classes are fleet-wide, not shard-local, so every shard
typically reports the *same* small set of root causes — matching
`dlq_aggregate_profile.rs`'s own reference "DLQ flood" shape
(`group_by = [WorkflowName, FailureSignature]`, 25 groups) and the Vantage
UI's default grouping. The harness builds `MERGE_PROFILE_SHARDS` partials
(default 8, a plausible mid-size fleet), each carrying the identical
`MERGE_PROFILE_GROUPS` (default 25) root-cause groups with per-shard counts
and sample ids, so the merge loop is dominated by the hit path exactly as a
real flood is. 500 repetitions (`MERGE_PROFILE_REPS`) of building a fresh
8-shard/25-group input and merging it is the measured workload.

## Profile

```bash
BIN=$(cargo bench -p autumn-harvest --features db \
  --bench dlq_merge_profile --no-run --message-format=json 2>/dev/null \
  | jq -r 'select(.reason=="compiler-artifact" and .target.name=="dlq_merge_profile") | .executable')
valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
callgrind_annotate --threshold=100 cg.out
```

Whole-process baseline: **922,841,106 Ir** (500 reps).

Building the harness's own input (8 partials × 25 groups × 3 sample ids,
each sample id assembled with `format!`) is real, unavoidable setup cost
that has nothing to do with the merge — `format!`/`core::fmt::write`/
`Formatter::pad_integral` alone account for roughly a third of the
whole-process flat profile. Rather than let that swamp the read, this
harness's setup was measured in isolation the same way
`docs/performance-dlq-aggregate.md` isolates `group_dead_letter_rows`:
temporarily replacing the `merge_dlq_aggregates` call with a no-op
(`partials.len()`) that still builds the identical input, re-profiling, and
subtracting.

| | Ir (500 reps) |
|:--|--:|
| Whole process (with merge) | 922,841,106 |
| Setup only (merge replaced with a no-op) | 686,967,166 |
| **Isolated to `merge_dlq_aggregates` + callees** | **235,873,940 (25.6% of whole process)** |

25.6% is well clear of this process's 5% "not worth changing" gate.

Flat profile of the whole-process run, filtered to the symbols attributable
to the merge (its own frame plus the callees it drives — `HashMap::entry`,
hashing, `String`/`Vec` cloning, `rollup_top_n`'s sort, `render_group_key`):

```
 29,150,282 ( 3.16%)  autumn_harvest::dlq::merge_dlq_aggregates
 49,430,000 ( 5.36%)  <core::hash::sip::Hasher<S> as core::hash::Hasher>::write
 17,360,000 ( 1.88%)  core::hash::BuildHasher::hash_one
 14,415,212 ( 1.56%)  hashbrown::rustc_entry::<impl HashMap<K,V,S,A>>::rustc_entry
  9,075,000 ( 0.98%)  <alloc::string::String as core::clone::Clone>::clone
  2,025,000 ( 0.22%)  <alloc::vec::Vec<T,A> as core::clone::Clone>::clone
  3,772,447 ( 0.41%)  core::slice::sort::shared::smallsort::small_sort_general_with_scratch
  1,975,000 ( 0.21%)  autumn_harvest::dlq::render_group_key
```

(plus a share of the generic `malloc`/`free` lines the merge's `Vec<String>`/
`Vec<Option<String>>` clones and `HashMap` resizes drive, which the isolated
235.9M-Ir figure above already captures in aggregate.)

## Hypothesis

`merge_dlq_aggregates`'s per-partial-group loop:

```rust
for group in partial.groups {
    let entry = merged
        .entry(group.key.clone())
        .or_insert_with(|| DlqRawGroup {
            key: group.key.clone(),
            count: 0,
            ..
        });
    entry.count += group.count;
    ..
}
```

clones `group.key` (a `Vec<Option<String>>`) **twice** per loop iteration —
once for the `entry()` lookup, once more inside the `or_insert_with`
closure to populate the new value's own `key` field — even though `group`
is consumed by value at the end of the same iteration and its `key` field
is never read again afterward. The second clone is pure waste: on the
`Vacant` branch (a genuine new group), `group.key` can be moved into the
new `DlqRawGroup` instead of cloned, exactly like `group_dead_letter_rows`
already does for its own per-row `key`.

This is deliberately **not** the `entry()`-vs-`get_mut()` question
`docs/performance-dlq-aggregate.md` already settled (and rejected, after an
adversarial-input regression). That investigation was about *whether to
call `entry()` at all* — replacing one hash+probe pass with two on a
group-miss. This change keeps `entry()` exactly as-is, on every iteration,
independent of hit/miss ratio; it only removes the second, structurally
redundant clone that pattern-matches on `Vacant`/miss and therefore cannot
regress the hit path at all.

Separately, after the merge loop, the final response is built by
re-deriving each output tuple's key from the merged map's *values* — which
already carry a copy of that same key:

```rust
let groups: Vec<(Vec<Option<String>>, DlqRawGroup)> = merged
    .into_values()
    .map(|group| (group.key.clone(), group))
    .collect();
```

`HashMap::into_iter()` already yields owned `(K, V)` pairs; cloning `K` back
out of `V` to reconstruct a pair the map already has is a third redundant
clone, paid once per **distinct** merged group (not per input row).

## Change

- Destructure `group` in the per-partial-group loop so the `Vacant` branch
  moves the already-cloned key into the new `DlqRawGroup` instead of
  cloning `group.key` a second time.
- Replace `merged.into_values().map(|group| (group.key.clone(), group))`
  with `merged.into_iter().collect()`.

No behavior change: both are pure removals of a clone whose value was
already available by move. `DlqRawGroup(key, count, first_seen, last_seen,
sample_ids)` is constructed identically on both branches; the final
`Vec<(key, group)>` pairs are identical because `merged`'s stored `key`
field is always structurally equal to the `HashMap` key it lives under (the
insert path is the only place either is set, and it sets both from the same
value).

## Measurement

<!-- filled in after the fix; see the follow-up commit -->

## Reproduce

```bash
BIN=$(cargo bench -p autumn-harvest --features db \
  --bench dlq_merge_profile --no-run --message-format=json 2>/dev/null \
  | jq -r 'select(.reason=="compiler-artifact" and .target.name=="dlq_merge_profile") | .executable')

# Instruction counts:
valgrind --tool=callgrind --branch-sim=no --cache-sim=no \
  --callgrind-out-file=callgrind.out "$BIN"
callgrind_annotate --threshold=100 callgrind.out

# Allocation counts/bytes:
valgrind --tool=dhat --dhat-out-file=dhat.json "$BIN"
```

`MERGE_PROFILE_SHARDS`, `MERGE_PROFILE_GROUPS`, `MERGE_PROFILE_SAMPLES` and
`MERGE_PROFILE_REPS` (all optional, defaults 8/25/3/500) control the
workload shape; see `benches/dlq_merge_profile.rs`'s module doc.
