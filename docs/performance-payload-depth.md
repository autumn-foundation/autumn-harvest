# `payload_depth::nests_past_cap`: not worth optimizing — negative result

This page records a profile of the depth walk that the worker runs on every
successful activity result (issue #2312). The walk is `nests_past_cap`, called
through `refuse_deep_result`.

**Outcome: not pursued. The production impact is undetermined.** No profile of
a real completion exists. The one idea that the profile suggests can save at
most 18.5% of the walk's own cost. The walk is 10.6% of this harness. The note
below gives the bound and the point at which it matters.

## 🎯 Workload

`benches/payload_depth_profile.rs` calls `refuse_deep_result` 14,000 times.
It runs 2,000 passes over a fixed mix of 7 results: three scalars, two order
documents, a 200-row array and a 100-key object. Every result passes the cap.
Each call takes an owned clone. The worker passes its own result and does not clone.

```bash
BIN=$(cargo bench -p autumn-harvest --no-default-features \
  --bench payload_depth_profile --no-run --message-format=json 2>/dev/null \
  | jq -r 'select(.reason=="compiler-artifact" and .target.name=="payload_depth_profile") | .executable')
valgrind --tool=callgrind --callgrind-out-file=cg.out "$BIN"
callgrind_annotate --inclusive=yes cg.out
valgrind --tool=dhat --dhat-out-file=dhat.json "$BIN"
```

## 📈 Profile

Callgrind, instructions (`Ir`):

| Function | Inclusive Ir | Share |
|---|---|---|
| Program total | 1,339,385,832 | 100.0% |
| `Vec<Value>::clone` (harness) | 692,383,263 | 51.7% |
| `BTreeMap::clone` (harness) | 582,475,533 | 43.5% |
| `nests_past_cap` | 141,524,114 | 10.6% |
| `BTreeMap` `Iter::next` (inside the walk) | 55,781,877 | 4.2% |

The rows are inclusive and overlap, so they do not sum to 100%. The clone costs
most of the instructions. The harness adds the clone. The worker adds no clone.
The worker still drops the result later, outside the walk.

Dhat reports 304,761,064 bytes in 2,391,408 blocks. Almost all come from the
clone. The walk keeps one `Vec` of iterators. For values nested up to 4 levels, as in
this mix, it costs at most 2 allocations per call. Deeper values grow the `Vec`
by doubling.

## 💡 Hypothesis

Reserve the walk stack up front. A nested value then costs 1 allocation, not 2.

`grow_one` is the only growth call inside the walk. It runs 6,003 times, once
for each of the 3 nested values in each pass. Its inclusive cost is 26,191,833
Ir. The direct `alloc` and `dealloc` calls cost 1.8M Ir. The rest is allocator
work that the harness clone also causes, so the 26.2M Ir figure is an upper
bound.

| Bound | Value |
|---|---|
| Saving, as a share of the walk | at most 18.5% |
| Saving, as a share of this harness | at most 2.0% |
| Saving per call, on average | at most 1,870 Ir |

The harness share is not the measure, because the clone and drop inflate the
denominator. The per-call figure does not depend on the harness. A saving of 5%
of a completion needs a completion that runs 37,000 Ir or fewer, on average.
Serialization, the event append and the database round trip are expected to
exceed that. This is not measured.

The walk already makes one pass and does not recurse. The walk has no other cost
large enough to win back.

## 📊 Decision

The idea stays unbuilt, because its upper bound is small. Reopen it if a profile
of a real completion shows the walk above 27% of that completion.

The walk must not recurse, because a refused value can be very deep (issue
#2225). Any future change must keep that property.
