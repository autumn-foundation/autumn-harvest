# `payload_depth::nests_past_cap`: not worth optimizing — negative result

This page records a profile of the depth walk that the worker runs on every
successful activity result (issue #2312). The walk is `nests_past_cap`, called
through `refuse_deep_result`.

**Outcome: no code change.** The walk is 10.57% of a harness that does nothing
else. In a real completion, serialization, the event append and a database
round trip surround it. Its share there is expected to be far below the 5% impact floor. No profile of a real completion exists.

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
The saving is at most one allocation per call on this mix. Even a free walk would remove
10.6% of this harness, and much less of a real completion.

The walk already makes one pass and does not recurse. The cap bounds its
memory. The walk has no other cost large enough to win back.

## 📊 Decision

Expected below the floor, so the walk stays as it is. Do not profile it again unless the
walk changes or a measurement shows it beside the real completion path.

The walk must not recurse, because a refused value can be very deep (issue
#2225). Any future change must keep that property.
