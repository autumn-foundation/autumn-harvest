# `ShapeCensus::judge` — dominant shape without path copies

`judge` called `dominant()`, which called `shapes()`. That call copied the
path of every distinct shape into a `ShapeSummary` and sorted the list. `judge`
read only the first path. The cost grew with shapes times path length per call.

Every number is a deterministic count from `valgrind`.

## Workload

`benches/shape_census_profile.rs`: 20,000 `observe` calls and 2,000 `judge`
calls over 300 shapes of 3 to 302 events, with a cubic skew.

```sh
cargo bench -p autumn-harvest --no-default-features --features testing \
  --bench shape_census_profile --no-run
valgrind --tool=callgrind <binary>
valgrind --tool=dhat <binary>
```

## Profile before the change

| Entry | Inclusive Ir | Share |
|---|---|---|
| `ShapeCensus::observe` | 624.2 M | 44.7% |
| `ShapeCensus::judge` | 739.4 M | 52.9% |
| `ShapeCensus::shapes` (from `judge`) | 543.4 M | 38.9% |

## Change

`judge` uses a private `dominant_entry` scan. It keeps the first entry with the
strictly highest count, which is the head of the stable sort in `shapes()`.
`dominant()` uses the same scan and copies one path.

## Result

| Counter | Before | After | Delta |
|---|---|---|---|
| Instructions (callgrind) | 1,397,239,460 | 756,894,211 | -45.8% |
| Blocks (dhat) | 691,543 | 87,543 | -87.3% |
| Bytes (dhat) | 1,551,803,022 | 35,003,022 | -97.7% |

`observe` is the remaining cost. It builds a path `Vec` for each call and
compares long paths in the `BTreeMap`. A change there needs a new key type.
