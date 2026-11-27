## Bolt: `ShapeCensus::judge` finds the dominant shape without copying every path

`ShapeCensus::judge` called `dominant()`. That call built a `ShapeSummary`
for every distinct shape, copied each path and sorted the list, only to read
the first entry. `judge` now scans the shapes once and copies no path. The
result is the same: the first shape in shape order with the highest count.
No `WorkflowEvent` variant, no SQL and no replay impact.

Measured by `shape_census_profile` under valgrind: instructions
1,397,239,460 → 756,894,211 (-45.8%). Allocation blocks 691,543 → 87,543
(-87.3%). Bytes 1,551,803,022 → 35,003,022 (-97.7%).
See `docs/performance-shape-census.md`.
