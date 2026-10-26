## Perf — the signal wait is no longer quadratic (issue #2349)

`HistoryMatcher::match_signal` had two quadratic paths:

- A wait found its stashed signal with a linear name compare. Waits in
  reverse order of N stashed signals cost O(N²) compares. The push handler
  claim, the non-blocking receive and the signal race searched the stash
  the same way.
- Each wait walked from the cursor and crossed every unclaimed sibling
  command. The cursor then went back to the first one. N waits beside N
  unclaimed commands cost O(N²) steps.

The fix:

- `SignalStash` keeps the stashed signals in push order. A stash with more
  than 16 slots also keeps a queue for each name. A search then walks one
  queue, in push order, as the old search did. A smaller stash scans its
  slots, at about the old cost.
- The race scan searched the stash at each consumed event. A stash entry
  names its own event, so only a consumed signal of the same name needs a
  search now.
- `signal_scan_step` holds the rule of the old walk for one event. A plain
  wait leaves the walk by the rule of #2330 and continues on the scan
  index. It visits each unconsumed barrier and runs the same step on it. A
  race branch keeps the walk, because it must stop at its own settle
  marker.

MEASUREMENT_PLACEHOLDER

Tests: the scaling test gains eight signal shapes and counts the stash
compares. The old code fails it for seven of them. The differential test
runs copies of the old signal wait, push handler claim, non-blocking
receive, signal race and pure signal reads as its oracle. A proptest
compares `SignalStash` with a plain `VecDeque`, with and without the name
map. Edge tests cover a deadline timer, an unexpected event, a predicate,
a cut and a race branch, on the walk and on the index.

Replay semantics do not change. No migration, no new `WorkflowEvent`
variant, and no `harvest_events` change.
