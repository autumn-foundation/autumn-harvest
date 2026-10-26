## Perf — the external stash no longer costs O(N²) (issue #2346)

The external stash of `HistoryMatcher` had three quadratic paths:

- Before each `match_*` call, the external stash scan visited each unconsumed
  external terminal at or after the cursor. A terminal that matches no stash
  entry stays unconsumed, so each later call visited it again. A plain loop of
  an activity and an external signal therefore replayed in O(N²).
- A terminal found its stash entry with a linear search. A fan-out of P
  external calls, and a plain loop of external signals, then cost O(P²)
  compares. The drift check searched the stash once for each event.
- A match removed its entry with `Vec::remove`, so a P-wide fan-out moved
  O(P²) bytes.

The fix:

- Only a new stash entry can make a terminal match. Each request helper now
  notes the key of its entry. A scan reads the terminals of the new keys,
  through a per-key index of the external terminals. It reads every terminal
  only outside the range of the last scan with work, after a rewind or a
  restore. The external scan no longer walks the history.
- `ExternalStash` keeps the entries in push order, with a map from each id to
  its first entry. A terminal finds its entry in O(1) in a fan-out. A remove
  from the front costs O(1).
- `match_external_cancel` and `match_external_await` now call
  `stash_transparent_external_event`, as the drain does.

At N=2000, the new `external_stash_profile` bench costs 96% fewer
instructions for `loop`, 92% fewer for `loop_seq` and 93% fewer for `fanout`.
All three shapes now grow about 1.95 times per doubling.
See `docs/performance-match-activity-fanout.md` and
`docs/design/DESIGN-2346.md`.

Tests: the scaling test gains four `ExternalLoop` shapes, six
`ExternalFanOut` shapes, `ExternalPairs` and a fan-out after a middle remove.
It now counts stash probes and runs the drift check. The old code fails it
for the first eleven. A proptest compares `ExternalStash` with a
plain `VecDeque` search. The differential test gains a rewind, a forward skip
and a bare `prepare_match`, and edge tests cover each terminal kind, a rewind
and a restore.

Replay semantics do not change. The differential property test compares the
new scan with a copy of the old walk.

No migration, no new `WorkflowEvent` variant, and no `harvest_events` change.
