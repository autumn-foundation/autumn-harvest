## Perf — the external stash scan no longer revisits unmatched terminals (issue #2346)

The external stash scan of `HistoryMatcher` visited each unconsumed external
terminal at or after the cursor, before each `match_*` call. A terminal that
matches no stash entry stays unconsumed, so each later call visited it again.
A plain loop of an activity and an external signal therefore replayed in
O(N²). Each `try_stash` also removed its entry with `Vec::remove`, so a P-wide
fan-out of external calls moved O(P²) bytes.

- Only a push can make a terminal match, so each stash helper now notes the
  key of its entry. A scan reads the terminals of the new keys through a
  per-key list in the scan index. It reads every terminal only in a range
  that no earlier scan covered, after a rewind or a restore.
- `match_external_cancel` and `match_external_await` now call
  `stash_transparent_external_event`, so every push notes its key in one
  place.
- The three stashes are `VecDeque`s, so a remove at the front costs O(1).

At N=2000, the new `external_stash_profile` bench costs 94% fewer
instructions for `loop`, 89% fewer for `loop_seq` and 89% fewer for `fanout`.
See `docs/performance-match-activity-fanout.md` and
`docs/design/DESIGN-2346.md`. A terminal still finds its stash entry with a
linear `find`, so the drain of a P-wide fan-out costs O(P²) compares, as
before.

Replay semantics do not change. The differential property test compares the
new scan with a copy of the old walk, and it now rewinds the cursor too.

No migration, no new `WorkflowEvent` variant, and no `harvest_events` change.
