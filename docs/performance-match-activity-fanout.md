# `HistoryMatcher::match_activity`: a fan-out replay is no longer quadratic

This page records the fix for issue #2330. The non-strict activity matcher
replayed a fan-out of N activities in O(N²) instructions. It now replays
one in about O(N log N). A sequential history costs the same as before.

**Outcome: shipped.** At N=1000 the fan-out replay costs 73% fewer
instructions. At N=2000 it costs 85% fewer.

## 🎯 Workload

`benches/match_activity_profile.rs` replays a recorded history through
`HistoryMatcher::new` and `match_activity`. Each activity carries an order
record as input and output. `MATCH_ACTIVITY_PROFILE_SHAPE` picks the shape:

- `fanout`: N schedules, then N completions in schedule order.
- `fanout_rev`: N schedules, then N completions in reverse order.
- `sequential`: N pairs of one schedule and one completion.

## 📈 Cause

`scan_activity_terminal` walked forward from each schedule to the terminal
event of its activity. In a fan-out, activity k stepped over the N−k later
schedules and the k−1 consumed completions. `settle_terminal` then walked the
same range again, to consume the progress events of the activity. Each
activity cost O(N), so the replay cost O(N²). Issue #2303 cut the cost of each
step, but not the number of steps.

## 🔧 Change

`activity_scan_index.rs` holds the positions that can change a scan result.
`HistoryMatcher::activity_scan_role` sorts each event into one role:

| Role | Events | Effect on a scan for activity X |
|---|---|---|
| `Scheduled(id)` | `ActivityScheduled` | Stops the scan when `id` is X. Otherwise an interleaved command. |
| `Terminal(id)` | `ActivityCompleted`, `ActivityFailed`, `ActivityTimedOut` | Ends the scan when `id` is X. Otherwise a step. |
| `Progress(id)` | `ActivityStarted`, `ActivityHeartbeat` | A step. A settle of `id` consumes it. |
| `Interleaved` | Child starts, markers, side effects, timer arms and cancels, local activity schedules | A step. The first one becomes the cursor after the scan. |
| `Barrier` | Signals and external events, which the scan stashes, and every other event, which stops it | Stash and continue, or stop. |
| `Pass` | Sibling terminals and update events | A step. |

The index keeps, for each activity, its stop and progress positions. It also
keeps one list of interleaved commands and one list of barriers. A scan asks
each list for its first unconsumed position. It then visits only barriers
before the stop, in history order, so it stashes the same events as the walk.

Each list skips consumed entries through skip links. `first_live` follows the
links and halves each path that it walks. A consumed event never becomes
unconsumed, so a link never goes stale. `formal/verus/src/scan_skip.rs`
proves that `first_live` returns the first live entry and keeps the links
sound.

The first 16 events of a scan (`LINEAR_SCAN_BUDGET`) are walked one by one,
with the same roles. Most terminals are near their schedule, so a sequential
history builds no index. A settle over a range of 16 events or fewer walks it
too.

The index builds on the first scan that passes the budget. It then adds new
events on each later scan. A cut view hides a tail, so a query ignores each
position past the visible end. `restore_view` brings back the same tail, so
the indexed prefix never changes.

## 📊 Measurement

Callgrind, total instructions (`Ir`) of the harness. "Before" is `caa8c01`.

| Shape | N | Before | After | Change |
|---|---|---|---|---|
| `fanout` | 250 | 9,488,535 | 6,256,427 | −34.1% |
| `fanout` | 500 | 26,858,748 | 12,076,024 | −55.0% |
| `fanout` | 1000 | 87,694,713 | 23,654,947 | −73.0% |
| `fanout` | 2000 | 307,061,590 | 46,798,702 | −84.8% |
| `fanout_rev` | 1000 | 105,092,333 | 23,668,178 | −77.5% |
| `fanout_rev` | 2000 | 376,781,638 | 46,869,264 | −87.6% |
| `sequential` | 1000 | 18,700,031 | 18,847,912 | +0.8% |
| `sequential` | 2000 | 35,161,711 | 35,459,584 | +0.8% |

The fan-out cost now grows about 1.98 times per doubling of N, so it is
linear in this range. The sequential matcher costs about 40 `Ir` more per
activity, for the role lookup in the walk.

Dhat at N=1000: the `fanout` index adds 330,844 bytes in 1,028 blocks, 10.2% of
the total. The peak grows by 228,763 bytes. The `sequential` history builds no
index, so its allocations are unchanged.

At N=1000 the matcher is now 20.1% of the `fanout` harness. The `SipHash` of
the activity id in the index map costs 3.9%. That is the next lever, but a
cheaper hasher needs its own argument about adversarial ids.

## 🧪 Tests

- `replay_probes_grow_near_linearly_with_the_fan_out` counts the consumed-set
  probes of a full replay at N=256 and N=512, for four shapes. The old scan
  failed it with a ratio of 3.98.
- `the_indexed_scan_matches_the_oracle` is a differential property test. It
  runs random histories and random matcher calls, cut views included, through
  the new scan and through a verbatim copy of the old one. It compares each
  result and the full matcher state. A wrong role in either the walk or the
  index fails it.
- `activity_scan_index::tests::a_query_matches_a_plain_walk` checks the skip
  list against a plain walk.

## 🔬 Reproduce

```bash
BIN=$(cargo bench -p autumn-harvest --no-default-features --features testing \
  --bench match_activity_profile --no-run --message-format=json 2>/dev/null \
  | jq -r 'select(.reason=="compiler-artifact" and .target.name=="match_activity_profile") | .executable')
for shape in fanout fanout_rev sequential; do
  for n in 250 500 1000 2000; do
    MATCH_ACTIVITY_PROFILE_SHAPE=$shape MATCH_ACTIVITY_PROFILE_N=$n \
      valgrind --tool=callgrind --branch-sim=no --cache-sim=no \
      --callgrind-out-file="cg.$shape.$n" "$BIN"
  done
done
MATCH_ACTIVITY_PROFILE_N=1000 valgrind --tool=dhat --dhat-out-file=dhat.json "$BIN"
```
