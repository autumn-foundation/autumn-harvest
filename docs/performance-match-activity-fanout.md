# `HistoryMatcher::match_activity`: a fan-out replay is no longer quadratic

This page records the fix for issue #2330. The non-strict activity matcher
replayed a fan-out of N activities in O(N²) instructions. It now replays
one in about O(N log N). A sequential history costs about the same (+0.7%).

**Outcome: shipped.** At N=1000 the fan-out bench costs 72% fewer
instructions. At N=2000 it costs 84% fewer.

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
| `Pass` | Timer fires, child terminals, local activity terminals and update events | A step. |

The index keeps, for each activity, its stop and progress positions. It also
keeps one list of interleaved commands, which holds every `ActivityScheduled`
too, and one list of barriers. A scan asks each list for its first unconsumed
position. It then visits only barriers before the stop, in history order, so
it stashes the same events as the walk.

The two global lists skip consumed entries through skip links. One activity
has few stops, so its own list uses a plain walk. `first_live` follows the
links and halves each path that it walks. A consumed event never becomes
unconsumed, so a link never goes stale. `formal/verus/src/scan_skip.rs`
proves that `first_live` returns the first live entry and keeps the links
sound.

### When a scan uses the index

A scan walks events one by one, with the same roles, until all three of
these hold:

1. It walked at least `LINEAR_SCAN_BUDGET` (4) events.
2. It passed an interleaved command. With none, the cursor moves past every
   walked event after the scan. No later scan walks them again, so the walk
   stays linear over the replay.
3. The index pays off. Until the index exists, the walks of the matcher must
   have cost more than a build would: 16 walk steps for each event of the
   history (`BUILD_COST_IN_WALK_STEPS`).

Rule 3 is a ski-rental rule. A small fan-out keeps its old cost, because its
walks never cost as much as a build. A large fan-out builds the index once,
and the walk before the build is at most a constant factor of the build. A
sequential history with long gaps, for example many retries or heartbeats,
never builds an index, because rule 2 never holds.

A settle walks its range when the range is 4 events or fewer, or when no
index exists. Otherwise it reads the progress positions from the index.

The first scan that leaves its walk builds the index. Each later use adds the
new events. A cut view hides a tail, so the scan ignores each position past
the visible end. `restore_view` brings back the same tail, so the indexed
prefix never changes.

## 📊 Measurement

Callgrind, total instructions (`Ir`) of the harness. "Before" is `caa8c01`.

| Shape | N | Before | After | Change |
|---|---|---|---|---|
| `fanout` | 250 | 9,488,535 | 6,383,811 | −32.7% |
| `fanout` | 500 | 26,858,748 | 12,345,160 | −54.0% |
| `fanout` | 1000 | 87,694,713 | 24,281,928 | −72.3% |
| `fanout` | 2000 | 307,061,590 | 47,924,432 | −84.4% |
| `fanout_rev` | 1000 | 105,092,333 | 24,203,647 | −77.0% |
| `fanout_rev` | 2000 | 376,781,638 | 47,935,720 | −87.3% |
| `sequential` | 1000 | 18,700,031 | 18,825,429 | +0.7% |
| `sequential` | 2000 | 35,161,711 | 35,412,341 | +0.7% |

The fan-out cost now grows 1.93 to 1.97 times per doubling of N, so it is
linear in this range. Inside the matcher, `match_activity` at N=1000 drops
from 69.0M to 5.3M `Ir` (−92%) for `fanout`. For `sequential` it rises from
330K to 345K `Ir` (+4.6%), about 15 `Ir` per activity.

A second bench, written for the review, measured the matcher alone on more
shapes. "Before" is `caa8c01`:

| Shape | Before | After |
|---|---|---|
| fan-out, N=17 | 28.9K | 33.6K |
| fan-out, N=32 | 86.2K | 130.8K |
| fan-out, N=64 | 320.8K | 286.2K |
| fan-out, N=128 | 1.19M | 590K |
| sequential, 17 progress events per activity, N=1000 | 996K | 1.04M |
| sequential, 64 progress events per activity, N=1000 | 2.73M | 2.87M |
| fan-out with 17 progress events per activity, N=1000 | 546M | 25.8M |
| 5,000 sequential activities, then a fan-out of 20 | 1.99M | 2.07M |

A fan-out of about 32 is the worst case of the ski-rental rule. It costs 52%
more than before, which is about 45K `Ir`. From N=64 up, a fan-out costs less
than before.

Dhat at N=1000: the `fanout` index adds 330,844 bytes in 1,038 blocks, about
10% of the total. The peak grows by 205,804 bytes. The `sequential` history
builds no index, so its allocations are unchanged. The index lives as long as
the matcher.

The `SipHash` of the activity id in the index map is the next lever. A cheaper
hasher needs its own argument about adversarial ids.

### Follow-up (issue #2338)

Issue #2338 found three more quadratic matcher paths. The section
[Issue #2338](#issue-2338-the-external-timer-and-child-scans) below fixes
them.

## 🧪 Tests

- `replay_steps_grow_near_linearly_with_the_fan_out` counts the scan steps of
  a full replay at N=256 and N=512, for four shapes. A step is a consumed-set
  probe, a settle step or an index build step. The old scan failed it with a
  ratio of 3.98.
- `the_indexed_scan_matches_the_oracle` is a differential property test. It
  runs random histories and random matcher calls, cut views included, through
  the new scan and through a copy of the old one. The copy keeps the logic and
  changes only the receiver and the names. The test compares each result and
  every field that a scan or a settle can change. Each case uses the
  production rule or forces every scan onto the index after 0, 1 or 3 walked
  events. The generator holds every event class that the old scan treated in
  its own way. Mutants of the walk, the index and the roles fail it.
- The edge tests run each case with and without padding, and with the
  production rule and a forced index.
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

## Issue #2338: the external, timer and child scans

**Outcome: shipped.** Three more scans now read the same index. At N=1000 the
`fanout_extsig` bench costs 80% fewer instructions, the `timers`
bench 79% fewer and the `children` bench 68% fewer.
`docs/design/DESIGN-2338.md` records the plan.

### Cause

- `scan_ahead_for_external_signal_terminals` runs in `prepare_match`, before
  each `match_*` call. While an external signal, cancel or await is stashed, it
  walked from the cursor to the end of history. One stashed signal made each
  call of a fan-out cost the rest of the history.
- The fire scan of `match_timer_strict` stepped over the later `TimerStarted`
  events and the consumed fires of earlier timers, as the activity scan did.
- The terminal scan of `match_child_workflow` stepped over every later child
  start.

Two other scans that the issue named are linear. They need no change:

- `scan_local_activity_terminal` stops at the first event of any other local
  activity. A worker runs a local activity inline, so its events are always
  adjacent.
- The external signal, cancel and await scans stop at the first command
  event, so they never step over a sibling of their own kind.

`replay_steps_grow_near_linearly_for_every_shape` keeps all five as guards.

One quadratic shape remained. The external scan visited an external terminal
that matches no stash entry again on each call. Issue #2346 fixes it. The
section [Issue #2346](#issue-2346-the-external-stash-scan-and-the-stash-search)
below records the fix.

### Change

- Three roles are new. `Fired(id)` marks a `TimerFired`, `ChildTerminal(id)` a
  child terminal, and `ExternalTerminal` the six external terminal events. An
  activity scan treats them as before: a step, a step and a stash.
- The external scan visits only the unconsumed external terminals at or after
  the cursor, in history order. It resolves each with the code of the old
  walk. Every other event was a no-op for that walk.
- The timer scan runs through the activity scan code. A `ScanTarget` picks the
  stop: an activity schedule or terminal, or the fire of one timer. The old
  timer rules and the activity roles agree on every event class.
- The child scan reads the terminal positions of its child. Like the old scan,
  it does not skip a consumed terminal.

All three use the ski-rental rule above. A walk moves to the index only after
the walks of the matcher cost more than a build.

### Measurement

Callgrind, total instructions (`Ir`) of the harness. "Before" is `e2c7bfb`.

| Bench | Shape | N | Before | After | Change |
|---|---|---|---|---|---|
| `match_activity_profile` | `fanout` | 500 | 12,320,261 | 12,488,526 | +1.4% |
| `match_activity_profile` | `fanout` | 1000 | 24,234,173 | 24,569,705 | +1.4% |
| `match_activity_profile` | `fanout` | 2000 | 47,833,937 | 48,508,146 | +1.4% |
| `match_activity_profile` | `fanout_extsig` | 500 | 38,623,150 | 13,057,175 | −66.2% |
| `match_activity_profile` | `fanout_extsig` | 1000 | 129,798,172 | 25,616,229 | −80.3% |
| `match_activity_profile` | `fanout_extsig` | 2000 | 465,940,560 | 50,739,416 | −89.1% |
| `match_activity_profile` | `sequential` | 500 | 9,624,003 | 9,703,530 | +0.8% |
| `match_activity_profile` | `sequential` | 1000 | 18,821,993 | 18,980,895 | +0.8% |
| `match_activity_profile` | `sequential` | 2000 | 35,407,161 | 35,724,799 | +0.9% |
| `timer_child_fanout_profile` | `timers` | 500 | 13,427,715 | 5,407,186 | −59.7% |
| `timer_child_fanout_profile` | `timers` | 1000 | 49,383,239 | 10,432,040 | −78.9% |
| `timer_child_fanout_profile` | `timers` | 2000 | 185,700,506 | 20,355,658 | −89.0% |
| `timer_child_fanout_profile` | `timers_rev` | 500 | 20,032,969 | 5,708,005 | −71.5% |
| `timer_child_fanout_profile` | `timers_rev` | 1000 | 74,281,950 | 10,988,155 | −85.2% |
| `timer_child_fanout_profile` | `timers_rev` | 2000 | 285,438,552 | 21,490,427 | −92.5% |
| `timer_child_fanout_profile` | `timers_seq` | 500 | 2,263,765 | 2,281,281 | +0.8% |
| `timer_child_fanout_profile` | `timers_seq` | 1000 | 4,094,835 | 4,127,351 | +0.8% |
| `timer_child_fanout_profile` | `timers_seq` | 2000 | 7,664,012 | 7,726,528 | +0.8% |
| `timer_child_fanout_profile` | `children` | 500 | 5,022,614 | 2,843,721 | −43.4% |
| `timer_child_fanout_profile` | `children` | 1000 | 16,272,109 | 5,258,871 | −67.7% |
| `timer_child_fanout_profile` | `children` | 2000 | 58,114,910 | 10,093,580 | −82.6% |
| `timer_child_fanout_profile` | `children_rev` | 500 | 5,030,074 | 2,919,336 | −42.0% |
| `timer_child_fanout_profile` | `children_rev` | 1000 | 16,271,824 | 5,405,844 | −66.8% |
| `timer_child_fanout_profile` | `children_rev` | 2000 | 58,121,515 | 10,393,662 | −82.1% |
| `timer_child_fanout_profile` | `children_seq` | 500 | 1,806,955 | 1,834,980 | +1.6% |
| `timer_child_fanout_profile` | `children_seq` | 1000 | 3,293,786 | 3,347,291 | +1.6% |
| `timer_child_fanout_profile` | `children_seq` | 2000 | 6,162,328 | 6,266,833 | +1.7% |

The quadratic shapes now grow about 2 times per doubling of N.

The shapes with no fan-out cost a little more. A sequential history costs
0.8% more, a sequential timer history 0.8% and a sequential child history
1.6%. Inside the matcher (the inclusive cost of `match_activity`,
`match_timer_strict` or `match_child_workflow`), that is about 21 instructions
for each activity, 43 for each timer and 40 for each child. A plain activity
fan-out with no stashed external call costs 1.4% more. Inside the matcher, that
is about 200 instructions for each activity. Most of it is in the inlined scan
walk. #2330 accepted a cost of the same size.

The scans no longer move the index out of the matcher and back. A move
copied the index twice and built an empty one, which created a `RandomState`
for each of its hash maps. With the larger index of this change, that cost
about 300 instructions for each indexed scan. A split borrow of the index and
the consumed sets now replaces it.

### Tests

- `replay_steps_grow_near_linearly_for_every_shape` counts the scan steps at
  N=256 and N=512 for nine shapes. The old code failed it for seven of them,
  with ratios from 3.79 to 3.98.
- `the_indexed_scan_matches_the_oracle` now also runs copies of the old
  external, timer and child scans as the oracle. It drives external signal and
  await calls, and it matches the timer or child at the cursor. Mutants of
  the new external, timer and child code fail it.
- Edge tests in `scan_fanout_tests` cover a second terminal of one signal or
  await, foreign and consumed fires, and a consumed child terminal. They also
  cover cut views, with the hidden position in the index and not.
- Five mutants were checked by hand: an external scan that skips a terminal,
  a timer target that stops at any fire, an off-by-one child query, a child
  walk that skips consumed terminals, and an await rule without the open
  check. The tests above fail each one.

### Reproduce

```bash
BIN=$(cargo bench -p autumn-harvest --no-default-features --features testing \
  --bench timer_child_fanout_profile --no-run --message-format=json 2>/dev/null \
  | jq -r 'select(.reason=="compiler-artifact" and .target.name=="timer_child_fanout_profile") | .executable')
for shape in timers timers_rev timers_seq children children_rev children_seq; do
  TIMER_CHILD_PROFILE_SHAPE=$shape TIMER_CHILD_PROFILE_N=1000 \
    valgrind --tool=callgrind --branch-sim=no --cache-sim=no \
    --callgrind-out-file="cg.$shape" "$BIN"
done
```

Use `MATCH_ACTIVITY_PROFILE_SHAPE=fanout_extsig` with the `match_activity_profile`
command above for the external scan.

## Issue #2346: the external stash scan and the stash search

**Outcome: shipped.** At N=2000 the `loop` bench of `external_stash_profile`
costs 96% fewer instructions, `loop_seq` 92% fewer and `fanout` 93% fewer.
`docs/design/DESIGN-2346.md` records the plan.

### Cause

- The external stash scan visited each unconsumed external terminal at or
  after the cursor. A terminal that matches no stash entry stays unconsumed.
  So each later call visited it again.
- A command between the external calls makes this happen. The drain at the
  cursor then stops at each command, and a later request stays out of the
  stash. A plain loop of an activity and an external signal is enough. The
  drain stashes each request just before the scan of its own call, so that
  scan visited every later terminal.
- A terminal found its stash entry with a linear search. In a fan-out, the
  search for terminal k stepped over the k earlier entries. A plain loop of
  external signals, and the literal shape of the issue, cost the same. The
  drift check `first_unconsumed_command_event` also searched the stash once
  for each event.
- The three external matchers removed a matched entry with `Vec::remove`. A
  fan-out matches its entries in stash order, so each remove shifted the rest
  of the stash. At P=2000 the `memmove` was 90% of the `fanout` replay.

The first scaling test did not count stash probes. So it found the scan
problem, but it called the literal shape of the issue linear. A review of
the first fix found the stash search.

### Change

- A terminal changes the stash only when its key has an eligible entry. The
  key is the kind and the id of the call. Only a push can add an eligible
  entry, so each request helper notes the key of its entry.
- From one scan with work to the next, no unconsumed terminal inside the
  covered range matches an eligible entry, unless its key is new. A scan
  reads the new keys there. It reads every terminal outside the range, after
  a rewind or a restore. With no new key, rewind or restore, the scan returns
  at once.
- `ExternalTerminalIndex` holds the external terminals, in one skip list and
  per key. The first scan with work builds it, with one cheap test for each
  event. The external scan no longer walks the history.
- `ExternalStash` keeps its entries in push order, with a map from each id to
  its first entry. A terminal finds its entry in O(1) in a fan-out, and a
  remove from the front costs O(1). The drift check reads the map.
- `match_external_cancel` and `match_external_await` kept their own copies of
  the stash code. They now call `stash_transparent_external_event`, as the
  drain does. That code cannot run today (DESIGN-2346 §0.1).
- `ExternalCalls` holds the three stashes, the memo and the external index
  behind one `Box`. `HistoryMatcher` then shrinks to 736 bytes, so the
  workflow futures that hold a context stay under the
  `clippy::large_futures` limit.
- `ScanRole::ExternalTerminal` carries no key. The external index reads the
  key from the event. A key in the role made the activity scan walk cost
  more: at N=1000, the inclusive cost of `match_activity` in the `fanout`
  shape was 5,618,970 with a key and 5,424,041 without it.

### Step counts

The scaling test counts scan steps at N=256 and N=512. A step is a
consumed-set probe, a stash probe, a child scan step, a settle step or an
index build step.

| Shape | Before | After | Ratio after |
|---|---|---|---|
| `ExternalLoop(None, Signal)` | 48,309 → 162,613 | 3,584 → 7,168 | 2.00 |
| `ExternalLoop(Some(Await), Signal)` | 81,471 → 294,207 | 3,592 → 7,176 | 2.00 |
| `ExternalFanOut(Signal, false)` | 33,921 → 133,377 | 2,049 → 4,097 | 2.00 |
| `ExternalFanOut(Signal, true)` | 34,438 → 134,406 | 2,567 → 5,127 | 2.00 |
| `ExternalPairs` | 33,926 → 133,382 | 1,800 → 3,592 | 2.00 |
| `ExternalFanOutAfterMiddleRemove` | — | 4,633 → 9,753 | 2.11 |

The other loop and fan-out shapes give the same counts as the rows above.
The "Before" column of the loop shapes is the first RED, which did not count
stash probes. The "Before" column of the fan-out shapes and `ExternalPairs`
is the second RED, after the scan fix. The "Before" counts of the fan-out
shapes come before the drift check that each fan-out replay now runs once.

`ExternalFanOutAfterMiddleRemove` removes a stash entry from the middle, so
each later search misses the position guess and takes the binary search. A
linear search in its place fails the test.

`SignalThenActivities`, `OpenAwaitThenActivities` and
`CancelThenActivities` keep an external call stashed. They gain about one
step for each event, for the build of the external index. The timer, child
and local activity shapes of #2338 do not change. `ExternalFanOut(Signal,
false)` replaces the `ExternalSignals` control of #2338.

### Measurement

Callgrind, total instructions (`Ir`) of the harness. The totals include the
build of the history. "Before" is `38209c8`.

| Bench | Shape | N | Before | After | Change |
|---|---|---|---|---|---|
| `external_stash_profile` | `loop` | 500 | 68,133,423 | 11,650,006 | −82.9% |
| `external_stash_profile` | `loop` | 1000 | 257,620,889 | 22,859,520 | −91.1% |
| `external_stash_profile` | `loop` | 2000 | 1,006,382,535 | 45,164,061 | −95.5% |
| `external_stash_profile` | `loop_seq` | 500 | 41,019,656 | 11,638,573 | −71.6% |
| `external_stash_profile` | `loop_seq` | 1000 | 144,444,619 | 22,828,346 | −84.2% |
| `external_stash_profile` | `loop_seq` | 2000 | 542,057,986 | 45,115,677 | −91.7% |
| `external_stash_profile` | `fanout` | 500 | 27,729,647 | 6,991,868 | −74.8% |
| `external_stash_profile` | `fanout` | 1000 | 99,353,150 | 13,549,106 | −86.4% |
| `external_stash_profile` | `fanout` | 2000 | 375,130,982 | 26,462,468 | −92.9% |

All three shapes now grow about 1.95 times per doubling of N.

Inside the matcher (the inclusive cost of `match_activity`), at N=1000:

| `match_activity_profile` shape | Before | After | Change |
|---|---|---|---|
| `sequential` | 366,236 | 369,998 | +1.0% |
| `fanout` | 5,498,859 | 5,425,393 | −1.3% |
| `fanout_extsig` | 6,618,848 | 6,098,692 | −7.9% |

A sequential history costs about 4 instructions more for each call. That is
the reset of the memo on an empty stash. The harness totals drop 3.4% to
5.0%. Most of that drop comes from other inlining of the harness payload
code, not from the matcher:

| `match_activity_profile` shape | Before | After |
|---|---|---|
| `sequential` | 18,980,926 | 18,228,917 |
| `fanout` | 24,566,919 | 23,738,795 |
| `fanout_extsig` | 25,611,380 | 24,337,304 |

### Tests

- `replay_steps_grow_near_linearly_for_every_shape` now counts stash probes.
  The old code fails it for the four `ExternalLoop` shapes, the six
  `ExternalFanOut` shapes and `ExternalPairs`. Each fan-out replay also runs
  the drift check once, so a linear drift check fails it too.
- `the_indexed_scan_matches_the_oracle` gains three calls: a cursor rewind,
  a forward skip and a bare `prepare_match`. The failure reason codes now
  carry their position, so the order of two failures of one key shows.
- `the_stash_matches_a_plain_search` compares `ExternalStash` with a plain
  `VecDeque` search after every push, search and remove.
- `the_scan_role_and_the_external_key_agree` checks each event kind of the
  differential test. The external index holds every event that the activity
  scan treats as an external terminal.
- Edge tests in `scan_fanout_tests` cover each terminal kind in a per-key
  pass, the order of one key across a restore and across a rewind, a
  terminal below the covered range, several awaits with one id, and a cut.
- Five mutants, each made by hand, fail the tests above:
  - no pass for the new keys,
  - no range below the covered range,
  - no range above it,
  - an await push that notes no key,
  - the range above the covered range before the new keys.

### Reproduce

```bash
BIN=$(cargo bench -p autumn-harvest --no-default-features --features testing \
  --bench external_stash_profile --no-run --message-format=json 2>/dev/null \
  | jq -r 'select(.reason=="compiler-artifact" and .target.name=="external_stash_profile") | .executable')
for shape in loop loop_seq fanout; do
  for n in 500 1000 2000; do
    EXTERNAL_STASH_PROFILE_SHAPE=$shape EXTERNAL_STASH_PROFILE_N=$n \
      valgrind --tool=callgrind --branch-sim=no --cache-sim=no \
      --callgrind-out-file="cg.$shape.$n" "$BIN"
  done
done
```
