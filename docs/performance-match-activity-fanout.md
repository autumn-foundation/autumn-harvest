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
activity cost O(N), so the replay cost O(N²). PR #2303 cut the cost of each
step, but not the number of steps. See
[the earlier change](#the-earlier-change-pr-2303).

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

### Re-measured on `trunk-dev` (2026-10-25)

The same harness ran again on `572c7ff`, with rustc 1.99.0 and valgrind
3.22.0 on a 4-vCPU host. Every cell stays within 0.03% of the "After"
column:

| Shape | N | After (PR #2340) | `572c7ff` |
|---|---|---|---|
| `fanout` | 1000 | 24,281,928 | 24,286,146 |
| `fanout` | 2000 | 47,924,432 | 47,933,569 |
| `fanout_rev` | 1000 | 24,203,647 | 24,210,879 |
| `fanout_rev` | 2000 | 47,935,720 | 47,944,448 |
| `sequential` | 1000 | 18,825,429 | 18,828,713 |
| `sequential` | 2000 | 35,412,341 | 35,416,385 |

Issue #2338 shipped after this run. It adds about 1.4% to `fanout` and 0.8%
to `sequential`. [Its own table](#measurement) gives the new numbers.

### Follow-up (issue #2338)

Issue #2338 found three more quadratic matcher paths. The section
[Issue #2338](#issue-2338-the-external-timer-and-child-scans) below fixes
them.

## The earlier change (PR #2303)

PR #2303 added `match_activity_profile`. Its `fanout` profile at N=1000 put
62.0% of the instructions in the `SipHash` of event positions.
`is_consumed` runs once per scanned event, and it probed three index sets
with the default hasher.

The change gives those three sets `EventIndexHasher`, a multiplicative
hasher in `replay.rs`. An event index is a history position, not data that
an attacker chooses, so `SipHash` protects nothing there. No code iterates
the sets, so the hasher cannot change behavior.

Callgrind, total `Ir` of the harness:

| Shape | N | Before | After | Change |
|---|---|---|---|---|
| `fanout` | 250 | 18,395,861 | 9,480,027 | −48.5% |
| `fanout` | 500 | 62,353,728 | 26,850,984 | −56.9% |
| `fanout` | 1000 | 228,281,482 | 87,688,451 | −61.6% |
| `fanout_rev` | 250 | 19,222,238 | 10,556,684 | −45.1% |
| `fanout_rev` | 500 | 65,476,976 | 31,575,854 | −51.8% |
| `fanout_rev` | 1000 | 241,115,115 | 105,086,035 | −56.4% |
| `sequential` | 1000 | 18,658,519 | 18,693,757 | +0.2% |

The scan stayed O(N²). Only the cost of each step fell. The "Before" column
of [the main table](#-measurement) is this "After" column, measured again on
`caa8c01`.

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
| `external_stash_profile` | `loop` | 500 | 68,133,423 | 11,637,294 | −82.9% |
| `external_stash_profile` | `loop` | 1000 | 257,620,889 | 22,845,680 | −91.1% |
| `external_stash_profile` | `loop` | 2000 | 1,006,382,535 | 45,138,999 | −95.5% |
| `external_stash_profile` | `loop_seq` | 500 | 41,019,656 | 11,624,485 | −71.7% |
| `external_stash_profile` | `loop_seq` | 1000 | 144,444,619 | 22,812,710 | −84.2% |
| `external_stash_profile` | `loop_seq` | 2000 | 542,057,986 | 45,082,309 | −91.7% |
| `external_stash_profile` | `fanout` | 500 | 27,729,647 | 6,997,288 | −74.8% |
| `external_stash_profile` | `fanout` | 1000 | 99,353,150 | 13,560,251 | −86.4% |
| `external_stash_profile` | `fanout` | 2000 | 375,130,982 | 26,483,386 | −92.9% |

All three shapes now grow about 1.95 times per doubling of N.

Inside the matcher (the inclusive cost of `match_activity`), at N=1000:

| `match_activity_profile` shape | Before | After | Change |
|---|---|---|---|
| `sequential` | 366,236 | 371,486 | +1.4% |
| `fanout` | 5,498,859 | 5,428,060 | −1.3% |
| `fanout_extsig` | 6,618,848 | 6,097,036 | −7.9% |

A sequential history costs about 5 instructions more for each call. That is
the reset of the memo on an empty stash, behind the box of the external
state. The harness totals drop 3.3% to 5.0%. Most of that drop comes from other inlining of the harness payload
code, not from the matcher:

| `match_activity_profile` shape | Before | After |
|---|---|---|
| `sequential` | 18,980,926 | 18,233,299 |
| `fanout` | 24,566,919 | 23,744,149 |
| `fanout_extsig` | 25,611,380 | 24,338,503 |

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

## Issue #2349: the signal wait

**Outcome: shipped.** At N=2000 the `rev` shape of `signal_wait_profile`
costs 84% fewer instructions, and `after_fanout` costs 93% fewer. Both now
grow about 2 times per doubling of N. `docs/design/DESIGN-2349.md` records
the plan.

### Workload

`benches/signal_wait_profile.rs` replays a history through
`HistoryMatcher::new`, `match_activity` and `match_signal`.
`SIGNAL_PROFILE_SHAPE` picks the shape:

- `fwd`: N signals, each with its own name. The workflow waits in arrival
  order.
- `rev`: the same signals. The workflow waits in reverse order.
- `after_fanout`: N activity schedules, then N signals. The workflow waits
  in arrival order, and no call claims the activities.
- `loop`: N steps of one activity. Each signal arrives before the
  completion of its activity, so the activity scan stashes it. Every
  signal has the name `go`.
- `loop_named`: `loop`, with a name per signal.

### Cause

- A wait found its stashed signal with a linear name compare. Waits in
  reverse order of N stashed signals cost O(N²) compares. The push handler
  claim, the non-blocking receive and the race scan searched the stash the
  same way. The race scan searched it once for each consumed event that it
  crossed.
- Each wait walked from the cursor and crossed every unclaimed sibling
  command. The cursor then went back to the first one. So the next wait
  crossed the same commands again.

### Change

- `SignalStash` keeps the stashed signals in push order. A stash with more
  than 16 slots also keeps a queue of push numbers for each name. A search
  then walks one queue. A smaller stash scans its slots. Either way, a
  search visits the entries of one name in push order, as the old search
  did.
- A stash entry names its own event. So the race scan searches the stash
  only at a consumed signal of the same name.
- `signal_scan_step` holds the rule of the old walk for one event. A plain
  wait leaves the walk by the rule of #2330 and continues on the scan
  index. It visits each unconsumed barrier and runs the same step on it.
  Between two barriers, it looks up the first interleaved command once. It
  skips a deadline timer of its own signal, as the walk does.
- A race branch keeps the walk. It must stop at its own settle marker, and
  the index has no lookup by marker name.

A name map on every stash made the `loop` shape cost 12.6% more. Each stash
paid a SipHash, and a clone of a new name. A stash usually holds one or two
signals, so the map now exists only above 16 slots.

### Measurement

Callgrind, total instructions (`Ir`) of the harness. The totals include the
build of the history. "Before" is `a9048c9`, with the bench of this change.

| Bench | Shape | N | Before | After | Change |
|---|---|---|---|---|---|
| `signal_wait_profile` | `fwd` | 500 | 2,490,395 | 2,498,721 | +0.3% |
| `signal_wait_profile` | `fwd` | 1000 | 4,740,181 | 4,759,007 | +0.4% |
| `signal_wait_profile` | `fwd` | 2000 | 8,983,478 | 9,023,304 | +0.4% |
| `signal_wait_profile` | `rev` | 500 | 7,927,824 | 4,319,340 | −45.5% |
| `signal_wait_profile` | `rev` | 1000 | 25,935,642 | 8,200,405 | −68.4% |
| `signal_wait_profile` | `rev` | 2000 | 91,149,488 | 14,821,110 | −83.7% |
| `signal_wait_profile` | `after_fanout` | 500 | 21,813,577 | 5,695,323 | −73.9% |
| `signal_wait_profile` | `after_fanout` | 1000 | 77,990,601 | 10,879,118 | −86.1% |
| `signal_wait_profile` | `after_fanout` | 2000 | 294,715,202 | 21,284,073 | −92.8% |
| `signal_wait_profile` | `loop` | 500 | 3,830,926 | 3,863,263 | +0.8% |
| `signal_wait_profile` | `loop` | 1000 | 6,741,836 | 6,815,647 | +1.1% |
| `signal_wait_profile` | `loop` | 2000 | 14,018,564 | 14,168,375 | +1.1% |
| `signal_wait_profile` | `loop_named` | 500 | 4,563,883 | 4,596,234 | +0.7% |
| `signal_wait_profile` | `loop_named` | 1000 | 8,200,672 | 8,274,497 | +0.9% |
| `signal_wait_profile` | `loop_named` | 2000 | 16,866,405 | 17,016,230 | +0.9% |
| `match_activity_profile` | `sequential` | 1000 | 18,221,261 | 18,190,324 | −0.2% |
| `match_activity_profile` | `fanout` | 1000 | 23,730,128 | 23,698,901 | −0.1% |
| `match_activity_profile` | `fanout_extsig` | 1000 | 24,327,184 | 24,295,015 | −0.1% |

Inside the matcher (the inclusive cost of `match_activity` and
`match_signal`), at N=1000:

| Shape | Before | After | Change |
|---|---|---|---|
| `fwd` | 724,974 | 731,975 | +1.0% |
| `rev` | 21,598,746 | 4,039,961 | −81.3% |
| `after_fanout` | 71,923,616 | 4,578,270 | −93.6% |
| `loop` | 1,489,030 | 1,523,030 | +2.3% |
| `loop_named` | 1,486,093 | 1,520,093 | +2.3% |

A wait with no stash costs about 7 instructions more. A loop step costs
about 34 more, in the stash push and the claim. #2338 and #2346 accepted a
cost of the same size.

### Step counts

The scaling test counts scan steps at N=256 and N=512. It now also counts
each stash entry that a signal search visits.

| Shape | Before | After | Ratio after |
|---|---|---|---|
| `Signals(false)` | 1,024 → 2,048 | 1,024 → 2,048 | 2.00 |
| `Signals(true)` | 32,899 → 131,331 | 514 → 1,026 | 2.00 |
| `SignalsAfterFanOut(false)` | 99,200 → 395,008 | 11,092 → 22,383 | 2.02 |
| `SignalsAfterFanOut(true)` | 33,155 → 131,843 | 770 → 1,538 | 2.00 |
| `SignalHandlers(false)` | 33,409 → 132,353 | 769 → 1,537 | 2.00 |
| `SignalHandlers(true)` | 33,409 → 132,353 | 769 → 1,537 | 2.00 |
| `TryReceiveReversed` | 33,409 → 132,353 | 769 → 1,537 | 2.00 |
| `RaceOverStash` | 67,083 → 265,227 | 1,035 → 2,059 | 1.99 |

### Tests

- `replay_steps_grow_near_linearly_for_every_shape` gains the eight signal
  shapes above. The old code fails it for seven of them.
- `the_indexed_scan_matches_the_oracle` runs copies of the old signal wait,
  push handler claim, non-blocking receive, signal race and pure signal
  reads as its oracle. Its histories now hold deadline timers, their fires
  and a race marker. Each case runs with a scan, with a name map, or with a
  switch between them. The test checks the stash invariant after every
  call.
- `the_signal_stash_matches_a_plain_deque` compares `SignalStash` with a
  plain `VecDeque` after every push, search, remove, cut and restore.
- `the_signal_step_acts_only_on_indexed_events` checks that the signal step
  changes nothing on an event that the scan index does not list.
- Edge tests in `scan_fanout_tests` cover a deadline timer as the rewind
  target, an unexpected event, a predicate, a cut and a race branch, on the
  walk and on the index.
- Seven mutants, each made by hand, fail the tests above:
  - no skip of a deadline timer of the awaited signal,
  - no interleaved lookup on the index,
  - a race branch that leaves the walk,
  - a race search that matches the wrong event,
  - an indexed scan that ignores a cut,
  - a restore with no sort,
  - a non-blocking receive that claims a reserved signal.

### Reproduce

```bash
BIN=$(cargo bench -p autumn-harvest --no-default-features --features testing \
  --bench signal_wait_profile --no-run --message-format=json 2>/dev/null \
  | jq -r 'select(.reason=="compiler-artifact" and .target.name=="signal_wait_profile") | .executable')
for shape in fwd rev after_fanout loop loop_named; do
  for n in 500 1000 2000; do
    SIGNAL_PROFILE_SHAPE=$shape SIGNAL_PROFILE_N=$n \
      valgrind --tool=callgrind --branch-sim=no --cache-sim=no \
      --callgrind-out-file="cg.$shape.$n" "$BIN"
  done
done
```
