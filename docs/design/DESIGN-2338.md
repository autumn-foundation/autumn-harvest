# Design — Issue #2338: the remaining O(N²) matcher scans

Issue #2330 indexed the activity terminal scan of `HistoryMatcher`. Its
review found other scans in `autumn-harvest/src/replay.rs` that still walk
history one event at a time. In a fan-out, a walk per call makes a replay
cost O(N²).

This change moves three scans onto the position index of #2330
(`activity_scan_index.rs`):

1. `scan_ahead_for_external_signal_terminals`, which `prepare_match` runs
   before every `match_*` call while an external signal, cancel or await is
   stashed.
2. The fire scan of `match_timer_strict` (`ctx.timer`).
3. The terminal scan of `match_child_workflow`.

**Replay semantics do not change.** A differential property test compares
each new scan with a copy of the old code. No migration, no new
`WorkflowEvent` variant and no `harvest_events` change.

---

## 0. Planning record

### 0.1 Facts (white hat)

A RED scaling test, `replay_steps_grow_near_linearly_for_every_shape`, counts
scan steps for nine shapes at N=256 and N=512. A ratio near 4 per doubling is
quadratic. A ratio near 2 is linear. The test fails at a ratio of 2.5. "After"
is the GREEN code.

| Shape | Before, N=256 | Before, N=512 | Ratio | After, N=256 | After, N=512 | Ratio |
|---|---|---|---|---|---|---|
| `SignalThenActivities` | 109,850 | 416,410 | 3.79 | 11,436 | 22,956 | 2.01 |
| `OpenAwaitThenActivities` | 109,847 | 416,407 | 3.79 | 11,433 | 22,953 | 2.01 |
| `CancelThenActivities` | 109,849 | 416,409 | 3.79 | 11,435 | 22,955 | 2.01 |
| `Timers` | 66,304 | 263,680 | 3.98 | 11,094 | 22,358 | 2.02 |
| `TimersReversed` | 66,559 | 264,191 | 3.97 | 11,139 | 22,659 | 2.03 |
| `Children` | 66,560 | 264,192 | 3.97 | 10,876 | 21,884 | 2.01 |
| `ChildrenReversed` | 66,560 | 264,192 | 3.97 | 10,903 | 22,167 | 2.03 |
| `LocalActivities` | 1,024 | 2,048 | 2.00 | 1,024 | 2,048 | 2.00 |
| `ExternalSignals` | 769 | 1,537 | 2.00 | 769 | 1,537 | 2.00 |

The issue named five scans. The code and the test give this result:

- **External stash scan.** Quadratic, as the issue measured. The walk runs
  to the end of history on every call while the stash is not empty.
- **Timer fire scan.** Quadratic. The scan steps over the later
  `TimerStarted` events and the consumed fires of earlier timers.
- **Child terminal scan.** Quadratic. The scan steps over every later child
  start. It does not even skip consumed events.
- **Local activity scan.** Linear. The scan stops at the first event of any
  other local activity, and a worker runs a local activity inline. The
  schedule and its terminal events are therefore always adjacent. The issue
  guessed O(N²) from the code. The test refutes it, and stays as a guard.
- **External signal, cancel and await scans.** Linear. Each scan stops at
  the first command event, so it never steps over a sibling of its own kind.
  `prepare_match` drains a fan-out of external signals into the stash at
  the cursor.

### 0.2 Brainstorm — how can these scans become linear?

| # | Idea | Verdict |
|---|------|---------|
| B1 | Skip the external stash walk when every stashed entry has a terminal. | Rejected. It is not exact. The walk also consumes a second terminal of the same signal or cancel id. A skip leaves it unconsumed, and the drift check then reports it. It also does not help an open await. |
| B2 | Keep a watermark of the range that the external walk already covered for each stash state. | Rejected. Cursor rewinds, cut views and stash changes each need a rule. The proof of equality is long, and an index is simpler. |
| B3 | Index the external terminal events in one skip list. Visit only unconsumed terminals at or after the cursor, in history order. | **Adopted.** It does the same work as the walk on the events that can change state. Every other event is a no-op for this scan. |
| B4 | Give the timer scan its own walk and index. | Rejected. Its event roles are the same as the activity scan roles (§1.2). A second copy can drift from the first. |
| B5 | Run the timer scan through the activity scan code, with a scan target. | **Adopted.** One walk and one index serve both. Only the stop differs. |
| B6 | Index the child terminals per child id. | **Adopted.** The old scan is a plain search for the first terminal of one child, so a per-id list answers it exactly. |
| B7 | Rewrite the race scans (`match_signal_or_timer`, `match_child_or_timer`, `match_timer_or_cancel`) too. | Deferred. Issue #2338 does not name them, and their logic is more complex. See §4. |
| B8 | A cheaper hasher for the index maps. | Deferred. #2330 records it as the next lever. It needs its own argument about adversarial ids. |

### 0.3 Reverse brainstorm — how can this change do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | The indexed external scan resolves a terminal that the walk did not, or the reverse. | The index visits the same unconsumed terminal positions, in the same order, with the same per-event code. The differential test runs the old walk as the oracle. |
| R2 | A duplicate terminal of one signal id is left unconsumed. | The index holds every terminal position. It does not stop at the first per id. Test: `a_second_terminal_of_one_signal_is_consumed`. |
| R3 | A sequential history now pays for an index build. | The ski-rental rule of #2330 applies to all scans. A walk moves to the index only after the walks cost more than a build. Bench `sequential` checks it. |
| R4 | The timer scan now stops where the old one did not, or the reverse. | §1.2 lists the old timer rules next to the activity roles. The differential test drives timer calls on both paths. |
| R5 | The child scan now skips a consumed terminal. | The old scan matched a consumed terminal. The index keeps every child terminal, and the query does not filter consumed positions. |
| R6 | A cut view lets a scan see a hidden position. | Every query drops a position at or past the visible end, as #2330 does. The differential test cuts and restores views. |
| R7 | A skip link goes stale. | A consumed event never becomes unconsumed. The code has no call that removes an index from a consumed set. `formal/verus/src/scan_skip.rs` proves the links sound under that rule. |
| R8 | The new roles change the activity scan. | A timer fire, a child terminal and an external terminal keep their old effect on an activity scan: a step, a step and a stash. The #2330 oracle test still runs. |

### 0.4 Six thinking hats

- **White (facts).** §0.1. Three scans are quadratic and two are linear.
- **Red (gut).** The external stash walk is the worst: one stashed signal
  makes every later call pay for the whole rest of the history.
- **Black (risks).** Replay determinism. A wrong scan replays a run
  differently, so the oracle test must cover every event class and every
  call. Index memory grows with history size.
- **Yellow (benefits).** A `join!` of an external call and a fan-out, a
  timer fan-out and a child fan-out all become linear. The shared index is
  built at most once per matcher.
- **Green (alternatives).** §0.2. A per-id external index also works, but
  it walks the stash on each call, and a stash can hold N entries.
- **Blue (process).** RED: the scaling test fails for seven shapes. GREEN:
  move the three scans. REFACTOR: share the walk code and the roles. Then
  measure with callgrind, and review from several angles.

---

## 1. Design

### 1.1 The external stash scan

The old walk visits each unconsumed event from the cursor to the end. Only a
signal, cancel or await terminal can change state. The index keeps the
positions of those terminals in one skip list. The new scan asks for the
first unconsumed terminal at or after the cursor, resolves it with the old
per-event code, and continues after it. The scan consumes a resolved
terminal, so the skip links pass over it on later calls.

The scan uses the walk until the ski-rental rule of #2330 says that the
index pays off. A small history keeps its old cost.

### 1.2 The timer fire scan

The old timer scan and the activity scan treat every event class the same
way:

| Event | Old timer scan | Activity role |
|---|---|---|
| Own `TimerFired` | Stop | `Fired(id)`: stop for that timer |
| Child start, detached spawn, timer arm or cancel, activity or local activity schedule, marker, side effect | Interleaved | `Scheduled` or `Interleaved` |
| Signal and external events | Stash | `Barrier` (transparent) or `ExternalTerminal` |
| Update events, activity progress and terminals, local activity terminals, child terminals, foreign fires | Step | `Progress`, `Terminal`, `ChildTerminal`, `Fired` or `Pass` |
| Any other event | Stop with no match | `Barrier` |

So the timer scan runs through the activity scan code with a scan target.
The target picks the stop: the activity schedule and terminal, or the fire
of the timer. The index keeps the fire positions of each timer.

### 1.3 The child terminal scan

The old scan returns the first terminal of the child after its start, and
it does not skip consumed events. The index keeps the terminal positions of
each child. The query returns the first one at or after the start, with no
consumed filter.

### 1.4 New roles

| Role | Events | Effect on an activity scan |
|---|---|---|
| `Fired(id)` | `TimerFired` | A step, as `Pass` was. |
| `ChildTerminal(id)` | `ChildWorkflowCompleted`, `ChildWorkflowFailed` | A step, as `Pass` was. |
| `ExternalTerminal` | The six external terminal events | A stash, as a transparent `Barrier` was. |

### 1.5 No move of the index

A scan used to move the index out of the matcher with `std::mem::take`, so it
could read the consumed sets while it held the index. The move copied the
index twice and built an empty one with a `RandomState` for each hash map.
This change adds two maps to the index, so the move cost more. A split borrow,
`index_and_consumed`, now gives the index and the consumed sets apart. It
costs nothing.

---

## 2. Acceptance criteria

- `replay_steps_grow_near_linearly_for_every_shape` passes for all nine
  shapes. It fails on the old code for seven of them: every shape of the
  three quadratic scans.
- `the_indexed_scan_matches_the_oracle` compares the old and new external,
  timer and child scans, with every scan forced onto the index or not.
- The #2330 scaling and edge tests pass unchanged.
- Callgrind shows a linear cost for the issue shape, `fanout_extsig`.

## 3. Measurement

See `docs/performance-match-activity-fanout.md`, section "Issue #2338".

## 4. Out of scope

- The race scans (§0.2 B7).
- A cheaper hasher for the index (§0.2 B8).
- The external scan still visits an external terminal that matches no stash
  entry on each call. A shape such as `join!(await_external_workflow, a loop
  of signal_external)` then costs O(K²) visits, fewer than the old walk.
  Issue #2346 fixes it. See `docs/design/DESIGN-2346.md`.
- A timer id that repeats in one fan-out. The worker records one
  `TimerStarted` for a live timer id, so this shape does not replay. The fire
  list of each timer is a skip list anyway, so a reused id costs no extra
  probes.
