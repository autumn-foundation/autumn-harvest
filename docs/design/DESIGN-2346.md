# Design — Issue #2346: the external stash scan revisits unmatched terminals

Issue #2338 moved `scan_ahead_for_external_signal_terminals` onto the scan
index. The scan visits only the unconsumed external terminals at or after the
cursor. A terminal that matches no stash entry stays unconsumed, so each
`prepare_match` visits it again while the stash is not empty.

This change makes the scan skip a terminal that an earlier scan left
unmatched. The scan reads it again only for a new stash entry of its key. A
stash keyed by id also lets a terminal find its entry in O(1) in a fan-out,
and a remove from the front costs O(1).

**Replay semantics do not change.** The differential property test of #2330
and #2338 compares the new scan with a copy of the old walk. No migration, no
new `WorkflowEvent` variant and no `harvest_events` change.

---

## 0. Planning record

### 0.1 Facts (white hat)

These facts describe the code before this change.

A RED scaling test counts scan steps at N=256 and N=512. A ratio near 4 per
doubling is quadratic. The test fails at a ratio of 2.5. §3 gives the counts.

`ExternalLoop(first, body)` runs a loop of an activity and a `body` call. A
`first` call comes before the loop. It ends last, and the workflow matches it
last. `ExternalFanOut(call, past_command)` stashes P calls of one kind, then
their terminals. With `past_command`, an activity comes between them.
`ExternalPairs` is the literal shape of the issue: an await, then signal
pairs, then the await terminal.

The code and the tests give these facts:

- **A command between the external calls makes the scan quadratic.** The
  drain at the cursor stops at each command. A later request stays out of the
  stash, so its terminal matches nothing. Each call visits every later
  terminal again.
- **A long-lived stash entry is not necessary.** With no `first` call, the
  drain still stashes each body request just before the scan of its own call.
  That scan visits every later terminal. So a plain loop of an activity and an
  external signal is quadratic.
- **The literal shape of the issue gives the scan no work.** The drain
  stashes every event at the first call. But each terminal then finds its
  entry with a linear search, after the earlier entries. So that shape costs
  O(P²) compares. A plain loop of external signals and a fan-out cost the same.
  The first scaling test did not count those compares. Review found them.
- **The drift check searches the stash once for each event.**
  `first_unconsumed_command_event` then costs O(N·P) at the end of a cycle.
- **The three matchers removed an entry with `Vec::remove`.** A fan-out
  matches its entries in stash order, so each remove shifted the rest of the
  stash. At P=2000 that `memmove` was 90% of the `fanout` replay.
- **The scan changes state at a terminal only when its key has an eligible
  stash entry.** A key is the kind and the id of the call. A signal or cancel
  entry is always eligible. An await entry is eligible while it is open.
- **The stash changes in four ways only.** A push adds an entry at the end. A
  remove takes one out. A terminal goes from `None` to `Some`, or from `Some`
  to `Some`. No code sets a terminal back to `None` or reorders the stash.
- **The consumed sets only grow.** No code removes an index from them.
- **No production path moves the cursor below the start of the last scan.**
  A settle rewinds to the first interleaved command, which is after the scan
  start. A test can set the cursor by hand, so the scan still handles a
  rewind.
- **The forward scans of the three external matchers cannot run.** After
  `prepare_match`, the drain has stashed every external event at the cursor.
  So the event at the cursor is never an external request, and each matcher
  returns `Diverged` before its forward scan.

### 0.2 Brainstorm — how can the external stash stop costing O(N²)?

| # | Idea | Verdict |
|---|------|---------|
| B1 | Note the key of each new stash entry. A scan resolves the terminals of the new keys through a per-key index. It reads every terminal only outside the range of the last scan with work. | **Adopted.** Only a new entry can make a terminal match. §1 gives the argument. |
| B2 | Keep a memo of the stash state and the scanned range. Scan again when either one changes. | Rejected alone. The stash changes at each loop step, so each step does a full pass. With "scan only the new entries" it becomes B1. |
| B3 | For each stash entry, look up the terminals of its key. | Rejected. #2338 rejected it too. A stash can hold N entries, so each call costs O(N). |
| B4 | Park an unmatched terminal: drop it from the shared skip list, and keep it per key. | Rejected. A skip list needs a dead entry to stay dead. A parked terminal comes back when its request arrives, or after a rewind. That is B1 with more state. |
| B5 | Resolve the terminals of a request when the code stashes it. | Rejected. The old scan resolves at the next `prepare_match`, from that cursor. An eager pass would use another range. |
| B6 | Use a `VecDeque` for the three stashes. | **Adopted**, inside B7. An in-order removal then costs O(1). |
| B7 | Key the stash by id. A map holds the first entry and the entry count of each id. | **Adopted.** A terminal finds its entry in O(1) in a fan-out, and in O(log n) after a remove in the middle. A repeated id walks every entry after its first entry, as the old search did. |
| B8 | Give the external scan its own index of the external terminals, with no walk. | **Adopted.** The scan had to walk the rest of the history until the activity index paid off, then build all of it. A build that tests each event once costs less. |
| B9 | A cheaper hasher for the id maps. | Deferred. #2338 deferred it too. It needs its own argument about adversarial ids. |
| B10 | Delete the forward scans that cannot run (§0.1). | Deferred. The code is dead, but it predates this change. A separate change can remove it. |
| B11 | Check the old walk against the new scan after every scan in every test. | Rejected. Each check needs a copy of the matcher. That makes every replay test cost O(N²), and it skews the step counter. The differential test and `Op::Prepare` cover the same paths. |

### 0.3 Reverse brainstorm — how can this change do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | A push site does not note its key. The scan then skips a terminal that the old walk resolved. | Every push goes through one of three request helpers, and each helper notes its key. The stash fields say so. `match_external_cancel` and `match_external_await` had their own copies of the stash code. They cannot run (§0.1), but they now call `stash_transparent_external_event` too. |
| R2 | The cursor goes back below the covered range. A terminal in the gap is skipped. | The scan does a full pass over the part of `[cursor, len)` below the covered range. `Op::Rewind` and `Op::Skip` in the differential test, and two edge tests, reach it. |
| R3 | A restore brings back events past the covered range. | The scan does a full pass over the part above the covered range. The differential test cuts and restores. An append moves the cursor to the end, so it adds no range. |
| R4 | The scan resolves two terminals of one key in the wrong order. A signal keeps the terminal of the last resolve, so the result changes. | The scan runs its three ranges in position order. Each range runs in position order. Two keys never touch the same entry or event. Two edge tests split one key over two ranges. |
| R5 | An await entry closes, and a second open entry with the same id then matches. | After a pass over a key, no unconsumed terminal of it is left in range, or no open entry of it exists. A close never opens an entry. Two edge tests stash several awaits with one id. |
| R6 | An empty stash resets the covered range to every position. | With no entry, no terminal can match. The next push notes its key, so the next scan sees it. |
| R7 | A history with no stashed call pays for the new state. | The empty-stash path adds one reset. The `sequential` shape of `match_activity_profile` measures it. |
| R8 | The keyed stash gives another entry than a plain search, or loses the order. | The differential test cannot see this, because both sides share the stash. A proptest compares the stash with a plain `VecDeque` search after every call. |
| R9 | A role function marks an external terminal that `ExternalKey::of_terminal` maps to no key. The external index then misses it. | A test checks that the two agree on every event that the differential test generates. A table test checks each key. |

### 0.4 Six thinking hats

- **White (facts).** §0.1. Four loop shapes are quadratic in the scan. The
  fan-out shapes and the literal issue shape are quadratic in the stash
  search. The drift check and the stash removal are quadratic too.
- **Red (gut).** A plain loop of an activity and an external signal is a
  common workflow. So is a sequential loop of external signals. Both were
  quadratic. The issue undersold the problem. The fix must cost little on the
  paths with no external call.
- **Black (risks).** Replay determinism. The new scan skips work by an
  invariant, not by a walk. A push site that forgets its note breaks the
  invariant in silence. The scan must check the covered range against every
  cursor move and view change. The keyed stash must give the same entry as a
  plain search.
- **Yellow (benefits).** With no new stash entry, rewind or restore, the
  external scan returns at once. After the stash empties, the next scan reads
  only the new keys. A terminal finds its entry in O(1) in a fan-out.
- **Green (alternatives).** §0.2.
- **Blue (process).** RED: four loop shapes fail the scaling test. GREEN: the
  memo and a per-key index. REFACTOR: one stash path, a `VecDeque`, a small
  scan role. Review from four angles found the stash search. RED: the scaling
  test counts stash probes, and six fan-out shapes fail. GREEN: the keyed
  stash. REFACTOR: an external index with no walk, more edge tests and a
  stash proptest. Then measure with callgrind, and review again.

---

## 1. Design

### 1.1 The invariant

Call a key *clean* when it is not in `new_keys`. From one scan with work to
the next, this holds:

> No unconsumed external terminal of a clean key inside `covered` matches an
> eligible stash entry.

Each event between two scans keeps it:

- A push notes its key, so the key is not clean.
- A remove, or a new terminal on an entry, can only make an entry ineligible.
- A consumed index stays consumed.
- A cursor move and a view change do not change `covered`. The scan compares
  `covered` with `[cursor, len)` at its start.

So if `[cursor, len)` is inside `covered` and `new_keys` is empty, the old
walk has no work, and the scan returns at once.

### 1.2 The scan

The scan splits `[cursor, len)` at the edges of `covered`:

1. A full pass below `covered`. Only a cursor rewind below `covered` makes
   this range.
2. A per-key pass inside `covered`, for each new key.
3. A full pass above `covered`. Only a restore makes this range.

A full pass is the scan of #2338: each unconsumed external terminal, in
history order. A per-key pass reads the terminals of one key, in history
order. The three ranges are in position order, so the terminals of one key
resolve in the old order. The terminals of two keys never touch the same
entry, so their relative order does not matter.

After a scan with work, `covered` is `[cursor, len)` and `new_keys` is empty.
An empty stash sets `covered` to every position.

### 1.3 Why a pass leaves its key clean

A signal or cancel entry stays eligible during a pass. So a pass consumes
every terminal of its key in its range, or none when no entry exists. An await
terminal matches the first open entry. A pass only closes entries. So a
terminal that matched nothing still matches nothing at the end of the pass.

### 1.4 The keyed stash

`ExternalStash` keeps its entries in a `VecDeque`, in push order, each with a
push number. A map holds the first entry and the entry count of each id.

- A search for the first entry of an id guesses its position from the push
  numbers. While every remove takes the front entry, the guess is right. A
  remove anywhere else leaves a gap, and a binary search then finds it.
- A repeated id walks every entry after the first entry of that id. A
  workflow gives each call a new id, so only an unusual history pays this.
  The old search walked the stash from the front for every id.
- A remove keeps the order of the other entries. A remove from the front
  costs O(1).

### 1.5 The external index

`ExternalTerminalIndex` holds every external terminal in one skip list, and
the terminals of each key. The first scan with work builds it, with one cheap
test for each event. A key with one terminal stores it with no allocation.
The external scan no longer walks, so the walk rule of #2330 governs only the
activity, timer and child scans.

### 1.6 One box for the external state

`ExternalCalls` holds the three stashes, the memo and the external index
behind one `Box`. A `WorkflowContext` holds its matcher inline, and some
workflow futures hold a context. Inline, the new state made 29 agent test
futures pass the `clippy::large_futures` limit of 16,384 bytes. With the box,
`HistoryMatcher` is 736 bytes, smaller than before this change. A matcher
pays one allocation when it starts.

---

## 2. Acceptance criteria

- `replay_steps_grow_near_linearly_for_every_shape` passes for every shape.
  The first RED failed it for the four `ExternalLoop` shapes. The second RED
  failed it for the six `ExternalFanOut` shapes and `ExternalPairs`. The test
  also guards the drift check and the binary search of the keyed stash.
- `the_indexed_scan_matches_the_oracle` passes. Its oracle is the old walk.
- `the_stash_matches_a_plain_search` passes. Its model is a plain `VecDeque`.
- Edge tests cover a rewind below `covered`, the order of one key across a
  restore and across a rewind, each terminal kind in a per-key pass, several
  awaits with one id, and a cut.
- Callgrind shows a linear cost for the `loop`, `loop_seq` and `fanout` shapes
  of `external_stash_profile`.

## 3. Measurement

See `docs/performance-match-activity-fanout.md`, section "Issue #2346". It
gives the step counts of both RED tests and the callgrind figures.

## 4. Out of scope

- The cheap hasher (§0.2 B9) and the dead forward scans (§0.2 B10).
- `try_stash` finds an entry by its target with a linear search. A fan-out
  matches in stash order, so the search ends at the first entry. A workflow
  that matches its calls in another order pays O(P) for each match.
- A scan with new keys inside a cut view narrows `covered` to the cut. After
  the restore, the next scan with work does a full pass over the restored
  tail. A cancelled scope cuts on each poll of its body. The cut itself costs
  O(tail) for each poll, so the pass adds a constant factor only.
- The `match_activity_profile` and `timer_child_fanout_profile` benches say
  that they build the history outside the measured loop. Their totals include
  the build. A separate change can fix that text.
