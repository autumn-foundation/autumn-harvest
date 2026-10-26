# Design — Issue #2346: the external stash scan revisits unmatched terminals

Issue #2338 moved `scan_ahead_for_external_signal_terminals` onto the scan
index. The scan visits only the unconsumed external terminals at or after the
cursor. A terminal that matches no stash entry stays unconsumed, so each
`prepare_match` visits it again while the stash is not empty.

This change makes the scan visit a terminal only when the visit can change
the stash. It also makes the in-order removal from the stash O(1).

**Replay semantics do not change.** The differential property test of #2330
and #2338 compares the new scan with a copy of the old walk. No migration, no
new `WorkflowEvent` variant and no `harvest_events` change.

---

## 0. Planning record

### 0.1 Facts (white hat)

A RED scaling test counts scan steps at N=256 and N=512. A ratio near 4 per
doubling is quadratic. The test fails at a ratio of 2.5. "After" is the GREEN
code.

| Shape | Before, N=256 | Before, N=512 | Ratio | After, N=256 | After, N=512 | Ratio |
|---|---|---|---|---|---|---|
| `ExternalLoop(None, Signal)` | 48,309 | 162,613 | 3.37 | see §3 | see §3 | see §3 |
| `ExternalLoop(Some(Await), Signal)` | 81,471 | 294,207 | 3.61 | see §3 | see §3 | see §3 |
| `ExternalLoop(Some(Signal), Await)` | 81,471 | 294,207 | 3.61 | see §3 | see §3 | see §3 |
| `ExternalLoop(Some(Await), Cancel)` | 81,471 | 294,207 | 3.61 | see §3 | see §3 | see §3 |
| `ExternalPairs` | 772 | 1,540 | 1.99 | see §3 | see §3 | see §3 |

`ExternalLoop(first, body)` runs a loop of an activity and a `body` call. A
`first` call comes before the loop. It ends last, and the workflow matches it
last.

The code and the test give these facts:

- **The literal shape of the issue is linear.** Its history holds external
  events only. The drain at the first call stashes every event, so the scan
  has no work. `ExternalPairs` is that shape.
- **A command between the pairs makes it quadratic.** The drain then stops at
  each command. A later request stays out of the stash, so its terminal
  matches nothing. Each call visits every later terminal again.
- **An open entry is not necessary.** With no `first` call, the drain still
  stashes each body request just before the scan of its own call. That scan
  visits every later terminal. So a plain loop of an activity and an external
  signal is quadratic too.
- **The scan changes state at a terminal only when its key has an eligible
  stash entry.** A key is the kind and the id of the call. A signal or cancel
  entry is always eligible. An await entry is eligible while it is open.
- **The stash changes in four ways only.** A push adds an entry at the end. A
  `remove` takes one out. A terminal goes from `None` to `Some`, or from
  `Some` to `Some`. No code sets a terminal back to `None` or reorders the
  stash.
- **The consumed sets only grow.** No code removes an index from them.
- **No production path moves the cursor below the start of the last scan.**
  A settle rewinds to the first interleaved command, which is after the scan
  start. A test can set the cursor by hand, so the scan still handles a
  rewind.
- **`try_stash` uses `Vec::position` and `Vec::remove`.** A P-wide `join_all`
  of external calls removes the first entry P times. Each remove shifts the
  rest of the stash, so the replay moves O(P²) bytes.

### 0.2 Brainstorm — how can the scan stop revisiting unmatched terminals?

| # | Idea | Verdict |
|---|------|---------|
| B1 | Index the external terminals per key. Note the key of each new stash entry. A scan resolves the terminals of the new keys only. It does a full pass only over a range that no earlier scan covered. | **Adopted.** Only a new entry can make a terminal match. §1 gives the argument. |
| B2 | Keep a memo of the stash state and the scanned range. Scan again when either one changes. | Rejected alone. The stash changes at each loop step, so each step does a full pass. With "scan only the new entries" it becomes B1. |
| B3 | For each stash entry, look up the terminals of its key. | Rejected. #2338 rejected it too. A stash can hold N entries, so each call costs O(N). |
| B4 | Park an unmatched terminal: drop it from the shared skip list, and keep it per key. | Rejected. A skip list needs a dead entry to stay dead. A parked terminal comes back when its request arrives, or after a rewind. That is B1 with more state. |
| B5 | Resolve the terminals of a request when the code stashes it. | Rejected. The old scan resolves at the next `prepare_match`, from that cursor. An eager pass would use another range. |
| B6 | Use a `VecDeque` for the three stashes. | **Adopted.** An in-order removal then costs O(1). The order and the remove rule stay the same. |
| B7 | Key the stash by id, so the `find` of a resolve costs O(1). | Deferred. Each resolve still runs a linear `find`, as before. §4 records it. |

### 0.3 Reverse brainstorm — how can this change do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | A push site does not note its key. The scan then skips a terminal that the old walk resolved. | Every push goes through one of three helpers, and each helper notes the key. The four inline pushes in `match_external_cancel` and `match_external_await` now call the helpers. The differential test runs those scans. |
| R2 | The cursor goes back below the covered range. A terminal in the gap is skipped. | The scan does a full pass over the part of `[cursor, len)` below the covered range. A unit test rewinds the cursor by hand. |
| R3 | A restore or an append brings back events past the covered range. | The scan does a full pass over the part above the covered range. The differential test cuts, restores and appends. |
| R4 | The scan resolves two terminals of one key in the wrong order. A signal keeps the terminal of the last resolve, so the result changes. | The scan runs its three ranges in position order. Each range runs in position order. Two keys never touch the same entry or event. |
| R5 | An await entry closes, and a second open entry with the same id then matches. | After a pass over a key, no unconsumed terminal of it is left in range, or no open entry of it exists. A close never opens an entry. A unit test stashes two awaits with one id. |
| R6 | An empty stash resets the covered range to every position. | With no entry, no terminal can match. The next push notes its key, so the next scan sees it. |
| R7 | A history with no stashed call pays for the new state. | The empty-stash path adds one reset. The walk rule of #2330 still applies. The `loop_seq` and `sequential` benches measure it. |
| R8 | The `VecDeque` change alters what a match returns. | `VecDeque::remove` keeps the order of the other entries, as `Vec::remove` does. The differential test compares every stash after every call. |

### 0.4 Six thinking hats

- **White (facts).** §0.1. Four loop shapes are quadratic, and the literal
  issue shape is linear. `try_stash` moves O(P²) bytes in a P-wide fan-out.
- **Red (gut).** A plain loop of an activity and an external signal is a
  common workflow, and it is quadratic today. The issue undersells the
  problem. The fix must cost nothing on the paths with no external call.
- **Black (risks).** Replay determinism. The new scan skips work by an
  invariant, not by a walk. A push site that forgets its note breaks the
  invariant in silence. The covered range must follow every cursor move and
  view change.
- **Yellow (benefits).** A call with no new stash entry costs O(1), whatever
  the stash holds. The index of #2338 grows by one map. Once the stash
  empties, no full pass runs again.
- **Green (alternatives).** §0.2.
- **Blue (process).** RED: four loop shapes fail the scaling test. GREEN:
  the per-key index, the new keys and the covered range. REFACTOR: route the
  inline pushes through the helpers, move the stash to `VecDeque`, add edge
  tests. Then measure with callgrind, and review from several angles.

---

## 1. Design

### 1.1 The invariant

Call a key *clean* when it is not in `new_keys`. After each scan this holds:

> No unconsumed external terminal of a clean key inside `covered` matches an
> eligible stash entry.

Each event between two scans keeps it:

- A push notes its key, so the key is not clean.
- A `remove` and a terminal change can only make an entry ineligible.
- A consumed index stays consumed.
- A cursor move and a view change do not change `covered`.

So if `[cursor, len)` is inside `covered` and `new_keys` is empty, the old
walk has no work, and the scan returns at once.

### 1.2 The scan

The scan splits `[cursor, len)` at the edges of `covered`:

1. A full pass below `covered`. Only a rewind or a cut below `covered`
   makes this range.
2. A per-key pass inside `covered`, for each new key.
3. A full pass above `covered`. Only a restore or an append makes this range.

A full pass is the scan of #2338: each unconsumed external terminal, in
history order. A per-key pass reads the terminals of one key, in history
order. The three ranges are in position order, so the terminals of one key
resolve in the old order. The terminals of two keys never touch the same
entry, so their relative order does not matter.

After the scan, `covered` is `[cursor, len)` and `new_keys` is empty. An
empty stash sets `covered` to every position.

The walk rule of #2330 still applies. Until the index pays off, a scan with
work walks `[cursor, len)` as the old code did.

### 1.3 Why a pass leaves its key clean

A signal or cancel entry stays eligible during a pass. So a pass consumes
every terminal of its key, or none when no entry exists. An await terminal
matches the first open entry. A pass only closes entries. So a terminal that
matched nothing still matches nothing at the end of the pass.

### 1.4 The stash type

The three stashes become `VecDeque`. `push` becomes `push_back`. `remove`
returns an `Option`, which `try_stash` already returns. An in-order removal is
then O(1).

---

## 2. Acceptance criteria

- `replay_steps_grow_near_linearly_for_every_shape` passes for all fourteen
  shapes. It fails on the old code for the four `ExternalLoop` shapes.
- `the_indexed_scan_matches_the_oracle` passes. Its oracle is the old walk.
- Edge tests cover a rewind below `covered`, two awaits with one id, a
  second terminal of one signal, a cut and a restore.
- Callgrind shows a linear cost for the `loop`, `loop_seq` and `fanout`
  shapes of `external_stash_profile`.

## 3. Measurement

See `docs/performance-match-activity-fanout.md`, section "Issue #2346".

## 4. Out of scope

- A resolve still runs a linear `find` over its stash (§0.2 B7). A scan that
  resolves P terminals against P entries then costs O(P²) compares. The old
  walk costs the same.
- The race scans and the hasher of the index stay as #2338 left them.
