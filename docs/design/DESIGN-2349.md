# Design — Issue #2349: the signal wait is quadratic

`HistoryMatcher::match_signal` has two quadratic paths:

1. A wait finds a stashed signal with a linear name compare. Waits in
   reverse order of N stashed signals then cost O(N²) compares.
2. Each wait walks from the cursor. It crosses every unclaimed sibling
   command, and the cursor goes back to the first one. N waits beside N
   unclaimed commands then cost O(N²) steps.

This change gives a large signal stash a queue per name, and moves the
signal scan onto the scan index of #2330.

**Replay semantics do not change.** A differential property test compares
the new code with a copy of the old code. No migration, no new
`WorkflowEvent` variant and no `harvest_events` change.

---

## 0. Planning record

### 0.1 Facts (white hat)

These facts describe the code before this change.

- **The harness of the issue reproduces on `trunk-dev`.** Callgrind, total
  `Ir` of `benches/signal_wait_profile.rs`:

  | Shape | n=250 | n=500 | n=1000 | n=2000 |
  |---|---|---|---|---|
  | `fwd` | 1,506,221 | 2,484,390 | 4,730,678 | 8,966,973 |
  | `rev` | 3,005,333 | 7,921,819 | 25,926,139 | 91,132,983 |
  | `after_fanout` | 6,396,744 | 21,799,574 | 77,965,084 | 294,666,685 |

- **The stash has 15 use sites.** `pending_signals` is a
  `VecDeque<(String, Value, usize)>`. Six sites search it by name with a
  linear scan: the wait, `claim_pending_signal`, `try_claim_pending_signal`,
  `has_unconsumed_signal` and the two candidate lookups of a predicate wait.
  `match_signal_or_timer` searches it by name twice. One of those searches
  runs once for each consumed event that the race scan crosses.
- **The stash keeps push order.** Each search takes the first entry in that
  order. `restore_view` sorts the stash by event index. In production, push
  order and index order agree. A test can break that with `Op::Skip`.
- **Every stashed entry names its own event.** `stash_signal` takes the name
  and the index of one `SignalReceived` event.
- **The signal scan has one rule per event class.** The comments in
  `match_signal_inner` say that its classes mirror `activity_scan_role`. A
  check of each arm confirms it, with two exceptions. A reserved deadline
  timer of the same signal name is a step, not an interleaved command. The
  settle marker of a race branch stops the scan.
- **The scan index has what the signal scan needs.** Its barrier list holds
  every signal, external event and unexpected event. Its interleaved list
  holds every command that the signal scan crosses. Every other event is a
  step for both scans.
- **The consumed sets only grow.** A skip list link never goes stale.
- **A stash is usually small.** The drain and the scans stash a signal
  just before its wait takes it. A loop of an activity and a signal holds
  one entry at a time.

### 0.2 Brainstorm — how can the signal wait stop costing O(N²)?

| # | Idea | Verdict |
|---|------|---------|
| B1 | Keep the stash in push order. Add a queue of push numbers per name. A removed entry leaves an empty slot until an end of the deque passes it. | **Adopted.** A wait finds its entry in O(1). Every search keeps its old order. |
| B2 | Key the stash by event index in a `BTreeMap`, with a set of indices per name. | Rejected. It changes the order when push order and index order differ. Each step also costs more than a deque. |
| B3 | Reuse `ExternalStash` of #2346 with the name as the id. | Rejected. Its id must be `Copy`. A repeated id walks every later entry, and a repeated name is the usual case for signals. |
| B4 | Run the signal scan through the activity scan code, with a new `ScanTarget`. | Rejected. The signal scan stashes or stops at each barrier, and its rules depend on the name and the predicate. A target cannot express them. |
| B5 | Move the per-event rules of the walk into one step function. The walk calls it for each event. On the index, the scan calls it for each barrier only. | **Adopted.** Both paths share one copy of the rules. |
| B6 | Keep a memo of the range that the last wait crossed, and skip it. | Rejected. The memo depends on the name, the predicate and the consumed state. The index already handles all three. |
| B7 | Use the index for a race branch too. | Deferred. The scan must stop at the settle marker of its own race. The index has no lookup by marker name. A race branch keeps the walk. |
| B8 | Use the index for the candidate lookups of a predicate wait. | Deferred. Those lookups are pure reads with `&self`. A skip list query must shorten its links. |
| B9 | Check the event in front of the stash search of the race scan. | **Adopted.** A stashed entry names its own event. Only a consumed signal of the same name needs a search. |
| B10 | Prove the stash invariant in Verus. | Rejected. Verus here proves integer kernels against a mirrored copy. The stash is a map of queues. A proptest model and an invariant check cover it. The reused skip list is proven already. |
| B11 | Build the name map only for a stash with more than 16 slots. A smaller stash scans its slots. | **Adopted** after the first measurement. A map on every stash made the `loop` shape cost 12.6% more. §1.1 gives the numbers. |
| B12 | Keep the old `VecDeque` for a small stash, and switch to the slots at the limit. | Rejected. Two layouts double every search and every remove. B11 gives the same cost with one layout. |
| B13 | A cheaper hasher for the name map. | Deferred. #2338 and #2346 deferred it too. A signal name comes from a client, so it needs its own argument about adversarial names. |

### 0.3 Reverse brainstorm — how can this change do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | A name queue and the slots disagree. A wait then takes the wrong entry, or none. | One type owns both. A test check asserts the invariant after every operation. A proptest compares the stash with a plain `VecDeque` model. |
| R2 | A search takes an entry in another order than the old linear scan. | Each queue holds push numbers in push order. `restore_view` rebuilds the stash in the old sorted order. The differential test runs the old searches as the oracle. |
| R3 | The indexed scan skips an event that the walk acts on. | The scan visits every barrier in order. A test checks that the step function is a no-op for each event class outside the barrier and interleaved lists. |
| R4 | The rewind target changes. A reserved deadline timer of the same name is in the interleaved list, but the walk does not rewind to it. | The interleaved lookup skips that timer and asks again. An edge test runs it on the walk and on the index. |
| R5 | A race branch crosses its settle marker on the index. | A race branch never leaves the walk. An edge test forces every scan onto the index and checks the bound. |
| R6 | The fast paths pay for the new state. `fwd` never stashes a signal. A loop stashes one signal at a time. | An empty stash skips the name lookup. A small stash has no name map (B11). The hot stash calls and the step function are inlined. The `fwd`, `loop` and `loop_named` shapes measure it. |
| R7 | A cut view hides an entry, and the restore puts it back in the wrong place. | The cut takes the hidden entries out in push order. The restore merges and sorts them, as the old code did. The differential test cuts and restores. |
| R8 | The new fields grow `HistoryMatcher` past the `clippy::large_futures` limit. | The stash adds two counters and an optional map, 64 bytes in all. Clippy runs with every feature, and it passes. |
| R9 | A predicate rejects a payload. The indexed scan then stashes in another order. | The scan visits barriers in history order, as the walk does. The differential test drives payload predicates. |

### 0.4 Six thinking hats

- **White (facts).** §0.1. Two separate quadratic paths. The scan index and
  the step counter of #2330 already exist.
- **Red (gut).** One approval per item is a common workflow shape. Waits in
  any order are normal. Waits beside a large fan-out are normal too. The fix
  must not slow the in-order wait.
- **Black (risks).** Replay determinism. The stash order decides which
  payload a wait gets. The rewind target decides which command the cursor
  meets next. The drift guard needs every crossed command to stay
  unconsumed.
- **Yellow (benefits).** A wait finds a stashed signal in O(1). A wait beside
  N unclaimed commands costs O(1) amortized once the index exists. The push
  handler claims and `try_receive_signal` get the name queue too.
- **Green (alternatives).** §0.2.
- **Blue (process).** RED: the scaling test gains signal shapes, and counts
  stash probes in the old searches. The differential test gains the old
  signal code as its oracle. GREEN: the name queues, then the indexed scan.
  Measure: a map on every stash costs too much in a loop, so the map comes
  only above 16 slots. REFACTOR: one step function, inlined hot calls, docs,
  callgrind. Then review from several angles.

---

## 1. Design

### 1.1 The signal stash

`SignalStash` holds the stashed signals:

- `slots` holds the entries in push order. A remove at an end pops the
  slot. A remove in the middle leaves `None`, and the deque drops it when it
  reaches an end.
- `front` is the push number of the first slot. The slot of push number `p`
  is `p - front`.
- `names` maps each name to the push numbers of its entries, in push order.
  It exists only while the stash has more than 16 slots. The first push past
  that limit builds it. The stash drops it when it empties. An empty queue
  keeps its key.

Without `names`, a search scans at most 16 slots. With `names`, the
invariant is: the queue of each name holds exactly the push numbers of the
live entries with that name, in ascending order. Either way, a search
visits the entries of one name in push order, as the old scan did.

A wait removes the first entry of its name. In a fan-out in order, that is
the front of the queue and of the deque, so it costs O(1). In reverse order,
it is the back of the deque, which costs O(1) too.

The first version kept `names` for every stash. Each stash then paid a
SipHash, and a clone of a new name. Callgrind on the `loop` shape at N=1000,
the inclusive cost of `match_activity` and `match_signal`: 1,489,030
instructions before, and 2,298,520 with a map on every stash. With no map
below 16 slots and the hot calls inlined, it costs 1,523,030. That is 34
instructions, or 2.3%, more for each loop step.

`restore_view` takes every entry out, adds the hidden entries, sorts by event
index and pushes them back. That is the order of the old sort. The cost is
O(n log n), as before.

### 1.2 The indexed signal scan

`signal_scan_step` holds the rules of the old walk for one unconsumed event.
It returns a result or lets the scan continue. The walk calls it for each
event.

A plain wait leaves the walk by the rule of #2330. It needs four walked
events, a crossed interleaved command, and an index that pays off. It then
continues on the index:

1. Find the first unconsumed barrier at or after the scan position.
2. While no interleaved command is known, find the first unconsumed
   interleaved command before that barrier. Skip a reserved deadline timer
   of the same name.
3. Run `signal_scan_step` on the barrier.
4. Continue after the barrier, or return at the end of the history.

Every event outside the two lists is a step for the old walk. So the scan
visits the same events in the same order, and it ends with the same result,
cursor and stash.

A race branch keeps the walk (§0.2 B7).

The walk calls `signal_scan_step` once for each event, so the step is
inlined. Out of line, the call and the move of its `Option<HistoryMatch>`
made the `fwd` shape cost 1.3% more.

## 2. Acceptance criteria

- `replay_steps_grow_near_linearly_for_every_shape` passes for the new
  signal shapes. The old code fails it.
- `the_indexed_scan_matches_the_oracle` passes. Its oracle includes the old
  signal wait, the old push handler claims and the old race search.
- `the_signal_stash_matches_a_plain_deque` passes.
- Edge tests cover a reserved deadline timer, an unexpected event, a
  predicate, a cut and a race branch, on the walk and on the index.
- Callgrind shows a linear cost for the `rev` and `after_fanout` shapes of
  `signal_wait_profile`. The `fwd`, `loop` and `loop_named` shapes cost
  about the same.

## 3. Measurement

See `docs/performance-match-activity-fanout.md`, section "Issue #2349".

## 4. Out of scope

- A race branch scan (§0.2 B7).
- The candidate lookups of a predicate wait (§0.2 B8). `begin_signal_wait`
  walks the recorded events from the cursor for each wait.
- The walk of `match_signal_or_timer` crosses unclaimed sibling commands, as
  the old signal walk did. A race starts at its own `TimerStarted`, so a
  sibling fan-out before it is claimed first.
- The race scan searches the queue of its own name at each consumed signal
  of that name. A stash with many entries of the race name can still cost
  O(k) for each of them.
- The cheap hasher (§0.2 B13).
