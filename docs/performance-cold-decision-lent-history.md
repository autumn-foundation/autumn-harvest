# Cold decision: the worker lends its history to the replay

**Outcome: shipped.** A cold decision of the Postgres worker no longer
copies the history for the replay. A cold decision that ends its run costs
47% fewer instructions. One that stays resident costs 13% fewer.

Every number is an instruction count from `valgrind --tool=callgrind`. It
is deterministic. Wall-clock time is not evidence on a shared machine.

## The cost

A cold decision replays the whole history. The worker needs the history
again after the replay: the persist paths read it, and the warm cache
stores it. The context took the history for good, so the worker gave it a
deep copy (`history_events.clone()`) on each cold decision.

A warm decision makes no such copy. It sends the new events to the
resident workflow (issue #1798). So the copy cost a decision after a cache
miss, a decision whose suspension cannot stay resident, and each decision
of a worker with resident workflows off.

## The change

The worker now lends the history (`WorkflowContext::lend_history`), and
`DriveResult::history` returns it. The SQLite drive got the same change
first. [`performance-sqlite-hot-path.md`](performance-sqlite-hot-path.md),
finding 6, explains the lent history matcher.

A resident workflow adds one case. It keeps its context, so its matcher
still needs its events after the worker takes the history back.
`HistoryMatcher::reclaim_events_keeping_a_copy` gives the worker the whole
history and leaves the matcher a copy. The copy has no output in each
activity completion that the replay matched. An owning matcher holds the
same events at that point, because it moved each matched output out. So
the resident state does not change.

## Workload

`benches/cold_decision_history_profile.rs` builds a history of `N`
completed sequential activities and runs one cold decision over it, as
`resident::start` does. `owned` copies the history first, as the worker
did. `lent` calls `resident::start_lending`. The decision asks for one
more activity and stays resident (`park`), or ends the run (`complete`).
The difference between 6 and 2 repeats, divided by 4, is the cost of one
decision.

## Result

| Activities | The decision | Owned | Lent | Change |
|---:|---|---:|---:|---:|
| 20 | ends the run | 717,421 | 390,948 | −45.5% |
| 100 | ends the run | 3,486,580 | 1,839,398 | −47.2% |
| 400 | ends the run | 13,865,606 | 7,266,827 | −47.6% |
| 20 | stays resident | 746,905 | 659,649 | −11.7% |
| 100 | stays resident | 3,542,013 | 3,094,762 | −12.6% |
| 400 | stays resident | 14,016,636 | 12,233,216 | −12.7% |

A decision that ends the run keeps one copy of each matched output, for
the workflow, and makes no other. A decision that stays resident still
makes one copy of the history, for the matcher, so it gains less.

## What the worker does when the history does not come back

A lent matcher gives the history back only when it holds exactly the
events that it got. A cold drive always does: a view cut of a cancelled
scope is closed when its poll returns, and only a warm resume appends. So
a missing history is a broken invariant. The worker then fails the task
with a dispatch error, and a later attempt loads the history again. It
does not go on with a history that it cannot trust.

## Proof

- `a_lent_matcher_keeps_the_copy_of_an_owning_matcher` compares the events
  that a lent matcher keeps with the events of an owning matcher.
- `assert_warm_matches_cold` in `resident.rs` now drives each of its
  workflows a third way, with each cold start lending its history. Each
  cold start must return the history as lent. The run must decide as the
  cold run decides, and resume as often as the owning warm run.
- The worker suites that run on a plain Postgres pass with the change:
  the end-to-end, resident, sticky, fork, mutex, cancellation scope, panic
  containment, history cap, update and cross-workflow signal suites.

## Reproduce

```bash
cargo bench -p autumn-harvest --no-default-features --features testing \
  --bench cold_decision_history_profile --no-run
BIN=target/release/deps/cold_decision_history_profile-<hash>

for REPS in 2 6; do
  COLD_DECISION_PROFILE_N=100 COLD_DECISION_PROFILE_MODE=lent \
  COLD_DECISION_PROFILE_END=park COLD_DECISION_PROFILE_REPS=$REPS \
    valgrind --tool=callgrind --branch-sim=no --cache-sim=no \
    --callgrind-out-file=cg.$REPS "$BIN"
done
```

## See also

- [`performance-replay.md`](performance-replay.md): why the matcher moves
  each matched output out.
- [ADR 0019](adr/0019-sqlite-drive-path.md): the lent history and its
  limits.
