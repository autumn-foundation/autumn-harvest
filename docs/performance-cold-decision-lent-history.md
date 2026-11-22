# Cold decision: the worker lends its history to the replay

**Outcome: shipped.** A cold decision of the Postgres worker no longer
copies the history for the replay, and it costs 47% fewer instructions. A
parked run has one history, which the worker and its resident workflow
share. A warm decision costs 24% to 26% fewer instructions.

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

## One history for a parked run

A resident workflow keeps its context, and so its matcher, between two
decisions. Its matcher held a second history of the run, next to the one
in the warm cache. A first version of this change kept that second copy.
It saved 13% of a cold decision that stays resident.

The worker and the resident workflow now share one history.

- A drive gives the whole history back, parked or not
  (`HistoryMatcher::reclaim_events`). The matcher of a parked workflow
  then holds no events. It records how many it gave back.
- A parked workflow reads no event. Its future waits on a channel, and
  the next decision checks the delta against the parked awaits.
- The next warm decision lends the history again
  (`ResidentWorkflow::resume_lending_with`). The matcher takes back the
  events that it gave (`HistoryMatcher::restore_events`), and each step
  takes its part of the delta by a move
  (`HistoryMatcher::append_consumed_owned`).
- The resume puts every event back in the history of the worker, in
  order, on a success and on a decline.

So a parked run holds one history, not two. A warm decision made a copy of
each delta event for the matcher, and it now makes none.

The matcher restores only as many events as it gave back. A history of
another length declines the resume (`ResumeDeclined::HistoryMismatch`),
and the decision replays cold. `ResidentWorkflow::resume_with` declines a
workflow that holds no events for the same reason.

## Workload

`benches/cold_decision_history_profile.rs` builds a history of `N`
completed sequential activities and runs one cold decision over it, as
`resident::start` does. `owned` copies the history first, as the worker
did. `lent` calls `resident::start_lending`. The decision asks for one
more activity and stays resident (`park`), or ends the run (`complete`).
`resume` runs one cold decision and then repeats a warm decision. Each
warm decision resumes the resident workflow with one activity result.
`owned` resumes with a borrowed delta, as the worker did. `lent` calls
`ResidentWorkflow::resume_lending`. The difference between 6 and 2
repeats, divided by 4, is the cost of one decision.

## Result

| Activities | The decision | Owned | Lent | Change |
|---:|---|---:|---:|---:|
| 20 | cold, ends the run | 717,560 | 389,698 | −45.7% |
| 100 | cold, ends the run | 3,488,760 | 1,838,734 | −47.3% |
| 400 | cold, ends the run | 13,872,507 | 7,268,259 | −47.6% |
| 20 | cold, stays resident | 746,462 | 411,887 | −44.8% |
| 100 | cold, stays resident | 3,539,383 | 1,861,783 | −47.4% |
| 400 | cold, stays resident | 14,025,373 | 7,290,070 | −48.0% |
| 20 | warm | 78,464 | 59,700 | −23.9% |
| 100 | warm | 84,957 | 63,237 | −25.6% |
| 400 | warm | 79,208 | 59,682 | −24.7% |

A cold decision keeps one copy of each matched output, for the workflow,
and makes no other. It costs the same whether it ends the run or stays
resident. A warm decision does not grow with the history. Its gain is the
copy of the delta that it no longer makes.

## What the worker does when the history does not come back

A lent matcher gives the history back only when it holds exactly the
events that it got, with the delta of a warm resume on top. A drive always
does: a view cut of a cancelled scope is closed when its poll returns, and
a warm resume counts each event that it appends. So a missing history is a
broken invariant. The worker then fails the task with a dispatch error,
and a later attempt loads the history again. It does not go on with a
history that it cannot trust. A warm resume reports that case as
`ResumeDeclined::HistoryNotReturned`. The worker does not replay cold
after it, because the history that it holds is not whole.

## Proof

- `a_released_matcher_takes_its_events_again` gives the events of a
  matcher back, restores them, appends a delta, and gets every event back
  in order.
- `restore_refuses_events_that_the_matcher_did_not_release` covers a
  history of another length, and a matcher that released nothing.
- `a_workflow_that_shares_its_history_declines_a_resume_with_none` fails
  when `resume_with` does not check for a workflow with no events.
- `a_lent_resume_declines_a_history_that_the_workflow_did_not_give_back`
  and `a_lent_resume_that_declines_its_delta_returns_every_event` cover
  the declines. Each one leaves the history of the caller whole.
- `a_lent_wake_with_nothing_new_waits_again_on_one_history` covers a wake
  that resolves nothing, and the decision after it.
- `assert_warm_matches_cold` and `assert_arrivals_match_cold` in
  `resident.rs` drive each of their workflows a third way. Each cold start
  and each warm resume lends the history. Each drive must return the
  history that the store holds. The run must decide as the cold run
  decides. It must resume and decline as often as the owning warm run.
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
  COLD_DECISION_PROFILE_END=resume COLD_DECISION_PROFILE_REPS=$REPS \
    valgrind --tool=callgrind --branch-sim=no --cache-sim=no \
    --callgrind-out-file=cg.$REPS "$BIN"
done
```

## See also

- [`performance-replay.md`](performance-replay.md): why the matcher moves
  each matched output out.
- [ADR 0019](adr/0019-sqlite-drive-path.md): the lent history and its
  limits.
