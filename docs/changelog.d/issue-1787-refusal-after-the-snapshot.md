## Fix — A refusal after the idle-wait snapshot still wakes the poll loop (issue #1787)

**Behaviour change.** Before an idle wait, the leader claim loop reads
`capacity_bound()`. When it is false, the wait does not wake on a released
permit, because a release cannot unblock a claim. With the default two
claim loops, a follower could record a refusal just after that read:

- its poll found a pool full, so the gate refused a kind
- its claim skipped an activity type at its adaptive limit

The follower then parked, and the refused backlog waited for the leader's
timer or listener. That is up to one `poll_interval`, 500 ms by default.
Nothing was lost.

Now each idle wait takes a `CapacityWait`. It registers for a refusal
before it reads `capacity_bound()`. `record_gate_refusal` and
`claim_exclusions` fire `refusal_recorded` when their flag turns on. A wait
that was not bound then ends, and the leader polls again. That poll sees
the refusal, so the wait after it wakes on a released permit.
`refusal_recorded` uses `notify_waiters`, so it stores no wake for a later
wait. All four idle waits use `CapacityWait`: the single-pool leader, the
multi-shard round with and without listeners, and the single-shard drain.

The race was found in review of autumn-foundation/autumn-harvest#2153,
after it merged.

**No migration. No new `WorkflowEvent` variant. No public API change.**

**Tests.**

- `a_refusal_after_the_snapshot_ends_the_idle_wait` and
  `a_limit_refusal_after_the_snapshot_ends_the_idle_wait` fail before the
  fix.
- `an_earlier_refusal_does_not_end_a_later_wait` pins that a wait counts
  only refusals after its own snapshot.
- `a_bound_wait_ends_on_a_released_permit_only` pins that a bound wait still
  ends only on a released permit, and that the wake clears the flag.
