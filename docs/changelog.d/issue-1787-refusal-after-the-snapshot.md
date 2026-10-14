## Fix — A refusal after the idle-wait snapshot still arms the capacity wake (issue #1787)

**Behaviour change.** Before an idle wait, the leader claim loop reads
`capacity_bound()`. When it is false, the wait does not wake on a released
permit, because a release cannot unblock a claim. With the default two
claim loops, a follower could record a refusal just after that read:

- its poll found a pool full, so the gate refused a kind
- its claim skipped an activity type at its adaptive limit

The follower then parked. When the permit or the limit slot freed, nothing
woke the leader, so the refused backlog waited for the leader's timer or
listener. That is up to one `poll_interval`, 500 ms by default. Nothing was
lost.

Now each idle wait that can wake on a released permit takes a
`CapacityWait`. It registers for a refusal before it reads
`capacity_bound()`. `record_gate_refusal` and `claim_exclusions` fire
`refusal_recorded` when their flag turns on. An unbound wait then waits for
a released permit, as a bound wait does. A refusal alone does not end the
wait, because a poll right after it would meet the same full pool.
`refusal_recorded` uses `notify_waiters`, so it stores no wake for a wait
that starts later. The single-pool leader, the multi-shard round with and
without listeners, and the single-shard drain use `CapacityWait`. A
multi-shard drain still leaves the wake to its round.

The single-pool leader now sleeps the NOTIFY settle delay after its select,
not inside it. A capacity, deferral or refusal wake can no longer cut that
delay short. `wait_on_shard_listeners` already worked this way.

The race was found in review of autumn-foundation/autumn-harvest#2153,
after it merged.

**No migration. No new `WorkflowEvent` variant. No public API change.**

**Tests.**

- `a_refusal_after_the_snapshot_arms_the_wake_on_a_released_permit` and
  `a_limit_refusal_after_the_snapshot_arms_the_wake_on_a_freed_slot` fail
  before the fix.
- `a_refusal_right_after_the_read_still_arms_the_wait` records a refusal
  between the read and the wait. It fails when the wait registers after the
  read.
- `an_earlier_refusal_does_not_arm_a_later_wait` pins that only a refusal
  after its own snapshot arms a wait.
- `a_bound_wait_ends_on_a_released_permit_only` pins that a refusal does not
  end a bound wait, and that the wake clears the flag.
