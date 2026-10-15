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
a released permit or a freed limit slot, as a bound wait does. A refusal
alone does not end the wait, because a poll right after it meets the same
full pool or limit. A release that `capacity_freed` stored before the
refusal still ends the wait at once, as it ends a bound wait.
`refusal_recorded` uses `notify_waiters`, so it stores no wake for a wait
that starts later. The single-pool leader, the multi-shard round with and
without listeners, and the single-shard drain use `CapacityWait`. A
multi-shard drain still leaves the wake to its round.

A wake on `capacity_freed` now clears `limit_refused` as well as
`pool_refused`. The poll after the wake sets `limit_refused` again while a
type is at its limit. Before, a limit refusal that the wake had already
served also bound the next wait.

The single-pool leader now sleeps the NOTIFY settle delay after its select,
not inside it. A capacity or deferral wake can no longer cut that delay
short. `wait_on_shard_listeners` already worked this way.

The race was found in review of autumn-foundation/autumn-harvest#2153,
after it merged.

**Known gap, not changed here.** A permit that only a sibling poll holds
goes back without a wake on `capacity_freed`. The leader's gate can refuse a
kind because of that hold. The backlog then waits up to one `poll_interval`.
A wake on every returned poll permit would loop under saturation, so this
change leaves it.

**No migration. No new `WorkflowEvent` variant. No public API change.**

**Tests.**

- `a_refusal_after_the_snapshot_arms_the_wake_on_a_released_permit` and
  `a_limit_refusal_after_the_snapshot_arms_the_wake_on_a_freed_slot` fail
  before the fix.
- `a_refusal_right_after_the_read_still_arms_the_wait` records a refusal in
  a hook that runs with the read. It fails when the wait registers after
  the read.
- `an_earlier_refusal_does_not_arm_a_later_wait` and
  `an_earlier_limit_refusal_does_not_arm_a_later_wait` fail when
  `notify_one` replaces `notify_waiters`.
- `a_bound_wait_ends_on_a_released_permit_without_a_refusal` and
  `a_bound_wait_ends_on_a_released_permit_only` pin that a released permit
  ends a bound wait, and that a refusal does not.
- `a_released_permit_does_not_cut_the_settle_delay_short` pins the settle
  delay after a NOTIFY, with paused time.
