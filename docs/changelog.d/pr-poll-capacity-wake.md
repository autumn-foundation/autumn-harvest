## Fix — A permit freed during a gated poll wakes the idle wait (issue #1787)

**Behaviour change.** A poll that finds a pool full claims only the other
kind. When it then goes idle, the wait also wakes on a released permit. The
worker read that condition after the claim. A permit could free during the
claim. The pools then looked free, so the wait did not listen for the
release. The backlog of the refused kind waited one full poll interval.

Now the gate records each refusal in a flag. While the flag is set, an
idle wait also wakes on `capacity_freed`. `Notify` keeps the release that
came before the wait, so the wait ends at once. This covers the
single-shard loop, the multi-shard loop and the listener wait, because each
one reads `capacity_bound`.

A read does not clear the flag. Several claim loops share it, so a read
that cleared it could drop a refusal that a follower recorded during the
read. A poll does not clear it either, because a poll of another shard
cannot claim the refused backlog. Only a wake on `capacity_freed` clears
it. The next poll then sees the gate again and sets the flag if it still
refuses. A stale flag costs at most one extra poll.

In a multi-shard round, the 10 ms closing wait of a shard's Postgres drain
no longer listens for a freed permit. It could take the stored wake of an
earlier gated shard. The round's own idle wait then slept a full poll
interval.

**Known limit.** Two release paths still do not notify `capacity_freed`: a
poll permit that goes back after a claim error, and a slot-tuner shrink
permit that a later tick returns. A wait then ends at the poll interval, as
before.

**Why.** `poll_capacity_gate_tests::a_freed_permit_starts_the_next_task_without_a_poll_interval_wait`
failed in about 5 to 8% of runs against Postgres 16, on `trunk-dev` too.
Each failure showed a gap of one poll interval, about 3 s, between two
300 ms activities.

**No migration. No new `WorkflowEvent` variant. No public API change.**

**Tests.** `a_permit_freed_during_a_gated_poll_still_wakes_the_idle_wait`
frees the activity permit between the gated poll and the idle wait.
`a_multi_shard_drain_leaves_the_capacity_wake_to_the_round` drains a second
shard after a gated poll and checks that the round still wakes.
`a_refusal_stays_armed_until_a_wait_wakes_on_it` records a follower refusal
between two reads of the leader. Each failed before its fix. The integration test above passed in every local
Postgres run after the fix.
