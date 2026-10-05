## Fix — buffered-schedule drain takes the fire claim (issue #1820)

**Problem.** `drain_buffered_schedule_runs` serves the `BufferOne` and
`BufferAll` overlap policies. It took no claim. Every replica runs the
scheduler, so two replicas could drain one buffered slot. `RejectDuplicate`
stopped the second execution. But both drains added the slot to
`runs_started`, so a schedule with `max_runs` exhausted early. Both also wrote
a `fired` decision, and both reserved a throttle token that the duplicate then
refunded. The drain's final write had no token fence, so it could also
overwrite a slot that the tick appended.

**What shipped.**

- The drain takes the same fire claim as the tick fire path: a
  `fire_claim_token` with a 30 s TTL on the database clock. If a peer holds a
  live claim, the drain skips the row.
- The drain checks capacity and admission gates on the snapshot first. A row
  that cannot drain costs no claim and no write.
- After the claim, the drain reads the row again. It uses the current buffer,
  budget and capacity.
- The final write of `buffered_runs` and `runs_started` matches only this
  drain's token, and it clears the claim. Every other exit releases the claim,
  but only while the token still holds it.
- The exhausting write now increments `runs_started` on the database side. A
  concurrent manual-trigger increment is no longer lost.
- `release_fire_claim` is now shared by the drain and the tick error path.
- The HA runbook describes the drain claim. The "tracked separately" note is
  gone.

**Limit.** The drain does not renew its 30 s claim. A drain that crashes or
runs longer can lose the claim. A peer then drains the same buffer again, and
`RejectDuplicate` stops the duplicate execution. `runs_started` still counts
each slot once.

**Behaviour change.** A schedule `PATCH` returns `409` while a drain holds the
claim, as it does during a fire.

**Invariants.** No migration. No new `WorkflowEvent` variant. No
`harvest_events` write.

**Tests.** `scheduler_ha_tests` gains five DB tests:

- A live peer claim blocks the drain.
- Two concurrent ticks start the buffered run once. A row lock makes both
  ticks contend, so the race is certain.
- A drain that lost its claim writes nothing and keeps the peer's claim.
- The drain releases its claim after a dispatch and when it has no capacity.
- An expired claim does not block a peer.

Four of them fail on the code before the fix. The suite also runs without
Docker through `HARVEST_TEST_DATABASE_URL`.
