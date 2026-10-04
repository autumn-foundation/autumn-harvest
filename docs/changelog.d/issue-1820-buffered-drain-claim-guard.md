## Fix — Buffered-schedule drain takes the fire claim (issue #1820)

**Problem.** `drain_buffered_schedule_runs` serves the `BufferOne` and
`BufferAll` overlap policies. It took no claim. Every replica runs the
scheduler, so two replicas could drain one buffered slot. `RejectDuplicate`
stopped the second execution, but both drains added the slot to
`runs_started`, so `max_runs` exhausted early. Both also wrote a `fired`
decision and a throttle reservation. The drain's final write had no token
fence, so it could also overwrite a slot that the tick appended.

**What shipped.**

- The drain takes the same fire claim as the tick fire path: a
  `fire_claim_token` with a 30 s TTL on the database clock. A live claim held
  by a peer makes the drain skip the row.
- After the claim, the drain reads the row again. It does not use the
  pre-claim snapshot for any decision.
- The final write of `buffered_runs` and `runs_started` matches only this
  drain's token, and it clears the claim. Every other exit releases the claim,
  also fenced on the token.
- `release_fire_claim` is now shared by the drain and the tick error path.
- The HA runbook describes the drain claim. The "tracked separately" note is
  gone.

**Behaviour change.** A schedule `PATCH` returns `409` while a drain holds
the claim, as it does during a fire.

**Invariants.** No migration. No new `WorkflowEvent` variant. No
`harvest_events` write.

**Tests.** `scheduler_ha_tests` gains four DB tests: a live peer claim blocks
the drain; two concurrent ticks start each buffered run once (five rounds);
the drain releases its claim after a dispatch and with no capacity; an expired
claim does not block a peer. Three of them failed before the fix. The suite
also runs without Docker through `HARVEST_TEST_DATABASE_URL`.
