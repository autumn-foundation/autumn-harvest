## Claim slots cap the claims per shard across the fleet (issue #2421)

`WorkerConfig::with_claim_slots_per_shard(n)` caps the claim statements that
run at once on one shard, across every worker. It is off by default. The
design is in `docs/design/DESIGN-2421.md`.

- A claim first takes one of `n` advisory locks of its shard with
  `pg_try_advisory_xact_lock`. The lock lives for the claim transaction.
  A claim that gets no slot sends no claim statement. Its loop tries its
  next shard in the same pass.
- The cap applies to the claim that scans the backlog: the leader, the
  followers and a weighted poll. A claim by id from a dispatch channel and
  an owner take read one known row, so they take no slot.
- Each slot key holds a class tag in its high word, the shard id and the
  slot. It is outside the `hashtext` range of the concurrency-key locks.
  Two shards on one database have separate slots. `docs/architecture.md`
  lock-order row 20 records why the lock adds no wait edge.
- New counter `harvest.db.claim_slot_miss{shard}`. New public API:
  `queue::ClaimSlots`, `queue::claim_task_with_slots`,
  `queue::take_claim_slot`, `queue::SlottedClaim` and
  `builder::MAX_CLAIM_SLOTS_PER_SHARD`. The effective config reports
  `claim_slots_per_shard`.
- No migration. No new `WorkflowEvent` variant. The claim statement does
  not change.
- **Breaking for struct literals.** `WorkerConfig` and
  `WorkerRuntimeConfig` gain the public field `claim_slots_per_shard`. Add
  `claim_slots_per_shard: None` to a struct literal. Validation rejects a
  value above 1024.
- **Upgrade.** Set one value on every worker of a fleet. A worker with no
  cap takes no slot, so a mixed fleet can exceed the cap.
- Evidence: `claim_slot_tests` in the integration suite. With 2 slots, 12
  claim loops run at most 2 claim statements at once. With no cap they
  reach 10. A worker with one full shard of two drains its backlog through
  the other inside one poll interval. Every activity of a backlog runs
  once with the cap on. After a 3 s `ACCESS EXCLUSIVE` stall, the claim
  rate returns to 90% of its rate before the stall in the first 1 s window,
  with the cap off and on. `docs/performance.md` § "Claim slots" has the
  table.
