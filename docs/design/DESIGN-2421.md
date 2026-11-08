# Design — claim slots: a fleet-wide cap on claims per shard

Each worker caps its own claim loops (`max_concurrent_claims`). Nothing caps
the claims that run at once on one shard across the fleet. A shard sees up to
`workers × max_concurrent_claims` claim statements at once.

DBOS found that an unbounded dequeue goes metastable past 8 to 16 shards
([post](https://www.dbos.dev/blog/one-million-workflows)). A slow dequeue
draws more dequeuers. More dequeuers raise lock contention, so each dequeue
gets slower again. Their fix is a small fixed set of advisory locks per
shard. A process must hold one to dequeue. On a miss it moves on to the next
shard and does not wait.

This change adds the same cap as an option: `claim_slots_per_shard`. It is
off by default. **No migration. No new `WorkflowEvent` variant. The claim
statement does not change.**

---

## 0. Planning record

### 0.1 Facts

| Fact | Effect on this design |
|---|---|
| The claim runs in one `READ COMMITTED` transaction that `claim_task_one_statement` opens. | A transaction-scoped slot lock taken first in that transaction lives for exactly one claim. |
| `run_poll_loop_multi` tries each shard in turn. An empty `poll_once` moves to the next shard in the same pass. `follower_claim` does the same. | A miss that returns "no task" moves the loop to its next claim target with no new loop code. |
| `queue_pause` owns the two-argument advisory keyspace. A source walk enforces it. | The slot lock uses the single-argument `bigint` form. |
| The concurrency-key locks use `hashtext(key)::bigint`, so they stay in the `int4` range. The DR keys put a tag in the high word (`954 << 32`, `1823 << 32`, `18230 << 32`). | A slot key with its own high-word tag cannot collide with them. |
| `pg_try_advisory_xact_lock` never waits. | The slot lock adds no wait edge, so it cannot close a deadlock cycle. |
| Two logical shards can share one database. The DR fence pass key keeps them apart with the shard id in the low word. | The slot key holds the shard id too. Two shards on one database have separate slots. |

### 0.2 Brainstorm — how can a fleet bound claims per shard?

| # | Idea | Verdict |
|---|---|---|
| B1 | N advisory slot locks per shard, taken with `pg_try_advisory_xact_lock` first in the claim transaction. A miss skips the shard. | **Adopted.** It is the DBOS fix. It needs no table, no migration and no new wait. |
| B2 | A slot table with `FOR UPDATE SKIP LOCKED` rows. | Rejected. It needs a migration and adds row churn on the hottest path. |
| B3 | A central token service in the API process. | Rejected. It adds a network hop and a new failure mode. |
| B4 | Lower `max_concurrent_claims` on each worker. | Rejected. It does not scale with the fleet size, and it throttles a worker that is alone on a shard. |
| B5 | Session-level slot locks held across claims. | Rejected. A crashed claim loop keeps its slot until its connection ends. A pooled connection can outlive the loop. |
| B6 | One statement that tries each slot from a random start. | **Adopted** with B1. One round trip per claim. A random start spreads the claimers over the slots. |
| B7 | A slot per queue rather than per shard. | Rejected. The contention is on the shard's table, not on one queue. |
| B8 | Slots for the dispatch-channel claim by id and the owner take. | Deferred. Both claim one known row and scan no backlog. |

### 0.3 Reverse brainstorm — how can this change do harm?

| # | How to make it harmful | Mitigation |
|---|---|---|
| R1 | The slot key collides with another advisory lock, so an unrelated lock blocks claims. | A high-word tag puts every slot key outside the `int4` range and away from each other tag. A unit test checks each known key. The two-argument space stays with `queue_pause`. |
| R2 | The slot statement takes more than one slot. | The statement stops at the first lock that it gets (`LIMIT 1`, no `ORDER BY`). A DB test counts the held slot locks. |
| R3 | A slot outlives its claim. | The lock is transaction-scoped. Commit and rollback both free it. A DB test checks that no slot is held after a claim. |
| R4 | A miss parks the loop, so a free shard waits. | A miss returns "no task". The loop tries its next claim target in the same pass. A DB test saturates one shard of two and drains the backlog well inside one poll interval. |
| R5 | A miss counts as an empty claim, so the leader runs the throttle check and the claim histogram gets a false sample. | A miss is its own outcome. It records `harvest.claim.slot_miss` and runs no throttle check. |
| R6 | The cap starves a shard after a stall. | Each claim frees its slot at commit. A recovery test stalls a shard with an `ACCESS EXCLUSIVE` lock, lifts it and measures the time to recover. |
| R7 | The cap changes exactly-once delivery. | The slot only gates entry. `FOR UPDATE SKIP LOCKED` still decides each row. A DB test runs a backlog with the cap on and counts each run. |
| R8 | A deadlock with the claim's row locks. | The slot lock is a try lock and no path waits for it. `docs/architecture.md` row 20 records the order. |
| R9 | Workers with different shard identities key one database two ways. | Each claim target keys by its own shard. A worker with no shard identity keys as shard 0, the default shard. |
| R10 | A large slot count makes each miss expensive. | Validation caps the count at 1024. A miss then tries at most 1024 locks in one statement. |
| R11 | A mixed fleet: some workers set the cap and some do not. | A worker with no cap takes no slot. The docs say to set one value on the whole fleet. |

### 0.4 Six thinking hats — B1 with B6

| Hat | Notes |
|---|---|
| White | The cap adds one statement per claim when it is on. The recovery measurements are in `docs/performance.md` § "Claim slots". |
| Red | Off by default, so no deployment changes on upgrade. That is easy to trust. |
| Black | (1) One more round trip per claim when on. (2) A worker on one shard that misses waits for its next wake. (3) A low cap can cut throughput under no contention. (4) Two shards on one database keep separate slots, so their sum can exceed N on that database. |
| Yellow | It reuses the shard rotation that exists. No schema change. The claim SQL does not change. |
| Green | An adaptive slot count from claim latency can come later. B8 can follow if a profile shows contention there. |
| Blue | Red: DB tests for the cap, the next-shard move, exactly-once and recovery fail. Green: the slot statement and the worker outcome. Refactor: docs, lock-order row, metric, then a multi-angle review. |

---

## 1. Design

### 1.1 The setting

`WorkerConfig::claim_slots_per_shard` is `Option<NonZeroU16>`. `None` (the
default) takes no slot. `with_claim_slots_per_shard(0)` sets `None`. Worker
validation rejects a value above `MAX_CLAIM_SLOTS_PER_SHARD` (1024).

### 1.2 The slot key

`queue::ClaimSlots` builds the key of slot `s` on shard `h`:

```text
(CLAIM_SLOT_LOCK_CLASS << 32) | (h << 16) | s
```

`h` is the low 16 bits of the shard id. A claim target with no shard keys
as shard 0.

### 1.3 The slot statement

The claim transaction runs this statement first:

```sql
SELECT s.slot FROM generate_series(0, $2 - 1) AS s(slot)
 WHERE pg_try_advisory_xact_lock($1 + ((s.slot + $3) % $2))
 LIMIT 1
```

`$1` is the shard's base key, `$2` the slot count and `$3` a random start.
No row means a miss. The transaction then ends with no claim.

### 1.4 The worker

`poll_once_as` passes the slots of its claim target. A miss returns `false`,
records `harvest.claim.slot_miss{shard}` and skips the throttle check. The
multi-shard leader and each follower then try their next claim target. A
weighted poll stops at a miss, because each queue shares the shard's slots.

The operator guide is [`docs/operations/claim-slots.md`](../operations/claim-slots.md).

## 2. Acceptance criteria

| AC | Statement |
|---|---|
| AC1 | Red first: a DB test runs more claim loops than the cap against one shard. It asserts that at most N claim statements run at once. It fails on `trunk-dev`. |
| AC2 | A worker that misses a slot on one shard claims on its next shard in the same poll. A test proves it with two shards and one saturated. |
| AC3 | Every task of a backlog runs exactly once with the cap on. |
| AC4 | A recovery test stalls claims on one shard, lifts the stall and asserts that claim throughput returns to within 10% of its pre-stall rate in a bounded time. It runs with the cap off and on. `docs/performance.md` records both. |
| AC5 | The setting is in the builder, the runtime config, the effective config and the docs, with a changelog fragment. |

## 3. Test plan

Each test is in `autumn-harvest/tests/integration/claim_slot_tests.rs`. The
red column is the run against the config field with no slot code.

| Phase | Test | Red | Green |
|---|---|---|---|
| Red | `claim_slots_bound_the_claims_that_run_at_once` | Fail: overlap 10 with 2 slots. | Pass: overlap 2. |
| Red | `a_slot_miss_moves_the_claim_to_the_next_shard` | Fail: claims ran on the full shard. | Pass. |
| Red | `every_task_runs_once_with_claim_slots` | Fail: overlap 6 with 2 slots. | Pass. |
| Red | `claim_throughput_recovers_after_a_stall_with_slots` | Fail: no slot miss. | Pass. |
| Control | `without_claim_slots_the_fleet_overlaps_past_two` | Pass. | Pass. |
| Control | `claim_throughput_recovers_after_a_stall_without_slots` | Pass. | Pass. |
| Green | `the_slot_statement_takes_one_slot_until_commit`, `a_full_shard_leaves_the_slots_of_another_shard_free` | — | Pass. |
| Green | Unit tests: key range and layout, statement shape, validation, builder, effective config | — | Pass. |
| Both | `claim_concurrency_tests` and the claim unit tests | Pass. | Pass. |

## 4. Follow-ups (not in this change)

1. An adaptive slot count.
2. Slots for the by-id claim and the owner take (B8).
3. The capacity scenario of issue #2419 decides the default.
