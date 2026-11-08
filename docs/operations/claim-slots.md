# Claim slots (issue #2421)

Each worker caps its own claims with `max_concurrent_claims`. A shard can
still see `workers × max_concurrent_claims` claim statements at once. Claim
slots cap that number across the whole fleet. The cap is off by default.

## Turn it on

```rust
use autumn_harvest::builder::WorkerConfig;

let config = WorkerConfig::default().with_claim_slots_per_shard(8);
```

- `0` turns the cap off. The largest value is 1024
  (`builder::MAX_CLAIM_SLOTS_PER_SHARD`). Validation rejects a larger one.
- Set one value on every worker of a fleet. A worker with no cap takes no
  slot, so a mixed fleet can run more claims than the cap.
- `GET /admin/config` reports the value as `claim_slots_per_shard`.

## How it works

1. A claim transaction first takes one of N advisory locks of its shard,
   with `pg_try_advisory_xact_lock`. It tries each slot once, from a random
   start.
2. A claim that gets no slot sends no claim statement. Its loop tries its
   next shard in the same pass. A worker on one shard waits for its next
   wake.
3. The lock ends with the transaction, at commit or rollback.

The cap applies to the claim that scans the backlog. A claim by id from a
[Redis dispatch channel](redis-dispatch.md) and an owner take read one known
row, so they take no slot.

Each shard has its own slots, also when two shards share one database. The
lock key holds the shard id. The lock never waits, so it cannot deadlock
with the locks of the claim. See lock-order row 20 in
[`docs/architecture.md`](../architecture.md#lock-order-table).

## Watch it

`harvest.db.claim_slot_miss{shard}` counts the claims that got no slot. A
steady rate on one shard means that the fleet runs more claim loops than the
shard has slots. That is the cap at work. Raise the slot count only if the
claim latency of the shard stays low (`harvest_db_query_duration{op="claim"}`).

## Choose a value

A cap below the claim loops of the fleet lowers the claim rate of an
uncontended shard. On a local test fleet of 12 loops, 4 slots cut the claim
rate by about 40%. See [`docs/performance.md`](../performance.md) §
"Claim slots". Use the cap when claim latency rises with the number of
claimers, for example after a `VACUUM` or a lock stall.
