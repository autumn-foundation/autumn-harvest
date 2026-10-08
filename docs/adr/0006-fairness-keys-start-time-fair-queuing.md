# ADR 0006: Fairness keys use start-time fair queuing in the claim

## Status

Accepted (issue #1976).

## Context

- Harvest weights queues, not keys. One tenant's flood in a shared queue
  delays every other tenant in that queue.
- Quotas, throttles and concurrency caps bound a tenant. They do not order
  the claim, so a tenant under its caps can still fill the queue head.
- Temporal ships weighted fairness keys with up to 1,000 weight overrides.
  DBOS partitions queues by tenant. Hatchet offers group round robin.
- The default claim already scans and sorts the eligible backlog (#1971).
  The claim statement is the engine's hottest statement.

## Decision

### 1. A fairness key per task, opt-in per worker

`harvest_task_queue.fairness_key` names the tenant of a task. A start sets
it, else the run's quota key is the key. Activities, children and
continue-as-new runs inherit it. A worker turns fairness on with
`WorkerConfig::with_fairness_keys(true)`. Off, the claim statement is
byte-identical.

### 2. Start-time fair queuing, charged by the claim

Each `(queue, key)` has a `pass` and a `last_start` in
`harvest_fairness_state`. The queue clock `V` is the largest `last_start`.
The claim sorts on the lag `max(pass, V) - V` after sticky rank and
priority, and before the due time. After the post-claim rechecks keep the
row, a second statement in the same transaction upserts the claimed key:
`last_start = start`, `pass = start + 1/w`. A lag is relative to its own
queue's clock, so a fair claim over several queues runs one statement per
queue.

`V` comes from the key rows, so no per-queue row is hot. A new key starts at
`V`, so idle time earns no credit.

### 3. Weights are rows, read at the next claim

`harvest_fairness_weights` holds overrides. The claim reads one override by
primary key. A queue holds at most 1,000 overrides. Weights are from 0.001
to 1000.

### 4. A splice, not a second statement

`queue::splice_fairness` derives the fair form of every claim variant from
its text. It adds no bind. A unit test pins that every gate and bind
survives.

## Consequences

- A key with no debt waits at most one claim per other active key. The
  model proof is `a_new_key_is_served_within_the_active_key_count`. The DB
  proof is `tenant_flood_holds_tenant_b_within_the_bound_with_fairness_keys`.
  The bound counts claims. At a fixed service rate, one claim stands for one
  task duration.
- Concurrent claimers read the same state. With `C` claimers the bound
  grows by up to `C - 1` claims. Each charge stays exact. Serializing the
  choice would need a per-queue lock, rejected below.
- New keys get a claim before any key in debt. While they arrive more slowly
  than the queue drains, that is max-min fair. When they arrive as fast as
  the queue drains, they take every claim. Two variants that advance `V` on
  a new key's claim broke the pair bound on the model, so SFQ stays.
- Each fair claim writes one state row. Concurrent claims of one key wait
  for that row. The claim benchmark measures the cost.
- State rows grow with keys. The retention janitor prunes idle rows that
  the claim cannot tell from no row. It also resets an idle queue, as the
  SFQ idle rule does. Keys that claim once never move `V`, so only the
  reset bounds them.
- Fairness is shard-local. A key bounds load, not access.
- `claim_task_batched` ignores keys.
- A fair claim skips the seek window of #1971 and runs the full scan, as
  priority ageing does. The window guard proves a pick from priority and
  due time only, and the lag can put any row first.

## Alternatives rejected

| Alternative | Why not |
|---|---|
| One queue per tenant, then queue weights | Queues are static worker config. A new tenant needs a restart. |
| Rank rows with `row_number()` over each key | The rank has no memory across claims, so the flood wins every tie. `FOR UPDATE` forbids window functions. |
| Order keys by running count over weight | Short tasks keep every count near zero, so ties go to the flood. |
| Fair-queuing tag stamped at enqueue | A runtime weight change misses rows already enqueued. Enqueue gets a hot row. |
| A per-queue clock row | Every keyed claim in a queue would lock one row. |
| Advance `V` on a new key's claim | Two variants broke the pair bound in `fairness_key_props`. |
