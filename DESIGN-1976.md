# Design — Issue #1976: weighted fairness keys within a queue

Harvest weights queues, not keys. One noisy tenant in a shared queue delays
every other tenant in that queue. This change adds a fairness key per task.
The claim rotates across keys in weighted round robin within a queue.

**One migration. No new `WorkflowEvent` variant. No change to the default
claim statement.**

---

## 0. Planning record

### 0.1 Brainstorm — how can the claim share a queue across keys?

| # | Idea | Verdict |
|---|------|---------|
| B1 | One queue per tenant, then use queue weights (#515). | Rejected. Queues are static worker config. A new tenant needs a worker restart. |
| B2 | Rank rows per key with `row_number()` and sort by rank / weight. | Rejected. The rank has no memory across claims. The oldest head always wins a tie, so a flood still wins. Window functions are not allowed with `FOR UPDATE`. |
| B3 | Order keys by running count / weight. | Rejected. Short tasks keep every count near zero. The tie then goes to the flood. |
| B4 | Stamp a fair-queuing tag on each row at enqueue. | Rejected. A runtime weight change does not reach rows already enqueued. Enqueue gets a hot row per key. |
| B5 | Start-time fair queuing (SFQ) with a per-key pass, charged by the claim statement. | **Adopted.** See §1. |
| B6 | Keep a per-queue virtual clock row. | Rejected. Every keyed claim in a queue would lock one row. B5 derives the clock from key rows instead. |

### 0.2 Reverse brainstorm — how can this change do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | Slow every claim, also for users who do not use keys. | Opt-in per worker (`with_fairness_keys`). Off, the claim statement is byte-identical. A unit test pins that. |
| R2 | Let a returning idle key spend saved credit in a burst. | The start tag is `max(pass, V)`. Idle time earns no credit. A property test pins it. |
| R3 | Let a new key enter behind the flood. | A new key enters at `V`. A property test pins that it is served within `N` claims, where `N` is the number of active keys. |
| R4 | Deadlock the claim on the state row. | The claim locks the task row (`SKIP LOCKED`), then the state row. No other writer locks a state row first. Prune uses `SKIP LOCKED`. |
| R5 | Let state rows grow without a bound. | `prune_fairness_state` deletes idle rows that hold no debt and do not set `V`. It also resets an idle queue. |
| R6 | Let one queue's large pass push its rows behind another queue. | The sort key is the lag `max(pass, V) - V`, not the pass. Each queue's front key has a small lag. |
| R7 | Break a pinned gate (pause, build, cap, rate limit, fence). | The fair form is a splice of the base text. A test asserts that every gate survives. |
| R8 | Let an operator store 10^6 overrides. | At most 1,000 overrides per queue. The cap is checked under a queue-scoped advisory lock. |
| R9 | Treat the key as an identity. | Docs: a key bounds load, not access. Any starter may set any key. |

### 0.3 Six thinking hats

| Hat | Notes |
|-----|-------|
| White | The default claim scans and sorts the eligible backlog (#1971). Rate-limit buckets already show a hot row per key, charged in the claim. Workflow task rows are woken in place, so one row carries a run's key for its life. Temporal: weights per key, up to 1,000 overrides. |
| Red | "A noisy tenant cannot hold a queue" is easy to sell. Operators want a weight knob without a restart. |
| Black | Each keyed claim writes one state row. Concurrent claims on one key wait for that row. Fairness is shard-local. Dispatch-channel claims do not sort by key, but they charge it. The batched path (not wired) ignores keys. A fair claim skips the #1971 seek window and runs the full scan. |
| Yellow | No new bind. Weights apply at the next claim. A single key gives the old order, so priority and due time keep their meaning within a key. |
| Green | B1–B6 in §0.1. Per-start weights are a later option. |
| Blue | Spec: a pure model and property tests. Red: a DB test where a flood holds tenant B past the bound. Green: migration, propagation, fair splice, worker toggle. Then overrides, prune, bench, docs, review. |

---

## 1. Design

### 1.1 Model (SFQ)

Each `(queue, key)` has a `pass` and a `last_start`. The queue clock `V` is
the largest `last_start` in the queue. It never decreases.

- Start tag of a key: `S = max(pass, V)`. A key with no row has `S = V`.
- The claim takes the row with the smallest lag `S - V` after sticky rank
  and effective priority. Due time breaks a tie.
- After the post-claim rechecks accept the row, the claim charges the key:
  `last_start = S`, `pass = S + 1 / weight`.

Properties, proven by `fairness_key_props`:

1. A key that becomes active is served within `N` claims (`N` active keys).
2. Two backlogged keys stay within `1/w_i + 1/w_j` of each other in
   `served / weight`.
3. `V` never decreases. Idle time earns no credit.
4. With one key, the order is the order without fairness.
5. New keys take only the claims they need. While they arrive more slowly
   than the queue drains, a backlogged key gets every other claim.
6. Prune changes no start tag.
7. An idle queue forgets all its state. Keys that claim once never move `V`,
   so only this reset bounds their rows.

### 1.2 Key source

`fairness_key` is a nullable column on `harvest_task_queue`. A start sets it
from `StartWorkflowParams::fairness_key`. If that is `None`, the run's quota
key is the key. Activities take the key of their workflow task. A child, a
continue-as-new run or a workflow retry takes the parent's key, else its own
quota key. A reset fork, a DLQ redrive or a re-run takes the source run's
quota key. A quota key that fails `validate_fairness_key` is not used. A row
with no key uses the default key `''`.

The key lives on task rows only. `harvest_workflow_executions` is at Diesel's
64-column limit, and a workflow task row lives as long as its run.

### 1.3 Storage

- `harvest_fairness_state (queue_name, fairness_key, pass, last_start,
  updated_at)`: written only by the claim and by prune.
- `harvest_fairness_weights (queue_name, fairness_key, weight, updated_by,
  updated_at)`: operator overrides. Weight is in `[0.001, 1000]`. Default 1.

### 1.4 Claim

`splice_fairness` derives the fair form of each sorted claim variant (base,
fenced, kind). It adds one sort term, the lag, before the due time. No bind,
join or lock changes. A by-id claim names one row, so it keeps the plain
statement. The charge, `FAIR_CHARGE_SQL`, is a separate upsert in the same
transaction. It runs only when the post-claim rechecks keep the row.

Four forms of the lag lookup were measured. The claim filters estimate one
candidate row, so a form that joins gets a nested loop.

| Form | Cost |
|---|---|
| Join on a CTE of the state rows | A nested loop: every state row once per candidate row. 2x claim time at 8 claimers. |
| One `jsonb` map in a CTE | A cross join copied the whole map for each row. 4.4 s per claim at 10,000 keys. |
| Join on a derived table | Hashed only with good statistics. Else a nested loop: about 0.6 s per claim at 10,000 rows. |
| One `jsonb` map in an `InitPlan` | Built once per claim, one lookup per row. About 20 to 40 ms over the base at 10,000 rows, for 256 or 10,000 keys. Chosen. |

A lag is relative to its own queue's clock. A fair claim over several queues
therefore runs one statement per queue, in a random order.

### 1.6 Review record

Four review agents and a Codex review read the first push. The changes:

| Finding | Change |
|---|---|
| The charge ran inside the claim statement, so a recheck that gave the row back still charged the key. | The charge is a separate statement after the rechecks. |
| Lags of two queues do not compare. A queue with one active key starved a sibling queue. | One statement per queue, in a random order. |
| A quota key can be 256 bytes, or hold whitespace or control characters. | The cap is 256 bytes. An invalid quota key is not used as a fairness key. |
| The admin routes cannot address `.` or `..`. | The validator rejects them. |
| A deferred start dropped an explicit key. An atomic batch with a bad key was not atomic. | The HTTP start rejects the combination. Batch pre-validation checks each key. |
| Prune probed the backlog once per state row. | One hashed anti-join, in bounded batches. Two state indexes. |
| A stream of new keys can hold a backlogged key. | Two clock variants were tried on the model and broke the pair bound. SFQ stays. Property 5 pins its max-min behaviour. The docs state the limit. |
| Priority ageing sorts before the lag. | Documented. A worker with both options warns. |

### 1.5 Runtime API

`fairness_keys::{set_fairness_weight, clear_fairness_weight,
list_fairness_weights, prune_fairness_state}`, the admin HTTP routes and the
`harvest queue fairness` CLI commands. A change applies at the next claim.

## 2. Tests

| Test | Phase |
|------|-------|
| `fairness_key_props` (property tests): newcomer bound, returning key, pair lag bound, monotone `V`, one-key order, invisible prune, idle reset | Spec |
| `tenant_flood_holds_tenant_b_within_the_bound_with_fairness_keys` (DB) | Red (199 claims), then green (at most 1) |
| `without_fairness_keys_a_flood_holds_tenant_b_past_the_bound` (DB) | Control |
| `fair_claim_matches_the_model_sequence` (DB) | Green |
| `weight_override_changes_share_at_runtime` (DB) | Green |
| `override_cap_is_1000_per_queue` (DB) | Green |
| `prune_deletes_only_rows_the_claim_cannot_tell_apart`, `prune_resets_a_queue_with_no_pending_task` (DB) | Green |
| `concurrent_fair_claims_keep_every_charge` (DB, 8 claimers) | Green |
| `by_id_claim_and_unkeyed_rows_charge_their_keys`, `multi_queue_claim_keeps_a_clock_per_queue`, `fairness_off_writes_no_state` (DB) | Green |
| `start_key_reaches_activities_and_children`, `worker_with_fairness_keys_serves_tenant_b_through_a_flood` (worker end-to-end) | Green |
| `fairness_off_claim_is_byte_identical`, `fair_claim_query_preserves_every_gate_and_bind` (unit) | Green |
| `claim_bench` section `fairness` | Bench |
