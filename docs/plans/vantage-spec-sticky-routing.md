# 🔭 Vantage: Spec for Sticky Cross-Worker Routing

## 👤 User Story
As an Infrastructure Engineer scaling our durable workflow engine, I want workflows to route subsequent tasks to the worker that already holds their state in the LRU cache, so that we minimize database load from redundant history replays and reduce end-to-end workflow execution latency.

## 📈 The "So What?" (Business Value)
Right now, any worker can pick up the next activity or timer for a workflow. This statelessness is great for reliability but terrible for performance at scale. When a workflow is dispatched to a new worker, that worker must load the entire event history from Postgres and replay it to rebuild the state. As workflows get longer and traffic scales, this redundant replay causes high CPU usage on workers and heavy read load on the database. By routing tasks to the worker that already has the workflow state in its LRU cache ("sticky routing"), we can skip the replay phase, drastically reducing database load and speeding up workflow progression.

**Metric Definition:**
- Success = 90% cache hit rate for workflow execution continuations.
- Database read load (SELECTs on `harvest_events`) drops by at least 40% for typical workloads.
- End-to-end workflow latency is reduced by avoiding replay overhead.

## 🕵️ Gap Analysis
- **Temporal/Cadence:** Solves this with a complex ring-hash and sticky task queues, requiring a dedicated frontend routing tier.
- **Current State (`autumn-harvest`):** We have a local LRU cache (`WorkflowCache`), but no mechanism to route tasks back to the worker holding the cache. Task claiming via `SKIP LOCKED` is entirely random based on DB locking.

## ✅ Acceptance Criteria
- Must introduce a mechanism to "pin" a workflow execution to a specific `WorkerId` for a configurable timeout duration.
- Must ensure that if the sticky worker crashes or takes too long, the task gracefully falls back to the general task queue so any worker can pick it up (no single point of failure).
- Must not require external distributed caching (e.g., Redis) or complex gossip protocols.
- Must cleanly integrate with our existing Postgres task queue (`harvest_task_queue`) and `SKIP LOCKED` claiming logic.

## 🚫 Out of Scope
- Complete cluster state synchronization or distributed locking.
- Re-architecting the task queue away from Postgres.
- Hard affinity (if the pinned worker is dead, we must prioritize progress over cache locality).
