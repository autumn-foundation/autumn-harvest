# 🔭 Vantage: Spec for Saga Primitives

## 👤 User Story
As a Developer orchestrating multi-step distributed business processes, I want to define compensation actions for my workflow steps, so that if a later step fails, the system automatically and durably rolls back the previous steps to maintain business consistency.

## 📈 The "So What?" (Business Value)
Distributed transactions are a reality in microservices. When an e-commerce order reserves inventory but fails to capture payment, we must release the inventory. Without native Saga primitives, engineers write custom, error-prone rollback logic that often fails during edge cases (e.g., worker crashes during the rollback itself). By providing robust Saga primitives, we reduce the cost of building reliable business processes and eliminate silent data inconsistencies that impact revenue and customer trust. Success = Zero orphaned state during multi-step workflow failures.

## ✅ Acceptance Criteria
- Must allow developers to register a compensation action (which is itself an activity) for any completed workflow step.
- Must guarantee that compensation actions are executed durably, with their own configurable retry policies, surviving process restarts and worker crashes.
- Must allow automatic execution of the compensation chain (in reverse order of completion) when a workflow encounters a terminal failure.
- Must support explicit triggering of compensations from within the workflow logic.
- Must handle "poison pill" compensations (compensations that exhaust all retries) by moving them to a Dead Letter Queue for manual operator intervention.

## 🚫 Out of Scope
- Distributed two-phase commit (2PC) or distributed locks across databases.
- Automatic, magic inference of what the "reverse" of an activity is (the developer must provide the compensation logic).
- Synchronous rollbacks (compensations are durably enqueued and run asynchronously like normal activities).
