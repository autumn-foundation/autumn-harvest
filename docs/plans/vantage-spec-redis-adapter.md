# 🔭 Vantage: Spec for Redis-backed Task Queue Adapter

## 👤 **User Story:**
As an Operator of high-throughput workflow engines, I want a Redis-backed task queue adapter (`autumn-harvest-redis`) so that I can sustain dispatch rates greater than 10,000 tasks per second without overloading the primary Postgres database with polling and locking queries.

## 📈 **The "So What?" (Business Value)**
What business problem does this solve? While Postgres is robust for state and event history, using it for high-frequency task queuing (via `SKIP LOCKED`) eventually hits a ceiling around 10k ops/sec due to lock contention and connection pooling limits. For enterprise users or hyper-growth startups scaling beyond this limit, moving the ephemeral task queues to an in-memory data store like Redis (specifically Redis Streams) provides a massive performance ceiling upgrade. It serves as a high-throughput "escape hatch" while preserving the durability of Postgres for workflow history. This enables the engine to handle spikes in traffic without degrading database performance for critical state updates.

**Metric Definition:**
- Success = Engine can reliably sustain > 10,000 tasks/second queue dispatch and claim operations.
- Postgres CPU and lock contention metrics remain stable during high queue throughput.

## 🕵️ **Gap Analysis:**
- **Temporal/Cadence:** Uses gRPC-based in-memory task queues built into their server with separate database backends, handling massive throughput by default but requiring significant operational overhead.
- **Current State (`autumn-harvest`):** Relies solely on Postgres `SELECT ... FOR UPDATE SKIP LOCKED` for task dispatch. This is operationally simple but creates an upper bound on task throughput.

## ✅ **Acceptance Criteria:**
- Must provide a new optional crate `autumn-harvest-redis`.
- Must implement the queue adapter interface using Redis Streams.
- Must ensure at-least-once delivery semantics for task execution.
- Must support delayed scheduling and visibility timeouts equivalent to the Postgres implementation.
- Must seamlessly integrate with the existing worker runtime (workers can pull from Redis queues).
- Must retain Postgres as the sole source of truth for workflow history and state.

## 🚫 **Out of Scope:**
- Replacing Postgres for workflow history, state, or event sourcing (Redis is strictly for the ephemeral task queue).
- Defaulting to Redis for all users (Postgres remains the default, operationally simple path).
- Managing Redis cluster deployment or replication (operator responsibility).
