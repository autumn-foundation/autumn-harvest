# 🔭 Vantage: Spec for Database Sharding

## 👤 User Story
As a Platform Engineer managing a high-throughput workflow cluster, I want to shard the workflow database across multiple PostgreSQL instances, so that we can scale beyond the write-throughput limits of a single master database without migrating to a new orchestration tool.

## 📈 The "So What?" (Business Value)
Currently, our durable workflow engine relies entirely on a single PostgreSQL instance for its event store and task queue. While this keeps operations simple for small to medium workloads, a single Postgres database eventually hits a write ceiling (TPS limit). Once a business scales its workflow volume significantly, the engine will bottleneck on the database, leading to slow task dispatch and potential outages. By introducing a sharding mechanism, we unlock horizontal scalability for the database layer. This ensures that large enterprises can adopt our engine for massive workloads without hitting an architectural wall, allowing them to stay within our ecosystem rather than undertaking a costly migration to Temporal or Cadence.

**Metric Definition:**
- Success = System can handle >50,000 workflow starts per second when linearly scaled across 5+ database shards.
- Operational overhead to add a new shard is <1 hour.

## 🕵️ Gap Analysis
- **Temporal/Cadence:** Uses Cassandra or heavily sharded MySQL/PostgreSQL architectures natively to support millions of concurrent workflows.
- **Current State (`autumn-harvest`):** Tightly coupled to a single PostgreSQL connection pool. No concept of routing executions to different databases.

## ✅ Acceptance Criteria
- Must introduce a mechanism to partition workflow executions across multiple, independent database shards.
- Must ensure that all events and tasks for a single workflow execution reside on the same shard (to maintain ACID guarantees per workflow).
- Must provide a consistent routing strategy (e.g., hash-based or range-based on Execution ID) to determine which shard owns a workflow.
- Must support gracefully adding new database shards to the cluster without downtime.
- Must not compromise the "SKIP LOCKED" task queue performance within a single shard.

## 🚫 Out of Scope
- Cross-shard transactions (Sagas must be used for distributed compensation).
- Rebalancing existing running workflows to a new shard (only new workflows will be routed to the new shard).
