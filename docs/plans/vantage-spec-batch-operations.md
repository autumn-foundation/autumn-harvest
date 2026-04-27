# Vantage Spec: Batch Operations

## 👤 User Story
As an Operations Engineer, I want to execute batch operations (start, signal, cancel, or terminate) across hundreds or thousands of workflow executions simultaneously, so that I can efficiently manage incidents, rollouts, or operational cleanups without writing one-off scripts.

## 💼 Business Value
**What business problem does this solve?**
When incidents occur (e.g., a downstream API is down for 3 hours), thousands of workflows might get stuck or need to be cancelled/restarted. Asking engineers to write custom scripts to query and mutate these workflows individually is error-prone, slow, and increases Mean Time to Recovery (MTTR). A first-class batch operations API transforms a stressful incident response into a single, auditable command.
**Complexity is a cost. Utility is a revenue.** We reduce the cost of operating the Autumn Harvest system by providing built-in bulk management capabilities.

## 🎯 Success Metrics
* **Success =** 10,000 workflows can be selected and have a signal dispatched to them within 5 seconds via the HTTP API/CLI.
* **Reliability =** Batch operations must be durable; if the Harvest server restarts mid-batch, the remaining operations must resume and complete.
* **Usability =** Operators can target workflows using existing `search_attrs` predicates.

## 🔍 Gap Analysis
Currently, the `autumn-harvest-cli` and management API only support single-workflow operations (e.g., `workflow cancel <id>`). To cancel 1,000 workflows, a user must write a bash script that iterates over `workflow list` output.
Compared to Temporal, which has robust Batch Operations (Start, Terminate, Cancel, Signal) based on visibility queries, Autumn Harvest is missing this critical enterprise feature.

## ✅ Acceptance Criteria
* Must expose a REST API endpoint for initiating a batch operation (e.g., `POST /api/harvest/batch-operations`).
* Must support the following actions: `Cancel`, `Terminate`, `Signal`.
* Must accept a query/filter to select target workflows (e.g., based on state, workflow name, or `search_attrs`).
* Must execute the batch operation asynchronously in the background. The API should return a `batch_job_id`.
* Must provide an API to check the status of a batch job (e.g., `GET /api/harvest/batch-operations/:id`), showing `total`, `completed`, and `failed` counts.
* The batch processor must process targeted workflows at a controlled rate to prevent overwhelming the task queues or database.
* The batch operation state itself must be durable (stored in a Postgres table, e.g., `harvest_batch_jobs`), allowing it to resume on restarts.

## 🚫 Out of Scope
* Batch Start (launching thousands of *new* workflows in one call) is excluded for Phase 1 of this feature to limit scope; focus is on managing existing workflows.
* Complex rollback of batch operations. If you batch cancel 1000 workflows, there is no "undo" button.
* Advanced UI for batch operations in `autumn-harvest-ui` (CLI and API first).