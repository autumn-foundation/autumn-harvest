# 🔭 Vantage: Spec for Cron Workflow Schedules

## 👤 User Story
As an Engineer, I want to trigger standalone workflows on a recurring cron schedule, so that I can execute periodic background jobs (e.g., nightly rollups, weekly reports) durably without needing to wrap them in an complex DAG definition.

## 📈 The "So What?" (Business Value)
Currently, `autumn-harvest` supports scheduled execution, but it is deeply coupled to the DAG feature. If an engineer wants to run a simple, single-function workflow once a day, they must learn and define a DAG, adding unnecessary overhead and complexity. "Cron Workflow Schedules" introduces the ability to schedule standard workflows directly. This lowers the barrier to entry, speeds up developer velocity for simple background tasks, and provides a familiar primitive (cron) directly attached to the workflow itself, ensuring simple jobs stay simple while maintaining all the durable execution guarantees of the engine.

**Metric Definition:**
- Success = A new workflow can be scheduled to run on a cron expression without using the `#[dag]` macro or `DagBuilder`.
- Reduced boilerplate for single-task recurring jobs.

## 🕵️ Gap Analysis
- **Temporal/Cadence:** Natively supports cron schedules directly on workflows. When a workflow completes, if it has a cron schedule, the server automatically schedules the next run via a `ContinueAsNew` mechanism under the hood.
- **Current State (`autumn-harvest`):** Scheduling is restricted to Phase 2 DAGs. Standalone workflows cannot be scheduled out of the box without building an external trigger or wrapping them in a single-node DAG.

## ✅ Acceptance Criteria
- Must allow defining a cron schedule directly on a workflow definition (e.g., via the `#[workflow]` macro or a scheduling API).
- Must automatically trigger the workflow according to the provided schedule.
- Must ensure executions triggered by the schedule are tracked and observable like standard workflows.
- Must handle overlaps appropriately (e.g., policy for whether to run or skip if the previous scheduled run is still executing).
- Must integrate cleanly with the existing engine infrastructure without reinventing the scheduling wheel built for DAGs.

## 🚫 Out of Scope
- Dynamic schedule modification of running cron workflows (Phase 2).
- Advanced jitter or complex backoff scheduling outside of standard cron syntax.
