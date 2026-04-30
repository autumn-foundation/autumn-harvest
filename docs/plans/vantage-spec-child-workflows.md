# 🔭 Vantage: Spec for Child Workflows

## 👤 User Story
As a Developer orchestrating complex or unbounded business processes, I want to spawn child workflows from within a parent workflow, so that I can modularize complex logic into smaller, reusable components and overcome execution history size limits for long-running workflows.

## 📈 The "So What?" (Business Value)
Orchestrating large, monolithic workflows creates bottlenecks and unmanageable execution histories (event logs grow too large). By supporting child workflows, teams can encapsulate repetitive business processes (e.g., onboarding a single user) and orchestrate them from a parent process (e.g., a bulk onboarding job). This modularity reduces code duplication, simplifies debugging, and ensures the system remains performant and within history size limits even for massive, complex operations. Success = Parent workflows can successfully trigger and await the completion of child workflows without bloating the parent's event history.

## 📊 Metric Definition
- **Success Rate:** Child workflow execution success rate >= 99.9%.
- **Event History:** Parent workflow event history does not include child workflow's internal execution events, reducing parent log size by >80% for highly modular workflows.
- **Latency:** Child workflow initiation latency from parent signal < 50ms.

## 🔍 Gap Analysis
Currently, workflows must execute all logic and activities directly. If a developer wants to reuse a sequence of activities or needs to run a very long process, they risk hitting event history size limits or creating a monolithic, hard-to-maintain workflow. Existing solutions like Temporal provide native child workflow support (`workflow.ExecuteChildWorkflow`) to solve this. Our architecture document acknowledges this need but currently lacks the implementation (`ctx.spawn_child_workflow_raw`).

## ✅ Acceptance Criteria
- Must provide a robust method (e.g. `ctx.spawn_child_workflow_raw`) to spawn a child workflow from within a parent workflow context.
- Must ensure that the parent workflow can await the completion (and result) of the child workflow.
- Must store child workflow executions in the database, with a clear relation (`parent_id`) linking back to the parent workflow execution.
- Must ensure that failures in a child workflow are properly propagated back to the parent workflow as catchable errors, unless explicitly configured otherwise.
- Must maintain a separate event history for the child workflow to prevent bloating the parent's event history.

## 🚫 Out of Scope
- Cross-cluster or cross-namespace child workflows (keep it within the same Harvest deployment).
- Automatic retries of entire child workflows (retries should be handled at the activity level within the child, or explicitly by the parent).
- Complex signaling schemas between parent and child beyond standard signal mechanisms.
