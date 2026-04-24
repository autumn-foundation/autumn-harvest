# 🔭 Vantage: Spec for Continue-As-New

## 👤 User Story
As an Operator running long-lived orchestrations (such as recurring billing cycles or persistent entity loops), I want a workflow to be able to restart itself with a new state, so that the workflow's event history does not grow infinitely large and cause performance degradation during replay.

## 📈 The "So What?" (Business Value)
What business problem does this solve? In event-sourced workflow engines, a workflow's history grows with every activity executed. For workflows designed to run infinitely (e.g., polling loops, IoT device monitors, or monthly billing schedules), an unbounded event history eventually leads to massive memory consumption, sluggish replay times, and database bloating. "Continue-As-New" is an optimization mechanism that atomically terminates the current workflow execution and starts a new one with a fresh event history, passing along necessary state. This feature allows businesses to run perpetual processes durably and efficiently without hitting infrastructural limits, unlocking new use cases like persistent actors and infinite schedules without forcing engineers to build complex custom cleanup logic.

**Metric Definition:**
- Success = Workflows can run indefinitely via `Continue-As-New` without event history exceeding 500 events per execution.
- Replay time for an indefinitely running workflow remains constant instead of growing linearly.

## 🕵️ Gap Analysis
- **Temporal/Cadence:** Offers `continue_as_new` as a first-class citizen, allowing a workflow function to return an error/special value that triggers the server to start a new execution with the same workflow ID.
- **Current State (`autumn-harvest`):** Workflows are assumed to be finite. An infinite loop of activities inside a single workflow will eventually exhaust Postgres limits or process memory when replaying thousands of events.

## ✅ Acceptance Criteria
- Must introduce a mechanism (e.g., returning a specific error type or calling a context method) to signal `Continue-As-New` to the worker runtime.
- Must ensure the transition is atomic: the previous execution completes and the new execution is enqueued reliably.
- Must allow passing a new input payload to the next iteration of the workflow.
- Must retain the same `WorkflowId` (logical identity) while generating a new `ExecutionId` (to start a fresh event history log).
- Must properly handle any existing signals or child workflows during the transition.

## 🚫 Out of Scope
- Automatic `Continue-As-New` based on history size limits (this will be user-initiated via code for Phase 1).
- Modifying the workflow type during `Continue-As-New` (it must restart the same workflow definition for now).
