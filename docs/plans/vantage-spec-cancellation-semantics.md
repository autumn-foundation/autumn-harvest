# 🔭 Vantage: Spec for Cancellation Semantics

## 👤 User Story
As a Developer building long-running workflows, I want to explicitly cancel running workflows and have that cancellation propagate to running activities, so that I don't waste compute resources or create unwanted side-effects when a business process is aborted.

## 📈 The "So What?" (Business Value)
When a user aborts an operation (e.g., cancels an order, stops a data pipeline), the workflow engine must stop immediately. Right now, workflows just run to completion or fail. If we can't cancel them, we waste worker CPU cycles, database IO, and potentially make external API calls that cost money and cause state inconsistencies. By providing robust cancellation semantics, we allow businesses to halt expensive or erroneous operations gracefully, saving money and preventing incorrect side-effects.

**Metric Definition:**
- Success = 99% of cancelled workflows stop execution and clean up resources within 5 seconds of the cancellation signal.
- Cost savings on worker compute and external API charges for aborted processes.

## 🕵️ Gap Analysis
- **Temporal/Cadence:** First-class support for cancellation scopes and propagation to activities.
- **Current State (`autumn-harvest`):** Workflows can only be stopped by failing or finishing. Activities cannot be interrupted mid-flight.

## ✅ Acceptance Criteria
- Must allow operators to trigger a cancellation on a workflow execution via the Management API.
- Must propagate the cancellation signal to the workflow coroutine, allowing it to perform cleanup before exiting.
- Must propagate cancellation to currently executing activities (e.g., via a cancellation token or context).
- Must gracefully handle the case where an activity ignores the cancellation (enforcing a hard timeout if necessary).
- Must record a `WorkflowCancelled` event in the history log.

## 🚫 Out of Scope
- Rolling back already completed activities (that is the job of Saga primitives).
- Preemptive OS-level thread killing of activities (cancellation is cooperative).
