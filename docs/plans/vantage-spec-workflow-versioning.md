# 🔭 Vantage: Spec for Workflow Versioning

## 👤 **User Story:**
As an Engineer maintaining workflows in production, I want a mechanism to version workflow code safely, so that I can patch bugs or add features to existing workflows without breaking in-flight executions due to non-deterministic replay failures.

## 📈 **The "So What?" (Business Value)**
What business problem does this solve? Workflows are durable and long-running, meaning code written today might need to process events generated weeks ago. Without versioning, any code change to a running workflow will cause a divergence during replay, resulting in a non-deterministic failure. This limits agility and prevents hotfixes. Providing a versioning API (e.g. `ctx.version(...)`) allows teams to evolve their business logic safely, ensuring new code paths are only taken by new executions or properly migrated existing ones, thus maximizing system uptime and developer velocity.

**Metric Definition:**
- Success = 0 non-deterministic replay errors when a workflow is updated with a properly versioned code change.
- Success = Ability to deprecate and remove old workflow versions once all associated executions have completed.

## 🕵️ **Gap Analysis:**
- **Temporal/Cadence:** Solves this issue with robust versioning APIs (e.g., `workflow.GetVersion()`), allowing workflows to check their current version and conditionally execute logic while recording version markers in the event history.
- **Current State (`autumn-harvest`):** Replay fails immediately if the workflow code diverges from the recorded history. There is no built-in mechanism to record a version marker or conditionally branch logic based on a version during replay.

## ✅ **Acceptance Criteria:**
- Must provide a versioning API (e.g. `ctx.version("change-id", min_version, max_version)`) that records a version marker in the event history.
- Must ensure that during replay, the recorded version marker is used to select the correct code path, bypassing non-determinism checks for the updated logic.
- Must support multiple concurrent version changes within a single workflow.
- Must not require external infrastructure changes (relies on existing event history in Postgres).
- Must provide clear compilation errors or runtime warnings if versioning is used improperly.

## 🚫 **Out of Scope:**
- Automatic migration of in-flight workflow state to new versions (users must write code to handle the branching logic).
- Versioning of Activity definitions (Activities are short-lived and their execution is not replayed, so they do not require deterministic versioning).
- UI/Dashboard visualizations for versioned workflows (Phase 2 or UI team).
