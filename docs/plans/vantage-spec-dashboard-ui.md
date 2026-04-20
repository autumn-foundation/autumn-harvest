# 🔭 Vantage: Spec for Dashboard UI

## 👤 User Story
As an Operator monitoring multi-step workflows, I want an embedded Dashboard UI so that I can visualize running workflows, inspect activity inputs/outputs, and manage failures (e.g., retrying or cancelling workflows) without needing to query PostgreSQL manually.

## 📈 The "So What?" (Business Value)
Visibility is the hardest part of durable execution. While our Postgres-backed engine ensures reliability, debugging failures by writing SQL queries against the `harvest_events` table is slow and error-prone. By providing an embedded, read-only Dashboard UI directly within the `autumn-harvest-plugin`, we eliminate the need to deploy and manage a separate observability service (unlike Temporal). This reduces Time To Resolution (TTR) for operational issues and increases developer confidence in deploying orchestrations.

**Metric Definition:**
- Success = Operators can locate a failed workflow and view its failure reason in under 3 clicks.
- Adoption = >80% of `autumn-harvest` users mount the Dashboard UI endpoint.

## 🕵️ Gap Analysis
- **Temporal Web:** Feature-rich but requires a separate deployment, complex authentication, and massive operational overhead.
- **Inngest/Trigger.dev:** SaaS dashboards, requiring external network access and lock-in.
- **Current State:** No UI. Operators must query the database directly.

## ✅ Acceptance Criteria
- Must be embedded within the `autumn-harvest-plugin` and mountable on a specific route (e.g., `/api/harvest/ui`).
- Must provide a paginated list of workflows with their current status (Running, Completed, Failed, Cancelled).
- Must display a detailed view of a single workflow, showing its event history, input payload, and current activity status.
- Must not require external JavaScript/CSS assets (e.g., using bundled assets or HTMX/Tailwind served from memory).
- Must be strictly read-only for Phase 1.

## 🚫 Out of Scope
- Workflow management actions (Cancel, Retry, Terminate) — to be implemented in a future phase.
- Complex authentication/authorization schemes (users will secure the route via standard Autumn middleware).
- Editing payload or state.
- Real-time WebSocket updates (polling is sufficient for V1).
