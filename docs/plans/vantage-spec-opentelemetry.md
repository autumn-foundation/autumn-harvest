# 🔭 Vantage: Spec for OpenTelemetry Integration

## 👤 User Story
As an Operator monitoring production workloads, I want our workflows and activities to emit standard OpenTelemetry spans and metrics, so that I can visualize distributed traces across our microservices and quickly diagnose performance bottlenecks or failures.

## 📈 The "So What?" (Business Value)
What business problem does this solve? Currently, developers have to rely on custom logs or database queries to trace workflows. This siloed approach increases Mean Time to Resolution (MTTR) when debugging complex, multi-service orchestrations. By standardizing on OpenTelemetry, we integrate seamlessly with existing observability stacks (Datadog, Jaeger, Grafana), reducing debugging time and saving engineering costs.

**Metric Definition:**
- Success = 100% of workflow and activity executions emit standard traces.
- 95% of operators can trace a request from their frontend through our durable workflows using their existing APM tools.

## 🕵️ Gap Analysis
- **Temporal/Cadence:** Offers built-in OpenTelemetry SDK integration and context propagation.
- **Current State (`autumn-harvest`):** No tracing support. Logging is standard text, making it difficult to correlate a specific web request to the background activities it spawns across different workers.

## ✅ Acceptance Criteria
- Must automatically create spans for workflow executions, activity executions, and timer delays.
- Must propagate trace context (Trace ID, Span ID) correctly across the Postgres task queue boundary (via headers or payload).
- Must emit standard metrics (e.g., active workflow count, activity duration, task queue depth).
- Must not enforce a specific OTel backend (the exporter must be configurable by the application developer).

## 🚫 Out of Scope
- Building a custom UI for tracing (users will use their existing tools like Jaeger, Datadog, etc.).
- Implementing our own logging framework.
- Adding auto-instrumentation to external third-party crates beyond our own execution context.
