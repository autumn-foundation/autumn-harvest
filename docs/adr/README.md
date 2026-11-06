# Architecture decision records

An ADR records one decision, its context and its consequences. Name a new
ADR `NNNN-<slug>.md`, with the next free number. Add one row to this
index, in file order. The guard `autumn-harvest-agent/tests/refine_docs.rs`
fails when an ADR has no row.

Some numbers name more than one ADR. Those records predate the rule of
one number per ADR, and they keep their names, because other pages link
them.

| ADR | Decision |
|---|---|
| [0001](0001-otel-trace-contract.md) | OpenTelemetry Trace Propagation Contract |
| [0002](0002-rust-native-execution-boundary.md) | Rust-Native Execution Boundary |
| [0003](0003-payload-codec-event-boundary.md) | Event payload codec boundary |
| [0004](0004-deterministic-simulation-testing.md) | Deterministic simulation testing |
| [0004](0004-partitioned-duplicate-append-detection.md) | Detect, do not prevent, a duplicate append across cohorts |
| [0004](0004-security-extras.md) | Security extras: audit chain, signed WASM modules, OTel semconv |
| [0004](0004-tenant-isolation-cells.md) | Tenant isolation with cells |
| [0005](0005-activity-timeout-retry-and-open-circuit.md) | Activity timeout retries and the open circuit |
| [0006](0006-agent-adapter-framework.md) | The agent adapter owns its primitives |
| [0006](0006-keyed-entity.md) | Keyed entity as sugar over a workflow |
| [0006](0006-oidc-sso-and-custom-roles.md) | SSO, custom roles and mTLS for Vantage and the management API |
| [0006](0006-worker-container-image.md) | The worker container image and Helm chart |
| [0007](0007-standalone-activity.md) | Standalone durable activity |
| [0008](0008-fairness-keys-start-time-fair-queuing.md) | Fairness keys use start-time fair queuing in the claim |
| [0009](0009-decision-owner.md) | One fenced owner per decision partition |
| [0010](0010-fenced-owner-lease.md) | The fenced owner lease |
| [0011](0011-stateright-model-checks.md) | Stateright model checks of the DST oracles |
| [0012](0012-owner-disk-cache.md) | The owner-local disk cache |
| [0013](0013-concurrent-agent-children.md) | Concurrent agent children run as child runs |
| [0014](0014-operator-refine-policy.md) | Operators decide the refine policy |
| [0015](0015-xtask-workspace-automation.md) | Workspace automation runs through `cargo xtask` |
| [0016](0016-agent-refinement.md) | Agent refinement is offline and operator-governed |
