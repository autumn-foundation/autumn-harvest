## Docs — upstream Weaver check for the OTel workflow draft (issue #2015)

**What shipped.** `scripts/check-otel-workflow-semconv.sh` runs the
upstream OpenTelemetry policy checks on the draft workflow model. It
mirrors `make check-policies` of `semantic-conventions` at a pinned
upstream commit. It reads the Weaver image digest and the policy package
commit from that tree. A negative control with an unresolved `ref` must
fail, so a pass is not vacuous. The model passes and adds no warning.

**CI.** The new `otel-semconv.yml` workflow runs the script when the
draft, the script or the workflow changes.

**Submission.** The status stays `pending`. Section 10 adds a step to move
the pins and run the script before a maintainer files the issue.

**Invariants.** No migration. No new `WorkflowEvent` variant. No engine
change.
