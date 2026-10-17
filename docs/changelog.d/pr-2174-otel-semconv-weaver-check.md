## Docs — upstream Weaver check for the OTel workflow draft (issue #2015)

**What shipped.** `scripts/check-otel-workflow-semconv.sh` runs the
upstream OpenTelemetry policy checks on the draft workflow model. It
mirrors the upstream `semantic-conventions` `make check-policies` recipe
at a pinned upstream commit. It reads the Weaver image digest and the
policy package commit from that tree. The model passes, and Weaver
reports no warning for it. A negative control with a namespace collision
must fail on the upstream naming policy, so a pass is not vacuous.

**CI.** The new `otel-semconv.yml` workflow runs the script on a ready
pull request that changes the draft. A weekly job runs it against
upstream `main`.

**Model.** The enum `workflow.operation.name` has no `examples`. Upstream
renders them in each span table, where they contradicted the span value.

**Submission.** The status stays `pending`. Section 10 adds a step to move
the pins and run the script before a maintainer files the issue. It lists
the files that the upstream pull request needs. Section 8 gains two open
questions.

**Tests.** `the_weaver_check_script_mirrors_upstream`,
`the_weaver_check_runs_when_the_model_changes` and
`the_page_records_the_weaver_check` guard the script, the workflow and the
page.

**Invariants.** No migration. No new `WorkflowEvent` variant. No engine
change.
