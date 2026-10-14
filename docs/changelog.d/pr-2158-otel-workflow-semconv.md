## Docs — OpenTelemetry workflow conventions draft (issue #2015)

**What shipped.** `docs/otel-workflow-semconv/README.md` is a draft
proposal of workflow semantic conventions for OpenTelemetry. It generalizes
the ADR 0001 span contract into a `workflow.*` namespace. It has one span
for each core operation and a replay rule. The Weaver model is in
`docs/otel-workflow-semconv/model/workflow/`, in the upstream layout. A
table maps each Harvest span and `ATTR_*` constant to its proposed name.
The page lists where Harvest does not yet conform to the draft.

**Submission.** The status is `pending`. The page holds the upstream issue
text and the steps to file it. A maintainer files it and records the link.

**ADR 0001.** The alignment note no longer says that an OTel SIG reserves
`workflow.*`. In October 2026, no such namespace or SIG exists. The note
links the draft.

**Invariants.** No migration. No new `WorkflowEvent` variant. No engine
change.

**Tests.** The `otel_workflow_semconv_docs` guard suite checks the draft
against ADR 0001 §2, the `ATTR_*` constants and the Weaver model shape. It
also checks the replay rule, the submission status and sentence length. The
`lint` job runs it.
