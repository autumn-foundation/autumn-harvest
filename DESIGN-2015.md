# Design — Issue #2015: propose workflow span conventions to OpenTelemetry

Issue #2015 asks for a standards play. OpenTelemetry has no workflow
semantic conventions. ADR 0001 already defines eight span kinds, the
`harvest.*` attributes and a replay rule. The deliverable is a draft
proposal in `docs/`, generalized away from Harvest names. It is then
submitted upstream, or a decision not to submit is recorded.

**No migration. No new `WorkflowEvent` variant. No engine change.** The
change adds a proposal page, a Weaver model, a guard suite and one CI step.

---

## 0. Planning record

### 0.1 Facts found before the plan

- ADR 0001 §2 lists eight spans. Each has a kind: `INTERNAL`, `PRODUCER`
  or `CONSUMER`.
- `telemetry.rs` defines ten `ATTR_*` constants. ADR 0001 §1 lists seven.
  The three signal attributes came later.
- The code also emits `harvest.timer.start`. ADR 0001 does not list it.
- ADR 0001 §4 is the replay rule. A replayed run gets a new root span with a
  link to the original. Activity spans are not emitted during replay.
- The OTel attribute registry has no `workflow` namespace. The nearest
  prior art is `cicd.pipeline.*`, which models CI/CD pipeline runs.
- Upstream conventions are Weaver YAML under `model/<area>/`, with Markdown
  under `docs/<area>/`. A new area needs a group of committed owners.
- Dapr began to codify its workflow telemetry with Weaver in 2026
  (dapr/dapr#9213). Its post proposes `PRODUCER` spans to schedule and
  `CONSUMER` spans to execute.
- Issue #1838 maps two Harvest metrics onto messaging conventions. This
  change covers spans only.

### 0.2 Brainstorm — what can the proposal contain?

| # | Idea | Verdict |
|---|------|---------|
| B1 | A prose-only write-up. | Rejected. Upstream reviews YAML models. Prose drifts from the model. |
| B2 | A Weaver YAML model, `registry.yaml` and `spans.yaml`, beside the proposal page. | **Adopted.** It is the upstream format, and a test can parse it. |
| B3 | A `workflow.*` namespace for every attribute. | **Adopted.** The registry has no such namespace, so nothing collides. |
| B4 | Reuse `messaging.*` for the queue and the signal. | Rejected for spans. A task queue is engine-internal. Listed as an open question. |
| B5 | One span group for each operation, with a `workflow.operation.name` enum. | **Adopted.** It follows the `cicd` and `messaging` pattern. |
| B6 | Reuse the general `error.type` attribute on failure. | **Adopted.** It avoids a new error attribute. |
| B7 | Propose `harvest.shard.id` as `workflow.shard.id`. | Rejected. Sharding is an engine detail. |
| B8 | Map the ADR 0001 names to the generic names in a table. | **Adopted.** Harvest users can read the proposal against their spans. |
| B9 | Open the upstream issue from this change. | Rejected. Submission is a maintainer action. The page records its status and holds the issue text. |
| B10 | Keep the ADR kind for each span, even where Dapr differs. | **Adopted.** The draft describes shipped practice. The kind question is open upstream. |

### 0.3 Reverse brainstorm — how can this change do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | The proposal drops one of the eight ADR spans. | A guard maps each ADR 0001 §2 span to a proposed span. |
| R2 | A proposed span has a kind other than its ADR span. | The same guard compares the kinds. |
| R3 | A new `ATTR_*` constant ships with no mapping. | A guard reads every `ATTR_*` constant. Each needs a target or a reason. |
| R4 | A Harvest name leaks into the generic model. | A guard rejects `harvest` in the YAML model. |
| R5 | The YAML is not valid Weaver shape. | A guard checks group types, ids, stability, briefs, kinds and refs. |
| R6 | A span refers to an attribute that the registry does not define. | A guard resolves each `ref`. Only `error.type` comes from outside. |
| R7 | The page tables drift from the YAML. | A guard compares the page tables with the model ids. |
| R8 | The replay rule is lost in the generalization. | A guard needs `workflow.replay` on the execute span and a normative replay section. |
| R9 | The page claims a submission that did not happen. | The status line must be `pending`, `submitted` with an upstream link, or `declined` with a reason. |
| R10 | Long sentences make the page hard to read. | A guard caps prose sentences and YAML briefs at 25 words. |
| R11 | A docs-only change skips the `test` matrix. | The `lint` job runs the guard suite. A guard checks the step. |
| R12 | Nobody finds the page. | ADR 0001 and `docs/telemetry.md` link it. A guard checks the ADR link. |

### 0.4 Six thinking hats

| Hat | Notes |
|-----|-------|
| White | ADR 0001 defines eight spans, seven core attributes and a replay rule. Upstream has no workflow area. Dapr works on its own model. |
| Red | Engine authors want one shared vocabulary. A dashboard that works across engines is attractive. Upstream review is slow, which feels risky. |
| Black | Upstream needs a group and a prototype. One small engine cannot carry a new area alone. Temporal and Dapr use other terms and kinds. |
| Yellow | The cost is low. The model is ready to file. Harvest gains standing in a standard that does not exist yet. |
| Green | Invite Dapr as co-proposer through dapr/dapr#9213. Offer the replay rule as the distinct contribution. |
| Blue | Ship the draft, the model and the guards. Record the submission as `pending` until a maintainer files it. |

### 0.5 Decisions

- Page: `docs/otel-workflow-semconv/README.md`. Model:
  `docs/otel-workflow-semconv/model/workflow/`.
- Namespace: `workflow.*`. Stability: `development` for every group.
- Span id: `span.workflow.<operation>.<kind>`. Span name:
  `{workflow.operation.name} {target}`.
- Replay: the execute span carries `workflow.replay`. A replayed operation
  emits no second span.
- Submission: a GitHub issue in `open-telemetry/semantic-conventions`,
  filed by a maintainer. The page holds the issue text.

---

## 1. TDD map

| Phase | Test | Proves |
|-------|------|--------|
| Red → green | `proposal_cites_the_issue_and_its_sources` | The page cites #2015, #1838 and ADR 0001. |
| Red → green | `every_adr_span_maps_to_a_proposed_span_of_the_same_kind` | R1, R2. |
| Red → green | `every_attr_constant_is_mapped_or_excluded_with_a_reason` | R3. |
| Red → green | `the_model_is_generalized_and_well_named` | R4. |
| Red → green | `the_model_has_the_weaver_shape` | R5, R6. |
| Red → green | `the_page_tables_match_the_model` | R7. |
| Red → green | `the_operation_enum_names_one_span_each` | B5. |
| Red → green | `the_replay_rule_is_normative` | R8. |
| Red → green | `the_submission_status_is_recorded` | R9. |
| Red → green | `prose_sentences_and_briefs_stay_short` | R10. |
| Red → green | `guards_run_on_docs_only_changes` | R11. |
| Red → green | `the_adr_and_the_telemetry_page_link_the_proposal` | R12. |
| Refactor | `helpers_parse_their_inputs` | The table and heading parsers. |

## 2. Acceptance criteria

| AC | Evidence |
|----|----------|
| A draft proposal exists in `docs/` | `docs/otel-workflow-semconv/README.md` and its Weaver model, pinned by `otel_workflow_semconv_docs`. |
| Submitted upstream, or a recorded decision not to | The `**Submission status:**` line of the page, checked by `the_submission_status_is_recorded`. |
