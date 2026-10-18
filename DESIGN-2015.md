# Design — Issue #2015: propose workflow span conventions to OpenTelemetry

Issue #2015 asks for a standards play. OpenTelemetry has no workflow
semantic conventions. ADR 0001 already defines eight span kinds, the
`harvest.*` attributes and a replay rule. The deliverable is a draft
proposal in `docs/`, generalized away from Harvest names. A maintainer
then submits it upstream or records a decision not to submit.

**No migration. No new `WorkflowEvent` variant. No engine change.** The
change adds a proposal page, a Weaver model, a guard suite and one CI step.

---

## 0. Planning record

### 0.1 Facts found before the plan

- ADR 0001 §2 lists eight spans. Each has a kind: `INTERNAL`, `PRODUCER`
  or `CONSUMER`.
- `telemetry.rs` defines more `ATTR_*` constants than the seven in ADR 0001
  §1. The signal attributes came later, and no span emits them yet.
- The code also emits `harvest.timer.start`. ADR 0001 does not list it.
- ADR 0001 §4 is the replay rule. A replayed run gets a new root span with a
  link to the original. A replay emits no activity spans.
- The OTel attribute registry has no `workflow` namespace. The nearest
  prior art is `cicd.pipeline.*`, which models CI/CD pipeline runs.
- Upstream conventions are Weaver YAML under `model/<area>/`, with Markdown
  under `docs/<area>/`. A new area needs a group of committed owners.
- Dapr began to codify its workflow telemetry with Weaver in 2026
  (dapr/dapr#9213). The OTel blog post on that work proposes `PRODUCER`
  spans to schedule and `CONSUMER` spans to execute.
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
| B10 | Keep the ADR kind for each span, even where Dapr differs. | Adopted, then reversed in review. The trace API defines `CONSUMER` for work that a producer starts. The execute spans are `CONSUMER`, and the mapping notes the Harvest kind. |
| B11 | Add a `workflow.system.name` attribute. | **Adopted in review.** Upstream naming asks for a system attribute in an area with several systems. |

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
| R10 | Long sentences make the page hard to read. | A guard caps prose sentences, the issue text and every YAML text at 25 words. |
| R11 | A docs-only change skips the `test` matrix. | The `lint` job runs the guard suite. A guard checks the step. |
| R12 | Nobody finds the page. | ADR 0001 and `docs/telemetry.md` link it. A guard checks both links. |
| R13 | The page claims that Harvest conforms where it does not. | Section 7 lists each known gap: span links, parents, attributes. |
| R14 | A hand copy in the issue text drifts from the model. | A guard needs each attribute and operation in the issue text. The prose states no counts. |

### 0.4 Six thinking hats

| Hat | Notes |
|-----|-------|
| White | ADR 0001 defines eight spans, seven core attributes and a replay rule. Upstream has no workflow area. Dapr works on its own model. |
| Red | Engine authors want one shared vocabulary. A dashboard that works across engines is attractive. Upstream review is slow, which feels risky. |
| Black | Upstream needs a group and a prototype. One small engine cannot carry a new area alone. Temporal and Dapr use other terms and kinds. |
| Yellow | The cost is low. The model is ready to file. Harvest gains standing in a standard that does not exist yet. |
| Green | Invite Dapr as co-proposer through dapr/dapr#9213. Offer the replay rule as the distinct contribution. |
| Blue | Ship the draft, the model and the guards. Record the submission as `pending` until a maintainer files it. |

### 0.5 Review record

Four review agents read the change: upstream format, fidelity to the
code, guard quality, and style. The fixes:

- The execute spans are `CONSUMER` (B10). `workflow.system.name` is new
  (B11). Each span carries `sampling_relevant` marks.
- A run id is conditionally required where an engine can lack it.
- `send_signal` has a span-name fallback.
- `workflow.replay` now states its meaning for a routine resume. That
  meaning matches Harvest.
- Section 7 lists where Harvest does not conform (R13). ADR 0001 §2.6 and
  §2.7 describe parents that the code does not set. That ADR error is out
  of scope here.
- The guards read numbered replay rules, wrapped constants, the type and
  kind columns, duplicate ids and the issue text. Five mutations confirm
  that each new check fails on a bad edit.
- The Codex review on the PR found that rule 2 let a replay emit the
  schedule spans again. Rule 2 now covers each operation that history
  records. The replay guard needs the schedule spans in that rule.
- Codex also found that a span could carry another span's operation value,
  that `error.type` was missing on some spans, and that the run id rule was
  too weak. Each span ref now pins its operation value. Each span carries
  `error.type` when its operation ends in an error. Guards check both.
- A later Codex round asked for start and end boundaries and an error
  status on each span. Each span note now states both, and a guard checks
  them. It also proposed an event for `fire_timer`. That changes the ADR
  span set, so it is open question 9 and goes to the maintainer.
- The next Codex round found that the end boundaries covered success only,
  and that `send_signal` used a narrower run id condition. Each end
  boundary now covers a failure, and every optional run id uses one
  condition. A guard checks both.
- Codex then found that the signal spans did not require the signal name.
  Their span names carry the workflow name only. Both now require
  `workflow.signal.name`, and a guard checks it.
- Codex found that the issue text linked the moving `trunk-dev` branch.
  The link now holds a `<commit>` placeholder, and the steps say to fill
  it. A guard rejects a branch link.
- Codex found that `fire_timer` could start before its parent cycle
  existed. The `fire_timer` and `deliver_signal` spans now start inside
  the `execute_workflow` cycle. A guard checks each child span.
- Codex found that rule 2 also forbade the first live `deliver_signal` or
  `fire_timer` span, when an engine writes the event first. Rule 2 now
  covers only what a replay cycle reads from history.

### 0.6 Decisions

- Page: `docs/otel-workflow-semconv/README.md`. Model:
  `docs/otel-workflow-semconv/model/workflow/`.
- Namespace: `workflow.*`. Stability: `development` for every group.
- Span id: `span.workflow.<operation>.<kind>`. Span name:
  `{workflow.operation.name} {target}`. The execute spans are `CONSUMER`.
- Replay: the execute span carries `workflow.replay`. A replayed operation
  emits no second span.
- Submission: a GitHub issue in `open-telemetry/semantic-conventions`,
  filed by a maintainer. The page holds the issue text.

---

## 1. TDD map

| Phase | Test | Proves |
|-------|------|--------|
| Red → green | `proposal_cites_the_issue_and_its_sources` | The page cites #2015, #1838 and ADR 0001. |
| Red → green | `every_adr_span_maps_to_its_own_proposed_span` | R1, R2. A changed kind needs a note. |
| Red → green | `every_attr_constant_is_mapped_or_excluded_with_a_reason` | R3. |
| Red → green | `the_model_is_generalized_and_well_named` | R4. |
| Red → green | `the_model_has_the_weaver_shape` | R5, R6. |
| Red → green | `the_page_tables_match_the_model` | R7, R14. |
| Red → green | `the_operation_enum_names_one_span_each` | B5. |
| Red → green | `the_replay_rule_is_normative` | R8. |
| Red → green | `the_submission_status_is_recorded` | R9. |
| Red → green | `prose_sentences_and_yaml_texts_stay_short` | R10. |
| Red → green | `guards_run_on_docs_only_changes` | R11. |
| Red → green | `the_adr_and_the_telemetry_page_link_the_proposal` | R12. |
| Refactor | `helpers_parse_their_inputs` | The table, heading, list, paragraph and fence parsers. |

## 2. Acceptance criteria

| AC | Evidence |
|----|----------|
| A draft proposal exists in `docs/` | `docs/otel-workflow-semconv/README.md` and its Weaver model, pinned by `otel_workflow_semconv_docs`. |
| Submitted upstream, or a recorded decision not to | The `**Submission status:**` line of the page, checked by `the_submission_status_is_recorded`. |

---

## 3. Follow-up: the upstream Weaver check

### 3.1 Facts found before the plan

- Issue #2015 stays open after PR #2158. The status is `pending`.
- Section 10 of the page said that the guards did not run Weaver. Upstream
  runs `make check-policies` on each pull request.
- The `make check-policies` recipe on upstream `main` pins the Weaver
  image by digest. It also pins the policy package commit.
- The recipe downloads its baseline archive inside the container. A
  proxied sandbox cannot do that. Its baseline also floats to the latest
  release.
- A local run of the same checks passed. It used Weaver 0.28.0, upstream
  `main` at `331541c`, and the `v1.44.0` baseline. The model added no
  warning.
- An unresolved `ref` in a copy of the model fails the same run.

### 3.2 Brainstorm — how can the check be done?

| # | Idea | Verdict |
|---|------|---------|
| F1 | Run upstream `make check-policies` as it is. | Rejected. It needs network in the container, and its baseline floats. |
| F2 | A script that mirrors the recipe with local mounts. It pins an upstream commit and reads the image and package pins from that tree. | **Adopted.** |
| F3 | Add the script to the required `lint` job. | Rejected. Each PR then pulls an image and clones three trees. |
| F4 | A separate workflow that runs when the model or the script changes. | **Adopted.** The `sandbox-kvm.yml` workflow uses the same pattern. |
| F5 | Record the result on the page and in the issue text. | **Adopted.** Reviewers upstream see that the model passes. |
| F6 | Fill the `<commit>` placeholder now. | Rejected. This change edits the draft, so its merge commit is the one to cite. |
| F7 | File the upstream issue from this session. | Rejected. Filing is a maintainer action. This session has no access to upstream. |
| F8 | Run the full upstream `make check`. | Rejected. The generated Markdown belongs to the upstream pull request. |
| F9 | Run a broken copy of the model, and expect a failure. | **Adopted.** It proves that the check reads the model and runs the policies. |

### 3.3 Reverse brainstorm — how can the check do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| H1 | The script checks the upstream tree without the model, and passes. | F9. The broken copy must fail on the upstream naming policy. |
| H2 | Upstream `main` moves and breaks the check. | The script pins a full commit SHA. |
| H3 | The page names one commit and the script checks another. | A guard compares the two. |
| H4 | The page claims a check that no workflow runs. | A guard reads the workflow, its triggers and its step. |
| H5 | The image is not pinned. | The script refuses an image with no `@sha256:` digest. |
| H6 | A failed command does not fail the script. | `set -euo pipefail`. A guard checks it. |
| H7 | The pin gets old, and the result misleads. | Section 10 says to move the pin and run the check before a maintainer files the issue. |
| H8 | The issue text claims more than the check proves. | The text names the checks, not upstream acceptance. |

### 3.4 Six thinking hats

| Hat | Notes |
|-----|-------|
| White | The model passes the upstream checks at a pinned commit. The page says the guards do not run Weaver. |
| Red | A proposal that passes upstream checks reads as serious work. |
| Black | Upstream can change its policies. The check then needs a new pin. Three clones make the job slow. |
| Yellow | The check finds a format defect before upstream review does. It costs one small workflow. |
| Green | The negative control proves that run 1 reads the model. |
| Blue | Add the script, the workflow and the page record. Keep the status `pending`. Ask the maintainer to file. |

### 3.5 TDD map

| Phase | Test | Proves |
|-------|------|--------|
| Red → green | `the_weaver_check_script_mirrors_upstream` | F2, F9, H1, H2, H5, H6. |
| Red → green | `the_weaver_check_runs_when_the_model_changes` | F4, H4. |
| Red → green | `the_page_records_the_weaver_check` | F5, H3, H7, H8. |

### 3.6 Review record

Four review agents read the change: the script, the guards and CI, the
docs, and fidelity to upstream. Codex also reviewed the pull request. The
fixes:

- The workflow runs when a draft becomes ready. Codex and two agents found
  this gap.
- The negative control is a namespace collision, not an unresolved `ref`.
  An unresolved `ref` fails before the policies run.
- Run 1 fails on a Weaver warning for the model. The page claim is now
  checked.
- The script requires a full SHA for the policy packages and checks for
  Docker first.
- A weekly `latest` job runs the script against upstream `main`. It finds
  a stale pin before a maintainer moves it.
- The guards read code lines, not comments. They read each pin from the
  start of a line. They check the job gating, the baseline and the step
  order.
- The enum `workflow.operation.name` has no `examples`. Upstream renders
  them in each span table, where they contradicted the span value.
- Section 8 gains the operation attribute and the system values that an
  upstream reviewer can raise. Section 10 lists the files that the upstream
  pull request needs.

---

## 4. Follow-up: ready the filing for the upstream process

### 4.1 Facts found before the plan

- PRs #2158 and #2174 merged the draft and the Weaver check. The status
  is still `pending`. The only open AC is the filing, or a decision not to
  file.
- The pins are current. On 2026-10-18, upstream `main` is `331541c` and
  the latest release is `v1.44.0`. The script passes.
- Upstream has an issue form for new conventions,
  `.github/ISSUE_TEMPLATE/new-conventions.yaml`. It asks for an area,
  with `area:new` for a new one, and a "Propose new conventions" text.
  Section 9 does not follow that form.
- The form and `CONTRIBUTING.md` point to the community
  `project-management.md`. New conventions need a project proposal. A pull
  request adds it to `open-telemetry/community/projects/`. A majority of
  the GC must approve it. Section 10 says only "read the project process".
- The proposal follows `project-template.md`. Each section that is not
  "Optional" or "Post-Approval" must have text.
- `dapr/dapr#9213` is a pull request, not an issue. It closed as stale on
  2026-04-23 and did not merge. Its text does not name Weaver. The OTel
  blog post on Dapr workflow observability says that Dapr is adopting
  Weaver. It also says that none of the span changes are in Dapr yet.
- On 2026-10-18, a search of the upstream issues found no workflow engine
  proposal.
- `CONTRIBUTING.md` also offers a path outside upstream. Conventions that
  one library owns live in that library's repository. Weaver work on
  decentralized conventions is in `open-telemetry/weaver#215`.

### 4.2 Brainstorm — what moves the filing forward?

| # | Idea | Verdict |
|---|------|---------|
| P1 | File the upstream issue from this session. | Rejected. The session has no access to upstream. Filing is a public action for a maintainer. |
| P2 | Record a decision not to file. | Rejected. That decision belongs to the maintainer. |
| P3 | Fit section 9 to the upstream issue form: the form, the area and the field. | **Adopted.** |
| P4 | Draft the community project proposal from `project-template.md`. | **Adopted.** It is the next gate after the issue. |
| P5 | Name leads and sponsors in the proposal. | Rejected. Nobody agreed yet. Each open slot says `Open.` |
| P6 | Correct the Dapr facts, and record each external fact with the date of the check. | **Adopted.** |
| P7 | Rewrite section 10 as the real order: check, issue, project proposal, upstream pull request. | **Adopted.** |
| P8 | Check the issue form and the project template at a pinned upstream commit in the script. | **Adopted.** The weekly `latest` job finds a changed template. |
| P9 | Stage the upstream pull request files: the generated docs, `.chloggen`, `areas.yaml`. | Rejected. They wait for an approved project, and upstream regenerates them. |
| P10 | Give the decline path a fallback: publish the model as an own Weaver registry. | **Adopted.** One paragraph in section 10. |
| P11 | Fill the `<commit>` placeholder now. | Rejected, as in F6. This change edits the draft. |

### 4.3 Reverse brainstorm — how can this change do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| K1 | The issue text states a false fact about another project. | Each link in the issue text has a row in a "Facts checked" table, with a date. |
| K2 | The proposal claims staff or sponsors that did not agree. | Each staffing slot names a GitHub handle or starts with `Open.` A guard checks it. |
| K3 | Upstream adds a required template section, and the proposal misses it. | The script compares the template at the pin with the proposal. The `latest` job reads `main`. |
| K4 | The form loses `area:new`, and section 9 tells the maintainer to pick it. | The script checks the form at the pin. |
| K5 | The page and the script pin different community commits. | A guard compares them. |
| K6 | Section 10 puts the project proposal before the issue that finds co-owners. | A guard checks the step order. |
| K7 | The proposal is an orphan page. | The README links it. A guard checks the link. |
| K8 | Long sentences return in the new page. | The sentence guard reads the proposal too. |
| K9 | The proposal hides that Harvest does not yet emit `workflow.*`. | The deliverables say so, and link section 7. |

### 4.4 Six thinking hats

| Hat | Notes |
|-----|-------|
| White | The draft passes the upstream checks. Upstream needs an issue, then a project proposal with leads and sponsors. The Dapr work is a closed pull request. |
| Red | A complete packet makes filing feel cheap. Empty staffing slots feel exposed, but they are true. |
| Black | One engine cannot staff a project. The project can stall at sponsorship. The Dapr claim was wrong, so other claims need a check too. |
| Yellow | The maintainer files in minutes. Co-owners see the full plan. The packet is honest about gaps. |
| Green | Use the issue to recruit co-owners. Invite the authors of `dapr/dapr#9213` there. Keep an own-registry fallback. |
| Blue | Fit the form, draft the proposal, record the facts, order the steps. Keep the status `pending`. |

### 4.5 TDD map

| Phase | Test | Proves |
|-------|------|--------|
| Red → green | `the_issue_text_fits_the_upstream_form` | P3. |
| Red → green | `the_project_proposal_covers_the_template` | P4, P5, K2, K7, K9. |
| Red → green | `every_issue_text_link_is_a_checked_fact` | P6, K1. |
| Red → green | `the_submission_steps_follow_the_upstream_process` | P7, P10, K6. |
| Red → green | `the_weaver_check_script_mirrors_upstream` (extended) | P8, K3, K4. |
| Red → green | `the_page_records_the_weaver_check` (extended) | K5. |
| Red → green | `prose_sentences_and_yaml_texts_stay_short` (extended) | K8. |
