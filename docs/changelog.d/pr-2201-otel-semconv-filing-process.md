## Docs — ready the OTel workflow draft for the upstream process (issue #2015)

**What shipped.** The draft now follows the upstream process for new
conventions. That process has two gates. An issue finds co-owners. Then
a community project proposal gets a TC review and a GC vote.

- Section 9 tells the maintainer to use the upstream `new-conventions.yaml`
  issue form, with the area `area:new` and a fixed title.
- New `docs/otel-workflow-semconv/project-proposal.md` drafts the project
  proposal from the community `project-template.md`. Each staffing slot
  says `Open.` until a person agrees. Its links are absolute.
- Section 10 lists the steps in order: CLA, check, issue, status,
  co-owners, project proposal, then the upstream pull request after the
  TC and the GC. A decline keeps a separate-registry fallback.
- A "Facts checked" table dates each external fact that the page and the
  proposal cite.

**Prior art.** `open-telemetry/semantic-conventions#1688` proposes general
`workflow.*` attributes with the same `workflow.name` and
`workflow.run.id`. The draft now cites it and aligns with it.

**Fact fixes.** `dapr/dapr#9213` opened in November 2025, adds a Weaver
model, and closed unmerged in April 2026. Open question 8 no longer says
that `db.system.name` has `temporal` or `dapr` members.

**Script.** `scripts/check-otel-workflow-semconv.sh` pins a community
commit. At the pins, it checks the issue form and the template sections of
the proposal. A copy without one section must fail. `SEMCONV_LATEST=1`
also reads community `main`.

**Tests.** New guards: `the_issue_text_fits_the_upstream_form`,
`the_project_proposal_covers_the_template`,
`every_cited_link_is_a_checked_fact`,
`the_submission_steps_follow_the_upstream_process` and
`the_draft_cites_the_overlapping_upstream_issue`. Extended guards:
`the_weaver_check_script_mirrors_upstream`,
`the_page_records_the_weaver_check` and
`prose_sentences_and_yaml_texts_stay_short`.

**Submission.** The status stays `pending`. A maintainer files the issue.

**Invariants.** No migration. No new `WorkflowEvent` variant. No engine
change.
