## Docs — ready the OTel workflow draft for the upstream process (issue #2015)

**What shipped.** The draft now follows the upstream process for new
conventions. That process has two gates: an issue that finds co-owners,
then a community project proposal that the GC approves.

- Section 9 tells the maintainer to use the upstream `new-conventions.yaml`
  issue form, with the area `area:new`.
- New `docs/otel-workflow-semconv/project-proposal.md` drafts the project
  proposal from the community `project-template.md`. Each staffing slot
  says `Open.` until a person agrees.
- Section 10 lists the real steps in order: check, issue, status, project
  proposal, upstream pull request. A decline keeps an own-registry fallback.
- A "Facts checked" table dates each external fact that section 9 cites.

**Fact fix.** `dapr/dapr#9213` is a pull request that closed unmerged in
April 2026. The page called it Dapr's Weaver work. The OTel blog post is
now the source for Dapr's Weaver adoption.

**Script.** `scripts/check-otel-workflow-semconv.sh` pins a community
commit. At the pins, it checks the issue form and the template sections
of the proposal. `SEMCONV_LATEST=1` also reads community `main`.

**Tests.** Four new guards: `the_issue_text_fits_the_upstream_form`,
`the_project_proposal_covers_the_template`,
`every_issue_text_link_is_a_checked_fact` and
`the_submission_steps_follow_the_upstream_process`. Three guards are
extended: the script pins, the page record and the sentence length.

**Submission.** The status stays `pending`. A maintainer files the issue.

**Invariants.** No migration. No new `WorkflowEvent` variant. No engine
change.
