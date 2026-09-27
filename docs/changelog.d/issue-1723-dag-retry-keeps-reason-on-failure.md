## Fix — Vantage DAG retry keeps the typed reason when the commit fails (issue #1723)

Before this fix, a failed retry commit redirected to the DAG
detail page with a flash. The reason the operator typed was lost.
A common cause is a stale confirm page: the run changed between the dry run
and the commit.

Now `POST /ui/dags/{dag_name}/runs/{run_exec_id}/retry` renders the confirm
page again in place when a commit starts no run (400/404/409 or an
internal error):

- A focused `degraded-banner` shows "This request did not start a retry."
  and the error. The text does not claim that no run exists, because a
  double click can fork on the first POST.
- A fresh dry run supplies the current node list. If it passes, the form
  shows again with the operator's reason in the textarea. The banner says
  that the node list is current.
- If the fresh dry run also fails, no form shows. The dry-run message joins
  the same banner when it differs. The reason stays on the page in a
  read-only field, so the operator can copy it.

Success and `AuditFailed` still redirect to the new run. `AuditFailed` must
not show the form, because a second click would fork a second run.

The pure decision is now `dag_retry_commit_next`, which returns
`DagRetryCommitNext::{Redirect, Redisplay}`. `dag_retry_dry_run` is the one
dry-run call for the GET page and the redisplay.

**Design choice.** The reason is not put in a redirect URL. A long reason
could go over the request-header limit and lose the data again. A URL would
also copy the reason into access logs and browser history. The schedule
backfill form (`BackfillFormEcho`) already uses the same in-place render.

No new `WorkflowEvent` variant, no migration, no API change. A dry run
writes nothing, so the redisplay adds no audit row. Audit rows for the
commit itself do not change.

**Tests.** Unit tests in `ui::tests` cover the decision for every failure
variant. They also cover the echo with a passing and a failing dry run,
message deduplication, the GET page, and HTML escaping of the reason. The
integration test `ui_dag_retry_commit_failure_keeps_entered_reason` in
`tests/ui_integration.rs` needs Docker and runs in CI.
