## Fix — Vantage DAG retry keeps the typed reason when the commit fails (issue #1723)

Before this fix, a failed retry commit (400/404/409) redirected to the DAG
detail page with a flash. The reason the operator typed was lost.
A common cause is a stale confirm page: the run changed between the dry run
and the commit.

Now `POST /ui/dags/{dag_name}/runs/{run_exec_id}/retry` renders the confirm
page again in place on a genuine failure:

- A focused `degraded-banner` shows "Retry not started." and the error.
- A fresh dry run supplies the current node list. If it passes, the form
  shows again with the operator's reason in the textarea.
- If the fresh dry run also fails, no form shows. The reason stays on the
  page in a read-only field, so the operator can copy it.

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
writes nothing, so the failure path adds no audit row.

**Tests.** Unit tests in `ui::tests` cover the decision for every failure
variant, the echo with a passing and a failing dry run, message deduplication,
the unchanged GET page, and HTML escaping of the echoed reason. The
integration test `ui_dag_retry_commit_failure_keeps_entered_reason` in
`tests/ui_integration.rs` needs Docker and runs in CI.
