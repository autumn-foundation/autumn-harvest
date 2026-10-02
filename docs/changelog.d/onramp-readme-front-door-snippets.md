## Phase X.Y — README cron-schedule and retry snippets compile

The README's "Scheduling workflows on a cron" snippet called `.workflows(..)`
and `.workflow_schedule(..)` on `autumn_web::app()`. That builder has neither
method, so the snippet failed with `E0599`. It now uses `HarvestBuilder`, the
type that owns `workflow_schedule`, and states that `HarvestPlugin` users
create schedules with the CLI or HTTP API. The `ActivityFailure` retry snippet
now imports `std::time::Duration`.

Docs only. No public API changes.

Test evidence: `scripts/check-readme-front-door-snippets.sh` (2 compile errors
to 0, wired into CI).
