## SQLite runs the `#[activity]` body with an `ActivityContext` (issue #2024)

**Breaking for `autumn-harvest-sqlite`.** `SqliteRuntime::register_activity`
now takes only `&ActivityInfo`. It awaits the `#[activity]` body on the drive's
async runtime, with a real `ActivityContext`. One activity fn now runs
unchanged on SQLite and on Postgres. The old two-argument form is
`register_activity_with_body(&info, closure)`. It runs the same audit and the
same declared defaults. `register_activity_raw` and `ActivitySpec::new` do not
change.

**`ActivityContext` values.** `info()` matches the Postgres worker for one
process: the run, the workflow type and id, the activity type and id, the
1-based `attempt`, the frozen `max_attempts`, `is_local = false`, the task
queue and the task row id. `deadline()` is the earlier of claim time plus
`start_to_close` and the `schedule_to_close` deadline.
`previous_failure()` is the previous attempt's error message.
`idempotency_key()` derives from the activity id, as on Postgres. The
cancellation token never fires on SQLite. `deadline()` is on the real clock,
also under the `_as_of` drivers.

**Known differences.** With no declared `retry`, SQLite runs one attempt and
reports `max_attempts() == 1`. Postgres applies its engine default. A re-run
after a crash keeps its `attempt` number. `state()`, `headers()` and
`metrics()` are empty on SQLite.

**Dropped drive.** A caller can drop the drive future while a body runs. The
next drive of that run now reclaims the `RUNNING` task and re-runs the body,
as after a crash.

**Heartbeats.** `ctx.heartbeat(..)` stores the latest payload on the task row.
A retry, or a reopen after a crash, reads it back with
`ctx.heartbeat_details()`. `heartbeat_timeout` is still rejected at
registration. No scanner times out a silent attempt yet.

**Core.** `ActivityContext::new_embedded` is a new hidden constructor for an
embedded backend. It is not for activity authors.

**Schema.** `harvest_tasks` gains two nullable SQLite columns,
`heartbeat_details` and `last_error`. `schema::migrate` adds them to an old
file. No Postgres migration. No new `WorkflowEvent` variant.

**Tests.** `autumn-harvest-sqlite/tests/integration/activity_context.rs`
covers the body, `info()`, retries, heartbeats across a retry and a crash, and
panic containment. `portable_fixture.rs` holds one `#[activity]` fn.
`portable_activity.rs` runs it on SQLite, and the core suite
`sqlite_portable_activity_tests` runs it on Postgres. Both assert the same
report. Every raw-closure test passes. The two-argument tests changed only the
method name. Design record: `DESIGN-2024.md`.
