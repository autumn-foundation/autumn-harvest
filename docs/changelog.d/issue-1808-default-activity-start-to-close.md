## Feature — Default activity start-to-close timeout (issue #1808)

**Behavior change.** `WorkerConfig::default()` now sets
`default_activity_start_to_close` to `DEFAULT_ACTIVITY_START_TO_CLOSE`
(10 minutes). Before, the field was `None`. A regular activity with no timeout
of its own could then hang on a live worker forever and hold a slot.

The value uses the #620 precedence: call-site override, then
`#[activity(start_to_close = ...)]`, then this default. The worker stores the
result on the task row at enqueue time. The existing timeout scanner enforces
it. A task enqueued before the upgrade keeps its stored value.

**Scope.**

- The default also applies to an activity with a `heartbeat_timeout`. An
  auto-heartbeat keeps a stuck attempt alive (issue #682), so only
  `start_to_close` can stop it.
- Local activities do not change. The 60 s local cap still bounds them.
- Reserved session activities keep no default, as in #620.
- External activities do not change. They use no task row.

**Opt-out.** `WorkerConfig::without_default_activity_start_to_close()` removes
the default. `with_default_activity_start_to_close(..)` replaces it.
`GET /admin/config` reports the active value as
`default_activity_start_to_close_ms`.

**Startup warning.** `HarvestBuilder::try_build` logs one `WARN` line. The
line names each regular activity type with no `start_to_close`, no
`schedule_to_close` and no `heartbeat_timeout`. It also gives the default that
bounds them, or says that they can run forever when the default is off. WASM
activities are not listed. Their runtime wall-clock ceiling bounds them. The
warning never blocks the build.

No migration. No new `WorkflowEvent` variant. The change does not touch
`harvest_events`.

**Upgrade.** See `docs/upgrading/0.7.0.md`, section 1.2.

**Tests.** `tests/integration/activity_default_timeout_tests.rs` builds the
registry through `HarvestBuilder` and runs a real worker:

- An activity that never returns gets a 600 s `start_to_close` on its row. At
  590 s the scanner leaves it alone. At 601 s the scanner reclaims it as
  `StartToClose`, and the workflow fails with `ActivityTimedOut`. Before the
  change, the scanner never reclaimed it.
- With the default removed, the row has no timeout. A day-old attempt is
  still not reclaimed.

Unit tests in `builder.rs` cover the default, the opt-out, the list of
unbounded types and the warning text, with the default on and off.
`effective_config.rs` covers the reported value.
