## Engine — Default activity start-to-close timeout (issue #1808)

**Breaking default.** `WorkerConfig::default()` now sets
`default_activity_start_to_close` to `DEFAULT_ACTIVITY_START_TO_CLOSE`
(10 minutes). Before, the field was `None`. With that value, a regular
activity with no timeout of its own could hang on a live worker forever and
hold a slot.

The value uses the #620 precedence: call-site override, then
`#[activity(start_to_close = ...)]`, then this default. The worker stores the
result on the task row at enqueue time. The existing timeout scanner enforces
it. A timeout fails the activity call with `ActivityTimedOut` and does not
retry. A task enqueued before the upgrade keeps its stored value.

**Scope.**

- The default skips an activity type that declares a `schedule_to_close` or a
  `heartbeat_timeout`. Each already bounds a running attempt. The same rule now
  applies to a default set with `with_default_activity_start_to_close`. Before,
  the #620 floor also applied to these types.
- An auto-heartbeat keeps a stuck attempt alive (issue #682). The activity
  guide tells authors to pair it with a `start_to_close`.
- A local activity also gets the default, and the local cap still applies.
  At the 60 s default cap, nothing changes.
- A WASM activity also gets the default. At the 300 s default
  `max_wall_clock`, nothing changes.
- Reserved session activities keep no default, as in #620.
- External activities do not change. A `harvest_external_tasks` row has its
  own `schedule_to_close`.

**Opt-out.** `WorkerConfig::without_default_activity_start_to_close()` removes
the default. `with_default_activity_start_to_close(..)` replaces it.
`GET /admin/config` reports the active value as
`worker.default_activity_start_to_close_ms`.

**Startup warning.** `HarvestBuilder::try_build` logs one `WARN` line. The
line names each regular activity type that the default governs: no
`start_to_close`, no `schedule_to_close` and no `heartbeat_timeout`. It gives
the default that bounds them. With the default off, it says that they can run
forever. The warning omits WASM activity types and uses the last registration
of a name, as the registry does. The warning never blocks the build.

`ActivityInfo::declares_attempt_bound` is the one rule for the warning and
the worker.

No migration. No new `WorkflowEvent` variant. The change does not touch
`harvest_events`.

**Upgrade.** See `docs/upgrading/0.7.0.md`, section 1.2.

**Tests.** `tests/integration/activity_default_timeout_tests.rs` builds the
registry through `HarvestBuilder` and runs a real worker:

- An activity that never returns gets a 600 s `start_to_close` on its row. At
  590 s the scanner leaves it alone. At 601 s the scanner reclaims it as
  `StartToClose`. The workflow fails with exactly one `ActivityTimedOut`, and
  the worker drops the hung future. Before the change, the scanner never
  reclaimed the task.
- With the default removed, the row has no timeout. The scanner does not
  reclaim even a day-old attempt.
- A declared `start_to_close` wins over the default.
- A `heartbeat_timeout` or a `schedule_to_close` type gets no default.

Unit tests in `builder.rs` cover the default, the opt-out, the list of
governed types, duplicate names and the warning text, with the default on and
off. `effective_config.rs` covers the reported value.
