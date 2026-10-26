## Fix — Calendar tests the schedule's local date (issue #2332)

A calendar skip policy tested the UTC date of a fire time. A `CronInTimezone` schedule outside about UTC±5h then met the wrong day. A Mon–Fri 08:00 `Asia/Tokyo` schedule with `weekends-off` lost its Monday fire.

`calendar::schedule_local_date` now returns the date in the schedule's timezone. Other schedules keep the UTC date. These paths use it: firing preview, backfill planning, the scheduler tick (pre-check and dispatch loop), and both overdue-schedule resolvers (one now delegates to the other).

The shift helper is now one function. A shifted local time inside a DST gap moves forward to the first valid time on the same local day. It used to fall back to the UTC time and could land a day late.

Upgrade note: a timezone schedule with a calendar can skip or shift different days than before. The new days match the local calendar. No migration is needed. See `docs/upgrading/0.8.0.md` section 1.13.

Tests: Tokyo and Los Angeles weekday previews, a local-day holiday, a local-day shift, backfill, the pure overdue resolver, a DST-gap shift, and the helper fallbacks.
