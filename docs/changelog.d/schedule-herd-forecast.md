## Phase X.Y — Schedule herd forecast

The schedule herd forecast finds the moments when many schedules fire
together. Twenty schedules on `0 * * * *` each pass validation, but the
scheduler then starts twenty runs at the top of each hour.

- `GET /admin/schedules/herd` reads every schedule and reports the herds in
  a window. It takes `from`, `hours`, `bucket_secs`, `min_herd` and `limit`.
  It is read-only, and a shard that does not answer makes it `partial`.
- `harvest schedule herd` calls the endpoint. It prints a table of the herds
  and of the schedules to move first. Pass `--output json` for the raw body.
- The report applies the jitter of each schedule, as the scheduler does. It
  lists paused, auto-paused and exhausted schedules as `inactive`. Interval
  schedules have no fixed phase, so it lists them as `unanchored`.
- Calendar exclusions and the `end_at` cutoff are not applied yet.
- `schedule_herd::forecast` and `forecast_shifted` hold the arithmetic. They
  are pure and send no SQL.
- The SQLite backend answers `501` for the endpoint. It has no port yet.
