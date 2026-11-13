## Phase X.Y — Throttle forecast

`throttle_forecast` tells a workflow author how a `ThrottlePolicy` paces a
burst of starts, before deploy.

- `forecast` replays arrival bursts through the token bucket. It reports the
  first and the last admission of each burst, its longest wait, and the
  starts that `schedule_to_start` drops.
- `forecast_schedules` walks real cron and interval schedules with
  `SlotWalker`. Slots at one instant share one bucket. So 300 tenants on one
  hourly cron under a `100/m` throttle show a last start 120 s late.
- `steady_state` tells whether a periodic burst drains before the next one,
  or how much lag each period adds.
- A test checks the closed form against a direct token-bucket model.
- The waits are lower bounds. The scanner tick and the per-key fire cap only
  add delay. The module is pure and sends no SQL.
