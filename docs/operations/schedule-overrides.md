# Runtime DAG schedule overrides

A DAG declares its schedule in code, through `#[dag(schedule = ...)]` or a
hand-built `DagInfo`. An operator override changes that schedule at runtime,
with no restart and no deploy (issue #2385, epic #2384).

## Why a row edit does not work

The scheduler converges each `harvest_schedules` row to the declared value on
every tick. A direct `UPDATE` of a DAG row lasts about one tick. A value read
from an environment variable at startup changes only on restart.

An override is stored in its own table, `harvest_schedule_overrides`. The
scheduler applies it to the code value before registration. So the row
converges to the override instead.

## Quick reference

```bash
# Run the DAG every minute instead of its declared cadence.
curl -X PATCH https://<host>/api/harvest/dags/nightly_etl \
     -H 'Content-Type: application/json' \
     -H 'X-Harvest-Actor: ops-team' \
     -d '{"schedule_expr": "interval:60", "reason": "INC-4821 catch-up"}'

# A cron cadence in a timezone, and a wider run cap.
curl -X PATCH https://<host>/api/harvest/dags/nightly_etl \
     -H 'Content-Type: application/json' \
     -d '{"schedule_expr": "cron:0 3 * * *", "timezone": "America/Chicago",
          "max_active_runs": 2}'

# Return one field to its code value.
curl -X PATCH https://<host>/api/harvest/dags/nightly_etl \
     -H 'Content-Type: application/json' \
     -d '{"max_active_runs": null}'

# Return every field to its code value.
curl -X PATCH https://<host>/api/harvest/dags/nightly_etl \
     -H 'Content-Type: application/json' \
     -d '{"clear_override": true}'
```

## Fields

| Field | Override value | `null` |
|---|---|---|
| `schedule_expr` | Cadence: bare cron, `cron:<expr>`, `cron_tz:<tz>:<expr>`, `interval:<secs>` or `manual` | Code cadence |
| `timezone` | IANA timezone of a cron `schedule_expr`. Valid only with one. | Not accepted |
| `catchup` | Catchup flag | Code value |
| `max_active_runs` | Run cap | Code value |
| `jitter_secs` | Fire jitter, in seconds | Code value |
| `overlap_policy` | `skip`, `buffer_one`, `buffer_all`, `cancel_other`, `terminate_other` or `allow_all` | Code value |
| `buffer_all_max` | Buffer cap under `buffer_all` | Code value |
| `clear_override` | `true` returns every field to code | Not accepted |
| `reason` | Stored with the override | |

An absent field keeps the stored override. The response is the DAG schedule
row with the effective values.

`paused` works as before, in the same request or alone. A pause does not
change the override, and clearing the override does not resume the DAG.

## Behaviour contract

- **Precedence.** Override, then code. A field with no override inherits the
  code value, so a later deploy that changes another field still applies.
- **Restarts.** An override is in Postgres. It survives a restart and applies
  to every replica.
- **Cadence change.** A new cadence re-anchors `next_run_at` at now. Elapsed
  slots of the old cadence never fire, as for a deploy that changes the
  cadence.
- **Validation.** The merged value is checked before any write. An invalid
  cadence, an unknown timezone, or a jitter that the new cadence cannot hold
  returns 400 and stores nothing. A 5 s interval cannot hold a 10 s jitter, so
  set `jitter_secs` in the same request.
- **Unscheduled DAGs.** A unified DAG with no declared schedule can get one
  from an override. A classic DAG takes no cadence override.
- **`manual`.** `schedule_expr: "manual"` stops automatic fires. A manual
  trigger still works. Use `paused` for an incident, since a pause also defers
  manual triggers.
- **Audit.** Every request writes a `dag.patch` audit row with its actor.
- **Fail-closed.** A scheduler pass that cannot read the override table skips
  its shard and logs a warning. It never falls back to code values, because
  each fallback would move `next_run_at` twice.

## Limits of this slice

- Overrides cover DAG schedules. Code-declared workflow schedules are #2386,
  and SLAs and execution timeouts are #2387.
- An override row lives on the shard that owns the DAG. A shard routing change
  does not move it yet (#2389).
- The SQLite backend has no override table yet (#2390).
