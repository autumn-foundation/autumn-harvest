# Runtime schedule overrides

A DAG declares its schedule in code, through `#[dag(schedule = ...)]` or a
hand-built `DagInfo`. A workflow schedule declares it through
`WorkerBuilder::workflow_schedule`. An operator override changes that schedule
at runtime, with no restart and no deploy (issues #2385 and #2386, epic
#2384).

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

The CLI sends the same requests (issue #2388):

```bash
harvest dags set-schedule nightly_etl --interval 60 --reason "INC-4821 catch-up"
harvest dags set-schedule nightly_etl --cron "0 3 * * *" \
    --timezone America/Chicago --max-active-runs 2
harvest dags clear-override nightly_etl --reason "INC-4821 closed"

# Every override, of schedules and run defaults, with actor, reason and time.
harvest overrides list
```

`set-schedule` sets only the fields that you give. To return one field to its
code value, send `null` for it with the API.

## Where to see an override

- `GET /admin/overrides` lists every override, from every shard pool.
- `GET /admin/config` counts them under `overrides`.
- The Vantage DAG page shows each schedule field three times: declared in
  code, the override, and the effective value. A field with an override has
  the `override` marker, and the page names the actor, the time and the
  reason.

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
- **Shard routing changes (issue #2389).** A DAG override lives on the shard
  that owns the DAG. When a routing change moves the DAG, the old shard's pass
  copies the override to the new owner, then deletes its own row. The copy
  commits first, so a failure leaves the override on at least one shard, and
  the next tick retries. An override that the new owner already holds wins.
  If the new owner comes later in shard order, it applies the override in the
  same tick. Otherwise it runs one tick on the code value first. Two shards on
  one pool share one table, so nothing moves between them.

## Code-declared workflow schedules

`PATCH /schedules/{id}` edits a schedule in place (issue #771). That edit is
right for a schedule made with `POST /schedules`. A schedule declared in code
converges back to its code value on every tick, so before #2386 its edit
lasted one tick while the API reported success.

Now the API checks whether the schedule is declared in code. It asks the code
catalog of the API process, the same test that `DELETE /schedules` uses.

- **Declared in code.** The request writes an override with
  `kind = 'workflow'`, keyed by the workflow name. The row converges in the
  same transaction, and every tick applies the override before it converges
  the row.
- **Made through the API.** The request edits the row, as before. It writes
  no override.

```bash
# Run a code-declared schedule every five minutes.
curl -X PATCH https://<host>/api/harvest/admin/schedules/<id> \
     -H 'Content-Type: application/json' \
     -H 'X-Harvest-Actor: ops-team' \
     -d '{"schedule_expr": "interval:300", "reason": "INC-4821"}'

# Return it to its code value.
curl -X PATCH https://<host>/api/harvest/admin/schedules/<id> \
     -H 'Content-Type: application/json' \
     -d '{"clear_override": true}'
```

The request body is the #771 body. Each field that it sets becomes part of the
override, and a field that it leaves out keeps the stored override. A JSON
`null` keeps its #771 meaning: it sets a nullable field such as `calendar` to
NULL, and the override stores that NULL. Use `clear_override: true` to return
to code. A clear takes no other field except `reason`, and returns 400
otherwise. Send the new values in a second request.

Rules that differ from a row edit:

- `input` returns 400. The input is a payload under the payload codecs, and an
  override of it is out of scope. Change it in code.
- `clear_override` and `reason` return 400 on a schedule made through the API,
  since it has no override.
- A schedule with `all_writable_shards` has one row per shard. A request names
  one row by its id, so the override applies on that row's shard only.
- The API process needs the code catalog. A process that does not declare the
  schedule treats it as API-made and edits the row, which the next tick of a
  process that does declare it reverts.

## Limits of this slice

- SLAs and execution timeouts are #2387.
- A workflow override lives on the default shard. A change of the default
  shard does not move it. A DAG override does move (#2389).
- The SQLite backend has no override table yet (#2390).
