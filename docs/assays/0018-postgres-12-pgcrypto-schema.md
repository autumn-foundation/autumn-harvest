# ⛏️ Prospect: does PG 12 run harvest outside `public`? (kill: 2 of 4 blind cells fail 3/3 with 42883 vs a 1-cell kill line, ledger #18)

> Status: **measured, kill.** The pre-registration is
> [`docs/rnd/2026-11-16-postgres-12-pgcrypto-schema-preregistration.md`](../rnd/2026-11-16-postgres-12-pgcrypto-schema-preregistration.md),
> committed and pushed (`87e19c8`, 09:13 UTC) before any PG build, apparatus or
> run. It is unchanged.

## 🎯 Question

On PG 12.22, can an operator install the core schema into a non-`public` schema
and run the engine's runtime `gen_random_uuid()` call? The engine calls this
topology supported (`event_partitioning_tests.rs:6614`, and the `tenant_a,public`
search paths that `shard.rs` documents).

**Decision:** the README "Postgres 12+" line stays, gains a stated `pgcrypto`
condition, or rises to 13+. **Decider:** the owner of the PR #2144 deferral. Nobody
is named. The deferral's first trigger (2026-11-12) has passed with no decision.

## 🔍 Prior art

- Ledger #15: the core migrations apply on PG 12 into `public`.
- Ledger #16's archived results: the same test binary passed
  `the_layout_works_when_harvest_is_installed_outside_public` on PG 16.15 and
  failed it on PG 12.22 with `gen_random_uuid() does not exist`. One run per arm.
  That predicted scenario S2, so S2 is reported as a replication and is not graded.
- The PostgreSQL manual: PG 13 moved `gen_random_uuid()` into core. On PG 12 it
  comes only from `pgcrypto`. `CREATE EXTENSION IF NOT EXISTS` does nothing when
  the extension exists in any schema.
- Ledger #17 is registered and not run. Its PG 12 exclusion 2 drops all of
  `event_partitioning_tests`. That module holds the only test that reaches this
  error, so #17 as registered cannot see this defect.

## 🧪 Apparatus

[`apparatus/0018-postgres-12-pgcrypto-schema/run.sh`](apparatus/0018-postgres-12-pgcrypto-schema/run.sh).
One `initdb` per arm. One fresh database per scenario per run. Each of the 173
`up.sql` files at `2ceb4e6` goes through `psql -1 -v ON_ERROR_STOP=1`. The search
path is set through `PGOPTIONS`, as a DSN `options` parameter sets it. The
pre-registration said "about 140 migration files"; the true count is 173.

Stubs:

- `psql` replaces the Diesel migrator, so `full_migrations_sql()` is not the input.
  The #16 probe failure did use the real bundle, through Diesel.
- The runtime probe runs two statements, not engine code. The first is the
  `throttle.rs:634` statement text with literal values. The second inserts into
  `harvest_audit_log` without `id`, so the column default runs. The probe stops at
  the first failure.
- Superuser role, so no `GRANT` setup is tested. Default config. Single node.
- Plugin migrations are out of scope.

**Discarded sweep (one, PG 12 only).** The first PG 12 sweep had two apparatus
defects. The audit-log probe omitted the `NOT NULL status` column, so it failed on
every cell that migrated. `PGOPTIONS` splits on spaces, so the S3 and S4 path
`harvest_alt, public` broke the connection. Both were fixed before any graded run
(`harvest_alt,public`, and `status = 'succeeded'`). The raw output is in
`results/discarded/`. In that sweep, S5 failed and S1 migrated, through code that
the fix did not touch. So those two cells were seen once before the graded runs.

## 📊 Assay

Three runs per arm. Every graded run is shown. Full lines are in `results/*.results`.

| cell | migration `search_path` | runtime `search_path` | PG 16.15 (control) | PG 14.24 | PG 12.22 |
|:--|:--|:--|:--|:--|:--|
| S1: empty `harvest_alt` | `harvest_alt` | `harvest_alt` | 3/3 pass | 3/3 pass | 3/3 pass |
| S3: `pgcrypto` in `public` | `harvest_alt,public` | `harvest_alt,public` | 3/3 pass | 3/3 pass | 3/3 pass |
| **S4**: `pgcrypto` in `public` | `harvest_alt,public` | `harvest_alt` | 3/3 pass | 3/3 pass | **3/3 fail**: migration passes, the `throttle.rs:634` INSERT fails, `42883` |
| **S5**: second install, `pgcrypto` in `harvest_a` | `harvest_b` | `harvest_b` | 3/3 pass | 3/3 pass | **3/3 fail**: `20260409000000_harvest_initial`, `42883` |
| S2 (not blind): `pgcrypto` in `public` | `harvest_alt` | `harvest_alt` | 3/3 pass | 3/3 pass | 3/3 fail: `20260409000000_harvest_initial`, `42883` (replicates #16) |

The error is `42883: function gen_random_uuid() does not exist` in every failed cell.
`pgcrypto` stays in the schema that held it first, every time. Variance: none. Every
cell gives the same result in all three runs.

**What S4 shows.** The migration passes because `public` is on the migration path.
The column defaults then hold the resolved function, so a later path change cannot
break them. The one statement in `src` that calls `gen_random_uuid()` by name
resolves at run time, so it fails when the runtime path drops `public`. An operator
gets this with a migration role whose path includes `public` and a worker DSN pinned
to one schema. The failure hits only when a start is throttled and deferred, so the
install looks healthy until then.

**What S5 shows.** Two harvest installs in one PG 12 database cannot both migrate,
unless `pgcrypto`'s schema is on the second install's path. Examples are two shards
in two schemas, or two tenants.

## 🏁 Verdict

**Kill**, for an unqualified "Postgres 12+".

- L0 (validity): **met.** The PG 16.15 control passed all 15 cells, migration and
  probe, 3 of 3.
- L1 (kill): **met.** Two blind cells, S4 and S5, fail on PG 12.22 in 3 of 3 runs
  with SQLSTATE `42883`. The line was 1 cell.
- L2 (kill): **not met.** PG 14.24 passed all 15 cells, 3 of 3.
- L3 (pursue): not met, because of L1.

The number that carries it: 2 of 4 blind cells fail 3/3 on PG 12, against a 1-cell
kill line. 0 of 15 cells fail on the control.

What this does not say: a single `public` install on PG 12 is not shown broken
(ledger #15, and S1 and S3 here). This assay says nothing about PG 12 engine paths
other than `gen_random_uuid()`. That remains #17's question. PG 13 was not run. It
has `gen_random_uuid()` in core, but this assay did not measure it.

**For the decider.** Each option below fits the evidence. The choice is not
Prospect's to make.

1. Raise the floor to 13+. This is a docs edit. No engine code reads the version.
2. Keep 12+ and state the condition: on PG 12, the schema that holds `pgcrypto`
   must be on the `search_path` of every harvest session and of every migration.
3. Remove the condition from the S4 cell: `throttle.rs:634` can omit `id` and use
   its column default. That alone does not fix S5.

**Note for ledger #17.** If #17 runs as registered, its exclusion 2 hides this
defect. Its report should cite this entry, so that a #17 pass does not read as a
clean PG 12 result.

## 💰 Cost to productionize

Not applicable. A kill has no build. Option 3 is under one hour of engine work and
goes through the normal Bolt/Echo review. A CI cell for "installed outside `public`"
on PG 12 would keep any option honest. The deferral already proposed a throwaway
Docker CI job, and this cell fits in it.

## 🔬 Reproduce

```sh
# PG 12.22 and 14.24 from source with contrib:
#   ./configure --prefix=/opt/pg<ver> --without-readline --without-icu
#   make -j4 world-bin && make install-world-bin
# PG 16 from the Ubuntu package. Run from the repository root, at 2ceb4e6 migrations.
A=docs/assays/apparatus/0018-postgres-12-pgcrypto-schema/run.sh
mkdir -p /var/tmp/a18 && chown postgres /var/tmp/a18
runuser -u postgres -- bash $A /opt/pg12.22/bin 54312 /var/tmp/a18/pg12 pg12 3
runuser -u postgres -- bash $A /usr/lib/postgresql/16/bin 54316 /var/tmp/a18/pg16 pg16 3
runuser -u postgres -- bash $A /opt/pg14.24/bin 54314 /var/tmp/a18/pg14 pg14 3
```

Each arm takes about 10 seconds.
