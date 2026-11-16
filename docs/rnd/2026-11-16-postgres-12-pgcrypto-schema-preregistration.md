# Pre-registration: PG 12 and `gen_random_uuid()` outside `public` (assay ledger #18)

> Committed **before** any apparatus, PG build or measurement for #18. Nothing in
> this file changes after the first graded run. **Status: registered.**

Registered 2026-11-16 on `trunk-dev` `2ceb4e6`. It feeds the Keystone deferral
`docs/rnd/2026-10-12-postgres-support-floor-deferral.md`. Its first trigger
(2026-11-12, PG 14 end of life) passed four days ago with no owner and no
decision.

## Why this assay and not #17

Ledger #17 (`2026-11-02-postgres-12-engine-paths-per-test-preregistration.md`)
waits for a go-ahead on a multi-hour sweep. This assay does not run #17 and does
not change any of its lines. It takes the one risk that #17 names as its
riskiest assumption and tests it alone, on an operator topology, in about two
hours:

> Whether an operator install can hit the same error is **not known**. This is
> the riskiest assumption.

#17 cannot see this risk well. Its PG 12 exclusion 2 drops all of
`event_partitioning_tests`, and the only test that reaches this error lives in
that module (`the_layout_works_when_harvest_is_installed_outside_public`). The
error happens in the core migrations, before any partition code runs.

## 🎯 Question

On PG 12.22, can an operator install the core schema into a non-`public` schema
and run the engine's runtime `gen_random_uuid()` call, under the search-path
setups below? The engine says it supports this topology: the comment at
`event_partitioning_tests.rs:6614` reads "a deployment installed under a
non-`public` schema is supported", and `shard.rs` documents
`tenant_a,public` search paths.

**Decision this feeds:** the README "Postgres 12+" line stays, gains a stated
`pgcrypto` condition, or rises to 13+. **Decider:** the owner of the PR #2144
deferral (still unassigned).

## 🔍 Prior art (read before registration)

- Ledger #15: all core migrations apply on PG 12 into `public`.
- Ledger #16, archived results: the same test binary passed
  `the_layout_works_when_harvest_is_installed_outside_public` on PG 16.15
  (`pg16-run1.results`). It failed on PG 12.22 (`pg12probe-run1.results`) with
  `migrate into the alternate schema: function gen_random_uuid() does not exist`.
  That is one run per arm. The fixture had already migrated `public`, so
  `pgcrypto` sat in `public`, and its alternate session searched only
  `harvest_alt`.
- PostgreSQL semantics, from the manual: PG 13 moved `gen_random_uuid()` into
  core (`pg_catalog`). On PG 12 it exists only in `pgcrypto`.
  `CREATE EXTENSION IF NOT EXISTS` does nothing when the extension exists in
  any schema. An unqualified function name resolves through `search_path`. A
  column `DEFAULT` stores the resolved function, so a later `search_path`
  change does not break it.
- `autumn-harvest/migrations/20260409000000_harvest_initial/up.sql:6` runs
  `CREATE EXTENSION IF NOT EXISTS "pgcrypto"` with no schema. About 24
  migration column defaults call `gen_random_uuid()`. One runtime statement in
  `src` does: `autumn-harvest/src/throttle.rs:634`.

So scenario S2 below is **not blind**. Its outcome is predicted by the #16
probe. S2 is reported as a replication and does not count for any line.

## Scenarios (one fresh database each)

The search path is set with `PGOPTIONS='-c search_path=...'`, which is how a DSN
`options` parameter sets it. Migrations are each `up.sql` in order, through
`psql -1 -v ON_ERROR_STOP=1`, as in #15.

| id | before migration | migration `search_path` | runtime `search_path` | blind |
|:--|:--|:--|:--|:--|
| S1 | empty schema `harvest_alt` | `harvest_alt` | `harvest_alt` | yes |
| S2 | `CREATE EXTENSION pgcrypto` in `public`, empty `harvest_alt` | `harvest_alt` | `harvest_alt` | **no** |
| S3 | as S2 | `harvest_alt, public` | `harvest_alt, public` | yes |
| S4 | as S2 | `harvest_alt, public` | `harvest_alt` | yes |
| S5 | S1 complete in `harvest_a`, empty `harvest_b` | `harvest_b` | `harvest_b` | yes |

S5 is two installs in one database, for example two shards or two tenants.

**Runtime probe** (only when the migration passed): run the statement text of
`throttle.rs:634` with literal values, then one `INSERT` into
`harvest_audit_log` that omits `id`, so the column default runs. A probe passes
when both statements succeed.

**Cell result:** pass, or fail with the first server error and SQLSTATE.

## ⚖️ Lines (set now, numeric)

Three runs per arm. A cell "fails on an arm" when it fails in at least 2 of 3
runs.

| line | rule | verdict if met |
|:--|:--|:--|
| L0 (validity) | the PG 16.15 control passes every cell, migration and probe, in 3 of 3 runs | else `undetermined`: the apparatus is broken |
| L1 (kill) | at least 1 **blind** cell (S1, S3, S4, S5) fails on PG 12.22 with SQLSTATE `42883` (undefined function) | unqualified "12+" is false for a topology the engine calls supported |
| L2 (kill) | at least 1 cell fails on PG 14.24 | the PG 14 line is false for that topology |
| L3 (pursue) | every blind cell passes on PG 12.22 in 3 of 3 runs | the `pgcrypto` risk does not reach these operator setups |
| L4 (undetermined) | any other result, for example a blind PG 12 failure with another SQLSTATE, or a cell that passes in only 1 or 2 runs | the report states what is unattributed |

Pursue needs L0 and L3. Kill needs L0 and L1 or L2. S2 is reported but is not
graded.

## Conditions

- PG 12.22 and PG 14.24 built from source with `contrib` (for `pgcrypto`). PG
  16 from the Ubuntu package as the control. `initdb` defaults, loopback only,
  superuser role.
- Migrations from `autumn-harvest/migrations` at `2ceb4e6`. The plugin
  migrations are out of scope.
- Every run is reported, including discarded runs and the reason for each.

## Control

PG 16 runs the same script, the same scenarios and the same probe. PG 13 and
later resolve `gen_random_uuid()` from `pg_catalog`, so the control should pass.
A control failure means the apparatus is wrong, not that PG 16 is.

## Riskiest assumption first

The risk most likely to kill a "12+ holds" answer is that the failure reaches
setups that operators really use. So the run order is S3, S4, S5, S1, then S2,
on PG 12 first, then the control and PG 14. S3 and S4 use the common
`tenant, public` path.

## Time box and budget

- Time box: 3 hours of wall time from this commit. No extension without a
  written re-charter.
- Compute: two PG source builds (about 10 minutes each on 4 cores). Each run is
  6 fresh databases and about 140 migration files.
- Disk: under 2 GB. No spend. No production data.

## Containment

Branch `claude/happy-gauss-oemhhi`. Apparatus only under
`docs/assays/apparatus/0018-postgres-12-pgcrypto-schema/`. It never merges into
engine code.

## Stubs expected

`psql` replaces the Diesel migrator, so `full_migrations_sql()` is not the
input. Superuser role, so no `GRANT` setup is tested. The runtime probe runs two
statements, not the engine's code. Single node. Default config.

## Scope — what this result does not mean

A pass does not say "12+ is supported". It says only that this one
`pgcrypto` risk does not reach the five setups. A kill does not say that a
`public` install on PG 12 is broken: ledger #15 shows that it migrates.
