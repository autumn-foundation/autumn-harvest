# 🏛️ Keystone [deferred]: raise the documented Postgres floor (trigger: 2026-11-12 PG14 end of life, or a first feature outside the partitioned layout that needs SQL newer than 12)

> **Update (2026-10-19).** Assay #15 (`docs/assays/0015-postgres-12-floor.md`) ran
> part of the evidence this record asked for. All 139 migrations apply on PG 12.22 and
> PG 14.24, and the schema matches PG 16. Engine query paths are not covered. The record
> still stands: "12+" holds at the schema layer and stays untested above it.

**Status:** deferral record. No RFC, no code change, no architecture review needed.
**Date:** 2026-10-12. **Scope:** `README.md` Requirements, `docs/partitioned-events.md`, `.github/workflows/ci.yml`.

## Decision left open

Should the README's "Postgres 12+" floor move up? Postgres 12 and 13 are already past
vendor end of life. Postgres 14 reaches end of life on 2026-11-12. PostgreSQL's own
versioning policy publishes these dates.

## Evidence

- Tier 4: the PostgreSQL project publishes the end-of-life dates above. Nothing in the repo
  records a signed contract or customer requirement for any Postgres version.
- Tier 2, run on `trunk-dev` at `9297ecc`: the README states that CI runs Harvest's own DB
  suites on Postgres 16 only. About 220 suites request `.with_tag("16")`.
  Postgres 12 to 15 are untested, so "12+" is a claim with no test behind it.
- Tier 2: no migration or engine SQL requires syntax newer than 12. The only
  `NULLS NOT DISTINCT` hits (PG 15 syntax) are catalog checks in `partition.rs`, not
  schema. The 24 `gen_random_uuid()` uses rely on the `pgcrypto` extension created in the
  initial migration. The partitioned layout already states a PG 14 floor
  (`docs/partitioned-events.md`, "Postgres version").
- No incident, support, or cross-team cost from the floor exists in the repo. The repo has
  no operational history (tier 1), so there is no data on which versions operators run.

## Do nothing for 12 months

The README keeps an untested claim. No dated fact makes that claim break before launch,
and nothing in git shows a cost paid today. Deferring costs a documentation inaccuracy and
nothing else.

## Door class

Two-way. Raising a documented floor is a docs edit plus a CI matrix entry. Reversal costs
well under one engineer-week. The implementing team decides it in a PR description.
It does not clear the impact floor for an RFC.

## Default path

Keep "12+". Do not widen CI. Do not drop support by decree.

## Seam kept open

`SELECT version()` is not read anywhere in the engine, so no runtime gate depends on the
floor. A floor change touches only docs and the CI image tags.

## Trigger to revisit

Revisit on the first of:

1. 2026-11-12, when PG14 reaches end of life. **Owner: unassigned.** A dated trigger needs
   a named owner to bind. Assign one in the issue tracker.
2. A PR that needs SQL newer than 12 outside the partitioned layout.
3. A customer or contract naming a Postgres version.

## Cheapest evidence to gather first (tier 3, under one day)

Run the core DB suite once against `postgres:12` and once against `postgres:14` in a
throwaway CI job. A failure turns "12+" into a measured false claim. A pass makes it a
tested one. Either result closes the question without an RFC.
