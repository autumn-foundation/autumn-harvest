# 🚦 Semaphore CI health — independent confirmation of PR #1713's fix for the `quota_enforcement_tests`/`integration_e2e.rs:1383` SOURCE-completion hang, via a rerun protocol that needs no Docker

**Status:** confirmation report, no code change. Continues the series from
`docs/rnd/2026-09-23-ci-health-semaphore-source-completion-hang-escalation.md`
(8 confirmed occurrences across this series, hard-gated on "no Docker in this
sandbox" for three sessions running). This session found the gate no longer
holds — see below — and used the reclaimed capability to independently
reproduce both the failure and the fix already proposed in open PR #1713,
without merging or otherwise altering it.

**Corrected across one Codex review round on this PR, three findings.**
First: the opening claim that `HARVEST_TEST_DATABASE_URL` bypasses Docker
for "every DB-backed test in this crate" overstated the fallback's
reach — one of `integration_e2e.rs`'s three setup helpers
(`setup_test_database_url()`) starts a testcontainer unconditionally and
never reads that variable; only the two helpers this report's own target
test actually uses do. Narrowed throughout. Second: this report's claim
that a branch lacked PR #1713's fix rested on `git merge-base
--is-ancestor` against the fix commit's SHA alone, which cannot rule out an
equivalent fix landing under a different commit — checked the branch's own
tree content directly instead (still missing). Third: this report's
`CREATE TABLE`-only inspection of the unpatched schema wrongly claimed the
`outcome` column was also missing; a later `ALTER TABLE` elsewhere in the
same bundle adds it. Corrected: the unpatched schema is missing exactly
`target_shard`/`target_workflow_name`, not `outcome`. All three corrected
inline, matching this series' convention.

## 🎯 Verdict path

Same verdict path as the whole series: `ci.yml`'s `pull_request` trigger
against `trunk-dev`, `Test DB (linux, shard 0)` — the shard `integration_e2e`
and `quota_enforcement_tests` collide on under the current 11-shard layout
(`33 % 11 = 44 % 11 = 0`, tracked separately by open PR #1707).

## 🔧 Environment correction: this sandbox has PostgreSQL, no Docker needed

Every report in this series since 09-21 recorded "no Docker daemon, `docker
ps` fails, `/var/run/docker.sock` absent" as a hard blocker on this role's
rerun-protocol requirement, and stopped there. That conclusion was correct
about Docker and incomplete about the consequence: this image also ships a
system PostgreSQL 16 package (`postgresql-16`), installed but not started
(`pg_lsclusters` showed `16/main ... down`). `pg_ctlcluster 16 main start`
brings it up without systemd (this container has no PID 1 init, confirmed by
`systemctl` failing with "Host is down" — `pg_ctlcluster` does not need it).

That, combined with a fact this series had already documented but not
connected — `HARVEST_TEST_DATABASE_URL` lets a DB-backed test in this crate
run against **any** reachable Postgres, bypassing testcontainers entirely —
means the rerun protocol this role's hard gate requires does not actually
need Docker for at least some tests in this repository. It needs a Postgres
server, the right schema on it, and a test that checks the env var, and this
sandbox can produce all three for the specific test this report targets.

**Correction (post-review, Codex on this PR).** An earlier draft of the
paragraph above said "every DB-backed test in this crate," which overstates
the fallback's reach. `integration_e2e.rs` defines three setup helpers, not
one: `setup_test_database_url()` (`integration_e2e.rs:512-518`) starts a
testcontainer **unconditionally** and never reads
`HARVEST_TEST_DATABASE_URL` at all; only `setup_test_db()`
(`integration_e2e.rs:478-484`) and `setup_test_database_url_or_env()`
(`integration_e2e.rs:538-543`, the one
`quota_enforcement_tests.rs`'s target test actually calls, confirmed by
reading its `setup_db()` at line 3656) check the env var first. Any test in
this crate that calls the Docker-only helper directly, or transitively
through a caller that does, still requires Docker — this report's claim is
correctly scoped only to tests reachable through one of the two
env-var-aware helpers, which is the specific path this report's own rerun
protocol exercised.

**The one thing this path cannot do** is exercise the testcontainers
provisioning code path itself. That matters here specifically because this
session's target defect (below) is a divergence between two hand-rolled
migration bundles, one of which (`integration_e2e.rs`'s `INIT_SQL`) exists
*only* to seed a testcontainers-provisioned database. Reproducing it required
manually applying that exact SQL text to a real Postgres, not booting a
container from it — the same schema, a different provisioning path. This
distinction is worth future sessions re-deriving explicitly rather than
re-concluding "no Docker → no rerun protocol" from the first `docker ps`
failure.

## 🌡️ Symptom (independently confirmed, not newly discovered)

This session did not need to find a new occurrence — the prior report's
finding stands, and PR #1713 (opened yesterday, `claude/fix-init-sql-missing-target-shard-migration-1685`,
still open, unmerged, `mergeable_state: clean`, 0 reviews) already diagnoses
it with a Docker-based rerun (2/2 fail unpatched, 15/15 + 47/47 + 14/14 pass
patched). This session's job was to check that diagnosis independently, and
to check whether the flake is still live on `trunk-dev` while the fix sits
unmerged.

**Still live:** run `35971823436` (`claude/cool-noether-dymixg`, PR #1724,
job `107552406206`, `Test DB (linux, shard 0)`, 2026-09-24T08:36:04Z) failed
with the byte-identical panic:

```
thread 'quota_enforcement_tests::completion_trigger_defers_to_outbox_when_target_quota_exceeded' panicked at autumn-harvest/tests/integration/integration_e2e.rs:1383:6:
workflow should reach expected state within timeout: Elapsed(())
```

Confirmed this branch does not carry PR #1713's fix:
`git merge-base --is-ancestor f63634b8893d2b946e8cee716456aa44ddaa82c8
origin/claude/cool-noether-dymixg` → not an ancestor. **Correction
(post-review, Codex on this PR):** an earlier draft stopped at that ancestry
check, which only proves this one commit is absent — a cherry-pick, squash,
or independently-written equivalent edit could carry the same fix under a
different SHA, the same class of gap this series' 09-23 report already
found and corrected for a different ancestry check. Read the branch's own
tree directly instead: `git show
origin/claude/cool-noether-dymixg:autumn-harvest/tests/integration/integration_e2e.rs`
shows the `INIT_SQL` bundle ending at the same last migration
(`20260920014641_harvest_staging_vacated_by`) as the unpatched dump this
report tested against, with no `include_str!` for
`20260920215812_harvest_completion_trigger_fires_target` anywhere in the
file — confirmed missing by content, not merely by commit ancestry. This is
the 9th confirmed occurrence of this exact signature across the series (8
from the 09-23 report, plus this one) — not a new independent data point
weakening the diagnosis, just the expected result of an unmerged fix on a
still-affected base.

## 🔍 Diagnosis — independently reproduced, not merely re-read

PR #1713's claim: `integration_e2e.rs`'s hand-rolled `INIT_SQL` constant
(a `concat!(include_str!(...))` bundle that seeds every testcontainers
database this suite's tests provision) never had migration
`20260920215812_harvest_completion_trigger_fires_target` added to it. That
migration adds `target_shard`/`target_workflow_name` to
`harvest_completion_trigger_fires`; `completion_trigger.rs`'s
`NewCompletionTriggerFireDb` inserts into those columns unconditionally, from
inside the SAME transaction that marks the source workflow `COMPLETED`. A
missing column makes the insert fail every time, rolling back the source's
own completion every time, so the test's wait for `COMPLETED` blocks for its
whole timeout and then panics — looking like a hang, not a schema error,
because nothing surfaces the underlying Postgres error to the test's own
log.

**Independently verified, this session, three ways:**

1. **Static, from the migration and schema files directly** (no DB needed):
   `migrations/20260920215812_harvest_completion_trigger_fires_target/up.sql`
   does add exactly `target_shard INTEGER` / `target_workflow_name
   VARCHAR(255)` to `harvest_completion_trigger_fires`; `schema.rs`'s
   generated definition of that table carries both columns (from the real
   migrated dev schema); `completion_trigger.rs:1600-1608`'s
   `NewCompletionTriggerFireDb` populates `target_shard`/`target_workflow_name`
   on every insert into that table, unconditionally.
2. **The current, unpatched `INIT_SQL`, dumped and inspected directly.**
   Added a temporary `#[test]` to `integration_e2e.rs` that wrote the
   constant to disk (reverted before this commit — it is not part of the fix
   and is not this report's contribution), confirming the table's `CREATE
   TABLE` statement declares only `source_exec_id UUID NOT NULL, trigger_id
   UUID NOT NULL, fired_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), PRIMARY KEY
   (source_exec_id, trigger_id)`, with no `target_shard`/`target_workflow_name`.
   **Correction (post-review, Codex on this PR):** an earlier draft of this
   bullet, and of the Reproduce section's `sed` command, said the table has
   no `outcome` column either. That is wrong — the `CREATE TABLE` statement
   alone lacks it, but migration `20260708000001_harvest_completion_trigger_condition`
   (already in the unpatched bundle) issues a later `ALTER TABLE
   harvest_completion_trigger_fires ADD COLUMN outcome TEXT NULL`, confirmed
   directly at `/tmp/semaphore_init_sql_unpatched.sql:972`. The `sed` command
   this report used to inspect the table (`sed -n
   '/CREATE TABLE .../, /);/p'`) stops at the `CREATE TABLE` statement's own
   closing `);` and so cannot see a later `ALTER TABLE` — it understates
   nothing about `target_shard`/`target_workflow_name` (no migration adds
   those columns anywhere in the unpatched bundle, confirmed by grepping the
   whole dump, not just the `CREATE TABLE` block), but it cannot support a
   claim about `outcome`. Corrected: the unpatched schema is missing exactly
   `target_shard` and `target_workflow_name`, not `outcome`. Applying this
   exact SQL to a real Postgres and attempting the insert
   `completion_trigger.rs` makes reproduces the precise error PR #1713
   names: `ERROR: column "target_shard" of relation
   "harvest_completion_trigger_fires" does not exist`.
3. **The test itself, run against both schemas, on this sandbox's local
   Postgres (no Docker, no testcontainers) — see Measurement.**

**Test-vs-product verdict: rendered.** This is a test-fixture bug, not a
product bug. The product's own migration is correct and was applied
correctly everywhere a real deployment would apply it; the defect is that
one integration-test file's independently-hand-maintained schema snapshot
fell out of sync with `migrations/` the day this migration landed
(2026-09-20) and nothing caught it until it started failing tests four days
later. `completion_trigger.rs`'s behavior against a correctly-migrated
database is not in question and this session found no evidence it should
be.

**Why the existing hygiene guard didn't catch this.**
`migration_hygiene.rs`'s `no_new_handrolled_migration_bundles_outside_allowlist`
guards against a *new* fixture reintroducing this hand-rolled-bundle pattern;
`integration_e2e.rs`'s `INIT_SQL` is already inside its allowlist (a known,
pre-existing exception, not something this guard flags going forward), and
nothing checks that an *allowlisted* bundle stays complete as new migrations
land. That gap is real and is the reason this specific class of bug has
recurred in this codebase before (the file's own comments cite the prior
`nd_block_tests`/`20260704000001_harvest_build_policy_ramp` incident) — not
a fault of PR #1713's fix, which is correctly scoped to the one line
missing. Flagged here as a Treatment candidate for a *future* session or
maintainer, not attempted in this report (see Treatment).

## 📊 Measurement

**Harness:** local PostgreSQL 16 (`pg_ctlcluster 16 main start`), two
throwaway databases seeded via `psql -f` from, respectively, the current
unpatched `INIT_SQL` dump and that same dump with
`20260920215812_harvest_completion_trigger_fires_target/up.sql` appended
(PR #1713's exact fix, applied as SQL rather than as the PR's diff, since
the diff only changes a Rust source constant). Test invoked via
`HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/<db>
cargo test -p autumn-harvest --test integration --features db
quota_enforcement_tests::completion_trigger_defers_to_outbox_when_target_quota_exceeded
-- --exact --test-threads=1`, run to completion each time (not
`--no-run`/compile-only).

| Schema | N | Pass | Fail | Notes |
|---|---|---|---|---|
| Unpatched (current `trunk-dev` `INIT_SQL`) | 5 | 0 | 5 | Every failure: 30.0–30.4s runtime, panic at `integration_e2e.rs:1383:6`, byte-identical to the CI signature this series has tracked since 09-21 |
| Patched (PR #1713's migration applied) | 20 | 20 | 0 | Every run: a few seconds, clean pass |

**Revert check:** satisfied by construction rather than by a separate
toggle — the "before" schema *is* the reverted state (the fix is a schema
addition, not a code branch), and it reproduces the failure deterministically
(5/5, not merely "sometimes"), which is stronger than the usual revert-check
bar of "goes red once more." Direct SQL confirms the mechanism rather than
just the symptom: `INSERT INTO harvest_completion_trigger_fires
(source_exec_id, trigger_id, target_shard, target_workflow_name) VALUES
(...)` against the unpatched schema fails with exactly the column-does-not-exist
error PR #1713's own report names.

**Before this session:** PR #1713's own Docker-based numbers (2/2 fail
unpatched, 15/15 + 47/47 + 14/14 pass patched, different harness, same
mechanism) — cited for completeness, not relied on in place of this
session's own independent numbers above.

**Ledger:** no quarantine ledger exists in this repository (unchanged from
every prior report in this series).

## 🔧 Treatment

**No code change from this report.** PR #1713 already contains the correct,
minimal fix (one `include_str!` line) and this session's independent
measurement confirms it by a different method than the PR's own. This
report's only recommendation is procedural:

1. **Merge PR #1713.** It has been open, unmerged, with a clean mergeable
   state and no negative review signal, for roughly 20 hours while `Test DB
   (linux, shard 0)` continues to fail on every affected branch (9th
   confirmed occurrence this morning, run `35971823436`). Every hour it
   stays unmerged is CI budget spent re-discovering an already-diagnosed,
   already-fixed defect. This is not this session's call to make unilaterally
   (the PR was opened by a different session, and merging is outside this
   role's charter without being asked), but it is this report's clearest
   actionable finding.
2. **Once merged, rebase PRs #1706 and #1707** (both blocked on this same
   panic on their own CI, per the 09-23 report) onto the fix and confirm
   both go green — PR #1713's own description already anticipates this.
3. **A real but separate follow-up, not urgent:** the allowlist gap
   identified in Diagnosis (an allowlisted hand-rolled migration bundle has
   no check that it stays complete as new migrations land) is a recurring
   bug class in this codebase, evidenced by this being at least its second
   occurrence. A harness check — diff each allowlisted bundle's included
   migration set against the full `migrations/` directory and fail if the
   bundle is missing one that predates it — would close this permanently
   for `integration_e2e.rs` and any future allowlist entry. PR #1713's own
   body independently proposes the more thorough alternative (migrate
   `setup_test_db()`/`setup_test_database_url()` onto
   `autumn_harvest::test_init_sql()`, the paved path, entirely) as
   deliberately out of scope for its own fix. Either is a reasonable
   next PR; neither is attempted here, consistent with keeping this report
   to confirmation only.

## 🔬 Reproduce

```sh
# Environment: this sandbox has no Docker daemon, but does have PostgreSQL 16
# installed (not started by default):
docker ps
# -> failed to connect to the docker API at unix:///var/run/docker.sock
pg_lsclusters
# -> 16  main  5432 down  postgres /var/lib/postgresql/16/main ...
pg_ctlcluster 16 main start
su postgres -c "psql -c \"ALTER USER postgres PASSWORD 'postgres';\""

# Dump the CURRENT (unpatched) hand-rolled INIT_SQL from integration_e2e.rs.
# It is `const` (not `pub`), so this requires a temporary in-tree probe —
# added, run, and reverted within this session, not part of the fix:
#   #[test]
#   fn semaphore_scratch_dump_init_sql() {
#       std::fs::write("/tmp/semaphore_init_sql_unpatched.sql", INIT_SQL).unwrap();
#   }
cargo test -p autumn-harvest --test integration --features db \
  integration_e2e::semaphore_scratch_dump_init_sql -- --exact
git checkout -- autumn-harvest/tests/integration/integration_e2e.rs   # revert the probe

# Confirm the CREATE TABLE statement's own columns -- this alone is NOT the
# final table shape (see the Diagnosis correction on `outcome` above): a
# later ALTER TABLE elsewhere in the bundle adds `outcome`. Grep the WHOLE
# dump, not just the CREATE TABLE block, to confirm what's actually missing:
sed -n '/CREATE TABLE harvest_completion_trigger_fires/,/);/p' /tmp/semaphore_init_sql_unpatched.sql
# -> source_exec_id, trigger_id, fired_at only (outcome added later, below)
grep -n "harvest_completion_trigger_fires" /tmp/semaphore_init_sql_unpatched.sql
# -> CREATE TABLE (above) plus one ALTER TABLE ... ADD COLUMN outcome TEXT NULL;
#    no target_shard/target_workflow_name ALTER anywhere in the file

# Build two throwaway databases: current (unpatched) and PR #1713's fix applied as SQL.
su postgres -c "psql -c 'CREATE DATABASE semaphore_unpatched OWNER postgres;'"
su postgres -c "psql -d semaphore_unpatched -f /tmp/semaphore_init_sql_unpatched.sql"
cat /tmp/semaphore_init_sql_unpatched.sql \
  autumn-harvest/migrations/20260920215812_harvest_completion_trigger_fires_target/up.sql \
  > /tmp/semaphore_init_sql_patched.sql
su postgres -c "psql -c 'CREATE DATABASE semaphore_patched OWNER postgres;'"
su postgres -c "psql -d semaphore_patched -f /tmp/semaphore_init_sql_patched.sql"

# Direct SQL confirmation of the mechanism:
su postgres -c "psql -d semaphore_unpatched -c \"INSERT INTO harvest_completion_trigger_fires (source_exec_id, trigger_id, target_shard, target_workflow_name) VALUES (gen_random_uuid(), gen_random_uuid(), 0, 'x');\""
# -> ERROR: column "target_shard" of relation "harvest_completion_trigger_fires" does not exist

# Rerun protocol, unpatched (baseline):
export HARVEST_TEST_DATABASE_URL="postgres://postgres:postgres@localhost:5432/semaphore_unpatched"
for i in 1 2 3 4 5; do
  timeout 60 cargo test -p autumn-harvest --test integration --features db \
    quota_enforcement_tests::completion_trigger_defers_to_outbox_when_target_quota_exceeded \
    -- --exact --test-threads=1 2>&1 | grep -q "test result: ok" && echo PASS || echo FAIL
done
# -> FAIL x5, each ~30s

# Rerun protocol, patched (PR #1713's fix):
export HARVEST_TEST_DATABASE_URL="postgres://postgres:postgres@localhost:5432/semaphore_patched"
for i in $(seq 1 20); do
  cargo test -p autumn-harvest --test integration --features db \
    quota_enforcement_tests::completion_trigger_defers_to_outbox_when_target_quota_exceeded \
    -- --exact --test-threads=1 2>&1 | grep -q "test result: ok" && echo PASS || echo FAIL
done
# -> PASS x20

# The flake is still live on trunk-dev-based branches pending PR #1713's merge:
# actions_list(method="list_workflow_runs", resource_id="ci.yml", event="pull_request", status="completed")
# -> run 35971823436 (claude/cool-noether-dymixg, PR #1724), Test DB (linux, shard 0),
#    2026-09-24T08:36:04Z, byte-identical panic at integration_e2e.rs:1383:6
git fetch origin f63634b8893d2b946e8cee716456aa44ddaa82c8
git merge-base --is-ancestor f63634b8893d2b946e8cee716456aa44ddaa82c8 origin/claude/cool-noether-dymixg
# -> not an ancestor -- necessary but not sufficient (Codex correction: an
#    equivalent fix could land under a different commit and still fail this
#    check). Confirm by content, not just ancestry:
git show origin/claude/cool-noether-dymixg:autumn-harvest/tests/integration/integration_e2e.rs \
  | grep -c "20260920215812_harvest_completion_trigger_fires_target"
# -> 0: no include_str! for the target-shard migration anywhere in this
#    branch's own tree, confirming it lacks the fix by content
```
