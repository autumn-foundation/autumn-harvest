## Phase 5.1 — Backup/restore runbook and PITR restore verification tooling (issue #943)

Harvest's durability story ended at "it's in Postgres, back that up". This slice
answers the question a backup does not: **after the restore, is the fleet safe
to resume, and what will happen the moment workers start?**

**New core module `autumn-harvest/src/backup_verify.rs`** — a pure vocabulary
plus a read-only DB half (`db`-gated):

- **Four-tier severity model.** `FindingSeverity::{Reclaimable, Advisory,
  Incoherent, Undetermined}`. Only `Incoherent` (exit **1**) and `Undetermined`
  (exit **2**) fail a drill. This tiering is load-bearing: a *correct* restore
  always contains dead-worker `RUNNING` rows and expired leases, so a tool that
  reported those as failures would cry wolf on every healthy drill and be
  ignored by the third one.
- **`Undetermined` outranks everything, including `Incoherent`.** A probe that
  could not run found nothing *because it did not look*. The canonical case —
  caught by manually smoke-testing the real binary, not by a test — is a
  "restore" that produced an **unmigrated/empty** database: every probe errors on
  a missing table, every condition reads as absent, and a naive tool reports a
  beautiful clean bill of health on an empty database. That is the single most
  dangerous output this tool could produce, so "we could not tell" is never
  allowed to render as a pass. Regression-pinned by three pure tests
  (`an_unrunnable_probe_is_undetermined_never_a_pass`,
  `undetermined_outranks_incoherent_and_reclaimable`,
  `advisory_classes_never_escalate_the_verdict`) and the DB test
  `an_unmigrated_restore_is_undetermined_never_a_pass`.
- **19 `FindingClass` variants** (8 reclaimable / 6 incoherent / 4 advisory / 1
  undetermined), each carrying a `const fn explanation()` that names the healing
  mechanism and its issue number, so the report teaches rather than just alarms.
- **`VerifyStatus::{Clean, ResumableWithReclaim, Incoherent, Unavailable}`** with
  `const fn exit_code()` → 0 / 0 / 1 / 2.

**Read-only by construction, three independent ways (AC4):**

1. Every connection issues `SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY`
   on connect, so any write fails with SQLSTATE `25006` — a Postgres-level
   guarantee, not a code-review promise.
2. **No mutating scanner is ever called.** The AC asks to "run the scanners once
   and report what they would reclaim", but `enforce_timeouts_once` and friends
   *mutate*. So verify reuses their **selection predicates verbatim** — the same
   `poison_pill::orphaned_running_tasks_query()`,
   `timeout::{heartbeat,start_to_close,schedule_to_start,schedule_to_close,workflow_execution}_timeout_query()`,
   `sessions::broken_session_candidates_query()`, `mutex::expired_leases_stmt()`
   `const` strings the live scanners use — and reports the rows they *would*
   claim. Sharing the `const` makes the report drift-proof by construction: a
   predicate change moves both at once.
3. A live-DSN guard that **fails closed** — an unparseable DSN compares as
   *matching*, so a malformed connection string can never sneak past.

Counts are exact and samples are bounded via `COUNT(*) OVER ()` computed
**before** `LIMIT`, so a report never trades accuracy for boundedness.

**Replay honesty.** Sampled non-terminal histories replay in **canary** mode, not
strict: a parked in-flight run legitimately suspends at its recorded frontier, so
strict replay would flag every healthy run in the fleet as divergent. An
unregistered workflow type is recorded as `replay_skipped_no_handler`, never as a
divergence, and `ReplaySummary::verified()` makes a report structurally unable to
claim replay coverage it does not have.

**Testing-harness additions** (`testing.rs`): `WorkflowReplayer::replay_canary_from_db`,
`is_workflow_registered`, `registered_workflow_names`; `replay_from_db` refactored
onto a shared private `snapshot_from_db`.

**CLI** (`harvest backup verify --shard <[N=]DSN>… [--i-know-this-is-scratch]
[--format text|json] [--replay-sample N] [--worker-stale-secs N]`): a local
command (early-return in `run_cli`, `unreachable!()` in `api_request`),
repeatable `--shard` with an optional `N=` prefix parsed so a DSN query string
containing `=` is never mangled, `CliError::{RestoreIncoherent,
RestoreUndetermined}` carrying the exit-code contract, and DSN redaction
everywhere a DSN is printed. The CLI's `autumn-harvest` dependency enables `db`
**unconditionally** (no new CLI Cargo feature), matching the house convention of
zero `#[cfg(feature)]` in the CLI and keeping every CI lint/test leg covering the
new code; diesel's `postgres_backend` + diesel-async/tokio-postgres are pure
Rust, so the Windows/macOS legs stay green.

**Multi-shard skew (AC3).** `harvest backup verify` accepts every shard in one
invocation and cross-checks parent/child terminal ordering and external
signal/cancel/await targets across them. A reference into a shard whose DSN was
*not* supplied degrades to the advisory `uninspected_shard_reference` rather than
silently passing — supplying every shard is what turns an advisory into a
verdict. Fleet skew is reported as `restore_point_skew_secs`.

**Runbook** `docs/runbooks/backup-restore.md`: backup approach per table class
(`pg_dump` vs physical/PITR, and why the durable-truth class wants PITR), exact
post-restore semantics for each in-flight artifact with the healing mechanism
named, the **plainly stated** at-least-once consequence (any activity whose
`ActivityCompleted` landed after the restore point *will run again*, against the
current outside world), the 30-minute drill, how to read the four verdicts, and
the multi-shard fencing procedure — **restore all shards, verify all shards,
then start workers**, never one shard at a time.

**Zero engine impact (AC5): no new `WorkflowEvent` variant, no migration, no
runtime behavior change.** This is a CLI + docs + read-mostly verification slice
over existing primitives; the only non-CLI production edits are three additive
`WorkflowReplayer` accessors.

**Tests.** 22 pure unit tests in `backup_verify.rs`; 10 testcontainers
integration tests in `tests/integration/backup_verify_tests.rs` seeding the
issue's ≥5 incoherence classes plus a pristine-restore control, an AC4
never-mutates assertion (snapshots row counts and task states before/after), a
genuine two-database cross-shard `child_terminal_rolled_back` case, an
unreachable shard, and the unmigrated-database false-clean regression; 17 CLI
tests in `autumn-harvest-cli/tests/integration/backup_verify_cli.rs` plus 3
clap-parse unit tests. DB suites ran green against a real local Postgres 16 and
are registered in `.github/ci/integration-suites.txt` for the Docker-backed
Linux run.
