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
- **21 `FindingClass` variants** (8 reclaimable / 7 incoherent / 5 advisory / 1
  undetermined), each carrying a `const fn explanation()` that names the healing
  mechanism and its issue number, so the report teaches rather than just alarms.
  The severity table is pinned exhaustively against `FindingClass::ALL`
  (`severity_truth_table_is_pinned_for_every_class`), so demoting any single
  class — which changes the exit code, and therefore whether an operator starts
  workers on a broken fleet — fails a test.
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

**Replay honesty.** The shipped `harvest` CLI links no application `#[workflow]`
handlers, so its replayer is structurally empty and check (a) is **always**
reported `NOT VERIFIED` — never silently claimed. `RestoreVerifyReport` carries
a top-level `replay_verified: bool` so a JSON consumer can gate on it without
re-deriving the rule, and the runbook's §4.3 leads with the limitation and gives
the embedder recipe (call `verify_restore` from a binary that registers your
handlers) for operators who want check (a) too.

**Post-review hardening** (multi-angle review before submission): the severity
table is now pinned exhaustively (a class demotion previously survived every
test); `non_terminal_executions` became `Option<u64>` so a failed count probe
reports `null` + `probe_failed` instead of a fabricated `0` that reads as a
drained fleet, and a failed newest-event probe no longer silently disables the
skew check; `probe_limit` is floored at `1` so a mis-set knob can never emit
`LIMIT 0` and fabricate an all-clear; a new `wedged_schedule_claim` class
(Incoherent) catches a torn claim pair (`fire_claim_token` set,
`fire_claimed_until` NULL), which the scheduler's claim predicate can never
match again — permanently wedged, not merely expired; the session-reclaim probe
mirrors the scanner's Rust-side "elapsed lease but a member is still RUNNING"
suppression instead of over-reporting; `--live-dsn` is repeatable so a sharded
fleet is fully guarded, and a run whose guard did not actually run (no live DSN,
or `--i-know-this-is-scratch`) now prints a `WARNING:` on stderr rather than
being silently inert; and the runbook documents the read-only pin's
connection-pooler caveat (session-scoped, so not durable under PgBouncer
transaction pooling) plus the one reused predicate that is faithful by review
rather than by construction.

**Tests.** 22 pure unit tests in `backup_verify.rs` (including the exhaustive
severity pin); 14 testcontainers integration tests in
`tests/integration/backup_verify_tests.rs` seeding the issue's ≥5 incoherence
classes plus a pristine-restore control, an AC4 never-mutates assertion that
snapshots **state** (not just row counts) across every probed table and asserts
the run genuinely found something, a direct SQLSTATE-25006 proof of the
read-only pin, genuine two-database cross-shard `child_terminal_rolled_back`
**and** `child_execution_missing` cases, the torn-claim and
suppressed-session-lease predicate-fidelity cases, an unreachable shard, and the
unmigrated-database false-clean regression; 22 CLI tests in
`autumn-harvest-cli/tests/integration/backup_verify_cli.rs` plus 3 clap-parse
unit tests. DB suites ran green against a real local Postgres 16 and are
registered in `.github/ci/integration-suites.txt` for the Docker-backed Linux
run.
