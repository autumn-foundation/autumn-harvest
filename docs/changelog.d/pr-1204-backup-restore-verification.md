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
- **24 `FindingClass` variants** (8 reclaimable / 9 incoherent / 6 advisory / 1
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

**Post-review hardening, round 2** (automated review of the opened PR, two
genuine P1s plus two correctness gaps, each verified against real code before
being fixed and each RED-probe verified by reverting the fix):

1. **P1 — a replayed workflow error was counted clean.** `replay_sample`
   collapsed `ReplayStatus` to a divergent/clean bool, so
   `ReplayStatus::WorkflowFailed` fell into the clean bucket. But the sample is
   drawn exclusively from **non-terminal** runs, whose recorded history contains
   no terminal failure — a replay failure there means the deployed handler now
   errors where the live run had not, which is exactly what a post-restore
   verification exists to catch. The engine's own replay canary (`run_canary`
   in `testing.rs`) already counts that as `replay_failed`; verify now matches
   it. New Incoherent class `ReplayWorkflowFailed`, new `ReplaySummary.failed`
   counter (folded into `verified()`/`merge()` and the CLI's text renderer).
2. **P1 — a resolved external request asserted nothing about its target.** The
   reference scan dropped every request that had a recorded terminal, so a
   restore that rolled the *target* shard back past a delivered signal/cancel
   read clean — the exact asymmetry the design already avoids for children via
   `ChildTerminalRolledBack`. Terminals are now split by outcome: a **failure**
   terminal (`External*Failed`) applied no effect and is still droppable, while
   a **success** terminal (`ExternalSignalDelivered` / `ExternalCancelDelivered`
   / `ExternalAwaitResolved`) asserts durable state on the target shard —
   per-effect, mirroring how the engine applies it. Cancel and await require the
   target to be terminal; a signal requires either a `harvest_signals` row (the
   `consumed` flag means rows persist after delivery) or a recorded
   `SignalReceived` event. New Incoherent class `ExternalEffectRolledBack`.
3. **Undecodable reference events now fail closed.** A malformed, legacy or
   newer-version payload on a child/external row was silently skipped, yet that
   row may be the very reference that would have exposed a missing target. It is
   now reported as `Undetermined` (the replay sample is not a backstop: the
   shipped CLI links no handlers, and an embedded replayer samples only a
   bounded subset).
4. **`dsn_targets_same_database` no longer misses a multi-host or `hostaddr`
   DSN.** The live-DSN guard parsed a single host/port; libpq accepts
   comma-separated `host=`/`hostaddr=`/`port=` lists, so a failover DSN could
   slip the guard. It now parses full identity lists (`DsnIdentity`) and matches
   on any overlap, with `hostaddr` taking precedence over `host` — a widening,
   so the guard can only become more conservative, never less.

Tests: 8 new DB integration tests (22 total) covering both P1s with their
matching clean controls — a delivered cancel whose target is terminal, a
delivered signal with a queued row, a *failed* request asserting nothing — plus
the undecodable-event case and a real registered-handler replay failure/success
pair; 4 new unit tests. Both P1 fixes were confirmed falsifiable by reverting
them and observing the corresponding test fail.

**Post-review hardening, round 2** (automated review of the round-1 fixes; two
genuine P1s, both in the new `ExternalEffectRolledBack` signal check, each
confirmed against engine source before being fixed and each RED-probe verified):

1. **P1 — a healthy restore was reported rolled back.** The signal check looked
   only at the target's own execution row, but BOTH continue-as-new
   (`worker.rs`) and workflow-level retry
   (`signal::forward_signals_to_retry_attempt`, issue #843) **reassign**
   `harvest_signals.workflow_exec_id` to the successor — the retry path moves
   the whole mailbox and re-arms `consumed`. An unconsumed signal produces no
   `SignalReceived` event, so after either transition the original target
   carries no row *and* no event, and the check reported `Incoherent` ("do not
   start workers") on a perfectly good restore. The lookup now walks the
   target's successor chain (`continued_from_exec_id` / `retry_of_exec_id`) via
   a recursive CTE. Cancel and await are unaffected — a continued-as-new target
   is `CONTINUED_AS_NEW` and a retried one is `FAILED`, both already terminal.
2. **P1 — a repeated channel name masked a rollback.** The check matched on
   `(workflow_exec_id, signal_name)`, so a target that had received *any* signal
   of that name read clean even when the specific delivery had been rolled back
   — reopening the false-clean the class exists to prevent. A delivered signal
   is now matched **exactly by its `idempotency_key`** (issue #521), the only
   per-delivery identity the engine persists on the target side, carried into
   the reference from `ExternalSignalRequested` and normalised the same way
   `send_signal_idempotent` does (an empty key is no key). `effect_verdict`
   became tri-state: with a key the answer is definitive either way; without
   one, absence is still a definitive `ExternalEffectRolledBack` but presence
   is reported as the new **advisory** `ExternalEffectUnverifiable`. Advisory
   rather than `Undetermined` because this is a permanent precision limit of the
   data model, not a probe that failed to run — escalating it would mean the
   tool could never return clean for any fleet using unkeyed external signals,
   which would push operators to ignore it.

Tests: 5 new DB integration tests (**27** total) — continue-as-new forwarding,
retry forwarding, keyed-exact rollback detection, its exact-key-present clean
control, and the unkeyed advisory (asserting it does *not* escalate the
verdict). All four behavioural tests were confirmed RED against a reverted
check; the fifth is a control that passes both ways by design.
