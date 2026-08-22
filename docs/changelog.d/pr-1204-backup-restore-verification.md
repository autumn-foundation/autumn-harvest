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

**Post-review hardening, round 3** — two more findings, both false-cleans on
the guarantees the tool exists to provide, both fixed TDD red→green:

1. **P1 — delivered external effects from a *terminal* caller went unchecked.**
   The cross-shard reference scan filtered owning executions to
   `RUNNING`/`PAUSED`/`SUSPENDED`. That rule belongs to the **child** checks (a
   terminal parent's awaited child may legitimately have been collected by
   retention, so scanning it would report `child_execution_missing` on a healthy
   restore) but was wrongly applied to external effects too. A caller that
   recorded `ExternalSignalDelivered` and then completed still asserted durable
   state on the *target* shard, and that assertion does not expire — if
   anything a terminal caller is the worse case, because no live caller remains
   to re-drive the delivery, so the target waits forever for a signal that
   nothing will resend. Verification exited 0 on it. The owner-state filter is
   now split by event class: external events are scanned from every retained
   caller, child events keep the narrow filter. The widening is paired with a
   `terminal_owners` guard in `build_refs` so a terminal caller's *unresolved*
   request is dropped rather than reported `external_target_missing` — nothing
   is waiting on it and its target may since have been collected, which would
   have traded one false clean for a false `Incoherent`.
2. **P2 — an omitted `dbname` compared as the empty string.** libpq (and so the
   server, since tokio-postgres simply omits `database` from the startup packet
   when unset) defaults the database name to the **connection user**. Storing
   `""` made `postgres://harvest@prod-host` compare unequal to
   `postgres://ops@prod-host/harvest` even though both reach database
   `harvest`, so the live-DSN guard waved a production target through without
   the `--i-know-this-is-scratch` acknowledgement. The identity now resolves a
   missing `dbname` to `get_user()`. With neither present the real name is the
   connecting OS username — not knowable here and different on the operator's
   machine than in the deployed config — so the identity is refused entirely,
   which makes the guard fail closed rather than guess.

Tests: 2 new DB integration tests (**29** total) — a delivered effect from a
`COMPLETED` caller is adjudicated, and an unresolved request from a
`TERMINATED` caller is *not* reported missing — plus 2 new DSN-guard unit
tests. Each half was independently falsified: the widening was confirmed RED
before the SQL split, and neutering the `terminal_owners` guard alone makes the
second test fail, so both halves are load-bearing rather than decoration.

Also fixes a CI break from round 2: three `dsn_guard_*` unit tests called a
`#[cfg(feature = "db")]` function without carrying the gate, so
`cargo test --no-default-features` failed to compile. The guard parses with
`tokio_postgres::Config` — deliberately the same parser `diesel_async` uses at
connect time — and `tokio-postgres` is enabled only by `db`, so the tests take
the gate too.

**AC coverage gap closed.** `RestorePointSkew` — the AC3 signal that two shards
were restored to materially different points — had only a pure unit test
(`compute_skew_needs_two_timestamps`); nothing exercised the detection
end to end. Added `detects_restore_point_skew_across_shards` (two shards, one
backdated an hour, asserting both the finding *and* the reported
`restore_point_skew_secs`) and the control
`shards_restored_to_the_same_point_are_not_flagged_as_skewed`, without which the
positive test would pass even if the check fired unconditionally — which would
make every healthy multi-shard drill report a finding. Falsified by neutering
the threshold comparison. **31** DB integration tests.

**Post-review hardening, round 4** (Codex, two P1s and a P2 — each verified
against real engine source before any code changed).

1. **P1 — a non-`COMPLETED` await terminal is a resolution, not a transport
   failure.** `execution::read_external_await_outcome` returns
   `ExternalAwaitOutcome::Terminal { reason_code, .. }` for a target that
   reached `FAILED`/`TIMED_OUT`/`CANCELLED`/`TERMINATED`, and both the worker
   (`worker.rs`) and the outbox (`timeout.rs`) record that as
   **`ExternalAwaitFailed`** — the same variant used for the genuine transport
   failure `target_unknown`. The scan bucketed the whole `*Failed` class as
   "applied no effect", so `build_refs` dropped it and a target shard restored
   to *before* the terminal the caller had observed was silently missed.
   Fixed by splitting on the reason code: the four terminal-outcome codes are
   adjudicated exactly like `ExternalAwaitResolved`; only `target_unknown` and
   `self_await` stay in the no-effect bucket. The match is deliberately an
   **allowlist of transport codes**, so an unrecognised future reason code
   falls into the adjudicated bucket — worst case a finding an operator can
   dismiss, never a silently skipped check.

2. **P1 — `CONTINUED_AS_NEW` is not proof an await resolved.**
   `read_external_await_outcome` *follows* a `CONTINUED_AS_NEW` target through
   its successor chain and resolves only once the chain **head** is terminal.
   `effect_verdict` shared one arm for `Cancel | Await` that accepted any
   `is_terminal_state()` — which includes `CONTINUED_AS_NEW` — so a resolved
   await whose successor had been restored back to `RUNNING` read as coherent.
   `Await` now has its own `await_verdict` walking the same chain the engine
   walks (the predecessor's own `WorkflowContinuedAsNew` event, not the
   `continued_from_exec_id` back-link, which is absent on pre-#701 rows). An
   absent successor row counts as ordinary retention — the predecessor's seal
   and the successor's insert are one transaction, so an absent successor was
   terminal. `Cancel` keeps the simple terminal check, which is correct for it:
   a delivered cancel against an already-terminal target is a documented no-op
   success (issue #492), so any terminal state is consistent.

3. **P2 — the cross-shard reference scan truncated instead of paginating.** A
   hard `LIMIT probe_limit` (default 1000) meant any fleet with more reference
   events than one probe reported `probe_failed` → `undetermined` → exit 2: a
   false "cannot verify" on a healthy restore, with no operator override (the
   CLI exposed `--replay-sample` and `--worker-stale-secs` but not
   `--probe-limit`). The scan now **pages** by keyset on the owner id, keeping
   the existing boundary-drop so no owner group is ever split, and re-fetching
   the dropped tail group from the start of the next page. Two bounded exits
   still raise a truncation note rather than reporting a clean prefix: a single
   execution carrying more reference events than one page (which cannot advance
   the cursor, and would otherwise either loop forever or skip that group
   silently), and an internal page ceiling. `--probe-limit` is now exposed and
   is a **page size**, not a ceiling. The per-page `COUNT(*) OVER ()` window
   function is gone, since completeness is now decided by a short page.

Tests: 6 new DB integration tests (**37** total), each paired with the control
that would pass a naive fix — `a_terminal_outcome_await_failure_is_adjudicated_not_discarded`
vs `a_target_unknown_await_failure_asserts_no_effect_on_the_target` (without
which "adjudicate every `ExternalAwaitFailed`" would pass);
`a_resolved_await_whose_successor_is_still_running_is_rolled_back` vs
`a_resolved_await_whose_successor_is_terminal_is_clean` (without which "treat
`CONTINUED_AS_NEW` as always-lost" would pass); and
`a_reference_scan_larger_than_the_probe_limit_is_paginated_not_truncated` vs
`pagination_still_detects_a_rollback_beyond_the_first_page` (without which
"page once and stop" would pass, silently narrowing the check). Plus CLI
parse coverage asserting the new flag and that its default tracks
`DEFAULT_PROBE_LIMIT` rather than a drifting literal.

**Post-review hardening, round 5** (two Codex P2s, both fixed):

1. **`--shard` accepted shard ids that cannot round-trip.** The parser rejected
   only an `i32` parse failure, so `65535` (the reserved `ShardId::UNENCODED`
   sentinel) and anything above it were accepted — but an `ExecutionId` carries
   its shard as `shard & 0xFFFF`, so `65536` truncates to `0`. Every target id
   read out of the database the operator supplied under `65536=` then decoded as
   shard 0, missed the supplied map, and was written off as "on an uninspected
   shard": an advisory, exit 0, and the shard the operator actually handed over
   never checked. `parse_shard_targets` now validates with
   `autumn_harvest::shard::is_encodable_shard` — the same rule the shard router
   uses, so the two cannot drift — and names the valid range in the error.
2. **The embedder recipe did not compile from a default-feature dependency.**
   `ShardTarget` / `VerifyOptions` / `verify_restore` and `WorkflowReplayer` are
   gated on `db` + `testing`, and `testing` is off by default, so an application
   following runbook §4 could not import either line until it independently
   discovered the feature. The section now leads with the required
   `Cargo.toml` stanza and notes that `testing = []` pulls in no extra runtime
   dependency (so it is safe in a drill binary and can stay off in the
   production service). The `--shard` flag row also states the encodable range.

Tests: `unencodable_shard_ids_are_rejected` (`65535` / `65536` / `70000`, each
asserting the error names the offending id) paired with the boundary control
`the_largest_encodable_shard_id_is_accepted` (`65534` still parses), so the
guard rejects only what genuinely cannot round-trip. RED confirmed before the
fix: the rejection test failed at `65535` while the control already passed.

**Post-review hardening, round 7** (two of four Codex findings fixed here; the
other two filed as #1205, see below):

1. **A numeric `host` bypassed the production-DSN guard.** `parse_dsn_identity`
   put every `Host::Tcp` value in the hostname set, so candidate
   `postgres://u@10.0.0.5/harvest` and live
   `postgres://u@prod-alias/harvest?hostaddr=10.0.0.5` — the same TCP
   destination and database — overlapped in neither the hostname set nor the
   address set, and the guard permitted a verification run against production
   without the acknowledgement flag. A numeric host *is* an address: comparing
   it needs no DNS, so it now parses with `IpAddr::from_str` and joins the
   address set. That also normalises IPv6 spellings on both sides (`[0:0:…:1]`
   vs `::1`), since `get_hostaddrs()` already yields `IpAddr`. This is **not**
   the limitation the function documents — that one is a `host` *name* against a
   bare `hostaddr`, which genuinely requires resolving DNS and is still refused
   by design. The change only ever produces *more* matches, which the guard's own
   doc names as the safe direction, and the `hosts.is_empty() &&
   hostaddrs.is_empty() → None` fail-closed path is unchanged.
2. **The embedder recipe called `exit_code` on the wrong receiver.** Round 5
   fixed the missing feature stanza but left `report.status.exit_code()`;
   `exit_code` is implemented on `RestoreVerifyReport`, not `VerifyStatus`, so
   the snippet still did not compile. Now `report.exit_code()`.

Test: `dsn_guard_matches_a_numeric_host_against_a_hostaddr` covers both
directions (numeric candidate vs pinned live, and the reverse), the IPv6
normalisation, and a numeric-vs-numeric regression — paired with the control
that a *different* address (`10.0.0.6` vs `10.0.0.5`) is still correctly treated
as a genuine scratch target, so the widening does not simply match everything.
RED confirmed before the fix on the first assertion; all 7 pre-existing
`dsn_guard_*` tests still pass.

The remaining two findings — retention being indistinguishable from a
pre-creation rollback when a cross-shard target row is absent (P1), and
`Finding::new` reporting `truncated: false` while clipping the sample list (P2)
— are filed as **#1205** rather than fixed here, since this round is past the
review-round budget agreed for this PR. Both were verified against source before
filing. The two fixed above were treated as deliberate, narrow exceptions: a
guard that stops a drill running against a live database is a hard constraint
rather than a review-iteration preference, and the runbook snippet was a factual
error in something round 5 had claimed to verify.
