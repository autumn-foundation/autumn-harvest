## Phase 3.x — Enforce the start-route capability-precise admin gate before the fail-closed runtime check (issue #685 follow-up)

Heals the no-DB plugin security test
`eris_unauthenticated_start_workflow_terminate_if_running_is_blocked`, which
began failing on trunk-dev after #1100 (issue #685) narrowed and **reordered**
the `POST /workflows/{name}/start` admin gate.

**Root cause (not a live-deployment security hole).** Pre-#1100 the admin gate
was a raw-string match placed at the top of `start_workflow`, before the
fail-closed `api_state.runtime()` check. #1100 replaced it with the
capability-precise gate
`effective_active_conflict_behavior(reuse, conflict) == Terminate && !admin`
and moved it to *after* both policy parses — which sit after the runtime check
and the #808 committed-replay probe. In a **running** deployment the gate still
correctly returns `401` for every request that can cancel a live run, so there
is no live security hole. But in the no-DB `unauthenticated_app()` harness the
runtime is unset, so `api_state.runtime()` returns `Config("harvest runtime is
not started")` → `400` *before* the gate is reached. The test's `401` assertion
now saw `400`. More importantly the reordering made the start-route auth gate
**un-observable** in the no-DB harness: a gate-present and a gate-removed build
both return `400`, so a test that accepted `400` would catch no regression.

**Fix.** Add a surgical, additive, **lenient early auth gate** before the
runtime check that reproduces the exact capability-precise condition on the two
pure request-body policy strings (no runtime, no DB needed). It is lenient — an
unparseable policy string makes the `Ok`/`Ok` guard fail, deferring to the
authoritative gate (so #808's "committed replay returns `200` despite an invalid
string" and "fresh invalid string → `400`" are preserved). Only a cleanly-parsed
`Terminate`-capable combo by a non-admin returns `401`. The authoritative gate
after the two parses is kept unchanged as a belt-and-suspenders backstop
(reachable only for requests the early gate let through — admin, or
non-`Terminate`). This restores the pre-#685 property that a cancel-capable
unauthenticated start is rejected with `401` regardless of runtime readiness
(defense in depth: authorize before the fail-closed resource check; `401` now
precedes the registry `404`, removing an existence oracle — exactly the
pre-#1100 ordering), and keeps #1100's narrowing intact: the flagship non-admin
idempotent-starter `reuse = terminate_if_running` + `conflict = use_existing`
(→ `Attach`) and `+ fail` (→ `Fail`) stay non-admin.

**No new `WorkflowEvent` variant, no migration, no route change.** Running
deployments are unaffected (same outcomes, `401` merely enforced earlier).

**Tests** (`autumn-harvest-plugin/tests/security.rs`): the previously-failing
test now asserts a clean `401` again; four new no-DB cases encode the narrowed
contract so a regression in either direction is caught —
`conflict_policy = terminate_existing` → `401`; and
`terminate_if_running + use_existing`, `terminate_if_running + fail`,
`conflict_policy = use_existing` are each `!= 401 && != 403` (allowed past the
auth gate).

**Also heals inherited trunk-dev Lint red** (out of this change's scope but
blocking the `cargo clippy -p autumn-harvest-plugin --all-targets` gate):
#1100's `StartWorkflowParams` literal sweep missed
`autumn-harvest-plugin/tests/progress_stream_integration.rs` (added by #1098),
which failed to compile with `missing field conflict_policy`. Added
`conflict_policy: WorkflowIdConflictPolicy::Unspecified` matching the sweep's
convention.

**Post-review hardening (Codex P2): the early gate now defers idempotency-keyed
requests.** The early gate above rejected any cleanly-parsed `Terminate`-capable
combo by a non-admin with `401` — with no regard for an idempotency key. But the
#808 committed-replay claim is keyed on `(workflow_name, idempotency_key)`
**alone** — independent of the reuse/conflict policy and the caller's auth — so a
keyed `Terminate` start committed by an **admin** can legitimately be replayed by
a later **non-admin** retry carrying the same key, which must return the #808
`200` no-op. The early gate's own comment (and the authoritative gate's) claimed
"a non-admin can never have committed a `Terminate` start, so no committed replay
exists" — flawed reasoning, since the committer and the retrier need not be the
same principal. **Fix:** narrow the early gate to fire only when **no** idempotency
key is present (a cheap presence-only boolean — header `Idempotency-Key` present,
or a non-empty body `idempotency_key`; no validation here, so the downstream
resolver still `400`s an empty/over-long key unchanged). Any keyed request defers
past the early gate to the downstream path, preserving BOTH #808 outcomes: a
committed-replay hit returns its `200` at the committed-replay probe
(caller/body-independent), while a genuinely-fresh keyed `Terminate` by a
non-admin is still `401`'d by the authoritative gate — which runs **after** that
probe, so the narrowing opens no hole. Both misleading comments are rewritten to
state the correct invariant (the early gate is an observable, key-absent-only auth
check; keyed requests defer downstream). New no-DB regression tests
(`eris_keyed_terminate_if_running_defers_past_early_gate`,
`eris_keyed_terminate_existing_conflict_defers_past_early_gate`) assert a keyed
cancel-capable request is no longer early-`401`'d (in the no-DB harness it
fails-closed at the runtime check → non-`401`), contrasted against the unkeyed
cases which still assert `401`.
