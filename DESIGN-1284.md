# Design — Issue #1284: quickstart's `harvest preflight` 401s in the `dev` profile

The quickstart's Step 3 (`examples/quickstart/README.md`) is

```bash
cargo run -p autumn-harvest-cli -- --base-url http://localhost:3000/api/harvest preflight
```

and it cannot succeed as written, for anyone, ever. `GET /api/harvest/admin/preflight`
is behind `require_harvest_admin` → `has_harvest_admin_access`, which in the `dev`
profile demands a `Session` carrying `admin_auth_session_key` (default `"user_id"`).
A bare `reqwest` call from the CLI carries no cookie, so the check fails and the
route answers `401 "authentication required"`.

The **decision the issue deferred to a human** — options (a) dev bypass,
(b) documented token/session step, (c) drop the example — was resolved to
**(a): the `dev` profile admits an unauthenticated caller.**

---

## 1. Acceptance criteria (derived from the issue)

The issue has no numbered AC list. These are derived from its Evidence / Mechanism
paragraphs plus the resolved direction, and are what §7's test matrix is written
against.

| AC | Requirement (source) |
|----|----------------------|
| AC1 | The exact documented command — `harvest --base-url … preflight`, no session, no token, no cookie — reaches `GET /admin/preflight` in the `dev` profile instead of `401`. ("running the exact next documented command returns `401 Unauthorized`") |
| AC2 | The bypass is confined to the `dev` profile. Every non-`dev` profile (`prod`, `staging`, `unknown`, …) stays exactly as fail-closed as it is today. ("This is very likely intentional, security-conscious behavior … which is exactly why #1283 didn't touch it") |
| AC3 | The bypass is confined to callers with **no established session**. A caller that *does* present an established session is still judged by the existing `admin_auth_session_key` rule, so `set_admin_auth_session_key` keeps meaning what it means today. |
| AC4 | An embedder-declared auth boundary still short-circuits to "admin" first (unchanged `admin_auth_boundary()` precedence), and the scoped-token path (#942) is untouched. |
| AC5 | The documented contract and the code agree. `set_deployment_profile`'s own doc comment already says "`dev` allows an unauthenticated local management API"; the README's preflight section already says the auth boundary matters "in non-dev profiles". The fix makes the code match the docs rather than the docs match the code. |
| AC6 | The widening is **observable**, not silent: a dev deployment running with an open management API says so at startup and in `harvest preflight`'s own `admin_auth_boundary` check. |
| AC7 | The quickstart README no longer needs an undocumented manual step for Step 3, and the top-level README's preflight section states the dev-profile rule explicitly. |

Implicit-but-binding (from the repo's own invariants, not the issue text):

| AC | Requirement |
|----|-------------|
| AC8 | No existing security assertion regresses. `autumn-harvest-plugin/tests/security.rs` already pins the dev-profile session-key semantics (`eris_builtin_guard_does_not_accept_hard_coded_admin_id`, `eris_builtin_guard_honors_configured_session_key`) and the unauthenticated-blocked set. All must still pass unchanged. |
| AC9 | `has_harvest_admin_access` is not only the `require_harvest_admin` gate — it also drives Vantage UI redaction (`ui.rs`), payload decode-on-read (`read_path_decoder`, #608) and the high-impact route guards in `api.rs`. The change's blast radius across all of those must be deliberate, not incidental. |
| AC10 | The read-only operator role (#776) boundary is unaffected: it is a separate, opt-in `enforce_read_only_class` layer keyed off `CLASSIFIED_ROUTES`, and nothing here touches it. |

---

## 2. Brainstorming — candidate designs

1. **`dev` ⇒ unconditional `true`.** Delete the session check in the dev branch
   entirely. Simplest possible reading of "dev allows an unauthenticated local
   management API".
2. **`session == None` ⇒ `true` in `dev`.** Treat "no `Session` extension at all"
   as the unauthenticated-CLI signal. This is the mechanism the issue itself
   names.
3. **No *established* session ⇒ `true` in `dev`.** Admit a caller that presented
   no session cookie — `session == None` (no session layer mounted) **or** a
   `Session` the layer minted fresh for this request (`!is_cookie_backed()`).
4. **Loopback-only bypass.** Admit unauthenticated callers in `dev` only when the
   peer address is loopback.
5. **Opt-in env knob** (`AUTUMN_HARVEST_DEV_ADMIN_OPEN=1`) that the quickstart
   documents.
6. **Mint a dev session/token in the quickstart** (option (b) from the issue):
   `harvest token bootstrap` → run the printed SQL → `HARVEST_TOKEN=… preflight`.
7. **Drop Step 3 from the quickstart** (option (c) from the issue).
8. **A dedicated `X-Harvest-Dev` header** the CLI sends and the dev profile honours.

### Elimination

- **(4) is not implementable here.** The peer address would have to arrive as an
  axum `ConnectInfo` extension. `grep -rn 'ConnectInfo|peer_addr'` over the whole
  workspace returns **nothing**, and autumn-web wraps the loopback `ConnectInfo`
  layer around the finished `Router` at its own `axum::serve` boundary — the
  plugin never sees it. Building this means changing how the host app is served,
  which is far outside a getting-started fix.
- **(5), (6), (7)** were the alternatives put to the maintainer and were not
  chosen. (6) additionally has a chicken-and-egg wrinkle worth recording:
  `POST /admin/tokens` is itself admin-gated, so the quickstart would have to use
  the offline `harvest token bootstrap` + manual SQL path — three extra steps in
  a doc whose promise is "under 5 minutes".
- **(8)** is security theatre: a header any caller can set is not a boundary, and
  it adds a CLI/server protocol coupling for nothing.
- **(1) breaks AC3 and AC8.** `eris_builtin_guard_does_not_accept_hard_coded_admin_id`
  asserts that in `dev`, a session carrying `admin_id` when the configured key is
  `operator_id` gets `401`. Unconditional `true` deletes that guarantee and that
  test. It would also silently strip the gate from a dev app that mounts its own
  auth middleware without going through `api_with_auth`.
- **(2) does not actually fix the bug.** This is the load-bearing finding of the
  investigation. The issue's stated mechanism — "a bare CLI request over
  `reqwest` (no cookies) always has `session = None`" — **is wrong for the
  quickstart app.** autumn-web builds its session layer *unconditionally*
  (`router.rs`: `build_session_layer(...)` is not an `option_layer`), and that
  layer inserts a `Session` extension on **every** request — minting a fresh,
  empty, non-cookie-backed one when no valid cookie is present
  (`session.rs`, "2. Create session handle and insert into extensions"). The
  harvest router is mounted with `app.nest(&path, router)`, i.e. *inside* that
  layer. So the quickstart's CLI request arrives as
  `Some(<empty, non-cookie-backed Session>)`, not `None`, and a `None ⇒ true`
  patch would leave the reported `401` exactly where it is. `session == None` is
  real, but only for standalone integrations that mount `harvest_api_router`
  directly with no session layer — which is what the repo's own integration
  tests do, and why the mechanism looked like `None` from the test bench.

**Chosen: (3).**

---

## 3. The rule

```rust
if api_state.deployment_profile() == "dev" {
    match session {
        // No session layer mounted at all: nothing could ever have
        // authenticated this caller.
        None => true,
        // A session the layer minted for this request: the caller presented no
        // (valid) session cookie, so no principal was ever established.
        Some(s) if !s.is_cookie_backed().await => true,
        // An established session: unchanged — must carry the configured key.
        Some(s) => s.contains_key(&api_state.admin_auth_session_key()).await,
    }
} else {
    // unchanged
}
```

`is_cookie_backed()` is autumn-web's own public accessor for exactly this
question — "did this session id come from a valid request cookie, or was it
generated for the current request?" Using it, rather than probing for an empty
data map, means the predicate tracks autumn-web's definition of *established*
and needs no new surface on `Session`.

Three properties fall out:

- It is **strictly a widening for callers who presented no session cookie.** A
  cookie-backed session takes the identical path it takes today, so AC3 and AC8
  hold by construction. Every existing test session is built with
  `Session::new_for_test`, which is `new_cookie_backed` — no existing assertion
  moves.
- It is **unreachable outside `dev`.** The `else` arm is untouched, and
  `admin_auth_boundary()` still returns `true` before either arm is reached.
- It **also fixes the dashboard**, which the issue does not mention: a browser
  hitting `http://localhost:3000/api/harvest/ui` in the quickstart has no
  harvest session either, so the admin-gated Vantage pages 401 today for the
  same reason. Same root cause, same fix, one behaviour.

---

## 4. Reverse brainstorming — how would we make this *worse*?

Deliberately enumerating the ways this change could do damage, then checking each
against the design.

| # | How to break it | Does the design? |
|---|-----------------|------------------|
| R1 | **Let the bypass escape `dev`.** Key it off `debug_assertions`, or off "profile is not prod", or off `admin_auth_boundary == false` alone — any of which opens a staging or unknown-profile deployment. | No. The predicate is inside the existing `profile == "dev"` arm and nothing else moves. AC2 is a test, not a comment (`dev_bypass_does_not_apply_to_non_dev_profiles`, plus the `unknown`-profile blocked set that already exists). |
| R2 | **Let a *populated* session in `dev` slip through the key check**, quietly deleting `set_admin_auth_session_key`'s meaning for embedders who mount their own middleware. | No — that is exactly why candidate (1) was rejected. Cookie-backed sessions are routed to the unchanged branch. |
| R3 | **Make it silent.** A dev deployment left running on a shared network with an open admin API and nothing in the logs saying so. | Addressed by AC6: a `tracing::warn!` at plugin build when `profile == dev` and no boundary is declared, and an explicit `unauthenticated_access` field on the preflight `admin_auth_boundary` check's detail so `harvest preflight` prints it. |
| R4 | **Widen more than the gate.** `has_harvest_admin_access` also feeds UI redaction, describe-route filtering and the high-impact route guards. Changing it changes all of them at once. | Deliberate and documented (AC9). In `dev`, with no boundary, all of these already treat any keyed session as admin; extending that to a cookieless caller is the same policy, and is what makes the dashboard work too (§3). Nothing outside `dev` moves. |
| R5 | **Confuse "no cookie" with "bad cookie".** A caller with a forged/stale cookie is non-cookie-backed and would be admitted. | True, and correct: a forged cookie establishes no principal, so it is the unauthenticated case. It is also unreachable outside `dev`. |
| R6 | **Regress the scoped-token path (#942).** `require_harvest_admin` short-circuits on `TokenPrincipal` before ever calling this function. | Untouched; `token_auth_integration.rs` is unmodified and still passes. |
| R7 | **Regress the read-only operator role (#776).** | Untouched: that is a separate opt-in layer over `CLASSIFIED_ROUTES` (AC10). |
| R8 | **Fix the code and leave the docs wrong**, so the next clean-room repro files the same issue about the *dashboard* or about non-dev. | AC7: the quickstart Step 3 gains a one-line note, and the README preflight section states the dev rule and the non-dev consequence. |
| R9 | **Test it only at the unit/router bench**, where `session == None` — i.e. write a test that passes for the wrong reason and never covers the real quickstart shape (`Some(non-cookie-backed)`). | This is the trap candidate (2) fell into. The red phase pins **both** shapes, and `Session::new_for_test_without_cookie` exists precisely to build the second. |

---

## 5. Six hats

**⚪ White (facts).**
`has_harvest_admin_access` (`autumn-harvest-plugin/src/api.rs`, ~5363) returns
`true` on a declared boundary, else requires a `Session`. `dev` requires
`contains_key(admin_auth_session_key)`; non-`dev` requires an admin role/flag
claim. `require_harvest_admin` short-circuits on `TokenPrincipal` first.
autumn-web's session layer is unconditional and always inserts a `Session`;
`Session::is_cookie_backed()` distinguishes a cookie-derived session from one
minted for the request. The plugin mounts via `app.nest`, inside that layer.
`set_deployment_profile`'s doc comment already promises "`dev` allows an
unauthenticated local management API". `check_admin_auth_boundary` already
reports `Pass` in `dev` with the message "admin API auth boundary is optional for
the dev profile". Nothing in the quickstart, `examples/quickstart/autumn.toml`,
or the README establishes a session.

**🔴 Red (instinct).**
Two reactions worth naming. First: a getting-started doc whose third step is
impossible is a bad first impression, and the failure mode is the worst kind —
it looks like the *reader* did something wrong. Second, pulling the other way:
"loosen an auth check to fix a docs bug" is a sentence that should make anyone
uneasy. The discomfort is what forced the scoping in §3 — the change is a
widening only for callers who presented no credential at all, only in `dev`, only
with no boundary declared, and it is loud about it.

**⚫ Black (risks).**
A `dev`-profile app bound to `0.0.0.0` on a shared network exposes its management
API. That is *already* true of every other harvest surface in `dev` and is
already what the code's own docs promise; what changes is that it becomes true in
practice rather than accidentally-not-true for cookieless callers. Mitigations:
the startup warning, the preflight detail, and the fact that `set_admin_auth_boundary(true)`
remains a one-call opt-out for an embedder with its own middleware. Second risk:
an embedder in `dev` with hand-rolled auth middleware that never calls
`api_with_auth` loses nothing under this design (R2) — but they *would* have lost
their gate under candidate (1), which is the strongest argument for (3) over (1).

**🟡 Yellow (upside).**
The quickstart works verbatim. The dashboard works verbatim, which the issue did
not even ask for. Code and its own doc comments stop contradicting each other. No
new config knob, no new env var, no new CLI flag, no migration, no
`WorkflowEvent` variant, no change to `harvest_events` — none of `CLAUDE.md`'s
append-only invariants are anywhere near this. The diff is one `match` in the
plugin, one warning, one preflight detail field, and docs.

**🟢 Green (alternatives).**
Nine candidates in §2. The two that survive as future work rather than
alternatives: a loopback-only restriction becomes available for free if
autumn-web ever surfaces `ConnectInfo` to nested plugin routers, and it would
tighten this without changing the contract; and if the dev-profile posture is
ever revisited wholesale, `set_admin_auth_boundary` is the seam to hang it on.

**🔵 Blue (process).**
Red → green → refactor, in that order, with the red phase written to fail against
the *quickstart's* request shape and not just the test bench's (R9). Then
multi-angle agent review, then an AC evidence table, then the PR against
`trunk-dev` (per `CLAUDE.md`), ready-for-review, with a `docs/changelog.d`
fragment.

---

## 6. Blast radius

`has_harvest_admin_access` callers, and what the change means for each **in the
`dev` profile with no declared boundary, for a caller with no session cookie**:

| Call site | Effect |
|-----------|--------|
| `api.rs` `require_harvest_admin` | Admin routes (incl. `/admin/preflight`) reachable — **the fix**. |
| `ui.rs:1392`, `ui.rs:1461` | Vantage dashboard admin pages and log panes render instead of `401` — same root cause, intended. |
| `api.rs` `read_path_decoder` | Payload decode-on-read (#608): the local caller is treated as admin, so — *and only where the deployment already opted in via `decode_payloads_on_read`* — reads return decoded rather than stored (possibly ciphertext) payloads. |
| `api.rs` `start_workflow` (both guards), `signal_with_start_workflow`, `update_with_start_workflow` | The `terminate_if_running` / terminate-existing start conflict guards admit the local caller. |
| `api.rs` `batch_start_workflows`, `batch_reset_workflows` | Bulk start/reset operations admit the local caller. |
| `api.rs` `stream_execution_events` | The event-stream guard admits the local caller. |

Call sites are named by function rather than line number on purpose: this table
ships in the same commit that inserts ~60 lines into `api.rs`, so any line
numbers written here would be stale before the first reader ever opened it.

All of these are `dev`-only and boundary-absent-only. In every other profile, and
in any deployment that declares a boundary or presents an established session,
the behaviour is byte-for-byte what it was.

---

## 7. Test matrix

`autumn-harvest-plugin/tests/security.rs` (no database required — the gate
answers before any handler runs).

| Test | AC | Shape |
|------|----|-------|
| `dev_profile_admits_preflight_with_no_session_extension` | AC1 | `dev`, no `Session` extension → not `401`/`403` |
| `dev_profile_admits_preflight_with_fresh_cookieless_session` | AC1 | `dev`, `Session::new_for_test_without_cookie` (the quickstart's real shape) → not `401`/`403` |
| `dev_profile_admits_admin_route_with_no_session_extension` | AC1 | as above on a second admin route, so the fix is the gate and not one handler |
| `dev_profile_still_requires_configured_key_for_established_session` | AC3 | `dev`, cookie-backed session with the wrong key → `401` (the existing `…hard_coded_admin_id` guarantee, restated against `/admin/preflight`) |
| `non_dev_profile_rejects_cookieless_session_on_admin_route` | AC2 | `prod`, cookieless session → `401` |
| `unknown_profile_rejects_cookieless_session_on_admin_route` | AC2 | default (`unknown`) profile, cookieless session → `401` |
| `declared_auth_boundary_still_short_circuits_in_dev` | AC4 | `dev` + `set_admin_auth_boundary(true)` → not `401` (precedence unchanged) |
| existing `eris_*` suite, unmodified | AC8 | all still pass |
| `preflight_integration.rs` dev-profile check detail | AC6 | `admin_auth_boundary` check carries `unauthenticated_access: true` in `dev`, `false` otherwise |

---

## 8. Out of scope

- Changing the non-`dev` posture in any way.
- A loopback/peer-address restriction (§2, elimination of candidate 4).
- The scoped-token (#942) and read-only-role (#776) paths.
- `scripts/clean-room-quickstart.sh` from #1283 — not on `trunk-dev` at the time
  of writing; its preflight step is a soft-fail and becomes a hard pass once this
  lands, with no edit needed.
