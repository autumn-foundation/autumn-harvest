## Phase 6.3 — The dev profile serves the management API unauthenticated, as documented (issue #1284)

The quickstart's documented Step 3 —

```bash
cargo run -p autumn-harvest-cli -- --base-url http://localhost:3000/api/harvest preflight
```

— returned `401 "authentication required"` from `GET /api/harvest/admin/preflight`
on a fresh `AUTUMN_PROFILE=dev` app, and could not succeed for anyone, ever,
without a manual step no doc mentioned. Found while clean-room-reproducing the
quickstart in #1283.

**What was wrong.** `has_harvest_admin_access` required a `Session` carrying
`admin_auth_session_key` in *every* profile, so a bare cookieless request always
failed the gate. That contradicted two things the codebase already said in
writing: `set_deployment_profile`'s own doc comment ("`dev` allows an
unauthenticated local management API") and preflight's `admin_auth_boundary`
check ("admin API auth boundary is optional for the dev profile"). The fix makes
the code match its contract rather than the reverse. The issue deferred the
direction to a human and it was resolved to option (a), the dev-profile bypass —
over (b) documenting a token-minting step and (c) dropping the example.

**The load-bearing correction to the issue's diagnosis.** The issue attributed
the 401 to `session = None` for a `reqwest` caller. That is true only for a
standalone integration that mounts `harvest_api_router` with no session layer —
which is what the repo's own test bench does, and is why it looked that way.
autumn-web builds its session layer **unconditionally** and that layer inserts a
`Session` on every request, minting a fresh empty one when no valid cookie is
present; `HarvestPlugin` mounts through `app.nest(..)`, *inside* it. So the real
quickstart's CLI call arrives as `Some(session)`, and a `None ⇒ allow` patch
would have passed every plausible test and left the reported 401 exactly where
it was. Both shapes are pinned by tests, and the second one is the point.

**What shipped.**

- `has_harvest_admin_access` admits a caller that established **no session
  principal** — no `Session` extension, or a `Session` that is not cookie-backed
  (`Session::is_cookie_backed()` is autumn-web's own accessor for "did this id
  come from a valid request cookie, or was it generated for this request?"). A
  forged or stale cookie lands here correctly: it establishes no principal
  either.
- The widening is bounded on three sides, each a test rather than a comment:
  **`dev` only** (every other profile, including the default `unknown` a
  standalone integration gets for free, is byte-for-byte unchanged and
  fail-closed); **no-established-session only** (a cookie-backed session is still
  judged by `admin_auth_session_key`, so an embedder running its own auth
  middleware in `dev` keeps exactly the gate it has today — this is why the
  simpler "`dev` ⇒ `true`" was rejected); and **boundary precedence unchanged**
  (`admin_auth_boundary()` still short-circuits above it, and the #942
  `TokenPrincipal` path short-circuits earlier still, in
  `require_harvest_admin`).
- Never silent. `plugin::warn_if_dev_admin_api_is_open` logs the posture at
  startup and names the two ways to close it; `check_admin_auth_boundary` reports
  it as a stable `unauthenticated_access` boolean (always present, never
  conditionally omitted) so a release script can gate on the field instead of
  string-matching a summary. The `dev`-profile summary now reads "admin API is
  reachable unauthenticated…" rather than "…boundary is optional".
- Side effect, deliberate and documented: `has_harvest_admin_access` also drives
  Vantage UI redaction, payload decode-on-read (`read_path_decoder`, #608) and
  the high-impact route guards, so the quickstart's **dashboard** admin pages work for the same reason
  and by the same bound. That was a second instance of the same bug the issue did
  not name.

**A second unrunnable documented command, found by the new CI step.** With the
401 fixed, the CI job got one step further and cargo itself failed: exit `101`,
"could not determine which binary to run", because `autumn-harvest-cli` ships
two binaries (`harvest`, `harvest-replay`) and declared no `default-run` — so
the bare `cargo run -p autumn-harvest-cli -- …` form the docs use in dozens of
places could not work as written. The same shape of bug as the 401, one layer
down. This branch fixed it with `default-run = "harvest"`; **#1319 landed the
same fix on `trunk-dev` independently**, with a `scripts/check-default-run.sh`
guard, while this PR was in review. The merge keeps #1319's version and its
guard — this branch carries no `Cargo.toml` change any more.

Worth recording because a plain `git merge` did *not* surface it: both sides
added the key in different parts of `[package]`, so git auto-merged them into a
**duplicate `default-run`** — an invalid manifest that no conflict marker warned
about.

**The quickstart CI step is the union of both issues' guards.** #1319's step
tolerated a `401` from the admin route as a known-deferred outcome, citing this
very issue. That tolerance is now removed: with #1284 fixed, a `401` there is a
regression, not a known state. The step keeps #1319's ambiguous-binary check and
its errexit-safe status capture (an `if` condition — the same `bash -e` hazard
this branch had independently found and fixed), and adds the raw-status
assertion so a re-gated route reads as "401 came back" rather than an opaque
exit code.

**No migration, no new `WorkflowEvent` variant, no `harvest_events` writer** —
none of the append-only invariants are anywhere near this. The read-only operator
role (#776) is untouched: it is a separate opt-in `enforce_read_only_class` layer
over `CLASSIFIED_ROUTES`.

**Docs.** `examples/quickstart/README.md` Step 3 says why no credential is
needed and links onward; the top-level README's *Deployment preflight* section
gains an "Authenticating the call" split (dev vs. everything else, with
`--token`/`HARVEST_TOKEN`); `docs/security-posture.md`'s "Local / development (no
auth)" posture is corrected — it claimed "all routes are reachable" without the
profile condition that actually makes it so — and its "CLI token semantics"
section now separates ungated from admin-gated routes.

**Tests** (all no-DB — the gate answers before any handler runs, so the whole
matrix runs without Postgres). `autumn-harvest-plugin/tests/security.rs`, eight
new `hermes_*` cases: `dev_profile_admits_preflight_with_no_session_extension`
and `…_with_fresh_cookieless_session` (the two shapes),
`…_admits_second_admin_route_when_unauthenticated` (proves the fix is the shared
gate, not one handler), `…_still_requires_configured_key_for_established_session`
and `…_admits_established_session_with_configured_key` (AC3 both ways),
`non_dev_profile_rejects_unauthenticated_admin_route` and
`unknown_profile_rejects_unauthenticated_admin_route` (AC2),
`declared_auth_boundary_still_short_circuits_in_dev` (AC4). Unit:
`plugin::tests::dev_admin_api_is_open_only_without_a_boundary_in_the_dev_profile`
(the warning predicate, including `"DEV"` and `""` non-matches) and
`preflight::tests::admin_auth_boundary_reports_unauthenticated_dev_access`. The
pre-existing `eris_*` suite is unmodified and green — 108/108 in `security.rs`,
1030/1030 in the plugin lib.

**Planning record.** `DESIGN-1284.md` carries the derived AC table, a
nine-candidate brainstorm with the elimination reasoning (including why a
loopback-only restriction is not implementable here — no `ConnectInfo` reaches a
nested plugin router), a reverse-brainstorm risk table (R1–R9), a six-hats pass,
and the blast-radius table for every `has_harvest_admin_access` call site.
