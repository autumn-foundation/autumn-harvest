## Phase — A standalone mount can reach a working admin credential (issue #1608)

The second of epic #1605's two roots, and the one that made the
standalone integration path unusable rather than merely awkward.

`harvest_api_router` gates its admin routes with `require_harvest_admin`,
which admits a request on one of three grounds: a verified
`TokenPrincipal` set by `enforce_token_scope` (issue #942), a declared
embedder auth boundary, or an autumn-web `Session`.

**A standalone mount reached none of the three.** `enforce_token_scope`
and `enforce_read_only_class` are both `pub`, but each had exactly one
call site, inside `HarvestPlugin`, behind `enable_api_tokens()` and
`api_with_role_auth` respectively. The three `HarvestApiState` settings
that `require_harvest_admin` and `preflight` read —
`set_admin_auth_boundary`, `set_deployment_profile` and
`set_admin_auth_session_key` — were likewise written from one non-test
place. The session path needs autumn-web by construction. So the scoped
API token, the one credential mechanism that is already
framework-neutral and is described as "standalone-token admin mode" in
`require_harvest_admin`'s own comment, could not actually be used
standalone.

**The fix: `StandaloneAdminAuth`.** A declaration an embedder builds, and
one `mount` call that applies it. Issue #1608 weighed exporting a
constructor against documenting the incantation and chose the
constructor, because the layer ordering is load-bearing and an embedder
reconstructing it from a doc comment gets it wrong. This goes one step
further and folds the three loose setters into the same call, so there is
no "right order at the right time" left for an embedder to get wrong
either. `mount` takes the composed router, so an embedder that nests
Vantage first gets Vantage covered by the read-only-role layer, exactly
as `HarvestPlugin` composes it.

**One ordering, one place.** The layer stack moved into
`api::apply_admin_auth_layers`, which both `HarvestPlugin` and
`StandaloneAdminAuth::mount` call. The two mount paths cannot drift, and
the argument for the ordering (token layer outside the read-only-class
layer, both inside the embedder's auth) now lives with the code that
implements it rather than in a comment in `plugin.rs`. Neither layer is
installed unless its opt-in is set, so a deployment that declares neither
does an identical amount of work as before — AC6 and AC7 of the original
issues are preserved by construction.

**Not in scope.** This is the primitive, not the blessed entry point:
issue #1613 is where a single standalone constructor belongs, and it
needs this. The example's own 401 preflight (issue #1609) is untouched;
it now has a credential available to it, but wiring one up is that
issue's work.

No new `WorkflowEvent` variant, no migration, no replay impact. No
behavior change on the plugin path.

**Tests.** New no-database suite
`autumn-harvest-plugin/tests/standalone_admin_auth.rs`, registered in
`.github/ci/integration-suites.txt` as an `allos` row. Each layer is
asserted present when declared and absent when not, so no assertion is
vacuous: the token layer refuses a claimed `hvst_` bearer it cannot
verify with 503 (a 401 would mean the layer never ran), and without the
opt-in the same request falls through to the admin gate and gets 401. A
declared boundary admits an admin route. The read-only-role layer denies
a read-only principal on a nested Vantage path, which carries no route
class and so fails closed. The declared profile and boundary are read
back out of the `preflight` report.

`token_auth_integration.rs` now assembles both of its apps through the
exported mount rather than a hand-written `from_fn_with_state` stack, so
the database-backed token suite — a minted token reaching an admin route,
a `read` token denied every mutating route, revocation and expiry — is
now evidence about the composition an embedder actually writes.
