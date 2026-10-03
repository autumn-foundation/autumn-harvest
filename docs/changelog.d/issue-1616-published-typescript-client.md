## Feature — a published TypeScript client, and a harvest-server evaluation (issue #1616)

Part of epic #1605. Part 1 ships code. Part 2 is a design note.

**Typed core responses.** Seven lifecycle routes now type every response
field: start, status, result, signal, cancel, terminate and health. The list
is `autumn_harvest_plugin::openapi::CORE_CLIENT_ROUTES`.

- A contract response field can now carry `nullable`, nested `fields`,
  `items`, and the type `any`. `any` publishes `x-harvest-any`, so a reader
  can tell an open value from a field with no type yet.
- The transform rejects a type name outside JSON Schema. It also rejects
  `fields` on a non-object, `items` on a non-array, and `nullable` with no
  concrete type.
- **Fix:** a bare `object` field now publishes `additionalProperties: true`.
  Before, `openapi-typescript` emitted `Record<string, never>` for it, which
  said the object was empty.
- **Fix:** `GET /workflows/{id}` declares `legal_hold`. The handler always
  sent it, but the contract did not list it.

**The client package.** `clients/typescript` builds `autumn-harvest-client`
from `docs/openapi.json`. No running app is necessary. It exports
`createHarvestClient`, a thin factory over `openapi-fetch`, and the generated
`paths`, `components` and `operations` types. The package version is the
crate version.

- `scripts/build-typescript-client.sh` generates, type-checks, tests, builds
  and packs the client. It then installs the tarball into a clean consumer
  and type-checks against it.
- The new CI job `typescript-client-package` runs the script on each pull
  request.
- `release.yml` runs the script before it creates the release, and attaches
  the tarball. The release fails when the package version differs from the
  tag.
- The package is not on the npm registry. That needs a package name and an
  `NPM_TOKEN` secret.

**`examples/typescript-client`** now reads `execution_id` and
`execution.state` through the generated types, with no casts.

**harvest-server.** `DESIGN-1616.md` evaluates a standalone binary. It
recommends against a prebuilt binary: workflows are Rust code, so the server
must compile with them. A `dylib` plugin has no stable ABI, and the WASM
feature sandboxes activities, not workflows. The recommended path is the
existing thin-binary pattern, `examples/standalone-runner`, then a
`cargo generate` template made from it.

Tests:

- `tests/openapi_response_conformance.rs` (new, DB) drives each core route
  against Postgres. It covers the normal, deduplicated, pinned, debounced,
  batched, flushed and throttled start shapes, and the 204 and 200 result
  shapes. Every body must match the published schema exactly.
- `openapi_spec` gains a typed-field gate for the core routes, a package
  version pin, and job-scoped checks of the CI and release wiring.
- Unit tests in `openapi.rs` cover each new contract key and each rejection.
- `clients/typescript/test` holds type-level and runtime tests.

No migration. No new `WorkflowEvent` variant. No `harvest_events` change. No
route change.
