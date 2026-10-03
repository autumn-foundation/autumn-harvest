## Feature — a published TypeScript client, and a harvest-server evaluation (issue #1616)

Part of epic #1605. Part 1 ships code. Part 2 is a design note.

**Typed core responses.** Seven lifecycle routes now declare a type on every
response field and every array element: start, status, result, signal,
cancel, terminate and health. The list is
`autumn_harvest_plugin::openapi::CORE_CLIENT_ROUTES`. A pass-through value,
such as workflow input, has the type `any` and reaches a client as `unknown`.

- A contract field can now carry `nullable`, nested `fields`, `items`, and the
  type `any`. `any` publishes `x-harvest-any`, so a reader can tell an open
  value from a field with no type yet.
- The transform rejects a type outside the contract set and an unknown field
  key. It also rejects `fields` on a non-object, `items` on a non-array, an
  empty `fields` list, a nameless field object, `nullable` with no concrete
  type, and a non-boolean `required` or `nullable`.
- **Fix:** a bare `object` field now publishes `additionalProperties: true`.
  Without it, `openapi-typescript` emits `Record<string, never>`. That type
  says the object is empty.
- **Fix:** `GET /workflows/{id}` declares `legal_hold`. The handler always
  sends it, but the contract did not list it.
- **Fix:** the start 200 and 201 bodies no longer list the six fields that only
  the deferred 202 sends. The 202 now marks `workflow_name` and `workflow_id`
  required.

**The client package.** `clients/typescript` builds `autumn-harvest-client`
from `docs/openapi.json`. No running app is necessary. The package version is
the crate version. It exports these:

- `createHarvestClient`, a thin factory over `openapi-fetch`. `baseUrl` is
  required, so a forgotten URL cannot send a credential to localhost.
- `StartedWorkflow`, `DeferredStart`, `WorkflowStatus`, `WorkflowOutcome` and
  `HealthResponse`, the core response bodies.
- `isStarted`, a guard that separates a started run from a deferred start.
- The generated `paths` and `operations` types.

**The pipeline.**

- `scripts/build-typescript-client.sh` generates, type-checks, tests, builds
  and packs the client. It then installs the tarball into a clean consumer
  and type-checks against it. Each npm registry call gets one retry. Progress
  goes to stderr, and stdout holds only the tarball path.
- The new CI job `typescript-client-package` runs the script. It blocks no
  merge until an admin adds it to the required checks.
- `release.yml` runs the script before it creates the release, and attaches
  the tarball. The release fails when the package version differs from the
  tag. The first release with the package is the one after 0.6.0.
- The package is not on the npm registry. That needs a package name and an
  `NPM_TOKEN` secret.

**`examples/typescript-client`** now reads `execution_id` and
`execution.state` through the generated types, with no casts.

**harvest-server.** `DESIGN-1616.md` evaluates a standalone binary. It
recommends against a prebuilt binary. Workflows are Rust code, so the same
build must compile the binary and the workflows. A `dylib` plugin has no
stable ABI. The `hot-code-swap` spike (#967) rules out WASM workflows with full
`WorkflowContext` parity. The recommended path is the existing thin-binary
pattern, `examples/standalone-runner`. A `cargo generate` template made from
it comes next.

Tests:

- `tests/openapi_response_conformance.rs` (new, DB) drives each core route
  against Postgres. Each step asserts its status and, for a start, the flag
  that names its shape. It covers these shapes: normal, attach, idempotent
  201 and 200, pinned, debounced, batched, flushed and throttled starts. It
  also covers the 204 and 200 results, and a repeated cancel and terminate.
  Every body must match the published schema. The check treats an object
  without `additionalProperties: true` as closed.
- `openapi_spec` gains a typed-field gate for the core routes, a package
  version pin, and job-scoped checks of the CI and release wiring.
- Unit tests in `openapi.rs` cover each new key and each rejection.
- `clients/typescript/test` holds type-level and runtime tests.

No migration. No new `WorkflowEvent` variant. No `harvest_events` change. No
route change.
