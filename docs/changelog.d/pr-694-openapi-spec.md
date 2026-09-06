## Phase 4.x — OpenAPI 3.1 spec for the management API (issue #694)

The management API now publishes an OpenAPI 3.1 document, so an integrator
generates a typed client in any language instead of hand-writing HTTP calls
against routes reverse-engineered from prose.

**What shipped.**

- `GET {api_path}/openapi.json` (`/api/harvest/openapi.json` by convention) —
  a read-only, never admin-gated route serving the document as
  `application/json`. Classified `RouteClass::PublicSafe` alongside
  `GET /health`: it describes the route surface, carries no execution state,
  and a client generator must reach it before it holds a credential.
- [`docs/openapi.json`](../openapi.json) — the same document, checked in,
  regenerated with
  `cargo run -p autumn-harvest-plugin --example emit_openapi > docs/openapi.json`.
- `autumn-harvest-plugin/src/openapi.rs` — the transform. `docs/api-contract.json`
  is the single source: it is compiled in with `include_str!`, and the document
  is built once behind a `LazyLock`. The served endpoint and the artifact are
  the same transform output, so they cannot disagree.
- [`docs/openapi.md`](../openapi.md) — the reader's page: the ten-minute typed
  client, what the document carries, and what it deliberately leaves open.
- [`examples/typescript-client`](../../examples/typescript-client) — a worked
  `openapi-typescript` + `openapi-fetch` client that starts a workflow and reads
  its status with no hand-written URL, method or `fetch` call.

**Design decisions.**

- **Derived, not annotated.** `autumn-web` generates OpenAPI from routes
  declared with its route macros, which attach `ApiDoc` metadata to a handler.
  `harvest_api_router` is a plain `axum::Router`, so its 168 routes carry none.
  The contract already records more than the macros could infer — per-parameter
  `required` flags, read-only class, idempotency rules, error responses — so the
  contract is the input and the module is a pure transform.
- **Fail closed on a contract gap.** The transform rejects a parameter with no
  `in` or no `required`, rather than guessing. Two new contract regression
  tests (`contract_params_declare_location_and_required`,
  `contract_declares_every_path_parameter`) name the offending route first, so
  the spec build never fails opaquely.
- **`x-harvest-read-only` per operation**, cross-checked against
  `autumn_harvest::audit::CLASSIFIED_ROUTES`, so tooling can separate reads from
  control-plane writes. The route category is the operation tag.
- **Security schemes are declared, not enforced** (issue #174):
  `HarvestBearerToken` and `HarvestSessionCookie`, plus an empty top-level
  requirement recording that enforcement is the embedder's.
- **A route that takes no body declares none.** A `DELETE` with an empty field
  list would otherwise tell a generated client to send `{}`.

**Contract defects the spec surfaced and fixed.** `GET /admin/schedules/{id}/runs`
and `GET /admin/schedules/{id}/decisions` never declared their `{id}` path
parameter, and 13 query parameters across three schedule routes declared neither
`in` nor `required` — a generated client could not have addressed the first two
at all. The two SSE routes now declare `content_type: text/event-stream`
explicitly rather than hiding it in prose.

**Invariants.** No new `WorkflowEvent` variant, no migration, no shard-semantics
change, no `harvest_events` write path. One additive read-only route.

**Test evidence.**

- `autumn-harvest-plugin/tests/openapi_spec.rs` (9 tests, one sorted line in
  `.github/ci/integration-suites.txt`): exact route coverage against
  `management_api_routes()` in both directions; parameters carry `in`,
  `required` and a schema, and path parameters match the path template;
  every operation declares a response with a schema; request bodies follow the
  contract; `x-harvest-read-only` matches `CLASSIFIED_ROUTES`; the security
  stub; structural validity with unique `operationId`s and resolvable `$ref`s;
  the checked-in artifact equals the generated document byte for byte; and the
  served endpoint returns exactly that document.
- 11 unit tests in `openapi.rs` for the transform edges: a missing `required`
  flag is rejected, an empty body list publishes no request body, duplicate
  statuses merge, repeated query keys explode, stream responses type as frames.
- CI: the `lint` job validates `docs/openapi.json` with `openapi-spec-validator`
  (zero errors) on every pull request, documentation-only ones included; the
  `changes` job now carves `docs/api-contract.json` and `docs/openapi.json` out
  of the documentation-only skip, so a contract-only pull request still runs the
  tests that compare them. A new `openapi-client-smoke` job generates a
  TypeScript client from the **served** document against a running quickstart
  app, typechecks it, and drives `POST /workflows/{name}/start` plus
  `GET /workflows/{id}` to `COMPLETED`.
