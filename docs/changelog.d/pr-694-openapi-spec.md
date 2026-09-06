## Phase 4.x — OpenAPI 3.1 spec for the management API (issue #694)

The management API now publishes an OpenAPI 3.1 document, so an integrator
generates a typed client in any language instead of hand-writing HTTP calls
against routes reverse-engineered from prose.

**What shipped.**

- `GET {api_path}/openapi.json` (`/api/harvest/openapi.json` by convention) —
  a read-only route serving the document as `application/json`, behind no admin
  gate. Classified `RouteClass::PublicSafe` alongside `GET /health`: it
  describes the route surface, carries no execution state, and a client
  generator must reach it before it holds a credential. An embedder that wraps
  the router with `api_with_auth` still gates it.
- `autumn-harvest-plugin/openapi.json` — the compact document, compiled into
  the crate with `include_str!` and served verbatim.
- [`docs/openapi.json`](../openapi.json) — the same document, pretty-printed
  for reading and for review diffs.
- `autumn-harvest-plugin/src/openapi.rs` — the transform and the serving half.
  `scripts/regenerate-openapi.sh` writes both copies from one run of the
  transform over `docs/api-contract.json`.
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
- **The crate ships what it serves.** A published crate packages no file from
  outside its own directory, so the library compiles in
  `autumn-harvest-plugin/openapi.json` rather than reaching into `docs/`. The
  handler returns those bytes with no transform, no parse and no allocation, so
  a contract defect can never reach a served request. The transform runs in the
  generator and in tests, where it names the offending route.
- **Fail closed on a contract gap.** The transform rejects five defects. A
  parameter with no `in` or no `required`. A body-bearing route with no
  `required` flag. A read method that documents a body. A colliding
  `operationId`. A description key that is neither a string nor a list of
  strings. Two new contract regression tests name the offending route first, so
  the spec build never fails opaquely:
  `contract_params_declare_location_and_required` and
  `contract_declares_every_path_parameter`.
- **`x-harvest-read-only` and `x-harvest-route-class` per operation**, both
  cross-checked against `autumn_harvest::audit::CLASSIFIED_ROUTES`, so tooling
  can separate reads from control-plane writes. The route category is the
  operation tag, and `x-harvest-stream` marks the two `text/event-stream`
  routes a blocking client would otherwise buffer forever.
- **Security schemes are declared, not enforced** (issue #174):
  `HarvestBearerToken` and `HarvestSessionCookie`, plus an empty document-level
  requirement recording that enforcement is the embedder's. A `public_safe`
  route overrides it with `security: []`, the only way to say positively that a
  route needs no credential.
- **A route that takes no body declares none.** A `DELETE` with an empty field
  list would otherwise tell a generated client to send `{}`.

**Contract defects the spec surfaced and fixed.**

- `GET /admin/schedules/{id}/runs` and `GET /admin/schedules/{id}/decisions`
  never declared their `{id}` path parameter, so a generated client could not
  have addressed either route at all.
- 13 query parameters across three schedule routes declared neither `in` nor
  `required`.
- `POST /admin/schedules/preview` declared neither a required body nor a
  required `schedule_expr`, though the handler deserializes both with no serde
  default. A client that honoured the old contract would have been rejected.
- `GET /admin/gates` and `GET /admin/tokens` declared a free-form request body
  on a read method. The transform now refuses one outright.
- Six mutating routes returned an undeclared second success status: `204` on
  the two `/result` routes, and `200` on `/workflows/{name}/start`,
  `signal-with-start`, `update-with-start`, `POST /batch-operations`, the DAG
  retry dry run and the delivery redrive. Each is now declared, with its own
  body where the shape differs.
- The by-id family returns the resolved execution id in
  `X-Harvest-Execution-Id`, and a still-running `/result` returns `Retry-After`
  with its `204`. Both are now declared response headers instead of prose.
- The two SSE routes declare `content_type: text/event-stream` explicitly
  rather than hiding it in prose.

**Invariants.** No new `WorkflowEvent` variant, no migration, no shard-semantics
change, no `harvest_events` write path. One additive read-only route.

**Test evidence.**

`autumn-harvest-plugin/tests/openapi_spec.rs` holds 12 tests, wired by one
sorted line in `.github/ci/integration-suites.txt`. They assert:

- Exact route coverage against `management_api_routes()`, in both directions.
- Every parameter carries `in`, `required` and a schema. Path parameters match
  the path template.
- The success response of every route declares a body schema, unless it is a
  `204`.
- Request bodies follow the contract, and a route with no documented body
  publishes none.
- `x-harvest-read-only` and `x-harvest-route-class` match `CLASSIFIED_ROUTES`.
  Only a `public_safe` route waives its credential. Four routes are pinned by
  hand as well.
- The security stub, documented response headers, and the stream markers.
- Structural validity: `openapi` is `3.1.0`, the server URL and document
  version are pinned, `operationId`s are unique, and every `$ref` resolves.
- Both checked-in copies equal the generated document byte for byte, and the
  served endpoint returns exactly those bytes.

`openapi.rs` holds 18 unit tests for the transform edges. A missing `required`
flag on a parameter or a body is rejected. A read method with a body is
rejected. A colliding `operationId` is rejected. A non-string note is rejected.
An empty body list publishes no request body. A free-form body keeps its
fields. An additional response publishes its own schema. Duplicate statuses
merge, and a repeated sentence is said once. Repeated query keys explode.
Stream responses type as frames.

CI adds three gates. The `lint` job validates both copies with
`openapi-spec-validator`, on every pull request, documentation-only ones
included. The `changes` job carves `docs/api-contract.json` and
`docs/openapi.json` out of the documentation-only skip, so a contract-only pull
request still runs the tests that compare them. A new `openapi-client-smoke`
job generates a TypeScript client from the **served** document against a
running quickstart app, typechecks it, and drives
`POST /workflows/{name}/start` plus `GET /workflows/{id}` to `COMPLETED`.
