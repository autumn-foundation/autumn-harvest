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
  `harvest_api_router` is a plain `axum::Router`, so its 169 routes carry none.
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
- Seven routes whose handler extracts a bare `Json<T>` declared an optional
  request body. Axum rejects a bodiless request there, so a client honouring
  the contract was rejected before the handler ran.
- `X-Harvest-Execution-Id` is stamped by `finalize_by_id` on delegated errors
  as well as successes, so it moved to a route-level `response_headers` list
  and now appears on every documented response of the by-id family.
- The `202` and `409` of `GET /workflows/{id}/update/{update_id}/result`
  documented no body, though the handler returns one for both. The transform
  now refuses an alternate response that declares neither a body nor a bodiless
  status.
- `POST /workflows/{name}/start` returns `202` when a debounce, event-batch or
  start-throttle policy defers admission, with a body that carries no
  `execution_id`. The status was undeclared, and `throttled`, `throttle_key`
  and `deferred_at` appeared nowhere in the contract.
- `POST /workflows/{id}/update/{update_name}` declared only its `202`. The
  default `wait=completed` path returns `200` with the handler output, and a
  failed or orphaned update returns `409` with a body. Both are now declared.
- The batch-operations dry run omitted `sample_cap`, which bounds the sample it
  returns beside it.
- Nineteen statuses that handlers really return were declared nowhere: the ten
  `207 Multi-Status` partial fan-outs (described in prose, never as a status),
  the reset dry-run `200` and conflict `409`, the `503` a health check returns
  when shard readiness is enforced, the `202` on cancel and DLQ replay, the
  `422` rejections on start and update, and the `503` an admission gate
  returns on the two with-start routes.
- `operator_id` is a non-`Option` field with no serde default on
  `POST /workflows/{id}/reset`, so axum rejects a body without it. The contract
  called it optional.
- Five alternate success responses said "same fields as the primary" in prose.
  They now carry the field list itself, so a generated client keeps
  `execution_id` on a reused start.
- Nine more statuses were selected in a helper rather than in the handler, so
  the first pass of the audit could not see them: the `207` all four pause and
  resume routes return on a partial fan-out, the `409` a paused execution
  returns to triage, legal-hold and resume, the `409` an exhausted schedule
  returns to backfill, the `400` and `413` of signal validation and the signal
  payload cap, the `413` of an oversized workflow input, and the `504` of an
  update-with-start wait window.
- Eleven request-body fields the API accepts were documented nowhere, so a
  typed client could not express them: `context_headers` and `priority` on
  start, `workflow_name` on the schedule patch, and eight on schedule creation
  including `timezone`, `end_at`, `max_runs` and `retry_policy`. The code-side
  `management_api_request_fields()` registry was missing them too.
- `POST /workflows/{name}/start` reads an `Idempotency-Key` request header that
  wins over the body field, and is the only way to recover an already-committed
  start when the body is malformed. It was undeclared.
- The whole by-id family resolves a business id before delegating, and the
  resolver fails closed with `503` when an expected shard has no pool or cannot
  be reached. Ten routes never declared it.
- `PATCH /tasks/{id}` has been mounted and audited since issue #249, but was
  missing from `management_api_routes()` and from the contract. It was
  therefore invisible to every existing guard, to the CLI coverage test, and to
  the published document. Found by the new router guard below.

**Portability.** `.gitattributes` pins `*.json` to LF. The generated document is
compiled into the plugin with `include_str!` and served verbatim, so a CRLF
checkout on Windows would make a Windows-built binary serve different bytes than
a Linux-built one. The Windows CI leg caught it through the byte-exact artifact
comparison.

**Invariants.** No new `WorkflowEvent` variant, no migration, no shard-semantics
change, no `harvest_events` write path. One additive read-only route.

**Test evidence.**

`autumn-harvest-plugin/tests/openapi_spec.rs` holds 13 tests, wired by one
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
- The route is nest-relative, so an embedding app that serves its own
  `/openapi.json` keeps it.

`docs/audits/openapi-response-coverage.py` reads the handlers and the contract,
and runs in the ungated `lint` job. It fails on three things: a status the
handler returns that the contract does not declare, a request-body field that is
mandatory on the wire but not marked required, and a field serde accepts that
the contract documents nowhere. Each handler is followed one level into the
helpers it calls, since a status is often chosen in a helper such as
`queue_pause_partial_status`. `map_error` is excluded: it translates a runtime
error variant, so its statuses belong to the error rather than to every route
that calls it. Both checks were confirmed to fail on a seeded gap.

Two new guards in `contract_regression.rs` read `src/api.rs` itself.
`every_registered_route_is_in_the_canonical_list` parses the router and fails
on a route missing from `management_api_routes()`; nothing checked that before,
and it found `PATCH /tasks/{id}`.
`contract_marks_a_mandatory_json_body_required` fails when a handler extracts a
bare `Json<T>` and the contract calls its body optional.

`openapi.rs` holds 20 unit tests for the transform edges. A missing `required`
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
