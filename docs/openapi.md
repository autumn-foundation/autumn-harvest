# OpenAPI 3.1 spec for the management API

Harvest publishes an [OpenAPI 3.1](https://spec.openapis.org/oas/v3.1.0)
document for the whole management API, so any language with an OpenAPI
generator gets a typed client with no hand-written HTTP (issue #694).

One source, three places it appears:

| Where | What |
| --- | --- |
| `GET {api_path}/openapi.json` | Served by `harvest_api_router`. Read-only, and behind no admin gate. |
| `autumn-harvest-plugin/openapi.json` | Compact. Compiled into the crate and served verbatim, so the endpoint runs no transform and can never fail. |
| [`docs/openapi.json`](openapi.json) | The same document, pretty-printed, for offline codegen and review diffs. |

All three come from [`docs/api-contract.json`](api-contract.json). The chain is
contract to canonical route list to document, and a test pins every link. See
[Guarantees](#guarantees).

The crate carries its own copy because a published crate can package no file
from outside its own directory. `scripts/regenerate-openapi.sh` writes both
copies from one transform, and a test fails when either drifts.

## Generate a typed client in under ten minutes

The worked example lives in
[`examples/typescript-client`](../examples/typescript-client/README.md), which
also says how to get a plugin running. With one serving
`http://localhost:3000/api/harvest`:

```sh
cd examples/typescript-client
npm ci
npm run generate    # openapi-typescript reads the served document
npm run typecheck
npm start           # starts a workflow and polls its status
```

`src/start-and-poll.ts` calls `POST /workflows/{workflow_name}/start` and
`GET /workflows/{id}` through [`openapi-fetch`](https://openapi-ts.dev/openapi-fetch/).
It contains no URL string, no method string and no `fetch` call.

Other generators read the same document:

```sh
# Python
openapi-python-client generate --url http://localhost:3000/api/harvest/openapi.json

# Go, Java, and 50 other targets. Use 7.x or newer: earlier versions reject
# OpenAPI 3.1 outright.
openapi-generator generate -i docs/openapi.json -g go -o ./harvest-client
```

Node 20 or newer for the TypeScript path. The first `cargo run -p quickstart`
compiles the workspace, which is the slow part; the client steps take seconds.

## What the document carries

- **Paths relative to the mount point.** `servers[0].url` is
  `/api/harvest`, the conventional prefix. Change it when
  `HarvestPlugin::api` mounts the router elsewhere.
- **`operationId` per operation**, derived from the method and path
  (`post_workflows_by_workflow_name_start`). It is stable across
  regenerations, so a generated method name does not churn.
- **Parameters with explicit `required` flags**, for path, query and header.
  A repeatable query key is an exploded array.
- **A request-body schema** for every route that takes a body. A route that
  takes none declares none, so a generated client does not send an empty
  object.
- **`x-harvest-read-only`** and **`x-harvest-route-class`** on every
  operation. Both mirror `autumn_harvest::audit::CLASSIFIED_ROUTES`, so tooling
  can separate reads from control-plane writes, and can see which routes are
  safe without any credential. The route category is also the operation tag.
- **`x-harvest-idempotency`**, where the contract records an idempotency rule.
- **`x-harvest-stream`** on the two `text/event-stream` routes. A blocking
  client that buffers a whole response would hang on those.
- **Response headers** where the contract records them: `X-Harvest-Execution-Id`
  on the by-id family, `Retry-After` on the `204` a still-running `/result`
  returns.
- **Security schemes** for the two credentials the management API accepts:
  `HarvestBearerToken` (scoped API tokens, issue #942) and
  `HarvestSessionCookie` (the embedding application session). The top-level
  `security` list also carries an empty requirement, because enforcement is
  the embedder's choice (issue #174). The two `public_safe` routes
  (`GET /health`, `GET /openapi.json`) override it with `security: []`, which
  says positively that Harvest itself asks for no credential there. An embedder
  that wraps the router with `HarvestPlugin::api_with_auth` gates every route,
  the document included, so a generator then needs a credential to read it. See
  [`security-posture.md`](security-posture.md).

### Limits worth knowing

- **Response properties are open.** The contract records field names, and
  sometimes a type, not full JSON Schemas. A generator therefore types most
  response properties as `unknown` / `any`. Per-workflow message schemas are a
  different contract; see `GET /workflows/registered/{name}/schema`.
- **Streaming routes** declare `text/event-stream` with a string body. The
  frame grammar is in the response description.
- **Conditional shapes** (the partial-availability envelopes of issue #756)
  are described in prose on the response, not as a `oneOf`.
- **Error responses carry no schema.** The contract records a description per
  error status, not a body shape.
- **Most query parameters are typed `string`**, because the contract records a
  type for only a few. A generated client sends the value as text, which is
  what the handler parses.
- **Inline schemas, no `$ref`.** Nothing is shared between operations, so
  `openapi-generator` names response models positionally.
- `GET /workflows/by-id/{workflow_name}/{workflow_id}/children` declares
  `workflow_name` twice: once as the parent path parameter, once as a query
  filter on the child type. It is legal OpenAPI, but a generator that flattens
  parameters into one argument list may emit two arguments with one name.

## Regenerate the artifacts

```sh
scripts/regenerate-openapi.sh
```

Run it whenever `docs/api-contract.json` changes. CI fails otherwise.

## Guarantees

Four checks hold the chain together:

1. `contract_regression::management_routes_match_contract` fails when
   `management_api_routes()` and the contract disagree. That function is the
   canonical route list every registration surface is checked against.
2. `openapi_spec::document_covers_every_management_route_exactly` fails when
   the document misses a mounted route, or invents one.
3. `openapi_spec::checked_in_artifacts_match_the_generated_document` fails
   when either checked-in copy is stale, and
   `openapi_spec::served_endpoint_returns_the_document` fails when the endpoint
   serves anything else.
4. The `lint` job validates `docs/openapi.json` with
   `openapi-spec-validator`, and it runs on documentation-only pull requests
   too.

## Why the document is derived, not annotated

`autumn-web` builds an OpenAPI document from routes declared with its route
macros, which attach `ApiDoc` metadata to each handler.
`harvest_api_router` is a plain `axum::Router`, so its routes carry no such
metadata. Annotating the handlers would also record less than the contract
already holds: per-parameter `required` flags, per-route read-only class,
idempotency rules, and error responses. The contract is therefore the input,
and `autumn-harvest-plugin/src/openapi.rs` is the transform.
