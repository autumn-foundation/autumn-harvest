# OpenAPI 3.1 spec for the management API

Harvest publishes an [OpenAPI 3.1](https://spec.openapis.org/oas/v3.1.0)
document for the whole management API, so any language with an OpenAPI
generator gets a typed client with no hand-written HTTP (issue #694).

Two copies, one source:

| Where | What |
| --- | --- |
| `GET {api_path}/openapi.json` | Served by `harvest_api_router`. Read-only, never admin-gated. |
| [`docs/openapi.json`](openapi.json) | The same document, checked in, for offline codegen and diff review. |

Both are generated from [`docs/api-contract.json`](api-contract.json), the
contract the router itself is pinned against. A route cannot reach the router
without reaching the document. See [Guarantees](#guarantees).

## Generate a typed client in under ten minutes

The worked example lives in [`examples/typescript-client`](../examples/typescript-client).
With a plugin running at `http://localhost:3000/api/harvest`:

```sh
cd examples/typescript-client
npm install
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

# Go, Java, and 50 other targets
openapi-generator generate -i docs/openapi.json -g go -o ./harvest-client
```

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
- **`x-harvest-read-only`** on every operation. It mirrors
  `autumn_harvest::audit::CLASSIFIED_ROUTES`, so tooling can separate reads
  from control-plane writes. The route category is also the operation tag.
- **`x-harvest-idempotency`**, where the contract records an idempotency rule.
- **Security schemes** for the two credentials the management API accepts:
  `HarvestBearerToken` (scoped API tokens, issue #942) and
  `HarvestSessionCookie` (the embedding application session). The top-level
  `security` list also carries an empty requirement, because enforcement is
  the embedder's choice (issue #174). See
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

## Regenerate the artifact

```sh
cargo run -p autumn-harvest-plugin --example emit_openapi > docs/openapi.json
```

Run it whenever `docs/api-contract.json` changes. CI fails otherwise.

## Guarantees

Four checks hold the chain together:

1. `contract_regression::management_routes_match_contract` fails when the
   router and the contract disagree.
2. `openapi_spec::document_covers_every_management_route_exactly` fails when
   the document misses a mounted route, or invents one.
3. `openapi_spec::checked_in_artifact_matches_the_generated_document` fails
   when `docs/openapi.json` is stale.
4. The `lint` job validates `docs/openapi.json` with
   `openapi-spec-validator`, and it runs on documentation-only pull requests
   too.

## Why the document is derived, not annotated

`autumn-web` builds an OpenAPI document from routes declared with its route
macros, which attach `ApiDoc` metadata to each handler.
`harvest_api_router` is a plain `axum::Router`, so its routes carry no such
metadata. Annotating 168 handlers would also record less than the contract
already holds: per-parameter `required` flags, per-route read-only class,
idempotency rules, and error responses. The contract is therefore the input,
and `autumn-harvest-plugin/src/openapi.rs` is the transform.
