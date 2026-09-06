# Generated TypeScript client

A typed client for the Harvest management API, generated from the OpenAPI 3.1
document the plugin serves (issue #694). It starts a workflow and reads its
status with no hand-written URL, method or `fetch` call.

## Run it

You need a running app that mounts `HarvestPlugin` with
`.api("/api/harvest")`. The repository's quickstart is one, and it registers
the `greeting` workflow this script starts:

```sh
# Terminal 1 — Postgres, then the app (see examples/quickstart/README.md).
docker compose -f examples/quickstart/compose.yaml up -d
AUTUMN_PROFILE=dev AUTUMN_MANIFEST_DIR=examples/quickstart cargo run -p quickstart
```

```sh
# Terminal 2 — the client. Node 20 or newer.
cd examples/typescript-client
npm ci
npm run generate     # openapi-typescript reads GET /api/harvest/openapi.json
npm run typecheck    # tsc proves the calls match the document
npm start            # POST /workflows/greeting/start, then poll GET /workflows/{id}
```

`npm start` prints `state=RUNNING` for about 35 seconds. The `greeting`
workflow sleeps on a durable timer between its two activities. Expected last
line:

```
generated client started a workflow and read its status
```

## Point it somewhere else

| Variable | Default | Meaning |
| --- | --- | --- |
| `HARVEST_OPENAPI_URL` | `http://localhost:3000/api/harvest/openapi.json` | Where `npm run generate` reads the document. A file path works too. |
| `HARVEST_BASE_URL` | `http://localhost:3000/api/harvest` | The API root the client calls. |
| `HARVEST_WORKFLOW` | `greeting` | The registered workflow to start. |
| `HARVEST_TIMEOUT_MS` | `90000` | How long to poll before failing. |

## What generates what

`npm run generate` writes `src/harvest-api.ts` from the document. That file is
build output and is not checked in. `src/start-and-poll.ts` imports its `paths`
type and passes it to [`openapi-fetch`](https://openapi-ts.dev/openapi-fetch/),
so a wrong path, method, parameter or body fails `npm run typecheck`.

Response property types are `unknown` because the contract records field names,
not full JSON Schemas. See [`docs/openapi.md`](../../docs/openapi.md).

## Against an authenticated deployment

`HarvestPlugin::api_with_auth` puts the embedder's middleware in front of every
route, including the document. Give the client a credential the same way for
both steps:

```sh
npm run generate -- --header "Authorization: Bearer $HARVEST_TOKEN"
```

```ts
const client = createClient<paths>({
  baseUrl,
  headers: { Authorization: `Bearer ${process.env.HARVEST_TOKEN}` },
});
```

CI runs these exact steps in the `openapi-client-smoke` job.
