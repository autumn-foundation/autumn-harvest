# autumn-harvest-client

A typed TypeScript client for the Harvest management API. The build generates
it from [`docs/openapi.json`](../../docs/openapi.json), so you do not need a
running Harvest to write client code (issue #1616).

## Install

Each GitHub release after 0.6.0 attaches the package. Releases up to 0.6.0
have no package. The package version is the crate version, so install the
client that matches your server. Replace `<version>` with that version:

```sh
npm install https://github.com/autumn-foundation/autumn-harvest/releases/download/v<version>/autumn-harvest-client-<version>.tgz
```

Node 20 or newer. The package is ESM; use `import`. On Node 20.19 or newer,
`require` works too. The package is not on the npm registry.

## Use

```ts
import { createHarvestClient, isStarted } from "autumn-harvest-client";

const harvest = createHarvestClient({
  baseUrl: "https://app.example.com/api/harvest",
  headers: { Authorization: `Bearer ${process.env.HARVEST_TOKEN}` },
});

const started = await harvest.POST("/workflows/{workflow_name}/start", {
  params: { path: { workflow_name: "greeting" } },
  body: { workflow_id: "order-42", input: "World" },
});
const run = started.data;
if (run === undefined) throw new Error(`start failed: ${started.response.status}`);
// A debounce, batch or throttle policy can defer the start. The 202 body then
// has no execution id.
if (!isStarted(run)) throw new Error("the start was deferred");

const status = await harvest.GET("/workflows/{id}", {
  params: { path: { id: run.execution_id } },
});
console.log(status.data?.execution.state); // "RUNNING"
```

`baseUrl` is the API root, including the prefix that `HarvestPlugin::api`
mounts. It has no default, so a forgotten URL cannot send a credential to
localhost. `DEFAULT_BASE_URL` holds the local development value,
`http://localhost:3000/api/harvest`.

`createHarvestClient` returns an [`openapi-fetch`](https://openapi-ts.dev/openapi-fetch/)
client. The package also exports these:

- `StartedWorkflow`, `DeferredStart`, `WorkflowStatus`, `WorkflowOutcome` and
  `HealthResponse`: the core response bodies.
- `isStarted`: true when a start body names a run.
- `paths` and `operations`: the generated types for every route.

## What has a type

Every path, method and parameter has a type. A request body lists its fields,
but most field values are `unknown`. Every field of these responses has a
type:

| Route | Use |
|-------|-----|
| `POST /workflows/{workflow_name}/start` | Start a run. |
| `GET /workflows/{id}` | Read the status. |
| `GET /workflows/{id}/result` | Read the outcome. |
| `POST /workflows/{id}/signal/{signal_name}` | Send a signal. |
| `POST /workflows/{id}/cancel` | Cancel a run. |
| `POST /workflows/{id}/terminate` | Terminate a run. |
| `GET /health` | Check that the server is ready. |

A value the server passes through, such as workflow input or output, is
`unknown`. So are the elements of `external_handoffs`. A test drives each of
these routes against Postgres and checks every body against its type. The
response fields of other routes are `unknown`. Narrow them in your code. See [`docs/openapi.md`](../../docs/openapi.md#limits-worth-knowing).

## Build it

```sh
scripts/build-typescript-client.sh   # from the repository root
```

The script generates, type-checks, tests, builds and packs the client. Its
last line is the tarball path.
