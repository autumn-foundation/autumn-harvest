// Typed client for the Harvest management API (issue #1616).
//
// The build generates `harvest-api.ts` from `docs/openapi.json`. This file
// adds one factory over `openapi-fetch`, names for the core response bodies,
// and one type guard.
import createClient from "openapi-fetch";
import type { Client, ClientOptions } from "openapi-fetch";

import type { paths } from "./harvest-api.js";

export type { components, operations, paths } from "./harvest-api.js";
export type { ClientOptions, Middleware } from "openapi-fetch";

/** The mount point that `HarvestPlugin::api` uses by convention. Opt in to it. */
export const DEFAULT_BASE_URL = "http://localhost:3000/api/harvest";

/** A client whose paths, parameters and bodies come from the document. */
export type HarvestClient = Client<paths>;

/** Options for `createHarvestClient`. `baseUrl` has no default. */
export type HarvestClientOptions = ClientOptions & { baseUrl: string };

type JsonBody<Response> = Response extends { content: { "application/json": infer Body } }
  ? Body
  : never;
type Responses<Route extends keyof paths, Method extends keyof paths[Route]> =
  paths[Route][Method] extends { responses: infer R } ? R : never;
type StartResponses = Responses<"/workflows/{workflow_name}/start", "post">;

/** A start that made or found a run: the 201 or the 200 body. */
export type StartedWorkflow = JsonBody<StartResponses[201]> | JsonBody<StartResponses[200]>;

/** A start that a debounce, batch or throttle policy deferred: the 202 body. */
export type DeferredStart = JsonBody<StartResponses[202]>;

/** The `GET /workflows/{id}` body. */
export type WorkflowStatus = JsonBody<Responses<"/workflows/{id}", "get">[200]>;

/** The `GET /workflows/{id}/result` body for a finished run. */
export type WorkflowOutcome = JsonBody<Responses<"/workflows/{id}/result", "get">[200]>;

/** The `GET /health` body. */
export type HealthResponse = JsonBody<Responses<"/health", "get">[200]>;

/** True when a start body names a run. A deferred start names none. */
export function isStarted(body: StartedWorkflow | DeferredStart): body is StartedWorkflow {
  return typeof (body as { execution_id?: unknown }).execution_id === "string";
}

/**
 * Make a client for one Harvest deployment.
 *
 * `baseUrl` is the API root, including the mount prefix. It has no default, so
 * a forgotten URL cannot send a credential to localhost. Put a credential in
 * `headers`, for example `{ Authorization: "Bearer <token>" }`.
 */
export function createHarvestClient(options: HarvestClientOptions): HarvestClient {
  if (typeof options.baseUrl !== "string" || options.baseUrl === "") {
    throw new TypeError("createHarvestClient needs a baseUrl, such as DEFAULT_BASE_URL");
  }
  return createClient<paths>(options);
}
