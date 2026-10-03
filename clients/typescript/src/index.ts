// Typed client for the Harvest management API (issue #1616).
//
// `harvest-api.ts` is generated from `docs/openapi.json` at build time.
// This file adds one factory over `openapi-fetch`.
import createClient from "openapi-fetch";
import type { Client, ClientOptions } from "openapi-fetch";

import type { paths } from "./harvest-api.js";

export type { components, operations, paths } from "./harvest-api.js";

/** The mount point that `HarvestPlugin::api` uses by convention. */
export const DEFAULT_BASE_URL = "http://localhost:3000/api/harvest";

/** A client whose paths, parameters and bodies come from the document. */
export type HarvestClient = Client<paths>;

/**
 * Make a client for one Harvest deployment.
 *
 * `baseUrl` is the API root, including the mount prefix. Put a credential
 * in `headers`, for example `{ Authorization: "Bearer <token>" }`.
 */
export function createHarvestClient(options: ClientOptions = {}): HarvestClient {
  return createClient<paths>({ baseUrl: DEFAULT_BASE_URL, ...options });
}
