// Start a workflow and read its status through a generated client.
//
// Every HTTP call below goes through `openapi-fetch`, driven by the types
// `openapi-typescript` generates from the served OpenAPI document. There is no
// hand-written URL, method, or fetch call anywhere in this file (issue #694).
//
// Run it against a live plugin:
//
//   npm install
//   npm run generate      # reads GET /api/harvest/openapi.json
//   npm run typecheck
//   npm start
import createClient from "openapi-fetch";

import type { paths } from "./harvest-api.js";

const baseUrl = process.env.HARVEST_BASE_URL ?? "http://localhost:3000/api/harvest";
const workflowName = process.env.HARVEST_WORKFLOW ?? "greeting";
const timeoutMs = Number(process.env.HARVEST_TIMEOUT_MS ?? 90_000);

const client = createClient<paths>({ baseUrl });

async function main(): Promise<void> {
  const started = await client.POST("/workflows/{workflow_name}/start", {
    params: { path: { workflow_name: workflowName } },
    body: { workflow_id: `ts-client-${Date.now()}`, input: "World" },
  });
  if (started.error !== undefined || started.data === undefined) {
    throw new Error(`start failed: ${JSON.stringify(started.error ?? started.response.status)}`);
  }

  // A debounce, batch or throttle policy can defer a start. The 202 body then
  // has no execution id, so narrow on the key (issue #1616).
  const run = started.data;
  if (!("execution_id" in run)) {
    throw new Error(`the start was deferred: ${JSON.stringify(run)}`);
  }
  const executionId = run.execution_id;
  console.log(`started ${workflowName} -> ${executionId}`);

  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const status = await client.GET("/workflows/{id}", {
      params: { path: { id: executionId } },
    });
    if (status.error !== undefined || status.data === undefined) {
      throw new Error(`status failed: ${JSON.stringify(status.error ?? status.response.status)}`);
    }

    const state = status.data.execution.state;
    console.log(`state=${state}`);
    if (state === "COMPLETED") {
      console.log("generated client started a workflow and read its status");
      return;
    }
    if (state === "FAILED" || state === "TERMINATED" || state === "CANCELLED" || state === "TIMED_OUT") {
      throw new Error(`workflow reached ${state}`);
    }
    await new Promise((resolve) => setTimeout(resolve, 1000));
  }
  throw new Error("workflow did not complete before the timeout");
}

await main();
