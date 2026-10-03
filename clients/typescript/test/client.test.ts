// Runtime tests for `createHarvestClient`, with a fake `fetch`.
import assert from "node:assert/strict";
import { test } from "node:test";

import { createHarvestClient } from "../src/index.js";

/** A fake `fetch` that records each request and returns `body`. */
function fakeFetch(status: number, body: unknown) {
  const seen: Request[] = [];
  const fetch = async (request: Request): Promise<Response> => {
    seen.push(request);
    return new Response(JSON.stringify(body), {
      status,
      headers: { "content-type": "application/json" },
    });
  };
  return { seen, fetch };
}

test("start sends POST to the mounted path and returns typed data", async () => {
  const { seen, fetch } = fakeFetch(201, {
    execution_id: "0b0f6a3e-0000-4000-8000-000000000001",
    workflow_name: "greeting",
    workflow_id: "w-1",
    state: "RUNNING",
  });
  const client = createHarvestClient({ baseUrl: "http://harvest.test/api/harvest", fetch });

  const started = await client.POST("/workflows/{workflow_name}/start", {
    params: { path: { workflow_name: "greeting" } },
    body: { workflow_id: "w-1", input: "World" },
  });

  assert.equal(seen.length, 1);
  assert.equal(seen[0].method, "POST");
  assert.equal(seen[0].url, "http://harvest.test/api/harvest/workflows/greeting/start");
  assert.deepEqual(await seen[0].json(), { workflow_id: "w-1", input: "World" });
  assert.equal(started.response.status, 201);
  // A deferred start (202) has no execution id, so the caller narrows.
  const run = started.data;
  assert.ok(run !== undefined && "execution_id" in run);
  assert.equal(run.execution_id, "0b0f6a3e-0000-4000-8000-000000000001");
});

test("headers reach every request", async () => {
  const { seen, fetch } = fakeFetch(200, { runtime_ready: true });
  const client = createHarvestClient({
    baseUrl: "http://harvest.test/api/harvest",
    headers: { Authorization: "Bearer t" },
    fetch,
  });

  await client.GET("/health");

  assert.equal(seen[0].headers.get("authorization"), "Bearer t");
});

test("the base URL defaults to the conventional mount point", async () => {
  const { seen, fetch } = fakeFetch(200, { runtime_ready: true });
  const client = createHarvestClient({ fetch });

  await client.GET("/health");

  assert.equal(new URL(seen[0].url).pathname, "/api/harvest/health");
});
