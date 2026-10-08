// Runtime tests for `createHarvestClient` and `isStarted`, with a fake `fetch`.
import assert from "node:assert/strict";
import { test } from "node:test";

import { DEFAULT_BASE_URL, createHarvestClient, isStarted } from "../src/index.js";

const BASE = "http://harvest.test/api/harvest";

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
  const client = createHarvestClient({ baseUrl: BASE, fetch });

  const started = await client.POST("/workflows/{workflow_name}/start", {
    params: { path: { workflow_name: "greeting" } },
    body: { workflow_id: "w-1", input: "World" },
  });

  assert.equal(seen.length, 1);
  assert.equal(seen[0].method, "POST");
  assert.equal(seen[0].url, `${BASE}/workflows/greeting/start`);
  assert.deepEqual(await seen[0].json(), { workflow_id: "w-1", input: "World" });
  assert.equal(started.response.status, 201);
  const run = started.data;
  assert.ok(run !== undefined && isStarted(run));
  assert.equal(run.execution_id, "0b0f6a3e-0000-4000-8000-000000000001");
});

test("a deferred start is not a started run", async () => {
  const { fetch } = fakeFetch(202, {
    debounced: true,
    workflow_name: "greeting",
    workflow_id: "w-1",
    debounce_key: "t-1",
  });
  const client = createHarvestClient({ baseUrl: BASE, fetch });

  const started = await client.POST("/workflows/{workflow_name}/start", {
    params: { path: { workflow_name: "greeting" } },
    body: { workflow_id: "w-1" },
  });

  assert.equal(started.response.status, 202);
  assert.ok(started.data !== undefined);
  assert.equal(isStarted(started.data), false);
});

test("an error status sets `error` and leaves `data` undefined", async () => {
  const { fetch } = fakeFetch(404, { error: "Execution not found" });
  const client = createHarvestClient({ baseUrl: BASE, fetch });

  const status = await client.GET("/workflows/{id}", { params: { path: { id: "missing" } } });

  assert.equal(status.response.status, 404);
  assert.equal(status.data, undefined);
  assert.deepEqual(status.error, { error: "Execution not found" });
});

test("headers reach every request", async () => {
  const { seen, fetch } = fakeFetch(200, { runtime_ready: true });
  const client = createHarvestClient({ baseUrl: BASE, headers: { Authorization: "Bearer t" }, fetch });

  await client.GET("/health");

  assert.equal(seen[0].headers.get("authorization"), "Bearer t");
});

test("the conventional mount point is an explicit opt-in", async () => {
  const { seen, fetch } = fakeFetch(200, { runtime_ready: true });
  const client = createHarvestClient({ baseUrl: DEFAULT_BASE_URL, fetch });

  await client.GET("/health");

  assert.equal(seen[0].url, "http://localhost:3000/api/harvest/health");
});

test("a missing base URL fails at once, not at the first request", () => {
  const unset = process.env.HARVEST_TEST_UNSET_BASE_URL;
  // A JavaScript caller can pass `undefined` past the type.
  assert.throws(() => createHarvestClient({ baseUrl: unset as string }), /baseUrl/);
  assert.throws(() => createHarvestClient({ baseUrl: "" }), /baseUrl/);
});
