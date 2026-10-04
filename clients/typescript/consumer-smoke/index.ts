// A consumer of the packed tarball. `build-typescript-client.sh` installs the
// tarball next to this file and type-checks it, so a broken `exports` or
// `types` entry fails the build.
import { DEFAULT_BASE_URL, createHarvestClient, isStarted } from "autumn-harvest-client";
import type { HealthResponse, WorkflowStatus } from "autumn-harvest-client";

type Equal<A, B> =
  (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;
const ready: Equal<HealthResponse["runtime_ready"], boolean> = true;
const state: Equal<WorkflowStatus["execution"]["state"], string> = true;

const harvest = createHarvestClient({ baseUrl: DEFAULT_BASE_URL });

export async function firstState(): Promise<string | undefined> {
  const started = await harvest.POST("/workflows/{workflow_name}/start", {
    params: { path: { workflow_name: "greeting" } },
    body: { workflow_id: "order-42", input: "World" },
  });
  if (started.data === undefined || !isStarted(started.data)) return undefined;
  const status = await harvest.GET("/workflows/{id}", {
    params: { path: { id: started.data.execution_id } },
  });
  return ready && state ? status.data?.execution.state : undefined;
}
