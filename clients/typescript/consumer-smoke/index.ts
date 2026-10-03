// A consumer of the packed tarball. `build-typescript-client.sh` installs the
// tarball next to this file and type-checks it, so a broken `exports` or
// `types` entry fails the build.
import { createHarvestClient, DEFAULT_BASE_URL } from "autumn-harvest-client";
import type { paths } from "autumn-harvest-client";

type Health =
  paths["/health"]["get"]["responses"][200]["content"]["application/json"];

const ready: Health["runtime_ready"] = true;
const harvest = createHarvestClient({ baseUrl: DEFAULT_BASE_URL });

export async function firstState(): Promise<string | undefined> {
  const started = await harvest.POST("/workflows/{workflow_name}/start", {
    params: { path: { workflow_name: "greeting" } },
    body: { workflow_id: "order-42", input: "World" },
  });
  const run = started.data;
  if (run === undefined || !("execution_id" in run)) return undefined;
  const status = await harvest.GET("/workflows/{id}", {
    params: { path: { id: run.execution_id } },
  });
  return ready ? status.data?.execution.state : undefined;
}
