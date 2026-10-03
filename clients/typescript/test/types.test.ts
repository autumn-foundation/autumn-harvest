// Type-level tests. `npm run typecheck` fails when one does not hold.
import { test } from "node:test";

import type {
  DeferredStart,
  HealthResponse,
  StartedWorkflow,
  WorkflowOutcome,
  WorkflowStatus,
  paths,
} from "../src/index.js";

type Equal<A, B> =
  (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;

function expectType<T extends true>(_proof: T): void {}

type Json<Route extends keyof paths, Method extends keyof paths[Route], Status extends number> =
  paths[Route][Method] extends { responses: infer R }
    ? Status extends keyof R
      ? R[Status] extends { content: { "application/json": infer Body } }
        ? Body
        : never
      : never
    : never;

type Started = Json<"/workflows/{workflow_name}/start", "post", 201>;
type Deferred = Json<"/workflows/{workflow_name}/start", "post", 202>;
type Status = Json<"/workflows/{id}", "get", 200>;
type Outcome = Json<"/workflows/{id}/result", "get", 200>;
type Signalled = Json<"/workflows/{id}/signal/{signal_name}", "post", 202>;
type Cancelled = Json<"/workflows/{id}/cancel", "post", 202>;
type Terminated = Json<"/workflows/{id}/terminate", "post", 202>;
type Health = Json<"/health", "get", 200>;

test("core responses carry real types, not unknown", () => {
  expectType<Equal<Started["execution_id"], string>>(true);
  expectType<Equal<Started["workflow_id"], string>>(true);
  expectType<Equal<Status["execution"]["state"], string>>(true);
  expectType<Equal<Status["execution"]["completed_at"], string | null>>(true);
  expectType<Equal<Status["execution"]["execution_timeout"], number[] | null>>(true);
  expectType<Equal<Status["parent_id"], string | null>>(true);
  expectType<Equal<Status["history_truncated"], boolean>>(true);
  expectType<Equal<Status["history"][number]["type"], string>>(true);
  expectType<Equal<Outcome["state"], string>>(true);
  expectType<Equal<Signalled["signal_delivered"], boolean>>(true);
  expectType<Equal<Cancelled["newly_cancelled"], boolean>>(true);
  expectType<Equal<Terminated["newly_terminated"], boolean>>(true);
  expectType<Equal<Terminated["failed_task_count"], number>>(true);
  expectType<Equal<Health["runtime_ready"], boolean>>(true);
  expectType<Equal<Health["queues"], string[]>>(true);
});

test("a deferred start has no execution id", () => {
  expectType<Equal<"execution_id" extends keyof Deferred ? true : false, false>>(true);
  expectType<Equal<Deferred["workflow_id"], string>>(true);
});

test("the named aliases are the route bodies", () => {
  expectType<Equal<StartedWorkflow, Started | Json<"/workflows/{workflow_name}/start", "post", 200>>>(true);
  expectType<Equal<DeferredStart, Deferred>>(true);
  expectType<Equal<WorkflowStatus, Status>>(true);
  expectType<Equal<WorkflowOutcome, Outcome>>(true);
  expectType<Equal<HealthResponse, Health>>(true);
});
