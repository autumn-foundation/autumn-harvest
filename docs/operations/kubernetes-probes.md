# Kubernetes probes and graceful shutdown (issue #1812)

Harvest has two probe routes. Use them for Kubernetes probes and for
load-balancer health checks.

| Route | Question | Fails when |
|-------|----------|------------|
| `GET /health/live` | Is the process wedged? | Never, while the process answers HTTP. |
| `GET /health/ready` | Can this replica take traffic? | See [Readiness](#readiness). |

The paths are relative to the Harvest mount. The examples use
`/api/harvest`. Both routes are `PublicSafe`, so they need no credentials.
An embedder auth layer still applies. See
[`docs/security-posture.md`](../security-posture.md).

## Liveness

`/health/live` always returns `200`. It reads one in-memory flag and does no
I/O. A liveness probe that touches the database restarts every pod during a
database outage. A restart does not fix the database.

```json
{ "alive": true, "draining": false }
```

## Readiness

`/health/ready` returns `200` only when all of these are true:

1. The Harvest runtime has started.
2. The replica is not draining.
3. The default shard answers `SELECT 1` within 1 second.
4. The shard readiness report is `ready`. This check applies only when
   `[harvest.readiness] require_shard_readiness = true`.

Otherwise it returns `503`. The checks run in this order. A failed check 1 or
2 skips the database checks, so a stopping replica does no extra I/O.

The probe reads only the default shard. One bad non-default shard therefore
does not remove every replica from the load balancer. To gate on every
writable shard, set `require_shard_readiness`.

```json
{
  "ready": false,
  "runtime_ready": true,
  "draining": true,
  "database_reachable": null,
  "shard_readiness_enforced": false,
  "shard_readiness": null,
  "reasons": ["draining"]
}
```

| Field | Meaning |
|-------|---------|
| `ready` | `true` when the status is `200`. |
| `runtime_ready` | The runtime has started. |
| `draining` | The replica is shutting down. |
| `database_reachable` | The default-shard result. `null` when the check did not run. |
| `shard_readiness_enforced` | The value of `require_shard_readiness`. |
| `shard_readiness` | The shard report. `null` when not enforced or not run. |
| `reasons` | Machine codes: `runtime_not_started`, `draining`, `database_unreachable`, `shard_not_ready`. Empty when ready. |

## Draining

Shutdown sets the draining flag first. `HarvestPlugin` sets it at the start
of its shutdown hook. `HarvestEmbeddingRuntime::stop` sets it at the start of
`stop()`. Readiness then fails before the worker stops in-flight work.

An embedder can set the flag earlier. Call
`HarvestApiState::begin_draining()` when SIGTERM arrives. A new runtime
`install()` clears the flag.

A remote drain (`POST /workers/{id}/drain`) does not change readiness. It
stops the worker, not the HTTP routes. See
[`docs/runbooks/safe-deploy.md`](../runbooks/safe-deploy.md).

## `/health`

`GET /health` stays for compatibility. Its status and body do not change. It
returns `200` before the runtime starts. Use `/health/ready` for readiness.

## Pod spec

```yaml
spec:
  terminationGracePeriodSeconds: 60
  containers:
    - name: app
      ports:
        - name: http
          containerPort: 8080
      livenessProbe:
        httpGet: { path: /api/harvest/health/live, port: http }
        periodSeconds: 10
        timeoutSeconds: 2
        failureThreshold: 3
      readinessProbe:
        httpGet: { path: /api/harvest/health/ready, port: http }
        periodSeconds: 5
        timeoutSeconds: 2
        failureThreshold: 3
      lifecycle:
        preStop:
          sleep: { seconds: 10 }
```

### Probes

- Set the readiness `timeoutSeconds` above the 1 second database budget. The
  value `2` lets Harvest answer `503` before the kubelet times out.
- Keep `failureThreshold` at `3` or more. One slow database answer then does
  not remove the replica.
- A `startupProbe` is not necessary. Liveness answers before the runtime
  starts.

### `preStop`

Kubernetes removes a terminating pod from its endpoints. Ingress controllers
and `kube-proxy` see the removal after a delay. During that delay, requests
still arrive. The `preStop` sleep keeps the process serving until they stop.

- The `sleep` action needs Kubernetes 1.30 or later. On an older cluster, use
  `exec: { command: ["sleep", "10"] }`. The image must then contain `sleep`.
- 5 to 15 seconds is usual. Use the propagation delay of your ingress.

### `terminationGracePeriodSeconds`

The kubelet sends SIGKILL when the grace period ends. The period covers the
`preStop` sleep and the full shutdown. Use this lower bound:

```text
terminationGracePeriodSeconds >= preStop sleep
                                + WorkerConfig::shutdown_timeout
                                + 10 s margin
```

The default `shutdown_timeout` is 30 seconds. With a 10 second `preStop`, use
at least 50 seconds. A shorter period kills in-flight tasks. Their leases then
expire and another worker retries them.

## Shutdown sequence

1. Kubernetes marks the pod `Terminating` and removes it from endpoints.
2. The `preStop` sleep runs. The process still serves requests.
3. Kubernetes sends SIGTERM.
4. Harvest sets the draining flag. `/health/ready` returns `503`.
5. The worker stops claiming tasks. It waits for in-flight tasks up to
   `shutdown_timeout`.
6. The API state clears. The process exits.
