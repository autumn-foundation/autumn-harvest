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

`/health/live` always returns `200`. It reads in-memory flags and does no
I/O. A liveness probe that touches the database restarts every pod during a
database outage. A restart does not fix the database.

```json
{ "alive": true, "draining": false }
```

## Readiness

`/health/ready` returns `200` only when all of these are true:

1. The Harvest runtime has started.
2. The replica is not draining.
3. The default shard answers `SELECT 1`.
4. The shard readiness verdict is `ready`. This check applies only when
   `[harvest.readiness] require_shard_readiness = true`.

Otherwise it returns `503`. The checks run in this order. A failed check 1 or
2 skips the database checks, so a stopping replica does no extra I/O.

Checks 3 and 4 share one 1 second budget. The pool checkout counts against
the budget, so an exhausted pool gives `503`, not a hung probe.

The replica caches the result of checks 3 and 4 for 1 second. Concurrent
requests wait for one check. The probe is public, so the cache limits its
database load. Checks 1 and 2 are not cached, so a drain shows at once.

Without `require_shard_readiness`, the probe reads only the default shard.
One bad non-default shard therefore does not remove every replica from the
load balancer.

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
| `shard_readiness` | `ready`, `degraded` or `unavailable`. `null` when the check did not run or timed out. |
| `reasons` | Machine codes. Empty when ready. |

The reason codes are `runtime_not_started`, `draining`,
`database_unreachable`, `shard_not_ready` and `shard_report_timeout`.

The body holds the shard verdict only. The full report is on the
admin-gated `GET /admin/shards/health`.

## Draining

The draining flag makes readiness fail before in-flight work stops.

- **`HarvestPlugin`.** autumn-web marks its own probe state at SIGTERM.
  Harvest reads that state, so `/health/ready` returns `503` at once. The
  Harvest shutdown hook also sets the flag.
- **`HarvestEmbedding`.** `HarvestEmbeddingRuntime::stop` sets the flag
  first. Call `HarvestApiState::begin_draining()` earlier, when SIGTERM
  arrives, if the server is still answering. See
  [`docs/embedding.md`](../embedding.md#shut-down).

The next runtime start clears the flag.

A remote drain (`POST /workers/{id}/drain`) does not change readiness. It
stops the worker, not the HTTP routes. See
[`docs/runbooks/safe-deploy.md`](../runbooks/safe-deploy.md).

## `/health`

`GET /health` stays for compatibility. Its status and body do not change. It
returns `200` before the runtime starts and during a drain. Use
`/health/ready` for readiness.

## Pod spec

```yaml
spec:
  terminationGracePeriodSeconds: 60
  containers:
    - name: app
      ports:
        - name: http
          containerPort: 3000 # Your server port. 3000 is the autumn-web default.
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
`preStop` sleep and the full shutdown. A shorter period kills in-flight
tasks. Their leases then expire, and another worker retries them.

For `HarvestPlugin`, autumn-web runs the shutdown in two phases. It first
waits `[server] prestop_grace_secs` (default 5). Then the request drain and
the shutdown hooks share one `[server] shutdown_timeout_secs` budget
(default 30). The Harvest worker drain runs in a shutdown hook. Use this
lower bound:

```text
terminationGracePeriodSeconds >= preStop sleep
                                + prestop_grace_secs
                                + shutdown_timeout_secs
                                + 10 s margin
```

Set `shutdown_timeout_secs` above `WorkerConfig::shutdown_timeout` (default
25 seconds, issue #1813). Otherwise autumn-web stops the worker drain before it ends. With
a 10 second `preStop`, `shutdown_timeout_secs = 45` and the other defaults,
use at least 70 seconds.

For `HarvestEmbedding`, use the server drain time of your process in place
of the two autumn-web values.

## Shutdown sequence (`HarvestPlugin`)

1. Kubernetes marks the pod `Terminating` and removes it from endpoints.
2. The `preStop` sleep runs. The process still serves requests.
3. Kubernetes sends SIGTERM.
4. autumn-web marks its probe state. `/health/ready` returns `503`.
5. autumn-web waits `prestop_grace_secs`. It then closes the listener and
   drains in-flight requests.
6. The Harvest shutdown hook stops the connectors and the outbox relay.
7. The worker stops claiming tasks. It waits for in-flight tasks up to
   `shutdown_timeout`. A remote drain `deadline_at` replaces this bound.
8. The API state clears. The process exits.
