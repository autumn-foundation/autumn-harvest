# autumn-harvest-worker

This chart installs a standalone Harvest worker (issue #1989). It sets the
probes, the drain and a migration job. The decision is in
[ADR 0006](../../docs/adr/0006-worker-container-image.md).

The default image runs the reference `standalone-runner`. Its workflows are
demos. To run your own workflows, build a worker image. See
[Your worker image](#your-worker-image).

## Install

The chart needs Kubernetes 1.30 or later, for the `preStop` sleep action.

1. Create a Secret that holds the database URL:

   ```bash
   kubectl create secret generic harvest-database \
     --from-literal=DATABASE_URL='postgres://harvest:<password>@db:5432/harvest?sslmode=require'
   ```

   The Secret must exist before the install. Argo CD runs the hook as a
   `PreSync` step, so keep the Secret outside the application, or sync it
   first.

2. Install the chart:

   ```bash
   helm install harvest charts/autumn-harvest-worker \
     --set database.existingSecret=harvest-database \
     --timeout 15m
   ```

   Helm runs `harvest migrate run` as a hook before it creates the worker.
   Helm waits for the hook for `--timeout`, 5 minutes by default. Keep it
   above `migrations.activeDeadlineSeconds` (600).

3. Outside `dev`, each admin route needs an API token. Seed the first one
   on your workstation, not in the cluster. A log collector keeps pod
   output, and this output holds the secret:

   ```bash
   docker run --rm ghcr.io/autumn-foundation/autumn-harvest:0.7.0 harvest token bootstrap
   ```

   Run the printed `INSERT` statement against the database, and keep the
   secret.

## What the chart sets

| Setting | Value | Why |
|---|---|---|
| Startup probe | `GET /api/harvest/health/live`, 150 s | The runner answers HTTP only after the runtime starts. |
| Liveness probe | `GET /api/harvest/health/live` | It does no I/O, so a database outage does not restart the pods. |
| Readiness probe | `GET /api/harvest/health/ready` | It fails before the runtime starts, during a drain, and when the database does not answer. |
| `preStop` sleep | `drain.preStopSleepSeconds` (10) | The pod serves until the ingress stops sending traffic. |
| Grace period | `drain.terminationGracePeriodSeconds` (60) | The chart refuses a value below `preStopSleepSeconds + shutdownSeconds + 10`. |
| Migration Job | `harvest migrate run`, pre-install and pre-upgrade hook | The schema is current before the new pods start. Concurrent runs serialize on the ledger lock when the role may take it. |
| Init container | `harvest migrate status --check` | A pod does not start on a database with a pending migration. It uses the app role. The chart skips it in `dev`. |
| Security context | User `65532`, read-only root, no capabilities | The image needs no write access and no privilege. |
| PodDisruptionBudget | `maxUnavailable: 1` | A node drain removes one worker at a time. |

[`docs/operations/kubernetes-probes.md`](../../docs/operations/kubernetes-probes.md)
explains the probe and drain values.

## Values

| Key | Default | Use |
|---|---|---|
| `database.existingSecret` | none, required | The Secret that holds the database URL. |
| `database.secretKey` | `DATABASE_URL` | The key in the Secret. |
| `profile` | `prod` | `AUTUMN_PROFILE`, the only profile selector. The chart refuses `AUTUMN_ENV` or `AUTUMN_PROFILE` in `env`. Do not use `dev` in a shared cluster: its admin API has no auth. |
| `image.repository` | `ghcr.io/autumn-foundation/autumn-harvest` | The worker image. |
| `image.tag` | the chart `appVersion` | The image tag. |
| `image.digest` | none | A digest wins over the tag. |
| `command`, `args` | the image `CMD` | The worker command. |
| `containerPort` | `8082` | The worker port. The chart sets `STANDALONE_RUNNER_ADDR`. |
| `probes.basePath` | `/api/harvest` | The mount of the Harvest router. |
| `drain.shutdownSeconds` | `35` | The time that the process needs after SIGTERM. |
| `migrations.enabled` | `true` | Run the migration hook. |
| `migrations.checkOnStart` | `true` | Run the init container check. |
| `migrations.includeDirs` | the plugin migrations | Extra migration directories in the image. |
| `migrations.existingSecret` | `database.existingSecret` | A Secret for a role that may run DDL. Only the hook uses it. |
| `migrations.serviceAccountName` | none | A service account for the hook. Create it before the install: the hook runs before the chart creates its own. |
| `migrations.podAnnotations` | none | For example `sidecar.istio.io/inject: "false"`. A sidecar that does not stop keeps the Job from completing. |
| `probes.startup.enabled` | `true` | The startup probe. Raise `failureThreshold` for a slow database. |

`values.yaml` lists every key.

## Your worker image

Build on the Harvest image. Your worker then has the CLI for the migration
job and the init container.

```dockerfile
FROM rust:1.99.0-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --release --locked --bin my-worker

FROM ghcr.io/autumn-foundation/autumn-harvest:0.7.0
COPY --from=build /src/target/release/my-worker /usr/local/bin/
CMD ["my-worker"]
```

Build on Debian 12 (bookworm). The runtime is `cc-debian12`, and a binary
that links a newer glibc does not start on it. The runtime has no OpenSSL,
so use rustls for TLS.

The worker must:

- read the database URL from `DATABASE_URL`;
- listen on `0.0.0.0:<containerPort>`, or read `STANDALONE_RUNNER_ADDR`;
- mount the Harvest router at `probes.basePath`;
- on SIGTERM, call `begin_draining()` on `harvest.api_state()`, stop the
  HTTP server, then call `stop()` on the runtime. See
  [`docs/embedding.md`](../../docs/embedding.md#shut-down).

Set `drain.shutdownSeconds` to the time your process needs after SIGTERM.

## Verify the image

The release signs each image with keyless Sigstore and attests its build
provenance. ADR 0006 has the `cosign verify` and
`gh attestation verify` commands. Pin the verified digest with
`image.digest`.
