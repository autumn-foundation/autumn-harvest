# Apparatus for assay #11: harvest vs Temporal, one box

Non-production. Not a workspace member, and not a Cargo crate at all — the
Temporal arm is a Go program, because the mature Temporal SDKs are Go, Java
and TypeScript. See
[`../../0011-harvest-vs-temporal-single-box.md`](../../0011-harvest-vs-temporal-single-box.md)
for the question, the pre-registration and the verdict this code answers.

**Read the pre-registration's bounding section before quoting any number from
here.** Four cores is near the bottom of Temporal's envelope and near the top
of harvest's. A harvest win at this venue is not evidence that harvest is
faster than Temporal, and the report does not say that it is.

## The two arms

The harvest arm is the `postgres` arm of
[`../0010-cross-mode-throughput/`](../0010-cross-mode-throughput/), run
unchanged. Running one binary for both assays is deliberate: it makes the
harvest number in this report and the harvest number in assay #10 the same
measurement rather than two that might drift.

The Temporal arm is `main.go` here. Shape parity is enforced by hand:

| | harvest arm | Temporal arm |
|:--|:--|:--|
| workflow | 3 sequential activities | 3 sequential activities |
| activity body | inert, counter only | inert, counter only |
| workflow slots | 8 | `MaxConcurrentWorkflowTaskExecutionSize: 8` |
| activity slots | 16 | `MaxConcurrentActivityExecutionSize: 16` |
| workers | 1 | 1 |
| retries | none declared | `MaximumAttempts: 1` |
| logging | `NoOpMetrics`, no subscriber | discarding `slog` handler |
| shape | seed the backlog, then start the worker | same |

## Running

The Temporal server and the harvest arm must never run at the same time.
Four cores cannot host both engines at once without each becoming the other's
noise, so the runner stops one before starting the other.

```bash
# Temporal server, against the SAME Postgres the harvest arm uses.
docker run -d --name temporal-bench --network host \
  -e DB=postgres12 -e DB_PORT=5432 \
  -e POSTGRES_USER=postgres -e POSTGRES_PWD=postgres -e POSTGRES_SEEDS=127.0.0.1 \
  -e DEFAULT_NAMESPACE=default \
  temporalio/auto-setup:1.25.2

go build -o assay11 .
./assay11

# Then stop it before running the harvest arm.
docker stop temporal-bench
```

| variable | default | meaning |
|:--|:--|:--|
| `ASSAY11_TEMPORAL_HOSTPORT` | `127.0.0.1:7233` | frontend address |
| `ASSAY11_WORKFLOWS` | `2000` | seeded workflows per drain run |
| `ASSAY11_REPS` | `3` | repetitions |
| `ASSAY11_CAP_SECS` | `900` | cap on one run, after which it is truncated |
