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
[`../0010-cross-mode-throughput/`](../0010-cross-mode-throughput/), run from
the same binary but **at this assay's own registered payload**, which assay
#10's L1 cannot use. The two assays therefore report two different harvest
measurements, not one shared one.

An earlier revision ran #10's arm unchanged and said the two assays shared a
single measurement. That was wrong: #10 seeds the canonical empty object,
because its L1 compares against a published figure taken that way, and #11
registers a ~40-byte payload for both of its arms. Running #10's arm here
measured #10's workload and reproduced its cell, not this assay's.

So the harvest arm must set `ASSAY10_INPUT_JSON`. Without it the run
reproduces assay #10's number, not this assay's:

```bash
ASSAY10_ARMS=postgres \
ASSAY10_WORKFLOWS=2000 \
ASSAY10_REPS=3 \
ASSAY10_INPUT_JSON='{"p":"0123456789abcdef0123456789abcdef"}' \
  cargo run --release --manifest-path \
  ../0010-cross-mode-throughput/Cargo.toml
```

The apparatus refuses to grade any pre-registered line on an overridden run,
and prints a "Not graded" notice instead, because assay #10's band cannot
grade a workload that is not assay #10's.

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

**Use `run.sh`, not the binary directly.** The harvest arm resets its database
before every repetition, so the Temporal arm has to start every repetition on
an empty database too. Temporal has no in-process reset, so `run.sh` makes a
repetition one whole process lifetime: it drops both Temporal databases, lets
auto-setup rebuild them, and runs exactly one repetition, three times over.
Running `./assay11` directly with the default three repetitions would leave
repetitions 2 and 3 measuring a database that still holds the earlier ones.

```bash
go build -o assay11 .
./run.sh

# It starts and removes the Temporal container itself, so nothing is left
# running against the Postgres the harvest arm needs.
```

The harvest arm runs separately, never at the same time, with the
`ASSAY10_INPUT_JSON` override given in [The two arms](#the-two-arms) above.
Omitting it reproduces assay #10's cell rather than this assay's.

| variable | default | meaning |
|:--|:--|:--|
| `ASSAY11_TEMPORAL_HOSTPORT` | `127.0.0.1:7233` | frontend address |
| `ASSAY11_WORKFLOWS` | `2000` | seeded workflows per drain run |
| `ASSAY11_REPS` | `3` | repetitions |
| `ASSAY11_CAP_SECS` | `900` | cap on one run, after which it is truncated |
