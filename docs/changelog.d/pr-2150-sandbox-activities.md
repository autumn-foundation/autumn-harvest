## Feature — sandboxed code activities on Firecracker microVMs

The new `sandbox-activities` feature lets an activity run code that an agent
writes at run time. The code can be Python, Node, shell or any program in the
guest image. It is off by default. The guide is
`docs/sandbox-activities.md`, and the design record is
`DESIGN-sandbox-activities.md`.

- **The request and the outcome.** An agent sends a `SandboxRequest` as JSON:
  a runtime, the code, files, stdin, arguments, variables, output paths and
  limits. Unknown fields are an error. A `SandboxPolicy` checks each request
  against runtimes, limit ceilings and size bounds. A non-zero exit, a signal
  or a timeout is an outcome. The outcome keeps a head and a tail of each
  stream, and a NUL byte becomes U+FFFD before it reaches `jsonb`.
- **Failures.** `SandboxDenied` and `SandboxProtocolViolation` are
  non-retryable. `SandboxUnavailable` and `SandboxCancelled` are retryable.
- **The Firecracker backend.** Each run gets a new, single-use microVM behind
  the jailer with cgroup v2. The microVM has a read-only root, a vsock device
  and a virtio-rng device. It has no network device, no metadata service and
  no serial console. Teardown kills the process, then the cgroup, then
  removes the jail, on every path. A lease file and a stale sweep remove the
  microVMs and cgroups of a crashed worker.
- **Speed.** A warm pool boots microVMs ahead.
  `FirecrackerLauncher::create_golden_snapshot` makes a golden snapshot for
  `BootMethod::Snapshot`. Each run carries fresh host entropy and the host
  clock, so clones of one snapshot do not share random state or a stale
  clock.
- **Host checks.** The first launch refuses a Firecracker or jailer version
  with a known escape CVE. `verify_host` also checks KVM, KSM, nested KVM,
  CPU vulnerabilities, cgroup v2, the jail base and the confinement.
- **The guest agent.** The new `autumn-harvest-sandbox-guest` crate builds
  the agent that runs as init in the microVM. It serves one run over vsock,
  then reboots. `scripts/sandbox/build-guest-image.sh` builds the guest
  image.
- **The process backend.** For development and CI, it runs the same executor
  as a host process. The run gets rlimits, a private work directory, no host
  environment, no new privileges and an empty network namespace. The default
  policy refuses it.
- **The activity and the tool.** `HarvestBuilder::sandbox_activity`
  registers a normal activity. Its `start_to_close` covers the queue wait,
  the wall clock, the overhead and the cleanup grace. The handler passes the
  time left in the attempt, so a retry does not overlap a live run. The code
  gets `HARVEST_IDEMPOTENCY_KEY` and `HARVEST_ATTEMPT`. The `sandbox` feature
  of `autumn-harvest-agent` adds `SandboxTool` (`run_code`).

No migration, no new `WorkflowEvent` variant, and no worker change other than
the adaptive-limit list. History records the request and the outcome, and
replay never runs the code again.

Evidence: the sandbox unit tests run on every OS in `test-nodb`, with a fake
VMM, fake `firecracker` and `jailer` scripts and a fake API server. The new
`Sandbox (KVM)` workflow boots real microVMs, and it runs the `root_` tests
with sudo. The `sandbox_activities_docs` guards check the guide and the
design record against the code.
