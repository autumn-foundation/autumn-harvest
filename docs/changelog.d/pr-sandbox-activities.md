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
  microVMs of a crashed worker. A warm pool and a snapshot boot cut the start
  time. The backend refuses Firecracker versions with a known escape CVE, and
  it checks the host posture.
- **The guest agent.** The new `autumn-harvest-sandbox-guest` crate builds
  the agent that runs as init in the microVM. It serves one run over vsock,
  reseeds the kernel random pool from host entropy, and reboots.
  `scripts/sandbox/build-guest-image.sh` builds the guest image.
- **The process backend.** For development and CI, it runs the same executor
  as a host process with rlimits, a private work directory, no host
  environment, no new privileges and an empty network namespace. The default
  policy refuses it.
- **The activity and the tool.** `HarvestBuilder::sandbox_activity`
  registers a normal activity. Its timeouts and result cap follow the policy,
  and it passes `HARVEST_IDEMPOTENCY_KEY` and `HARVEST_ATTEMPT` to the code.
  The `sandbox` feature of `autumn-harvest-agent` adds `SandboxTool`
  (`run_code`).

No migration, no new `WorkflowEvent` variant, and no worker change. History
records the request and the outcome, and replay never runs the code again.

Evidence: the sandbox unit tests run on every OS in `test-nodb`, with a fake
VMM, fake `firecracker` and `jailer` scripts and a fake API server. The new
`Sandbox (KVM)` workflow boots real microVMs and runs
`sandbox::firecracker::kvm_tests`. The `sandbox_activities_docs` guards check
the guide against the code.
