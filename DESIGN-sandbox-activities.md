# Design — sandboxed code activities: many small lambdas for agents

The ask: give activities the power to run any code that an agent writes. Use
a sandbox that is stronger than the WASM activity runtime. Evaluate
Firecracker. No issue exists, so this record derives the acceptance criteria
from the research and the plan.

**No migration. No new `WorkflowEvent` variant. No worker change.** A sandbox
run is an ordinary activity. The feature is `sandbox-activities`, off by
default. It needs no database, and it adds no new crate.

---

## 0. Planning record

### 0.1 Facts found before the plan

From the codebase:

- `wasm-activities` runs pre-published WASM modules with fuel, epoch and
  memory limits. A guest cannot use a file system, a process or a native
  library. Python, Node and shell code do not run there.
- An activity handler is a plain `fn` pointer. A handler reads shared state
  with `ActivityContext::state`, and the builder registers state with
  `HarvestBuilder::state`.
- An activity records its input in `ActivityScheduled` and its result in
  `ActivityCompleted`. Replay reads the result and never runs the body.
- The default activity result cap is 2 MiB. `ActivityInfo::max_result_bytes`
  raises it for one activity.
- The agent crate runs each tool call as an activity, through the `Tool`
  trait.
- CI builds `--all-features` on Linux, macOS and Windows. No CI runner has
  `/dev/kvm`, and this development host has none either.

From the research (four reports; sources are in `docs/sandbox-activities.md`):

- Firecracker 1.17 is current. It boots to `/sbin/init` in about 125 ms with
  about 5 MiB of VMM overhead. A snapshot load takes about 3 ms.
- The jailer is mandatory in production. It defaults to cgroup v1, so pass
  `--cgroup-version 2`. The jail root is
  `<base>/<exec name>/<id>/root`.
- The VM id allows letters, digits and `-`, up to 64 bytes. A `drive_id`
  rejects `-`.
- The host reaches a guest vsock port through a Unix socket: write
  `CONNECT <port>\n` in one write, then read `OK <n>\n`. If nothing listens,
  Firecracker closes the socket. The guest must accept within 2 s.
- On x86, `poweroff` does not stop Firecracker. `reboot` with `reboot=k`
  does. `8250.nr_uarts=0` removes the unbounded serial console.
- CVE-2026-5747 (virtio-PCI, fixed in 1.14.4 and 1.15.1) and CVE-2026-1386
  (jailer symlink, fixed in 1.13.2 and 1.14.1) set the minimum version.
- VMGenID reseeds the guest kernel CSPRNG after a snapshot restore on Linux
  5.18 and later. User-space random state stays cloned.
- Postgres `jsonb` rejects `\u0000`. Code that prints a NUL byte would break
  the activity result insert.
- serde_json keeps a recursion limit of 128. Parsing into `Value` uses up to
  25 times the input size in heap.
- Rust `Command` inherits the signal mask and the environment.
  `PR_SET_PDEATHSIG` follows the forking thread and clears on a uid change.
- Products converge on one shape: run code, return the exit code, stdout,
  stderr and files. A non-zero exit is data, not an error. Network is denied
  by default.

### 0.2 Brainstorm — how can an activity run any code?

| # | Idea | Verdict |
|---|------|---------|
| B1 | Compile agent code to WASM (Pyodide, componentize-py). | Rejected as the only path. Native extensions, subprocesses and most CLIs do not work. WASM activities stay for small pure functions. |
| B2 | A container per run (runc, Docker). | Rejected. The host kernel is shared, and runc escapes recur (CVE-2019-5736, CVE-2024-21626). It needs a daemon. |
| B3 | gVisor `runsc`. | Deferred. It is a good second backend for hosts without KVM. The backend trait admits it later. |
| B4 | A Firecracker microVM per run, behind the jailer. | **Adopted** as the security boundary. Each run gets its own guest kernel and a minimal device model. |
| B5 | A Linux process sandbox: rlimits, an empty network namespace, no host environment. | **Adopted** for development and CI, and as the executor inside the guest. The default policy refuses it as a boundary. |
| B6 | Send the plan over vsock as length-prefixed JSON frames. | **Adopted.** No guest network stack, no per-run disk image, and it works after a snapshot restore. |
| B7 | MMDS, a second drive or the kernel command line for the payload. | Rejected. MMDS needs a network device. A drive needs an image per run. The command line is tiny. |
| B8 | A warm pool of booted, single-use microVMs. | **Adopted.** It hides the boot time. A VM never serves two runs. |
| B9 | Restore each run from a golden snapshot. | **Adopted as an option.** The host sends fresh entropy, and the guest reseeds its kernel pool before the code runs. |
| B10 | Reuse one VM for a session of runs. | Rejected for now. State would leak between runs, and a retry would see changed state. |
| B11 | A non-zero exit, a signal or a timeout is an outcome. | **Adopted.** The agent reads stderr and fixes its code. Only infrastructure faults retry. |
| B12 | A new history event for a sandbox run. | Rejected. An activity already records input and output. |
| B13 | An agent `Tool` that runs code. | **Adopted.** The agent crate gets a `sandbox` feature. |
| B14 | Network egress through a filtering proxy. | Deferred. The first version denies the network. `NetworkPolicy::Allow` exists for an operator who provides a filtered network. |
| B15 | Hand-roll the Firecracker client instead of an SDK crate. | **Adopted.** The SDK crates are young or stale. About 150 lines of HTTP/1.1 over a Unix socket cover the API. |

### 0.3 Reverse brainstorm — how can this design do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | A guest declares a 4 GiB frame, and the host allocates it. | The reader checks the length against a cap before it reads the body, and it never preallocates. |
| R2 | Code floods stdout and fills host memory. | A capture keeps a fixed head and tail, counts the rest and drains the pipe. |
| R3 | Code prints a NUL byte, and the `jsonb` insert of the result fails. | The capture replaces NUL with U+FFFD. |
| R4 | Code leaves a background process that outlives the run. | The child is a session leader. The executor kills the whole group at exit, timeout and cancel. The microVM dies after one run. |
| R5 | Code spins forever. | A host-side wall-clock limit kills it. The outcome says `timed_out`. |
| R6 | Code reads host secrets from the environment. | `env_clear`. The code sees only `PATH`, `HOME`, `TMPDIR`, `LANG` and the request variables. |
| R7 | A request file path writes outside the work directory (`../`, `/`). | Paths must be relative, with normal components only. The guest checks them again. Files open with `O_EXCL` and `O_NOFOLLOW`. |
| R8 | Code links an output path to a host file such as `/etc/shadow`. | Outputs open with `openat2(RESOLVE_BENEATH, RESOLVE_NO_SYMLINKS)`, non-blocking, and must be regular files. |
| R9 | An agent asks for a huge limit. | A value over a ceiling is a non-retryable denial that names the ceiling. |
| R10 | Code exfiltrates data over the network. | The microVM has no network device. The process sandbox enters an empty network namespace, and it fails closed when it cannot. |
| R11 | An operator runs hostile code in the process sandbox by mistake. | The default policy requires microVM isolation. The runtime refuses a weaker backend. |
| R12 | A guest sends a stale or forged outcome. | The host checks the run id, the protocol version and every bound of the outcome. |
| R13 | A VM, a jail directory or a cgroup leaks after a cancel or a crash. | One teardown path runs on success, error, timeout, cancel and drop. It uses `cgroup.kill`, then removes the jail. A startup sweep removes stale jails. |
| R14 | A deterministic failure retries until the budget is gone. | Denials and protocol violations are non-retryable. |
| R15 | The host environment leaks into the jailer. | The launcher clears the environment of the jailer too. |
| R16 | Clones of one snapshot share random state. | Each run carries 32 bytes of host entropy. The guest mixes them in and forces a reseed before the code runs. |
| R17 | Many runs at once exhaust the host. | A semaphore bounds the runs of one runtime. |
| R18 | A slow boot holds the activity forever. | A boot deadline ends the attempt as a retryable `SandboxUnavailable`. |
| R19 | A guest sends deeply nested JSON. | The frame cap bounds the bytes, and the serde_json recursion limit rejects the depth. |
| R20 | Code inherits a host file descriptor. | The child marks every descriptor above 2 close-on-exec. |
| R21 | Code inherits a blocked signal mask. | The child resets its signal mask. |
| R22 | A compromised VMM rewrites the jail pid file to make the host kill a process. | The host never reads a pid from the jail. It kills through its own child handle and the cgroup. |
| R23 | The host runs a Firecracker with a known escape CVE. | The backend refuses a version below the patched minimum. |
| R24 | The guest prints to the serial console without end. | The kernel arguments disable the serial port. |
| R25 | The sandbox run outlives the activity timeout, so a retry runs the code twice. | The registration sets `start_to_close` above the wall-clock ceiling plus the backend overhead. |
| R26 | An outcome is over the activity result cap. | The registration raises `max_result_bytes` to the policy outcome bound. |

### 0.4 Six thinking hats

| Hat | Notes |
|-----|-------|
| White | Firecracker gives a guest kernel per run, in about 125 ms, for about 5 MiB. Snapshots restore in about 3 ms. No CI runner here has KVM. The engine already records activity input and output. |
| Red | Teams fear LLM code on a production host. They want "it just runs Python". They also fear a sandbox that is too slow to use in a loop. |
| Black | KVM is absent in CI and in many cloud VMs. Building a kernel and a rootfs is operator work. The process backend is weak. The real KVM path cannot run in this CI, so a fake VMM must stand in. |
| Yellow | Any language, a real kernel boundary, and output that the agent can read. Replay never runs code again. The engine needs no change. |
| Green | A warm pool and snapshots for speed. One executor inside and outside the VM. A gVisor backend and an egress proxy later. Head-and-tail capture for tracebacks. |
| Blue | Ship the runtime, both backends, the guest agent, the activity and the agent tool. Prove each defense with a test. Mark the KVM path as verified by a fake VMM, plus an opt-in real test. |

### 0.5 Decisions

- Module `autumn_harvest::sandbox`, behind `sandbox-activities`. Linux-only
  parts sit behind `cfg(target_os = "linux")`, so every OS builds the feature.
- `SandboxRequest` in, `SandboxOutcome` out, both plain JSON.
- `SandboxPolicy` holds runtimes, default and ceiling limits, size bounds,
  the network policy and the minimum isolation. The default requires a
  microVM and denies the network.
- `SandboxBackend` is a trait. `ProcessBackend` and `FirecrackerBackend`
  implement it.
- Error types: `SandboxDenied` (non-retryable), `SandboxUnavailable`
  (retryable), `SandboxProtocolViolation` (non-retryable) and
  `SandboxCancelled` (retryable).
- The guest agent is a binary crate, `autumn-harvest-sandbox-guest`. It is
  init in the microVM, listens on vsock port 52, and reboots after one run.
- Firecracker runs behind the jailer with cgroup v2. A `VmLauncher` trait
  lets a fake VMM stand in for tests.
- No PCI, no network device, no MMDS, no serial console.

---

## 1. Acceptance criteria

The evidence column is filled in when the work is done. See §3.

| AC | Statement |
|----|-----------|
| AC1 | The feature is off by default, adds no crate, and builds on Linux, macOS and Windows. |
| AC2 | An agent submits code, a runtime, files, stdin, arguments, environment and output paths as JSON. Unknown fields are an error. |
| AC3 | The policy checks each request. A violation is a non-retryable denial that names the rule and the limit. |
| AC4 | The default policy denies the network and refuses a backend weaker than a microVM. |
| AC5 | A non-zero exit, a signal and a timeout are outcomes. Stdout and stderr keep a bounded head and tail, with byte counts and truncation flags. NUL never reaches history. |
| AC6 | Requested output files return, bounded. A symlink, a FIFO or a path outside the work directory never returns. |
| AC7 | Infrastructure faults are retryable. Denials and protocol violations are not. A cancel stops the run. |
| AC8 | The process executor clears the environment, runs in a private directory, applies rlimits, makes the child a session leader, closes inherited descriptors, resets the signal mask, sets no-new-privileges and kills the whole group. |
| AC9 | The process executor denies the network with an empty namespace, and it fails closed when it cannot create one. |
| AC10 | The wire protocol uses versioned, length-prefixed frames. It checks the cap before it reads a body, and it checks the run id. |
| AC11 | The guest agent answers a ping, runs one plan, reseeds its random pool from host entropy, and checks the plan again. |
| AC12 | The Firecracker config, the jailer arguments and the kernel arguments match the documented API: cgroup v2, a read-only root, vsock, entropy, no network, no serial. |
| AC13 | The API client speaks HTTP/1.1 over a Unix socket, bounds the response, and reports `fault_message`. |
| AC14 | The vsock connector sends `CONNECT` in one write and retries until the boot deadline. |
| AC15 | Each microVM serves one run. Teardown runs on success, error, timeout, cancel and drop, and it removes the jail and the cgroup. |
| AC16 | A warm pool keeps booted microVMs ready, hands out each one once, refills, and kills all at shutdown. |
| AC17 | A snapshot boot method loads a golden snapshot through the API, and each run carries fresh entropy. |
| AC18 | The backend refuses a Firecracker version below the patched minimum, and it reports host posture risks. |
| AC19 | A runtime bounds the runs at one time, and a cancel ends the wait for a slot. |
| AC20 | `HarvestBuilder::sandbox_activity` registers a normal activity. Its handler honors cancellation, injects the idempotency key, and sets timeouts and the result cap from the policy. |
| AC21 | The agent crate offers a `Tool` that runs code through the runtime. Its effect follows the network policy. |
| AC22 | A guide, this design record and the changelog fragment exist in ASD-STE100 style, and the comment hygiene gate passes. |
| AC23 | CI runs the new tests on every OS where they apply. The KVM test is opt-in and documented. |

---

## 2. TDD map

Red: each test below is written first and fails against a stub. Green: the
implementation makes it pass. Refactor: the code is cleaned with the tests
green.

| Area | Tests | Proves |
|------|-------|--------|
| Policy | `sandbox::policy::tests` | AC3, AC4 |
| Request | `sandbox::request::tests` | AC2, AC3, R7, R9 |
| Outcome | `sandbox::outcome::tests` | AC5, R12 |
| Capture | `sandbox::capture::tests` | AC5, R2, R3 |
| Errors | `sandbox::error::tests` | AC7, R14 |
| Protocol | `sandbox::protocol::tests` | AC10, R1, R19 |
| Runtime | `sandbox::runtime::tests` | AC4, AC19, R11, R12, R17 |
| Executor | `sandbox::exec::tests` | AC5, AC6, AC8, AC9, R4 to R8, R10, R20, R21 |
| Guest | `sandbox::guest::tests` | AC11, R16 |
| Firecracker | `sandbox::firecracker::*::tests` | AC12 to AC18, R13, R15, R18, R22 to R24 |
| Activity | `sandbox::activity::tests` | AC20, R25, R26 |
| Agent tool | `autumn-harvest-agent` `sandbox` tests | AC21 |
| Docs | `sandbox_activities_docs` guard suite | AC22 |

---

## 3. Evidence

Filled in at the end of the work.
