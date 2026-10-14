# Sandboxed code activities

A sandboxed code activity runs code that an agent wrote. The code can be
Python, Node, shell or any program in the guest image. Each run gets a new
Firecracker microVM with its own guest kernel, no network and a read-only
root file system. The microVM serves one run and then goes away.

The feature is `sandbox-activities`. It is off by default. It needs no
database and no migration, and it adds no history event. A sandbox run is an
ordinary activity, so history records the request and the outcome. Replay
reads the outcome and never runs the code again.

The design record is [`DESIGN-sandbox-activities.md`](../DESIGN-sandbox-activities.md).

## When to use it

| Need | Use |
|------|-----|
| A small pure function, deterministic, published ahead of time | WASM activities (`wasm-activities`) |
| Code that an agent writes at run time, in any language, with files and subprocesses | Sandboxed code activities |
| Trusted code with full host access | A native `#[activity]` |

## The request and the outcome

An agent sends a request as JSON. Unknown fields are an error.

```json
{
  "runtime": "python3",
  "code": "import sys\nprint(sum(map(int, sys.argv[1:])))",
  "args": ["1", "2", "3"],
  "stdin": "optional text",
  "files": [{"path": "data/in.csv", "text": "a,b\n1,2\n"}],
  "env": {"MODE": "fast"},
  "outputs": ["out/report.txt"],
  "limits": {"timeout_ms": 10000, "memory_mib": 256}
}
```

| Field | Meaning |
|-------|---------|
| `runtime` | A name from the policy: `python3`, `node`, `bash` or `sh` by default. |
| `code` | The code. The executor writes it to the file of the runtime, such as `main.py`. |
| `args` | Arguments after the code file. |
| `stdin` | Text for stdin. |
| `files` | Input files. Set `text` or `base64`, and `executable` if needed. |
| `env` | Variables. The code gets no host environment. A request cannot set `LD_*`, `DYLD_*`, `HARVEST_*` or an executor name. |
| `outputs` | Relative paths of files to return. |
| `limits` | `timeout_ms`, `cpu_seconds`, `memory_mib` and `max_output_bytes`. Each must be at most the policy ceiling. |

The policy bounds the size of each request and each outcome. Both bounds must
fit the 64 MiB guest frame, or the policy is not valid.

The outcome is JSON too:

```json
{
  "status": {"kind": "exited", "code": 0},
  "stdout": "6\n",
  "stderr": "",
  "stdout_bytes": 2,
  "stderr_bytes": 0,
  "stdout_truncated": false,
  "stderr_truncated": false,
  "outputs": [{"path": "out/report.txt", "content": "<base64>"}],
  "skipped_outputs": [],
  "duration_ms": 41,
  "isolation": "micro_vm",
  "image": "sha256:..."
}
```

- `status.kind` is `exited`, `signaled` or `timed_out`. These are outcomes,
  not failures. The agent reads stderr and fixes its code.
- A signal of `24` is the CPU limit. Code that ignores it gets signal `9`
  one second later. A signal of `25` is the file size limit.
- The outcome keeps the head and the tail of each stream. The `*_bytes`
  fields count every byte. A NUL byte becomes U+FFFD, because Postgres
  `jsonb` rejects `\u0000`.
- Each requested output appears once: in `outputs` or in `skipped_outputs`.
  `skipped_outputs` names each output that the run does not return, with the
  reason. A file can be missing, too large, a symbolic link, a hard link, or
  not a regular file.
- The host sets `isolation` and `image`. The guest cannot change them. The
  image name has at most 256 bytes and no control character. The host also
  caps `duration_ms` at the time that it measured.
- A reader of the outcome skips an unknown field. So a later release can add
  a field, and an older worker still reads the result.

## Failures

A failure means that the code does not reach an outcome.

| Error type | Retry | Cause |
|------------|-------|-------|
| `SandboxDenied` | No | The request breaks the policy or the exec limits, the runtime command does not exist, or the worker configuration is not valid. |
| `SandboxUnavailable` | Yes | The backend cannot boot, connect or finish in time. No slot is free, or the attempt has too little time left. |
| `SandboxProtocolViolation` | No | The guest sends a malformed, oversized or wrong answer, or speaks another protocol version. |
| `SandboxCancelled` | Yes | The caller cancels the activity. The backend kills the run. |

## Development: the process backend

The process backend runs the code as a host process. It needs no KVM, so it
works in CI and on a laptop. It shares the host kernel, so it does not
contain hostile code. The default policy refuses it.

```rust,ignore
use std::sync::Arc;
use autumn_harvest::builder::HarvestBuilder;
use autumn_harvest::sandbox::process::ProcessBackend;
use autumn_harvest::sandbox::{SandboxActivity, SandboxPolicy, SandboxRuntime};

let runtime = SandboxRuntime::new(
    Arc::new(ProcessBackend::default()),
    SandboxPolicy::for_development(),
)?;
let harvest = HarvestBuilder::new()
    .sandbox_activity(SandboxActivity::new("run_code", Arc::new(runtime)))
    .build();
```

The process executor gives each run:

- a new private work directory, removed after the run;
- no host environment;
- a new session, and a kill of the whole process group at exit, timeout,
  cancel and drop;
- close-on-exec on each inherited descriptor, and an empty signal mask. On a
  kernel without `close_range`, a walk of `/proc/self/fd` marks them. When
  neither works, the run fails closed;
- `PR_SET_NO_NEW_PRIVS`;
- rlimits for CPU time, data memory, file size, open files and core dumps.
  The data rlimit is the memory limit plus 1 GiB, because V8 in Node 24
  reserves about 600 MiB of address space at start. The CPU rlimit applies
  to each process;
- with `ExecOptions::cgroup_root`, a cgroup for each run. Then `cpu_seconds`
  bounds all processes of the run together, and `memory_mib` bounds their
  memory, with no swap. The end of the run kills each process in the cgroup.
  The cgroup root must give the memory controller to its children, or the
  run fails closed. The guest agent always sets it;
- an empty network namespace when the policy denies the network. When the
  host cannot create one, the run fails closed with `SandboxUnavailable`;
- a process limit. A root worker runs the code as a dedicated uid. A
  non-root worker runs each run in a new user namespace, which counts the
  processes of that run alone. That needs Linux 5.14 or later and user
  namespaces. Without them, the run fails closed;
- uid and gid `65534` when the worker runs as root. The executor refuses
  uid 0 and gid 0 for the code.

The process backend has these limits. Inside a microVM, none of them
matters, because the microVM ends after one run.

- It has no seccomp filter, no mount namespace and no cgroup.
- Without `ExecOptions::cgroup_root`, a process that calls `setsid` leaves
  the group, and the executor cannot kill it.
- The code can write wherever its uid can write, such as `/tmp`. The
  executor removes only the work directory, also when the code moves it.
- On a root host, concurrent runs share the uid `nobody`, unless `run_as`
  names another uid. A non-root worker refuses `run_as`.

The process backend is the only backend that can give code a network. A
runtime refuses a policy that allows the network on a backend without one.

## Production: the Firecracker backend

### Host requirements

- Linux with `/dev/kvm`, cgroup v2 with `cgroup.kill` (Linux 5.14), and root
  for the jailer.
- Absolute paths in `LauncherConfig`. Firecracker runs in the VM directory,
  so the launcher refuses a relative path.
- Images and snapshot files on the file system of the jail root, so each
  launch hard-links them. The launcher refuses a file on another file system,
  because a copy per launch has no time bound.
- `JailerConfig::cgroup_root` set to the cgroup v2 mount of the jailer: the
  first `cgroup2` line in `/proc/mounts`. The default is `/sys/fs/cgroup`.
  The launcher refuses another path, because teardown kills the cgroups
  under it.
- Firecracker 1.14.4 or later on 1.14, or 1.15.1 or later. The first launch
  checks the versions of `firecracker` and `jailer`. It refuses older
  versions: CVE-2026-5747 and CVE-2026-1386.
- KSM off, nested KVM off, and no CPU vulnerability that reports
  `Vulnerable`.
- Root owns each trusted path: the jail base, network namespace, Firecracker
  and jailer binaries, kernel, root file system and snapshot. No such path
  holds a symbolic link. Only root may write each directory above them,
  except a sticky directory such as /tmp. The launcher refuses other paths,
  and it refuses a jail uid or gid of 0.
- A kernel, root file system and snapshot that the jail gid or others can
  read. A golden snapshot gets mode `0640` and the jail group.
- A worker that runs as root. The launcher refuses a jailed configuration in
  any other process, because the jailer needs root.
- SMT off on a host that serves more than one tenant. Swap off.
- An empty network namespace for the jailer, through `JailerConfig::netns`.
  Give its path under `/run/netns`, because `/var/run` is a symbolic link on
  most hosts.
  The microVM has no network device. The namespace also cuts the network of
  an escaped VMM.
- One launcher per tenant, each with its own jail uid and gid. The microVMs
  of one launcher share their uid, so an escaped VMM can signal its
  siblings.
- Extra cgroup values in `JailerConfig::extra_cgroups` that name plain
  controller files. The launcher refuses `cpu.max`, `memory.max`,
  `memory.swap.max`, `pids.max` and the `cgroup.` core files, because it sets
  the limits itself.

Call `verify_host` before the first run. It checks the versions and the host,
including cgroup v2 at `JailerConfig::cgroup_root`. Under
`PosturePolicy::Enforce`, the default, it refuses a critical finding.
An unjailed launcher is a critical finding too, and so is a jailer with no
network namespace. SMT and swap give warnings.

### The guest image

```sh
scripts/sandbox/build-guest-image.sh /var/lib/harvest/image
```

The script writes `vmlinux`, `rootfs.ext4` and `image.sha256`. It builds
them in a stage directory first, so a failed build keeps the old image. A
failed final rename puts the old files back. The root file system is a
container image, `python:3.12-alpine` with `bash` and `nodejs` by default,
plus the guest agent at `/sbin/harvest-guest-agent`. Set
`BASE_IMAGE` for another image, and pin it by digest. Run the script as root,
or install `fakeroot`, so root owns the files in the image.

The kernel comes from the Firecracker CI artifacts. The script pins a default
kernel by its SHA-256 digest and checks the digest on each build. For another
kernel, set `KERNEL_URL` and `KERNEL_SHA256` together. The script refuses a
`KERNEL_URL` without a digest.

A jailed launcher also needs root to own `vmlinux` and `rootfs.ext4` on the
host. Fakeroot does not give them root. After a non-root build, install them
as root:

```sh
sudo install -o root -g root -m 0644 image/vmlinux image/rootfs.ext4 /srv/harvest-image/
```

The guest agent is process 1. It mounts `/proc`, `/sys`, the cgroup v2 file
system, `/dev`, `/tmp` and a `/work` tmpfs. It listens on vsock port 52. Each
run mixes 32 bytes of host entropy into the kernel pool and forces a reseed.
Each run also sets the guest clock from the host clock. The host reads its
clock only after the guest has decoded the request, so a slow transfer does
not put the guest clock behind. Then the agent runs the code as uid 1000 and
reboots, and Firecracker exits.

Each run gets its own cgroup in the guest. So `cpu_seconds` bounds the CPU
time of all processes of the run together. When the run passes it, the agent
kills each process, and the outcome reports the signal `SIGXCPU`. The cgroup
also bounds the memory of the run to `memory_mib`. A run past it ends with
`SIGKILL` from the OOM killer.

### Configuration

```rust,ignore
use std::sync::Arc;
use std::time::Duration;
use autumn_harvest::sandbox::firecracker::{
    BackendSettings, BootMethod, Confinement, FirecrackerBackend, FirecrackerLauncher,
    GuestImage, JailerConfig, LauncherConfig, MachineSpec, PosturePolicy,
};
use autumn_harvest::sandbox::{SandboxPolicy, SandboxRuntime};

let mut jail = JailerConfig::new("/usr/local/bin/jailer", 10_000, 10_000);
// An empty namespace from `ip netns add harvest-sandbox`.
jail.netns = Some("/run/netns/harvest-sandbox".into());
let launcher = FirecrackerLauncher::new(LauncherConfig {
    firecracker_bin: "/usr/local/bin/firecracker".into(),
    image: GuestImage::new("/var/lib/harvest/image/vmlinux", "/var/lib/harvest/image/rootfs.ext4")
        .with_name("rootfs-sha256:..."),
    machine: MachineSpec { vcpu_count: 1, mem_size_mib: 512 },
    boot: BootMethod::ConfigFile,
    confinement: Confinement::Jailer(jail),
    guest_port: 52,
    api_timeout: Duration::from_secs(5),
})?;
launcher.verify_host(PosturePolicy::Enforce).await?;
launcher.sweep_stale();
let backend = FirecrackerBackend::new(Arc::new(launcher), BackendSettings::default())?
    .with_warm_pool(4);
let runtime = SandboxRuntime::new(Arc::new(backend), SandboxPolicy::default())?;
```

The backend takes the guest port from the launcher.

What each microVM gets:

- one vCPU and 512 MiB by default, with `cpu.max`, `memory.max`,
  `memory.swap.max` and `pids.max` on its cgroup. The microVM size bounds
  the memory that the code uses;
- a read-only root drive, a vsock device and a virtio-rng device;
- no network device, no metadata service and no serial console;
- the kernel arguments `reboot=k panic=1 8250.nr_uarts=0`;
- an 8 GiB `fsize` limit on the VMM, by default.

Teardown kills the process, then the cgroup through `cgroup.kill`, then
removes the jail. It runs after success, error, timeout and cancel. It also
runs when the caller drops the run future. The host never reads a pid from
the jail. A cgroup that is still there after the teardown stays with the
launcher. The launcher kills and removes it again before each launch and in
each `sweep_stale`.

Each launcher holds a lease file next to its VM directories. `sweep_stale`
removes the microVMs and the cgroups of each owner whose lease is not locked.
So a crashed worker leaves nothing behind after the next start. A sweep kills
only the cgroups of owners with a lease or a VM directory under its own VM
root. So launchers with other VM roots can share one cgroup parent. A dead
lease stays until the microVMs and the cgroups of its owner are gone.

The jailer puts each microVM cgroup under `JailerConfig::cgroup_parent`. That
is outside the cgroup of a systemd service by default. So a stop of the
service does not kill the microVMs, but the next `sweep_stale` does. To tie
them to the service, set `Delegate=yes`. Then set `cgroup_parent` to a cgroup
inside the service subtree. The worker process must then run in a leaf of that
subtree, because cgroup v2 keeps processes only in leaves.

### Speed

| Path | Time to a listening agent |
|------|---------------------------|
| Cold boot | About 125 ms in the Firecracker specification, plus guest init. |
| Warm pool | None: the pool boots ahead. Set `with_warm_pool(n)`. |
| Snapshot | About 3 ms for the load call, then lazy page faults. |

`BootMethod::Snapshot` loads a golden snapshot for each run. It needs the
jailer, so each restore sees the same paths.
`FirecrackerLauncher::create_golden_snapshot` boots a golden microVM, waits
for its agent, pauses it and saves the files. No code runs in it. The files
hold the guest memory, so the golden microVM must hold no secret. The new
pair replaces an old pair only after both copies are complete, and a failed
rename puts the old file back. Create it before the launchers that use it
start, because a launch between the two renames can see a mixed pair. A host
crash between the two renames can also leave a mixed pair, so create the
snapshot again after such a crash.

Clones of one snapshot share their user-space random state. Each run
therefore carries fresh host entropy, and VMGenID reseeds the kernel on Linux
5.18 and later. A restored microVM also starts with the clock of the
snapshot, and each run sets the clock again. Rebuild the snapshot on each
change of Firecracker, the host kernel or the CPU model.

## The agent tool

The `sandbox` feature of `autumn-harvest-agent` adds `SandboxTool`. Its name
is `run_code`, and its input schema is the request above.

```rust,ignore
use std::sync::Arc;
use autumn_harvest_agent::sandbox::SandboxTool;

let harness = AgentHarness::new(model).tool(SandboxTool::new(Arc::new(runtime)).shared());
```

| Isolation | Network | Tool effect |
|-----------|---------|-------------|
| microVM | Denied | `Internal` |
| Host process | Denied | `Write` |
| Either | Allowed | `External` |

A non-zero exit is a tool result. A denial is a tool error that the model can
read. The harness cuts a long tool result. So the tool returns a bounded
view. It holds the status, a head and a tail of each stream, and a short part
of each text output file. The bounds count the JSON text, so escaped
characters cannot push the status out of the cut. An output entry that does
not fit is only counted.

## Durability

- The registration sets `start_to_close` to `max_attempt_time`. That is the
  queue wait, the wall-clock ceiling, the backend overhead and the cleanup
  grace together. The backend overhead is at least what the backend needs:
  for Firecracker, the boot timeout, the request transfer, the response grace
  and the teardown.
- The Firecracker backend starts the response grace after the guest takes
  the request, so a large request takes no time from the code. The grace
  covers the guest setup, the cleanup and the outcome of a full 64 MiB frame.
- The handler gives the runtime the time left in the attempt. The runtime
  keeps the cleanup grace for teardown. It does not start code that cannot
  finish in the rest. So a retry does not overlap a live run.
- A worker crash can leave a run alive until the next `sweep_stale`. Code
  with outside effects should use `HARVEST_IDEMPOTENCY_KEY`.
  `HARVEST_ATTEMPT` holds the attempt.
- The registration raises `max_result_bytes` to the outcome bound of the
  policy, and `max_input_bytes` to its request bound.
- `with_requires("kvm")` sends the tasks only to workers with that label.
- Do not put secrets in a request. History keeps the request, and the code
  can read it.

## Testing

Most tests need no KVM. A fake VMM stands in for Firecracker: it speaks the
vsock `CONNECT` handshake, and it runs the real guest code and the real
executor. Other tests run fake `firecracker` and `jailer` scripts, and a fake
API server.

```sh
cargo test -p autumn-harvest --no-default-features --features sandbox-activities --lib sandbox
cargo test -p autumn-harvest-sandbox-guest
cargo test -p autumn-harvest-agent --features sqlite,eval,sandbox
```

The `Sandbox (KVM)` workflow boots real microVMs on a GitHub runner with
`/dev/kvm`. It runs on each pull request that changes the sandbox code, and
weekly. It builds the guest image and runs the ignored KVM tests. Run them on
a KVM host like this:

```sh
scripts/sandbox/build-guest-image.sh /tmp/image
HARVEST_FIRECRACKER_BIN=/usr/local/bin/firecracker \
  HARVEST_GUEST_KERNEL=/tmp/image/vmlinux HARVEST_GUEST_ROOTFS=/tmp/image/rootfs.ext4 \
  cargo test -p autumn-harvest --no-default-features --features sandbox-activities --lib \
  sandbox::firecracker::kvm_tests -- --ignored --skip root_ --test-threads=1
```

The tests whose names start with `root_` need root. They include the jailer
and snapshot tests. Set `HARVEST_JAILER_BIN`, and run the test binary as root
with the filter `root_` and `--ignored`. The workflow does both.

## Research sources

- Firecracker documentation: <https://github.com/firecracker-microvm/firecracker/tree/main/docs>
- Firecracker, NSDI 2020: <https://www.usenix.org/conference/nsdi20/presentation/agache>
- The jailer: <https://github.com/firecracker-microvm/firecracker/blob/main/docs/jailer.md>
- Vsock: <https://github.com/firecracker-microvm/firecracker/blob/main/docs/vsock.md>
- Production host setup: <https://github.com/firecracker-microvm/firecracker/blob/main/docs/prod-host-setup.md>
- Snapshot clone safety: <https://github.com/firecracker-microvm/firecracker/blob/main/docs/snapshotting/random-for-clones.md>
- Security advisories: <https://github.com/firecracker-microvm/firecracker/security/advisories>
- Postgres `jsonb` and `\u0000`: <https://www.postgresql.org/docs/current/datatype-json.html>
- cgroup v2 `cgroup.kill`: <https://docs.kernel.org/admin-guide/cgroup-v2.html>
- `openat2`: <https://man7.org/linux/man-pages/man2/openat2.2.html>
- `close_range`: <https://man7.org/linux/man-pages/man2/close_range.2.html>
