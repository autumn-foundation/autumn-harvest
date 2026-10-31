# Sandboxed code activities

A sandboxed code activity runs code that an agent wrote. The code can be
Python, Node, shell or any program in the guest image. Each run gets a new
Firecracker microVM with its own guest kernel, no network and a read-only
root file system. The microVM serves one run and then goes away.

The feature is `sandbox-activities`. It is off by default. It needs no
database and no migration, and it adds no history event. A sandbox run is an
ordinary activity, so history records the request and the outcome. Replay
reads the outcome and never runs the code again.

The design record is [`docs/design/DESIGN-sandbox-activities.md`](design/DESIGN-sandbox-activities.md).

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
fit the 64 MiB guest frame, or the policy is not valid. Each runtime with the
largest request must also fit the 2 MiB total exec limit of Linux. The
executor gives each child a stack limit of 8 MiB, or the hard limit when that
is lower. The exec allowance is a quarter of it. So a process backend under a
hard stack limit below 8 MiB refuses a policy over that smaller allowance.

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
- A read of an output stops at `max_output_file_bytes` or at the bytes left
  of `max_output_files_total_bytes`, whichever is less. A file that does not
  fit is skipped unread.
- The policy sizes the outcome frame by `max_output_files_total_bytes`. A
  larger `max_output_file_bytes` returns no more bytes, so the frame check
  does not count it.
- Each output list holds at most `MAX_OUTPUT_ENTRIES` (4096) entries. The
  host decoder stops there, so a guest cannot fill host memory with tiny
  entries. A policy may not allow more outputs.
- A policy allows at most `MAX_REQUEST_FILES` (4095) files. The guest work
  directory holds 65,536 inodes, and each file can take one for each of its
  16 path components.
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

An error text can quote the VMM or the guest. Each NUL byte in it becomes
U+FFFD before it reaches history or a model.

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

- a new private work directory, removed after the run. A removal that fails
  ends the run as `SandboxUnavailable`, and the drop tries again;
- no host environment;
- a new session, and a kill of the whole process group at exit, timeout,
  cancel and drop;
- close-on-exec on each inherited descriptor, and an empty signal mask. On a
  kernel without `close_range`, a walk of `/proc/self/fd` marks them. When
  neither works, the run fails closed;
- `PR_SET_NO_NEW_PRIVS`;
- rlimits for CPU time, data memory, file size, open files and core dumps.
  A policy allows 64 to 1,048,576 open files (`MIN_OPEN_FILES`,
  `MAX_OPEN_FILES`). `setrlimit` refuses more than the default `fs.nr_open`.
  A policy also refuses more than `u32::MAX` CPU seconds, and more than 1 PiB
  of memory or file size (`MAX_LIMIT_BYTES`). A larger value could reach
  `RLIM_INFINITY`, which removes the limit with no error.
  A child cannot raise a hard limit above the one of the worker without root
  and `CAP_SYS_RESOURCE`. A non-root child enters a user namespace first, and
  a container often drops that capability. So the process backend refuses a
  policy limit above a hard limit of the worker, as a config error. Raise the
  worker limit, such as with systemd `LimitNOFILE`, or lower the policy.
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
  uid 0 and gid 0 for the code. It also refuses `u32::MAX`, which is
  `(uid_t)-1` and which `setuid` and `setgid` refuse. The launcher refuses
  the same ids for the jail, and for `harvest.uid` and `harvest.gid`.

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
  for the jailer. `verify_host` wants the KVM character device 10:232, open
  to read and write. A masked device, such as a plain file, is critical.
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
  versions: CVE-2026-5747 and CVE-2026-1386. The launcher copies each
  binary that passes into a directory of its own, and each launch runs the
  copies. So a deploy cannot swap a binary between the check and the launch.
  A deploy that replaced or changed either binary gives a new check and new
  copies. The check reads at most 64 KiB of `--version` output, so a broken
  wrapper cannot fill the memory of the worker. The launcher refuses a binary
  above 256 MiB (`MAX_VMM_BINARY_BYTES`), because each check copies it.
- A jail base, or an unjailed `work_root`, that lets the worker run programs.
  The copies run from there, so the launcher refuses a `noexec` mount. It
  also refuses a `nodev` jail base, because Firecracker cannot open
  `/dev/kvm` in a jail there.
- KSM off, nested KVM off, and each CPU vulnerability `Not affected` or
  mitigated. Any other CPU status, such as `Vulnerable` or `Unknown`, is
  critical. A KSM, nested KVM or CPU vulnerability state that cannot be read,
  or that is unknown, is also critical. A kernel without KSM, or without a
  loaded KVM vendor module, is safe for that check.
- Root owns each trusted path: the jail base, network namespace, Firecracker
  and jailer binaries, kernel, root file system, snapshot and snapshot
  manifest. No such path holds a symbolic link. Only root may write each
  directory above them, except a sticky directory such as /tmp. Only its owner
  may write the VM root under the jail base, even a sticky one, because the
  sweep trusts its entries. The launcher refuses other paths, and
  it refuses a jail uid or gid of 0.
- A kernel, root file system and snapshot that the jail gid or others can
  read. A golden snapshot gets mode `0640` and the jail group.
- A worker that runs as root. The launcher refuses a jailed configuration in
  any other process, because the jailer needs root.
- SMT off on a host that serves more than one tenant. Swap off.
- An empty network namespace for the jailer, through `JailerConfig::netns`.
  Give its path under `/run/netns`, because `/var/run` is a symbolic link on
  most hosts. The jailer joins it with `setns`, so the launcher refuses a
  path that is not a network namespace handle.
  The microVM has no network device. The namespace also cuts the network of
  an escaped VMM.
- A Firecracker binary whose file name is not a name in the jail root. The
  jailer copies the binary to the root under its file name, so a name such
  as `vmlinux` or `dev` is a config error.
- One launcher per tenant, each with its own jail uid and gid. The microVMs
  of one launcher share their uid, so an escaped VMM can signal its
  siblings.
- Extra cgroup values in `JailerConfig::extra_cgroups` that name plain
  controller files. The launcher refuses `cpu.max`, `memory.max`,
  `memory.swap.max`, `pids.max` and the `cgroup.` core files, because it sets
  the limits itself. The jailer gets each value as an argument. So the
  launcher also refuses values that do not fit the exec limits. A key over
  255 bytes (`NAME_MAX`) is a config error too, because the jailer cannot
  open that file. So is an empty value or a value with `=`, which the jailer
  cannot parse.
- A cgroup root whose `cgroup.controllers` lists `cpu`, `memory`, `pids` and
  the controller of each extra key. The jailer refuses each launch without
  them. The jailer also writes `memory.swap.max`, so the host needs swap
  accounting.

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
failed final rename puts the old files back. When that fails too, the script
keeps the old files in a backup directory and names it. The root file system is a
container image, `python:3.12-alpine` with `bash` and `nodejs` by default,
plus the guest agent at `/sbin/harvest-guest-agent`. The image controls its
links, so the script resolves `/sbin` and each directory it makes inside the
image, as a chroot would. No image link can take a root write to the host. Set
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

A jailed launcher pins its image at its start. It hard-links the kernel, the
root file system and the snapshot pair into `hsbx-<owner>-image` under its VM
root, and each launch links from there. So an image that lands one file at a
time never gives a launch a mixed pair. A new image takes effect at the next
worker start. An unjailed launcher reads the configured paths at each launch.

Publish a new image by rename, never by a write in place. A hard link shares
its inode, so `cp` onto `rootfs.ext4` also changes the pinned file. The pin
records the inode, the size and the modification time of each file. It also
holds an inotify watch for a write to each file. So it sees a `cp -p` that
keeps the size and the time. The watch starts before the stamps and the
checks, and each link must be a watched file. Each launch checks both, and a changed file
fails the launch as a config error until the worker restarts. A write during
a running microVM still reaches it, because the microVM reads its image while
it runs. So the backend checks the image again after each run. After a write,
the run returns a config error and no outcome. The watch needs one inotify instance per
launcher. A full `fs.inotify` limit fails the launcher start as a config error.

The pin holds a shared `flock` on each image directory while it links. The
script holds an exclusive `flock` on the output directory while it renames,
and so does a snapshot publish. So a launcher never pins half a publish. A
tool that publishes in another way must take the same lock. The script needs
`flock` from util-linux.

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
    machine: MachineSpec::default(),
    boot: BootMethod::ConfigFile,
    confinement: Confinement::Jailer(jail),
    guest_port: 52,
    api_timeout: Duration::from_secs(5),
})?;
launcher.verify_host(PosturePolicy::Enforce).await?;
launcher.sweep_stale();
let backend = FirecrackerBackend::new(Arc::new(launcher), BackendSettings::default())?
    .with_warm_pool(4)?;
let runtime = SandboxRuntime::new(Arc::new(backend), SandboxPolicy::default())?;
```

The backend takes the guest port from the launcher.

What each microVM gets:

- one vCPU and 2176 MiB by default, with `cpu.max`, `memory.max`,
  `memory.swap.max` and `pids.max` on its cgroup. That is the 2 GiB memory
  ceiling of the default policy plus a 128 MiB guest reserve
  (`GUEST_MEMORY_RESERVE_MIB`). Firecracker maps guest memory on first use,
  so the unused part costs no host memory. The runtime refuses a policy
  whose memory ceiling is above the microVM size less the reserve. The
  launcher refuses more than 1022 GiB (`MAX_GUEST_MEMORY_MIB`), which
  Firecracker cannot map;
- a read-only root drive, a vsock device and a virtio-rng device;
- no network device, no metadata service and no serial console;
- the kernel arguments `reboot=k panic=1 8250.nr_uarts=0`;
- an 8 GiB `fsize` limit on the VMM, by default. A golden snapshot writes
  the whole guest memory to one file, so `create_golden_snapshot` refuses a
  limit below the guest memory. The launcher refuses a limit below 4096
  bytes (`MIN_FSIZE_LIMIT`), because the jailer writes its pid file first;
- a `no-file` limit of 1024 on the VMM, by default. The launcher refuses a
  limit below 128 (`MIN_NOFILE_LIMIT`), because each launch would fail. It
  also refuses a limit above the host `fs.nr_open`, which `setrlimit` refuses.
  Without `CAP_SYS_RESOURCE`, the jailer cannot raise the hard limits that it
  inherits from the worker. Then the launcher also refuses a `no-file` or
  `fsize` limit above the worker hard limit. With no `no-file` limit, the
  jailer sets 2048.

`GuestImage::extra_boot_args` adds kernel arguments. The launcher refuses an
extra argument that replaces one of its own, and a word that starts with `-`.
The kernel gives `--` and the words after it to init, so such a word could
change the mode of the guest agent.
`harvest.work_mib=<n>` sets the size of the guest `/work` tmpfs, which is
256 MiB by default. The launcher refuses a value that is not a whole number
from 1 to `MAX_WORK_MIB`, so the byte count fits 64 bits. The runtime refuses
a policy whose largest input does not fit, with 64 KiB more for each file.

The whole line may hold at most 1791 bytes (`MAX_BOOT_ARGS_BYTES`).
Firecracker adds its own arguments and refuses a line over 2047 bytes at
each boot. So the launcher refuses a longer line as a config error.

Teardown kills the process group of the VMM, then the cgroup through
`cgroup.kill`, then removes the jail. The VMM is not reaped before the group
kill, so its pid still names its group. So an unjailed wrapper leaves no
helper in that group behind either. An unjailed helper that leaves the group,
for example with `setsid`, outlives the teardown. Only the cgroup of the
jailer holds every process. Teardown runs after success, error, timeout and
cancel. It also
runs when the caller drops the run future. The host never reads a pid from
the jail. Without `cgroup.kill`, teardown freezes the cgroup and waits until
it reports `frozen 1`. Only then does it send `SIGKILL` to each pid in it, so
no pid can name another process. A cgroup that is still there after the
teardown stays with the
launcher. The launcher kills and removes it again before each launch and in
each `sweep_stale`. Each microVM holds the launcher lease until its teardown
ends, so a teardown that outlives the launcher keeps its owner live. The lease
file stays while a cgroup is left, so a later sweep removes the cgroup.

Each launcher holds a lease file next to its VM directories. `sweep_stale`
removes the microVMs and the cgroups of each owner whose lease is not locked.
So a crashed worker leaves nothing behind after the next start. A sweep kills
only the cgroups of owners with a lease or a VM directory under its own VM
root. So launchers with other VM roots can share one cgroup parent. A dead
lease stays until the microVMs and the cgroups of its owner are gone. A scan
of the VM root or the cgroup parent that fails keeps each dead lease too. A
sweep skips the VMs of a live owner. So a launcher keeps each VM directory
that its teardown could not remove, and removes it again before each launch
and in each sweep.

The jailer puts each microVM cgroup under `JailerConfig::cgroup_parent`. That
is outside the cgroup of a systemd service by default. So a stop of the
service does not kill the microVMs, but the next `sweep_stale` does. To tie
them to the service, set `Delegate=yes`. Then set `cgroup_parent` to a cgroup
inside the service subtree. The worker process must then run in a leaf of that
subtree, because cgroup v2 keeps processes only in leaves. Each component of
`cgroup_parent` must fit a file name (255 bytes), and each VM cgroup file must
fit a path (4095 bytes).

### Speed

| Path | Time to a listening agent |
|------|---------------------------|
| Cold boot | About 125 ms in the Firecracker specification, plus guest init. |
| Warm pool | None: the pool boots ahead. Set `with_warm_pool(n)`, with `n` at most 1024 (`MAX_WARM_POOL`). |
| Snapshot | About 3 ms for the load call, then lazy page faults. |

`BootMethod::Snapshot` loads a golden snapshot for each run. It needs the
jailer, so each restore sees the same paths.
`FirecrackerLauncher::create_golden_snapshot` boots a golden microVM, waits
for its agent, pauses it and saves the files. No code runs in it. The files
hold the guest memory, so the golden microVM must hold no secret. The new
pair replaces an old pair only after both copies are complete, and a failed
rename puts the old file back. When that fails too, the old file stays under a
hidden backup name, and the error names it. The renames hold the directory
lock, so a launcher that starts meanwhile waits. A host crash between the two
renames can leave a mixed pair, so create the snapshot again after such a
crash.

A restored guest keeps the boot arguments and the machine of its snapshot,
such as the `harvest.port` of its agent. So the snapshot gets a manifest next
to its state file, `snapshot.state.json`, with these values. It also holds the
SHA-256 digests of the kernel and the root file system. A launcher with other
values, or a snapshot without a manifest, is a config error.

Clones of one snapshot share their user-space random state. Each run
therefore carries fresh host entropy, and VMGenID reseeds the kernel on Linux
5.18 and later. A restored microVM also starts with the clock of the
snapshot, and each run sets the clock again. Rebuild the snapshot on each
change of Firecracker, the host kernel or the CPU model. The manifest holds
all three. A launcher refuses a snapshot of another host kernel or CPU model
at its start, and of another Firecracker at its first version check.

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
  grace together. Each timeout counts up to `MAX_TIMEOUT`, one year, so a
  longer one, such as `Duration::MAX`, means no bound. A policy wall clock
  above it is a config error. The attempt time then fits the engine clock.
  The backend overhead is at least what the backend needs:
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
