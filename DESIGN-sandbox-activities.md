# Design — sandboxed code activities: many small lambdas for agents

The ask: give activities the power to run any code that an agent writes. Use
a sandbox that is stronger than the WASM activity runtime. Evaluate
Firecracker. No issue exists, so this record derives the acceptance criteria
from the research and the plan.

**No migration. No new `WorkflowEvent` variant. No worker change.** A sandbox
run is an ordinary activity. The feature is `sandbox-activities`, off by
default. It needs no database, and a default build gains no dependency. The
guest agent is a separate binary crate that only the guest image uses.

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
- CI builds `--all-features` on Linux, macOS and Windows. The default CI
  jobs have no `/dev/kvm`, and this development host has none either. A
  GitHub Linux runner can enable `/dev/kvm` with a udev rule.

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
| B14 | Network egress through a filtering proxy. | Deferred. The first version denies the network. `NetworkPolicy::Allow` works only on the process backend. A runtime refuses it on a backend with no network. |
| B15 | Hand-roll the Firecracker client instead of an SDK crate. | **Adopted.** The SDK crates are young or stale. About 150 lines of HTTP/1.1 over a Unix socket cover the API. |

### 0.3 Reverse brainstorm — how can this design do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | A guest declares a 4 GiB frame, and the host allocates it. | The reader checks the length against a cap before it reads the body, and it never preallocates. |
| R2 | Code floods stdout and fills host memory. | A capture keeps a fixed head and tail, counts the rest and drains the pipe. |
| R3 | Code prints a NUL byte, and the `jsonb` insert of the result fails. | The capture replaces NUL with U+FFFD. |
| R4 | Code leaves a background process that outlives the run. | The microVM dies after one run. In the process backend, the child is a session leader, and the executor kills the group at exit, timeout, cancel and drop. A process that calls `setsid` escapes the group there, and the guide says so. |
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
| R23 | The host runs a Firecracker or a jailer with a known escape CVE. | The first launch checks both versions and refuses one below the patched minimum, with or without `verify_host`. |
| R24 | The guest prints to the serial console without end. | The kernel arguments disable the serial port. |
| R25 | The sandbox run outlives the activity timeout, so a retry runs the code twice. | `start_to_close` is the queue wait, the wall-clock ceiling, the overhead and the cleanup grace. The runtime bounds the queue wait, takes the time left in the attempt, and does not start code that cannot finish in it. |
| R26 | An outcome is over the activity result cap. | The registration raises `max_result_bytes` to the policy outcome bound. |
| R27 | A guest sends a frame of junk arrays, and a tagged serde decoder copies it into a tree many times larger. | The host decodes a guest message and a run status field by field, and skips unknown fields without a copy. |
| R28 | A leftover process of the code uid plants a link in a new work directory, and the root executor writes through it. | The work directory belongs to the executor until the input files are in place. |
| R29 | Code hard-links a host file into an output path. | The executor skips an output with more than one hard link. |
| R30 | A microVM restored from a snapshot runs code with the clock of the snapshot. | The host sends its clock after the guest has decoded the request. The guest sets its clock before the code runs. |
| R31 | An operator runs an unjailed VMM or a writable shared image in production. | `verify_host` reports an unjailed launcher as critical. A jailed launcher refuses a shared image that the jail uid owns or can write. |

### 0.4 Six thinking hats

| Hat | Notes |
|-----|-------|
| White | Firecracker gives a guest kernel per run, in about 125 ms, for about 5 MiB. Snapshots restore in about 3 ms. The default CI jobs have no KVM. The engine already records activity input and output. |
| Red | Teams fear LLM code on a production host. They want "it just runs Python". They also fear a sandbox that is too slow to use in a loop. |
| Black | KVM is absent in the default CI jobs and in many cloud VMs. Building a kernel and a rootfs is operator work. The process backend is weak. A fake VMM must stand in for most tests. |
| Yellow | Any language, a real kernel boundary, and output that the agent can read. Replay never runs code again. The engine needs no change. |
| Green | A warm pool and snapshots for speed. One executor inside and outside the VM. A gVisor backend and an egress proxy later. Head-and-tail capture for tracebacks. |
| Blue | Ship the runtime, both backends, the guest agent, the activity and the agent tool. Prove each defense with a test. A dedicated workflow boots real microVMs on a runner with KVM. |

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

### 0.6 Review

Four review agents read the work: security, correctness and concurrency,
engine integration, and docs, tests and CI. The fixes changed these
decisions. R25, R27 to R31 and the new rows of §3 record them.

- The runtime owns the time budget of an attempt, not the registration
  alone.
- The host decodes guest data field by field, and bounds and cleans each
  guest string that can reach history.
- The version gate runs at the first launch, not only in `verify_host`.
- Tests that need root carry a `root_` prefix. The `Sandbox (KVM)`
  workflow runs them with sudo.
- The agent tool returns a bounded view, and its effect follows the
  isolation.
- A golden snapshot has a creator, `create_golden_snapshot`, and a KVM test.

---

## 1. Acceptance criteria

Section 3 lists the evidence for each criterion.

| AC | Statement |
|----|-----------|
| AC1 | The feature is off by default, adds no dependency to a default build, and builds on Linux, macOS and Windows. |
| AC2 | An agent submits code, a runtime, files, stdin, arguments, environment and output paths as JSON. Unknown fields are an error. |
| AC3 | The policy checks each request. A violation is a non-retryable denial that names the rule and the limit. |
| AC4 | The default policy denies the network and refuses a backend weaker than a microVM. |
| AC5 | A non-zero exit, a signal and a timeout are outcomes. Stdout and stderr keep a bounded head and tail, with byte counts and truncation flags. NUL never reaches history. |
| AC6 | Requested output files return, bounded. A symlink, a FIFO or a path outside the work directory never returns. |
| AC7 | Infrastructure faults are retryable. Denials and protocol violations are not. A cancel stops the run. |
| AC8 | The process executor clears the environment, runs in a private directory and applies rlimits. It makes the child a session leader and closes inherited descriptors. It resets the signal mask, sets no-new-privileges and kills the whole group. |
| AC9 | The process executor denies the network with an empty namespace, and it fails closed when it cannot create one. |
| AC10 | The wire protocol uses versioned, length-prefixed frames. It checks the cap before it reads a body, and it checks the run id. |
| AC11 | The guest agent answers a ping, runs one plan, reseeds its random pool from host entropy, and checks the plan again. |
| AC12 | The Firecracker config, the jailer arguments and the kernel arguments match the documented API: cgroup v2, a read-only root, vsock, entropy, no network, no serial. |
| AC13 | The API client speaks HTTP/1.1 over a Unix socket, bounds the response, and reports `fault_message`. |
| AC14 | The vsock connector sends `CONNECT` in one write and retries until the boot deadline. |
| AC15 | Each microVM serves one run. Teardown runs on success, error, timeout, cancel and drop, and it removes the jail and the cgroup. |
| AC16 | A warm pool keeps booted microVMs ready, hands out each one once, refills, and kills all at shutdown. |
| AC17 | A launcher creates a golden snapshot, and a snapshot boot loads it through the API. Each run carries fresh entropy and the host clock. |
| AC18 | The backend refuses a Firecracker version below the patched minimum, and it reports host posture risks. |
| AC19 | A runtime bounds the runs at one time and the wait for a slot. A cancel ends the wait. |
| AC20 | `HarvestBuilder::sandbox_activity` registers a normal activity. Its handler honors cancellation and the attempt deadline, and injects the idempotency key. The registration sets timeouts and the result cap from the policy. |
| AC21 | The agent crate offers a `Tool` that runs code through the runtime. Its effect follows the network policy and the isolation. |
| AC22 | A guide, this design record and the changelog fragment exist in ASD-STE100 style, and the comment hygiene gate passes. |
| AC23 | CI runs the new tests on every OS where they apply. A dedicated workflow runs the KVM tests on real microVMs, and the guide documents them. |

---

## 2. TDD map

Red: the author writes each test below first, and it fails against a stub.
Green: the implementation makes it pass. Refactor: the author cleans the
code while the tests stay green.

| Area | Tests | Proves |
|------|-------|--------|
| Policy | `sandbox::policy::tests` | AC3, AC4 |
| Request | `sandbox::request::tests` | AC2, AC3, R7, R9 |
| Outcome | `sandbox::outcome::tests` | AC5, R12 |
| Capture | `sandbox::capture::tests` | AC5, R2, R3 |
| Errors | `sandbox::error::tests` | AC7, R14 |
| Protocol | `sandbox::protocol::tests` | AC10, R1, R19 |
| Protocol decoders | `sandbox::protocol::tests`, `sandbox::outcome::tests` | R27 |
| Runtime | `sandbox::runtime::tests` | AC4, AC19, R11, R12, R17, R25 |
| Executor | `sandbox::exec::tests` | AC5, AC6, AC8, AC9, R4 to R8, R10, R20, R21, R28, R29 |
| Guest | `sandbox::guest::tests` | AC11, R16, R30 |
| Firecracker | `sandbox::firecracker::*::tests` | AC12 to AC18, R13, R15, R18, R22 to R24, R31 |
| Activity | `sandbox::activity::tests` | AC20, R25, R26 |
| Agent tool | `autumn-harvest-agent` `sandbox` tests | AC21 |
| Docs | `sandbox_activities_docs` guard suite | AC22 |
| Real microVMs | `sandbox::firecracker::kvm_tests`, ignored without KVM | AC12, AC15, AC16, AC17, AC23, R30 |

---

## 3. Evidence

Each row names the tests that prove the criterion. The guard
`each_acceptance_criterion_has_evidence_that_exists` checks that each named
test exists. CI runs each test in `test-nodb` on Linux, macOS and Windows,
or in the `Sandbox (KVM)` workflow.

| AC | Evidence |
|----|----------|
| AC1 | `sandbox-activities` is not a default feature, and it adds only `libc`. CI runs the guide commands on three operating systems: `each_guide_test_command_runs_in_ci`. The guest crate tests compile on each of them. |
| AC2 | `unknown_request_fields_are_rejected_by_serde`, `a_python_request_becomes_a_plan_with_the_code_file`, `base64_files_are_decoded_and_bad_base64_is_denied`, `every_schema_field_is_a_request_field`. |
| AC3 | `an_unknown_runtime_is_denied_and_names_the_allowed_ones`, `a_limit_over_the_ceiling_is_denied_and_names_the_ceiling`, `traversal_and_absolute_paths_are_denied`, `too_many_files_args_env_or_outputs_are_denied`, `bad_input_is_a_denial`, `a_plan_that_names_an_output_twice_is_denied`, `exec_strings_past_the_total_limit_are_a_config_error`. |
| AC4 | `the_default_policy_requires_a_microvm_and_denies_the_network`, `the_default_policy_refuses_a_process_backend`, `a_policy_that_allows_the_network_needs_a_backend_with_one`. |
| AC5 | `a_timeout_kills_the_run_and_is_an_outcome`, `a_python_traceback_is_an_outcome_not_a_failure`, `a_long_stream_keeps_the_head_and_the_tail`, `a_truncated_stream_cuts_on_character_boundaries`, `a_nul_byte_never_reaches_the_outcome`, `a_nul_in_a_skip_reason_breaks_the_bounds`. |
| AC6 | `requested_outputs_are_returned`, `a_symlinked_output_is_skipped`, `a_hard_linked_output_is_skipped`, `a_fifo_output_is_skipped_without_blocking`, `the_total_output_budget_is_enforced`. |
| AC7 | `a_denial_and_a_config_error_are_not_retryable`, `an_unavailable_backend_is_retryable`, `a_protocol_violation_is_not_retryable`, `a_nul_in_an_error_does_not_reach_history`, `a_nul_in_an_error_does_not_reach_the_model`, `a_nul_in_the_vmm_stderr_does_not_reach_the_report`, `an_activity_cancel_stops_the_run`, `a_cancel_kills_the_run`, `a_guest_that_does_not_take_the_request_is_unavailable`, `a_slow_request_transfer_takes_no_time_from_the_outcome_budget`. |
| AC8 | `the_child_sees_no_host_environment`, `the_work_directory_is_private_and_removed`, `the_cpu_limit_ends_a_spinning_run`, `the_child_leads_its_own_session`, `an_inherited_descriptor_does_not_reach_the_child`, `the_signal_mask_is_reset`, `no_new_privileges_is_set`, `a_background_process_is_killed_when_the_main_process_exits`, `the_cleanup_finds_a_work_directory_that_the_code_moved`, `the_cleanup_finds_a_work_directory_under_a_locked_directory`, `the_work_directory_parent_ignores_the_umask`, `root_a_forking_run_cannot_pass_its_cpu_budget`, `root_the_run_cgroup_kills_a_process_that_left_the_group`, `root_the_run_cgroup_bounds_the_memory`, `a_run_cgroup_that_keeps_a_process_fails_the_run`, `a_run_cgroup_that_cannot_be_checked_fails_the_run`, `the_descriptor_walk_fails_closed_when_a_flag_update_fails`, `the_cpu_poll_shrinks_with_the_budget_left_and_the_cpus`, `the_main_process_stays_unreaped_until_the_group_is_killed`, `the_cpu_watch_fails_closed_when_cpu_stat_is_unreadable`, `a_lost_cpu_time_is_a_retryable_failure`, `the_wait_without_a_pidfd_does_not_reap_the_main_process`. On KVM: `a_forking_run_cannot_pass_its_cpu_budget`, `a_run_cannot_pass_its_memory_limit`. |
| AC9 | `the_network_is_denied_or_the_run_fails_closed`. In a real microVM: `a_microvm_has_no_network`. |
| AC10 | `a_frame_is_a_big_endian_length_and_json`, `an_oversized_declared_length_is_rejected_before_the_body`, `a_wrong_version_is_malformed`, `the_decoder_stops_at_the_output_entry_cap`, `a_policy_cannot_allow_more_outputs_than_an_outcome_holds`, `a_wrong_run_id_is_a_protocol_violation`, `a_guest_message_decodes_in_any_key_order_and_skips_unknown_fields`, `a_malformed_reason_is_bounded_and_has_no_nul`. |
| AC11 | `a_ping_gets_a_pong`, `a_run_sets_the_clock_after_the_reseed_and_before_the_code`, `the_clock_is_taken_after_the_request_transfer`, `an_outcome_before_the_start_is_a_protocol_violation`, `a_start_without_a_run_is_refused`, `a_failed_reseed_stops_the_run`, `a_plan_that_fails_the_guest_check_is_a_protocol_violation`, `the_agent_serves_pings_then_one_run_then_stops`. |
| AC12 | `the_config_file_has_the_documented_shape`, `the_config_has_no_network_and_no_metadata_service`, `the_boot_args_stop_on_reboot_drop_serial_and_start_the_agent`, `the_default_machine_fits_the_default_policy`, `the_launcher_reports_the_memory_a_run_can_use`, `a_memory_ceiling_above_the_backend_is_a_config_error`, `an_extra_boot_arg_cannot_reach_the_guest_agent`, `the_arguments_use_cgroup_v2_and_pass_firecracker_args_last`, `an_extra_cgroup_cannot_replace_a_launcher_limit`, `a_cgroup_parent_stays_below_the_cgroup_root`, `the_init_path_is_one_kernel_word`. A real boot: `the_api_boot_runs_code`. |
| AC13 | `a_put_sends_one_request_with_a_content_length_body`, `the_api_boot_is_bounded_as_a_whole`, `an_error_status_reports_the_fault_message`, `an_oversized_body_is_refused`, `a_server_that_never_answers_times_out`. |
| AC14 | `the_handshake_hands_back_a_raw_stream`, `a_closed_attempt_is_retried_until_the_guest_listens`, `garbage_and_endless_replies_never_succeed`, `a_stopped_vmm_ends_the_wait_at_once`, `the_wildcard_vsock_port_is_a_config_error`, `a_wildcard_guest_port_is_a_config_error`, `the_backend_asks_the_launcher_for_its_port_once`. |
| AC15 | `a_run_boots_a_vm_runs_the_code_and_tears_the_vm_down`, `dropping_the_run_tears_the_vm_down`, `the_sweep_removes_only_the_vms_of_dead_owners`, `the_sweep_kills_the_cgroups_of_dead_owners`, `a_cgroup_that_outlives_the_teardown_is_tried_again`, `a_launch_keeps_the_image_of_the_launcher_start`, `a_launcher_does_not_pin_half_a_publish`, `a_lock_that_stays_held_ends_the_wait`, `a_missing_cgroup_ends_the_kill_at_once`, `the_sweep_leaves_the_cgroups_of_a_launcher_with_another_vm_root`, `a_dead_lease_stays_until_its_cgroups_are_gone`, `a_cgroup_scan_that_fails_keeps_the_dead_leases`, `a_launcher_dropped_with_leftover_cgroups_keeps_its_lease`, `a_cgroup_that_cannot_be_checked_is_kept`, `a_cgroup_that_cannot_be_checked_is_not_gone`, `a_vm_that_outlives_its_launcher_keeps_the_lease`, `the_sweep_keeps_the_vms_of_an_owner_whose_lease_it_cannot_open`, `a_cgroup_without_cgroup_kill_has_each_process_killed`, `the_fallback_signals_no_pid_before_the_freeze_holds`, `the_cgroup_root_must_be_the_jailer_cgroup_mount`, `a_relative_launcher_path_is_a_config_error`, `the_launch_budget_covers_the_version_probes_and_the_api_boot`. On KVM: `a_microvm_times_out_and_is_torn_down`, `root_jailed_microvm_runs_code_and_removes_the_jail`. |
| AC16 | `each_vm_is_handed_out_once_and_the_pool_refills`, `a_guest_that_hangs_after_the_handshake_does_not_stop_the_refill`, `a_warm_vm_that_died_in_the_pool_is_replaced`, `the_teardown_of_a_dead_warm_vm_takes_no_run_time`, `shutdown_tears_down_a_vm_that_is_still_booting`. On KVM: `a_warm_pool_serves_each_run_from_a_new_microvm`. |
| AC17 | `a_snapshot_load_resumes_from_a_file_backend`, `root_golden_snapshot_boots_pings_pauses_and_copies_the_files_out`, `a_failed_snapshot_copy_keeps_the_old_pair`, `a_golden_snapshot_needs_an_api_timeout`, `a_golden_snapshot_needs_a_boot_timeout`, `a_snapshot_launch_keeps_the_pair_of_the_launcher_start`, `a_snapshot_publish_waits_for_a_pin`, `a_failed_second_rename_keeps_the_old_pair`, `a_rename_that_fails_puts_back_the_targets_that_moved`, `each_job_gets_a_fresh_run_id_and_fresh_entropy`. On KVM: `root_snapshot_boots_get_fresh_entropy_and_the_host_clock`. |
| AC18 | `versions_with_known_escapes_are_refused`, `a_launch_refuses_a_firecracker_with_a_known_escape`, `each_weak_setting_is_found`, `an_unreadable_cpu_vulnerability_state_is_critical`, `an_unreadable_ksm_state_is_critical`, `an_unknown_nested_kvm_state_is_critical`, `a_jail_base_that_others_can_write_is_critical`, `the_cgroup_check_reads_the_configured_mount`, `a_shared_image_that_others_can_write_is_a_config_error`, `an_image_on_another_file_system_is_a_config_error`, `a_launcher_path_that_is_not_utf8_is_a_config_error`. |
| AC19 | `runs_at_one_time_never_exceed_the_bound`, `a_cancel_while_waiting_for_a_slot_returns_cancelled`, `a_full_runtime_is_unavailable_after_the_queue_wait`. |
| AC20 | `the_builder_registers_the_activity_and_its_runtime`, `the_info_sets_bounds_from_the_policy`, `the_code_gets_the_idempotency_key_and_the_attempt`, `an_attempt_deadline_too_close_for_the_run_is_a_retryable_failure`, `the_runtime_gives_a_run_the_overhead_of_its_backend`, `oversized_engine_variables_are_a_config_error`, `the_request_bound_covers_the_engine_variables`, `the_info_carries_the_routing_options`. |
| AC21 | `the_tool_is_named_run_code_and_describes_itself`, `the_effect_follows_the_network_and_the_isolation`, `a_failing_program_is_a_result_not_a_tool_error`, `the_view_keeps_the_status_and_stderr_within_the_harness_cut`. |
| AC22 | `the_pages_keep_sentences_short`, `the_guide_has_a_failure_row_per_error_type`, `the_guide_names_the_enforced_firecracker_minimums`. The comment hygiene gate passes. |
| AC23 | `guards_run_on_docs_only_changes`, `each_guide_test_command_runs_in_ci`, `the_image_script_pins_its_default_kernel_by_digest`, `the_image_script_refuses_a_kernel_url_without_a_digest`, `a_failed_image_build_keeps_the_old_image`, `a_failed_publish_puts_back_the_old_image`, `a_publish_waits_for_a_launcher_that_pins`. The `Sandbox (KVM)` workflow runs `a_microvm_runs_python_and_returns_an_output_file` and the other KVM tests. |
