//! The jailer command line and the jail layout.
//!
//! The jailer runs as root. It creates the chroot
//! `<chroot_base>/<exec name>/<id>/root`, creates a cgroup, drops to the
//! VM uid and gid, and then runs Firecracker inside the chroot.

use std::path::{Path, PathBuf};

use super::config::MachineSpec;
use crate::sandbox::error::SandboxError;

/// The default chroot base of the jailer.
pub const DEFAULT_CHROOT_BASE: &str = "/srv/jailer";

/// The default parent cgroup of each VM cgroup.
pub const DEFAULT_CGROUP_PARENT: &str = "harvest-sandbox";

/// The cgroup v2 mount point.
pub const DEFAULT_CGROUP_ROOT: &str = "/sys/fs/cgroup";

/// Memory that the VMM needs on top of the guest memory, in MiB.
pub const VMM_MEMORY_OVERHEAD_MIB: u64 = 64;

/// Processes and threads that one Firecracker process may have.
pub const VMM_MAX_PIDS: u32 = 64;

/// How to run Firecracker under the jailer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JailerConfig {
    /// The jailer binary.
    pub jailer_bin: PathBuf,
    /// The uid of the VMM process.
    pub uid: u32,
    /// The gid of the VMM process.
    pub gid: u32,
    /// The chroot base directory. It must be root-owned and not writable by
    /// other users.
    pub chroot_base: PathBuf,
    /// The parent cgroup, relative to `cgroup_root`.
    pub cgroup_parent: String,
    /// The cgroup v2 mount point, for teardown.
    pub cgroup_root: PathBuf,
    /// More cgroup values, on top of the CPU, memory and pid limits.
    pub extra_cgroups: Vec<(String, String)>,
    /// A network namespace to join. `None` keeps the namespace of the jailer.
    pub netns: Option<PathBuf>,
    /// The `fsize` rlimit of the VMM, in bytes.
    pub fsize_limit: Option<u64>,
    /// The `no-file` rlimit of the VMM.
    pub nofile_limit: Option<u64>,
}

impl JailerConfig {
    /// A jailer config with the default directories.
    #[must_use]
    pub fn new(jailer_bin: impl Into<PathBuf>, uid: u32, gid: u32) -> Self {
        Self {
            jailer_bin: jailer_bin.into(),
            uid,
            gid,
            chroot_base: PathBuf::from(DEFAULT_CHROOT_BASE),
            cgroup_parent: DEFAULT_CGROUP_PARENT.to_owned(),
            cgroup_root: PathBuf::from(DEFAULT_CGROUP_ROOT),
            extra_cgroups: Vec::new(),
            netns: None,
            fsize_limit: None,
            nofile_limit: Some(1024),
        }
    }
}

/// The file name of the Firecracker binary. The jailer uses it as a
/// directory name.
///
/// # Errors
///
/// Returns [`SandboxError::Config`] for a path without a UTF-8 file name.
pub fn exec_name(firecracker_bin: &Path) -> Result<String, SandboxError> {
    firecracker_bin
        .file_name()
        .and_then(|name| name.to_str())
        .map(str::to_owned)
        .ok_or_else(|| {
            SandboxError::Config(format!(
                "the Firecracker path {} has no UTF-8 file name",
                firecracker_bin.display()
            ))
        })
}

/// The jail directory of one VM: `<chroot_base>/<exec name>/<id>`.
#[must_use]
pub fn jail_dir(config: &JailerConfig, exec_name: &str, vm_id: &str) -> PathBuf {
    config.chroot_base.join(exec_name).join(vm_id)
}

/// The chroot root of one VM: `<jail dir>/root`.
#[must_use]
pub fn chroot_root(config: &JailerConfig, exec_name: &str, vm_id: &str) -> PathBuf {
    jail_dir(config, exec_name, vm_id).join("root")
}

/// The cgroup directory of one VM: `<cgroup_root>/<parent>/<id>`.
#[must_use]
pub fn cgroup_dir(config: &JailerConfig, vm_id: &str) -> PathBuf {
    config.cgroup_root.join(&config.cgroup_parent).join(vm_id)
}

/// The cgroup values for a VM of size `machine`.
///
/// `cpu.max` grants one CPU per vCPU. `memory.max` is the guest memory plus
/// [`VMM_MEMORY_OVERHEAD_MIB`]. `pids.max` is [`VMM_MAX_PIDS`]. The extra
/// values follow.
#[must_use]
pub fn cgroup_values(config: &JailerConfig, machine: MachineSpec) -> Vec<(String, String)> {
    let cpu_quota = u64::from(machine.vcpu_count) * 100_000;
    let memory = (u64::from(machine.mem_size_mib) + VMM_MEMORY_OVERHEAD_MIB) * 1024 * 1024;
    let mut values = vec![
        ("cpu.max".to_owned(), format!("{cpu_quota} 100000")),
        ("memory.max".to_owned(), memory.to_string()),
        ("pids.max".to_owned(), VMM_MAX_PIDS.to_string()),
    ];
    values.extend(config.extra_cgroups.iter().cloned());
    values
}

/// The jailer arguments that run `firecracker_bin` as `vm_id`.
///
/// The arguments after `--` go to Firecracker. The jailer always runs with
/// cgroup v2, never daemonizes, and stays the parent of Firecracker, so the
/// host keeps a handle on the process.
#[must_use]
pub fn jailer_args(
    config: &JailerConfig,
    firecracker_bin: &Path,
    vm_id: &str,
    machine: MachineSpec,
    firecracker_args: &[String],
) -> Vec<String> {
    let mut args = vec![
        "--id".to_owned(),
        vm_id.to_owned(),
        "--exec-file".to_owned(),
        firecracker_bin.display().to_string(),
        "--uid".to_owned(),
        config.uid.to_string(),
        "--gid".to_owned(),
        config.gid.to_string(),
        "--chroot-base-dir".to_owned(),
        config.chroot_base.display().to_string(),
        "--cgroup-version".to_owned(),
        "2".to_owned(),
        "--parent-cgroup".to_owned(),
        config.cgroup_parent.clone(),
    ];
    for (key, value) in cgroup_values(config, machine) {
        args.push("--cgroup".to_owned());
        args.push(format!("{key}={value}"));
    }
    if let Some(nofile) = config.nofile_limit {
        args.push("--resource-limit".to_owned());
        args.push(format!("no-file={nofile}"));
    }
    if let Some(fsize) = config.fsize_limit {
        args.push("--resource-limit".to_owned());
        args.push(format!("fsize={fsize}"));
    }
    if let Some(netns) = &config.netns {
        args.push("--netns".to_owned());
        args.push(netns.display().to_string());
    }
    args.push("--".to_owned());
    args.extend(firecracker_args.iter().cloned());
    args
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn config() -> JailerConfig {
        JailerConfig::new("/usr/bin/jailer", 10_000, 10_000)
    }

    #[test]
    fn the_jail_layout_follows_the_jailer() {
        let config = config();
        assert_eq!(
            jail_dir(&config, "firecracker", "hsbx-a-1"),
            PathBuf::from("/srv/jailer/firecracker/hsbx-a-1")
        );
        assert_eq!(
            chroot_root(&config, "firecracker", "hsbx-a-1"),
            PathBuf::from("/srv/jailer/firecracker/hsbx-a-1/root")
        );
        assert_eq!(
            cgroup_dir(&config, "hsbx-a-1"),
            PathBuf::from("/sys/fs/cgroup/harvest-sandbox/hsbx-a-1")
        );
    }

    #[test]
    fn the_exec_name_is_the_file_name() {
        assert_eq!(exec_name(Path::new("/opt/fc/firecracker")).unwrap(), "firecracker");
        assert!(exec_name(Path::new("/")).is_err());
    }

    #[test]
    fn the_cgroup_values_bound_cpu_memory_and_pids() {
        let mut config = config();
        config.extra_cgroups.push(("cpuset.cpus".into(), "2-3".into()));
        let values = cgroup_values(
            &config,
            MachineSpec {
                vcpu_count: 2,
                mem_size_mib: 256,
            },
        );
        assert_eq!(
            values,
            [
                ("cpu.max".to_owned(), "200000 100000".to_owned()),
                ("memory.max".to_owned(), (320u64 * 1024 * 1024).to_string()),
                ("pids.max".to_owned(), "64".to_owned()),
                ("cpuset.cpus".to_owned(), "2-3".to_owned()),
            ]
        );
    }

    #[test]
    fn the_arguments_use_cgroup_v2_and_pass_firecracker_args_last() {
        let mut config = config();
        config.netns = Some(PathBuf::from("/var/run/netns/vm1"));
        config.fsize_limit = Some(1 << 30);
        let args = jailer_args(
            &config,
            Path::new("/opt/fc/firecracker"),
            "hsbx-a-1",
            MachineSpec::default(),
            &["--config-file".into(), "vm-config.json".into(), "--no-api".into()],
        );
        let expected: Vec<String> = [
            "--id",
            "hsbx-a-1",
            "--exec-file",
            "/opt/fc/firecracker",
            "--uid",
            "10000",
            "--gid",
            "10000",
            "--chroot-base-dir",
            "/srv/jailer",
            "--cgroup-version",
            "2",
            "--parent-cgroup",
            "harvest-sandbox",
            "--cgroup",
            "cpu.max=100000 100000",
            "--cgroup",
            "memory.max=603979776",
            "--cgroup",
            "pids.max=64",
            "--resource-limit",
            "no-file=1024",
            "--resource-limit",
            "fsize=1073741824",
            "--netns",
            "/var/run/netns/vm1",
            "--",
            "--config-file",
            "vm-config.json",
            "--no-api",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect();
        assert_eq!(args, expected);
    }

    #[test]
    fn the_jailer_never_daemonizes() {
        let args = jailer_args(
            &config(),
            Path::new("/opt/fc/firecracker"),
            "hsbx-a-1",
            MachineSpec::default(),
            &[],
        );
        assert!(!args.iter().any(|a| a == "--daemonize"));
        assert!(!args.iter().any(|a| a == "--new-pid-ns"));
        assert_eq!(args.last().map(String::as_str), Some("--"));
    }
}
