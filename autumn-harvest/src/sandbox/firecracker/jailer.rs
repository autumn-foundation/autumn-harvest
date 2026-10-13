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
    /// The chroot base directory. It and each directory above it must belong
    /// to root, and only root may write them. The launcher checks this.
    pub chroot_base: PathBuf,
    /// The parent cgroup, relative to `cgroup_root`.
    pub cgroup_parent: String,
    /// The cgroup v2 mount point, for teardown. It must be the first cgroup
    /// v2 mount in `/proc/mounts`, which the jailer uses. The launcher
    /// checks this.
    pub cgroup_root: PathBuf,
    /// More cgroup values, on top of the CPU, memory and pid limits. Each
    /// key must be a plain controller file, such as `cpuset.cpus`. The
    /// launcher refuses a key that it sets itself, and a `cgroup.` core file.
    pub extra_cgroups: Vec<(String, String)>,
    /// A network namespace to join. `None` keeps the namespace of the jailer,
    /// and `verify_host` reports that as critical. The microVM has no network
    /// device either way. An empty namespace,
    /// such as one from `ip netns add`, also cuts the network of an escaped
    /// VMM. Give the path under `/run/netns`. The launcher refuses a symbolic
    /// link in the path, and `/var/run` is one on most hosts.
    pub netns: Option<PathBuf>,
    /// The `fsize` rlimit of the VMM, in bytes. It must hold a snapshot of
    /// the guest memory. The default is [`DEFAULT_FSIZE_LIMIT`].
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
            fsize_limit: Some(DEFAULT_FSIZE_LIMIT),
            nofile_limit: Some(1024),
        }
    }
}

/// The default `fsize` rlimit of the VMM: 8 GiB. A larger guest memory needs
/// a larger limit to write a snapshot.
pub const DEFAULT_FSIZE_LIMIT: u64 = 8 << 30;

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
/// values come first. The jailer writes the values in order, so a launcher
/// limit always wins.
#[must_use]
pub fn cgroup_values(config: &JailerConfig, machine: MachineSpec) -> Vec<(String, String)> {
    let mut values = config.extra_cgroups.clone();
    values.extend(launcher_cgroup_values(machine));
    values
}

/// The CPU, memory, swap and pid limits that the launcher sets.
fn launcher_cgroup_values(machine: MachineSpec) -> [(String, String); 4] {
    let cpu_quota = u64::from(machine.vcpu_count) * 100_000;
    let memory = (u64::from(machine.mem_size_mib) + VMM_MEMORY_OVERHEAD_MIB) * 1024 * 1024;
    [
        ("cpu.max".to_owned(), format!("{cpu_quota} 100000")),
        ("memory.max".to_owned(), memory.to_string()),
        ("pids.max".to_owned(), VMM_MAX_PIDS.to_string()),
        // Guest memory must not reach the swap device.
        ("memory.swap.max".to_owned(), "0".to_owned()),
    ]
}

/// Check that each extra cgroup value names a plain controller file and
/// leaves the launcher limits in place.
///
/// # Errors
///
/// [`SandboxError::Config`] for a key that is not a plain file name, a
/// `cgroup.` core file, or a key that [`cgroup_values`] sets.
pub fn check_extra_cgroups(config: &JailerConfig) -> Result<(), SandboxError> {
    let limits = launcher_cgroup_values(MachineSpec::default());
    for (key, value) in &config.extra_cgroups {
        // A slash, a space or `=` could reach another file or another value.
        let plain = key.contains('.')
            && !key.starts_with('.')
            && key
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'));
        let problem = if !plain {
            "is not a plain controller file name"
        } else if key.starts_with("cgroup.") {
            "is a cgroup core file, not a limit"
        } else if limits.iter().any(|(limit, _)| limit == key) {
            "would replace a launcher limit"
        } else if value.chars().any(char::is_control) {
            "has a control character in its value"
        } else {
            continue;
        };
        return Err(SandboxError::Config(format!(
            "the extra cgroup value {key:?} {problem}"
        )));
    }
    Ok(())
}

/// Check that `cgroup_parent` names a cgroup below `cgroup_root`.
///
/// The jailer takes it as a path relative to the hierarchy, and teardown
/// joins it to `cgroup_root`. An absolute path or `..` would leave the
/// hierarchy, so the parent must be plain path components.
///
/// # Errors
///
/// [`SandboxError::Config`] for a parent that can leave the hierarchy.
pub fn check_cgroup_parent(config: &JailerConfig) -> Result<(), SandboxError> {
    let parent = &config.cgroup_parent;
    let plain = !parent.is_empty()
        && !parent.chars().any(char::is_control)
        && Path::new(parent)
            .components()
            .all(|part| matches!(part, std::path::Component::Normal(_)));
    if plain {
        Ok(())
    } else {
        Err(SandboxError::Config(format!(
            "the cgroup parent {parent:?} must be a relative path of plain names below the \
             cgroup root"
        )))
    }
}

/// Check that `cgroup_root` is the cgroup v2 mount of the jailer.
///
/// # Errors
///
/// [`SandboxError::Config`] when it is not.
pub fn check_cgroup_root(config: &JailerConfig, mounts: &str) -> Result<(), SandboxError> {
    // The jailer creates each cgroup under the first cgroup v2 mount in
    // /proc/mounts. Teardown must kill that cgroup, not another path.
    let mount = mounts.lines().find_map(|line| {
        let mut fields = line.split(' ');
        let (_, dir, kind) = (fields.next()?, fields.next()?, fields.next()?);
        (kind == "cgroup2").then(|| unescape_mount_path(dir))
    });
    match mount {
        Some(mount) if mount == config.cgroup_root => Ok(()),
        Some(mount) => Err(SandboxError::Config(format!(
            "the jailer uses the cgroup v2 mount {}, so JailerConfig::cgroup_root must be it, \
             not {}",
            mount.display(),
            config.cgroup_root.display()
        ))),
        None => Err(SandboxError::Config(
            "the jailer needs cgroup v2, and the mount table has no cgroup2 mount".into(),
        )),
    }
}

/// A path from the mount table. The table writes some bytes, such as a
/// space, as a backslash and three octal digits.
fn unescape_mount_path(field: &str) -> PathBuf {
    use std::os::unix::ffi::OsStringExt as _;
    let bytes = field.as_bytes();
    let mut path = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while let Some(&byte) = bytes.get(i) {
        let escaped = bytes
            .get(i + 1..i + 4)
            .filter(|digits| byte == b'\\' && digits.iter().all(|d| (b'0'..=b'7').contains(d)))
            .and_then(|digits| std::str::from_utf8(digits).ok())
            .and_then(|digits| u8::from_str_radix(digits, 8).ok());
        if let Some(value) = escaped {
            path.push(value);
            i += 4;
        } else {
            path.push(byte);
            i += 1;
        }
    }
    PathBuf::from(std::ffi::OsString::from_vec(path))
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

    #[test]
    fn a_cgroup_parent_stays_below_the_cgroup_root() {
        for parent in ["", "/etc", "../escape", "a/../../b", "./a", "a\nb"] {
            let config = JailerConfig {
                cgroup_parent: parent.to_owned(),
                ..config()
            };
            assert!(
                matches!(check_cgroup_parent(&config), Err(SandboxError::Config(ref m)) if m.contains("cgroup parent")),
                "{parent:?}"
            );
        }
        for parent in ["harvest-sandbox", "workers/harvest-sandbox"] {
            let config = JailerConfig {
                cgroup_parent: parent.to_owned(),
                ..config()
            };
            check_cgroup_parent(&config).unwrap();
        }
    }

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
        assert_eq!(
            exec_name(Path::new("/opt/fc/firecracker")).unwrap(),
            "firecracker"
        );
        assert!(exec_name(Path::new("/")).is_err());
    }

    #[test]
    fn the_cgroup_values_bound_cpu_memory_and_pids() {
        let mut config = config();
        config
            .extra_cgroups
            .push(("cpuset.cpus".into(), "2-3".into()));
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
                ("cpuset.cpus".to_owned(), "2-3".to_owned()),
                ("cpu.max".to_owned(), "200000 100000".to_owned()),
                ("memory.max".to_owned(), (320u64 * 1024 * 1024).to_string()),
                ("pids.max".to_owned(), "64".to_owned()),
                ("memory.swap.max".to_owned(), "0".to_owned()),
            ]
        );
    }

    #[test]
    fn an_extra_cgroup_cannot_replace_a_launcher_limit() {
        for (key, value) in [
            ("memory.max", "max"),
            ("memory.swap.max", "max"),
            ("pids.max", "max"),
            ("cpu.max", "max 100000"),
            ("../memory.max", "1"),
            ("memory.max=max x", "1"),
            ("memory .max", "1"),
            ("cgroup.procs", "1"),
            ("", "1"),
            ("cpuset.cpus", "2-3\n"),
        ] {
            let mut config = config();
            config.extra_cgroups.push((key.into(), value.into()));
            let err = check_extra_cgroups(&config).expect_err(key);
            assert!(matches!(err, SandboxError::Config(_)), "{err:?}");
        }
        let mut config = config();
        config.extra_cgroups = vec![
            ("cpuset.cpus".into(), "2-3".into()),
            ("memory.high".into(), "200M".into()),
            ("hugetlb.2MB.max".into(), "0".into()),
            ("cpu.weight".into(), "50".into()),
        ];
        check_extra_cgroups(&config).unwrap();
    }

    #[test]
    fn the_cgroup_root_must_be_the_jailer_cgroup_mount() {
        let mounts = "proc /proc proc rw 0 0\n\
                      cgroup2 /sys/fs/cgroup/unified cgroup2 rw 0 0\n\
                      cgroup2 /mnt/other cgroup2 rw 0 0\n";
        let mut config = config();
        // The default path is not the mount that the jailer uses here.
        let err = check_cgroup_root(&config, mounts).expect_err("a hybrid host");
        assert!(
            matches!(err, SandboxError::Config(ref m) if m.contains("/sys/fs/cgroup/unified")),
            "{err:?}"
        );
        config.cgroup_root = PathBuf::from("/sys/fs/cgroup/unified/");
        check_cgroup_root(&config, mounts).unwrap();
        // The jailer takes the first cgroup v2 mount.
        config.cgroup_root = PathBuf::from("/mnt/other");
        assert!(check_cgroup_root(&config, mounts).is_err());
        // The mount table writes a space as \040.
        config.cgroup_root = PathBuf::from("/mnt/a b");
        check_cgroup_root(&config, "cgroup2 /mnt/a\\040b cgroup2 rw 0 0\n").unwrap();
        assert!(matches!(
            check_cgroup_root(&config, "proc /proc proc rw 0 0\n"),
            Err(SandboxError::Config(_))
        ));
    }

    #[test]
    fn the_launcher_limits_are_written_last() {
        let mut config = config();
        config
            .extra_cgroups
            .push(("memory.max".into(), "max".into()));
        let values = cgroup_values(&config, MachineSpec::default());
        let last = values.iter().rposition(|(key, _)| key == "memory.max");
        assert_eq!(
            last.map(|i| values[i].1.as_str()),
            Some(
                ((u64::from(MachineSpec::default().mem_size_mib) + VMM_MEMORY_OVERHEAD_MIB) << 20)
                    .to_string()
                    .as_str()
            )
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
            &[
                "--config-file".into(),
                "vm-config.json".into(),
                "--no-api".into(),
            ],
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
            "--cgroup",
            "memory.swap.max=0",
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
