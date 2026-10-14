//! The Firecracker VM configuration document and the guest kernel arguments.
//!
//! The same parts serve two boot methods. `--config-file` reads them as one
//! document with kebab-case keys. The API takes each part in its own `PUT`.
//! Firecracker rejects unknown fields, so each optional field is omitted when
//! it is empty.

use std::path::PathBuf;

use serde::Serialize;

use crate::sandbox::error::SandboxError;

/// The vsock port of the guest agent.
pub const GUEST_AGENT_PORT: u32 = 52;

/// The guest context id. Firecracker maps vsock per device, so each microVM
/// can use the same id.
pub const GUEST_CID: u32 = 3;

/// The vsock socket, relative to the working directory of Firecracker.
pub const VSOCK_SOCKET: &str = "v.sock";

/// The kernel file name in the VM directory.
pub const KERNEL_FILE: &str = "vmlinux";

/// The root file system file name in the VM directory.
pub const ROOTFS_FILE: &str = "rootfs.ext4";

/// The configuration document name in the VM directory.
pub const CONFIG_FILE: &str = "vm-config.json";

/// The guest agent path in the root file system. The kernel runs it as init.
pub const GUEST_INIT: &str = "/sbin/harvest-guest-agent";

/// The prefix of each VM id that this crate creates.
pub const VM_ID_PREFIX: &str = "hsbx-";

/// The suffix of the directory where a launcher pins its image, after
/// `hsbx-<owner>`.
pub const PINNED_IMAGE_SUFFIX: &str = "-image";

/// The suffix of a directory where a launcher keeps the VMM binaries that a
/// version check passed, after a VM id of its owner.
pub const VMM_COPIES_SUFFIX: &str = "-vmm";

/// The root drive id. Firecracker resource ids reject `-`.
pub const ROOT_DRIVE_ID: &str = "rootfs";

/// The guest memory that the guest kernel and the guest agent keep. A run
/// gets the rest of the microVM.
pub const GUEST_MEMORY_RESERVE_MIB: u32 = 128;

/// The size of one microVM.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MachineSpec {
    /// Virtual CPUs, 1 to 32.
    pub vcpu_count: u8,
    /// Guest memory, in MiB.
    pub mem_size_mib: u32,
}

impl Default for MachineSpec {
    /// One vCPU, and the 2 GiB memory ceiling of the default policy plus
    /// [`GUEST_MEMORY_RESERVE_MIB`]. Firecracker maps guest memory on first
    /// use, so the unused part costs no host memory.
    fn default() -> Self {
        Self {
            vcpu_count: 1,
            mem_size_mib: 2048 + GUEST_MEMORY_RESERVE_MIB,
        }
    }
}

impl MachineSpec {
    /// The memory that one run can use: the guest memory less
    /// [`GUEST_MEMORY_RESERVE_MIB`].
    #[must_use]
    pub const fn run_memory_bytes(&self) -> u64 {
        (self.mem_size_mib.saturating_sub(GUEST_MEMORY_RESERVE_MIB) as u64) << 20
    }
}

/// The kernel and root file system that each microVM boots.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GuestImage {
    /// An uncompressed `vmlinux` kernel on the host.
    pub kernel: PathBuf,
    /// An ext4 root file system on the host. The microVM opens it read-only.
    pub rootfs: PathBuf,
    /// The init path in the root file system: one word with no quote.
    pub init: String,
    /// More kernel arguments. An argument may not use a key that the
    /// launcher sets, such as `init` or `harvest.port`, and it may not hold
    /// a double quote.
    pub extra_boot_args: Vec<String>,
    /// A name for the image, such as a digest. Outcomes report it.
    pub name: Option<String>,
}

impl GuestImage {
    /// An image from a kernel and a root file system, with the default init.
    #[must_use]
    pub fn new(kernel: impl Into<PathBuf>, rootfs: impl Into<PathBuf>) -> Self {
        Self {
            kernel: kernel.into(),
            rootfs: rootfs.into(),
            init: GUEST_INIT.to_owned(),
            extra_boot_args: Vec::new(),
            name: None,
        }
    }

    /// Name the image, for the outcomes.
    #[must_use]
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }
}

/// The `boot-source` part.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BootSource {
    /// The kernel path, as Firecracker sees it.
    pub kernel_image_path: String,
    /// The kernel command line.
    pub boot_args: String,
}

/// One entry of the `drives` part.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Drive {
    /// The drive id. No `-`.
    pub drive_id: String,
    /// The image path, as Firecracker sees it.
    pub path_on_host: String,
    /// Whether this drive is the root device.
    pub is_root_device: bool,
    /// Whether the guest sees the drive as read-only.
    pub is_read_only: bool,
}

/// The `machine-config` part.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MachineConfig {
    /// Virtual CPUs.
    pub vcpu_count: u8,
    /// Guest memory, in MiB.
    pub mem_size_mib: u32,
    /// Simultaneous multithreading. Always off.
    pub smt: bool,
}

/// The `vsock` part.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Vsock {
    /// The guest context id.
    pub guest_cid: u32,
    /// The host socket path, as Firecracker sees it.
    pub uds_path: String,
}

/// The `entropy` part: one virtio-rng device with no rate limiter.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct Entropy {}

/// The whole `--config-file` document.
///
/// It has no `network-interfaces`, `mmds-config`, `balloon` or `logger`
/// part, so the microVM has no network and no metadata service.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct VmConfigFile {
    /// The kernel and its command line.
    #[serde(rename = "boot-source")]
    pub boot_source: BootSource,
    /// The read-only root drive.
    pub drives: Vec<Drive>,
    /// The size of the microVM.
    #[serde(rename = "machine-config")]
    pub machine_config: MachineConfig,
    /// The vsock device of the guest agent.
    pub vsock: Vsock,
    /// The virtio-rng device.
    pub entropy: Entropy,
}

/// Where the VM files are, as Firecracker sees them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VmPaths {
    /// The kernel path.
    pub kernel: String,
    /// The root file system path.
    pub rootfs: String,
    /// The vsock socket path.
    pub vsock: String,
}

impl VmPaths {
    /// The paths inside a jail: plain names in the chroot root.
    #[must_use]
    pub fn jailed() -> Self {
        Self {
            kernel: KERNEL_FILE.to_owned(),
            rootfs: ROOTFS_FILE.to_owned(),
            vsock: VSOCK_SOCKET.to_owned(),
        }
    }
}

/// The guest kernel command line.
///
/// - `reboot=k` makes a guest reboot stop Firecracker, and `panic=1` turns a
///   panic into a reboot.
/// - `8250.nr_uarts=0` removes the serial console, so the guest cannot flood
///   the host with console output.
/// - `init=` starts the guest agent as process 1, and `harvest.port=` names
///   its vsock port.
#[must_use]
pub fn boot_args(image: &GuestImage, port: u32) -> String {
    let mut args = launcher_boot_args(image, port);
    args.extend(image.extra_boot_args.iter().cloned());
    args.join(" ")
}

/// The boot arguments that the launcher sets, before the extra ones.
fn launcher_boot_args(image: &GuestImage, port: u32) -> Vec<String> {
    vec![
        "reboot=k".to_owned(),
        "panic=1".to_owned(),
        "nomodule".to_owned(),
        "8250.nr_uarts=0".to_owned(),
        "i8042.noaux".to_owned(),
        "i8042.nomux".to_owned(),
        "i8042.dumbkbd".to_owned(),
        "swiotlb=noforce".to_owned(),
        "quiet".to_owned(),
        "loglevel=1".to_owned(),
        format!("init={}", image.init),
        format!("harvest.port={port}"),
    ]
}

/// Check that the init path is one kernel word. Check also that no extra
/// boot argument replaces an argument of the launcher or reaches the guest
/// agent as an option.
///
/// # Errors
///
/// Returns [`SandboxError::Config`] that names the argument.
pub fn check_boot_args(image: &GuestImage) -> Result<(), SandboxError> {
    // The kernel splits the line at whitespace and removes double quotes.
    // So a space or a quote can add an argument, such as a second `init=`.
    // The line also ends at a NUL, so no argument may hold a control
    // character.
    let unsafe_char = |c: char| c == '"' || c.is_control();
    let unsafe_word = |word: &str| word.chars().any(|c| unsafe_char(c) || c.is_whitespace());
    if image.init.is_empty() || unsafe_word(&image.init) {
        return Err(SandboxError::Config(format!(
            "the init path {:?} must be one word with no quote and no control character",
            image.init
        )));
    }
    if let Some(arg) = image
        .extra_boot_args
        .iter()
        .find(|arg| arg.chars().any(unsafe_char))
    {
        return Err(SandboxError::Config(format!(
            "the extra boot argument {arg:?} has a quote or a control character"
        )));
    }
    // The kernel and the guest agent take the last value of a key. So an
    // extra argument with a launcher key would win, for example the port.
    let key = |arg: &str| arg.split_once('=').map_or(arg, |(key, _)| key).to_owned();
    let reserved: Vec<String> = launcher_boot_args(image, 0)
        .iter()
        .map(|arg| key(arg))
        .collect();
    // The launcher joins the arguments with spaces, so check each word.
    for arg in image
        .extra_boot_args
        .iter()
        .flat_map(|arg| arg.split_whitespace())
    {
        // The kernel gives `--`, each word after it and each unknown word
        // to init. No kernel parameter starts with `-`, so such a word is
        // an option of the guest agent, which could change its mode.
        if arg.starts_with('-') {
            return Err(SandboxError::Config(format!(
                "the extra boot argument {arg:?} would reach the guest agent as an option"
            )));
        }
        let name = key(arg);
        if reserved.contains(&name) {
            return Err(SandboxError::Config(format!(
                "the extra boot argument {arg:?} would replace the launcher argument {name:?}"
            )));
        }
    }
    Ok(())
}

/// Build the VM document for `image` at `paths`.
#[must_use]
pub fn vm_config(
    image: &GuestImage,
    machine: MachineSpec,
    paths: &VmPaths,
    port: u32,
) -> VmConfigFile {
    VmConfigFile {
        boot_source: BootSource {
            kernel_image_path: paths.kernel.clone(),
            boot_args: boot_args(image, port),
        },
        drives: vec![Drive {
            drive_id: ROOT_DRIVE_ID.to_owned(),
            path_on_host: paths.rootfs.clone(),
            is_root_device: true,
            is_read_only: true,
        }],
        machine_config: MachineConfig {
            vcpu_count: machine.vcpu_count,
            mem_size_mib: machine.mem_size_mib,
            smt: false,
        },
        vsock: Vsock {
            guest_cid: GUEST_CID,
            uds_path: paths.vsock.clone(),
        },
        entropy: Entropy {},
    }
}

/// Check a VM id: 1 to 64 bytes of ASCII letters, digits or `-`.
///
/// # Errors
///
/// Returns [`SandboxError::Config`] that names the id.
pub fn validate_vm_id(id: &str) -> Result<(), SandboxError> {
    let valid =
        (1..=64).contains(&id.len()) && id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-');
    if valid {
        Ok(())
    } else {
        Err(SandboxError::Config(format!(
            "VM id {id:?} is not 1 to 64 bytes of ASCII letters, digits or '-'"
        )))
    }
}

/// A new VM id: [`VM_ID_PREFIX`], the owner tag, `-`, then 16 hex digits.
///
/// The id is short on purpose. It is part of each socket path, and a Unix
/// socket path holds at most [`MAX_SOCKET_PATH_BYTES`] bytes.
#[must_use]
pub fn new_vm_id(owner: &str) -> String {
    let random = uuid::Uuid::new_v4().simple().to_string();
    format!("{VM_ID_PREFIX}{owner}-{}", &random[..VM_ID_RANDOM_HEX])
}

/// The hex digits of the random part of a VM id: 64 bits.
pub const VM_ID_RANDOM_HEX: usize = 16;

/// The length of an owner tag in a VM id.
pub const OWNER_TAG_HEX: usize = 8;

/// The longest path that a Unix socket address holds, without the NUL.
pub const MAX_SOCKET_PATH_BYTES: usize = 107;

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn image() -> GuestImage {
        GuestImage::new("/images/vmlinux", "/images/rootfs.ext4")
    }

    #[test]
    fn the_config_file_has_the_documented_shape() {
        let config = vm_config(
            &image(),
            MachineSpec {
                vcpu_count: 1,
                mem_size_mib: 512,
            },
            &VmPaths::jailed(),
            52,
        );
        let json = serde_json::to_value(&config).unwrap();
        let args = boot_args(&image(), 52);
        assert_eq!(
            json,
            serde_json::json!({
                "boot-source": {"kernel_image_path": "vmlinux", "boot_args": args},
                "drives": [{
                    "drive_id": "rootfs",
                    "path_on_host": "rootfs.ext4",
                    "is_root_device": true,
                    "is_read_only": true
                }],
                "machine-config": {"vcpu_count": 1, "mem_size_mib": 512, "smt": false},
                "vsock": {"guest_cid": 3, "uds_path": "v.sock"},
                "entropy": {}
            })
        );
    }

    #[test]
    fn the_config_has_no_network_and_no_metadata_service() {
        let config = vm_config(&image(), MachineSpec::default(), &VmPaths::jailed(), 52);
        let json = serde_json::to_value(&config).unwrap();
        let keys: Vec<&str> = json
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        for absent in [
            "network-interfaces",
            "mmds-config",
            "balloon",
            "logger",
            "metrics",
        ] {
            assert!(!keys.contains(&absent), "{absent} must be absent");
        }
    }

    #[test]
    fn the_boot_args_stop_on_reboot_drop_serial_and_start_the_agent() {
        let args = boot_args(&image(), 52);
        for part in [
            "reboot=k",
            "panic=1",
            "8250.nr_uarts=0",
            "init=/sbin/harvest-guest-agent",
            "harvest.port=52",
        ] {
            assert!(
                args.split(' ').any(|a| a == part),
                "{part} missing from {args}"
            );
        }
        assert!(!args.contains("console=ttyS0"));
    }

    #[test]
    fn extra_boot_args_are_appended() {
        let mut image = image();
        image.extra_boot_args.push("harvest.debug=1".into());
        assert!(boot_args(&image, 52).ends_with(" harvest.debug=1"));
    }

    #[test]
    fn an_extra_boot_arg_cannot_replace_a_launcher_arg() {
        for reserved in [
            "harvest.port=53",
            "init=/bin/sh",
            "8250.nr_uarts=1",
            "panic=0",
            // The launcher joins the arguments with spaces.
            "debug harvest.port=53",
        ] {
            let mut image = image();
            image.extra_boot_args.push(reserved.into());
            assert!(
                matches!(check_boot_args(&image), Err(SandboxError::Config(_))),
                "{reserved}"
            );
        }
        let mut image = image();
        image.extra_boot_args.push("harvest.debug=1".into());
        check_boot_args(&image).unwrap();
    }

    #[test]
    fn an_extra_boot_arg_cannot_reach_the_guest_agent() {
        for to_init in [
            "--",
            "-- --listen-unix /tmp/agent.sock",
            "--listen-unix /tmp/agent.sock",
            "nosmt -s",
        ] {
            let mut image = image();
            image.extra_boot_args.push(to_init.into());
            assert!(
                matches!(check_boot_args(&image), Err(SandboxError::Config(_))),
                "{to_init}"
            );
        }
        let mut image = image();
        image.extra_boot_args.extend([
            "nosmt".into(),
            "mitigations=auto".into(),
            "random.trust_cpu=on".into(),
        ]);
        check_boot_args(&image).unwrap();
    }

    #[test]
    fn the_init_path_is_one_kernel_word() {
        for init in [
            "",
            "/sbin/agent init=/bin/false",
            "/sbin/a\tb",
            "/sbin/\"a\"",
            // The kernel command line ends at a NUL.
            "/sbin/harvest-guest-agent\0x",
            "/sbin/a\u{7f}b",
        ] {
            let mut image = image();
            image.init = init.into();
            assert!(
                matches!(check_boot_args(&image), Err(SandboxError::Config(_))),
                "{init:?}"
            );
        }
        // The kernel removes the quotes, so this would set init.
        let mut image = image();
        image.extra_boot_args.push("\"init=/bin/sh\"".into());
        assert!(
            matches!(check_boot_args(&image), Err(SandboxError::Config(_))),
            "a quoted extra argument"
        );
        let mut with_nul = self::tests::image();
        with_nul.extra_boot_args.push("harvest.debug=1\0".into());
        assert!(
            matches!(check_boot_args(&with_nul), Err(SandboxError::Config(_))),
            "an extra argument with a NUL"
        );
    }

    #[test]
    fn the_default_machine_fits_the_default_policy() {
        let machine = MachineSpec::default();
        assert!(
            machine.run_memory_bytes() >= crate::sandbox::policy::MAX_LIMITS.memory_bytes,
            "{machine:?}"
        );
    }

    #[test]
    fn the_root_drive_id_has_no_hyphen() {
        assert!(!ROOT_DRIVE_ID.contains('-'));
    }

    #[test]
    fn vm_ids_are_checked() {
        assert!(validate_vm_id("hsbx-abc-123").is_ok());
        assert!(validate_vm_id(&"a".repeat(64)).is_ok());
        for bad in ["", "a_b", "a b", "../x", "a/b", &"a".repeat(65), "é"] {
            assert!(validate_vm_id(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn new_vm_ids_are_valid_unique_and_tagged() {
        let a = new_vm_id("0123abcd");
        let b = new_vm_id("0123abcd");
        assert_ne!(a, b);
        assert!(a.starts_with("hsbx-0123abcd-"), "{a}");
        validate_vm_id(&a).unwrap();
    }
}
