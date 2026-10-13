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

/// The root drive id. Firecracker resource ids reject `-`.
pub const ROOT_DRIVE_ID: &str = "rootfs";

/// The size of one microVM.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MachineSpec {
    /// Virtual CPUs, 1 to 32.
    pub vcpu_count: u8,
    /// Guest memory, in MiB.
    pub mem_size_mib: u32,
}

impl Default for MachineSpec {
    fn default() -> Self {
        Self {
            vcpu_count: 1,
            mem_size_mib: 512,
        }
    }
}

/// The kernel and root file system that each microVM boots.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GuestImage {
    /// An uncompressed `vmlinux` kernel on the host.
    pub kernel: PathBuf,
    /// An ext4 root file system on the host. The microVM opens it read-only.
    pub rootfs: PathBuf,
    /// The init path in the root file system.
    pub init: String,
    /// More kernel arguments.
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
    let mut args = vec![
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
    ];
    args.extend(image.extra_boot_args.iter().cloned());
    args.join(" ")
}

/// Build the VM document for `image` at `paths`.
#[must_use]
pub fn vm_config(image: &GuestImage, machine: MachineSpec, paths: &VmPaths, port: u32) -> VmConfigFile {
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
    let valid = (1..=64).contains(&id.len())
        && id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-');
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
        let config = vm_config(&image(), MachineSpec::default(), &VmPaths::jailed(), 52);
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
        let keys: Vec<&str> = json.as_object().unwrap().keys().map(String::as_str).collect();
        for absent in ["network-interfaces", "mmds-config", "balloon", "logger", "metrics"] {
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
            assert!(args.split(' ').any(|a| a == part), "{part} missing from {args}");
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
