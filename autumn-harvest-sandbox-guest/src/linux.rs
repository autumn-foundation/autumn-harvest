//! The Linux duties of process 1 in the microVM.

use std::io;

/// `RNDADDENTROPY`: `_IOW('R', 0x03, int[2])`.
pub const RNDADDENTROPY: libc::c_ulong = 0x4008_5203;

/// `RNDRESEEDCRNG`: `_IO('R', 0x07)`.
pub const RNDRESEEDCRNG: libc::c_ulong = 0x5207;

/// Mount `/proc`, `/sys`, `/dev`, `/tmp` and the `/work` tmpfs of `work_mib`
/// MiB. A mount that already exists is not an error.
///
/// # Errors
///
/// Returns the first other mount error.
pub fn mount_basics(work_mib: u64) -> io::Result<()> {
    let _ = work_mib;
    todo!("mount_basics")
}

/// Mix 32 bytes of host entropy into the kernel pool, credit them, and force
/// a reseed of the kernel CSPRNG.
///
/// # Errors
///
/// Returns the error of the open or of an ioctl.
pub fn reseed_kernel(entropy: &[u8; 32]) -> io::Result<()> {
    let _ = entropy;
    todo!("reseed_kernel")
}

/// Bind and listen on a vsock port of any local context id.
///
/// # Errors
///
/// Returns the socket error.
pub fn vsock_listener(port: u32) -> io::Result<std::os::fd::OwnedFd> {
    let _ = port;
    todo!("vsock_listener")
}

/// Accept one vsock connection as a Tokio stream.
///
/// # Errors
///
/// Returns the accept error.
pub async fn accept_vsock(
    listener: &std::sync::Arc<std::os::fd::OwnedFd>,
) -> io::Result<tokio::net::UnixStream> {
    let _ = listener;
    todo!("accept_vsock")
}

/// Sync the disks and restart. With `reboot=k`, Firecracker exits.
pub fn reboot() -> ! {
    todo!("reboot")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_ioctl_numbers_match_the_kernel_macros() {
        // _IOC(dir, type, nr, size) = dir << 30 | size << 16 | type << 8 | nr.
        let ioc = |dir: u64, kind: u64, nr: u64, size: u64| (dir << 30) | (size << 16) | (kind << 8) | nr;
        assert_eq!(RNDADDENTROPY, ioc(1, u64::from(b'R'), 0x03, 8));
        assert_eq!(RNDRESEEDCRNG, ioc(0, u64::from(b'R'), 0x07, 0));
    }
}
