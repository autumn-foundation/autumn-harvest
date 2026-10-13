//! The Linux duties of process 1 in the microVM.

use std::ffi::CString;
use std::io;
use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd};
use std::sync::Arc;

/// The request type of `ioctl`: glibc and musl declare it apart.
#[cfg(target_env = "gnu")]
pub type Ioctl = libc::c_ulong;
/// The request type of `ioctl`: glibc and musl declare it apart.
#[cfg(not(target_env = "gnu"))]
pub type Ioctl = libc::c_int;

/// `RNDADDENTROPY`: `_IOW('R', 0x03, int[2])`.
pub const RNDADDENTROPY: Ioctl = 0x4008_5203;

/// `RNDRESEEDCRNG`: `_IO('R', 0x07)`.
pub const RNDRESEEDCRNG: Ioctl = 0x5207;

/// The argument of `RNDADDENTROPY` with a 32-byte buffer.
#[repr(C)]
struct RandPoolInfo {
    entropy_count: libc::c_int,
    buf_size: libc::c_int,
    buf: [u8; 32],
}

/// Mount `/proc`, `/sys`, `/dev` and `/tmp`. A mount that already exists is
/// not an error.
///
/// # Errors
///
/// Returns the first other mount error.
pub fn mount_basics() -> io::Result<()> {
    let hardened = libc::MS_NOSUID | libc::MS_NODEV | libc::MS_NOEXEC;
    mount("proc", "/proc", "proc", hardened, "")?;
    mount("sysfs", "/sys", "sysfs", hardened, "")?;
    mount("devtmpfs", "/dev", "devtmpfs", libc::MS_NOSUID, "mode=0755")?;
    mount(
        "tmpfs",
        "/tmp",
        "tmpfs",
        libc::MS_NOSUID | libc::MS_NODEV,
        "mode=1777,size=64m",
    )
}

/// Mount the `/work` tmpfs of `work_mib` MiB. The guest memory bounds it
/// too.
///
/// # Errors
///
/// Returns the mount error.
pub fn mount_work(work_mib: u64) -> io::Result<()> {
    let options = format!("mode=0755,size={work_mib}m,nr_inodes=65536");
    mount(
        "tmpfs",
        "/work",
        "tmpfs",
        libc::MS_NOSUID | libc::MS_NODEV,
        &options,
    )
}

fn mount(
    source: &str,
    target: &str,
    kind: &str,
    flags: libc::c_ulong,
    data: &str,
) -> io::Result<()> {
    let c = |s: &str| CString::new(s).map_err(|_| io::Error::from_raw_os_error(libc::EINVAL));
    let (source, target_c, kind, data) = (c(source)?, c(target)?, c(kind)?, c(data)?);
    // The root file system is read-only, so the image must hold each mount
    // point. A missing one is created when the root is writable.
    let _ = std::fs::create_dir_all(target);
    // SAFETY: mount reads four valid C strings.
    let result = unsafe {
        libc::mount(
            source.as_ptr(),
            target_c.as_ptr(),
            kind.as_ptr(),
            flags,
            data.as_ptr().cast(),
        )
    };
    if result == 0 {
        return Ok(());
    }
    let err = io::Error::last_os_error();
    if err.raw_os_error() == Some(libc::EBUSY) {
        return Ok(());
    }
    Err(io::Error::new(
        err.kind(),
        format!("cannot mount {target}: {err}"),
    ))
}

/// Mix 32 bytes of host entropy into the kernel pool, credit them, and force
/// a reseed of the kernel CSPRNG.
///
/// # Errors
///
/// Returns the error of the open or of an ioctl.
pub fn reseed_kernel(entropy: &[u8; 32]) -> io::Result<()> {
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open("/dev/urandom")?;
    let info = RandPoolInfo {
        entropy_count: 256,
        buf_size: 32,
        buf: *entropy,
    };
    // SAFETY: both ioctls take a valid descriptor; the first reads a valid
    // RandPoolInfo with a 32-byte buffer.
    unsafe {
        if libc::ioctl(file.as_raw_fd(), RNDADDENTROPY, &raw const info) != 0 {
            return Err(io::Error::last_os_error());
        }
        if libc::ioctl(file.as_raw_fd(), RNDRESEEDCRNG) != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

/// Bind and listen on a vsock port of any local context id.
///
/// # Errors
///
/// Returns the socket error.
pub fn vsock_listener(port: u32) -> io::Result<OwnedFd> {
    // SAFETY: socket returns a new descriptor or -1.
    let fd = unsafe { libc::socket(libc::AF_VSOCK, libc::SOCK_STREAM | libc::SOCK_CLOEXEC, 0) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the descriptor is new and owned here.
    let socket = unsafe { OwnedFd::from_raw_fd(fd) };
    // SAFETY: sockaddr_vm is a plain C struct, and zero is valid.
    let mut addr: libc::sockaddr_vm = unsafe { std::mem::zeroed() };
    addr.svm_family = libc::sa_family_t::try_from(libc::AF_VSOCK)
        .map_err(|_| io::Error::from_raw_os_error(libc::EINVAL))?;
    addr.svm_port = port;
    addr.svm_cid = libc::VMADDR_CID_ANY;
    let len = libc::socklen_t::try_from(std::mem::size_of::<libc::sockaddr_vm>())
        .map_err(|_| io::Error::from_raw_os_error(libc::EINVAL))?;
    // SAFETY: bind and listen read a valid address of the given length.
    unsafe {
        if libc::bind(socket.as_raw_fd(), (&raw const addr).cast(), len) != 0 {
            return Err(io::Error::last_os_error());
        }
        if libc::listen(socket.as_raw_fd(), 16) != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(socket)
}

/// Accept one vsock connection as a Tokio stream.
///
/// Tokio has no vsock type. A Unix stream wrapper works, because the agent
/// only reads and writes the descriptor.
///
/// # Errors
///
/// Returns the accept error.
pub async fn accept_vsock(listener: &Arc<OwnedFd>) -> io::Result<tokio::net::UnixStream> {
    let listener = Arc::clone(listener);
    let fd = tokio::task::spawn_blocking(move || {
        // SAFETY: accept4 on a listening descriptor; null address pointers
        // are allowed.
        let fd = unsafe {
            libc::accept4(
                listener.as_raw_fd(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                libc::SOCK_CLOEXEC,
            )
        };
        if fd < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(fd)
        }
    })
    .await
    .map_err(io::Error::other)??;
    // SAFETY: the descriptor is new and owned here.
    let stream = unsafe { std::os::unix::net::UnixStream::from_raw_fd(fd) };
    stream.set_nonblocking(true)?;
    tokio::net::UnixStream::from_std(stream)
}

/// Sync the disks and restart. With `reboot=k`, Firecracker exits.
pub fn reboot() -> ! {
    // SAFETY: sync and reboot take no pointers.
    unsafe {
        libc::sync();
        libc::reboot(libc::RB_AUTOBOOT);
    }
    // The reboot failed: the agent is not process 1, or lacks the right.
    std::process::exit(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[allow(
        clippy::useless_conversion,
        reason = "Ioctl is u64 on glibc and i32 on musl"
    )]
    fn the_ioctl_numbers_match_the_kernel_macros() {
        // _IOC(dir, type, nr, size) = dir << 30 | size << 16 | type << 8 | nr.
        let ioc =
            |dir: u64, kind: u64, nr: u64, size: u64| (dir << 30) | (size << 16) | (kind << 8) | nr;
        assert_eq!(
            u64::try_from(RNDADDENTROPY).ok(),
            Some(ioc(1, u64::from(b'R'), 0x03, 8))
        );
        assert_eq!(
            u64::try_from(RNDRESEEDCRNG).ok(),
            Some(ioc(0, u64::from(b'R'), 0x07, 0))
        );
    }

    #[test]
    fn the_entropy_struct_matches_the_kernel_layout() {
        assert_eq!(std::mem::size_of::<RandPoolInfo>(), 8 + 32);
    }
}
