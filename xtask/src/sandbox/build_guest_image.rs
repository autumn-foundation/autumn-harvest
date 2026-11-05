//! `cargo xtask sandbox build-guest-image`: the port of
//! `scripts/sandbox/build-guest-image.sh`.
//!
//! Build a guest image for the Firecracker sandbox backend.
//!
//! The image is two files in `OUT_DIR`:
//!
//! - `vmlinux`: a guest kernel from the Firecracker CI artifacts. Linux 6.1
//!   has virtio-vsock, virtio-rng and `VMGenID`. `VMGenID` reseeds the kernel
//!   CSPRNG after a snapshot restore. The kernel runs untrusted code, so the
//!   command checks its SHA-256 digest on each build.
//! - `rootfs.ext4`: the file system of a container image, with the guest
//!   agent at `/sbin/harvest-guest-agent`. The microVM mounts it read-only.
//!
//! The command also writes `image.sha256` with the digests of both files.
//! Give the kernel digest and the rootfs digest to `GuestImage::with_name`.
//! Then each outcome names the image that ran the code.
//!
//! Usage:
//!
//! ```sh
//! cargo xtask sandbox build-guest-image OUT_DIR
//! ```
//!
//! Environment:
//!
//! - `BASE_IMAGE`: the container image (default `python:3.12-alpine`).
//! - `EXTRA_APK`: Alpine packages to add (default `bash nodejs`). An empty
//!   value adds none.
//! - `KERNEL_URL`: a kernel URL. The default is a pinned 6.1 kernel of the
//!   Firecracker CI artifacts.
//! - `KERNEL_SHA256`: the SHA-256 digest of the kernel. `KERNEL_URL` needs it.
//! - `AGENT_BIN`: a prebuilt static agent. The default builds one with cargo.
//!
//! Needs: docker, mkfs.ext4 (e2fsprogs 1.43 or later), curl, sha256sum, root
//! or fakeroot. The agent build also needs the rustup target
//! `x86_64-unknown-linux-musl`.
//!
//! Fakeroot gives root the files in the image, not the image files. A jailed
//! launcher trusts only image files that root owns. So after a non-root
//! build, root must install `vmlinux` and `rootfs.ext4`. The command prints
//! the install command.
//!
//! A failed step stops the build with the exit code of that step, as
//! `set -e` does. A failed step of the command itself exits with 1.

use std::fmt;
use std::fs::File;
use std::io::Write as _;
use std::path::{Component, Path, PathBuf};
use std::process::{Command, ExitCode, ExitStatus, Stdio};

use smop::prelude::*;

use super::publish_image;
use crate::util::repo_root;

/// The default kernel URL. The Sandbox (KVM) workflow boots it.
const DEFAULT_KERNEL_URL: &str =
    "https://s3.amazonaws.com/spec.ccfc.min/firecracker-ci/v1.15/x86_64/vmlinux-6.1.155";

/// The SHA-256 digest that pins the default kernel.
const DEFAULT_KERNEL_SHA256: &str =
    "e20e46d0c36c55c0d1014eb20576171b3f3d922260d9f792017aeff53af3d4f2";

/// The default container image of the root file system.
const DEFAULT_BASE_IMAGE: &str = "python:3.12-alpine";

/// The default Alpine packages of the root file system.
const DEFAULT_EXTRA_APK: &str = "bash nodejs";

/// The rustup target of the static guest agent.
const MUSL_TARGET: &str = "x86_64-unknown-linux-musl";

/// The root file system step. It runs as root, or under fakeroot.
///
/// The arguments are the rootfs directory, the agent, the image file and the
/// xtask binary. The `image_path` function runs `cargo xtask sandbox
/// image-path`.
///
/// The image controls its links. A root write through `/sbin` or `/work`
/// could then reach the host. So each write goes to a path that
/// `image_path` resolves inside the image, as a chroot would.
const ROOTFS_SCRIPT: &str = r#"
  rootfs="$1"; agent="$2"; image="$3"; xtask="$4"
  image_path() { "${xtask}" sandbox image-path "$@"; }
  tar --numeric-owner -x -C "${rootfs}"
  rm -f "${rootfs}/.dockerenv"
  sbin="$(image_path "${rootfs}" /sbin)"
  mkdir -p "${sbin}"
  rm -f "${sbin}/harvest-guest-agent"
  install -o 0 -g 0 -m 0755 "${agent}" "${sbin}/harvest-guest-agent"
  for dir in /work /proc /sys /dev /tmp; do
    mkdir -p "$(image_path "${rootfs}" "${dir}")"
  done
  size_mib=$(( $(du -sm "${rootfs}" | cut -f1) + 64 ))
  mkfs.ext4 -q -F -L harvest-rootfs -d "${rootfs}" "${image}" "${size_mib}M"
"#;

/// Arguments of `cargo xtask sandbox build-guest-image`.
#[derive(Debug, Args)]
pub struct Args {
    /// The directory that receives `vmlinux`, `rootfs.ext4` and `image.sha256`.
    pub out: Option<std::ffi::OsString>,
}

/// A step failed. The build exits with this code, as `set -e` does.
#[derive(Debug)]
struct StepFailed(u8);

impl fmt::Display for StepFailed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "a step failed with exit code {}", self.0)
    }
}

impl std::error::Error for StepFailed {}

/// The kernel that the image boots. The digest is never optional.
#[derive(Debug, PartialEq, Eq)]
struct Kernel {
    url: String,
    sha256: String,
}

/// Picks the kernel from `KERNEL_URL` and `KERNEL_SHA256`.
///
/// An unset variable and an empty one are the same. A custom URL needs its
/// own digest. The default URL uses the pinned digest, unless
/// `KERNEL_SHA256` names another.
fn kernel(url: Option<String>, sha256: Option<String>) -> Result<Kernel, &'static str> {
    let url = url.filter(|value| !value.is_empty());
    let sha256 = sha256.filter(|value| !value.is_empty());
    match url {
        Some(url) => sha256
            .map(|sha256| Kernel { url, sha256 })
            .ok_or("build-guest-image: set KERNEL_SHA256 with KERNEL_URL"),
        None => Ok(Kernel {
            url: DEFAULT_KERNEL_URL.to_owned(),
            sha256: sha256.unwrap_or_else(|| DEFAULT_KERNEL_SHA256.to_owned()),
        }),
    }
}

/// Reads an environment variable. A value that is not UTF-8 counts as unset.
fn env_var(name: &str) -> Option<String> {
    std::env::var(name).ok()
}

/// Runs `cargo xtask sandbox build-guest-image`.
///
/// The group dispatcher fixes the `Result` return type.
#[allow(clippy::unnecessary_wraps)]
pub fn run(args: Args) -> Result<ExitCode> {
    let Some(out) = publish_image::path_arg(args.out) else {
        eprintln!("usage: cargo xtask sandbox build-guest-image OUT_DIR");
        return Ok(ExitCode::FAILURE);
    };
    // The container and the stage directory are removed on drop. The guard
    // keeps this process alive after an interrupt, so those drops still run,
    // as the `EXIT` trap of the old script did.
    let _guard = crate::interrupt::install();
    match build(&out) {
        Ok(()) => Ok(ExitCode::SUCCESS),
        Err(err) => {
            if let Some(StepFailed(code)) = err.downcast_ref::<StepFailed>() {
                return Ok(ExitCode::from(*code));
            }
            eprintln!("build-guest-image: {err:#}");
            Ok(ExitCode::FAILURE)
        }
    }
}

/// Removes the container of the root file system step when it drops.
#[derive(Default)]
struct Container(Option<String>);

impl Drop for Container {
    fn drop(&mut self) {
        if let Some(id) = self.0.take() {
            let _ = Command::new("docker")
                .args(["rm", "-f", &id])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
    }
}

/// Builds the image into `out`.
fn build(out: &Path) -> Result<()> {
    let root = repo_root();
    let base_image = env_var("BASE_IMAGE")
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_BASE_IMAGE.to_owned());
    let extra_apk = env_var("EXTRA_APK").unwrap_or_else(|| DEFAULT_EXTRA_APK.to_owned());
    let kernel = match kernel(env_var("KERNEL_URL"), env_var("KERNEL_SHA256")) {
        Ok(kernel) => kernel,
        Err(message) => {
            eprintln!("{message}");
            return Err(StepFailed(1).into());
        }
    };

    std::fs::create_dir_all(out).with_context(|| format!("cannot create {}", out.display()))?;
    let out = absolute(out)?;
    let work = smop::fs::temp_dir()?;
    // The new files wait in a stage directory in OUT_DIR, on the same file
    // system. A failed step keeps the old image, and the end renames each file.
    let stage = smop::fs::TempDir::with_prefix_in(".stage.", &out)
        .with_context(|| format!("cannot make a stage directory in {}", out.display()))?;
    // The container drops before the directories, as the old cleanup did.
    let mut container = Container::default();

    let agent = agent(&root)?;
    fetch_kernel(&kernel, stage.path())?;

    // 3. The root file system: export the container file system, add the agent.
    let mut create = Command::new("docker");
    create.args(["create", &base_image]);
    if extra_apk.is_empty() {
        create.arg("true");
    } else {
        create.args(["sh", "-c", &format!("apk add --no-cache {extra_apk}")]);
    }
    let (status, id) = capture(&mut create)?;
    let id = id.trim_end_matches('\n').to_owned();
    if !id.is_empty() {
        container.0 = Some(id.clone());
    }
    check(status)?;
    step(
        Command::new("docker")
            .args(["start", "-a", &id])
            .stdout(Stdio::null()),
    )?;
    let rootfs = work.path().join("rootfs");
    std::fs::create_dir_all(&rootfs)
        .with_context(|| format!("cannot create {}", rootfs.display()))?;
    // The image files must keep their owners, mostly root. Without root, tar
    // makes the caller the owner of each file. The caller can have the uid
    // of the code. So a non-root build runs the extraction and mkfs under
    // fakeroot. Fakeroot records the owners from the archive.
    let is_root = uid()? == 0;
    let mut extract = if is_root {
        Command::new("sh")
    } else {
        if smop::path::which("fakeroot").is_err() {
            eprintln!("build-guest-image: run as root, or install fakeroot");
            return Err(StepFailed(1).into());
        }
        let mut fakeroot = Command::new("fakeroot");
        fakeroot.args(["--", "sh"]);
        fakeroot
    };
    let xtask = std::env::current_exe().context("cannot find the xtask binary")?;
    extract
        .args(["-euc", ROOTFS_SCRIPT, "build-rootfs"])
        .arg(&rootfs)
        .arg(&agent)
        .arg(stage.path().join("rootfs.ext4"))
        .arg(&xtask);
    pipe_into(Command::new("docker").args(["export", &id]), &mut extract)?;

    // 4. The digests, then the new image replaces the old one. A failed rename
    // puts the old files back. A crash between the renames can still mix the
    // files, and image.sha256 then shows it.
    let sums =
        File::create(stage.path().join("image.sha256")).context("cannot create image.sha256")?;
    step(
        Command::new("sha256sum")
            .args(["vmlinux", "rootfs.ext4"])
            .current_dir(stage.path())
            .stdout(sums),
    )?;
    match publish_image::publish(stage.path(), &out) {
        Ok(true) => {}
        Ok(false) => return Err(StepFailed(1).into()),
        Err(err) => {
            eprintln!("publish-image: {err:#}");
            return Err(StepFailed(1).into());
        }
    }
    let sums = std::fs::read(out.join("image.sha256")).context("cannot read image.sha256")?;
    std::io::stdout()
        .write_all(&sums)
        .context("cannot print image.sha256")?;

    if !is_root {
        eprintln!("build-guest-image: a jailed launcher needs root to own the image files:");
        eprintln!(
            "  sudo install -o root -g root -m 0644 {out}/vmlinux {out}/rootfs.ext4 DIR",
            out = out.display()
        );
    }
    drop(container);
    drop(stage);
    drop(work);
    Ok(())
}

/// 1. The guest agent: a static musl binary, so the rootfs needs no libc match.
///
/// Returns `AGENT_BIN` when it is set. Otherwise builds the agent.
fn agent(root: &Path) -> Result<PathBuf> {
    if let Some(agent) = env_var("AGENT_BIN").filter(|value| !value.is_empty()) {
        return Ok(PathBuf::from(agent));
    }
    // Run in the workspace, so rustup uses the toolchain that the repo pins.
    step(
        Command::new("rustup")
            .args(["target", "add", MUSL_TARGET])
            .current_dir(root)
            .stdout(Stdio::null()),
    )?;
    step(
        Command::new("cargo")
            .args(["build", "--release", "--locked", "--target", MUSL_TARGET])
            .args(["-p", "autumn-harvest-sandbox-guest"])
            .current_dir(root),
    )?;
    Ok(root
        .join("target")
        .join(MUSL_TARGET)
        .join("release/harvest-guest-agent"))
}

/// 2. The kernel, checked against its digest.
fn fetch_kernel(kernel: &Kernel, stage: &Path) -> Result<()> {
    println!("kernel: {}", kernel.url);
    let vmlinux = stage.join("vmlinux");
    step(
        Command::new("curl")
            .args(["-fsSL", "--retry", "3", "-o"])
            .arg(&vmlinux)
            .arg(&kernel.url),
    )?;
    if !digest_matches(&kernel.sha256, &vmlinux) {
        eprintln!(
            "build-guest-image: the kernel digest is not {}",
            kernel.sha256
        );
        return Err(StepFailed(1).into());
    }
    Ok(())
}

/// Returns `path` as an absolute path without `.` or `..` components, as
/// `cd "$path" && pwd` prints it.
fn absolute(path: &Path) -> Result<PathBuf> {
    let full = std::path::absolute(path)
        .with_context(|| format!("cannot make {} absolute", path.display()))?;
    let mut clean = PathBuf::new();
    for component in full.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                clean.pop();
            }
            other => clean.push(other),
        }
    }
    Ok(clean)
}

/// Checks the file at `path` against `sha256` with `sha256sum -c`.
fn digest_matches(sha256: &str, path: &Path) -> bool {
    let child = Command::new("sha256sum")
        .args(["-c", "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .spawn();
    let Ok(mut child) = child else {
        return false;
    };
    let line = format!("{sha256}  {}\n", path.display());
    let written = child
        .stdin
        .take()
        .is_some_and(|mut stdin| stdin.write_all(line.as_bytes()).is_ok());
    child.wait().is_ok_and(|status| status.success()) && written
}

/// Returns the user id of this process, as `id -u` prints it.
fn uid() -> Result<u32> {
    let (status, text) = capture(Command::new("id").arg("-u"))?;
    check(status)?;
    text.trim()
        .parse()
        .with_context(|| format!("`id -u` printed {text:?}"))
}

/// Runs `command`, and fails with its exit code when it fails.
fn step(command: &mut Command) -> Result<()> {
    let status = spawn(command)?.wait().context("cannot wait for a step")?;
    check(status)
}

/// Runs `command` and returns its status and its standard output.
fn capture(command: &mut Command) -> Result<(ExitStatus, String)> {
    command.stdout(Stdio::piped());
    let output = spawn(command)?
        .wait_with_output()
        .context("cannot wait for a step")?;
    let text = String::from_utf8(output.stdout).context("a step printed text that is not UTF-8")?;
    Ok((output.status, text))
}

/// Pipes the standard output of `from` into `into`, as `from | into` does
/// under `pipefail`.
fn pipe_into(from: &mut Command, into: &mut Command) -> Result<()> {
    let mut source = spawn(from.stdout(Stdio::piped()))?;
    let stdout = source.stdout.take().context("no pipe from docker export")?;
    let sink = spawn(into.stdin(stdout));
    // The command still holds the read end of the pipe. Drop it, so a sink
    // that exits first leaves no reader, and the source gets a broken pipe.
    into.stdin(Stdio::null());
    let sink_status = match sink {
        Ok(mut sink) => sink.wait().context("cannot wait for a step")?,
        Err(err) => {
            let _ = source.kill();
            let _ = source.wait();
            return Err(err);
        }
    };
    let source_status = source.wait().context("cannot wait for a step")?;
    check(sink_status)?;
    check(source_status)
}

/// Starts `command`. A missing program fails with 127, as in a shell.
///
/// After an interrupt, it starts nothing and fails with 130.
fn spawn(command: &mut Command) -> Result<std::process::Child> {
    stop_on_interrupt()?;
    command.spawn().map_err(|err| {
        let program = command.get_program().to_string_lossy().into_owned();
        if err.kind() == std::io::ErrorKind::NotFound {
            eprintln!("build-guest-image: {program}: command not found");
            StepFailed(127).into()
        } else {
            eprintln!("build-guest-image: {program}: {err}");
            StepFailed(126).into()
        }
    })
}

/// Fails with 130 once an interrupt has arrived.
fn stop_on_interrupt() -> Result<()> {
    if crate::interrupt::requested() {
        eprintln!("build-guest-image: interrupted");
        return Err(StepFailed(crate::interrupt::EXIT_CODE).into());
    }
    Ok(())
}

/// Maps a failed status to its exit code, as a shell reports it.
///
/// After an interrupt, it fails with 130 whatever the status.
fn check(status: ExitStatus) -> Result<()> {
    stop_on_interrupt()?;
    if status.success() {
        return Ok(());
    }
    Err(StepFailed(shell_code(status)).into())
}

/// Returns the exit code that a shell reports for `status`.
fn shell_code(status: ExitStatus) -> u8 {
    if let Some(code) = status.code() {
        return u8::try_from(code & 0xff).unwrap_or(1);
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt as _;
        if let Some(signal) = status.signal() {
            return u8::try_from(128 + (signal & 0x7f)).unwrap_or(1);
        }
    }
    1
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A sink that exits first must not leave the source blocked on a full
    /// pipe. The shell pipeline ends with a broken pipe there, so the port
    /// must too.
    #[cfg(unix)]
    #[test]
    fn a_sink_that_exits_first_does_not_hang_the_pipe() {
        let (done, outcome) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let result = pipe_into(&mut Command::new("yes"), &mut Command::new("false"));
            let _ = done.send(result.is_err());
        });
        let failed = outcome
            .recv_timeout(std::time::Duration::from_secs(30))
            .expect("pipe_into must return when the sink exits first");
        assert!(failed, "a failed sink must fail the pipe");
    }

    #[test]
    fn the_default_kernel_is_pinned_by_digest() {
        assert_eq!(DEFAULT_KERNEL_SHA256.len(), 64);
        assert!(DEFAULT_KERNEL_SHA256.chars().all(|c| c.is_ascii_hexdigit()));
        let pinned = kernel(None, None).unwrap();
        assert_eq!(pinned.url, DEFAULT_KERNEL_URL);
        assert_eq!(pinned.sha256, DEFAULT_KERNEL_SHA256);
        let empty = kernel(Some(String::new()), Some(String::new())).unwrap();
        assert_eq!(empty, pinned);
    }

    #[test]
    fn a_custom_digest_replaces_the_pin() {
        let other = kernel(None, Some("ab".repeat(32))).unwrap();
        assert_eq!(other.url, DEFAULT_KERNEL_URL);
        assert_eq!(other.sha256, "ab".repeat(32));
        let custom = kernel(Some("file:///k".to_owned()), Some("cd".repeat(32))).unwrap();
        assert_eq!(custom.url, "file:///k");
    }

    #[test]
    fn a_kernel_url_needs_a_digest() {
        assert_eq!(
            kernel(Some("file:///k".to_owned()), None).unwrap_err(),
            "build-guest-image: set KERNEL_SHA256 with KERNEL_URL"
        );
        assert!(kernel(Some("file:///k".to_owned()), Some(String::new())).is_err());
    }

    #[test]
    fn the_out_dir_is_absolute_and_clean() {
        let cwd = std::env::current_dir().unwrap();
        assert_eq!(absolute(Path::new("a/./b/../c")).unwrap(), cwd.join("a/c"));
        assert_eq!(absolute(Path::new("/x/y/..")).unwrap(), PathBuf::from("/x"));
    }

    #[test]
    fn the_rootfs_step_writes_through_image_path_only() {
        assert!(ROOTFS_SCRIPT.contains("sbin=\"$(image_path \"${rootfs}\" /sbin)\""));
        assert!(ROOTFS_SCRIPT.contains("sandbox image-path"));
        // The guard of the guide reads this file, so the flag is split.
        let follows_links = concat!("install", " -D");
        assert!(
            !ROOTFS_SCRIPT.contains(follows_links),
            "{follows_links} follows links"
        );
    }

    #[cfg(unix)]
    #[test]
    fn a_signal_maps_to_128_plus_its_number() {
        use std::os::unix::process::ExitStatusExt as _;
        assert_eq!(shell_code(ExitStatus::from_raw(15)), 143);
        assert_eq!(shell_code(ExitStatus::from_raw(3 << 8)), 3);
    }
}
