#!/usr/bin/env bash
# Build a guest image for the Firecracker sandbox backend.
#
# The image is two files in OUT_DIR:
#
# - vmlinux: a guest kernel from the Firecracker CI artifacts. Linux 6.1 has
#   virtio-vsock, virtio-rng and VMGenID, which reseeds the kernel CSPRNG after
#   a snapshot restore. The kernel runs untrusted code, so the script checks
#   its SHA-256 digest on each build.
# - rootfs.ext4: the file system of a container image, with the guest agent at
#   /sbin/harvest-guest-agent. The microVM mounts it read-only.
#
# The script also writes image.sha256 with the digests of both files. Give the
# kernel digest and the rootfs digest to `GuestImage::with_name`, so each
# outcome names the image that ran the code.
#
# Usage:
#   scripts/sandbox/build-guest-image.sh OUT_DIR
#
# Environment:
#   BASE_IMAGE   the container image (default python:3.12-alpine)
#   EXTRA_APK    Alpine packages to add (default "bash nodejs"); empty adds none
#   KERNEL_URL   a kernel URL; the default is a pinned 6.1 kernel of the
#                Firecracker CI artifacts
#   KERNEL_SHA256  the SHA-256 digest of the kernel; needed with KERNEL_URL
#   AGENT_BIN    a prebuilt static agent; the default builds one with cargo
#
# Needs: docker, mkfs.ext4 (e2fsprogs 1.43 or later), curl, root or
# fakeroot, and for the agent build the rustup target
# x86_64-unknown-linux-musl.
#
# Fakeroot gives root the files in the image, not the image files. A jailed
# launcher trusts only image files that root owns. So after a non-root build,
# root must install vmlinux and rootfs.ext4; the script prints the command.
set -euo pipefail

out="${1:?usage: build-guest-image.sh OUT_DIR}"
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
base_image="${BASE_IMAGE:-python:3.12-alpine}"
extra_apk="${EXTRA_APK-bash nodejs}"

# The default kernel, pinned by digest. The Sandbox (KVM) workflow boots it.
default_kernel_url="https://s3.amazonaws.com/spec.ccfc.min/firecracker-ci/v1.15/x86_64/vmlinux-6.1.155"
default_kernel_sha256="e20e46d0c36c55c0d1014eb20576171b3f3d922260d9f792017aeff53af3d4f2"
if [ -n "${KERNEL_URL:-}" ]; then
  kernel_url="${KERNEL_URL}"
  kernel_sha256="${KERNEL_SHA256:-}"
  if [ -z "${kernel_sha256}" ]; then
    echo "build-guest-image: set KERNEL_SHA256 with KERNEL_URL" >&2
    exit 1
  fi
else
  kernel_url="${default_kernel_url}"
  kernel_sha256="${KERNEL_SHA256:-${default_kernel_sha256}}"
fi

mkdir -p "${out}"
out="$(cd "${out}" && pwd)"
work="$(mktemp -d)"
# The new files wait in a stage directory in OUT_DIR, on the same file
# system. A failed step keeps the old image, and the end renames each file.
stage="$(mktemp -d "${out}/.stage.XXXXXX")"
container=""
cleanup() {
  if [ -n "${container}" ]; then
    docker rm -f "${container}" >/dev/null 2>&1 || true
  fi
  rm -rf "${work}" "${stage}"
}
trap cleanup EXIT

# 1. The guest agent: a static musl binary, so the rootfs needs no libc match.
agent="${AGENT_BIN:-}"
if [ -z "${agent}" ]; then
  # Run in the workspace, so rustup uses the toolchain that the repo pins.
  (
    cd "${root}"
    rustup target add x86_64-unknown-linux-musl >/dev/null
    cargo build --release --locked --target x86_64-unknown-linux-musl \
      -p autumn-harvest-sandbox-guest
  )
  agent="${root}/target/x86_64-unknown-linux-musl/release/harvest-guest-agent"
fi

# 2. The kernel, checked against its digest.
echo "kernel: ${kernel_url}"
curl -fsSL --retry 3 -o "${stage}/vmlinux" "${kernel_url}"
if ! echo "${kernel_sha256}  ${stage}/vmlinux" | sha256sum -c - >/dev/null; then
  echo "build-guest-image: the kernel digest is not ${kernel_sha256}" >&2
  exit 1
fi

# 3. The root file system: export the container file system, add the agent.
if [ -n "${extra_apk}" ]; then
  container="$(docker create "${base_image}" sh -c "apk add --no-cache ${extra_apk}")"
else
  container="$(docker create "${base_image}" true)"
fi
docker start -a "${container}" >/dev/null
mkdir -p "${work}/rootfs"
# The image files must keep their owners, mostly root. Without root, tar
# makes the caller the owner of each file, and the caller can have the uid
# of the code. So a non-root build runs the extraction and mkfs under
# fakeroot, which records the owners from the archive.
as_root=()
if [ "$(id -u)" -ne 0 ]; then
  if ! command -v fakeroot >/dev/null; then
    echo "build-guest-image: run as root, or install fakeroot" >&2
    exit 1
  fi
  as_root=(fakeroot --)
fi
docker export "${container}" | "${as_root[@]}" sh -euc '
  rootfs="$1"; agent="$2"; image="$3"
  tar --numeric-owner -x -C "${rootfs}"
  rm -f "${rootfs}/.dockerenv"
  install -D -o 0 -g 0 -m 0755 "${agent}" "${rootfs}/sbin/harvest-guest-agent"
  mkdir -p "${rootfs}/work" "${rootfs}/proc" "${rootfs}/sys" "${rootfs}/dev" "${rootfs}/tmp"
  size_mib=$(( $(du -sm "${rootfs}" | cut -f1) + 64 ))
  mkfs.ext4 -q -F -L harvest-rootfs -d "${rootfs}" "${image}" "${size_mib}M"
' build-rootfs "${work}/rootfs" "${agent}" "${stage}/rootfs.ext4"

# 4. The digests, then the new image replaces the old one. A failed rename
# puts the old files back. A crash between the renames can still mix the
# files, and image.sha256 then shows it.
(cd "${stage}" && sha256sum vmlinux rootfs.ext4 > image.sha256)
bash "${root}/scripts/sandbox/publish-image.sh" "${stage}" "${out}"
cat "${out}/image.sha256"

if [ "$(id -u)" -ne 0 ]; then
  echo "build-guest-image: a jailed launcher needs root to own the image files:" >&2
  echo "  sudo install -o root -g root -m 0644 ${out}/vmlinux ${out}/rootfs.ext4 DIR" >&2
fi
