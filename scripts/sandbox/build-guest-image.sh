#!/usr/bin/env bash
# Build a guest image for the Firecracker sandbox backend.
#
# The image is two files in OUT_DIR:
#
# - vmlinux: a guest kernel from the Firecracker CI artifacts. Linux 6.1 has
#   virtio-vsock, virtio-rng and VMGenID, which reseeds the kernel CSPRNG after
#   a snapshot restore.
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
#   KERNEL_URL   a kernel URL; the default is the newest 6.1 kernel of the
#                Firecracker CI series KERNEL_CI_SERIES (default v1.15)
#   KERNEL_SHA256  the expected digest of the kernel; checked when set
#   AGENT_BIN    a prebuilt static agent; the default builds one with cargo
#
# Needs: docker, mkfs.ext4 (e2fsprogs 1.43 or later), curl, root or
# fakeroot, and for the agent build the rustup target
# x86_64-unknown-linux-musl.
set -euo pipefail

out="${1:?usage: build-guest-image.sh OUT_DIR}"
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
base_image="${BASE_IMAGE:-python:3.12-alpine}"
extra_apk="${EXTRA_APK-bash nodejs}"
KERNEL_CI_SERIES="${KERNEL_CI_SERIES:-v1.15}"
s3="https://s3.amazonaws.com/spec.ccfc.min"

mkdir -p "${out}"
out="$(cd "${out}" && pwd)"
work="$(mktemp -d)"
container=""
cleanup() {
  if [ -n "${container}" ]; then
    docker rm -f "${container}" >/dev/null 2>&1 || true
  fi
  rm -rf "${work}"
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

# 2. The kernel: the newest 6.1 build of one Firecracker CI series.
kernel_url="${KERNEL_URL:-}"
if [ -z "${kernel_url}" ]; then
  prefix="firecracker-ci/${KERNEL_CI_SERIES}/x86_64/vmlinux-6.1."
  key="$(curl -fsSL "${s3}?list-type=2&prefix=${prefix}" \
    | grep -oP "(?<=<Key>)${prefix//./\\.}[0-9]+(?=</Key>)" | sort -V | tail -1 || true)"
  if [ -z "${key}" ]; then
    echo "no 6.1 kernel under ${prefix}" >&2
    exit 1
  fi
  kernel_url="${s3}/${key}"
fi
echo "kernel: ${kernel_url}"
curl -fsSL --retry 3 -o "${out}/vmlinux" "${kernel_url}"
if [ -n "${KERNEL_SHA256:-}" ]; then
  echo "${KERNEL_SHA256}  ${out}/vmlinux" | sha256sum -c -
fi

# 3. The root file system: export the container file system, add the agent.
if [ -n "${extra_apk}" ]; then
  container="$(docker create "${base_image}" sh -c "apk add --no-cache ${extra_apk}")"
else
  container="$(docker create "${base_image}" true)"
fi
docker start -a "${container}" >/dev/null
mkdir -p "${work}/rootfs"
rm -f "${out}/rootfs.ext4"
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
' build-rootfs "${work}/rootfs" "${agent}" "${out}/rootfs.ext4"

# 4. The digests.
(cd "${out}" && sha256sum vmlinux rootfs.ext4 > image.sha256)
cat "${out}/image.sha256"
