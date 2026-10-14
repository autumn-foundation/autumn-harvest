#!/usr/bin/env bash
# Move a built guest image from STAGE_DIR into OUT_DIR.
#
# Each file of the stage replaces its namesake in OUT_DIR by a rename.
# A hard link keeps each old file first. When a rename fails, each file
# that already moved gets its old version back, so OUT_DIR keeps a whole
# image. STAGE_DIR must be on the file system of OUT_DIR.
#
# A jailed launcher pins the image under a shared flock(1) of OUT_DIR. The
# publish holds the lock exclusively, so a launcher never pins half an image.
#
# Usage:
#   scripts/sandbox/publish-image.sh STAGE_DIR OUT_DIR
set -euo pipefail

stage="${1:?usage: publish-image.sh STAGE_DIR OUT_DIR}"
out="${2:?usage: publish-image.sh STAGE_DIR OUT_DIR}"
files=(rootfs.ext4 vmlinux image.sha256)

exec 9<"${out}"
if ! flock -w 60 -x 9; then
  echo "publish-image: ${out} stayed locked; nothing is published" >&2
  exit 1
fi

backup="$(mktemp -d "${out}/.old.XXXXXX")"
trap 'rm -rf "${backup}"' EXIT
# A target that cannot be linked, such as a directory, stops the publish
# before any rename.
for file in "${files[@]}"; do
  if [ -e "${out}/${file}" ]; then
    ln "${out}/${file}" "${backup}/${file}"
  fi
done

moved=()
for file in "${files[@]}"; do
  if mv -fT "${stage}/${file}" "${out}/${file}"; then
    moved+=("${file}")
    continue
  fi
  for back in ${moved[@]+"${moved[@]}"}; do
    if [ -e "${backup}/${back}" ]; then
      mv -fT "${backup}/${back}" "${out}/${back}"
    else
      rm -f "${out}/${back}"
    fi
  done
  echo "publish-image: could not publish ${file}; ${out} keeps the old image" >&2
  exit 1
done
