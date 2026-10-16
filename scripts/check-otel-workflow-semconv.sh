#!/usr/bin/env bash
# Run the upstream OpenTelemetry policy checks on the workflow model (issue #2015).
#
# The script mirrors `make check-policies` of the upstream
# `semantic-conventions` repository. It pins one upstream commit. It reads the
# Weaver image and the policy package pins from that commit. The container
# gets local mounts only, so it needs no network.
#
# The script runs Weaver two times:
# 1. On the model at its upstream path, `model/workflow`. It must pass.
# 2. On a negative control, a copy with one unresolved `ref`. It must fail.
#    The second run proves that the first run read the model.
#
# WEAVER_REGISTRY names the image registry. The default is the Google mirror
# of Docker Hub, which has no pull limit. The digest pins the content.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
model="${root}/docs/otel-workflow-semconv/model/workflow"

# Upstream `main` on 2026-10-15, and the latest release on that day. To move
# the pins, change both values, run this script, and update section 10 of
# docs/otel-workflow-semconv/README.md.
semconv_commit="331541c4057c53d133c32efadaa7b656636eda77"
baseline_tag="v1.44.0"
semconv_repo="https://github.com/open-telemetry/semantic-conventions.git"
registry="${WEAVER_REGISTRY:-mirror.gcr.io}"

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# clone_at <repository> <ref> <directory>: a shallow clone of one ref.
clone_at() {
  git init -q "$3"
  git -C "$3" fetch -q --depth 1 "$1" "$2"
  git -C "$3" -c advice.detachedHead=false checkout -q FETCH_HEAD
}

semconv="${work}/semconv"
clone_at "${semconv_repo}" "${semconv_commit}" "${semconv}"
clone_at "${semconv_repo}" "refs/tags/${baseline_tag}" "${work}/baseline"

image="$(awk '$4 == "weaver" { print $2 }' "${semconv}/dependencies.Dockerfile")"
case "${image}" in
  *@sha256:*) image="${registry}/${image}" ;;
  *)
    echo "error: upstream names the Weaver image '${image}' with no @sha256: digest" >&2
    exit 1
    ;;
esac

packages="$(sed -n 's/^WEAVER_PACKAGES_REPO=//p' "${semconv}/Makefile")"
packages_repo="${packages%@*}"
packages_ref="${packages##*@}"
if [ -z "${packages}" ] || [ "${packages_repo}" = "${packages}" ]; then
  echo "error: upstream does not pin WEAVER_PACKAGES_REPO to a commit: '${packages}'" >&2
  exit 1
fi
clone_at "${packages_repo}" "${packages_ref}" "${work}/packages"

# Upstream filters some known findings in `.weaver.toml`. Older commits lack it.
config=()
if [ -f "${semconv}/.weaver.toml" ]; then
  config=(--mount "type=bind,source=${semconv}/.weaver.toml,target=/home/weaver/.weaver.toml,readonly")
fi

# check <model directory>: run the upstream checks with that model in place.
check() {
  rm -rf "${semconv}/model/workflow"
  cp -R "$1" "${semconv}/model/workflow"
  local cache
  cache="$(mktemp -d "${work}/cache.XXXXXX")"
  local flags=(--v2 --registry=/home/weaver/source)
  if [ "${#config[@]}" -gt 0 ]; then
    flags+=(--config=/home/weaver/.weaver.toml)
  fi
  docker run --rm --network none -u "$(id -u):$(id -g)" \
    --env USER=weaver --env HOME=/home/weaver \
    -v "${cache}:/home/weaver/.weaver" \
    --mount "type=bind,source=${semconv}/policies,target=/home/weaver/policies,readonly" \
    --mount "type=bind,source=${semconv}/model,target=/home/weaver/source,readonly" \
    --mount "type=bind,source=${work}/baseline/model,target=/home/weaver/baseline,readonly" \
    --mount "type=bind,source=${work}/packages,target=/home/weaver/packages,readonly" \
    "${config[@]}" \
    "${image}" registry check "${flags[@]}" \
    --baseline-registry=/home/weaver/baseline \
    --policy=/home/weaver/policies \
    --policy=/home/weaver/packages/policies/check/naming_conventions \
    --policy=/home/weaver/packages/policies/check/stability \
    --policy=/home/weaver/packages/policies/check/entity_associations \
    --policy=/home/weaver/packages/policies/check/backwards-compatibility
}

echo "Upstream ${semconv_commit}, baseline ${baseline_tag}, image ${image}"
echo "Run 1: the model must pass."
check "${model}"

echo "Run 2: the negative control must fail."
broken="${work}/broken"
cp -R "${model}" "${broken}"
sed 's/ref: workflow\.signal\.id$/ref: workflow.no_such_attribute/' \
  "${model}/spans.yaml" >"${broken}/spans.yaml"
grep -q 'ref: workflow.no_such_attribute' "${broken}/spans.yaml" || {
  echo "error: the negative control did not change spans.yaml" >&2
  exit 1
}
if check "${broken}" >"${work}/broken.log" 2>&1; then
  cat "${work}/broken.log" >&2
  echo "error: the negative control passed, so run 1 proves nothing" >&2
  exit 1
fi
grep -q 'workflow.no_such_attribute' "${work}/broken.log" || {
  cat "${work}/broken.log" >&2
  echo "error: the negative control failed for another reason" >&2
  exit 1
}
echo "The model passes the upstream policy checks."
