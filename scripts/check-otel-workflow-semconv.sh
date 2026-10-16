#!/usr/bin/env bash
# Run the upstream OpenTelemetry policy checks on the workflow model (issue #2015).
#
# The script mirrors the `make check-policies` recipe of the upstream
# `semantic-conventions` repository. It pins one upstream commit. It reads the
# Weaver image and the policy package pins from that commit. The container
# runs with `--network none` and reads local mounts only.
#
# The script runs Weaver two times:
# 1. On the model at its upstream path, `model/workflow`. It must pass, and
#    Weaver must report no warning for the model.
# 2. On a negative control, a copy with a namespace collision. It must fail on
#    the upstream naming policy. This proves that run 1 read the model and
#    ran the package policies.
#
# SEMCONV_LATEST=1 replaces the pins with upstream `main` and the latest
# release. A weekly job uses it to find upstream changes before a pin moves.
#
# WEAVER_REGISTRY names the image registry. The default is `mirror.gcr.io`, a
# Docker Hub mirror. The digest pins the content.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
model="${root}/docs/otel-workflow-semconv/model/workflow"

# Upstream `main` on 2026-10-15, and the latest release on that day. To move
# the pins, change both values and run this script. Then update section 10 of
# docs/otel-workflow-semconv/README.md.
semconv_commit="331541c4057c53d133c32efadaa7b656636eda77"
baseline_tag="v1.44.0"
semconv_repo="https://github.com/open-telemetry/semantic-conventions.git"
registry="${WEAVER_REGISTRY:-mirror.gcr.io}"

command -v docker >/dev/null || {
  echo "error: the script needs docker" >&2
  exit 1
}

if [ "${SEMCONV_LATEST:-0}" = "1" ]; then
  semconv_commit="$(git ls-remote "${semconv_repo}" refs/heads/main | cut -f1)"
  baseline_tag="$(git ls-remote --tags --refs "${semconv_repo}" 'v*' |
    sed 's#.*refs/tags/##' | sort -V | tail -n 1)"
fi

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
    echo "error: upstream names the Weaver image '${image}' with no digest" >&2
    exit 1
    ;;
esac

packages="$(sed -n 's/^WEAVER_PACKAGES_REPO=//p' "${semconv}/Makefile" | tr -d '[:space:]')"
packages_repo="${packages%@*}"
packages_ref="${packages##*@}"
if ! [[ "${packages_ref}" =~ ^[0-9a-f]{40}$ ]] || [ "${packages_repo}" = "${packages}" ]; then
  echo "error: upstream does not pin WEAVER_PACKAGES_REPO to a commit: '${packages}'" >&2
  exit 1
fi
clone_at "${packages_repo}" "${packages_ref}" "${work}/packages"

# Upstream filters some known findings in `.weaver.toml`. Older commits lack it.
config_mount=()
config_flag=()
if [ -f "${semconv}/.weaver.toml" ]; then
  config_mount=(--mount "type=bind,source=${semconv}/.weaver.toml,target=/home/weaver/.weaver.toml,readonly")
  config_flag=(--config=/home/weaver/.weaver.toml)
fi

# place <model directory>: put that model at its upstream path.
place() {
  rm -rf "${semconv}/model/workflow"
  cp -R "$1" "${semconv}/model/workflow"
}

# run_weaver: run the upstream checks on the placed model.
run_weaver() {
  local cache
  cache="$(mktemp -d "${work}/cache.XXXXXX")"
  docker run --rm --network none -u "$(id -u):$(id -g)" \
    --env USER=weaver --env HOME=/home/weaver \
    -v "${cache}:/home/weaver/.weaver" \
    --mount "type=bind,source=${semconv}/policies,target=/home/weaver/policies,readonly" \
    --mount "type=bind,source=${semconv}/model,target=/home/weaver/source,readonly" \
    --mount "type=bind,source=${work}/baseline/model,target=/home/weaver/baseline,readonly" \
    --mount "type=bind,source=${work}/packages,target=/home/weaver/packages,readonly" \
    ${config_mount[@]+"${config_mount[@]}"} \
    "${image}" registry check --v2 \
    ${config_flag[@]+"${config_flag[@]}"} \
    --registry=/home/weaver/source \
    --baseline-registry=/home/weaver/baseline \
    --policy=/home/weaver/policies \
    --policy=/home/weaver/packages/policies/check/naming_conventions \
    --policy=/home/weaver/packages/policies/check/stability \
    --policy=/home/weaver/packages/policies/check/entity_associations \
    --policy=/home/weaver/packages/policies/check/backwards-compatibility
}

echo "Upstream ${semconv_commit}, baseline ${baseline_tag}, image ${image}"
echo "Run 1: the model must pass with no warning."
place "${model}"
run_weaver 2>&1 | tee "${work}/model.log"
# Weaver wraps long lines, so join them before the search.
if tr -d '\n │' <"${work}/model.log" |
  grep -qE 'weaver/source/workflow/|span\.workflow\.|registry\.workflow'; then
  echo "error: Weaver reports a finding for the model" >&2
  exit 1
fi

echo "Run 2: the negative control must fail on a package policy."
broken="${work}/broken"
cp -R "${model}" "${broken}"
# The attributes list ends the registry, so an appended entry joins it.
# `workflow.signal.id.kind` makes `workflow.signal.id` a namespace.
cat >>"${broken}/registry.yaml" <<'YAML'
      - id: workflow.signal.id.kind
        type: string
        stability: development
        brief: A negative control.
        examples: ["x"]
YAML
place "${broken}"
if run_weaver >"${work}/broken.log" 2>&1; then
  cat "${work}/broken.log" >&2
  echo "error: the negative control passed, so run 1 proves nothing" >&2
  exit 1
fi
tr -d '\n │' <"${work}/broken.log" |
  grep -qF 'naming_convention_attribute_namespace_collision' || {
  cat "${work}/broken.log" >&2
  echo "error: the negative control failed for another reason" >&2
  exit 1
}
echo "The model passes the upstream policy checks."
