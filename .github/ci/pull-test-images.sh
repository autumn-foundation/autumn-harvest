#!/usr/bin/env bash
# Pre-pull the named Docker images, or the ones that this shard's `linux`
# manifest rows need.
#
# `testcontainers` retries nothing on a pull, so a registry blip fails a
# live-DB test. This script retries each pull. It reads SEMAPHORE_SHARD_COUNT
# and SEMAPHORE_SHARD_INDEX through `run-suites.sh`, as the suite step does.
# `ci.yml` (`test-db-linux`) and `coverage-nightly.yml` (`db`) call it.
#
# Every tag the suites actually request, not just the testcontainers-modules
# default (Codex review: the default alone left the suites that override it
# just as exposed as before):
# - `postgres:11-alpine`: the unoverridden default. `autumn-web`'s own
#   `TestDb` uses it, for example in the webhook suites.
# - `postgres:16`: the tag that about 220 of this repo's own DB suites
#   request via `.with_tag("16")`.
# - `postgres:16-alpine`: one outlier suite's override.
# - `redis:5.0`: the Redis default, overridden nowhere. The issue #1312
#   dispatch suites need it, and the jobs force them via
#   HARVEST_TEST_REQUIRE_REDIS.
# - `redis:7.4-alpine`: the TLS-only server of the issue #1834 `tls_redis`
#   suite. Redis 5 has no TLS.
# - `softwaremill/elasticmq:1.5.2`: only on the shard that runs the
#   `connector_sqs_broker` suite. "bytes remaining on stream" failed it on
#   PR #1773 because it was not pre-pulled.
# - The MinIO and fake-gcs-server emulators: only on the shards that run
#   their suites (issue #1983).
#
# Arguments name the images to pull instead. Then the script reads no
# shard. `dst-nightly.yml` passes `postgres:16` (issue #2280).
set -euo pipefail

if [ "$#" -gt 0 ]; then
  for image in "$@"; do
    if [ -z "$image" ]; then
      echo "::error::an image argument is empty"
      exit 1
    fi
  done
  images="$*"
else
  images="postgres:11-alpine postgres:16 postgres:16-alpine redis:5.0 redis:7.4-alpine"
  # Pull ElasticMQ only on the shard that runs `connector_sqs_broker`.
  # The dry run prints this shard's cargo invocations without running
  # them, so no shard number is hardcoded here. It is captured first:
  # `grep -q` closing a pipe early would trip `pipefail`.
  plan="$(DRY_RUN=1 bash .github/ci/run-suites.sh run linux)"
  if grep -q -- '--test connector_sqs_broker' <<<"$plan"; then
    images="$images softwaremill/elasticmq:1.5.2"
  fi
  # Issue #1983: the object-store emulators, only on the shards that
  # run their suites. The official MinIO images are no longer
  # published, so the suite uses the `pgsty/minio` community build.
  if grep -q -- '--test object_store_s3_minio' <<<"$plan"; then
    images="$images pgsty/minio:RELEASE.2026-08-04T00-00-00Z"
  fi
  if grep -q -- '--test object_store_gcs_emulator' <<<"$plan"; then
    images="$images fsouza/fake-gcs-server:1.56.1"
  fi
fi
# Docker Hub rate-limits anonymous pulls, and the ECR Public mirror
# has a data limit. An official image (no slash in its name) comes
# from the Google mirror first, then from ECR Public. The tag then
# gives it the name that the suites request.
mirrors="mirror.gcr.io/library public.ecr.aws/docker/library"
for image in $images; do
  for attempt in 1 2 3 4 5; do
    if [[ "$image" != */* ]]; then
      for mirror in $mirrors; do
        if docker pull "$mirror/$image"; then
          docker tag "$mirror/$image" "$image"
          continue 3
        fi
      done
    fi
    if docker pull "$image"; then
      continue 2
    fi
    echo "pull of $image failed (attempt $attempt/5)"
    sleep $((attempt * 5))
  done
  echo "::error::failed to pull $image after 5 attempts"
  exit 1
done
