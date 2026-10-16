#!/usr/bin/env bash
# Pre-pull the Docker images that this shard's `linux` manifest rows need.
#
# `testcontainers` retries nothing on a pull, so a registry blip fails a
# live-DB test. This script retries each pull. It reads SEMAPHORE_SHARD_COUNT
# and SEMAPHORE_SHARD_INDEX through `run-suites.sh`, as the suite step does.
# `ci.yml` (`test-db-linux`) and `coverage-nightly.yml` (`db`) call it. The
# `test-db-linux` comment above its call lists why each image is here.
set -euo pipefail

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
