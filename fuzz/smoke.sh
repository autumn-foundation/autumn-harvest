#!/usr/bin/env bash
# Short local/manual smoke run of every fuzz target (~15s each). This is NOT a
# soak — it is a "does each target still build and run clean on a quick pass"
# check. For a real campaign, run a single target with a larger budget:
#
#   cargo +nightly fuzz run fuzz_det_check_source -- -max_total_time=300
#
# Requires a nightly toolchain and cargo-fuzz:
#   rustup toolchain install nightly
#   cargo install cargo-fuzz
set -euo pipefail

# Run from the fuzz/ crate directory regardless of caller cwd.
cd "$(dirname "$0")"

TARGETS=(
  fuzz_workflow_event_deser
  fuzz_det_check_source
  fuzz_validate_target_url
  fuzz_failure_signature
)

MAX_TIME="${MAX_TOTAL_TIME:-15}"

for t in "${TARGETS[@]}"; do
  echo "=== fuzzing ${t} for ${MAX_TIME}s ==="
  cargo +nightly fuzz run "${t}" -- -max_total_time="${MAX_TIME}"
done

echo "smoke run complete: all targets ran clean"
