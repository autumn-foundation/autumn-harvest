#!/usr/bin/env bash
# Fails if the README's cron-schedule snippet or its `ActivityFailure` retry
# snippet stop compiling against the real crates.
#
# Mechanism this guards against: both snippets are pasted from the README, the
# front door. The schedule snippet calls `.workflows(..)` and
# `.workflow_schedule(..)` on `autumn_web::app()`. `AppBuilder` has neither
# method: `HarvestPlugin` has no `workflow_schedule`, and `HarvestBuilder` is not
# the app builder. rustc reports `E0599: no method named workflows found for
# struct AppBuilder`, and nothing in the message points at `HarvestBuilder` or
# the CLI/HTTP schedule routes the README documents two paragraphs later. The
# retry snippet names `Duration` but imports only `autumn_harvest::prelude::*`,
# which does not re-export it (`E0433`).
#
# Usage: ./scripts/check-readme-front-door-snippets.sh

set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

example_name="_onramp_doc_check_readme_front_door"
example_path="autumn-harvest-plugin/examples/${example_name}.rs"
trap 'rm -f "$example_path"' EXIT
rm -f "$example_path"

# Print the first ```rust fence body of README.md that contains $1.
readme_block() {
  awk -v needle="$1" '
    /^```rust$/ { buf = ""; inb = 1; next }
    inb && /^```$/ { inb = 0; if (index(buf, needle)) { printf "%s", buf; exit } next }
    inb { buf = buf $0 "\n" }
  ' README.md
}

schedule="$(readme_block 'WorkflowSchedule::new(')"
retry="$(readme_block 'ActivityFailure::non_retryable')"

if [ -z "$schedule" ] || [ -z "$retry" ]; then
  echo "could not extract the README WorkflowSchedule block or the" \
    "ActivityFailure block; has the README been restructured? Update this guard." >&2
  exit 1
fi

{
  echo "#![allow(dead_code, unused)]"
  echo "mod schedule {"
  echo "use autumn_harvest::prelude::*;"
  echo "#[workflow]"
  echo "async fn daily_billing_report(ctx: &WorkflowContext, input: serde_json::Value) -> HarvestResult<()> { Ok(()) }"
  echo "fn wiring() -> Result<(), Box<dyn std::error::Error>> {"
  echo "$schedule"
  echo "Ok(()) }"
  echo "}"
  echo "mod retry {"
  echo "$retry"
  echo "}"
  echo "fn main() {}"
} >"$example_path"

if output="$(cargo check --quiet -p autumn-harvest-plugin --example "$example_name" 2>&1)"; then
  echo "OK: README schedule and retry snippets compile."
  exit 0
fi

errors="$(grep -c '^error\[' <<<"$output")"
echo "README front-door snippets do not compile ($errors error(s)):" >&2
echo "$output" >&2
exit 1
