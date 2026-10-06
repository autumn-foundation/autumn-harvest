#!/usr/bin/env bash
# Daily RUSTSEC advisory scan (issue #1826).
#
# New advisories land with no code change. The CI gate runs `cargo deny` on
# code changes only, so it does not see them. This script runs the advisory
# check on a schedule.
#
# The exit code is the exit code of `cargo deny`. With ADVISORY_ALERT=true, the
# script also writes to the alert issue:
#
# - A failed scan opens the alert issue, or comments on the open one. The body
#   lists each RUSTSEC id in the output and quotes the tail of the output.
# - A clean scan closes the open alert issue.
#
# After it writes the alert or the clean result, the script sets the step
# output `reported=true`. Any other red run has no report.
# `advisory-scan.sh run-failed` reports such a run. GitHub tells only the last
# editor of a cron about a failed scheduled run, so that run is otherwise
# silent.
#
# A failed `gh` call stops the script with a non-zero exit. An API error then
# never reads as "no open alert".
#
# Needs GH_TOKEN with `issues: write`, GITHUB_REPOSITORY, `jq`, and
# `cargo-deny` on PATH. GITHUB_SERVER_URL defaults to https://github.com.
set -euo pipefail
shopt -s inherit_errexit

repo="${GITHUB_REPOSITORY:?GITHUB_REPOSITORY is not set}"
server="${GITHUB_SERVER_URL:-https://github.com}"
run_url="${server}/${repo}/actions/runs/${GITHUB_RUN_ID:-unknown}"
title="Advisory scan: cargo deny check advisories failed"
footer="Opened by \`.github/workflows/advisory-scan.yml\` (issue #1826)."
# GitHub caps an issue body at 65536 characters. Quote at most this many bytes
# of scan output, which leaves room for the rest of the body.
max_quote=60000

# Prints the number of the open issue with title $1, or nothing. It reads
# every page of the REST listing, because the search index can lag a
# just-opened issue. Only issues that this workflow opened count, so an issue
# that a person opens with the same title is not commented on or closed. The
# listing also holds pull requests, so jq drops them and keeps only an exact
# title match.
open_issue_titled() {
  gh api --paginate -X GET "repos/${repo}/issues" -f state=open -f per_page=100 \
    -f "creator=github-actions[bot]" \
    | jq -rs --arg title "$1" \
      '(add // []) | map(select(.pull_request == null and .title == $title))
       | .[0].number // empty'
}

# Comments on the open alert issue, or opens one, with the body in file $1.
post_alert() {
  local issue
  issue="$(open_issue_titled "$title")"
  if [ -n "$issue" ]; then
    gh issue comment "$issue" --repo "$repo" --body-file "$1"
    echo "advisory-scan: commented on #${issue}"
  else
    gh issue create --repo "$repo" --title "$title" --body-file "$1"
    echo "advisory-scan: opened an issue: ${title}"
  fi
}

# Sets the step output that tells the workflow this run has reported.
mark_reported() {
  if [ -n "${GITHUB_OUTPUT:-}" ]; then
    echo "reported=true" >>"$GITHUB_OUTPUT"
  fi
}

case "${1:-}" in
  "" | run-failed) ;;
  *)
    echo "advisory-scan: unknown argument: $1" >&2
    exit 2
    ;;
esac

body="$(mktemp)"
log="$(mktemp)"
trap 'rm -f "$body" "$log"' EXIT

if [ "${1:-}" = run-failed ]; then
  cat >"$body" <<EOF
The advisory scan failed before it reported a result: ${run_url}

Until it is fixed, nothing checks for new RUSTSEC advisories.

${footer}
EOF
  post_alert "$body"
  exit 0
fi

status=0
cargo deny --all-features --color never check advisories >"$log" 2>&1 || status=$?
cat "$log"

if [ "${ADVISORY_ALERT:-false}" != true ]; then
  exit "$status"
fi

if [ "$status" -eq 0 ]; then
  issue="$(open_issue_titled "$title")"
  if [ -n "$issue" ]; then
    gh issue close "$issue" --repo "$repo" \
      --comment "A later scan found no advisory: ${run_url}"
    echo "advisory-scan: closed #${issue}"
  fi
  mark_reported
  exit 0
fi

ids="$({ grep -oE 'RUSTSEC-[0-9]{4}-[0-9]{4}' "$log" || true; } | sort -u | paste -sd ' ' -)"
{
  echo "\`cargo deny check advisories\` failed with exit code ${status}: ${run_url}"
  echo
  echo "RUSTSEC ids in the output: ${ids:-none. Read the output below.}"
  echo
  echo "Fix each finding, or add a reasoned \`ignore\` entry to \`deny.toml\`."
  echo
  echo '````text'
  tail -c "$max_quote" "$log"
  echo '````'
  echo
  echo "$footer"
} >"$body"
post_alert "$body"
mark_reported
exit "$status"
