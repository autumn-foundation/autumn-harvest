#!/usr/bin/env bash
# Chaos nightly watchdog (issue #1790).
#
# Counts the successful scheduled runs of chaos.yml in the last 48 hours.
# With no success, it opens one alert issue, or comments on the open one.
# After a success, it closes the open alert issue.
#
# A failed `gh` call or a reply with no run count stops the script with a
# non-zero exit. An API error then never reads as "no runs" or as "green".
#
# `chaos-watchdog.sh self-failed` reports a red watchdog run on the alert
# issue. The workflow runs it in an `if: failure()` step. GitHub tells only
# the last editor of a cron about a failed scheduled run, so a red watchdog
# is otherwise silent.
#
# Needs GH_TOKEN with `actions: read` and `issues: write`, GITHUB_REPOSITORY,
# and `jq`. GITHUB_SERVER_URL defaults to https://github.com. GitHub Actions
# sets both variables, and the ubuntu-latest runner has `jq`.
set -euo pipefail
shopt -s inherit_errexit

repo="${GITHUB_REPOSITORY:?GITHUB_REPOSITORY is not set}"
server="${GITHUB_SERVER_URL:-https://github.com}"
window_hours=48
title="Chaos nightly: no successful scheduled run in ${window_hours} h"
footer="Opened by \`.github/workflows/chaos-watchdog.yml\` (issue #1790)."

# Prints the number of the open alert issue, or nothing. The search is fuzzy,
# so jq keeps only an exact title match.
open_alert_issue() {
  gh issue list --repo "$repo" --state open \
    --search "\"${title}\" in:title" --json number,title \
    | jq -r --arg title "$title" \
      'map(select(.title == $title)) | .[0].number // empty'
}

# Comments on the open alert issue, or opens one when none is open.
post_alert() {
  local body="$1"
  local issue
  issue="$(open_alert_issue)"
  if [ -n "$issue" ]; then
    gh issue comment "$issue" --repo "$repo" --body "$body"
    echo "chaos-watchdog: commented on #${issue}"
  else
    gh issue create --repo "$repo" --title "$title" --body "$body"
    echo "chaos-watchdog: opened an alert issue"
  fi
}

if [ "${1:-}" = self-failed ]; then
  run_url="${server}/${repo}/actions/runs/${GITHUB_RUN_ID:-unknown}"
  post_alert "The chaos watchdog run failed: ${run_url}

Until it is fixed, nothing checks that the nightly chaos run succeeds.

${footer}"
  exit 0
fi

since="$(date -u -d "-${window_hours} hours" +%Y-%m-%dT%H:%M:%SZ)"
successes="$(gh api -X GET "repos/${repo}/actions/workflows/chaos.yml/runs" \
  -f event=schedule -f status=success -f "created=>=${since}" -f per_page=1 \
  | jq -r '.total_count')"
if ! [[ "$successes" =~ ^[0-9]+$ ]]; then
  echo "chaos-watchdog: the run query gave no count: '${successes}'" >&2
  exit 1
fi

if [ "$successes" -gt 0 ]; then
  echo "chaos-watchdog: ${successes} successful scheduled run(s) since ${since}"
  issue="$(open_alert_issue)"
  if [ -n "$issue" ]; then
    gh issue close "$issue" --repo "$repo" \
      --comment "A scheduled Chaos run succeeded since ${since}. Closing this alert."
  fi
  exit 0
fi

runs_url="${server}/${repo}/actions/workflows/chaos.yml?query=event%3Aschedule"
post_alert "No scheduled run of \`chaos.yml\` succeeded since ${since} (${window_hours} h).

The chaos suite is the only fault-injection check for crash convergence and scheduler exactly-once rules. Find the cause in the nightly runs: ${runs_url}

If the list is empty, the workflow did not start. Check that \`.github/workflows/chaos.yml\` parses and that its schedule is on.

${footer}"
