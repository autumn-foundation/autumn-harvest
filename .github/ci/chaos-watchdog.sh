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
# `chaos-watchdog.sh self-failed` reports a red watchdog run on its own issue.
# The workflow runs it in an `if: failure()` step. GitHub tells only the last
# editor of a cron about a failed scheduled run, so a red watchdog is otherwise
# silent. That issue has its own title, because the nightly can be green. The
# next clean watchdog run closes it.
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
failed_title="Chaos watchdog: a watchdog run failed"
footer="Opened by \`.github/workflows/chaos-watchdog.yml\` (issue #1790)."

# Prints the number of the open issue with title $1, or nothing. It reads
# every page of the REST listing, because the search index can lag a
# just-opened issue. The listing also holds pull requests, so jq drops them
# and keeps only an exact title match.
open_issue_titled() {
  gh api --paginate -X GET "repos/${repo}/issues" -f state=open -f per_page=100 \
    | jq -rs --arg title "$1" \
      '(add // []) | map(select(.pull_request == null and .title == $title))
       | .[0].number // empty'
}

# Comments on the open issue with title $1, or opens one with body $2.
post_alert() {
  local issue_title="$1"
  local body="$2"
  local issue
  issue="$(open_issue_titled "$issue_title")"
  if [ -n "$issue" ]; then
    gh issue comment "$issue" --repo "$repo" --body "$body"
    echo "chaos-watchdog: commented on #${issue}"
  else
    gh issue create --repo "$repo" --title "$issue_title" --body "$body"
    echo "chaos-watchdog: opened an issue: ${issue_title}"
  fi
}

# Closes the open issue with title $1, if there is one, with comment $2.
close_alert() {
  local issue
  issue="$(open_issue_titled "$1")"
  if [ -n "$issue" ]; then
    gh issue close "$issue" --repo "$repo" --comment "$2"
    echo "chaos-watchdog: closed #${issue}"
  fi
}

if [ "${1:-}" = self-failed ]; then
  run_url="${server}/${repo}/actions/runs/${GITHUB_RUN_ID:-unknown}"
  post_alert "$failed_title" "The chaos watchdog run failed: ${run_url}

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

close_alert "$failed_title" "A later watchdog run completed its check. Closing this issue."

if [ "$successes" -gt 0 ]; then
  echo "chaos-watchdog: ${successes} successful scheduled run(s) since ${since}"
  close_alert "$title" "A scheduled Chaos run succeeded since ${since}. Closing this alert."
  exit 0
fi

runs_url="${server}/${repo}/actions/workflows/chaos.yml?query=event%3Aschedule"
post_alert "$title" "No scheduled run of \`chaos.yml\` succeeded since ${since} (${window_hours} h).

The chaos suite is the only fault-injection check for crash convergence and scheduler exactly-once rules. Find the cause in the nightly runs: ${runs_url}

If the list is empty, the workflow did not start. Check that \`.github/workflows/chaos.yml\` parses and that its schedule is on.

${footer}"
