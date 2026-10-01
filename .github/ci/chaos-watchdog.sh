#!/usr/bin/env bash
# Chaos nightly watchdog (issue #1790).
#
# Counts the successful scheduled runs of chaos.yml in the last 48 hours.
# With no success, it opens one alert issue, or comments on the open one.
# After a success, it closes the open alert issue.
#
# A failed `gh` call stops the script with a non-zero exit. The watchdog run
# then fails, so an API error never reads as "no runs" or as "green".
#
# Needs GH_TOKEN with `actions: read` and `issues: write`, and
# GITHUB_REPOSITORY. GITHUB_SERVER_URL defaults to https://github.com.
# GitHub Actions sets both.
set -euo pipefail

repo="${GITHUB_REPOSITORY:?GITHUB_REPOSITORY is not set}"
server="${GITHUB_SERVER_URL:-https://github.com}"
window_hours=48
title="Chaos nightly: no successful scheduled run in ${window_hours} h"
since="$(date -u -d "-${window_hours} hours" +%Y-%m-%dT%H:%M:%SZ)"

successes="$(gh api -X GET "repos/${repo}/actions/workflows/chaos.yml/runs" \
  -f event=schedule -f status=success -f "created=>=${since}" -f per_page=1 \
  --jq '.total_count')"
if ! [[ "$successes" =~ ^[0-9]+$ ]]; then
  echo "chaos-watchdog: run count is not a number: '${successes}'" >&2
  exit 1
fi

# Match the exact title. The search is only a pre-filter.
issue="$(gh issue list --repo "$repo" --state open \
  --search "\"${title}\" in:title" --json number,title \
  --jq "map(select(.title == \"${title}\")) | .[0].number // empty")"

if [ "$successes" -gt 0 ]; then
  echo "chaos-watchdog: ${successes} successful scheduled run(s) since ${since}"
  if [ -n "$issue" ]; then
    gh issue close "$issue" --repo "$repo" \
      --comment "A scheduled Chaos run succeeded since ${since}. Closing this alert."
  fi
  exit 0
fi

runs_url="${server}/${repo}/actions/workflows/chaos.yml?query=event%3Aschedule"
body="No scheduled run of \`chaos.yml\` succeeded since ${since} (${window_hours} h).

The chaos suite is the only fault-injection check for crash convergence and scheduler exactly-once rules. Find the cause in the nightly runs: ${runs_url}

If the list is empty, the workflow did not start. Check that \`.github/workflows/chaos.yml\` parses and that its schedule is on.

Opened by \`.github/workflows/chaos-watchdog.yml\` (issue #1790)."

if [ -n "$issue" ]; then
  gh issue comment "$issue" --repo "$repo" --body "$body"
  echo "chaos-watchdog: no success since ${since}; commented on #${issue}"
else
  gh issue create --repo "$repo" --title "$title" --body "$body"
  echo "chaos-watchdog: no success since ${since}; opened an alert issue"
fi
