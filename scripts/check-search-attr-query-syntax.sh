#!/usr/bin/env bash
# Fails if any documented `search_attr=` query string uses `key=value`
# instead of the API's actual `key:value` separator.
#
# Mechanism this guards against: `parse_workflow_filters` in
# autumn-harvest-plugin/src/api.rs parses each `search_attr` query value with
# `value.split_once(':')` and 400s with "invalid search_attr '...'; expected
# 'key:value'" when no colon is present. README.md documents the correct
# `?search_attr=tenant:acme` shape, but examples/billing-autumn-web/README.md
# — the repository's own "reference example for the less tiny path" and the
# first thing a reader building a real integration is pointed at — gave
# `search_attr=tenant_id=acme` (an `=`, matching the surrounding URL's own
# `key=value` pairs, not the API's `key:value` shape) in the very curl a
# reader is told to run right after starting a checkout. Reproduced live: that
# exact command against a running billing-autumn-web returns
# `400 Bad Request`. No CI job runs this example, so nothing else catches this
# class of drift.
#
# Usage: ./scripts/check-search-attr-query-syntax.sh

set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

violations=0

while IFS=: read -r file line_no line; do
  # Every `search_attr=<value>` occurrence in a documented query string.
  # `key:value` is correct; anything else (starting with `=`, or with an `=`
  # before the first `:`) is the defect class this guards against.
  while read -r match; do
    value="${match#search_attr=}"
    # A bare `search_attr=` mention with no runnable value after it (a
    # backtick, end of sentence, …) is prose about the param, not a
    # copy-pasteable example — nothing to check.
    if [ -z "$value" ]; then
      continue
    fi
    if [[ "$value" =~ ^[^:=]+: ]]; then
      continue
    fi
    echo "$file:$line_no: 'search_attr=$value' does not match the API's" \
      "'key:value' shape (parse_workflow_filters splits on ':', not '=')." >&2
    violations=$((violations + 1))
  done < <(grep -oE "search_attr=[^&'\"\` ]*" <<<"$line")
done < <(grep -rnE "search_attr=" --include="*.md" . 2>/dev/null | grep -vE "^\./target/")

if [ "$violations" -gt 0 ]; then
  echo >&2
  echo "Fix: use ':' to separate the search-attr key from its value," \
    "e.g. 'search_attr=tenant_id:acme'." >&2
  exit 1
fi

echo "OK: every documented search_attr= query string uses 'key:value'."
