#!/bin/bash
# Apparatus for assay #19. Usage: walk.sh <stripper-binary> [crlf]
set -u
STRIP=$1; MODE=${2:-lf}
total=0; same=0; diff=0; skipped=0
for c in $(git log --format=%H origin/trunk-dev -- autumn-harvest-plugin/openapi.json); do
  if ! git cat-file -e "$c:autumn-harvest-plugin/openapi.json" 2>/dev/null || ! git cat-file -e "$c:docs/openapi.json" 2>/dev/null; then
    echo "$c SKIP(missing copy)"; skipped=$((skipped+1)); continue; fi
  total=$((total+1))
  if [ "$MODE" = crlf ]; then git show "$c:docs/openapi.json" | sed 's/$/\r/' > /tmp/p.json; else git show "$c:docs/openapi.json" > /tmp/p.json; fi
  got=$($STRIP < /tmp/p.json | sha256sum | cut -c1-16)
  want=$(git show "$c:autumn-harvest-plugin/openapi.json" | sha256sum | cut -c1-16)
  if [ "$got" = "$want" ]; then same=$((same+1)); r=SAME; else diff=$((diff+1)); r=DIFF; fi
  echo "$c $(git show -s --format=%ad --date=short $c) $r $want $got"
done
echo "mode=$MODE graded=$total identical=$same differing=$diff skipped=$skipped"
