#!/usr/bin/env bash
# Back to clean baseline: checks out main and redeploys it.
# Optional: --purge-landing  deletes all landing files and fully refreshes the
# pipeline (needed after scenario 2, whose bad file keeps failing bronze).
set -euo pipefail
source "$(dirname "$0")/common.sh"

if ! git diff --quiet || ! git diff --cached --quiet; then
  echo "Uncommitted changes present. Commit or stash first."; exit 1
fi
git fetch origin main --quiet
git checkout main
git merge --ff-only origin/main
databricks bundle deploy -t "$TARGET" -p "$PROFILE"

if [ "${1:-}" = "--purge-landing" ]; then
  LANDING="dbfs:/Volumes/$CATALOG/$SCHEMA/landing/orders"
  read -r -p "Delete ALL synthetic files in $LANDING and full-refresh the pipeline? [y/N] " ans
  [ "$ans" = "y" ] || { echo "Skipped purge."; exit 0; }
  databricks fs rm -r "$LANDING" -p "$PROFILE" || true
  databricks fs mkdir "$LANDING" -p "$PROFILE"
  PID=$(databricks bundle summary -t "$TARGET" -p "$PROFILE" -o json | python -c "import sys,json;print(json.load(sys.stdin)['resources']['pipelines']['orders_pipeline']['id'])")
  databricks pipelines start-update "$PID" --full-refresh-all -p "$PROFILE"
fi
echo "Baseline deployed from main."
