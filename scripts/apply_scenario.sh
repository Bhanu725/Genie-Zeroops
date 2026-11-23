#!/usr/bin/env bash
# Usage: ./scripts/apply_scenario.sh N   (N = 1..5)
# Creates branch scenario-N from main, applies the patch, commits, pushes,
# deploys from that branch and triggers a job run. Never commits on main.
set -euo pipefail
source "$(dirname "$0")/common.sh"

N="${1:?usage: apply_scenario.sh N (1-5)}"
PATCH=$(ls scenarios/0${N}_*.patch 2>/dev/null | head -1 || true)
[ -n "$PATCH" ] || { echo "No patch for scenario $N"; exit 1; }

case "$N" in
  1) MSG="Rename country to customer_country in silver_orders" ;;
  2) MSG="Generator: emit total_amount as string instead of order_amount" ;;
  3) MSG="silver_orders: drop rows with any null column" ;;
  4) MSG="Generator: stop emitting orders" ;;
  5) MSG="silver_orders: cast order_amount to tinyint" ;;
  *) echo "Scenario must be 1-5"; exit 1 ;;
esac

if ! git diff --quiet || ! git diff --cached --quiet; then
  echo "Working tree has uncommitted changes. Commit or stash first."; exit 1
fi

git fetch origin main --quiet
git checkout -B "scenario-$N" origin/main
git apply --whitespace=nowarn "$PATCH"
git add -A src
git commit -m "$MSG"
git push -u origin "scenario-$N" --force-with-lease

databricks bundle validate -t "$TARGET" -p "$PROFILE"
databricks bundle deploy -t "$TARGET" -p "$PROFILE"
echo "Triggering job run for scenario $N ..."
databricks bundle run zeroops_orders_job -t "$TARGET" -p "$PROFILE" || echo "Run ended with a failure (expected for scenarios 1, 2, 5)."
echo "Done. Use ./scripts/status.sh to check state. Return to baseline with ./scripts/reset_baseline.sh"
