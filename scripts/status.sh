#!/usr/bin/env bash
# Shows latest job run + pipeline update status and row counts of the 3 tables.
set -euo pipefail
source "$(dirname "$0")/common.sh"

SUMMARY=$(databricks bundle summary -t "$TARGET" -p "$PROFILE" -o json)
read -r JOB_ID PIPE_ID < <(echo "$SUMMARY" | python -c "import sys,json;r=json.load(sys.stdin)['resources'];print(r['jobs']['zeroops_orders_job']['id'], r['pipelines']['orders_pipeline']['id'])")

JOB_ID="${JOB_ID%$'\r'}"; PIPE_ID="${PIPE_ID%$'\r'}"
echo "== Latest job runs =="
databricks jobs list-runs --job-id "$JOB_ID" --limit 3 -p "$PROFILE" -o json | python -c "
import sys,json
d=json.load(sys.stdin)
runs=d if isinstance(d,list) else d.get('runs',[])
for r in runs:
    s=r.get('state',{})
    print(r['run_id'], s.get('life_cycle_state'), s.get('result_state'), s.get('state_message',''))"

echo "== Latest pipeline updates =="
databricks pipelines list-updates "$PIPE_ID" --max-results 3 -p "$PROFILE" -o json | python -c "
import sys,json
for u in json.load(sys.stdin).get('updates',[]):
    print(u['update_id'], u.get('state'), u.get('creation_time'))"

echo "== Row counts =="
WH=$(databricks warehouses list -p "$PROFILE" -o json | python -c "import sys,json;print(json.load(sys.stdin)[0]['id'])")
WH="${WH%$'\r'}"
for t in bronze_orders silver_orders gold_daily_revenue; do
  body="{\"warehouse_id\":\"$WH\",\"statement\":\"SELECT count(*) FROM $CATALOG.$SCHEMA.$t\",\"wait_timeout\":\"50s\"}"
  printf "%-20s " "$t"
  databricks api post /api/2.0/sql/statements --json "$body" -p "$PROFILE" | python -c "
import sys,json
d=json.load(sys.stdin)
print(d['result']['data_array'][0][0] if d['status']['state']=='SUCCEEDED' else d['status'])"
done
