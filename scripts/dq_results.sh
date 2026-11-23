#!/usr/bin/env bash
# Prints the latest run of the observability checks (dq_check_results).
set -euo pipefail
source "$(dirname "$0")/common.sh"

WH=$(databricks warehouses list -p "$PROFILE" -o json | python -c "import sys,json;print(json.load(sys.stdin)[0]['id'])")
WH="${WH%$'\r'}"
T="$CATALOG.$SCHEMA.dq_check_results"
SQL="SELECT check_ts, check_name, severity, status, observed, threshold, left(details,100) FROM $T WHERE check_ts = (SELECT max(check_ts) FROM $T) ORDER BY status DESC, check_name"
body="{\"warehouse_id\":\"$WH\",\"statement\":\"$SQL\",\"wait_timeout\":\"50s\"}"
databricks api post /api/2.0/sql/statements --json "$body" -p "$PROFILE" | python -c "
import sys,json
d=json.load(sys.stdin)
if d['status']['state']!='SUCCEEDED':
    print(d['status']); sys.exit(1)
for r in d['result']['data_array']:
    print(' | '.join(str(x) for x in r[1:]))"
