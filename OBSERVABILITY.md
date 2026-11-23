# Free observability layer (replacement for Genie ZeroOps)

Everything runs on serverless compute with built-in Databricks features. There is no vendor account.

## Pieces
1. **Pipeline expectations** (`src/orders_pipeline.py`):
   `order_id_not_null`, `order_amount_not_null` (drop), `amount_in_range`, `valid_country`, `positive_revenue`.
   Violations show in the pipeline UI and the event log.
2. **Job task `observability_checks`** (`src/observability_checks.py`), runs after the pipeline even if it failed
   (`run_if: ALL_DONE`). It writes every result to `workspace.zeroops_poc.dq_check_results` and **fails the job**
   if a CRITICAL check fails.
3. **Results table** `dq_check_results`: `check_ts, check_name, severity, status, observed, threshold, details`.
4. `./scripts/dq_results.sh` prints the latest run's results.

## Checks
| Check | Severity | Passes when |
|---|---|---|
| `bronze_freshness_minutes` | CRITICAL | newest bronze row is ≤ 90 min old |
| `<table>_row_count` (bronze, silver, gold) | CRITICAL | > 0 rows |
| `silver_to_bronze_ratio` | CRITICAL | silver / bronze ≥ 0.95 |
| `gold_matches_silver` | CRITICAL | sum of gold `order_count` = silver row count |
| `silver_orders_schema_contract`, `gold_daily_revenue_schema_contract` | CRITICAL | column names and types match the contract |
| `silver_amount_in_range` | WARN | no silver `order_amount` outside 5..500 |
| `pipeline_expectation_failures` | WARN | no expectation failures in the latest update |

Thresholds are job parameters in `databricks.yml` (`freshness_max_minutes`, `min_silver_ratio`).

## Expected detection per scenario
| # | Scenario | Detected by | Notes |
|---|---|---|---|
| 1 | Code bug | Pipeline failure; job fails. Checks task records contract/consistency FAILs for the stale gold. | Root cause (commit) is manual: read the failed update's error and `git log` for the deployed commit. |
| 2 | Schema change | Bronze fails with `failOnNewColumns` (job fails). Checks task records freshness FAIL after 90 min. | Error message names the new column `total_amount`. |
| 3 | Silent dropna | `silver_to_bronze_ratio` (~0.25) and `gold_matches_silver`. Job turns red from the checks task. | The pipeline itself stays green. Works immediately, no baseline history needed. |
| 4 | Stale table | `bronze_freshness_minutes` | Only fires once the newest data is over 90 min old, so it needs the schedule on or a later manual run. |
| 5 | Type overflow | Pipeline failure (ANSI cast error); contract check flags the type if the table updated. | Fix is manual. |

## What this does NOT do (vs ZeroOps)
No automatic root-cause analysis, no commit mapping and no fix PRs. Detection and alerting only.

## Alerting (optional, in the UI)
Job → Notifications → add email on failure. Because CRITICAL check failures fail the job, one notification covers
both loud failures and silent data-quality breaches.

## Results template
| Scenario | Detected? | Which check/signal | Time to detect | Notes |
|---|---|---|---|---|
| 1 Code bug | | | | |
| 2 Schema change | | | | |
| 3 Silent dropna | | | | |
| 4 Stale table | | | | |
| 5 Type overflow | | | | |
