# Databricks notebook source
# Free observability layer: freshness, volume/completeness, consistency, schema
# contract and pipeline-expectation metrics. Results are appended to
# <catalog>.<schema>.dq_check_results. The task fails if any CRITICAL check fails.

# COMMAND ----------

from datetime import datetime, timezone

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "zeroops_poc")
dbutils.widgets.text("pipeline_id", "")
dbutils.widgets.text("freshness_max_minutes", "90")
dbutils.widgets.text("min_silver_ratio", "0.95")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
pipeline_id = dbutils.widgets.get("pipeline_id")
FRESHNESS_MAX_MIN = float(dbutils.widgets.get("freshness_max_minutes"))
MIN_SILVER_RATIO = float(dbutils.widgets.get("min_silver_ratio"))


def fq(table):
    return f"`{catalog}`.`{schema}`.`{table}`"


run_ts = datetime.now(timezone.utc)
results = []


def record(name, severity, ok, observed, threshold, details=""):
    results.append((run_ts, name, severity, "PASS" if ok else "FAIL",
                    str(observed), str(threshold), details[:500]))


def check(name, severity, threshold, fn):
    """fn returns (ok, observed, details). Any exception is recorded as a FAIL."""
    try:
        ok, observed, details = fn()
        record(name, severity, ok, observed, threshold, details)
    except Exception as e:  # noqa: BLE001
        record(name, severity, False, "error", threshold, f"{type(e).__name__}: {e}")


def scalar(sql):
    return spark.sql(sql).first()[0]

# COMMAND ----------
# Freshness: minutes since the newest bronze row was ingested.


def _freshness():
    lag = scalar(
        "SELECT (unix_timestamp(current_timestamp()) - unix_timestamp(max(_ingested_at))) / 60 "
        f"FROM {fq('bronze_orders')}")
    ok = lag is not None and lag <= FRESHNESS_MAX_MIN
    return ok, None if lag is None else round(lag, 1), "minutes since last bronze ingestion"


check("bronze_freshness_minutes", "CRITICAL", f"<= {FRESHNESS_MAX_MIN}", _freshness)

# COMMAND ----------
# Volume / completeness: tables non-empty, and silver keeps most of bronze.


def _non_empty(table):
    def run():
        c = scalar(f"SELECT count(*) FROM {fq(table)}")
        return c > 0, c, ""
    return run


for t in ["bronze_orders", "silver_orders", "gold_daily_revenue"]:
    check(f"{t}_row_count", "CRITICAL", "> 0", _non_empty(t))


def _ratio():
    b = scalar(f"SELECT count(*) FROM {fq('bronze_orders')}")
    s = scalar(f"SELECT count(*) FROM {fq('silver_orders')}")
    r = (s / b) if b else 0.0
    return r >= MIN_SILVER_RATIO, round(r, 4), f"silver={s}, bronze={b}"


check("silver_to_bronze_ratio", "CRITICAL", f">= {MIN_SILVER_RATIO}", _ratio)

# COMMAND ----------
# Consistency: gold must account for every silver row. Value sanity in silver.


def _gold_consistency():
    g = scalar(f"SELECT coalesce(sum(order_count), 0) FROM {fq('gold_daily_revenue')}")
    s = scalar(f"SELECT count(*) FROM {fq('silver_orders')}")
    return g == s, g, f"gold order_count sum={g}, silver rows={s}"


check("gold_matches_silver", "CRITICAL", "== silver rows", _gold_consistency)


def _amount_range():
    bad = scalar(f"SELECT count(*) FROM {fq('silver_orders')} WHERE order_amount < 5 OR order_amount > 500")
    return bad == 0, bad, "rows outside 5..500"


check("silver_amount_in_range", "WARN", "== 0", _amount_range)

# COMMAND ----------
# Schema contract: column names and types of silver and gold.

CONTRACT = {
    "silver_orders": {"order_id": "STRING", "order_ts": "TIMESTAMP", "country": "STRING",
                      "order_amount": "DECIMAL", "promo_code": "STRING", "_ingested_at": "TIMESTAMP"},
    "gold_daily_revenue": {"order_date": "DATE", "country": "STRING",
                           "revenue": "DECIMAL", "order_count": "LONG"},
}


def _contract(table):
    def run():
        rows = spark.sql(
            f"SELECT column_name, data_type FROM `{catalog}`.information_schema.columns "
            f"WHERE table_schema = '{schema}' AND table_name = '{table}'").collect()
        actual = {r.column_name: r.data_type for r in rows}
        expected = CONTRACT[table]
        missing = sorted(set(expected) - set(actual))
        extra = sorted(set(actual) - set(expected))
        wrong = sorted(c for c in expected if c in actual and actual[c] != expected[c])
        ok = not (missing or extra or wrong)
        details = f"missing={missing} extra={extra} type_mismatch={[(c, actual[c]) for c in wrong]}"
        return ok, "match" if ok else "drift", details
    return run


for t in CONTRACT:
    check(f"{t}_schema_contract", "CRITICAL", "exact match", _contract(t))

# COMMAND ----------
# Pipeline expectation metrics from the event log (latest update).


def _expectations():
    if not pipeline_id:
        return True, "skipped", "no pipeline_id"
    rows = spark.sql(f"""
        WITH latest AS (
          SELECT origin.update_id AS uid FROM event_log('{pipeline_id}')
          WHERE event_type = 'create_update' ORDER BY timestamp DESC LIMIT 1),
        exp AS (
          SELECT explode(from_json(details:flow_progress.data_quality.expectations,
                 'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>')) AS e
          FROM event_log('{pipeline_id}')
          WHERE event_type = 'flow_progress'
            AND origin.update_id = (SELECT uid FROM latest)
            AND details:flow_progress.data_quality.expectations IS NOT NULL)
        SELECT e.name AS name, sum(e.failed_records) AS failed, sum(e.passed_records) AS passed
        FROM exp GROUP BY e.name""").collect()
    total_failed = sum((r.failed or 0) for r in rows)
    detail = "; ".join(f"{r.name}: failed={r.failed} passed={r.passed}" for r in rows) or "no expectation metrics"
    return total_failed == 0, total_failed, detail


check("pipeline_expectation_failures", "WARN", "== 0", _expectations)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, TimestampType

spark.sql(f"""CREATE TABLE IF NOT EXISTS {fq('dq_check_results')} (
    check_ts TIMESTAMP, check_name STRING, severity STRING, status STRING,
    observed STRING, threshold STRING, details STRING)""")
out_schema = StructType(
    [StructField("check_ts", TimestampType())]
    + [StructField(n, StringType()) for n in ["check_name", "severity", "status", "observed", "threshold", "details"]])
out = spark.createDataFrame(results, out_schema)
out.write.mode("append").saveAsTable(f"{catalog}.{schema}.dq_check_results")
display(out)

critical = [r for r in results if r[2] == "CRITICAL" and r[3] == "FAIL"]
if critical:
    raise Exception("CRITICAL data-quality checks failed: "
                    + ", ".join(f"{r[1]} (observed {r[4]}; {r[6]})" for r in critical))
print("All critical checks passed.")
