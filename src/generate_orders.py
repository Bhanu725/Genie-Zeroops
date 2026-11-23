# Databricks notebook source
# Writes a batch of synthetic JSON orders to the UC landing volume.

# COMMAND ----------

import json
import random
import uuid
from datetime import datetime, timedelta, timezone

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "zeroops_poc")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`landing`")

landing_path = f"/Volumes/{catalog}/{schema}/landing/orders"
dbutils.fs.mkdirs(landing_path)

# COMMAND ----------

N_ORDERS = 500
COUNTRIES = ["IN", "US", "UK", "DE", "SG"]

now = datetime.now(timezone.utc)
records = []
for _ in range(N_ORDERS):
    ts = now - timedelta(seconds=random.randint(0, 3599))
    records.append({
        "order_id": str(uuid.uuid4()),
        "order_ts": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "country": random.choice(COUNTRIES),
        "order_amount": round(random.uniform(5, 500), 2),
        "promo_code": "DIWALI10" if random.random() < 0.25 else None,
    })

# COMMAND ----------

if records:
    file_path = f"{landing_path}/orders_{now.strftime('%Y%m%dT%H%M%S')}.json"
    dbutils.fs.put(file_path, "\n".join(json.dumps(r) for r in records), True)
    print(f"Wrote {len(records)} orders to {file_path}")
else:
    print("No orders generated")
