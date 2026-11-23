from pyspark import pipelines as dp
from pyspark.sql import functions as F

LANDING_PATH = spark.conf.get("landing_path")


@dp.table(name="bronze_orders", comment="Raw orders ingested with Auto Loader")
def bronze_orders():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "failOnNewColumns")
        .load(LANDING_PATH)
        .withColumn("_ingested_at", F.current_timestamp())
    )


@dp.materialized_view(name="silver_orders", comment="Typed, deduplicated orders")
@dp.expect_or_drop("order_amount_not_null", "order_amount IS NOT NULL")
def silver_orders():
    return (
        spark.read.table("bronze_orders")
        .select(
            "order_id",
            F.to_timestamp("order_ts").alias("order_ts"),
            F.col("country"),
            F.col("order_amount").cast("decimal(12,2)").alias("order_amount"),
            "promo_code",
            "_ingested_at",
        )
        .dropDuplicates(["order_id"])
    )


@dp.materialized_view(name="gold_daily_revenue", comment="Daily revenue by country")
def gold_daily_revenue():
    return (
        spark.read.table("silver_orders")
        .groupBy(F.to_date("order_ts").alias("order_date"), F.col("country"))
        .agg(
            F.sum("order_amount").alias("revenue"),
            F.count("*").alias("order_count"),
        )
    )
