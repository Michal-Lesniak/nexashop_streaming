# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — Revenue per product (5-min tumbling window)
# MAGIC Źródło: `bronze.orders_raw`. Watermark 10 min, append, trigger 30s.

# COMMAND ----------

from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()

CATALOG    = spark.conf.get("nexashop.catalog", "nexashop")
BASE_PATH  = spark.conf.get("nexashop.base_path", "dbfs:/mnt/nexashop")
CHECKPOINT = f"{BASE_PATH}/_checkpoints/silver_revenue_5min"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver")

# COMMAND ----------

orders = (
    spark.readStream.table(f"{CATALOG}.bronze.orders_raw")
        .filter("_is_valid = true AND event_type = 'CREATED'")
        .withColumn("order_value", F.col("quantity") * F.col("unit_price"))
        .withWatermark("event_timestamp", "10 minutes")
)

agg = (
    orders.groupBy(
        F.window("event_timestamp", "5 minutes").alias("w"),
        F.col("product_id"),
    )
    .agg(
        F.sum("order_value").alias("revenue"),
        F.count("order_id").alias("orders_count"),
        F.avg("order_value").alias("avg_order_value"),
        F.countDistinct("customer_id").alias("unique_customers"),
    )
    .select(
        F.col("w.start").alias("window_start"),
        F.col("w.end").alias("window_end"),
        "product_id",
        "revenue",
        "orders_count",
        "avg_order_value",
        "unique_customers",
    )
)

# COMMAND ----------

query = (
    agg.writeStream
        .format("delta")
        .option("checkpointLocation", CHECKPOINT)
        .option("mergeSchema", "true")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .queryName("silver_revenue_by_product_5min")
        .toTable(f"{CATALOG}.silver.revenue_by_product_5min")
)

# Metryki per-query są zbierane przez `MetricsListener` z gold/gold_streaming_metrics.py
# (StreamingQueryListener zarejestrowany globalnie na SparkSession).
query.awaitTermination()
