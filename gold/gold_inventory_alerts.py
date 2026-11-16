# Databricks notebook source
# MAGIC %md
# MAGIC # BONUS A — Inventory low-stock alerts
# MAGIC Źródło: syntetyczny stream (patrz `data_generator/inventory_generator.py`).
# MAGIC Trigger: `availableNow` — przetwarza partie batchowo.

# COMMAND ----------

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType,
)
from delta.tables import DeltaTable

spark = SparkSession.builder.getOrCreate()

CATALOG      = spark.conf.get("nexashop.catalog", "nexashop")
BASE_PATH    = spark.conf.get("nexashop.base_path", "dbfs:/mnt/nexashop")
LANDING      = f"{BASE_PATH}/landing/inventory/"
SCHEMA_LOC   = f"{BASE_PATH}/_schemas/inventory"
CHECKPOINT   = f"{BASE_PATH}/_checkpoints/gold_low_stock"
TABLE        = f"{CATALOG}.gold.low_stock_alerts"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.gold")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        product_id    STRING,
        warehouse_id  STRING,
        current_stock INT,
        alert_at      TIMESTAMP
    ) USING DELTA
""")

inventory_schema = StructType([
    StructField("product_id",    StringType(),    False),
    StructField("current_stock", IntegerType(),   True),
    StructField("warehouse_id",  StringType(),    True),
    StructField("timestamp",     TimestampType(), True),
])

low_stock = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format",         "json")
        .option("cloudFiles.schemaLocation", SCHEMA_LOC)
        .schema(inventory_schema)
        .load(LANDING)
        .filter("current_stock IS NOT NULL AND current_stock < 5")
        .withColumn("alert_at", F.current_timestamp())
        .select("product_id", "warehouse_id", "current_stock", "alert_at")
)


def _merge(batch_df, batch_id: int) -> None:
    if batch_df.rdd.isEmpty():
        return
    tgt = DeltaTable.forName(spark, TABLE)
    (
        tgt.alias("t")
        .merge(batch_df.alias("s"), "t.product_id = s.product_id AND t.warehouse_id = s.warehouse_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


query = (
    low_stock.writeStream
        .foreachBatch(_merge)
        .option("checkpointLocation", CHECKPOINT)
        .trigger(availableNow=True)
        .queryName("gold_low_stock_alerts")
        .start()
)

query.awaitTermination()
