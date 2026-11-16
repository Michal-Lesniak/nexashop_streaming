# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — Orders (Auto Loader → Delta)
# MAGIC Trigger: 30s. Zapis do `bronze.orders_raw`.

# COMMAND ----------

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType,
)

spark = SparkSession.builder.getOrCreate()

CATALOG            = spark.conf.get("nexashop.catalog", "nexashop")
BASE_PATH          = spark.conf.get("nexashop.base_path", "dbfs:/mnt/nexashop")
ORDERS_LANDING     = f"{BASE_PATH}/landing/orders/"
SCHEMA_LOCATION    = f"{BASE_PATH}/_schemas/bronze_orders"
CHECKPOINT         = f"{BASE_PATH}/_checkpoints/bronze_orders"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA  IF NOT EXISTS {CATALOG}.bronze")

orders_schema = StructType([
    StructField("order_id",        StringType(),    False),
    StructField("customer_id",     StringType(),    True),
    StructField("product_id",      StringType(),    True),
    StructField("quantity",        IntegerType(),   True),
    StructField("unit_price",      DoubleType(),    True),
    StructField("event_timestamp", TimestampType(), True),
    StructField("event_type",      StringType(),    True),
    StructField("session_id",      StringType(),    True),
    StructField("warehouse_id",    StringType(),    True),
])

# COMMAND ----------

raw = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format",          "json")
        .option("cloudFiles.schemaLocation",  SCHEMA_LOCATION)
        .option("cloudFiles.inferColumnTypes", "false")
        .schema(orders_schema)
        .load(ORDERS_LANDING)
)

enriched = (
    raw
    .withColumn("_ingestion_timestamp", F.current_timestamp())
    .withColumn("_source_file",         F.col("_metadata.file_path"))
    .withColumn(
        "_is_valid",
        F.col("order_id").isNotNull()
        & F.col("event_timestamp").isNotNull()
        & (F.coalesce(F.col("quantity"),   F.lit(0)) > 0)
        & (F.coalesce(F.col("unit_price"), F.lit(0)) > 0),
    )
    .withColumn("_partition_date", F.to_date("event_timestamp"))
    .withColumn("order_value",     F.col("quantity") * F.col("unit_price"))
)

# COMMAND ----------

query = (
    enriched.writeStream
        .format("delta")
        .option("checkpointLocation",       CHECKPOINT)
        .option("mergeSchema",              "true")
        .partitionBy("_partition_date")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .queryName("bronze_orders_raw")
        .toTable(f"{CATALOG}.bronze.orders_raw")
)

query.awaitTermination()
