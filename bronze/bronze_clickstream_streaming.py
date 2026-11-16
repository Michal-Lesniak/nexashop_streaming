# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — Clickstream (Auto Loader → Delta)
# MAGIC Trigger: 10s. Partycjonowanie: date + hour.

# COMMAND ----------

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
)

spark = SparkSession.builder.getOrCreate()

CATALOG         = spark.conf.get("nexashop.catalog", "nexashop")
BASE_PATH       = spark.conf.get("nexashop.base_path", "dbfs:/mnt/nexashop")
LANDING         = f"{BASE_PATH}/landing/clickstream/"
SCHEMA_LOCATION = f"{BASE_PATH}/_schemas/bronze_clickstream"
CHECKPOINT      = f"{BASE_PATH}/_checkpoints/bronze_clickstream"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.bronze")

click_schema = StructType([
    StructField("event_id",        StringType(),    False),
    StructField("session_id",      StringType(),    True),
    StructField("customer_id",     StringType(),    True),
    StructField("page_type",       StringType(),    True),
    StructField("product_id",      StringType(),    True),
    StructField("action",          StringType(),    True),
    StructField("event_timestamp", TimestampType(), True),
    StructField("device_type",     StringType(),    True),
])

# COMMAND ----------

raw = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format",          "json")
        .option("cloudFiles.schemaLocation",  SCHEMA_LOCATION)
        .option("cloudFiles.maxFilesPerTrigger", "500")
        .schema(click_schema)
        .load(LANDING)
)

PAGE_TYPES_ALLOWED = ["HOME", "PRODUCT", "CART", "CHECKOUT", "ORDER_CONFIRM"]

enriched = (
    raw
    .withColumn("_ingestion_timestamp", F.current_timestamp())
    .withColumn("_source_file",         F.col("_metadata.file_path"))
    .withColumn(
        "_is_valid",
        F.col("event_id").isNotNull()
        & F.col("session_id").isNotNull()
        & F.col("event_timestamp").isNotNull()
        & F.col("page_type").isin(PAGE_TYPES_ALLOWED),
    )
    .withColumn("_partition_date", F.to_date("event_timestamp"))
    .withColumn("_partition_hour", F.hour("event_timestamp"))
)

# COMMAND ----------

query = (
    enriched.writeStream
        .format("delta")
        .option("checkpointLocation", CHECKPOINT)
        .option("mergeSchema",        "true")
        .partitionBy("_partition_date", "_partition_hour")
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .queryName("bronze_clickstream_raw")
        .toTable(f"{CATALOG}.bronze.clickstream_raw")
)

query.awaitTermination()
