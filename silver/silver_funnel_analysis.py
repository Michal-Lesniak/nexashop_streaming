# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — Funnel 1-min (COMPLETE mode → memory sink)
# MAGIC Źródło: `bronze.clickstream_raw`. Memory sink `funnel_live`.

# COMMAND ----------

from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()

CATALOG    = spark.conf.get("nexashop.catalog", "nexashop")
BASE_PATH  = spark.conf.get("nexashop.base_path", "dbfs:/mnt/nexashop")
CHECKPOINT = f"{BASE_PATH}/_checkpoints/silver_funnel_1min"

# COMMAND ----------

clicks = (
    spark.readStream.table(f"{CATALOG}.bronze.clickstream_raw")
        .filter("_is_valid = true")
)

def _count_pages_in_window():
    return (
        clicks.groupBy(F.window("event_timestamp", "1 minute").alias("w"))
        .agg(
            F.countDistinct(F.when(F.col("page_type") == "HOME",          F.col("session_id"))).alias("sessions_home"),
            F.countDistinct(F.when(F.col("page_type") == "PRODUCT",       F.col("session_id"))).alias("sessions_product"),
            F.countDistinct(F.when(F.col("page_type") == "CART",          F.col("session_id"))).alias("sessions_cart"),
            F.countDistinct(F.when(F.col("page_type") == "CHECKOUT",      F.col("session_id"))).alias("sessions_checkout"),
            F.countDistinct(F.when(F.col("page_type") == "ORDER_CONFIRM", F.col("session_id"))).alias("sessions_confirmed"),
        )
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "sessions_home",
            "sessions_product",
            "sessions_cart",
            "sessions_checkout",
            "sessions_confirmed",
        )
    )

funnel = _count_pages_in_window()

query = (
    funnel.writeStream
        .format("memory")
        .queryName("funnel_live")
        .outputMode("complete")
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime="10 seconds")
        .start()
)

# Przykładowy podgląd po uruchomieniu w notebooku:
# display(spark.sql("SELECT * FROM funnel_live ORDER BY window_start DESC LIMIT 20"))

query.awaitTermination()
