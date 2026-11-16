# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — Attribution (stream-stream join orders ⋈ clickstream)
# MAGIC Okno join: klik 0–30 min przed zamówieniem. Watermark 30 min po obu stronach.

# COMMAND ----------

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

spark = SparkSession.builder.getOrCreate()

CATALOG    = spark.conf.get("nexashop.catalog", "nexashop")
BASE_PATH  = spark.conf.get("nexashop.base_path", "dbfs:/mnt/nexashop")
CHECKPOINT = f"{BASE_PATH}/_checkpoints/silver_attribution"
TABLE      = f"{CATALOG}.silver.attribution_events"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        order_id                     STRING,
        customer_id                  STRING,
        product_id                   STRING,
        order_value                  DOUBLE,
        session_id                   STRING,
        device_type                  STRING,
        seconds_from_click_to_order  BIGINT,
        order_ts                     TIMESTAMP,
        click_ts                     TIMESTAMP
    ) USING DELTA
""")

# COMMAND ----------

orders = (
    spark.readStream.table(f"{CATALOG}.bronze.orders_raw")
        .filter("_is_valid = true AND event_type = 'CREATED'")
        .withColumn("order_value", F.col("quantity") * F.col("unit_price"))
        .withColumnRenamed("event_timestamp", "order_ts")
        .withColumnRenamed("session_id", "order_session_id")
        .withWatermark("order_ts", "30 minutes")
        .select("order_id", "customer_id", "product_id", "order_value", "order_ts", "order_session_id")
)

clicks = (
    spark.readStream.table(f"{CATALOG}.bronze.clickstream_raw")
        .filter("_is_valid = true AND product_id IS NOT NULL AND action IN ('CLICK','VIEW','ADD_TO_CART')")
        .withColumnRenamed("event_timestamp", "click_ts")
        .withWatermark("click_ts", "30 minutes")
        .select(
            F.col("customer_id").alias("click_customer_id"),
            F.col("product_id").alias("click_product_id"),
            "session_id",
            "device_type",
            "click_ts",
        )
)

joined = (
    orders.join(
        clicks,
        F.expr("""
            customer_id = click_customer_id
            AND product_id = click_product_id
            AND click_ts <= order_ts
            AND click_ts >= order_ts - INTERVAL 30 MINUTES
        """),
        how="inner",
    )
    .withColumn("seconds_from_click_to_order",
                F.col("order_ts").cast("long") - F.col("click_ts").cast("long"))
    .select(
        "order_id", "customer_id", "product_id", "order_value",
        "session_id", "device_type", "seconds_from_click_to_order",
        "order_ts", "click_ts",
    )
)

# COMMAND ----------

def _merge_batch(batch_df, batch_id: int) -> None:
    if batch_df.rdd.isEmpty():
        return
    # Deduplicate in-batch first (same order+product may join wiele klików — bierzemy najkrótszy czas)
    dedup = (
        batch_df
        .withColumn("_rk", F.row_number().over(
            Window.partitionBy("order_id", "product_id")
                  .orderBy(F.col("seconds_from_click_to_order").asc())
        ))
        .filter("_rk = 1")
        .drop("_rk")
    )
    tgt = DeltaTable.forName(spark, TABLE)
    (
        tgt.alias("t")
        .merge(dedup.alias("s"), "t.order_id = s.order_id AND t.product_id = s.product_id")
        .whenNotMatchedInsertAll()
        .execute()
    )


query = (
    joined.writeStream
        .foreachBatch(_merge_batch)
        .option("checkpointLocation", CHECKPOINT)
        .outputMode("append")
        .trigger(processingTime="60 seconds")
        .queryName("silver_attribution_events")
        .start()
)

query.awaitTermination()
