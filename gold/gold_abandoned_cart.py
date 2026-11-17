# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — Abandoned cart alerts (applyInPandasWithState)
# MAGIC Klucz: session_id. Timeout: 15 min processing time.

# COMMAND ----------

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Iterator, List

import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType,
)
from delta.tables import DeltaTable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s | %(message)s",
)
logger = logging.getLogger("nexashop.gold.abandoned_cart")

spark = SparkSession.builder.getOrCreate()

CATALOG    = spark.conf.get("nexashop.catalog", "nexashop")
BASE_PATH  = spark.conf.get("nexashop.base_path", "dbfs:/mnt/nexashop")
CHECKPOINT = f"{BASE_PATH}/_checkpoints/gold_abandoned_cart"
TABLE      = f"{CATALOG}.gold.abandoned_cart_alerts"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.gold")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        session_id    STRING,
        customer_id   STRING,
        cart_items    STRING,
        items_count   INT,
        total_value   DOUBLE,
        last_activity TIMESTAMP,
        alert_at      TIMESTAMP
    ) USING DELTA
""")

TIMEOUT_MS = 15 * 60 * 1000


OUTPUT_SCHEMA = StructType([
    StructField("session_id",    StringType()),
    StructField("customer_id",   StringType()),
    StructField("cart_items",    StringType()),
    StructField("items_count",   IntegerType()),
    StructField("total_value",   DoubleType()),
    StructField("last_activity", TimestampType()),
    StructField("alert_at",      TimestampType()),
])

STATE_SCHEMA = StructType([
    StructField("customer_id",   StringType()),
    StructField("items_json",    StringType()),
    StructField("items_count",   IntegerType()),
    StructField("total_value",   DoubleType()),
    StructField("last_activity", TimestampType()),
])


EMPTY_OUTPUT = pd.DataFrame({
    "session_id":    pd.Series(dtype="object"),
    "customer_id":   pd.Series(dtype="object"),
    "cart_items":    pd.Series(dtype="object"),
    "items_count":   pd.Series(dtype="Int32"),
    "total_value":   pd.Series(dtype="float64"),
    "last_activity": pd.Series(dtype="datetime64[ns]"),
    "alert_at":      pd.Series(dtype="datetime64[ns]"),
})


# COMMAND ----------

def _cart_state_fn(
    key, pdf_iter, state: GroupState
) -> Iterator[pd.DataFrame]:
    """applyInPandasWithState handler.

    Agreguje dodania/usunięcia do koszyka; na timeout emituje alert
    porzuconego koszyka i czyści stan.
    """
    session_id = key[0]

    try:
        if state.hasTimedOut:
            if state.exists:
                customer_id, items_json, items_count, total_value, last_activity = state.get
                state.remove()
                if items_count and items_count > 0:
                    logger.info(
                        "abandoned_cart alert session=%s items=%d value=%.2f",
                        session_id, items_count, total_value,
                    )
                    yield pd.DataFrame([{
                        "session_id":    session_id,
                        "customer_id":   customer_id,
                        "cart_items":    items_json,
                        "items_count":   int(items_count),
                        "total_value":   float(total_value),
                        "last_activity": last_activity,
                        "alert_at":      datetime.now(timezone.utc),
                    }])
                    return
            return

        if state.exists:
            customer_id, items_json, items_count, total_value, last_activity = state.get
            items: List[dict] = json.loads(items_json) if items_json else []
        else:
            customer_id   = None
            items         = []
            items_count   = 0
            total_value   = 0.0
            last_activity = None

        rows = [pdf for pdf in pdf_iter]
        if rows:
            df = pd.concat(rows, ignore_index=True).sort_values("event_timestamp")
            for _, r in df.iterrows():
                customer_id   = r["customer_id"] or customer_id
                last_activity = r["event_timestamp"]
                action        = r["action"]
                product_id    = r["product_id"]
                value         = float(r["unit_price"] or 0.0)
                if action == "ADD_TO_CART" and product_id:
                    items.append({"product_id": product_id, "price": value})
                    items_count += 1
                    total_value += value
                elif action == "REMOVE_FROM_CART" and product_id:
                    for i, it in enumerate(items):
                        if it["product_id"] == product_id:
                            items.pop(i)
                            items_count = max(0, items_count - 1)
                            total_value = max(0.0, total_value - value)
                            break
                elif action == "PURCHASE":
                    items = []
                    items_count = 0
                    total_value = 0.0

        state.update((
            customer_id,
            json.dumps(items),
            int(items_count),
            float(total_value),
            last_activity,
        ))
        state.setTimeoutDuration(TIMEOUT_MS)

    except Exception:
        logger.exception("cart_state_fn failed for session_id=%s", session_id)
        raise

    yield EMPTY_OUTPUT


# COMMAND ----------

clicks = (
    spark.readStream.table(f"{CATALOG}.bronze.clickstream_raw")
        .filter("_is_valid = true AND session_id IS NOT NULL")
        .withColumn("unit_price", F.lit(0.0))   # koszyk zasilany z atrybucji w prod. — tu placeholder
        .select("session_id", "customer_id", "product_id", "action", "event_timestamp", "unit_price")
)

alerts = (
    clicks.groupBy("session_id")
        .applyInPandasWithState(
            func=_cart_state_fn,
            outputStructType=OUTPUT_SCHEMA,
            stateStructType=STATE_SCHEMA,
            outputMode="append",
            timeoutConf=GroupStateTimeout.ProcessingTimeTimeout,
        )
)


def _merge_alerts(batch_df, batch_id: int) -> None:
    try:
        if batch_df.rdd.isEmpty():
            return
        tgt = DeltaTable.forName(spark, TABLE)
        (
            tgt.alias("t")
            .merge(batch_df.alias("s"), "t.session_id = s.session_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        logger.info("merged batch_id=%d into %s", batch_id, TABLE)
    except Exception:
        logger.exception("merge failed for batch_id=%d", batch_id)
        raise


query = (
    alerts.writeStream
        .foreachBatch(_merge_alerts)
        .option("checkpointLocation", CHECKPOINT)
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .queryName("gold_abandoned_cart_alerts")
        .start()
)

logger.info("gold_abandoned_cart_alerts started; checkpoint=%s", CHECKPOINT)
query.awaitTermination()
