# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — Fraud detection (applyInPandasWithState)
# MAGIC Klucz: customer_id. Reguły scoringowe + log audytowy.

# COMMAND ----------

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Iterator

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
logger = logging.getLogger("nexashop.gold.fraud")

spark = SparkSession.builder.getOrCreate()

CATALOG      = spark.conf.get("nexashop.catalog", "nexashop")
BASE_PATH    = spark.conf.get("nexashop.base_path", "dbfs:/mnt/nexashop")
CHECKPOINT   = f"{BASE_PATH}/_checkpoints/gold_fraud"
TABLE_ALERTS = f"{CATALOG}.gold.fraud_alerts"
TABLE_AUDIT  = f"{CATALOG}.gold.fraud_audit_log"

STATE_TIMEOUT_MS = 10 * 60 * 1000
WINDOW_MIN = 5
ALERT_THRESHOLD = 50

for ddl in (
    f"""
    CREATE TABLE IF NOT EXISTS {TABLE_ALERTS} (
        customer_id        STRING,
        fraud_score        INT,
        orders_last_5min   INT,
        total_value_5min   DOUBLE,
        reasons            STRING,
        alert_at           TIMESTAMP
    ) USING DELTA
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {TABLE_AUDIT} (
        customer_id        STRING,
        fraud_score        INT,
        orders_last_5min   INT,
        total_value_5min   DOUBLE,
        reasons            STRING,
        logged_at          TIMESTAMP
    ) USING DELTA
    """,
):
    spark.sql(ddl)


OUTPUT_SCHEMA = StructType([
    StructField("customer_id",      StringType()),
    StructField("fraud_score",      IntegerType()),
    StructField("orders_last_5min", IntegerType()),
    StructField("total_value_5min", DoubleType()),
    StructField("reasons",          StringType()),
    StructField("alert_at",         TimestampType()),
])

STATE_SCHEMA = StructType([
    StructField("order_timestamps",      StringType()),      # CSV ISO
    StructField("order_values",          StringType()),
    StructField("last_score",            IntegerType()),
    StructField("last_alert_at",         TimestampType()),
])


EMPTY_OUTPUT = pd.DataFrame({
    "customer_id":      pd.Series(dtype="object"),
    "fraud_score":      pd.Series(dtype="Int32"),
    "orders_last_5min": pd.Series(dtype="Int32"),
    "total_value_5min": pd.Series(dtype="float64"),
    "reasons":          pd.Series(dtype="object"),
    "alert_at":         pd.Series(dtype="datetime64[ns]"),
})


def _score(orders_in_5min: int, total_value_5min: float) -> tuple[int, list[str]]:
    score   = 0
    reasons: list[str] = []
    if orders_in_5min > 5:
        score += 50
        reasons.append(f"orders_last_5min={orders_in_5min}>5 (+50)")
    if total_value_5min > 5000:
        score += 30
        reasons.append(f"total_value_5min={total_value_5min:.2f}>5000 (+30)")
    return score, reasons


def _fraud_state_fn(
    key, pdf_iter, state: GroupState
) -> Iterator[pd.DataFrame]:
    customer_id = key[0]

    try:
        if state.hasTimedOut:
            if state.exists:
                logger.info("state timeout, removing customer_id=%s", customer_id)
                state.remove()
            return

        if state.exists:
            ts_csv, vals_csv, last_score, last_alert_at = state.get
            timestamps = [datetime.fromisoformat(t) for t in ts_csv.split("|") if t]
            values     = [float(v) for v in vals_csv.split("|") if v]
        else:
            timestamps, values, last_score, last_alert_at = [], [], 0, None

        for pdf in pdf_iter:
            for _, r in pdf.iterrows():
                ts = r["event_timestamp"]
                if hasattr(ts, "to_pydatetime"):
                    ts = ts.to_pydatetime()
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                timestamps.append(ts)
                values.append(float(r["order_value"]))


        now_utc = datetime.now(timezone.utc)
        cutoff  = now_utc - timedelta(minutes=WINDOW_MIN)
        kept    = [(t, v) for t, v in zip(timestamps, values) if t >= cutoff]
        timestamps = [t for t, _ in kept]
        values     = [v for _, v in kept]

        orders_count   = len(timestamps)
        total_value    = float(sum(values))
        score, reasons = _score(orders_count, total_value)

        emit_alert = (
            score >= ALERT_THRESHOLD
            and score != last_score
        )

        new_last_alert_at = now_utc if emit_alert else last_alert_at

        state.update((
            "|".join(t.isoformat() for t in timestamps),
            "|".join(f"{v:.2f}" for v in values),
            int(score),
            new_last_alert_at,
        ))
        state.setTimeoutDuration(STATE_TIMEOUT_MS)

        if emit_alert:
            logger.info(
                "fraud alert customer_id=%s score=%d orders=%d total=%.2f",
                customer_id, score, orders_count, total_value,
            )
            yield pd.DataFrame([{
                "customer_id":      customer_id,
                "fraud_score":      int(score),
                "orders_last_5min": int(orders_count),
                "total_value_5min": float(total_value),
                "reasons":          "; ".join(reasons),
                "alert_at":         now_utc,
            }])
            return

    except Exception:
        logger.exception("fraud_state_fn failed for customer_id=%s", customer_id)
        raise

    yield EMPTY_OUTPUT


# COMMAND ----------

orders = (
    spark.readStream.table(f"{CATALOG}.bronze.orders_raw")
        .filter("_is_valid = true AND event_type = 'CREATED' AND customer_id IS NOT NULL")
        .withColumn("order_value", F.col("quantity") * F.col("unit_price"))
        .select("customer_id", "order_id", "order_value", "event_timestamp")
)

alerts = (
    orders.groupBy("customer_id")
        .applyInPandasWithState(
            func=_fraud_state_fn,
            outputStructType=OUTPUT_SCHEMA,
            stateStructType=STATE_SCHEMA,
            outputMode="append",
            timeoutConf=GroupStateTimeout.ProcessingTimeTimeout,
        )
)


def _write(batch_df, batch_id: int) -> None:
    try:
        if batch_df.rdd.isEmpty():
            return

        (
            batch_df.withColumn("logged_at", F.current_timestamp())
                    .select("customer_id", "fraud_score", "orders_last_5min",
                            "total_value_5min", "reasons", "logged_at")
                    .write.format("delta").mode("append").saveAsTable(TABLE_AUDIT)
        )

        tgt = DeltaTable.forName(spark, TABLE_ALERTS)
        (
            tgt.alias("t")
            .merge(batch_df.alias("s"), "t.customer_id = s.customer_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        logger.info("merged fraud batch_id=%d into %s", batch_id, TABLE_ALERTS)
    except Exception:
        logger.exception("fraud write failed for batch_id=%d", batch_id)
        raise


query = (
    alerts.writeStream
        .foreachBatch(_write)
        .option("checkpointLocation", CHECKPOINT)
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .queryName("gold_fraud_alerts")
        .start()
)

logger.info("gold_fraud_alerts started; checkpoint=%s", CHECKPOINT)
query.awaitTermination()
