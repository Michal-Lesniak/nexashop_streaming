# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — streaming metrics
# MAGIC Prosty listener zapisujący `StreamingQueryProgress` do Delta.

# COMMAND ----------

import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession, Row
from pyspark.sql.streaming import StreamingQueryListener

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s | %(message)s",
)
logger = logging.getLogger("nexashop.gold.metrics")

spark = SparkSession.builder.getOrCreate()

CATALOG = spark.conf.get("nexashop.catalog", "nexashop")
TABLE   = f"{CATALOG}.gold.streaming_metrics"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.gold")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        query_name             STRING,
        batch_id               BIGINT,
        num_input_rows         BIGINT,
        input_rows_per_second  DOUBLE,
        processing_rate        DOUBLE,
        batch_duration_ms      BIGINT,
        recorded_at            TIMESTAMP
    ) USING DELTA
""")


class MetricsListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        pass

    def onQueryProgress(self, event):
        p = event.progress
        row = Row(
            query_name            = p.name or "unknown",
            batch_id              = int(p.batchId),
            num_input_rows        = int(p.numInputRows),
            input_rows_per_second = float(p.inputRowsPerSecond or 0.0),
            processing_rate       = float(p.processedRowsPerSecond or 0.0),
            batch_duration_ms     = int(p.batchDuration),
            recorded_at           = datetime.now(timezone.utc),
        )
        try:
            spark.createDataFrame([row]).write.format("delta").mode("append").saveAsTable(TABLE)
        except Exception:                         # noqa: BLE001
            logger.exception(
                "metrics write failed for query=%s batch_id=%s",
                row.query_name, row.batch_id,
            )

    def onQueryTerminated(self, event):
        pass


# COMMAND ----------

listener = MetricsListener()
spark.streams.addListener(listener)
logger.info("StreamingQueryListener registered. Keep this notebook attached while queries run.")

# COMMAND ----------

# W razie potrzeby usuń listener:
# spark.streams.removeListener(listener)
