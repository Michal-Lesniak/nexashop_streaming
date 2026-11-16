# Databricks notebook source
# MAGIC %md
# MAGIC # Streaming health check
# MAGIC PASS/WARN/FAIL dla każdej aktywnej query + widok ostatnich metryk.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

CATALOG        = spark.conf.get("nexashop.catalog", "nexashop")
METRICS_TABLE  = f"{CATALOG}.gold.streaming_metrics"
EXPECTED       = [
    "bronze_orders_raw",
    "bronze_clickstream_raw",
    "silver_revenue_by_product_5min",
    "funnel_live",
    "silver_attribution_events",
    "gold_abandoned_cart_alerts",
    "gold_fraud_alerts",
]

# COMMAND ----------

def _status(query_name: str) -> str:
    q = next((q for q in spark.streams.active if q.name == query_name), None)
    if q is None:
        return "FAIL — not active"
    try:
        recent = q.recentProgress[-3:]
    except Exception:
        recent = []
    if not recent:
        return "WARN — no progress yet"
    rows = sum(p.get("numInputRows", 0) for p in recent)
    if rows == 0:
        return "WARN — zero rows in last 3 batches"
    return f"PASS — {rows} rows / last 3 batches"


results = [(name, _status(name)) for name in EXPECTED]
print("\n=== STREAMING HEALTH CHECK ===")
for name, s in results:
    print(f"  {name:<40} {s}")

# COMMAND ----------

# Ostatnie 30 min metryk
recent_metrics = (
    spark.table(METRICS_TABLE)
         .filter(F.col("recorded_at") >= F.current_timestamp() - F.expr("INTERVAL 30 MINUTES"))
         .orderBy(F.col("recorded_at").desc())
)

try:
    display(recent_metrics)              # type: ignore[name-defined]
except NameError:
    recent_metrics.show(30, truncate=False)
