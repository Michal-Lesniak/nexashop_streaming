# Databricks notebook source
# MAGIC %md
# MAGIC # Inventory Generator (BONUS A)
# MAGIC Generuje syntetyczne zdarzenia inventory snapshot do `landing/inventory/`.
# MAGIC Część produktów ma `current_stock < 5` — żeby `gold.low_stock_alerts` miał co pokazać.

# COMMAND ----------

import json
import random
import uuid
from datetime import datetime, timezone

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

BASE_PATH      = spark.conf.get("nexashop.base_path", "dbfs:/mnt/nexashop")
INVENTORY_PATH = f"{BASE_PATH}/landing/inventory/"

NUM_PRODUCTS = 200
WAREHOUSES   = ["WH_WAW", "WH_KRK", "WH_WRO", "WH_GDA"]

# Udział produktów z niskim stanem — w realu rzadko, tu ~15% żeby alerty były widoczne
LOW_STOCK_RATIO = 0.15

# COMMAND ----------

def generate_inventory_event() -> dict:
    is_low = random.random() < LOW_STOCK_RATIO
    return {
        "event_id":      str(uuid.uuid4()),
        "product_id":    f"PROD_{random.randint(1, NUM_PRODUCTS):03d}",
        "warehouse_id":  random.choice(WAREHOUSES),
        "current_stock": random.randint(0, 4) if is_low else random.randint(5, 500),
        "timestamp":     datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }


events  = [generate_inventory_event() for _ in range(100)]
content = "\n".join(json.dumps(e) for e in events)
path    = f"{INVENTORY_PATH}batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

dbutils.fs.put(path, content, overwrite=True)  # type: ignore[name-defined]
print(f"  ✓ {len(events)} zdarzeń inventory → {path}")
print(f"  low-stock (<5): {sum(1 for e in events if e['current_stock'] < 5)}")
