# Databricks notebook source
# MAGIC %md
# MAGIC # NexaShop Event Generator
# MAGIC Generator syntetycznych zdarzeń e-commerce (orders + clickstream).
# MAGIC Uruchom PRZED notebookami z `bronze/`.
# MAGIC
# MAGIC Ścieżki docelowe są kontrolowane przez `spark.conf`:
# MAGIC - `nexashop.base_path` (domyślnie `dbfs:/mnt/nexashop`)
# MAGIC
# MAGIC Dzięki temu generator pisze do dokładnie tej samej landing zone, z której
# MAGIC czyta Auto Loader w `bronze_orders_streaming` / `bronze_clickstream_streaming`.

# COMMAND ----------

import json
import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

BASE_PATH        = spark.conf.get("nexashop.base_path", "dbfs:/mnt/nexashop")
ORDERS_PATH      = f"{BASE_PATH}/landing/orders/"
CLICKSTREAM_PATH = f"{BASE_PATH}/landing/clickstream/"

NUM_CUSTOMERS = 500
NUM_PRODUCTS  = 200
WAREHOUSES    = ["WH_WAW", "WH_KRK", "WH_WRO", "WH_GDA"]
PAGE_TYPES    = ["HOME", "PRODUCT", "CART", "CHECKOUT", "ORDER_CONFIRM"]
ACTIONS       = ["VIEW", "CLICK", "ADD_TO_CART", "REMOVE_FROM_CART", "PURCHASE"]
DEVICES       = ["MOBILE", "DESKTOP", "TABLET"]

PRODUCTS = {
    f"PROD_{i:03d}": {
        "name":     f"Produkt {i}",
        "price":    round(random.uniform(9.99, 999.99), 2),
        "category": random.choice(["Elektronika", "Odzież", "Dom", "Sport", "Kosmetyki"]),
    }
    for i in range(1, NUM_PRODUCTS + 1)
}


def _iso_minutes_ago(minutes: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat().replace("+00:00", "Z")


# COMMAND ----------

def generate_order_event(
    customer_id: Optional[str] = None,
    product_id: Optional[str] = None,
    event_type: str = "CREATED",
    minutes_ago: int = 0,
) -> Dict[str, Any]:
    pid   = product_id or f"PROD_{random.randint(1, NUM_PRODUCTS):03d}"
    price = PRODUCTS.get(pid, {}).get("price", round(random.uniform(9.99, 999.99), 2))
    return {
        "order_id":        str(uuid.uuid4()),
        "customer_id":     customer_id or f"CUST_{random.randint(1000, 1000 + NUM_CUSTOMERS):04d}",
        "product_id":      pid,
        "quantity":        random.randint(1, 5),
        "unit_price":      price,
        "event_timestamp": _iso_minutes_ago(minutes_ago),
        "event_type":      event_type,
        "session_id":      str(uuid.uuid4()),
        "warehouse_id":    random.choice(WAREHOUSES),
    }


def generate_click_event(
    session_id: Optional[str] = None,
    customer_id: Optional[str] = None,
    page_type: Optional[str] = None,
    action: Optional[str] = None,
    product_id: Optional[str] = None,
    minutes_ago: int = 0,
) -> Dict[str, Any]:
    pt  = page_type or random.choice(PAGE_TYPES)
    pid = product_id if pt in ["PRODUCT", "CART"] else None
    return {
        "event_id":        str(uuid.uuid4()),
        "session_id":      session_id or str(uuid.uuid4()),
        "customer_id":     customer_id,
        "page_type":       pt,
        "product_id":      pid or (f"PROD_{random.randint(1, NUM_PRODUCTS):03d}" if pt == "PRODUCT" else None),
        "action":          action or random.choice(ACTIONS[:3]),
        "event_timestamp": _iso_minutes_ago(minutes_ago),
        "device_type":     random.choice(DEVICES),
    }


def write_events(events: List[Dict], path: str, batch_name: str) -> None:
    content   = "\n".join(json.dumps(e) for e in events)
    full_path = f"{path}{batch_name}.json"
    dbutils.fs.put(full_path, content, overwrite=True)  # type: ignore[name-defined]
    print(f"  ✓ {len(events):>4} zdarzeń → {full_path}")


# COMMAND ----------
# Scenariusz 1 — normalny ruch e-commerce (orders + clickstream)
print("=== Normalne zdarzenia ===")
normal_orders: List[Dict[str, Any]] = []
normal_clicks: List[Dict[str, Any]] = []

for _ in range(20):
    sid, cid = str(uuid.uuid4()), f"CUST_{random.randint(1000, 1500):04d}"
    pid      = f"PROD_{random.randint(1, NUM_PRODUCTS):03d}"
    normal_clicks += [
        generate_click_event(sid, cid, "HOME",          "VIEW",        minutes_ago=random.randint(5, 30)),
        generate_click_event(sid, cid, "PRODUCT",       "VIEW",        pid, minutes_ago=random.randint(3, 28)),
        generate_click_event(sid, cid, "PRODUCT",       "ADD_TO_CART", pid, minutes_ago=random.randint(2, 25)),
        generate_click_event(sid, cid, "CART",          "VIEW",        minutes_ago=random.randint(1, 20)),
        generate_click_event(sid, cid, "CHECKOUT",      "CLICK",       minutes_ago=random.randint(0, 15)),
        generate_click_event(sid, cid, "ORDER_CONFIRM", "VIEW",        minutes_ago=random.randint(0, 10)),
    ]
    normal_orders.append(generate_order_event(cid, pid, "CREATED", minutes_ago=random.randint(0, 12)))

stamp = datetime.now().strftime("%H%M%S")
write_events(normal_orders, ORDERS_PATH,      f"batch_normal_{stamp}")
write_events(normal_clicks, CLICKSTREAM_PATH, f"batch_normal_{stamp}")

# COMMAND ----------
# Scenariusz 2 — porzucone koszyki (test stateful: flatMapGroupsWithState)
print("\n=== Porzucone koszyki ===")
abandoned_clicks: List[Dict[str, Any]] = []
for _ in range(10):
    sid = str(uuid.uuid4())
    cid = f"CUST_{random.randint(1000, 1500):04d}" if random.random() > 0.3 else None
    pid = f"PROD_{random.randint(1, NUM_PRODUCTS):03d}"
    abandoned_clicks += [
        generate_click_event(sid, cid, "PRODUCT", "VIEW",        pid, minutes_ago=25),
        generate_click_event(sid, cid, "PRODUCT", "ADD_TO_CART", pid, minutes_ago=20),
        # brak kolejnych → timeout 15 min wyemituje alert
    ]

write_events(abandoned_clicks, CLICKSTREAM_PATH, f"batch_abandoned_{datetime.now().strftime('%H%M%S')}")

# COMMAND ----------
# Scenariusz 3 — fraud (test stateful: mapGroupsWithState)
print("\n=== Fraud ===")
fraud_cid = f"CUST_FRAUD_{random.randint(9000, 9999)}"
fraud_orders = [
    generate_order_event(customer_id=fraud_cid, event_type="CREATED", minutes_ago=random.randint(0, 4))
    for _ in range(8)  # 8 zamówień w <5 min → fraud_score >= 50
]
write_events(fraud_orders, ORDERS_PATH, f"batch_fraud_{datetime.now().strftime('%H%M%S')}")
print(f"  Fraud customer_id: {fraud_cid}")

# COMMAND ----------
# Scenariusz 4 (opcjonalny) — ciągłe generowanie co 30 s
CONTINUOUS_GENERATION = False
if CONTINUOUS_GENERATION:
    print("\n=== Ciągłe generowanie (10 × 30 s) ===")
    for i in range(10):
        batch_orders = [generate_order_event(minutes_ago=random.randint(0, 2)) for _ in range(random.randint(10, 30))]
        batch_clicks = [generate_click_event(minutes_ago=random.randint(0, 2)) for _ in range(random.randint(30, 100))]
        ts = datetime.now().strftime("%H%M%S")
        write_events(batch_orders, ORDERS_PATH,      f"continuous_{ts}_{i:04d}")
        write_events(batch_clicks, CLICKSTREAM_PATH, f"continuous_{ts}_{i:04d}")
        time.sleep(30)

# COMMAND ----------
# Weryfikacja — pliki w landing zone
print("\n=== landing/orders/ ===")
display(dbutils.fs.ls(ORDERS_PATH))         # type: ignore[name-defined]
print("\n=== landing/clickstream/ ===")
display(dbutils.fs.ls(CLICKSTREAM_PATH))    # type: ignore[name-defined]
