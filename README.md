# NexaShop — Real-Time Analytics Platform (M09)

Produkcyjny streaming pipeline dla NexaShop: od zdarzeń e-commerce (orders + clickstream) przez warstwy Bronze/Silver/Gold, aż po alerty i monitoring.

## Architektura

```
          ┌─────────────────────┐
          │   Event generator   │  (data_generator/nexashop_event_generator.py)
          │ orders + clickstream│
          └──────────┬──────────┘
                     │ JSON files (landing)
                     ▼
     ┌────────────────────────────────┐
     │  BRONZE — Auto Loader → Delta  │
     │  bronze.orders_raw        (30s)│
     │  bronze.clickstream_raw   (10s)│
     └──────────────┬─────────────────┘
                    │ _is_valid filter
   ┌────────────────┼──────────────────────────┐
   ▼                ▼                          ▼
 SILVER         SILVER (COMPLETE)         SILVER (stream-stream join)
 revenue_by_    funnel_1min               attribution_events
 product_5min   (memory sink)             orders ⋈ clickstream
 window 5min,   window 1min               (30-min join window,
 watermark 10m                             watermark 30m, MERGE)
         │                                        │
         ▼                                        ▼
       GOLD                                    GOLD
   streaming_metrics                      attribution + metryki
         │
   ┌─────┴─────────────┐
   ▼                   ▼
 GOLD (stateful)   GOLD (stateful)
 abandoned_cart    fraud_alerts
 flatMapGroups     mapGroupsWith
 WithState         State + audit log
```

## Katalog

```
nexashop_m09_streaming/
├── bronze/
│   ├── bronze_orders_streaming.py
│   └── bronze_clickstream_streaming.py
├── silver/
│   ├── silver_revenue_aggregations.py
│   ├── silver_funnel_analysis.py
│   └── silver_attribution_join.py
├── gold/
│   ├── gold_abandoned_cart.py
│   ├── gold_fraud_detection.py
│   ├── gold_streaming_metrics.py
│   └── gold_inventory_alerts.py          ← BONUS A
├── monitoring/
│   └── streaming_health_check.py
├── workflow/
│   └── nexashop_workflow.json             ← BONUS B (Databricks Jobs API)
└── data_generator/README.md
```

## Konfiguracja

Każdy notebook czyta dwa parametry z `spark.conf`:
- `nexashop.catalog`  (domyślnie `nexashop`)
- `nexashop.base_path` (domyślnie `dbfs:/mnt/nexashop`)

Checkpointy są **unikalne per query** (`_checkpoints/<query_name>`).

## Uzasadnienie wyborów

| Decyzja | Uzasadnienie |
|---------|--------------|
| Trigger 10s dla clickstream | Dużo zdarzeń, chcemy szybki funnel w memory sink. |
| Trigger 30s dla orders/revenue | Rzadsze zdarzenia, mniej batchy → mniej overhead DBIO. |
| Watermark 10 min revenue | Tolerujemy late events z 2× okna, ale nie więcej (coffee break RAM). |
| Watermark 30 min attribution | Klik może być <= 30 min przed zamówieniem — potrzebujemy obu stron widocznych w state. |
| Append + MERGE dla attribution | Deduplikacja po (order_id, product_id); inner join daje potencjalne duplikaty. |
| flatMapGroupsWithState (cart) | Chcemy emitować alert **tylko** przy timeout — flatMap pozwala zwrócić 0 albo 1 wiersz. |
| mapGroupsWithState (fraud) | Zawsze emitujemy aktualny score — map pasuje lepiej. |
| COMPLETE + memory sink (funnel) | Dashboard "co się dzieje teraz" + debugowalność. Nie zapisujemy historii. |

## Trudności i jak rozwiązałem

- **Stream-stream join z MERGE**: Spark nie wspiera outputMode=update dla join, więc używam `foreachBatch` + `DeltaTable.merge`. Dodatkowy problem — klik może matchować wiele razy, więc w batchu robię `row_number` po `seconds_from_click_to_order`.
- **Stateful z pandas**: `applyInPandasWithState` wymaga spójnego schematu stanu — serializuję listę elementów koszyka do JSON, bo Spark w state nie lubi list.
- **Checkpointy**: każdy query ma własny, inaczej Spark rzuca `ConcurrentCheckpointException`.

## Co bym zrobił w produkcji

- **Kafka/Event Hub** zamiast plików JSON w landing — Auto Loader ok na dev, ale w prod brak backpressure control.
- **Unity Catalog governance**: tagowanie PII w `customer_id`, row-level masking.
- **Dedykowane klastry** per task w workflow (teraz dzielą jeden klaster — przy większym ruchu trzeba izolować bronze od gold).
- **Alerting**: zamiast tylko tabeli `fraud_alerts` dorzucić EventBridge/SNS → PagerDuty dla score > 75.
- **Autoscaling** na bronze + stałe dla gold (stateful nie lubi częstej zmiany partition count).
- **Backfill strategy**: `trigger=availableNow` + oddzielny job do rebuildów.

## Checkpoint cleanup

Po reset state (np. zmiana schematu):

```bash
dbfs rm -r dbfs:/mnt/nexashop/_checkpoints/<query>
dbfs rm -r dbfs:/mnt/nexashop/_schemas/<query>
```
