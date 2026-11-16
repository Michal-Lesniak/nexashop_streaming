# Raport — Zadanie zaliczeniowe Miesiąc 9

**Imię i nazwisko:** Michał Leśniak
**Projekt:** NexaShop Real-Time Analytics
**Data:** 2026-11-10

---

## 1. Opis architektury pipeline'u

Pipeline Medallion (Bronze/Silver/Gold) na Delta Lake. Zdarzenia JSON (orders, clickstream) trafiają do landing zone, Auto Loader je inkrementalnie ładuje do Bronze. W Silver powstają: agregacja przychodu (5-min tumbling + watermark 10 min), funnel 1-min w memory sink (COMPLETE), oraz stream-stream join atrybucji klik→zamówienie (30-min window). Gold dodaje stateful detekcje: porzucony koszyk (`flatMapGroupsWithState`, timeout 15 min) i fraud score (`mapGroupsWithState` + audit log).

Diagram: patrz `README.md`.

## 2. Uzasadnienie triggerów i watermarków

| Komponent | Trigger | Watermark | Dlaczego |
|-----------|---------|-----------|----------|
| Bronze orders | 30 s | — | Niska objętość, batch size optymalny dla Auto Loader. |
| Bronze clickstream | 10 s | — | Duża objętość, chcemy świeży funnel. |
| Silver revenue | 30 s | 10 min | Dopuszczamy 2× długość okna na opóźnienia; więcej → za duży stan. |
| Silver funnel | 10 s | — | COMPLETE mode nie potrzebuje watermarku; state trzymany w memory. |
| Silver attribution | 60 s | 30 min (obie strony) | Klik może pojawić się do 30 min przed zamówieniem; dłuższe watermarki → OOM state. |
| Gold cart | 30 s | processing-time timeout 15 min | Branże e-commerce: po 15 min bezczynności koszyk uznajemy za porzucony. |
| Gold fraud | 30 s | processing-time timeout 10 min | Trzymamy ostatnie 5 min zamówień; dłuższy timeout → state puchnie. |

## 3. Największe problemy i ich rozwiązanie

**Problem 1 — Stream-stream join + MERGE (deduplikacja)**
`outputMode="update"` nie jest wspierany dla joinów. Rozwiązanie: `foreachBatch` z `DeltaTable.merge` + `row_number` po najkrótszym czasie klik→zamówienie, żeby wybrać atrybucję "ostatni klik".

**Problem 2 — `applyInPandasWithState` i schemat stanu**
Spark nie pozwala trzymać list Python w stanie. Serializuję `cart_items` do JSON string przy `state.update(...)`.

**Problem 3 — Konflikty checkpointów**
Pierwsza iteracja miała wspólny katalog `_checkpoints/` — Spark rzucał `ConcurrentCheckpointException`. Każda query ma teraz własny podkatalog.

## 4. Co w produkcji zrobiłbym inaczej

- **Źródło = Kafka/Event Hub** zamiast Auto Loader na plikach. Backpressure, replay, exactly-once przy końcu pipeline'u.
- **UC governance** — masking dla `customer_id`, osobny katalog PII.
- **Alerting end-to-end** — SNS/PagerDuty dla fraud_score > 75; obecnie tylko zapis do Delta.
- **Osobne klastry** per warstwa; bronze = autoscaling, gold stateful = fixed.
- **Backfill** przez `trigger=availableNow` + dedykowany job (nie reset checkpointu produkcyjnej query).
- **Lineage** — OpenLineage emitter + Marquez; obecnie historia tylko w Delta history.
- **Metryki** — Prometheus scraper na `StreamingQueryProgress` zamiast tabeli Delta; szybsze dashboardy.
