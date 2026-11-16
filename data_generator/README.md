# Data generators

Dwa generatory syntetycznych zdarzeń do testowania pipeline'u. Uruchom je
w Databricks jako notebooki **przed** streaming queries z `bronze/` / `gold/`.

| Plik | Co generuje | Dla czego |
|------|-------------|-----------|
| `nexashop_event_generator.py` | orders + clickstream (JSON → landing) | bronze/silver/gold stateful |
| `inventory_generator.py`      | inventory snapshots (JSON → landing) | BONUS A (`gold_inventory_alerts.py`) |

## Konfiguracja

Oba generatory czytają `spark.conf["nexashop.base_path"]` (domyślnie
`dbfs:/mnt/nexashop`) — ta sama zmienna co reszta pipeline'u, więc dane
lądują w katalogu, z którego Auto Loader czyta.

Jeśli chcesz pisać do innego storage (np. ADLS), ustaw przed uruchomieniem:

```python
spark.conf.set("nexashop.base_path", "abfss://nexashop@<account>.dfs.core.windows.net")
```

## Scenariusze w `nexashop_event_generator.py`

1. **Normalne zdarzenia** — pełna ścieżka funnel HOME → PRODUCT → CART → CHECKOUT → ORDER_CONFIRM.
2. **Porzucone koszyki** — ADD_TO_CART bez CHECKOUT (trigger dla `gold.abandoned_cart_alerts`).
3. **Fraud** — 8 zamówień tego samego klienta w <5 min (trigger dla `gold.fraud_alerts`).
4. **Ciągłe generowanie** (opcja — `CONTINUOUS_GENERATION=True`): 10 × 30 s batchy po 10–30 orders i 30–100 clicks.
