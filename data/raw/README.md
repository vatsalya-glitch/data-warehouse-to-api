# Raw Sample Data

Synthetic data standing in for tables that would already exist in a real company's warehouse. Loaded into DuckDB by `pipeline/load_raw_data.py` at the start of every `python -m pipeline.run`.

These are committed, static files — not generated at run time. They were produced once by a throwaway script (not part of this repo) using `Faker`, with a fixed random seed for reproducibility.

| File | → warehouse table | Rows | Notes |
|---|---|---|---|
| `customers.csv` | `raw_customers` | 300 | Weighted loyalty tiers (bronze most common, platinum rarest) |
| `orders.csv` | `raw_orders` | 800 | References only customer IDs that exist in `customers.csv` |
| `order_items.csv` | `raw_order_items` | ~1,960 | 1–4 line items per order — deliberately many-to-one, so `order_items.sql` has to `GROUP BY` rather than dedup |
| `shipment_events.csv` | `raw_shipment_events` | ~1,165 | ~85% of orders have at least one event, some have 2–3 (dedup to latest); the other ~15% have **none** on purpose — this is what makes the LEFT JOIN nulls in the final lookup real |
| `support_tickets.csv` | `raw_support_tickets` | ~150 | ~15% of orders have 1–2 tickets filed against them specifically (order-scoped, not just customer-scoped — `order_id` is a real column here), mostly closed, some open; `created_at` is anchored after that order's `order_date` |

## A caveat worth knowing: dates are fixed, not relative to "today"

Dates in these files are anchored to a fixed reference point (2026-09-24), not generated relative to whenever you happen to clone the repo. That's a direct consequence of being static files instead of a generator that runs fresh each time.

The domain query `include/sql/ecommerce_orders/warehouse/domain/orders.sql` filters `WHERE order_date >= CURRENT_DATE - INTERVAL {rolling_window_days} DAY`, using the *real* current date at pipeline run time — not the fixed anchor these files were generated against. As real time passes beyond `rolling_window_days` (currently 90, in `include/config/dag_config.yaml`) past the anchor date, this data will start falling outside the window, and eventually the driving domain will be empty and the pipeline's data-quality gate will correctly refuse to build the lookup.

This isn't fixed yet — it's a known tradeoff of switching from a live generator to static fixtures, flagged for a deliberate decision (e.g., a much longer window, or anchoring the window to the data's own max date instead of `CURRENT_DATE`) rather than silently working around it.

## Regenerating

There's no generator script in this repo by design — these files are hand-maintained data, not build output. To regenerate them (e.g., to change volumes or add new edge cases), write a one-off script using the same column schema as the tables above, run it locally, and overwrite these CSVs — but don't add the generator itself back into the repo.
