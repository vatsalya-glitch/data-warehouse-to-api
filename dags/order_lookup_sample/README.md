# Reference Pipeline: Ecommerce Order Lookup

**Purpose**: This is a STRUCTURAL REFERENCE implementation, not a working pipeline. It demonstrates the three-phase pattern described in [`../../docs/design-pattern.md`](../../docs/design-pattern.md), laid out as an actual, config-driven Airflow project.

**Example domain**: Ecommerce order lookup — fictional, used consistently across this whole repository. `orders` is the driving domain (it defines which entities are in scope); `customer_attributes`, `order_items`, `shipment`, and `customer_support` are enrichment domains LEFT JOINed onto it. Replace all business logic with real data.

**Where this fits in the repo**: see the "Repository Map" in the [root README](../../README.md) for how this concrete implementation relates to `docs/` (concept) and `examples/` (single-file walkthrough).

## The Pattern in Three Phases

### Phase 1: Preflight
Run before any data writes. Validates schemas and catches mismatches early:
1. Create or replace schema for every domain table (fresh start)
2. Dry-run each domain INSERT query (validate types without scanning data)
3. Check column coverage: fail if any column exists upstream but isn't wired into the final lookup

**Result**: If any check fails, stop. Don't proceed to build phase.

### Phase 2: Build
Run only after preflight passes. Assemble denormalized lookup:
1. **Per division**: Build each domain table with TRUNCATE + INSERT (full refresh)
   - `orders` (driving) — one row per order, defines scope
   - `customer_attributes` — customer profile, joins on `customer_id`
   - `order_items` — line items, **aggregated** (not just deduped) to one row per `order_id`
   - `shipment` — latest shipment status, joins on `order_id`
   - `customer_support` — open ticket summary, joins on `customer_id`
2. **Data quality gate**: Check driving domain (`orders`) is non-empty, no duplicate `order_id`
   - If DQ fails, stop here. Leave yesterday's lookup table in place (never overwrite with bad data)
3. **Final assembly**: LEFT JOIN all four enrichment domains onto `orders`
   - Result: one row per order, enriched with customer, items, shipment, and support context

**Result**: Complete, validated lookup table in warehouse. Never touches serving DB yet.

### Phase 3: Serve
Run only after build passes. Sync to serving database, zero downtime:
1. **Export** lookup table to object storage in shards (decouple export from import speed)
2. **Import** shards in parallel into staging table in serving DB (never touch live table)
3. **Build indexes** on staging table (after load, not before)
4. **Atomic swap**: rename `staging_order_lookup` → `live_order_lookup` inside one transaction
   - Catalog-only operation: zero downtime, live table never empty or half-updated
5. **Reconcile**: compare warehouse row count to serving DB row count
6. **Cleanup**: drop table from two cycles ago (keep one cycle for rollback safety)

**Result**: Live table updated with zero downtime. Previous cycle kept for manual rollback.

## Folder Layout

Every file below actually exists in this directory — this list is generated to match the real tree, not aspirational.

```
dags/order_lookup_sample/
├── README.md                                  # This file
├── main.py                                    # DAG: 3 builder functions, wired at the bottom
├── dag_config.yaml                            # What to build (divisions, domain queries, thresholds)
├── infra_config.yaml                          # How to connect (connection IDs, tunables)
├── utils/
│   ├── dq_utils.py                           # Data quality check interfaces (stubs)
│   └── serving_sync_utils.py                 # Export/import/swap interfaces (stubs)
└── domains/ecommerce_orders/                  # One folder per division (see dag_config.yaml)
    ├── warehouse/
    │   ├── ddl/
    │   │   └── orders.sql                    # CREATE OR REPLACE — fresh schema, run in preflight
    │   ├── domain/
    │   │   ├── orders.sql                    # TRUNCATE + INSERT — driving domain
    │   │   ├── customer_attributes.sql       # TRUNCATE + INSERT — enrichment, joins on customer_id
    │   │   ├── order_items.sql               # TRUNCATE + INSERT — enrichment, GROUP BY to order grain
    │   │   ├── shipment.sql                  # TRUNCATE + INSERT — enrichment, joins on order_id
    │   │   └── customer_support.sql          # TRUNCATE + INSERT — enrichment, joins on customer_id
    │   ├── lookup/
    │   │   └── ecommerce_orders_lookup.sql   # Final LEFT JOIN assembly (all 5 domains)
    │   └── dq/
    │       └── dq_lookup.sql                 # Non-empty + unique-order_id assertion
    └── serving_db/
        ├── ddl/
        │   └── serving_ddl.sql               # live_order_lookup schema (created once)
        └── sync/
            ├── create_staging_table.sql      # Fresh staging table, dropped/recreated each run
            ├── export_lookup_to_object_storage.sql  # Sharded export from warehouse
            ├── swap_staging_to_live.sql      # Atomic rename swap
            └── drop_old_table.sql            # Drop the table from two cycles ago

tests/order_lookup_sample/
└── test_dag_structure.py                      # Placeholder test file
```

**Note**: only `orders.sql` has a DDL file under `warehouse/ddl/` in this reference — a real project would have one DDL file per domain table (`customer_attributes.sql`, `order_items.sql`, `shipment.sql`, `customer_support.sql` too). This is a deliberate gap left in the reference: it's the same file shape repeated four more times, so one example stands for all five.

**Two flattening techniques, side by side**: `customer_attributes`, `shipment`, and `customer_support` collapse to one row per key with `QUALIFY ROW_NUMBER() ... = 1` (pick the latest record). `order_items` is naturally many rows per order, so it collapses with `GROUP BY order_id` (aggregate the children) instead — see the comment at the top of `order_items.sql` for why the technique differs.

## Config-Driven: No Python Code Changes to Add Data

**To add a new domain query:**
1. Edit `dag_config.yaml`: add query file path to `domain_queries`
2. Create SQL file with TRUNCATE + INSERT logic

**To change business parameter (e.g., lookback window):**
1. Edit `dag_config.yaml`: change `rolling_window_days`
2. DAG reads this value and injects it into all domain queries as `{rolling_window_days}`

**To change connection or tuning:**
1. Edit `infra_config.yaml`: warehouse connection ID, serving DB concurrency, etc.

No Python changes needed. The DAG code stays stable as data grows.

## Key Design Decisions (Why This Shape?)

| Decision | Alternative | Why? |
|----------|-----------|------|
| Full refresh (TRUNCATE + INSERT) | Incremental/MERGE | Simpler correctness story; reprocess bounded window each run |
| Three hard gates (preflight → build → serve) | Single long DAG | Fail early before writes; each phase is independently verifiable |
| Config-driven parameters | Hardcoded in SQL | One place to change; prevents drift across files |
| LEFT JOIN from driving domain (`orders`) | INNER JOIN all domains | An order with no shipment record yet still appears in the lookup, with null shipment fields, instead of vanishing |
| GROUP BY for `order_items`, QUALIFY for the rest | One dedup technique everywhere | The right collapse strategy depends on whether the source is naturally many-to-one (aggregate) or one-of-many-versions (pick latest) |
| Atomic rename swap | TRUNCATE + re-insert | Zero downtime; live table never empty or half-updated |
| Keep previous table one cycle | No backup | Manual rollback safety net if swap is bad |

## To Use This as a Reference

1. **Read** `main.py` to understand the three builder functions (preflight, build, serve)
2. **Read** `dag_config.yaml` and `infra_config.yaml` to see how parameters flow
3. **Scan** the SQL stubs under `domains/` to see query SHAPE (TRUNCATE, QUALIFY, GROUP BY, LEFT JOIN, atomic swap)
4. **Read** `utils/dq_utils.py` and `utils/serving_sync_utils.py` to see interfaces
5. **Replace TODO comments** with real logic for your warehouse, serving DB, and business domain

This is a **structural template**, not a working implementation.

## Notes

- All SQL queries are illustrative stubs (2-5 lines each) showing SHAPE, not real business logic
- All Python functions have TODO comments marking where to replace with real code
- Example domain is fictional (ecommerce order lookup) to avoid confusion with real tables
- No credentials, project IDs, or real hostnames (use config files instead)
- This demonstrates ONE division/entity type; scale to multiple divisions by adding entries under `division:` in `dag_config.yaml`

## See Also

- **[`../../docs/design-pattern.md`](../../docs/design-pattern.md)** — Full architecture rationale (the "why" behind every phase here)
- **[`../../examples/ecommerce_order_lookup_walkthrough.sql`](../../examples/ecommerce_order_lookup_walkthrough.sql)** — The same pattern as one linear SQL file, same domain, easier to read start-to-finish
- **[`../../docs/reliability-checklist.md`](../../docs/reliability-checklist.md)** — Pre-production validation checklist
