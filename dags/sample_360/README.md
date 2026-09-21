# Sample Airflow Pipeline: Warehouse to Serving Database

**Purpose**: This is a STRUCTURAL REFERENCE implementation, not a working pipeline. It demonstrates the three-phase pattern for syncing denormalized warehouse data into a fast, read-optimized serving database.

**Example domain**: Library lending system (loans, patrons, books). Replace all business logic with real data.

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
   - Example: `loan_records`, `patron_profile`, `book_inventory`
2. **Data quality gate**: Check driving domain is non-empty, no duplicate keys
   - If DQ fails, stop here. Leave yesterday's lookup table in place (never overwrite with bad data)
3. **Final assembly**: LEFT JOIN all domains onto driving domain
   - Result: one row per entity with all enrichments

**Result**: Complete, validated lookup table in warehouse. Never touches serving DB yet.

### Phase 3: Serve
Run only after build passes. Sync to serving database, zero downtime:
1. **Export** lookup table to object storage in shards (decouple export from import speed)
2. **Import** shards in parallel into staging table in serving DB (never touch live table)
3. **Build indexes** on staging table (after load, not before)
4. **Atomic swap**: rename `staging_lookup` → `live_lookup` inside one transaction
   - Catalog-only operation: zero downtime, live table never empty or half-updated
5. **Reconcile**: compare warehouse row count to serving DB row count
6. **Cleanup**: drop table from two cycles ago (keep one cycle for rollback safety)

**Result**: Live table updated with zero downtime. Previous cycle kept for manual rollback.

## Folder Layout

```
dags/sample_360/
├── main.py                                    # DAG with three builder functions
├── dag_config.yaml                            # What to build (data-driven config)
├── infra_config.yaml                          # How to connect (credentials, tunables)
├── utils/
│   ├── dq_utils.py                           # Data quality check interfaces (stubs)
│   └── serving_sync_utils.py                 # Export/import/swap interfaces (stubs)
└── domains/library_branch_a/
    ├── warehouse/
    │   ├── ddl/
    │   │   ├── loan_records.sql              # Fresh schema for loan domain
    │   │   ├── patron_profile.sql
    │   │   └── book_inventory.sql
    │   ├── domain/
    │   │   ├── loan_records.sql              # TRUNCATE + INSERT query
    │   │   ├── patron_profile.sql
    │   │   └── book_inventory.sql
    │   ├── lookup/
    │   │   └── library_branch_a_lookup.sql   # Final LEFT JOIN assembly
    │   └── dq/
    │       └── dq_lookup.sql                 # Data quality assertion query
    └── serving_db/
        ├── ddl/
        │   └── serving_ddl.sql               # Live table schema
        └── sync/
            ├── create_staging_table.sql      # Fresh staging table
            ├── swap_staging_to_live.sql      # Atomic rename swap
            └── cleanup_old_table.sql         # Drop previous-previous cycle

tests/sample_360/
└── test_dag_structure.py                      # Placeholder test file
```

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
| LEFT JOIN from driving domain | INNER JOIN all domains | Prevents silent row loss if enrichment domain is missing rows |
| Atomic rename swap | TRUNCATE + re-insert | Zero downtime; live table never empty or half-updated |
| Keep previous table one cycle | No backup | Manual rollback safety net if swap is bad |

## To Use This as a Reference

1. **Read** `main.py` to understand the three builder functions (preflight, build, serve)
2. **Read** `dag_config.yaml` and `infra_config.yaml` to see how parameters flow
3. **Scan** the SQL stubs under `domains/` to see query SHAPE (TRUNCATE, QUALIFY, LEFT JOIN, atomic swap)
4. **Read** `utils/dq_utils.py` and `utils/serving_sync_utils.py` to see interfaces
5. **Replace TODO comments** with real logic for your warehouse, serving DB, and business domain

This is a **structural template**, not a working implementation.

## Notes

- All SQL queries are illustrative stubs (2-5 lines each) showing SHAPE, not real business logic
- All Python functions have TODO comments marking where to replace with real code
- Example domain is fictional (library lending) to avoid confusion with real tables
- No credentials, project IDs, or real hostnames (use config files instead)
- This demonstrates ONE division/entity type; scale to multiple divisions by adding `domain_configs` entries

## See Also

- **`../design-pattern.md`** — Full architecture rationale
- **`../examples/bigquery_postgres_example.sql`** — Working example with real-looking queries
- **`../reliability-checklist.md`** — Pre-production validation
