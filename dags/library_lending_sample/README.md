# Reference Pipeline: Library Lending Lookup

**Purpose**: This is a STRUCTURAL REFERENCE implementation, not a working pipeline. It demonstrates the three-phase pattern described in [`../../docs/design-pattern.md`](../../docs/design-pattern.md), laid out as an actual, config-driven Airflow project.

**Example domain**: Library lending system (loans, patrons, books) — fictional, used consistently across this whole repository. Replace all business logic with real data.

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

Every file below actually exists in this directory — this list is generated to match the real tree, not aspirational.

```
dags/library_lending_sample/
├── README.md                                  # This file
├── main.py                                    # DAG: 3 builder functions, wired at the bottom
├── dag_config.yaml                            # What to build (divisions, domain queries, thresholds)
├── infra_config.yaml                          # How to connect (connection IDs, tunables)
├── utils/
│   ├── dq_utils.py                           # Data quality check interfaces (stubs)
│   └── serving_sync_utils.py                 # Export/import/swap interfaces (stubs)
└── domains/library_branch_a/                  # One folder per division (see dag_config.yaml)
    ├── warehouse/
    │   ├── ddl/
    │   │   └── loan_records.sql              # CREATE OR REPLACE — fresh schema, run in preflight
    │   ├── domain/
    │   │   ├── loan_records.sql              # TRUNCATE + INSERT — driving domain
    │   │   ├── patron_profile.sql            # TRUNCATE + INSERT — enrichment
    │   │   └── book_inventory.sql            # TRUNCATE + INSERT — enrichment
    │   ├── lookup/
    │   │   └── library_branch_a_lookup.sql   # Final LEFT JOIN assembly
    │   └── dq/
    │       └── dq_lookup.sql                 # Non-empty + unique-key assertion
    └── serving_db/
        ├── ddl/
        │   └── serving_ddl.sql               # Live table schema (created once)
        └── sync/
            ├── create_staging_table.sql      # Fresh staging table, dropped/recreated each run
            ├── export_lookup_to_object_storage.sql  # Sharded export from warehouse
            ├── swap_staging_to_live.sql      # Atomic rename swap
            └── drop_old_table.sql            # Drop the table from two cycles ago

tests/library_lending_sample/
└── test_dag_structure.py                      # Placeholder test file
```

**Note**: only `loan_records.sql` has a DDL file under `warehouse/ddl/` in this reference — a real project would have one DDL file per domain table (`patron_profile.sql`, `book_inventory.sql` too). This is a deliberate gap left in the reference: it's the same file shape repeated, so one example stands for all three.

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
- This demonstrates ONE division/entity type; scale to multiple divisions by adding entries under `division:` in `dag_config.yaml`

## See Also

- **[`../../docs/design-pattern.md`](../../docs/design-pattern.md)** — Full architecture rationale (the "why" behind every phase here)
- **[`../../examples/library_lending_walkthrough.sql`](../../examples/library_lending_walkthrough.sql)** — The same pattern as one linear SQL file, same domain, easier to read start-to-finish
- **[`../../docs/reliability-checklist.md`](../../docs/reliability-checklist.md)** — Pre-production validation checklist
