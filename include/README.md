# Reference Pipeline: Ecommerce Order Lookup

**Purpose**: This is the real, runnable implementation of the three-phase pattern described in [`../docs/design-pattern.md`](../docs/design-pattern.md) — clone the repo, `pip install -r requirements.txt`, and `python -m pipeline.run` actually executes it end to end. It uses the standard Astronomer/Airflow `dags/` + `include/` split, with the real Python logic in a third top-level package, `pipeline/`.

**Example domain**: Ecommerce order lookup — fictional, used consistently across this whole repository. `orders` is the driving domain (it defines which entities are in scope); `customer_attributes`, `order_items`, `shipment`, and `customer_support` are enrichment domains LEFT JOINed onto it.

**Where this fits in the repo**: see the "Repository Map" in the [root README](../README.md) for how this relates to `docs/` (concept) and `examples/` (single-file, production-flavored walkthrough).

## Why three top-level packages: `pipeline/`, `dags/`, `include/`

- **`pipeline/`** — the real logic. Python that talks to DuckDB (the warehouse) and SQLite (the serving DB). Runnable standalone with `python -m pipeline.run`; no orchestrator required.
- **`dags/`** — a thin Airflow wrapper. `order_lookup_sample.py` imports `pipeline.preflight`, `pipeline.build`, `pipeline.serve` and calls them from `PythonOperator` tasks. It's the same shape `astro dev init` scaffolds: **DAG files live in `dags/` and nothing else does**, so Airflow's scheduler only ever re-parses thin orchestration code.
- **`include/`** (this directory) — config and SQL, read by `pipeline/`, never parsed by Airflow as DAGs.

```
pipeline/                     ← the real implementation — run this
├── warehouse.py, serving_db.py    DuckDB / SQLite connection wrappers
├── preflight.py, build.py, serve.py    the three phases
├── seed.py                    synthetic data generator
├── run.py, lookup.py          CLI entrypoints

dags/order_lookup_sample.py   ← thin Airflow wrapper around pipeline/ (optional)

include/                      ← this directory: config + SQL, read by pipeline/
├── config/                    what to build + how to connect
└── sql/                       one query per file, organized by division

tests/
├── pipeline/                  real end-to-end tests (temp DuckDB + SQLite)
└── dags/                      structural checks on the DAG file
```

## Why DuckDB and SQLite

The production target for this pattern is a real warehouse (BigQuery, Snowflake, Redshift) and a real serving database (Postgres, MySQL) — that's what `docs/design-pattern.md` and `examples/ecommerce_order_lookup_walkthrough.sql` describe. This implementation substitutes:

- **DuckDB** for the warehouse — embedded, file-based, and close enough to BigQuery's SQL dialect that almost none of the query *shape* changes: it natively supports `QUALIFY`, window functions, and `CREATE OR REPLACE TABLE AS SELECT`.
- **SQLite** for the serving database — embedded, stdlib (zero extra install), and deliberately a *different engine* from DuckDB. The whole point of the pattern is "the warehouse is wrong for point lookups, sync to something else" — making both sides DuckDB would erase that distinction. SQLite's `ALTER TABLE ... RENAME TO` makes the atomic-swap trick work exactly as documented.

Both are zero-server: no account, no credentials, no `docker run`. `infra_config.yaml` marks each with a `production_equivalent` field for exactly this reason — it's documentation, not a switch; `pipeline/` only ever talks to DuckDB and SQLite.

## The Pattern in Three Phases

### Phase 1: Preflight (`pipeline/preflight.py`)
Runs before any real data is written:
1. Runs each domain's DDL (`include/sql/.../warehouse/ddl/*.sql`) for real — but that only ever (re)creates an empty, schema-only placeholder table, never touches real data.
2. Dry-runs each domain's SELECT (`EXPLAIN`, no data scanned) — catches a type/column mismatch immediately.
3. Compares the DDL's declared columns against what the SELECT actually produces — schema drift between "what we said this looks like" and "what the query returns" fails loudly here.
4. Coverage check: every column declared in a domain's DDL (except `_loaded_at`) must appear somewhere in the final lookup query's own SQL text — catching "a column was added to a domain but nobody wired it into the join." (This is a text-scan heuristic, documented as such in `preflight.py` — not full semantic analysis, but it catches the concrete failure mode it exists for.)

**Result**: any failure raises `PreflightError` before Phase 2 runs. Nothing has been touched yet, so there's nothing to roll back.

### Phase 2: Build (`pipeline/build.py`)
Runs only after preflight passes:
1. Build each domain table with a full refresh: `CREATE OR REPLACE TABLE ... AS SELECT` (DuckDB's single-statement equivalent of TRUNCATE + INSERT).
   - `orders` (driving) — one row per order, defines scope
   - `customer_attributes` — customer profile, joins on `customer_id`
   - `order_items` — line items, **aggregated** (`GROUP BY`, not deduped) to one row per `order_id`
   - `shipment` — latest shipment status, joins on `order_id`
   - `customer_support` — open ticket summary, joins on `customer_id`
2. Data-quality gate: driving domain (`orders`) must be non-empty and have unique `order_id`. Raises `DataQualityError` if not — **the lookup table is never rebuilt on a bad run**, so a failure leaves yesterday's good lookup table in place.
3. Final assembly: LEFT JOIN all four enrichment domains onto `orders`.

**Result**: complete, validated `sample_lookup_current` table in the warehouse. Serving DB untouched so far.

### Phase 3: Serve (`pipeline/serve.py`)
Runs only after the DQ gate passes:
1. Export the lookup table to a local Parquet file (`tmp/exports/` — "object storage" is a folder here instead of GCS/S3).
2. Import it into a fresh `staging_order_lookup` table in SQLite (dropped and recreated each run).
3. Build indexes on staging, after the bulk load.
4. Atomic swap, with two-cycle rollback safety: drop the table from two cycles ago, demote the current backup to that slot, then rename `live_order_lookup` → `_old` and `staging_order_lookup` → `live_order_lookup` — all in one transaction (`pipeline/serving_db.py:rotate_and_swap`).
5. Reconcile: compare warehouse and serving-DB row counts; warn (not fail — the swap already happened) if they diverge beyond tolerance.

**Result**: `live_order_lookup` updated with zero downtime; `python -m pipeline.lookup <order_id>` queries it directly.

## Folder Layout

Every file below actually exists — this list is generated to match the real tree, not aspirational.

```
include/
├── README.md                                  # This file
├── config/
│   ├── dag_config.yaml                       # What to build (divisions, domain queries, thresholds)
│   └── infra_config.yaml                     # How to connect (DuckDB/SQLite paths, tunables)
└── sql/ecommerce_orders/                      # One folder per division (see dag_config.yaml)
    ├── warehouse/
    │   ├── ddl/                               # Schema CONTRACTS — read by preflight, never run as "the build"
    │   │   ├── orders.sql
    │   │   ├── customer_attributes.sql
    │   │   ├── order_items.sql
    │   │   ├── shipment.sql
    │   │   └── customer_support.sql
    │   ├── domain/                            # CREATE OR REPLACE TABLE AS SELECT — the real build queries
    │   │   ├── orders.sql
    │   │   ├── customer_attributes.sql
    │   │   ├── order_items.sql
    │   │   ├── shipment.sql
    │   │   └── customer_support.sql
    │   ├── lookup/
    │   │   └── ecommerce_orders_lookup.sql   # Final LEFT JOIN assembly (all 5 domains)
    │   └── dq/
    │       └── dq_lookup.sql                 # Non-empty + unique-order_id assertion (documents the check;
    │                                          # pipeline/build.py implements the parameterized version)
    └── serving_db/
        ├── ddl/
        │   └── serving_ddl.sql               # live_order_lookup schema (created once)
        └── sync/
            ├── create_staging_table.sql      # Fresh staging table, dropped/recreated each run
            ├── export_lookup_to_object_storage.sql  # Runs against the WAREHOUSE connection (DuckDB COPY)
            ├── swap_staging_to_live.sql      # Documents the atomic swap; pipeline/serving_db.py implements
            │                                  # the full version with existence checks (see rotate_and_swap)
            └── drop_old_table.sql            # Documents cleanup; folded into rotate_and_swap too
```

**Two flattening techniques, side by side**: `customer_attributes`, `shipment`, and `customer_support` collapse to one row per key with `QUALIFY ROW_NUMBER() ... = 1` (pick the latest record). `order_items` is naturally many rows per order, so it collapses with `GROUP BY order_id` (aggregate the children) instead — see the comment at the top of `order_items.sql` for why the technique differs.

## Config-Driven: No Python Code Changes to Add Data

**To add a new domain query:**
1. Edit `include/config/dag_config.yaml`: add the query file path to `domain_queries`, and add a matching DDL file under `warehouse/ddl/`
2. Create the SQL file under `include/sql/<division>/warehouse/domain/`

**To change a business parameter (e.g., lookback window):**
1. Edit `include/config/dag_config.yaml`: change `rolling_window_days`
2. `pipeline/build.py` reads this value and injects it into every domain query as `{rolling_window_days}`

**To change connection or tuning:**
1. Edit `include/config/infra_config.yaml`: DuckDB/SQLite paths, import concurrency, indexes, etc.

No changes to `pipeline/*.py` or `dags/order_lookup_sample.py` needed for any of the above — both read the same two config files at runtime.

## Key Design Decisions (Why This Shape?)

| Decision | Alternative | Why? |
|----------|-----------|------|
| `pipeline/` + `dags/` + `include/` split | One monolithic script | `pipeline/` runs standalone (no orchestrator needed) and is what `dags/` calls — the DAG can't drift out of sync with what actually runs |
| DuckDB (warehouse) + SQLite (serving) | Two DuckDB files | Different engines for different jobs is the whole point of the pattern; collapsing them into one engine would erase that distinction |
| Full refresh (`CREATE OR REPLACE TABLE AS SELECT`) | Incremental/MERGE | Simpler correctness story; reprocess bounded window each run |
| Three hard gates (preflight → build → serve) | Single long script | Fail early before writes; each phase is independently testable (see `tests/pipeline/`) |
| DDL files as schema *contracts*, checked by preflight | No separate schema declaration | Catches drift between "what we said this table looks like" and what the query actually returns, before any real data moves |
| LEFT JOIN from driving domain (`orders`) | INNER JOIN all domains | An order with no shipment record yet still appears in the lookup, with null shipment fields, instead of vanishing |
| GROUP BY for `order_items`, QUALIFY for the rest | One dedup technique everywhere | The right collapse strategy depends on whether the source is naturally many-to-one (aggregate) or one-of-many-versions (pick latest) |
| Atomic rename swap with two-cycle rotation | TRUNCATE + re-insert | Zero downtime; live table never empty or half-updated; one prior cycle always available for manual rollback |

## Notes

- Example domain is fictional (ecommerce order lookup) to avoid confusion with real tables
- No credentials, project IDs, or real hostnames — `infra_config.yaml` holds local file paths instead
- This demonstrates ONE division/entity type; scale to multiple divisions by adding entries under `division:` in `dag_config.yaml`
- `pipeline/seed.py` is deterministic (fixed random seed) so re-running produces the same synthetic dataset shape, useful for tests and for reproducing a specific scenario

## See Also

- **[`../docs/design-pattern.md`](../docs/design-pattern.md)** — Full architecture rationale, in production terms (the "why" behind every phase here)
- **[`../examples/ecommerce_order_lookup_walkthrough.sql`](../examples/ecommerce_order_lookup_walkthrough.sql)** — The same pattern as one linear, production-flavored SQL file
- **[`../docs/reliability-checklist.md`](../docs/reliability-checklist.md)** — Pre-production validation checklist
- **[`../tests/pipeline/test_pipeline.py`](../tests/pipeline/test_pipeline.py)** — Real tests: schema drift detection, DQ gate blocking bad data, atomic swap rotation
