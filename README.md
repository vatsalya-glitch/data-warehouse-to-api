# Data Warehouse to API

Production-ready architecture for transforming wide, denormalized data warehouse queries into fast, concurrent-safe point lookups. A three-phase batch pipeline (Preflight → Build → Serve) with built-in reliability, configuration-driven parameters, and atomic table swaps for zero-downtime updates.

## Problem

Your data warehouse (BigQuery, Snowflake, Redshift) excels at analytical queries over massive datasets, but point lookups are slow and expensive:
- Unpredictable query latency under concurrent load
- Per-byte scan charges add up at volume
- Application teams need a hard dependency on a stable, fast lookup table

## Solution

A daily (or otherwise scheduled) batch pipeline that:
1. **Preflight**: Validates schema and data before touching anything
2. **Build**: Assembles domain tables and creates a denormalized lookup
3. **Serve**: Exports to object storage, imports to Postgres (or similar), and performs an atomic swap

Result: A lightweight serving database that applications query for millisecond-latency, high-concurrency point lookups. The live table is never empty or half-updated.

## Run It Locally

This repo is implementation-ready: clone it, install one requirements file, and the whole three-phase pipeline runs against real (synthetic) data — no cloud account, no server to start.

```bash
git clone <this-repo>
cd data-warehouse-to-api
pip install -r requirements.txt

python -m pipeline.run                    # loads data/raw/*.csv, then runs all 3 phases
python -m pipeline.lookup ORDER-00001     # point-lookup the result, like a real app would
```

`pipeline/run.py` loads the static synthetic data under `data/raw/` (customers, orders, line items, shipments, support tickets) into a local `warehouse.duckdb` on every run, then executes Preflight → Build → Serve for real, landing the result in `serving.db`. Run it again and you'll see the atomic swap and two-cycle backup rotation happen for real. Run the tests to see the data-quality gate actually block bad data:

```bash
pytest tests/
```

**Why DuckDB and SQLite instead of BigQuery and Postgres?** Both are embedded — no server, no account, no credentials — so the pattern runs identically on your laptop and in CI. See [`docs/implementation.md`](docs/implementation.md) for the full reasoning and how each maps to its production equivalent.

**Want to see it run under Airflow instead?** `dags/order_lookup_sample.py` is a thin wrapper that calls the exact same `pipeline/` functions from Airflow tasks — see [Reference: Airflow](#reference-airflow-orchestration) below. It's optional; nothing above requires it.

## Repository Map

Every doc and example in this repo uses the **same fictional domain — an ecommerce order lookup** (`orders` as the driving domain, enriched with `customer_attributes`, `order_items`, `shipment`, and `customer_support`) — so nothing needs re-explaining as you move between files.

| Where | What it is | Read this when |
|---|---|---|
| [`docs/design-pattern.md`](docs/design-pattern.md) | The architecture explained in prose — the "why" behind each phase, in production terms (BigQuery/Snowflake + Postgres) | You want to understand the pattern before writing any code |
| [`examples/ecommerce_order_lookup_walkthrough.sql`](examples/ecommerce_order_lookup_walkthrough.sql) | One SQL file, Phase 1 → 2 → 3, read top to bottom, in the same production-flavored SQL as the design doc | You want to see the whole flow in five minutes |
| [`pipeline/`](pipeline/) | **The real, runnable implementation** — DuckDB warehouse, SQLite serving DB, real Python, actually tested | You want to run the pattern and see it work |
| [`dags/`](dags/) + [`config/`](config/) + [`sql/`](sql/) | The same real implementation, orchestrated by Airflow instead of `python -m pipeline.run` | You want to see how this maps onto a production orchestrator |

```
pipeline/            ← the real implementation (run this)
├── config.py          loads config/*.yaml
├── warehouse.py       DuckDB: dry-run validation, column introspection, execution
├── serving_db.py       SQLite: staging, atomic swap + rotation, reconciliation
├── preflight.py, build.py, serve.py    the three phases, real logic
├── load_raw_data.py   loads data/raw/*.csv into the warehouse
├── run.py             CLI: python -m pipeline.run
└── lookup.py          CLI: python -m pipeline.lookup <order_id>

dags/order_lookup_sample.py   ← thin Airflow wrapper calling the same pipeline/ functions
config/                dag_config.yaml (what to build) + infra_config.yaml (how to connect)
sql/ecommerce_orders/   one query per file — the SQL pipeline/ actually executes
data/raw/              static synthetic source data (CSV) — see data/raw/README.md

tests/
├── pipeline/           real end-to-end tests (temp DuckDB + SQLite, actually run the pipeline)
└── dags/               structural checks on the DAG file and config/SQL alignment
```

There is **one config system** either way: `config/dag_config.yaml` (what to build) and `infra_config.yaml` (how to connect) — read by `pipeline/config.py` whether you run it standalone or under Airflow. See the [Config-Driven Parameters](docs/design-pattern.md#config-driven-parameters-not-hardcoded-literals) section of the design doc for the reasoning, and [`docs/implementation.md`](docs/implementation.md) for a full folder-by-folder tour.

## Reference: Airflow Orchestration

`dags/order_lookup_sample.py` is not required to run this repo, but it's real, not illustrative: it imports `pipeline.preflight`, `pipeline.build`, and `pipeline.serve` directly and calls them from `PythonOperator` tasks, so it can't drift out of sync with the standalone runner. It needs `apache-airflow`, which is deliberately **not** in `requirements.txt` — install it separately if you want to try it:

```bash
pip install -r requirements.txt -r requirements-airflow.txt \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt"
```

## Key Features

✅ **Reliability**: Schema validation and data-quality gates before writes  
✅ **Zero-downtime**: Atomic rename-based table swaps  
✅ **Rollback safety**: Previous table kept for one cycle  
✅ **Configuration-driven**: One config file, no SQL duplication  
✅ **Idempotent**: Retryable, safe to repeat  
✅ **Row-count reconciliation**: Verify warehouse-to-serving accuracy  

## Architecture Overview

```
Source tables (warehouse)
        │
        ▼
┌───────────────────┐
│ Phase 1: Preflight │   validate schema before touching any data
└─────────┬──────────┘
          ▼
┌───────────────────┐
│ Phase 2: Build     │   assemble per-domain tables → denormalized lookup
└─────────┬──────────┘
          ▼
┌───────────────────┐
│ Phase 3: Serve     │   export → import → atomic swap into serving DB
└─────────┬──────────┘
          ▼
   Application / API reads from serving DB
```

## When This Fits

✓ Application needs point lookups by ID  
✓ Some staleness acceptable (hours, not seconds)  
✓ Data volume bounded for full-refresh batch jobs  

## When It Doesn't

✗ Need sub-second freshness → use streaming/CDC  
✗ Ad-hoc filtering on lookups → use warehouse or search index  
✗ Data volume outgrows batch window → evolve to incremental loading  

## Documentation

- [Design Pattern](docs/design-pattern.md) — Complete technical guide
- [Walkthrough](examples/ecommerce_order_lookup_walkthrough.sql) — Single-file, linear read of the full pattern
- [`docs/implementation.md`](docs/implementation.md) — Folder-by-folder tour of the real implementation and why DuckDB/SQLite stand in for BigQuery/Postgres
- [Reliability Checklist](docs/reliability-checklist.md) — Pre-production validation

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

See [LICENSE](LICENSE) for details.
