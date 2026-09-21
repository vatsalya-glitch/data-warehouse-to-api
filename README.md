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

## Quick Start

1. Read the [Design Pattern](docs/design-pattern.md) to understand the architecture and why each phase is shaped the way it is
2. Read the [walkthrough](examples/library_lending_walkthrough.sql) — the same pattern as one linear SQL file, top to bottom
3. Browse [`dags/library_lending_sample/`](dags/library_lending_sample/) — the same pattern laid out as an actual, config-driven Airflow project (real folder structure, real config files, one query per file)
4. Adapt `dags/library_lending_sample/` to your own warehouse, serving database, and business domain

## Repository Map

Every doc and example in this repo uses the **same fictional domain — a library lending system** (loans, patrons, books) — so nothing needs re-explaining as you move between files. Three views of one pattern, in increasing order of concreteness:

| Where | What it is | Read this when |
|---|---|---|
| [`docs/design-pattern.md`](docs/design-pattern.md) | The architecture explained in prose — the "why" behind each phase | You want to understand the pattern before writing any code |
| [`examples/library_lending_walkthrough.sql`](examples/library_lending_walkthrough.sql) | One SQL file, Phase 1 → 2 → 3, read top to bottom | You want to see the whole flow in five minutes |
| [`dags/library_lending_sample/`](dags/library_lending_sample/) | The same pattern as a real project layout — config files, one SQL file per query, an Airflow DAG that wires them together | You're about to build this for real and want a folder structure to copy |

There is **one config system**, defined in `dags/library_lending_sample/dag_config.yaml` (what to build) and `infra_config.yaml` (how to connect) — see their inline comments and the [Config-Driven Parameters](docs/design-pattern.md#config-driven-parameters-not-hardcoded-literals) section of the design doc for the reasoning.

Everything under `dags/library_lending_sample/` is an **illustrative stub**, not working code — SQL bodies are 2–5 lines and Python functions are interfaces with `# TODO` markers. It shows you the shape to build, not a pipeline to copy-paste and run.

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
- [Walkthrough](examples/library_lending_walkthrough.sql) — Single-file, linear read of the full pattern
- [Reference Pipeline](dags/library_lending_sample/) — The pattern as a real, config-driven Airflow project
- [Reliability Checklist](docs/reliability-checklist.md) — Pre-production validation

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

See [LICENSE](LICENSE) for details.
