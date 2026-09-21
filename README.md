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

1. Read the [Design Pattern](docs/design-pattern.md) to understand the architecture
2. Explore the [examples](examples/) for your warehouse and serving database
3. Adapt the [config template](config/config.example.yaml) to your schema
4. Deploy using your orchestration tool (Airflow, dbt Cloud, etc.)

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
- [Examples](examples/) — BigQuery + Postgres reference implementation
- [Configuration](config/) — Config templates and parameters
- [Reliability Checklist](docs/reliability-checklist.md) — Pre-production validation

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

See [LICENSE](LICENSE) for details.
