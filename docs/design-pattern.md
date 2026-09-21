# Design Pattern: A Reliable Batch Pipeline for Serving a Warehouse-Backed Lookup API

*A reusable architecture for turning wide, joined warehouse data into a fast, always-available lookup table for downstream applications (APIs, chatbots, internal tools).*

## The problem this pattern solves

A common need in data engineering: some application — a support tool, a chatbot, an internal dashboard — needs to look up a single entity (a customer, an order, a device) by ID and get back a rich, denormalized view assembled from many different source tables, in milliseconds. The data warehouse where all that source data lives (BigQuery, Snowflake, Redshift, etc.) is great at analytical joins but is not built to serve millisecond, high-concurrency point lookups the way an OLTP database is.

The naive answer — "just query the warehouse directly from the app" — breaks down for a few reasons: warehouse query latency is unpredictable under concurrent load, warehouses charge per byte scanned so point lookups get expensive at volume, and application teams don't want a hard dependency on the analytics team's query patterns changing underneath them.

The pattern below solves this with a daily (or otherwise scheduled) batch pipeline that assembles the joined view once, on a schedule, and syncs the result into a lightweight serving database that the application queries directly. It was developed and refined running a production pipeline of exactly this shape, and the specific problems it ran into — and the fixes — are baked into the design below.

## High-level architecture

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

Each phase is a hard gate: nothing in Phase 2 runs until every check in Phase 1 passes, and nothing in Phase 3 runs until Phase 2's own data-quality checks pass. The guiding principle is **fail before you write, not after** — every validation that can happen before data moves, does.

## Phase 1 — Preflight: validate before you touch anything

Before any table is written, the pipeline:

1. Runs `CREATE OR REPLACE TABLE` (or equivalent) for every target table, so the schema is guaranteed fresh and matches the current table definition.
2. **Dry-runs** every INSERT query against that freshly-created schema. Most warehouses (BigQuery included) support a dry-run mode that validates column types and counts without scanning or writing any data — this catches a schema mismatch in milliseconds instead of after a multi-minute job fails.
3. Compares the columns produced by each domain-building query against the columns expected by the final assembly step, and fails the whole run if anything is missing. This closes a specific failure mode: someone adds a field to a domain table but forgets to also add it to the query that assembles the final lookup, and the omission stays silent until someone downstream is confused.

```sql
-- illustrative: BigQuery dry-run validation
-- (jobConfig.dryRun = true — no data is scanned or written)
SELECT * FROM `<candidate insert query>` LIMIT 0
```

This phase is cheap (dry-runs cost nothing) and turns "the pipeline silently produced wrong data" into "the pipeline refused to run" — a much easier failure to catch in review or CI.

## Phase 2 — Build: assemble domain tables, then the lookup

Each source domain (orders, support tickets, shipments, customer profile — whatever your entity's constituent parts are) gets its own query that reads from source tables and writes one clean, deduplicated table. A few sub-patterns recur here:

**Full refresh over incremental, by default.** `TRUNCATE` + `INSERT` is simpler to reason about than incremental/MERGE logic, and it means every run reflects a consistent point-in-time view. The tradeoff is reprocessing a full lookback window every run — acceptable as long as that window is bounded (see "config-driven windows" below) and the source data volume is manageable. Reach for incremental loading only once full-refresh cost becomes a real problem, not by default.

**Deduplicate with a window function, not by trusting the source.** Source tables are frequently at a finer grain than you want (e.g., one row per line item, when you want one row per order). Rather than assuming uniqueness, every domain query explicitly collapses to the target grain with a deterministic tie-break:

```sql
SELECT *
FROM raw_source
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY entity_id
    ORDER BY updated_at DESC, secondary_tiebreak DESC
) = 1
```

**Pick one domain as the "driving" table for the final join, and LEFT JOIN everything else onto it.** If you INNER JOIN across all domains, an entity is silently dropped from the whole lookup the moment any *one* domain doesn't have a matching row for it — which is very easy to not notice until someone asks "where did all the X go?" weeks later. Decide which domain defines "this entity is in scope" (usually your primary transactional table), and LEFT JOIN every enrichment domain onto it, so a missing enrichment produces nulls in one field rather than deleting the row.

**Gate the final assembly behind a data-quality check on the domain tables.** Before the final denormalized table is rebuilt, check that the driving domain table actually has rows (not empty) and that primary keys are unique within it. If this check fails, stop before truncating the final lookup table — a failed run should leave yesterday's still-valid lookup table in place, not replace it with an empty or broken one.

## Phase 3 — Serve: get it into a fast, always-available database

The final lookup table now lives in the warehouse, which still isn't the right place to serve point lookups from. The sync to a serving database (Postgres, in the reference implementation) follows this sequence:

1. **Duplicate check on the primary key**, cheap and early, before the expensive import step starts — catching a duplicate here costs seconds; catching it after a 30-minute import (when a unique index build fails) costs the whole run.
2. **Export to shard files** (object storage, CSV/Parquet) rather than streaming rows through the orchestrator — warehouses are good at bulk exports, and this decouples export speed from import speed.
3. **Import shards in parallel** into a *staging* table in the serving database — never the live table directly. Concurrency is capped explicitly (`import_max_concurrency` or equivalent), matched to what the serving database can actually sustain without starving other traffic.
4. **Build indexes on staging, after the bulk load, not before.** Index maintenance during a bulk insert is much slower than building the index once at the end.
5. **Atomic swap: rename staging to live inside one transaction.** This is the load-bearing trick of the whole pattern:

```sql
BEGIN;
ALTER TABLE live_table RENAME TO live_table_old;
ALTER TABLE staging_table RENAME TO live_table;
COMMIT;
```

   This is a catalog-only operation — no rows move — so it completes in roughly constant time regardless of table size, and the live table is **never empty and never half-updated** from the application's point of view. A reader mid-query either sees the old complete table or the new complete table, never a partial one.

6. **Reconcile row counts** between the warehouse source-of-truth and what actually landed in the serving database, after the swap, as a final correctness check.
7. **Keep the previous live table around for one cycle** (as `_old`) before dropping it, as a manual rollback safety net, and only drop it at the very end of the next successful run.

## Design decisions worth calling out explicitly

| Decision | Alternative considered | Why this choice |
|---|---|---|
| Full refresh (TRUNCATE + INSERT) | Incremental MERGE | Simpler correctness story; revisit only if reprocessing cost becomes real |
| Date-sharded intermediate table (`table_YYYYMMDD`) | Native partitioning on one table | Isolates each run's data completely — a bad run can be inspected or discarded without touching prior days |
| QUALIFY/window-function dedup | Trusting source uniqueness | Explicit, auditable, survives source-grain surprises |
| LEFT JOIN from one driving domain | INNER JOIN across all domains | Prevents silent, hard-to-notice row loss |
| Rename-based atomic swap | TRUNCATE + re-insert into live table | Zero downtime; live table is never empty or partial |
| Config-driven parameters (see below) | Hardcoded literals in SQL | One place to change a business rule; no per-file drift |

## Config-driven parameters, not hardcoded literals

Any value that's a business decision rather than a structural constant — a lookback window, a concurrency limit, a threshold — belongs in one config file, not repeated as a literal across every query file that needs it. The pattern:

```yaml
# config.yaml
rolling_window_days: 90
```

```python
# orchestration code
config = load_config("config.yaml")
WINDOW_DAYS = config["rolling_window_days"]

sql = load_sql_file("domain_query.sql").format(rolling_window_days=WINDOW_DAYS)
```

```sql
-- domain_query.sql
WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {rolling_window_days} DAY)
```

This looks trivial, but it's what turns "change the lookback window" from a multi-file grep-and-replace (easy to miss one file) into a one-line config edit. The same approach generalizes to any repeated business parameter: retention windows, concurrency caps, feature flags for which source domains are active.

## Reliability checklist for a pipeline built this way

- Schema dry-run before every write.
- A coverage check that fails loudly if a field exists upstream but isn't propagated downstream — don't let that be silent.
- A data-quality gate (non-empty, unique keys) between "build" and "serve," so a bad build never overwrites a good serving table.
- Idempotent, retry-safe steps: dropping-and-recreating a staging table, `IF EXISTS` on cleanup steps, so a retried run doesn't fail on "already exists."
- Alerting on failure wired to wherever your team actually looks (chat ops channel, on-call tool) — a silent pipeline failure is worse than a loud one.
- One rollback-safety cycle: don't drop the previous live table until the *next* run has already succeeded.

## When this pattern fits (and when it doesn't)

This is a good fit when: the consuming application needs point lookups by a stable ID, some staleness (hours, not seconds) is acceptable, and the data volume is bounded enough that a full-refresh batch job completes comfortably within your scheduling window.

It's a poor fit when: the application needs near-real-time freshness (this calls for a streaming/CDC pattern instead), the lookup needs ad-hoc filtering rather than point lookups by ID (that's what the warehouse itself, or a search index, is for), or data volume has grown large enough that full-refresh reprocessing no longer fits the schedule — at that point, incremental/CDC loading into the domain tables is the natural next evolution of this same three-phase shape.

## Seeing it as code

- [`../examples/library_lending_walkthrough.sql`](../examples/library_lending_walkthrough.sql) — every phase above, as one SQL file, read top to bottom
- [`../dags/library_lending_sample/`](../dags/library_lending_sample/) — the same pattern laid out as a real, config-driven Airflow project (one query per file, wired by `main.py`)
