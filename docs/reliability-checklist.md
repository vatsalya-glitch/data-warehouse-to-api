# Reliability Checklist

Use this checklist before deploying your batch pipeline to production.

## Phase 1: Preflight

- [ ] Schema dry-run implemented for every target table
- [ ] Dry-run validates column types and counts without scanning data
- [ ] Coverage check compares upstream columns against downstream expectations
- [ ] Missing upstream fields cause the pipeline to fail (not silently drop)
- [ ] All queries tested with sample data before production deployment

## Phase 2: Build

- [ ] Full-refresh strategy chosen (TRUNCATE + INSERT) or incremental justified
- [ ] Lookback window is bounded and configured (not hardcoded)
- [ ] Deduplication uses window functions (QUALIFY, ROW_NUMBER) explicitly
- [ ] Tie-break logic is documented (e.g., "latest by updated_at DESC")
- [ ] One "driving" domain table chosen for final join
- [ ] All enrichment domains LEFT JOIN onto driving table (no INNER JOINs)
- [ ] Data-quality gate checks before final assembly:
  - [ ] Driving domain table is non-empty
  - [ ] Primary keys are unique within driving domain
  - [ ] Row counts make sense (no unexplained 90% drop)
- [ ] Failed data-quality check leaves yesterday's lookup table intact
- [ ] Date-sharded intermediate tables isolate each run (e.g., `table_YYYYMMDD`)

## Phase 3: Serve

- [ ] Duplicate check on primary key implemented before import
- [ ] Export uses object storage (CSV/Parquet) not streaming through orchestrator
- [ ] Staging table used for import, never the live table directly
- [ ] Import concurrency explicitly capped (`import_max_concurrency`)
- [ ] Concurrency limit matched to serving database sustainable load
- [ ] Indexes built after bulk load, not before
- [ ] Atomic swap implemented:
  ```sql
  BEGIN;
  ALTER TABLE live_table RENAME TO live_table_old;
  ALTER TABLE staging_table RENAME TO live_table;
  COMMIT;
  ```
- [ ] Atomic swap completes in constant time regardless of table size
- [ ] Live table never empty or half-updated from application's perspective
- [ ] Row-count reconciliation happens after swap
- [ ] Warehouse row count matches serving database row count (within tolerance)
- [ ] Previous live table (`_old`) kept for one cycle as rollback safety net
- [ ] Previous table dropped only at end of *next* successful run

## Idempotency & Retry Safety

- [ ] All steps can be safely retried without causing failures
- [ ] Staging tables use `DROP IF EXISTS` before creation
- [ ] All cleanup steps use `IF EXISTS`
- [ ] No "already exists" errors on retry
- [ ] Transactional boundaries clear (atomic operations identified)

## Monitoring & Alerting

- [ ] Pipeline failure alerts wired to team alert channel (Slack, PagerDuty, etc.)
- [ ] Alerting happens before anyone notices missing data downstream
- [ ] Row-count reconciliation failures trigger alerts
- [ ] Schema validation failures trigger alerts
- [ ] Data-quality gate failures trigger alerts
- [ ] Silent pipeline failures impossible (alerting is failsafe)

## Configuration Management

- [ ] All business parameters live in config (YAML, TOML, or equivalent) — a "what to build" file and a "how to connect" file is fine (see `config/dag_config.yaml` + `infra_config.yaml`); scattering the same parameter across many SQL files is not
- [ ] No hardcoded literals in SQL queries:
  - [ ] Lookback window
  - [ ] Concurrency limits
  - [ ] Threshold values
  - [ ] Table names (if dynamic)
- [ ] Config file has examples and documentation for each parameter
- [ ] Config changes don't require code changes or redeploy

## Testing & Validation

- [ ] Full pipeline dry-run tested end-to-end
- [ ] Query-level tests written for:
  - [ ] Each domain table deduplication
  - [ ] Final assembly join logic
  - [ ] Data-quality gate logic
- [ ] Edge cases tested:
  - [ ] Empty domains (optional enrichments)
  - [ ] Duplicate primary keys (should be caught)
  - [ ] Missing expected fields (should be caught)
  - [ ] Schema drift upstream (should be caught)
- [ ] Rollback procedure tested and documented
- [ ] Recovery from failed import tested

## Pre-Production Sign-Off

- [ ] All items above completed and verified
- [ ] Load test done: pipeline completes within SLA
- [ ] Concurrent reader load test done: serving database handles app traffic
- [ ] Failover tested: previous table serves if current swap fails
- [ ] Team trained on runbook and manual rollback
- [ ] On-call team has runbook and escalation path
