"""
Sample Airflow DAG: Warehouse to Serving Database Three-Phase Pipeline

Example domain: Ecommerce order lookup
- Orders: the driving domain (defines which entities are in scope)
- Customer attributes: customer profile (enrichment, joins on customer_id)
- Order items: line items, aggregated to one row per order (enrichment)
- Shipment: latest shipment status per order (enrichment)
- Customer support: open ticket summary per customer (enrichment)

This DAG demonstrates the structural PATTERN, not a working pipeline.
Replace all TODO sections with real business logic.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
import yaml
import sys
import os

# Load configuration (illustrative only)
DAG_DIR = os.path.dirname(__file__)
DAG_CONFIG_PATH = os.path.join(DAG_DIR, "dag_config.yaml")
INFRA_CONFIG_PATH = os.path.join(DAG_DIR, "infra_config.yaml")

with open(DAG_CONFIG_PATH) as f:
    dag_config = yaml.safe_load(f)

with open(INFRA_CONFIG_PATH) as f:
    infra_config = yaml.safe_load(f)


# ============================================================================
# PHASE 1: PREFLIGHT — Validate schemas before any writes
# ============================================================================

def _build_preflight(divisions_config: dict) -> TaskGroup:
    """
    Phase 1 — Preflight validation gate. Runs before any data is written.

    For every domain query declared in config:
      1. Run its DDL (CREATE OR REPLACE TABLE) to guarantee a fresh schema.
      2. Dry-run its INSERT query against that schema (no data written) to
         catch type/column mismatches immediately.

    All dry-run results feed into one final coverage check that fails the
    whole run if a field exists in a domain table but isn't propagated to
    the final lookup query.
    """

    def validate_domain_schema(domain_name: str, **context):
        """Dry-run domain query to validate schema."""
        # TODO: replace with real warehouse connection
        context["task_instance"].log.info(
            f"Validating schema for domain: {domain_name}"
        )
        # TODO: call warehouse API with dry_run=true on domain query
        # Example: bigquery.run_query(sql, dry_run=True)

    def check_column_coverage(**context):
        """
        Fail if any column in domain tables isn't represented in lookup.
        This catches "someone added a column and forgot to wire it into the join."
        """
        # TODO: query INFORMATION_SCHEMA on warehouse and lookup query
        # Fail if any domain column is missing from final lookup
        context["task_instance"].log.info(
            "✓ Column coverage check passed"
        )

    with TaskGroup("preflight_validation") as preflight_group:
        # One task per domain
        for division_name, division_config in divisions_config.items():
            for domain_query in division_config.get("domain_queries", []):
                domain_name = domain_query.split("/")[-1].replace(".sql", "")

                validate = PythonOperator(
                    task_id=f"validate_{division_name}_{domain_name}",
                    python_callable=validate_domain_schema,
                    op_kwargs={"domain_name": domain_name},
                )

        # Final coverage check
        coverage = PythonOperator(
            task_id="check_column_coverage",
            python_callable=check_column_coverage,
        )

    return preflight_group


# ============================================================================
# PHASE 2: BUILD — Assemble domain tables and final lookup
# ============================================================================

def _build_division_tasks(divisions_config: dict) -> list:
    """
    Phase 2 — Data build. Runs only after preflight passes.

    Per division:
      1. One task per domain query (full refresh — TRUNCATE+INSERT)
      2. Data-quality gate (non-empty driving table, no duplicate keys)
      3. Final lookup-assembly task that joins all domains

    A failed DQ gate stops here — the lookup table is never truncated on a
    bad run, so a failure leaves yesterday's good data in place.
    """

    def build_domain_table(domain_name: str, query_file: str, **context):
        """Execute domain query (TRUNCATE + INSERT)."""
        # TODO: replace with real warehouse connection
        context["task_instance"].log.info(
            f"Building domain table: {domain_name}"
        )
        # with open(query_file) as f:
        #     sql = f.read()
        #     rolling_window = dag_config["rolling_window_days"]
        #     sql = sql.format(rolling_window_days=rolling_window)
        # TODO: execute on warehouse with TRUNCATE+INSERT

    def run_dq_checks(division_name: str, primary_key: str, min_rows: int, **context):
        """
        Assert driving domain table is non-empty and has no duplicate keys.
        Fail if checks don't pass — don't proceed to final lookup.
        SQL: domains/<division>/warehouse/dq/dq_lookup.sql

        `primary_key` and `min_rows` come straight from this division's entry
        in dag_config.yaml — changing a threshold there changes what this
        check enforces, with no code change here.
        """
        # TODO: replace with real queries (see utils.dq_utils.run_domain_dq_checks)
        context["task_instance"].log.info(
            f"Running DQ checks for division: {division_name} "
            f"(primary_key={primary_key}, min_rows={min_rows})"
        )
        # TODO: query driving domain table
        # - Check row count >= min_rows
        # - Check no duplicate values in primary_key column
        # Raise exception if either check fails

    def build_lookup(division_name: str, lookup_query: str, **context):
        """
        Assemble final denormalized lookup via LEFT JOINs.
        SQL: domains/<division>/<lookup_query>  (path comes from dag_config.yaml)
        """
        # TODO: replace with real query
        context["task_instance"].log.info(
            f"Building final lookup for division: {division_name} "
            f"using {lookup_query}"
        )
        # TODO: execute the query at lookup_query against the warehouse
        # Result: one row per entity, with all enrichment columns

    division_groups = []

    for division_name, division_config in divisions_config.items():
        with TaskGroup(f"division_{division_name}") as division_group:
            # Build each domain
            domain_tasks = []
            for domain_query in division_config.get("domain_queries", []):
                domain_name = domain_query.split("/")[-1].replace(".sql", "")

                task = PythonOperator(
                    task_id=f"build_{domain_name}",
                    python_callable=build_domain_table,
                    op_kwargs={
                        "domain_name": domain_name,
                        "query_file": domain_query,
                    },
                )
                domain_tasks.append(task)

            # DQ gate (only after all domains built)
            dq = PythonOperator(
                task_id="dq_checks",
                python_callable=run_dq_checks,
                op_kwargs={
                    "division_name": division_name,
                    "primary_key": division_config["primary_key"],
                    "min_rows": division_config.get("min_rows", 0),
                },
            )

            # Final lookup (only if DQ passes)
            lookup = PythonOperator(
                task_id="build_lookup",
                python_callable=build_lookup,
                op_kwargs={
                    "division_name": division_name,
                    "lookup_query": division_config["lookup_query"],
                },
            )

            domain_tasks >> dq >> lookup
            division_groups.append(division_group)

    return division_groups


# ============================================================================
# PHASE 3: SERVE — Export, import, swap, reconcile
# ============================================================================

def _build_serving_sync() -> TaskGroup:
    """
    Phase 3 — Serving sync. Exports the finished lookup table to object
    storage, imports it in parallel shards into a staging table in the
    serving database, builds indexes on staging (after load, not before),
    then does an atomic rename-based swap into the live table inside one
    transaction.

    Ends with row-count reconciliation and cleanup.
    """

    def export_lookup_to_storage(**context):
        """
        Export lookup table to object storage in shards.
        SQL shape: domains/<division>/serving_db/sync/export_lookup_to_object_storage.sql
        """
        # TODO: replace with real export logic
        context["task_instance"].log.info(
            "Exporting lookup table to object storage"
        )
        # TODO: call warehouse export API (e.g., BigQuery.extract_table),
        # running the query in export_lookup_to_object_storage.sql
        # Output: sharded files in gs://bucket/path/
        # Discover the resulting shard URIs with:
        #   utils.serving_sync_utils.list_export_shards(...)

    def import_shards_to_staging(**context):
        """
        Bulk-load shards into serving DB staging table.
        Staging table schema: domains/<division>/serving_db/sync/create_staging_table.sql
        """
        # TODO: replace with real import logic
        context["task_instance"].log.info(
            "Importing shards to staging table"
        )
        # TODO: first run create_staging_table.sql to get a fresh, empty staging table
        # TODO: for each shard in parallel (up to infra_config["serving_db"]["import_max_concurrency"]):
        #   - utils.serving_sync_utils.import_single_shard(conn, shard_uri, staging_table)

    def build_staging_indexes(**context):
        """Build indexes on staging table after bulk load."""
        # TODO: replace with real index creation
        context["task_instance"].log.info(
            "Building indexes on staging table"
        )
        # TODO: for each index in infra_config["serving_db"]["staging_indexes"]:
        #   - CREATE INDEX on staging table

    def atomic_swap_and_reconcile(**context):
        """
        Atomic rename swap (staging → live) and reconcile row counts.
        SQL: domains/<division>/serving_db/sync/swap_staging_to_live.sql

        The swap is a catalog-only operation (zero downtime):
          live_lookup → live_lookup_old
          staging_lookup → live_lookup
        """
        # TODO: replace with real swap logic
        context["task_instance"].log.info(
            "Performing atomic swap and reconciliation"
        )
        # TODO: run swap_staging_to_live.sql inside one transaction
        # TODO: utils.dq_utils.run_reconciliation_checks(...) —
        # query row counts on warehouse vs serving_db, log warning if they diverge

    def cleanup_old_tables(**context):
        """
        Drop previous cycle's backup table.
        SQL: domains/<division>/serving_db/sync/drop_old_table.sql
        """
        # TODO: replace with real cleanup
        context["task_instance"].log.info(
            "Cleaning up old tables"
        )
        # TODO: run drop_old_table.sql (drops live_lookup_old_old, keeps one cycle only)

    with TaskGroup("serving_sync") as serving_group:
        export = PythonOperator(
            task_id="export_to_storage",
            python_callable=export_lookup_to_storage,
        )

        import_shards = PythonOperator(
            task_id="import_to_staging",
            python_callable=import_shards_to_staging,
        )

        indexes = PythonOperator(
            task_id="build_indexes",
            python_callable=build_staging_indexes,
        )

        swap = PythonOperator(
            task_id="swap_and_reconcile",
            python_callable=atomic_swap_and_reconcile,
        )

        cleanup = PythonOperator(
            task_id="cleanup",
            python_callable=cleanup_old_tables,
        )

        export >> import_shards >> indexes >> swap >> cleanup

    return serving_group


# ============================================================================
# DAG DEFINITION
# ============================================================================

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}

dag = DAG(
    "order_lookup_sample",
    default_args=default_args,
    description="Sample: warehouse-to-serving-DB pipeline (structural reference)",
    schedule_interval=dag_config.get("schedule_interval", "0 2 * * *"),
    catchup=False,
    tags=["sample", "reference"],
)

# Build phases from config — this is what makes the pipeline config-driven:
# adding a division means adding an entry under `division:` in dag_config.yaml,
# not touching this file.
divisions = dag_config["division"]

preflight = _build_preflight(divisions)
build_phases = _build_division_tasks(divisions)
serving = _build_serving_sync()

# Wire phases: preflight → build → serve
with dag:
    preflight >> build_phases >> serving
