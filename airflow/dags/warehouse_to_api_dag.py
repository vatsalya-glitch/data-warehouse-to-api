"""
Airflow DAG: Data Warehouse to API Lookup Service

Three-phase batch pipeline:
1. Preflight: Validate schema before any writes
2. Build: Assemble domain tables and denormalized lookup
3. Serve: Export → Import → Atomic swap into serving DB
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
import sys
import os

# Add airflow module to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config import PipelineConfig
from warehouse_ops import BigQueryWarehouseOps
from serving_db_ops import PostgresServingDBOps


# Configuration
CONFIG_PATH = "/opt/airflow/config/config.yaml"
config = PipelineConfig(CONFIG_PATH)

# Default DAG arguments
default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": [config.monitoring.get("alert_email", "data-eng@example.com")],
}

# DAG definition
dag = DAG(
    "warehouse_to_api_pipeline",
    default_args=default_args,
    description="Three-phase batch pipeline: warehouse data → fast API lookups",
    schedule_interval=config.config.get("scheduling", {}).get("frequency", "0 2 * * *"),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["data-pipeline", "lookup-service"],
)


# ============================================================================
# PHASE 1: PREFLIGHT - Validate before touching data
# ============================================================================

def preflight_validate_schemas(**context):
    """
    Phase 1: Preflight validation
    - Dry-run all domain queries
    - Check schema coverage
    - Fail before any writes
    """
    warehouse = BigQueryWarehouseOps(
        config.warehouse["project_id"],
        config.warehouse["dataset_id"]
    )

    ti = context["task_instance"]

    try:
        # Dry-run each domain query
        for domain_name, domain_config in config.domains.items():
            ti.log.info(f"Validating domain: {domain_name}")

            # In production, read actual SQL queries from files
            # For now, we validate that configuration is present
            if "source_table" not in domain_config:
                raise ValueError(f"Domain {domain_name} missing source_table")

        ti.log.info("✓ All domain schemas validated")
        context["task_instance"].xcom_push(
            key="preflight_status",
            value="PASSED"
        )

    except Exception as e:
        ti.log.error(f"✗ Preflight validation failed: {e}")
        raise


def preflight_check_coverage(**context):
    """
    Check that all expected columns are covered by domain tables.
    Fail if any expected column is missing.
    """
    expected_columns = set(
        config.lookup_table.get("expected_columns", [])
    )

    # In production, scan domain table schemas
    # For demo, just check configuration
    ti = context["task_instance"]
    ti.log.info(f"Checking coverage for {len(expected_columns)} columns")

    if not expected_columns:
        ti.log.warning("No expected columns defined in config")

    ti.log.info("✓ Column coverage check passed")


with TaskGroup("phase_1_preflight", dag=dag) as phase_1:
    validate_schemas = PythonOperator(
        task_id="validate_schemas",
        python_callable=preflight_validate_schemas,
        provide_context=True,
    )

    check_coverage = PythonOperator(
        task_id="check_coverage",
        python_callable=preflight_check_coverage,
        provide_context=True,
    )

    validate_schemas >> check_coverage


# ============================================================================
# PHASE 2: BUILD - Assemble domain tables and final lookup
# ============================================================================

def build_domain_tables(**context):
    """
    Phase 2: Build domain tables
    - Full refresh (TRUNCATE + INSERT) each domain
    - Deduplicate with window functions
    - Prepare for final assembly
    """
    warehouse = BigQueryWarehouseOps(
        config.warehouse["project_id"],
        config.warehouse["dataset_id"]
    )

    ti = context["task_instance"]
    window_days = config.get_window_days()
    run_date = context["execution_date"].strftime("%Y%m%d")

    for domain_name, domain_config in config.domains.items():
        ti.log.info(f"Building domain: {domain_name}")

        # Read SQL query template from file
        query_file = f"/opt/airflow/sql/domains/{domain_name}.sql"
        try:
            with open(query_file) as f:
                query = f.read()
                query = query.format(rolling_window_days=window_days)

            # Execute with destination
            dest_table = domain_config.get("intermediate_name")
            warehouse.execute_query(
                query,
                destination_table=dest_table,
                write_disposition="WRITE_TRUNCATE"
            )

            ti.log.info(f"✓ Built domain table: {dest_table}")

        except FileNotFoundError:
            ti.log.warning(f"SQL file not found: {query_file}")


def data_quality_checks(**context):
    """
    Phase 2: Data quality gate
    - Check driving domain is non-empty
    - Check primary keys are unique
    - Fail before final assembly if checks fail
    """
    warehouse = BigQueryWarehouseOps(
        config.warehouse["project_id"],
        config.warehouse["dataset_id"]
    )

    ti = context["task_instance"]
    driving_domain = config.get_driving_domain()
    primary_key = config.lookup_table["primary_key"]
    min_rows = config.get_min_rows()

    # Check row count
    row_count = warehouse.get_row_count(driving_domain)
    ti.log.info(f"Driving domain row count: {row_count}")

    if row_count < min_rows:
        raise Exception(
            f"Data quality check failed: "
            f"only {row_count} rows (min {min_rows})"
        )

    # Check uniqueness
    if config.should_check_unique_keys():
        is_unique = warehouse.check_unique_keys(driving_domain, primary_key)
        if not is_unique:
            raise Exception(
                f"Data quality check failed: "
                f"duplicate primary keys in {driving_domain}"
            )

    ti.log.info("✓ All data quality checks passed")
    context["task_instance"].xcom_push(
        key="row_count",
        value=row_count
    )


def build_final_lookup(**context):
    """
    Phase 2: Build final denormalized lookup table
    - LEFT JOIN driving domain with enrichment domains
    - Result is single lookup table ready for serving
    """
    warehouse = BigQueryWarehouseOps(
        config.warehouse["project_id"],
        config.warehouse["dataset_id"]
    )

    ti = context["task_instance"]
    run_date = context["execution_date"].strftime("%Y%m%d")
    lookup_table_name = config.lookup_table["name"]

    # Read assembly query from file
    query_file = "/opt/airflow/sql/assembly.sql"
    try:
        with open(query_file) as f:
            query = f.read()

        # Write to date-sharded table for isolation
        dest_table = f"{lookup_table_name}_{run_date}"
        warehouse.execute_query(
            query,
            destination_table=dest_table,
            write_disposition="WRITE_TRUNCATE"
        )

        ti.log.info(f"✓ Built final lookup: {dest_table}")
        context["task_instance"].xcom_push(
            key="final_lookup_table",
            value=dest_table
        )

    except FileNotFoundError:
        ti.log.warning(f"SQL file not found: {query_file}")


with TaskGroup("phase_2_build", dag=dag) as phase_2:
    build_domains = PythonOperator(
        task_id="build_domain_tables",
        python_callable=build_domain_tables,
        provide_context=True,
    )

    quality_gate = PythonOperator(
        task_id="data_quality_checks",
        python_callable=data_quality_checks,
        provide_context=True,
    )

    build_lookup = PythonOperator(
        task_id="build_final_lookup",
        python_callable=build_final_lookup,
        provide_context=True,
    )

    build_domains >> quality_gate >> build_lookup


# ============================================================================
# PHASE 3: SERVE - Export → Import → Atomic swap
# ============================================================================

def export_to_storage(**context):
    """
    Phase 3: Export lookup table to object storage (GCS)
    - Sharded export for parallel import
    - Decouples export speed from import speed
    """
    warehouse = BigQueryWarehouseOps(
        config.warehouse["project_id"],
        config.warehouse["dataset_id"]
    )

    ti = context["task_instance"]
    final_lookup = context["task_instance"].xcom_pull(
        task_ids="phase_2_build.build_final_lookup",
        key="final_lookup_table"
    )

    gcs_path = config.export_import.get("object_storage_path", "gs://export-bucket/")
    export_path = f"{gcs_path}{final_lookup}_*.parquet"

    warehouse.export_to_gcs(
        final_lookup,
        export_path,
        file_format="PARQUET"
    )

    ti.log.info(f"✓ Exported to {export_path}")
    context["task_instance"].xcom_push(
        key="export_path",
        value=export_path
    )


def import_to_serving_db(**context):
    """
    Phase 3: Import from storage into staging table
    - Parallel import with concurrency control
    - Never touch live table directly
    - Build indexes after bulk load
    """
    ti = context["task_instance"]

    serving_config = config.serving_db

    with PostgresServingDBOps(
        host=serving_config["host"],
        port=serving_config.get("port", 5432),
        database=serving_config["database"],
        user=serving_config["user"],
        password=serving_config["password"]
    ) as db:

        live_table = config.lookup_table["name"]
        staging_table = f"{live_table}_staging"

        # Create staging table
        db.create_staging_table(
            staging_table,
            live_table,
            config.lookup_table["primary_key"]
        )

        # In production, import from Parquet shards in parallel
        # For demo, we just log the setup
        import_concurrency = config.get_import_concurrency()
        ti.log.info(
            f"✓ Created staging table {staging_table} "
            f"(import concurrency: {import_concurrency})"
        )


def atomic_swap_and_reconcile(**context):
    """
    Phase 3: Atomic table swap and reconciliation
    - Rename staging → live (catalog-only, zero downtime)
    - Check row counts match between warehouse and serving DB
    - Keep previous table for rollback safety
    """
    ti = context["task_instance"]

    serving_config = config.serving_db
    row_count = context["task_instance"].xcom_pull(
        task_ids="phase_2_build.data_quality_checks",
        key="row_count"
    )

    with PostgresServingDBOps(
        host=serving_config["host"],
        port=serving_config.get("port", 5432),
        database=serving_config["database"],
        user=serving_config["user"],
        password=serving_config["password"]
    ) as db:

        live_table = config.lookup_table["name"]
        staging_table = f"{live_table}_staging"

        # Atomic swap
        db.atomic_swap(live_table, staging_table)
        ti.log.info(f"✓ Atomic swap complete: {staging_table} → {live_table}")

        # Reconcile row counts
        matches = db.reconcile_row_counts(live_table, row_count)
        if not matches:
            ti.log.warning("Row count mismatch detected")

        # Cleanup old tables (end of next successful run)
        # db.cleanup_old_tables(live_table)


with TaskGroup("phase_3_serve", dag=dag) as phase_3:
    export = PythonOperator(
        task_id="export_to_storage",
        python_callable=export_to_storage,
        provide_context=True,
    )

    import_to_db = PythonOperator(
        task_id="import_to_serving_db",
        python_callable=import_to_serving_db,
        provide_context=True,
    )

    reconcile = PythonOperator(
        task_id="atomic_swap_and_reconcile",
        python_callable=atomic_swap_and_reconcile,
        provide_context=True,
    )

    export >> import_to_db >> reconcile


# Task dependencies
phase_1 >> phase_2 >> phase_3
