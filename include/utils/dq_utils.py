"""Data quality utilities for order_lookup_sample pipeline.

This module contains stub interfaces for data quality checks.
Replace all TODO sections with real warehouse queries.
"""


def validate_insert_schema(warehouse_conn, insert_sql: str) -> None:
    """
    Dry-run `insert_sql`; raise if it doesn't match the target table's schema.

    This runs the INSERT query in dry-run mode (no data written) to catch
    type/column mismatches immediately, not after a real run.

    Args:
        warehouse_conn: Connection to warehouse (e.g., BigQuery client)
        insert_sql: The INSERT query to validate

    Raises:
        SchemaValidationError: if column types don't match
    """
    # TODO: replace with real warehouse dry-run call
    # Example for BigQuery:
    #   job_config = bigquery.QueryJobConfig(dry_run=True)
    #   query_job = warehouse_conn.query(insert_sql, job_config=job_config)
    #   # If query_job raises, schema mismatch caught here
    pass


def run_domain_dq_checks(
    warehouse_conn,
    domain_table: str,
    min_rows: int = 0,
    check_unique_keys: bool = True,
    primary_key: str = None
) -> None:
    """
    Assert the driving domain table is non-empty and has no duplicate keys.

    Called after all domain tables are built, before final lookup assembly.
    If this check fails, the whole run stops — the lookup table is never
    overwritten by bad data.

    Args:
        warehouse_conn: Connection to warehouse
        domain_table: Name of the domain table to check
        min_rows: Fail if row count < this
        check_unique_keys: If True, check for duplicate primary keys
        primary_key: Column name to check for uniqueness

    Raises:
        DataQualityError: if any check fails
    """
    # TODO: query row count
    # Example:
    #   SELECT COUNT(*) FROM domain_table
    # Fail if < min_rows

    # TODO: if check_unique_keys, query for duplicates
    # Example:
    #   SELECT COUNT(*), COUNT(DISTINCT primary_key)
    #   FROM domain_table
    #   WHERE primary_key IS NOT NULL
    # Fail if these don't match
    pass


def run_schema_coverage_checks(
    warehouse_conn,
    expected_columns: list,
    final_lookup_table: str
) -> None:
    """
    Fail if any domain table column isn't represented in the final lookup query.

    This catches the specific failure mode: "someone added a column to a
    domain table and forgot to wire it into the final lookup join."

    Args:
        warehouse_conn: Connection to warehouse
        expected_columns: List of column names that should exist
        final_lookup_table: The final lookup table to check against

    Raises:
        CoverageError: if any expected column is missing from lookup
    """
    # TODO: query INFORMATION_SCHEMA on the warehouse
    # Example:
    #   SELECT column_name FROM information_schema.columns
    #   WHERE table_name = final_lookup_table
    # Check that every expected_column exists in this result set
    pass


def run_reconciliation_checks(
    warehouse_conn,
    serving_db_conn,
    lookup_table: str,
    warehouse_row_count: int,
    tolerance: float = 0.01
) -> None:
    """
    Compare warehouse row count to serving DB row count after the swap.

    If counts differ by more than `tolerance`, this logs a warning but
    doesn't fail (the swap already happened).

    Args:
        warehouse_conn: Connection to warehouse
        serving_db_conn: Connection to serving database
        lookup_table: Name of the lookup table
        warehouse_row_count: Row count from warehouse (source of truth)
        tolerance: Acceptable difference (0.01 = 1%)
    """
    # TODO: query serving DB
    # Example:
    #   SELECT COUNT(*) FROM live_lookup
    # Compare to warehouse_row_count
    # If |serving_count - warehouse_count| / warehouse_count > tolerance:
    #   log warning but don't raise (swap is already done)
    pass
