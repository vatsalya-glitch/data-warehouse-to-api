"""
Phase 2 — Build. Runs only after preflight passes.

For the division's domain queries: build each domain table with a full
refresh (CREATE OR REPLACE TABLE AS SELECT). Then gate the final assembly
behind a data-quality check on the driving domain — if it fails, the
lookup table is never touched, so a bad run leaves yesterday's good lookup
table in place. Only if the gate passes do we rebuild the lookup table.
"""

from pathlib import Path

from pipeline.config import SQL_DIR
from pipeline.warehouse import Warehouse


class DataQualityError(Exception):
    """Raised when the driving domain fails its non-empty/unique-key check.
    The caller should stop here — the lookup table is untouched."""


def build_domain_tables(warehouse: Warehouse, dag_config: dict, division_name: str) -> None:
    division_config = dag_config["division"][division_name]
    division_sql_dir = SQL_DIR / division_name
    window_days = dag_config["rolling_window_days"]

    for domain_query in division_config["domain_queries"]:
        sql = warehouse.read_sql(
            division_sql_dir / domain_query, rolling_window_days=window_days
        )
        warehouse.execute(sql)


def run_data_quality_gate(warehouse: Warehouse, dag_config: dict, division_name: str) -> int:
    """Check the driving domain is non-empty and has unique primary keys.
    Returns the row count (used later for reconciliation). Raises
    DataQualityError if either check fails."""
    division_config = dag_config["division"][division_name]
    driving_table = f"sample_{division_config['driving_domain']}"
    primary_key = division_config["primary_key"]
    min_rows = division_config.get("min_rows", 0)

    row_count = warehouse.row_count(driving_table)
    if row_count < min_rows:
        raise DataQualityError(
            f"Driving domain '{driving_table}' has {row_count} rows, "
            f"below the configured minimum of {min_rows}."
        )

    if not division_config.get("allow_duplicate_keys", False):
        distinct_count = warehouse.distinct_count(driving_table, primary_key)
        if distinct_count != row_count:
            raise DataQualityError(
                f"Driving domain '{driving_table}' has {row_count} rows but "
                f"only {distinct_count} distinct '{primary_key}' values — "
                f"duplicate primary keys detected."
            )

    return row_count


def build_lookup(warehouse: Warehouse, dag_config: dict, division_name: str) -> None:
    """Assemble the final denormalized lookup. Only called after the DQ
    gate passes."""
    division_config = dag_config["division"][division_name]
    division_sql_dir = SQL_DIR / division_name

    lookup_select = warehouse.read_sql(division_sql_dir / division_config["lookup_query"])
    warehouse.execute(f"CREATE OR REPLACE TABLE sample_lookup_current AS {lookup_select}")


def run_build(warehouse: Warehouse, dag_config: dict, division_name: str) -> int:
    """Run the full Phase 2 sequence. Returns the driving-domain row count."""
    build_domain_tables(warehouse, dag_config, division_name)
    row_count = run_data_quality_gate(warehouse, dag_config, division_name)
    build_lookup(warehouse, dag_config, division_name)
    return row_count
