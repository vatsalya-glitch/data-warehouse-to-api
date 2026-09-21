"""
Phase 3 — Serve. Runs only after Phase 2's data-quality gate passes.

Export the finished lookup table to a local "object storage" folder,
import it into a fresh staging table in the serving database (SQLite),
build indexes after the bulk load, then perform an atomic rename-based
swap into the live table — zero downtime, live table never empty or
half-updated. Finishes with row-count reconciliation and two-cycle
rollback-safety cleanup.
"""

from pathlib import Path

from pipeline.config import PROJECT_ROOT, SQL_DIR, resolve_path
from pipeline.serving_db import ServingDB
from pipeline.warehouse import Warehouse

LIVE_TABLE = "live_order_lookup"
STAGING_TABLE = "staging_order_lookup"
WAREHOUSE_LOOKUP_TABLE = "sample_lookup_current"


class ReconciliationWarning(Exception):
    """Raised (not necessarily fatal) when warehouse and serving-DB row
    counts diverge by more than the configured tolerance. The swap has
    already happened by the time this is checked."""


def export_lookup(warehouse: Warehouse, infra_config: dict, division_name: str) -> Path:
    export_dir = resolve_path(infra_config["object_storage"]["export_dir"]) / division_name
    export_dir.mkdir(parents=True, exist_ok=True)
    export_path = export_dir / "shard_0.parquet"

    sql_path = SQL_DIR / division_name / "serving_db" / "sync" / "export_lookup_to_object_storage.sql"
    sql = warehouse.read_sql(sql_path, export_path=str(export_path))
    warehouse.execute(sql)
    return export_path


def ensure_live_table(serving_db: ServingDB, division_name: str) -> None:
    ddl_path = SQL_DIR / division_name / "serving_db" / "ddl" / "serving_ddl.sql"
    serving_db.executescript(serving_db.read_sql(ddl_path))


def import_to_staging(serving_db: ServingDB, division_name: str, shard_paths: list[Path]) -> int:
    create_sql_path = SQL_DIR / division_name / "serving_db" / "sync" / "create_staging_table.sql"
    serving_db.executescript(serving_db.read_sql(create_sql_path))

    total_rows = 0
    for shard_path in shard_paths:
        total_rows += serving_db.import_parquet(shard_path, STAGING_TABLE)
    return total_rows


def build_staging_indexes(serving_db: ServingDB, infra_config: dict) -> None:
    indexes = infra_config["serving_db"].get("staging_indexes", [])
    serving_db.build_indexes(STAGING_TABLE, indexes)


def swap_and_cleanup(serving_db: ServingDB, division_name: str) -> None:
    swap_sql_path = SQL_DIR / division_name / "serving_db" / "sync" / "swap_staging_to_live.sql"
    # rotate_and_swap implements the same sequence documented in
    # swap_staging_to_live.sql + drop_old_table.sql, with the existence
    # checks a static SQL file can't express (ALTER TABLE has no RENAME
    # IF EXISTS) -- see its docstring in pipeline/serving_db.py.
    _ = swap_sql_path  # referenced for documentation; logic lives in rotate_and_swap
    serving_db.rotate_and_swap(LIVE_TABLE, STAGING_TABLE)


def reconcile(
    warehouse: Warehouse,
    serving_db: ServingDB,
    warehouse_row_count: int,
    tolerance: float,
) -> None:
    serving_row_count = serving_db.row_count(LIVE_TABLE)
    if warehouse_row_count == 0:
        return
    diff = abs(serving_row_count - warehouse_row_count) / warehouse_row_count
    if diff > tolerance:
        raise ReconciliationWarning(
            f"Row count mismatch after swap: warehouse={warehouse_row_count}, "
            f"serving={serving_row_count} (diff={diff:.1%}, tolerance={tolerance:.1%})"
        )


def run_serve(
    warehouse: Warehouse,
    infra_config: dict,
    division_name: str,
    warehouse_row_count: int,
) -> None:
    with ServingDB(resolve_path(infra_config["serving_db"]["sqlite_path"])) as serving_db:
        ensure_live_table(serving_db, division_name)

        shard_path = export_lookup(warehouse, infra_config, division_name)
        import_to_staging(serving_db, division_name, [shard_path])
        build_staging_indexes(serving_db, infra_config)
        swap_and_cleanup(serving_db, division_name)

        tolerance = infra_config.get("row_count_reconciliation_tolerance", 0.01)
        reconcile(warehouse, serving_db, warehouse_row_count, tolerance)
