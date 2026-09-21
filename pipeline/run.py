"""
CLI entrypoint: python -m pipeline.run

Runs the full three-phase pipeline for every division in dag_config.yaml.
Seeds the warehouse with synthetic data on first run (if warehouse.duckdb
doesn't exist yet).
"""

import sys

from pipeline.build import DataQualityError, run_build
from pipeline.config import load_config, resolve_path
from pipeline.preflight import PreflightError, run_preflight
from pipeline.seed import seed_warehouse
from pipeline.serve import ReconciliationWarning, run_serve
from pipeline.warehouse import Warehouse


def main() -> None:
    dag_config, infra_config = load_config()
    duckdb_path = resolve_path(infra_config["warehouse"]["duckdb_path"])

    if not duckdb_path.exists():
        print(f"No warehouse found at {duckdb_path} — seeding sample data...")
        seed_warehouse(str(duckdb_path))
        print()

    for division_name in dag_config["division"]:
        print(f"=== Division: {division_name} ===")

        with Warehouse(duckdb_path) as warehouse:
            print("Phase 1: Preflight...")
            try:
                run_preflight(warehouse, dag_config, division_name)
            except PreflightError as exc:
                print(f"  PREFLIGHT FAILED: {exc}")
                sys.exit(1)
            print("  preflight passed")

            print("Phase 2: Build...")
            try:
                row_count = run_build(warehouse, dag_config, division_name)
            except DataQualityError as exc:
                print(f"  BUILD FAILED (data quality gate): {exc}")
                print("  Yesterday's lookup table, if any, is untouched.")
                sys.exit(1)
            print(f"  built lookup with {row_count} rows")

            print("Phase 3: Serve...")
            try:
                run_serve(warehouse, infra_config, division_name, row_count)
            except ReconciliationWarning as exc:
                print(f"  WARNING: {exc}")
            print("  atomic swap complete, serving DB updated")

        print()

    print("Pipeline run complete. Try: python -m pipeline.lookup ORDER-00001")


if __name__ == "__main__":
    main()
