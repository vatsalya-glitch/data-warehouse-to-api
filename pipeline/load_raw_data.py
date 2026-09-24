"""
Load the static raw data files (data/raw/*.csv) into the warehouse.

These CSVs stand in for tables that would already exist in a real
company's warehouse (raw.ecommerce_orders, raw.ecommerce_customers, etc).
They're checked into the repo as data, not generated at run time — see
data/raw/README.md for what's in them and why.
"""

from pathlib import Path

from pipeline.config import RAW_DATA_DIR
from pipeline.warehouse import Warehouse

# Maps each CSV file to the raw_* table the domain queries read from.
RAW_TABLES = {
    "customers.csv": "raw_customers",
    "orders.csv": "raw_orders",
    "order_items.csv": "raw_order_items",
    "shipment_events.csv": "raw_shipment_events",
    "support_tickets.csv": "raw_support_tickets",
}


def load_raw_data(warehouse: Warehouse, data_dir: Path = RAW_DATA_DIR) -> None:
    """(Re)create each raw_* table from its CSV file. DuckDB's read_csv_auto
    infers types (DATE vs TIMESTAMP vs VARCHAR) directly from the CSV --
    no separate schema declaration needed for these raw tables, since
    they're not the ones preflight validates against a DDL contract
    (that check applies to the domain tables pipeline/build.py builds
    from these, not to the raw sources themselves)."""
    for csv_name, table_name in RAW_TABLES.items():
        csv_path = data_dir / csv_name
        if not csv_path.exists():
            raise FileNotFoundError(f"Missing raw data file: {csv_path}")
        warehouse.execute(
            f"CREATE OR REPLACE TABLE {table_name} AS "
            f"SELECT * FROM read_csv_auto('{csv_path}')"
        )


if __name__ == "__main__":
    from pipeline.config import load_config, resolve_path

    _, infra_config = load_config()
    duckdb_path = resolve_path(infra_config["warehouse"]["duckdb_path"])

    with Warehouse(duckdb_path) as warehouse:
        load_raw_data(warehouse)
        print(f"Loaded raw data into {duckdb_path}:")
        for table_name in RAW_TABLES.values():
            count = warehouse.row_count(table_name)
            print(f"  {table_name}: {count} rows")
