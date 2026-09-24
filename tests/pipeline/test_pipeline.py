"""
Real end-to-end tests for the pipeline package, run against temporary
DuckDB + SQLite files (never the real warehouse.duckdb / serving.db).
"""

import copy

import pytest

from pipeline.build import DataQualityError, run_build, run_data_quality_gate
from pipeline.config import load_config
from pipeline.load_raw_data import load_raw_data
from pipeline.preflight import PreflightError, run_preflight
from pipeline.serve import run_serve
from pipeline.serving_db import ServingDB
from pipeline.warehouse import Warehouse

DIVISION = "ecommerce_orders"


@pytest.fixture
def dag_config():
    config, _ = load_config()
    return copy.deepcopy(config)


@pytest.fixture
def infra_config(tmp_path):
    _, config = load_config()
    config = copy.deepcopy(config)
    # Redirect every path at tmp_path so tests never touch the real
    # warehouse.duckdb / serving.db / tmp/exports at the repo root.
    config["warehouse"]["duckdb_path"] = str(tmp_path / "warehouse.duckdb")
    config["serving_db"]["sqlite_path"] = str(tmp_path / "serving.db")
    config["object_storage"]["export_dir"] = str(tmp_path / "exports")
    return config


@pytest.fixture
def seeded_warehouse(infra_config):
    with Warehouse(infra_config["warehouse"]["duckdb_path"]) as warehouse:
        load_raw_data(warehouse)  # loads the static files under data/raw/
        yield warehouse


def test_preflight_passes_on_seeded_data(seeded_warehouse, dag_config):
    # Should not raise.
    run_preflight(seeded_warehouse, dag_config, DIVISION)


def test_preflight_catches_schema_drift(seeded_warehouse, dag_config, tmp_path, monkeypatch):
    """If a domain query stops producing a column its DDL declares, preflight
    must fail before anything is built -- this is the exact failure mode
    the coverage/drift check exists for."""
    from pipeline import preflight as preflight_module

    broken_sql = "SELECT order_id, customer_id FROM raw_orders"  # missing columns
    broken_path = tmp_path / "broken_orders.sql"
    broken_path.write_text(broken_sql)

    division_sql_dir = preflight_module.SQL_DIR / DIVISION
    real_read_sql = seeded_warehouse.read_sql

    def fake_read_sql(path, **kwargs):
        if path == division_sql_dir / "warehouse/domain/orders.sql":
            return broken_sql
        return real_read_sql(path, **kwargs)

    monkeypatch.setattr(seeded_warehouse, "read_sql", fake_read_sql)

    with pytest.raises(PreflightError, match="Schema drift"):
        run_preflight(seeded_warehouse, dag_config, DIVISION)


def test_build_dedups_and_aggregates_correctly(seeded_warehouse, dag_config):
    run_preflight(seeded_warehouse, dag_config, DIVISION)
    row_count = run_build(seeded_warehouse, dag_config, DIVISION)

    # No duplicate order_id in the driving domain.
    total = seeded_warehouse.row_count("sample_orders")
    distinct = seeded_warehouse.distinct_count("sample_orders", "order_id")
    assert total == distinct == row_count

    # order_items collapsed from many-rows-per-order-item to one row per
    # order_id, via aggregation (not a tie-break pick): the domain table
    # has at most one row per distinct order_id in raw_order_items, never
    # one row per line item.
    item_rows = seeded_warehouse.row_count("sample_order_items")
    item_distinct_orders = seeded_warehouse.distinct_count("sample_order_items", "order_id")
    assert item_rows == item_distinct_orders

    # The lookup table exists and has exactly one row per order (LEFT JOIN
    # must not have fanned out or dropped rows).
    lookup_rows = seeded_warehouse.row_count("sample_lookup_current")
    assert lookup_rows == row_count


def test_build_blocks_on_empty_driving_domain(seeded_warehouse, dag_config):
    """A failed DQ gate must stop before the lookup table is touched.

    Calls run_data_quality_gate directly (not run_build): run_build always
    rebuilds every domain fresh from the raw_* tables first, which would
    undo a corrupted sample_orders before the gate ever saw it. The real
    way this table ends up empty is the raw source being empty/filtered
    out -- which is exactly what CREATE OR REPLACE TABLE ... WHERE 1=0
    simulates -- and the gate is what's under test here.
    """
    run_preflight(seeded_warehouse, dag_config, DIVISION)
    run_build(seeded_warehouse, dag_config, DIVISION)  # good run, establishes a baseline

    good_lookup_row_count = seeded_warehouse.row_count("sample_lookup_current")
    assert good_lookup_row_count > 0

    # Simulate the driving domain's source producing zero in-scope rows.
    seeded_warehouse.execute("CREATE OR REPLACE TABLE sample_orders AS SELECT * FROM raw_orders WHERE 1=0")

    with pytest.raises(DataQualityError):
        run_data_quality_gate(seeded_warehouse, dag_config, DIVISION)

    # The lookup table must be untouched -- still has yesterday's good rows,
    # since build_lookup() is only ever called after the gate passes.
    assert seeded_warehouse.row_count("sample_lookup_current") == good_lookup_row_count


def test_build_blocks_on_duplicate_primary_key(seeded_warehouse, dag_config):
    run_preflight(seeded_warehouse, dag_config, DIVISION)

    # Force a duplicate order_id into the driving domain, simulating a
    # source-data bug that slips past dedup. Parens scope the LIMIT to the
    # second arm only -- `UNION ALL SELECT ... LIMIT 5` without them limits
    # the whole unioned result instead of appending 5 duplicate rows.
    seeded_warehouse.execute(
        "CREATE OR REPLACE TABLE sample_orders AS "
        "SELECT * FROM raw_orders "
        "UNION ALL "
        "(SELECT * FROM raw_orders LIMIT 5)"
    )

    with pytest.raises(DataQualityError, match="duplicate"):
        run_data_quality_gate(seeded_warehouse, dag_config, DIVISION)


def test_full_pipeline_end_to_end(seeded_warehouse, dag_config, infra_config):
    run_preflight(seeded_warehouse, dag_config, DIVISION)
    row_count = run_build(seeded_warehouse, dag_config, DIVISION)
    run_serve(seeded_warehouse, infra_config, DIVISION, row_count)

    with ServingDB(infra_config["serving_db"]["sqlite_path"]) as serving_db:
        assert serving_db.table_exists("live_order_lookup")
        assert serving_db.row_count("live_order_lookup") == row_count

        # Spot-check that a LEFT JOIN miss produced NULLs, not a dropped row.
        cursor = serving_db.execute(
            "SELECT COUNT(*) FROM live_order_lookup WHERE carrier IS NULL"
        )
        orders_without_shipment = cursor.fetchone()[0]
        assert orders_without_shipment > 0  # seed data guarantees some exist


def test_second_run_rotates_backup_tables(seeded_warehouse, dag_config, infra_config):
    """Two runs should leave exactly live + _old + _old_old, never more."""
    for _ in range(3):
        row_count = run_build(seeded_warehouse, dag_config, DIVISION)
        run_serve(seeded_warehouse, infra_config, DIVISION, row_count)

    with ServingDB(infra_config["serving_db"]["sqlite_path"]) as serving_db:
        tables = {
            row[0]
            for row in serving_db.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        assert tables == {
            "live_order_lookup",
            "live_order_lookup_old",
            "live_order_lookup_old_old",
        }
