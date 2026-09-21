"""
Placeholder test file for the order_lookup_sample DAG.

This is a reference structure only. Replace with real tests.
Layout mirrors the Astronomer/Airflow convention: tests/dags/ tests the
files in dags/, against config and SQL that live under include/.
"""

import pytest
from pathlib import Path
import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DAGS_DIR = PROJECT_ROOT / "dags"
INCLUDE_DIR = PROJECT_ROOT / "include"


def test_dag_file_exists():
    """
    Smoke test: DAG file should exist.

    TODO: replace with a real DAG-loads-without-errors test, e.g. using
    Airflow's DagBag: DagBag(dag_folder=str(DAGS_DIR), include_examples=False)
    """
    dag_file = DAGS_DIR / "order_lookup_sample.py"
    assert dag_file.exists(), "DAG file not found"


def test_config_files_exist():
    """
    Verify required config files exist under include/config/.

    TODO: replace with config validation
    """
    config_dir = INCLUDE_DIR / "config"

    for filename in ["dag_config.yaml", "infra_config.yaml"]:
        filepath = config_dir / filename
        assert filepath.exists(), f"Missing required file: {filename}"


def test_config_is_valid_yaml():
    """
    Verify config files are valid YAML.

    TODO: replace with schema validation
    """
    config_dir = INCLUDE_DIR / "config"

    for config_file in ["dag_config.yaml", "infra_config.yaml"]:
        with open(config_dir / config_file) as f:
            try:
                yaml.safe_load(f)
            except yaml.YAMLError as e:
                pytest.fail(f"Invalid YAML in {config_file}: {e}")


def test_domain_sql_files_exist():
    """
    Verify every domain_queries and lookup_query path in dag_config.yaml
    resolves to a real file under include/sql/.

    TODO: extend to cover ddl/ and dq/ paths too
    """
    with open(INCLUDE_DIR / "config" / "dag_config.yaml") as f:
        dag_config = yaml.safe_load(f)

    for division_name, division_config in dag_config["division"].items():
        division_sql_dir = INCLUDE_DIR / "sql" / division_name

        for query_path in division_config.get("domain_queries", []):
            assert (division_sql_dir / query_path).exists(), (
                f"Missing domain query file: {division_name}/{query_path}"
            )

        lookup_query = division_config.get("lookup_query")
        if lookup_query:
            assert (division_sql_dir / lookup_query).exists(), (
                f"Missing lookup query file: {division_name}/{lookup_query}"
            )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
