"""
Placeholder test file for the order_lookup_sample DAG.

This is a reference structure only. Replace with real tests.
Layout mirrors the Astronomer/Airflow convention: tests/dags/ tests the
files in dags/, against config/ and sql/ at the repo root.
"""

import pytest
import yaml

from pipeline.config import CONFIG_DIR, PROJECT_ROOT, SQL_DIR

DAGS_DIR = PROJECT_ROOT / "dags"


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
    Verify required config files exist under config/.

    TODO: replace with config validation
    """
    for filename in ["dag_config.yaml", "infra_config.yaml"]:
        filepath = CONFIG_DIR / filename
        assert filepath.exists(), f"Missing required file: {filename}"


def test_config_is_valid_yaml():
    """
    Verify config files are valid YAML.

    TODO: replace with schema validation
    """
    for config_file in ["dag_config.yaml", "infra_config.yaml"]:
        with open(CONFIG_DIR / config_file) as f:
            try:
                yaml.safe_load(f)
            except yaml.YAMLError as e:
                pytest.fail(f"Invalid YAML in {config_file}: {e}")


def test_domain_sql_files_exist():
    """
    Verify every domain_queries and lookup_query path in dag_config.yaml
    resolves to a real file under sql/.

    TODO: extend to cover ddl/ and dq/ paths too
    """
    with open(CONFIG_DIR / "dag_config.yaml") as f:
        dag_config = yaml.safe_load(f)

    for division_name, division_config in dag_config["division"].items():
        division_sql_dir = SQL_DIR / division_name

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
