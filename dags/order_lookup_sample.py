"""
Airflow DAG: Ecommerce Order Lookup Pipeline

This is a thin wrapper around the real pipeline/ package -- every task
below calls the exact same functions `python -m pipeline.run` calls
(pipeline.preflight.run_preflight, pipeline.build.run_build,
pipeline.serve.run_serve). No logic is duplicated or re-implemented in
Airflow-specific form, so this DAG can't drift out of sync with the
standalone runner.

Requires apache-airflow (see requirements-airflow.txt). This is NOT
required to run the pipeline itself -- Airflow here is the production
orchestration reference; see pipeline/run.py for the local runner that
requirements.txt alone supports. See docs/implementation.md for why
DAG files live in dags/ and nothing else does, and docs/design-pattern.md
for the pattern this implements in production terms (targeting
BigQuery/Snowflake + Postgres rather than DuckDB/SQLite).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from pipeline.build import run_build
from pipeline.config import load_config, resolve_path
from pipeline.preflight import run_preflight
from pipeline.serve import run_serve
from pipeline.warehouse import Warehouse

dag_config, infra_config = load_config()


def _open_warehouse() -> Warehouse:
    return Warehouse(resolve_path(infra_config["warehouse"]["duckdb_path"]))


def _task_preflight(division_name: str, **context) -> None:
    with _open_warehouse() as warehouse:
        run_preflight(warehouse, dag_config, division_name)


def _task_build(division_name: str, **context) -> None:
    with _open_warehouse() as warehouse:
        row_count = run_build(warehouse, dag_config, division_name)
    context["task_instance"].xcom_push(key="row_count", value=row_count)


def _task_serve(division_name: str, **context) -> None:
    row_count = context["task_instance"].xcom_pull(
        task_ids=f"division_{division_name}.build", key="row_count"
    )
    with _open_warehouse() as warehouse:
        run_serve(warehouse, infra_config, division_name, row_count)


default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}

dag = DAG(
    "order_lookup_sample",
    default_args=default_args,
    description="Ecommerce order lookup pipeline (real implementation, orchestrated by Airflow)",
    schedule_interval=dag_config.get("schedule_interval", "0 2 * * *"),
    catchup=False,
    tags=["sample", "reference"],
)

# Build phases from config — this is what makes the pipeline config-driven:
# adding a division means adding an entry under `division:` in dag_config.yaml,
# not touching this file.
with dag:
    for division_name in dag_config["division"]:
        with TaskGroup(f"division_{division_name}"):
            preflight = PythonOperator(
                task_id="preflight",
                python_callable=_task_preflight,
                op_kwargs={"division_name": division_name},
            )
            build = PythonOperator(
                task_id="build",
                python_callable=_task_build,
                op_kwargs={"division_name": division_name},
            )
            serve = PythonOperator(
                task_id="serve",
                python_callable=_task_serve,
                op_kwargs={"division_name": division_name},
            )
            preflight >> build >> serve
