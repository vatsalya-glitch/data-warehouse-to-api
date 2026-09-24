"""Configuration loader shared by the standalone runner and the Airflow DAG."""

from pathlib import Path
import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent
INCLUDE_DIR = PROJECT_ROOT / "include"
SQL_DIR = INCLUDE_DIR / "sql"
CONFIG_DIR = INCLUDE_DIR / "config"
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"


def load_config() -> tuple[dict, dict]:
    """Load dag_config.yaml (what to build) and infra_config.yaml (how to connect)."""
    with open(CONFIG_DIR / "dag_config.yaml") as f:
        dag_config = yaml.safe_load(f)
    with open(CONFIG_DIR / "infra_config.yaml") as f:
        infra_config = yaml.safe_load(f)
    return dag_config, infra_config


def resolve_path(relative_path: str) -> Path:
    """Resolve a path from infra_config.yaml relative to the project root,
    so paths in config work the same whether you run from the repo root or
    elsewhere."""
    path = Path(relative_path)
    return path if path.is_absolute() else PROJECT_ROOT / path
