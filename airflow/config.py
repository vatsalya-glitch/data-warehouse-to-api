"""Configuration loader for data-warehouse-to-api pipeline."""

import yaml
from pathlib import Path
from typing import Any, Dict


class PipelineConfig:
    """Load and validate pipeline configuration from YAML."""

    def __init__(self, config_path: str):
        """
        Initialize configuration from YAML file.

        Args:
            config_path: Path to config.yaml file
        """
        self.config_path = Path(config_path)
        self.config = self._load_config()
        self._validate_config()

    def _load_config(self) -> Dict[str, Any]:
        """Load YAML configuration file."""
        with open(self.config_path) as f:
            return yaml.safe_load(f)

    def _validate_config(self) -> None:
        """Validate required configuration sections exist."""
        required_sections = ["warehouse", "serving_db", "lookup_table", "domains"]
        for section in required_sections:
            if section not in self.config:
                raise ValueError(f"Missing required config section: {section}")

    @property
    def warehouse(self) -> Dict[str, Any]:
        return self.config["warehouse"]

    @property
    def serving_db(self) -> Dict[str, Any]:
        return self.config["serving_db"]

    @property
    def lookup_table(self) -> Dict[str, Any]:
        return self.config["lookup_table"]

    @property
    def domains(self) -> Dict[str, Dict[str, Any]]:
        return self.config["domains"]

    @property
    def join_config(self) -> Dict[str, Any]:
        return self.config.get("join", {})

    @property
    def data_quality(self) -> Dict[str, Any]:
        return self.config.get("data_quality", {})

    @property
    def lookback(self) -> Dict[str, Any]:
        return self.config.get("lookback", {})

    @property
    def export_import(self) -> Dict[str, Any]:
        return self.config.get("export_import", {})

    @property
    def monitoring(self) -> Dict[str, Any]:
        return self.config.get("monitoring", {})

    def get_window_days(self) -> int:
        """Get rolling window in days for data processing."""
        return self.lookback.get("window_days", 90)

    def get_driving_domain(self) -> str:
        """Get name of driving domain for joins."""
        return self.join_config.get("driving_domain")

    def get_enrichment_domains(self) -> list:
        """Get list of enrichment domains to LEFT JOIN."""
        return self.join_config.get("enrichment_domains", [])

    def get_import_concurrency(self) -> int:
        """Get max concurrent imports to serving database."""
        return self.export_import.get("import_max_concurrency", 4)

    def get_min_rows(self) -> int:
        """Get minimum expected row count for data quality check."""
        return self.data_quality.get("min_rows", 0)

    def should_check_unique_keys(self) -> bool:
        """Check if primary key uniqueness should be validated."""
        return self.data_quality.get("unique_primary_key", True)

    def get_row_count_tolerance(self) -> float:
        """Get allowed row count change tolerance (0.15 = ±15%)."""
        return self.data_quality.get("row_count_change_tolerance", 0.15)
