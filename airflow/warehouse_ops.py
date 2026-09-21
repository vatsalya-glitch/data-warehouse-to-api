"""Warehouse operations for BigQuery data processing."""

from datetime import datetime
from typing import List, Dict, Any
from google.cloud import bigquery
from airflow.utils.log.logging_mixin import LoggingMixin


class BigQueryWarehouseOps(LoggingMixin):
    """Operations for BigQuery warehouse processing."""

    def __init__(self, project_id: str, dataset_id: str):
        """
        Initialize BigQuery operations.

        Args:
            project_id: Google Cloud project ID
            dataset_id: BigQuery dataset ID
        """
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.client = bigquery.Client(project=project_id)

    def _get_table_id(self, table_name: str) -> str:
        """Get fully qualified table ID."""
        return f"{self.project_id}.{self.dataset_id}.{table_name}"

    def create_fresh_schema(
        self,
        table_name: str,
        schema: List[bigquery.SchemaField]
    ) -> None:
        """
        Create or replace table with fresh schema (empty).

        Args:
            table_name: Name of table to create
            schema: BigQuery schema definition
        """
        table_id = self._get_table_id(table_name)
        table = bigquery.Table(table_id, schema=schema)
        table = self.client.create_table(table, exists_ok=True)
        self.log.info(f"Created fresh schema for {table_id}")

    def dry_run_query(self, query: str) -> Dict[str, Any]:
        """
        Dry-run a query to validate schema without scanning data.

        Args:
            query: SQL query to validate

        Returns:
            Job statistics including column info
        """
        job_config = bigquery.QueryJobConfig(dry_run=True)
        query_job = self.client.query(query, job_config=job_config)

        self.log.info(f"Dry-run successful. Columns: {query_job.schema}")
        return {
            "column_count": len(query_job.schema),
            "columns": [f.name for f in query_job.schema],
        }

    def execute_query(
        self,
        query: str,
        destination_table: str = None,
        write_disposition: str = "WRITE_TRUNCATE"
    ) -> bigquery.LoadJob:
        """
        Execute a query with optional destination table.

        Args:
            query: SQL query to execute
            destination_table: Optional destination table
            write_disposition: How to handle existing data

        Returns:
            Completed job
        """
        job_config = bigquery.QueryJobConfig()

        if destination_table:
            job_config.destination = self._get_table_id(destination_table)
            job_config.write_disposition = write_disposition

        query_job = self.client.query(query, job_config=job_config)
        query_job.result()  # Wait for completion

        if destination_table:
            self.log.info(f"Loaded {query_job.output_rows} rows to {destination_table}")

        return query_job

    def get_row_count(self, table_name: str) -> int:
        """
        Get row count for a table.

        Args:
            table_name: Name of table

        Returns:
            Row count
        """
        table_id = self._get_table_id(table_name)
        table = self.client.get_table(table_id)
        return table.num_rows

    def check_unique_keys(
        self,
        table_name: str,
        primary_key: str
    ) -> bool:
        """
        Check if primary key is unique in table.

        Args:
            table_name: Name of table
            primary_key: Primary key column name

        Returns:
            True if all keys are unique
        """
        table_id = self._get_table_id(table_name)
        query = f"""
            SELECT
                COUNT(*) as total_rows,
                COUNT(DISTINCT {primary_key}) as unique_keys
            FROM `{table_id}`
            WHERE {primary_key} IS NOT NULL
        """

        result = self.client.query(query).result()
        row = list(result)[0]

        is_unique = row.total_rows == row.unique_keys
        self.log.info(
            f"Uniqueness check for {table_name}: "
            f"{row.unique_keys} unique keys of {row.total_rows} total rows"
        )
        return is_unique

    def check_row_count_change(
        self,
        current_count: int,
        previous_count: int,
        tolerance: float = 0.15
    ) -> bool:
        """
        Check if row count change is within acceptable tolerance.

        Args:
            current_count: Current row count
            previous_count: Previous day's row count
            tolerance: Acceptable change (0.15 = ±15%)

        Returns:
            True if change is within tolerance
        """
        if previous_count == 0:
            self.log.warning("No previous count for comparison")
            return True

        change = abs(current_count - previous_count) / previous_count
        within_tolerance = change <= tolerance

        self.log.info(
            f"Row count change: {change:.1%} "
            f"(tolerance: {tolerance:.1%})"
        )
        return within_tolerance

    def export_to_gcs(
        self,
        table_name: str,
        gcs_path: str,
        file_format: str = "PARQUET"
    ) -> None:
        """
        Export table to Google Cloud Storage.

        Args:
            table_name: Table to export
            gcs_path: GCS destination path (gs://bucket/path/*)
            file_format: Format (PARQUET or CSV)
        """
        table_id = self._get_table_id(table_name)
        extract_job = self.client.extract_table(
            table_id,
            gcs_path,
            job_config=bigquery.ExtractJobConfig(
                destination_format=(
                    bigquery.DestinationFormat.PARQUET
                    if file_format == "PARQUET"
                    else bigquery.DestinationFormat.CSV
                )
            ),
        )
        extract_job.result()
        self.log.info(f"Exported {table_name} to {gcs_path}")

    def get_table_schema(self, table_name: str) -> List[bigquery.SchemaField]:
        """
        Get schema for a table.

        Args:
            table_name: Table name

        Returns:
            List of SchemaField objects
        """
        table_id = self._get_table_id(table_name)
        table = self.client.get_table(table_id)
        return table.schema
