"""Serving database operations for PostgreSQL."""

from typing import Dict, Any, List
import psycopg2
from psycopg2 import sql
from airflow.utils.log.logging_mixin import LoggingMixin


class PostgresServingDBOps(LoggingMixin):
    """Operations for PostgreSQL serving database."""

    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str
    ):
        """
        Initialize PostgreSQL connection.

        Args:
            host: Database host
            port: Database port
            database: Database name
            user: Username
            password: Password
        """
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.conn = None

    def _get_connection(self):
        """Get or create database connection."""
        if self.conn is None or self.conn.closed:
            self.conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
        return self.conn

    def execute(self, query: str, params=None) -> List[tuple]:
        """
        Execute query and return results.

        Args:
            query: SQL query
            params: Query parameters

        Returns:
            List of result rows
        """
        conn = self._get_connection()
        with conn.cursor() as cur:
            cur.execute(query, params)
            conn.commit()
            if cur.description:
                return cur.fetchall()
            return []

    def execute_batch(self, queries: List[str]) -> None:
        """
        Execute multiple queries in sequence.

        Args:
            queries: List of SQL queries
        """
        conn = self._get_connection()
        with conn.cursor() as cur:
            for query in queries:
                cur.execute(query)
            conn.commit()

    def check_duplicate_keys(
        self,
        table_name: str,
        primary_key: str
    ) -> bool:
        """
        Check for duplicate primary keys (before import).

        Args:
            table_name: Table to check
            primary_key: Primary key column

        Returns:
            True if no duplicates
        """
        query = f"""
            SELECT COUNT(*) as total, COUNT(DISTINCT {primary_key}) as unique_keys
            FROM {table_name}
            WHERE {primary_key} IS NOT NULL
        """

        result = self.execute(query)
        if result:
            total, unique_keys = result[0]
            no_duplicates = total == unique_keys
            self.log.info(
                f"Duplicate check: {unique_keys} unique keys of {total} total"
            )
            return no_duplicates
        return True

    def create_staging_table(
        self,
        staging_table: str,
        live_table: str,
        primary_key: str
    ) -> None:
        """
        Create fresh staging table (drop if exists).

        Args:
            staging_table: Name of staging table to create
            live_table: Live table to mirror schema from
            primary_key: Primary key column
        """
        query = f"""
            DROP TABLE IF EXISTS {staging_table} CASCADE;
            CREATE TABLE {staging_table} AS
            SELECT * FROM {live_table} WHERE 1=0;

            ALTER TABLE {staging_table}
            ADD CONSTRAINT {staging_table}_pk PRIMARY KEY ({primary_key});
        """

        self.execute_batch(query.split(";"))
        self.log.info(f"Created staging table {staging_table}")

    def build_indexes(
        self,
        table_name: str,
        indexes: List[Dict[str, Any]]
    ) -> None:
        """
        Build indexes on table (after bulk load).

        Args:
            table_name: Table to index
            indexes: List of index definitions
                     Each: {"name": "idx_name", "columns": ["col1", "col2"]}
        """
        queries = []
        for idx in indexes:
            col_list = ", ".join(idx["columns"])
            query = f"CREATE INDEX {idx['name']} ON {table_name}({col_list});"
            queries.append(query)

        if queries:
            self.execute_batch(queries)
            self.log.info(f"Built {len(queries)} indexes on {table_name}")

    def atomic_swap(
        self,
        live_table: str,
        staging_table: str,
        old_table_suffix: str = "_old"
    ) -> None:
        """
        Perform atomic table swap (rename staging to live).

        Args:
            live_table: Name of live table
            staging_table: Name of staging table
            old_table_suffix: Suffix for backup of previous live table
        """
        old_table = f"{live_table}{old_table_suffix}"

        query = f"""
            BEGIN;
            ALTER TABLE {live_table} RENAME TO {old_table};
            ALTER TABLE {staging_table} RENAME TO {live_table};
            COMMIT;
        """

        # Execute as single transaction
        conn = self._get_connection()
        with conn.cursor() as cur:
            cur.execute(query)
            conn.commit()

        self.log.info(f"Atomic swap: {staging_table} -> {live_table}")

    def reconcile_row_counts(
        self,
        live_table: str,
        warehouse_count: int,
        tolerance: float = 0.01
    ) -> bool:
        """
        Check row count matches between warehouse and serving DB.

        Args:
            live_table: Table in serving DB
            warehouse_count: Row count from warehouse
            tolerance: Acceptable difference (0.01 = 1%)

        Returns:
            True if counts match within tolerance
        """
        result = self.execute(f"SELECT COUNT(*) FROM {live_table}")
        serving_count = result[0][0] if result else 0

        if warehouse_count == 0:
            self.log.warning("Warehouse count is 0")
            return serving_count == 0

        diff = abs(serving_count - warehouse_count) / warehouse_count
        matches = diff <= tolerance

        self.log.info(
            f"Row count reconciliation: "
            f"warehouse={warehouse_count}, serving={serving_count}, "
            f"diff={diff:.1%}"
        )
        return matches

    def cleanup_old_tables(
        self,
        live_table: str,
        old_table_suffix: str = "_old",
        old_old_table_suffix: str = "_old_old"
    ) -> None:
        """
        Drop the oldest backup table (keep previous cycle only).

        Args:
            live_table: Live table name
            old_table_suffix: Suffix for one-cycle-old table
            old_old_table_suffix: Suffix for two-cycle-old table
        """
        old_old_table = f"{live_table}{old_old_table_suffix}"

        query = f"DROP TABLE IF EXISTS {old_old_table} CASCADE;"
        self.execute(query)
        self.log.info(f"Dropped old backup table {old_old_table}")

    def close(self) -> None:
        """Close database connection."""
        if self.conn and not self.conn.closed:
            self.conn.close()
            self.log.info("Closed database connection")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
