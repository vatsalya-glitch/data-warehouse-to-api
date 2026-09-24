"""Serving database operations (SQLite) — the real implementation behind Phase 3.

SQLite stands in for the production serving database (Postgres/MySQL) so
the pipeline runs locally with no server. See include/README.md. It's
deliberately a different engine from the warehouse (DuckDB) — the whole
point of the pattern is that the warehouse and the serving store are
different tools for different jobs.
"""

import sqlite3
from pathlib import Path

import pandas as pd


def _sqlite_safe(value):
    """Convert a pandas/DuckDB cell value to something sqlite3 can bind.

    DuckDB -> pandas produces pandas.Timestamp for DATE/TIMESTAMP columns,
    and either NaN or pandas.NA for nulls depending on the column's dtype;
    sqlite3 only binds None, int, float, str, and bytes.

    Any integer column with at least one NULL becomes pandas' nullable
    Int32/Int64 dtype, and its non-null values come through as
    numpy.int32/int64 objects, not plain Python int. sqlite3 doesn't
    recognize those -- it silently stores them as raw BLOBs via numpy's
    buffer protocol instead of INTEGER, with no error. Caught this because
    open_ticket_count has real NULLs (most orders have no ticket); a
    column that happens to never be NULL in the sample data (like
    item_count) hid the same latent bug by luck. `.item()` unwraps any
    numpy scalar to its native Python type.
    """
    if value is None:
        return None
    if pd.isna(value):
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat(sep=" ")
    if hasattr(value, "item"):
        return value.item()
    return value


class ServingDB:
    """Thin wrapper around a SQLite connection with the operations Phase 3
    needs: staging table lifecycle, parquet import, atomic swap with
    two-cycle rotation, and reconciliation."""

    def __init__(self, sqlite_path: str | Path):
        # isolation_level=None (autocommit) so the explicit BEGIN/COMMIT in
        # rotate_and_swap controls the transaction, not Python's own
        # implicit one — see include/sql/.../swap_staging_to_live.sql
        self.conn = sqlite3.connect(str(sqlite_path), isolation_level=None)

    def read_sql(self, path: Path, **format_kwargs) -> str:
        text = path.read_text()
        return text.format(**format_kwargs) if format_kwargs else text

    def executescript(self, sql: str) -> None:
        self.conn.executescript(sql)

    def execute(self, sql: str, params: tuple = ()) -> sqlite3.Cursor:
        return self.conn.execute(sql, params)

    def table_exists(self, table: str) -> bool:
        row = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
            (table,),
        ).fetchone()
        return row is not None

    def row_count(self, table: str) -> int:
        return self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

    def import_parquet(self, parquet_path: Path, table: str) -> int:
        """Bulk-load one shard file into `table`. Uses DuckDB in-process to
        read the Parquet file (SQLite has no native Parquet reader) and
        hands rows to SQLite via executemany — this is the local stand-in
        for "COPY shard into staging, up to import_max_concurrency shards
        in parallel"; SQLite is single-writer so shards import sequentially
        here regardless of that config value."""
        import duckdb

        rows_df = duckdb.sql(f"SELECT * FROM read_parquet('{parquet_path}')").df()
        if rows_df.empty:
            return 0

        columns = list(rows_df.columns)
        placeholders = ", ".join("?" for _ in columns)
        col_list = ", ".join(columns)
        records = [
            tuple(_sqlite_safe(v) for v in row)
            for row in rows_df.itertuples(index=False, name=None)
        ]

        self.conn.executemany(
            f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})", records
        )
        return len(records)

    def build_indexes(self, table: str, indexes: list[dict]) -> None:
        for idx in indexes:
            unique = "UNIQUE " if idx.get("unique") else ""
            columns = ", ".join(idx["columns"])
            self.conn.execute(
                f"CREATE {unique}INDEX IF NOT EXISTS {idx['name']} ON {table}({columns})"
            )

    def rotate_and_swap(self, live_table: str, staging_table: str) -> None:
        """Atomic swap with two-cycle rotation, in one transaction:
          1. Drop the table from two cycles ago (<live>_old_old), if present
          2. Demote this cycle's about-to-be-replaced backup (<live>_old
             from the PREVIOUS run) to <live>_old_old
          3. Rename <live> -> <live>_old (this run's outgoing table)
          4. Rename <staging> -> <live> (this run's new table, now live)

        Steps 1-2 are conditional (skipped on the first run or two, before
        those tables exist) since ALTER TABLE RENAME has no IF EXISTS.
        """
        old_table = f"{live_table}_old"
        old_old_table = f"{live_table}_old_old"

        self.conn.execute("BEGIN")
        try:
            if self.table_exists(old_old_table):
                self.conn.execute(f"DROP TABLE {old_old_table}")
            if self.table_exists(old_table):
                self.conn.execute(f"ALTER TABLE {old_table} RENAME TO {old_old_table}")
            self.conn.execute(f"ALTER TABLE {live_table} RENAME TO {old_table}")
            self.conn.execute(f"ALTER TABLE {staging_table} RENAME TO {live_table}")
            self.conn.execute("COMMIT")
        except Exception:
            self.conn.execute("ROLLBACK")
            raise

    def close(self) -> None:
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
