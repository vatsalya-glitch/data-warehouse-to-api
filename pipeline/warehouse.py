"""Warehouse operations (DuckDB) — the real implementation behind Phase 1 and 2.

DuckDB stands in for the production warehouse (BigQuery/Snowflake/Redshift)
so the pipeline runs locally with no server. See include/README.md.
"""

import re
from pathlib import Path

import duckdb

# Matches the SELECT body of a `CREATE OR REPLACE TABLE <name> AS <select>`
# statement, so preflight can DESCRIBE/EXPLAIN just the query part without
# needing a second, hand-maintained copy of each domain query.
_CTAS_SELECT_PATTERN = re.compile(r"(?is)\bAS\b\s*(SELECT\b.*)")
_LINE_COMMENT_PATTERN = re.compile(r"--[^\n]*")


def _strip_line_comments(sql_text: str) -> str:
    """Remove `-- ...` comments so they can't accidentally contain text
    (like the words "AS SELECT") that confuses statement parsing below."""
    return _LINE_COMMENT_PATTERN.sub("", sql_text)


def extract_select_body(sql_text: str) -> str:
    """Return the SELECT portion of a CREATE [OR REPLACE] TABLE ... AS SELECT
    statement, or the text unchanged if it's already a bare SELECT."""
    stripped = _strip_line_comments(sql_text).strip().rstrip(";")
    match = _CTAS_SELECT_PATTERN.search(stripped)
    return match.group(1) if match else stripped


class Warehouse:
    """Thin wrapper around a DuckDB connection with the operations the
    three-phase pipeline needs: dry-run validation, column introspection,
    full-refresh execution, row/uniqueness checks, and export."""

    def __init__(self, duckdb_path: str | Path):
        self.conn = duckdb.connect(str(duckdb_path))

    def read_sql(self, path: Path, **format_kwargs) -> str:
        text = path.read_text()
        return text.format(**format_kwargs) if format_kwargs else text

    def dry_run(self, sql: str) -> None:
        """Validate a statement (schema/columns/types) without writing or
        scanning data. Raises duckdb.Error on a schema mismatch — this is
        Phase 1's "fail before you write" check."""
        self.conn.execute(f"EXPLAIN {sql}")

    def describe_columns(self, select_sql: str) -> list[str]:
        """Column names a SELECT would produce, without executing it."""
        df = self.conn.execute(f"DESCRIBE {select_sql}").fetchdf()
        return df["column_name"].tolist()

    def execute(self, sql: str) -> None:
        self.conn.execute(sql)

    def row_count(self, table: str) -> int:
        return self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

    def distinct_count(self, table: str, column: str) -> int:
        return self.conn.execute(
            f"SELECT COUNT(DISTINCT {column}) FROM {table}"
        ).fetchone()[0]

    def table_exists(self, table: str) -> bool:
        result = self.conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            [table],
        ).fetchone()[0]
        return result > 0

    def export_parquet(self, table: str, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.conn.execute(f"COPY {table} TO '{path}' (FORMAT PARQUET)")

    def close(self) -> None:
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
