"""
Phase 1 — Preflight. Runs before any real data is written.

For every domain query declared in dag_config.yaml:
  1. Run its DDL (CREATE OR REPLACE TABLE) to guarantee a fresh, empty
     schema — this is real execution, but only ever touches an empty
     placeholder table, never data.
  2. Dry-run its SELECT body (EXPLAIN, no data scanned) to catch a
     type/column mismatch immediately.
  3. Compare the DDL's declared columns against what the SELECT actually
     produces — schema drift between "what we said this table looks like"
     and "what the query actually returns" fails loudly here.

Then, with all domain tables now existing (empty) with the right schema:
  4. DESCRIBE the lookup query to confirm it resolves against them.
  5. Coverage check: every non-bookkeeping column declared in a domain's
     DDL must appear somewhere in the lookup query's own SQL text. This is
     a text-scan heuristic, not full semantic analysis — it catches the
     concrete failure mode "a column was added to a domain but nobody
     wired it into the join," which is the one this check exists for.
"""

import re
from pathlib import Path

from pipeline.config import SQL_DIR
from pipeline.warehouse import Warehouse, extract_select_body


class PreflightError(Exception):
    """Raised when any preflight check fails. The caller should stop before
    Phase 2 — no domain table has real data in it yet, so there's nothing
    to roll back."""


def _ddl_path_for(division_sql_dir: Path, domain_query: str) -> Path:
    return division_sql_dir / domain_query.replace("/domain/", "/ddl/")


def run_preflight(warehouse: Warehouse, dag_config: dict, division_name: str) -> None:
    division_config = dag_config["division"][division_name]
    division_sql_dir = SQL_DIR / division_name
    window_days = dag_config["rolling_window_days"]

    domain_declared_columns: dict[str, set[str]] = {}

    for domain_query in division_config["domain_queries"]:
        domain_name = Path(domain_query).stem

        ddl_path = _ddl_path_for(division_sql_dir, domain_query)
        if not ddl_path.exists():
            raise PreflightError(
                f"Missing schema contract for domain '{domain_name}': {ddl_path}"
            )
        warehouse.execute(warehouse.read_sql(ddl_path))
        table_name = f"sample_{domain_name}"
        declared_columns = set(warehouse.describe_columns(f"SELECT * FROM {table_name}"))

        domain_sql = warehouse.read_sql(
            division_sql_dir / domain_query, rolling_window_days=window_days
        )
        select_body = extract_select_body(domain_sql)

        try:
            warehouse.dry_run(select_body)
        except Exception as exc:
            raise PreflightError(
                f"Domain query for '{domain_name}' failed dry-run validation: {exc}"
            ) from exc

        actual_columns = set(warehouse.describe_columns(select_body))

        if declared_columns != actual_columns:
            missing = declared_columns - actual_columns
            extra = actual_columns - declared_columns
            raise PreflightError(
                f"Schema drift in domain '{domain_name}': DDL declares "
                f"{sorted(declared_columns)} but the query produces "
                f"{sorted(actual_columns)}. Missing from query: {sorted(missing) or 'none'}. "
                f"Not in DDL: {sorted(extra) or 'none'}."
            )

        domain_declared_columns[domain_name] = declared_columns

    # Coverage check: every domain column (except _loaded_at, which the
    # lookup regenerates fresh) should appear somewhere in the lookup SQL.
    lookup_path = division_sql_dir / division_config["lookup_query"]
    lookup_text = lookup_path.read_text()

    for domain_name, columns in domain_declared_columns.items():
        for column in columns:
            if column == "_loaded_at":
                continue
            if not re.search(rf"\b{re.escape(column)}\b", lookup_text):
                raise PreflightError(
                    f"Coverage check failed: column '{column}' exists in domain "
                    f"'{domain_name}' but was not found anywhere in "
                    f"{lookup_path.name} — it looks like it was never wired "
                    f"into the final lookup."
                )

    # Now that every domain table exists (empty) with the declared schema,
    # confirm the lookup query itself resolves against them.
    try:
        warehouse.dry_run(lookup_text)
    except Exception as exc:
        raise PreflightError(f"Lookup query failed dry-run validation: {exc}") from exc
