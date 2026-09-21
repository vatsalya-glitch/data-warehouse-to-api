"""
CLI: python -m pipeline.lookup ORDER-00001

Queries the serving database directly — this is what a real application
or support tool would do: a single indexed point lookup, no warehouse
query involved.
"""

import json
import sys

from pipeline.config import load_config, resolve_path
from pipeline.serving_db import ServingDB


def lookup_order(order_id: str) -> dict | None:
    _, infra_config = load_config()
    sqlite_path = resolve_path(infra_config["serving_db"]["sqlite_path"])

    with ServingDB(sqlite_path) as db:
        cursor = db.execute(
            "SELECT * FROM live_order_lookup WHERE order_id = ?", (order_id,)
        )
        row = cursor.fetchone()
        if row is None:
            return None
        columns = [description[0] for description in cursor.description]
        return dict(zip(columns, row))


def main() -> None:
    if len(sys.argv) != 2:
        print("Usage: python -m pipeline.lookup <order_id>")
        print("Example: python -m pipeline.lookup ORDER-00001")
        sys.exit(1)

    order_id = sys.argv[1]
    result = lookup_order(order_id)

    if result is None:
        print(f"No order found with order_id = {order_id}")
        sys.exit(1)

    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
