"""
Generate synthetic "raw" source data and load it into the warehouse (DuckDB).

This stands in for tables that would already exist in a real company's
warehouse (raw.ecommerce_orders, raw.ecommerce_customers, etc). Running it
is what makes the rest of the pipeline have something real to build from.

Deterministic: seeded, so re-running produces the same data (useful for
tests and for reproducing a bug report).
"""

import random
from datetime import datetime, timedelta

import duckdb
from faker import Faker

SEED = 20240101
N_CUSTOMERS = 300
N_ORDERS = 800
LOYALTY_TIERS = ["bronze", "silver", "gold", "platinum"]
ORDER_STATUSES = ["placed", "processing", "shipped", "delivered", "cancelled"]
CARRIERS = ["ups", "fedex", "usps", "dhl"]
DELIVERY_STATUSES = ["label_created", "in_transit", "out_for_delivery", "delivered"]
TICKET_STATUSES = ["open", "pending", "closed"]


def _random_recent_datetime(rng: random.Random, days_back: int) -> datetime:
    return datetime.now() - timedelta(
        days=rng.randint(0, days_back),
        hours=rng.randint(0, 23),
        minutes=rng.randint(0, 59),
    )


def generate_customers(fake: Faker, rng: random.Random) -> list[dict]:
    customers = []
    for i in range(1, N_CUSTOMERS + 1):
        signup = _random_recent_datetime(rng, 700)
        customers.append(
            {
                "customer_id": f"CUST-{i:05d}",
                "customer_name": fake.name(),
                "customer_email": fake.email(),
                "loyalty_tier": rng.choices(
                    LOYALTY_TIERS, weights=[40, 30, 20, 10]
                )[0],
                "signup_date": signup.date(),
                # a handful of customers have a later profile update than signup
                "updated_at": signup + timedelta(days=rng.randint(0, 30)),
            }
        )
    return customers


def generate_orders(rng: random.Random, customer_ids: list[str]) -> list[dict]:
    orders = []
    for i in range(1, N_ORDERS + 1):
        order_dt = _random_recent_datetime(rng, 120)
        orders.append(
            {
                "order_id": f"ORDER-{i:05d}",
                "customer_id": rng.choice(customer_ids),
                "order_date": order_dt.date(),
                "order_status": rng.choices(
                    ORDER_STATUSES, weights=[10, 15, 25, 40, 10]
                )[0],
                "total_amount": round(rng.uniform(9.99, 499.99), 2),
                "updated_at": order_dt + timedelta(hours=rng.randint(0, 48)),
            }
        )
    return orders


def generate_order_items(fake: Faker, rng: random.Random, order_ids: list[str]) -> list[dict]:
    items = []
    item_id = 1
    for order_id in order_ids:
        for _ in range(rng.randint(1, 4)):
            items.append(
                {
                    "order_item_id": f"ITEM-{item_id:06d}",
                    "order_id": order_id,
                    "product_id": f"PROD-{rng.randint(1, 500):04d}",
                    "quantity": rng.randint(1, 3),
                    "unit_price": round(rng.uniform(4.99, 199.99), 2),
                }
            )
            item_id += 1
    return items


def generate_shipment_events(rng: random.Random, orders: list[dict]) -> list[dict]:
    """~85% of orders have at least one shipment event; some have several
    (only the latest should survive the domain query's dedup)."""
    events = []
    event_id = 1
    for order in orders:
        if order["order_status"] == "placed":
            continue  # too early to have shipped
        if rng.random() > 0.85:
            continue  # simulate some orders with no shipment record yet
        n_events = rng.randint(1, 3)
        ship_date = order["order_date"] + timedelta(days=rng.randint(1, 3))
        for seq in range(n_events):
            events.append(
                {
                    "event_id": f"EVT-{event_id:06d}",
                    "order_id": order["order_id"],
                    "carrier": rng.choice(CARRIERS),
                    "tracking_number": fake_tracking_number(rng),
                    "ship_date": ship_date,
                    "delivery_status": DELIVERY_STATUSES[
                        min(seq, len(DELIVERY_STATUSES) - 1)
                    ],
                    "event_timestamp": datetime.combine(ship_date, datetime.min.time())
                    + timedelta(hours=seq * 6 + rng.randint(0, 5)),
                }
            )
            event_id += 1
    return events


def fake_tracking_number(rng: random.Random) -> str:
    return "".join(rng.choices("0123456789", k=12))


def generate_support_tickets(rng: random.Random, customer_ids: list[str]) -> list[dict]:
    """~20% of customers have at least one ticket; most are closed, some open."""
    tickets = []
    ticket_id = 1
    for customer_id in customer_ids:
        if rng.random() > 0.20:
            continue
        for _ in range(rng.randint(1, 2)):
            tickets.append(
                {
                    "ticket_id": f"TICKET-{ticket_id:05d}",
                    "customer_id": customer_id,
                    "ticket_status": rng.choices(
                        TICKET_STATUSES, weights=[15, 10, 75]
                    )[0],
                    "created_at": _random_recent_datetime(rng, 90),
                }
            )
            ticket_id += 1
    return tickets


def seed_warehouse(duckdb_path: str) -> None:
    """Generate synthetic data and load it into warehouse.duckdb as raw_* tables."""
    fake = Faker()
    Faker.seed(SEED)
    rng = random.Random(SEED)

    customers = generate_customers(fake, rng)
    orders = generate_orders(rng, [c["customer_id"] for c in customers])
    order_items = generate_order_items(fake, rng, [o["order_id"] for o in orders])
    shipment_events = generate_shipment_events(rng, orders)
    support_tickets = generate_support_tickets(rng, [c["customer_id"] for c in customers])

    con = duckdb.connect(duckdb_path)
    try:
        import pandas as pd

        con.register("customers_df", pd.DataFrame(customers))
        con.execute("CREATE OR REPLACE TABLE raw_customers AS SELECT * FROM customers_df")

        con.register("orders_df", pd.DataFrame(orders))
        con.execute("CREATE OR REPLACE TABLE raw_orders AS SELECT * FROM orders_df")

        con.register("order_items_df", pd.DataFrame(order_items))
        con.execute("CREATE OR REPLACE TABLE raw_order_items AS SELECT * FROM order_items_df")

        con.register("shipment_events_df", pd.DataFrame(shipment_events))
        con.execute(
            "CREATE OR REPLACE TABLE raw_shipment_events AS SELECT * FROM shipment_events_df"
        )

        con.register("support_tickets_df", pd.DataFrame(support_tickets))
        con.execute(
            "CREATE OR REPLACE TABLE raw_support_tickets AS SELECT * FROM support_tickets_df"
        )

        print(f"Seeded {duckdb_path}:")
        for table in [
            "raw_customers",
            "raw_orders",
            "raw_order_items",
            "raw_shipment_events",
            "raw_support_tickets",
        ]:
            count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"  {table}: {count} rows")
    finally:
        con.close()


if __name__ == "__main__":
    import sys
    from pipeline.config import load_config

    dag_config, infra_config = load_config()
    duckdb_path = infra_config["warehouse"]["duckdb_path"]
    seed_warehouse(duckdb_path)
