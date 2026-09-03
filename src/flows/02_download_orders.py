# generate_orders.py

import csv
import random
from datetime import datetime

from prefect import flow, get_run_logger
from prefect.assets import materialize

from assets.orders_asset import ORDERS_ASSET, ORDERS_FILE


@materialize(ORDERS_ASSET)
def generate_orders(num_orders: int = 10):
    logger = get_run_logger()

    ORDERS_FILE.parent.mkdir(parents=True, exist_ok=True)

    products = [
        "Laptop",
        "Keyboard",
        "Mouse",
        "Monitor",
        "Headphones",
    ]

    orders = [
        {
            "order_id": i,
            "product": random.choice(products),
            "quantity": random.randint(1, 5),
            "amount": round(random.uniform(10, 1000), 2),
            "created_at": datetime.now().isoformat(),
        }
        for i in range(1, num_orders + 1)
    ]

    with ORDERS_FILE.open("w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "order_id",
                "product",
                "quantity",
                "amount",
                "created_at",
            ],
        )

        writer.writeheader()
        writer.writerows(orders)

    logger.info(f"Generated {num_orders} orders")


@flow(name="generate-orders")
def generate_orders_flow(num_orders: int = 10):
    generate_orders(num_orders)


if __name__ == "__main__":
    generate_orders_flow(20)
