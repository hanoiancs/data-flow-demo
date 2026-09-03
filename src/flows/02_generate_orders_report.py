# read_orders.py

import csv

from prefect import flow, get_run_logger
from prefect.events.schemas.deployment_triggers import DeploymentEventTrigger

from assets.orders_asset import ORDERS_ASSET, ORDERS_FILE

orders_updated = DeploymentEventTrigger(
    expect={"prefect.asset.materialization.succeeded"},
    match={
        "prefect.resource.id": ORDERS_ASSET.key,
    },
)


@flow(name="read-orders")
def read_orders_flow():
    logger = get_run_logger()

    logger.info(f"Reading {ORDERS_FILE}")

    with ORDERS_FILE.open(newline="") as f:
        reader = csv.DictReader(f)

        for order in reader:
            logger.info(
                f"Order {order['order_id']}: "
                f"{order['product']} x {order['quantity']} "
                f"= ${order['amount']}"
            )


if __name__ == "__main__":
    read_orders_flow.serve(
        name="read-orders",
        triggers=[orders_updated],
    )
