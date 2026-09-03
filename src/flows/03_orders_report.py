import csv
import random
from pathlib import Path

from prefect import flow, get_run_logger, task

DATA_FILE = Path("./data/orders.csv")


@task
def generate_orders(num_orders: int = 10):
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)

    products = [
        "Laptop",
        "Keyboard",
        "Mouse",
        "Monitor",
    ]

    orders = [
        {
            "order_id": i,
            "product": random.choice(products),
            "quantity": random.randint(1, 5),
            "amount": round(random.uniform(10, 1000), 2),
        }
        for i in range(1, num_orders + 1)
    ]

    with DATA_FILE.open("w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["order_id", "product", "quantity", "amount"],
        )
        writer.writeheader()
        writer.writerows(orders)

    return DATA_FILE


@task
def read_orders(file_path: Path):
    logger = get_run_logger()

    with file_path.open(newline="") as f:
        reader = csv.DictReader(f)

        for order in reader:
            logger.info(
                f"Order {order['order_id']}: "
                f"{order['product']} × {order['quantity']} "
                f"= ${order['amount']}"
            )


@flow(name="orders")
def orders_flow(num_orders: int = 10):
    # Task 1
    file_path = generate_orders(num_orders)

    # Task 2
    # Prefect automatically creates the dependency:
    # generate_orders -> read_orders
    read_orders(file_path)


if __name__ == "__main__":
    orders_flow(20)
