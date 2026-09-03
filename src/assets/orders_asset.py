from pathlib import Path

from prefect.assets import Asset, AssetProperties

ORDERS_FILE = Path("./data/orders.csv").resolve()

ORDERS_ASSET = Asset(
    key=f"file://{ORDERS_FILE}",
    properties=AssetProperties(
        name="Orders CSV",
        description="Generated order data",
    ),
)
