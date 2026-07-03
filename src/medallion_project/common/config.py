from dataclasses import dataclass


@dataclass(frozen=True)
class LocalConfig:
    raw_orders_path: str = "data/raw/orders.csv"

    bronze_orders_path: str = "data/bronze/orders"

    silver_orders_path: str = "data/silver/orders"

    gold_daily_sales_path: str = "data/gold/daily_sales"