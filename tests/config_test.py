from medallion_project.common.config import LocalConfig

config = LocalConfig()

print(config.raw_orders_path)
print(config.bronze_orders_path)
print(config.silver_orders_path)
print(config.gold_daily_sales_path)